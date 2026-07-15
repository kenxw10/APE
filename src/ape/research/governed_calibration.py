from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from ape.db.models import CalibrationRun, ResearchCandidate
from ape.repositories.inputs import StrategyConfigVersionInput
from ape.repositories.strategy_v2 import StrategyV2Repository
from ape.research import CALIBRATION_SCHEMA_VERSION, REPLAY_SCHEMA_VERSION
from ape.research.calibration import (
    LIFECYCLE_DRAFT,
    CalibrationResult,
    CandidateSpec,
    bounded_candidate_specs,
    complete_search_space_snapshot,
    run_bounded_calibration,
)
from ape.research.cohort import (
    CALIBRATION_EPOCH_MARKET_COUNT,
    CalibrationInputLimitError,
    CleanCalibrationCohort,
    build_clean_calibration_cohort,
    completed_epoch_size,
    extract_compact_calibration_events,
    next_epoch_market_count,
)
from ape.research.replay import ReplayTrade
from ape.research.repository import FrozenReplayProgress, ResearchRepository
from ape.strategy.momentum_v2 import V2_ARCHITECTURE_VERSION, V2_FEATURE_SCHEMA_VERSION

CALIBRATION_CANDIDATE_BATCH_SIZE = 8
CALIBRATION_FRONTIER_LIMIT = 20
CALIBRATION_RESULT_CLASSIFICATIONS = frozenset(
    {
        "INSUFFICIENT_CLEAN_DATA",
        "NO_CANDIDATE_SIGNALS",
        "SIGNALS_WITHOUT_EXECUTABLE_FILLS",
        "FILLS_WITHOUT_CLOSED_TRADES",
        "CLOSED_TRADES_WITHOUT_POSITIVE_HOLDOUT",
        "POSITIVE_RESEARCH_CANDIDATE",
        "CALIBRATION_BLOCKED",
        "CALIBRATION_FAILED",
    }
)
_RAW_METRIC_KEYS = frozenset(
    {
        "score_margin_distribution",
        "edge_margin_distribution",
        "desired_ask_distribution",
        "spread_distribution",
        "depth_distribution",
        "per_market_maxima",
        "top_near_miss_samples",
        "market_net_pnl",
    }
)
_FINALIST_EVIDENCE_PHASE = "FINALIST_EVIDENCE_COMMITTED"
_NON_COMPLETED_STATUSES = frozenset({"CALIBRATION_BLOCKED", "CALIBRATION_FAILED"})


@dataclass(frozen=True)
class GovernedCalibrationResult:
    run_id: str | None
    status: str
    classification: str
    eligible_market_count: int
    completed_epoch_size: int
    next_epoch_market_count: int
    calibration_due: bool
    cohort_hash: str
    epoch_hash: str | None
    search_space_hash: str | None
    candidate_count: int
    candidates_completed: int
    reused_existing_run: bool
    frontier: dict[str, Any] | None
    cohort_summary: dict[str, Any]

    def metadata(self) -> dict[str, Any]:
        return {
            "calibration_epoch_size": self.completed_epoch_size,
            "calibration_cohort_hash": self.cohort_hash,
            "calibration_epoch_hash": self.epoch_hash,
            "calibration_due": self.calibration_due,
            "calibration_run_id": self.run_id,
            "calibration_status": self.status,
            "calibration_candidate_count": self.candidate_count,
            "calibration_candidates_completed": self.candidates_completed,
            "calibration_candidate_batch_size": CALIBRATION_CANDIDATE_BATCH_SIZE,
            "calibration_reused_existing_run": self.reused_existing_run,
            "calibration_result_classification": self.classification,
            "current_eligible_market_count": self.eligible_market_count,
            "next_epoch_market_count": self.next_epoch_market_count,
            "calibration_search_space_hash": self.search_space_hash,
            "calibration_cohort_summary": deepcopy(self.cohort_summary),
        }


CalibrationEvaluator = Callable[..., CalibrationResult]
ProgressCallback = Callable[[dict[str, Any]], None]


def _lineage_runs(
    repository: ResearchRepository,
    *,
    code_commit_sha: str,
    search_space_lineage_hash: str,
) -> list[CalibrationRun]:
    return [
        run
        for run in repository.list_calibration_runs_for_code_commit(code_commit_sha)
        if isinstance(run.partition_manifest, dict)
        and run.partition_manifest.get("search_space_lineage_hash")
        == search_space_lineage_hash
        and isinstance(run.partition_manifest.get("epoch_size"), int)
    ]


def _select_due_epoch(
    runs: list[CalibrationRun],
    *,
    eligible_market_count: int,
) -> tuple[int, bool, CalibrationRun | None]:
    capacity = completed_epoch_size(eligible_market_count)
    if capacity < CALIBRATION_EPOCH_MARKET_COUNT:
        return capacity, False, None
    in_progress = sorted(
        (
            run
            for run in runs
            if run.finished_at is None
            and int(run.partition_manifest["epoch_size"]) <= capacity
        ),
        key=lambda run: (
            int(run.partition_manifest["epoch_size"]),
            run.started_at,
            run.id,
        ),
    )
    if in_progress:
        run = in_progress[0]
        return int(run.partition_manifest["epoch_size"]), True, run

    completed = {
        int(run.partition_manifest["epoch_size"])
        for run in runs
        if run.finished_at is not None
        and run.status not in _NON_COMPLETED_STATUSES
        and int(run.partition_manifest["epoch_size"]) <= capacity
    }
    for epoch_size in range(
        CALIBRATION_EPOCH_MARKET_COUNT,
        capacity + 1,
        CALIBRATION_EPOCH_MARKET_COUNT,
    ):
        if epoch_size not in completed:
            return epoch_size, True, None

    if not completed:
        return CALIBRATION_EPOCH_MARKET_COUNT, True, None
    latest_epoch_size = max(completed)
    latest_run = max(
        (
            run
            for run in runs
            if run.finished_at is not None
            and run.status not in _NON_COMPLETED_STATUSES
            and int(run.partition_manifest["epoch_size"]) == latest_epoch_size
        ),
        key=lambda run: (run.finished_at, run.id),
    )
    return latest_epoch_size, False, latest_run


def run_governed_calibration(
    session: Session,
    *,
    snapshot,
    replay_run_id: str,
    baseline_config_version_id: str,
    code_commit_sha: str,
    checked_at: datetime,
    progress_callback: ProgressCallback | None = None,
    candidate_evaluator: CalibrationEvaluator = run_bounded_calibration,
    candidate_specs: tuple[CandidateSpec, ...] | None = None,
) -> GovernedCalibrationResult:
    """Evaluate one immutable clean-cohort epoch in durable candidate batches."""
    repository = ResearchRepository(session)

    def report_reader(progress: FrozenReplayProgress) -> None:
        _report(
            progress_callback,
            calibration_events_scanned=progress.events_scanned,
            calibration_pages_scanned=progress.pages_completed,
            calibration_partitions_scanned=progress.partitions_completed,
            calibration_max_page_size=progress.max_page_size,
            calibration_dataset_watermark=progress.watermark_id,
            calibration_last_progress_at=_iso(datetime.now(UTC)),
        )

    cohort = build_clean_calibration_cohort(
        session,
        snapshot=snapshot,
        baseline_config_version_id=baseline_config_version_id,
        code_commit_sha=code_commit_sha,
        progress_callback=report_reader,
    )
    eligible_count = int(cohort.manifest["eligible_market_count"])
    supplied_specs = tuple(candidate_specs) if candidate_specs is not None else None
    lineage_seed = f"calibration-lineage-{code_commit_sha}"
    lineage_specs = supplied_specs or bounded_candidate_specs(lineage_seed)
    lineage_snapshot = complete_search_space_snapshot(lineage_seed, lineage_specs)
    search_space_lineage_hash = _hash(
        {
            "code_commit_sha": code_commit_sha,
            "search_space_hash": lineage_snapshot["snapshot_sha256"],
        }
    )
    lineage_runs = _lineage_runs(
        repository,
        code_commit_sha=code_commit_sha,
        search_space_lineage_hash=search_space_lineage_hash,
    )
    target_epoch_size, calibration_due, lineage_run = _select_due_epoch(
        lineage_runs,
        eligible_market_count=eligible_count,
    )
    if target_epoch_size < CALIBRATION_EPOCH_MARKET_COUNT:
        next_count = CALIBRATION_EPOCH_MARKET_COUNT
        run, reused = _persist_insufficient_run(
            repository,
            replay_run_id=replay_run_id,
            cohort=cohort,
            code_commit_sha=code_commit_sha,
            checked_at=checked_at,
            next_count=next_count,
        )
        session.commit()
        return GovernedCalibrationResult(
            run.calibration_run_id,
            run.status,
            "INSUFFICIENT_CLEAN_DATA",
            eligible_count,
            0,
            next_count,
            False,
            cohort.manifest["cohort_hash"],
            None,
            None,
            256,
            0,
            reused,
            None,
            _cohort_summary(cohort.manifest),
        )

    epoch = (
        deepcopy(lineage_run.partition_manifest)
        if lineage_run is not None
        else cohort.epoch_manifest(target_epoch_size)
    )
    epoch_size = int(epoch["epoch_size"])
    next_count = next_epoch_market_count(epoch_size)
    identity_seed = _hash(
        {
            "epoch_hash": epoch["epoch_hash"],
            "code_commit_sha": code_commit_sha,
            "calibration_schema_version": CALIBRATION_SCHEMA_VERSION,
        }
    )
    seed_run_id = f"calibration-{identity_seed[:24]}"
    specs = supplied_specs or bounded_candidate_specs(seed_run_id)
    search_snapshot = complete_search_space_snapshot(seed_run_id, specs)
    search_hash = str(search_snapshot["snapshot_sha256"])
    run_id = "calibration-" + _hash(
        {
            "epoch_hash": epoch["epoch_hash"],
            "search_space_hash": search_hash,
            "code_commit_sha": code_commit_sha,
        }
    )[:24]
    existing = repository.get_calibration_run(run_id)
    if not calibration_due and existing is not None and existing.finished_at is not None:
        frontier = _frontier_payload(existing)
        return GovernedCalibrationResult(
            run_id,
            existing.status,
            existing.status,
            eligible_count,
            epoch_size,
            next_count,
            False,
            cohort.manifest["cohort_hash"],
            epoch["epoch_hash"],
            search_hash,
            len(specs),
            existing.evaluated_candidate_count,
            True,
            frontier,
            _cohort_summary(cohort.manifest),
        )

    run = existing or repository.create_calibration_run(
        {
            "calibration_run_id": run_id,
            "status": "RUNNING",
            "calibration_schema_version": CALIBRATION_SCHEMA_VERSION,
            "replay_run_id": replay_run_id,
            "dataset_hash": epoch["epoch_hash"],
            "code_commit_sha": code_commit_sha,
            "random_seed": int(hashlib.sha256(seed_run_id.encode()).hexdigest()[:8], 16),
            "search_space_snapshot": search_snapshot,
            "partition_manifest": {
                **epoch,
                "cohort_manifest": cohort.manifest,
                "search_space_hash": search_hash,
                "search_space_lineage_hash": search_space_lineage_hash,
                "current_eligible_market_count": eligible_count,
                "current_completed_epoch_size": epoch_size,
                "next_epoch_market_count": next_count,
                "calibration_due": calibration_due,
                "reader_progress": {},
            },
            "frozen_holdout_hash": None,
            "evaluated_candidate_count": 0,
            "selected_candidate_id": None,
            "training_metrics": {
                "current_candidate_id": None,
                "current_fold": None,
                "current_partition": "compact_extract",
            },
            "validation_metrics": {},
            "test_metrics": None,
            "holdout_metrics": None,
            "bootstrap_metrics": None,
            "penalties": None,
            "warnings": [],
            "blockers": [],
            "started_at": checked_at,
            "finished_at": None,
            "holdout_used_at": None,
        }
    )
    if existing is not None and existing.finished_at is None:
        recovery = _recover_finalist_state(
            session,
            repository,
            run,
            cohort=cohort,
            epoch=epoch,
            search_hash=search_hash,
            candidate_count=len(specs),
            checked_at=checked_at,
        )
        if recovery is not None:
            return recovery
        run.status = "RUNNING"
        run.blockers = []
    session.commit()
    _report(
        progress_callback,
        **_run_progress(run, cohort, epoch, search_hash, len(specs)),
    )
    try:
        events, reader_progress = extract_compact_calibration_events(
            session,
            snapshot=snapshot,
            cohort=cohort,
            epoch_manifest=epoch,
            progress_callback=report_reader,
        )
    except CalibrationInputLimitError as exc:
        _finish_blocked(run, checked_at, str(exc))
        session.commit()
        return _result_from_run(run, cohort, epoch, search_hash, len(specs), False)

    manifest = deepcopy(run.partition_manifest or {})
    manifest["reader_progress"] = reader_progress
    run.partition_manifest = manifest
    session.commit()
    epoch_outcomes = [
        cohort.outcomes_by_market[ticker] for ticker in epoch["ordered_market_tickers"]
    ]
    all_metrics = deepcopy(run.validation_metrics or {})
    start = min(run.evaluated_candidate_count, len(specs))
    for batch_start in range(start, len(specs), CALIBRATION_CANDIDATE_BATCH_SIZE):
        batch = specs[batch_start : batch_start + CALIBRATION_CANDIDATE_BATCH_SIZE]
        current_batch_start = batch_start

        def report_candidate(
            details: dict[str, Any], batch_offset: int = current_batch_start
        ) -> None:
            _report(
                progress_callback,
                **_run_progress(run, cohort, epoch, search_hash, len(specs)),
                calibration_candidate_index=batch_offset
                + int(details.get("candidate_index", 0)),
                calibration_current_candidate_id=details.get("current_candidate_id"),
                calibration_current_fold=details.get("current_fold"),
                calibration_current_partition=details.get("current_partition"),
                calibration_current_candidate_batch=(
                    batch_offset // CALIBRATION_CANDIDATE_BATCH_SIZE
                )
                + 1,
            )

        try:
            batch_result = candidate_evaluator(
                calibration_run_id=seed_run_id,
                events=events,
                outcomes=epoch_outcomes,
                candidate_specs=batch,
                evaluate_finalist=False,
                progress_callback=report_candidate,
            )
        except Exception as error:
            session.rollback()
            failed_run = repository.get_calibration_run(run_id)
            if failed_run is not None:
                failed_run.status = "CALIBRATION_FAILED"
                failed_run.blockers = [f"candidate_batch_failed:{type(error).__name__}"]
                failed_run.training_metrics = {
                    "current_candidate_id": batch[0].candidate_id,
                    "current_candidate_batch": (
                        batch_start // CALIBRATION_CANDIDATE_BATCH_SIZE
                    )
                    + 1,
                    "current_partition": "candidate_batch_failed",
                    "last_progress_at": _iso(datetime.now(UTC)),
                }
                session.commit()
            raise
        if batch_result.status != "COMPLETED":
            _finish_blocked(run, checked_at, "candidate_batch_incomplete")
            session.commit()
            return _result_from_run(run, cohort, epoch, search_hash, len(specs), False)
        for evaluated in batch_result.candidates:
            metrics = _compact_metrics(
                batch_result.candidate_metrics.get(evaluated.candidate_id, {})
            )
            all_metrics[evaluated.candidate_id] = metrics
            _persist_candidate(
                session,
                repository,
                run=run,
                baseline_config_version_id=baseline_config_version_id,
                candidate=evaluated,
                metrics=metrics,
                code_commit_sha=code_commit_sha,
                checked_at=checked_at,
            )
            _persist_partition_trades(
                repository,
                replay_run_id=replay_run_id,
                candidate=evaluated,
                baseline_config_version_id=baseline_config_version_id,
                trades=batch_result.candidate_partition_replay_trades.get(
                    evaluated.candidate_id,
                    {
                        "search_development": batch_result.candidate_replay_trades.get(
                            evaluated.candidate_id, ()
                        )
                    },
                ),
            )
        run.validation_metrics = all_metrics
        run.evaluated_candidate_count = batch_start + len(batch)
        run.training_metrics = {
            "current_candidate_id": batch[-1].candidate_id,
            "current_candidate_batch": (
                batch_start // CALIBRATION_CANDIDATE_BATCH_SIZE
            )
            + 1,
            "current_partition": "search_complete",
            "last_progress_at": _iso(datetime.now(UTC)),
        }
        session.commit()
        _report(
            progress_callback,
            **_run_progress(run, cohort, epoch, search_hash, len(specs)),
        )

    selected_id = select_finalist(all_metrics)
    selected = _resolved_candidate_spec(repository, specs, selected_id)
    if selected is not None and run.holdout_used_at is None:
        try:
            finalist_result = candidate_evaluator(
                calibration_run_id=seed_run_id,
                events=events,
                outcomes=epoch_outcomes,
                candidate_specs=(selected,),
                evaluate_finalist=True,
                progress_callback=None,
            )
        except Exception as error:
            session.rollback()
            failed_run = repository.get_calibration_run(run_id)
            if failed_run is not None:
                failed_run.status = "CALIBRATION_FAILED"
                failed_run.blockers = [f"finalist_evaluation_failed:{type(error).__name__}"]
                failed_run.training_metrics = {
                    "current_candidate_id": selected.candidate_id,
                    "current_partition": "finalist_failed",
                    "last_progress_at": _iso(datetime.now(UTC)),
                }
                session.commit()
            raise
        finalist_metrics = _compact_metrics(
            finalist_result.candidate_metrics.get(selected.candidate_id, {})
        )
        all_metrics[selected.candidate_id] = finalist_metrics
        run.holdout_used_at = checked_at
        run.frozen_holdout_hash = finalist_result.partition_manifest.get("holdout_hash")
        _persist_partition_trades(
            repository,
            replay_run_id=replay_run_id,
            candidate=selected,
            baseline_config_version_id=baseline_config_version_id,
            trades=finalist_result.candidate_partition_replay_trades.get(
                selected.candidate_id, {}
            ),
        )
        candidate_row = repository.get_candidate(selected.candidate_id)
        if candidate_row is not None:
            candidate_row.validation_metrics = finalist_metrics
            candidate_row.test_metrics = finalist_metrics.get("development_test")
            candidate_row.holdout_metrics = finalist_metrics.get("holdout")
        run.selected_candidate_id = selected_id
        # The search JSON was mutated in place; force the finalist replacement to persist.
        run.validation_metrics = deepcopy(all_metrics)
        run.holdout_metrics = finalist_metrics.get("holdout")
        run.bootstrap_metrics = finalist_metrics.get("bootstrap")
        run.penalties = finalist_metrics.get("penalties")
        run.training_metrics = {
            "current_candidate_id": selected_id,
            "current_partition": "finalist_evidence_committed",
            "finalist_phase": _FINALIST_EVIDENCE_PHASE,
            "last_progress_at": _iso(datetime.now(UTC)),
        }
        flag_modified(run, "validation_metrics")
        session.commit()

    return _finalize_run(
        session,
        run,
        cohort=cohort,
        epoch=epoch,
        search_hash=search_hash,
        candidate_count=len(specs),
        checked_at=checked_at,
        reused=False,
    )


def select_finalist(metrics: dict[str, dict[str, Any]]) -> str | None:
    ranked = [
        (candidate_id, values)
        for candidate_id, values in metrics.items()
        if values.get("status") == "EVALUATED"
        and candidate_id != "candidate-baseline-v2"
        and _int(values.get("entry_signal_count")) > 0
    ]
    if not ranked:
        return None
    ranked.sort(
        key=lambda item: (
            -_decimal(
                (item[1].get("penalties") or {}).get(
                    "adjusted_lower_confidence_expectancy", "-Infinity"
                )
            ),
            item[0],
        )
    )
    return ranked[0][0]


def classify_calibration_result(
    metrics: dict[str, dict[str, Any]], selected_id: str | None
) -> str:
    evaluated = [value for value in metrics.values() if value.get("status") == "EVALUATED"]
    if not evaluated:
        return "CALIBRATION_BLOCKED"
    if max((_int(value.get("entry_signal_count")) for value in evaluated), default=0) == 0:
        return "NO_CANDIDATE_SIGNALS"
    if max(
        (_int(value.get("executable_entry_fill_count")) for value in evaluated),
        default=0,
    ) == 0:
        return "SIGNALS_WITHOUT_EXECUTABLE_FILLS"
    if max((_int(value.get("closed_position_count")) for value in evaluated), default=0) == 0:
        return "FILLS_WITHOUT_CLOSED_TRADES"
    selected = metrics.get(selected_id or "", {})
    holdout = selected.get("holdout") if isinstance(selected.get("holdout"), dict) else {}
    bootstrap = (
        holdout.get("bootstrap", {}).get("net_pnl_per_market", {})
        if isinstance(holdout, dict)
        else {}
    )
    adjusted = _decimal(
        (selected.get("penalties") or {}).get(
            "adjusted_lower_confidence_expectancy", "-Infinity"
        )
    )
    if (
        _decimal(holdout.get("net_pnl_per_market", "0")) <= 0
        or _decimal(bootstrap.get("lower", "0")) <= 0
        or adjusted <= 0
    ):
        return "CLOSED_TRADES_WITHOUT_POSITIVE_HOLDOUT"
    return "POSITIVE_RESEARCH_CANDIDATE"


def build_candidate_frontier(
    metrics: dict[str, dict[str, Any]],
    *,
    selected_id: str | None,
    limit: int = CALIBRATION_FRONTIER_LIMIT,
) -> list[dict[str, Any]]:
    ranked = sorted(
        metrics,
        key=lambda candidate_id: (
            -_decimal(
                (metrics[candidate_id].get("penalties") or {}).get(
                    "adjusted_lower_confidence_expectancy", "-Infinity"
                )
            ),
            candidate_id,
        ),
    )
    required = ["candidate-baseline-v2", selected_id]
    selected_ids = list(dict.fromkeys([*ranked[: max(1, min(limit, 20))], *required]))
    rows = []
    for candidate_id in selected_ids:
        if candidate_id is None or candidate_id not in metrics:
            continue
        value = metrics[candidate_id]
        bootstrap = value.get("bootstrap", {}).get("net_pnl_per_market", {})
        rows.append(
            {
                "candidate_id": candidate_id,
                "model_type": value.get("model_type"),
                "entry_signal_count": value.get("entry_signal_count", 0),
                "executable_entry_fill_count": value.get(
                    "executable_entry_fill_count", 0
                ),
                "closed_position_count": value.get("closed_position_count", 0),
                "net_pnl_cents": value.get("net_pnl_cents", "0"),
                "net_pnl_per_market": value.get("net_pnl_per_market", "0"),
                "lower_95": bootstrap.get("lower"),
                "upper_95": bootstrap.get("upper"),
                "adjusted_lower_confidence_expectancy": (
                    value.get("penalties") or {}
                ).get("adjusted_lower_confidence_expectancy"),
                "entry_frequency_per_100_markets": value.get(
                    "entry_frequency_per_100_markets", "0"
                ),
                "signal_to_fill_rate": value.get("signal_to_fill_rate", "0"),
                "changed_parameter_count": value.get("changed_parameter_count", 0),
                "parameter_diff_from_baseline": value.get(
                    "parameter_diff_from_baseline", {}
                ),
                "fold_stability": (value.get("walk_forward_validation") or {}).get(
                    "fold_count", 0
                ),
                "holdout_status": "EVALUATED"
                if isinstance(value.get("holdout"), dict)
                else "NOT_EVALUATED",
                "qualification_reason": value.get("reason")
                or ("SELECTED_FINALIST" if candidate_id == selected_id else "RANKED"),
            }
        )
    return rows


def _persist_insufficient_run(
    repository: ResearchRepository,
    *,
    replay_run_id: str,
    cohort: CleanCalibrationCohort,
    code_commit_sha: str,
    checked_at: datetime,
    next_count: int,
) -> tuple[CalibrationRun, bool]:
    run_id = "calibration-insufficient-" + _hash(
        {
            "cohort_hash": cohort.manifest["cohort_hash"],
            "code_commit_sha": code_commit_sha,
        }
    )[:24]
    existing = repository.get_calibration_run(run_id)
    if existing is not None:
        return existing, True
    run = repository.create_calibration_run(
        {
            "calibration_run_id": run_id,
            "status": "INSUFFICIENT_CLEAN_DATA",
            "calibration_schema_version": CALIBRATION_SCHEMA_VERSION,
            "replay_run_id": replay_run_id,
            "dataset_hash": cohort.manifest["cohort_hash"],
            "code_commit_sha": code_commit_sha,
            "random_seed": int(_hash(run_id)[:8], 16),
            "search_space_snapshot": None,
            "partition_manifest": {
                "cohort_manifest": cohort.manifest,
                "current_eligible_market_count": cohort.manifest[
                    "eligible_market_count"
                ],
                "current_completed_epoch_size": 0,
                "next_epoch_market_count": next_count,
                "calibration_due": False,
            },
            "frozen_holdout_hash": None,
            "evaluated_candidate_count": 0,
            "selected_candidate_id": None,
            "training_metrics": None,
            "validation_metrics": {},
            "test_metrics": {
                "classification": "INSUFFICIENT_CLEAN_DATA",
                "frontier": [],
            },
            "holdout_metrics": None,
            "bootstrap_metrics": None,
            "penalties": None,
            "warnings": ["calibration_requires_50_clean_markets"],
            "blockers": ["insufficient_clean_calibration_markets"],
            "started_at": checked_at,
            "finished_at": checked_at,
            "holdout_used_at": None,
        }
    )
    return run, False


def _persist_candidate(
    session: Session,
    repository: ResearchRepository,
    *,
    run: CalibrationRun,
    baseline_config_version_id: str,
    candidate: CandidateSpec,
    metrics: dict[str, Any],
    code_commit_sha: str,
    checked_at: datetime,
) -> ResearchCandidate | None:
    if candidate.model_type == "BASELINE":
        return None
    artifact = candidate.model_artifact or {}
    config_version_id = f"research-{candidate.candidate_id}"
    StrategyV2Repository(session).ensure_config_version(
        StrategyConfigVersionInput(
            strategy_config_version_id=config_version_id,
            strategy_id=candidate.generated_strategy_id,
            architecture_version=V2_ARCHITECTURE_VERSION,
            feature_schema_version=V2_FEATURE_SCHEMA_VERSION,
            parameter_snapshot=candidate.parameters,
            parameter_hash=_hash(candidate.parameters),
            code_commit_sha=code_commit_sha,
            source="RESEARCH_CALIBRATION",
            parent_config_version_id=baseline_config_version_id,
            calibration_run_id=run.calibration_run_id,
            lifecycle_state=LIFECYCLE_DRAFT,
            approval_state="RESEARCH_ONLY",
            model_type=candidate.model_type,
            model_artifact_checksum=_hash(artifact),
            data_cutoff=checked_at,
            candidate_id=candidate.candidate_id,
        )
    )
    return repository.create_candidate(
        {
            "candidate_id": candidate.candidate_id,
            "strategy_config_version_id": config_version_id,
            "calibration_run_id": run.calibration_run_id,
            "parent_strategy_config_version_id": baseline_config_version_id,
            "generated_strategy_id": candidate.generated_strategy_id,
            "architecture_version": V2_ARCHITECTURE_VERSION,
            "feature_schema_version": V2_FEATURE_SCHEMA_VERSION,
            "replay_schema_version": REPLAY_SCHEMA_VERSION,
            "model_type": candidate.model_type,
            "parameter_snapshot": candidate.parameters,
            "feature_columns": list(candidate.feature_columns),
            "model_artifact": artifact,
            "model_artifact_checksum": _hash(artifact),
            "training_metrics": metrics.get("training"),
            "validation_metrics": metrics,
            "test_metrics": None,
            "holdout_metrics": None,
            "governance_report": {
                "status": "RESEARCH_ONLY",
                "automatic_promotion": False,
            },
            "lifecycle_state": LIFECYCLE_DRAFT,
            "eligibility_status": "RESEARCH_ONLY",
        }
    )


def _persist_partition_trades(
    repository: ResearchRepository,
    *,
    replay_run_id: str,
    candidate: CandidateSpec,
    baseline_config_version_id: str,
    trades: dict[str, Iterable[ReplayTrade]],
) -> None:
    config_version_id = (
        baseline_config_version_id
        if candidate.model_type == "BASELINE"
        else f"research-{candidate.candidate_id}"
    )
    for partition, rows in trades.items():
        for trade in rows:
            repository.insert_replay_trade(
                _trade_values(
                    replay_run_id=replay_run_id,
                    trade=trade,
                    candidate_id=candidate.candidate_id,
                    strategy_config_version_id=config_version_id,
                    evidence_partition=partition,
                )
            )


def _finalist_evidence_is_complete(
    repository: ResearchRepository, run: CalibrationRun
) -> bool:
    training = run.training_metrics if isinstance(run.training_metrics, dict) else {}
    if training.get("finalist_phase") != _FINALIST_EVIDENCE_PHASE:
        return False
    if run.holdout_used_at is None or not run.frozen_holdout_hash:
        return False
    selected_id = run.selected_candidate_id
    metrics = run.validation_metrics if isinstance(run.validation_metrics, dict) else {}
    selected_metrics = metrics.get(selected_id or "")
    if not isinstance(selected_metrics, dict):
        return False
    if not isinstance(selected_metrics.get("development_test"), dict):
        return False
    holdout = selected_metrics.get("holdout")
    if not isinstance(holdout, dict):
        return False
    if run.holdout_metrics != holdout:
        return False
    if run.bootstrap_metrics != selected_metrics.get("bootstrap"):
        return False
    if run.penalties != selected_metrics.get("penalties"):
        return False
    candidate = repository.get_candidate(selected_id or "")
    if candidate is None:
        return False
    if candidate.validation_metrics != selected_metrics:
        return False
    if candidate.test_metrics != selected_metrics.get("development_test"):
        return False
    if candidate.holdout_metrics != holdout:
        return False
    trades = repository.list_recent_replay_trades(
        limit=500_000,
        candidate_id=selected_id,
        replay_run_id=run.replay_run_id,
    )
    return all(
        isinstance(trade.measurements, dict)
        and trade.measurements.get("evidence_partition")
        in {"search_development", "development_test", "frozen_holdout"}
        for trade in trades
    )


def _finalize_run(
    session: Session,
    run: CalibrationRun,
    *,
    cohort: CleanCalibrationCohort,
    epoch: dict[str, Any],
    search_hash: str,
    candidate_count: int,
    checked_at: datetime,
    reused: bool,
) -> GovernedCalibrationResult:
    all_metrics = deepcopy(run.validation_metrics or {})
    selected_id = run.selected_candidate_id
    classification = classify_calibration_result(all_metrics, selected_id)
    frontier = build_candidate_frontier(all_metrics, selected_id=selected_id)
    next_experiment = (
        "STRUCTURAL_TRIGGER_EXPERIMENT_REQUIRED"
        if classification == "NO_CANDIDATE_SIGNALS"
        else None
    )
    run.status = classification
    run.selected_candidate_id = selected_id
    run.validation_metrics = all_metrics
    run.test_metrics = {
        "frontier_schema_version": "calibration_frontier_v1",
        "classification": classification,
        "next_experiment": next_experiment,
        "baseline_candidate_id": "candidate-baseline-v2",
        "selected_finalist_id": selected_id,
        "frontier": frontier,
    }
    selected_metrics = all_metrics.get(selected_id or "", {})
    run.holdout_metrics = selected_metrics.get("holdout")
    run.bootstrap_metrics = selected_metrics.get("bootstrap")
    run.penalties = selected_metrics.get("penalties")
    run.finished_at = checked_at
    run.training_metrics = {
        "current_candidate_id": selected_id,
        "current_candidate_batch": (candidate_count - 1) // CALIBRATION_CANDIDATE_BATCH_SIZE + 1,
        "current_partition": "completed",
        "last_progress_at": _iso(datetime.now(UTC)),
    }
    session.commit()
    return _result_from_run(
        run,
        cohort,
        epoch,
        search_hash,
        candidate_count,
        reused,
    )


def _recover_finalist_state(
    session: Session,
    repository: ResearchRepository,
    run: CalibrationRun,
    *,
    cohort: CleanCalibrationCohort,
    epoch: dict[str, Any],
    search_hash: str,
    candidate_count: int,
    checked_at: datetime,
) -> GovernedCalibrationResult | None:
    training = run.training_metrics if isinstance(run.training_metrics, dict) else {}
    phase = training.get("finalist_phase")
    if run.holdout_used_at is None and phase != _FINALIST_EVIDENCE_PHASE:
        return None
    if _finalist_evidence_is_complete(repository, run):
        return _finalize_run(
            session,
            run,
            cohort=cohort,
            epoch=epoch,
            search_hash=search_hash,
            candidate_count=candidate_count,
            checked_at=checked_at,
            reused=True,
        )
    run.status = "CALIBRATION_FAILED"
    run.blockers = ["finalist_evidence_incomplete_after_holdout"]
    run.finished_at = checked_at
    run.test_metrics = {
        "classification": "CALIBRATION_FAILED",
        "frontier": [],
    }
    run.training_metrics = {
        **training,
        "current_partition": "finalist_recovery_failed",
        "last_progress_at": _iso(datetime.now(UTC)),
    }
    session.commit()
    return _result_from_run(
        run,
        cohort,
        epoch,
        search_hash,
        candidate_count,
        True,
    )


def _trade_values(
    *,
    replay_run_id: str,
    trade: ReplayTrade,
    candidate_id: str,
    strategy_config_version_id: str,
    evidence_partition: str,
) -> dict[str, Any]:
    measurements = {
        **(trade.measurements if isinstance(trade.measurements, dict) else {}),
        "evidence_partition": evidence_partition,
    }
    return {
        "trade_id": (
            f"{replay_run_id}-{candidate_id}-{evidence_partition}-{trade.trade_id}"
        ),
        "replay_run_id": replay_run_id,
        "candidate_id": candidate_id,
        "strategy_config_version_id": strategy_config_version_id,
        "market_ticker": trade.market_ticker,
        "side": trade.side,
        "entry_decision_at": trade.entry_decision_at,
        "entry_fill_at": trade.entry_fill_at,
        "entry_limit": trade.entry_limit,
        "entry_fill_price": trade.entry_fill_price,
        "entry_fill_event_id": trade.entry_fill_event_id,
        "exit_trigger_at": trade.exit_trigger_at,
        "exit_intent_at": trade.exit_intent_at,
        "exit_fill_at": trade.exit_fill_at,
        "exit_limit": trade.exit_limit,
        "exit_fill_price": trade.exit_fill_price,
        "exit_fill_event_id": trade.exit_fill_event_id,
        "status": trade.status,
        "gross_pnl_cents": trade.gross_pnl_cents,
        "fee_cents": trade.fee_cents,
        "net_pnl_cents": trade.net_pnl_cents,
        "holding_duration_ms": trade.holding_duration_ms,
        "mfe_cents": trade.mfe_cents,
        "mae_cents": trade.mae_cents,
        "time_to_mfe_ms": trade.time_to_mfe_ms,
        "time_to_mae_ms": trade.time_to_mae_ms,
        "entry_reason": trade.entry_reason,
        "exit_reason": trade.exit_reason,
        "timing_tier": trade.timing_tier,
        "volatility_regime": measurements.get("volatility_regime"),
        "liquidity_regime": measurements.get("liquidity_regime"),
        "entry_feature_snapshot_id": measurements.get("entry_feature_snapshot_id"),
        "exit_feature_snapshot_id": measurements.get("exit_feature_snapshot_id"),
        "lifecycle_version": measurements.get("lifecycle_version"),
        "measurements": measurements,
    }


def _compact_metrics(value: dict[str, Any]) -> dict[str, Any]:
    result = {}
    for key, item in value.items():
        if key in _RAW_METRIC_KEYS:
            continue
        if key == "zero_entry_report" and isinstance(item, dict):
            result[key] = {
                name: nested
                for name, nested in item.items()
                if name not in _RAW_METRIC_KEYS and name != "distribution_sampling"
            }
            continue
        result[key] = deepcopy(item)
    return result


def _resolved_candidate_spec(
    repository: ResearchRepository,
    specs: tuple[CandidateSpec, ...],
    candidate_id: str | None,
) -> CandidateSpec | None:
    if candidate_id is None:
        return None
    spec = next((item for item in specs if item.candidate_id == candidate_id), None)
    if spec is None:
        return None
    row = repository.get_candidate(candidate_id)
    if row is None:
        return spec
    return CandidateSpec(
        candidate_id=row.candidate_id,
        generated_strategy_id=row.generated_strategy_id,
        model_type=row.model_type,
        parameters=deepcopy(row.parameter_snapshot),
        feature_columns=tuple(row.feature_columns or ()),
        model_artifact=deepcopy(row.model_artifact),
    )


def _finish_blocked(run: CalibrationRun, checked_at: datetime, reason: str) -> None:
    run.status = "CALIBRATION_BLOCKED"
    run.blockers = [reason]
    run.finished_at = checked_at
    run.test_metrics = {"classification": "CALIBRATION_BLOCKED", "frontier": []}


def _run_progress(
    run: CalibrationRun,
    cohort: CleanCalibrationCohort,
    epoch: dict[str, Any],
    search_hash: str,
    candidate_count: int,
) -> dict[str, Any]:
    return {
        "calibration_epoch_size": epoch["epoch_size"],
        "calibration_cohort_hash": cohort.manifest["cohort_hash"],
        "calibration_epoch_hash": epoch["epoch_hash"],
        "calibration_due": run.finished_at is None,
        "calibration_run_id": run.calibration_run_id,
        "calibration_status": run.status,
        "calibration_candidate_count": candidate_count,
        "calibration_candidates_completed": run.evaluated_candidate_count,
        "calibration_candidate_batch_size": CALIBRATION_CANDIDATE_BATCH_SIZE,
        "calibration_search_space_hash": search_hash,
        "calibration_last_progress_at": _iso(datetime.now(UTC)),
        "calibration_reused_existing_run": False,
    }


def _result_from_run(
    run: CalibrationRun,
    cohort: CleanCalibrationCohort,
    epoch: dict[str, Any],
    search_hash: str,
    candidate_count: int,
    reused: bool,
) -> GovernedCalibrationResult:
    return GovernedCalibrationResult(
        run.calibration_run_id,
        run.status,
        run.status,
        int(cohort.manifest["eligible_market_count"]),
        int(epoch["epoch_size"]),
        next_epoch_market_count(int(epoch["epoch_size"])),
        run.finished_at is None,
        cohort.manifest["cohort_hash"],
        epoch["epoch_hash"],
        search_hash,
        candidate_count,
        run.evaluated_candidate_count,
        reused,
        _frontier_payload(run),
        _cohort_summary(cohort.manifest),
    )


def _frontier_payload(run: CalibrationRun) -> dict[str, Any] | None:
    return deepcopy(run.test_metrics) if isinstance(run.test_metrics, dict) else None


def _cohort_summary(manifest: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "cohort_schema_version",
        "cohort_hash",
        "frozen_replay_watermark",
        "eligible_market_count",
        "eligible_feature_frame_count",
        "included_event_count_by_type",
        "architecture_version_distribution",
        "feature_schema_version_distribution",
        "replay_schema_version_distribution",
        "exclusion_counts_by_reason",
        "excluded_market_counts_by_reason",
        "maximum_relevant_event_gap_seconds",
        "source_completeness",
        "earliest_market_time",
        "latest_market_time",
        "earliest_event_time",
        "latest_event_time",
        "input_outcome_hash",
        "current_baseline_config_version",
        "code_commit_sha",
        "reader_progress",
    )
    return {key: deepcopy(manifest.get(key)) for key in keys}


def _report(callback: ProgressCallback | None, **values: Any) -> None:
    if callback is not None:
        callback(values)


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (ArithmeticError, ValueError):
        return Decimal("-Infinity")


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _iso(value: datetime) -> str:
    return (value if value.tzinfo is not None else value.replace(tzinfo=UTC)).isoformat()
