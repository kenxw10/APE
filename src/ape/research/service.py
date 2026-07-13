from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from ape.config import AppConfig
from ape.kalshi.client import KalshiRestClient
from ape.repositories.inputs import StrategyConfigVersionInput, WorkerHeartbeatInput
from ape.repositories.strategy_v2 import StrategyV2Repository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.research import (
    CALIBRATION_SCHEMA_VERSION,
    REPLAY_SCHEMA_VERSION,
    RESEARCH_LABEL_SCHEMA_VERSION,
)
from ape.research.archive import (
    ARCHIVE_MAX_BATCHES_PER_CYCLE,
    ARCHIVE_SOURCE_STAGES,
    ArchiveResult,
    archive_research_batch,
    archive_research_coverage,
    archive_research_events,
    archive_research_source_pending,
    reconcile_market_outcomes,
    refresh_research_archive_labels,
)
from ape.research.calibration import (
    LIFECYCLE_DRAFT,
    complete_search_space_snapshot,
    run_bounded_calibration,
)
from ape.research.replay import DeterministicReplayEngine, ReplayTrade
from ape.research.repository import ResearchRepository
from ape.strategy.momentum_v2 import (
    V2_ARCHITECTURE_VERSION,
    V2_FEATURE_SCHEMA_VERSION,
    V2_PARAMETERS,
    built_in_config_version,
    resolve_code_version,
)
from ape.worker.services import WORKER_SERVICE_RESEARCH

LOGGER = logging.getLogger(__name__)


class ResearchWorker:
    """Database-only research worker. It owns no websocket, trading, or retention loop."""

    def __init__(self, *, config: AppConfig, safety, session_factory, started_at: datetime) -> None:
        self.config = config
        self.safety = safety
        self.session_factory = session_factory
        self.started_at = started_at

    async def run(self, *, stop_event, max_iterations: int | None = None) -> None:
        iterations = 0
        while not stop_event.is_set():
            await asyncio.to_thread(self.run_once)
            iterations += 1
            if max_iterations is not None and iterations >= max_iterations:
                return
            await asyncio.to_thread(stop_event.wait, self.config.research_poll_seconds)

    def run_once(self) -> dict[str, Any]:
        checked_at = datetime.now(UTC)
        if self.session_factory is None:
            return {"status": "blocked", "blockers": ["research_database_not_configured"]}
        cycle_started_at = checked_at
        result = _cycle_progress(
            state="running",
            stage="startup",
            cycle_started_at=cycle_started_at,
        )
        self._write_heartbeat(checked_at, result)
        LOGGER.info("Research cycle starting.")
        try:
            result = _cycle_progress(
                state="running",
                stage="archive",
                cycle_started_at=cycle_started_at,
            )
            self._write_heartbeat(datetime.now(UTC), result)
            archive, archive_budget_exhausted = self._archive_stage(
                checked_at=checked_at,
                cycle_started_at=cycle_started_at,
            )
            if archive_budget_exhausted:
                result = {
                    "status": "partial",
                    **_cycle_progress(
                        state="partial",
                        stage="archive",
                        cycle_started_at=cycle_started_at,
                        last_successful_stage="archive",
                        archive=archive,
                        warnings=["research_archive_batch_budget_exhausted"],
                    ),
                }
                self._write_heartbeat(datetime.now(UTC), result)
                LOGGER.warning("Research archive batch budget exhausted; replay deferred.")
                return result

            result = _cycle_progress(
                state="running",
                stage="association_labels",
                cycle_started_at=cycle_started_at,
                last_successful_stage="archive",
                archive=archive,
            )
            self._write_heartbeat(datetime.now(UTC), result)
            with self.session_factory() as session:
                refresh_research_archive_labels(session)
                session.commit()

            result = _cycle_progress(
                state="running",
                stage="coverage",
                cycle_started_at=cycle_started_at,
                last_successful_stage="association_labels",
                archive=archive,
            )
            self._write_heartbeat(datetime.now(UTC), result)
            with self.session_factory() as session:
                coverage = archive_research_coverage(session, now=checked_at)
                session.commit()
            archive = ArchiveResult(
                archived_events=archive.archived_events,
                archived_by_type=archive.archived_by_type,
                outcomes_reconciled=archive.outcomes_reconciled,
                coverage=coverage,
            )

            def report_replay_progress(stage: str, details: dict[str, Any]) -> None:
                nonlocal result
                progress_details = dict(details)
                last_successful_stage = progress_details.pop(
                    "last_successful_stage", "coverage"
                )
                result = _cycle_progress(
                    state="running",
                    stage=stage,
                    cycle_started_at=cycle_started_at,
                    last_successful_stage=last_successful_stage,
                    archive=archive,
                    **progress_details,
                )
                self._write_heartbeat(datetime.now(UTC), result)

            with self.session_factory() as session:
                result = run_research_cycle(
                    self.config,
                    session,
                    checked_at=checked_at,
                    archive_result=archive,
                    progress_callback=report_replay_progress,
                )
                session.commit()
            result = {
                **result,
                **_cycle_progress(
                    state="healthy" if result["status"] == "completed" else "degraded",
                    stage="complete",
                    cycle_started_at=cycle_started_at,
                    last_successful_stage="calibration"
                    if self.config.calibration_enabled
                    else "baseline_replay",
                    archive=archive,
                ),
            }
            self._write_heartbeat(datetime.now(UTC), result)
            LOGGER.info(
                "Research cycle completed archive_events=%s replay_run=%s calibration=%s.",
                archive.archived_events,
                result.get("replay_run_id"),
                result.get("calibration_status"),
            )
            return result
        except SQLAlchemyError as error:
            LOGGER.exception("Research cycle database stage failed.")
            result = {
                "status": "error",
                "blockers": ["research_database_error"],
                **_cycle_progress(
                    state="error",
                    stage=result.get("current_stage", "unknown"),
                    cycle_started_at=cycle_started_at,
                    last_successful_stage=result.get("last_successful_stage"),
                    error=error,
                ),
            }
            self._write_heartbeat(datetime.now(UTC), result)
            return result
        except Exception as error:
            LOGGER.exception("Research cycle unexpected stage failed.")
            result = {
                "status": "error",
                "blockers": ["research_cycle_unexpected_error"],
                **_cycle_progress(
                    state="error",
                    stage=result.get("current_stage", "unknown"),
                    cycle_started_at=cycle_started_at,
                    last_successful_stage=result.get("last_successful_stage"),
                    error=error,
                ),
            }
            self._write_heartbeat(datetime.now(UTC), result)
            return result

    def _archive_stage(
        self,
        *,
        checked_at: datetime,
        cycle_started_at: datetime,
    ) -> tuple[ArchiveResult, bool]:
        del checked_at
        counts: dict[str, int] = {}
        archived_events = 0
        batch_count = 0
        for stage_index, source_stage in enumerate(ARCHIVE_SOURCE_STAGES):
            while batch_count < ARCHIVE_MAX_BATCHES_PER_CYCLE:
                with self.session_factory() as session:
                    batch = archive_research_batch(session, source_stage=source_stage)
                    if batch.source_rows:
                        session.commit()
                if batch.source_rows == 0:
                    break
                batch_count += 1
                archived_events += batch.archived_events
                for event_type, count in batch.archived_by_type.items():
                    counts[event_type] = counts.get(event_type, 0) + count
                self._write_heartbeat(
                    datetime.now(UTC),
                    _cycle_progress(
                        state="running",
                        stage="archive",
                        cycle_started_at=cycle_started_at,
                        last_successful_stage="archive",
                        archive=ArchiveResult(archived_events, counts, 0, {}),
                        last_archive_batch={
                            "source_stage": batch.source_stage,
                            "source_rows": batch.source_rows,
                            "archived_events": batch.archived_events,
                            "batch_count": batch_count,
                        },
                    ),
                )
            if batch_count >= ARCHIVE_MAX_BATCHES_PER_CYCLE:
                with self.session_factory() as session:
                    current_pending = archive_research_source_pending(
                        session, source_stage=source_stage
                    )
                if current_pending:
                    return ArchiveResult(archived_events, counts, 0, {}), True
                for remaining_stage in ARCHIVE_SOURCE_STAGES[stage_index + 1 :]:
                    with self.session_factory() as session:
                        if archive_research_source_pending(
                            session, source_stage=remaining_stage
                        ):
                            return ArchiveResult(archived_events, counts, 0, {}), True
                return ArchiveResult(archived_events, counts, 0, {}), False
        return ArchiveResult(archived_events, counts, 0, {}), False

    def _write_heartbeat(self, heartbeat_at: datetime, result: dict[str, Any]) -> None:
        """Persist liveness from a fresh transaction so failed work cannot erase it."""
        try:
            with self.session_factory() as session:
                _record_research_heartbeat(
                    session,
                    self.config,
                    self.safety,
                    self.started_at,
                    heartbeat_at,
                    result,
                )
                session.commit()
        except SQLAlchemyError:
            LOGGER.warning("Research heartbeat persistence failed.", exc_info=True)


class MarketOutcomeReconciler:
    """Public-data-only reconciler run by the market-data worker, never research credentials."""

    def __init__(
        self,
        *,
        config: AppConfig,
        safety,
        session_factory,
        started_at: datetime,
        market_client: KalshiRestClient | None = None,
    ) -> None:
        self.config = config
        self.safety = safety
        self.session_factory = session_factory
        self.started_at = started_at
        # Deliberately omit credentials: reconciliation owns only public market detail.
        self.market_client = market_client or KalshiRestClient(
            base_url=config.kalshi_api_base_url,
            timeout_seconds=config.kalshi_rest_timeout_seconds,
        )

    async def run(self, *, stop_event, max_iterations: int | None = None) -> None:
        iterations = 0
        while not stop_event.is_set():
            await asyncio.to_thread(self.run_once)
            iterations += 1
            if max_iterations is not None and iterations >= max_iterations:
                return
            await asyncio.to_thread(stop_event.wait, max(self.config.research_poll_seconds, 60.0))

    def run_once(self) -> None:
        if self.session_factory is None:
            return
        try:
            with self.session_factory() as session:
                reconcile_market_outcomes(session, client=self.market_client)
                session.commit()
        except SQLAlchemyError:
            LOGGER.warning("Market outcome reconciliation failed.", exc_info=True)


def run_research_cycle(
    config: AppConfig,
    session,
    *,
    checked_at: datetime | None = None,
    archive_result: ArchiveResult | None = None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Execute archive -> labels -> baseline replay -> optional bounded calibration."""
    checked_at = checked_at or datetime.now(UTC)
    archive = archive_result or archive_research_events(session, now=checked_at)
    repository = ResearchRepository(session)
    events = repository.list_events(limit=None)
    outcomes = repository.list_complete_outcomes()
    baseline = StrategyV2Repository(session).ensure_config_version(
        built_in_config_version("btc15_momentum_v2", V2_PARAMETERS)
    )
    replay = DeterministicReplayEngine().replay(events, outcomes=outcomes)
    outcome_input_hash = _replay_outcome_input_hash(outcomes)
    run_id = (
        "replay-"
        + _hash(
            {
                "dataset": replay.dataset_hash,
                "outcomes": outcome_input_hash,
                "baseline": baseline.strategy_config_version_id,
            }
        )[:24]
    )
    replay_run = repository.create_replay_run(
        {
            "replay_run_id": run_id,
            "status": "RUNNING",
            "replay_engine_version": REPLAY_SCHEMA_VERSION,
            "label_schema_version": RESEARCH_LABEL_SCHEMA_VERSION,
            "code_commit_sha": resolve_code_version(),
            "baseline_strategy_config_version_id": baseline.strategy_config_version_id,
            "dataset_hash": replay.dataset_hash,
            "data_cutoff": checked_at,
            "start_at": events[0].event_time if events else None,
            "end_at": events[-1].event_time if events else None,
            "unique_market_count": len(
                {event.market_ticker for event in events if event.market_ticker}
            ),
            "event_count": replay.event_count,
            "partition_manifest": None,
            "cost_model": replay.cost_model,
            "zero_entry_report": replay.zero_entry_report,
            "blocker_funnel": replay.blocker_funnel,
            "raw_metrics": {
                "decision_count": len(replay.decisions),
                "trade_count": len(replay.trades),
                "archive_coverage": archive.coverage,
                "outcome_input_hash": outcome_input_hash,
            },
            "adjusted_metrics": None,
            "warnings": [],
            "blockers": [],
            "started_at": checked_at,
        }
    )
    for trade in replay.trades:
        repository.insert_replay_trade(
            _replay_trade_values(
                replay_run_id=run_id,
                trade=trade,
                candidate_id=None,
                strategy_config_version_id=baseline.strategy_config_version_id,
                evidence_partition="full_dataset_baseline",
            )
        )
    repository.finish_replay_run(replay_run, status="COMPLETED", finished_at=checked_at)
    if progress_callback is not None:
        # Commit replay evidence before calibration so a later calibration failure
        # cannot roll back a completed archive/replay stage.
        session.commit()
        progress_callback(
            "baseline_replay",
            {
                "last_successful_stage": "baseline_replay",
                "replay_run_id": run_id,
                "zero_entry_report": replay.zero_entry_report,
            },
        )
    calibration_status = "DISABLED"
    calibration_run_id = None
    if config.calibration_enabled:
        if progress_callback is not None:
            progress_callback(
                "calibration",
                {
                    "last_successful_stage": "baseline_replay",
                    "replay_run_id": run_id,
                    "zero_entry_report": replay.zero_entry_report,
                },
            )
        calibration_run_id = (
            "calibration-" + _hash({"replay": run_id, "dataset": replay.dataset_hash})[:24]
        )
        existing_calibration = repository.get_calibration_run(calibration_run_id)
        if existing_calibration is not None and existing_calibration.holdout_used_at is not None:
            calibration_status = existing_calibration.status
        else:
            calibration = run_bounded_calibration(
                calibration_run_id=calibration_run_id, events=events, outcomes=outcomes
            )
            calibration_status = calibration.status
            selected_metrics = calibration.candidate_metrics.get(
                calibration.selected_candidate_id or "", {}
            )
            calibration_run = repository.create_calibration_run(
                {
                    "calibration_run_id": calibration_run_id,
                    "status": calibration.status,
                    "calibration_schema_version": CALIBRATION_SCHEMA_VERSION,
                    "replay_run_id": run_id,
                    "dataset_hash": replay.dataset_hash,
                    "code_commit_sha": resolve_code_version(),
                    "random_seed": int(_hash(calibration_run_id)[:8], 16),
                    "search_space_snapshot": complete_search_space_snapshot(
                        calibration_run_id,
                        calibration.candidates,
                    ),
                    "partition_manifest": calibration.partition_manifest,
                    "frozen_holdout_hash": calibration.partition_manifest.get("holdout_hash"),
                    "evaluated_candidate_count": len(calibration.candidates),
                    "selected_candidate_id": calibration.selected_candidate_id,
                    "training_metrics": None,
                    "validation_metrics": calibration.candidate_metrics,
                    "test_metrics": None,
                    "holdout_metrics": selected_metrics.get("holdout"),
                    "bootstrap_metrics": selected_metrics.get("bootstrap"),
                    "penalties": selected_metrics.get("penalties"),
                    "warnings": list(calibration.warnings),
                    "blockers": list(calibration.blockers),
                    "started_at": checked_at,
                    "finished_at": checked_at,
                    "holdout_used_at": checked_at if calibration.selected_candidate_id else None,
                }
            )
            if calibration.status == "COMPLETED":
                for candidate in calibration.candidates:
                    candidate_metrics = calibration.candidate_metrics.get(
                        candidate.candidate_id,
                        {},
                    )
                    candidate_partition_trades = (
                        calibration.candidate_partition_replay_trades.get(
                            candidate.candidate_id,
                            {
                                "search_development": calibration.candidate_replay_trades.get(
                                    candidate.candidate_id,
                                    (),
                                )
                            },
                        )
                    )
                    if candidate.model_type == "BASELINE":
                        for partition, partition_trades in candidate_partition_trades.items():
                            for trade in partition_trades:
                                repository.insert_replay_trade(
                                    _replay_trade_values(
                                        replay_run_id=run_id,
                                        trade=trade,
                                        candidate_id=candidate.candidate_id,
                                        strategy_config_version_id=(
                                            baseline.strategy_config_version_id
                                        ),
                                        evidence_partition=partition,
                                    )
                                )
                        continue
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
                            code_commit_sha=resolve_code_version(),
                            source="RESEARCH_CALIBRATION",
                            parent_config_version_id=baseline.strategy_config_version_id,
                            calibration_run_id=calibration_run.calibration_run_id,
                            lifecycle_state=LIFECYCLE_DRAFT,
                            approval_state="RESEARCH_ONLY",
                            model_type=candidate.model_type,
                            model_artifact_checksum=_hash(artifact),
                            data_cutoff=checked_at,
                            candidate_id=candidate.candidate_id,
                        )
                    )
                    repository.create_candidate(
                        {
                            "candidate_id": candidate.candidate_id,
                            "strategy_config_version_id": config_version_id,
                            "calibration_run_id": calibration_run.calibration_run_id,
                            "parent_strategy_config_version_id": (
                                baseline.strategy_config_version_id
                            ),
                            "generated_strategy_id": candidate.generated_strategy_id,
                            "architecture_version": V2_ARCHITECTURE_VERSION,
                            "feature_schema_version": V2_FEATURE_SCHEMA_VERSION,
                            "replay_schema_version": REPLAY_SCHEMA_VERSION,
                            "model_type": candidate.model_type,
                            "parameter_snapshot": candidate.parameters,
                            "feature_columns": list(candidate.feature_columns),
                            "model_artifact": artifact,
                            "model_artifact_checksum": _hash(artifact),
                            "training_metrics": candidate_metrics.get("training"),
                            "validation_metrics": candidate_metrics,
                            "test_metrics": candidate_metrics.get("development_test"),
                            "holdout_metrics": candidate_metrics.get("holdout"),
                            "governance_report": None,
                            "lifecycle_state": LIFECYCLE_DRAFT,
                            "eligibility_status": "RESEARCH_ONLY",
                        }
                    )
                    for partition, partition_trades in candidate_partition_trades.items():
                        for trade in partition_trades:
                            repository.insert_replay_trade(
                                _replay_trade_values(
                                    replay_run_id=run_id,
                                    trade=trade,
                                    candidate_id=candidate.candidate_id,
                                    strategy_config_version_id=config_version_id,
                                    evidence_partition=partition,
                                )
                            )
                    if candidate.candidate_id == calibration.selected_candidate_id:
                        repository.advance_candidate_governance(
                            candidate_id=candidate.candidate_id,
                            actor="ape-research-worker",
                        )
    return {
        "status": "completed",
        "archive": archive.coverage,
        "archive_event_count": archive.archived_events,
        "replay_run_id": run_id,
        "zero_entry_report": replay.zero_entry_report,
        "calibration_status": calibration_status,
        "calibration_run_id": calibration_run_id,
        "warnings": [],
        "blockers": [],
    }


def _replay_trade_values(
    *,
    replay_run_id: str,
    trade: ReplayTrade,
    candidate_id: str | None,
    strategy_config_version_id: str,
    evidence_partition: str,
) -> dict[str, Any]:
    trade_prefix = replay_run_id if candidate_id is None else f"{replay_run_id}-{candidate_id}"
    measurements = {
        **(trade.measurements if isinstance(trade.measurements, dict) else {}),
        "evidence_partition": evidence_partition,
    }
    return {
        "trade_id": f"{trade_prefix}-{evidence_partition}-{trade.trade_id}",
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
        "lifecycle_version": measurements.get(
            "lifecycle_version", "momentum_v2_lifecycle_v2"
        ),
        "measurements": measurements,
    }


def _record_research_heartbeat(
    session,
    config: AppConfig,
    safety,
    started_at: datetime,
    heartbeat_at: datetime,
    result: dict[str, Any],
) -> None:
    archive = result.get("archive")
    last_error = result.get("last_error")
    WorkerHeartbeatRepository(session).record_heartbeat(
        WorkerHeartbeatInput(
            service_name=WORKER_SERVICE_RESEARCH,
            started_at=started_at,
            heartbeat_at=heartbeat_at,
            app_mode=config.app_mode.value,
            is_safe=safety.is_safe,
            metadata={
                "mode": "research",
                "research": {
                    "enabled": config.research_enabled,
                    "calibration_enabled": config.calibration_enabled,
                    "poll_seconds": config.research_poll_seconds,
                    "worker_role": "research",
                    "worker_state": result.get("worker_state", "healthy"),
                    "cycle_state": result.get("cycle_state", "healthy"),
                    "current_stage": result.get("current_stage"),
                    "last_successful_stage": result.get("last_successful_stage"),
                    "cycle_started_at": result.get("cycle_started_at"),
                    "cycle_finished_at": result.get("cycle_finished_at"),
                    "last_archive_run": archive,
                    "last_archive_batch": result.get("last_archive_batch"),
                    "last_replay_run": result.get("replay_run_id"),
                    "last_calibration_run": result.get("calibration_run_id"),
                    "zero_entry_report": result.get("zero_entry_report"),
                    "last_error": last_error,
                    "statement_timeout_detected": bool(
                        isinstance(last_error, dict)
                        and last_error.get("statement_timeout_detected")
                    ),
                    "warnings": _bounded_strings(result.get("warnings")),
                    "blockers": _bounded_strings(result.get("blockers")),
                },
            },
        )
    )


def _cycle_progress(
    *,
    state: str,
    stage: str,
    cycle_started_at: datetime,
    last_successful_stage: str | None = None,
    archive: ArchiveResult | None = None,
    warnings: list[str] | None = None,
    blockers: list[str] | None = None,
    error: Exception | None = None,
    **details: Any,
) -> dict[str, Any]:
    last_error = _sanitized_error(error) if error is not None else None
    return {
        "worker_state": state,
        "cycle_state": state,
        "current_stage": stage,
        "last_successful_stage": last_successful_stage,
        "cycle_started_at": cycle_started_at.isoformat(),
        "cycle_finished_at": datetime.now(UTC).isoformat()
        if state in {"healthy", "partial", "error", "degraded"}
        else None,
        "archive": archive.coverage if archive is not None else None,
        "archive_event_count": archive.archived_events if archive is not None else None,
        "last_error": last_error,
        "warnings": _bounded_strings(warnings),
        "blockers": _bounded_strings(blockers),
        **details,
    }


def _sanitized_error(error: Exception) -> dict[str, Any]:
    statement_timeout = _is_statement_timeout(error)
    return {
        "type": error.__class__.__name__[:80],
        "code": "research_statement_timeout"
        if statement_timeout
        else "research_stage_failed",
        "statement_timeout_detected": statement_timeout,
    }


def _is_statement_timeout(error: Exception) -> bool:
    message = str(error).lower()
    return any(
        marker in message
        for marker in (
            "statement timeout",
            "query canceled",
            "query_cancelled",
            "canceling statement",
        )
    )


def _bounded_strings(value: Any, *, limit: int = 8) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item)[:128] for item in value[:limit]]


def _hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _replay_outcome_input_hash(outcomes) -> str:
    """Hash every resolved-outcome field that can change replay or label evidence."""
    return _hash(
        [
            {
                "outcome_id": outcome.outcome_id,
                "market_ticker": outcome.market_ticker,
                "market_open_at": outcome.market_open_at,
                "market_close_at": outcome.market_close_at,
                "expiration_at": outcome.expiration_at,
                "boundary": outcome.boundary,
                "result_side": outcome.result_side,
                "settlement_value": outcome.settlement_value,
                "final_reference_value": outcome.final_reference_value,
                "final_minute_reference_average": outcome.final_minute_reference_average,
                "outcome_status": outcome.outcome_status,
                "outcome_source": outcome.outcome_source,
                "source_payload_hash": outcome.source_payload_hash,
                "resolved_at": outcome.resolved_at,
                "expected_frame_count": outcome.expected_frame_count,
                "actual_frame_count": outcome.actual_frame_count,
                "coverage_percentage": outcome.coverage_percentage,
                "maximum_event_gap_seconds": outcome.maximum_event_gap_seconds,
                "quality_flags": outcome.quality_flags,
            }
            for outcome in outcomes
        ]
    )
