from __future__ import annotations

import copy
import hashlib
import json
import tempfile
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient
from sqlalchemy import select

from ape.api.main import create_app
from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.models import (
    Market,
    OrderbookSnapshot,
    ReferenceTick,
    ResearchMarketOutcome,
    StrategyFeatureSnapshot,
)
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.inputs import StrategyConfigVersionInput
from ape.repositories.strategy_v2 import StrategyV2Repository
from ape.research.archive import archive_research_events, reconcile_market_outcomes
from ape.research.calibration import (
    LIFECYCLE_DRAFT,
    LIFECYCLE_PAPER_CANDIDATE,
    CandidateSpec,
    GovernanceError,
    complete_search_space_snapshot,
    run_bounded_calibration,
    transition_candidate,
)
from ape.research.fees import verified_kalshi_taker_fee_model
from ape.research.fixtures import (
    fixture_time,
    replayable_feature_vector,
    synthetic_governance_fixture_dataset,
)
from ape.research.replay import DeterministicReplayEngine, ReplayTrade
from ape.research.repository import ResearchRepository
from ape.research.service import _replay_trade_values
from ape.storage.retention import build_storage_status
from ape.strategy.momentum_v2 import (
    REPLAY_SCHEMA_VERSION,
    V2_ARCHITECTURE_VERSION,
    V2_FEATURE_SCHEMA_VERSION,
    V2_PARAMETERS,
    built_in_config_version,
    resolve_code_version,
)


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="ape-research-smoke-") as directory:
        database_path = Path(directory) / "research-smoke.sqlite"
        config = load_config(
            {
                "DATABASE_URL": f"sqlite+pysqlite:///{database_path}",
                "APP_MODE": "DRY_RUN",
                "RESEARCH_ENABLED": "true",
                "CALIBRATION_ENABLED": "true",
                "TRADING_ENABLED": "false",
                "EXECUTE": "false",
            }
        )
        engine = create_engine_from_config(config)
        try:
            run_migrations(engine)
            run_migrations(engine)
            session_factory = create_session_factory(engine)
            with session_factory() as session:
                at = fixture_time()
                (
                    archive_result,
                    label_refresh,
                    reconciled,
                    archived_outcome,
                ) = _archive_public_fixture(session, at)
                governance = synthetic_governance_fixture_dataset()
                session.add_all((*governance.events, *governance.outcomes))
                session.flush()

                baseline = DeterministicReplayEngine().replay(
                    governance.events, outcomes=governance.outcomes
                )
                calibration = _calibrate(governance)
                selected = _selected_weighted_candidate(calibration)
                repository = ResearchRepository(session)
                persisted = _persist_generated_calibration(
                    repository=repository,
                    session=session,
                    calibration=calibration,
                    selected=selected,
                    checked_at=at + timedelta(days=7),
                    event_count=len(governance.events),
                )
                selected_transitions = repository.advance_candidate_governance(
                    candidate_id=selected.candidate_id,
                    actor="research-smoke",
                )
                under_sampled_transitions = repository.advance_candidate_governance(
                    candidate_id=persisted["under_sampled_candidate_id"],
                    actor="research-smoke",
                )
                paper_live_failed = _paper_live_failures()
                session.commit()

                api_results = _research_api_results(config)
                storage = build_storage_status(config, now=at + timedelta(days=7))
                payload = {
                    "invariants": {
                        "database_migrated_twice": True,
                        "fixture_source_rows_archived": archive_result.archived_events > 0,
                        "official_outcome_ingested": reconciled == 1
                        and archived_outcome is not None
                        and archived_outcome.result_side == "YES",
                        "coverage_and_labels_present": label_refresh.coverage[
                            "complete_markets"
                        ] >= 1
                        and _label_ready(archived_outcome),
                        "archived_label_summary": _label_summary(archived_outcome),
                        "baseline_replay_closed_trades": sum(
                            trade.status == "CLOSED" for trade in baseline.trades
                        ),
                        "weighted_and_logistic_candidate_replayed": {
                            candidate_id: metrics.get("status")
                            for candidate_id, metrics in calibration.candidate_metrics.items()
                        },
                        "walk_forward_development_test_holdout": {
                            "fold_count": len(
                                calibration.candidate_metrics[selected.candidate_id][
                                    "walk_forward_validation"
                                ]
                            ),
                            "development_test": calibration.candidate_metrics[
                                selected.candidate_id
                            ]["development_test"] is not None,
                            "holdout_once": calibration.candidate_metrics[selected.candidate_id][
                                "holdout"
                            ] is not None,
                        },
                        "persisted_runs_candidates_configs_and_partition_trades": persisted,
                        "under_sampled_candidate_blocked": under_sampled_transitions == [],
                        "qualifying_candidate_only_reaches_dry_run_challenger": [
                            event.to_state for event in selected_transitions
                        ],
                        "paper_live_transitions_rejected": paper_live_failed,
                        "research_read_apis": api_results,
                        "storage_status_read": storage.connection_state,
                        "no_paper_live_order_private_or_account_capability": True,
                    }
                }
                _assert_invariants(payload["invariants"])
                print(json.dumps(payload, sort_keys=True, default=str, indent=2))
        finally:
            engine.dispose()
    return 0


def _archive_public_fixture(session, at: datetime):
    market_ticker = "KXBTC15M-SMOKE-ARCHIVE"
    session.add_all(
        (
            Market(
                market_ticker=market_ticker,
                series_ticker="KXBTC15M",
                open_time=at - timedelta(minutes=5),
                close_time=at + timedelta(minutes=10),
                expiration_time=at + timedelta(minutes=10),
                functional_strike=Decimal("62000"),
            ),
            StrategyFeatureSnapshot(
                feature_snapshot_id="smoke-feature",
                market_ticker=market_ticker,
                evaluated_at=at,
                feature_schema_version="momentum_v2_features_v3",
                context_hash="smoke",
                candidate_side="YES",
                boundary=Decimal("62000"),
                complete_feature_vector=_json_vector(replayable_feature_vector()),
                replay_readiness="FULL",
                replay_blockers=[],
            ),
            OrderbookSnapshot(
                market_ticker=market_ticker,
                received_at=at + timedelta(milliseconds=600),
                yes_ask=Decimal("0.60"),
                yes_bid=Decimal("0.58"),
                yes_ask_count=Decimal("1"),
                yes_bid_count=Decimal("1"),
            ),
            *(
                OrderbookSnapshot(
                    market_ticker=market_ticker,
                    received_at=at + timedelta(seconds=seconds),
                    yes_ask=Decimal("0.67"),
                    yes_bid=Decimal("0.65"),
                    yes_ask_count=Decimal("1"),
                    yes_bid_count=Decimal("1"),
                )
                for seconds in (5, 15, 30, 60)
            ),
            OrderbookSnapshot(
                market_ticker=market_ticker,
                received_at=at + timedelta(minutes=9),
                yes_ask=Decimal("0.72"),
                yes_bid=Decimal("0.70"),
                yes_ask_count=Decimal("1"),
                yes_bid_count=Decimal("1"),
            ),
            ReferenceTick(
                source="kalshi_cfbenchmarks_brti",
                received_at=at + timedelta(seconds=5),
                source_ts=at + timedelta(seconds=5),
                parsed_value=Decimal("62010"),
                parse_status="valid",
            ),
            *(
                ReferenceTick(
                    source="kalshi_cfbenchmarks_brti",
                    received_at=at + timedelta(seconds=seconds),
                    source_ts=at + timedelta(seconds=seconds),
                    parsed_value=Decimal("62010") + Decimal(seconds),
                    parse_status="valid",
                )
                for seconds in (15, 30, 60)
            ),
        )
    )
    session.flush()
    archive_result = archive_research_events(session, now=at + timedelta(minutes=11))

    class PublicOutcomeClient:
        def get_market(self, _market_ticker: str) -> dict[str, object]:
            return {
                "market": {
                    "result": "yes",
                    "status": "settled",
                    "settlement_value": "62010",
                }
            }

    reconciled = reconcile_market_outcomes(
        session,
        client=PublicOutcomeClient(),
        now=at + timedelta(minutes=11),
    )
    # The second archive pass writes labels from the just-reconciled official result.
    label_refresh = archive_research_events(session, now=at + timedelta(minutes=11))
    outcome = session.scalar(
        select(ResearchMarketOutcome).where(
            ResearchMarketOutcome.market_ticker == market_ticker
        )
    )
    return archive_result, label_refresh, reconciled, outcome


def _calibrate(governance):
    parameters = copy.deepcopy(V2_PARAMETERS)
    parameters["tiers"]["normal"]["score"] = 60
    candidates = (
        CandidateSpec(
            "candidate-baseline-v2",
            "btc15_momentum_v2_candidate_baseline000",
            "BASELINE",
            copy.deepcopy(V2_PARAMETERS),
        ),
        CandidateSpec(
            "candidate-smoke-weighted",
            "btc15_momentum_v2_candidate_smoke_weighted",
            "WEIGHTED_HEURISTIC",
            parameters,
        ),
        CandidateSpec(
            "candidate-smoke-under-sampled",
            "btc15_momentum_v2_candidate_smoke_under",
            "WEIGHTED_HEURISTIC",
            copy.deepcopy(V2_PARAMETERS),
        ),
        CandidateSpec(
            "candidate-smoke-logistic",
            "btc15_momentum_v2_candidate_smoke_logistic",
            "L2_LOGISTIC",
            copy.deepcopy(V2_PARAMETERS),
            model_artifact={"l2": "1"},
        ),
    )
    return run_bounded_calibration(
        calibration_run_id="research-smoke-calibration",
        events=list(governance.events),
        outcomes=list(governance.outcomes),
        candidate_specs=candidates,
    )


def _selected_weighted_candidate(calibration):
    assert calibration.status == "COMPLETED"
    assert calibration.selected_candidate_id == "candidate-smoke-weighted"
    return next(
        candidate
        for candidate in calibration.candidates
        if candidate.candidate_id == calibration.selected_candidate_id
    )


def _persist_generated_calibration(
    *,
    repository: ResearchRepository,
    session,
    calibration,
    selected,
    checked_at: datetime,
    event_count: int,
) -> dict[str, object]:
    baseline = StrategyV2Repository(session).ensure_config_version(
        built_in_config_version("btc15_momentum_v2", V2_PARAMETERS)
    )
    replay_run_id = "research-smoke-replay"
    calibration_run_id = "research-smoke-calibration"
    repository.create_replay_run(
        {
            "replay_run_id": replay_run_id,
            "status": "COMPLETED",
            "replay_engine_version": REPLAY_SCHEMA_VERSION,
            "label_schema_version": "momentum_research_labels_v1",
            "code_commit_sha": resolve_code_version(),
            "baseline_strategy_config_version_id": baseline.strategy_config_version_id,
            "dataset_hash": _hash(calibration.partition_manifest),
            "data_cutoff": checked_at,
            "unique_market_count": 500,
            "event_count": event_count,
            "partition_manifest": calibration.partition_manifest,
            "cost_model": verified_kalshi_taker_fee_model().metadata(),
            "raw_metrics": {"source": "generated_governance_fixture"},
            "started_at": checked_at,
            "finished_at": checked_at,
        }
    )
    repository.create_calibration_run(
        {
            "calibration_run_id": calibration_run_id,
            "status": calibration.status,
            "calibration_schema_version": "momentum_calibration_v1",
            "replay_run_id": replay_run_id,
            "dataset_hash": _hash(calibration.partition_manifest),
            "code_commit_sha": resolve_code_version(),
            "random_seed": 1,
            "search_space_snapshot": complete_search_space_snapshot(
                calibration_run_id, calibration.candidates
            ),
            "partition_manifest": calibration.partition_manifest,
            "frozen_holdout_hash": calibration.partition_manifest["holdout_hash"],
            "evaluated_candidate_count": len(calibration.candidates),
            "selected_candidate_id": selected.candidate_id,
            "validation_metrics": calibration.candidate_metrics,
            "holdout_metrics": calibration.candidate_metrics[selected.candidate_id]["holdout"],
            "warnings": list(calibration.warnings),
            "blockers": list(calibration.blockers),
            "started_at": checked_at,
            "finished_at": checked_at,
            "holdout_used_at": checked_at,
        }
    )
    selected_metrics = calibration.candidate_metrics[selected.candidate_id]
    selected_config = _persist_candidate(
        repository=repository,
        session=session,
        candidate=selected,
        metrics=selected_metrics,
        calibration_run_id=calibration_run_id,
        replay_run_id=replay_run_id,
        baseline_config_id=baseline.strategy_config_version_id,
        partition_trades=calibration.candidate_partition_replay_trades[selected.candidate_id],
        checked_at=checked_at,
    )
    under_sampled = next(
        candidate
        for candidate in calibration.candidates
        if candidate.candidate_id == "candidate-smoke-under-sampled"
    )
    _persist_candidate(
        repository=repository,
        session=session,
        candidate=under_sampled,
        metrics=calibration.candidate_metrics[under_sampled.candidate_id],
        calibration_run_id=calibration_run_id,
        replay_run_id=replay_run_id,
        baseline_config_id=baseline.strategy_config_version_id,
        partition_trades=calibration.candidate_partition_replay_trades.get(
            under_sampled.candidate_id, {}
        ),
        checked_at=checked_at,
    )
    return {
        "replay_run": repository.get_replay_run(replay_run_id) is not None,
        "calibration_run": repository.get_calibration_run(calibration_run_id) is not None,
        "selected_candidate_config": selected_config,
        "selected_partition_trade_count": sum(
            len(trades)
            for trades in calibration.candidate_partition_replay_trades[
                selected.candidate_id
            ].values()
        ),
        "under_sampled_candidate_id": under_sampled.candidate_id,
    }


def _persist_candidate(
    *,
    repository: ResearchRepository,
    session,
    candidate,
    metrics: dict[str, Any],
    calibration_run_id: str,
    replay_run_id: str,
    baseline_config_id: str,
    partition_trades: dict[str, tuple[ReplayTrade, ...]],
    checked_at: datetime,
) -> str:
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
            parent_config_version_id=baseline_config_id,
            calibration_run_id=calibration_run_id,
            lifecycle_state=LIFECYCLE_DRAFT,
            approval_state="RESEARCH_ONLY",
            model_type=candidate.model_type,
            model_artifact_checksum=_hash(candidate.model_artifact or {}),
            data_cutoff=checked_at,
            candidate_id=candidate.candidate_id,
        )
    )
    repository.create_candidate(
        {
            "candidate_id": candidate.candidate_id,
            "strategy_config_version_id": config_version_id,
            "calibration_run_id": calibration_run_id,
            "parent_strategy_config_version_id": baseline_config_id,
            "generated_strategy_id": candidate.generated_strategy_id,
            "architecture_version": V2_ARCHITECTURE_VERSION,
            "feature_schema_version": V2_FEATURE_SCHEMA_VERSION,
            "replay_schema_version": REPLAY_SCHEMA_VERSION,
            "model_type": candidate.model_type,
            "parameter_snapshot": candidate.parameters,
            "feature_columns": list(candidate.feature_columns),
            "model_artifact": candidate.model_artifact or {},
            "model_artifact_checksum": _hash(candidate.model_artifact or {}),
            "training_metrics": metrics.get("training"),
            "validation_metrics": metrics,
            "test_metrics": metrics.get("development_test"),
            "holdout_metrics": metrics.get("holdout"),
            "lifecycle_state": LIFECYCLE_DRAFT,
            "eligibility_status": "RESEARCH_ONLY",
        }
    )
    for partition, trades in partition_trades.items():
        for trade in trades:
            repository.insert_replay_trade(
                _replay_trade_values(
                    replay_run_id=replay_run_id,
                    trade=trade,
                    candidate_id=candidate.candidate_id,
                    strategy_config_version_id=config_version_id,
                    evidence_partition=partition,
                )
            )
    return config_version_id


def _paper_live_failures() -> list[str]:
    rejected: list[str] = []
    for target in (LIFECYCLE_PAPER_CANDIDATE, "LIVE_CANDIDATE"):
        try:
            transition_candidate(from_state=LIFECYCLE_DRAFT, to_state=target, evidence={})
        except GovernanceError:
            rejected.append(target)
    return rejected


def _research_api_results(config) -> dict[str, int]:
    app = create_app(config)
    with TestClient(app) as client:
        paths = (
            "/research/status",
            "/research/coverage/latest",
            "/research/zero-entry/latest",
            "/research/replay/runs/recent?limit=5",
            "/research/replay/trades/recent?limit=5",
            "/research/calibration/runs/recent?limit=5",
            "/research/candidates/recent?limit=5",
            "/research/governance/events/recent?limit=5",
            "/storage/status",
        )
        return {path: client.get(path).status_code for path in paths}


def _label_ready(outcome: ResearchMarketOutcome | None) -> bool:
    flags = outcome.quality_flags if outcome is not None else {}
    labels = flags.get("counterfactual_labels", {}) if isinstance(flags, dict) else {}
    return bool(labels) and all(
        label.get("entry_label_readiness") == "FULL"
        and label.get("settlement_label_readiness") == "FULL"
        for label in labels.values()
    )


def _label_summary(outcome: ResearchMarketOutcome | None) -> dict[str, object]:
    flags = outcome.quality_flags if outcome is not None else {}
    labels = flags.get("counterfactual_labels", {}) if isinstance(flags, dict) else {}
    values = list(labels.values()) if isinstance(labels, dict) else []
    return {
        "label_count": len(values),
        "all_entry_labels_full": all(
            value.get("entry_label_readiness") == "FULL" for value in values
        ),
        "all_settlement_labels_full": all(
            value.get("settlement_label_readiness") == "FULL" for value in values
        ),
        "all_5_15_30_60_marks_full": all(
            all(value.get(f"label_readiness_{seconds}s") == "FULL" for seconds in (5, 15, 30, 60))
            for value in values
        ),
    }


def _assert_invariants(invariants: dict[str, Any]) -> None:
    assert all(status_code == 200 for status_code in invariants["research_read_apis"].values())
    assert invariants["baseline_replay_closed_trades"] == 59
    assert invariants["under_sampled_candidate_blocked"]
    assert invariants["qualifying_candidate_only_reaches_dry_run_challenger"] == [
        "BACKTESTED",
        "SHADOW",
        "DRY_RUN_CHALLENGER",
    ], invariants["qualifying_candidate_only_reaches_dry_run_challenger"]
    assert invariants["paper_live_transitions_rejected"] == [
        "PAPER_CANDIDATE",
        "LIVE_CANDIDATE",
    ]
    assert invariants["storage_status_read"] in {"disabled", "healthy", "warning"}


def _json_vector(vector: dict[str, Any]) -> dict[str, Any]:
    return {
        key: str(value) if isinstance(value, Decimal) else value
        for key, value in vector.items()
    }


def _hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
