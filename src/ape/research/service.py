from __future__ import annotations

import asyncio
import hashlib
import json
import logging
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
from ape.research.archive import archive_research_events, reconcile_market_outcomes
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
        try:
            with self.session_factory() as session:
                result = run_research_cycle(self.config, session, checked_at=checked_at)
                _record_research_heartbeat(
                    session, self.config, self.safety, self.started_at, checked_at, result
                )
                session.commit()
                return result
        except SQLAlchemyError:
            LOGGER.warning("Research cycle persistence failed.", exc_info=True)
            return {"status": "error", "blockers": ["research_database_error"]}


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
            if self.session_factory is not None:
                try:
                    with self.session_factory() as session:
                        reconcile_market_outcomes(session, client=self.market_client)
                        session.commit()
                except SQLAlchemyError:
                    LOGGER.warning("Market outcome reconciliation failed.", exc_info=True)
            iterations += 1
            if max_iterations is not None and iterations >= max_iterations:
                return
            await asyncio.to_thread(stop_event.wait, max(self.config.research_poll_seconds, 60.0))


def run_research_cycle(
    config: AppConfig, session, *, checked_at: datetime | None = None
) -> dict[str, Any]:
    """Execute archive -> labels -> baseline replay -> optional bounded calibration."""
    checked_at = checked_at or datetime.now(UTC)
    archive = archive_research_events(session, now=checked_at)
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
    calibration_status = "DISABLED"
    calibration_run_id = None
    if config.calibration_enabled:
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
                    repository.advance_candidate_governance(
                        candidate_id=candidate.candidate_id,
                        actor="ape-research-worker",
                    )
    repository.finish_replay_run(replay_run, status="COMPLETED", finished_at=checked_at)
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
                    "worker_role": "research",
                    "last_archive_run": result.get("archive"),
                    "last_replay_run": result.get("replay_run_id"),
                    "last_calibration_run": result.get("calibration_run_id"),
                    "zero_entry_report": result.get("zero_entry_report"),
                    "warnings": result.get("warnings", []),
                    "blockers": result.get("blockers", []),
                },
            },
        )
    )


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
