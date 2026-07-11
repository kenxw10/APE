from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from ape.config import AppConfig, AppMode
from ape.db.models import (
    Market,
    OrderbookSnapshot,
    PublicTrade,
    ReferenceTick,
    StrategyDecision,
    StrategyDryRunEvent,
    StrategyDryRunPosition,
)
from ape.db.session import create_engine_from_config, create_session_factory
from ape.kalshi.reference_messages import BRTI_SOURCE
from ape.repositories.inputs import (
    JsonPayload,
    StrategyDecisionInput,
    StrategyDryRunEventInput,
    StrategyDryRunPositionInput,
    WorkerHeartbeatInput,
)
from ape.repositories.markets import MarketsRepository
from ape.repositories.orderbook import OrderbookRepository
from ape.repositories.public_trades import PublicTradesRepository
from ape.repositories.reference_ticks import ReferenceTicksRepository
from ape.repositories.strategy_decisions import StrategyDecisionsRepository
from ape.repositories.strategy_dry_run import (
    OPEN_POSITION_STATUS,
    StrategyDryRunRepository,
)
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.safety import SafetyAssessment, assess_startup_safety
from ape.worker.feed_liveness import load_market_feed_liveness, load_reference_feed_liveness
from ape.worker.services import (
    WORKER_SERVICE_AGGREGATE,
    WORKER_SERVICE_STRATEGY,
)

LOGGER = logging.getLogger(__name__)

STATE_NO_ACTIVE_MARKET = "NO_ACTIVE_MARKET"
STATE_MARKET_NOT_PARSEABLE = "MARKET_NOT_PARSEABLE"
STATE_OBSERVE_ONLY_MARKET = "OBSERVE_ONLY_MARKET"
STATE_REFERENCE_STALE = "REFERENCE_STALE"
STATE_KALSHI_STALE = "KALSHI_STALE"
STATE_BOOK_UNUSABLE = "BOOK_UNUSABLE"
STATE_TOO_EARLY = "TOO_EARLY"
STATE_TOO_LATE_FOR_ENTRY = "TOO_LATE_FOR_ENTRY"
STATE_TOO_CLOSE_TO_BOUNDARY = "TOO_CLOSE_TO_BOUNDARY"
STATE_NO_DIRECTIONAL_CANDIDATE = "NO_DIRECTIONAL_CANDIDATE"
STATE_CONTRACT_NOT_CONFIRMED = "CONTRACT_NOT_CONFIRMED"
STATE_RISK_BLOCKED = "RISK_BLOCKED"
STATE_LIVE_GUARD_BLOCKED = "LIVE_GUARD_BLOCKED"
STATE_IMPULSE_TOO_WEAK = "IMPULSE_TOO_WEAK"
STATE_CHOP_FILTER_BLOCKED = "CHOP_FILTER_BLOCKED"
STATE_SPREAD_TOO_WIDE = "SPREAD_TOO_WIDE"
STATE_DEPTH_TOO_THIN = "DEPTH_TOO_THIN"
STATE_ENTER_DRY_RUN = "ENTER_DRY_RUN"
STATE_MANAGE_POSITION = "MANAGE_POSITION"
STATE_EXIT_SIGNAL = "EXIT_SIGNAL"
STATE_FORCE_EXIT = "FORCE_EXIT"

CONTROL_STRATEGY_ID = "btc15_momentum_v1"
CHALLENGER_STRATEGY_ID = "btc15_momentum_v1_fast"

DECISION_STATES = {
    STATE_NO_ACTIVE_MARKET,
    STATE_MARKET_NOT_PARSEABLE,
    STATE_OBSERVE_ONLY_MARKET,
    STATE_REFERENCE_STALE,
    STATE_KALSHI_STALE,
    STATE_BOOK_UNUSABLE,
    STATE_TOO_EARLY,
    STATE_TOO_LATE_FOR_ENTRY,
    STATE_TOO_CLOSE_TO_BOUNDARY,
    STATE_NO_DIRECTIONAL_CANDIDATE,
    STATE_CONTRACT_NOT_CONFIRMED,
    STATE_RISK_BLOCKED,
    STATE_LIVE_GUARD_BLOCKED,
    STATE_IMPULSE_TOO_WEAK,
    STATE_CHOP_FILTER_BLOCKED,
    STATE_SPREAD_TOO_WIDE,
    STATE_DEPTH_TOO_THIN,
    STATE_ENTER_DRY_RUN,
    STATE_MANAGE_POSITION,
    STATE_EXIT_SIGNAL,
    STATE_FORCE_EXIT,
}


@dataclass
class StrategyObserverRuntimeStatus:
    enabled: bool
    connection_state: str = "disabled"
    last_evaluated_at: datetime | None = None
    last_decision_state: str | None = None
    last_primary_reason: str | None = None
    last_decision_id: str | None = None
    dry_run_enabled: bool = False
    dry_run_open_position_count: int = 0
    dry_run_latest_event_type: str | None = None
    dry_run_latest_position_id: str | None = None
    warnings: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    variants: dict[str, dict[str, Any]] = field(default_factory=dict)

    def as_metadata(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "connection_state": self.connection_state,
            "last_evaluated_at": _isoformat_or_none(self.last_evaluated_at),
            "last_decision_state": self.last_decision_state,
            "last_primary_reason": self.last_primary_reason,
            "last_decision_id": self.last_decision_id,
            "warnings": self.warnings,
            "blockers": self.blockers,
        }

    def dry_run_metadata(self) -> dict[str, Any]:
        return {
            "enabled": self.dry_run_enabled,
            "open_position_count": self.dry_run_open_position_count,
            "latest_event_type": self.dry_run_latest_event_type,
            "latest_position_id": self.dry_run_latest_position_id,
            "warnings": self.warnings,
            "blockers": self.blockers,
        }

    def variant_metadata(self) -> dict[str, dict[str, Any]]:
        return self.variants


@dataclass(frozen=True)
class StrategyDecisionSnapshot:
    found: bool
    decision_id: str | None = None
    strategy_id: str | None = None
    evaluated_at: datetime | None = None
    decision_state: str | None = None
    primary_reason: str | None = None
    app_mode: str | None = None
    market_ticker: str | None = None
    candidate_side: str | None = None
    boundary: Decimal | None = None
    brti_value: Decimal | None = None
    distance_bps: Decimal | None = None
    seconds_left: int | None = None
    measurements: JsonPayload | None = None
    blockers: JsonPayload | None = None
    warnings: JsonPayload | None = None
    raw_context_hash: str | None = None


@dataclass(frozen=True)
class StrategyRecentDecisionsSnapshot:
    limit: int
    count: int
    decisions: list[StrategyDecisionSnapshot]
    checked_at: datetime


@dataclass(frozen=True)
class StrategyStatusSnapshot:
    enabled: bool
    worker_observed_enabled: bool | None
    connection_state: str
    variants: JsonPayload
    app_mode: str
    trading_enabled: bool
    execute: bool
    is_safe: bool
    latest_decision_id: str | None
    latest_evaluated_at: datetime | None
    latest_decision_state: str | None
    latest_primary_reason: str | None
    market_ticker: str | None
    candidate_side: str | None
    boundary: Decimal | None
    brti_value: Decimal | None
    distance_bps: Decimal | None
    seconds_left: int | None
    latest_measurements_summary: JsonPayload | None
    market_liveness_source: str | None
    reference_liveness_source: str | None
    market_component_heartbeat_at: str | None
    reference_component_heartbeat_at: str | None
    market_component_heartbeat_age_ms: int | None
    reference_component_heartbeat_age_ms: int | None
    liveness_source_mismatch: bool | None
    market_feed_transport_state: str | None
    market_feed_subscription_state: str | None
    market_feed_snapshot_state: str | None
    market_feed_active_ticker_state: str | None
    market_feed_sequence_state: str | None
    market_data_quiet: bool | None
    orderbook_recovery_action: str | None
    market_feed_state: str | None
    market_subscription_recovery_count: int | None
    market_subscription_recovery_last_reason: str | None
    market_subscription_recovery_last_action: str | None
    market_subscription_recovery_last_result: str | None
    market_subscription_recovery_last_at: str | None
    market_snapshot_resync_count: int | None
    market_snapshot_resync_last_result: str | None
    market_rollover_recovery_count: int | None
    market_transport_reconnect_count: int | None
    market_unrecovered_blocker_count: int | None
    market_recovery_attempt_in_progress: bool | None
    market_recovery_attempt_age_ms: int | None
    gate_results_summary: JsonPayload | None
    decision_age_seconds: float | None
    stale: bool
    warnings: list[str]
    blockers: list[str]
    checked_at: datetime


@dataclass(frozen=True)
class StrategyGateSummarySnapshot:
    limit: int
    count: int
    checked_at: datetime
    by_state: JsonPayload
    by_reason: JsonPayload
    by_gate: JsonPayload
    latest_decision: StrategyDecisionSnapshot
    latest_enter_dry_run: StrategyDecisionSnapshot
    latest_blockers: list[str]
    current_open_position_count: int


@dataclass(frozen=True)
class StrategyDryRunPositionSnapshot:
    found: bool
    position_id: str | None = None
    market_ticker: str | None = None
    strategy_id: str | None = None
    side_candidate: str | None = None
    status: str | None = None
    opened_at: datetime | None = None
    open_price: Decimal | None = None
    contract_count: int | None = None
    boundary: Decimal | None = None
    brti_at_entry: Decimal | None = None
    distance_bps_at_entry: Decimal | None = None
    decision_id: str | None = None
    closed_at: datetime | None = None
    close_price: Decimal | None = None
    close_reason: str | None = None
    realized_pnl_cents: Decimal | None = None
    measurements_summary: JsonPayload | None = None


@dataclass(frozen=True)
class StrategyDryRunEventSnapshot:
    found: bool
    event_id: str | None = None
    strategy_id: str | None = None
    position_id: str | None = None
    decision_id: str | None = None
    event_type: str | None = None
    market_ticker: str | None = None
    occurred_at: datetime | None = None
    side_candidate: str | None = None
    price: Decimal | None = None
    contract_count: int | None = None
    reason: str | None = None
    measurements_summary: JsonPayload | None = None


@dataclass(frozen=True)
class StrategyDryRunPositionsSnapshot:
    limit: int
    count: int
    positions: list[StrategyDryRunPositionSnapshot]
    checked_at: datetime


@dataclass(frozen=True)
class StrategyDryRunEventsSnapshot:
    limit: int
    count: int
    events: list[StrategyDryRunEventSnapshot]
    checked_at: datetime


@dataclass(frozen=True)
class StrategyDryRunStatusSnapshot:
    enabled: bool
    worker_observed_enabled: bool | None
    app_mode: str
    trading_enabled: bool
    execute: bool
    is_safe: bool
    open_position_count: int
    max_open_positions: int
    latest_event: StrategyDryRunEventSnapshot
    latest_enter_decision: StrategyDecisionSnapshot
    last_evaluated_at: datetime | None
    warnings: list[str]
    blockers: list[str]
    checked_at: datetime


@dataclass(frozen=True)
class StrategyVariantsComparisonSnapshot:
    window_seconds: int
    generated_at: datetime
    challenger_enabled: bool | None
    safety: JsonPayload
    variants: JsonPayload
    warnings: list[str]
    blockers: list[str]


@dataclass(frozen=True)
class DryRunLedgerResult:
    open_position_count: int = 0
    latest_event_type: str | None = None
    latest_position_id: str | None = None


class StrategyObserver:
    def __init__(
        self,
        *,
        config: AppConfig,
        safety: SafetyAssessment,
        session_factory: sessionmaker[Session] | None,
        started_at: datetime,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config
        self.safety = safety
        self.session_factory = session_factory
        self.started_at = started_at
        self.now = now or (lambda: datetime.now(UTC))
        self.status = StrategyObserverRuntimeStatus(enabled=config.strategy_observer_enabled)
        self.status.dry_run_enabled = _dry_run_runtime_enabled(config, safety)
        self._last_strategy_heartbeat_at: datetime | None = None

    async def run(
        self,
        *,
        stop_event: threading.Event,
        max_iterations: int | None = None,
    ) -> None:
        if not self.config.strategy_observer_enabled:
            self.status.connection_state = "disabled"
            self.record_heartbeat()
            return

        iterations = 0
        while not stop_event.is_set():
            iterations += 1
            self.evaluate_once()

            if max_iterations is not None and iterations >= max_iterations:
                return

            await _sleep_or_stop(stop_event, self.config.strategy_observer_poll_seconds)

    def evaluate_once(self) -> StrategyDecisionInput | None:
        if self.session_factory is None:
            self.status.connection_state = "not_configured"
            self.status.blockers = ["database_not_configured_for_strategy_observer"]
            self.record_heartbeat()
            return None

        try:
            with self.session_factory() as session:
                decisions = evaluate_strategy_variants(
                    config=self.config,
                    safety=self.safety,
                    session=session,
                    now=self.now(),
                )
                repository = StrategyDecisionsRepository(session)
                ledger_results: dict[str, DryRunLedgerResult] = {}
                for variant_config, decision in decisions:
                    if repository.get_decision_by_id(decision.decision_id) is None:
                        repository.insert_decision(decision)
                    ledger_results[decision.strategy_id] = _apply_dry_run_ledger(
                        config=variant_config,
                        session=session,
                        decision=decision,
                    )
                session.commit()
        except IntegrityError:
            LOGGER.info("Strategy observer decision already exists for current bucket.")
            self.record_heartbeat()
            return None
        except SQLAlchemyError as exc:
            LOGGER.warning("Strategy observer evaluation failed.", exc_info=True)
            self.status.connection_state = "error"
            self.status.blockers = ["strategy_observer_database_error"]
            self.status.warnings = [exc.__class__.__name__]
            self.record_heartbeat()
            return None

        control_config, decision = decisions[0]
        ledger_result = ledger_results[decision.strategy_id]
        self.status.connection_state = "running"
        self.status.last_evaluated_at = decision.evaluated_at
        self.status.last_decision_state = decision.decision_state
        self.status.last_primary_reason = decision.primary_reason
        self.status.last_decision_id = decision.decision_id
        self.status.dry_run_enabled = _dry_run_runtime_enabled(control_config, self.safety)
        self.status.dry_run_open_position_count = ledger_result.open_position_count
        self.status.dry_run_latest_event_type = ledger_result.latest_event_type
        self.status.dry_run_latest_position_id = ledger_result.latest_position_id
        self.status.blockers = list(decision.blockers or [])
        self.status.warnings = list(decision.warnings or [])
        self.status.variants = {
            variant_decision.strategy_id: _variant_runtime_metadata(
                config=variant_config,
                decision=variant_decision,
                ledger_result=ledger_results[variant_decision.strategy_id],
            )
            for variant_config, variant_decision in decisions
        }
        self.record_heartbeat()
        return decision

    def record_heartbeat(self) -> None:
        if self.session_factory is None:
            return

        try:
            with self.session_factory() as session:
                repository = WorkerHeartbeatRepository(session)
                metadata = {
                    "mode": "strategy_observer",
                    "strategy": {
                        "observer": self.status.as_metadata(),
                        "dry_run": self.status.dry_run_metadata(),
                        "variants": self.status.variant_metadata(),
                    },
                }
                heartbeat_at = self.now()
                repository.record_heartbeat(
                    WorkerHeartbeatInput(
                        service_name=WORKER_SERVICE_STRATEGY,
                        started_at=self.started_at,
                        heartbeat_at=heartbeat_at,
                        app_mode=self.config.app_mode.value,
                        is_safe=self.safety.is_safe,
                        metadata=metadata,
                    )
                )
                latest_heartbeat = repository.get_latest_heartbeat(
                    WORKER_SERVICE_AGGREGATE
                )
                metadata_keys = _enabled_collector_metadata_keys(self.config)
                if latest_heartbeat is not None and metadata_keys:
                    _preserve_existing_worker_metadata(
                        metadata,
                        latest_heartbeat.metadata_,
                        keys=metadata_keys,
                    )
                repository.record_heartbeat(
                    WorkerHeartbeatInput(
                        service_name=WORKER_SERVICE_AGGREGATE,
                        started_at=self.started_at,
                        heartbeat_at=heartbeat_at,
                        app_mode=self.config.app_mode.value,
                        is_safe=self.safety.is_safe,
                        metadata=metadata,
                    )
                )
                session.commit()
                self._last_strategy_heartbeat_at = heartbeat_at
        except SQLAlchemyError:
            LOGGER.warning("Strategy observer heartbeat persistence failed.", exc_info=True)


def evaluate_strategy_observer(
    *,
    config: AppConfig,
    safety: SafetyAssessment,
    session: Session,
    now: datetime | None = None,
) -> StrategyDecisionInput:
    evaluated_at = _as_utc(now or datetime.now(UTC))
    thresholds = _thresholds(config)

    market: Market | None = None
    active_market: Market | None = None
    reference_tick: ReferenceTick | None = None
    orderbook: OrderbookSnapshot | None = None
    latest_trade: PublicTrade | None = None
    reference_worker_metadata: dict[str, Any] | None = None
    orderbook_worker_metadata: dict[str, Any] | None = None
    reference_worker_heartbeat_at: datetime | None = None
    reference_worker_heartbeat_age_ms: int | None = None
    market_liveness_source = "missing"
    reference_liveness_source = "missing"
    market_component_heartbeat_at: datetime | None = None
    reference_component_heartbeat_at: datetime | None = None
    market_component_heartbeat_age_ms: int | None = None
    reference_component_heartbeat_age_ms: int | None = None
    latest_aggregate_heartbeat_mode: str | None = None
    latest_component_heartbeat_mode: str | None = None
    liveness_source_mismatch = False
    boundary: Decimal | None = None
    boundary_source: str | None = None
    brti_value: Decimal | None = None
    brti_backend_age_ms: int | None = None
    brti_source_age_ms: int | None = None
    brti_age_ms: int | None = None
    brti_strategy_fresh_age_ms: int | None = None
    brti_reference_source_warn_ms = config.strategy_reference_source_warn_ms
    brti_reference_source_hard_limit_ms = config.strategy_reference_source_max_age_ms
    brti_reference_status_category: str | None = None
    brti_reference_transport_stale = False
    brti_reference_persistence_stale = False
    brti_reference_worker_heartbeat_stale = False
    brti_reference_trade_ready_fresh = False
    brti_reference_backend_transport_lag_ms: int | None = None
    brti_reference_time_since_last_valid_tick_ms: int | None = None
    brti_reference_stale_reason: str | None = None
    brti_reference_stream_age_ms: int | None = None
    brti_reference_last_valid_message_at: datetime | None = None
    brti_reference_last_valid_message_source_ts: datetime | None = None
    brti_reference_last_valid_message_value: str | None = None
    brti_reference_last_duplicate_valid_message_at: datetime | None = None
    brti_reference_valid_message_carried_forward = False
    brti_reference_carry_forward_allowed = False
    brti_reference_carry_forward_age_ms: int | None = None
    brti_reference_liveness_reason: str | None = None
    orderbook_age_ms: int | None = None
    orderbook_stream_age_ms: int | None = None
    orderbook_stream_connection_state: str | None = None
    orderbook_stream_active_market_ticker: str | None = None
    orderbook_stream_warnings: list[str] = []
    orderbook_stream_blockers: list[str] = []
    market_feed_transport_state = "unknown"
    market_feed_subscription_state = "unknown"
    market_feed_snapshot_state = "missing"
    market_feed_active_ticker_state = "missing"
    market_feed_sequence_state = "unknown"
    market_data_quiet = False
    market_data_quiet_age_ms: int | None = None
    orderbook_snapshot_age_ms: int | None = None
    orderbook_recovery_action: str | None = None
    orderbook_carry_forward_allowed = False
    orderbook_carry_forward_age_ms: int | None = None
    orderbook_liveness_reason: str | None = None
    orderbook_snapshot_source: str | None = None
    latest_trade_age_ms: int | None = None
    seconds_since_open: int | None = None
    seconds_left: int | None = None
    candidate_side: str | None = None
    distance_bps: Decimal | None = None
    desired_bid: Decimal | None = None
    desired_ask: Decimal | None = None
    desired_spread: Decimal | None = None
    desired_spread_cents: Decimal | None = None
    desired_mid: Decimal | None = None
    desired_top_book_size: Decimal | None = None
    reference_ticks: list[ReferenceTick] = []
    orderbook_history: list[OrderbookSnapshot] = []
    recent_trades: list[PublicTrade] = []
    brti_short_price: Decimal | None = None
    brti_medium_price: Decimal | None = None
    brti_long_price: Decimal | None = None
    brti_short_move_bps: Decimal | None = None
    brti_medium_move_bps: Decimal | None = None
    brti_long_move_bps: Decimal | None = None
    brti_directional_tick_ratio: Decimal | None = None
    brti_short_point_count = 0
    brti_medium_point_count = 0
    brti_long_point_count = 0
    boundary_cross_count: int | None = None
    retrace_fraction: Decimal | None = None
    contract_mid_move_cents: Decimal | None = None
    ask_pullback_cents: Decimal | None = None
    recent_trade_count = 0
    candidate_trade_ratio: Decimal | None = None
    dry_run_risk_state: str | None = None
    dry_run_intended_entry_price: Decimal | None = None
    dry_run_intended_contract_count: int | None = None
    dry_run_position_id: str | None = None
    managing_position: StrategyDryRunPosition | None = None
    feed_failure_position: StrategyDryRunPosition | None = None
    open_positions: list[StrategyDryRunPosition] = []
    accumulated_warnings: list[str] = []

    def decision(
        state: str,
        reason: str,
        *,
        blockers: list[str] | None = None,
        warnings: list[str] | None = None,
    ) -> StrategyDecisionInput:
        decision_warnings = warnings if warnings is not None else list(accumulated_warnings)
        measurements = _measurements(
            config=config,
            safety=safety,
            decision_state=state,
            primary_reason=reason,
            decision_warnings=decision_warnings,
            evaluated_at=evaluated_at,
            thresholds=thresholds,
            market=market,
            boundary=boundary,
            boundary_source=boundary_source,
            reference_tick=reference_tick,
            reference_worker_metadata=reference_worker_metadata,
            orderbook_worker_metadata=orderbook_worker_metadata,
            reference_worker_heartbeat_at=reference_worker_heartbeat_at,
            reference_worker_heartbeat_age_ms=reference_worker_heartbeat_age_ms,
            market_liveness_source=market_liveness_source,
            reference_liveness_source=reference_liveness_source,
            market_component_heartbeat_at=market_component_heartbeat_at,
            reference_component_heartbeat_at=reference_component_heartbeat_at,
            market_component_heartbeat_age_ms=market_component_heartbeat_age_ms,
            reference_component_heartbeat_age_ms=reference_component_heartbeat_age_ms,
            latest_aggregate_heartbeat_mode=latest_aggregate_heartbeat_mode,
            latest_component_heartbeat_mode=latest_component_heartbeat_mode,
            liveness_source_mismatch=liveness_source_mismatch,
            brti_value=brti_value,
            brti_backend_age_ms=brti_backend_age_ms,
            brti_source_age_ms=brti_source_age_ms,
            brti_age_ms=brti_age_ms,
            brti_strategy_fresh_age_ms=brti_strategy_fresh_age_ms,
            brti_reference_source_warn_ms=brti_reference_source_warn_ms,
            brti_reference_source_hard_limit_ms=brti_reference_source_hard_limit_ms,
            brti_reference_status_category=brti_reference_status_category,
            brti_reference_transport_stale=brti_reference_transport_stale,
            brti_reference_persistence_stale=brti_reference_persistence_stale,
            brti_reference_worker_heartbeat_stale=brti_reference_worker_heartbeat_stale,
            brti_reference_trade_ready_fresh=brti_reference_trade_ready_fresh,
            brti_reference_backend_transport_lag_ms=(
                brti_reference_backend_transport_lag_ms
            ),
            brti_reference_time_since_last_valid_tick_ms=(
                brti_reference_time_since_last_valid_tick_ms
            ),
            brti_reference_stale_reason=brti_reference_stale_reason,
            brti_reference_stream_age_ms=brti_reference_stream_age_ms,
            brti_reference_last_valid_message_at=(
                brti_reference_last_valid_message_at
            ),
            brti_reference_last_valid_message_source_ts=(
                brti_reference_last_valid_message_source_ts
            ),
            brti_reference_last_valid_message_value=(
                brti_reference_last_valid_message_value
            ),
            brti_reference_last_duplicate_valid_message_at=(
                brti_reference_last_duplicate_valid_message_at
            ),
            brti_reference_valid_message_carried_forward=(
                brti_reference_valid_message_carried_forward
            ),
            brti_reference_carry_forward_allowed=(
                brti_reference_carry_forward_allowed
            ),
            brti_reference_carry_forward_age_ms=(
                brti_reference_carry_forward_age_ms
            ),
            brti_reference_liveness_reason=brti_reference_liveness_reason,
            orderbook=orderbook,
            orderbook_age_ms=orderbook_age_ms,
            orderbook_stream_age_ms=orderbook_stream_age_ms,
            orderbook_stream_connection_state=orderbook_stream_connection_state,
            orderbook_stream_active_market_ticker=(
                orderbook_stream_active_market_ticker
            ),
            orderbook_stream_warnings=orderbook_stream_warnings,
            orderbook_stream_blockers=orderbook_stream_blockers,
            market_feed_transport_state=market_feed_transport_state,
            market_feed_subscription_state=market_feed_subscription_state,
            market_feed_snapshot_state=market_feed_snapshot_state,
            market_feed_active_ticker_state=market_feed_active_ticker_state,
            market_feed_sequence_state=market_feed_sequence_state,
            market_data_quiet=market_data_quiet,
            market_data_quiet_age_ms=market_data_quiet_age_ms,
            orderbook_snapshot_age_ms=orderbook_snapshot_age_ms,
            orderbook_recovery_action=orderbook_recovery_action,
            orderbook_carry_forward_allowed=orderbook_carry_forward_allowed,
            orderbook_carry_forward_age_ms=orderbook_carry_forward_age_ms,
            orderbook_liveness_reason=orderbook_liveness_reason,
            orderbook_snapshot_source=orderbook_snapshot_source,
            latest_trade=latest_trade,
            latest_trade_age_ms=latest_trade_age_ms,
            seconds_since_open=seconds_since_open,
            seconds_left=seconds_left,
            candidate_side=candidate_side,
            distance_bps=distance_bps,
            desired_bid=desired_bid,
            desired_ask=desired_ask,
            desired_spread=desired_spread,
            desired_spread_cents=desired_spread_cents,
            desired_mid=desired_mid,
            desired_top_book_size=desired_top_book_size,
            brti_short_price=brti_short_price,
            brti_medium_price=brti_medium_price,
            brti_long_price=brti_long_price,
            brti_short_move_bps=brti_short_move_bps,
            brti_medium_move_bps=brti_medium_move_bps,
            brti_long_move_bps=brti_long_move_bps,
            brti_directional_tick_ratio=brti_directional_tick_ratio,
            brti_short_point_count=brti_short_point_count,
            brti_medium_point_count=brti_medium_point_count,
            brti_long_point_count=brti_long_point_count,
            boundary_cross_count=boundary_cross_count,
            retrace_fraction=retrace_fraction,
            contract_mid_move_cents=contract_mid_move_cents,
            ask_pullback_cents=ask_pullback_cents,
            recent_trade_count=recent_trade_count,
            candidate_trade_ratio=candidate_trade_ratio,
            dry_run_risk_state=dry_run_risk_state,
            dry_run_intended_entry_price=dry_run_intended_entry_price,
            dry_run_intended_contract_count=dry_run_intended_contract_count,
            dry_run_position_id=dry_run_position_id,
            managing_position=managing_position,
        )
        measurements["gate_trace"] = _full_gate_trace(
            config=config,
            decision_state=state,
            primary_reason=reason,
            evaluated_at=evaluated_at,
            market=market,
            boundary=boundary,
            brti_value=brti_value,
            orderbook=orderbook,
            seconds_since_open=seconds_since_open,
            seconds_left=seconds_left,
            reference_ticks=reference_ticks,
            orderbook_history=orderbook_history,
            recent_trades=recent_trades,
        )
        context_hash = _stable_hash(
            {
                "strategy_id": config.strategy_id,
                "state": state,
                "reason": reason,
                "market_id": getattr(market, "id", None),
                "market_ticker": getattr(market, "market_ticker", None),
                "reference_tick_id": getattr(reference_tick, "id", None),
                "orderbook_id": getattr(orderbook, "id", None),
                "latest_trade_id": getattr(latest_trade, "id", None),
                "boundary": _decimal_text(boundary),
                "brti_value": _decimal_text(brti_value),
                "candidate_side": candidate_side,
                "thresholds": thresholds,
                "safety": {
                    "app_mode": safety.mode,
                    "trading_enabled": safety.trading_enabled,
                    "execute": safety.execute,
                    "is_safe": safety.is_safe,
                },
            }
        )
        return StrategyDecisionInput(
            decision_id=_decision_id(
                evaluated_at=evaluated_at,
                poll_seconds=config.strategy_observer_poll_seconds,
                strategy_id=config.strategy_id,
                market_ticker=getattr(market, "market_ticker", None),
                context_hash=context_hash,
            ),
            evaluated_at=evaluated_at,
            decision_state=state,
            primary_reason=reason,
            app_mode=config.app_mode.value,
            strategy_id=config.strategy_id,
            market_ticker=getattr(market, "market_ticker", None),
            candidate_side=candidate_side,
            boundary=boundary,
            brti_value=brti_value,
            distance_bps=distance_bps,
            seconds_left=seconds_left,
            measurements=measurements,
            blockers=(
                blockers
                if blockers is not None
                else ([] if state == STATE_OBSERVE_ONLY_MARKET else [reason])
            ),
            warnings=decision_warnings,
            raw_context_hash=context_hash,
        )

    def load_managed_exit_quote() -> None:
        nonlocal desired_ask
        nonlocal desired_bid
        nonlocal desired_mid
        nonlocal desired_spread
        nonlocal desired_spread_cents
        nonlocal desired_top_book_size
        nonlocal latest_trade
        nonlocal latest_trade_age_ms
        nonlocal orderbook
        nonlocal orderbook_age_ms

        if market is None or candidate_side is None:
            return

        exit_orderbook = OrderbookRepository(session).get_latest_snapshot(
            market.market_ticker
        )
        orderbook = exit_orderbook
        orderbook_age_ms = (
            None
            if exit_orderbook is None
            else _age_ms(exit_orderbook.received_at, evaluated_at)
        )
        if (
            exit_orderbook is None
            or orderbook_age_ms is None
            or orderbook_age_ms > config.strategy_kalshi_book_max_age_ms
        ):
            return

        desired_bid, desired_ask, desired_spread = _desired_book(
            exit_orderbook,
            candidate_side,
        )
        desired_spread_cents = (
            None if desired_spread is None else desired_spread * Decimal("100")
        )
        desired_mid = _midpoint(desired_bid, desired_ask)
        desired_top_book_size = _desired_exit_book_size(exit_orderbook, candidate_side)
        latest_trade = PublicTradesRepository(session).get_latest_trade(
            market.market_ticker
        )
        if latest_trade is not None:
            latest_trade_age_ms = _age_ms(latest_trade.received_at, evaluated_at)

    def manage_feed_failure_position() -> bool:
        nonlocal candidate_side
        nonlocal dry_run_position_id
        nonlocal managing_position

        if managing_position is not None:
            return True
        if feed_failure_position is None:
            return False

        managing_position = feed_failure_position
        dry_run_position_id = managing_position.position_id
        candidate_side = managing_position.side_candidate
        return True

    if not safety.is_safe:
        return decision(
            STATE_LIVE_GUARD_BLOCKED,
            "startup_safety_not_observer_safe",
            blockers=safety.blockers,
            warnings=safety.warnings,
        )

    dry_run_repository = StrategyDryRunRepository(session)
    markets_repository = MarketsRepository(session)
    active_market = markets_repository.get_active_market(
        now=evaluated_at,
        series_ticker=config.kalshi_btc15_series_ticker,
    )
    if _dry_run_runtime_enabled(config, safety):
        open_positions = dry_run_repository.list_open_positions(
            strategy_id=config.strategy_id
        )
        if open_positions:
            stale_positions = [
                position
                for position in open_positions
                if (
                    active_market is None
                    or position.market_ticker != active_market.market_ticker
                )
            ]
            active_market_positions = [
                position
                for position in open_positions
                if active_market is not None
                and position.market_ticker == active_market.market_ticker
            ]
            active_market_position = (
                _oldest_dry_run_position(active_market_positions)
                if active_market_positions
                else None
            )
            feed_failure_position = active_market_position
            can_open_additional = (
                len(open_positions) < config.strategy_dry_run_max_open_positions
                and (
                    not config.strategy_dry_run_one_entry_per_market
                    or active_market_position is None
                )
            )
            if stale_positions:
                managing_position = _oldest_dry_run_position(stale_positions)
            elif not can_open_additional:
                managing_position = _oldest_dry_run_position(open_positions)

        if managing_position is not None:
            dry_run_position_id = managing_position.position_id
            candidate_side = managing_position.side_candidate
            market = markets_repository.get_market_by_ticker(
                managing_position.market_ticker
            )
            if market is None:
                return decision(
                    STATE_FORCE_EXIT,
                    "dry_run_position_market_missing",
                    blockers=["dry_run_force_exit_required"],
                )

    if managing_position is None:
        market = active_market
    elif market is not None and (
        market.open_time is None
        or market.close_time is None
        or _as_utc(market.close_time) <= evaluated_at
    ):
        return decision(
            STATE_FORCE_EXIT,
            "dry_run_position_market_closed_or_expired",
            blockers=["dry_run_force_exit_required"],
        )

    if market is None:
        return decision(STATE_NO_ACTIVE_MARKET, "no_active_persisted_market")

    if managing_position is None:
        market = active_market
    elif (
        active_market is None
        or active_market.market_ticker != managing_position.market_ticker
    ):
        return decision(
            STATE_FORCE_EXIT,
            "dry_run_position_no_longer_active_market",
            blockers=["dry_run_force_exit_required"],
        )

    seconds_since_open = _seconds_between(market.open_time, evaluated_at)
    seconds_left = _seconds_between(evaluated_at, market.close_time)
    boundary, boundary_source = _market_boundary(market)
    if boundary is None:
        if managing_position is not None and managing_position.boundary is not None:
            boundary = Decimal(managing_position.boundary)
            boundary_source = "dry_run_position_boundary"
        else:
            if managing_position is not None:
                return decision(
                    STATE_FORCE_EXIT,
                    "dry_run_position_boundary_missing",
                    blockers=["dry_run_force_exit_required"],
                )
            return decision(STATE_MARKET_NOT_PARSEABLE, "market_boundary_not_parseable")

    if boundary is None:
        return decision(STATE_MARKET_NOT_PARSEABLE, "market_boundary_not_parseable")

    market_liveness = load_market_feed_liveness(
        session,
        config,
        checked_at=evaluated_at,
        active_market_ticker=market.market_ticker,
    )
    reference_liveness = load_reference_feed_liveness(
        session,
        config,
        checked_at=evaluated_at,
    )
    reference_worker_metadata = reference_liveness.metadata
    orderbook_worker_metadata = market_liveness.metadata
    reference_worker_heartbeat_at = reference_liveness.heartbeat_at
    reference_worker_heartbeat_age_ms = reference_liveness.heartbeat_age_ms
    market_liveness_source = market_liveness.source
    reference_liveness_source = reference_liveness.source
    market_component_heartbeat_at = market_liveness.component_heartbeat_at
    reference_component_heartbeat_at = reference_liveness.component_heartbeat_at
    market_component_heartbeat_age_ms = market_liveness.component_heartbeat_age_ms
    reference_component_heartbeat_age_ms = reference_liveness.component_heartbeat_age_ms
    latest_aggregate_heartbeat_mode = (
        market_liveness.latest_aggregate_heartbeat_mode
        or reference_liveness.latest_aggregate_heartbeat_mode
    )
    latest_component_heartbeat_mode = (
        market_liveness.latest_component_heartbeat_mode
        or reference_liveness.latest_component_heartbeat_mode
    )
    liveness_source_mismatch = (
        market_liveness.liveness_source_mismatch
        or reference_liveness.liveness_source_mismatch
    )
    accumulated_warnings.extend(market_liveness.warnings)
    accumulated_warnings.extend(reference_liveness.warnings)
    if orderbook_worker_metadata is not None:
        orderbook_stream_connection_state = _metadata_text(
            orderbook_worker_metadata,
            "connection_state",
        )
        orderbook_stream_active_market_ticker = _metadata_text(
            orderbook_worker_metadata,
            "active_market_ticker",
        )
        orderbook_stream_warnings = _metadata_string_list(
            orderbook_worker_metadata,
            "warnings",
        )
        orderbook_stream_blockers = _metadata_string_list(
            orderbook_worker_metadata,
            "blockers",
        )
        orderbook_stream_age_ms = market_liveness.stream_age_ms
        market_feed_transport_state = market_liveness.market_feed_transport_state
        market_feed_subscription_state = market_liveness.market_feed_subscription_state
        market_feed_snapshot_state = market_liveness.market_feed_snapshot_state
        market_feed_active_ticker_state = market_liveness.market_feed_active_ticker_state
        market_feed_sequence_state = market_liveness.market_feed_sequence_state
        market_data_quiet = market_liveness.market_data_quiet
        market_data_quiet_age_ms = market_liveness.market_data_quiet_age_ms
        orderbook_snapshot_age_ms = market_liveness.orderbook_snapshot_age_ms
        orderbook_recovery_action = market_liveness.orderbook_recovery_action
    if reference_worker_metadata is not None:
        brti_reference_status_category = _metadata_text(
            reference_worker_metadata,
            "status_category",
        )
        brti_reference_last_valid_message_at = _metadata_datetime(
            reference_worker_metadata,
            "last_valid_message_at",
        )
        brti_reference_last_valid_message_source_ts = _metadata_datetime(
            reference_worker_metadata,
            "last_valid_message_source_ts",
        )
        brti_reference_last_valid_message_value = _metadata_text(
            reference_worker_metadata,
            "last_valid_message_value",
        )
        brti_reference_last_duplicate_valid_message_at = _metadata_datetime(
            reference_worker_metadata,
            "last_duplicate_valid_message_at",
        )
        brti_reference_valid_message_carried_forward = _metadata_bool(
            reference_worker_metadata,
            "valid_message_carried_forward",
        )
        brti_reference_stream_age_ms = reference_liveness.stream_age_ms
        brti_reference_backend_transport_lag_ms = (
            _metadata_int(reference_worker_metadata, "backend_transport_lag_ms")
            or _age_ms(
                _metadata_datetime(reference_worker_metadata, "last_message_at"),
                evaluated_at,
            )
        )
        brti_reference_time_since_last_valid_tick_ms = (
            _metadata_int(reference_worker_metadata, "time_since_last_valid_tick_ms")
            or _age_ms(
                _metadata_datetime(reference_worker_metadata, "last_valid_tick_at"),
                evaluated_at,
            )
        )
        brti_reference_worker_heartbeat_stale = (
            _metadata_bool(reference_worker_metadata, "worker_heartbeat_stale")
            or (
                reference_worker_heartbeat_age_ms is not None
                and reference_worker_heartbeat_age_ms
                > int(config.kalshi_cfbenchmarks_heartbeat_stale_after_seconds * 1000)
            )
            or _metadata_status_category_is(reference_worker_metadata, "worker_stale")
            or _metadata_has_warning(
                reference_worker_metadata,
                "brti_reference_worker_heartbeat_stale",
            )
        )
        brti_reference_transport_stale = (
            _metadata_bool(reference_worker_metadata, "transport_stale")
            or _metadata_status_category_is(reference_worker_metadata, "stale_transport")
            or _metadata_has_warning(
                reference_worker_metadata,
                "brti_reference_transport_stale",
                "brti_reference_first_tick_timeout",
                "brti_reference_no_valid_tick_timeout",
                "brti_reference_reconnect_requested",
            )
        )
        brti_reference_persistence_stale = (
            _metadata_bool(reference_worker_metadata, "persistence_stale")
            or _metadata_status_category_is(reference_worker_metadata, "stale_persistence")
            or _metadata_status_category_is(reference_worker_metadata, "persistence_error")
            or _metadata_has_warning(
                reference_worker_metadata,
                "brti_reference_persistence_stale",
                "brti_persistence_failed",
            )
        )

    reference_repository = ReferenceTicksRepository(session)
    latest_reference_tick = reference_repository.get_latest_tick(BRTI_SOURCE)
    reference_tick = reference_repository.get_latest_valid_tick(BRTI_SOURCE)
    if reference_tick is None:
        metadata_stale_reason = _metadata_stale_reason(reference_worker_metadata)
        if metadata_stale_reason is not None:
            brti_reference_stale_reason = metadata_stale_reason
        elif latest_reference_tick is not None:
            brti_reference_stale_reason = "brti_reference_value_unusable"
        else:
            brti_reference_stale_reason = "brti_reference_missing"
        if manage_feed_failure_position():
            load_managed_exit_quote()
            return decision(
                STATE_FORCE_EXIT,
                "dry_run_position_reference_unusable",
                blockers=["dry_run_force_exit_required"],
            )
        return decision(STATE_REFERENCE_STALE, brti_reference_stale_reason)

    brti_value = reference_tick.parsed_value
    brti_backend_age_ms = _age_ms(reference_tick.received_at, evaluated_at)
    brti_source_age_ms = _reference_source_age_ms(reference_tick, evaluated_at)
    brti_strategy_fresh_age_ms = brti_backend_age_ms
    brti_reference_time_since_last_valid_tick_ms = (
        brti_reference_time_since_last_valid_tick_ms or brti_backend_age_ms
    )
    brti_age_ms = max(
        age
        for age in (brti_backend_age_ms, brti_source_age_ms)
        if age is not None
    )
    brti_reference_stale_reason = _strategy_reference_stale_reason(
        config=config,
        reference_tick=reference_tick,
        brti_backend_age_ms=brti_backend_age_ms,
        brti_source_age_ms=brti_source_age_ms,
        brti_reference_stream_age_ms=brti_reference_stream_age_ms,
        reference_worker_metadata=reference_worker_metadata,
        worker_heartbeat_stale=brti_reference_worker_heartbeat_stale,
        transport_stale=brti_reference_transport_stale,
        persistence_stale=brti_reference_persistence_stale,
    )
    brti_reference_carry_forward_age_ms = brti_backend_age_ms
    brti_reference_carry_forward_allowed = (
        brti_reference_stale_reason is None
        and brti_backend_age_ms is not None
        and brti_backend_age_ms > config.strategy_reference_max_age_ms
        and brti_reference_stream_age_ms is not None
        and brti_reference_stream_age_ms <= config.strategy_reference_stream_max_age_ms
        and brti_backend_age_ms <= config.strategy_reference_carry_forward_max_age_ms
    )
    brti_reference_liveness_reason = (
        "brti_reference_carried_forward"
        if brti_reference_carry_forward_allowed
        else brti_reference_stale_reason
    )
    if brti_reference_carry_forward_allowed:
        brti_strategy_fresh_age_ms = brti_reference_stream_age_ms
        brti_reference_valid_message_carried_forward = True
        accumulated_warnings.append("brti_reference_carried_forward")
    brti_reference_trade_ready_fresh = (
        brti_reference_stale_reason is None
        and brti_backend_age_ms is not None
        and (
            brti_backend_age_ms <= config.strategy_reference_max_age_ms
            or brti_reference_carry_forward_allowed
            or not config.strategy_reference_require_trade_ready_fresh
        )
    )
    if (
        brti_reference_stale_reason is None
        and brti_source_age_ms is not None
        and brti_source_age_ms > config.strategy_reference_source_warn_ms
    ):
        accumulated_warnings.append("brti_reference_source_age_warning")

    if brti_reference_stale_reason is not None:
        if manage_feed_failure_position():
            load_managed_exit_quote()
            return decision(
                STATE_FORCE_EXIT,
                "dry_run_position_reference_stale",
                blockers=["dry_run_force_exit_required"],
            )
        return decision(STATE_REFERENCE_STALE, brti_reference_stale_reason)

    orderbook = OrderbookRepository(session).get_latest_snapshot(market.market_ticker)
    if orderbook is None:
        if manage_feed_failure_position():
            return decision(
                STATE_FORCE_EXIT,
                "dry_run_position_orderbook_missing",
                blockers=["dry_run_force_exit_required"],
            )
        return decision(STATE_KALSHI_STALE, "kalshi_orderbook_missing")

    orderbook_age_ms = _age_ms(orderbook.received_at, evaluated_at)
    orderbook_liveness_reason = _strategy_orderbook_stale_reason(
        config=config,
        orderbook=orderbook,
        orderbook_age_ms=orderbook_age_ms,
        orderbook_worker_metadata=orderbook_worker_metadata,
        orderbook_stream_age_ms=orderbook_stream_age_ms,
        orderbook_stream_connection_state=orderbook_stream_connection_state,
        orderbook_stream_active_market_ticker=orderbook_stream_active_market_ticker,
        orderbook_stream_warnings=orderbook_stream_warnings,
        orderbook_stream_blockers=orderbook_stream_blockers,
        market_feed_transport_state=market_feed_transport_state,
        market_feed_subscription_state=market_feed_subscription_state,
        market_feed_snapshot_state=market_feed_snapshot_state,
        market_feed_active_ticker_state=market_feed_active_ticker_state,
        market_feed_sequence_state=market_feed_sequence_state,
        market_ticker=market.market_ticker,
    )
    orderbook_carry_forward_age_ms = orderbook_age_ms
    orderbook_carry_forward_allowed = (
        orderbook_liveness_reason is None
        and orderbook_age_ms is not None
        and orderbook_age_ms > config.strategy_kalshi_book_max_age_ms
        and orderbook_age_ms <= config.strategy_kalshi_book_carry_forward_max_age_ms
    )
    orderbook_snapshot_source = (
        "carried_forward"
        if orderbook_carry_forward_allowed
        else (
            "fresh_update"
            if orderbook_liveness_reason is None and not market_data_quiet
            else "carried_forward"
            if orderbook_liveness_reason is None and market_data_quiet
            else "blocked"
        )
    )
    if orderbook_carry_forward_allowed:
        accumulated_warnings.append("kalshi_orderbook_data_quiet_carried_forward")
        orderbook_liveness_reason = "kalshi_orderbook_data_quiet_carried_forward"
    elif orderbook_liveness_reason is None and market_data_quiet:
        accumulated_warnings.append("kalshi_orderbook_data_quiet_carried_forward")
        orderbook_liveness_reason = "kalshi_orderbook_data_quiet_carried_forward"

    if (
        orderbook_snapshot_source == "blocked"
        and orderbook_liveness_reason is not None
    ):
        if manage_feed_failure_position():
            return decision(
                STATE_FORCE_EXIT,
                "dry_run_position_orderbook_stale",
                blockers=["dry_run_force_exit_required"],
            )
        return decision(STATE_KALSHI_STALE, orderbook_liveness_reason)

    latest_trade = PublicTradesRepository(session).get_latest_trade(market.market_ticker)
    if latest_trade is not None:
        latest_trade_age_ms = _age_ms(latest_trade.received_at, evaluated_at)

    reference_since = evaluated_at - timedelta(
        seconds=config.strategy_brti_lookback_long_seconds
    )
    reference_ticks = ReferenceTicksRepository(session).get_ticks_since(
        BRTI_SOURCE,
        reference_since,
        limit=max(config.strategy_brti_lookback_long_seconds * 4, 256),
    )
    orderbook_since = evaluated_at - timedelta(
        seconds=max(
            config.strategy_contract_lookback_seconds,
            config.strategy_contract_ask_pullback_lookback_seconds,
        )
    )
    orderbook_history = OrderbookRepository(session).get_snapshots_since(
        market.market_ticker,
        orderbook_since,
        limit=512,
    )
    recent_trades = PublicTradesRepository(session).get_trades_since(
        market.market_ticker,
        evaluated_at - timedelta(seconds=config.strategy_trade_confirmation_lookback_seconds),
        limit=250,
    )

    if (
        managing_position is None
        and open_positions
        and active_market is not None
        and brti_value is not None
        and brti_value > 0
    ):
        at_risk_position = _select_active_dry_run_position_needing_management(
            config=config,
            positions=open_positions,
            active_market_ticker=active_market.market_ticker,
            orderbook=orderbook,
            evaluated_at=evaluated_at,
            seconds_left=seconds_left,
            boundary=boundary,
            brti_value=brti_value,
        )
        if at_risk_position is not None:
            managing_position = at_risk_position
            dry_run_position_id = managing_position.position_id
            candidate_side = managing_position.side_candidate

    if managing_position is None:
        if (
            seconds_since_open is not None
            and seconds_since_open < config.strategy_no_entry_first_seconds
        ):
            return decision(STATE_TOO_EARLY, "entry_window_too_early")

        if seconds_left is not None and seconds_left < config.strategy_no_entry_last_seconds:
            return decision(STATE_TOO_LATE_FOR_ENTRY, "entry_window_too_late")

    if brti_value is None or brti_value <= 0:
        if managing_position is not None:
            return decision(
                STATE_FORCE_EXIT,
                "dry_run_position_reference_value_unusable",
                blockers=["dry_run_force_exit_required"],
            )
        return decision(STATE_NO_DIRECTIONAL_CANDIDATE, "no_directional_candidate")

    if managing_position is None and brti_value == boundary:
        return decision(STATE_NO_DIRECTIONAL_CANDIDATE, "no_directional_candidate")

    if managing_position is None:
        candidate_side = "YES" if brti_value > boundary else "NO"
    distance_bps = (abs(brti_value - boundary) / brti_value) * Decimal("10000")
    if (
        managing_position is None
        and distance_bps < Decimal(str(config.strategy_min_boundary_distance_bps))
    ):
        return decision(STATE_TOO_CLOSE_TO_BOUNDARY, "boundary_distance_below_threshold")

    desired_bid, desired_ask, desired_spread = _desired_book(orderbook, candidate_side)
    desired_spread_cents = (
        None if desired_spread is None else desired_spread * Decimal("100")
    )
    desired_mid = _midpoint(desired_bid, desired_ask)
    desired_top_book_size = (
        _desired_exit_book_size(orderbook, candidate_side)
        if managing_position is not None
        else _desired_top_book_size(orderbook, candidate_side)
    )
    if (
        desired_bid is None
        or desired_ask is None
        or desired_spread is None
        or desired_ask < desired_bid
        or desired_spread < 0
    ):
        if managing_position is not None:
            return decision(
                STATE_FORCE_EXIT,
                "dry_run_position_book_unusable",
                blockers=["dry_run_force_exit_required"],
            )
        return decision(STATE_BOOK_UNUSABLE, "kalshi_orderbook_side_missing")

    if (
        desired_spread_cents is None
        or desired_spread_cents > Decimal(str(config.strategy_max_spread_cents))
    ):
        if managing_position is not None:
            return decision(
                STATE_FORCE_EXIT,
                "dry_run_position_spread_too_wide",
                blockers=["dry_run_force_exit_required"],
            )
        return decision(STATE_SPREAD_TOO_WIDE, "desired_side_spread_too_wide")

    if desired_top_book_size is None or desired_top_book_size < Decimal(
        str(config.strategy_min_top_book_size_contracts)
    ):
        if managing_position is not None:
            return decision(
                STATE_FORCE_EXIT,
                "dry_run_position_depth_too_thin",
                blockers=["dry_run_force_exit_required"],
            )
        return decision(STATE_DEPTH_TOO_THIN, "desired_side_depth_too_thin")

    if managing_position is not None:
        management_state, management_reason = _dry_run_management_decision(
            config=config,
            position=managing_position,
            evaluated_at=evaluated_at,
            seconds_left=seconds_left,
            candidate_side=candidate_side,
            boundary=boundary,
            brti_value=brti_value,
            desired_bid=desired_bid,
        )
        return decision(
            management_state,
            management_reason,
            blockers=(
                ["dry_run_force_exit_required"]
                if management_state == STATE_FORCE_EXIT
                else []
            ),
        )

    dry_run_entry_bucket = int(
        evaluated_at.timestamp()
        / max(config.strategy_dry_run_min_seconds_between_decisions, 0.001)
    )
    dry_run_position_id = _dry_run_position_id(
        config=config,
        market_ticker=market.market_ticker,
        decision_id=f"entry-{dry_run_entry_bucket}",
    )
    if (
        _dry_run_runtime_enabled(config, safety)
        and dry_run_repository.get_position_by_id(dry_run_position_id) is not None
    ):
        dry_run_risk_state = "entry_bucket_already_entered"
        return decision(
            STATE_RISK_BLOCKED,
            "dry_run_entry_bucket_already_entered",
        )

    dry_run_intended_entry_price = _intended_entry_price(
        desired_ask,
        config.strategy_dry_run_entry_price_offset_cents,
        max_entry_price=Decimal(str(config.strategy_dry_run_max_entry_price)),
    )
    dry_run_intended_contract_count = config.strategy_dry_run_position_size_contracts
    if desired_ask < Decimal(str(config.strategy_dry_run_min_entry_price)):
        return decision(
            STATE_CONTRACT_NOT_CONFIRMED,
            "dry_run_intended_entry_price_too_low",
        )
    if desired_ask > Decimal(str(config.strategy_dry_run_max_entry_price)):
        return decision(
            STATE_CONTRACT_NOT_CONFIRMED,
            "dry_run_intended_entry_price_too_high",
        )

    impulse = _brti_impulse_metrics(
        config=config,
        ticks=reference_ticks,
        evaluated_at=evaluated_at,
        current_value=brti_value,
        candidate_side=candidate_side,
    )
    brti_short_price = impulse["short_price"]
    brti_medium_price = impulse["medium_price"]
    brti_long_price = impulse["long_price"]
    brti_short_move_bps = impulse["short_move_bps"]
    brti_medium_move_bps = impulse["medium_move_bps"]
    brti_long_move_bps = impulse["long_move_bps"]
    brti_directional_tick_ratio = impulse["directional_tick_ratio"]
    brti_short_point_count = int(impulse["short_point_count"])
    brti_medium_point_count = int(impulse["medium_point_count"])
    brti_long_point_count = int(impulse["long_point_count"])
    if impulse["reason"] is not None:
        return decision(STATE_IMPULSE_TOO_WEAK, str(impulse["reason"]))

    chop = _brti_chop_metrics(
        config=config,
        ticks=reference_ticks,
        evaluated_at=evaluated_at,
        boundary=boundary,
        current_value=brti_value,
        candidate_side=candidate_side,
        short_move_bps=brti_short_move_bps,
        medium_move_bps=brti_medium_move_bps,
    )
    boundary_cross_count = chop["boundary_cross_count"]
    retrace_fraction = chop["retrace_fraction"]
    if chop["reason"] is not None:
        return decision(STATE_CHOP_FILTER_BLOCKED, str(chop["reason"]))

    contract = _contract_confirmation_metrics(
        config=config,
        evaluated_at=evaluated_at,
        orderbook_history=orderbook_history,
        candidate_side=candidate_side,
        desired_mid=desired_mid,
        desired_ask=desired_ask,
    )
    contract_mid_move_cents = contract["mid_move_cents"]
    ask_pullback_cents = contract["ask_pullback_cents"]
    if contract["reason"] is not None:
        return decision(STATE_CONTRACT_NOT_CONFIRMED, str(contract["reason"]))

    trade_confirmation = _trade_confirmation_metrics(
        trades=recent_trades,
        candidate_side=candidate_side,
    )
    recent_trade_count = int(trade_confirmation["trade_count"])
    candidate_trade_ratio = trade_confirmation["candidate_trade_ratio"]
    if recent_trade_count < config.strategy_trade_confirmation_min_trades:
        accumulated_warnings.append("recent_trade_confirmation_insufficient_trades")
    elif (
        candidate_trade_ratio is not None
        and candidate_trade_ratio < Decimal(str(config.strategy_trade_confirmation_min_ratio))
    ):
        return decision(
            STATE_CONTRACT_NOT_CONFIRMED,
            "recent_trade_confirmation_weak",
        )

    if not _dry_run_runtime_enabled(config, safety):
        dry_run_risk_state = "dry_run_disabled"
        return decision(
            STATE_OBSERVE_ONLY_MARKET,
            (
                "dry_run_disabled_observe_only"
                if config.app_mode is AppMode.DRY_RUN
                else "observer_decision_ledger_only"
            ),
        )

    open_position_count = dry_run_repository.count_open_positions(
        strategy_id=config.strategy_id
    )
    if open_position_count >= config.strategy_dry_run_max_open_positions:
        dry_run_risk_state = "max_open_positions_reached"
        return decision(
            STATE_RISK_BLOCKED,
            "dry_run_max_open_positions_reached",
        )

    entered_this_market = dry_run_repository.has_any_position_for_market(
        strategy_id=config.strategy_id,
        market_ticker=market.market_ticker,
    )
    if config.strategy_dry_run_one_entry_per_market and entered_this_market:
        dry_run_risk_state = "one_entry_per_market_blocked"
        return decision(
            STATE_RISK_BLOCKED,
            "dry_run_one_entry_per_market_blocked",
        )

    dry_run_risk_state = "entry_allowed"
    return decision(
        STATE_ENTER_DRY_RUN,
        "dry_run_entry_signal",
        blockers=[],
    )


def evaluate_strategy_variants(
    *,
    config: AppConfig,
    safety: SafetyAssessment,
    session: Session,
    now: datetime | None = None,
) -> list[tuple[AppConfig, StrategyDecisionInput]]:
    """Evaluate configured dry-run variants on one timestamp and DB transaction."""

    evaluated_at = _as_utc(now or datetime.now(UTC))
    if session.bind is not None and session.bind.dialect.name == "postgresql":
        # Keep both variants on the same committed market/reference snapshot.
        session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    return [
        (
            variant_config,
            evaluate_strategy_observer(
                config=variant_config,
                safety=safety,
                session=session,
                now=evaluated_at,
            ),
        )
        for variant_config in strategy_variant_configs(config, safety)
    ]


def strategy_variant_configs(
    config: AppConfig,
    safety: SafetyAssessment,
) -> list[AppConfig]:
    control = replace(config, strategy_id=CONTROL_STRATEGY_ID)
    if not (
        config.strategy_challenger_enabled
        and _dry_run_runtime_enabled(control, safety)
    ):
        return [control]

    challenger = replace(
        control,
        strategy_id=CHALLENGER_STRATEGY_ID,
        strategy_brti_lookback_short_seconds=20,
        strategy_brti_lookback_medium_seconds=60,
        strategy_brti_lookback_long_seconds=120,
        strategy_contract_lookback_seconds=30,
    )
    return [control, challenger]


def _variant_runtime_metadata(
    *,
    config: AppConfig,
    decision: StrategyDecisionInput,
    ledger_result: DryRunLedgerResult,
) -> dict[str, Any]:
    return {
        "strategy_id": decision.strategy_id,
        "enabled": True,
        "thresholds": _thresholds(config),
        "latest_decision_id": decision.decision_id,
        "latest_evaluated_at": _isoformat_or_none(decision.evaluated_at),
        "latest_decision_state": decision.decision_state,
        "latest_primary_reason": decision.primary_reason,
        "open_position_count": ledger_result.open_position_count,
        "latest_event_type": ledger_result.latest_event_type,
        "latest_position_id": ledger_result.latest_position_id,
        "warnings": list(decision.warnings or []),
        "blockers": list(decision.blockers or []),
    }


def build_strategy_status(
    config: AppConfig,
    *,
    now: datetime | None = None,
) -> StrategyStatusSnapshot:
    checked_at = _as_utc(now or datetime.now(UTC))
    safety = assess_startup_safety(config)
    latest_decision: StrategyDecision | None = None
    worker_metadata: dict[str, Any] | None = None
    variants: JsonPayload = {}
    warnings: list[str] = []
    blockers: list[str] = []

    if not config.database_url:
        if config.strategy_observer_enabled:
            blockers.append("database_not_configured_for_strategy_status")
        return _status_snapshot(
            config=config,
            safety=safety,
            checked_at=checked_at,
            latest_decision=None,
            worker_metadata=None,
            variants=variants,
            warnings=warnings,
            blockers=blockers,
        )

    try:
        engine = create_engine_from_config(config)
        try:
            session_factory = create_session_factory(engine)
            with session_factory() as session:
                latest_decision = StrategyDecisionsRepository(session).get_latest_decision(
                    strategy_id=CONTROL_STRATEGY_ID
                )
                heartbeat_repository = WorkerHeartbeatRepository(session)
                component_heartbeat = heartbeat_repository.get_latest_heartbeat(
                    WORKER_SERVICE_STRATEGY
                )
                worker_metadata = _strategy_worker_metadata(
                    component_heartbeat.metadata_ if component_heartbeat else None
                )
                variants = _strategy_variants_worker_metadata(
                    component_heartbeat.metadata_ if component_heartbeat else None
                )
                if worker_metadata is None:
                    heartbeat = heartbeat_repository.get_latest_heartbeat(
                        WORKER_SERVICE_AGGREGATE
                    )
                    worker_metadata = _strategy_worker_metadata(
                        heartbeat.metadata_ if heartbeat else None
                    )
                    variants = _strategy_variants_worker_metadata(
                        heartbeat.metadata_ if heartbeat else None
                    )
                    if worker_metadata is not None:
                        warnings.append("feed_liveness_legacy_aggregate_fallback")
        finally:
            engine.dispose()
    except SQLAlchemyError:
        warnings.append("strategy_status_database_error")

    return _status_snapshot(
        config=config,
        safety=safety,
        checked_at=checked_at,
        latest_decision=latest_decision,
        worker_metadata=worker_metadata,
        variants=variants,
        warnings=warnings,
        blockers=blockers,
    )


def build_latest_strategy_decision(
    config: AppConfig,
    *,
    strategy_id: str | None = None,
) -> StrategyDecisionSnapshot:
    effective_strategy_id = strategy_id or CONTROL_STRATEGY_ID
    if not config.database_url:
        return StrategyDecisionSnapshot(found=False)

    try:
        engine = create_engine_from_config(config)
        try:
            session_factory = create_session_factory(engine)
            with session_factory() as session:
                decision = StrategyDecisionsRepository(session).get_latest_decision(
                    strategy_id=effective_strategy_id
                )
                return strategy_decision_snapshot(decision)
        finally:
            engine.dispose()
    except SQLAlchemyError:
        return StrategyDecisionSnapshot(
            found=False,
            warnings=["strategy_decision_database_error"],
        )


def build_recent_strategy_decisions(
    config: AppConfig,
    *,
    limit: int,
    strategy_id: str | None = None,
    now: datetime | None = None,
) -> StrategyRecentDecisionsSnapshot:
    effective_strategy_id = strategy_id or CONTROL_STRATEGY_ID
    capped_limit = min(max(limit, 1), 500)
    checked_at = _as_utc(now or datetime.now(UTC))
    if not config.database_url:
        return StrategyRecentDecisionsSnapshot(
            limit=capped_limit,
            count=0,
            decisions=[],
            checked_at=checked_at,
        )

    try:
        engine = create_engine_from_config(config)
        try:
            session_factory = create_session_factory(engine)
            with session_factory() as session:
                rows = StrategyDecisionsRepository(session).list_recent_decisions(
                    limit=capped_limit,
                    strategy_id=effective_strategy_id,
                )
        finally:
            engine.dispose()
    except SQLAlchemyError:
        rows = []

    decisions = [strategy_decision_snapshot(row) for row in rows]
    return StrategyRecentDecisionsSnapshot(
        limit=capped_limit,
        count=len(decisions),
        decisions=decisions,
        checked_at=checked_at,
    )


def build_recent_strategy_gate_summary(
    config: AppConfig,
    *,
    limit: int,
    strategy_id: str | None = None,
    now: datetime | None = None,
) -> StrategyGateSummarySnapshot:
    effective_strategy_id = strategy_id or CONTROL_STRATEGY_ID
    capped_limit = min(max(limit, 1), 500)
    checked_at = _as_utc(now or datetime.now(UTC))
    rows: list[StrategyDecision] = []
    current_open_position_count = 0
    if config.database_url:
        try:
            engine = create_engine_from_config(config)
            try:
                session_factory = create_session_factory(engine)
                with session_factory() as session:
                    rows = StrategyDecisionsRepository(session).list_recent_decisions(
                        limit=capped_limit,
                        strategy_id=effective_strategy_id,
                    )
                    current_open_position_count = StrategyDryRunRepository(
                        session
                    ).count_open_positions(
                        strategy_id=effective_strategy_id
                    )
            finally:
                engine.dispose()
        except SQLAlchemyError:
            rows = []
            current_open_position_count = 0

    by_state: dict[str, int] = {}
    by_reason: dict[str, int] = {}
    by_gate: dict[str, dict[str, Any]] = {}
    latest_enter_dry_run: StrategyDecision | None = None

    for row in rows:
        _increment_counter(by_state, row.decision_state)
        _increment_counter(by_reason, row.primary_reason)
        if row.decision_state == STATE_ENTER_DRY_RUN and latest_enter_dry_run is None:
            latest_enter_dry_run = row

        measurements = row.measurements if isinstance(row.measurements, dict) else {}
        gate_results = measurements.get("gate_results")
        if not isinstance(gate_results, dict):
            continue
        for gate_name, gate_value in gate_results.items():
            if not isinstance(gate_value, dict):
                continue
            gate_summary = by_gate.setdefault(
                str(gate_name),
                {
                    "count": 0,
                    "status_counts": {},
                    "reason_counts": {},
                    "latest_status": None,
                    "latest_reason": None,
                },
            )
            gate_summary["count"] += 1
            status = str(gate_value.get("status") or "unknown")
            _increment_counter(gate_summary["status_counts"], status)
            reason = gate_value.get("reason")
            if reason is not None:
                _increment_counter(gate_summary["reason_counts"], str(reason))
            if gate_summary["latest_status"] is None:
                gate_summary["latest_status"] = status
                gate_summary["latest_reason"] = reason

    latest_decision = rows[0] if rows else None
    latest_blockers = (
        _string_list(latest_decision.blockers) if latest_decision is not None else []
    )
    return StrategyGateSummarySnapshot(
        limit=capped_limit,
        count=len(rows),
        checked_at=checked_at,
        by_state=by_state,
        by_reason=by_reason,
        by_gate=by_gate,
        latest_decision=strategy_decision_snapshot(latest_decision),
        latest_enter_dry_run=strategy_decision_snapshot(latest_enter_dry_run),
        latest_blockers=latest_blockers,
        current_open_position_count=current_open_position_count,
    )


def build_strategy_variants_comparison(
    config: AppConfig,
    *,
    window_seconds: int,
    now: datetime | None = None,
) -> StrategyVariantsComparisonSnapshot:
    generated_at = _as_utc(now or datetime.now(UTC))
    since = generated_at - timedelta(seconds=window_seconds)
    safety = assess_startup_safety(config)
    warnings: list[str] = []
    blockers: list[str] = []
    variants: dict[str, Any] = {}
    heartbeat_variants: dict[str, Any] = {}

    if not config.database_url:
        blockers.append("database_not_configured_for_strategy_variant_comparison")
        return StrategyVariantsComparisonSnapshot(
            window_seconds=window_seconds,
            generated_at=generated_at,
            challenger_enabled=None,
            safety=safety.to_dict(),
            variants=variants,
            warnings=warnings,
            blockers=blockers,
        )

    try:
        engine = create_engine_from_config(config)
        try:
            session_factory = create_session_factory(engine)
            with session_factory() as session:
                heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat(
                    WORKER_SERVICE_STRATEGY
                )
                heartbeat_variants = _strategy_variants_worker_metadata(
                    heartbeat.metadata_ if heartbeat else None
                )
                decisions = StrategyDecisionsRepository(session)
                dry_run = StrategyDryRunRepository(session)
                strategy_ids = set(heartbeat_variants) | {
                    CONTROL_STRATEGY_ID,
                    CHALLENGER_STRATEGY_ID,
                }
                for strategy_id in sorted(strategy_ids):
                    latest = decisions.get_latest_decision(strategy_id=strategy_id)
                    variants[strategy_id] = {
                        "worker": heartbeat_variants.get(strategy_id),
                        "configured_thresholds": (
                            latest.measurements.get("config")
                            if latest is not None
                            and isinstance(latest.measurements, dict)
                            else None
                        ),
                        **decisions.comparison_summary_since(
                            strategy_id=strategy_id,
                            since=since,
                        ),
                        **dry_run.comparison_summary_since(
                            strategy_id=strategy_id,
                            since=since,
                        ),
                    }
        finally:
            engine.dispose()
    except SQLAlchemyError:
        warnings.append("strategy_variant_comparison_database_error")

    challenger = heartbeat_variants.get(CHALLENGER_STRATEGY_ID)
    return StrategyVariantsComparisonSnapshot(
        window_seconds=window_seconds,
        generated_at=generated_at,
        challenger_enabled=(
            bool(challenger.get("enabled")) if isinstance(challenger, dict) else None
        ),
        safety=safety.to_dict(),
        variants=variants,
        warnings=warnings,
        blockers=blockers,
    )


def build_strategy_dry_run_status(
    config: AppConfig,
    *,
    strategy_id: str | None = None,
    now: datetime | None = None,
) -> StrategyDryRunStatusSnapshot:
    effective_strategy_id = strategy_id or config.strategy_id
    checked_at = _as_utc(now or datetime.now(UTC))
    safety = assess_startup_safety(config)
    enabled = _dry_run_runtime_enabled(config, safety)
    worker_metadata: dict[str, Any] | None = None
    open_position_count = 0
    latest_event: StrategyDryRunEvent | None = None
    latest_decision: StrategyDecision | None = None
    latest_enter_decision: StrategyDecision | None = None
    warnings: list[str] = []
    blockers: list[str] = []

    if config.strategy_dry_run_enabled and not config.strategy_observer_enabled:
        blockers.append("strategy_dry_run_requires_strategy_observer_enabled")

    if not config.database_url:
        if config.strategy_dry_run_enabled:
            blockers.append("database_not_configured_for_strategy_dry_run")
        return StrategyDryRunStatusSnapshot(
            enabled=enabled,
            worker_observed_enabled=None,
            app_mode=config.app_mode.value,
            trading_enabled=config.trading_enabled,
            execute=config.execute,
            is_safe=safety.is_safe,
            open_position_count=0,
            max_open_positions=config.strategy_dry_run_max_open_positions,
            latest_event=StrategyDryRunEventSnapshot(found=False),
            latest_enter_decision=StrategyDecisionSnapshot(found=False),
            last_evaluated_at=None,
            warnings=warnings,
            blockers=blockers,
            checked_at=checked_at,
        )

    try:
        engine = create_engine_from_config(config)
        try:
            session_factory = create_session_factory(engine)
            with session_factory() as session:
                dry_run_repository = StrategyDryRunRepository(session)
                open_position_count = dry_run_repository.count_open_positions(
                    strategy_id=effective_strategy_id
                )
                latest_event = dry_run_repository.get_latest_event(
                    strategy_id=effective_strategy_id
                )
                latest_decision = StrategyDecisionsRepository(session).get_latest_decision(
                    strategy_id=effective_strategy_id
                )
                latest_enter_id = dry_run_repository.get_latest_enter_decision_id(
                    strategy_id=effective_strategy_id
                )
                if latest_enter_id is not None:
                    latest_enter_decision = StrategyDecisionsRepository(
                        session
                    ).get_decision_by_id(latest_enter_id)
                heartbeat_repository = WorkerHeartbeatRepository(session)
                component_heartbeat = heartbeat_repository.get_latest_heartbeat(
                    WORKER_SERVICE_STRATEGY
                )
                worker_metadata = _strategy_dry_run_worker_metadata(
                    component_heartbeat.metadata_ if component_heartbeat else None
                )
                if worker_metadata is None:
                    heartbeat = heartbeat_repository.get_latest_heartbeat(
                        WORKER_SERVICE_AGGREGATE
                    )
                    worker_metadata = _strategy_dry_run_worker_metadata(
                        heartbeat.metadata_ if heartbeat else None
                    )
                    if worker_metadata is not None:
                        warnings.append("feed_liveness_legacy_aggregate_fallback")
        finally:
            engine.dispose()
    except SQLAlchemyError:
        warnings.append("strategy_dry_run_status_database_error")

    worker_observed_enabled = (
        None if worker_metadata is None else bool(worker_metadata.get("enabled"))
    )
    if worker_observed_enabled is not None:
        enabled = worker_observed_enabled
    warnings.extend(_string_list(worker_metadata.get("warnings") if worker_metadata else []))
    blockers.extend(_string_list(worker_metadata.get("blockers") if worker_metadata else []))
    if config.strategy_dry_run_enabled and config.app_mode is not AppMode.DRY_RUN:
        blockers.append("strategy_dry_run_requires_app_mode_dry_run")
    if config.trading_enabled or config.execute:
        blockers.append("strategy_dry_run_requires_trading_and_execute_false")

    return StrategyDryRunStatusSnapshot(
        enabled=enabled,
        worker_observed_enabled=worker_observed_enabled,
        app_mode=config.app_mode.value,
        trading_enabled=config.trading_enabled,
        execute=config.execute,
        is_safe=safety.is_safe,
        open_position_count=open_position_count,
        max_open_positions=config.strategy_dry_run_max_open_positions,
        latest_event=strategy_dry_run_event_snapshot(latest_event),
        latest_enter_decision=strategy_decision_snapshot(latest_enter_decision),
        last_evaluated_at=latest_decision.evaluated_at if latest_decision else None,
        warnings=_unique_strings(warnings),
        blockers=_unique_strings(blockers),
        checked_at=checked_at,
    )


def build_open_strategy_dry_run_positions(
    config: AppConfig,
    *,
    strategy_id: str | None = None,
    now: datetime | None = None,
) -> StrategyDryRunPositionsSnapshot:
    effective_strategy_id = strategy_id or config.strategy_id
    checked_at = _as_utc(now or datetime.now(UTC))
    if not config.database_url:
        return StrategyDryRunPositionsSnapshot(
            limit=config.strategy_dry_run_max_open_positions,
            count=0,
            positions=[],
            checked_at=checked_at,
        )

    try:
        engine = create_engine_from_config(config)
        try:
            session_factory = create_session_factory(engine)
            with session_factory() as session:
                positions = StrategyDryRunRepository(session).list_open_positions(
                    strategy_id=effective_strategy_id
                )
        finally:
            engine.dispose()
    except SQLAlchemyError:
        positions = []

    snapshots = [strategy_dry_run_position_snapshot(row) for row in positions]
    return StrategyDryRunPositionsSnapshot(
        limit=config.strategy_dry_run_max_open_positions,
        count=len(snapshots),
        positions=snapshots,
        checked_at=checked_at,
    )


def build_recent_strategy_dry_run_positions(
    config: AppConfig,
    *,
    limit: int,
    strategy_id: str | None = None,
    now: datetime | None = None,
) -> StrategyDryRunPositionsSnapshot:
    effective_strategy_id = strategy_id or config.strategy_id
    capped_limit = min(max(limit, 1), 500)
    checked_at = _as_utc(now or datetime.now(UTC))
    if not config.database_url:
        return StrategyDryRunPositionsSnapshot(
            limit=capped_limit,
            count=0,
            positions=[],
            checked_at=checked_at,
        )

    try:
        engine = create_engine_from_config(config)
        try:
            session_factory = create_session_factory(engine)
            with session_factory() as session:
                rows = StrategyDryRunRepository(session).list_recent_positions(
                    limit=capped_limit,
                    strategy_id=effective_strategy_id,
                )
        finally:
            engine.dispose()
    except SQLAlchemyError:
        rows = []

    snapshots = [strategy_dry_run_position_snapshot(row) for row in rows]
    return StrategyDryRunPositionsSnapshot(
        limit=capped_limit,
        count=len(snapshots),
        positions=snapshots,
        checked_at=checked_at,
    )


def build_recent_strategy_dry_run_events(
    config: AppConfig,
    *,
    limit: int,
    strategy_id: str | None = None,
    now: datetime | None = None,
) -> StrategyDryRunEventsSnapshot:
    effective_strategy_id = strategy_id or CONTROL_STRATEGY_ID
    capped_limit = min(max(limit, 1), 500)
    checked_at = _as_utc(now or datetime.now(UTC))
    if not config.database_url:
        return StrategyDryRunEventsSnapshot(
            limit=capped_limit,
            count=0,
            events=[],
            checked_at=checked_at,
        )

    try:
        engine = create_engine_from_config(config)
        try:
            session_factory = create_session_factory(engine)
            with session_factory() as session:
                rows = StrategyDryRunRepository(session).list_recent_events(
                    limit=capped_limit,
                    strategy_id=effective_strategy_id,
                )
        finally:
            engine.dispose()
    except SQLAlchemyError:
        rows = []

    snapshots = [strategy_dry_run_event_snapshot(row) for row in rows]
    return StrategyDryRunEventsSnapshot(
        limit=capped_limit,
        count=len(snapshots),
        events=snapshots,
        checked_at=checked_at,
    )


def strategy_decision_snapshot(
    decision: StrategyDecision | None,
) -> StrategyDecisionSnapshot:
    if decision is None:
        return StrategyDecisionSnapshot(found=False)

    return StrategyDecisionSnapshot(
        found=True,
        decision_id=decision.decision_id,
        strategy_id=decision.strategy_id,
        evaluated_at=decision.evaluated_at,
        decision_state=decision.decision_state,
        primary_reason=decision.primary_reason,
        app_mode=decision.app_mode,
        market_ticker=decision.market_ticker,
        candidate_side=decision.candidate_side,
        boundary=decision.boundary,
        brti_value=decision.brti_value,
        distance_bps=decision.distance_bps,
        seconds_left=decision.seconds_left,
        measurements=decision.measurements,
        blockers=decision.blockers,
        warnings=decision.warnings,
        raw_context_hash=decision.raw_context_hash,
    )


def strategy_dry_run_position_snapshot(
    position: StrategyDryRunPosition | None,
) -> StrategyDryRunPositionSnapshot:
    if position is None:
        return StrategyDryRunPositionSnapshot(found=False)

    return StrategyDryRunPositionSnapshot(
        found=True,
        position_id=position.position_id,
        market_ticker=position.market_ticker,
        strategy_id=position.strategy_id,
        side_candidate=position.side_candidate,
        status=position.status,
        opened_at=position.opened_at,
        open_price=position.open_price,
        contract_count=position.contract_count,
        boundary=position.boundary,
        brti_at_entry=position.brti_at_entry,
        distance_bps_at_entry=position.distance_bps_at_entry,
        decision_id=position.decision_id,
        closed_at=position.closed_at,
        close_price=position.close_price,
        close_reason=position.close_reason,
        realized_pnl_cents=position.realized_pnl_cents,
        measurements_summary=_measurement_summary(position.measurements),
    )


def strategy_dry_run_event_snapshot(
    event: StrategyDryRunEvent | None,
) -> StrategyDryRunEventSnapshot:
    if event is None:
        return StrategyDryRunEventSnapshot(found=False)

    return StrategyDryRunEventSnapshot(
        found=True,
        event_id=event.event_id,
        strategy_id=event.strategy_id,
        position_id=event.position_id,
        decision_id=event.decision_id,
        event_type=event.event_type,
        market_ticker=event.market_ticker,
        occurred_at=event.occurred_at,
        side_candidate=event.side_candidate,
        price=event.price,
        contract_count=event.contract_count,
        reason=event.reason,
        measurements_summary=_measurement_summary(event.measurements),
    )


def _status_snapshot(
    *,
    config: AppConfig,
    safety: SafetyAssessment,
    checked_at: datetime,
    latest_decision: StrategyDecision | None,
    worker_metadata: dict[str, Any] | None,
    variants: JsonPayload,
    warnings: list[str],
    blockers: list[str],
) -> StrategyStatusSnapshot:
    worker_observed_enabled = (
        None if worker_metadata is None else bool(worker_metadata.get("enabled"))
    )
    effective_enabled = (
        config.strategy_observer_enabled
        if worker_observed_enabled is None
        else worker_observed_enabled
    )
    worker_warnings = _string_list(worker_metadata.get("warnings") if worker_metadata else [])
    worker_blockers = _string_list(worker_metadata.get("blockers") if worker_metadata else [])
    warnings = [*warnings, *worker_warnings]
    blockers = [*blockers, *worker_blockers]

    decision_age_seconds = None
    stale = False
    if latest_decision is not None:
        decision_age_seconds = max(
            0.0,
            (checked_at - _as_utc(latest_decision.evaluated_at)).total_seconds(),
        )
        stale = (
            effective_enabled
            and decision_age_seconds > config.strategy_observer_decision_ttl_seconds
        )
    elif effective_enabled:
        stale = True
        blockers.append("strategy_decision_missing")

    if worker_metadata is not None:
        connection_state = str(worker_metadata.get("connection_state") or "unknown")
    elif not effective_enabled:
        connection_state = "disabled"
    elif blockers:
        connection_state = "blocked"
    else:
        connection_state = "unknown"

    latest_measurements_summary = (
        _measurement_summary(latest_decision.measurements)
        if latest_decision is not None
        else None
    )
    gate_results_summary = (
        _gate_results_summary(latest_decision.measurements.get("gate_results"))
        if latest_decision is not None
        and isinstance(latest_decision.measurements, dict)
        else None
    )

    return StrategyStatusSnapshot(
        enabled=effective_enabled,
        worker_observed_enabled=worker_observed_enabled,
        connection_state=connection_state,
        variants=variants,
        app_mode=config.app_mode.value,
        trading_enabled=config.trading_enabled,
        execute=config.execute,
        is_safe=safety.is_safe,
        latest_decision_id=latest_decision.decision_id if latest_decision else None,
        latest_evaluated_at=latest_decision.evaluated_at if latest_decision else None,
        latest_decision_state=latest_decision.decision_state if latest_decision else None,
        latest_primary_reason=latest_decision.primary_reason if latest_decision else None,
        market_ticker=latest_decision.market_ticker if latest_decision else None,
        candidate_side=latest_decision.candidate_side if latest_decision else None,
        boundary=latest_decision.boundary if latest_decision else None,
        brti_value=latest_decision.brti_value if latest_decision else None,
        distance_bps=latest_decision.distance_bps if latest_decision else None,
        seconds_left=latest_decision.seconds_left if latest_decision else None,
        latest_measurements_summary=latest_measurements_summary,
        market_liveness_source=_summary_text(
            latest_measurements_summary,
            "market_liveness_source",
        ),
        reference_liveness_source=_summary_text(
            latest_measurements_summary,
            "reference_liveness_source",
        ),
        market_component_heartbeat_at=_summary_text(
            latest_measurements_summary,
            "market_component_heartbeat_at",
        ),
        reference_component_heartbeat_at=_summary_text(
            latest_measurements_summary,
            "reference_component_heartbeat_at",
        ),
        market_component_heartbeat_age_ms=_summary_int(
            latest_measurements_summary,
            "market_component_heartbeat_age_ms",
        ),
        reference_component_heartbeat_age_ms=_summary_int(
            latest_measurements_summary,
            "reference_component_heartbeat_age_ms",
        ),
        liveness_source_mismatch=_summary_bool(
            latest_measurements_summary,
            "liveness_source_mismatch",
        ),
        market_feed_transport_state=_summary_text(
            latest_measurements_summary,
            "market_feed_transport_state",
        ),
        market_feed_subscription_state=_summary_text(
            latest_measurements_summary,
            "market_feed_subscription_state",
        ),
        market_feed_snapshot_state=_summary_text(
            latest_measurements_summary,
            "market_feed_snapshot_state",
        ),
        market_feed_active_ticker_state=_summary_text(
            latest_measurements_summary,
            "market_feed_active_ticker_state",
        ),
        market_feed_sequence_state=_summary_text(
            latest_measurements_summary,
            "market_feed_sequence_state",
        ),
        market_data_quiet=_summary_bool(
            latest_measurements_summary,
            "market_data_quiet",
        ),
        orderbook_recovery_action=_summary_text(
            latest_measurements_summary,
            "orderbook_recovery_action",
        ),
        market_feed_state=_summary_text(
            latest_measurements_summary,
            "market_feed_state",
        ),
        market_subscription_recovery_count=_summary_int(
            latest_measurements_summary,
            "market_subscription_recovery_count",
        ),
        market_subscription_recovery_last_reason=_summary_text(
            latest_measurements_summary,
            "market_subscription_recovery_last_reason",
        ),
        market_subscription_recovery_last_action=_summary_text(
            latest_measurements_summary,
            "market_subscription_recovery_last_action",
        ),
        market_subscription_recovery_last_result=_summary_text(
            latest_measurements_summary,
            "market_subscription_recovery_last_result",
        ),
        market_subscription_recovery_last_at=_summary_text(
            latest_measurements_summary,
            "market_subscription_recovery_last_at",
        ),
        market_snapshot_resync_count=_summary_int(
            latest_measurements_summary,
            "market_snapshot_resync_count",
        ),
        market_snapshot_resync_last_result=_summary_text(
            latest_measurements_summary,
            "market_snapshot_resync_last_result",
        ),
        market_rollover_recovery_count=_summary_int(
            latest_measurements_summary,
            "market_rollover_recovery_count",
        ),
        market_transport_reconnect_count=_summary_int(
            latest_measurements_summary,
            "market_transport_reconnect_count",
        ),
        market_unrecovered_blocker_count=_summary_int(
            latest_measurements_summary,
            "market_unrecovered_blocker_count",
        ),
        market_recovery_attempt_in_progress=_summary_bool(
            latest_measurements_summary,
            "market_recovery_attempt_in_progress",
        ),
        market_recovery_attempt_age_ms=_summary_int(
            latest_measurements_summary,
            "market_recovery_attempt_age_ms",
        ),
        gate_results_summary=gate_results_summary,
        decision_age_seconds=decision_age_seconds,
        stale=stale,
        warnings=_unique_strings(warnings),
        blockers=_unique_strings(blockers),
        checked_at=checked_at,
    )


def _measurements(
    *,
    config: AppConfig,
    safety: SafetyAssessment,
    decision_state: str,
    primary_reason: str,
    decision_warnings: list[str],
    evaluated_at: datetime,
    thresholds: dict[str, Any],
    market: Market | None,
    boundary: Decimal | None,
    boundary_source: str | None,
    reference_tick: ReferenceTick | None,
    reference_worker_metadata: dict[str, Any] | None,
    orderbook_worker_metadata: dict[str, Any] | None,
    reference_worker_heartbeat_at: datetime | None,
    reference_worker_heartbeat_age_ms: int | None,
    market_liveness_source: str,
    reference_liveness_source: str,
    market_component_heartbeat_at: datetime | None,
    reference_component_heartbeat_at: datetime | None,
    market_component_heartbeat_age_ms: int | None,
    reference_component_heartbeat_age_ms: int | None,
    latest_aggregate_heartbeat_mode: str | None,
    latest_component_heartbeat_mode: str | None,
    liveness_source_mismatch: bool,
    brti_value: Decimal | None,
    brti_backend_age_ms: int | None,
    brti_source_age_ms: int | None,
    brti_age_ms: int | None,
    brti_strategy_fresh_age_ms: int | None,
    brti_reference_source_warn_ms: int,
    brti_reference_source_hard_limit_ms: int,
    brti_reference_status_category: str | None,
    brti_reference_transport_stale: bool,
    brti_reference_persistence_stale: bool,
    brti_reference_worker_heartbeat_stale: bool,
    brti_reference_trade_ready_fresh: bool,
    brti_reference_backend_transport_lag_ms: int | None,
    brti_reference_time_since_last_valid_tick_ms: int | None,
    brti_reference_stale_reason: str | None,
    brti_reference_stream_age_ms: int | None,
    brti_reference_last_valid_message_at: datetime | None,
    brti_reference_last_valid_message_source_ts: datetime | None,
    brti_reference_last_valid_message_value: str | None,
    brti_reference_last_duplicate_valid_message_at: datetime | None,
    brti_reference_valid_message_carried_forward: bool,
    brti_reference_carry_forward_allowed: bool,
    brti_reference_carry_forward_age_ms: int | None,
    brti_reference_liveness_reason: str | None,
    orderbook: OrderbookSnapshot | None,
    orderbook_age_ms: int | None,
    orderbook_stream_age_ms: int | None,
    orderbook_stream_connection_state: str | None,
    orderbook_stream_active_market_ticker: str | None,
    orderbook_stream_warnings: list[str],
    orderbook_stream_blockers: list[str],
    market_feed_transport_state: str,
    market_feed_subscription_state: str,
    market_feed_snapshot_state: str,
    market_feed_active_ticker_state: str,
    market_feed_sequence_state: str,
    market_data_quiet: bool,
    market_data_quiet_age_ms: int | None,
    orderbook_snapshot_age_ms: int | None,
    orderbook_recovery_action: str | None,
    orderbook_carry_forward_allowed: bool,
    orderbook_carry_forward_age_ms: int | None,
    orderbook_liveness_reason: str | None,
    orderbook_snapshot_source: str | None,
    latest_trade: PublicTrade | None,
    latest_trade_age_ms: int | None,
    seconds_since_open: int | None,
    seconds_left: int | None,
    candidate_side: str | None,
    distance_bps: Decimal | None,
    desired_bid: Decimal | None,
    desired_ask: Decimal | None,
    desired_spread: Decimal | None,
    desired_spread_cents: Decimal | None,
    desired_mid: Decimal | None,
    desired_top_book_size: Decimal | None,
    brti_short_price: Decimal | None,
    brti_medium_price: Decimal | None,
    brti_long_price: Decimal | None,
    brti_short_move_bps: Decimal | None,
    brti_medium_move_bps: Decimal | None,
    brti_long_move_bps: Decimal | None,
    brti_directional_tick_ratio: Decimal | None,
    brti_short_point_count: int,
    brti_medium_point_count: int,
    brti_long_point_count: int,
    boundary_cross_count: int | None,
    retrace_fraction: Decimal | None,
    contract_mid_move_cents: Decimal | None,
    ask_pullback_cents: Decimal | None,
    recent_trade_count: int,
    candidate_trade_ratio: Decimal | None,
    dry_run_risk_state: str | None,
    dry_run_intended_entry_price: Decimal | None,
    dry_run_intended_contract_count: int | None,
    dry_run_position_id: str | None,
    managing_position: StrategyDryRunPosition | None,
) -> dict[str, Any]:
    gate_results = _gate_results(
        config=config,
        decision_state=decision_state,
        primary_reason=primary_reason,
        decision_warnings=decision_warnings,
        market=market,
        boundary=boundary,
        boundary_source=boundary_source,
        brti_value=brti_value,
        brti_backend_age_ms=brti_backend_age_ms,
        brti_source_age_ms=brti_source_age_ms,
        brti_strategy_fresh_age_ms=brti_strategy_fresh_age_ms,
        brti_reference_stale_reason=brti_reference_stale_reason,
        brti_reference_stream_age_ms=brti_reference_stream_age_ms,
        brti_reference_carry_forward_allowed=brti_reference_carry_forward_allowed,
        brti_reference_carry_forward_age_ms=brti_reference_carry_forward_age_ms,
        brti_reference_valid_message_carried_forward=(
            brti_reference_valid_message_carried_forward
        ),
        brti_reference_transport_stale=brti_reference_transport_stale,
        brti_reference_persistence_stale=brti_reference_persistence_stale,
        brti_reference_worker_heartbeat_stale=brti_reference_worker_heartbeat_stale,
        orderbook_age_ms=orderbook_age_ms,
        orderbook_stream_age_ms=orderbook_stream_age_ms,
        market_feed_transport_state=market_feed_transport_state,
        market_feed_subscription_state=market_feed_subscription_state,
        market_feed_snapshot_state=market_feed_snapshot_state,
        market_feed_active_ticker_state=market_feed_active_ticker_state,
        market_feed_sequence_state=market_feed_sequence_state,
        market_data_quiet=market_data_quiet,
        market_data_quiet_age_ms=market_data_quiet_age_ms,
        orderbook_snapshot_age_ms=orderbook_snapshot_age_ms,
        orderbook_recovery_action=orderbook_recovery_action,
        orderbook_carry_forward_allowed=orderbook_carry_forward_allowed,
        orderbook_carry_forward_age_ms=orderbook_carry_forward_age_ms,
        orderbook_liveness_reason=orderbook_liveness_reason,
        orderbook_snapshot_source=orderbook_snapshot_source,
        seconds_left=seconds_left,
        distance_bps=distance_bps,
        desired_bid=desired_bid,
        desired_ask=desired_ask,
        desired_mid=desired_mid,
        desired_spread=desired_spread,
        desired_spread_cents=desired_spread_cents,
        desired_top_book_size=desired_top_book_size,
        dry_run_intended_entry_price=dry_run_intended_entry_price,
        brti_short_move_bps=brti_short_move_bps,
        brti_medium_move_bps=brti_medium_move_bps,
        brti_long_move_bps=brti_long_move_bps,
        boundary_cross_count=boundary_cross_count,
        retrace_fraction=retrace_fraction,
        contract_mid_move_cents=contract_mid_move_cents,
        ask_pullback_cents=ask_pullback_cents,
        recent_trade_count=recent_trade_count,
        candidate_trade_ratio=candidate_trade_ratio,
        dry_run_risk_state=dry_run_risk_state,
    )
    return {
        "evaluated_at": _isoformat_or_none(evaluated_at),
        "market_ticker": getattr(market, "market_ticker", None),
        "event_ticker": getattr(market, "event_ticker", None),
        "open_time": _isoformat_or_none(getattr(market, "open_time", None)),
        "close_time": _isoformat_or_none(getattr(market, "close_time", None)),
        "seconds_since_open": seconds_since_open,
        "seconds_left": seconds_left,
        "boundary_source": boundary_source,
        "boundary": _decimal_text(boundary),
        "brti_value": _decimal_text(brti_value),
        "brti_received_at": _isoformat_or_none(getattr(reference_tick, "received_at", None)),
        "brti_source_ts": _isoformat_or_none(getattr(reference_tick, "source_ts", None)),
        "brti_age_ms": brti_age_ms,
        "brti_backend_age_ms": brti_backend_age_ms,
        "brti_source_age_ms": brti_source_age_ms,
        "brti_strategy_fresh_age_ms": brti_strategy_fresh_age_ms,
        "brti_reference_source_warn_ms": brti_reference_source_warn_ms,
        "brti_reference_source_hard_limit_ms": brti_reference_source_hard_limit_ms,
        "brti_reference_stale_reason": brti_reference_stale_reason,
        "brti_reference_status_category": brti_reference_status_category,
        "brti_reference_transport_stale": brti_reference_transport_stale,
        "brti_reference_persistence_stale": brti_reference_persistence_stale,
        "brti_reference_worker_heartbeat_stale": (
            brti_reference_worker_heartbeat_stale
        ),
        "brti_reference_trade_ready_fresh": brti_reference_trade_ready_fresh,
        "brti_reference_backend_transport_lag_ms": (
            brti_reference_backend_transport_lag_ms
        ),
        "brti_reference_time_since_last_valid_tick_ms": (
            brti_reference_time_since_last_valid_tick_ms
        ),
        "brti_reference_stream_age_ms": brti_reference_stream_age_ms,
        "brti_reference_last_valid_message_at": _isoformat_or_none(
            brti_reference_last_valid_message_at
        ),
        "brti_reference_last_valid_message_source_ts": _isoformat_or_none(
            brti_reference_last_valid_message_source_ts
        ),
        "brti_reference_last_valid_message_value": (
            brti_reference_last_valid_message_value
        ),
        "brti_reference_last_duplicate_valid_message_at": _isoformat_or_none(
            brti_reference_last_duplicate_valid_message_at
        ),
        "brti_reference_valid_message_carried_forward": (
            brti_reference_valid_message_carried_forward
        ),
        "brti_reference_carry_forward_allowed": (
            brti_reference_carry_forward_allowed
        ),
        "brti_reference_carry_forward_age_ms": brti_reference_carry_forward_age_ms,
        "brti_reference_carry_forward_max_age_ms": (
            config.strategy_reference_carry_forward_max_age_ms
        ),
        "brti_reference_liveness_reason": brti_reference_liveness_reason,
        "brti_reference_connection_state": _metadata_text(
            reference_worker_metadata,
            "connection_state",
        ),
        "brti_reference_recovery_state": _metadata_text(
            reference_worker_metadata,
            "recovery_state",
        ),
        "brti_reference_warnings": _metadata_string_list(
            reference_worker_metadata,
            "warnings",
        ),
        "brti_reference_blockers": _metadata_string_list(
            reference_worker_metadata,
            "blockers",
        ),
        "brti_reference_last_error_type": _metadata_text(
            reference_worker_metadata,
            "last_error_type",
        ),
        "brti_reference_last_message_at": _metadata_text(
            reference_worker_metadata,
            "last_message_at",
        ),
        "brti_reference_last_persisted_at": _metadata_text(
            reference_worker_metadata,
            "last_persisted_at",
        ),
        "brti_reference_last_valid_tick_at": _metadata_text(
            reference_worker_metadata,
            "last_valid_tick_at",
        ),
        "brti_reference_stale_since": _metadata_text(
            reference_worker_metadata,
            "stale_since",
        ),
        "brti_reference_consecutive_stale_count": _metadata_int(
            reference_worker_metadata,
            "consecutive_stale_count",
        ),
        "brti_reference_consecutive_reconnect_count": _metadata_int(
            reference_worker_metadata,
            "consecutive_reconnect_count",
        ),
        "brti_reference_worker_heartbeat_at": _isoformat_or_none(
            reference_worker_heartbeat_at
        ),
        "brti_reference_worker_heartbeat_age_ms": reference_worker_heartbeat_age_ms,
        "market_liveness_source": market_liveness_source,
        "reference_liveness_source": reference_liveness_source,
        "market_component_heartbeat_at": _isoformat_or_none(
            market_component_heartbeat_at
        ),
        "reference_component_heartbeat_at": _isoformat_or_none(
            reference_component_heartbeat_at
        ),
        "market_component_heartbeat_age_ms": market_component_heartbeat_age_ms,
        "reference_component_heartbeat_age_ms": reference_component_heartbeat_age_ms,
        "latest_aggregate_heartbeat_mode": latest_aggregate_heartbeat_mode,
        "latest_component_heartbeat_mode": latest_component_heartbeat_mode,
        "liveness_source_mismatch": liveness_source_mismatch,
        "distance_bps": _decimal_text(distance_bps),
        "candidate_side": candidate_side,
        "yes_bid": _decimal_text(getattr(orderbook, "yes_bid", None)),
        "yes_ask": _decimal_text(getattr(orderbook, "yes_ask", None)),
        "no_bid": _decimal_text(getattr(orderbook, "no_bid", None)),
        "no_ask": _decimal_text(getattr(orderbook, "no_ask", None)),
        "yes_spread": _decimal_text(getattr(orderbook, "yes_spread", None)),
        "no_spread": _decimal_text(getattr(orderbook, "no_spread", None)),
        "desired_side_bid": _decimal_text(desired_bid),
        "desired_side_ask": _decimal_text(desired_ask),
        "desired_side_mid": _decimal_text(desired_mid),
        "desired_side_spread": _decimal_text(desired_spread),
        "desired_side_spread_cents": _decimal_text(desired_spread_cents),
        "desired_top_book_size": _decimal_text(desired_top_book_size),
        "orderbook_received_at": _isoformat_or_none(getattr(orderbook, "received_at", None)),
        "orderbook_age_ms": orderbook_age_ms,
        "orderbook_sequence_number": getattr(orderbook, "sequence_number", None),
        "orderbook_stream_last_message_at": _metadata_text(
            orderbook_worker_metadata,
            "last_message_at",
        ),
        "orderbook_stream_last_ticker_at": _metadata_text(
            orderbook_worker_metadata,
            "last_ticker_at",
        ),
        "orderbook_stream_last_trade_at": _metadata_text(
            orderbook_worker_metadata,
            "last_trade_at",
        ),
        "orderbook_stream_last_orderbook_at": _metadata_text(
            orderbook_worker_metadata,
            "last_orderbook_at",
        ),
        "orderbook_stream_age_ms": orderbook_stream_age_ms,
        "orderbook_stream_connection_state": orderbook_stream_connection_state,
        "orderbook_stream_active_market_ticker": orderbook_stream_active_market_ticker,
        "worker_role": _metadata_text(orderbook_worker_metadata, "worker_role"),
        "connection_id": _metadata_text(orderbook_worker_metadata, "connection_id"),
        "protocol_connection_state": _metadata_text(
            orderbook_worker_metadata,
            "protocol_connection_state",
        ),
        "subscription_reconciled": _metadata_bool(
            orderbook_worker_metadata,
            "subscription_reconciled",
        ),
        "orderbook_sid_confirmed": _metadata_bool(
            orderbook_worker_metadata,
            "orderbook_sid_confirmed",
        ),
        "ticker_sid_confirmed": _metadata_bool(
            orderbook_worker_metadata,
            "ticker_sid_confirmed",
        ),
        "trade_sid_confirmed": _metadata_bool(
            orderbook_worker_metadata,
            "trade_sid_confirmed",
        ),
        "last_list_subscriptions_at": _metadata_text(
            orderbook_worker_metadata,
            "last_list_subscriptions_at",
        ),
        "last_list_subscriptions_result": _metadata_text(
            orderbook_worker_metadata,
            "last_list_subscriptions_result",
        ),
        "in_flight_snapshot_request": _metadata_bool(
            orderbook_worker_metadata,
            "in_flight_snapshot_request",
        ),
        "snapshot_request_age_ms": _metadata_int(
            orderbook_worker_metadata,
            "snapshot_request_age_ms",
        ),
        "protocol_event_recent_error_count": _metadata_int(
            orderbook_worker_metadata,
            "protocol_event_recent_error_count",
        ),
        "db_writer_queue_depth": _metadata_int(
            orderbook_worker_metadata,
            "db_writer_queue_depth",
        ),
        "db_writer_queue_oldest_age_ms": _metadata_int(
            orderbook_worker_metadata,
            "db_writer_queue_oldest_age_ms",
        ),
        "db_writer_last_flush_ms": _metadata_int(
            orderbook_worker_metadata,
            "db_writer_last_flush_ms",
        ),
        "db_writer_slow_flush_count": _metadata_int(
            orderbook_worker_metadata,
            "db_writer_slow_flush_count",
        ),
        "orderbook_persistence_pending": _metadata_bool(
            orderbook_worker_metadata,
            "orderbook_persistence_pending",
        ),
        "orderbook_persistence_pending_count": _metadata_int(
            orderbook_worker_metadata,
            "orderbook_persistence_pending_count",
        ),
        "orderbook_persistence_pending_age_ms": _metadata_int(
            orderbook_worker_metadata,
            "orderbook_persistence_pending_age_ms",
        ),
        "reconnect_reason": _metadata_text(orderbook_worker_metadata, "reconnect_reason"),
        "close_code": _metadata_int(orderbook_worker_metadata, "close_code"),
        "close_reason": _metadata_text(orderbook_worker_metadata, "close_reason"),
        "orderbook_stream_warnings": orderbook_stream_warnings,
        "orderbook_stream_blockers": orderbook_stream_blockers,
        "market_feed_transport_state": market_feed_transport_state,
        "market_feed_subscription_state": market_feed_subscription_state,
        "market_feed_snapshot_state": market_feed_snapshot_state,
        "market_feed_active_ticker_state": market_feed_active_ticker_state,
        "market_feed_sequence_state": market_feed_sequence_state,
        "market_data_quiet": market_data_quiet,
        "market_data_quiet_age_ms": market_data_quiet_age_ms,
        "orderbook_snapshot_age_ms": orderbook_snapshot_age_ms,
        "orderbook_recovery_action": orderbook_recovery_action,
        "market_feed_state": _metadata_text(
            orderbook_worker_metadata,
            "market_feed_state",
        ),
        "market_subscription_recovery_count": _metadata_int(
            orderbook_worker_metadata,
            "market_subscription_recovery_count",
        ),
        "market_subscription_recovery_last_reason": _metadata_text(
            orderbook_worker_metadata,
            "market_subscription_recovery_last_reason",
        ),
        "market_subscription_recovery_last_action": _metadata_text(
            orderbook_worker_metadata,
            "market_subscription_recovery_last_action",
        ),
        "market_subscription_recovery_last_result": _metadata_text(
            orderbook_worker_metadata,
            "market_subscription_recovery_last_result",
        ),
        "market_subscription_recovery_last_at": _metadata_text(
            orderbook_worker_metadata,
            "market_subscription_recovery_last_at",
        ),
        "market_snapshot_resync_count": _metadata_int(
            orderbook_worker_metadata,
            "market_snapshot_resync_count",
        ),
        "market_snapshot_resync_last_result": _metadata_text(
            orderbook_worker_metadata,
            "market_snapshot_resync_last_result",
        ),
        "market_rollover_recovery_count": _metadata_int(
            orderbook_worker_metadata,
            "market_rollover_recovery_count",
        ),
        "market_transport_reconnect_count": _metadata_int(
            orderbook_worker_metadata,
            "market_transport_reconnect_count",
        ),
        "market_unrecovered_blocker_count": _metadata_int(
            orderbook_worker_metadata,
            "market_unrecovered_blocker_count",
        ),
        "market_recovery_attempt_in_progress": _metadata_bool(
            orderbook_worker_metadata,
            "market_recovery_attempt_in_progress",
        ),
        "market_recovery_attempt_age_ms": _metadata_int(
            orderbook_worker_metadata,
            "market_recovery_attempt_age_ms",
        ),
        "orderbook_carry_forward_allowed": orderbook_carry_forward_allowed,
        "orderbook_carry_forward_age_ms": orderbook_carry_forward_age_ms,
        "orderbook_carry_forward_max_age_ms": (
            config.strategy_kalshi_book_carry_forward_max_age_ms
        ),
        "orderbook_liveness_reason": orderbook_liveness_reason,
        "orderbook_snapshot_source": orderbook_snapshot_source,
        "latest_trade_received_at": _isoformat_or_none(
            getattr(latest_trade, "received_at", None)
        ),
        "latest_trade_age_ms": latest_trade_age_ms,
        "brti_lookback_short_price": _decimal_text(brti_short_price),
        "brti_lookback_medium_price": _decimal_text(brti_medium_price),
        "brti_lookback_long_price": _decimal_text(brti_long_price),
        "brti_move_short_bps": _decimal_text(brti_short_move_bps),
        "brti_move_medium_bps": _decimal_text(brti_medium_move_bps),
        "brti_move_long_bps": _decimal_text(brti_long_move_bps),
        "brti_directional_tick_ratio": _decimal_text(brti_directional_tick_ratio),
        "brti_short_point_count": brti_short_point_count,
        "brti_medium_point_count": brti_medium_point_count,
        "brti_long_point_count": brti_long_point_count,
        "boundary_cross_count": boundary_cross_count,
        "retrace_fraction": _decimal_text(retrace_fraction),
        "contract_mid_move_cents": _decimal_text(contract_mid_move_cents),
        "ask_pullback_cents": _decimal_text(ask_pullback_cents),
        "recent_trade_count": recent_trade_count,
        "candidate_trade_ratio": _decimal_text(candidate_trade_ratio),
        "dry_run_enabled": config.strategy_dry_run_enabled,
        "strategy_id": config.strategy_id,
        "dry_run_risk_state": dry_run_risk_state,
        "dry_run_intended_entry_price": _decimal_text(dry_run_intended_entry_price),
        "strategy_dry_run_min_entry_price": _decimal_text(
            config.strategy_dry_run_min_entry_price
        ),
        "strategy_dry_run_max_entry_price": _decimal_text(
            config.strategy_dry_run_max_entry_price
        ),
        "dry_run_intended_contract_count": dry_run_intended_contract_count,
        "dry_run_position_id": dry_run_position_id,
        "managed_position_id": getattr(managing_position, "position_id", None),
        "managed_position_open_price": _decimal_text(
            getattr(managing_position, "open_price", None)
        ),
        "safety_mode": safety.mode,
        "trading_enabled": safety.trading_enabled,
        "execute": safety.execute,
        "observer_only": config.app_mode is AppMode.OBSERVER,
        "config": thresholds,
        "series_ticker": config.kalshi_btc15_series_ticker,
        "gate_results": gate_results,
        "gate_results_summary": _gate_results_summary(gate_results),
    }


def _thresholds(config: AppConfig) -> dict[str, Any]:
    return {
        "strategy_observer_poll_seconds": config.strategy_observer_poll_seconds,
        "strategy_observer_decision_ttl_seconds": config.strategy_observer_decision_ttl_seconds,
        "strategy_dry_run_enabled": config.strategy_dry_run_enabled,
        "strategy_id": config.strategy_id,
        "strategy_dry_run_max_open_positions": config.strategy_dry_run_max_open_positions,
        "strategy_dry_run_one_entry_per_market": (
            config.strategy_dry_run_one_entry_per_market
        ),
        "strategy_dry_run_position_size_contracts": (
            config.strategy_dry_run_position_size_contracts
        ),
        "strategy_dry_run_entry_price_offset_cents": (
            config.strategy_dry_run_entry_price_offset_cents
        ),
        "strategy_dry_run_min_seconds_between_decisions": (
            config.strategy_dry_run_min_seconds_between_decisions
        ),
        "strategy_brti_lookback_short_seconds": config.strategy_brti_lookback_short_seconds,
        "strategy_brti_lookback_medium_seconds": config.strategy_brti_lookback_medium_seconds,
        "strategy_brti_lookback_long_seconds": config.strategy_brti_lookback_long_seconds,
        "strategy_brti_min_move_short_bps": config.strategy_brti_min_move_short_bps,
        "strategy_brti_min_move_medium_bps": config.strategy_brti_min_move_medium_bps,
        "strategy_brti_min_move_long_bps": config.strategy_brti_min_move_long_bps,
        "strategy_brti_directional_tick_ratio_min": (
            config.strategy_brti_directional_tick_ratio_min
        ),
        "strategy_brti_max_boundary_crosses_90s": (
            config.strategy_brti_max_boundary_crosses_90s
        ),
        "strategy_brti_max_retrace_fraction": config.strategy_brti_max_retrace_fraction,
        "strategy_contract_lookback_seconds": config.strategy_contract_lookback_seconds,
        "strategy_contract_min_mid_move_cents": (
            config.strategy_contract_min_mid_move_cents
        ),
        "strategy_contract_ask_pullback_lookback_seconds": (
            config.strategy_contract_ask_pullback_lookback_seconds
        ),
        "strategy_contract_max_ask_pullback_cents": (
            config.strategy_contract_max_ask_pullback_cents
        ),
        "strategy_trade_confirmation_lookback_seconds": (
            config.strategy_trade_confirmation_lookback_seconds
        ),
        "strategy_trade_confirmation_min_ratio": (
            config.strategy_trade_confirmation_min_ratio
        ),
        "strategy_trade_confirmation_min_trades": (
            config.strategy_trade_confirmation_min_trades
        ),
        "strategy_min_top_book_size_contracts": config.strategy_min_top_book_size_contracts,
        "strategy_dry_run_max_entry_price": config.strategy_dry_run_max_entry_price,
        "strategy_dry_run_min_entry_price": config.strategy_dry_run_min_entry_price,
        "strategy_min_boundary_distance_bps": config.strategy_min_boundary_distance_bps,
        "strategy_reference_max_age_ms": config.strategy_reference_max_age_ms,
        "strategy_reference_source_max_age_ms": (
            config.strategy_reference_source_max_age_ms
        ),
        "strategy_reference_source_warn_ms": config.strategy_reference_source_warn_ms,
        "strategy_reference_require_trade_ready_fresh": (
            config.strategy_reference_require_trade_ready_fresh
        ),
        "strategy_reference_stream_max_age_ms": (
            config.strategy_reference_stream_max_age_ms
        ),
        "strategy_reference_carry_forward_max_age_ms": (
            config.strategy_reference_carry_forward_max_age_ms
        ),
        "strategy_reference_allow_duplicate_source_ts_carry_forward": (
            config.strategy_reference_allow_duplicate_source_ts_carry_forward
        ),
        "strategy_kalshi_book_max_age_ms": config.strategy_kalshi_book_max_age_ms,
        "strategy_kalshi_book_stream_max_age_ms": (
            config.strategy_kalshi_book_stream_max_age_ms
        ),
        "strategy_kalshi_book_carry_forward_max_age_ms": (
            config.strategy_kalshi_book_carry_forward_max_age_ms
        ),
        "strategy_kalshi_book_require_stream_live": (
            config.strategy_kalshi_book_require_stream_live
        ),
        "strategy_no_entry_first_seconds": config.strategy_no_entry_first_seconds,
        "strategy_no_entry_last_seconds": config.strategy_no_entry_last_seconds,
        "strategy_min_entry_ask": config.strategy_min_entry_ask,
        "strategy_max_entry_ask": config.strategy_max_entry_ask,
        "strategy_max_spread_cents": config.strategy_max_spread_cents,
    }


def _measurement_summary(measurements: Any) -> JsonPayload | None:
    if not isinstance(measurements, dict):
        return None
    keys = (
        "boundary",
        "brti_value",
        "brti_age_ms",
        "brti_strategy_fresh_age_ms",
        "brti_reference_source_warn_ms",
        "brti_reference_source_hard_limit_ms",
        "brti_reference_status_category",
        "brti_reference_transport_stale",
        "brti_reference_persistence_stale",
        "brti_reference_worker_heartbeat_stale",
        "brti_reference_trade_ready_fresh",
        "brti_reference_stream_age_ms",
        "brti_reference_carry_forward_allowed",
        "brti_reference_carry_forward_age_ms",
        "brti_reference_liveness_reason",
        "market_liveness_source",
        "reference_liveness_source",
        "market_component_heartbeat_at",
        "reference_component_heartbeat_at",
        "market_component_heartbeat_age_ms",
        "reference_component_heartbeat_age_ms",
        "latest_aggregate_heartbeat_mode",
        "latest_component_heartbeat_mode",
        "liveness_source_mismatch",
        "distance_bps",
        "candidate_side",
        "seconds_left",
        "desired_side_bid",
        "desired_side_ask",
        "desired_side_mid",
        "desired_side_spread_cents",
        "desired_top_book_size",
        "brti_move_short_bps",
        "brti_move_medium_bps",
        "brti_move_long_bps",
        "brti_directional_tick_ratio",
        "boundary_cross_count",
        "retrace_fraction",
        "contract_mid_move_cents",
        "ask_pullback_cents",
        "recent_trade_count",
        "candidate_trade_ratio",
        "dry_run_risk_state",
        "dry_run_intended_entry_price",
        "dry_run_position_id",
        "orderbook_age_ms",
        "orderbook_stream_age_ms",
        "market_feed_transport_state",
        "market_feed_subscription_state",
        "market_feed_snapshot_state",
        "market_feed_active_ticker_state",
        "market_feed_sequence_state",
        "market_data_quiet",
        "market_data_quiet_age_ms",
        "orderbook_snapshot_age_ms",
        "orderbook_recovery_action",
        "market_feed_state",
        "market_subscription_recovery_count",
        "market_subscription_recovery_last_reason",
        "market_subscription_recovery_last_action",
        "market_subscription_recovery_last_result",
        "market_subscription_recovery_last_at",
        "market_snapshot_resync_count",
        "market_snapshot_resync_last_result",
        "market_rollover_recovery_count",
        "market_transport_reconnect_count",
        "market_unrecovered_blocker_count",
        "market_recovery_attempt_in_progress",
        "market_recovery_attempt_age_ms",
        "orderbook_carry_forward_allowed",
        "orderbook_carry_forward_age_ms",
        "orderbook_liveness_reason",
        "orderbook_snapshot_source",
        "latest_trade_age_ms",
        "brti_reference_stale_reason",
        "brti_reference_connection_state",
        "brti_reference_recovery_state",
        "gate_results_summary",
    )
    return {key: measurements.get(key) for key in keys if key in measurements}


def _summary_text(summary: JsonPayload | None, key: str) -> str | None:
    if not isinstance(summary, dict):
        return None
    value = summary.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _summary_int(summary: JsonPayload | None, key: str) -> int | None:
    if not isinstance(summary, dict):
        return None
    value = summary.get(key)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _summary_bool(summary: JsonPayload | None, key: str) -> bool | None:
    if not isinstance(summary, dict):
        return None
    value = summary.get(key)
    return value if isinstance(value, bool) else None


def _gate_results_summary(gate_results: Any) -> JsonPayload | None:
    if not isinstance(gate_results, dict):
        return None
    summary: dict[str, Any] = {}
    for gate, value in gate_results.items():
        if not isinstance(value, dict):
            continue
        summary[str(gate)] = {
            "status": value.get("status"),
            "reason": value.get("reason"),
        }
    return summary


def _gate_results(
    *,
    config: AppConfig,
    decision_state: str,
    primary_reason: str,
    decision_warnings: list[str],
    market: Market | None,
    boundary: Decimal | None,
    boundary_source: str | None,
    brti_value: Decimal | None,
    brti_backend_age_ms: int | None,
    brti_source_age_ms: int | None,
    brti_strategy_fresh_age_ms: int | None,
    brti_reference_stale_reason: str | None,
    brti_reference_stream_age_ms: int | None,
    brti_reference_carry_forward_allowed: bool,
    brti_reference_carry_forward_age_ms: int | None,
    brti_reference_valid_message_carried_forward: bool,
    brti_reference_transport_stale: bool,
    brti_reference_persistence_stale: bool,
    brti_reference_worker_heartbeat_stale: bool,
    orderbook_age_ms: int | None,
    orderbook_stream_age_ms: int | None,
    market_feed_transport_state: str,
    market_feed_subscription_state: str,
    market_feed_snapshot_state: str,
    market_feed_active_ticker_state: str,
    market_feed_sequence_state: str,
    market_data_quiet: bool,
    market_data_quiet_age_ms: int | None,
    orderbook_snapshot_age_ms: int | None,
    orderbook_recovery_action: str | None,
    orderbook_carry_forward_allowed: bool,
    orderbook_carry_forward_age_ms: int | None,
    orderbook_liveness_reason: str | None,
    orderbook_snapshot_source: str | None,
    seconds_left: int | None,
    distance_bps: Decimal | None,
    desired_bid: Decimal | None,
    desired_ask: Decimal | None,
    desired_mid: Decimal | None,
    desired_spread: Decimal | None,
    desired_spread_cents: Decimal | None,
    desired_top_book_size: Decimal | None,
    dry_run_intended_entry_price: Decimal | None,
    brti_short_move_bps: Decimal | None,
    brti_medium_move_bps: Decimal | None,
    brti_long_move_bps: Decimal | None,
    boundary_cross_count: int | None,
    retrace_fraction: Decimal | None,
    contract_mid_move_cents: Decimal | None,
    ask_pullback_cents: Decimal | None,
    recent_trade_count: int,
    candidate_trade_ratio: Decimal | None,
    dry_run_risk_state: str | None,
) -> dict[str, Any]:
    reference_status = "pass" if brti_value is not None else "not_evaluated"
    reference_reason = None
    if decision_state == STATE_REFERENCE_STALE or primary_reason.startswith(
        "dry_run_position_reference"
    ):
        reference_status = "block"
        reference_reason = brti_reference_stale_reason or primary_reason
    elif "brti_reference_source_age_warning" in decision_warnings:
        reference_status = "warn"
        reference_reason = "brti_reference_source_age_warning"
    elif brti_reference_carry_forward_allowed:
        reference_status = "warn"
        reference_reason = "brti_reference_carried_forward"

    timing_status = "not_evaluated" if seconds_left is None else "pass"
    timing_reason = None
    if primary_reason in {"entry_window_too_early", "entry_window_too_late"}:
        timing_status = "block"
        timing_reason = primary_reason

    boundary_distance_status = "not_evaluated" if distance_bps is None else "pass"
    boundary_distance_reason = None
    if primary_reason == "boundary_distance_below_threshold":
        boundary_distance_status = "block"
        boundary_distance_reason = primary_reason

    book_status = "not_evaluated" if desired_bid is None and desired_ask is None else "pass"
    book_reason = None
    book_block_prefixes = (
        "dry_run_position_orderbook",
        "dry_run_position_book",
        "dry_run_position_spread",
        "dry_run_position_depth",
    )
    if decision_state in {
        STATE_KALSHI_STALE,
        STATE_BOOK_UNUSABLE,
        STATE_SPREAD_TOO_WIDE,
        STATE_DEPTH_TOO_THIN,
    } or primary_reason.startswith(book_block_prefixes):
        book_status = "block"
        book_reason = primary_reason
    elif orderbook_carry_forward_allowed or (
        market_data_quiet
        and orderbook_liveness_reason == "kalshi_orderbook_data_quiet_carried_forward"
    ):
        book_status = "warn"
        book_reason = "kalshi_orderbook_data_quiet_carried_forward"

    entry_price_status = (
        "not_evaluated" if dry_run_intended_entry_price is None else "pass"
    )
    entry_price_reason = None
    if primary_reason in {
        "dry_run_intended_entry_price_too_low",
        "dry_run_intended_entry_price_too_high",
        "dry_run_intended_entry_price_outside_range",
    }:
        entry_price_status = "block"
        entry_price_reason = primary_reason

    impulse_status = "not_evaluated" if brti_short_move_bps is None else "pass"
    impulse_reason = None
    if decision_state == STATE_IMPULSE_TOO_WEAK:
        impulse_status = "block"
        impulse_reason = primary_reason

    chop_status = "not_evaluated" if boundary_cross_count is None else "pass"
    chop_reason = None
    if decision_state == STATE_CHOP_FILTER_BLOCKED:
        chop_status = "block"
        chop_reason = primary_reason

    contract_status = "not_evaluated" if contract_mid_move_cents is None else "pass"
    contract_reason = None
    contract_reasons = {
        "insufficient_contract_history",
        "contract_mid_move_below_threshold",
        "ask_pullback_above_threshold",
    }
    if decision_state == STATE_CONTRACT_NOT_CONFIRMED and primary_reason in contract_reasons:
        contract_status = "block"
        contract_reason = primary_reason

    trade_status = "not_evaluated" if recent_trade_count == 0 else "pass"
    trade_reason = None
    if "recent_trade_confirmation_insufficient_trades" in decision_warnings:
        trade_status = "warn"
        trade_reason = "recent_trade_confirmation_insufficient_trades"
    if primary_reason == "recent_trade_confirmation_weak":
        trade_status = "block"
        trade_reason = primary_reason

    dry_run_risk_status = "not_evaluated" if dry_run_risk_state is None else "pass"
    dry_run_risk_reason = None
    if dry_run_risk_state == "dry_run_disabled":
        dry_run_risk_status = (
            "block" if config.app_mode is AppMode.DRY_RUN else "not_evaluated"
        )
        dry_run_risk_reason = (
            primary_reason if dry_run_risk_status == "block" else None
        )
    elif decision_state == STATE_RISK_BLOCKED:
        dry_run_risk_status = "block"
        dry_run_risk_reason = primary_reason

    safety_status = "pass"
    safety_reason = None
    if decision_state == STATE_LIVE_GUARD_BLOCKED:
        safety_status = "block"
        safety_reason = primary_reason

    return {
        "safety": {"status": safety_status, "reason": safety_reason},
        "market": {
            "status": (
                "block"
                if decision_state == STATE_NO_ACTIVE_MARKET
                else "not_evaluated" if market is None else "pass"
            ),
            "reason": primary_reason if decision_state == STATE_NO_ACTIVE_MARKET else None,
            "ticker": getattr(market, "market_ticker", None),
        },
        "boundary": {
            "status": (
                "block"
                if decision_state == STATE_MARKET_NOT_PARSEABLE
                else "not_evaluated" if boundary is None else "pass"
            ),
            "reason": (
                primary_reason if decision_state == STATE_MARKET_NOT_PARSEABLE else None
            ),
            "boundary": _decimal_text(boundary),
            "source": boundary_source,
        },
        "reference": {
            "status": reference_status,
            "reason": reference_reason,
            "stream_age_ms": brti_reference_stream_age_ms,
            "backend_age_ms": brti_backend_age_ms,
            "source_age_ms": brti_source_age_ms,
            "strategy_fresh_age_ms": brti_strategy_fresh_age_ms,
            "carried_forward": brti_reference_valid_message_carried_forward,
            "carry_forward_allowed": brti_reference_carry_forward_allowed,
            "carry_forward_age_ms": brti_reference_carry_forward_age_ms,
            "transport_stale": brti_reference_transport_stale,
            "persistence_stale": brti_reference_persistence_stale,
            "worker_heartbeat_stale": brti_reference_worker_heartbeat_stale,
        },
        "timing": {
            "status": timing_status,
            "reason": timing_reason,
            "seconds_left": seconds_left,
        },
        "boundary_distance": {
            "status": boundary_distance_status,
            "reason": boundary_distance_reason,
            "distance_bps": _decimal_text(distance_bps),
        },
        "book": {
            "status": book_status,
            "reason": book_reason,
            "snapshot_source": orderbook_snapshot_source,
            "stream_age_ms": orderbook_stream_age_ms,
            "book_age_ms": orderbook_age_ms,
            "transport_state": market_feed_transport_state,
            "subscription_state": market_feed_subscription_state,
            "snapshot_state": market_feed_snapshot_state,
            "active_ticker_state": market_feed_active_ticker_state,
            "sequence_state": market_feed_sequence_state,
            "market_data_quiet": market_data_quiet,
            "market_data_quiet_age_ms": market_data_quiet_age_ms,
            "snapshot_age_ms": orderbook_snapshot_age_ms,
            "recovery_action": orderbook_recovery_action,
            "carry_forward_allowed": orderbook_carry_forward_allowed,
            "carry_forward_age_ms": orderbook_carry_forward_age_ms,
            "liveness_reason": orderbook_liveness_reason,
            "desired_bid": _decimal_text(desired_bid),
            "desired_ask": _decimal_text(desired_ask),
            "desired_mid": _decimal_text(desired_mid),
            "spread_cents": _decimal_text(desired_spread_cents),
            "top_book_size": _decimal_text(desired_top_book_size),
        },
        "entry_price": {
            "status": entry_price_status,
            "reason": entry_price_reason,
            "desired_ask": _decimal_text(desired_ask),
            "intended_entry_price": _decimal_text(dry_run_intended_entry_price),
            "min_entry_price": _decimal_text(config.strategy_dry_run_min_entry_price),
            "max_entry_price": _decimal_text(config.strategy_dry_run_max_entry_price),
        },
        "impulse": {
            "status": impulse_status,
            "reason": impulse_reason,
            "short_move_bps": _decimal_text(brti_short_move_bps),
            "medium_move_bps": _decimal_text(brti_medium_move_bps),
            "long_move_bps": _decimal_text(brti_long_move_bps),
        },
        "chop": {
            "status": chop_status,
            "reason": chop_reason,
            "boundary_cross_count": boundary_cross_count,
            "retrace_fraction": _decimal_text(retrace_fraction),
        },
        "contract_confirmation": {
            "status": contract_status,
            "reason": contract_reason,
            "mid_move_cents": _decimal_text(contract_mid_move_cents),
            "ask_pullback_cents": _decimal_text(ask_pullback_cents),
        },
        "trade_confirmation": {
            "status": trade_status,
            "reason": trade_reason,
            "trade_count": recent_trade_count,
            "candidate_trade_ratio": _decimal_text(candidate_trade_ratio),
        },
        "dry_run_risk": {
            "status": dry_run_risk_status,
            "reason": dry_run_risk_reason,
            "state": dry_run_risk_state,
        },
    }


def _market_boundary(market: Market) -> tuple[Decimal | None, str | None]:
    if market.functional_strike is not None:
        return Decimal(market.functional_strike), "functional_strike"
    if market.floor_strike is not None:
        return Decimal(market.floor_strike), "floor_strike"
    return None, None


def _valid_reference_tick(tick: ReferenceTick | None) -> bool:
    return (
        tick is not None
        and tick.parse_status == "valid"
        and tick.parsed_value is not None
        and tick.received_at is not None
    )


def _reference_source_age_ms(tick: ReferenceTick, evaluated_at: datetime) -> int | None:
    ages: list[int] = []
    if tick.source_age_ms is not None:
        ages.append(max(0, tick.source_age_ms))
    if tick.source_ts is not None:
        computed_age = _age_ms(tick.source_ts, evaluated_at)
        if computed_age is not None:
            ages.append(computed_age)
    return max(ages) if ages else None


def _strategy_reference_stale_reason(
    *,
    config: AppConfig,
    reference_tick: ReferenceTick | None,
    brti_backend_age_ms: int | None,
    brti_source_age_ms: int | None,
    brti_reference_stream_age_ms: int | None,
    reference_worker_metadata: dict[str, Any] | None,
    worker_heartbeat_stale: bool,
    transport_stale: bool,
    persistence_stale: bool,
) -> str | None:
    metadata_reason = _metadata_stale_reason(reference_worker_metadata)
    if not _valid_reference_tick(reference_tick):
        return metadata_reason or "brti_reference_missing"
    if worker_heartbeat_stale:
        return "brti_reference_worker_heartbeat_stale"
    if transport_stale:
        return "brti_reference_transport_stale"
    if persistence_stale:
        return "brti_reference_persistence_stale"
    if (
        brti_source_age_ms is not None
        and brti_source_age_ms > config.strategy_reference_source_max_age_ms
    ):
        return "brti_reference_source_age_exceeds_hard_limit"
    if brti_backend_age_ms is None:
        return "brti_reference_backend_age_exceeds_limit"
    if brti_backend_age_ms <= config.strategy_reference_max_age_ms:
        return metadata_reason
    if not config.strategy_reference_allow_duplicate_source_ts_carry_forward:
        return "brti_reference_backend_age_exceeds_limit"
    if brti_reference_stream_age_ms is None:
        return "brti_reference_backend_age_exceeds_limit"
    if brti_reference_stream_age_ms > config.strategy_reference_stream_max_age_ms:
        return "brti_reference_stream_stale"
    if brti_backend_age_ms > config.strategy_reference_carry_forward_max_age_ms:
        return "brti_reference_carry_forward_age_exceeds_limit"
    if not config.strategy_reference_require_trade_ready_fresh:
        return None
    return metadata_reason


def _strategy_orderbook_stale_reason(
    *,
    config: AppConfig,
    orderbook: OrderbookSnapshot | None,
    orderbook_age_ms: int | None,
    orderbook_worker_metadata: dict[str, Any] | None,
    orderbook_stream_age_ms: int | None,
    orderbook_stream_connection_state: str | None,
    orderbook_stream_active_market_ticker: str | None,
    orderbook_stream_warnings: list[str],
    orderbook_stream_blockers: list[str],
    market_feed_transport_state: str,
    market_feed_subscription_state: str,
    market_feed_snapshot_state: str,
    market_feed_active_ticker_state: str,
    market_feed_sequence_state: str,
    market_ticker: str,
) -> str | None:
    if orderbook is None:
        return "kalshi_orderbook_missing"
    if orderbook_age_ms is None:
        return "kalshi_orderbook_age_exceeds_limit"

    if not isinstance(orderbook_worker_metadata, dict):
        return (
            None
            if orderbook_age_ms <= config.strategy_kalshi_book_max_age_ms
            else "kalshi_orderbook_age_exceeds_limit"
        )

    stream_live_reason = _orderbook_stream_unusable_reason(
        metadata=orderbook_worker_metadata,
        stream_age_ms=orderbook_stream_age_ms,
        connection_state=orderbook_stream_connection_state,
        active_market_ticker=orderbook_stream_active_market_ticker,
        warnings=orderbook_stream_warnings,
        blockers=orderbook_stream_blockers,
        market_ticker=market_ticker,
    )
    enforce_stream_live = (
        config.strategy_kalshi_book_require_stream_live
        or orderbook_age_ms > config.strategy_kalshi_book_max_age_ms
    )
    if enforce_stream_live:
        if stream_live_reason is not None:
            return stream_live_reason
        protocol_reason = _market_protocol_unusable_reason(
            config=config,
            metadata=orderbook_worker_metadata,
        )
        if protocol_reason is not None:
            return protocol_reason
        recovery_reason = _market_recovery_stale_reason(orderbook_worker_metadata)
        if recovery_reason is not None:
            return recovery_reason
        if market_feed_transport_state == "stale":
            return "kalshi_orderbook_transport_stale"
        if market_feed_subscription_state != "subscribed":
            return "kalshi_orderbook_subscription_inactive"
        if market_feed_active_ticker_state == "mismatch":
            return "kalshi_orderbook_active_ticker_mismatch"
        if market_feed_active_ticker_state == "missing":
            return "kalshi_orderbook_active_ticker_missing"
        if market_feed_snapshot_state == "missing":
            return "kalshi_orderbook_uninitialized"
        if market_feed_snapshot_state == "resync_pending":
            return "kalshi_orderbook_snapshot_resync_pending"
        if market_feed_snapshot_state == "stale_cap_exceeded":
            return "kalshi_orderbook_carry_forward_age_exceeds_limit"
        if market_feed_sequence_state in {"gap", "reset"}:
            return "kalshi_orderbook_sequence_gap_or_reset"

    if orderbook_age_ms <= config.strategy_kalshi_book_max_age_ms:
        return None
    if market_feed_transport_state == "stale":
        return "kalshi_orderbook_transport_stale"
    if (
        market_feed_transport_state == "unknown"
        and not _legacy_orderbook_stream_live(
            stream_age_ms=orderbook_stream_age_ms,
            stream_max_age_ms=config.strategy_kalshi_book_stream_max_age_ms,
            connection_state=orderbook_stream_connection_state,
        )
    ):
        return "kalshi_orderbook_transport_stale"
    if orderbook_age_ms > config.strategy_kalshi_book_carry_forward_max_age_ms:
        return "kalshi_orderbook_carry_forward_age_exceeds_limit"
    return None


def _orderbook_stream_unusable_reason(
    *,
    metadata: dict[str, Any] | None,
    stream_age_ms: int | None,
    connection_state: str | None,
    active_market_ticker: str | None,
    warnings: list[str],
    blockers: list[str],
    market_ticker: str,
) -> str | None:
    if not isinstance(metadata, dict):
        return "kalshi_orderbook_age_exceeds_limit"
    recovery_reason = _market_recovery_stale_reason(metadata)
    if recovery_reason is not None:
        return recovery_reason
    if connection_state != "subscribed":
        return "kalshi_orderbook_subscription_inactive"
    if _metadata_bool_present(metadata, "subscription_reconciled") is False:
        return "kalshi_orderbook_subscription_unreconciled"
    if _metadata_bool_present(metadata, "orderbook_sid_confirmed") is False:
        return "kalshi_orderbook_orderbook_sid_unconfirmed"
    if active_market_ticker is not None and active_market_ticker != market_ticker:
        return "kalshi_orderbook_active_ticker_mismatch"
    if metadata.get("orderbook_initialized") is not True:
        return "kalshi_orderbook_uninitialized"
    sequence_reset_reasons = {
        "orderbook_sequence_gap_reset",
        "orderbook_reset_after_buffer_overflow",
        "kalshi_websocket_buffer_overflow",
        "kalshi_orderbook_sequence_gap_or_reset",
    }
    reported_reasons = warnings + blockers
    liveness_reason = _metadata_text(metadata, "orderbook_liveness_reason")
    if liveness_reason is not None:
        reported_reasons.append(liveness_reason)
    if any(reason in sequence_reset_reasons for reason in reported_reasons):
        return "kalshi_orderbook_sequence_gap_or_reset"
    snapshot_resync_failure_reasons = {
        "kalshi_orderbook_snapshot_resync_failed",
        "orderbook_snapshot_resync_unavailable",
    }
    if any(reason in snapshot_resync_failure_reasons for reason in reported_reasons):
        return "kalshi_orderbook_snapshot_resync_failed"
    if any(
        reason.startswith(("invalid_orderbook_delta_", "invalid_orderbook_snapshot_"))
        for reason in reported_reasons
    ):
        return "kalshi_orderbook_invalid_update"
    if _has_market_critical_persistence_blocker(blockers):
        return "kalshi_orderbook_db_writer_backpressure"
    if blockers:
        return "kalshi_orderbook_transport_stale"
    del stream_age_ms
    return None


def _market_protocol_unusable_reason(
    *,
    config: AppConfig,
    metadata: dict[str, Any] | None,
) -> str | None:
    if not isinstance(metadata, dict):
        return None
    if _metadata_bool_present(metadata, "subscription_reconciled") is False:
        return "kalshi_orderbook_subscription_unreconciled"
    if _metadata_bool_present(metadata, "orderbook_sid_confirmed") is False:
        return "kalshi_orderbook_orderbook_sid_unconfirmed"
    if _metadata_bool(metadata, "in_flight_snapshot_request"):
        snapshot_age_ms = _metadata_int(metadata, "snapshot_request_age_ms")
        timeout_ms = int(config.kalshi_ws_snapshot_timeout_seconds * 1000)
        if snapshot_age_ms is None or snapshot_age_ms > timeout_ms:
            return "kalshi_orderbook_snapshot_resync_timeout"
    blockers = _metadata_string_list(metadata, "blockers")
    if _has_market_critical_persistence_blocker(blockers):
        return "kalshi_orderbook_db_writer_backpressure"
    critical_depth = _metadata_int(metadata, "db_writer_critical_queue_depth")
    if critical_depth is None:
        critical_depth = _metadata_int(metadata, "db_writer_queue_depth") or 0
    critical_age_ms = _metadata_int(metadata, "db_writer_critical_queue_oldest_age_ms")
    if critical_age_ms is None:
        critical_age_ms = _metadata_int(metadata, "db_writer_queue_oldest_age_ms") or 0
    if critical_depth >= config.market_db_writer_backpressure_block_depth:
        return "kalshi_orderbook_db_writer_backpressure"
    if critical_age_ms >= config.market_db_writer_backpressure_max_age_ms:
        return "kalshi_orderbook_db_writer_backpressure"
    if (_metadata_int(metadata, "protocol_event_recent_error_count") or 0) > 0:
        return "kalshi_orderbook_protocol_errors"
    return None


def _has_market_critical_persistence_blocker(blockers: list[str]) -> bool:
    return any(
        blocker in blockers
        for blocker in {
            "market_critical_persistence_failed",
            "market_critical_persistence_backpressure",
        }
    )


def _market_recovery_stale_reason(metadata: dict[str, Any] | None) -> str | None:
    if not isinstance(metadata, dict):
        return None
    feed_state = _metadata_text(metadata, "market_feed_state")
    last_reason = _metadata_text(metadata, "market_subscription_recovery_last_reason")
    last_action = _metadata_text(metadata, "market_subscription_recovery_last_action")
    last_result = _metadata_text(metadata, "market_subscription_recovery_last_result")
    in_progress = _metadata_bool(metadata, "market_recovery_attempt_in_progress")

    if feed_state == "ROLLING_MARKET" or last_reason in {
        "market_roll_reresolve",
        "market_roll_resubscribe_pending",
        "market_roll_snapshot_pending",
    }:
        return "kalshi_orderbook_market_roll_snapshot_pending"

    if in_progress:
        if last_action in {"subscribe", "resubscribe", "wait_for_subscription_ack"}:
            return "kalshi_orderbook_subscription_recovery_pending"
        if last_action == "get_snapshot":
            snapshot_state = _metadata_text(metadata, "market_feed_snapshot_state")
            snapshot_source = _metadata_text(metadata, "orderbook_snapshot_source")
            liveness_status = _metadata_text(metadata, "orderbook_liveness_status")
            if (
                snapshot_state == "initialized"
                and snapshot_source != "blocked"
                and liveness_status != "resync_pending"
            ):
                return None
            return "kalshi_orderbook_snapshot_resync_pending"
        if last_action == "reconnect":
            return "kalshi_orderbook_transport_stale"

    if last_result == "failed":
        if last_reason in {
            "kalshi_orderbook_subscription_ack_timeout",
            "kalshi_orderbook_subscription_recovery_failed",
            "orderbook_sid_missing",
        }:
            return "kalshi_orderbook_subscription_recovery_failed"
        if last_reason == "kalshi_orderbook_snapshot_resync_failed":
            return "kalshi_orderbook_snapshot_resync_failed"
        if last_reason == "kalshi_orderbook_transport_stale":
            return "kalshi_orderbook_transport_stale"
    return None


def _legacy_orderbook_stream_live(
    *,
    stream_age_ms: int | None,
    stream_max_age_ms: int,
    connection_state: str | None,
) -> bool:
    return (
        connection_state == "subscribed"
        and stream_age_ms is not None
        and stream_age_ms <= stream_max_age_ms
    )


def _metadata_stale_reason(metadata: dict[str, Any] | None) -> str | None:
    warnings = _metadata_string_list(metadata, "warnings")
    for warning in (
        "brti_reference_first_tick_timeout",
        "brti_reference_no_valid_tick_timeout",
        "brti_reference_reconnect_requested",
        "brti_reference_transport_stale",
        "brti_reference_persistence_stale",
        "brti_reference_worker_heartbeat_stale",
        "brti_reference_duplicate_conflict",
        "brti_persistence_failed",
    ):
        if warning in warnings:
            return warning
    return None


def _metadata_text(metadata: dict[str, Any] | None, key: str) -> str | None:
    if not isinstance(metadata, dict):
        return None
    value = metadata.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _metadata_datetime(metadata: dict[str, Any] | None, key: str) -> datetime | None:
    text = _metadata_text(metadata, key)
    if text is None:
        return None
    try:
        return _as_utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None


def _metadata_bool(metadata: dict[str, Any] | None, key: str) -> bool:
    if not isinstance(metadata, dict):
        return False
    value = metadata.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def _metadata_bool_present(metadata: dict[str, Any] | None, key: str) -> bool | None:
    if not isinstance(metadata, dict) or key not in metadata:
        return None
    return _metadata_bool(metadata, key)


def _metadata_has_warning(
    metadata: dict[str, Any] | None,
    *warnings: str,
) -> bool:
    metadata_warnings = set(_metadata_string_list(metadata, "warnings"))
    metadata_blockers = set(_metadata_string_list(metadata, "blockers"))
    return any(
        warning in metadata_warnings or warning in metadata_blockers
        for warning in warnings
    )


def _metadata_status_category_is(
    metadata: dict[str, Any] | None,
    status_category: str,
) -> bool:
    return _metadata_text(metadata, "status_category") == status_category


def _metadata_int(metadata: dict[str, Any] | None, key: str) -> int | None:
    if not isinstance(metadata, dict):
        return None
    try:
        return int(metadata.get(key))
    except (TypeError, ValueError):
        return None


def _metadata_string_list(metadata: dict[str, Any] | None, key: str) -> list[str]:
    if not isinstance(metadata, dict):
        return []
    value = metadata.get(key)
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item is not None]


def _desired_book(
    orderbook: OrderbookSnapshot,
    candidate_side: str | None,
) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    if candidate_side == "YES":
        bid = _decimal_or_none(orderbook.yes_bid)
        ask = _decimal_or_none(orderbook.yes_ask)
        spread = _decimal_or_none(orderbook.yes_spread)
    elif candidate_side == "NO":
        bid = _decimal_or_none(orderbook.no_bid)
        ask = _decimal_or_none(orderbook.no_ask)
        spread = _decimal_or_none(orderbook.no_spread)
    else:
        return None, None, None

    if spread is None and bid is not None and ask is not None:
        spread = ask - bid

    return bid, ask, spread


def _desired_top_book_size(
    orderbook: OrderbookSnapshot,
    candidate_side: str | None,
) -> Decimal | None:
    if candidate_side == "YES":
        return _decimal_or_none(orderbook.yes_ask_count) or _decimal_or_none(
            orderbook.yes_ask_size
        )
    if candidate_side == "NO":
        return _decimal_or_none(orderbook.no_ask_count) or _decimal_or_none(
            orderbook.no_ask_size
        )
    return None


def _desired_exit_book_size(
    orderbook: OrderbookSnapshot,
    candidate_side: str | None,
) -> Decimal | None:
    if candidate_side == "YES":
        return _decimal_or_none(orderbook.yes_bid_count) or _decimal_or_none(
            orderbook.yes_bid_size
        )
    if candidate_side == "NO":
        return _decimal_or_none(orderbook.no_bid_count) or _decimal_or_none(
            orderbook.no_bid_size
        )
    return None


def _midpoint(
    bid: Decimal | None,
    ask: Decimal | None,
) -> Decimal | None:
    if bid is None or ask is None:
        return None
    return (bid + ask) / Decimal("2")


def _brti_impulse_metrics(
    *,
    config: AppConfig,
    ticks: list[ReferenceTick],
    evaluated_at: datetime,
    current_value: Decimal,
    candidate_side: str | None,
) -> dict[str, Any]:
    valid_ticks = [
        tick
        for tick in ticks
        if _valid_reference_tick(tick) and tick.parsed_value is not None
    ]
    metrics: dict[str, Any] = {
        "short_price": None,
        "medium_price": None,
        "long_price": None,
        "short_move_bps": None,
        "medium_move_bps": None,
        "long_move_bps": None,
        "directional_tick_ratio": None,
        "short_point_count": 0,
        "medium_point_count": 0,
        "long_point_count": 0,
        "reason": None,
    }
    if candidate_side not in {"YES", "NO"}:
        metrics["reason"] = "no_directional_candidate"
        return metrics

    lookbacks = (
        ("short", config.strategy_brti_lookback_short_seconds),
        ("medium", config.strategy_brti_lookback_medium_seconds),
        ("long", config.strategy_brti_lookback_long_seconds),
    )
    for label, seconds in lookbacks:
        window = _ticks_in_window(valid_ticks, evaluated_at, seconds)
        metrics[f"{label}_point_count"] = len(window)
        if len(window) < 3:
            metrics["reason"] = "insufficient_reference_history"
            return metrics
        start_value = _decimal_or_none(window[0].parsed_value)
        metrics[f"{label}_price"] = start_value
        metrics[f"{label}_move_bps"] = _signed_move_bps(
            start_value,
            current_value,
        )

    thresholds = {
        "short": config.strategy_brti_min_move_short_bps,
        "medium": config.strategy_brti_min_move_medium_bps,
        "long": config.strategy_brti_min_move_long_bps,
    }
    for label, threshold in thresholds.items():
        move = metrics[f"{label}_move_bps"]
        if not _move_passes(candidate_side, move, Decimal(str(threshold))):
            metrics["reason"] = f"weak_{label}_brti_move"
            return metrics

    ratio = _directional_tick_ratio(valid_ticks[-31:], candidate_side)
    metrics["directional_tick_ratio"] = ratio
    if ratio is None:
        metrics["reason"] = "insufficient_directional_ticks"
    elif ratio < Decimal(str(config.strategy_brti_directional_tick_ratio_min)):
        metrics["reason"] = "directional_tick_ratio_below_threshold"
    return metrics


def _brti_chop_metrics(
    *,
    config: AppConfig,
    ticks: list[ReferenceTick],
    evaluated_at: datetime,
    boundary: Decimal,
    current_value: Decimal,
    candidate_side: str | None,
    short_move_bps: Decimal | None,
    medium_move_bps: Decimal | None,
) -> dict[str, Any]:
    valid_ticks = [
        tick
        for tick in _ticks_in_window(
            ticks,
            evaluated_at,
            config.strategy_brti_lookback_medium_seconds,
        )
        if _valid_reference_tick(tick) and tick.parsed_value is not None
    ]
    metrics: dict[str, Any] = {
        "boundary_cross_count": _boundary_cross_count(valid_ticks, boundary),
        "retrace_fraction": None,
        "reason": None,
    }
    if metrics["boundary_cross_count"] > config.strategy_brti_max_boundary_crosses_90s:
        metrics["reason"] = "boundary_cross_count_above_threshold"
        return metrics

    if _moves_oppose(short_move_bps, medium_move_bps):
        metrics["reason"] = "short_move_opposes_medium_move"
        return metrics

    retrace = _retrace_fraction(valid_ticks, current_value, candidate_side)
    metrics["retrace_fraction"] = retrace
    if retrace is not None and retrace > Decimal(str(config.strategy_brti_max_retrace_fraction)):
        metrics["reason"] = "retrace_fraction_above_threshold"
    return metrics


def _contract_confirmation_metrics(
    *,
    config: AppConfig,
    evaluated_at: datetime,
    orderbook_history: list[OrderbookSnapshot],
    candidate_side: str | None,
    desired_mid: Decimal | None,
    desired_ask: Decimal | None,
) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "mid_move_cents": None,
        "ask_pullback_cents": None,
        "reason": None,
    }
    if desired_mid is None or desired_ask is None:
        metrics["reason"] = "desired_side_book_unusable"
        return metrics

    mids = [
        _midpoint(*_desired_book(snapshot, candidate_side)[:2])
        for snapshot in orderbook_history
    ]
    valid_mids = [mid for mid in mids if mid is not None]
    if len(valid_mids) < 2:
        metrics["reason"] = "insufficient_contract_history"
        return metrics

    mid_move_cents = (desired_mid - valid_mids[0]) * Decimal("100")
    metrics["mid_move_cents"] = mid_move_cents
    if mid_move_cents < Decimal(str(config.strategy_contract_min_mid_move_cents)):
        metrics["reason"] = "contract_mid_move_below_threshold"
        return metrics

    ask_cutoff = evaluated_at - timedelta(
        seconds=config.strategy_contract_ask_pullback_lookback_seconds
    )
    asks = [
        _desired_book(snapshot, candidate_side)[1]
        for snapshot in orderbook_history
        if snapshot.received_at is not None and _as_utc(snapshot.received_at) >= ask_cutoff
    ]
    valid_asks = [ask for ask in asks if ask is not None]
    if valid_asks:
        ask_pullback_cents = (max(valid_asks) - desired_ask) * Decimal("100")
        metrics["ask_pullback_cents"] = ask_pullback_cents
        if ask_pullback_cents > Decimal(
            str(config.strategy_contract_max_ask_pullback_cents)
        ):
            metrics["reason"] = "ask_pullback_above_threshold"
    return metrics


def _trade_confirmation_metrics(
    *,
    trades: list[PublicTrade],
    candidate_side: str | None,
) -> dict[str, Any]:
    relevant = [
        trade
        for trade in trades
        if _trade_side(trade) in {"YES", "NO"}
    ]
    if not relevant:
        return {
            "trade_count": 0,
            "candidate_trade_ratio": None,
        }
    candidate_count = sum(1 for trade in relevant if _trade_side(trade) == candidate_side)
    return {
        "trade_count": len(relevant),
        "candidate_trade_ratio": Decimal(candidate_count) / Decimal(len(relevant)),
    }


def _oldest_dry_run_position(
    positions: list[StrategyDryRunPosition],
) -> StrategyDryRunPosition:
    return min(
        positions,
        key=lambda position: (
            _as_utc(position.opened_at),
            int(position.id or 0),
        ),
    )


def _select_active_dry_run_position_needing_management(
    *,
    config: AppConfig,
    positions: list[StrategyDryRunPosition],
    active_market_ticker: str,
    orderbook: OrderbookSnapshot,
    evaluated_at: datetime,
    seconds_left: int | None,
    boundary: Decimal,
    brti_value: Decimal,
) -> StrategyDryRunPosition | None:
    active_positions = [
        position
        for position in positions
        if position.market_ticker == active_market_ticker
    ]
    for position in sorted(
        active_positions,
        key=lambda row: (_as_utc(row.opened_at), int(row.id or 0)),
    ):
        desired_bid, desired_ask, desired_spread = _desired_book(
            orderbook,
            position.side_candidate,
        )
        if (
            desired_bid is None
            or desired_ask is None
            or desired_spread is None
            or desired_ask < desired_bid
            or desired_spread < 0
        ):
            return position

        desired_spread_cents = desired_spread * Decimal("100")
        if desired_spread_cents > Decimal(str(config.strategy_max_spread_cents)):
            return position

        desired_exit_size = _desired_exit_book_size(orderbook, position.side_candidate)
        if desired_exit_size is None or desired_exit_size < Decimal(
            str(config.strategy_min_top_book_size_contracts)
        ):
            return position

        management_state, _ = _dry_run_management_decision(
            config=config,
            position=position,
            evaluated_at=evaluated_at,
            seconds_left=seconds_left,
            candidate_side=position.side_candidate,
            boundary=boundary,
            brti_value=brti_value,
            desired_bid=desired_bid,
        )
        if management_state in {STATE_EXIT_SIGNAL, STATE_FORCE_EXIT}:
            return position

    return None


def _dry_run_management_decision(
    *,
    config: AppConfig,
    position: StrategyDryRunPosition,
    evaluated_at: datetime,
    seconds_left: int | None,
    candidate_side: str | None,
    boundary: Decimal,
    brti_value: Decimal,
    desired_bid: Decimal,
) -> tuple[str, str]:
    entry_price = _decimal_or_none(position.open_price)
    if entry_price is None:
        return STATE_FORCE_EXIT, "dry_run_position_entry_price_missing"
    if seconds_left is not None and seconds_left <= 20:
        return STATE_FORCE_EXIT, "dry_run_position_seconds_left_force_exit"
    if seconds_left is None:
        return STATE_FORCE_EXIT, "dry_run_position_seconds_left_missing"
    if seconds_left > 60 and desired_bid >= entry_price + Decimal("0.10"):
        return STATE_EXIT_SIGNAL, "dry_run_profit_target_reached"
    if desired_bid >= Decimal("0.88"):
        return STATE_EXIT_SIGNAL, "dry_run_high_bid_exit_signal"
    if desired_bid <= entry_price - Decimal("0.12"):
        return STATE_EXIT_SIGNAL, "dry_run_stop_loss_reached"
    adverse_distance_bps = (abs(brti_value - boundary) / brti_value) * Decimal("10000")
    if (
        candidate_side == "YES"
        and brti_value < boundary
        and adverse_distance_bps >= Decimal("1.5")
    ):
        return STATE_EXIT_SIGNAL, "dry_run_brti_crossed_boundary_against_yes"
    if (
        candidate_side == "NO"
        and brti_value > boundary
        and adverse_distance_bps >= Decimal("1.5")
    ):
        return STATE_EXIT_SIGNAL, "dry_run_brti_crossed_boundary_against_no"
    return STATE_MANAGE_POSITION, "dry_run_position_open"


def _ticks_in_window(
    ticks: list[ReferenceTick],
    evaluated_at: datetime,
    seconds: int,
) -> list[ReferenceTick]:
    cutoff = _as_utc(evaluated_at) - timedelta(seconds=seconds)
    return [
        tick
        for tick in ticks
        if tick.received_at is not None and _as_utc(tick.received_at) >= cutoff
    ]


def _signed_move_bps(
    start_value: Decimal | None,
    end_value: Decimal | None,
) -> Decimal | None:
    if start_value is None or end_value is None or start_value <= 0:
        return None
    return ((end_value - start_value) / start_value) * Decimal("10000")


def _move_passes(
    candidate_side: str | None,
    move_bps: Decimal | None,
    threshold_bps: Decimal,
) -> bool:
    if move_bps is None:
        return False
    if candidate_side == "YES":
        return move_bps >= threshold_bps
    if candidate_side == "NO":
        return move_bps <= -threshold_bps
    return False


def _directional_tick_ratio(
    ticks: list[ReferenceTick],
    candidate_side: str | None,
) -> Decimal | None:
    directional = 0
    candidate_direction = 0
    last_value: Decimal | None = None
    for tick in ticks:
        value = _decimal_or_none(tick.parsed_value)
        if value is None:
            continue
        if last_value is not None and value != last_value:
            directional += 1
            if (candidate_side == "YES" and value > last_value) or (
                candidate_side == "NO" and value < last_value
            ):
                candidate_direction += 1
        last_value = value
    if directional == 0:
        return None
    return Decimal(candidate_direction) / Decimal(directional)


def _boundary_cross_count(ticks: list[ReferenceTick], boundary: Decimal) -> int:
    crosses = 0
    previous_sign: int | None = None
    for tick in ticks:
        value = _decimal_or_none(tick.parsed_value)
        if value is None or value == boundary:
            continue
        sign = 1 if value > boundary else -1
        if previous_sign is not None and sign != previous_sign:
            crosses += 1
        previous_sign = sign
    return crosses


def _moves_oppose(
    short_move_bps: Decimal | None,
    medium_move_bps: Decimal | None,
) -> bool:
    if short_move_bps is None or medium_move_bps is None:
        return False
    return (short_move_bps > 0 > medium_move_bps) or (
        short_move_bps < 0 < medium_move_bps
    )


def _retrace_fraction(
    ticks: list[ReferenceTick],
    current_value: Decimal,
    candidate_side: str | None,
) -> Decimal | None:
    values = [
        value
        for value in (_decimal_or_none(tick.parsed_value) for tick in ticks)
        if value is not None
    ]
    if len(values) < 3:
        return None
    start = values[0]
    if candidate_side == "YES":
        peak = max(values)
        impulse = peak - start
        if impulse <= 0:
            return Decimal("0")
        return max(Decimal("0"), peak - current_value) / impulse
    if candidate_side == "NO":
        trough = min(values)
        impulse = start - trough
        if impulse <= 0:
            return Decimal("0")
        return max(Decimal("0"), current_value - trough) / impulse
    return None


def _trade_side(trade: PublicTrade) -> str | None:
    for value in (trade.side_inferred, trade.taker_side):
        if value is None:
            continue
        normalized = str(value).strip().upper()
        if normalized in {"YES", "NO"}:
            return normalized
    return None


def _intended_entry_price(
    desired_ask: Decimal,
    offset_cents: int,
    *,
    max_entry_price: Decimal,
) -> Decimal:
    return min(
        desired_ask + (Decimal(offset_cents) / Decimal("100")),
        max_entry_price,
    )


def _dry_run_position_id(
    *,
    config: AppConfig,
    market_ticker: str,
    decision_id: str,
) -> str:
    if config.strategy_dry_run_one_entry_per_market:
        raw = f"dryrun-{config.strategy_id}-{market_ticker}"
    else:
        raw = f"dryrun-{config.strategy_id}-{market_ticker}-{decision_id}"
    return _safe_decision_id_part(raw)[:128]


def _dry_run_event_id(
    *,
    event_type: str,
    decision_id: str,
    position_id: str | None,
) -> str:
    raw = f"dryrun-event-{event_type}-{decision_id}-{position_id or 'none'}"
    return _safe_decision_id_part(raw)[:128]


def _dry_run_runtime_enabled(config: AppConfig, safety: SafetyAssessment) -> bool:
    return (
        config.app_mode is AppMode.DRY_RUN
        and config.strategy_dry_run_enabled
        and config.strategy_observer_enabled
        and safety.is_safe
        and not config.trading_enabled
        and not config.execute
    )


def _apply_dry_run_ledger(
    *,
    config: AppConfig,
    session: Session,
    decision: StrategyDecisionInput,
) -> DryRunLedgerResult:
    repository = StrategyDryRunRepository(session)
    position_id = _measurement_text(decision.measurements, "dry_run_position_id")
    latest_position_id: str | None = position_id

    if decision.decision_state == STATE_ENTER_DRY_RUN and position_id is not None:
        entry_price = _decimal_or_none(
            _measurement_text(decision.measurements, "dry_run_intended_entry_price")
        )
        if entry_price is not None and decision.market_ticker is not None:
            repository.insert_position_if_absent(
                StrategyDryRunPositionInput(
                    position_id=position_id,
                    strategy_id=config.strategy_id,
                    market_ticker=decision.market_ticker,
                    decision_id=decision.decision_id,
                    side_candidate=decision.candidate_side or "UNKNOWN",
                    economic_side=decision.candidate_side or "UNKNOWN",
                    opened_at=decision.evaluated_at,
                    open_price=entry_price,
                    contract_count=config.strategy_dry_run_position_size_contracts,
                    boundary=decision.boundary,
                    brti_at_entry=decision.brti_value,
                    distance_bps_at_entry=decision.distance_bps,
                    entry_reason=decision.primary_reason,
                    status=OPEN_POSITION_STATUS,
                    measurements=decision.measurements,
                )
            )
            repository.insert_event_if_absent(
                StrategyDryRunEventInput(
                    event_id=_dry_run_event_id(
                        event_type=STATE_ENTER_DRY_RUN,
                        decision_id=decision.decision_id,
                        position_id=position_id,
                    ),
                    strategy_id=config.strategy_id,
                    position_id=position_id,
                    decision_id=decision.decision_id,
                    event_type=STATE_ENTER_DRY_RUN,
                    market_ticker=decision.market_ticker,
                    occurred_at=decision.evaluated_at,
                    side_candidate=decision.candidate_side,
                    price=entry_price,
                    contract_count=config.strategy_dry_run_position_size_contracts,
                    reason=decision.primary_reason,
                    measurements=decision.measurements,
                )
            )

    if decision.decision_state in {STATE_MANAGE_POSITION, STATE_EXIT_SIGNAL, STATE_FORCE_EXIT}:
        if position_id is not None:
            position = repository.get_position_by_id(position_id)
            close_price = _decimal_or_none(
                _measurement_text(decision.measurements, "desired_side_bid")
            )
            if decision.decision_state in {STATE_EXIT_SIGNAL, STATE_FORCE_EXIT}:
                close_status = (
                    "FORCE_CLOSED"
                    if decision.decision_state == STATE_FORCE_EXIT
                    else "CLOSED"
                )
                realized_pnl_cents = _realized_pnl_cents(position, close_price)
                repository.close_position(
                    position_id=position_id,
                    closed_at=decision.evaluated_at,
                    close_price=close_price,
                    close_reason=decision.primary_reason,
                    status=close_status,
                    realized_pnl_cents=realized_pnl_cents,
                    measurements=decision.measurements,
                )
            repository.insert_event_if_absent(
                StrategyDryRunEventInput(
                    event_id=_dry_run_event_id(
                        event_type=decision.decision_state,
                        decision_id=decision.decision_id,
                        position_id=position_id,
                    ),
                    strategy_id=config.strategy_id,
                    position_id=position_id,
                    decision_id=decision.decision_id,
                    event_type=decision.decision_state,
                    market_ticker=decision.market_ticker,
                    occurred_at=decision.evaluated_at,
                    side_candidate=decision.candidate_side,
                    price=close_price,
                    contract_count=(
                        None if position is None else int(position.contract_count)
                    ),
                    reason=decision.primary_reason,
                    measurements=decision.measurements,
                )
            )

    latest_event = repository.get_latest_event(strategy_id=config.strategy_id)
    return DryRunLedgerResult(
        open_position_count=repository.count_open_positions(strategy_id=config.strategy_id),
        latest_event_type=latest_event.event_type if latest_event else None,
        latest_position_id=latest_position_id,
    )


def _measurement_text(measurements: JsonPayload | None, key: str) -> str | None:
    if not isinstance(measurements, dict):
        return None
    value = measurements.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _realized_pnl_cents(
    position: StrategyDryRunPosition | None,
    close_price: Decimal | None,
) -> Decimal | None:
    if position is None or close_price is None:
        return None
    open_price = _decimal_or_none(position.open_price)
    if open_price is None:
        return None
    return (close_price - open_price) * Decimal("100") * Decimal(position.contract_count)


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        return None


def _age_ms(value: datetime | None, now: datetime) -> int | None:
    if value is None:
        return None
    return max(0, int((_as_utc(now) - _as_utc(value)).total_seconds() * 1000))


def _latest_datetime(*values: datetime | None) -> datetime | None:
    present = [_as_utc(value) for value in values if value is not None]
    return max(present) if present else None


def _seconds_between(start: datetime | None, end: datetime | None) -> int | None:
    if start is None or end is None:
        return None
    return max(0, int((_as_utc(end) - _as_utc(start)).total_seconds()))


def _decision_id(
    *,
    evaluated_at: datetime,
    poll_seconds: float,
    strategy_id: str,
    market_ticker: str | None,
    context_hash: str,
) -> str:
    bucket_size = max(poll_seconds, 0.001)
    bucket = int(evaluated_at.timestamp() / bucket_size)
    safe_strategy_id = _safe_decision_id_part(strategy_id)
    safe_ticker = _safe_decision_id_part(market_ticker or "none")
    return f"strategy-{safe_strategy_id}-{safe_ticker}-{bucket}-{context_hash[:12]}"[:128]


def _safe_decision_id_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "-", value).strip("-") or "none"


def _stable_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(_json_safe(payload), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, datetime):
        return _isoformat_or_none(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _decimal_text(value: Any) -> str | None:
    decimal_value = _decimal_or_none(value)
    if decimal_value is None:
        return None
    return format(decimal_value.normalize(), "f")


def _strategy_worker_metadata(metadata: Any) -> dict[str, Any] | None:
    if not isinstance(metadata, dict):
        return None
    strategy_metadata = metadata.get("strategy")
    if not isinstance(strategy_metadata, dict):
        return None
    observer_metadata = strategy_metadata.get("observer")
    return observer_metadata if isinstance(observer_metadata, dict) else None


def _strategy_dry_run_worker_metadata(metadata: Any) -> dict[str, Any] | None:
    if not isinstance(metadata, dict):
        return None
    strategy_metadata = metadata.get("strategy")
    if not isinstance(strategy_metadata, dict):
        return None
    dry_run_metadata = strategy_metadata.get("dry_run")
    return dry_run_metadata if isinstance(dry_run_metadata, dict) else None


def _strategy_variants_worker_metadata(metadata: Any) -> dict[str, Any]:
    if not isinstance(metadata, dict):
        return {}
    strategy_metadata = metadata.get("strategy")
    if not isinstance(strategy_metadata, dict):
        return {}
    variants = strategy_metadata.get("variants")
    if not isinstance(variants, dict):
        return {}
    return {
        str(strategy_id): value
        for strategy_id, value in variants.items()
        if isinstance(value, dict)
    }


def _full_gate_trace(
    *,
    config: AppConfig,
    decision_state: str,
    primary_reason: str,
    evaluated_at: datetime,
    market: Market | None,
    boundary: Decimal | None,
    brti_value: Decimal | None,
    orderbook: OrderbookSnapshot | None,
    seconds_since_open: int | None,
    seconds_left: int | None,
    reference_ticks: list[ReferenceTick],
    orderbook_history: list[OrderbookSnapshot],
    recent_trades: list[PublicTrade],
) -> JsonPayload:
    """Record every pure entry gate without changing the canonical decision."""

    prerequisite_ready = all(
        value is not None
        for value in (market, boundary, brti_value, orderbook)
    ) and bool(brti_value and brti_value > 0)
    base: dict[str, Any] = {
        "strategy_id": config.strategy_id,
        "evaluated_at": _isoformat_or_none(evaluated_at),
        "thresholds": _thresholds(config),
        "canonical_state": decision_state,
        "canonical_reason": primary_reason,
        "prerequisites_ready": prerequisite_ready,
        "gates": {},
        "passing_gate_count": 0,
        "blocking_gate_count": 0,
        "canonical_primary_gate": None,
    }
    if not prerequisite_ready:
        return base

    assert boundary is not None
    assert brti_value is not None
    assert orderbook is not None
    candidate_side = "YES" if brti_value > boundary else "NO"
    distance_bps = abs(brti_value - boundary) / brti_value * Decimal("10000")
    desired_bid, desired_ask, desired_spread = _desired_book(orderbook, candidate_side)
    desired_mid = _midpoint(desired_bid, desired_ask)
    desired_spread_cents = (
        desired_spread * Decimal("100") if desired_spread is not None else None
    )
    desired_top_book_size = _desired_top_book_size(orderbook, candidate_side)
    gates: dict[str, dict[str, Any]] = {}

    def add_gate(
        name: str,
        *,
        status: str,
        reason: str | None,
        actual: dict[str, Any],
        thresholds: dict[str, Any],
        margins: dict[str, Any],
        canonical_reasons: set[str],
    ) -> None:
        canonical = primary_reason in canonical_reasons
        gates[name] = {
            "status": status,
            "reason": reason,
            "actual": _json_safe(actual),
            "thresholds": _json_safe(thresholds),
            "signed_margins": _json_safe(margins),
            "affects_canonical_decision": canonical,
            "analysis_only": not canonical,
        }

    first_seconds = config.strategy_no_entry_first_seconds
    last_seconds = config.strategy_no_entry_last_seconds
    timing_reason = (
        "entry_window_too_early"
        if seconds_since_open is not None and seconds_since_open < first_seconds
        else "entry_window_too_late"
        if seconds_left is not None and seconds_left < last_seconds
        else None
    )
    add_gate(
        "timing",
        status="block" if timing_reason else "pass",
        reason=timing_reason,
        actual={"seconds_since_open": seconds_since_open, "seconds_left": seconds_left},
        thresholds={"no_entry_first_seconds": first_seconds, "no_entry_last_seconds": last_seconds},
        margins={
            "after_open_seconds": (
                None if seconds_since_open is None else seconds_since_open - first_seconds
            ),
            "before_close_seconds": (
                None if seconds_left is None else seconds_left - last_seconds
            ),
        },
        canonical_reasons={"entry_window_too_early", "entry_window_too_late"},
    )
    boundary_threshold = Decimal(str(config.strategy_min_boundary_distance_bps))
    boundary_reason = (
        "boundary_distance_below_threshold"
        if distance_bps < boundary_threshold
        else None
    )
    add_gate(
        "boundary_distance",
        status="block" if boundary_reason else "pass",
        reason=boundary_reason,
        actual={"distance_bps": distance_bps, "candidate_side": candidate_side},
        thresholds={"min_boundary_distance_bps": boundary_threshold},
        margins={"distance_bps": distance_bps - boundary_threshold},
        canonical_reasons={"boundary_distance_below_threshold"},
    )
    book_reason = (
        "kalshi_orderbook_side_missing"
        if desired_bid is None
        or desired_ask is None
        or desired_spread is None
        or desired_ask < desired_bid
        or desired_spread < 0
        else None
    )
    add_gate(
        "book",
        status="block" if book_reason else "pass",
        reason=book_reason,
        actual={
            "desired_bid": desired_bid,
            "desired_ask": desired_ask,
            "desired_spread_cents": desired_spread_cents,
        },
        thresholds={},
        margins={},
        canonical_reasons={"kalshi_orderbook_side_missing"},
    )
    max_spread = Decimal(str(config.strategy_max_spread_cents))
    spread_reason = (
        "desired_side_spread_too_wide"
        if desired_spread_cents is None or desired_spread_cents > max_spread
        else None
    )
    add_gate(
        "spread",
        status="block" if spread_reason else "pass",
        reason=spread_reason,
        actual={"spread_cents": desired_spread_cents},
        thresholds={"max_spread_cents": max_spread},
        margins={
            "spread_cents": (
                None if desired_spread_cents is None else max_spread - desired_spread_cents
            )
        },
        canonical_reasons={"desired_side_spread_too_wide"},
    )
    min_depth = Decimal(str(config.strategy_min_top_book_size_contracts))
    depth_reason = (
        "desired_side_depth_too_thin"
        if desired_top_book_size is None or desired_top_book_size < min_depth
        else None
    )
    add_gate(
        "depth",
        status="block" if depth_reason else "pass",
        reason=depth_reason,
        actual={"top_book_size": desired_top_book_size},
        thresholds={"min_top_book_size_contracts": min_depth},
        margins={
            "top_book_size": (
                None if desired_top_book_size is None else desired_top_book_size - min_depth
            )
        },
        canonical_reasons={"desired_side_depth_too_thin"},
    )
    min_entry = Decimal(str(config.strategy_dry_run_min_entry_price))
    max_entry = Decimal(str(config.strategy_dry_run_max_entry_price))
    raw_ask_reason = (
        "dry_run_intended_entry_price_too_low"
        if desired_ask is None or desired_ask < min_entry
        else "dry_run_intended_entry_price_too_high"
        if desired_ask > max_entry
        else None
    )
    add_gate(
        "raw_entry_ask",
        status="block" if raw_ask_reason else "pass",
        reason=raw_ask_reason,
        actual={"desired_ask": desired_ask},
        thresholds={"min_entry_price": min_entry, "max_entry_price": max_entry},
        margins={
            "above_min": None if desired_ask is None else desired_ask - min_entry,
            "below_max": None if desired_ask is None else max_entry - desired_ask,
        },
        canonical_reasons={
            "dry_run_intended_entry_price_too_low",
            "dry_run_intended_entry_price_too_high",
        },
    )
    intended_entry = (
        None
        if desired_ask is None
        else _intended_entry_price(
            desired_ask,
            config.strategy_dry_run_entry_price_offset_cents,
            max_entry_price=max_entry,
        )
    )
    add_gate(
        "intended_entry_price",
        status="pass" if intended_entry is not None else "not_evaluated",
        reason=None,
        actual={"intended_entry_price": intended_entry},
        thresholds={"max_entry_price": max_entry},
        margins={
            "below_max": None if intended_entry is None else max_entry - intended_entry
        },
        canonical_reasons=set(),
    )
    impulse = _brti_impulse_metrics(
        config=config,
        ticks=reference_ticks,
        evaluated_at=evaluated_at,
        current_value=brti_value,
        candidate_side=candidate_side,
    )
    add_gate(
        "impulse",
        status="block" if impulse["reason"] else "pass",
        reason=impulse["reason"],
        actual={
            "short_move_bps": impulse["short_move_bps"],
            "medium_move_bps": impulse["medium_move_bps"],
            "long_move_bps": impulse["long_move_bps"],
            "directional_tick_ratio": impulse["directional_tick_ratio"],
            "point_counts": {
                "short": impulse["short_point_count"],
                "medium": impulse["medium_point_count"],
                "long": impulse["long_point_count"],
            },
        },
        thresholds={
            "short_move_bps": config.strategy_brti_min_move_short_bps,
            "medium_move_bps": config.strategy_brti_min_move_medium_bps,
            "long_move_bps": config.strategy_brti_min_move_long_bps,
            "directional_tick_ratio": config.strategy_brti_directional_tick_ratio_min,
        },
        margins={
            "short_move_bps": _margin(
                impulse["short_move_bps"], config.strategy_brti_min_move_short_bps
            ),
            "medium_move_bps": _margin(
                impulse["medium_move_bps"], config.strategy_brti_min_move_medium_bps
            ),
            "long_move_bps": _margin(
                impulse["long_move_bps"], config.strategy_brti_min_move_long_bps
            ),
            "directional_tick_ratio": _margin(
                impulse["directional_tick_ratio"],
                config.strategy_brti_directional_tick_ratio_min,
            ),
        },
        canonical_reasons={
            "insufficient_reference_history",
            "weak_short_brti_move",
            "weak_medium_brti_move",
            "weak_long_brti_move",
            "insufficient_directional_ticks",
            "directional_tick_ratio_below_threshold",
        },
    )
    chop = _brti_chop_metrics(
        config=config,
        ticks=reference_ticks,
        evaluated_at=evaluated_at,
        boundary=boundary,
        current_value=brti_value,
        candidate_side=candidate_side,
        short_move_bps=impulse["short_move_bps"],
        medium_move_bps=impulse["medium_move_bps"],
    )
    add_gate(
        "chop",
        status="block" if chop["reason"] else "pass",
        reason=chop["reason"],
        actual={
            "boundary_cross_count": chop["boundary_cross_count"],
            "retrace_fraction": chop["retrace_fraction"],
        },
        thresholds={
            "max_boundary_crosses": config.strategy_brti_max_boundary_crosses_90s,
            "max_retrace_fraction": config.strategy_brti_max_retrace_fraction,
        },
        margins={
            "boundary_crosses": (
                None
                if chop["boundary_cross_count"] is None
                else config.strategy_brti_max_boundary_crosses_90s
                - chop["boundary_cross_count"]
            ),
            "retrace_fraction": _margin(
                config.strategy_brti_max_retrace_fraction,
                chop["retrace_fraction"],
            ),
        },
        canonical_reasons={
            "boundary_cross_count_above_threshold",
            "short_move_opposes_medium_move",
            "retrace_fraction_above_threshold",
        },
    )
    contract = _contract_confirmation_metrics(
        config=config,
        evaluated_at=evaluated_at,
        orderbook_history=orderbook_history,
        candidate_side=candidate_side,
        desired_mid=desired_mid,
        desired_ask=desired_ask,
    )
    add_gate(
        "contract_confirmation",
        status="block" if contract["reason"] else "pass",
        reason=contract["reason"],
        actual={
            "mid_move_cents": contract["mid_move_cents"],
            "ask_pullback_cents": contract["ask_pullback_cents"],
        },
        thresholds={
            "min_mid_move_cents": config.strategy_contract_min_mid_move_cents,
            "max_ask_pullback_cents": config.strategy_contract_max_ask_pullback_cents,
        },
        margins={
            "mid_move_cents": _margin(
                contract["mid_move_cents"], config.strategy_contract_min_mid_move_cents
            ),
            "ask_pullback_cents": _margin(
                config.strategy_contract_max_ask_pullback_cents,
                contract["ask_pullback_cents"],
            ),
        },
        canonical_reasons={
            "insufficient_contract_history",
            "contract_mid_move_below_threshold",
            "ask_pullback_above_threshold",
        },
    )
    trade = _trade_confirmation_metrics(
        trades=recent_trades,
        candidate_side=candidate_side,
    )
    trade_count = int(trade["trade_count"])
    trade_ratio = trade["candidate_trade_ratio"]
    trade_status = "pass"
    trade_reason = None
    if trade_count < config.strategy_trade_confirmation_min_trades:
        trade_status = "warn"
        trade_reason = "recent_trade_confirmation_insufficient_trades"
    elif (
        trade_ratio is not None
        and trade_ratio < Decimal(str(config.strategy_trade_confirmation_min_ratio))
    ):
        trade_status = "block"
        trade_reason = "recent_trade_confirmation_weak"
    add_gate(
        "trade_confirmation",
        status=trade_status,
        reason=trade_reason,
        actual={"trade_count": trade_count, "candidate_trade_ratio": trade_ratio},
        thresholds={
            "min_trades": config.strategy_trade_confirmation_min_trades,
            "min_candidate_ratio": config.strategy_trade_confirmation_min_ratio,
        },
        margins={
            "trade_count": trade_count - config.strategy_trade_confirmation_min_trades,
            "candidate_trade_ratio": _margin(
                trade_ratio,
                config.strategy_trade_confirmation_min_ratio,
            ),
        },
        canonical_reasons={"recent_trade_confirmation_weak"},
    )
    base["gates"] = gates
    base["passing_gate_count"] = sum(
        1 for gate in gates.values() if gate["status"] == "pass"
    )
    base["blocking_gate_count"] = sum(
        1 for gate in gates.values() if gate["status"] == "block"
    )
    base["canonical_primary_gate"] = next(
        (
            name
            for name, gate in gates.items()
            if gate["affects_canonical_decision"]
        ),
        None,
    )
    return base


def _margin(
    value: Decimal | float | int | None,
    threshold: Decimal | float | int | None,
) -> Decimal | None:
    if value is None or threshold is None:
        return None
    return Decimal(str(value)) - Decimal(str(threshold))


def _reference_worker_metadata(metadata: Any) -> dict[str, Any] | None:
    if not isinstance(metadata, dict):
        return None
    reference_metadata = metadata.get("reference")
    if not isinstance(reference_metadata, dict):
        return None
    brti_metadata = reference_metadata.get("brti")
    return brti_metadata if isinstance(brti_metadata, dict) else None


def _ws_worker_metadata(metadata: Any) -> dict[str, Any] | None:
    if not isinstance(metadata, dict):
        return None
    ws_metadata = metadata.get("ws")
    return ws_metadata if isinstance(ws_metadata, dict) else None


def _preserve_existing_worker_metadata(
    metadata: dict[str, Any],
    existing_metadata: Any,
    *,
    keys: tuple[str, ...],
) -> None:
    if not isinstance(existing_metadata, dict):
        return
    for key in keys:
        if key not in metadata and isinstance(existing_metadata.get(key), dict):
            metadata[key] = existing_metadata[key]


def _enabled_collector_metadata_keys(config: AppConfig) -> tuple[str, ...]:
    keys: list[str] = []
    if config.kalshi_ws_enabled:
        keys.append("ws")
    if (
        config.kalshi_cfbenchmarks_enabled
        and config.kalshi_cfbenchmarks_subscribe_on_worker
    ):
        keys.append("reference")
    if config.storage_retention_enabled:
        keys.append("storage")
    return tuple(keys)


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _increment_counter(counter: dict[str, int], value: str | None) -> None:
    if value is None:
        return
    counter[value] = counter.get(value, 0) + 1


def _unique_strings(values: list[str]) -> list[str]:
    unique: list[str] = []
    for value in values:
        if value not in unique:
            unique.append(value)
    return unique


async def _sleep_or_stop(stop_event: threading.Event, seconds: float) -> None:
    deadline = datetime.now(UTC).timestamp() + seconds
    while not stop_event.is_set() and datetime.now(UTC).timestamp() < deadline:
        await asyncio.sleep(min(0.1, seconds))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _isoformat_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _as_utc(value).isoformat().replace("+00:00", "Z")
