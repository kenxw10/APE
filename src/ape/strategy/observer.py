from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from ape.config import AppConfig
from ape.db.models import (
    Market,
    OrderbookSnapshot,
    PublicTrade,
    ReferenceTick,
    StrategyDecision,
)
from ape.db.session import create_engine_from_config, create_session_factory
from ape.kalshi.reference_messages import BRTI_SOURCE
from ape.repositories.inputs import JsonPayload, StrategyDecisionInput, WorkerHeartbeatInput
from ape.repositories.markets import MarketsRepository
from ape.repositories.orderbook import OrderbookRepository
from ape.repositories.public_trades import PublicTradesRepository
from ape.repositories.reference_ticks import ReferenceTicksRepository
from ape.repositories.strategy_decisions import StrategyDecisionsRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.safety import SafetyAssessment, assess_startup_safety

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
}


@dataclass
class StrategyObserverRuntimeStatus:
    enabled: bool
    connection_state: str = "disabled"
    last_evaluated_at: datetime | None = None
    last_decision_state: str | None = None
    last_primary_reason: str | None = None
    last_decision_id: str | None = None
    warnings: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)

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


@dataclass(frozen=True)
class StrategyDecisionSnapshot:
    found: bool
    decision_id: str | None = None
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
    decision_age_seconds: float | None
    stale: bool
    warnings: list[str]
    blockers: list[str]
    checked_at: datetime


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
                decision = evaluate_strategy_observer(
                    config=self.config,
                    safety=self.safety,
                    session=session,
                    now=self.now(),
                )
                repository = StrategyDecisionsRepository(session)
                if repository.get_decision_by_id(decision.decision_id) is None:
                    repository.insert_decision(decision)
                    session.commit()
                else:
                    session.rollback()
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

        self.status.connection_state = "running"
        self.status.last_evaluated_at = decision.evaluated_at
        self.status.last_decision_state = decision.decision_state
        self.status.last_primary_reason = decision.primary_reason
        self.status.last_decision_id = decision.decision_id
        self.status.blockers = list(decision.blockers or [])
        self.status.warnings = list(decision.warnings or [])
        self.record_heartbeat()
        return decision

    def record_heartbeat(self) -> None:
        if self.session_factory is None:
            return

        try:
            with self.session_factory() as session:
                WorkerHeartbeatRepository(session).record_heartbeat(
                    WorkerHeartbeatInput(
                        service_name="ape-worker",
                        started_at=self.started_at,
                        heartbeat_at=self.now(),
                        app_mode=self.config.app_mode.value,
                        is_safe=self.safety.is_safe,
                        metadata={
                            "mode": "strategy_observer",
                            "strategy": {"observer": self.status.as_metadata()},
                        },
                    )
                )
                session.commit()
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
    reference_tick: ReferenceTick | None = None
    orderbook: OrderbookSnapshot | None = None
    latest_trade: PublicTrade | None = None
    boundary: Decimal | None = None
    boundary_source: str | None = None
    brti_value: Decimal | None = None
    brti_backend_age_ms: int | None = None
    brti_source_age_ms: int | None = None
    brti_age_ms: int | None = None
    orderbook_age_ms: int | None = None
    latest_trade_age_ms: int | None = None
    seconds_since_open: int | None = None
    seconds_left: int | None = None
    candidate_side: str | None = None
    distance_bps: Decimal | None = None
    desired_bid: Decimal | None = None
    desired_ask: Decimal | None = None
    desired_spread: Decimal | None = None
    desired_spread_cents: Decimal | None = None

    def decision(
        state: str,
        reason: str,
        *,
        blockers: list[str] | None = None,
        warnings: list[str] | None = None,
    ) -> StrategyDecisionInput:
        measurements = _measurements(
            config=config,
            safety=safety,
            evaluated_at=evaluated_at,
            thresholds=thresholds,
            market=market,
            boundary=boundary,
            boundary_source=boundary_source,
            reference_tick=reference_tick,
            brti_value=brti_value,
            brti_backend_age_ms=brti_backend_age_ms,
            brti_source_age_ms=brti_source_age_ms,
            brti_age_ms=brti_age_ms,
            orderbook=orderbook,
            orderbook_age_ms=orderbook_age_ms,
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
        )
        context_hash = _stable_hash(
            {
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
                market_ticker=getattr(market, "market_ticker", None),
                context_hash=context_hash,
            ),
            evaluated_at=evaluated_at,
            decision_state=state,
            primary_reason=reason,
            app_mode=config.app_mode.value,
            market_ticker=getattr(market, "market_ticker", None),
            candidate_side=candidate_side,
            boundary=boundary,
            brti_value=brti_value,
            distance_bps=distance_bps,
            seconds_left=seconds_left,
            measurements=measurements,
            blockers=blockers or ([] if state == STATE_OBSERVE_ONLY_MARKET else [reason]),
            warnings=warnings or [],
            raw_context_hash=context_hash,
        )

    if not safety.is_safe:
        return decision(
            STATE_LIVE_GUARD_BLOCKED,
            "startup_safety_not_observer_safe",
            blockers=safety.blockers,
            warnings=safety.warnings,
        )

    market = MarketsRepository(session).get_active_market(
        now=evaluated_at,
        series_ticker=config.kalshi_btc15_series_ticker,
    )
    if market is None:
        return decision(STATE_NO_ACTIVE_MARKET, "no_active_persisted_market")

    seconds_since_open = _seconds_between(market.open_time, evaluated_at)
    seconds_left = _seconds_between(evaluated_at, market.close_time)
    boundary, boundary_source = _market_boundary(market)
    if boundary is None:
        return decision(STATE_MARKET_NOT_PARSEABLE, "market_boundary_not_parseable")

    reference_tick = ReferenceTicksRepository(session).get_latest_tick(BRTI_SOURCE)
    if not _valid_reference_tick(reference_tick):
        return decision(STATE_REFERENCE_STALE, "brti_reference_missing_or_invalid")

    brti_value = reference_tick.parsed_value
    brti_backend_age_ms = _age_ms(reference_tick.received_at, evaluated_at)
    brti_source_age_ms = _reference_source_age_ms(reference_tick, evaluated_at)
    brti_age_ms = max(
        age
        for age in (brti_backend_age_ms, brti_source_age_ms)
        if age is not None
    )
    if brti_age_ms > config.strategy_reference_max_age_ms:
        return decision(STATE_REFERENCE_STALE, "brti_reference_age_exceeds_limit")

    orderbook = OrderbookRepository(session).get_latest_snapshot(market.market_ticker)
    if orderbook is None:
        return decision(STATE_KALSHI_STALE, "kalshi_orderbook_missing")

    orderbook_age_ms = _age_ms(orderbook.received_at, evaluated_at)
    if orderbook_age_ms is None or orderbook_age_ms > config.strategy_kalshi_book_max_age_ms:
        return decision(STATE_KALSHI_STALE, "kalshi_orderbook_age_exceeds_limit")

    latest_trade = PublicTradesRepository(session).get_latest_trade(market.market_ticker)
    if latest_trade is not None:
        latest_trade_age_ms = _age_ms(latest_trade.received_at, evaluated_at)

    if (
        seconds_since_open is not None
        and seconds_since_open < config.strategy_no_entry_first_seconds
    ):
        return decision(STATE_TOO_EARLY, "entry_window_too_early")

    if seconds_left is not None and seconds_left < config.strategy_no_entry_last_seconds:
        return decision(STATE_TOO_LATE_FOR_ENTRY, "entry_window_too_late")

    if brti_value is None or brti_value <= 0 or brti_value == boundary:
        return decision(STATE_NO_DIRECTIONAL_CANDIDATE, "no_directional_candidate")

    candidate_side = "YES" if brti_value > boundary else "NO"
    distance_bps = (abs(brti_value - boundary) / brti_value) * Decimal("10000")
    if distance_bps < Decimal(str(config.strategy_min_boundary_distance_bps)):
        return decision(STATE_TOO_CLOSE_TO_BOUNDARY, "boundary_distance_below_threshold")

    desired_bid, desired_ask, desired_spread = _desired_book(orderbook, candidate_side)
    desired_spread_cents = (
        None if desired_spread is None else desired_spread * Decimal("100")
    )
    if (
        desired_bid is None
        or desired_ask is None
        or desired_spread is None
        or desired_ask < desired_bid
        or desired_spread < 0
        or desired_spread_cents is None
        or desired_spread_cents > Decimal(str(config.strategy_max_spread_cents))
    ):
        return decision(STATE_BOOK_UNUSABLE, "desired_side_book_unusable")

    if (
        desired_ask < Decimal(str(config.strategy_min_entry_ask))
        or desired_ask > Decimal(str(config.strategy_max_entry_ask))
    ):
        return decision(STATE_CONTRACT_NOT_CONFIRMED, "desired_side_ask_outside_range")

    return decision(
        STATE_OBSERVE_ONLY_MARKET,
        "observer_decision_ledger_only",
    )


def build_strategy_status(
    config: AppConfig,
    *,
    now: datetime | None = None,
) -> StrategyStatusSnapshot:
    checked_at = _as_utc(now or datetime.now(UTC))
    safety = assess_startup_safety(config)
    latest_decision: StrategyDecision | None = None
    worker_metadata: dict[str, Any] | None = None
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
            warnings=warnings,
            blockers=blockers,
        )

    try:
        engine = create_engine_from_config(config)
        try:
            session_factory = create_session_factory(engine)
            with session_factory() as session:
                latest_decision = StrategyDecisionsRepository(session).get_latest_decision()
                heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")
                worker_metadata = _strategy_worker_metadata(
                    heartbeat.metadata_ if heartbeat else None
                )
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
        warnings=warnings,
        blockers=blockers,
    )


def build_latest_strategy_decision(config: AppConfig) -> StrategyDecisionSnapshot:
    if not config.database_url:
        return StrategyDecisionSnapshot(found=False)

    try:
        engine = create_engine_from_config(config)
        try:
            session_factory = create_session_factory(engine)
            with session_factory() as session:
                decision = StrategyDecisionsRepository(session).get_latest_decision()
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
    now: datetime | None = None,
) -> StrategyRecentDecisionsSnapshot:
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
                    limit=capped_limit
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


def strategy_decision_snapshot(
    decision: StrategyDecision | None,
) -> StrategyDecisionSnapshot:
    if decision is None:
        return StrategyDecisionSnapshot(found=False)

    return StrategyDecisionSnapshot(
        found=True,
        decision_id=decision.decision_id,
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


def _status_snapshot(
    *,
    config: AppConfig,
    safety: SafetyAssessment,
    checked_at: datetime,
    latest_decision: StrategyDecision | None,
    worker_metadata: dict[str, Any] | None,
    warnings: list[str],
    blockers: list[str],
) -> StrategyStatusSnapshot:
    worker_observed_enabled = (
        None if worker_metadata is None else bool(worker_metadata.get("enabled"))
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
            config.strategy_observer_enabled
            and decision_age_seconds > config.strategy_observer_decision_ttl_seconds
        )
    elif config.strategy_observer_enabled:
        stale = True
        blockers.append("strategy_decision_missing")

    if worker_metadata is not None:
        connection_state = str(worker_metadata.get("connection_state") or "unknown")
    elif not config.strategy_observer_enabled:
        connection_state = "disabled"
    elif blockers:
        connection_state = "blocked"
    else:
        connection_state = "unknown"

    return StrategyStatusSnapshot(
        enabled=config.strategy_observer_enabled,
        worker_observed_enabled=worker_observed_enabled,
        connection_state=connection_state,
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
        latest_measurements_summary=(
            _measurement_summary(latest_decision.measurements)
            if latest_decision is not None
            else None
        ),
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
    evaluated_at: datetime,
    thresholds: dict[str, Any],
    market: Market | None,
    boundary: Decimal | None,
    boundary_source: str | None,
    reference_tick: ReferenceTick | None,
    brti_value: Decimal | None,
    brti_backend_age_ms: int | None,
    brti_source_age_ms: int | None,
    brti_age_ms: int | None,
    orderbook: OrderbookSnapshot | None,
    orderbook_age_ms: int | None,
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
) -> dict[str, Any]:
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
        "desired_side_spread": _decimal_text(desired_spread),
        "desired_side_spread_cents": _decimal_text(desired_spread_cents),
        "orderbook_received_at": _isoformat_or_none(getattr(orderbook, "received_at", None)),
        "orderbook_age_ms": orderbook_age_ms,
        "orderbook_sequence_number": getattr(orderbook, "sequence_number", None),
        "latest_trade_received_at": _isoformat_or_none(
            getattr(latest_trade, "received_at", None)
        ),
        "latest_trade_age_ms": latest_trade_age_ms,
        "safety_mode": safety.mode,
        "trading_enabled": safety.trading_enabled,
        "execute": safety.execute,
        "observer_only": True,
        "config": thresholds,
        "series_ticker": config.kalshi_btc15_series_ticker,
    }


def _thresholds(config: AppConfig) -> dict[str, Any]:
    return {
        "strategy_observer_poll_seconds": config.strategy_observer_poll_seconds,
        "strategy_observer_decision_ttl_seconds": config.strategy_observer_decision_ttl_seconds,
        "strategy_min_boundary_distance_bps": config.strategy_min_boundary_distance_bps,
        "strategy_reference_max_age_ms": config.strategy_reference_max_age_ms,
        "strategy_kalshi_book_max_age_ms": config.strategy_kalshi_book_max_age_ms,
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
        "distance_bps",
        "candidate_side",
        "seconds_left",
        "desired_side_bid",
        "desired_side_ask",
        "desired_side_spread_cents",
        "orderbook_age_ms",
        "latest_trade_age_ms",
    )
    return {key: measurements.get(key) for key in keys if key in measurements}


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


def _seconds_between(start: datetime | None, end: datetime | None) -> int | None:
    if start is None or end is None:
        return None
    return max(0, int((_as_utc(end) - _as_utc(start)).total_seconds()))


def _decision_id(
    *,
    evaluated_at: datetime,
    poll_seconds: float,
    market_ticker: str | None,
    context_hash: str,
) -> str:
    bucket_size = max(poll_seconds, 0.001)
    bucket = int(evaluated_at.timestamp() / bucket_size)
    safe_ticker = _safe_decision_id_part(market_ticker or "none")
    return f"strategy-{safe_ticker}-{bucket}-{context_hash[:12]}"[:128]


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


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


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
