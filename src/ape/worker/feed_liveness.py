from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from ape.config import AppConfig
from ape.db.models import OrderbookSnapshot, PublicTrade, ReferenceTick, WorkerHeartbeat
from ape.kalshi.reference_messages import BRTI_SOURCE
from ape.repositories.orderbook import OrderbookRepository
from ape.repositories.public_trades import PublicTradesRepository
from ape.repositories.reference_ticks import ReferenceTicksRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.worker.services import (
    WORKER_SERVICE_AGGREGATE,
    WORKER_SERVICE_MARKET_WS,
    WORKER_SERVICE_MARKET_WS_LEGACY,
    WORKER_SERVICE_REFERENCE_BRTI,
)

FEED_LIVENESS_LEGACY_FALLBACK_WARNING = "feed_liveness_legacy_aggregate_fallback"
LIVENESS_SOURCE_COMPONENT = "component"
LIVENESS_SOURCE_LEGACY_FALLBACK = "legacy_aggregate_fallback"
LIVENESS_SOURCE_MISSING = "missing"


@dataclass(frozen=True)
class MarketFeedLiveness:
    source: str
    metadata: dict[str, Any] | None
    heartbeat_at: datetime | None
    heartbeat_age_ms: int | None
    started_at: datetime | None
    component_heartbeat_at: datetime | None
    component_heartbeat_age_ms: int | None
    latest_aggregate_heartbeat_mode: str | None
    latest_component_heartbeat_mode: str | None
    liveness_source_mismatch: bool
    warnings: list[str]
    worker_role: str | None
    connection_id: str | None
    protocol_connection_state: str | None
    active_market_ticker: str | None
    subscription_reconciled: bool
    orderbook_sid_confirmed: bool
    ticker_sid_confirmed: bool
    trade_sid_confirmed: bool
    last_list_subscriptions_at: datetime | None
    last_list_subscriptions_result: str | None
    in_flight_snapshot_request: bool
    snapshot_request_age_ms: int | None
    protocol_event_recent_error_count: int
    ws_reader_queue_depth: int
    ws_reader_queue_oldest_age_ms: int | None
    db_writer_queue_depth: int
    db_writer_queue_oldest_age_ms: int | None
    db_writer_last_flush_ms: int | None
    db_writer_slow_flush_count: int
    reconnect_reason: str | None
    close_code: int | None
    close_reason: str | None
    latest_orderbook: OrderbookSnapshot | None
    latest_trade: PublicTrade | None
    latest_orderbook_received_at: datetime | None
    latest_trade_received_at: datetime | None
    stream_last_message_at: datetime | None
    stream_age_ms: int | None
    transport_alive: bool
    transport_last_pong_at: datetime | None
    transport_age_ms: int | None
    transport_liveness_reason: str | None
    last_market_data_message_at: datetime | None
    market_data_message_age_ms: int | None
    market_feed_transport_state: str
    market_feed_subscription_state: str
    market_feed_snapshot_state: str
    market_feed_active_ticker_state: str
    market_feed_sequence_state: str
    market_data_quiet: bool
    market_data_quiet_age_ms: int | None
    orderbook_snapshot_age_ms: int | None
    orderbook_snapshot_source: str | None
    orderbook_recovery_action: str | None
    market_feed_state: str | None
    market_subscription_recovery_count: int
    market_subscription_recovery_last_reason: str | None
    market_subscription_recovery_last_action: str | None
    market_subscription_recovery_last_result: str | None
    market_subscription_recovery_last_at: datetime | None
    market_snapshot_resync_count: int
    market_snapshot_resync_last_result: str | None
    market_rollover_recovery_count: int
    market_transport_reconnect_count: int
    market_unrecovered_blocker_count: int
    market_recovery_attempt_in_progress: bool
    market_recovery_attempt_age_ms: int | None


@dataclass(frozen=True)
class ReferenceFeedLiveness:
    source: str
    metadata: dict[str, Any] | None
    heartbeat_at: datetime | None
    heartbeat_age_ms: int | None
    started_at: datetime | None
    component_heartbeat_at: datetime | None
    component_heartbeat_age_ms: int | None
    latest_aggregate_heartbeat_mode: str | None
    latest_component_heartbeat_mode: str | None
    liveness_source_mismatch: bool
    warnings: list[str]
    latest_tick: ReferenceTick | None
    latest_valid_tick: ReferenceTick | None
    stream_last_valid_message_at: datetime | None
    stream_age_ms: int | None


def load_market_feed_liveness(
    session: Session,
    config: AppConfig,
    *,
    checked_at: datetime,
    active_market_ticker: str | None = None,
) -> MarketFeedLiveness:
    repository = WorkerHeartbeatRepository(session)
    component = repository.get_latest_heartbeat(WORKER_SERVICE_MARKET_WS)
    if component is None:
        component = repository.get_latest_heartbeat(WORKER_SERVICE_MARKET_WS_LEGACY)
    aggregate = repository.get_latest_heartbeat(WORKER_SERVICE_AGGREGATE)
    component_metadata = _ws_metadata(component)
    aggregate_metadata = _ws_metadata(aggregate)
    selected, metadata, source, warnings = _select_component_metadata(
        component=component,
        component_metadata=component_metadata,
        aggregate=aggregate,
        aggregate_metadata=aggregate_metadata,
    )
    component_heartbeat_at = _heartbeat_at(component) if component_metadata else None
    heartbeat_at = _heartbeat_at(selected)
    active_ticker = (
        active_market_ticker
        or _str_or_none((metadata or {}).get("active_market_ticker"))
    )
    orderbook_repository = OrderbookRepository(session)
    latest_orderbook = (
        orderbook_repository.get_latest_snapshot(active_ticker)
        if active_ticker
        else orderbook_repository.get_latest_snapshot_any()
    )
    latest_trade = PublicTradesRepository(session).get_latest_trade(active_ticker)
    stream_last_message_at = _latest_datetime(
        _datetime_or_none((metadata or {}).get("last_message_at")),
        _datetime_or_none((metadata or {}).get("last_ticker_at")),
        _datetime_or_none((metadata or {}).get("last_trade_at")),
        _datetime_or_none((metadata or {}).get("last_orderbook_at")),
    )
    last_market_data_message_at = _latest_datetime(
        _datetime_or_none((metadata or {}).get("last_market_data_message_at")),
        _datetime_or_none((metadata or {}).get("last_ticker_at")),
        _datetime_or_none((metadata or {}).get("last_trade_at")),
        _datetime_or_none((metadata or {}).get("last_orderbook_at")),
    )
    transport_last_pong_at = _datetime_or_none(
        (metadata or {}).get("transport_last_pong_at")
    )
    transport_age_ms = _age_ms(transport_last_pong_at, checked_at)
    transport_state = _transport_state(
        alive=_bool_or_false((metadata or {}).get("transport_alive")),
        transport_age_ms=transport_age_ms,
        timeout_ms=int(config.kalshi_ws_heartbeat_timeout_seconds * 1000),
    )
    stored_transport_state = _str_or_none(
        (metadata or {}).get("market_feed_transport_state")
    )
    if transport_state == "unknown" and stored_transport_state == "stale":
        transport_state = "stale"
    orderbook_snapshot_age_ms = _age_ms(
        _as_utc(latest_orderbook.received_at) if latest_orderbook else None,
        checked_at,
    )
    active_ticker_state = _active_ticker_state(
        expected=active_market_ticker,
        observed=_str_or_none((metadata or {}).get("active_market_ticker")),
    )
    market_data_age_ms = _age_ms(last_market_data_message_at, checked_at)
    market_data_quiet = _bool_or_false((metadata or {}).get("market_data_quiet")) or (
        market_data_age_ms is not None
        and market_data_age_ms > config.strategy_kalshi_book_stream_max_age_ms
    )
    market_data_quiet_age_ms = _int_or_none(
        (metadata or {}).get("market_data_quiet_age_ms")
    ) or (market_data_age_ms if market_data_quiet else None)
    orderbook_recovery_action = _str_or_none(
        (metadata or {}).get("orderbook_recovery_action")
    )
    if orderbook_recovery_action == "none":
        orderbook_recovery_action = None
    if orderbook_recovery_action is None and (
        market_data_quiet
        or (
            orderbook_snapshot_age_ms is not None
            and orderbook_snapshot_age_ms
            > config.strategy_kalshi_book_carry_forward_max_age_ms
        )
    ):
        orderbook_recovery_action = "request_snapshot"
    return MarketFeedLiveness(
        source=source,
        metadata=metadata,
        heartbeat_at=heartbeat_at,
        heartbeat_age_ms=_age_ms(heartbeat_at, checked_at),
        started_at=_started_at(selected),
        component_heartbeat_at=component_heartbeat_at,
        component_heartbeat_age_ms=_age_ms(component_heartbeat_at, checked_at),
        latest_aggregate_heartbeat_mode=_metadata_mode(aggregate),
        latest_component_heartbeat_mode=_metadata_mode(component),
        liveness_source_mismatch=_liveness_source_mismatch(
            source=source,
            component=component,
            aggregate=aggregate,
        ),
        warnings=warnings,
        worker_role=_str_or_none((metadata or {}).get("worker_role")),
        connection_id=_str_or_none((metadata or {}).get("connection_id")),
        protocol_connection_state=_str_or_none(
            (metadata or {}).get("protocol_connection_state")
        ),
        active_market_ticker=active_ticker,
        subscription_reconciled=_bool_or_false(
            (metadata or {}).get("subscription_reconciled")
        ),
        orderbook_sid_confirmed=_bool_or_false(
            (metadata or {}).get("orderbook_sid_confirmed")
        ),
        ticker_sid_confirmed=_bool_or_false(
            (metadata or {}).get("ticker_sid_confirmed")
        ),
        trade_sid_confirmed=_bool_or_false(
            (metadata or {}).get("trade_sid_confirmed")
        ),
        last_list_subscriptions_at=_datetime_or_none(
            (metadata or {}).get("last_list_subscriptions_at")
        ),
        last_list_subscriptions_result=_str_or_none(
            (metadata or {}).get("last_list_subscriptions_result")
        ),
        in_flight_snapshot_request=_bool_or_false(
            (metadata or {}).get("in_flight_snapshot_request")
        ),
        snapshot_request_age_ms=_int_or_none(
            (metadata or {}).get("snapshot_request_age_ms")
        ),
        protocol_event_recent_error_count=_int_or_zero(
            (metadata or {}).get("protocol_event_recent_error_count")
        ),
        ws_reader_queue_depth=_int_or_zero(
            (metadata or {}).get("ws_reader_queue_depth")
        ),
        ws_reader_queue_oldest_age_ms=_int_or_none(
            (metadata or {}).get("ws_reader_queue_oldest_age_ms")
        ),
        db_writer_queue_depth=_int_or_zero(
            (metadata or {}).get("db_writer_queue_depth")
        ),
        db_writer_queue_oldest_age_ms=_int_or_none(
            (metadata or {}).get("db_writer_queue_oldest_age_ms")
        ),
        db_writer_last_flush_ms=_int_or_none(
            (metadata or {}).get("db_writer_last_flush_ms")
        ),
        db_writer_slow_flush_count=_int_or_zero(
            (metadata or {}).get("db_writer_slow_flush_count")
        ),
        reconnect_reason=_str_or_none((metadata or {}).get("reconnect_reason")),
        close_code=_int_or_none((metadata or {}).get("close_code")),
        close_reason=_str_or_none((metadata or {}).get("close_reason")),
        latest_orderbook=latest_orderbook,
        latest_trade=latest_trade,
        latest_orderbook_received_at=(
            _as_utc(latest_orderbook.received_at) if latest_orderbook else None
        ),
        latest_trade_received_at=(
            _as_utc(latest_trade.received_at) if latest_trade else None
        ),
        stream_last_message_at=stream_last_message_at,
        stream_age_ms=_age_ms(stream_last_message_at, checked_at),
        transport_alive=_bool_or_false((metadata or {}).get("transport_alive")),
        transport_last_pong_at=transport_last_pong_at,
        transport_age_ms=transport_age_ms,
        transport_liveness_reason=_str_or_none(
            (metadata or {}).get("transport_liveness_reason")
        ),
        last_market_data_message_at=last_market_data_message_at,
        market_data_message_age_ms=market_data_age_ms,
        market_feed_transport_state=transport_state,
        market_feed_subscription_state=_metadata_state(
            metadata,
            "market_feed_subscription_state",
            _subscription_state(_str_or_none((metadata or {}).get("connection_state"))),
        ),
        market_feed_snapshot_state=_metadata_state(
            metadata,
            "market_feed_snapshot_state",
            _snapshot_state(
                initialized=_bool_or_false((metadata or {}).get("orderbook_initialized")),
                orderbook_snapshot_age_ms=orderbook_snapshot_age_ms,
                carry_forward_max_age_ms=(
                    config.strategy_kalshi_book_carry_forward_max_age_ms
                ),
            ),
        ),
        market_feed_active_ticker_state=_metadata_state(
            metadata,
            "market_feed_active_ticker_state",
            active_ticker_state,
        ),
        market_feed_sequence_state=_metadata_state(
            metadata,
            "market_feed_sequence_state",
            _sequence_state(metadata),
        ),
        market_data_quiet=market_data_quiet,
        market_data_quiet_age_ms=market_data_quiet_age_ms,
        orderbook_snapshot_age_ms=orderbook_snapshot_age_ms,
        orderbook_snapshot_source=_str_or_none(
            (metadata or {}).get("orderbook_snapshot_source")
        ),
        orderbook_recovery_action=orderbook_recovery_action,
        market_feed_state=_str_or_none((metadata or {}).get("market_feed_state")),
        market_subscription_recovery_count=_int_or_zero(
            (metadata or {}).get("market_subscription_recovery_count")
        ),
        market_subscription_recovery_last_reason=_str_or_none(
            (metadata or {}).get("market_subscription_recovery_last_reason")
        ),
        market_subscription_recovery_last_action=_str_or_none(
            (metadata or {}).get("market_subscription_recovery_last_action")
        ),
        market_subscription_recovery_last_result=_str_or_none(
            (metadata or {}).get("market_subscription_recovery_last_result")
        ),
        market_subscription_recovery_last_at=_datetime_or_none(
            (metadata or {}).get("market_subscription_recovery_last_at")
        ),
        market_snapshot_resync_count=_int_or_zero(
            (metadata or {}).get("market_snapshot_resync_count")
        ),
        market_snapshot_resync_last_result=_str_or_none(
            (metadata or {}).get("market_snapshot_resync_last_result")
        ),
        market_rollover_recovery_count=_int_or_zero(
            (metadata or {}).get("market_rollover_recovery_count")
        ),
        market_transport_reconnect_count=_int_or_zero(
            (metadata or {}).get("market_transport_reconnect_count")
        ),
        market_unrecovered_blocker_count=_int_or_zero(
            (metadata or {}).get("market_unrecovered_blocker_count")
        ),
        market_recovery_attempt_in_progress=_bool_or_false(
            (metadata or {}).get("market_recovery_attempt_in_progress")
        ),
        market_recovery_attempt_age_ms=_int_or_none(
            (metadata or {}).get("market_recovery_attempt_age_ms")
        ),
    )


def load_reference_feed_liveness(
    session: Session,
    config: AppConfig,
    *,
    checked_at: datetime,
) -> ReferenceFeedLiveness:
    del config
    repository = WorkerHeartbeatRepository(session)
    component = repository.get_latest_heartbeat(WORKER_SERVICE_REFERENCE_BRTI)
    aggregate = repository.get_latest_heartbeat(WORKER_SERVICE_AGGREGATE)
    component_metadata = _reference_metadata(component)
    aggregate_metadata = _reference_metadata(aggregate)
    selected, metadata, source, warnings = _select_component_metadata(
        component=component,
        component_metadata=component_metadata,
        aggregate=aggregate,
        aggregate_metadata=aggregate_metadata,
    )
    component_heartbeat_at = _heartbeat_at(component) if component_metadata else None
    heartbeat_at = _heartbeat_at(selected)
    reference_repository = ReferenceTicksRepository(session)
    latest_tick = reference_repository.get_latest_tick(BRTI_SOURCE)
    latest_valid_tick = reference_repository.get_latest_valid_tick(BRTI_SOURCE)
    stream_last_valid_message_at = _datetime_or_none(
        (metadata or {}).get("last_valid_message_at")
    )
    return ReferenceFeedLiveness(
        source=source,
        metadata=metadata,
        heartbeat_at=heartbeat_at,
        heartbeat_age_ms=_age_ms(heartbeat_at, checked_at),
        started_at=_started_at(selected),
        component_heartbeat_at=component_heartbeat_at,
        component_heartbeat_age_ms=_age_ms(component_heartbeat_at, checked_at),
        latest_aggregate_heartbeat_mode=_metadata_mode(aggregate),
        latest_component_heartbeat_mode=_metadata_mode(component),
        liveness_source_mismatch=_liveness_source_mismatch(
            source=source,
            component=component,
            aggregate=aggregate,
        ),
        warnings=warnings,
        latest_tick=latest_tick,
        latest_valid_tick=latest_valid_tick,
        stream_last_valid_message_at=stream_last_valid_message_at,
        stream_age_ms=_age_ms(stream_last_valid_message_at, checked_at),
    )


def _select_component_metadata(
    *,
    component: WorkerHeartbeat | None,
    component_metadata: dict[str, Any] | None,
    aggregate: WorkerHeartbeat | None,
    aggregate_metadata: dict[str, Any] | None,
) -> tuple[WorkerHeartbeat | None, dict[str, Any] | None, str, list[str]]:
    if component is not None and component_metadata is not None:
        return component, component_metadata, LIVENESS_SOURCE_COMPONENT, []
    if aggregate_metadata is not None:
        return (
            aggregate,
            aggregate_metadata,
            LIVENESS_SOURCE_LEGACY_FALLBACK,
            [FEED_LIVENESS_LEGACY_FALLBACK_WARNING],
        )
    return None, None, LIVENESS_SOURCE_MISSING, []


def _ws_metadata(heartbeat: WorkerHeartbeat | None) -> dict[str, Any] | None:
    if heartbeat is None or not isinstance(heartbeat.metadata_, dict):
        return None
    ws_metadata = heartbeat.metadata_.get("ws")
    return ws_metadata if isinstance(ws_metadata, dict) else None


def _reference_metadata(heartbeat: WorkerHeartbeat | None) -> dict[str, Any] | None:
    if heartbeat is None or not isinstance(heartbeat.metadata_, dict):
        return None
    reference = heartbeat.metadata_.get("reference")
    if not isinstance(reference, dict):
        return None
    brti = reference.get("brti")
    return brti if isinstance(brti, dict) else None


def _metadata_mode(heartbeat: WorkerHeartbeat | None) -> str | None:
    if heartbeat is None or not isinstance(heartbeat.metadata_, dict):
        return None
    return _str_or_none(heartbeat.metadata_.get("mode"))


def _heartbeat_at(heartbeat: WorkerHeartbeat | None) -> datetime | None:
    return _as_utc(heartbeat.heartbeat_at) if heartbeat is not None else None


def _started_at(heartbeat: WorkerHeartbeat | None) -> datetime | None:
    return _as_utc(heartbeat.started_at) if heartbeat is not None else None


def _liveness_source_mismatch(
    *,
    source: str,
    component: WorkerHeartbeat | None,
    aggregate: WorkerHeartbeat | None,
) -> bool:
    del component, aggregate
    return source == LIVENESS_SOURCE_LEGACY_FALLBACK


def _age_ms(value_at: datetime | None, checked_at: datetime) -> int | None:
    if value_at is None:
        return None
    return max(0, int((_as_utc(checked_at) - _as_utc(value_at)).total_seconds() * 1000))


def _latest_datetime(*values: datetime | None) -> datetime | None:
    present = [_as_utc(value) for value in values if value is not None]
    return max(present) if present else None


def _datetime_or_none(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _as_utc(value)
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _bool_or_false(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _int_or_zero(value: Any) -> int:
    parsed = _int_or_none(value)
    return parsed if parsed is not None else 0


def _metadata_state(
    metadata: dict[str, Any] | None,
    key: str,
    fallback: str,
) -> str:
    return _str_or_none((metadata or {}).get(key)) or fallback


def _transport_state(
    *,
    alive: bool,
    transport_age_ms: int | None,
    timeout_ms: int,
) -> str:
    if transport_age_ms is None:
        return "unknown"
    return "healthy" if alive and transport_age_ms <= timeout_ms else "stale"


def _subscription_state(connection_state: str | None) -> str:
    if connection_state == "subscribed":
        return "subscribed"
    if connection_state == "error":
        return "error"
    if connection_state in {"disabled", "not_configured"}:
        return "unsubscribed"
    return "unknown"


def _snapshot_state(
    *,
    initialized: bool,
    orderbook_snapshot_age_ms: int | None,
    carry_forward_max_age_ms: int,
) -> str:
    if not initialized:
        return "missing"
    if (
        orderbook_snapshot_age_ms is not None
        and orderbook_snapshot_age_ms > carry_forward_max_age_ms
    ):
        return "stale_cap_exceeded"
    return "initialized"


def _active_ticker_state(*, expected: str | None, observed: str | None) -> str:
    if expected is None and observed is None:
        return "missing"
    if expected is not None and observed is not None and expected != observed:
        return "mismatch"
    return "match"


def _sequence_state(metadata: dict[str, Any] | None) -> str:
    warnings = _string_list((metadata or {}).get("warnings"))
    blockers = _string_list((metadata or {}).get("blockers"))
    reasons = set(warnings + blockers)
    if reasons.intersection(
        {
            "orderbook_sequence_gap_reset",
            "orderbook_reset_after_buffer_overflow",
            "kalshi_websocket_buffer_overflow",
            "kalshi_orderbook_sequence_gap_or_reset",
        }
    ):
        return "gap"
    if _bool_or_false((metadata or {}).get("orderbook_initialized")):
        return "clean"
    return "unknown"


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item is not None]
