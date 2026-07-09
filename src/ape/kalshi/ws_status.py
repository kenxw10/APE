from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy.exc import SQLAlchemyError

from ape.config import AppConfig
from ape.db.session import create_engine_from_config, create_session_factory
from ape.kalshi.diagnostics import build_kalshi_config_diagnostic
from ape.worker.feed_liveness import (
    FEED_LIVENESS_LEGACY_FALLBACK_WARNING,
    load_market_feed_liveness,
)


@dataclass(frozen=True)
class KalshiWsStatusSnapshot:
    configured: bool
    enabled: bool
    signer_ready: bool
    endpoint_host: str
    endpoint_path: str
    connection_state: str
    active_market_ticker: str | None
    liveness_source: str
    worker_heartbeat_at: datetime | None
    worker_heartbeat_age_ms: int | None
    worker_started_at: datetime | None
    component_heartbeat_at: datetime | None
    component_heartbeat_age_ms: int | None
    latest_aggregate_heartbeat_mode: str | None
    latest_component_heartbeat_mode: str | None
    liveness_source_mismatch: bool
    worker_role: str | None
    connection_id: str | None
    protocol_connection_state: str | None
    subscribed_channels: list[str]
    subscription_ids: dict[str, int]
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
    orderbook_persistence_pending: bool
    orderbook_persistence_pending_count: int
    orderbook_persistence_pending_since: datetime | None
    orderbook_persistence_pending_age_ms: int | None
    reconnect_reason: str | None
    close_code: int | None
    close_reason: str | None
    last_connected_at: datetime | None
    last_message_at: datetime | None
    last_ticker_at: datetime | None
    last_orderbook_at: datetime | None
    last_trade_at: datetime | None
    latest_orderbook_received_at: datetime | None
    latest_trade_received_at: datetime | None
    orderbook_stream_age_ms: int | None
    orderbook_liveness_reason: str | None
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
    reconnect_count: int
    last_error_type: str | None
    last_error_message: str | None
    warnings: list[str]
    blockers: list[str]
    diagnostic_samples: list[dict[str, Any]]
    stale: bool
    checked_at: datetime


def build_kalshi_ws_status(
    config: AppConfig,
    *,
    now: datetime | None = None,
) -> KalshiWsStatusSnapshot:
    checked_at = now or datetime.now(UTC)
    diagnostic = build_kalshi_config_diagnostic(config)
    parsed_endpoint = urlsplit(config.kalshi_ws_base_url)
    warnings: list[str] = []
    blockers: list[str] = []
    heartbeat_metadata: dict[str, Any] = {}
    liveness_source = "missing"
    worker_heartbeat_at: datetime | None = None
    worker_heartbeat_age_ms: int | None = None
    worker_started_at: datetime | None = None
    component_heartbeat_at: datetime | None = None
    component_heartbeat_age_ms: int | None = None
    latest_aggregate_heartbeat_mode: str | None = None
    latest_component_heartbeat_mode: str | None = None
    liveness_source_mismatch = False
    worker_role: str | None = None
    connection_id: str | None = None
    protocol_connection_state: str | None = None
    subscription_reconciled = False
    orderbook_sid_confirmed = False
    ticker_sid_confirmed = False
    trade_sid_confirmed = False
    last_list_subscriptions_at: datetime | None = None
    last_list_subscriptions_result: str | None = None
    in_flight_snapshot_request = False
    snapshot_request_age_ms: int | None = None
    protocol_event_recent_error_count = 0
    ws_reader_queue_depth = 0
    ws_reader_queue_oldest_age_ms: int | None = None
    db_writer_queue_depth = 0
    db_writer_queue_oldest_age_ms: int | None = None
    db_writer_last_flush_ms: int | None = None
    db_writer_slow_flush_count = 0
    orderbook_persistence_pending = False
    orderbook_persistence_pending_count = 0
    orderbook_persistence_pending_since: datetime | None = None
    orderbook_persistence_pending_age_ms: int | None = None
    reconnect_reason: str | None = None
    close_code: int | None = None
    close_reason: str | None = None
    active_market_ticker: str | None = None
    latest_orderbook_at: datetime | None = None
    latest_trade_at: datetime | None = None
    orderbook_stream_age_ms: int | None = None
    transport_alive = False
    transport_last_pong_at: datetime | None = None
    transport_age_ms: int | None = None
    transport_liveness_reason: str | None = None
    last_market_data_message_at: datetime | None = None
    market_data_message_age_ms: int | None = None
    market_feed_transport_state = "unknown"
    market_feed_subscription_state = "unknown"
    market_feed_snapshot_state = "missing"
    market_feed_active_ticker_state = "missing"
    market_feed_sequence_state = "unknown"
    market_data_quiet = False
    market_data_quiet_age_ms: int | None = None
    orderbook_snapshot_age_ms: int | None = None
    orderbook_snapshot_source: str | None = None
    orderbook_recovery_action: str | None = None
    market_feed_state: str | None = None
    market_subscription_recovery_count = 0
    market_subscription_recovery_last_reason: str | None = None
    market_subscription_recovery_last_action: str | None = None
    market_subscription_recovery_last_result: str | None = None
    market_subscription_recovery_last_at: datetime | None = None
    market_snapshot_resync_count = 0
    market_snapshot_resync_last_result: str | None = None
    market_rollover_recovery_count = 0
    market_transport_reconnect_count = 0
    market_unrecovered_blocker_count = 0
    market_recovery_attempt_in_progress = False
    market_recovery_attempt_age_ms: int | None = None

    if not config.kalshi_ws_enabled:
        connection_state = "disabled"
    elif not diagnostic.signer_ready:
        connection_state = "not_configured"
        blockers.append("kalshi_ws_credentials_not_configured_or_not_parseable")
    else:
        connection_state = "waiting_for_worker"

    if config.database_url:
        try:
            engine = create_engine_from_config(config)
            try:
                session_factory = create_session_factory(engine)
                with session_factory() as session:
                    liveness = load_market_feed_liveness(
                        session,
                        config,
                        checked_at=checked_at,
                    )
                    heartbeat_metadata = liveness.metadata or {}
                    warnings.extend(liveness.warnings)
                    liveness_source = liveness.source
                    worker_heartbeat_at = liveness.heartbeat_at
                    worker_heartbeat_age_ms = liveness.heartbeat_age_ms
                    worker_started_at = liveness.started_at
                    component_heartbeat_at = liveness.component_heartbeat_at
                    component_heartbeat_age_ms = liveness.component_heartbeat_age_ms
                    latest_aggregate_heartbeat_mode = (
                        liveness.latest_aggregate_heartbeat_mode
                    )
                    latest_component_heartbeat_mode = (
                        liveness.latest_component_heartbeat_mode
                    )
                    liveness_source_mismatch = liveness.liveness_source_mismatch
                    worker_role = liveness.worker_role
                    connection_id = liveness.connection_id
                    protocol_connection_state = liveness.protocol_connection_state
                    subscription_reconciled = liveness.subscription_reconciled
                    orderbook_sid_confirmed = liveness.orderbook_sid_confirmed
                    ticker_sid_confirmed = liveness.ticker_sid_confirmed
                    trade_sid_confirmed = liveness.trade_sid_confirmed
                    last_list_subscriptions_at = liveness.last_list_subscriptions_at
                    last_list_subscriptions_result = (
                        liveness.last_list_subscriptions_result
                    )
                    in_flight_snapshot_request = liveness.in_flight_snapshot_request
                    snapshot_request_age_ms = liveness.snapshot_request_age_ms
                    protocol_event_recent_error_count = (
                        liveness.protocol_event_recent_error_count
                    )
                    ws_reader_queue_depth = liveness.ws_reader_queue_depth
                    ws_reader_queue_oldest_age_ms = (
                        liveness.ws_reader_queue_oldest_age_ms
                    )
                    db_writer_queue_depth = liveness.db_writer_queue_depth
                    db_writer_queue_oldest_age_ms = (
                        liveness.db_writer_queue_oldest_age_ms
                    )
                    db_writer_last_flush_ms = liveness.db_writer_last_flush_ms
                    db_writer_slow_flush_count = liveness.db_writer_slow_flush_count
                    orderbook_persistence_pending = (
                        liveness.orderbook_persistence_pending
                    )
                    orderbook_persistence_pending_count = (
                        liveness.orderbook_persistence_pending_count
                    )
                    orderbook_persistence_pending_since = (
                        liveness.orderbook_persistence_pending_since
                    )
                    orderbook_persistence_pending_age_ms = (
                        liveness.orderbook_persistence_pending_age_ms
                    )
                    reconnect_reason = liveness.reconnect_reason
                    close_code = liveness.close_code
                    close_reason = liveness.close_reason
                    active_market_ticker = liveness.active_market_ticker
                    latest_orderbook_at = liveness.latest_orderbook_received_at
                    latest_trade_at = liveness.latest_trade_received_at
                    orderbook_stream_age_ms = liveness.stream_age_ms
                    transport_alive = liveness.transport_alive
                    transport_last_pong_at = liveness.transport_last_pong_at
                    transport_age_ms = liveness.transport_age_ms
                    transport_liveness_reason = liveness.transport_liveness_reason
                    last_market_data_message_at = liveness.last_market_data_message_at
                    market_data_message_age_ms = liveness.market_data_message_age_ms
                    market_feed_transport_state = liveness.market_feed_transport_state
                    market_feed_subscription_state = (
                        liveness.market_feed_subscription_state
                    )
                    market_feed_snapshot_state = liveness.market_feed_snapshot_state
                    market_feed_active_ticker_state = (
                        liveness.market_feed_active_ticker_state
                    )
                    market_feed_sequence_state = liveness.market_feed_sequence_state
                    market_data_quiet = liveness.market_data_quiet
                    market_data_quiet_age_ms = liveness.market_data_quiet_age_ms
                    orderbook_snapshot_age_ms = liveness.orderbook_snapshot_age_ms
                    orderbook_snapshot_source = liveness.orderbook_snapshot_source
                    orderbook_recovery_action = liveness.orderbook_recovery_action
                    market_feed_state = liveness.market_feed_state
                    market_subscription_recovery_count = (
                        liveness.market_subscription_recovery_count
                    )
                    market_subscription_recovery_last_reason = (
                        liveness.market_subscription_recovery_last_reason
                    )
                    market_subscription_recovery_last_action = (
                        liveness.market_subscription_recovery_last_action
                    )
                    market_subscription_recovery_last_result = (
                        liveness.market_subscription_recovery_last_result
                    )
                    market_subscription_recovery_last_at = (
                        liveness.market_subscription_recovery_last_at
                    )
                    market_snapshot_resync_count = liveness.market_snapshot_resync_count
                    market_snapshot_resync_last_result = (
                        liveness.market_snapshot_resync_last_result
                    )
                    market_rollover_recovery_count = (
                        liveness.market_rollover_recovery_count
                    )
                    market_transport_reconnect_count = (
                        liveness.market_transport_reconnect_count
                    )
                    market_unrecovered_blocker_count = (
                        liveness.market_unrecovered_blocker_count
                    )
                    market_recovery_attempt_in_progress = (
                        liveness.market_recovery_attempt_in_progress
                    )
                    market_recovery_attempt_age_ms = (
                        liveness.market_recovery_attempt_age_ms
                    )
            finally:
                engine.dispose()
        except SQLAlchemyError:
            blockers.append("database_unavailable_for_ws_diagnostics")
            if config.kalshi_ws_enabled:
                connection_state = "diagnostics_unavailable"
    elif config.kalshi_ws_enabled:
        blockers.append("database_not_configured_for_ws_diagnostics")

    if heartbeat_metadata:
        connection_state = (
            _str_or_none(heartbeat_metadata.get("connection_state")) or connection_state
        )
        warnings.extend(_string_list(heartbeat_metadata.get("warnings")))
        blockers.extend(_string_list(heartbeat_metadata.get("blockers")))

    enabled = _bool_or_none(heartbeat_metadata.get("enabled"))
    effective_enabled = config.kalshi_ws_enabled if enabled is None else enabled
    configured = _bool_or_none(heartbeat_metadata.get("configured"))
    effective_configured = diagnostic.configured if configured is None else configured
    signer_ready = _bool_or_none(heartbeat_metadata.get("signer_ready"))
    effective_signer_ready = diagnostic.signer_ready if signer_ready is None else signer_ready

    last_message_at = _datetime_or_none(heartbeat_metadata.get("last_message_at"))
    if last_message_at is None:
        last_message_at = _latest_datetime(latest_orderbook_at, latest_trade_at)

    stale = (
        effective_enabled
        and market_feed_transport_state == "stale"
        or _is_stale(
            enabled=effective_enabled and market_feed_transport_state == "unknown",
            last_message_at=last_message_at,
            checked_at=checked_at,
            stale_after_seconds=config.kalshi_ws_heartbeat_timeout_seconds,
        )
    )
    if stale:
        warnings.append("kalshi_orderbook_transport_stale")
    if market_data_quiet and not stale:
        warnings.append("kalshi_orderbook_data_quiet_carried_forward")

    final_warnings = sorted(set(warnings))
    final_blockers = sorted(set(blockers))
    last_error_type = _str_or_none(heartbeat_metadata.get("last_error_type"))
    last_error_message = _str_or_none(heartbeat_metadata.get("last_error_message"))
    if _healthy_stream_recovered_error(
        connection_state=connection_state,
        stale=stale,
        warnings=[
            warning
            for warning in final_warnings
            if warning != FEED_LIVENESS_LEGACY_FALLBACK_WARNING
        ],
        blockers=final_blockers,
        last_message_at=last_message_at,
        latest_orderbook_at=latest_orderbook_at,
        latest_trade_at=latest_trade_at,
    ):
        last_error_type = None
        last_error_message = None

    return KalshiWsStatusSnapshot(
        configured=effective_configured,
        enabled=effective_enabled,
        signer_ready=effective_signer_ready,
        endpoint_host=parsed_endpoint.netloc,
        endpoint_path=parsed_endpoint.path,
        connection_state=connection_state,
        active_market_ticker=active_market_ticker,
        liveness_source=liveness_source,
        worker_heartbeat_at=worker_heartbeat_at,
        worker_heartbeat_age_ms=worker_heartbeat_age_ms,
        worker_started_at=worker_started_at,
        component_heartbeat_at=component_heartbeat_at,
        component_heartbeat_age_ms=component_heartbeat_age_ms,
        latest_aggregate_heartbeat_mode=latest_aggregate_heartbeat_mode,
        latest_component_heartbeat_mode=latest_component_heartbeat_mode,
        liveness_source_mismatch=liveness_source_mismatch,
        worker_role=worker_role,
        connection_id=connection_id,
        protocol_connection_state=protocol_connection_state,
        subscribed_channels=_string_list(heartbeat_metadata.get("subscribed_channels")),
        subscription_ids=_int_dict(heartbeat_metadata.get("subscription_ids")),
        subscription_reconciled=subscription_reconciled,
        orderbook_sid_confirmed=orderbook_sid_confirmed,
        ticker_sid_confirmed=ticker_sid_confirmed,
        trade_sid_confirmed=trade_sid_confirmed,
        last_list_subscriptions_at=last_list_subscriptions_at,
        last_list_subscriptions_result=last_list_subscriptions_result,
        in_flight_snapshot_request=in_flight_snapshot_request,
        snapshot_request_age_ms=snapshot_request_age_ms,
        protocol_event_recent_error_count=protocol_event_recent_error_count,
        ws_reader_queue_depth=ws_reader_queue_depth,
        ws_reader_queue_oldest_age_ms=ws_reader_queue_oldest_age_ms,
        db_writer_queue_depth=db_writer_queue_depth,
        db_writer_queue_oldest_age_ms=db_writer_queue_oldest_age_ms,
        db_writer_last_flush_ms=db_writer_last_flush_ms,
        db_writer_slow_flush_count=db_writer_slow_flush_count,
        orderbook_persistence_pending=orderbook_persistence_pending,
        orderbook_persistence_pending_count=orderbook_persistence_pending_count,
        orderbook_persistence_pending_since=orderbook_persistence_pending_since,
        orderbook_persistence_pending_age_ms=orderbook_persistence_pending_age_ms,
        reconnect_reason=reconnect_reason,
        close_code=close_code,
        close_reason=close_reason,
        last_connected_at=_datetime_or_none(heartbeat_metadata.get("last_connected_at")),
        last_message_at=last_message_at,
        last_ticker_at=_datetime_or_none(heartbeat_metadata.get("last_ticker_at")),
        last_orderbook_at=_latest_datetime(
            _datetime_or_none(heartbeat_metadata.get("last_orderbook_at")),
            latest_orderbook_at,
        ),
        last_trade_at=_latest_datetime(
            _datetime_or_none(heartbeat_metadata.get("last_trade_at")),
            latest_trade_at,
        ),
        latest_orderbook_received_at=latest_orderbook_at,
        latest_trade_received_at=latest_trade_at,
        orderbook_stream_age_ms=orderbook_stream_age_ms,
        orderbook_liveness_reason=_str_or_none(
            heartbeat_metadata.get("orderbook_liveness_reason")
        ),
        transport_alive=transport_alive,
        transport_last_pong_at=transport_last_pong_at,
        transport_age_ms=transport_age_ms,
        transport_liveness_reason=transport_liveness_reason,
        last_market_data_message_at=last_market_data_message_at,
        market_data_message_age_ms=market_data_message_age_ms,
        market_feed_transport_state=market_feed_transport_state,
        market_feed_subscription_state=market_feed_subscription_state,
        market_feed_snapshot_state=market_feed_snapshot_state,
        market_feed_active_ticker_state=market_feed_active_ticker_state,
        market_feed_sequence_state=market_feed_sequence_state,
        market_data_quiet=market_data_quiet,
        market_data_quiet_age_ms=market_data_quiet_age_ms,
        orderbook_snapshot_age_ms=orderbook_snapshot_age_ms,
        orderbook_snapshot_source=orderbook_snapshot_source,
        orderbook_recovery_action=orderbook_recovery_action,
        market_feed_state=market_feed_state,
        market_subscription_recovery_count=market_subscription_recovery_count,
        market_subscription_recovery_last_reason=(
            market_subscription_recovery_last_reason
        ),
        market_subscription_recovery_last_action=(
            market_subscription_recovery_last_action
        ),
        market_subscription_recovery_last_result=(
            market_subscription_recovery_last_result
        ),
        market_subscription_recovery_last_at=market_subscription_recovery_last_at,
        market_snapshot_resync_count=market_snapshot_resync_count,
        market_snapshot_resync_last_result=market_snapshot_resync_last_result,
        market_rollover_recovery_count=market_rollover_recovery_count,
        market_transport_reconnect_count=market_transport_reconnect_count,
        market_unrecovered_blocker_count=market_unrecovered_blocker_count,
        market_recovery_attempt_in_progress=market_recovery_attempt_in_progress,
        market_recovery_attempt_age_ms=market_recovery_attempt_age_ms,
        reconnect_count=_int_or_zero(heartbeat_metadata.get("reconnect_count")),
        last_error_type=last_error_type,
        last_error_message=last_error_message,
        warnings=final_warnings,
        blockers=final_blockers,
        diagnostic_samples=_diagnostic_samples(
            heartbeat_metadata.get("diagnostic_samples")
        ),
        stale=stale,
        checked_at=checked_at,
    )


def _healthy_stream_recovered_error(
    *,
    connection_state: str,
    stale: bool,
    warnings: list[str],
    blockers: list[str],
    last_message_at: datetime | None,
    latest_orderbook_at: datetime | None,
    latest_trade_at: datetime | None,
) -> bool:
    latest_persisted_at = _latest_datetime(latest_orderbook_at, latest_trade_at)
    return (
        connection_state == "subscribed"
        and not stale
        and not warnings
        and not blockers
        and last_message_at is not None
        and latest_persisted_at is not None
        and latest_persisted_at >= _as_utc(last_message_at)
    )


def _is_stale(
    *,
    enabled: bool,
    last_message_at: datetime | None,
    checked_at: datetime,
    stale_after_seconds: float,
) -> bool:
    if not enabled:
        return False
    if last_message_at is None:
        return True
    return (checked_at - last_message_at).total_seconds() > stale_after_seconds


def _latest_datetime(*values: datetime | None) -> datetime | None:
    present = [_as_utc(value) for value in values if value is not None]
    return max(present) if present else None


def _dict_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item is not None]


def _int_dict(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    parsed: dict[str, int] = {}
    for key, item in value.items():
        try:
            parsed[str(key)] = int(item)
        except (TypeError, ValueError):
            continue
    return parsed


def _diagnostic_samples(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)][:3]


def _datetime_or_none(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _as_utc(value)
    if not isinstance(value, str) or not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_or_zero(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _bool_or_none(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    return None
