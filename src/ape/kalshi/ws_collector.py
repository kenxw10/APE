from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
import uuid
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from inspect import isawaitable
from typing import Any

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from ape.config import AppConfig
from ape.kalshi.client import KalshiRestClient
from ape.kalshi.diagnostics import build_kalshi_config_diagnostic
from ape.kalshi.errors import KalshiError
from ape.kalshi.reference_messages import (
    BRTI_SOURCE,
    ParsedReferenceMessage,
    is_cfbenchmarks_value_payload,
    parse_cfbenchmarks_value_message,
)
from ape.kalshi.resolver import ResolverState, resolve_active_btc15_market
from ape.kalshi.ws_client import (
    build_cfbenchmarks_subscribe_message,
    build_list_subscriptions_message,
    build_subscribe_message,
    build_update_subscription_message,
    connect_websocket,
    create_websocket_auth_headers,
)
from ape.kalshi.ws_messages import ParsedWsMessage, parse_ws_payload
from ape.kalshi.ws_state import OrderbookState
from ape.repositories.inputs import (
    KalshiWsProtocolEventInput,
    OrderbookSnapshotInput,
    PublicTradeInput,
    ReferenceTickInput,
    WorkerHeartbeatInput,
)
from ape.repositories.kalshi_ws_protocol import KalshiWsProtocolEventRepository
from ape.repositories.orderbook import OrderbookRepository
from ape.repositories.public_trades import PublicTradesRepository
from ape.repositories.reference_ticks import ReferenceTicksRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.safety import SafetyAssessment
from ape.worker.services import (
    WORKER_SERVICE_AGGREGATE,
    WORKER_SERVICE_MARKET_WS,
    WORKER_SERVICE_REFERENCE_BRTI,
)

LOGGER = logging.getLogger(__name__)
MAX_HEARTBEAT_INTERVAL_SECONDS = 10.0
MIN_HEARTBEAT_INTERVAL_SECONDS = 1.0
MAX_DIAGNOSTIC_SAMPLES = 3
MAX_DIAGNOSTIC_KEYS = 20
PROTOCOL_ERROR_WINDOW_SECONDS = 1800
PROTOCOL_ERROR_EVENTS = {
    "websocket_error",
    "subscription_error",
    "list_subscriptions_error",
    "update_subscription_error",
    "get_snapshot_error",
    "reconnect_failed",
}
PROTOCOL_RECOVERY_EVENTS = {
    "reconnect_completed",
}
CRITICAL_PROTOCOL_EVENTS = PROTOCOL_ERROR_EVENTS | PROTOCOL_RECOVERY_EVENTS | {
    "websocket_open",
    "websocket_close",
    "subscribed_received",
    "reconnect_started",
    "reconnect_scheduled",
}
ROUTINE_PROTOCOL_EVENTS = {
    "orderbook_delta_received",
    "ticker_received",
    "trade_received",
    "client_ping_sent",
    "client_pong_received",
    "server_ping_received",
    "server_pong_sent",
    "db_write_slow",
    "queue_backpressure",
}
MARKET_DB_WRITE_DISABLED = "disabled"
MARKET_DB_WRITE_QUEUED = "queued"
MARKET_DB_WRITE_FULL = "full"
MARKET_DB_WRITE_COALESCED = "coalesced"

WebSocketFactory = Callable[
    [str, dict[str, str], float, float],
    Awaitable[Any],
]
Resolver = Callable[..., Any]


@dataclass(frozen=True)
class _DbWriterItem:
    kind: str
    payload: Any
    enqueued_at: datetime
    orderbook_recovery_result: str | None = None
    orderbook_recovery_reason: str | None = None
    orderbook_recovery_action: str | None = None
    clear_orderbook_recovery_warnings: bool = False
    clear_orderbook_delta_warnings: bool = False
    retry_count: int = 0


@dataclass
class KalshiWsCollectorStatus:
    enabled: bool
    worker_role: str = "all"
    connection_id: str | None = None
    protocol_connection_state: str = "disconnected"
    configured: bool = False
    signer_ready: bool = False
    connection_state: str = "disabled"
    active_market_ticker: str | None = None
    subscribed_channels: list[str] = field(default_factory=list)
    subscription_ids: dict[str, int] = field(default_factory=dict)
    subscription_request_ids: dict[str, int] = field(default_factory=dict)
    subscription_reconciled: bool = False
    orderbook_sid_confirmed: bool = False
    ticker_sid_confirmed: bool = False
    trade_sid_confirmed: bool = False
    list_subscriptions_request_id: int | None = None
    last_list_subscriptions_at: datetime | None = None
    last_list_subscriptions_result: str | None = None
    in_flight_snapshot_request: bool = False
    snapshot_request_id: int | None = None
    snapshot_request_started_at: datetime | None = None
    snapshot_request_age_ms: int | None = None
    protocol_event_recent_error_count: int = 0
    ws_reader_queue_depth: int = 0
    ws_reader_queue_oldest_age_ms: int | None = None
    db_writer_queue_depth: int = 0
    db_writer_queue_oldest_age_ms: int | None = None
    db_writer_critical_queue_depth: int = 0
    db_writer_critical_queue_oldest_age_ms: int | None = None
    db_writer_diagnostic_queue_depth: int = 0
    db_writer_diagnostic_queue_oldest_age_ms: int | None = None
    db_writer_last_flush_ms: int | None = None
    db_writer_last_critical_flush_ms: int | None = None
    db_writer_last_diagnostic_flush_ms: int | None = None
    db_writer_slow_flush_count: int = 0
    db_writer_dropped_diagnostic_count: int = 0
    db_writer_coalesced_orderbook_count: int = 0
    db_writer_coalesced_trade_count: int = 0
    db_writer_dropped_superseded_count: int = 0
    latest_state_persisted_at: datetime | None = None
    latest_state_persisted_age_ms: int | None = None
    latest_state_persistence_lag_ms: int | None = None
    historical_persistence_lag_ms: int | None = None
    protocol_events_enqueued: int = 0
    protocol_events_persisted: int = 0
    protocol_events_sampled_out: int = 0
    protocol_events_dropped_backpressure: int = 0
    protocol_errors_persisted: int = 0
    protocol_event_queue_depth: int = 0
    protocol_event_oldest_age_ms: int | None = None
    orderbook_persistence_pending_count: int = 0
    orderbook_persistence_pending_since: datetime | None = None
    orderbook_persistence_pending_age_ms: int | None = None
    reconnect_reason: str | None = None
    close_code: int | None = None
    close_reason: str | None = None
    last_connected_at: datetime | None = None
    last_message_at: datetime | None = None
    transport_alive: bool = False
    transport_last_ping_at: datetime | None = None
    transport_last_pong_at: datetime | None = None
    transport_age_ms: int | None = None
    transport_liveness_reason: str | None = None
    last_market_data_message_at: datetime | None = None
    market_data_message_age_ms: int | None = None
    last_ticker_at: datetime | None = None
    last_orderbook_at: datetime | None = None
    last_trade_at: datetime | None = None
    orderbook_initialized: bool = False
    orderbook_sequence_number: int | None = None
    orderbook_liveness_status: str = "disabled"
    orderbook_liveness_reason: str | None = None
    market_feed_transport_state: str = "unknown"
    market_feed_subscription_state: str = "unknown"
    market_feed_snapshot_state: str = "missing"
    market_feed_active_ticker_state: str = "missing"
    market_feed_sequence_state: str = "unknown"
    market_data_quiet: bool = False
    market_data_quiet_age_ms: int | None = None
    orderbook_snapshot_source: str = "blocked"
    orderbook_recovery_action: str = "none"
    market_feed_state: str = "DISCONNECTED"
    market_subscription_recovery_count: int = 0
    market_subscription_recovery_last_reason: str | None = None
    market_subscription_recovery_last_action: str | None = None
    market_subscription_recovery_last_result: str | None = None
    market_subscription_recovery_last_at: datetime | None = None
    market_snapshot_resync_count: int = 0
    market_snapshot_resync_last_result: str | None = None
    market_rollover_recovery_count: int = 0
    market_transport_reconnect_count: int = 0
    market_unrecovered_blocker_count: int = 0
    market_recovery_attempt_in_progress: bool = False
    market_recovery_attempt_started_at: datetime | None = None
    market_recovery_attempt_age_ms: int | None = None
    reconnect_count: int = 0
    last_error_type: str | None = None
    last_error_message: str | None = None
    warnings: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    diagnostic_samples: list[dict[str, Any]] = field(default_factory=list)

    def as_metadata(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "worker_role": self.worker_role,
            "connection_id": self.connection_id,
            "protocol_connection_state": self.protocol_connection_state,
            "configured": self.configured,
            "signer_ready": self.signer_ready,
            "connection_state": self.connection_state,
            "active_market_ticker": self.active_market_ticker,
            "subscribed_channels": self.subscribed_channels,
            "subscription_ids": self.subscription_ids,
            "subscription_request_ids": self.subscription_request_ids,
            "subscription_reconciled": self.subscription_reconciled,
            "orderbook_sid_confirmed": self.orderbook_sid_confirmed,
            "ticker_sid_confirmed": self.ticker_sid_confirmed,
            "trade_sid_confirmed": self.trade_sid_confirmed,
            "last_list_subscriptions_at": _isoformat_or_none(
                self.last_list_subscriptions_at
            ),
            "last_list_subscriptions_result": self.last_list_subscriptions_result,
            "in_flight_snapshot_request": self.in_flight_snapshot_request,
            "snapshot_request_age_ms": self.snapshot_request_age_ms,
            "protocol_event_recent_error_count": (
                self.protocol_event_recent_error_count
            ),
            "ws_reader_queue_depth": self.ws_reader_queue_depth,
            "ws_reader_queue_oldest_age_ms": self.ws_reader_queue_oldest_age_ms,
            "db_writer_queue_depth": self.db_writer_queue_depth,
            "db_writer_queue_oldest_age_ms": self.db_writer_queue_oldest_age_ms,
            "db_writer_critical_queue_depth": self.db_writer_critical_queue_depth,
            "db_writer_critical_queue_oldest_age_ms": (
                self.db_writer_critical_queue_oldest_age_ms
            ),
            "db_writer_diagnostic_queue_depth": self.db_writer_diagnostic_queue_depth,
            "db_writer_diagnostic_queue_oldest_age_ms": (
                self.db_writer_diagnostic_queue_oldest_age_ms
            ),
            "db_writer_last_flush_ms": self.db_writer_last_flush_ms,
            "db_writer_last_critical_flush_ms": self.db_writer_last_critical_flush_ms,
            "db_writer_last_diagnostic_flush_ms": (
                self.db_writer_last_diagnostic_flush_ms
            ),
            "db_writer_slow_flush_count": self.db_writer_slow_flush_count,
            "db_writer_dropped_diagnostic_count": (
                self.db_writer_dropped_diagnostic_count
            ),
            "db_writer_coalesced_orderbook_count": (
                self.db_writer_coalesced_orderbook_count
            ),
            "db_writer_coalesced_trade_count": self.db_writer_coalesced_trade_count,
            "db_writer_dropped_superseded_count": (
                self.db_writer_dropped_superseded_count
            ),
            "latest_state_persisted_at": _isoformat_or_none(
                self.latest_state_persisted_at
            ),
            "latest_state_persisted_age_ms": self.latest_state_persisted_age_ms,
            "latest_state_persistence_lag_ms": self.latest_state_persistence_lag_ms,
            "historical_persistence_lag_ms": self.historical_persistence_lag_ms,
            "protocol_events_enqueued": self.protocol_events_enqueued,
            "protocol_events_persisted": self.protocol_events_persisted,
            "protocol_events_sampled_out": self.protocol_events_sampled_out,
            "protocol_events_dropped_backpressure": (
                self.protocol_events_dropped_backpressure
            ),
            "protocol_errors_persisted": self.protocol_errors_persisted,
            "protocol_event_queue_depth": self.protocol_event_queue_depth,
            "protocol_event_oldest_age_ms": self.protocol_event_oldest_age_ms,
            "orderbook_persistence_pending": (
                self.orderbook_persistence_pending_count > 0
            ),
            "orderbook_persistence_pending_count": (
                self.orderbook_persistence_pending_count
            ),
            "orderbook_persistence_pending_since": _isoformat_or_none(
                self.orderbook_persistence_pending_since
            ),
            "orderbook_persistence_pending_age_ms": (
                self.orderbook_persistence_pending_age_ms
            ),
            "reconnect_reason": self.reconnect_reason,
            "close_code": self.close_code,
            "close_reason": self.close_reason,
            "last_connected_at": _isoformat_or_none(self.last_connected_at),
            "last_message_at": _isoformat_or_none(self.last_message_at),
            "transport_alive": self.transport_alive,
            "transport_last_ping_at": _isoformat_or_none(self.transport_last_ping_at),
            "transport_last_pong_at": _isoformat_or_none(self.transport_last_pong_at),
            "transport_age_ms": self.transport_age_ms,
            "transport_liveness_reason": self.transport_liveness_reason,
            "last_market_data_message_at": _isoformat_or_none(
                self.last_market_data_message_at
            ),
            "market_data_message_age_ms": self.market_data_message_age_ms,
            "last_ticker_at": _isoformat_or_none(self.last_ticker_at),
            "last_orderbook_at": _isoformat_or_none(self.last_orderbook_at),
            "last_trade_at": _isoformat_or_none(self.last_trade_at),
            "orderbook_initialized": self.orderbook_initialized,
            "orderbook_sequence_number": self.orderbook_sequence_number,
            "orderbook_liveness_status": self.orderbook_liveness_status,
            "orderbook_liveness_reason": self.orderbook_liveness_reason,
            "market_feed_transport_state": self.market_feed_transport_state,
            "market_feed_subscription_state": self.market_feed_subscription_state,
            "market_feed_snapshot_state": self.market_feed_snapshot_state,
            "market_feed_active_ticker_state": self.market_feed_active_ticker_state,
            "market_feed_sequence_state": self.market_feed_sequence_state,
            "market_data_quiet": self.market_data_quiet,
            "market_data_quiet_age_ms": self.market_data_quiet_age_ms,
            "orderbook_snapshot_source": self.orderbook_snapshot_source,
            "orderbook_recovery_action": self.orderbook_recovery_action,
            "market_feed_state": self.market_feed_state,
            "market_subscription_recovery_count": (
                self.market_subscription_recovery_count
            ),
            "market_subscription_recovery_last_reason": (
                self.market_subscription_recovery_last_reason
            ),
            "market_subscription_recovery_last_action": (
                self.market_subscription_recovery_last_action
            ),
            "market_subscription_recovery_last_result": (
                self.market_subscription_recovery_last_result
            ),
            "market_subscription_recovery_last_at": _isoformat_or_none(
                self.market_subscription_recovery_last_at
            ),
            "market_snapshot_resync_count": self.market_snapshot_resync_count,
            "market_snapshot_resync_last_result": (
                self.market_snapshot_resync_last_result
            ),
            "market_rollover_recovery_count": self.market_rollover_recovery_count,
            "market_transport_reconnect_count": self.market_transport_reconnect_count,
            "market_unrecovered_blocker_count": self.market_unrecovered_blocker_count,
            "market_recovery_attempt_in_progress": (
                self.market_recovery_attempt_in_progress
            ),
            "market_recovery_attempt_started_at": _isoformat_or_none(
                self.market_recovery_attempt_started_at
            ),
            "market_recovery_attempt_age_ms": self.market_recovery_attempt_age_ms,
            "reconnect_count": self.reconnect_count,
            "last_error_type": self.last_error_type,
            "last_error_message": self.last_error_message,
            "warnings": self.warnings,
            "blockers": self.blockers,
            "diagnostic_samples": self.diagnostic_samples,
        }


@dataclass
class BrtiReferenceStatus:
    enabled: bool
    configured: bool = False
    signer_ready: bool = False
    source: str = BRTI_SOURCE
    index_ids: list[str] = field(default_factory=list)
    subscription_id: int | None = None
    subscribed_channels: list[str] = field(default_factory=list)
    connection_state: str = "disabled"
    last_connected_at: datetime | None = None
    last_successful_subscribe_at: datetime | None = None
    last_subscription_ack_at: datetime | None = None
    last_message_at: datetime | None = None
    last_persisted_at: datetime | None = None
    last_valid_tick_at: datetime | None = None
    last_valid_message_at: datetime | None = None
    last_valid_message_source_ts: datetime | None = None
    last_valid_message_value: str | None = None
    last_duplicate_valid_message_at: datetime | None = None
    last_duplicate_valid_message_source_ts: datetime | None = None
    last_duplicate_valid_message_value: str | None = None
    valid_message_age_ms: int | None = None
    valid_message_duplicate_source_ts: bool = False
    valid_message_carried_forward: bool = False
    reference_stream_live: bool = False
    last_healthy_at: datetime | None = None
    last_recovered_at: datetime | None = None
    stale_since: datetime | None = None
    last_stale_check_at: datetime | None = None
    latest_source_ts: datetime | None = None
    latest_value: str | None = None
    latest_trailing_60s_avg: str | None = None
    latest_trailing_60s_window_size: int | None = None
    latest_final_minute_average: str | None = None
    final_minute_average_status: str | None = None
    source_age_ms: int | None = None
    subscription_request_id: int | None = None
    reconnect_count: int = 0
    inter_arrival_ms: int | None = None
    source_gap_ms: int | None = None
    recovery_state: str = "idle"
    consecutive_stale_count: int = 0
    consecutive_reconnect_count: int = 0
    consecutive_fresh_tick_count: int = 0
    duplicate_source_ts_count: int = 0
    out_of_order_source_ts_count: int = 0
    skipped_tick_count: int = 0
    last_skipped_reason: str | None = None
    last_skipped_at: datetime | None = None
    last_error_type: str | None = None
    last_error_message: str | None = None
    warnings: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)

    def as_metadata(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "configured": self.configured,
            "signer_ready": self.signer_ready,
            "source": self.source,
            "index_ids": self.index_ids,
            "subscription_id": self.subscription_id,
            "subscribed_channels": self.subscribed_channels,
            "connection_state": self.connection_state,
            "last_connected_at": _isoformat_or_none(self.last_connected_at),
            "last_successful_subscribe_at": _isoformat_or_none(
                self.last_successful_subscribe_at
            ),
            "last_subscription_ack_at": _isoformat_or_none(
                self.last_subscription_ack_at
            ),
            "last_message_at": _isoformat_or_none(self.last_message_at),
            "last_persisted_at": _isoformat_or_none(self.last_persisted_at),
            "last_valid_tick_at": _isoformat_or_none(self.last_valid_tick_at),
            "last_valid_message_at": _isoformat_or_none(self.last_valid_message_at),
            "last_valid_message_source_ts": _isoformat_or_none(
                self.last_valid_message_source_ts
            ),
            "last_valid_message_value": self.last_valid_message_value,
            "brti_reference_transport_alive": self.connection_state
            in {"connected", "subscribed"}
            and not self.blockers,
            "brti_reference_last_valid_message_age_ms": self.valid_message_age_ms,
            "brti_reference_no_valid_tick_timeout": (
                "brti_reference_no_valid_tick_timeout" in self.warnings
                or "brti_reference_no_valid_tick_timeout" in self.blockers
            ),
            "brti_reference_reconnect_requested": (
                "brti_reference_reconnect_requested" in self.warnings
                or "brti_reference_reconnect_requested" in self.blockers
            ),
            "last_duplicate_valid_message_at": _isoformat_or_none(
                self.last_duplicate_valid_message_at
            ),
            "last_duplicate_valid_message_source_ts": _isoformat_or_none(
                self.last_duplicate_valid_message_source_ts
            ),
            "last_duplicate_valid_message_value": self.last_duplicate_valid_message_value,
            "valid_message_age_ms": self.valid_message_age_ms,
            "valid_message_duplicate_source_ts": self.valid_message_duplicate_source_ts,
            "valid_message_carried_forward": self.valid_message_carried_forward,
            "reference_stream_live": self.reference_stream_live,
            "last_healthy_at": _isoformat_or_none(self.last_healthy_at),
            "last_recovered_at": _isoformat_or_none(self.last_recovered_at),
            "stale_since": _isoformat_or_none(self.stale_since),
            "last_stale_check_at": _isoformat_or_none(self.last_stale_check_at),
            "latest_source_ts": _isoformat_or_none(self.latest_source_ts),
            "latest_value": self.latest_value,
            "latest_trailing_60s_avg": self.latest_trailing_60s_avg,
            "latest_trailing_60s_window_size": self.latest_trailing_60s_window_size,
            "latest_final_minute_average": self.latest_final_minute_average,
            "final_minute_average_status": self.final_minute_average_status,
            "source_age_ms": self.source_age_ms,
            "subscription_request_id": self.subscription_request_id,
            "reconnect_count": self.reconnect_count,
            "inter_arrival_ms": self.inter_arrival_ms,
            "source_gap_ms": self.source_gap_ms,
            "recovery_state": self.recovery_state,
            "consecutive_stale_count": self.consecutive_stale_count,
            "consecutive_reconnect_count": self.consecutive_reconnect_count,
            "consecutive_fresh_tick_count": self.consecutive_fresh_tick_count,
            "duplicate_source_ts_count": self.duplicate_source_ts_count,
            "out_of_order_source_ts_count": self.out_of_order_source_ts_count,
            "skipped_tick_count": self.skipped_tick_count,
            "last_skipped_reason": self.last_skipped_reason,
            "last_skipped_at": _isoformat_or_none(self.last_skipped_at),
            "last_error_type": self.last_error_type,
            "last_error_message": self.last_error_message,
            "warnings": self.warnings,
            "blockers": self.blockers,
        }


class KalshiWsCollector:
    def __init__(
        self,
        *,
        config: AppConfig,
        safety: SafetyAssessment,
        session_factory: sessionmaker[Session] | None,
        started_at: datetime,
        websocket_factory: WebSocketFactory | None = None,
        resolver: Resolver = resolve_active_btc15_market,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config
        self.safety = safety
        self.session_factory = session_factory
        self.started_at = started_at
        self.websocket_factory = websocket_factory or _default_websocket_factory
        self.resolver = resolver
        self.now = now or (lambda: datetime.now(UTC))
        self.status = KalshiWsCollectorStatus(enabled=config.kalshi_ws_enabled)
        self.brti_status = BrtiReferenceStatus(
            enabled=config.kalshi_cfbenchmarks_enabled,
            index_ids=list(config.kalshi_cfbenchmarks_index_ids),
        )
        self._last_heartbeat_at: datetime | None = None
        self._last_market_heartbeat_at: datetime | None = None
        self._last_reference_heartbeat_at: datetime | None = None
        self._last_snapshot_resync_requested_at: datetime | None = None
        self._last_market_subscribe_requested_at: datetime | None = None
        self._next_request_id = 1000
        self._force_next_heartbeat = False
        self._forced_diagnostic_signatures: set[str] = set()
        self._connection_sequence = 0
        self._market_reconnect_completion_pending_reason: str | None = None
        self._db_writer_queue: asyncio.Queue[_DbWriterItem] | None = None
        self._db_critical_queue: asyncio.Queue[_DbWriterItem] | None = None
        self._db_diagnostic_queue: asyncio.Queue[_DbWriterItem] | None = None
        self._db_critical_enqueued_at: deque[datetime] = deque()
        self._db_critical_inflight_at: deque[datetime] = deque()
        self._db_diagnostic_enqueued_at: deque[datetime] = deque()
        self._db_writer_enqueued_at: deque[datetime] = self._db_critical_enqueued_at
        self._db_writer_task: asyncio.Task[None] | None = None
        self._pending_orderbook_writes: dict[str, _DbWriterItem] = {}
        self._pending_orderbook_markers: set[str] = set()
        self._protocol_event_sequence: dict[str, int] = {}
        self._protocol_error_events: deque[datetime] = deque()
        self._protocol_error_reset_at: datetime | None = None

    async def run(
        self,
        *,
        stop_event: threading.Event,
        max_cycles: int | None = None,
    ) -> None:
        if not self._collector_enabled():
            self.status.connection_state = "disabled"
            self.status.warnings = ["kalshi_ws_disabled"]
            self.brti_status.connection_state = "disabled"
            self.record_heartbeat()
            return

        if self._dedicated_reference_enabled():
            tasks = []
            if self.config.kalshi_ws_enabled:
                tasks.append(
                    self._run_loop(
                        stop_event,
                        max_cycles=max_cycles,
                        include_market=True,
                        include_reference=False,
                    )
                )
            tasks.append(
                self._run_loop(
                    stop_event,
                    max_cycles=max_cycles,
                    include_market=False,
                    include_reference=True,
                )
            )
            await asyncio.gather(*tasks)
            return

        await self._run_loop(
            stop_event,
            max_cycles=max_cycles,
            include_market=True,
            include_reference=True,
        )

    async def run_market_data(
        self,
        *,
        stop_event: threading.Event,
        max_cycles: int | None = None,
    ) -> None:
        self.status.worker_role = "market-data"
        self.brti_status.connection_state = "disabled"
        if not self.config.kalshi_ws_enabled:
            self.status.connection_state = "disabled"
            self.status.protocol_connection_state = "disabled"
            self.status.warnings = ["kalshi_ws_disabled"]
            self.record_market_heartbeat()
            return
        await self._run_loop(
            stop_event,
            max_cycles=max_cycles,
            include_market=True,
            include_reference=False,
        )

    async def run_reference_brti(
        self,
        *,
        stop_event: threading.Event,
        max_cycles: int | None = None,
    ) -> None:
        self.status.connection_state = "disabled"
        self.status.protocol_connection_state = "disabled"
        self.brti_status.connection_state = "disabled"
        if not self._reference_collection_enabled():
            self.brti_status.warnings = ["kalshi_cfbenchmarks_disabled"]
            self.record_reference_heartbeat()
            return
        await self._run_loop(
            stop_event,
            max_cycles=max_cycles,
            include_market=False,
            include_reference=True,
        )

    async def _run_loop(
        self,
        stop_event: threading.Event,
        *,
        max_cycles: int | None,
        include_market: bool,
        include_reference: bool,
    ) -> None:
        cycles = 0
        while not stop_event.is_set():
            cycles += 1
            await self._run_cycle(
                stop_event,
                include_market=include_market,
                include_reference=include_reference,
            )
            if max_cycles is not None and cycles >= max_cycles:
                return
            reconnect_count = (
                self.status.reconnect_count
                if include_market
                else self.brti_status.reconnect_count
            )
            await _sleep_or_stop(
                stop_event,
                min(
                    self.config.kalshi_ws_reconnect_seconds * max(1, reconnect_count),
                    self.config.kalshi_ws_max_reconnect_seconds,
                ),
            )

    async def _run_cycle(
        self,
        stop_event: threading.Event,
        *,
        include_market: bool,
        include_reference: bool,
    ) -> None:
        diagnostic = build_kalshi_config_diagnostic(self.config)
        market_ws_enabled = include_market and self.config.kalshi_ws_enabled
        brti_enabled = include_reference and self._reference_collection_enabled()

        if include_market:
            self.status.configured = diagnostic.configured
            self.status.signer_ready = diagnostic.signer_ready
            self.status.blockers = []
            self.status.warnings = []
        if include_reference:
            self.brti_status.configured = diagnostic.configured
            self.brti_status.signer_ready = diagnostic.signer_ready
            self.brti_status.blockers = []
            self.brti_status.warnings = []

        if not diagnostic.signer_ready:
            if market_ws_enabled:
                self.status.connection_state = "not_configured"
                self.status.blockers = ["kalshi_ws_credentials_not_configured_or_not_parseable"]
            if brti_enabled:
                self.brti_status.connection_state = "not_configured"
                self.brti_status.blockers = [
                    "kalshi_cfbenchmarks_credentials_not_configured_or_not_parseable"
                ]
            self.record_heartbeat(
                include_market=include_market,
                include_reference=include_reference,
            )
            return

        if self.session_factory is None:
            if market_ws_enabled:
                self.status.connection_state = "not_configured"
                self.status.blockers = ["database_not_configured_for_ws_persistence"]
            if brti_enabled:
                self.brti_status.connection_state = "not_configured"
                self.brti_status.blockers = [
                    "database_not_configured_for_reference_persistence"
                ]
            self.record_heartbeat()
            return

        market_ticker: str | None = None
        market_close_time: datetime | None = None
        market_subscription_enabled = False
        if market_ws_enabled:
            try:
                with self.session_factory() as session:
                    resolver_result = self.resolver(
                        config=self.config,
                        client=_rest_client(self.config),
                        session=session,
                        now=self.now(),
                    )
            except KalshiError as exc:
                self._set_error("resolver_error", exc)
                self.record_heartbeat(
                    include_market=include_market,
                    include_reference=include_reference,
                )
                if not brti_enabled:
                    return
            except SQLAlchemyError as exc:
                self._set_error("resolver_database_error", exc)
                self.status.blockers = ["market_resolver_database_error"]
                self.record_heartbeat(
                    include_market=include_market,
                    include_reference=include_reference,
                )
                if not brti_enabled:
                    return
            else:
                if resolver_result.state is not ResolverState.RESOLVED_OBSERVER_ONLY:
                    self.status.connection_state = resolver_result.state.value
                    self.status.blockers = resolver_result.blockers or [
                        resolver_result.state.value
                    ]
                    self.status.warnings = resolver_result.warnings
                    self.status.active_market_ticker = (
                        resolver_result.market.market_ticker
                        if resolver_result.market
                        else None
                    )
                    self.record_heartbeat(
                        include_market=include_market,
                        include_reference=include_reference,
                    )
                    if not brti_enabled:
                        return
                elif resolver_result.market is None:
                    self.status.connection_state = "no_active_market"
                    self.status.blockers = ["no_active_market"]
                    self.record_heartbeat(
                        include_market=include_market,
                        include_reference=include_reference,
                    )
                    if not brti_enabled:
                        return
                else:
                    previous_market_ticker = self.status.active_market_ticker
                    market_ticker = resolver_result.market.market_ticker
                    market_close_time = resolver_result.market.close_time
                    rollover = (
                        previous_market_ticker is not None
                        and previous_market_ticker != market_ticker
                    )
                    self.status.active_market_ticker = market_ticker
                    self.status.last_market_data_message_at = None
                    self.status.market_data_message_age_ms = None
                    self.status.market_data_quiet = False
                    self.status.market_data_quiet_age_ms = None
                    self.status.orderbook_initialized = False
                    self.status.orderbook_sequence_number = None
                    self.status.orderbook_liveness_status = "waiting_for_snapshot"
                    self.status.orderbook_liveness_reason = (
                        "kalshi_orderbook_uninitialized"
                    )
                    self.status.market_feed_snapshot_state = "missing"
                    self.status.market_feed_sequence_state = "unknown"
                    self.status.orderbook_snapshot_source = "blocked"
                    self.status.orderbook_recovery_action = "none"
                    if rollover:
                        checked_at = self.now()
                        self.status.connection_state = "market_roll_resubscribe_pending"
                        self.status.orderbook_recovery_action = "resubscribe"
                        self._add_warning("market_roll_resubscribe_pending")
                        self._start_market_recovery(
                            reason="market_roll_resubscribe_pending",
                            action="resubscribe",
                            result="requested",
                            checked_at=checked_at,
                            count_rollover=True,
                        )
                    market_subscription_enabled = True
        elif include_market:
            self.status.connection_state = "disabled"
            self.status.warnings = ["kalshi_ws_disabled"]

        market_reconnect_attempt_active = bool(
            market_subscription_enabled
            and (self.status.reconnect_count > 0 or self.status.reconnect_reason)
        )
        self._market_reconnect_completion_pending_reason = (
            self.status.reconnect_reason if market_reconnect_attempt_active else None
        )

        try:
            headers = create_websocket_auth_headers(
                endpoint=self.config.kalshi_ws_base_url,
                api_key_id=self.config.kalshi_api_key_id,
                private_key_pem=self.config.kalshi_private_key,
            )
            if market_subscription_enabled:
                self._start_market_connection()
                if market_reconnect_attempt_active:
                    self._record_protocol_event(
                        "reconnect_started",
                        event_subtype=self.status.reconnect_reason,
                        recovery_action="reconnect",
                        recovery_result="started",
                    )
            websocket = await self.websocket_factory(
                self.config.kalshi_ws_base_url,
                headers,
                self.config.kalshi_ws_connect_timeout_seconds,
                self.config.kalshi_ws_heartbeat_timeout_seconds,
            )
            try:
                if market_subscription_enabled:
                    await self._start_market_db_writer()
                    self.status.connection_state = "connected"
                    self.status.protocol_connection_state = "open"
                    self.status.transport_alive = True
                    self.status.transport_last_pong_at = self.now()
                    self.status.transport_liveness_reason = None
                    self._record_protocol_event("websocket_open")
                if brti_enabled:
                    self.brti_status.connection_state = "connected"
                    self.brti_status.last_connected_at = self.now()
                    self.brti_status.recovery_state = "connecting"
                if include_market:
                    self.status.last_connected_at = self.now()
                self.record_heartbeat(
                    include_market=include_market,
                    include_reference=include_reference,
                )

                await self._subscribe(
                    websocket,
                    market_ticker,
                    include_reference=brti_enabled,
                )
                if market_subscription_enabled:
                    self.status.connection_state = "subscribed"
                if brti_enabled:
                    self.brti_status.connection_state = "subscribed"
                    self.brti_status.last_successful_subscribe_at = self.now()
                    self.brti_status.recovery_state = "waiting_for_fresh_tick"
                self.record_heartbeat(
                    include_market=include_market,
                    include_reference=include_reference,
                )

                read_result = await self._read_messages(
                    websocket,
                    market_ticker,
                    market_close_time,
                    stop_event,
                    include_reference=brti_enabled,
                )
                if include_market:
                    if _market_reconnect_result(read_result):
                        self._market_reconnect_completion_pending_reason = None
                        self.status.connection_state = "reconnect_pending"
                        self.status.reconnect_reason = read_result
                        self.status.reconnect_count += 1
                        self._record_protocol_event(
                            "reconnect_scheduled",
                            event_subtype=read_result,
                            recovery_action="reconnect",
                            recovery_result="scheduled",
                        )
                        self.record_heartbeat(
                            include_market=True,
                            include_reference=False,
                        )
                    else:
                        self._complete_market_reconnect_if_pending()
                        self.status.reconnect_count = 0
                        self.status.reconnect_reason = None
                if include_reference and not _reference_reconnect_result(read_result):
                    self.brti_status.reconnect_count = 0
            finally:
                if market_subscription_enabled:
                    self._record_protocol_event(
                        "websocket_close",
                        close_code=_websocket_close_code(websocket),
                        close_reason=_websocket_close_reason(websocket),
                    )
                    self.status.protocol_connection_state = "closed"
                    self.status.close_code = _websocket_close_code(websocket)
                    self.status.close_reason = _websocket_close_reason(websocket)
                await _close_websocket(websocket)
                if market_subscription_enabled:
                    await self._stop_market_db_writer()
        except Exception as exc:
            self._market_reconnect_completion_pending_reason = None
            if include_market:
                self.status.reconnect_count += 1
                self.status.reconnect_reason = exc.__class__.__name__
            if market_ws_enabled:
                self._set_error(exc.__class__.__name__, exc)
                self._record_protocol_event(
                    "websocket_error",
                    exception_type=exc.__class__.__name__,
                    exception_message=str(exc),
                )
                self._record_protocol_event(
                    "reconnect_failed",
                    event_subtype=exc.__class__.__name__,
                    recovery_action="reconnect",
                    recovery_result="failed",
                )
            if brti_enabled:
                self.brti_status.reconnect_count += 1
                self._set_reference_error(exc.__class__.__name__, exc)
            self.record_heartbeat(
                include_market=include_market,
                include_reference=include_reference,
            )

    async def _subscribe(
        self,
        websocket: Any,
        market_ticker: str | None,
        *,
        include_reference: bool,
    ) -> None:
        request_id = 1
        subscribed_channels: list[str] = []
        subscription_request_ids: dict[str, int] = {}

        if market_ticker is not None and self.config.kalshi_ws_subscribe_orderbook:
            message = build_subscribe_message(
                request_id=request_id,
                channels=["orderbook_delta"],
                market_ticker=market_ticker,
                use_yes_price=True,
            )
            await websocket.send(json.dumps(message))
            self._record_protocol_event(
                "subscribe_sent",
                channel="orderbook_delta",
                command_id=request_id,
                command_type="subscribe",
                payload_summary_json=_payload_summary(message),
            )
            subscribed_channels.append("orderbook_delta")
            subscription_request_ids["orderbook_delta"] = request_id
            request_id += 1

        secondary_channels: list[str] = []
        if market_ticker is not None and self.config.kalshi_ws_subscribe_ticker:
            secondary_channels.append("ticker")
        if market_ticker is not None and self.config.kalshi_ws_subscribe_trades:
            secondary_channels.append("trade")

        if secondary_channels:
            message = build_subscribe_message(
                request_id=request_id,
                channels=secondary_channels,
                market_ticker=market_ticker,
            )
            await websocket.send(json.dumps(message))
            for channel in secondary_channels:
                self._record_protocol_event(
                    "subscribe_sent",
                    channel=channel,
                    command_id=request_id,
                    command_type="subscribe",
                    payload_summary_json=_payload_summary(message),
                )
            subscribed_channels.extend(secondary_channels)
            for channel in secondary_channels:
                subscription_request_ids[channel] = request_id
            request_id += 1

        if include_reference:
            message = build_cfbenchmarks_subscribe_message(
                request_id=request_id,
                index_ids=list(self.config.kalshi_cfbenchmarks_index_ids),
            )
            await websocket.send(json.dumps(message))
            self.brti_status.subscription_request_id = request_id
            self.brti_status.subscribed_channels = ["cfbenchmarks_value"]

        if market_ticker is not None:
            self.status.subscribed_channels = subscribed_channels
            self.status.subscription_ids = {}
            self.status.subscription_request_ids = subscription_request_ids
            self.status.subscription_reconciled = False
            self.status.orderbook_sid_confirmed = False
            self.status.ticker_sid_confirmed = False
            self.status.trade_sid_confirmed = False
            self._last_market_subscribe_requested_at = self.now()
            self._start_market_recovery(
                reason="market_subscription_startup",
                action="subscribe",
                result="requested",
                checked_at=self._last_market_subscribe_requested_at,
            )
            if subscribed_channels:
                await self._request_list_subscriptions(websocket)

    async def _request_list_subscriptions(self, websocket: Any) -> None:
        request_id = self._next_request_id
        self._next_request_id += 1
        message = build_list_subscriptions_message(request_id=request_id)
        await websocket.send(json.dumps(message))
        self.status.list_subscriptions_request_id = request_id
        self.status.last_list_subscriptions_result = "requested"
        self._record_protocol_event(
            "list_subscriptions_sent",
            command_id=request_id,
            command_type="list_subscriptions",
            payload_summary_json=_payload_summary(message),
        )

    async def _read_messages(
        self,
        websocket: Any,
        market_ticker: str | None,
        market_close_time: datetime | None,
        stop_event: threading.Event,
        include_reference: bool,
    ) -> str | None:
        orderbook = OrderbookState(market_ticker=market_ticker or "")
        message_iterator = websocket.__aiter__()

        while not stop_event.is_set():
            if _market_window_closed(self.now(), market_close_time):
                self.status.connection_state = "market_roll_reresolve"
                self._add_warning("active_market_window_closed")
                self._add_warning("market_roll_reresolve")
                self.status.orderbook_liveness_status = "resync_pending"
                self.status.orderbook_liveness_reason = "snapshot_resync_pending"
                self.status.market_feed_snapshot_state = "resync_pending"
                self.status.orderbook_snapshot_source = "blocked"
                self.status.orderbook_recovery_action = "resubscribe"
                self._start_market_recovery(
                    reason="market_roll_snapshot_pending",
                    action="resubscribe",
                    result="requested",
                    checked_at=self.now(),
                    count_rollover=True,
                )
                self.record_heartbeat(
                    include_market=market_ticker is not None,
                    include_reference=include_reference,
                )
                return "market_roll_reresolve"

            try:
                raw_message = await _next_websocket_message(
                    message_iterator,
                    _minimum_timeout_seconds(
                        _seconds_until_market_close(self.now(), market_close_time),
                        self._seconds_until_market_transport_check(self.now())
                        if market_ticker is not None
                        else None,
                        self._seconds_until_reference_check(self.now())
                        if include_reference
                        else None,
                    ),
                )
            except StopAsyncIteration:
                return None
            except TimeoutError:
                checked_at = self.now()
                if market_ticker is not None:
                    transport_alive = await self._prove_market_transport_alive(
                        websocket,
                        checked_at,
                    )
                    if transport_alive:
                        await self._request_orderbook_snapshot_if_due(
                            websocket,
                            market_ticker=market_ticker,
                            orderbook=orderbook,
                            checked_at=checked_at,
                            reason="market_data_quiet",
                        )
                        reconnect_reason = self._market_reconnect_needed_reason()
                        self.record_heartbeat(
                            include_market=True,
                            include_reference=False,
                        )
                        if reconnect_reason is not None:
                            return reconnect_reason
                    else:
                        self._start_market_recovery(
                            reason="kalshi_orderbook_transport_stale",
                            action="reconnect",
                            result="failed",
                            checked_at=checked_at,
                            count_transport=True,
                        )
                        self.record_heartbeat(
                            include_market=True,
                            include_reference=False,
                        )
                        return "kalshi_orderbook_transport_stale"

                if (
                    include_reference
                    and self._reference_stale_reason(checked_at) is not None
                ):
                    reconnect_reason = self._handle_reference_stale_if_due(checked_at)
                    self.record_heartbeat(
                        include_market=market_ticker is not None,
                        include_reference=True,
                    )
                    if reconnect_reason is not None:
                        return reconnect_reason
                    continue
                if market_ticker is not None:
                    continue
                self.status.connection_state = "market_roll_reresolve"
                self._add_warning("active_market_window_closed")
                self.record_heartbeat(
                    include_market=market_ticker is not None,
                    include_reference=include_reference,
                )
                return "market_roll_reresolve"

            received_at = self.now()
            parsed_json = _json_or_none(raw_message)
            if parsed_json is None:
                self._add_warning("invalid_websocket_json")
                self.record_heartbeat(
                    force=False,
                    include_market=market_ticker is not None,
                    include_reference=include_reference,
                )
                if include_reference:
                    reconnect_reason = self._handle_reference_stale_if_due(received_at)
                    if reconnect_reason is not None:
                        self.record_heartbeat(
                            include_market=market_ticker is not None,
                            include_reference=True,
                        )
                        return reconnect_reason
                continue

            if include_reference and is_cfbenchmarks_value_payload(parsed_json):
                reference_message = parse_cfbenchmarks_value_message(
                    parsed_json,
                    received_at=received_at,
                    allowed_index_ids=self.config.kalshi_cfbenchmarks_index_ids,
                    persist_raw_payload=self.config.kalshi_cfbenchmarks_persist_raw_payload,
                )
                self.brti_status.last_message_at = received_at
                self._handle_reference_message(reference_message)
                self.record_heartbeat(
                    force=(
                        self._consume_force_next_heartbeat()
                        or self._reference_liveness_heartbeat_due(received_at)
                    ),
                    include_market=False,
                    include_reference=True,
                )
                reconnect_reason = self._handle_reference_stale_if_due(received_at)
                if reconnect_reason is not None:
                    self.record_heartbeat(include_market=False, include_reference=True)
                    return reconnect_reason
                continue

            if include_reference and self._handle_reference_control_payload(
                parsed_json,
                received_at=received_at,
                market_ticker=market_ticker,
            ):
                self.record_heartbeat(
                    force=(
                        self._consume_force_next_heartbeat()
                        or self._reference_liveness_heartbeat_due(received_at)
                    ),
                    include_market=False,
                    include_reference=True,
                )
                reconnect_reason = self._handle_reference_stale_if_due(received_at)
                if reconnect_reason is not None:
                    self.record_heartbeat(include_market=False, include_reference=True)
                    return reconnect_reason
                continue

            if market_ticker is None:
                self.record_heartbeat(
                    force=False,
                    include_market=False,
                    include_reference=include_reference,
                )
                if include_reference:
                    reconnect_reason = self._handle_reference_stale_if_due(received_at)
                    if reconnect_reason is not None:
                        self.record_heartbeat(
                            include_market=False,
                            include_reference=True,
                        )
                        return reconnect_reason
                continue

            message = parse_ws_payload(
                parsed_json,
                target_market_ticker=market_ticker,
                received_at=received_at,
            )
            self.status.last_message_at = received_at
            self._record_market_protocol_message(message, parsed_json)
            resubscribe_reason = await self._handle_message(
                websocket,
                message,
                orderbook,
                received_at,
            )
            if resubscribe_reason is not None:
                self.status.connection_state = "resubscribe_pending"
                self._add_warning("kalshi_ws_resubscribe_requested")
                self.record_heartbeat(include_market=True, include_reference=False)
                return resubscribe_reason
            if _market_reconnect_confirmation_message(message):
                self._complete_market_reconnect_if_pending()
            if message.kind in {"ticker", "trade"} and orderbook.initialized:
                await self._request_orderbook_snapshot_if_due(
                    websocket,
                    market_ticker=market_ticker,
                    orderbook=orderbook,
                    checked_at=received_at,
                    reason="market_data_quiet",
                )
                reconnect_reason = self._market_reconnect_needed_reason()
                if reconnect_reason is not None:
                    self.status.connection_state = "resubscribe_pending"
                    self._add_warning("kalshi_ws_resubscribe_requested")
                    self.record_heartbeat(include_market=True, include_reference=False)
                    return reconnect_reason
            self.record_heartbeat(
                force=(
                    self._consume_force_next_heartbeat()
                    or self._market_liveness_heartbeat_due(received_at)
                ),
                include_market=True,
                include_reference=False,
            )
            if include_reference:
                reconnect_reason = self._handle_reference_stale_if_due(received_at)
                if reconnect_reason is not None:
                    self.record_heartbeat(include_market=False, include_reference=True)
                    return reconnect_reason

        return None

    def _complete_market_reconnect_if_pending(self) -> bool:
        reason = self._market_reconnect_completion_pending_reason
        if reason is None:
            return False
        self._record_protocol_event(
            "reconnect_completed",
            event_subtype=reason,
            recovery_result="completed",
        )
        self.status.reconnect_count = 0
        self.status.reconnect_reason = None
        self._market_reconnect_completion_pending_reason = None
        self._force_next_heartbeat = True
        return True

    def _handle_reference_message(self, message: ParsedReferenceMessage) -> None:
        if message.kind == "ignored":
            return

        if message.kind == "invalid" or message.tick is None:
            self._add_reference_warning(message.reason or "invalid_cfbenchmarks_message")
            return

        self._set_reference_subscription_id(message.tick.subscription_id)
        if message.warning:
            self._add_reference_warning(message.warning)
        if self._persist_reference_tick(message.tick):
            self._clear_reference_warnings(
                "brti_persistence_failed",
                "brti_duplicate_or_out_of_order_source_ts",
            )
            self._clear_reference_error()
            self._force_next_heartbeat = True

    async def _handle_message(
        self,
        websocket: Any,
        message: ParsedWsMessage,
        orderbook: OrderbookState,
        received_at: datetime,
    ) -> str | None:
        if message.kind == "control":
            self._handle_market_control_message(message)
            return None

        if message.kind == "ticker":
            self._mark_market_data_message(received_at)
            self.status.last_ticker_at = received_at
            return None

        if message.kind == "orderbook_snapshot":
            self._mark_market_data_message(received_at)
            previous_liveness_status = self.status.orderbook_liveness_status
            previous_liveness_reason = self.status.orderbook_liveness_reason
            orderbook.apply_snapshot(message)
            self.status.in_flight_snapshot_request = False
            self.status.snapshot_request_id = None
            self.status.snapshot_request_started_at = None
            self.status.snapshot_request_age_ms = None
            self._sync_orderbook_status(orderbook, status="live", reason=None)
            self._record_protocol_event(
                "orderbook_snapshot_received",
                channel="orderbook_delta",
                sid=message.sid,
                seq=message.seq,
                raw_payload_hash=message.raw_payload_hash,
                payload_summary_json=_payload_summary(message.raw_payload),
            )
            self.status.orderbook_snapshot_source = (
                "resynced_snapshot"
                if previous_liveness_status == "resync_pending"
                else "fresh_update"
            )
            self.status.orderbook_recovery_action = "none"
            snapshot = orderbook.snapshot_input(
                received_at=received_at,
                sequence_number=message.seq,
                raw_payload_hash=message.raw_payload_hash,
                raw_payload=message.raw_payload,
            )
            if not self._persist_orderbook(
                snapshot,
                queued_recovery_result="snapshot_initialized",
                queued_clear_recovery_warnings=True,
            ):
                return None
            self.status.market_snapshot_resync_last_result = "snapshot_initialized"
            self._finish_market_recovery(
                result="snapshot_initialized",
                checked_at=received_at,
            )
            if self._db_writer_queue is None:
                self.status.last_orderbook_at = received_at
            liveness_recovered = (
                previous_liveness_status != "live"
                or previous_liveness_reason is not None
            )
            recovery_metadata_cleared = self._clear_orderbook_recovery_warnings()
            if recovery_metadata_cleared or liveness_recovered:
                self.record_market_heartbeat()
            return None

        if message.kind == "orderbook_delta":
            self._mark_market_data_message(received_at)
            self._record_protocol_event(
                "orderbook_delta_received",
                channel="orderbook_delta",
                sid=message.sid,
                seq=message.seq,
                raw_payload_hash=message.raw_payload_hash,
                payload_summary_json=_payload_summary(message.raw_payload),
            )
            if not orderbook.initialized:
                self._sync_orderbook_status(
                    orderbook,
                    status="blocked",
                    reason="kalshi_orderbook_uninitialized",
                )
                if not any(
                    warning
                    in {
                        "orderbook_sequence_gap_reset",
                        "orderbook_reset_after_buffer_overflow",
                        "kalshi_websocket_buffer_overflow",
                    }
                    for warning in self.status.warnings
                ):
                    self._add_warning("orderbook_delta_before_snapshot")
                requested = await self._request_orderbook_snapshot(
                    websocket,
                    market_ticker=message.market_ticker or orderbook.market_ticker,
                    checked_at=received_at,
                    reason="orderbook_delta_before_snapshot",
                )
                if requested:
                    return None
                return self._market_reconnect_needed_reason()
            if orderbook.has_sequence_gap(message.seq):
                orderbook.reset()
                self._sync_orderbook_status(
                    orderbook,
                    status="resync_pending",
                    reason="kalshi_orderbook_sequence_gap_or_reset",
                )
                self._add_warning("orderbook_sequence_gap_reset")
                requested = await self._request_orderbook_snapshot(
                    websocket,
                    market_ticker=message.market_ticker or orderbook.market_ticker,
                    checked_at=received_at,
                    reason="sequence_gap",
                )
                return None if requested else "kalshi_orderbook_sequence_gap_or_reset"
            orderbook.apply_delta(message)
            self._sync_orderbook_status(orderbook, status="live", reason=None)
            self.status.orderbook_snapshot_source = "fresh_update"
            self.status.orderbook_recovery_action = "none"
            snapshot = orderbook.snapshot_input(
                received_at=received_at,
                sequence_number=message.seq,
                raw_payload_hash=message.raw_payload_hash,
                raw_payload=message.raw_payload,
            )
            if not self._persist_orderbook(
                snapshot,
                queued_clear_delta_warnings=True,
            ):
                return None
            if self._db_writer_queue is None:
                self.status.last_orderbook_at = received_at
            warnings_cleared = self._clear_orderbook_delta_warnings()
            if warnings_cleared:
                self.record_market_heartbeat()
            return None

        if message.kind == "trade" and message.trade is not None:
            self._mark_market_data_message(received_at)
            self._record_protocol_event(
                "trade_received",
                channel="trade",
                sid=message.sid,
                seq=message.seq,
                raw_payload_hash=message.raw_payload_hash,
                payload_summary_json=_payload_summary(message.raw_payload),
            )
            if not self._persist_trade(message.trade):
                return None
            if self._db_writer_queue is None:
                self.status.last_trade_at = received_at
            warnings_cleared = self._clear_warning_prefixes("invalid_trade_")
            warnings_cleared = (
                self._clear_warnings("invalid_trade_price_or_size") or warnings_cleared
            )
            if message.warning:
                self._add_warning(message.warning)
            if warnings_cleared:
                self.record_market_heartbeat()
            return None

        if message.kind == "invalid":
            if message.reason == "kalshi_websocket_buffer_overflow":
                orderbook.reset()
                self._sync_orderbook_status(
                    orderbook,
                    status="resync_pending",
                    reason="kalshi_orderbook_sequence_gap_or_reset",
                )
                self._add_warning("orderbook_reset_after_buffer_overflow")
                self._add_warning(message.reason)
                requested = await self._request_orderbook_snapshot(
                    websocket,
                    market_ticker=orderbook.market_ticker,
                    checked_at=received_at,
                    reason="buffer_overflow",
                )
                return None if requested else "orderbook_reset_after_buffer_overflow"
            if message.reason == "kalshi_websocket_error":
                raw_payload = (
                    message.raw_payload if isinstance(message.raw_payload, dict) else {}
                )
                request_id = _int_or_none(raw_payload.get("id"))
                error_message = _websocket_error_message(raw_payload)
                if _already_subscribed_error(error_message):
                    self._record_protocol_event(
                        "already_subscribed_received",
                        command_id=request_id,
                        sid=message.sid,
                        seq=message.seq,
                        raw_code=_websocket_error_code(raw_payload),
                        raw_message=error_message,
                        raw_payload_hash=message.raw_payload_hash,
                        payload_summary_json=_payload_summary(raw_payload),
                    )
                    self._add_warning("kalshi_ws_already_subscribed_pending_reconcile")
                    try:
                        await self._request_list_subscriptions(websocket)
                    except Exception as exc:
                        self._set_error("list_subscriptions_failed", exc)
                        self._add_warning("kalshi_ws_already_subscribed_unconfirmed")
                    self._force_next_heartbeat = True
                    return None
                self._record_protocol_event(
                    "websocket_error",
                    command_id=request_id,
                    sid=message.sid,
                    seq=message.seq,
                    raw_code=_websocket_error_code(raw_payload),
                    raw_message=error_message,
                    raw_payload_hash=message.raw_payload_hash,
                    payload_summary_json=_payload_summary(raw_payload),
                )
                if (
                    request_id is not None
                    and request_id in self.status.subscription_request_ids.values()
                ):
                    self.status.orderbook_recovery_action = "reconnect"
                    self._start_market_recovery(
                        reason="kalshi_orderbook_subscription_recovery_failed",
                        action="reconnect",
                        result="failed",
                        checked_at=received_at,
                        count_subscription=True,
                    )
                    self._mark_unrecovered_market_blocker(
                        "kalshi_orderbook_subscription_recovery_failed",
                        checked_at=received_at,
                    )
                    self._finish_market_recovery(
                        result="failed",
                        checked_at=received_at,
                    )
                    self._force_next_heartbeat = True
                    return "kalshi_orderbook_subscription_recovery_failed"
                self._force_next_heartbeat = True
            if self._record_parse_diagnostic(message):
                self._force_next_heartbeat = True
            self._add_warning(message.reason or "invalid_websocket_message")
            return None

        return None

    def _handle_reference_control_payload(
        self,
        payload: Any,
        *,
        received_at: datetime,
        market_ticker: str | None,
    ) -> bool:
        if not self._reference_collection_enabled() or not isinstance(payload, dict):
            return False

        message_type = _safe_text(payload.get("type"))
        if message_type not in {"subscribed", "ok", "unsubscribed", "error"}:
            return False

        sid = _int_or_none(payload.get("sid"))
        request_id = _int_or_none(payload.get("id"))
        msg_sid = _message_sid(payload)
        matched = False
        request_subscription_id = self.brti_status.subscription_request_id
        if request_subscription_id is not None and request_id == request_subscription_id:
            matched = True
        subscription_id = self.brti_status.subscription_id
        if subscription_id is not None and sid == subscription_id:
            matched = True
        if subscription_id is not None and msg_sid == subscription_id:
            matched = True
        if not matched and market_ticker is None and request_id is None and sid is None:
            matched = True
        if not matched:
            return False

        if msg_sid is not None:
            self.brti_status.subscription_id = msg_sid
        elif sid is not None and request_id is None:
            self.brti_status.subscription_id = sid

        self.brti_status.last_message_at = received_at
        if message_type != "error":
            if message_type in {"subscribed", "ok"}:
                self.brti_status.last_subscription_ack_at = received_at
                self.brti_status.recovery_state = "waiting_for_fresh_tick"
            return True

        self._set_reference_error(
            "kalshi_cfbenchmarks_subscription_error",
            RuntimeError(_websocket_error_message(payload)),
        )
        self._add_reference_warning("kalshi_cfbenchmarks_subscription_error")
        self._add_reference_blocker("kalshi_cfbenchmarks_subscription_error")
        self._force_next_heartbeat = True
        return True

    def _sync_orderbook_status(
        self,
        orderbook: OrderbookState,
        *,
        status: str,
        reason: str | None,
    ) -> None:
        self.status.orderbook_initialized = orderbook.initialized
        self.status.orderbook_sequence_number = orderbook.last_sequence_number
        self.status.orderbook_liveness_status = status
        self.status.orderbook_liveness_reason = reason
        self._refresh_market_feed_state(self.now())

    def _clear_orderbook_recovery_warnings(self) -> bool:
        warnings_cleared = self._clear_warning_prefixes("invalid_orderbook_snapshot_")
        warnings_cleared = (
            self._clear_warnings(
                "orderbook_delta_before_snapshot",
                "orderbook_sequence_gap_reset",
                "orderbook_reset_after_buffer_overflow",
                "kalshi_websocket_buffer_overflow",
                "kalshi_ws_resubscribe_requested",
                "snapshot_resync_pending",
                "market_roll_reresolve",
                "market_roll_resubscribe_pending",
                "kalshi_orderbook_subscription_ack_timeout",
                "kalshi_orderbook_subscription_recovery_failed",
                "orderbook_snapshot_resync_unavailable",
                "kalshi_orderbook_snapshot_resync_failed",
            )
            or warnings_cleared
        )
        blockers_cleared = self._clear_blockers(
            "kalshi_orderbook_subscription_ack_timeout",
            "kalshi_orderbook_subscription_recovery_failed",
            "kalshi_orderbook_snapshot_resync_failed",
        )
        return warnings_cleared or blockers_cleared

    def _clear_orderbook_delta_warnings(self) -> bool:
        warnings_cleared = self._clear_warning_prefixes("invalid_orderbook_delta_")
        return (
            self._clear_warnings("orderbook_delta_before_snapshot") or warnings_cleared
        )

    def _start_market_recovery(
        self,
        *,
        reason: str,
        action: str,
        result: str,
        checked_at: datetime,
        count_subscription: bool = False,
        count_snapshot: bool = False,
        count_rollover: bool = False,
        count_transport: bool = False,
    ) -> None:
        changed = (
            self.status.market_subscription_recovery_last_reason != reason
            or self.status.market_subscription_recovery_last_action != action
            or self.status.market_subscription_recovery_last_result != result
        )
        if not self.status.market_recovery_attempt_in_progress:
            self.status.market_recovery_attempt_started_at = checked_at
        self.status.market_recovery_attempt_in_progress = True
        self.status.market_subscription_recovery_last_reason = reason
        self.status.market_subscription_recovery_last_action = action
        self.status.market_subscription_recovery_last_result = result
        self.status.market_subscription_recovery_last_at = checked_at
        if changed:
            if count_subscription:
                self.status.market_subscription_recovery_count += 1
            if count_snapshot:
                self.status.market_snapshot_resync_count += 1
            if count_rollover:
                self.status.market_rollover_recovery_count += 1
            if count_transport:
                self.status.market_transport_reconnect_count += 1
        self._refresh_market_feed_state(checked_at)
        self._force_next_heartbeat = True

    def _finish_market_recovery(self, *, result: str, checked_at: datetime) -> None:
        self.status.market_subscription_recovery_last_result = result
        self.status.market_subscription_recovery_last_at = checked_at
        self.status.market_recovery_attempt_in_progress = False
        self.status.market_recovery_attempt_started_at = None
        self.status.market_recovery_attempt_age_ms = None
        self._refresh_market_feed_state(checked_at)
        self._force_next_heartbeat = True

    def _mark_unrecovered_market_blocker(
        self,
        reason: str,
        *,
        checked_at: datetime,
    ) -> None:
        self.status.market_unrecovered_blocker_count += 1
        self.status.market_subscription_recovery_last_result = "failed"
        self.status.market_subscription_recovery_last_at = checked_at
        self.status.orderbook_liveness_status = "blocked"
        self.status.orderbook_liveness_reason = reason
        self._add_warning(reason)
        self._add_blocker(reason)
        self._force_next_heartbeat = True
        self._refresh_market_feed_state(checked_at)

    def _market_reconnect_needed_reason(self) -> str | None:
        if self.status.orderbook_recovery_action == "reconnect":
            return (
                self.status.orderbook_liveness_reason
                or self.status.market_subscription_recovery_last_reason
                or "kalshi_orderbook_subscription_recovery_failed"
            )
        if self.status.orderbook_liveness_status == "blocked" and (
            self.status.orderbook_liveness_reason
            in {
                "kalshi_orderbook_snapshot_resync_failed",
                "kalshi_orderbook_subscription_ack_timeout",
                "kalshi_orderbook_subscription_recovery_failed",
            }
        ):
            return self.status.orderbook_liveness_reason
        return None

    def _handle_market_control_message(self, message: ParsedWsMessage) -> None:
        payload = message.raw_payload if isinstance(message.raw_payload, dict) else {}
        request_id = _int_or_none(payload.get("id"))
        subscription_id = message.sid or _message_sid(payload)
        message_type = _safe_text(payload.get("type")) or message.control_type
        if request_id is not None and request_id == self.status.list_subscriptions_request_id:
            self.status.last_list_subscriptions_at = message.source_ts or self.now()
            self.status.last_list_subscriptions_result = message_type or "ok"
            self.status.list_subscriptions_request_id = None
            self._reconcile_list_subscriptions(payload)
            self.status.subscription_reconciled = self._subscriptions_confirmed()
            if self.status.subscription_reconciled:
                self._clear_warnings(
                    "kalshi_ws_already_subscribed_pending_reconcile",
                    "kalshi_ws_already_subscribed_unconfirmed",
                )
            self._record_protocol_event(
                "list_subscriptions_ok",
                command_id=request_id,
                command_type="list_subscriptions",
                event_subtype=self.status.last_list_subscriptions_result,
                subscription_state_after=self.status.market_feed_subscription_state,
                payload_summary_json=_payload_summary(payload),
            )
            return

        if request_id is None or subscription_id is None:
            return

        pending_channels = [
            channel
            for channel, existing_id in self.status.subscription_request_ids.items()
            if existing_id == request_id
        ]
        acknowledged_channels = set(_message_channels(payload))
        if acknowledged_channels:
            channels_to_ack = [
                channel
                for channel in pending_channels
                if channel in acknowledged_channels
            ]
        elif len(pending_channels) == 1:
            channels_to_ack = pending_channels
        else:
            channels_to_ack = []

        for channel in channels_to_ack:
            if channel in self.status.subscription_request_ids:
                self.status.subscription_ids[channel] = subscription_id
                del self.status.subscription_request_ids[channel]
                self._refresh_sid_confirmations()
                self._record_protocol_event(
                    "subscribed_received",
                    channel=channel,
                    command_id=request_id,
                    command_type="subscribe",
                    sid=subscription_id,
                    event_subtype=message_type,
                    subscription_state_after=self.status.market_feed_subscription_state,
                    payload_summary_json=_payload_summary(payload),
                )
                if channel == "orderbook_delta":
                    self._start_market_recovery(
                        reason="market_subscription_ack",
                        action="subscribe",
                        result="sid_confirmed",
                        checked_at=message.source_ts or self.now(),
                    )
                if self.status.list_subscriptions_request_id is None:
                    self.status.subscription_reconciled = self._subscriptions_confirmed()

    def _mark_market_data_message(self, received_at: datetime) -> None:
        self.status.last_market_data_message_at = received_at
        self.status.market_data_message_age_ms = 0
        self.status.market_data_quiet = False
        self.status.market_data_quiet_age_ms = 0
        self._mark_market_transport_alive(received_at)

    def _mark_market_transport_alive(self, checked_at: datetime) -> None:
        self.status.transport_alive = True
        self.status.transport_last_pong_at = checked_at
        self.status.transport_age_ms = 0
        self.status.transport_liveness_reason = None
        self._clear_warnings("kalshi_orderbook_transport_stale")
        self._refresh_market_feed_state(checked_at)

    async def _prove_market_transport_alive(
        self,
        websocket: Any,
        checked_at: datetime,
    ) -> bool:
        ping = getattr(websocket, "ping", None)
        if not callable(ping):
            self.status.transport_alive = False
            self.status.transport_liveness_reason = "websocket_ping_unavailable"
            self.status.market_feed_transport_state = "unknown"
            return False

        self.status.transport_last_ping_at = checked_at
        self._record_protocol_event(
            "client_ping_sent",
            ping_sent_at=checked_at,
        )
        try:
            pong_waiter = ping()
            if isawaitable(pong_waiter):
                pong_waiter = await asyncio.wait_for(
                    pong_waiter,
                    timeout=self.config.kalshi_ws_heartbeat_timeout_seconds,
                )
            if isawaitable(pong_waiter):
                await asyncio.wait_for(
                    pong_waiter,
                    timeout=self.config.kalshi_ws_heartbeat_timeout_seconds,
                )
        except Exception as exc:
            self.status.transport_alive = False
            self.status.transport_liveness_reason = "kalshi_orderbook_transport_stale"
            self._add_warning("kalshi_orderbook_transport_stale")
            self._set_error("market_ws_ping_failed", exc)
            self._record_protocol_event(
                "websocket_error",
                event_subtype="client_ping_failed",
                exception_type=exc.__class__.__name__,
                exception_message=str(exc),
            )
            self._refresh_market_feed_state(checked_at)
            return False

        pong_at = self.now()
        self._mark_market_transport_alive(pong_at)
        self._record_protocol_event(
            "server_pong_received",
            ping_sent_at=checked_at,
            pong_received_at=pong_at,
            round_trip_ms=_age_ms(checked_at, pong_at),
        )
        return True

    async def _request_orderbook_snapshot_if_due(
        self,
        websocket: Any,
        *,
        market_ticker: str,
        orderbook: OrderbookState,
        checked_at: datetime,
        reason: str,
    ) -> bool:
        self._refresh_market_feed_state(checked_at)
        quiet = self.status.market_data_quiet
        book_age_ms = _age_ms(self.status.last_orderbook_at, checked_at)
        approaching_cap = (
            book_age_ms is not None
            and book_age_ms
            >= int(self.config.strategy_kalshi_book_carry_forward_max_age_ms * 0.8)
        )
        needs_snapshot = (
            quiet
            or approaching_cap
            or not orderbook.initialized
            or self.status.orderbook_liveness_status == "resync_pending"
        )
        if not needs_snapshot:
            self.status.orderbook_recovery_action = "none"
            return False
        if self.status.in_flight_snapshot_request:
            if self._snapshot_request_timed_out(checked_at):
                self.status.orderbook_recovery_action = "reconnect"
                self.status.market_snapshot_resync_last_result = "timeout"
                self._mark_unrecovered_market_blocker(
                    "kalshi_orderbook_snapshot_resync_timeout",
                    checked_at=checked_at,
                )
                self._start_market_recovery(
                    reason="kalshi_orderbook_snapshot_resync_timeout",
                    action="reconnect",
                    result="failed",
                    checked_at=checked_at,
                    count_snapshot=True,
                )
                self._record_protocol_event(
                    "reconnect_scheduled",
                    event_subtype="snapshot_timeout",
                    recovery_action="reconnect",
                    recovery_result="scheduled",
                )
                return False
            return False
        if not self._snapshot_resync_due(checked_at):
            return False
        return await self._request_orderbook_snapshot(
            websocket,
            market_ticker=market_ticker,
            checked_at=checked_at,
            reason=reason,
        )

    async def _request_orderbook_snapshot(
        self,
        websocket: Any,
        *,
        market_ticker: str,
        checked_at: datetime,
        reason: str,
    ) -> bool:
        self._refresh_sid_confirmations()
        subscription_id = self.status.subscription_ids.get("orderbook_delta")
        if subscription_id is None:
            if self.status.subscription_request_ids.get("orderbook_delta") is not None:
                self._start_market_recovery(
                    reason="orderbook_sid_pending",
                    action="wait_for_subscription_ack",
                    result="waiting",
                    checked_at=checked_at,
                    count_subscription=True,
                )
                pending_since = (
                    self._last_market_subscribe_requested_at
                    or self.status.last_connected_at
                    or checked_at
                )
                pending_age = (
                    _as_utc(checked_at) - _as_utc(pending_since)
                ).total_seconds()
                if pending_age >= max(
                    1.0,
                    self.config.kalshi_ws_heartbeat_timeout_seconds,
                ):
                    self.status.orderbook_recovery_action = "reconnect"
                    self._mark_unrecovered_market_blocker(
                        "kalshi_orderbook_subscription_ack_timeout",
                        checked_at=checked_at,
                    )
                    self._start_market_recovery(
                        reason="kalshi_orderbook_subscription_ack_timeout",
                        action="reconnect",
                        result="failed",
                        checked_at=checked_at,
                        count_subscription=True,
                    )
                    self.status.market_recovery_attempt_in_progress = False
                    self.status.market_recovery_attempt_started_at = None
                    self.status.market_recovery_attempt_age_ms = None
                    self.status.subscription_request_ids.pop("orderbook_delta", None)
                    return False
                self.status.orderbook_recovery_action = "wait_for_subscription_ack"
                return False
            if "orderbook_delta" not in self.status.subscribed_channels:
                self.status.orderbook_recovery_action = "wait_for_subscription_ack"
                self._start_market_recovery(
                    reason="orderbook_subscription_not_confirmed",
                    action="wait_for_subscription_ack",
                    result="waiting",
                    checked_at=checked_at,
                )
                return False
            self.status.orderbook_recovery_action = "resubscribe"
            self._start_market_recovery(
                reason="orderbook_sid_missing",
                action="resubscribe",
                result="failed",
                checked_at=checked_at,
                count_subscription=True,
            )
            self._mark_unrecovered_market_blocker(
                "kalshi_orderbook_subscription_recovery_failed",
                checked_at=checked_at,
            )
            self._add_warning("orderbook_snapshot_resync_unavailable")
            return False
        if not self.status.orderbook_sid_confirmed:
            self.status.orderbook_recovery_action = "wait_for_subscription_ack"
            self._start_market_recovery(
                reason="orderbook_sid_unconfirmed",
                action="wait_for_subscription_ack",
                result="waiting",
                checked_at=checked_at,
            )
            return False

        request_id = self._next_request_id
        self._next_request_id += 1
        message = build_update_subscription_message(
            request_id=request_id,
            subscription_id=subscription_id,
            market_ticker=market_ticker,
            action="get_snapshot",
        )
        try:
            await websocket.send(json.dumps(message))
        except Exception as exc:
            self.status.orderbook_recovery_action = "reconnect"
            self._start_market_recovery(
                reason="kalshi_orderbook_snapshot_resync_failed",
                action="reconnect",
                result="failed",
                checked_at=checked_at,
                count_snapshot=True,
            )
            self._mark_unrecovered_market_blocker(
                "kalshi_orderbook_snapshot_resync_failed",
                checked_at=checked_at,
            )
            self.status.market_snapshot_resync_last_result = "failed"
            self._finish_market_recovery(
                result="failed",
                checked_at=checked_at,
            )
            self._add_warning("kalshi_orderbook_snapshot_resync_failed")
            self._set_error("orderbook_snapshot_resync_failed", exc)
            return False

        self._last_snapshot_resync_requested_at = checked_at
        self.status.in_flight_snapshot_request = True
        self.status.snapshot_request_id = request_id
        self.status.snapshot_request_started_at = checked_at
        self.status.snapshot_request_age_ms = 0
        background_refresh = (
            reason == "market_data_quiet"
            and self.status.orderbook_initialized
            and self.status.orderbook_liveness_status != "resync_pending"
        )
        self.status.orderbook_recovery_action = "request_snapshot"
        if background_refresh:
            self.status.orderbook_liveness_status = "live"
            self.status.orderbook_liveness_reason = None
            self.status.market_feed_snapshot_state = "initialized"
            self.status.orderbook_snapshot_source = "carried_forward"
        else:
            self.status.orderbook_liveness_status = "resync_pending"
            self.status.orderbook_liveness_reason = "snapshot_resync_pending"
            self.status.market_feed_snapshot_state = "resync_pending"
            self.status.orderbook_snapshot_source = "blocked"
            self._add_warning("snapshot_resync_pending")
        self._start_market_recovery(
            reason=reason,
            action="get_snapshot",
            result="requested",
            checked_at=checked_at,
            count_snapshot=True,
        )
        self._record_protocol_event(
            "get_snapshot_sent",
            channel="orderbook_delta",
            command_id=request_id,
            command_type="update_subscription",
            command_action="get_snapshot",
            sid=subscription_id,
            expected_sid=subscription_id,
            recovery_action="get_snapshot",
            recovery_result="requested",
            payload_summary_json=_payload_summary(message),
        )
        self.status.market_snapshot_resync_last_result = "requested"
        self._force_next_heartbeat = True
        if reason == "market_roll":
            self._add_warning("market_roll_reresolve")
        return True

    def _snapshot_resync_due(self, checked_at: datetime) -> bool:
        if self._last_snapshot_resync_requested_at is None:
            return True
        elapsed = (
            checked_at.astimezone(UTC)
            - self._last_snapshot_resync_requested_at.astimezone(UTC)
        ).total_seconds()
        return elapsed >= self.config.kalshi_ws_snapshot_min_interval_seconds

    def _refresh_market_feed_state(self, checked_at: datetime) -> None:
        self.status.market_recovery_attempt_age_ms = (
            _age_ms(self.status.market_recovery_attempt_started_at, checked_at)
            if self.status.market_recovery_attempt_in_progress
            else None
        )
        self.status.snapshot_request_age_ms = (
            _age_ms(self.status.snapshot_request_started_at, checked_at)
            if self.status.in_flight_snapshot_request
            else None
        )
        self._refresh_sid_confirmations()
        self._refresh_db_writer_metrics(checked_at)
        transport_age_ms = _age_ms(self.status.transport_last_pong_at, checked_at)
        self.status.transport_age_ms = transport_age_ms
        if self.status.transport_last_pong_at is None:
            self.status.market_feed_transport_state = "unknown"
        elif (
            self.status.transport_alive
            and transport_age_ms is not None
            and transport_age_ms
            <= int(self.config.kalshi_ws_heartbeat_timeout_seconds * 1000)
        ):
            self.status.market_feed_transport_state = "healthy"
            self.status.transport_liveness_reason = None
        else:
            self.status.market_feed_transport_state = "stale"
            self.status.transport_alive = False
            self.status.transport_liveness_reason = "kalshi_orderbook_transport_stale"

        if self.status.connection_state == "subscribed" and self._subscriptions_confirmed():
            self.status.market_feed_subscription_state = "subscribed"
        elif self.status.connection_state == "subscribed":
            self.status.market_feed_subscription_state = "unreconciled"
        elif self.status.connection_state == "error":
            self.status.market_feed_subscription_state = "error"
        elif self.status.connection_state in {"disabled", "not_configured"}:
            self.status.market_feed_subscription_state = "unsubscribed"
        else:
            self.status.market_feed_subscription_state = "unknown"

        if not self.status.active_market_ticker:
            self.status.market_feed_active_ticker_state = "missing"
        else:
            self.status.market_feed_active_ticker_state = "match"

        if self.status.orderbook_liveness_status == "resync_pending":
            self.status.market_feed_snapshot_state = "resync_pending"
        elif not self.status.orderbook_initialized:
            self.status.market_feed_snapshot_state = "missing"
        else:
            book_age_ms = _age_ms(self.status.last_orderbook_at, checked_at)
            self.status.market_feed_snapshot_state = (
                "stale_cap_exceeded"
                if book_age_ms is not None
                and book_age_ms > self.config.strategy_kalshi_book_carry_forward_max_age_ms
                else "initialized"
            )

        reasons = self.status.warnings + self.status.blockers
        if any(
            reason
            in {
                "orderbook_sequence_gap_reset",
                "orderbook_reset_after_buffer_overflow",
                "kalshi_websocket_buffer_overflow",
            }
            for reason in reasons
        ):
            self.status.market_feed_sequence_state = "gap"
        elif (
            self.status.orderbook_initialized
            and self.status.orderbook_sequence_number is not None
        ):
            self.status.market_feed_sequence_state = "clean"
        else:
            self.status.market_feed_sequence_state = "unknown"

        last_market_data = _latest_datetime(
            self.status.last_market_data_message_at,
            self.status.last_ticker_at,
            self.status.last_orderbook_at,
            self.status.last_trade_at,
        )
        self.status.last_market_data_message_at = last_market_data
        market_data_age_ms = _age_ms(last_market_data, checked_at)
        self.status.market_data_message_age_ms = market_data_age_ms
        self.status.market_data_quiet = (
            market_data_age_ms is not None
            and market_data_age_ms > self.config.strategy_kalshi_book_stream_max_age_ms
        )
        self.status.market_data_quiet_age_ms = (
            market_data_age_ms if self.status.market_data_quiet else None
        )
        self.status.market_feed_state = self._market_feed_state()

    def _market_feed_state(self) -> str:
        connection_state = self.status.connection_state
        if connection_state in {"disabled", "not_configured", "no_active_market"}:
            return "DISCONNECTED"
        if connection_state in {"connected"}:
            return "CONNECTING"
        if connection_state in {
            "market_roll_reresolve",
            "market_roll_resubscribe_pending",
            "market_roll_snapshot_pending",
        }:
            return "ROLLING_MARKET"
        if self.status.market_feed_transport_state == "stale":
            return (
                "RECOVERING_TRANSPORT"
                if self.status.orderbook_recovery_action == "reconnect"
                else "BLOCKED_UNRECOVERED"
            )
        if self.status.market_recovery_attempt_in_progress:
            if self.status.orderbook_recovery_action in {
                "resubscribe",
                "wait_for_subscription_ack",
            }:
                return "RECOVERING_SUBSCRIPTION"
            if self.status.orderbook_recovery_action in {"request_snapshot"}:
                return "SNAPSHOT_RESYNC_PENDING"
            if self.status.orderbook_recovery_action == "reconnect":
                return "RECOVERING_TRANSPORT"
        if self.status.orderbook_liveness_status == "blocked":
            return "BLOCKED_UNRECOVERED"
        if self.status.orderbook_liveness_status == "resync_pending":
            return "SNAPSHOT_RESYNC_PENDING"
        if self.status.market_feed_subscription_state != "subscribed":
            return "SUBSCRIBING"
        if not self.status.orderbook_initialized:
            return "SUBSCRIBED_WAITING_SNAPSHOT"
        if self.status.market_data_quiet:
            return "QUIET_CARRY_FORWARD"
        if self.status.orderbook_liveness_status == "live":
            return "LIVE"
        return "BLOCKED_UNRECOVERED"

    def _persist_orderbook(
        self,
        snapshot: OrderbookSnapshotInput,
        *,
        queued_recovery_result: str | None = None,
        queued_clear_recovery_warnings: bool = False,
        queued_clear_delta_warnings: bool = False,
    ) -> bool:
        if self.session_factory is None:
            self._add_warning("database_not_configured_for_orderbook")
            return False
        enqueue_result = self._enqueue_market_db_write(
            "orderbook",
            snapshot,
            snapshot.received_at,
            orderbook_recovery_result=queued_recovery_result,
            clear_orderbook_recovery_warnings=queued_clear_recovery_warnings,
            clear_orderbook_delta_warnings=queued_clear_delta_warnings,
        )
        if enqueue_result == MARKET_DB_WRITE_QUEUED:
            self._mark_orderbook_persistence_pending(snapshot.received_at)
            return False
        if enqueue_result == MARKET_DB_WRITE_COALESCED:
            return False
        if enqueue_result == MARKET_DB_WRITE_FULL:
            return False

        try:
            with self.session_factory() as session:
                OrderbookRepository(session).insert_snapshot(snapshot)
                session.commit()
        except SQLAlchemyError as exc:
            LOGGER.warning("Kalshi WS orderbook persistence failed.", exc_info=True)
            self._set_error(exc.__class__.__name__, exc)
            self._add_warning("orderbook_persistence_failed")
            self._add_blocker("orderbook_persistence_failed")
            self.record_market_heartbeat()
            return False

        self._mark_persistence_success(
            warning="orderbook_persistence_failed",
            blockers=("orderbook_persistence_failed",),
        )
        return True

    def _persist_trade(self, trade: PublicTradeInput) -> bool:
        if self.session_factory is None:
            self._add_warning("database_not_configured_for_trades")
            return False
        enqueue_result = self._enqueue_market_db_write(
            "trade",
            trade,
            trade.received_at,
        )
        if enqueue_result == MARKET_DB_WRITE_QUEUED:
            return True
        if enqueue_result == MARKET_DB_WRITE_FULL:
            return False

        try:
            with self.session_factory() as session:
                PublicTradesRepository(session).insert_trade(trade)
                session.commit()
        except SQLAlchemyError as exc:
            LOGGER.warning("Kalshi WS trade persistence failed.", exc_info=True)
            self._set_error(exc.__class__.__name__, exc)
            self._add_warning("trade_persistence_failed")
            self.record_market_heartbeat()
            return False

        self._mark_persistence_success(warning="trade_persistence_failed")
        return True

    async def _start_market_db_writer(self) -> None:
        if self._db_critical_queue is not None or self._db_diagnostic_queue is not None:
            return
        self._db_critical_queue = asyncio.Queue(
            maxsize=self.config.market_db_writer_critical_queue_max_size
        )
        self._db_diagnostic_queue = asyncio.Queue(
            maxsize=self.config.market_db_writer_diagnostic_queue_max_size
        )
        self._db_writer_queue = self._db_critical_queue
        self._db_critical_enqueued_at.clear()
        self._db_critical_inflight_at.clear()
        self._db_diagnostic_enqueued_at.clear()
        self._db_writer_enqueued_at = self._db_critical_enqueued_at
        self._pending_orderbook_writes.clear()
        self._pending_orderbook_markers.clear()
        self._db_writer_task = asyncio.create_task(self._market_db_writer_loop())
        self._refresh_db_writer_metrics(self.now())

    async def _stop_market_db_writer(self) -> None:
        critical_queue = self._db_critical_queue or self._db_writer_queue
        diagnostic_queue = self._db_diagnostic_queue
        task = self._db_writer_task
        if critical_queue is None or task is None:
            return
        try:
            await asyncio.wait_for(
                self._join_market_db_queues(critical_queue, diagnostic_queue),
                timeout=max(1.0, self.config.kalshi_ws_heartbeat_timeout_seconds),
            )
        except TimeoutError:
            self._add_warning("market_db_writer_drain_timeout")
            self._clear_orderbook_persistence_pending(self.now())
        self._refresh_db_writer_metrics(self.now())
        self.record_market_heartbeat()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        self._db_writer_task = None
        self._db_writer_queue = None
        self._db_critical_queue = None
        self._db_diagnostic_queue = None
        self._db_critical_enqueued_at.clear()
        self._db_critical_inflight_at.clear()
        self._db_diagnostic_enqueued_at.clear()
        self._pending_orderbook_writes.clear()
        self._pending_orderbook_markers.clear()
        self._refresh_db_writer_metrics(self.now())

    async def _join_market_db_queues(
        self,
        critical_queue: asyncio.Queue[_DbWriterItem],
        diagnostic_queue: asyncio.Queue[_DbWriterItem] | None,
    ) -> None:
        await critical_queue.join()
        if diagnostic_queue is not None:
            await diagnostic_queue.join()

    async def _market_db_writer_loop(self) -> None:
        while True:
            batch = self._next_market_db_batch()
            if not batch:
                await asyncio.sleep(self.config.market_db_writer_flush_interval_ms / 1000)
                continue
            started = time.perf_counter()
            try:
                await asyncio.to_thread(
                    self._write_market_db_batch,
                    [item for _, _, item in batch],
                )
            except SQLAlchemyError as exc:
                LOGGER.warning("Kalshi market DB writer failed.", exc_info=True)
                self._set_error(exc.__class__.__name__, exc)
                for _, _, item in batch:
                    if self._requeue_failed_critical_item(item, self.now()):
                        continue
                    if item.kind == "orderbook":
                        self._add_warning("orderbook_persistence_failed")
                        self._add_blocker("market_critical_persistence_failed")
                        self._finish_orderbook_persistence_pending(self.now())
                    elif item.kind == "trade":
                        self._add_warning("trade_persistence_failed")
                        self._add_blocker("market_critical_persistence_failed")
                    else:
                        self._add_warning("protocol_event_persistence_failed")
            finally:
                flush_ms = max(0, int((time.perf_counter() - started) * 1000))
                self.status.db_writer_last_flush_ms = flush_ms
                if any(queue_name == "critical" for queue_name, _, _ in batch):
                    self.status.db_writer_last_critical_flush_ms = flush_ms
                if any(queue_name == "diagnostic" for queue_name, _, _ in batch):
                    self.status.db_writer_last_diagnostic_flush_ms = flush_ms
                if flush_ms >= self.config.kalshi_ws_db_slow_write_ms:
                    self.status.db_writer_slow_flush_count += 1
                    self._record_protocol_event(
                        "db_write_slow",
                        event_subtype="batch",
                        latency_ms=flush_ms,
                    )
                for queue_name, raw_item, _ in batch:
                    self._market_db_queue_task_done(queue_name, raw_item)
                self._refresh_db_writer_metrics(self.now())
                if any(
                    item.kind in {"orderbook", "trade"} for _, _, item in batch
                ) or self._consume_force_next_heartbeat():
                    self.record_market_heartbeat()
                else:
                    self.record_market_heartbeat(force=False)
                await asyncio.sleep(
                    max(
                        self.config.market_db_writer_flush_interval_ms,
                        self.config.market_orderbook_snapshot_min_interval_ms,
                    )
                    / 1000
                )

    def _write_market_db_item(self, item: _DbWriterItem) -> None:
        self._write_market_db_batch([item])

    def _write_market_db_batch(self, items: list[_DbWriterItem]) -> None:
        if self.session_factory is None:
            return
        with self.session_factory() as session:
            orderbooks = [item.payload for item in items if item.kind == "orderbook"]
            trades = [item.payload for item in items if item.kind == "trade"]
            protocols = [item.payload for item in items if item.kind == "protocol"]
            unsupported = [
                item.kind
                for item in items
                if item.kind not in {"orderbook", "trade", "protocol"}
            ]
            if unsupported:
                raise ValueError(f"Unsupported market DB writer item kind: {unsupported[0]}")
            OrderbookRepository(session).insert_snapshots(orderbooks)
            PublicTradesRepository(session).insert_trades(trades)
            KalshiWsProtocolEventRepository(session).insert_events(protocols)
            session.commit()
        for item in items:
            self._mark_market_db_item_committed(item)

    def _mark_market_db_item_committed(self, item: _DbWriterItem) -> None:
        if item.kind == "orderbook":
            self._mark_persistence_success(
                warning="orderbook_persistence_failed",
                blockers=(
                    "orderbook_persistence_failed",
                    "market_critical_persistence_failed",
                    "market_critical_persistence_backpressure",
                ),
            )
            if _datetime_at_least(
                item.payload.received_at,
                self.status.last_orderbook_at,
            ):
                self.status.last_orderbook_at = item.payload.received_at
            if _datetime_at_least(
                item.payload.received_at,
                self.status.latest_state_persisted_at,
            ):
                self.status.latest_state_persisted_at = item.payload.received_at
                self.status.latest_state_persistence_lag_ms = _age_ms(
                    item.payload.received_at,
                    self.now(),
                )
            matches_active_market = _orderbook_commit_matches_active_market(
                item.payload,
                self.status.active_market_ticker,
            )
            recovery_still_current = self._queued_orderbook_recovery_still_current(
                item
            )
            if (
                item.orderbook_recovery_result is not None
                and matches_active_market
                and recovery_still_current
            ):
                self.status.market_snapshot_resync_last_result = (
                    item.orderbook_recovery_result
                )
                self.status.orderbook_recovery_action = "none"
                self._finish_market_recovery(
                    result=item.orderbook_recovery_result,
                    checked_at=item.payload.received_at,
                )
            if (
                item.clear_orderbook_recovery_warnings
                and matches_active_market
                and recovery_still_current
            ):
                self._clear_orderbook_recovery_warnings()
            if item.clear_orderbook_delta_warnings and matches_active_market:
                self._clear_orderbook_delta_warnings()
            self._finish_orderbook_persistence_pending(item.payload.received_at)
        elif item.kind == "trade":
            self._mark_persistence_success(
                warning="trade_persistence_failed",
                blockers=(
                    "market_critical_persistence_failed",
                    "market_critical_persistence_backpressure",
                ),
            )
            self.status.last_trade_at = item.payload.received_at
        elif item.kind == "protocol":
            self.status.protocol_events_persisted += 1
            if _protocol_event_counts_as_error(
                item.payload.event_type,
                close_code=item.payload.close_code,
            ):
                self.status.protocol_errors_persisted += 1

    def _next_market_db_batch(self) -> list[tuple[str, _DbWriterItem, _DbWriterItem]]:
        max_batch = max(1, self.config.market_db_writer_max_batch_size)
        max_flush_seconds = max(1, self.config.market_db_writer_max_flush_ms) / 1000
        started = time.perf_counter()
        batch: list[tuple[str, _DbWriterItem, _DbWriterItem]] = []
        while len(batch) < max_batch:
            queue_name, raw_item = self._pop_next_market_db_queue_item()
            if raw_item is None or queue_name is None:
                break
            resolved = self._resolve_market_db_item(raw_item)
            if resolved is None:
                self._market_db_queue_task_done(queue_name, raw_item)
                continue
            batch.append((queue_name, raw_item, resolved))
            if (
                queue_name == "diagnostic"
                and len(batch) >= self.config.market_protocol_event_max_per_flush
            ):
                break
            if time.perf_counter() - started >= max_flush_seconds:
                break
        return batch

    def _pop_next_market_db_queue_item(
        self,
    ) -> tuple[str | None, _DbWriterItem | None]:
        critical_queue = self._db_critical_queue or self._db_writer_queue
        if critical_queue is not None:
            try:
                item = critical_queue.get_nowait()
                enqueued_at = (
                    self._db_critical_enqueued_at.popleft()
                    if self._db_critical_enqueued_at
                    else item.enqueued_at
                )
                self._db_critical_inflight_at.append(enqueued_at)
                return "critical", item
            except asyncio.QueueEmpty:
                pass
        diagnostic_queue = self._db_diagnostic_queue
        if diagnostic_queue is not None:
            try:
                item = diagnostic_queue.get_nowait()
                if self._db_diagnostic_enqueued_at:
                    self._db_diagnostic_enqueued_at.popleft()
                return "diagnostic", item
            except asyncio.QueueEmpty:
                pass
        return None, None

    def _resolve_market_db_item(self, item: _DbWriterItem) -> _DbWriterItem | None:
        if item.kind != "orderbook_marker":
            return item
        market_ticker = str(item.payload)
        self._pending_orderbook_markers.discard(market_ticker)
        return self._pending_orderbook_writes.pop(market_ticker, None)

    def _market_db_queue_task_done(
        self,
        queue_name: str,
        item: _DbWriterItem,
    ) -> None:
        queue = (
            self._db_diagnostic_queue
            if queue_name == "diagnostic"
            else self._db_critical_queue or self._db_writer_queue
        )
        if queue is not None:
            queue.task_done()
        if queue_name == "critical" and self._db_critical_inflight_at:
            self._db_critical_inflight_at.popleft()

    def _requeue_failed_critical_item(
        self,
        item: _DbWriterItem,
        checked_at: datetime,
    ) -> bool:
        if item.kind not in {"orderbook", "trade"} or item.retry_count >= 1:
            return False
        queue = self._db_critical_queue or self._db_writer_queue
        if queue is None:
            return False
        retry_item = replace(
            item,
            enqueued_at=checked_at,
            retry_count=item.retry_count + 1,
        )
        try:
            queue.put_nowait(retry_item)
        except asyncio.QueueFull:
            self._mark_critical_queue_backpressure(item.kind, checked_at)
            return False
        self._db_critical_enqueued_at.append(checked_at)
        if item.kind == "orderbook":
            self._add_warning("orderbook_persistence_failed")
        else:
            self._add_warning("trade_persistence_failed")
        self._add_blocker("market_critical_persistence_failed")
        self._force_next_heartbeat = True
        return True

    def _queued_orderbook_recovery_still_current(self, item: _DbWriterItem) -> bool:
        if item.orderbook_recovery_result is None:
            return False
        if item.orderbook_recovery_reason is None:
            return True
        return (
            self.status.market_subscription_recovery_last_reason
            == item.orderbook_recovery_reason
            and self.status.market_subscription_recovery_last_action
            == item.orderbook_recovery_action
        )

    def _mark_orderbook_persistence_pending(self, queued_at: datetime) -> None:
        if self.status.orderbook_persistence_pending_count == 0:
            self.status.orderbook_persistence_pending_since = queued_at
        self.status.orderbook_persistence_pending_count += 1
        self.status.orderbook_persistence_pending_age_ms = _age_ms(
            self.status.orderbook_persistence_pending_since,
            queued_at,
        )
        self._refresh_market_feed_state(queued_at)
        self._force_next_heartbeat = True

    def _finish_orderbook_persistence_pending(self, checked_at: datetime) -> None:
        if self.status.orderbook_persistence_pending_count > 0:
            self.status.orderbook_persistence_pending_count -= 1
        if self.status.orderbook_persistence_pending_count == 0:
            self.status.orderbook_persistence_pending_since = None
            self.status.orderbook_persistence_pending_age_ms = None
            self._clear_blockers("orderbook_persistence_pending")
        else:
            self.status.orderbook_persistence_pending_age_ms = _age_ms(
                self.status.orderbook_persistence_pending_since,
                checked_at,
            )
        self._refresh_market_feed_state(checked_at)
        self._force_next_heartbeat = True

    def _clear_orderbook_persistence_pending(self, checked_at: datetime) -> None:
        self.status.orderbook_persistence_pending_count = 0
        self.status.orderbook_persistence_pending_since = None
        self.status.orderbook_persistence_pending_age_ms = None
        self._clear_blockers("orderbook_persistence_pending")
        self._refresh_market_feed_state(checked_at)
        self._force_next_heartbeat = True

    def _enqueue_market_db_write(
        self,
        kind: str,
        payload: Any,
        enqueued_at: datetime,
        *,
        orderbook_recovery_result: str | None = None,
        clear_orderbook_recovery_warnings: bool = False,
        clear_orderbook_delta_warnings: bool = False,
    ) -> str:
        if kind == "protocol":
            return self._enqueue_protocol_db_write(payload, enqueued_at)

        queue = self._db_critical_queue or self._db_writer_queue
        if queue is None:
            return MARKET_DB_WRITE_DISABLED
        item = _DbWriterItem(
            kind=kind,
            payload=payload,
            enqueued_at=enqueued_at,
            orderbook_recovery_result=orderbook_recovery_result,
            orderbook_recovery_reason=(
                self.status.market_subscription_recovery_last_reason
                if orderbook_recovery_result is not None
                else None
            ),
            orderbook_recovery_action=(
                self.status.market_subscription_recovery_last_action
                if orderbook_recovery_result is not None
                else None
            ),
            clear_orderbook_recovery_warnings=clear_orderbook_recovery_warnings,
            clear_orderbook_delta_warnings=clear_orderbook_delta_warnings,
        )
        if kind == "orderbook":
            return self._enqueue_orderbook_db_write(item, enqueued_at)
        try:
            queue.put_nowait(item)
        except asyncio.QueueFull:
            self._mark_critical_queue_backpressure(kind, enqueued_at)
            return MARKET_DB_WRITE_FULL
        self._db_critical_enqueued_at.append(enqueued_at)
        self._refresh_db_writer_metrics(enqueued_at)
        return MARKET_DB_WRITE_QUEUED

    def _enqueue_orderbook_db_write(
        self,
        item: _DbWriterItem,
        enqueued_at: datetime,
    ) -> str:
        queue = self._db_critical_queue or self._db_writer_queue
        if queue is None:
            return MARKET_DB_WRITE_DISABLED
        market_ticker = item.payload.market_ticker
        previous = self._pending_orderbook_writes.get(market_ticker)
        if previous is not None:
            item = _coalesced_orderbook_item(previous, item)
        self._pending_orderbook_writes[market_ticker] = item
        if previous is not None:
            self.status.db_writer_coalesced_orderbook_count += 1
            self.status.db_writer_dropped_superseded_count += 1
        if market_ticker in self._pending_orderbook_markers:
            self._refresh_db_writer_metrics(enqueued_at)
            return MARKET_DB_WRITE_COALESCED
        marker = _DbWriterItem(
            kind="orderbook_marker",
            payload=market_ticker,
            enqueued_at=enqueued_at,
        )
        try:
            queue.put_nowait(marker)
        except asyncio.QueueFull:
            self._mark_critical_queue_backpressure("orderbook", enqueued_at)
            return MARKET_DB_WRITE_FULL
        self._pending_orderbook_markers.add(market_ticker)
        self._db_critical_enqueued_at.append(enqueued_at)
        self._refresh_db_writer_metrics(enqueued_at)
        return MARKET_DB_WRITE_QUEUED

    def _enqueue_protocol_db_write(
        self,
        event: KalshiWsProtocolEventInput,
        enqueued_at: datetime,
    ) -> str:
        if not self._should_persist_protocol_event(event):
            self.status.protocol_events_sampled_out += 1
            self._refresh_db_writer_metrics(enqueued_at)
            return MARKET_DB_WRITE_COALESCED
        item = _DbWriterItem(kind="protocol", payload=event, enqueued_at=enqueued_at)
        critical = _protocol_event_is_critical(event.event_type)
        if critical:
            queue = self._db_critical_queue or self._db_writer_queue
        else:
            queue = self._db_diagnostic_queue
        if queue is None:
            return MARKET_DB_WRITE_DISABLED
        try:
            queue.put_nowait(item)
        except asyncio.QueueFull:
            if critical:
                self._mark_critical_queue_backpressure("protocol", enqueued_at)
            else:
                self.status.db_writer_dropped_diagnostic_count += 1
                self.status.protocol_events_dropped_backpressure += 1
                self._add_warning("market_diagnostic_persistence_backpressure")
                self._force_next_heartbeat = True
            self._refresh_db_writer_metrics(enqueued_at)
            return MARKET_DB_WRITE_FULL
        self.status.protocol_events_enqueued += 1
        if critical:
            self._db_critical_enqueued_at.append(enqueued_at)
        else:
            self._db_diagnostic_enqueued_at.append(enqueued_at)
        self._refresh_db_writer_metrics(enqueued_at)
        return MARKET_DB_WRITE_QUEUED

    def _mark_critical_queue_backpressure(
        self,
        kind: str,
        checked_at: datetime,
    ) -> None:
        self._add_warning("market_critical_persistence_backpressure")
        self._add_blocker("market_critical_persistence_backpressure")
        self._clear_warnings("market_db_writer_queue_backpressure")
        self._clear_blockers("market_db_writer_queue_backpressure")
        self._force_next_heartbeat = True
        self._refresh_db_writer_metrics(checked_at)
        self._record_protocol_event(
            "queue_backpressure",
            event_subtype=kind,
            recovery_action="drop_or_block",
            recovery_result="critical_queue_full",
        )

    def _should_persist_protocol_event(self, event: KalshiWsProtocolEventInput) -> bool:
        if _protocol_event_is_critical(event.event_type):
            return True
        if event.event_type not in ROUTINE_PROTOCOL_EVENTS:
            return True
        rate = (
            self.config.market_protocol_event_error_sample_rate
            if _protocol_event_counts_as_error(
                event.event_type,
                close_code=event.close_code,
            )
            else self.config.market_protocol_event_sample_rate
        )
        if rate >= 1:
            return True
        if rate <= 0:
            return False
        self._protocol_event_sequence[event.event_type] = (
            self._protocol_event_sequence.get(event.event_type, 0) + 1
        )
        interval = max(1, int(round(1 / rate)))
        return self._protocol_event_sequence[event.event_type] % interval == 1

    def _record_protocol_event(
        self,
        event_type: str,
        *,
        channel: str | None = None,
        command_id: int | None = None,
        command_type: str | None = None,
        command_action: str | None = None,
        sid: int | None = None,
        expected_sid: int | None = None,
        seq: int | None = None,
        event_subtype: str | None = None,
        raw_code: str | None = None,
        raw_message: str | None = None,
        close_code: int | None = None,
        close_reason: str | None = None,
        exception_type: str | None = None,
        exception_message: str | None = None,
        latency_ms: int | None = None,
        round_trip_ms: int | None = None,
        ping_sent_at: datetime | None = None,
        pong_received_at: datetime | None = None,
        server_ping_received_at: datetime | None = None,
        client_pong_sent_at: datetime | None = None,
        subscription_state_before: str | None = None,
        subscription_state_after: str | None = None,
        recovery_action: str | None = None,
        recovery_result: str | None = None,
        raw_payload_hash: str | None = None,
        payload_summary_json: Any | None = None,
    ) -> None:
        if self.session_factory is None:
            return
        created_at = self.now()
        counts_as_error = _protocol_event_counts_as_error(
            event_type,
            close_code=close_code,
        )
        if counts_as_error:
            self._protocol_error_events.append(created_at)
        elif event_type in PROTOCOL_RECOVERY_EVENTS:
            self._protocol_error_reset_at = created_at
            self._protocol_error_events.clear()
        self._refresh_protocol_error_count(created_at)
        event = KalshiWsProtocolEventInput(
            event_type=event_type,
            created_at=created_at,
            worker_service=WORKER_SERVICE_MARKET_WS,
            worker_role=self.status.worker_role,
            connection_id=self.status.connection_id,
            channel=channel,
            active_market_ticker=self.status.active_market_ticker,
            command_id=command_id,
            command_type=command_type,
            command_action=command_action,
            sid=sid,
            expected_sid=expected_sid,
            seq=seq,
            event_subtype=event_subtype,
            raw_code=raw_code,
            raw_message=_safe_protocol_text(raw_message),
            close_code=close_code,
            close_reason=_safe_protocol_text(close_reason),
            exception_type=exception_type,
            exception_message=_safe_protocol_text(exception_message),
            latency_ms=latency_ms,
            round_trip_ms=round_trip_ms,
            ping_sent_at=ping_sent_at,
            pong_received_at=pong_received_at,
            server_ping_received_at=server_ping_received_at,
            client_pong_sent_at=client_pong_sent_at,
            subscription_state_before=subscription_state_before,
            subscription_state_after=subscription_state_after,
            recovery_action=recovery_action,
            recovery_result=recovery_result,
            raw_payload_hash=raw_payload_hash,
            payload_summary_json=payload_summary_json,
        )
        enqueue_result = self._enqueue_market_db_write("protocol", event, created_at)
        if enqueue_result in {
            MARKET_DB_WRITE_QUEUED,
            MARKET_DB_WRITE_FULL,
            MARKET_DB_WRITE_COALESCED,
        }:
            return
        try:
            with self.session_factory() as session:
                KalshiWsProtocolEventRepository(session).insert_event(event)
                session.commit()
            self.status.protocol_events_persisted += 1
            if counts_as_error:
                self.status.protocol_errors_persisted += 1
        except SQLAlchemyError:
            LOGGER.warning("Kalshi WS protocol event persistence failed.", exc_info=True)

    def _record_market_protocol_message(
        self,
        message: ParsedWsMessage,
        payload: Any,
    ) -> None:
        if message.kind == "ticker":
            self._record_protocol_event(
                "ticker_received",
                channel="ticker",
                sid=message.sid,
                seq=message.seq,
                raw_payload_hash=message.raw_payload_hash,
                payload_summary_json=_payload_summary(payload),
            )
        elif message.kind == "control":
            self._record_protocol_event(
                f"{message.control_type or 'control'}_received",
                sid=message.sid,
                seq=message.seq,
                event_subtype=message.control_type,
                raw_payload_hash=message.raw_payload_hash,
                payload_summary_json=_payload_summary(payload),
            )

    def _start_market_connection(self) -> None:
        self._connection_sequence += 1
        self.status.connection_id = (
            f"{self.started_at.strftime('%Y%m%dT%H%M%S')}-"
            f"{self._connection_sequence}-{uuid.uuid4().hex[:8]}"
        )
        self.status.protocol_connection_state = "opening"
        self.status.subscription_reconciled = False
        self.status.orderbook_sid_confirmed = False
        self.status.ticker_sid_confirmed = False
        self.status.trade_sid_confirmed = False
        self.status.last_list_subscriptions_at = None
        self.status.last_list_subscriptions_result = None
        self.status.in_flight_snapshot_request = False
        self.status.snapshot_request_id = None
        self.status.snapshot_request_started_at = None
        self.status.snapshot_request_age_ms = None
        self.status.close_code = None
        self.status.close_reason = None

    def _reconcile_list_subscriptions(self, payload: dict[str, Any]) -> None:
        active_market_ticker = self.status.active_market_ticker
        requested_channels = set(self.status.subscribed_channels)
        for entry in _list_subscription_entries(payload):
            sid = _int_or_none(entry.get("sid"))
            if sid is None:
                continue
            if not _subscription_entry_matches_market(entry, active_market_ticker):
                continue
            for channel in _subscription_entry_channels(entry):
                if channel not in requested_channels:
                    continue
                self.status.subscription_ids[channel] = sid
                self.status.subscription_request_ids.pop(channel, None)
        self._refresh_sid_confirmations()

    def _refresh_sid_confirmations(self) -> None:
        self.status.orderbook_sid_confirmed = (
            not self.config.kalshi_ws_subscribe_orderbook
            or "orderbook_delta" in self.status.subscription_ids
        )
        self.status.ticker_sid_confirmed = (
            not self.config.kalshi_ws_subscribe_ticker
            or "ticker" in self.status.subscription_ids
        )
        self.status.trade_sid_confirmed = (
            not self.config.kalshi_ws_subscribe_trades
            or "trade" in self.status.subscription_ids
        )

    def _subscriptions_confirmed(self) -> bool:
        self._refresh_sid_confirmations()
        requested_channels = set(self.status.subscribed_channels)
        if not requested_channels:
            return False
        return all(channel in self.status.subscription_ids for channel in requested_channels)

    def _snapshot_request_timed_out(self, checked_at: datetime) -> bool:
        if (
            not self.status.in_flight_snapshot_request
            or self.status.snapshot_request_started_at is None
        ):
            return False
        age_seconds = (
            _as_utc(checked_at) - _as_utc(self.status.snapshot_request_started_at)
        ).total_seconds()
        return age_seconds >= self.config.kalshi_ws_snapshot_timeout_seconds

    def _refresh_protocol_error_count(self, checked_at: datetime) -> None:
        window_start = _as_utc(checked_at) - timedelta(
            seconds=PROTOCOL_ERROR_WINDOW_SECONDS
        )
        if self._protocol_error_reset_at is not None:
            window_start = max(window_start, _as_utc(self._protocol_error_reset_at))
        while self._protocol_error_events and (
            _as_utc(self._protocol_error_events[0]) < window_start
        ):
            self._protocol_error_events.popleft()
        self.status.protocol_event_recent_error_count = len(self._protocol_error_events)

    def _refresh_db_writer_metrics(self, checked_at: datetime) -> None:
        self.status.orderbook_persistence_pending_age_ms = _age_ms(
            self.status.orderbook_persistence_pending_since,
            checked_at,
        )
        self.status.latest_state_persisted_age_ms = _age_ms(
            self.status.latest_state_persisted_at,
            checked_at,
        )
        critical_queue = self._db_critical_queue or self._db_writer_queue
        diagnostic_queue = self._db_diagnostic_queue
        self.status.ws_reader_queue_depth = 0
        self.status.ws_reader_queue_oldest_age_ms = None
        if critical_queue is None:
            self.status.db_writer_queue_depth = 0
            self.status.db_writer_queue_oldest_age_ms = None
            self.status.db_writer_critical_queue_depth = 0
            self.status.db_writer_critical_queue_oldest_age_ms = None
            self.status.db_writer_diagnostic_queue_depth = 0
            self.status.db_writer_diagnostic_queue_oldest_age_ms = None
            self.status.protocol_event_queue_depth = 0
            self.status.protocol_event_oldest_age_ms = None
            self.status.historical_persistence_lag_ms = None
            return
        critical_oldest_values = [
            value
            for value in (
                self._db_critical_enqueued_at[0]
                if self._db_critical_enqueued_at
                else None,
                self._db_critical_inflight_at[0]
                if self._db_critical_inflight_at
                else None,
            )
            if value is not None
        ]
        critical_oldest = (
            min(_as_utc(value) for value in critical_oldest_values)
            if critical_oldest_values
            else None
        )
        diagnostic_oldest = (
            self._db_diagnostic_enqueued_at[0]
            if self._db_diagnostic_enqueued_at
            else None
        )
        self.status.db_writer_critical_queue_depth = critical_queue.qsize() + len(
            self._db_critical_inflight_at
        )
        self.status.db_writer_critical_queue_oldest_age_ms = _age_ms(
            critical_oldest,
            checked_at,
        )
        self.status.db_writer_diagnostic_queue_depth = (
            diagnostic_queue.qsize() if diagnostic_queue is not None else 0
        )
        self.status.db_writer_diagnostic_queue_oldest_age_ms = _age_ms(
            diagnostic_oldest,
            checked_at,
        )
        self.status.db_writer_queue_depth = self.status.db_writer_critical_queue_depth
        self.status.db_writer_queue_oldest_age_ms = (
            self.status.db_writer_critical_queue_oldest_age_ms
        )
        self.status.protocol_event_queue_depth = (
            self.status.db_writer_diagnostic_queue_depth
        )
        self.status.protocol_event_oldest_age_ms = (
            self.status.db_writer_diagnostic_queue_oldest_age_ms
        )
        self.status.historical_persistence_lag_ms = (
            self.status.db_writer_diagnostic_queue_oldest_age_ms
        )
        critical_age = self.status.db_writer_critical_queue_oldest_age_ms or 0
        critical_depth = self.status.db_writer_critical_queue_depth
        if (
            critical_depth >= self.config.market_db_writer_backpressure_block_depth
            or critical_age >= self.config.market_db_writer_backpressure_max_age_ms
        ):
            self._add_warning("market_critical_persistence_backpressure")
            self._add_blocker("market_critical_persistence_backpressure")
        elif critical_depth >= self.config.market_db_writer_backpressure_warn_depth:
            self._add_warning("market_critical_persistence_backpressure")
            self._clear_blockers("market_critical_persistence_backpressure")
        elif "market_critical_persistence_backpressure" in self.status.blockers:
            self._add_warning("market_critical_persistence_backpressure")
        else:
            self._clear_warnings("market_critical_persistence_backpressure")
            self._clear_blockers("market_critical_persistence_backpressure")

    def _persist_reference_tick(self, tick: ReferenceTickInput) -> bool:
        if self.session_factory is None:
            self._add_reference_warning("database_not_configured_for_reference")
            return False

        try:
            with self.session_factory() as session:
                repository = ReferenceTicksRepository(session)
                latest_received = repository.get_latest_tick(tick.source)
                latest = repository.get_latest_tick_with_source_ts(tick.source)
                latest_valid = repository.get_latest_valid_tick(tick.source)
                if (
                    tick.source_ts is not None
                    and latest is not None
                    and latest.source_ts is not None
                    and _as_utc(tick.source_ts) <= _as_utc(latest.source_ts)
                ):
                    reason = (
                        "duplicate_source_ts"
                        if _as_utc(tick.source_ts) == _as_utc(latest.source_ts)
                        else "out_of_order_source_ts"
                    )
                    if reason == "duplicate_source_ts":
                        self.brti_status.duplicate_source_ts_count += 1
                    else:
                        self.brti_status.out_of_order_source_ts_count += 1
                    self.brti_status.skipped_tick_count += 1
                    self.brti_status.last_skipped_reason = reason
                    self.brti_status.last_skipped_at = self.now()
                    if self._duplicate_reference_message_can_carry_forward(
                        tick,
                        latest_valid,
                        reason=reason,
                    ):
                        self._mark_reference_valid_message(
                            tick,
                            duplicate=True,
                            carried_forward=True,
                        )
                        self._clear_reference_warnings(
                            "brti_duplicate_or_out_of_order_source_ts",
                            "brti_reference_duplicate_conflict",
                            "brti_reference_first_tick_timeout",
                            "brti_reference_no_valid_tick_timeout",
                            "brti_reference_reconnect_requested",
                        )
                        self._clear_reference_blockers(
                            "brti_reference_duplicate_conflict",
                        )
                        self._clear_reference_error()
                        self._force_next_heartbeat = True
                        return True
                    if (
                        reason == "duplicate_source_ts"
                        and _reference_input_valid(tick)
                    ):
                        self._add_reference_warning(
                            "brti_reference_duplicate_conflict"
                        )
                        self._add_reference_blocker(
                            "brti_reference_duplicate_conflict"
                        )
                    else:
                        self._add_reference_warning(
                            "brti_duplicate_or_out_of_order_source_ts"
                        )
                    self._force_next_heartbeat = True
                    return False
                row = repository.insert_tick(tick)
                session.commit()
        except SQLAlchemyError as exc:
            LOGGER.warning("Kalshi BRTI persistence failed.", exc_info=True)
            self._set_reference_error(exc.__class__.__name__, exc)
            self._add_reference_warning("brti_persistence_failed")
            self.record_reference_heartbeat()
            return False

        self.brti_status.connection_state = "subscribed"
        if latest_received is not None and latest_received.received_at is not None:
            self.brti_status.inter_arrival_ms = max(
                0,
                int(
                    (
                        _as_utc(row.received_at)
                        - _as_utc(latest_received.received_at)
                    ).total_seconds()
                    * 1000
                ),
            )
        if (
            latest is not None
            and latest.source_ts is not None
            and row.source_ts is not None
        ):
            self.brti_status.source_gap_ms = max(
                0,
                int(
                    (
                        _as_utc(row.source_ts)
                        - _as_utc(latest.source_ts)
                    ).total_seconds()
                    * 1000
                ),
            )
        self.brti_status.last_persisted_at = row.received_at
        reference_tick_valid = _reference_tick_valid(row)
        if reference_tick_valid:
            self.brti_status.last_valid_tick_at = row.received_at
            self.brti_status.last_healthy_at = row.received_at
        self.brti_status.latest_source_ts = row.source_ts
        self.brti_status.latest_value = _decimal_text_or_none(row.parsed_value)
        self.brti_status.latest_trailing_60s_avg = _decimal_text_or_none(
            row.trailing_60s_avg
        )
        self.brti_status.latest_trailing_60s_window_size = row.trailing_60s_window_size
        self.brti_status.latest_final_minute_average = _decimal_text_or_none(
            row.last_60s_windowed_average_15min
        )
        self.brti_status.final_minute_average_status = row.final_minute_average_status
        self.brti_status.source_age_ms = row.source_age_ms
        if reference_tick_valid:
            self._mark_reference_valid_message(
                tick,
                duplicate=False,
                carried_forward=False,
            )
            self._clear_reference_warnings(
                "brti_duplicate_or_out_of_order_source_ts",
                "brti_reference_duplicate_conflict",
            )
            self._clear_reference_blockers("brti_reference_duplicate_conflict")
        return True

    def _duplicate_reference_message_can_carry_forward(
        self,
        tick: ReferenceTickInput,
        latest_valid,
        *,
        reason: str,
    ) -> bool:
        if not self.config.strategy_reference_allow_duplicate_source_ts_carry_forward:
            return False
        if reason != "duplicate_source_ts":
            return False
        if not _reference_input_valid(tick):
            return False
        if latest_valid is None or not _reference_tick_valid(latest_valid):
            return False
        if latest_valid.source_ts is None or tick.source_ts is None:
            return False
        if _as_utc(latest_valid.source_ts) != _as_utc(tick.source_ts):
            return False
        return latest_valid.parsed_value == tick.parsed_value

    def _mark_reference_valid_message(
        self,
        tick: ReferenceTickInput,
        *,
        duplicate: bool,
        carried_forward: bool,
    ) -> None:
        received_at = _as_utc(tick.received_at)
        self.brti_status.last_valid_message_at = received_at
        self.brti_status.last_valid_message_source_ts = tick.source_ts
        self.brti_status.last_valid_message_value = _decimal_text_or_none(
            tick.parsed_value
        )
        self.brti_status.valid_message_age_ms = 0
        self.brti_status.valid_message_duplicate_source_ts = duplicate
        self.brti_status.valid_message_carried_forward = carried_forward
        self.brti_status.reference_stream_live = True
        if duplicate:
            self.brti_status.last_duplicate_valid_message_at = received_at
            self.brti_status.last_duplicate_valid_message_source_ts = tick.source_ts
            self.brti_status.last_duplicate_valid_message_value = (
                _decimal_text_or_none(tick.parsed_value)
            )
        self._mark_reference_fresh(received_at)

    def _seconds_until_reference_check(self, checked_at: datetime) -> float | None:
        reason = self._reference_stale_reason(checked_at)
        if reason is not None:
            if self.brti_status.last_stale_check_at is not None:
                grace_deadline = _as_utc(self.brti_status.last_stale_check_at) + timedelta(
                    seconds=self.config.kalshi_cfbenchmarks_status_grace_seconds
                )
                return max(0.0, (grace_deadline - _as_utc(checked_at)).total_seconds())
            return 0.0

        if self._reference_subscription_error_active():
            return None

        last_valid = _latest_datetime(
            self.brti_status.last_valid_message_at,
            self.brti_status.last_valid_tick_at,
        )
        last_connected = self.brti_status.last_connected_at
        if last_connected is not None and (
            last_valid is None or _as_utc(last_valid) < _as_utc(last_connected)
        ):
            deadline = _as_utc(last_connected) + timedelta(
                seconds=self.config.kalshi_cfbenchmarks_first_tick_timeout_seconds
            )
        elif last_valid is not None:
            deadline = _as_utc(last_valid) + timedelta(
                seconds=(
                    self.config.kalshi_cfbenchmarks_no_valid_tick_reconnect_seconds
                )
            )
        elif last_connected is not None:
            deadline = _as_utc(last_connected) + timedelta(
                seconds=self.config.kalshi_cfbenchmarks_first_tick_timeout_seconds
            )
        else:
            return None

        if self.brti_status.last_stale_check_at is not None:
            grace_deadline = _as_utc(self.brti_status.last_stale_check_at) + timedelta(
                seconds=self.config.kalshi_cfbenchmarks_status_grace_seconds
            )
            if grace_deadline > deadline:
                deadline = grace_deadline

        return max(0.0, (deadline - _as_utc(checked_at)).total_seconds())

    def _seconds_until_market_transport_check(self, checked_at: datetime) -> float | None:
        if not self.config.kalshi_ws_enabled:
            return None
        base = self.status.transport_last_pong_at or self.status.last_connected_at
        if base is None:
            return 0.0
        interval_seconds = min(
            MIN_HEARTBEAT_INTERVAL_SECONDS,
            max(0.001, self.config.strategy_kalshi_book_stream_max_age_ms / 2000),
        )
        elapsed = (_as_utc(checked_at) - _as_utc(base)).total_seconds()
        return max(0.0, interval_seconds - elapsed)

    def _reference_stale_reason(self, checked_at: datetime) -> str | None:
        if not self._reference_collection_enabled():
            return None
        if self._reference_subscription_error_active():
            return None

        last_valid = _latest_datetime(
            self.brti_status.last_valid_message_at,
            self.brti_status.last_valid_tick_at,
        )
        last_connected = self.brti_status.last_connected_at
        if last_connected is not None and (
            last_valid is None or _as_utc(last_valid) < _as_utc(last_connected)
        ):
            elapsed = (_as_utc(checked_at) - _as_utc(last_connected)).total_seconds()
            if elapsed > self.config.kalshi_cfbenchmarks_first_tick_timeout_seconds:
                return "brti_reference_first_tick_timeout"
            return None

        if last_valid is None:
            return None

        elapsed = (_as_utc(checked_at) - _as_utc(last_valid)).total_seconds()
        if elapsed > self.config.kalshi_cfbenchmarks_no_valid_tick_reconnect_seconds:
            return "brti_reference_no_valid_tick_timeout"
        return None

    def _handle_reference_stale_if_due(self, checked_at: datetime) -> str | None:
        reason = self._reference_stale_reason(checked_at)
        if reason is None:
            return None

        if self.brti_status.last_stale_check_at is not None:
            elapsed = (
                _as_utc(checked_at) - _as_utc(self.brti_status.last_stale_check_at)
            ).total_seconds()
            if elapsed < self.config.kalshi_cfbenchmarks_status_grace_seconds:
                return None

        self.brti_status.last_stale_check_at = checked_at
        self.brti_status.stale_since = self.brti_status.stale_since or checked_at
        self.brti_status.consecutive_stale_count += 1
        self.brti_status.consecutive_fresh_tick_count = 0
        self.brti_status.recovery_state = "waiting_for_fresh_tick"
        self.brti_status.connection_state = "stale"
        self._add_reference_warning(reason)
        self._force_next_heartbeat = True

        max_stale = max(
            1,
            self.config.kalshi_cfbenchmarks_max_consecutive_stale_before_reconnect,
        )
        if self.brti_status.consecutive_stale_count < max_stale:
            return None

        self.brti_status.connection_state = "reconnect_pending"
        self.brti_status.recovery_state = "reconnecting"
        self.brti_status.consecutive_reconnect_count += 1
        self.brti_status.reconnect_count += 1
        self._add_reference_warning("brti_reference_reconnect_requested")
        return reason

    def _mark_reference_fresh(self, checked_at: datetime) -> None:
        was_recovering = self.brti_status.recovery_state in {
            "reconnecting",
            "waiting_for_fresh_tick",
            "recovering",
            "stale",
        }
        self.brti_status.consecutive_stale_count = 0
        self.brti_status.last_stale_check_at = None
        self.brti_status.stale_since = None
        self.brti_status.consecutive_fresh_tick_count += 1
        required_ticks = max(
            1,
            self.config.kalshi_cfbenchmarks_recovery_required_fresh_ticks,
        )
        if was_recovering and self.brti_status.consecutive_fresh_tick_count < required_ticks:
            self.brti_status.recovery_state = "recovering"
        else:
            if was_recovering:
                self.brti_status.last_recovered_at = checked_at
            self.brti_status.recovery_state = "healthy"
            self.brti_status.consecutive_reconnect_count = 0
            self.brti_status.reconnect_count = 0
        self._clear_reference_warnings(
            "brti_reference_first_tick_timeout",
            "brti_reference_no_valid_tick_timeout",
            "brti_reference_reconnect_requested",
        )

    def _reference_subscription_error_active(self) -> bool:
        return "kalshi_cfbenchmarks_subscription_error" in self.brti_status.blockers

    def record_heartbeat(
        self,
        *,
        force: bool = True,
        include_market: bool = True,
        include_reference: bool = True,
    ) -> None:
        if self.session_factory is None:
            return

        heartbeat_at = self.now()
        if include_market and self.config.kalshi_ws_enabled:
            self._record_market_heartbeat_at(
                heartbeat_at,
                force=(force or self._market_liveness_heartbeat_due(heartbeat_at)),
            )
        if include_reference and self._reference_collection_enabled():
            self._record_reference_heartbeat_at(
                heartbeat_at,
                force=(force or self._reference_liveness_heartbeat_due(heartbeat_at)),
            )
        if not force and not self._heartbeat_due(heartbeat_at):
            return
        self._refresh_reference_valid_message_age(heartbeat_at)

        try:
            with self.session_factory() as session:
                repository = WorkerHeartbeatRepository(session)
                metadata = {
                    "mode": self._heartbeat_mode(),
                    "ws": self.status.as_metadata(),
                    "reference": {
                        "brti": self.brti_status.as_metadata(),
                    },
                }
                latest_heartbeat = repository.get_latest_heartbeat(
                    WORKER_SERVICE_AGGREGATE
                )
                metadata_keys = _enabled_non_collector_metadata_keys(self.config)
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
        except SQLAlchemyError:
            LOGGER.warning("Kalshi worker heartbeat persistence failed.", exc_info=True)
            return

        self._last_heartbeat_at = heartbeat_at

    def record_market_heartbeat(self, *, force: bool = True) -> None:
        self.record_heartbeat(
            force=force,
            include_market=True,
            include_reference=False,
        )

    def record_reference_heartbeat(self, *, force: bool = True) -> None:
        self.record_heartbeat(
            force=force,
            include_market=False,
            include_reference=True,
        )

    def _record_market_heartbeat_at(
        self,
        heartbeat_at: datetime,
        *,
        force: bool,
    ) -> None:
        if not force and not self._market_liveness_heartbeat_due(heartbeat_at):
            return
        self._refresh_protocol_error_count(heartbeat_at)
        self._refresh_market_feed_state(heartbeat_at)
        if self._record_component_heartbeat(
            service_name=WORKER_SERVICE_MARKET_WS,
            heartbeat_at=heartbeat_at,
            metadata={"mode": "market_ws", "ws": self.status.as_metadata()},
            failure_message="Kalshi market WS heartbeat persistence failed.",
        ):
            self._last_market_heartbeat_at = heartbeat_at

    def _record_reference_heartbeat_at(
        self,
        heartbeat_at: datetime,
        *,
        force: bool,
    ) -> None:
        if not force and not self._reference_liveness_heartbeat_due(heartbeat_at):
            return
        self._refresh_reference_valid_message_age(heartbeat_at)
        if self._record_component_heartbeat(
            service_name=WORKER_SERVICE_REFERENCE_BRTI,
            heartbeat_at=heartbeat_at,
            metadata={
                "mode": "reference_brti",
                "reference": {"brti": self.brti_status.as_metadata()},
            },
            failure_message="Kalshi BRTI heartbeat persistence failed.",
        ):
            self._last_reference_heartbeat_at = heartbeat_at

    def _record_component_heartbeat(
        self,
        *,
        service_name: str,
        heartbeat_at: datetime,
        metadata: dict[str, Any],
        failure_message: str,
    ) -> bool:
        if self.session_factory is None:
            return False
        try:
            with self.session_factory() as session:
                WorkerHeartbeatRepository(session).record_heartbeat(
                    WorkerHeartbeatInput(
                        service_name=service_name,
                        started_at=self.started_at,
                        heartbeat_at=heartbeat_at,
                        app_mode=self.config.app_mode.value,
                        is_safe=self.safety.is_safe,
                        metadata=metadata,
                    )
                )
                session.commit()
        except SQLAlchemyError:
            LOGGER.warning(failure_message, exc_info=True)
            return False
        return True

    def _refresh_reference_valid_message_age(self, heartbeat_at: datetime) -> None:
        if self.brti_status.last_valid_message_at is None:
            self.brti_status.valid_message_age_ms = None
            self.brti_status.reference_stream_live = False
            return
        age_ms = max(
            0,
            int(
                (
                    _as_utc(heartbeat_at)
                    - _as_utc(self.brti_status.last_valid_message_at)
                ).total_seconds()
                * 1000
            ),
        )
        self.brti_status.valid_message_age_ms = age_ms
        self.brti_status.reference_stream_live = (
            age_ms <= self.config.strategy_reference_stream_max_age_ms
        )

    def _collector_enabled(self) -> bool:
        return self.config.kalshi_ws_enabled or self._reference_collection_enabled()

    def _dedicated_reference_enabled(self) -> bool:
        return (
            self._reference_collection_enabled()
            and self.config.kalshi_cfbenchmarks_dedicated_connection
        )

    def _reference_collection_enabled(self) -> bool:
        return (
            self.config.kalshi_cfbenchmarks_enabled
            and self.config.kalshi_cfbenchmarks_subscribe_on_worker
        )

    def _heartbeat_mode(self) -> str:
        if self.config.kalshi_ws_enabled:
            return "kalshi_ws"
        if self._reference_collection_enabled():
            return "reference_ws"
        return "idle"

    def _heartbeat_due(self, heartbeat_at: datetime) -> bool:
        if self._last_heartbeat_at is None:
            return True
        elapsed = (
            heartbeat_at.astimezone(UTC) - self._last_heartbeat_at.astimezone(UTC)
        ).total_seconds()
        return elapsed >= heartbeat_interval_seconds(self.config)

    def _market_liveness_heartbeat_due(self, heartbeat_at: datetime) -> bool:
        if not self.config.kalshi_ws_enabled:
            return False
        if self._last_market_heartbeat_at is None:
            return True
        interval_seconds = min(
            MIN_HEARTBEAT_INTERVAL_SECONDS,
            max(0.001, self.config.strategy_kalshi_book_stream_max_age_ms / 2000),
        )
        elapsed = (
            heartbeat_at.astimezone(UTC)
            - self._last_market_heartbeat_at.astimezone(UTC)
        ).total_seconds()
        return elapsed >= interval_seconds

    def _reference_liveness_heartbeat_due(self, heartbeat_at: datetime) -> bool:
        if not self._reference_collection_enabled():
            return False
        if self._last_reference_heartbeat_at is None:
            return True
        interval_seconds = min(
            MIN_HEARTBEAT_INTERVAL_SECONDS,
            max(0.001, self.config.strategy_reference_stream_max_age_ms / 2000),
        )
        elapsed = (
            heartbeat_at.astimezone(UTC)
            - self._last_reference_heartbeat_at.astimezone(UTC)
        ).total_seconds()
        return elapsed >= interval_seconds

    def _set_error(self, error_type: str, exc: Exception) -> None:
        self.status.connection_state = "error"
        self.status.last_error_type = error_type
        self.status.last_error_message = _redacted_error_message(exc, self.config)

    def _set_reference_error(self, error_type: str, exc: Exception) -> None:
        self.brti_status.connection_state = "error"
        self.brti_status.last_error_type = error_type
        self.brti_status.last_error_message = _redacted_error_message(exc, self.config)

    def _clear_error(self) -> bool:
        existing = self.status.last_error_type or self.status.last_error_message
        self.status.last_error_type = None
        self.status.last_error_message = None
        return bool(existing)

    def _clear_reference_error(self) -> bool:
        existing = self.brti_status.last_error_type or self.brti_status.last_error_message
        self.brti_status.last_error_type = None
        self.brti_status.last_error_message = None
        return bool(existing)

    def _add_warning(self, warning: str) -> None:
        if warning not in self.status.warnings:
            self.status.warnings.append(warning)

    def _add_reference_warning(self, warning: str) -> None:
        if warning not in self.brti_status.warnings:
            self.brti_status.warnings.append(warning)

    def _add_blocker(self, blocker: str) -> None:
        if blocker not in self.status.blockers:
            self.status.blockers.append(blocker)

    def _add_reference_blocker(self, blocker: str) -> None:
        if blocker not in self.brti_status.blockers:
            self.brti_status.blockers.append(blocker)

    def _set_reference_subscription_id(self, value: Any) -> None:
        subscription_id = _int_or_none(value)
        if subscription_id is not None:
            self.brti_status.subscription_id = subscription_id

    def _clear_warnings(self, *warnings: str) -> bool:
        if not warnings:
            return False
        warning_set = set(warnings)
        existing = self.status.warnings
        self.status.warnings = [
            warning for warning in self.status.warnings if warning not in warning_set
        ]
        return self.status.warnings != existing

    def _clear_reference_warnings(self, *warnings: str) -> bool:
        if not warnings:
            return False
        warning_set = set(warnings)
        existing = self.brti_status.warnings
        self.brti_status.warnings = [
            warning for warning in self.brti_status.warnings if warning not in warning_set
        ]
        return self.brti_status.warnings != existing

    def _clear_reference_blockers(self, *blockers: str) -> bool:
        if not blockers:
            return False
        blocker_set = set(blockers)
        existing = self.brti_status.blockers
        self.brti_status.blockers = [
            blocker
            for blocker in self.brti_status.blockers
            if blocker not in blocker_set
        ]
        return self.brti_status.blockers != existing

    def _clear_warning_prefixes(self, *prefixes: str) -> bool:
        if not prefixes:
            return False
        existing = self.status.warnings
        self.status.warnings = [
            warning
            for warning in self.status.warnings
            if not any(warning.startswith(prefix) for prefix in prefixes)
        ]
        return self.status.warnings != existing

    def _clear_blockers(self, *blockers: str) -> bool:
        if not blockers:
            return False
        blocker_set = set(blockers)
        existing = self.status.blockers
        self.status.blockers = [
            blocker for blocker in self.status.blockers if blocker not in blocker_set
        ]
        return self.status.blockers != existing

    def _mark_persistence_success(
        self,
        *,
        warning: str,
        blockers: tuple[str, ...] = (),
    ) -> None:
        warnings_cleared = self._clear_warnings(warning)
        blockers_cleared = self._clear_blockers(*blockers)
        error_cleared = False
        if not self._has_persistence_failure():
            error_cleared = self._clear_error()

        if warnings_cleared or blockers_cleared or error_cleared:
            self.status.connection_state = "subscribed"
            self._force_next_heartbeat = True

    def _has_persistence_failure(self) -> bool:
        persistence_failures = {
            "orderbook_persistence_failed",
            "trade_persistence_failed",
        }
        return bool(
            persistence_failures.intersection(self.status.warnings)
            or persistence_failures.intersection(self.status.blockers)
        )

    def _record_parse_diagnostic(self, message: ParsedWsMessage) -> bool:
        sample = _invalid_message_diagnostic_sample(message)
        if sample is None:
            return False
        signature = _diagnostic_sample_signature(sample)
        force_heartbeat = signature not in self._forced_diagnostic_signatures
        if not force_heartbeat:
            return False
        self._forced_diagnostic_signatures.add(signature)
        self.status.diagnostic_samples.append(sample)
        self.status.diagnostic_samples = self.status.diagnostic_samples[
            -MAX_DIAGNOSTIC_SAMPLES:
        ]
        return True

    def _consume_force_next_heartbeat(self) -> bool:
        force = self._force_next_heartbeat
        self._force_next_heartbeat = False
        return force


async def _default_websocket_factory(
    endpoint: str,
    headers: dict[str, str],
    connect_timeout_seconds: float,
    heartbeat_timeout_seconds: float,
) -> Any:
    return await connect_websocket(
        endpoint=endpoint,
        headers=headers,
        connect_timeout_seconds=connect_timeout_seconds,
        heartbeat_timeout_seconds=heartbeat_timeout_seconds,
    )


def _rest_client(config: AppConfig) -> KalshiRestClient:
    return KalshiRestClient(
        base_url=config.kalshi_api_base_url,
        api_key_id=config.kalshi_api_key_id,
        private_key_pem=config.kalshi_private_key,
        timeout_seconds=config.kalshi_rest_timeout_seconds,
    )


def _json_or_none(raw_message: Any) -> Any | None:
    if isinstance(raw_message, dict):
        return raw_message
    if isinstance(raw_message, bytes):
        raw_message = raw_message.decode("utf-8")
    if not isinstance(raw_message, str):
        return None
    try:
        return json.loads(raw_message)
    except ValueError:
        return None


async def _next_websocket_message(
    message_iterator: Any,
    timeout_seconds: float | None,
) -> Any:
    next_message = anext(message_iterator)
    if timeout_seconds is None:
        return await next_message
    return await asyncio.wait_for(next_message, timeout=timeout_seconds)


def _market_window_closed(now: datetime, close_time: datetime | None) -> bool:
    if close_time is None:
        return False
    return now.astimezone(UTC) >= close_time.astimezone(UTC)


def _seconds_until_market_close(now: datetime, close_time: datetime | None) -> float | None:
    if close_time is None:
        return None
    return max(0.0, (close_time.astimezone(UTC) - now.astimezone(UTC)).total_seconds())


def _minimum_timeout_seconds(*values: float | None) -> float | None:
    present = [value for value in values if value is not None]
    return min(present) if present else None


def _decimal_text_or_none(value: Any) -> str | None:
    return str(value) if value is not None else None


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _latest_datetime(*values: datetime | None) -> datetime | None:
    present = [_as_utc(value) for value in values if value is not None]
    return max(present) if present else None


def _datetime_at_least(value: datetime, minimum: datetime | None) -> bool:
    return minimum is None or _as_utc(value) >= _as_utc(minimum)


def _age_ms(value_at: datetime | None, checked_at: datetime) -> int | None:
    if value_at is None:
        return None
    return max(
        0,
        int((_as_utc(checked_at) - _as_utc(value_at)).total_seconds() * 1000),
    )


def _reference_reconnect_result(value: str | None) -> bool:
    return value in {
        "brti_reference_first_tick_timeout",
        "brti_reference_no_valid_tick_timeout",
    }


def _market_reconnect_result(value: str | None) -> bool:
    return value in {
        "kalshi_orderbook_transport_stale",
        "kalshi_orderbook_snapshot_resync_failed",
        "kalshi_orderbook_snapshot_resync_timeout",
        "kalshi_orderbook_subscription_ack_timeout",
        "kalshi_orderbook_subscription_recovery_failed",
        "kalshi_orderbook_sequence_gap_or_reset",
        "orderbook_reset_after_buffer_overflow",
        "orderbook_sequence_gap_reset",
    }


def _market_reconnect_confirmation_message(message: ParsedWsMessage) -> bool:
    return message.kind in {
        "control",
        "ticker",
        "trade",
        "orderbook_snapshot",
        "orderbook_delta",
    }


def _protocol_event_is_critical(event_type: str) -> bool:
    return event_type in CRITICAL_PROTOCOL_EVENTS or event_type.endswith("_error")


def _protocol_event_counts_as_error(
    event_type: str,
    *,
    close_code: int | None = None,
) -> bool:
    if event_type in PROTOCOL_ERROR_EVENTS:
        return True
    if event_type == "websocket_close":
        return close_code not in {None, 1000, 1001}
    return False


def _coalesced_orderbook_item(
    previous: _DbWriterItem,
    current: _DbWriterItem,
) -> _DbWriterItem:
    if current.orderbook_recovery_result is not None:
        recovery_result = current.orderbook_recovery_result
        recovery_reason = current.orderbook_recovery_reason
        recovery_action = current.orderbook_recovery_action
    else:
        recovery_result = previous.orderbook_recovery_result
        recovery_reason = previous.orderbook_recovery_reason
        recovery_action = previous.orderbook_recovery_action
    return replace(
        current,
        orderbook_recovery_result=recovery_result,
        orderbook_recovery_reason=recovery_reason,
        orderbook_recovery_action=recovery_action,
        clear_orderbook_recovery_warnings=(
            current.clear_orderbook_recovery_warnings
            or previous.clear_orderbook_recovery_warnings
        ),
        clear_orderbook_delta_warnings=(
            current.clear_orderbook_delta_warnings
            or previous.clear_orderbook_delta_warnings
        ),
    )


def _orderbook_commit_matches_current(
    snapshot: OrderbookSnapshotInput,
    current_sequence_number: int | None,
) -> bool:
    return (
        snapshot.sequence_number is None
        or snapshot.sequence_number == current_sequence_number
    )


def _orderbook_commit_matches_active_market(
    snapshot: OrderbookSnapshotInput,
    active_market_ticker: str | None,
) -> bool:
    return active_market_ticker is None or snapshot.market_ticker == active_market_ticker


def _reference_tick_valid(value: Any) -> bool:
    return (
        getattr(value, "parse_status", None) == "valid"
        and getattr(value, "parsed_value", None) is not None
        and getattr(value, "received_at", None) is not None
    )


def _reference_input_valid(value: ReferenceTickInput) -> bool:
    return (
        value.parse_status == "valid"
        and value.parsed_value is not None
        and value.received_at is not None
    )


def _invalid_message_diagnostic_sample(message: ParsedWsMessage) -> dict[str, Any] | None:
    reason = message.reason or "invalid_websocket_message"
    if not reason.startswith(
        ("invalid_orderbook_snapshot_", "invalid_orderbook_delta_", "invalid_trade_")
    ):
        return None
    raw_payload = message.raw_payload if isinstance(message.raw_payload, dict) else {}
    msg_payload = raw_payload.get("msg")
    msg = msg_payload if isinstance(msg_payload, dict) else {}
    message_type = _safe_text(raw_payload.get("type"))

    sample: dict[str, Any] = {
        "reason": reason,
        "message_type": message_type,
        "market_ticker": message.market_ticker or _safe_text(msg.get("market_ticker")),
        "payload_keys": _bounded_keys(raw_payload),
        "message_keys": _bounded_keys(msg),
        "raw_payload_hash": message.raw_payload_hash,
    }

    if message_type == "orderbook_snapshot" or reason.startswith(
        "invalid_orderbook_snapshot_"
    ):
        sample["snapshot"] = {
            "yes": _levels_diagnostic_shape(msg.get("yes_dollars_fp")),
            "no": _levels_diagnostic_shape(msg.get("no_dollars_fp")),
        }
    elif message_type == "orderbook_delta" or reason.startswith("invalid_orderbook_delta_"):
        sample["delta"] = _fields_diagnostic(
            msg,
            ("price_dollars", "delta_fp", "side"),
        )
    elif message_type == "trade" or reason.startswith("invalid_trade_"):
        sample["trade"] = _fields_diagnostic(
            msg,
            (
                "price_dollars",
                "yes_price_dollars",
                "no_price_dollars",
                "count",
                "count_fp",
                "taker_outcome_side",
                "taker_side",
                "taker_book_side",
            ),
        )

    return sample


def _diagnostic_sample_signature(sample: dict[str, Any]) -> str:
    return json.dumps(
        {key: value for key, value in sample.items() if key != "raw_payload_hash"},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


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


def _enabled_non_collector_metadata_keys(config: AppConfig) -> tuple[str, ...]:
    keys: list[str] = []
    if config.strategy_observer_enabled:
        keys.append("strategy")
    if config.storage_retention_enabled:
        keys.append("storage")
    return tuple(keys)


def _levels_diagnostic_shape(value: Any) -> dict[str, Any]:
    if not isinstance(value, list):
        return {
            "present": value is not None,
            "level_count": None,
            "shape": _value_shape(value),
            "level_samples": [],
        }
    return {
        "present": True,
        "level_count": len(value),
        "shape": {"type": "list", "length": len(value)},
        "level_samples": [_value_shape(item) for item in value[:3]],
    }


def _fields_diagnostic(payload: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    return {
        field: {
            "present": field in payload,
            "shape": _value_shape(payload.get(field)) if field in payload else None,
        }
        for field in fields
    }


def _value_shape(value: Any, *, depth: int = 0) -> dict[str, Any]:
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "bool"}
    if isinstance(value, int):
        return {"type": "int"}
    if isinstance(value, float):
        return {"type": "float"}
    if isinstance(value, str):
        return {"type": "string", "length": len(value), "blank": value.strip() == ""}
    if isinstance(value, list | tuple):
        shape: dict[str, Any] = {"type": "list", "length": len(value)}
        if depth < 2:
            shape["items"] = [_value_shape(item, depth=depth + 1) for item in value[:3]]
        return shape
    if isinstance(value, dict):
        shape = {
            "type": "object",
            "key_count": len(value),
            "keys": _bounded_keys(value),
        }
        return shape
    return {"type": type(value).__name__}


def _bounded_keys(payload: dict[str, Any]) -> list[str]:
    return [_safe_key(key) for key in list(payload.keys())[:MAX_DIAGNOSTIC_KEYS]]


def _safe_key(key: Any) -> str:
    text = str(key)[:80]
    lowered = text.lower()
    if any(
        marker in lowered
        for marker in (
            "authorization",
            "header",
            "signature",
            "secret",
            "private",
            "api_key",
            "access_key",
            "signed",
        )
    ):
        return "[redacted_key]"
    return text


def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text[:120] if text else None


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _message_sid(payload: dict[str, Any]) -> int | None:
    message = payload.get("msg")
    if not isinstance(message, dict):
        return None
    return _int_or_none(message.get("sid"))


def _message_channels(payload: dict[str, Any]) -> list[str]:
    message = payload.get("msg")
    if not isinstance(message, dict):
        return []
    return _subscription_entry_channels(message)


def _list_subscription_entries(payload: dict[str, Any]) -> list[dict[str, Any]]:
    message = payload.get("msg")
    if isinstance(message, list):
        return [entry for entry in message if isinstance(entry, dict)]
    if not isinstance(message, dict):
        return []

    entries: list[dict[str, Any]] = []
    if "sid" in message and ("channel" in message or "channels" in message):
        entries.append(message)
    for key in ("subscriptions", "subscription"):
        value = message.get(key)
        if isinstance(value, list):
            entries.extend(entry for entry in value if isinstance(entry, dict))
        elif isinstance(value, dict):
            entries.append(value)
    return entries


def _subscription_entry_channels(entry: dict[str, Any]) -> list[str]:
    channels = _string_list(entry.get("channels"))
    channel = _safe_text(entry.get("channel"))
    if channel is not None:
        channels.append(channel)
    deduped: list[str] = []
    for value in channels:
        if value not in deduped:
            deduped.append(value)
    return deduped


def _subscription_entry_matches_market(
    entry: dict[str, Any],
    active_market_ticker: str | None,
) -> bool:
    if active_market_ticker is None:
        return True
    market_tickers = _string_list(entry.get("market_tickers"))
    market_ticker = _safe_text(entry.get("market_ticker"))
    if market_ticker is not None:
        market_tickers.append(market_ticker)
    if not market_tickers:
        return True
    return active_market_ticker in market_tickers


def _websocket_error_message(payload: dict[str, Any]) -> str:
    message = payload.get("msg")
    if isinstance(message, dict):
        code = _safe_text(message.get("code"))
        text = _safe_text(
            message.get("msg")
            or message.get("message")
            or message.get("reason")
            or message.get("error")
        )
        if code and text:
            return f"{code}: {text}"
        if text:
            return text
        if code:
            return f"code={code}"
    text = _safe_text(message)
    return text or "Kalshi cfbenchmarks_value subscription error."


def _websocket_error_code(payload: dict[str, Any]) -> str | None:
    message = payload.get("msg")
    if isinstance(message, dict):
        return _safe_text(message.get("code"))
    return None


def _already_subscribed_error(message: str | None) -> bool:
    return bool(message and "already subscribed" in message.lower())


def _payload_summary(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    summary: dict[str, Any] = {
        "type": _safe_text(payload.get("type")),
        "cmd": _safe_text(payload.get("cmd")),
        "id": _int_or_none(payload.get("id")),
        "sid": _int_or_none(payload.get("sid")),
        "seq": _int_or_none(payload.get("seq")),
    }
    params = payload.get("params")
    if isinstance(params, dict):
        summary["params"] = {
            "channels": _string_list(params.get("channels")),
            "action": _safe_text(params.get("action")),
            "sids": _int_list(params.get("sids")),
            "market_ticker": _safe_text(params.get("market_ticker")),
            "market_tickers": _string_list(params.get("market_tickers")),
            "index_ids": _string_list(params.get("index_ids")),
        }
    message = payload.get("msg")
    if isinstance(message, dict):
        summary["msg"] = {
            "channel": _safe_text(message.get("channel")),
            "market_ticker": _safe_text(message.get("market_ticker")),
            "sid": _int_or_none(message.get("sid")),
            "code": _safe_text(message.get("code")),
            "text": _safe_text(
                message.get("msg")
                or message.get("message")
                or message.get("reason")
                or message.get("error")
            ),
        }
    return {
        key: value
        for key, value in summary.items()
        if value not in (None, [], {})
    }


def _safe_protocol_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = value[:500]
    lowered = text.lower()
    if any(
        marker in lowered
        for marker in (
            "private key",
            "kalshi-access-signature",
            "authorization",
            "secret",
        )
    ):
        return "[redacted]"
    return text


def _int_list(value: Any) -> list[int]:
    if not isinstance(value, list | tuple):
        return []
    parsed = [_int_or_none(item) for item in value]
    return [item for item in parsed if item is not None]


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list | tuple):
        return []
    return [str(item)[:128] for item in value if str(item).strip()]


def _websocket_close_code(websocket: Any) -> int | None:
    return _int_or_none(
        getattr(websocket, "close_code", None)
        or getattr(websocket, "close_rcvd", None)
    )


def _websocket_close_reason(websocket: Any) -> str | None:
    return _safe_text(
        getattr(websocket, "close_reason", None)
        or getattr(websocket, "close_rcvd", None)
    )


def heartbeat_interval_seconds(config: AppConfig) -> float:
    return min(
        max(
            config.kalshi_ws_heartbeat_timeout_seconds / 3,
            MIN_HEARTBEAT_INTERVAL_SECONDS,
        ),
        MAX_HEARTBEAT_INTERVAL_SECONDS,
    )


async def _close_websocket(websocket: Any) -> None:
    close = getattr(websocket, "close", None)
    if not callable(close):
        return
    try:
        result = close()
        if isawaitable(result):
            await result
    except Exception:
        LOGGER.debug("Kalshi WebSocket close failed.", exc_info=True)


async def _sleep_or_stop(stop_event: threading.Event, seconds: float) -> None:
    deadline = asyncio.get_running_loop().time() + seconds
    while not stop_event.is_set() and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(min(0.25, max(0, deadline - asyncio.get_running_loop().time())))


def _redacted_error_message(exc: Exception, config: AppConfig) -> str:
    text = str(exc)[:500]
    for value in (config.kalshi_api_key_id, config.kalshi_private_key):
        if value:
            text = text.replace(value, "[redacted]")
    return text


def _isoformat_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
