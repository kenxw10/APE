from __future__ import annotations

import asyncio
import json
import logging
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
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
    build_subscribe_message,
    connect_websocket,
    create_websocket_auth_headers,
)
from ape.kalshi.ws_messages import ParsedWsMessage, parse_ws_payload
from ape.kalshi.ws_state import OrderbookState
from ape.repositories.inputs import ReferenceTickInput, WorkerHeartbeatInput
from ape.repositories.orderbook import OrderbookRepository
from ape.repositories.public_trades import PublicTradesRepository
from ape.repositories.reference_ticks import ReferenceTicksRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.safety import SafetyAssessment

LOGGER = logging.getLogger(__name__)
MAX_HEARTBEAT_INTERVAL_SECONDS = 10.0
MIN_HEARTBEAT_INTERVAL_SECONDS = 1.0
MAX_DIAGNOSTIC_SAMPLES = 3
MAX_DIAGNOSTIC_KEYS = 20

WebSocketFactory = Callable[
    [str, dict[str, str], float, float],
    Awaitable[Any],
]
Resolver = Callable[..., Any]


@dataclass
class KalshiWsCollectorStatus:
    enabled: bool
    configured: bool = False
    signer_ready: bool = False
    connection_state: str = "disabled"
    active_market_ticker: str | None = None
    subscribed_channels: list[str] = field(default_factory=list)
    subscription_ids: dict[str, int] = field(default_factory=dict)
    last_connected_at: datetime | None = None
    last_message_at: datetime | None = None
    last_ticker_at: datetime | None = None
    last_orderbook_at: datetime | None = None
    last_trade_at: datetime | None = None
    reconnect_count: int = 0
    last_error_type: str | None = None
    last_error_message: str | None = None
    warnings: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    diagnostic_samples: list[dict[str, Any]] = field(default_factory=list)

    def as_metadata(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "configured": self.configured,
            "signer_ready": self.signer_ready,
            "connection_state": self.connection_state,
            "active_market_ticker": self.active_market_ticker,
            "subscribed_channels": self.subscribed_channels,
            "subscription_ids": self.subscription_ids,
            "last_connected_at": _isoformat_or_none(self.last_connected_at),
            "last_message_at": _isoformat_or_none(self.last_message_at),
            "last_ticker_at": _isoformat_or_none(self.last_ticker_at),
            "last_orderbook_at": _isoformat_or_none(self.last_orderbook_at),
            "last_trade_at": _isoformat_or_none(self.last_trade_at),
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
    connection_state: str = "disabled"
    last_message_at: datetime | None = None
    last_persisted_at: datetime | None = None
    latest_source_ts: datetime | None = None
    latest_value: str | None = None
    latest_trailing_60s_avg: str | None = None
    latest_trailing_60s_window_size: int | None = None
    latest_final_minute_average: str | None = None
    final_minute_average_status: str | None = None
    source_age_ms: int | None = None
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
            "connection_state": self.connection_state,
            "last_message_at": _isoformat_or_none(self.last_message_at),
            "last_persisted_at": _isoformat_or_none(self.last_persisted_at),
            "latest_source_ts": _isoformat_or_none(self.latest_source_ts),
            "latest_value": self.latest_value,
            "latest_trailing_60s_avg": self.latest_trailing_60s_avg,
            "latest_trailing_60s_window_size": self.latest_trailing_60s_window_size,
            "latest_final_minute_average": self.latest_final_minute_average,
            "final_minute_average_status": self.final_minute_average_status,
            "source_age_ms": self.source_age_ms,
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
        self._force_next_heartbeat = False
        self._forced_diagnostic_signatures: set[str] = set()

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

        cycles = 0
        while not stop_event.is_set():
            cycles += 1
            await self._run_cycle(stop_event)
            if max_cycles is not None and cycles >= max_cycles:
                return
            await _sleep_or_stop(
                stop_event,
                min(
                    self.config.kalshi_ws_reconnect_seconds * max(1, self.status.reconnect_count),
                    self.config.kalshi_ws_max_reconnect_seconds,
                ),
            )

    async def _run_cycle(self, stop_event: threading.Event) -> None:
        diagnostic = build_kalshi_config_diagnostic(self.config)
        self.status.configured = diagnostic.configured
        self.status.signer_ready = diagnostic.signer_ready
        self.status.blockers = []
        self.status.warnings = []
        self.brti_status.configured = diagnostic.configured
        self.brti_status.signer_ready = diagnostic.signer_ready
        self.brti_status.blockers = []
        self.brti_status.warnings = []

        market_ws_enabled = self.config.kalshi_ws_enabled
        brti_enabled = self._reference_collection_enabled()

        if not diagnostic.signer_ready:
            if market_ws_enabled:
                self.status.connection_state = "not_configured"
                self.status.blockers = ["kalshi_ws_credentials_not_configured_or_not_parseable"]
            if brti_enabled:
                self.brti_status.connection_state = "not_configured"
                self.brti_status.blockers = [
                    "kalshi_cfbenchmarks_credentials_not_configured_or_not_parseable"
                ]
            self.record_heartbeat()
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
                self.record_heartbeat()
                if not brti_enabled:
                    return
            except SQLAlchemyError as exc:
                self._set_error("resolver_database_error", exc)
                self.status.blockers = ["market_resolver_database_error"]
                self.record_heartbeat()
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
                    self.record_heartbeat()
                    if not brti_enabled:
                        return
                elif resolver_result.market is None:
                    self.status.connection_state = "no_active_market"
                    self.status.blockers = ["no_active_market"]
                    self.record_heartbeat()
                    if not brti_enabled:
                        return
                else:
                    market_ticker = resolver_result.market.market_ticker
                    market_close_time = resolver_result.market.close_time
                    self.status.active_market_ticker = market_ticker
                    market_subscription_enabled = True
        else:
            self.status.connection_state = "disabled"
            self.status.warnings = ["kalshi_ws_disabled"]

        try:
            headers = create_websocket_auth_headers(
                endpoint=self.config.kalshi_ws_base_url,
                api_key_id=self.config.kalshi_api_key_id,
                private_key_pem=self.config.kalshi_private_key,
            )
            websocket = await self.websocket_factory(
                self.config.kalshi_ws_base_url,
                headers,
                self.config.kalshi_ws_connect_timeout_seconds,
                self.config.kalshi_ws_heartbeat_timeout_seconds,
            )
            try:
                if market_subscription_enabled:
                    self.status.connection_state = "connected"
                if brti_enabled:
                    self.brti_status.connection_state = "connected"
                self.status.last_connected_at = self.now()
                self.record_heartbeat()

                await self._subscribe(websocket, market_ticker)
                if market_subscription_enabled:
                    self.status.connection_state = "subscribed"
                if brti_enabled:
                    self.brti_status.connection_state = "subscribed"
                self.record_heartbeat()

                await self._read_messages(
                    websocket,
                    market_ticker,
                    market_close_time,
                    stop_event,
                )
                self.status.reconnect_count = 0
            finally:
                await _close_websocket(websocket)
        except Exception as exc:
            self.status.reconnect_count += 1
            if market_ws_enabled:
                self._set_error(exc.__class__.__name__, exc)
            if brti_enabled:
                self._set_reference_error(exc.__class__.__name__, exc)
            self.record_heartbeat()

    async def _subscribe(self, websocket: Any, market_ticker: str | None) -> None:
        request_id = 1
        subscribed_channels: list[str] = []
        subscription_ids: dict[str, int] = {}

        if market_ticker is not None and self.config.kalshi_ws_subscribe_orderbook:
            message = build_subscribe_message(
                request_id=request_id,
                channels=["orderbook_delta"],
                market_ticker=market_ticker,
                use_yes_price=True,
            )
            await websocket.send(json.dumps(message))
            subscribed_channels.append("orderbook_delta")
            subscription_ids["orderbook_delta"] = request_id
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
            subscribed_channels.extend(secondary_channels)
            for channel in secondary_channels:
                subscription_ids[channel] = request_id
            request_id += 1

        if self._reference_collection_enabled():
            message = build_cfbenchmarks_subscribe_message(
                request_id=request_id,
                index_ids=list(self.config.kalshi_cfbenchmarks_index_ids),
            )
            await websocket.send(json.dumps(message))
            self.brti_status.subscription_id = request_id

        self.status.subscribed_channels = subscribed_channels
        self.status.subscription_ids = subscription_ids

    async def _read_messages(
        self,
        websocket: Any,
        market_ticker: str | None,
        market_close_time: datetime | None,
        stop_event: threading.Event,
    ) -> None:
        orderbook = OrderbookState(market_ticker=market_ticker or "")
        message_iterator = websocket.__aiter__()

        while not stop_event.is_set():
            if _market_window_closed(self.now(), market_close_time):
                self.status.connection_state = "market_roll_reresolve"
                self._add_warning("active_market_window_closed")
                self.record_heartbeat()
                return

            try:
                raw_message = await _next_websocket_message(
                    message_iterator,
                    _seconds_until_market_close(self.now(), market_close_time),
                )
            except StopAsyncIteration:
                return
            except TimeoutError:
                self.status.connection_state = "market_roll_reresolve"
                self._add_warning("active_market_window_closed")
                self.record_heartbeat()
                return

            received_at = self.now()
            parsed_json = _json_or_none(raw_message)
            if parsed_json is None:
                self._add_warning("invalid_websocket_json")
                self.record_heartbeat(force=False)
                continue

            if is_cfbenchmarks_value_payload(parsed_json):
                reference_message = parse_cfbenchmarks_value_message(
                    parsed_json,
                    received_at=received_at,
                    allowed_index_ids=self.config.kalshi_cfbenchmarks_index_ids,
                    persist_raw_payload=self.config.kalshi_cfbenchmarks_persist_raw_payload,
                )
                self.brti_status.last_message_at = received_at
                self._handle_reference_message(reference_message)
                self.record_heartbeat(force=self._consume_force_next_heartbeat())
                continue

            if self._handle_reference_control_payload(parsed_json, received_at=received_at):
                self.record_heartbeat(force=self._consume_force_next_heartbeat())
                continue

            if market_ticker is None:
                self.record_heartbeat(force=False)
                continue

            message = parse_ws_payload(
                parsed_json,
                target_market_ticker=market_ticker,
                received_at=received_at,
            )
            self.status.last_message_at = received_at
            resubscribe_reason = self._handle_message(message, orderbook, received_at)
            if resubscribe_reason is not None:
                self.status.connection_state = "resubscribe_pending"
                self._add_warning("kalshi_ws_resubscribe_requested")
                self.record_heartbeat()
                return
            self.record_heartbeat(force=self._consume_force_next_heartbeat())

    def _handle_reference_message(self, message: ParsedReferenceMessage) -> None:
        if message.kind == "ignored":
            return

        if message.kind == "invalid" or message.tick is None:
            self._add_reference_warning(message.reason or "invalid_cfbenchmarks_message")
            return

        if message.warning:
            self._add_reference_warning(message.warning)
        if self._persist_reference_tick(message.tick):
            self._clear_reference_warnings(
                "brti_persistence_failed",
                "brti_duplicate_or_out_of_order_source_ts",
            )
            self._clear_reference_error()
            self._force_next_heartbeat = True

    def _handle_message(
        self,
        message: ParsedWsMessage,
        orderbook: OrderbookState,
        received_at: datetime,
    ) -> str | None:
        if message.kind == "control":
            return None

        if message.kind == "ticker":
            self.status.last_ticker_at = received_at
            return None

        if message.kind == "orderbook_snapshot":
            orderbook.apply_snapshot(message)
            snapshot = orderbook.snapshot_input(
                received_at=received_at,
                sequence_number=message.seq,
                raw_payload_hash=message.raw_payload_hash,
                raw_payload=message.raw_payload,
            )
            if not self._persist_orderbook(snapshot):
                return None
            self.status.last_orderbook_at = received_at
            warnings_cleared = self._clear_warning_prefixes(
                "invalid_orderbook_snapshot_"
            )
            warnings_cleared = (
                self._clear_warnings("orderbook_delta_before_snapshot") or warnings_cleared
            )
            if warnings_cleared:
                self.record_heartbeat()
            return None

        if message.kind == "orderbook_delta":
            if not orderbook.initialized:
                self._add_warning("orderbook_delta_before_snapshot")
                return None
            if orderbook.has_sequence_gap(message.seq):
                orderbook.reset()
                self._add_warning("orderbook_sequence_gap_reset")
                return "orderbook_sequence_gap_reset"
            orderbook.apply_delta(message)
            snapshot = orderbook.snapshot_input(
                received_at=received_at,
                sequence_number=message.seq,
                raw_payload_hash=message.raw_payload_hash,
                raw_payload=message.raw_payload,
            )
            if not self._persist_orderbook(snapshot):
                return None
            self.status.last_orderbook_at = received_at
            warnings_cleared = self._clear_warning_prefixes("invalid_orderbook_delta_")
            warnings_cleared = (
                self._clear_warnings("orderbook_delta_before_snapshot") or warnings_cleared
            )
            if warnings_cleared:
                self.record_heartbeat()
            return None

        if message.kind == "trade" and message.trade is not None:
            if not self._persist_trade(message.trade):
                return None
            self.status.last_trade_at = received_at
            warnings_cleared = self._clear_warning_prefixes("invalid_trade_")
            warnings_cleared = (
                self._clear_warnings("invalid_trade_price_or_size") or warnings_cleared
            )
            if message.warning:
                self._add_warning(message.warning)
            if warnings_cleared:
                self.record_heartbeat()
            return None

        if message.kind == "invalid":
            if message.reason == "kalshi_websocket_buffer_overflow":
                orderbook.reset()
                self._add_warning("orderbook_reset_after_buffer_overflow")
                self._add_warning(message.reason)
                return "orderbook_reset_after_buffer_overflow"
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
    ) -> bool:
        if not self._reference_collection_enabled() or not isinstance(payload, dict):
            return False

        message_type = _safe_text(payload.get("type"))
        if message_type not in {"subscribed", "ok", "unsubscribed", "error"}:
            return False

        sid = _int_or_none(payload.get("sid"))
        subscription_id = self.brti_status.subscription_id
        if subscription_id is not None and sid not in {None, subscription_id}:
            return False

        self.brti_status.last_message_at = received_at
        if message_type != "error":
            return True

        self._set_reference_error(
            "kalshi_cfbenchmarks_subscription_error",
            RuntimeError(_websocket_error_message(payload)),
        )
        self._add_reference_warning("kalshi_cfbenchmarks_subscription_error")
        self._add_reference_blocker("kalshi_cfbenchmarks_subscription_error")
        self._force_next_heartbeat = True
        return True

    def _persist_orderbook(self, snapshot) -> bool:
        if self.session_factory is None:
            self._add_warning("database_not_configured_for_orderbook")
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
            self.record_heartbeat()
            return False

        self._mark_persistence_success(
            warning="orderbook_persistence_failed",
            blockers=("orderbook_persistence_failed",),
        )
        return True

    def _persist_trade(self, trade) -> bool:
        if self.session_factory is None:
            self._add_warning("database_not_configured_for_trades")
            return False

        try:
            with self.session_factory() as session:
                PublicTradesRepository(session).insert_trade(trade)
                session.commit()
        except SQLAlchemyError as exc:
            LOGGER.warning("Kalshi WS trade persistence failed.", exc_info=True)
            self._set_error(exc.__class__.__name__, exc)
            self._add_warning("trade_persistence_failed")
            self.record_heartbeat()
            return False

        self._mark_persistence_success(warning="trade_persistence_failed")
        return True

    def _persist_reference_tick(self, tick: ReferenceTickInput) -> bool:
        if self.session_factory is None:
            self._add_reference_warning("database_not_configured_for_reference")
            return False

        try:
            with self.session_factory() as session:
                repository = ReferenceTicksRepository(session)
                latest = repository.get_latest_tick(tick.source)
                if (
                    tick.source_ts is not None
                    and latest is not None
                    and latest.source_ts is not None
                    and _as_utc(tick.source_ts) <= _as_utc(latest.source_ts)
                ):
                    self._add_reference_warning("brti_duplicate_or_out_of_order_source_ts")
                    self._force_next_heartbeat = True
                    return False
                row = repository.insert_tick(tick)
                session.commit()
        except SQLAlchemyError as exc:
            LOGGER.warning("Kalshi BRTI persistence failed.", exc_info=True)
            self._set_reference_error(exc.__class__.__name__, exc)
            self._add_reference_warning("brti_persistence_failed")
            self.record_heartbeat()
            return False

        self.brti_status.connection_state = "subscribed"
        self.brti_status.last_persisted_at = row.received_at
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
        return True

    def record_heartbeat(self, *, force: bool = True) -> None:
        if self.session_factory is None:
            return

        heartbeat_at = self.now()
        if not force and not self._heartbeat_due(heartbeat_at):
            return

        try:
            with self.session_factory() as session:
                WorkerHeartbeatRepository(session).record_heartbeat(
                    WorkerHeartbeatInput(
                        service_name="ape-worker",
                        started_at=self.started_at,
                        heartbeat_at=heartbeat_at,
                        app_mode=self.config.app_mode.value,
                        is_safe=self.safety.is_safe,
                        metadata={
                            "mode": self._heartbeat_mode(),
                            "ws": self.status.as_metadata(),
                            "reference": {
                                "brti": self.brti_status.as_metadata(),
                            },
                        },
                    )
                )
                session.commit()
        except SQLAlchemyError:
            LOGGER.warning("Kalshi worker heartbeat persistence failed.", exc_info=True)
            return

        self._last_heartbeat_at = heartbeat_at

    def _collector_enabled(self) -> bool:
        return self.config.kalshi_ws_enabled or self._reference_collection_enabled()

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
        if force_heartbeat:
            self._forced_diagnostic_signatures.add(signature)
        self.status.diagnostic_samples.append(sample)
        self.status.diagnostic_samples = self.status.diagnostic_samples[
            -MAX_DIAGNOSTIC_SAMPLES:
        ]
        return force_heartbeat

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


def _decimal_text_or_none(value: Any) -> str | None:
    return str(value) if value is not None else None


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


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
