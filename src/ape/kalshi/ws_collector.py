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
from ape.kalshi.resolver import ResolverState, resolve_active_btc15_market
from ape.kalshi.ws_client import (
    build_subscribe_message,
    connect_websocket,
    create_websocket_auth_headers,
)
from ape.kalshi.ws_messages import ParsedWsMessage, parse_ws_payload
from ape.kalshi.ws_state import OrderbookState
from ape.repositories.inputs import WorkerHeartbeatInput
from ape.repositories.orderbook import OrderbookRepository
from ape.repositories.public_trades import PublicTradesRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.safety import SafetyAssessment

LOGGER = logging.getLogger(__name__)
MAX_HEARTBEAT_INTERVAL_SECONDS = 10.0
MIN_HEARTBEAT_INTERVAL_SECONDS = 1.0

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
        self._last_heartbeat_at: datetime | None = None

    async def run(
        self,
        *,
        stop_event: threading.Event,
        max_cycles: int | None = None,
    ) -> None:
        if not self.config.kalshi_ws_enabled:
            self.status.connection_state = "disabled"
            self.status.warnings = ["kalshi_ws_disabled"]
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

        if not diagnostic.signer_ready:
            self.status.connection_state = "not_configured"
            self.status.blockers = ["kalshi_ws_credentials_not_configured_or_not_parseable"]
            self.record_heartbeat()
            return

        if self.session_factory is None:
            self.status.connection_state = "not_configured"
            self.status.blockers = ["database_not_configured_for_ws_persistence"]
            self.record_heartbeat()
            return

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
            return
        except SQLAlchemyError as exc:
            self._set_error("resolver_database_error", exc)
            self.status.blockers = ["market_resolver_database_error"]
            self.record_heartbeat()
            return

        if resolver_result.state is not ResolverState.RESOLVED_OBSERVER_ONLY:
            self.status.connection_state = resolver_result.state.value
            self.status.blockers = resolver_result.blockers or [resolver_result.state.value]
            self.status.warnings = resolver_result.warnings
            self.status.active_market_ticker = (
                resolver_result.market.market_ticker if resolver_result.market else None
            )
            self.record_heartbeat()
            return

        if resolver_result.market is None:
            self.status.connection_state = "no_active_market"
            self.status.blockers = ["no_active_market"]
            self.record_heartbeat()
            return

        market_ticker = resolver_result.market.market_ticker
        self.status.active_market_ticker = market_ticker

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
                self.status.connection_state = "connected"
                self.status.last_connected_at = self.now()
                self.record_heartbeat()

                await self._subscribe(websocket, market_ticker)
                self.status.connection_state = "subscribed"
                self.record_heartbeat()

                await self._read_messages(
                    websocket,
                    market_ticker,
                    resolver_result.market.close_time,
                    stop_event,
                )
                self.status.reconnect_count = 0
            finally:
                await _close_websocket(websocket)
        except Exception as exc:
            self.status.reconnect_count += 1
            self._set_error(exc.__class__.__name__, exc)
            self.record_heartbeat()

    async def _subscribe(self, websocket: Any, market_ticker: str) -> None:
        request_id = 1
        subscribed_channels: list[str] = []
        subscription_ids: dict[str, int] = {}

        if self.config.kalshi_ws_subscribe_orderbook:
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
        if self.config.kalshi_ws_subscribe_ticker:
            secondary_channels.append("ticker")
        if self.config.kalshi_ws_subscribe_trades:
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

        self.status.subscribed_channels = subscribed_channels
        self.status.subscription_ids = subscription_ids

    async def _read_messages(
        self,
        websocket: Any,
        market_ticker: str,
        market_close_time: datetime | None,
        stop_event: threading.Event,
    ) -> None:
        orderbook = OrderbookState(market_ticker=market_ticker)
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
            self.record_heartbeat(force=False)

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
            self._persist_orderbook(snapshot)
            self.status.last_orderbook_at = received_at
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
            self._persist_orderbook(snapshot)
            self.status.last_orderbook_at = received_at
            return None

        if message.kind == "trade" and message.trade is not None:
            self._persist_trade(message.trade)
            self.status.last_trade_at = received_at
            if message.warning:
                self._add_warning(message.warning)
            return None

        if message.kind == "invalid":
            if message.reason == "kalshi_websocket_buffer_overflow":
                orderbook.reset()
                self._add_warning("orderbook_reset_after_buffer_overflow")
                self._add_warning(message.reason)
                return "orderbook_reset_after_buffer_overflow"
            self._add_warning(message.reason or "invalid_websocket_message")
            return None

        return None

    def _persist_orderbook(self, snapshot) -> None:
        if self.session_factory is None:
            self._add_warning("database_not_configured_for_orderbook")
            return

        with self.session_factory() as session:
            OrderbookRepository(session).insert_snapshot(snapshot)
            session.commit()

    def _persist_trade(self, trade) -> None:
        if self.session_factory is None:
            self._add_warning("database_not_configured_for_trades")
            return

        with self.session_factory() as session:
            PublicTradesRepository(session).insert_trade(trade)
            session.commit()

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
                            "mode": "kalshi_ws" if self.config.kalshi_ws_enabled else "idle",
                            "ws": self.status.as_metadata(),
                        },
                    )
                )
                session.commit()
        except SQLAlchemyError:
            LOGGER.warning("Kalshi worker heartbeat persistence failed.", exc_info=True)
            return

        self._last_heartbeat_at = heartbeat_at

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

    def _add_warning(self, warning: str) -> None:
        if warning not in self.status.warnings:
            self.status.warnings.append(warning)


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
