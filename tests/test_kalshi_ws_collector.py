from __future__ import annotations

import asyncio
import json
import threading
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.models import WorkerHeartbeat
from ape.db.session import create_engine_from_config, create_session_factory
from ape.kalshi.resolver import ResolverResult, ResolverState
from ape.kalshi.ws_collector import KalshiWsCollector
from ape.repositories.inputs import MarketInput
from ape.repositories.orderbook import OrderbookRepository
from ape.repositories.public_trades import PublicTradesRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.safety import assess_startup_safety

NOW = datetime(2026, 7, 5, 14, 35, tzinfo=UTC)


class FakeWebSocket:
    def __init__(self, messages: list[dict[str, Any]]) -> None:
        self.messages = [json.dumps(message) for message in messages]
        self.sent: list[dict[str, Any]] = []
        self.closed = False

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))

    async def close(self) -> None:
        self.closed = True

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        if not self.messages:
            raise StopAsyncIteration
        return self.messages.pop(0)


class AdvancingFakeWebSocket(FakeWebSocket):
    def __init__(self, messages: list[dict[str, Any]], advance_time) -> None:
        super().__init__(messages)
        self.advance_time = advance_time

    async def __anext__(self) -> str:
        self.advance_time()
        return await super().__anext__()


def test_collector_subscribes_and_persists_mock_messages(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_collector.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_WS_RECONNECT_SECONDS": "1",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    websocket = FakeWebSocket(
        [
            {
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": "KXBTC15M-TEST",
                    "yes_dollars_fp": [["0.60", "10"]],
                    "no_dollars_fp": [["0.65", "8"]],
                    "ts_ms": 1780000000000,
                },
            },
            {
                "type": "trade",
                "sid": 2,
                "seq": 2,
                "msg": {
                    "trade_id": "trade-1",
                    "market_ticker": "KXBTC15M-TEST",
                    "yes_price_dollars": "0.61",
                    "count_fp": "2",
                    "taker_side": "yes",
                    "ts_ms": 1780000001000,
                },
            },
        ]
    )

    async def websocket_factory(*_args):
        return websocket

    collector = KalshiWsCollector(
        config=config,
        safety=assess_startup_safety(config),
        session_factory=session_factory,
        started_at=NOW,
        websocket_factory=websocket_factory,
        resolver=_resolved_market,
        now=lambda: NOW,
    )

    try:
        asyncio.run(collector.run(stop_event=threading.Event(), max_cycles=1))

        with session_factory() as session:
            latest_book = OrderbookRepository(session).get_latest_snapshot("KXBTC15M-TEST")
            latest_trade = PublicTradesRepository(session).get_latest_trade("KXBTC15M-TEST")
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert latest_book is not None
            assert latest_book.yes_bid == Decimal("0.60000000")
            assert latest_book.yes_ask == Decimal("0.65000000")
            assert latest_book.book_status == "ok"
            assert latest_trade is not None
            assert latest_trade.trade_id == "trade-1"
            assert latest_trade.taker_side == "yes"
            assert heartbeat is not None
            assert heartbeat.metadata_["ws"]["connection_state"] == "subscribed"
            assert heartbeat.metadata_["ws"]["active_market_ticker"] == "KXBTC15M-TEST"

        assert websocket.sent == [
            {
                "id": 1,
                "cmd": "subscribe",
                "params": {
                    "channels": ["orderbook_delta"],
                    "market_ticker": "KXBTC15M-TEST",
                    "use_yes_price": True,
                },
            },
            {
                "id": 2,
                "cmd": "subscribe",
                "params": {
                    "channels": ["ticker", "trade"],
                    "market_ticker": "KXBTC15M-TEST",
                },
            },
        ]
    finally:
        engine.dispose()


def test_collector_catches_database_error_while_resolving_market(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_resolver_db_error.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_WS_RECONNECT_SECONDS": "1",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)

    def resolver(**_kwargs) -> ResolverResult:
        raise SQLAlchemyError("database restart")

    collector = KalshiWsCollector(
        config=config,
        safety=assess_startup_safety(config),
        session_factory=session_factory,
        started_at=NOW,
        resolver=resolver,
        now=lambda: NOW,
    )

    try:
        asyncio.run(collector.run(stop_event=threading.Event(), max_cycles=1))

        with session_factory() as session:
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert heartbeat is not None
            ws_metadata = heartbeat.metadata_["ws"]
            assert ws_metadata["connection_state"] == "error"
            assert ws_metadata["last_error_type"] == "resolver_database_error"
            assert "market_resolver_database_error" in ws_metadata["blockers"]
    finally:
        engine.dispose()


def test_collector_throttles_heartbeats_for_live_websocket_traffic(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_throttled_heartbeats.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_WS_RECONNECT_SECONDS": "1",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    current_time = NOW

    def advance_time() -> None:
        nonlocal current_time
        current_time = current_time + timedelta(seconds=1)

    websocket = AdvancingFakeWebSocket(
        [
            {
                "type": "ticker",
                "sid": 2,
                "seq": index,
                "msg": {"market_ticker": "KXBTC15M-TEST"},
            }
            for index in range(1, 51)
        ],
        advance_time,
    )

    async def websocket_factory(*_args):
        return websocket

    collector = KalshiWsCollector(
        config=config,
        safety=assess_startup_safety(config),
        session_factory=session_factory,
        started_at=NOW,
        websocket_factory=websocket_factory,
        resolver=_resolved_market,
        now=lambda: current_time,
    )

    try:
        asyncio.run(collector.run(stop_event=threading.Event(), max_cycles=1))

        with session_factory() as session:
            heartbeat_count = session.scalar(select(func.count()).select_from(WorkerHeartbeat))
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert heartbeat_count == 7
            assert heartbeat is not None
            assert heartbeat.metadata_["ws"]["last_message_at"] == "2026-07-05T14:35:50Z"
    finally:
        engine.dispose()


def test_collector_resubscribes_after_sequence_gap(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_sequence_gap.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_WS_RECONNECT_SECONDS": "0.01",
            "KALSHI_WS_MAX_RECONNECT_SECONDS": "0.01",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    first_websocket = FakeWebSocket(
        [
            {
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": "KXBTC15M-TEST",
                    "yes_dollars_fp": [["0.60", "10"]],
                    "no_dollars_fp": [["0.65", "8"]],
                },
            },
            {
                "type": "orderbook_delta",
                "sid": 1,
                "seq": 3,
                "msg": {
                    "market_ticker": "KXBTC15M-TEST",
                    "side": "yes",
                    "price_dollars": "0.61",
                    "delta_fp": "4",
                },
            },
        ]
    )
    second_websocket = FakeWebSocket(
        [
            {
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": "KXBTC15M-TEST",
                    "yes_dollars_fp": [["0.62", "12"]],
                    "no_dollars_fp": [["0.67", "7"]],
                },
            },
        ]
    )
    websocket_sequence = [first_websocket, second_websocket]

    async def websocket_factory(*_args):
        return websocket_sequence.pop(0)

    collector = KalshiWsCollector(
        config=config,
        safety=assess_startup_safety(config),
        session_factory=session_factory,
        started_at=NOW,
        websocket_factory=websocket_factory,
        resolver=_resolved_market,
        now=lambda: NOW,
    )

    try:
        asyncio.run(collector.run(stop_event=threading.Event(), max_cycles=2))

        with session_factory() as session:
            latest_book = OrderbookRepository(session).get_latest_snapshot("KXBTC15M-TEST")
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert latest_book is not None
            assert latest_book.sequence_number == 1
            assert latest_book.yes_bid == Decimal("0.62000000")
            assert latest_book.yes_ask == Decimal("0.67000000")
            assert heartbeat is not None
            assert heartbeat.metadata_["ws"]["connection_state"] == "subscribed"
            assert first_websocket.closed is True
            assert second_websocket.sent[0]["params"]["market_ticker"] == "KXBTC15M-TEST"
    finally:
        engine.dispose()


def test_collector_resets_backoff_after_successful_websocket_cycle(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_backoff_reset.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_WS_RECONNECT_SECONDS": "0.01",
            "KALSHI_WS_MAX_RECONNECT_SECONDS": "0.01",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    websocket = FakeWebSocket(
        [
            {
                "type": "ticker",
                "sid": 2,
                "seq": 1,
                "msg": {"market_ticker": "KXBTC15M-TEST"},
            }
        ]
    )
    websocket_factory_calls = 0

    async def websocket_factory(*_args):
        nonlocal websocket_factory_calls
        websocket_factory_calls += 1
        if websocket_factory_calls == 1:
            raise RuntimeError("temporary websocket outage")
        return websocket

    collector = KalshiWsCollector(
        config=config,
        safety=assess_startup_safety(config),
        session_factory=session_factory,
        started_at=NOW,
        websocket_factory=websocket_factory,
        resolver=_resolved_market,
        now=lambda: NOW,
    )

    try:
        asyncio.run(collector.run(stop_event=threading.Event(), max_cycles=2))

        assert websocket_factory_calls == 2
        assert collector.status.reconnect_count == 0
        assert collector.status.connection_state == "subscribed"
    finally:
        engine.dispose()


def test_collector_resets_orderbook_on_buffer_overflow(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_buffer_overflow.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_WS_RECONNECT_SECONDS": "1",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    websocket = FakeWebSocket(
        [
            {
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": "KXBTC15M-TEST",
                    "yes_dollars_fp": [["0.60", "10"]],
                    "no_dollars_fp": [["0.65", "8"]],
                },
            },
            {
                "type": "error",
                "sid": 1,
                "seq": 2,
                "msg": {"code": 25, "msg": "Subscription buffer overflow"},
            },
            {
                "type": "orderbook_delta",
                "sid": 1,
                "seq": 3,
                "msg": {
                    "market_ticker": "KXBTC15M-TEST",
                    "side": "yes",
                    "price_dollars": "0.61",
                    "delta_fp": "4",
                },
            },
        ]
    )

    async def websocket_factory(*_args):
        return websocket

    collector = KalshiWsCollector(
        config=config,
        safety=assess_startup_safety(config),
        session_factory=session_factory,
        started_at=NOW,
        websocket_factory=websocket_factory,
        resolver=_resolved_market,
        now=lambda: NOW,
    )

    try:
        asyncio.run(collector.run(stop_event=threading.Event(), max_cycles=1))

        with session_factory() as session:
            latest_book = OrderbookRepository(session).get_latest_snapshot("KXBTC15M-TEST")
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert latest_book is not None
            assert latest_book.sequence_number == 1
            assert heartbeat is not None
            warnings = heartbeat.metadata_["ws"]["warnings"]
            assert "orderbook_reset_after_buffer_overflow" in warnings
            assert "kalshi_websocket_buffer_overflow" in warnings
            assert "kalshi_ws_resubscribe_requested" in warnings
            assert "orderbook_delta_before_snapshot" not in warnings
            assert heartbeat.metadata_["ws"]["connection_state"] == "resubscribe_pending"
    finally:
        engine.dispose()


def test_collector_re_resolves_when_market_window_closes(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_market_roll.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_WS_RECONNECT_SECONDS": "1",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    websocket = FakeWebSocket(
        [
            {
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": "KXBTC15M-TEST",
                    "yes_dollars_fp": [["0.60", "10"]],
                    "no_dollars_fp": [["0.65", "8"]],
                },
            },
        ]
    )

    async def websocket_factory(*_args):
        return websocket

    def resolver(**_kwargs) -> ResolverResult:
        return _resolved_market(close_time=NOW)

    collector = KalshiWsCollector(
        config=config,
        safety=assess_startup_safety(config),
        session_factory=session_factory,
        started_at=NOW,
        websocket_factory=websocket_factory,
        resolver=resolver,
        now=lambda: NOW,
    )

    try:
        asyncio.run(collector.run(stop_event=threading.Event(), max_cycles=1))

        with session_factory() as session:
            latest_book = OrderbookRepository(session).get_latest_snapshot("KXBTC15M-TEST")
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert latest_book is None
            assert heartbeat is not None
            assert heartbeat.metadata_["ws"]["connection_state"] == "market_roll_reresolve"
            assert "active_market_window_closed" in heartbeat.metadata_["ws"]["warnings"]
            assert websocket.closed is True
    finally:
        engine.dispose()


def _resolved_market(*, close_time: datetime | None = None, **_kwargs) -> ResolverResult:
    return ResolverResult(
        state=ResolverState.RESOLVED_OBSERVER_ONLY,
        configured=True,
        signer_ready=True,
        series_ticker="KXBTC15M",
        query_scope={},
        market=MarketInput(
            market_ticker="KXBTC15M-TEST",
            series_ticker="KXBTC15M",
            close_time=close_time,
        ),
        boundary=None,
        blockers=[],
        warnings=[],
        resolver_decision_reason="test_resolved_market",
        parser_version="test",
        raw_payload_hash=None,
        persisted=False,
        resolved_at=NOW,
    )


def _test_private_key_pem() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
