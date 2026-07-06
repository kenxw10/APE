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
from ape.kalshi.reference_messages import BRTI_SOURCE
from ape.kalshi.resolver import ResolverResult, ResolverState
from ape.kalshi.ws_collector import KalshiWsCollector
from ape.kalshi.ws_status import build_kalshi_ws_status
from ape.repositories.inputs import MarketInput, WorkerHeartbeatInput
from ape.repositories.orderbook import OrderbookRepository
from ape.repositories.public_trades import PublicTradesRepository
from ape.repositories.reference_ticks import ReferenceTicksRepository
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
                    "yes_dollars_fp": [["0.6000", "10.00"]],
                    "no_dollars_fp": [["0.6500", "8.00"]],
                    "ts_ms": 1780000000000,
                },
            },
            {
                "type": "orderbook_delta",
                "sid": 1,
                "seq": 2,
                "msg": {
                    "market_ticker": "KXBTC15M-TEST",
                    "side": "yes",
                    "price_dollars": "0.6200",
                    "delta_fp": "4.25",
                    "ts_ms": 1780000000500,
                },
            },
            {
                "type": "trade",
                "sid": 2,
                "seq": 3,
                "msg": {
                    "trade_id": "trade-1",
                    "market_ticker": "KXBTC15M-TEST",
                    "yes_price_dollars": "0.61",
                    "count_fp": "2.50",
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
            assert latest_book.sequence_number == 2
            assert latest_book.yes_bid == Decimal("0.62000000")
            assert latest_book.yes_ask == Decimal("0.65000000")
            assert latest_book.yes_bid_size is None
            assert latest_book.yes_bid_count == Decimal("4.25000000")
            assert latest_book.book_status == "ok"
            assert latest_trade is not None
            assert latest_trade.trade_id == "trade-1"
            assert latest_trade.count is None
            assert latest_trade.trade_count == Decimal("2.50000000")
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


def test_collector_heartbeat_preserves_strategy_metadata(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_preserve_strategy.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_WS_ENABLED": "true",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)

    try:
        with session_factory() as session:
            WorkerHeartbeatRepository(session).record_heartbeat(
                WorkerHeartbeatInput(
                    service_name="ape-worker",
                    started_at=NOW - timedelta(minutes=1),
                    heartbeat_at=NOW - timedelta(seconds=1),
                    app_mode="OBSERVER",
                    is_safe=True,
                    metadata={
                        "mode": "strategy_observer",
                        "strategy": {
                            "observer": {
                                "enabled": True,
                                "connection_state": "running",
                                "last_decision_id": "strategy-1",
                            }
                        },
                    },
                )
            )
            session.commit()

        collector = KalshiWsCollector(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=session_factory,
            started_at=NOW,
            now=lambda: NOW,
        )
        collector.status.connection_state = "subscribed"
        collector.record_heartbeat()

        with session_factory() as session:
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert heartbeat is not None
            assert heartbeat.metadata_["ws"]["connection_state"] == "subscribed"
            assert heartbeat.metadata_["strategy"]["observer"]["last_decision_id"] == "strategy-1"
    finally:
        engine.dispose()


def test_collector_subscribes_to_brti_with_market_channels_and_persists_tick(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_brti_market.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_DEDICATED_CONNECTION": "false",
            "KALSHI_WS_RECONNECT_SECONDS": "1",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    websocket = FakeWebSocket(
        [
            {
                "type": "subscribed",
                "id": 3,
                "msg": {
                    "sid": 99,
                    "channel": "cfbenchmarks_value",
                },
            },
            _brti_payload(sid=99),
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
            latest_tick = ReferenceTicksRepository(session).get_latest_tick(BRTI_SOURCE)
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert latest_tick is not None
            assert latest_tick.parsed_value == Decimal("68000.12000000")
            assert latest_tick.trailing_60s_avg == Decimal("67999.50000000")
            assert latest_tick.final_minute_average_status == "absent"
            assert heartbeat is not None
            brti_metadata = heartbeat.metadata_["reference"]["brti"]
            assert brti_metadata["connection_state"] == "subscribed"
            assert brti_metadata["subscription_id"] == 99
            assert brti_metadata["subscription_request_id"] == 3
            assert brti_metadata["latest_value"] == "68000.12"

        assert websocket.sent[-1] == {
            "id": 3,
            "cmd": "subscribe",
            "params": {
                "channels": ["cfbenchmarks_value"],
                "index_ids": ["BRTI"],
            },
        }
        assert "market_ticker" not in websocket.sent[-1]["params"]
    finally:
        engine.dispose()


def test_collector_uses_dedicated_brti_connection_by_default(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_brti_dedicated.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_WS_RECONNECT_SECONDS": "1",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    market_websocket = FakeWebSocket(
        [
            {
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": "KXBTC15M-TEST",
                    "yes_dollars_fp": [["0.6000", "10.00"]],
                    "no_dollars_fp": [["0.6500", "8.00"]],
                    "ts_ms": 1780000000000,
                },
            }
        ]
    )
    brti_websocket = FakeWebSocket([_brti_payload(sid=1)])
    websocket_sequence = [market_websocket, brti_websocket]

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
        asyncio.run(collector.run(stop_event=threading.Event(), max_cycles=1))

        with session_factory() as session:
            latest_book = OrderbookRepository(session).get_latest_snapshot("KXBTC15M-TEST")
            latest_tick = ReferenceTicksRepository(session).get_latest_tick(BRTI_SOURCE)
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert latest_book is not None
            assert latest_tick is not None
            assert latest_tick.parse_status == "valid"
            assert heartbeat is not None
            ws_metadata = heartbeat.metadata_["ws"]
            assert ws_metadata["subscribed_channels"] == ["orderbook_delta", "ticker", "trade"]
            brti_metadata = heartbeat.metadata_["reference"]["brti"]
            assert brti_metadata["subscribed_channels"] == ["cfbenchmarks_value"]
            assert brti_metadata["subscription_request_id"] == 1
            assert brti_metadata["connection_state"] == "subscribed"

        assert market_websocket.sent == [
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
        assert brti_websocket.sent == [
            {
                "id": 1,
                "cmd": "subscribe",
                "params": {
                    "channels": ["cfbenchmarks_value"],
                    "index_ids": ["BRTI"],
                },
            }
        ]
        assert "market_ticker" not in brti_websocket.sent[0]["params"]
    finally:
        engine.dispose()


def test_collector_dedicated_brti_failure_does_not_stop_market_ws(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_brti_failure_market_ok.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_WS_RECONNECT_SECONDS": "1",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    market_websocket = FakeWebSocket(
        [
            {
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": "KXBTC15M-TEST",
                    "yes_dollars_fp": [["0.6000", "10.00"]],
                    "no_dollars_fp": [["0.6500", "8.00"]],
                },
            }
        ]
    )
    brti_websocket = FakeWebSocket(
        [
            {
                "type": "error",
                "id": 1,
                "msg": {
                    "code": 403,
                    "msg": "missing entitlement for BRTI",
                },
            }
        ]
    )
    websocket_sequence = [market_websocket, brti_websocket]

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
        asyncio.run(collector.run(stop_event=threading.Event(), max_cycles=1))

        with session_factory() as session:
            latest_book = OrderbookRepository(session).get_latest_snapshot("KXBTC15M-TEST")
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert latest_book is not None
            assert heartbeat is not None
            assert heartbeat.metadata_["ws"]["connection_state"] == "subscribed"
            brti_metadata = heartbeat.metadata_["reference"]["brti"]
            assert brti_metadata["connection_state"] == "error"
            assert (
                brti_metadata["last_error_type"]
                == "kalshi_cfbenchmarks_subscription_error"
            )
            assert "missing entitlement" in brti_metadata["last_error_message"]
    finally:
        engine.dispose()


def test_collector_market_roll_does_not_stop_dedicated_brti(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_market_roll_brti_ok.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_WS_RECONNECT_SECONDS": "1",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    market_websocket = FakeWebSocket([])
    brti_websocket = FakeWebSocket([_brti_payload(sid=1)])
    websocket_sequence = [market_websocket, brti_websocket]

    async def websocket_factory(*_args):
        return websocket_sequence.pop(0)

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
            latest_tick = ReferenceTicksRepository(session).get_latest_tick(BRTI_SOURCE)
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert latest_tick is not None
            assert latest_tick.parse_status == "valid"
            assert heartbeat is not None
            assert heartbeat.metadata_["ws"]["connection_state"] == "market_roll_reresolve"
            assert heartbeat.metadata_["reference"]["brti"]["connection_state"] == "subscribed"
            assert market_websocket.closed is True
    finally:
        engine.dispose()


def test_collector_matches_brti_subscribe_error_by_request_id_with_market_channels(
    tmp_path,
) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_brti_error_by_id.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_DEDICATED_CONNECTION": "false",
            "KALSHI_WS_RECONNECT_SECONDS": "1",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    websocket = FakeWebSocket(
        [
            {
                "type": "error",
                "id": 3,
                "msg": {
                    "code": 403,
                    "msg": "missing entitlement for BRTI",
                },
            }
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
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert heartbeat is not None
            ws_metadata = heartbeat.metadata_["ws"]
            assert "kalshi_websocket_error" not in ws_metadata["warnings"]
            brti_metadata = heartbeat.metadata_["reference"]["brti"]
            assert brti_metadata["connection_state"] == "error"
            assert (
                brti_metadata["last_error_type"]
                == "kalshi_cfbenchmarks_subscription_error"
            )
            assert "missing entitlement" in brti_metadata["last_error_message"]
            assert "kalshi_cfbenchmarks_subscription_error" in brti_metadata["warnings"]
            assert "kalshi_cfbenchmarks_subscription_error" in brti_metadata["blockers"]
            assert brti_metadata["subscription_request_id"] == 3
    finally:
        engine.dispose()


def test_collector_does_not_consume_sidless_market_error_as_brti(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_sidless_market_error.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_DEDICATED_CONNECTION": "false",
            "KALSHI_WS_RECONNECT_SECONDS": "1",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    websocket = FakeWebSocket(
        [
            {
                "type": "error",
                "msg": {
                    "code": 403,
                    "msg": "market subscription rejected",
                },
            }
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
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert heartbeat is not None
            ws_metadata = heartbeat.metadata_["ws"]
            assert "kalshi_websocket_error" in ws_metadata["warnings"]
            brti_metadata = heartbeat.metadata_["reference"]["brti"]
            assert brti_metadata["connection_state"] == "subscribed"
            assert brti_metadata["last_error_type"] is None
            assert "kalshi_cfbenchmarks_subscription_error" not in brti_metadata["warnings"]
            assert "kalshi_cfbenchmarks_subscription_error" not in brti_metadata["blockers"]
    finally:
        engine.dispose()


def test_collector_can_persist_brti_without_market_websocket_enabled(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_brti_only.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_WS_RECONNECT_SECONDS": "1",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    websocket = FakeWebSocket([_brti_payload()])

    async def websocket_factory(*_args):
        return websocket

    def resolver(**_kwargs) -> ResolverResult:
        raise AssertionError("market resolver should not run for BRTI-only collection")

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
            latest_tick = ReferenceTicksRepository(session).get_latest_tick(BRTI_SOURCE)
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert latest_tick is not None
            assert latest_tick.parse_status == "valid"
            assert heartbeat is not None
            assert heartbeat.metadata_["mode"] == "reference_ws"
            assert heartbeat.metadata_["ws"]["connection_state"] == "disabled"
            assert heartbeat.metadata_["reference"]["brti"]["connection_state"] == "subscribed"

        assert websocket.sent == [
            {
                "id": 1,
                "cmd": "subscribe",
                "params": {
                    "channels": ["cfbenchmarks_value"],
                    "index_ids": ["BRTI"],
                },
            }
        ]
    finally:
        engine.dispose()


def test_collector_starts_brti_when_market_resolution_fails(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_brti_no_market.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_WS_RECONNECT_SECONDS": "1",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    websocket = FakeWebSocket([_brti_payload()])

    async def websocket_factory(*_args):
        return websocket

    collector = KalshiWsCollector(
        config=config,
        safety=assess_startup_safety(config),
        session_factory=session_factory,
        started_at=NOW,
        websocket_factory=websocket_factory,
        resolver=_no_active_market,
        now=lambda: NOW,
    )

    try:
        asyncio.run(collector.run(stop_event=threading.Event(), max_cycles=1))

        with session_factory() as session:
            latest_tick = ReferenceTicksRepository(session).get_latest_tick(BRTI_SOURCE)
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert latest_tick is not None
            assert latest_tick.parse_status == "valid"
            assert heartbeat is not None
            assert heartbeat.metadata_["ws"]["connection_state"] == "no_active_market"
            assert "no_active_market" in heartbeat.metadata_["ws"]["blockers"]
            brti_metadata = heartbeat.metadata_["reference"]["brti"]
            assert brti_metadata["connection_state"] == "subscribed"
            assert brti_metadata["latest_value"] == "68000.12"

        assert websocket.sent == [
            {
                "id": 1,
                "cmd": "subscribe",
                "params": {
                    "channels": ["cfbenchmarks_value"],
                    "index_ids": ["BRTI"],
                },
            }
        ]
    finally:
        engine.dispose()


def test_collector_surfaces_brti_subscription_error_without_market_ticker(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_brti_error.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_WS_RECONNECT_SECONDS": "1",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    websocket = FakeWebSocket(
        [
            {
                "type": "error",
                "id": 1,
                "seq": 1,
                "msg": {
                    "code": 403,
                    "msg": "missing entitlement for BRTI",
                },
            }
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
        now=lambda: NOW,
    )

    try:
        asyncio.run(collector.run(stop_event=threading.Event(), max_cycles=1))

        with session_factory() as session:
            latest_tick = ReferenceTicksRepository(session).get_latest_tick(BRTI_SOURCE)
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert latest_tick is None
            assert heartbeat is not None
            brti_metadata = heartbeat.metadata_["reference"]["brti"]
            assert brti_metadata["connection_state"] == "error"
            assert (
                brti_metadata["last_error_type"]
                == "kalshi_cfbenchmarks_subscription_error"
            )
            assert "missing entitlement" in brti_metadata["last_error_message"]
            assert "kalshi_cfbenchmarks_subscription_error" in brti_metadata["warnings"]
            assert "kalshi_cfbenchmarks_subscription_error" in brti_metadata["blockers"]
    finally:
        engine.dispose()


def test_collector_persists_malformed_brti_tick_without_crashing(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_brti_malformed.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_WS_RECONNECT_SECONDS": "1",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    websocket = FakeWebSocket([_brti_payload(value="bad")])

    async def websocket_factory(*_args):
        return websocket

    collector = KalshiWsCollector(
        config=config,
        safety=assess_startup_safety(config),
        session_factory=session_factory,
        started_at=NOW,
        websocket_factory=websocket_factory,
        now=lambda: NOW,
    )

    try:
        asyncio.run(collector.run(stop_event=threading.Event(), max_cycles=1))

        with session_factory() as session:
            latest_tick = ReferenceTicksRepository(session).get_latest_tick(BRTI_SOURCE)
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert latest_tick is not None
            assert latest_tick.parse_status == "malformed_value"
            assert latest_tick.parsed_value is None
            assert heartbeat is not None
            assert "brti_malformed_value" in heartbeat.metadata_["reference"]["brti"]["warnings"]
    finally:
        engine.dispose()


def test_collector_skips_duplicate_or_out_of_order_brti_source_timestamp(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_brti_duplicate.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_WS_RECONNECT_SECONDS": "1",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    websocket = FakeWebSocket([_brti_payload(seq=1), _brti_payload(seq=2)])

    async def websocket_factory(*_args):
        return websocket

    collector = KalshiWsCollector(
        config=config,
        safety=assess_startup_safety(config),
        session_factory=session_factory,
        started_at=NOW,
        websocket_factory=websocket_factory,
        now=lambda: NOW,
    )

    try:
        asyncio.run(collector.run(stop_event=threading.Event(), max_cycles=1))

        with session_factory() as session:
            ticks = ReferenceTicksRepository(session).get_recent_ticks(BRTI_SOURCE, limit=10)
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert len(ticks) == 1
            assert ticks[0].sequence_number == 1
            assert heartbeat is not None
            assert (
                "brti_duplicate_or_out_of_order_source_ts"
                in heartbeat.metadata_["reference"]["brti"]["warnings"]
            )
    finally:
        engine.dispose()


def test_collector_skips_duplicate_brti_after_malformed_tick_without_source_ts(
    tmp_path,
) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_brti_duplicate_after_null.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_WS_RECONNECT_SECONDS": "1",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    websocket = FakeWebSocket(
        [
            _brti_payload(seq=1),
            _brti_payload(seq=2, value="bad", include_source_ts=False),
            _brti_payload(seq=3),
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
        now=lambda: NOW,
    )

    try:
        asyncio.run(collector.run(stop_event=threading.Event(), max_cycles=1))

        with session_factory() as session:
            ticks = ReferenceTicksRepository(session).get_recent_ticks(BRTI_SOURCE, limit=10)
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            sequence_numbers = {tick.sequence_number for tick in ticks}
            assert sequence_numbers == {1, 2}
            assert all(tick.sequence_number != 3 for tick in ticks)
            assert any(tick.source_ts is None for tick in ticks)
            assert heartbeat is not None
            assert (
                "brti_duplicate_or_out_of_order_source_ts"
                in heartbeat.metadata_["reference"]["brti"]["warnings"]
            )
    finally:
        engine.dispose()


def test_collector_clears_stale_brti_error_after_successful_persist(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_brti_error_clear.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_WS_RECONNECT_SECONDS": "1",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    websocket = FakeWebSocket([_brti_payload()])

    async def websocket_factory(*_args):
        return websocket

    collector = KalshiWsCollector(
        config=config,
        safety=assess_startup_safety(config),
        session_factory=session_factory,
        started_at=NOW,
        websocket_factory=websocket_factory,
        now=lambda: NOW,
    )
    collector.brti_status.last_error_type = "SQLAlchemyError"
    collector.brti_status.last_error_message = "old reference persistence failure"

    try:
        asyncio.run(collector.run(stop_event=threading.Event(), max_cycles=1))

        with session_factory() as session:
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert heartbeat is not None
            brti_metadata = heartbeat.metadata_["reference"]["brti"]
            assert brti_metadata["last_error_type"] is None
            assert brti_metadata["last_error_message"] is None
            assert brti_metadata["warnings"] == []
    finally:
        engine.dispose()


def test_collector_records_bounded_safe_parse_diagnostic_samples(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_parse_diagnostics.sqlite'}"
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
                    "yes_dollars_fp": [[{"value": "0.60"}, "10.00"]],
                    "no_dollars_fp": [["0.65", "8.00"]],
                    "debug_secret": "PRIVATE KEY BLOCK",
                },
            },
            {
                "type": "orderbook_delta",
                "sid": 1,
                "seq": 2,
                "msg": {
                    "market_ticker": "KXBTC15M-TEST",
                    "side": "yes",
                    "price_dollars": "0.61",
                    "delta_fp": {"value": "4.00"},
                    "access_signature": "KALSHI-ACCESS-SIGNATURE",
                },
            },
            {
                "type": "trade",
                "sid": 2,
                "seq": 3,
                "msg": {
                    "trade_id": "trade-1",
                    "market_ticker": "KXBTC15M-TEST",
                    "yes_price_dollars": "0.61",
                    "count_fp": "2.345",
                },
            },
            {
                "type": "trade",
                "sid": 2,
                "seq": 4,
                "msg": {
                    "trade_id": "trade-2",
                    "market_ticker": "KXBTC15M-TEST",
                    "yes_price_dollars": {"value": "0.61"},
                    "count_fp": "2.00",
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
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert heartbeat is not None
            samples = heartbeat.metadata_["ws"]["diagnostic_samples"]
            assert len(samples) == 3
            assert all("raw_payload" not in sample for sample in samples)
            assert all(sample["raw_payload_hash"] for sample in samples)
            assert {sample["reason"] for sample in samples} == {
                "invalid_orderbook_delta_delta_fp",
                "invalid_trade_count_fp",
                "invalid_trade_price",
            }

            rendered_samples = json.dumps(samples)
            assert "PRIVATE KEY BLOCK" not in rendered_samples
            assert "KALSHI-ACCESS-SIGNATURE" not in rendered_samples
            assert "access_signature" not in rendered_samples
    finally:
        engine.dispose()


def test_collector_throttles_repeated_invalid_parse_heartbeats(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_repeated_invalid.sqlite'}"
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
                "seq": index,
                "msg": {
                    "market_ticker": "KXBTC15M-TEST",
                    "yes_dollars_fp": [["0.6000", {"value": "10.00"}]],
                },
            }
            for index in range(1, 21)
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
            heartbeat_count = session.scalar(select(func.count()).select_from(WorkerHeartbeat))
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert heartbeat_count == 3
            assert heartbeat is not None
            ws_metadata = heartbeat.metadata_["ws"]
            assert ws_metadata["warnings"] == ["invalid_orderbook_snapshot_yes_level_size"]
            assert len(ws_metadata["diagnostic_samples"]) == 1
            assert (
                ws_metadata["diagnostic_samples"][0]["reason"]
                == "invalid_orderbook_snapshot_yes_level_size"
            )
    finally:
        engine.dispose()


def test_collector_persists_valid_snapshot_after_invalid_live_like_snapshot(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_snapshot_recovery.sqlite'}"
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
                    "yes_dollars_fp": [["0.6000", {"value": "10.00"}]],
                },
            },
            {
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 2,
                "msg": {
                    "market_ticker": "KXBTC15M-TEST",
                    "yes_dollars_fp": [["0.6000", "1,200.50"]],
                    "no_dollars_fp": [["0.6500", "8.00"]],
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
            assert latest_book.sequence_number == 2
            assert latest_book.yes_bid_size is None
            assert latest_book.yes_bid_count == Decimal("1200.50000000")
            assert heartbeat is not None
            assert (
                "invalid_orderbook_snapshot_yes_level_size"
                not in heartbeat.metadata_["ws"]["warnings"]
            )
            assert heartbeat.metadata_["ws"]["last_orderbook_at"] == "2026-07-05T14:35:00Z"
    finally:
        engine.dispose()


def test_collector_clears_stale_error_after_successful_orderbook_persist(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_orderbook_error_clear.sqlite'}"
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

    collector = KalshiWsCollector(
        config=config,
        safety=assess_startup_safety(config),
        session_factory=session_factory,
        started_at=NOW,
        websocket_factory=websocket_factory,
        resolver=_resolved_market,
        now=lambda: NOW,
    )
    collector.status.last_error_type = "ProgrammingError"
    collector.status.last_error_message = "UndefinedColumn: orderbook_snapshots.yes_bid_count"

    try:
        asyncio.run(collector.run(stop_event=threading.Event(), max_cycles=1))

        with session_factory() as session:
            latest_book = OrderbookRepository(session).get_latest_snapshot("KXBTC15M-TEST")
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert latest_book is not None
            assert heartbeat is not None
            ws_metadata = heartbeat.metadata_["ws"]
            assert ws_metadata["last_error_type"] is None
            assert ws_metadata["last_error_message"] is None
            assert ws_metadata["warnings"] == []
            assert ws_metadata["blockers"] == []

        status = build_kalshi_ws_status(config, now=NOW)
        assert status.last_error_type is None
        assert status.last_error_message is None
    finally:
        engine.dispose()


def test_collector_clears_stale_error_after_successful_trade_persist(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_trade_error_clear.sqlite'}"
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
                "type": "trade",
                "sid": 2,
                "seq": 1,
                "msg": {
                    "trade_id": "trade-1",
                    "market_ticker": "KXBTC15M-TEST",
                    "yes_price_dollars": "0.61",
                    "count_fp": "2.50",
                    "taker_side": "yes",
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
    collector.status.last_error_type = "ProgrammingError"
    collector.status.last_error_message = "UndefinedColumn: orderbook_snapshots.yes_bid_count"

    try:
        asyncio.run(collector.run(stop_event=threading.Event(), max_cycles=1))

        with session_factory() as session:
            latest_trade = PublicTradesRepository(session).get_latest_trade("KXBTC15M-TEST")
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert latest_trade is not None
            assert heartbeat is not None
            ws_metadata = heartbeat.metadata_["ws"]
            assert ws_metadata["last_error_type"] is None
            assert ws_metadata["last_error_message"] is None
            assert ws_metadata["warnings"] == []
            assert ws_metadata["blockers"] == []
    finally:
        engine.dispose()


def test_collector_does_not_clear_db_error_on_ticker_only_message(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_ticker_no_clear.sqlite'}"
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
                "type": "ticker",
                "sid": 2,
                "seq": 1,
                "msg": {"market_ticker": "KXBTC15M-TEST"},
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
    collector.status.last_error_type = "ProgrammingError"
    collector.status.last_error_message = "UndefinedColumn: orderbook_snapshots.yes_bid_count"

    try:
        asyncio.run(collector.run(stop_event=threading.Event(), max_cycles=1))

        assert collector.status.last_error_type == "ProgrammingError"
        assert collector.status.last_error_message is not None
    finally:
        engine.dispose()


def test_collector_records_orderbook_persistence_failure(monkeypatch, tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_orderbook_failure.sqlite'}"
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

    def fail_insert(self, snapshot) -> None:
        raise SQLAlchemyError("orderbook insert failed")

    monkeypatch.setattr(OrderbookRepository, "insert_snapshot", fail_insert)

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

            assert latest_book is None
            assert heartbeat is not None
            ws_metadata = heartbeat.metadata_["ws"]
            assert ws_metadata["connection_state"] == "error"
            assert ws_metadata["last_error_type"] == "SQLAlchemyError"
            assert "orderbook_persistence_failed" in ws_metadata["warnings"]
            assert "orderbook_persistence_failed" in ws_metadata["blockers"]
            assert ws_metadata["last_orderbook_at"] is None
    finally:
        engine.dispose()


def test_collector_clears_orderbook_persistence_failure_after_success(
    monkeypatch,
    tmp_path,
) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ws_orderbook_recovery.sqlite'}"
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
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 2,
                "msg": {
                    "market_ticker": "KXBTC15M-TEST",
                    "yes_dollars_fp": [["0.62", "11"]],
                    "no_dollars_fp": [["0.67", "9"]],
                },
            },
        ]
    )
    original_insert = OrderbookRepository.insert_snapshot
    attempts = 0

    def flaky_insert(self, snapshot):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise SQLAlchemyError("temporary orderbook insert failure")
        return original_insert(self, snapshot)

    monkeypatch.setattr(OrderbookRepository, "insert_snapshot", flaky_insert)

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

            assert attempts == 2
            assert latest_book is not None
            assert latest_book.sequence_number == 2
            assert heartbeat is not None
            ws_metadata = heartbeat.metadata_["ws"]
            assert ws_metadata["connection_state"] == "subscribed"
            assert ws_metadata["last_error_type"] is None
            assert ws_metadata["last_error_message"] is None
            assert "orderbook_persistence_failed" not in ws_metadata["warnings"]
            assert "orderbook_persistence_failed" not in ws_metadata["blockers"]
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


def _no_active_market(**_kwargs) -> ResolverResult:
    return ResolverResult(
        state=ResolverState.NO_ACTIVE_MARKET,
        configured=True,
        signer_ready=True,
        series_ticker="KXBTC15M",
        query_scope={},
        market=None,
        boundary=None,
        blockers=["no_active_market"],
        warnings=[],
        resolver_decision_reason="test_no_active_market",
        parser_version="test",
        raw_payload_hash=None,
        persisted=False,
        resolved_at=NOW,
    )


def _brti_payload(
    *,
    seq: int = 7,
    sid: int = 3,
    value: str = "68000.12",
    include_source_ts: bool = True,
) -> dict[str, Any]:
    source_ts = NOW.replace(second=0)
    source_ts_ms = int(source_ts.timestamp() * 1000)
    data = {
        "type": "value",
        "id": "BRTI",
        "value": value,
    }
    if include_source_ts:
        data["time"] = source_ts_ms
    return {
        "type": "cfbenchmarks_value",
        "sid": sid,
        "seq": seq,
        "msg": {
            "index_id": "BRTI",
            "received_at": "2026-07-05T14:35:00Z",
            "data": json.dumps(data),
            "avg_60s_data": {
                "value": "67999.50",
                "window_size": 60,
                "window_start_ts_ms": source_ts_ms - 60_000,
                "window_end_ts_exclusive": source_ts_ms,
            },
        },
    }


def _test_private_key_pem() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
