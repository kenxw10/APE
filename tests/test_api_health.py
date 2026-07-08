from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

from ape.api.main import create_app
from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.inputs import OrderbookSnapshotInput, PublicTradeInput, WorkerHeartbeatInput
from ape.repositories.markets import MarketsRepository
from ape.repositories.orderbook import OrderbookRepository
from ape.repositories.public_trades import PublicTradesRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository


def test_health_works_without_secrets() -> None:
    app = create_app(load_config({}))

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["service"] == "ape-api"
    assert body["environment"] == "local"
    assert body["app_mode"] == "OBSERVER"
    assert body["safety"]["is_safe"] is True
    assert body["safety"]["blockers"] == []


def test_safety_works_without_secrets() -> None:
    app = create_app(load_config({}))

    with TestClient(app) as client:
        response = client.get("/safety")

    assert response.status_code == 200
    body = response.json()
    assert body["mode"] == "OBSERVER"
    assert body["trading_enabled"] is False
    assert body["execute"] is False
    assert body["is_safe"] is True
    assert body["blockers"] == []


def test_db_status_is_not_configured_without_database_url() -> None:
    app = create_app(load_config({}))

    with TestClient(app) as client:
        response = client.get("/db/status")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "not_configured"
    assert body["configured"] is False


def test_db_status_checks_configured_database(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_status.sqlite'}"
    app = create_app(load_config({"DATABASE_URL": database_url}))

    with TestClient(app) as client:
        response = client.get("/db/status")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["configured"] is True


def test_readiness_is_not_ready_without_database_url() -> None:
    app = create_app(load_config({}))

    with TestClient(app) as client:
        response = client.get("/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "not_ready"
    assert body["ready"] is False
    assert body["safety"]["is_safe"] is True
    assert body["database"]["status"] == "not_configured"
    assert body["database"]["configured"] is False


def test_readiness_is_ready_with_configured_database(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_ready.sqlite'}"
    engine = create_engine_from_config(load_config({"DATABASE_URL": database_url}))
    try:
        run_migrations(engine)
    finally:
        engine.dispose()

    app = create_app(load_config({"DATABASE_URL": database_url}))

    with TestClient(app) as client:
        response = client.get("/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert body["ready"] is True
    assert body["safety"]["is_safe"] is True
    assert body["database"]["status"] == "ok"
    assert body["database"]["configured"] is True


def test_readiness_is_blocked_when_safety_is_unsafe() -> None:
    app = create_app(load_config({"EXECUTE": "true"}))
    client = TestClient(app)

    response = client.get("/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "blocked"
    assert body["ready"] is False
    assert body["safety"]["is_safe"] is False


def test_kalshi_status_is_safe_without_credentials() -> None:
    app = create_app(load_config({}))

    with TestClient(app) as client:
        response = client.get("/kalshi/status")

    assert response.status_code == 200
    body = response.json()
    assert body["configured"] is False
    assert body["signer_ready"] is False
    assert body["api_key_configured"] is False
    assert body["private_key_configured"] is False
    assert body["private_key_parseable"] is False
    assert body["series_ticker"] == "KXBTC15M"
    assert "PRIVATE KEY" not in response.text
    assert "KALSHI-ACCESS-SIGNATURE" not in response.text


def test_active_market_is_not_configured_without_credentials() -> None:
    app = create_app(load_config({}))

    with TestClient(app) as client:
        response = client.get("/markets/active")

    assert response.status_code == 200
    body = response.json()
    assert body["state"] == "not_configured"
    assert body["configured"] is False
    assert body["market"] is None


def test_active_market_uses_mocked_kalshi_client_and_persists_to_db(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_api_active_market.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
        }
    )
    engine = create_engine_from_config(config)
    try:
        run_migrations(engine)
    finally:
        engine.dispose()

    app = create_app(config)
    app.state.kalshi_client_factory = lambda _settings: _FakeKalshiClient()

    with TestClient(app) as client:
        response = client.get("/markets/active")

    assert response.status_code == 200
    body = response.json()
    assert body["state"] == "resolved_observer_only"
    assert body["configured"] is True
    assert body["signer_ready"] is True
    assert body["market"]["market_ticker"] == "KXBTC15M-ACTIVE"
    assert body["market"]["series_ticker"] == "KXBTC15M"
    assert body["market"]["price_level_structure"] == "binary"
    assert body["persisted"] is True
    assert "PRIVATE KEY" not in response.text
    assert "KALSHI-ACCESS-SIGNATURE" not in response.text

    engine = create_engine_from_config(config)
    try:
        session_factory = create_session_factory(engine)
        with session_factory() as session:
            stored = MarketsRepository(session).get_market_by_ticker("KXBTC15M-ACTIVE")
            assert stored is not None
            assert stored.raw_payload_hash == body["raw_payload_hash"]
            assert stored.series_ticker == "KXBTC15M"
            assert stored.price_level_structure == "binary"
    finally:
        engine.dispose()


def test_ws_status_is_safe_when_disabled_without_credentials() -> None:
    app = create_app(load_config({}))

    with TestClient(app) as client:
        response = client.get("/ws/status")

    assert response.status_code == 200
    body = response.json()
    assert body["enabled"] is False
    assert body["configured"] is False
    assert body["connection_state"] == "disabled"
    assert body["stale"] is False
    assert "PRIVATE KEY" not in response.text
    assert "KALSHI-ACCESS-SIGNATURE" not in response.text


def test_ws_status_reports_persisted_market_data(tmp_path) -> None:
    now = datetime.now(UTC)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_api_ws_status.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            OrderbookRepository(session).insert_snapshot(
                OrderbookSnapshotInput(
                    market_ticker="KXBTC15M-ACTIVE",
                    received_at=now,
                    yes_bid=Decimal("0.60"),
                    yes_ask=Decimal("0.65"),
                    book_status="ok",
                )
            )
            PublicTradesRepository(session).insert_trade(
                PublicTradeInput(
                    market_ticker="KXBTC15M-ACTIVE",
                    trade_id="trade-1",
                    received_at=now,
                    executed_at=now,
                    price=Decimal("0.61"),
                    count=2,
                    taker_side="yes",
                )
            )
            WorkerHeartbeatRepository(session).record_heartbeat(
                WorkerHeartbeatInput(
                    service_name="ape-worker",
                    started_at=now - timedelta(minutes=1),
                    heartbeat_at=now,
                    app_mode="OBSERVER",
                    is_safe=True,
                    metadata={
                        "mode": "kalshi_ws",
                        "ws": {
                            "enabled": True,
                            "configured": True,
                            "signer_ready": True,
                            "connection_state": "subscribed",
                            "active_market_ticker": "KXBTC15M-ACTIVE",
                            "subscribed_channels": ["ticker", "orderbook_delta", "trade"],
                            "subscription_ids": {"ticker": 2, "orderbook_delta": 1, "trade": 2},
                            "last_message_at": _isoformat_z(now),
                            "transport_alive": True,
                            "transport_last_pong_at": _isoformat_z(now),
                            "transport_age_ms": 0,
                            "transport_liveness_reason": None,
                            "last_market_data_message_at": _isoformat_z(now),
                            "market_data_message_age_ms": 0,
                            "last_orderbook_at": _isoformat_z(now),
                            "last_trade_at": _isoformat_z(now),
                            "market_feed_transport_state": "healthy",
                            "market_feed_subscription_state": "subscribed",
                            "market_feed_snapshot_state": "initialized",
                            "market_feed_active_ticker_state": "match",
                            "market_feed_sequence_state": "clean",
                            "market_data_quiet": False,
                            "market_data_quiet_age_ms": None,
                            "orderbook_snapshot_source": "fresh_update",
                            "orderbook_recovery_action": "none",
                            "last_error_type": "ProgrammingError",
                            "last_error_message": (
                                "UndefinedColumn: orderbook_snapshots.yes_bid_count"
                            ),
                            "warnings": [],
                            "blockers": [],
                            "diagnostic_samples": [
                                {
                                    "reason": "invalid_orderbook_snapshot_yes_level_size",
                                    "message_type": "orderbook_snapshot",
                                    "raw_payload_hash": "hash-1",
                                },
                                {
                                    "reason": "invalid_orderbook_delta_delta_fp",
                                    "message_type": "orderbook_delta",
                                    "raw_payload_hash": "hash-2",
                                },
                                {
                                    "reason": "invalid_trade_count_fp",
                                    "message_type": "trade",
                                    "raw_payload_hash": "hash-3",
                                },
                                {
                                    "reason": "invalid_trade_price",
                                    "message_type": "trade",
                                    "raw_payload_hash": "hash-4",
                                },
                            ],
                        },
                    },
                )
            )
            session.commit()

        app = create_app(config)
        with TestClient(app) as client:
            response = client.get("/ws/status")

        assert response.status_code == 200
        body = response.json()
        assert body["enabled"] is True
        assert body["configured"] is True
        assert body["connection_state"] == "subscribed"
        assert body["active_market_ticker"] == "KXBTC15M-ACTIVE"
        assert body["subscribed_channels"] == ["ticker", "orderbook_delta", "trade"]
        assert body["market_feed_transport_state"] == "healthy"
        assert body["market_feed_subscription_state"] == "subscribed"
        assert body["market_feed_snapshot_state"] == "initialized"
        assert body["market_feed_active_ticker_state"] == "match"
        assert body["market_feed_sequence_state"] == "clean"
        assert body["market_data_quiet"] is False
        assert body["orderbook_snapshot_source"] == "fresh_update"
        assert body["orderbook_recovery_action"] is None
        assert body["latest_orderbook_received_at"] is not None
        assert body["latest_trade_received_at"] is not None
        assert body["last_error_type"] is None
        assert body["last_error_message"] is None
        assert len(body["diagnostic_samples"]) == 3
        assert body["diagnostic_samples"][0]["reason"] == (
            "invalid_orderbook_snapshot_yes_level_size"
        )
        assert body["stale"] is False
        assert "PRIVATE KEY" not in response.text
        assert "KALSHI-ACCESS-SIGNATURE" not in response.text
    finally:
        engine.dispose()


def test_ws_status_keeps_error_when_only_ticker_is_fresh(tmp_path) -> None:
    now = datetime.now(UTC)
    persisted_at = now - timedelta(minutes=5)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_api_ws_status_ticker_only.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            OrderbookRepository(session).insert_snapshot(
                OrderbookSnapshotInput(
                    market_ticker="KXBTC15M-ACTIVE",
                    received_at=persisted_at,
                    yes_bid=Decimal("0.60"),
                    yes_ask=Decimal("0.65"),
                    book_status="ok",
                )
            )
            WorkerHeartbeatRepository(session).record_heartbeat(
                WorkerHeartbeatInput(
                    service_name="ape-worker",
                    started_at=now - timedelta(minutes=1),
                    heartbeat_at=now,
                    app_mode="OBSERVER",
                    is_safe=True,
                    metadata={
                        "mode": "kalshi_ws",
                        "ws": {
                            "enabled": True,
                            "configured": True,
                            "signer_ready": True,
                            "connection_state": "subscribed",
                            "active_market_ticker": "KXBTC15M-ACTIVE",
                            "subscribed_channels": ["ticker", "orderbook_delta", "trade"],
                            "subscription_ids": {"ticker": 2, "orderbook_delta": 1, "trade": 2},
                            "last_message_at": _isoformat_z(now),
                            "last_ticker_at": _isoformat_z(now),
                            "last_orderbook_at": _isoformat_z(persisted_at),
                            "last_error_type": "ProgrammingError",
                            "last_error_message": "orderbook persistence failed",
                            "warnings": [],
                            "blockers": [],
                        },
                    },
                )
            )
            session.commit()

        app = create_app(config)
        with TestClient(app) as client:
            response = client.get("/ws/status")

        assert response.status_code == 200
        body = response.json()
        assert body["connection_state"] == "subscribed"
        assert body["latest_orderbook_received_at"] is not None
        assert body["last_error_type"] == "ProgrammingError"
        assert body["last_error_message"] == "orderbook persistence failed"
        assert body["stale"] is False
    finally:
        engine.dispose()


class _FakeKalshiClient:
    def get_markets(self, **_kwargs):
        now = datetime.now(UTC)
        open_time = _isoformat_z(now - timedelta(minutes=5))
        close_time = _isoformat_z(now + timedelta(minutes=10))

        return {
            "markets": [
                {
                    "ticker": "KXBTC15M-ACTIVE",
                    "event_ticker": "KXBTC15M-26JUL051200",
                    "status": "open",
                    "title": "Bitcoin price above $62,000 at settlement?",
                    "yes_sub_title": "Above $62,000",
                    "no_sub_title": "At or below $62,000",
                    "open_time": open_time,
                    "close_time": close_time,
                    "expected_expiration_time": close_time,
                    "expiration_time": close_time,
                    "latest_expiration_time": close_time,
                    "functional_strike": "62000",
                    "price_level_structure": "binary",
                    "rules_primary": "Observer metadata only.",
                }
            ]
        }


def _isoformat_z(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _test_private_key_pem() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
