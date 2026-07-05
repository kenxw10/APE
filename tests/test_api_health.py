from __future__ import annotations

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

from ape.api.main import create_app
from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.markets import MarketsRepository


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
    finally:
        engine.dispose()


class _FakeKalshiClient:
    def get_markets(self, **_kwargs):
        return {
            "markets": [
                {
                    "ticker": "KXBTC15M-ACTIVE",
                    "event_ticker": "KXBTC15M-26JUL051200",
                    "series_ticker": "KXBTC15M",
                    "status": "open",
                    "title": "Bitcoin price above $62,000 at settlement?",
                    "yes_sub_title": "Above $62,000",
                    "no_sub_title": "At or below $62,000",
                    "open_time": "2026-07-05T00:00:00Z",
                    "close_time": "2026-07-06T00:00:00Z",
                    "expected_expiration_time": "2026-07-06T00:00:00Z",
                    "expiration_time": "2026-07-06T00:00:00Z",
                    "latest_expiration_time": "2026-07-06T00:00:00Z",
                    "functional_strike": "62000",
                    "rules_primary": "Observer metadata only.",
                }
            ]
        }


def _test_private_key_pem() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
