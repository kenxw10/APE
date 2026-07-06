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
from ape.kalshi.reference_messages import BRTI_SOURCE
from ape.repositories.inputs import ReferenceTickInput, WorkerHeartbeatInput
from ape.repositories.reference_ticks import ReferenceTicksRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository


def test_brti_status_disabled_without_credentials() -> None:
    app = create_app(load_config({}))

    with TestClient(app) as client:
        response = client.get("/reference/brti/status")

    assert response.status_code == 200
    body = response.json()
    assert body["enabled"] is False
    assert body["configured"] is False
    assert body["connection_state"] == "disabled"
    assert body["stale"] is False
    assert "PRIVATE KEY" not in response.text


def test_brti_status_enabled_without_credentials_is_safe() -> None:
    app = create_app(load_config({"KALSHI_CFBENCHMARKS_ENABLED": "true"}))

    with TestClient(app) as client:
        response = client.get("/reference/brti/status")

    assert response.status_code == 200
    body = response.json()
    assert body["enabled"] is True
    assert body["signer_ready"] is False
    assert body["connection_state"] == "not_configured"
    assert "kalshi_cfbenchmarks_credentials_not_configured_or_not_parseable" in body["blockers"]
    assert body["stale"] is False
    assert "PRIVATE KEY" not in response.text


def test_brti_latest_without_rows_returns_safe_empty_shape(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_brti_latest_empty.sqlite'}"
    config = load_config({"DATABASE_URL": database_url})
    engine = create_engine_from_config(config)
    try:
        run_migrations(engine)

        app = create_app(config)
        with TestClient(app) as client:
            response = client.get("/reference/brti/latest")

        assert response.status_code == 200
        body = response.json()
        assert body["found"] is False
        assert body["source"] == BRTI_SOURCE
        assert body["raw_payload_hash"] is None
        assert "raw_payload" not in body
    finally:
        engine.dispose()


def test_brti_status_and_latest_report_persisted_tick(tmp_path) -> None:
    now = datetime.now(UTC)
    source_ts = now - timedelta(seconds=1)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_brti_status.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            ReferenceTicksRepository(session).insert_tick(
                ReferenceTickInput(
                    source=BRTI_SOURCE,
                    received_at=now,
                    source_ts=source_ts,
                    kalshi_received_at=now,
                    raw_value="68000.12",
                    parsed_value=Decimal("68000.12"),
                    trailing_60s_avg=Decimal("67999.50"),
                    trailing_60s_window_size=60,
                    last_60s_windowed_average_15min=Decimal("68001.25"),
                    final_minute_average_window_size=60,
                    final_minute_average_status="present",
                    sequence_number=7,
                    subscription_id="3",
                    source_age_ms=1000,
                    parse_status="valid",
                    raw_payload_hash="hash-1",
                    raw_payload={"safe": "payload"},
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
                        "reference": {
                            "brti": {
                                "enabled": True,
                                "configured": True,
                                "signer_ready": True,
                                "source": BRTI_SOURCE,
                                "index_ids": ["BRTI"],
                                "subscription_id": 3,
                                "connection_state": "subscribed",
                                "last_message_at": _isoformat_z(now),
                                "last_persisted_at": _isoformat_z(now),
                                "latest_source_ts": _isoformat_z(source_ts),
                                "latest_value": "68000.12",
                                "warnings": [],
                                "blockers": [],
                            }
                        },
                    },
                )
            )
            session.commit()

        app = create_app(config)
        with TestClient(app) as client:
            status_response = client.get("/reference/brti/status")
            latest_response = client.get("/reference/brti/latest")

        assert status_response.status_code == 200
        status = status_response.json()
        assert status["enabled"] is True
        assert status["connection_state"] == "subscribed"
        assert status["latest_parsed_value"] == "68000.12000000"
        assert status["latest_trailing_60s_window_size"] == 60
        assert status["latest_final_minute_average"] == "68001.25000000"
        assert status["final_minute_average_status"] == "present"
        assert status["source_age_ms"] == 1000
        assert status["stale"] is False

        assert latest_response.status_code == 200
        latest = latest_response.json()
        assert latest["found"] is True
        assert latest["source"] == BRTI_SOURCE
        assert latest["parsed_value"] == "68000.12000000"
        assert latest["parse_status"] == "valid"
        assert latest["raw_payload_hash"] == "hash-1"
        assert "raw_payload" not in latest
    finally:
        engine.dispose()


def test_brti_status_uses_worker_enabled_state_when_api_disabled(tmp_path) -> None:
    now = datetime.now(UTC)
    source_ts = now - timedelta(seconds=1)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_brti_worker_enabled.sqlite'}"
    config = load_config({"DATABASE_URL": database_url})
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            ReferenceTicksRepository(session).insert_tick(
                ReferenceTickInput(
                    source=BRTI_SOURCE,
                    received_at=now,
                    source_ts=source_ts,
                    kalshi_received_at=now,
                    parsed_value=Decimal("68000.12"),
                    trailing_60s_avg=Decimal("67999.50"),
                    trailing_60s_window_size=60,
                    source_age_ms=1000,
                    parse_status="valid",
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
                        "reference": {
                            "brti": {
                                "enabled": True,
                                "configured": True,
                                "signer_ready": True,
                                "source": BRTI_SOURCE,
                                "index_ids": ["BRTI"],
                                "subscription_id": 3,
                                "connection_state": "subscribed",
                                "last_message_at": _isoformat_z(now),
                                "last_persisted_at": _isoformat_z(now),
                                "latest_source_ts": _isoformat_z(source_ts),
                                "latest_value": "68000.12",
                                "warnings": [],
                                "blockers": [],
                            }
                        },
                    },
                )
            )
            session.commit()

        app = create_app(config)
        with TestClient(app) as client:
            response = client.get("/reference/brti/status")

        assert response.status_code == 200
        body = response.json()
        assert body["enabled"] is True
        assert body["configured"] is True
        assert body["signer_ready"] is True
        assert body["index_ids"] == ["BRTI"]
        assert body["connection_state"] == "subscribed"
        assert body["stale"] is False
        assert body["blockers"] == []
    finally:
        engine.dispose()


def test_brti_status_reports_stale_persisted_tick(tmp_path) -> None:
    now = datetime(2026, 7, 5, 12, 0, 10, tzinfo=UTC)
    old_tick_time = now - timedelta(seconds=10)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_brti_stale.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_STALE_AFTER_SECONDS": "3",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            ReferenceTicksRepository(session).insert_tick(
                ReferenceTickInput(
                    source=BRTI_SOURCE,
                    received_at=old_tick_time,
                    source_ts=old_tick_time,
                    parse_status="valid",
                    parsed_value=Decimal("68000.12"),
                )
            )
            session.commit()

        app = create_app(config)
        with TestClient(app) as client:
            response = client.get("/reference/brti/status")

        assert response.status_code == 200
        body = response.json()
        assert body["stale"] is True
        assert "brti_reference_stale" in body["warnings"]
    finally:
        engine.dispose()


def test_brti_status_reports_stale_source_age(tmp_path) -> None:
    now = datetime.now(UTC)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_brti_stale_source_age.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_STALE_AFTER_SECONDS": "60",
            "KALSHI_CFBENCHMARKS_MAX_SOURCE_AGE_MS": "3000",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            ReferenceTicksRepository(session).insert_tick(
                ReferenceTickInput(
                    source=BRTI_SOURCE,
                    received_at=now,
                    source_ts=now - timedelta(seconds=5),
                    parse_status="valid",
                    parsed_value=Decimal("68000.12"),
                    source_age_ms=5000,
                )
            )
            session.commit()

        app = create_app(config)
        with TestClient(app) as client:
            response = client.get("/reference/brti/status")

        assert response.status_code == 200
        body = response.json()
        assert body["source_age_ms"] == 5000
        assert body["stale"] is True
        assert "brti_reference_source_age_stale" in body["warnings"]
    finally:
        engine.dispose()


def _test_private_key_pem() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")


def _isoformat_z(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
