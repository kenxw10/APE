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
    assert body["status_category"] == "disabled"
    assert body["recommended_action"] is None
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
    assert body["status_category"] == "waiting"
    assert "kalshi_cfbenchmarks_credentials_not_configured_or_not_parseable" in body["blockers"]
    assert body["recommended_action"] == "check_worker_kalshi_credentials"
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
        assert body["status_category"] == "healthy"
        assert body["stale_reason"] is None
        assert body["time_since_last_valid_tick_ms"] is not None
        assert body["stale"] is False
        assert body["blockers"] == []
    finally:
        engine.dispose()


def test_brti_status_reports_stale_persisted_tick(tmp_path) -> None:
    now = datetime.now(UTC)
    old_tick_time = now - timedelta(seconds=10)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_brti_stale.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_PERSISTENCE_STALE_AFTER_SECONDS": "3",
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
            WorkerHeartbeatRepository(session).record_heartbeat(
                WorkerHeartbeatInput(
                    service_name="ape-worker",
                    started_at=now - timedelta(minutes=1),
                    heartbeat_at=now,
                    app_mode="OBSERVER",
                    is_safe=True,
                    metadata={
                        "reference": {
                            "brti": {
                                "enabled": True,
                                "configured": True,
                                "signer_ready": True,
                                "connection_state": "subscribed",
                                "last_message_at": _isoformat_z(now),
                                "last_persisted_at": _isoformat_z(old_tick_time),
                                "warnings": [],
                                "blockers": [],
                            }
                        }
                    },
                )
            )
            session.commit()

        app = create_app(config)
        with TestClient(app) as client:
            response = client.get("/reference/brti/status")

        assert response.status_code == 200
        body = response.json()
        assert body["stale"] is True
        assert body["status_category"] == "stale_persistence"
        assert body["stale_reason"] == "brti_reference_persistence_stale"
        assert body["stale_age_ms"] is not None
        assert body["transport_stale"] is False
        assert body["persistence_stale"] is True
        assert "brti_reference_persistence_stale" in body["warnings"]
    finally:
        engine.dispose()


def test_brti_source_age_over_legacy_threshold_does_not_fail_observer_health(tmp_path) -> None:
    now = datetime.now(UTC)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_brti_stale_source_age.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_MAX_SOURCE_AGE_MS": "3000",
            "KALSHI_CFBENCHMARKS_SOURCE_AGE_WARN_MS": "45000",
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
            WorkerHeartbeatRepository(session).record_heartbeat(
                WorkerHeartbeatInput(
                    service_name="ape-worker",
                    started_at=now - timedelta(minutes=1),
                    heartbeat_at=now,
                    app_mode="OBSERVER",
                    is_safe=True,
                    metadata={
                        "reference": {
                            "brti": {
                                "enabled": True,
                                "configured": True,
                                "signer_ready": True,
                                "connection_state": "subscribed",
                                "last_message_at": _isoformat_z(now),
                                "last_persisted_at": _isoformat_z(now),
                                "warnings": [],
                                "blockers": [],
                            }
                        }
                    },
                )
            )
            session.commit()

        app = create_app(config)
        with TestClient(app) as client:
            response = client.get("/reference/brti/status")

        assert response.status_code == 200
        body = response.json()
        assert body["source_age_ms"] == 5000
        assert body["source_stale"] is False
        assert body["transport_stale"] is False
        assert body["persistence_stale"] is False
        assert body["trade_ready_fresh"] is False
        assert body["stale"] is False
        assert "brti_reference_source_age_stale" not in body["warnings"]
    finally:
        engine.dispose()


def test_brti_status_reports_source_age_warning_without_global_stale(tmp_path) -> None:
    now = datetime.now(UTC)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_brti_source_age_warning.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_SOURCE_AGE_WARN_MS": "45000",
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
                    source_ts=now - timedelta(seconds=50),
                    kalshi_received_at=now,
                    parse_status="valid",
                    parsed_value=Decimal("68000.12"),
                    source_age_ms=50000,
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
                        "reference": {
                            "brti": {
                                "enabled": True,
                                "configured": True,
                                "signer_ready": True,
                                "connection_state": "subscribed",
                                "last_message_at": _isoformat_z(now),
                                "last_persisted_at": _isoformat_z(now),
                                "warnings": [],
                                "blockers": [],
                            }
                        }
                    },
                )
            )
            session.commit()

        app = create_app(config)
        with TestClient(app) as client:
            response = client.get("/reference/brti/status")

        assert response.status_code == 200
        body = response.json()
        assert body["source_stale"] is True
        assert body["status_category"] == "upstream_lag"
        assert body["stale_reason"] == "brti_reference_source_age_stale"
        assert body["stale"] is False
        assert "brti_reference_source_age_stale" in body["warnings"]
    finally:
        engine.dispose()


def test_brti_status_reports_transport_stale_without_recent_backend_message(tmp_path) -> None:
    now = datetime.now(UTC)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_brti_transport_stale.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_TRANSPORT_STALE_AFTER_SECONDS": "3",
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
                    source_ts=now,
                    kalshi_received_at=now,
                    parse_status="valid",
                    parsed_value=Decimal("68000.12"),
                    source_age_ms=1000,
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
                        "reference": {
                            "brti": {
                                "enabled": True,
                                "configured": True,
                                "signer_ready": True,
                                "connection_state": "subscribed",
                                "last_message_at": _isoformat_z(now - timedelta(seconds=10)),
                                "last_persisted_at": _isoformat_z(now),
                                "warnings": [],
                                "blockers": [],
                            }
                        }
                    },
                )
            )
            session.commit()

        app = create_app(config)
        with TestClient(app) as client:
            response = client.get("/reference/brti/status")

        assert response.status_code == 200
        body = response.json()
        assert body["transport_stale"] is True
        assert body["persistence_stale"] is False
        assert body["status_category"] == "stale_transport"
        assert body["stale_reason"] == "brti_reference_transport_stale"
        assert body["stale"] is True
        assert "brti_reference_transport_stale" in body["warnings"]
    finally:
        engine.dispose()


def test_brti_status_reports_stale_worker_heartbeat(tmp_path) -> None:
    now = datetime.now(UTC)
    old_heartbeat_time = now - timedelta(seconds=30)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_brti_worker_stale.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_HEARTBEAT_STALE_AFTER_SECONDS": "3",
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
                    source_ts=now,
                    kalshi_received_at=now,
                    parse_status="valid",
                    parsed_value=Decimal("68000.12"),
                    source_age_ms=1000,
                )
            )
            WorkerHeartbeatRepository(session).record_heartbeat(
                WorkerHeartbeatInput(
                    service_name="ape-worker",
                    started_at=old_heartbeat_time - timedelta(minutes=1),
                    heartbeat_at=old_heartbeat_time,
                    app_mode="OBSERVER",
                    is_safe=True,
                    metadata={
                        "reference": {
                            "brti": {
                                "enabled": True,
                                "configured": True,
                                "signer_ready": True,
                                "connection_state": "subscribed",
                                "last_message_at": _isoformat_z(now),
                                "last_persisted_at": _isoformat_z(now),
                                "warnings": [],
                                "blockers": [],
                            }
                        }
                    },
                )
            )
            session.commit()

        app = create_app(config)
        with TestClient(app) as client:
            response = client.get("/reference/brti/status")

        assert response.status_code == 200
        body = response.json()
        assert body["worker_heartbeat_stale"] is True
        assert body["status_category"] == "worker_stale"
        assert body["stale_reason"] == "brti_reference_worker_heartbeat_stale"
        assert body["stale"] is True
        assert body["recommended_action"] == "restart_or_inspect_railway_worker"
    finally:
        engine.dispose()


def test_brti_series_returns_bounded_recent_points_without_raw_payload(tmp_path) -> None:
    now = datetime.now(UTC)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_brti_series.sqlite'}"
    config = load_config({"DATABASE_URL": database_url})
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            repository = ReferenceTicksRepository(session)
            repository.insert_tick(
                ReferenceTickInput(
                    source=BRTI_SOURCE,
                    received_at=now - timedelta(minutes=20),
                    source_ts=now - timedelta(minutes=20),
                    kalshi_received_at=now - timedelta(minutes=20),
                    parse_status="valid",
                    parsed_value=Decimal("67000.00"),
                    raw_payload_hash="old-hash",
                    raw_payload={"secret": "not returned"},
                )
            )
            repository.insert_tick(
                ReferenceTickInput(
                    source=BRTI_SOURCE,
                    received_at=now - timedelta(seconds=3),
                    source_ts=now - timedelta(seconds=4),
                    kalshi_received_at=now - timedelta(seconds=3),
                    parse_status="valid",
                    parsed_value=Decimal("68000.12"),
                    trailing_60s_avg=Decimal("67999.50"),
                    last_60s_windowed_average_15min=Decimal("68001.25"),
                    final_minute_average_status="present",
                    source_age_ms=1000,
                    sequence_number=1,
                    raw_payload_hash="first-hash",
                    raw_payload={"secret": "not returned"},
                )
            )
            repository.insert_tick(
                ReferenceTickInput(
                    source=BRTI_SOURCE,
                    received_at=now - timedelta(seconds=1),
                    source_ts=now - timedelta(seconds=2),
                    kalshi_received_at=now - timedelta(seconds=1),
                    parse_status="valid",
                    parsed_value=Decimal("68001.12"),
                    trailing_60s_avg=Decimal("68000.50"),
                    last_60s_windowed_average_15min=Decimal("68002.25"),
                    final_minute_average_status="present",
                    source_age_ms=1000,
                    sequence_number=2,
                    raw_payload_hash="latest-hash",
                    raw_payload={"secret": "not returned"},
                )
            )
            session.commit()

        app = create_app(config)
        with TestClient(app) as client:
            response = client.get(
                "/reference/brti/series?window_seconds=999&max_points=99999"
            )
            latest_only_response = client.get(
                "/reference/brti/series?window_seconds=900&max_points=1&include_final_minute=true"
            )

        assert response.status_code == 200
        body = response.json()
        assert body["source"] == BRTI_SOURCE
        assert body["window_seconds"] == 900
        assert body["max_points"] == 16000
        assert body["point_count"] == 2
        assert [point["raw_payload_hash"] for point in body["points"]] == [
            "first-hash",
            "latest-hash",
        ]
        assert body["points"][0]["last_60s_windowed_average_15min"] is None
        assert "raw_payload" not in body["points"][0]

        assert latest_only_response.status_code == 200
        latest_only = latest_only_response.json()
        assert latest_only["point_count"] == 1
        assert latest_only["points"][0]["raw_payload_hash"] == "latest-hash"
        assert latest_only["points"][0]["last_60s_windowed_average_15min"] == "68002.25000000"
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
