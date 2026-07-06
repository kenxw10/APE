from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from ape.api.main import create_app
from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.inputs import StorageRetentionRunInput, WorkerHeartbeatInput
from ape.repositories.storage_retention import StorageRetentionRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.storage import retention as retention_module


def test_storage_status_works_without_database_url() -> None:
    app = create_app(load_config({}))

    with TestClient(app) as client:
        response = client.get("/storage/status")

    assert response.status_code == 200
    body = response.json()
    assert body["database_configured"] is False
    assert body["enabled"] is False
    assert body["connection_state"] == "disabled"
    assert body["latest_run_found"] is False
    assert body["table_stats"] == []


def test_storage_status_reports_tables_without_retention_run(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_storage_status.sqlite'}"
    engine = create_engine_from_config(load_config({"DATABASE_URL": database_url}))
    run_migrations(engine)
    engine.dispose()
    app = create_app(load_config({"DATABASE_URL": database_url}))

    with TestClient(app) as client:
        response = client.get("/storage/status")

    assert response.status_code == 200
    body = response.json()
    assert body["database_configured"] is True
    assert body["latest_run_found"] is False
    assert {stat["table_name"] for stat in body["table_stats"]} >= {
        "orderbook_snapshots",
        "public_trades",
        "reference_ticks",
        "worker_heartbeats",
        "strategy_decisions",
        "markets",
    }
    assert "large-secret-payload" not in response.text


def test_storage_status_reports_latest_successful_run(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_storage_success.sqlite'}"
    now = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    _insert_retention_run(database_url, now, status="success")
    app = create_app(load_config({"DATABASE_URL": database_url}))

    with TestClient(app) as client:
        response = client.get("/storage/status")

    assert response.status_code == 200
    body = response.json()
    assert body["latest_run_found"] is True
    assert body["latest_run_status"] == "success"
    assert body["latest_deleted_rows"] == {"orderbook_snapshots": 10}
    assert body["latest_raw_payload_stripped_rows"] == {"reference_ticks": 3}


def test_storage_status_warns_on_latest_failed_run(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_storage_failed.sqlite'}"
    now = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    _insert_retention_run(database_url, now, status="failed")
    app = create_app(load_config({"DATABASE_URL": database_url}))

    with TestClient(app) as client:
        response = client.get("/storage/status")

    assert response.status_code == 200
    body = response.json()
    assert body["latest_run_status"] == "failed"
    assert "latest_storage_retention_run_failed" in body["warnings"]


def test_storage_status_warns_on_stale_running_run(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_storage_running.sqlite'}"
    checked_at = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    _insert_retention_run(
        database_url,
        checked_at,
        status="running",
        started_at=checked_at - timedelta(seconds=60),
        unfinished=True,
    )
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "STORAGE_RETENTION_ENABLED": "true",
            "STORAGE_RETENTION_MAX_RUN_SECONDS": "20",
        }
    )

    snapshot = retention_module.build_storage_status(config, now=checked_at)

    assert snapshot.latest_run_status == "running"
    assert "storage_retention_running_run_stale" in snapshot.warnings


def test_storage_status_uses_worker_observed_enabled(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_storage_worker.sqlite'}"
    now = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    engine = create_engine_from_config(load_config({"DATABASE_URL": database_url}))
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            WorkerHeartbeatRepository(session).record_heartbeat(
                WorkerHeartbeatInput(
                    service_name="ape-worker",
                    heartbeat_at=now,
                    app_mode="OBSERVER",
                    is_safe=True,
                    metadata={
                        "storage": {
                            "retention": {
                                "enabled": True,
                                "connection_state": "idle",
                                "warnings": [],
                                "blockers": [],
                            }
                        }
                    },
                )
            )
            session.commit()
    finally:
        engine.dispose()

    app = create_app(load_config({"DATABASE_URL": database_url}))
    with TestClient(app) as client:
        response = client.get("/storage/status")

    assert response.status_code == 200
    body = response.json()
    assert body["enabled"] is True
    assert body["worker_observed_enabled"] is True
    assert body["connection_state"] == "idle"


def test_storage_status_warn_and_critical_thresholds(tmp_path, monkeypatch) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_storage_threshold.sqlite'}"
    engine = create_engine_from_config(load_config({"DATABASE_URL": database_url}))
    run_migrations(engine)
    engine.dispose()
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "STORAGE_RETENTION_STATUS_WARN_BYTES": "100",
            "STORAGE_RETENTION_STATUS_CRITICAL_BYTES": "200",
        }
    )
    monkeypatch.setattr(
        retention_module.StorageRetentionRepository,
        "database_total_bytes",
        lambda self: 150,
    )

    warning_snapshot = retention_module.build_storage_status(config)
    assert "database_size_warning" in warning_snapshot.warnings
    assert "database_size_critical" not in warning_snapshot.blockers

    monkeypatch.setattr(
        retention_module.StorageRetentionRepository,
        "database_total_bytes",
        lambda self: 250,
    )
    critical_snapshot = retention_module.build_storage_status(config)
    assert "database_size_critical" in critical_snapshot.blockers


def _insert_retention_run(
    database_url: str,
    now: datetime,
    *,
    status: str,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    unfinished: bool = False,
) -> None:
    engine = create_engine_from_config(load_config({"DATABASE_URL": database_url}))
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            StorageRetentionRepository(session).start_run(
                StorageRetentionRunInput(
                    run_id=f"run-{status}",
                    started_at=started_at or now,
                    finished_at=None if unfinished else finished_at or now,
                    status=status,
                    dry_run=False,
                    duration_ms=10,
                    deleted_rows={"orderbook_snapshots": 10},
                    raw_payload_stripped_rows={"reference_ticks": 3},
                    warnings=[],
                    blockers=[],
                )
            )
            session.commit()
    finally:
        engine.dispose()
