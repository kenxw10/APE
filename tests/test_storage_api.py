from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
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
    assert body["configured_enabled"] is False
    assert body["effective_enabled"] is False
    assert body["connection_state"] == "disabled"
    assert body["liveness_source"] == "missing"
    assert body["worker_heartbeat_stale"] is False
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
        "strategy_dry_run_events",
        "strategy_dry_run_positions",
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
    assert body["latest_total_deleted_rows"] == 10
    assert body["latest_total_raw_payload_stripped_rows"] == 3
    assert body["latest_run_budget_exhausted"] is False
    assert body["latest_db_error_count"] == 0


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
    now = datetime.now(UTC)
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
                                "worker_role": "maintenance",
                                "connection_state": "idle",
                                "warnings": [],
                                "blockers": [],
                            }
                        }
                    },
                )
            )
            WorkerHeartbeatRepository(session).record_heartbeat(
                WorkerHeartbeatInput(
                    service_name="ape-worker.maintenance",
                    started_at=now - timedelta(seconds=30),
                    heartbeat_at=now,
                    app_mode="OBSERVER",
                    is_safe=True,
                    metadata={
                        "mode": "storage_retention",
                        "storage": {
                            "retention": {
                                "enabled": True,
                                "worker_role": "maintenance",
                                "connection_state": "idle",
                                "warnings": [],
                                "blockers": [],
                            }
                        },
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
    assert body["configured_enabled"] is False
    assert body["worker_observed_enabled"] is True
    assert body["effective_enabled"] is True
    assert body["connection_state"] == "idle"
    assert body["liveness_source"] == "component"
    assert body["worker_role"] == "maintenance"
    assert body["worker_heartbeat_at"] is not None
    assert body["worker_heartbeat_age_ms"] is not None
    assert body["worker_started_at"] is not None
    assert body["component_heartbeat_at"] is not None
    assert body["component_heartbeat_age_ms"] is not None
    assert body["latest_component_heartbeat_mode"] == "storage_retention"
    assert body["latest_aggregate_heartbeat_mode"] is None
    assert body["liveness_source_mismatch"] is False
    assert body["worker_heartbeat_stale"] is False
    assert body["retention_config"]["enabled"] is True
    assert body["retention_config"]["configured_enabled"] is False
    assert body["retention_config"]["worker_observed_enabled"] is True
    assert body["retention_config"]["effective_enabled"] is True


@pytest.mark.parametrize(
    ("service_name", "expected_liveness_source"),
    [
        ("ape-worker.maintenance", "component"),
        ("ape-worker", "legacy_aggregate_fallback"),
    ],
)
def test_storage_status_handles_heartbeat_without_started_at(
    tmp_path,
    service_name: str,
    expected_liveness_source: str,
) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / f'ape_storage_{service_name}.sqlite'}"
    now = datetime.now(UTC)
    engine = create_engine_from_config(load_config({"DATABASE_URL": database_url}))
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            WorkerHeartbeatRepository(session).record_heartbeat(
                WorkerHeartbeatInput(
                    service_name=service_name,
                    heartbeat_at=now,
                    app_mode="OBSERVER",
                    is_safe=True,
                    metadata={
                        "mode": "storage_retention",
                        "storage": {
                            "retention": {
                                "enabled": True,
                                "worker_role": "maintenance",
                                "connection_state": "idle",
                                "warnings": [],
                                "blockers": [],
                            }
                        },
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
    assert body["liveness_source"] == expected_liveness_source
    assert body["worker_started_at"] is None


def test_storage_status_falls_back_to_legacy_aggregate_liveness(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_storage_legacy.sqlite'}"
    now = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    engine = create_engine_from_config(load_config({"DATABASE_URL": database_url}))
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            WorkerHeartbeatRepository(session).record_heartbeat(
                WorkerHeartbeatInput(
                    service_name="ape-worker",
                    started_at=now - timedelta(seconds=30),
                    heartbeat_at=now,
                    app_mode="OBSERVER",
                    is_safe=True,
                    metadata={
                        "mode": "storage_retention",
                        "storage": {
                            "retention": {
                                "enabled": True,
                                "worker_role": "maintenance",
                                "connection_state": "idle",
                                "warnings": [],
                                "blockers": [],
                            }
                        },
                    },
                )
            )
            session.commit()
    finally:
        engine.dispose()

    snapshot = retention_module.build_storage_status(
        load_config({"DATABASE_URL": database_url}),
        now=now,
    )

    assert snapshot.enabled is True
    assert snapshot.liveness_source == "legacy_aggregate_fallback"
    assert snapshot.liveness_source_mismatch is True
    assert "feed_liveness_legacy_aggregate_fallback" in snapshot.warnings


def test_storage_status_reports_stale_maintenance_heartbeat(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_storage_stale_worker.sqlite'}"
    checked_at = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    old_heartbeat_at = checked_at - timedelta(seconds=1000)
    engine = create_engine_from_config(load_config({"DATABASE_URL": database_url}))
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            WorkerHeartbeatRepository(session).record_heartbeat(
                WorkerHeartbeatInput(
                    service_name="ape-worker.maintenance",
                    started_at=old_heartbeat_at,
                    heartbeat_at=old_heartbeat_at,
                    app_mode="OBSERVER",
                    is_safe=True,
                    metadata={
                        "mode": "storage_retention",
                        "storage": {
                            "retention": {
                                "enabled": True,
                                "worker_role": "maintenance",
                                "connection_state": "idle",
                                "warnings": [],
                                "blockers": [],
                            }
                        },
                    },
                )
            )
            session.commit()
    finally:
        engine.dispose()

    snapshot = retention_module.build_storage_status(
        load_config(
            {
                "DATABASE_URL": database_url,
                "STORAGE_RETENTION_INTERVAL_SECONDS": "300",
            }
        ),
        now=checked_at,
    )

    assert snapshot.worker_heartbeat_stale is True
    assert snapshot.worker_heartbeat_age_ms == 1_000_000
    assert "storage_retention_worker_heartbeat_stale" in snapshot.warnings


def test_storage_status_uses_worker_reported_interval_for_heartbeat_staleness(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_storage_worker_interval.sqlite'}"
    checked_at = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    heartbeat_at = checked_at - timedelta(seconds=180)
    engine = create_engine_from_config(load_config({"DATABASE_URL": database_url}))
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            WorkerHeartbeatRepository(session).record_heartbeat(
                WorkerHeartbeatInput(
                    service_name="ape-worker.maintenance",
                    started_at=heartbeat_at,
                    heartbeat_at=heartbeat_at,
                    app_mode="OBSERVER",
                    is_safe=True,
                    metadata={
                        "mode": "storage_retention",
                        "storage": {
                            "retention": {
                                "enabled": True,
                                "interval_seconds": 300,
                                "worker_role": "maintenance",
                                "connection_state": "idle",
                                "warnings": [],
                                "blockers": [],
                            }
                        },
                    },
                )
            )
            session.commit()
    finally:
        engine.dispose()

    snapshot = retention_module.build_storage_status(
        load_config(
            {
                "DATABASE_URL": database_url,
                "STORAGE_RETENTION_INTERVAL_SECONDS": "60",
            }
        ),
        now=checked_at,
    )

    assert snapshot.worker_heartbeat_stale is False
    assert "storage_retention_worker_heartbeat_stale" not in snapshot.warnings


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
