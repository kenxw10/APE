from __future__ import annotations

import logging

from sqlalchemy import func, select

from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.models import StorageRetentionRun, WorkerHeartbeat
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.worker.main import configure_logging, run_worker


def test_configure_logging_reapplies_requested_level() -> None:
    root_logger = logging.getLogger()
    previous_level = root_logger.level
    previous_handlers = list(root_logger.handlers)

    try:
        logging.basicConfig(level=logging.INFO, force=True)

        configure_logging("DEBUG")

        assert root_logger.isEnabledFor(logging.DEBUG)
    finally:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
        for handler in previous_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(previous_level)


def test_worker_disabled_websocket_records_idle_heartbeat(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_worker.sqlite'}"
    config = load_config({"DATABASE_URL": database_url})
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)

    try:
        run_worker(config, max_iterations=1)

        with session_factory() as session:
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert heartbeat is not None
            assert heartbeat.metadata_["mode"] == "idle"
            assert heartbeat.metadata_["ws"]["connection_state"] == "disabled"
    finally:
        engine.dispose()


def test_worker_disabled_websocket_survives_idle_heartbeat_db_failure(tmp_path, caplog) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_worker_without_migrations.sqlite'}"
    config = load_config({"DATABASE_URL": database_url})

    with caplog.at_level(logging.WARNING):
        run_worker(config, max_iterations=1)

    assert "Idle worker heartbeat persistence failed." in caplog.text


def test_worker_disabled_websocket_throttles_idle_heartbeats(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_worker_throttled.sqlite'}"
    config = load_config({"DATABASE_URL": database_url, "WORKER_POLL_SECONDS": "0.01"})
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)

    try:
        run_worker(config, max_iterations=5)

        with session_factory() as session:
            heartbeat_count = session.scalar(select(func.count()).select_from(WorkerHeartbeat))

            assert heartbeat_count == 1
    finally:
        engine.dispose()


def test_worker_enabled_storage_retention_runs_periodic_task(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_worker_retention.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "STORAGE_RETENTION_ENABLED": "true",
            "STORAGE_RETENTION_INTERVAL_SECONDS": "300",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)

    try:
        run_worker(config, max_iterations=1)

        with session_factory() as session:
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")
            run_count = session.scalar(select(func.count()).select_from(StorageRetentionRun))

            assert heartbeat is not None
            assert heartbeat.metadata_["storage"]["retention"]["enabled"] is True
            assert heartbeat.metadata_["storage"]["retention"]["last_status"] == "success"
            assert run_count == 1
    finally:
        engine.dispose()
