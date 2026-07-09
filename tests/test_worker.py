from __future__ import annotations

import logging

from sqlalchemy import func, select

import ape.worker.main as worker_main
from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.models import StorageRetentionRun, WorkerHeartbeat
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.worker.main import configure_logging, run_worker
from ape.worker.services import WORKER_SERVICE_MARKET_WS, WORKER_SERVICE_REFERENCE_BRTI


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


def test_worker_main_cli_role_overrides_invalid_env_role(monkeypatch) -> None:
    captured: dict[str, str | None] = {}

    def fake_run_worker(config, *, worker_role=None) -> None:
        captured["config_role"] = config.ape_worker_role
        captured["worker_role"] = worker_role

    monkeypatch.setenv("APE_WORKER_ROLE", "stale-invalid-role")
    monkeypatch.setattr(worker_main, "run_worker", fake_run_worker)

    assert worker_main.main(["--role", "market-data"]) == 0
    assert captured == {
        "config_role": "market-data",
        "worker_role": "market-data",
    }


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
            market_heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat(
                WORKER_SERVICE_MARKET_WS
            )
            reference_heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat(
                WORKER_SERVICE_REFERENCE_BRTI
            )

            assert heartbeat is not None
            assert heartbeat.metadata_["mode"] == "idle"
            assert heartbeat.metadata_["ws"]["connection_state"] == "disabled"
            assert heartbeat.metadata_["strategy"]["dry_run"]["enabled"] is False
            assert market_heartbeat is not None
            assert market_heartbeat.metadata_["mode"] == "market_ws"
            assert reference_heartbeat is not None
            assert reference_heartbeat.metadata_["mode"] == "reference_brti"
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

            assert heartbeat_count == 5
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


def test_worker_market_data_role_only_starts_market_loop(monkeypatch) -> None:
    calls: list[str] = []

    class FakeCollector:
        def __init__(self, **_kwargs) -> None:
            calls.append("collector_init")

        async def run_market_data(self, **_kwargs) -> None:
            calls.append("market")

        async def run_reference_brti(self, **_kwargs) -> None:
            calls.append("reference")

    class FakeStrategyObserver:
        def __init__(self, **_kwargs) -> None:
            calls.append("strategy_init")

    class FakeRetentionWorker:
        def __init__(self, **_kwargs) -> None:
            calls.append("retention_init")

    monkeypatch.setattr(worker_main, "KalshiWsCollector", FakeCollector)
    monkeypatch.setattr(worker_main, "StrategyObserver", FakeStrategyObserver)
    monkeypatch.setattr(worker_main, "StorageRetentionWorker", FakeRetentionWorker)

    config = load_config(
        {
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STORAGE_RETENTION_ENABLED": "true",
        }
    )

    run_worker(config, max_iterations=1, worker_role="market-data")

    assert calls == ["collector_init", "market"]


def test_worker_reference_role_only_starts_brti_loop(monkeypatch) -> None:
    calls: list[str] = []

    class FakeCollector:
        def __init__(self, **_kwargs) -> None:
            calls.append("collector_init")

        async def run_market_data(self, **_kwargs) -> None:
            calls.append("market")

        async def run_reference_brti(self, **_kwargs) -> None:
            calls.append("reference")

    monkeypatch.setattr(worker_main, "KalshiWsCollector", FakeCollector)

    config = load_config(
        {
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STORAGE_RETENTION_ENABLED": "true",
        }
    )

    run_worker(config, max_iterations=1, worker_role="reference-brti")

    assert calls == ["collector_init", "reference"]


def test_worker_strategy_role_starts_no_websocket_collector(monkeypatch) -> None:
    calls: list[str] = []

    class FakeCollector:
        def __init__(self, **_kwargs) -> None:
            raise AssertionError("strategy role must not start Kalshi WebSockets")

    class FakeStrategyObserver:
        def __init__(self, **_kwargs) -> None:
            calls.append("strategy_init")

        async def run(self, **_kwargs) -> None:
            calls.append("strategy")

    monkeypatch.setattr(worker_main, "KalshiWsCollector", FakeCollector)
    monkeypatch.setattr(worker_main, "StrategyObserver", FakeStrategyObserver)

    config = load_config(
        {
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STORAGE_RETENTION_ENABLED": "true",
        }
    )

    run_worker(config, max_iterations=1, worker_role="strategy")

    assert calls == ["strategy_init", "strategy"]


def test_worker_maintenance_role_only_starts_retention(monkeypatch) -> None:
    calls: list[str] = []

    class FakeCollector:
        def __init__(self, **_kwargs) -> None:
            raise AssertionError("maintenance role must not start Kalshi WebSockets")

    class FakeStrategyObserver:
        def __init__(self, **_kwargs) -> None:
            raise AssertionError("maintenance role must not start strategy")

    class FakeRetentionWorker:
        def __init__(self, **_kwargs) -> None:
            calls.append("retention_init")

        async def run(self, **_kwargs) -> None:
            calls.append("retention")

    monkeypatch.setattr(worker_main, "KalshiWsCollector", FakeCollector)
    monkeypatch.setattr(worker_main, "StrategyObserver", FakeStrategyObserver)
    monkeypatch.setattr(worker_main, "StorageRetentionWorker", FakeRetentionWorker)

    config = load_config(
        {
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STORAGE_RETENTION_ENABLED": "true",
        }
    )

    run_worker(config, max_iterations=1, worker_role="maintenance")

    assert calls == ["retention_init", "retention"]
