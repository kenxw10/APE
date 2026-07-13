from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

from ape.api.main import create_app
from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.models import ResearchReplayEvent
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.inputs import WorkerHeartbeatInput
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.research.repository import ResearchRepository
from ape.research.status import build_research_status
from ape.worker.services import WORKER_SERVICE_RESEARCH


def test_research_routes_are_bounded_read_only_and_safe_without_data(tmp_path) -> None:
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'research-api.sqlite'}",
            "APP_MODE": "DRY_RUN",
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    try:
        app = create_app(config)
        with TestClient(app) as client:
            for route in (
                "/research/status",
                "/research/coverage/latest",
                "/research/zero-entry/latest",
                "/research/replay/runs/recent",
                "/research/replay/trades/recent",
                "/research/calibration/runs/recent",
                "/research/candidates/recent",
                "/research/governance/events/recent",
            ):
                response = client.get(route)
                assert response.status_code == 200
            assert client.get("/research/replay/runs/recent?limit=501").status_code == 422
            assert (
                client.get("/research/replay/trades/recent?candidate_id=bad%20value").status_code
                == 422
            )
            assert (
                client.get("/research/candidates/recent?lifecycle_state=LIVE_ACTIVE").status_code
                == 422
            )
            research_routes = [
                route for route in app.routes if getattr(route, "path", "").startswith("/research/")
            ]
            assert all(route.methods == {"GET"} for route in research_routes)
    finally:
        engine.dispose()


def test_zero_entry_route_returns_bounded_database_error(tmp_path, monkeypatch) -> None:
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'zero-entry-db-error.sqlite'}",
            "APP_MODE": "DRY_RUN",
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)

    def raise_database_error(_self) -> None:
        raise SQLAlchemyError("research table unavailable")

    try:
        monkeypatch.setattr(
            ResearchRepository,
            "latest_zero_entry_report",
            raise_database_error,
        )
        with TestClient(create_app(config)) as client:
            response = client.get("/research/zero-entry/latest")

        assert response.status_code == 200
        assert response.json() == {
            "configured": True,
            "report": None,
            "error": "research_database_error",
        }
    finally:
        engine.dispose()


def test_research_status_normalizes_naive_sqlite_timestamps(tmp_path) -> None:
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'research-status.sqlite'}",
            "APP_MODE": "DRY_RUN",
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    naive_at = datetime(2026, 7, 12, 12, 0)
    try:
        with session_factory() as session:
            WorkerHeartbeatRepository(session).record_heartbeat(
                WorkerHeartbeatInput(
                    service_name=WORKER_SERVICE_RESEARCH,
                    heartbeat_at=naive_at,
                    app_mode="DRY_RUN",
                    is_safe=True,
                    metadata={"research": {"enabled": True, "calibration_enabled": False}},
                )
            )
            session.add(
                ResearchReplayEvent(
                    event_id="naive-research-event",
                    market_ticker="KXBTC15M-STATUS",
                    event_type="MARKET",
                    event_time=naive_at,
                    received_at=naive_at,
                    source_table="markets",
                    source_row_id="naive-status",
                    source_hash="fixture",
                    replay_schema_version="momentum_v2_replay_v1",
                    payload={},
                    event_hash="naive-status",
                    replay_readiness="FULL",
                    blockers=[],
                )
            )
            session.add(
                ResearchReplayEvent(
                    event_id="naive-coverage-report",
                    market_ticker=None,
                    event_type="COVERAGE_REPORT",
                    event_time=naive_at + timedelta(seconds=10),
                    received_at=naive_at + timedelta(seconds=10),
                    source_table="research_coverage_reports",
                    source_row_id="naive-coverage",
                    source_hash="fixture",
                    replay_schema_version="momentum_v2_replay_v1",
                    payload={"coverage": "synthetic"},
                    event_hash="naive-coverage",
                    replay_readiness="FULL",
                    blockers=[],
                )
            )
            session.commit()

        status = build_research_status(
            config,
            now=naive_at.replace(tzinfo=UTC) + timedelta(seconds=10),
        )

        assert status["heartbeat_fresh"] is True
        assert status["event_lag_seconds"] == 10
    finally:
        engine.dispose()


def test_research_status_uses_worker_observed_enabled_state(tmp_path) -> None:
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'research-worker-state.sqlite'}",
            "RESEARCH_ENABLED": "false",
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)
    try:
        with factory() as session:
            WorkerHeartbeatRepository(session).record_heartbeat(
                WorkerHeartbeatInput(
                    service_name=WORKER_SERVICE_RESEARCH,
                    heartbeat_at=at,
                    app_mode="DRY_RUN",
                    is_safe=True,
                    metadata={
                        "research": {
                            "enabled": True,
                            "calibration_enabled": True,
                            "worker_role": "research",
                            "last_archive_run": {"archived_events": 10},
                        }
                    },
                )
            )
            session.commit()

        status = build_research_status(config, now=at + timedelta(seconds=10))

        assert status["api_local_configuration"]["research_enabled"] is False
        assert status["worker_observed_enabled"] is True
        assert status["effective_enabled"] is True
        assert status["last_archive_run"] == {"archived_events": 10}
        assert status["last_outcome_label_run"] is None
    finally:
        engine.dispose()
