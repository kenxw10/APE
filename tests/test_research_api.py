from __future__ import annotations

from fastapi.testclient import TestClient

from ape.api.main import create_app
from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.session import create_engine_from_config


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
            research_routes = [
                route for route in app.routes if getattr(route, "path", "").startswith("/research/")
            ]
            assert all(route.methods == {"GET"} for route in research_routes)
    finally:
        engine.dispose()
