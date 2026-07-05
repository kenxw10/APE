from __future__ import annotations

from fastapi.testclient import TestClient

from ape.api.main import create_app
from ape.config import load_config


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

