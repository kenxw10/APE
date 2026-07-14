from __future__ import annotations

import pytest
from scripts import railway_start_api, railway_start_worker


def test_railway_api_start_steps_run_migrations_then_api() -> None:
    assert railway_start_api.STARTUP_STEPS == (
        "python -m ape.db.migrations",
        "python -m ape.api.main",
    )


def test_railway_worker_start_steps_run_migrations_then_worker() -> None:
    assert railway_start_worker.STARTUP_STEPS == (
        "python -m ape.db.migrations",
        "python -m ape.worker.main",
    )


def test_railway_api_start_stops_when_migrations_fail(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(railway_start_api, "migrations_main", lambda: 1)
    monkeypatch.setattr(railway_start_api, "api_main", lambda: calls.append("api"))

    assert railway_start_api.run() == 1
    assert calls == []


def test_railway_worker_start_stops_when_migrations_fail(monkeypatch) -> None:
    calls: list[str] = []

    def worker_main() -> int:
        calls.append("worker")
        return 0

    monkeypatch.setattr(railway_start_worker, "migrations_main", lambda: 1)
    monkeypatch.setattr(railway_start_worker, "worker_main", worker_main)

    assert railway_start_worker.run() == 1
    assert calls == []


def test_railway_worker_start_runs_migrations_then_worker(monkeypatch) -> None:
    calls: list[str] = []

    def migrations_main() -> int:
        calls.append("migrations")
        return 0

    def worker_main() -> int:
        calls.append("worker")
        return 0

    monkeypatch.setattr(railway_start_worker, "migrations_main", migrations_main)
    monkeypatch.setattr(railway_start_worker, "worker_main", worker_main)

    assert railway_start_worker.run() == 0
    assert calls == ["migrations", "worker"]


@pytest.mark.parametrize(
    ("launcher", "service_name"),
    (
        (railway_start_api, "api"),
        (railway_start_worker, "worker"),
    ),
)
def test_both_railway_launchers_fail_closed_before_services(
    launcher, service_name: str, monkeypatch
) -> None:
    calls: list[str] = []

    monkeypatch.setattr(launcher, "migrations_main", lambda: 1)
    monkeypatch.setattr(launcher, f"{service_name}_main", lambda: calls.append(service_name))

    assert launcher.run() == 1
    assert calls == []
