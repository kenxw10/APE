from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient

from ape.api.main import create_app
from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.inputs import StrategyDecisionInput, WorkerHeartbeatInput
from ape.repositories.strategy_decisions import StrategyDecisionsRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository


def test_strategy_status_is_disabled_without_database() -> None:
    app = create_app(load_config({}))

    with TestClient(app) as client:
        status_response = client.get("/strategy/status")
        latest_response = client.get("/strategy/decisions/latest")
        recent_response = client.get("/strategy/decisions/recent")

    assert status_response.status_code == 200
    status = status_response.json()
    assert status["enabled"] is False
    assert status["connection_state"] == "disabled"
    assert status["stale"] is False
    assert status["latest_decision_id"] is None
    assert latest_response.json()["found"] is False
    assert recent_response.json()["count"] == 0


def test_strategy_status_reports_latest_decision_and_worker_metadata(tmp_path) -> None:
    now = datetime.now(UTC)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_strategy_api.sqlite'}"
    config = load_config({"DATABASE_URL": database_url})
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            StrategyDecisionsRepository(session).insert_decision(
                StrategyDecisionInput(
                    decision_id="strategy-KXBTC15M-ACTIVE-1-abc",
                    evaluated_at=now,
                    decision_state="OBSERVE_ONLY_MARKET",
                    primary_reason="observer_decision_ledger_only",
                    app_mode="OBSERVER",
                    market_ticker="KXBTC15M-ACTIVE",
                    candidate_side="YES",
                    boundary=Decimal("62000"),
                    brti_value=Decimal("62100"),
                    distance_bps=Decimal("16.10305958"),
                    seconds_left=300,
                    measurements={
                        "boundary": "62000",
                        "brti_value": "62100",
                        "distance_bps": "16.10305958",
                        "candidate_side": "YES",
                        "seconds_left": 300,
                        "desired_side_ask": "0.62",
                    },
                    blockers=[],
                    warnings=[],
                    raw_context_hash="abc",
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
                        "mode": "strategy_observer",
                        "strategy": {
                            "observer": {
                                "enabled": True,
                                "connection_state": "running",
                                "last_evaluated_at": now.isoformat(),
                                "last_decision_state": "OBSERVE_ONLY_MARKET",
                                "last_primary_reason": "observer_decision_ledger_only",
                                "last_decision_id": "strategy-KXBTC15M-ACTIVE-1-abc",
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
            status_response = client.get("/strategy/status")
            latest_response = client.get("/strategy/decisions/latest")
            recent_response = client.get("/strategy/decisions/recent?limit=1")

        assert status_response.status_code == 200
        status = status_response.json()
        assert status["enabled"] is True
        assert status["worker_observed_enabled"] is True
        assert status["connection_state"] == "running"
        assert status["latest_decision_state"] == "OBSERVE_ONLY_MARKET"
        assert status["latest_primary_reason"] == "observer_decision_ledger_only"
        assert status["candidate_side"] == "YES"
        assert status["stale"] is False
        assert status["latest_measurements_summary"]["desired_side_ask"] == "0.62"

        assert latest_response.json()["found"] is True
        recent = recent_response.json()
        assert recent["count"] == 1
        assert recent["decisions"][0]["decision_state"] == "OBSERVE_ONLY_MARKET"
    finally:
        engine.dispose()
