from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient

from ape.api.main import create_app
from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.inputs import (
    StrategyDecisionInput,
    StrategyDryRunEventInput,
    StrategyDryRunPositionInput,
    WorkerHeartbeatInput,
)
from ape.repositories.strategy_decisions import StrategyDecisionsRepository
from ape.repositories.strategy_dry_run import StrategyDryRunRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository


def test_strategy_status_is_disabled_without_database() -> None:
    app = create_app(load_config({}))

    with TestClient(app) as client:
        status_response = client.get("/strategy/status")
        latest_response = client.get("/strategy/decisions/latest")
        recent_response = client.get("/strategy/decisions/recent")
        dry_run_status_response = client.get("/strategy/dry-run/status")

    assert status_response.status_code == 200
    status = status_response.json()
    assert status["enabled"] is False
    assert status["connection_state"] == "disabled"
    assert status["stale"] is False
    assert status["latest_decision_id"] is None
    assert latest_response.json()["found"] is False
    assert recent_response.json()["count"] == 0
    dry_run_status = dry_run_status_response.json()
    assert dry_run_status["enabled"] is False
    assert dry_run_status["open_position_count"] == 0
    assert dry_run_status["latest_event"]["found"] is False


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


def test_strategy_dry_run_endpoints_report_read_only_ledger(tmp_path) -> None:
    now = datetime.now(UTC)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_strategy_dry_run_api.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "APP_MODE": "DRY_RUN",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STRATEGY_DRY_RUN_ENABLED": "true",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            StrategyDecisionsRepository(session).insert_decision(
                StrategyDecisionInput(
                    decision_id="strategy-KXBTC15M-ACTIVE-1-enter",
                    evaluated_at=now,
                    decision_state="ENTER_DRY_RUN",
                    primary_reason="dry_run_entry_signal",
                    app_mode="DRY_RUN",
                    market_ticker="KXBTC15M-ACTIVE",
                    candidate_side="YES",
                    boundary=Decimal("62000"),
                    brti_value=Decimal("62100"),
                    distance_bps=Decimal("16.10305958"),
                    seconds_left=300,
                    measurements={
                        "desired_side_ask": "0.62",
                        "dry_run_position_id": "dryrun-btc15-KXBTC15M-ACTIVE",
                    },
                    blockers=[],
                    warnings=[],
                    raw_context_hash="enter",
                )
            )
            repository = StrategyDryRunRepository(session)
            repository.insert_position_if_absent(
                StrategyDryRunPositionInput(
                    position_id="dryrun-btc15-KXBTC15M-ACTIVE",
                    strategy_id="btc15_momentum_v1",
                    market_ticker="KXBTC15M-ACTIVE",
                    decision_id="strategy-KXBTC15M-ACTIVE-1-enter",
                    side_candidate="YES",
                    economic_side="YES",
                    opened_at=now,
                    open_price=Decimal("0.63"),
                    contract_count=1,
                    boundary=Decimal("62000"),
                    brti_at_entry=Decimal("62100"),
                    distance_bps_at_entry=Decimal("16.10305958"),
                    entry_reason="dry_run_entry_signal",
                    status="OPEN",
                    measurements={"desired_side_ask": "0.62"},
                )
            )
            repository.insert_event_if_absent(
                StrategyDryRunEventInput(
                    event_id="dryrun-event-enter",
                    position_id="dryrun-btc15-KXBTC15M-ACTIVE",
                    decision_id="strategy-KXBTC15M-ACTIVE-1-enter",
                    event_type="ENTER_DRY_RUN",
                    market_ticker="KXBTC15M-ACTIVE",
                    occurred_at=now,
                    side_candidate="YES",
                    price=Decimal("0.63"),
                    contract_count=1,
                    reason="dry_run_entry_signal",
                    measurements={"desired_side_ask": "0.62"},
                )
            )
            WorkerHeartbeatRepository(session).record_heartbeat(
                WorkerHeartbeatInput(
                    service_name="ape-worker",
                    started_at=now - timedelta(minutes=1),
                    heartbeat_at=now,
                    app_mode="DRY_RUN",
                    is_safe=True,
                    metadata={
                        "mode": "strategy_observer",
                        "strategy": {
                            "dry_run": {
                                "enabled": True,
                                "open_position_count": 1,
                                "latest_event_type": "ENTER_DRY_RUN",
                                "latest_position_id": "dryrun-btc15-KXBTC15M-ACTIVE",
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
            status_response = client.get("/strategy/dry-run/status")
            open_response = client.get("/strategy/dry-run/positions/open")
            recent_positions_response = client.get(
                "/strategy/dry-run/positions/recent?limit=10"
            )
            recent_events_response = client.get("/strategy/dry-run/events/recent?limit=10")

        assert status_response.status_code == 200
        status = status_response.json()
        assert status["enabled"] is True
        assert status["open_position_count"] == 1
        assert status["latest_event"]["event_type"] == "ENTER_DRY_RUN"
        assert status["latest_enter_decision"]["decision_state"] == "ENTER_DRY_RUN"

        open_positions = open_response.json()
        assert open_positions["count"] == 1
        assert open_positions["positions"][0]["position_id"] == (
            "dryrun-btc15-KXBTC15M-ACTIVE"
        )
        assert "raw_payload" not in open_positions["positions"][0]

        assert recent_positions_response.json()["count"] == 1
        events = recent_events_response.json()
        assert events["count"] == 1
        assert events["events"][0]["event_type"] == "ENTER_DRY_RUN"
        assert "order_id" not in events["events"][0]
    finally:
        engine.dispose()
