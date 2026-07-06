from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.inputs import (
    MarketInput,
    OrderbookSnapshotInput,
    ReferenceTickInput,
    WorkerHeartbeatInput,
)
from ape.repositories.markets import MarketsRepository
from ape.repositories.orderbook import OrderbookRepository
from ape.repositories.reference_ticks import ReferenceTicksRepository
from ape.repositories.strategy_decisions import StrategyDecisionsRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.safety import assess_startup_safety
from ape.strategy.observer import (
    STATE_BOOK_UNUSABLE,
    STATE_OBSERVE_ONLY_MARKET,
    STATE_REFERENCE_STALE,
    StrategyObserver,
    evaluate_strategy_observer,
)


@pytest.fixture
def session(tmp_path):
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_strategy.sqlite'}"
    engine = create_engine_from_config(load_config({"DATABASE_URL": database_url}))
    run_migrations(engine)
    session_factory = create_session_factory(engine)

    try:
        with session_factory() as db_session:
            yield db_session
    finally:
        engine.dispose()


def test_strategy_observer_evaluates_observer_only_market(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({})
    safety = assess_startup_safety(config)
    _seed_observable_context(session, now=now)

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_OBSERVE_ONLY_MARKET
    assert decision.primary_reason == "observer_decision_ledger_only"
    assert decision.candidate_side == "YES"
    assert decision.boundary == Decimal("62000")
    assert decision.measurements["observer_only"] is True
    assert decision.measurements["desired_side_ask"] == "0.62"
    assert decision.measurements["config"]["strategy_max_spread_cents"] == 4
    assert "ENTER" not in decision.decision_state


def test_strategy_observer_prioritizes_reference_before_book(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({})
    safety = assess_startup_safety(config)
    _seed_market(session, now=now)

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_REFERENCE_STALE
    assert decision.primary_reason == "brti_reference_missing_or_invalid"


def test_strategy_observer_blocks_unusable_desired_book(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({})
    safety = assess_startup_safety(config)
    _seed_observable_context(
        session,
        now=now,
        yes_bid=Decimal("0.60"),
        yes_ask=Decimal("0.69"),
        yes_spread=Decimal("0.09"),
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_BOOK_UNUSABLE
    assert decision.primary_reason == "desired_side_book_unusable"
    assert decision.measurements["desired_side_spread_cents"] == "9"


def test_strategy_observer_runtime_records_decision_and_heartbeat(tmp_path) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_strategy_runtime.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "STRATEGY_OBSERVER_ENABLED": "true",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            _seed_observable_context(session, now=now)
            WorkerHeartbeatRepository(session).record_heartbeat(
                WorkerHeartbeatInput(
                    service_name="ape-worker",
                    started_at=now - timedelta(minutes=1),
                    heartbeat_at=now - timedelta(seconds=1),
                    app_mode="OBSERVER",
                    is_safe=True,
                    metadata={
                        "mode": "kalshi_ws",
                        "ws": {
                            "enabled": True,
                            "connection_state": "subscribed",
                            "active_market_ticker": "KXBTC15M-ACTIVE",
                        },
                        "reference": {
                            "brti": {
                                "enabled": True,
                                "connection_state": "subscribed",
                            }
                        },
                    },
                )
            )
            session.commit()

        observer = StrategyObserver(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=session_factory,
            started_at=now - timedelta(minutes=1),
            now=lambda: now,
        )
        observer.evaluate_once()

        with session_factory() as session:
            latest_decision = StrategyDecisionsRepository(session).get_latest_decision()
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert latest_decision is not None
            assert latest_decision.decision_state == STATE_OBSERVE_ONLY_MARKET
            assert heartbeat is not None
            observer_metadata = heartbeat.metadata_["strategy"]["observer"]
            assert observer_metadata["enabled"] is True
            assert observer_metadata["connection_state"] == "running"
            assert observer_metadata["last_decision_state"] == STATE_OBSERVE_ONLY_MARKET
            assert heartbeat.metadata_["ws"]["connection_state"] == "subscribed"
            assert heartbeat.metadata_["reference"]["brti"]["connection_state"] == "subscribed"
    finally:
        engine.dispose()


def _seed_observable_context(
    session,
    *,
    now: datetime,
    yes_bid: Decimal = Decimal("0.60"),
    yes_ask: Decimal = Decimal("0.62"),
    yes_spread: Decimal = Decimal("0.02"),
) -> None:
    _seed_market(session, now=now)
    ReferenceTicksRepository(session).insert_tick(
        ReferenceTickInput(
            source="kalshi_cfbenchmarks_brti",
            received_at=now - timedelta(milliseconds=500),
            source_ts=now - timedelta(milliseconds=500),
            raw_value="62100",
            parsed_value=Decimal("62100"),
            source_age_ms=500,
            parse_status="valid",
        )
    )
    OrderbookRepository(session).insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker="KXBTC15M-ACTIVE",
            received_at=now - timedelta(milliseconds=500),
            sequence_number=123,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            no_bid=Decimal("0.38"),
            no_ask=Decimal("0.40"),
            yes_spread=yes_spread,
            no_spread=Decimal("0.02"),
            book_status="ok",
        )
    )
    session.commit()


def _seed_market(session, *, now: datetime) -> None:
    MarketsRepository(session).upsert_market(
        MarketInput(
            market_ticker="KXBTC15M-ACTIVE",
            event_ticker="KXBTC15M-26JUL051200",
            series_ticker="KXBTC15M",
            open_time=now - timedelta(minutes=10),
            close_time=now + timedelta(minutes=5),
            functional_strike=Decimal("62000"),
            resolver_decision_reason="market_interval_contains_now",
        )
    )
    session.commit()
