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
    PublicTradeInput,
    ReferenceTickInput,
    StrategyDryRunPositionInput,
    WorkerHeartbeatInput,
)
from ape.repositories.markets import MarketsRepository
from ape.repositories.orderbook import OrderbookRepository
from ape.repositories.public_trades import PublicTradesRepository
from ape.repositories.reference_ticks import ReferenceTicksRepository
from ape.repositories.strategy_decisions import StrategyDecisionsRepository
from ape.repositories.strategy_dry_run import StrategyDryRunRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.safety import assess_startup_safety
from ape.strategy.observer import (
    STATE_CONTRACT_NOT_CONFIRMED,
    STATE_ENTER_DRY_RUN,
    STATE_IMPULSE_TOO_WEAK,
    STATE_OBSERVE_ONLY_MARKET,
    STATE_REFERENCE_STALE,
    STATE_SPREAD_TOO_WIDE,
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


def test_strategy_dry_run_records_hypothetical_entry_and_event(tmp_path) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_strategy_dry_run.sqlite'}"
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
            _seed_observable_context(session, now=now)

        observer = StrategyObserver(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=session_factory,
            started_at=now - timedelta(minutes=1),
            now=lambda: now,
        )
        decision = observer.evaluate_once()

        with session_factory() as session:
            latest_decision = StrategyDecisionsRepository(session).get_latest_decision()
            dry_run_repository = StrategyDryRunRepository(session)
            open_positions = dry_run_repository.list_open_positions(
                strategy_id=config.strategy_id
            )
            events = dry_run_repository.list_recent_events(limit=10)
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

        assert decision is not None
        assert latest_decision is not None
        assert latest_decision.decision_state == STATE_ENTER_DRY_RUN
        assert len(open_positions) == 1
        assert open_positions[0].decision_id == latest_decision.decision_id
        assert open_positions[0].open_price == Decimal("0.63")
        assert len(events) == 1
        assert events[0].event_type == STATE_ENTER_DRY_RUN
        assert heartbeat is not None
        assert heartbeat.metadata_["strategy"]["dry_run"]["enabled"] is True
        assert heartbeat.metadata_["strategy"]["dry_run"]["open_position_count"] == 1
    finally:
        engine.dispose()


def test_strategy_entry_bounds_use_offset_adjusted_dry_run_price(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({"APP_MODE": "DRY_RUN", "STRATEGY_DRY_RUN_ENABLED": "true"})
    safety = assess_startup_safety(config)
    _seed_observable_context(
        session,
        now=now,
        yes_bid=Decimal("0.76"),
        yes_ask=Decimal("0.78"),
        yes_spread=Decimal("0.02"),
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_CONTRACT_NOT_CONFIRMED
    assert decision.primary_reason == "dry_run_intended_entry_price_outside_range"
    assert decision.measurements["dry_run_intended_entry_price"] == "0.79"


def test_strategy_entry_bounds_allow_offset_to_reach_minimum(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config(
        {
            "APP_MODE": "DRY_RUN",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STRATEGY_DRY_RUN_ENABLED": "true",
        }
    )
    safety = assess_startup_safety(config)
    _seed_observable_context(
        session,
        now=now,
        yes_bid=Decimal("0.53"),
        yes_ask=Decimal("0.55"),
        yes_spread=Decimal("0.02"),
        initial_yes_bid=Decimal("0.49"),
        initial_yes_ask=Decimal("0.51"),
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_ENTER_DRY_RUN
    assert decision.measurements["dry_run_intended_entry_price"] == "0.56"


def test_strategy_dry_run_allows_additional_entry_when_multi_position_enabled(
    session,
) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config(
        {
            "APP_MODE": "DRY_RUN",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STRATEGY_DRY_RUN_ENABLED": "true",
            "STRATEGY_DRY_RUN_MAX_OPEN_POSITIONS": "2",
            "STRATEGY_DRY_RUN_ONE_ENTRY_PER_MARKET": "false",
        }
    )
    safety = assess_startup_safety(config)
    _seed_observable_context(session, now=now)
    StrategyDryRunRepository(session).insert_position_if_absent(
        StrategyDryRunPositionInput(
            position_id="dryrun-existing-position",
            strategy_id=config.strategy_id,
            market_ticker="KXBTC15M-ACTIVE",
            decision_id="strategy-existing-enter",
            side_candidate="YES",
            economic_side="YES",
            opened_at=now - timedelta(seconds=30),
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

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_ENTER_DRY_RUN
    assert decision.primary_reason == "dry_run_entry_signal"
    assert decision.measurements["managed_position_id"] is None


def test_strategy_dry_run_mode_without_flag_stays_observe_only(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({"APP_MODE": "DRY_RUN"})
    safety = assess_startup_safety(config)
    _seed_observable_context(session, now=now)

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_OBSERVE_ONLY_MARKET
    assert decision.primary_reason == "dry_run_disabled_observe_only"
    assert decision.measurements["dry_run_risk_state"] == "dry_run_disabled"


def test_strategy_blocks_weak_brti_impulse(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({"STRATEGY_BRTI_MIN_MOVE_LONG_BPS": "999"})
    safety = assess_startup_safety(config)
    _seed_observable_context(session, now=now)

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_IMPULSE_TOO_WEAK
    assert decision.primary_reason == "weak_long_brti_move"
    assert decision.measurements["brti_move_long_bps"] is not None


def test_strategy_observer_prioritizes_reference_before_book(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({})
    safety = assess_startup_safety(config)
    _seed_market(session, now=now)
    WorkerHeartbeatRepository(session).record_heartbeat(
        WorkerHeartbeatInput(
            service_name="ape-worker",
            started_at=now - timedelta(minutes=1),
            heartbeat_at=now,
            app_mode="OBSERVER",
            is_safe=True,
            metadata={
                "reference": {
                    "brti": {
                        "connection_state": "reconnect_pending",
                        "recovery_state": "reconnecting",
                        "warnings": ["brti_reference_first_tick_timeout"],
                        "blockers": [],
                        "consecutive_stale_count": 1,
                        "consecutive_reconnect_count": 1,
                    }
                }
            },
        )
    )
    session.commit()

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_REFERENCE_STALE
    assert decision.primary_reason == "brti_reference_missing_or_invalid"
    assert (
        decision.measurements["brti_reference_stale_reason"]
        == "brti_reference_first_tick_timeout"
    )
    assert decision.measurements["brti_reference_connection_state"] == "reconnect_pending"
    assert decision.measurements["brti_reference_recovery_state"] == "reconnecting"


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

    assert decision.decision_state == STATE_SPREAD_TOO_WIDE
    assert decision.primary_reason == "desired_side_spread_too_wide"
    assert decision.measurements["desired_side_spread_cents"] == "9"


def test_strategy_observer_runtime_records_decision_and_heartbeat(tmp_path) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_strategy_runtime.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "STRATEGY_OBSERVER_ENABLED": "true",
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
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


def test_strategy_observer_heartbeat_drops_stale_collector_metadata_when_disabled(
    tmp_path,
) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_strategy_drop_collectors.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "STRATEGY_OBSERVER_ENABLED": "true",
            "KALSHI_WS_ENABLED": "false",
            "KALSHI_CFBENCHMARKS_ENABLED": "false",
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
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

            assert heartbeat is not None
            assert heartbeat.metadata_["strategy"]["observer"]["enabled"] is True
            assert "ws" not in heartbeat.metadata_
            assert "reference" not in heartbeat.metadata_
    finally:
        engine.dispose()


def _seed_observable_context(
    session,
    *,
    now: datetime,
    yes_bid: Decimal = Decimal("0.60"),
    yes_ask: Decimal = Decimal("0.62"),
    yes_spread: Decimal = Decimal("0.02"),
    initial_yes_bid: Decimal = Decimal("0.55"),
    initial_yes_ask: Decimal = Decimal("0.57"),
) -> None:
    _seed_market(session, now=now)
    reference_repository = ReferenceTicksRepository(session)
    for seconds_ago in range(180, -1, -5):
        value = Decimal("62020") + Decimal(180 - seconds_ago) * Decimal("0.5")
        received_at = now - timedelta(seconds=seconds_ago, milliseconds=500)
        reference_repository.insert_tick(
            ReferenceTickInput(
                source="kalshi_cfbenchmarks_brti",
                received_at=received_at,
                source_ts=received_at,
                raw_value=str(value),
                parsed_value=value,
                source_age_ms=500,
                parse_status="valid",
            )
        )
    orderbook_repository = OrderbookRepository(session)
    orderbook_repository.insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker="KXBTC15M-ACTIVE",
            received_at=now - timedelta(seconds=45),
            sequence_number=122,
            yes_bid=initial_yes_bid,
            yes_ask=initial_yes_ask,
            no_bid=Decimal("0.43"),
            no_ask=Decimal("0.45"),
            yes_spread=Decimal("0.02"),
            no_spread=Decimal("0.02"),
            yes_ask_count=Decimal("3"),
            no_ask_count=Decimal("3"),
            book_status="ok",
        )
    )
    orderbook_repository.insert_snapshot(
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
            yes_ask_count=Decimal("3"),
            no_ask_count=Decimal("3"),
            book_status="ok",
        )
    )
    PublicTradesRepository(session).insert_trade(
        PublicTradeInput(
            market_ticker="KXBTC15M-ACTIVE",
            received_at=now - timedelta(seconds=10),
            executed_at=now - timedelta(seconds=10),
            price=Decimal("0.61"),
            trade_count=Decimal("1"),
            side_inferred="YES",
        )
    )
    PublicTradesRepository(session).insert_trade(
        PublicTradeInput(
            market_ticker="KXBTC15M-ACTIVE",
            received_at=now - timedelta(seconds=8),
            executed_at=now - timedelta(seconds=8),
            price=Decimal("0.62"),
            trade_count=Decimal("1"),
            side_inferred="YES",
        )
    )
    PublicTradesRepository(session).insert_trade(
        PublicTradeInput(
            market_ticker="KXBTC15M-ACTIVE",
            received_at=now - timedelta(seconds=5),
            executed_at=now - timedelta(seconds=5),
            price=Decimal("0.62"),
            trade_count=Decimal("1"),
            side_inferred="YES",
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
