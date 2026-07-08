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
    STATE_EXIT_SIGNAL,
    STATE_FORCE_EXIT,
    STATE_IMPULSE_TOO_WEAK,
    STATE_KALSHI_STALE,
    STATE_LIVE_GUARD_BLOCKED,
    STATE_MANAGE_POSITION,
    STATE_NO_ACTIVE_MARKET,
    STATE_OBSERVE_ONLY_MARKET,
    STATE_REFERENCE_STALE,
    STATE_RISK_BLOCKED,
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
        assert decision.blockers == []
        assert latest_decision.decision_state == STATE_ENTER_DRY_RUN
        assert latest_decision.blockers == []
        assert len(open_positions) == 1
        assert open_positions[0].decision_id == latest_decision.decision_id
        assert open_positions[0].open_price == Decimal("0.63")
        assert len(events) == 1
        assert events[0].event_type == STATE_ENTER_DRY_RUN
        assert heartbeat is not None
        assert heartbeat.metadata_["strategy"]["observer"]["blockers"] == []
        assert heartbeat.metadata_["strategy"]["dry_run"]["blockers"] == []
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
    assert decision.primary_reason == "dry_run_intended_entry_price_too_high"
    assert decision.measurements["dry_run_intended_entry_price"] == "0.79"
    assert decision.measurements["gate_results"]["entry_price"]["status"] == "block"
    assert (
        decision.measurements["gate_results"]["contract_confirmation"]["status"]
        != "block"
    )


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


def test_strategy_warns_when_trade_confirmation_sample_too_small(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config(
        {
            "APP_MODE": "DRY_RUN",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STRATEGY_DRY_RUN_ENABLED": "true",
            "STRATEGY_TRADE_CONFIRMATION_MIN_TRADES": "4",
        }
    )
    safety = assess_startup_safety(config)
    _seed_observable_context(session, now=now)

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_ENTER_DRY_RUN
    assert decision.primary_reason == "dry_run_entry_signal"
    assert "recent_trade_confirmation_insufficient_trades" in decision.warnings
    assert decision.measurements["recent_trade_count"] == 3
    assert decision.measurements["gate_results"]["trade_confirmation"]["status"] == "warn"
    assert (
        decision.measurements["gate_results"]["trade_confirmation"]["reason"]
        == "recent_trade_confirmation_insufficient_trades"
    )


def test_strategy_warns_but_enters_when_brti_source_age_is_warning_only(
    session,
) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config(
        {
            "APP_MODE": "DRY_RUN",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STRATEGY_DRY_RUN_ENABLED": "true",
            "STRATEGY_REFERENCE_SOURCE_WARN_MS": "10000",
            "STRATEGY_REFERENCE_SOURCE_MAX_AGE_MS": "45000",
        }
    )
    safety = assess_startup_safety(config)
    _seed_observable_context(session, now=now)
    ReferenceTicksRepository(session).insert_tick(
        ReferenceTickInput(
            source="kalshi_cfbenchmarks_brti",
            received_at=now,
            source_ts=now - timedelta(seconds=12),
            raw_value="62110",
            parsed_value=Decimal("62110"),
            source_age_ms=12_000,
            parse_status="valid",
        )
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_ENTER_DRY_RUN
    assert "brti_reference_source_age_warning" in decision.warnings
    assert decision.measurements["brti_reference_trade_ready_fresh"] is True
    assert decision.measurements["gate_results"]["reference"]["status"] == "warn"


def test_strategy_blocks_when_brti_backend_age_exceeds_limit(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({"STRATEGY_REFERENCE_MAX_AGE_MS": "2000"})
    safety = assess_startup_safety(config)
    _seed_observable_context(
        session,
        now=now,
        reference_received_lag_ms=3_000,
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_REFERENCE_STALE
    assert decision.primary_reason == "brti_reference_backend_age_exceeds_limit"
    assert decision.measurements["gate_results"]["reference"]["status"] == "block"


def test_strategy_keeps_hard_brti_age_block_when_trade_ready_fresh_relaxed(
    session,
) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config(
        {
            "STRATEGY_REFERENCE_MAX_AGE_MS": "2000",
            "STRATEGY_REFERENCE_REQUIRE_TRADE_READY_FRESH": "false",
        }
    )
    safety = assess_startup_safety(config)
    _seed_observable_context(
        session,
        now=now,
        reference_received_lag_ms=3_000,
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_REFERENCE_STALE
    assert decision.primary_reason == "brti_reference_backend_age_exceeds_limit"
    assert decision.measurements["gate_results"]["reference"]["status"] == "block"


def test_strategy_carries_forward_old_brti_when_valid_messages_are_fresh(
    session,
) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({"STRATEGY_REFERENCE_MAX_AGE_MS": "2000"})
    safety = assess_startup_safety(config)
    _seed_observable_context(
        session,
        now=now,
        reference_received_lag_ms=5_000,
    )
    latest_tick = ReferenceTicksRepository(session).get_latest_valid_tick(
        "kalshi_cfbenchmarks_brti"
    )
    assert latest_tick is not None
    _record_feed_heartbeat(
        session,
        now=now,
        brti_last_valid_message_at=now - timedelta(seconds=1),
        brti_last_valid_message_source_ts=latest_tick.source_ts,
        brti_last_valid_message_value=str(latest_tick.parsed_value),
        brti_carried_forward=True,
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state != STATE_REFERENCE_STALE
    assert "brti_reference_carried_forward" in decision.warnings
    assert decision.measurements["brti_reference_carry_forward_allowed"] is True
    assert decision.measurements["brti_strategy_fresh_age_ms"] == 1000
    assert decision.measurements["gate_results"]["reference"]["status"] == "warn"
    assert (
        decision.measurements["gate_results"]["reference"]["reason"]
        == "brti_reference_carried_forward"
    )


def test_strategy_blocks_old_brti_when_valid_message_stream_is_stale(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({"STRATEGY_REFERENCE_MAX_AGE_MS": "2000"})
    safety = assess_startup_safety(config)
    _seed_observable_context(
        session,
        now=now,
        reference_received_lag_ms=5_000,
    )
    latest_tick = ReferenceTicksRepository(session).get_latest_valid_tick(
        "kalshi_cfbenchmarks_brti"
    )
    assert latest_tick is not None
    _record_feed_heartbeat(
        session,
        now=now,
        brti_last_valid_message_at=now - timedelta(seconds=4),
        brti_last_valid_message_source_ts=latest_tick.source_ts,
        brti_last_valid_message_value=str(latest_tick.parsed_value),
        brti_carried_forward=True,
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_REFERENCE_STALE
    assert decision.primary_reason == "brti_reference_stream_stale"
    assert decision.measurements["gate_results"]["reference"]["status"] == "block"


def test_strategy_blocks_old_brti_when_carry_forward_cap_exceeded(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({"STRATEGY_REFERENCE_MAX_AGE_MS": "2000"})
    safety = assess_startup_safety(config)
    _seed_observable_context(
        session,
        now=now,
        reference_received_lag_ms=16_000,
    )
    latest_tick = ReferenceTicksRepository(session).get_latest_valid_tick(
        "kalshi_cfbenchmarks_brti"
    )
    assert latest_tick is not None
    _record_feed_heartbeat(
        session,
        now=now,
        brti_last_valid_message_at=now - timedelta(seconds=1),
        brti_last_valid_message_source_ts=latest_tick.source_ts,
        brti_last_valid_message_value=str(latest_tick.parsed_value),
        brti_carried_forward=True,
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_REFERENCE_STALE
    assert decision.primary_reason == "brti_reference_carry_forward_age_exceeds_limit"
    assert decision.measurements["gate_results"]["reference"]["status"] == "block"


def test_strategy_carries_forward_old_orderbook_when_stream_is_live(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({"STRATEGY_KALSHI_BOOK_MAX_AGE_MS": "2000"})
    safety = assess_startup_safety(config)
    _seed_observable_context(
        session,
        now=now,
        latest_orderbook_received_at=now - timedelta(seconds=5),
    )
    _record_feed_heartbeat(session, now=now)

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state != STATE_KALSHI_STALE
    assert "kalshi_orderbook_carried_forward" in decision.warnings
    assert decision.measurements["orderbook_carry_forward_allowed"] is True
    assert decision.measurements["orderbook_snapshot_source"] == "carried_forward"
    assert decision.measurements["gate_results"]["book"]["status"] == "warn"
    assert (
        decision.measurements["gate_results"]["book"]["reason"]
        == "kalshi_orderbook_carried_forward"
    )


def test_strategy_blocks_old_orderbook_when_stream_is_stale(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({"STRATEGY_KALSHI_BOOK_MAX_AGE_MS": "2000"})
    safety = assess_startup_safety(config)
    _seed_observable_context(
        session,
        now=now,
        latest_orderbook_received_at=now - timedelta(seconds=5),
    )
    _record_feed_heartbeat(
        session,
        now=now,
        orderbook_stream_last_message_at=now - timedelta(seconds=4),
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_KALSHI_STALE
    assert decision.primary_reason == "kalshi_orderbook_stream_stale"
    assert decision.measurements["gate_results"]["book"]["status"] == "block"


def test_strategy_blocks_old_orderbook_when_active_ticker_mismatches(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({"STRATEGY_KALSHI_BOOK_MAX_AGE_MS": "2000"})
    safety = assess_startup_safety(config)
    _seed_observable_context(
        session,
        now=now,
        latest_orderbook_received_at=now - timedelta(seconds=5),
    )
    _record_feed_heartbeat(
        session,
        now=now,
        orderbook_active_market_ticker="KXBTC15M-OTHER",
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_KALSHI_STALE
    assert decision.primary_reason == "kalshi_orderbook_active_ticker_mismatch"


def test_strategy_blocks_old_orderbook_when_sequence_reset_warning_active(
    session,
) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({"STRATEGY_KALSHI_BOOK_MAX_AGE_MS": "2000"})
    safety = assess_startup_safety(config)
    _seed_observable_context(
        session,
        now=now,
        latest_orderbook_received_at=now - timedelta(seconds=5),
    )
    _record_feed_heartbeat(
        session,
        now=now,
        orderbook_warnings=["orderbook_sequence_gap_reset"],
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_KALSHI_STALE
    assert decision.primary_reason == "kalshi_orderbook_sequence_gap_or_reset"


def test_strategy_blocks_old_orderbook_when_carry_forward_cap_exceeded(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({"STRATEGY_KALSHI_BOOK_MAX_AGE_MS": "2000"})
    safety = assess_startup_safety(config)
    _seed_observable_context(
        session,
        now=now,
        latest_orderbook_received_at=now - timedelta(seconds=31),
    )
    _record_feed_heartbeat(session, now=now)

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_KALSHI_STALE
    assert decision.primary_reason == "kalshi_orderbook_carry_forward_age_exceeds_limit"


def test_strategy_gate_summary_blocks_contract_ask_pullback(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({})
    safety = assess_startup_safety(config)
    _seed_observable_context(session, now=now)
    OrderbookRepository(session).insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker="KXBTC15M-ACTIVE",
            received_at=now - timedelta(seconds=10),
            sequence_number=124,
            yes_bid=Decimal("0.60"),
            yes_ask=Decimal("0.66"),
            no_bid=Decimal("0.34"),
            no_ask=Decimal("0.40"),
            yes_spread=Decimal("0.06"),
            no_spread=Decimal("0.06"),
            yes_bid_count=Decimal("3"),
            yes_ask_count=Decimal("3"),
            no_bid_count=Decimal("3"),
            no_ask_count=Decimal("3"),
            book_status="ok",
        )
    )
    session.commit()

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_CONTRACT_NOT_CONFIRMED
    assert decision.primary_reason == "ask_pullback_above_threshold"
    assert (
        decision.measurements["gate_results"]["contract_confirmation"]["status"]
        == "block"
    )
    assert (
        decision.measurements["gate_results"]["contract_confirmation"]["reason"]
        == "ask_pullback_above_threshold"
    )


def test_strategy_gate_summary_blocks_insufficient_contract_history(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config(
        {
            "STRATEGY_CONTRACT_LOOKBACK_SECONDS": "1",
            "STRATEGY_CONTRACT_ASK_PULLBACK_LOOKBACK_SECONDS": "1",
        }
    )
    safety = assess_startup_safety(config)
    _seed_observable_context(session, now=now)

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_CONTRACT_NOT_CONFIRMED
    assert decision.primary_reason == "insufficient_contract_history"
    assert (
        decision.measurements["gate_results"]["contract_confirmation"]["status"]
        == "block"
    )
    assert (
        decision.measurements["gate_results"]["contract_confirmation"]["reason"]
        == "insufficient_contract_history"
    )


def test_strategy_gate_summary_blocks_unsafe_startup(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({"TRADING_ENABLED": "true"})
    safety = assess_startup_safety(config)
    _seed_observable_context(session, now=now)

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_LIVE_GUARD_BLOCKED
    assert decision.primary_reason == "startup_safety_not_observer_safe"
    assert decision.measurements["gate_results"]["safety"]["status"] == "block"
    assert (
        decision.measurements["gate_results"]["safety"]["reason"]
        == "startup_safety_not_observer_safe"
    )
    assert decision.measurements["gate_results"]["market"]["status"] == "not_evaluated"
    assert (
        decision.measurements["gate_results"]["boundary"]["status"]
        == "not_evaluated"
    )


def test_strategy_gate_summary_marks_boundary_unevaluated_without_market(
    session,
) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({})
    safety = assess_startup_safety(config)

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_NO_ACTIVE_MARKET
    assert decision.measurements["gate_results"]["market"]["status"] == "block"
    assert (
        decision.measurements["gate_results"]["boundary"]["status"]
        == "not_evaluated"
    )


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


def test_strategy_dry_run_blocks_duplicate_entry_in_same_bucket(tmp_path) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    current_time = {"value": now}
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_strategy_bucket.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "APP_MODE": "DRY_RUN",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STRATEGY_DRY_RUN_ENABLED": "true",
            "STRATEGY_DRY_RUN_MAX_OPEN_POSITIONS": "2",
            "STRATEGY_DRY_RUN_ONE_ENTRY_PER_MARKET": "false",
            "STRATEGY_DRY_RUN_MIN_SECONDS_BETWEEN_DECISIONS": "10",
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
            now=lambda: current_time["value"],
        )
        first_decision = observer.evaluate_once()
        current_time["value"] = now + timedelta(seconds=1)
        second_decision = observer.evaluate_once()

        with session_factory() as session:
            dry_run_repository = StrategyDryRunRepository(session)
            open_positions = dry_run_repository.list_open_positions(
                strategy_id=config.strategy_id
            )
            events = dry_run_repository.list_recent_events(limit=10)

        assert first_decision is not None
        assert first_decision.decision_state == STATE_ENTER_DRY_RUN
        assert second_decision is not None
        assert second_decision.decision_state == STATE_RISK_BLOCKED
        assert second_decision.primary_reason == "dry_run_entry_bucket_already_entered"
        assert len(open_positions) == 1
        assert len(events) == 1
        assert events[0].decision_id == first_decision.decision_id
    finally:
        engine.dispose()


def test_strategy_dry_run_prioritizes_older_expired_position_before_new_entry(
    session,
) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config(
        {
            "APP_MODE": "DRY_RUN",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STRATEGY_DRY_RUN_ENABLED": "true",
            "STRATEGY_DRY_RUN_MAX_OPEN_POSITIONS": "3",
            "STRATEGY_DRY_RUN_ONE_ENTRY_PER_MARKET": "false",
        }
    )
    safety = assess_startup_safety(config)
    _seed_observable_context(session, now=now)
    MarketsRepository(session).upsert_market(
        MarketInput(
            market_ticker="KXBTC15M-OLD",
            event_ticker="KXBTC15M-26JUL051145",
            series_ticker="KXBTC15M",
            open_time=now - timedelta(minutes=25),
            close_time=now - timedelta(minutes=10),
            functional_strike=Decimal("62000"),
            resolver_decision_reason="market_interval_contains_now",
        )
    )
    repository = StrategyDryRunRepository(session)
    repository.insert_position_if_absent(
        StrategyDryRunPositionInput(
            position_id="dryrun-expired-position",
            strategy_id=config.strategy_id,
            market_ticker="KXBTC15M-OLD",
            decision_id="strategy-old-enter",
            side_candidate="YES",
            economic_side="YES",
            opened_at=now - timedelta(minutes=20),
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
    repository.insert_position_if_absent(
        StrategyDryRunPositionInput(
            position_id="dryrun-active-position",
            strategy_id=config.strategy_id,
            market_ticker="KXBTC15M-ACTIVE",
            decision_id="strategy-active-enter",
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

    assert decision.decision_state == STATE_FORCE_EXIT
    assert decision.primary_reason == "dry_run_position_market_closed_or_expired"
    assert decision.measurements["managed_position_id"] == "dryrun-expired-position"


def test_strategy_dry_run_force_exits_only_stale_position_after_roll(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config(
        {
            "APP_MODE": "DRY_RUN",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STRATEGY_DRY_RUN_ENABLED": "true",
            "STRATEGY_DRY_RUN_MAX_OPEN_POSITIONS": "3",
            "STRATEGY_DRY_RUN_ONE_ENTRY_PER_MARKET": "false",
        }
    )
    safety = assess_startup_safety(config)
    _seed_observable_context(session, now=now)
    MarketsRepository(session).upsert_market(
        MarketInput(
            market_ticker="KXBTC15M-OLD",
            event_ticker="KXBTC15M-26JUL051145",
            series_ticker="KXBTC15M",
            open_time=now - timedelta(minutes=25),
            close_time=now - timedelta(minutes=10),
            functional_strike=Decimal("62000"),
            resolver_decision_reason="market_interval_contains_now",
        )
    )
    StrategyDryRunRepository(session).insert_position_if_absent(
        StrategyDryRunPositionInput(
            position_id="dryrun-only-stale-position",
            strategy_id=config.strategy_id,
            market_ticker="KXBTC15M-OLD",
            decision_id="strategy-old-enter",
            side_candidate="YES",
            economic_side="YES",
            opened_at=now - timedelta(minutes=20),
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

    assert decision.decision_state == STATE_FORCE_EXIT
    assert decision.primary_reason == "dry_run_position_market_closed_or_expired"
    assert decision.measurements["managed_position_id"] == (
        "dryrun-only-stale-position"
    )


def test_strategy_dry_run_prioritizes_older_profit_target_before_new_entry(
    session,
) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config(
        {
            "APP_MODE": "DRY_RUN",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STRATEGY_DRY_RUN_ENABLED": "true",
            "STRATEGY_DRY_RUN_MAX_OPEN_POSITIONS": "3",
            "STRATEGY_DRY_RUN_ONE_ENTRY_PER_MARKET": "false",
        }
    )
    safety = assess_startup_safety(config)
    _seed_observable_context(
        session,
        now=now,
        yes_bid=Decimal("0.74"),
        yes_ask=Decimal("0.76"),
    )
    repository = StrategyDryRunRepository(session)
    repository.insert_position_if_absent(
        StrategyDryRunPositionInput(
            position_id="dryrun-older-profit-position",
            strategy_id=config.strategy_id,
            market_ticker="KXBTC15M-ACTIVE",
            decision_id="strategy-older-enter",
            side_candidate="YES",
            economic_side="YES",
            opened_at=now - timedelta(minutes=3),
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
    repository.insert_position_if_absent(
        StrategyDryRunPositionInput(
            position_id="dryrun-newer-open-position",
            strategy_id=config.strategy_id,
            market_ticker="KXBTC15M-ACTIVE",
            decision_id="strategy-newer-enter",
            side_candidate="YES",
            economic_side="YES",
            opened_at=now - timedelta(seconds=30),
            open_price=Decimal("0.72"),
            contract_count=1,
            boundary=Decimal("62000"),
            brti_at_entry=Decimal("62100"),
            distance_bps_at_entry=Decimal("16.10305958"),
            entry_reason="dry_run_entry_signal",
            status="OPEN",
            measurements={"desired_side_ask": "0.71"},
        )
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_EXIT_SIGNAL
    assert decision.primary_reason == "dry_run_profit_target_reached"
    assert decision.measurements["managed_position_id"] == (
        "dryrun-older-profit-position"
    )


def test_strategy_dry_run_management_uses_exit_bid_depth(session) -> None:
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
        yes_bid_count=Decimal("3"),
        yes_ask_count=Decimal("0"),
    )
    StrategyDryRunRepository(session).insert_position_if_absent(
        StrategyDryRunPositionInput(
            position_id="dryrun-managed-position",
            strategy_id=config.strategy_id,
            market_ticker="KXBTC15M-ACTIVE",
            decision_id="strategy-managed-enter",
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

    assert decision.decision_state == STATE_MANAGE_POSITION
    assert decision.primary_reason == "dry_run_position_open"
    assert decision.blockers == []
    assert decision.measurements["desired_top_book_size"] == "3"
    assert decision.measurements["managed_position_id"] == "dryrun-managed-position"


def test_strategy_dry_run_force_exit_prices_stale_reference_with_fresh_book(
    tmp_path,
) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_strategy_force_exit.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "APP_MODE": "DRY_RUN",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STRATEGY_DRY_RUN_ENABLED": "true",
            "STRATEGY_DRY_RUN_MAX_OPEN_POSITIONS": "2",
            "STRATEGY_DRY_RUN_ONE_ENTRY_PER_MARKET": "false",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    position_id = "dryrun-managed-stale-reference"
    try:
        with session_factory() as session:
            _seed_observable_context(session, now=now)
            ReferenceTicksRepository(session).insert_tick(
                ReferenceTickInput(
                    source="kalshi_cfbenchmarks_brti",
                    received_at=now,
                    source_ts=now - timedelta(seconds=50),
                    raw_value="62100",
                    parsed_value=Decimal("62100"),
                    source_age_ms=50_000,
                    parse_status="valid",
                )
            )
            StrategyDryRunRepository(session).insert_position_if_absent(
                StrategyDryRunPositionInput(
                    position_id=position_id,
                    strategy_id=config.strategy_id,
                    market_ticker="KXBTC15M-ACTIVE",
                    decision_id="strategy-managed-enter",
                    side_candidate="YES",
                    economic_side="YES",
                    opened_at=now - timedelta(seconds=30),
                    open_price=Decimal("0.50"),
                    contract_count=1,
                    boundary=Decimal("62000"),
                    brti_at_entry=Decimal("62100"),
                    distance_bps_at_entry=Decimal("16.10305958"),
                    entry_reason="dry_run_entry_signal",
                    status="OPEN",
                    measurements={"desired_side_ask": "0.62"},
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
        decision = observer.evaluate_once()

        with session_factory() as session:
            dry_run_repository = StrategyDryRunRepository(session)
            position = dry_run_repository.get_position_by_id(position_id)
            events = dry_run_repository.list_recent_events(
                limit=10,
                strategy_id=config.strategy_id,
            )

        assert decision is not None
        assert decision.decision_state == STATE_FORCE_EXIT
        assert decision.primary_reason == "dry_run_position_reference_stale"
        assert decision.measurements["desired_side_bid"] == "0.6"
        assert position is not None
        assert position.status == "FORCE_CLOSED"
        assert position.close_price == Decimal("0.60")
        assert position.realized_pnl_cents == Decimal("10")
        assert len(events) == 1
        assert events[0].event_type == STATE_FORCE_EXIT
        assert events[0].price == Decimal("0.60")
    finally:
        engine.dispose()


def test_strategy_dry_run_force_exits_stale_book_under_multi_position_capacity(
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
    _seed_observable_context(
        session,
        now=now,
        latest_orderbook_received_at=now - timedelta(seconds=5),
    )
    StrategyDryRunRepository(session).insert_position_if_absent(
        StrategyDryRunPositionInput(
            position_id="dryrun-stale-book-position",
            strategy_id=config.strategy_id,
            market_ticker="KXBTC15M-ACTIVE",
            decision_id="strategy-stale-book-enter",
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

    assert decision.decision_state == STATE_FORCE_EXIT
    assert decision.primary_reason == "dry_run_position_orderbook_stale"
    assert decision.measurements["managed_position_id"] == (
        "dryrun-stale-book-position"
    )


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
    assert decision.measurements["gate_results"]["dry_run_risk"]["status"] == "block"
    assert (
        decision.measurements["gate_results"]["dry_run_risk"]["reason"]
        == "dry_run_disabled_observe_only"
    )


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
    assert decision.primary_reason == "brti_reference_first_tick_timeout"
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
    yes_bid_count: Decimal = Decimal("3"),
    yes_ask_count: Decimal = Decimal("3"),
    no_bid_count: Decimal = Decimal("3"),
    no_ask_count: Decimal = Decimal("3"),
    latest_orderbook_received_at: datetime | None = None,
    reference_received_lag_ms: int = 500,
) -> None:
    _seed_market(session, now=now)
    reference_repository = ReferenceTicksRepository(session)
    for seconds_ago in range(180, -1, -5):
        value = Decimal("62020") + Decimal(180 - seconds_ago) * Decimal("0.5")
        received_at = now - timedelta(
            seconds=seconds_ago,
            milliseconds=reference_received_lag_ms,
        )
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
            yes_bid_count=yes_bid_count,
            yes_ask_count=yes_ask_count,
            no_bid_count=no_bid_count,
            no_ask_count=no_ask_count,
            book_status="ok",
        )
    )
    orderbook_repository.insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker="KXBTC15M-ACTIVE",
            received_at=latest_orderbook_received_at
            or now - timedelta(milliseconds=500),
            sequence_number=123,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            no_bid=Decimal("0.38"),
            no_ask=Decimal("0.40"),
            yes_spread=yes_spread,
            no_spread=Decimal("0.02"),
            yes_bid_count=yes_bid_count,
            yes_ask_count=yes_ask_count,
            no_bid_count=no_bid_count,
            no_ask_count=no_ask_count,
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


def _record_feed_heartbeat(
    session,
    *,
    now: datetime,
    orderbook_stream_last_message_at: datetime | None = None,
    orderbook_active_market_ticker: str = "KXBTC15M-ACTIVE",
    orderbook_warnings: list[str] | None = None,
    brti_last_valid_message_at: datetime | None = None,
    brti_last_valid_message_source_ts: datetime | None = None,
    brti_last_valid_message_value: str | None = "62110",
    brti_carried_forward: bool = False,
) -> None:
    stream_at = orderbook_stream_last_message_at or now - timedelta(seconds=1)
    brti_message_at = brti_last_valid_message_at or now - timedelta(seconds=1)
    WorkerHeartbeatRepository(session).record_heartbeat(
        WorkerHeartbeatInput(
            service_name="ape-worker",
            started_at=now - timedelta(minutes=1),
            heartbeat_at=now - timedelta(milliseconds=250),
            app_mode="OBSERVER",
            is_safe=True,
            metadata={
                "mode": "kalshi_ws",
                "ws": {
                    "enabled": True,
                    "connection_state": "subscribed",
                    "active_market_ticker": orderbook_active_market_ticker,
                    "last_message_at": stream_at.isoformat(),
                    "last_ticker_at": stream_at.isoformat(),
                    "last_trade_at": stream_at.isoformat(),
                    "last_orderbook_at": (now - timedelta(seconds=5)).isoformat(),
                    "orderbook_initialized": True,
                    "orderbook_sequence_number": 123,
                    "warnings": orderbook_warnings or [],
                    "blockers": [],
                },
                "reference": {
                    "brti": {
                        "enabled": True,
                        "connection_state": "subscribed",
                        "last_message_at": brti_message_at.isoformat(),
                        "last_valid_message_at": brti_message_at.isoformat(),
                        "last_valid_message_source_ts": (
                            brti_last_valid_message_source_ts.isoformat()
                            if brti_last_valid_message_source_ts is not None
                            else None
                        ),
                        "last_valid_message_value": brti_last_valid_message_value,
                        "valid_message_age_ms": int(
                            (now - brti_message_at).total_seconds() * 1000
                        ),
                        "valid_message_carried_forward": brti_carried_forward,
                        "reference_stream_live": (
                            (now - brti_message_at).total_seconds() <= 3
                        ),
                        "warnings": [],
                        "blockers": [],
                    }
                },
            },
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
