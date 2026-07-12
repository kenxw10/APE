from __future__ import annotations

from dataclasses import replace
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
    StrategyDecisionInput,
    StrategyDryRunEventInput,
    StrategyDryRunPositionInput,
    StrategyTradeIntentInput,
    WorkerHeartbeatInput,
)
from ape.repositories.markets import MarketsRepository
from ape.repositories.orderbook import OrderbookRepository
from ape.repositories.public_trades import PublicTradesRepository
from ape.repositories.reference_ticks import ReferenceTicksRepository
from ape.repositories.strategy_decisions import StrategyDecisionsRepository
from ape.repositories.strategy_dry_run import StrategyDryRunRepository
from ape.repositories.strategy_v2 import StrategyV2Repository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.research.pin import PinnedCandidate
from ape.safety import assess_startup_safety
from ape.strategy import observer as observer_module
from ape.strategy.momentum_v2 import (
    STATE_V2_ENTRY_SIGNAL,
    STATE_V2_HARD_GATE_BLOCKED,
    V2_PARAMETERS,
    V2_STRATEGY_ID,
)
from ape.strategy.observer import (
    CHALLENGER_STRATEGY_ID,
    CONTROL_STRATEGY_ID,
    STATE_CHOP_FILTER_BLOCKED,
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
    build_strategy_dry_run_status,
    build_strategy_variants_comparison,
    evaluate_strategy_observer,
    evaluate_strategy_variants,
    strategy_variant_configs,
)
from ape.worker.services import (
    WORKER_SERVICE_AGGREGATE,
    WORKER_SERVICE_MARKET_WS,
    WORKER_SERVICE_REFERENCE_BRTI,
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


def test_v2_boundary_cross_decision_does_not_create_an_entry_intent(session) -> None:
    now = datetime(2026, 7, 11, 12, 10, tzinfo=UTC)

    observer_module._apply_v2_hypothetical_lifecycle(
        config=load_config({}),
        session=session,
        decision=StrategyDecisionInput(
            decision_id="v2-boundary-cross-research-only",
            evaluated_at=now,
            decision_state=STATE_V2_HARD_GATE_BLOCKED,
            primary_reason="v2_candidate_mode_not_enabled",
            app_mode="DRY_RUN",
            strategy_id=V2_STRATEGY_ID,
            market_ticker="KXBTC15M-BOUNDARY",
            candidate_side="YES",
            measurements={
                "candidate_mode": "BOUNDARY_CROSS_HOLD",
                "intended_entry_price": "0.62",
            },
        ),
    )

    assert (
        StrategyV2Repository(session).list_recent_intents(
            strategy_id=V2_STRATEGY_ID,
            limit=10,
            action="ENTRY",
        )
        == []
    )


def test_strategy_blocks_on_market_protocol_readiness_failures() -> None:
    config = load_config({})
    base_metadata = {
        "connection_state": "subscribed",
        "active_market_ticker": "KXBTC15M-TEST",
        "orderbook_initialized": True,
        "market_recovery_attempt_in_progress": False,
        "subscription_reconciled": True,
        "orderbook_sid_confirmed": True,
        "in_flight_snapshot_request": False,
        "db_writer_queue_depth": 0,
        "db_writer_critical_queue_depth": 0,
        "db_writer_critical_queue_oldest_age_ms": 0,
        "db_writer_diagnostic_queue_depth": 0,
        "protocol_event_recent_error_count": 0,
    }

    def stale_reason(metadata):
        metadata_warnings = metadata.get("warnings", [])
        metadata_blockers = metadata.get("blockers", [])
        return observer_module._strategy_orderbook_stale_reason(
            config=config,
            orderbook=object(),
            orderbook_age_ms=2500,
            orderbook_worker_metadata=metadata,
            orderbook_stream_age_ms=100,
            orderbook_stream_connection_state="subscribed",
            orderbook_stream_active_market_ticker="KXBTC15M-TEST",
            orderbook_stream_warnings=(
                metadata_warnings if isinstance(metadata_warnings, list) else []
            ),
            orderbook_stream_blockers=(
                metadata_blockers if isinstance(metadata_blockers, list) else []
            ),
            market_feed_transport_state="healthy",
            market_feed_subscription_state="subscribed",
            market_feed_snapshot_state="initialized",
            market_feed_active_ticker_state="match",
            market_feed_sequence_state="clean",
            market_ticker="KXBTC15M-TEST",
        )

    assert stale_reason({**base_metadata, "subscription_reconciled": False}) == (
        "kalshi_orderbook_subscription_unreconciled"
    )
    assert stale_reason({**base_metadata, "orderbook_sid_confirmed": False}) == (
        "kalshi_orderbook_orderbook_sid_unconfirmed"
    )
    assert (
        stale_reason(
            {
                **base_metadata,
                "in_flight_snapshot_request": True,
                "snapshot_request_age_ms": 11_000,
            }
        )
        == "kalshi_orderbook_snapshot_resync_timeout"
    )
    assert stale_reason({**base_metadata, "db_writer_critical_queue_depth": 1500}) == (
        "kalshi_orderbook_db_writer_backpressure"
    )
    assert stale_reason({**base_metadata, "db_writer_critical_queue_oldest_age_ms": 10_000}) == (
        "kalshi_orderbook_db_writer_backpressure"
    )
    assert stale_reason({**base_metadata, "blockers": ["market_critical_persistence_failed"]}) == (
        "kalshi_orderbook_db_writer_backpressure"
    )
    assert stale_reason(
        {**base_metadata, "blockers": ["market_critical_persistence_backpressure"]}
    ) == ("kalshi_orderbook_db_writer_backpressure")
    assert (
        stale_reason(
            {
                **base_metadata,
                "db_writer_diagnostic_queue_depth": 10_000,
                "orderbook_persistence_pending": True,
            }
        )
        is None
    )
    assert stale_reason({**base_metadata, "protocol_event_recent_error_count": 1}) == (
        "kalshi_orderbook_protocol_errors"
    )


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
            open_positions = dry_run_repository.list_open_positions(strategy_id=config.strategy_id)
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


def test_strategy_challenger_runs_on_same_timestamp_with_separate_ledgers(tmp_path) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_strategy_challenger.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "APP_MODE": "DRY_RUN",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STRATEGY_DRY_RUN_ENABLED": "true",
            "STRATEGY_CHALLENGER_ENABLED": "true",
        }
    )
    safety = assess_startup_safety(config)
    assert [variant.strategy_id for variant in strategy_variant_configs(config, safety)] == [
        CONTROL_STRATEGY_ID,
        CHALLENGER_STRATEGY_ID,
    ]
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            _seed_observable_context(session, now=now)
            OrderbookRepository(session).insert_snapshot(
                OrderbookSnapshotInput(
                    market_ticker="KXBTC15M-ACTIVE",
                    received_at=now - timedelta(seconds=30),
                    sequence_number=124,
                    yes_bid=Decimal("0.55"),
                    yes_ask=Decimal("0.57"),
                    no_bid=Decimal("0.43"),
                    no_ask=Decimal("0.45"),
                    yes_spread=Decimal("0.02"),
                    no_spread=Decimal("0.02"),
                    yes_bid_count=Decimal("3"),
                    yes_ask_count=Decimal("3"),
                    no_bid_count=Decimal("3"),
                    no_ask_count=Decimal("3"),
                    book_status="ok",
                )
            )
            ReferenceTicksRepository(session).insert_tick(
                ReferenceTickInput(
                    source="kalshi_cfbenchmarks_brti",
                    received_at=now,
                    source_ts=now,
                    raw_value="62130",
                    parsed_value=Decimal("62130"),
                    source_age_ms=0,
                    parse_status="valid",
                )
            )
            session.commit()

        observer = StrategyObserver(
            config=config,
            safety=safety,
            session_factory=session_factory,
            started_at=now - timedelta(minutes=1),
            now=lambda: now,
        )
        control = observer.evaluate_once()

        with session_factory() as session:
            decisions = StrategyDecisionsRepository(session).list_recent_decisions(limit=10)
            dry_run = StrategyDryRunRepository(session)
            control_open_positions = dry_run.list_open_positions(strategy_id=CONTROL_STRATEGY_ID)
            challenger_open_positions = dry_run.list_open_positions(
                strategy_id=CHALLENGER_STRATEGY_ID
            )
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")
            comparison = build_strategy_variants_comparison(
                config,
                window_seconds=60,
                now=now,
            )

        assert control is not None
        assert control.strategy_id == CONTROL_STRATEGY_ID
        assert {decision.strategy_id for decision in decisions} == {
            CONTROL_STRATEGY_ID,
            CHALLENGER_STRATEGY_ID,
        }
        assert {decision.evaluated_at for decision in decisions} == {decisions[0].evaluated_at}
        assert {decision.measurements["brti_received_at"] for decision in decisions} == {
            decisions[0].measurements["brti_received_at"]
        }
        assert len(control_open_positions) == 1
        assert len(challenger_open_positions) == 1
        assert heartbeat is not None
        assert set(heartbeat.metadata_["strategy"]["variants"]) == {
            CONTROL_STRATEGY_ID,
            CHALLENGER_STRATEGY_ID,
        }
        assert comparison.challenger_enabled is True
        assert comparison.variants[CONTROL_STRATEGY_ID]["total_decisions"] == 1
        assert comparison.variants[CHALLENGER_STRATEGY_ID]["current_open_positions"] == 1
    finally:
        engine.dispose()


def test_strategy_challenger_is_disabled_without_the_opt_in_flag() -> None:
    config = load_config(
        {
            "APP_MODE": "DRY_RUN",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STRATEGY_DRY_RUN_ENABLED": "true",
        }
    )
    variants = strategy_variant_configs(config, assess_startup_safety(config))

    assert [variant.strategy_id for variant in variants] == [CONTROL_STRATEGY_ID]


def test_strategy_observer_persists_one_feature_snapshot_and_config_attribution(
    tmp_path,
) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_v2_feature_snapshot.sqlite'}"
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
        first = observer.evaluate_once()
        second = observer.evaluate_once()

        assert first is not None
        assert second is not None
        with session_factory() as session:
            snapshots = StrategyV2Repository(session).list_recent_feature_snapshots(limit=10)
            stored = StrategyDecisionsRepository(session).get_decision_by_id(first.decision_id)

        assert len(snapshots) == 1
        assert stored is not None
        assert stored.feature_snapshot_id == snapshots[0].feature_snapshot_id
        assert stored.strategy_config_version_id is not None
        assert stored.code_commit_sha is not None
    finally:
        engine.dispose()


def test_v2_pending_intent_resolves_without_current_candidate_side(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    fill_time = now - timedelta(milliseconds=500)
    market_ticker = "KXBTC15M-ACTIVE"
    intents = StrategyV2Repository(session)
    StrategyDecisionsRepository(session).insert_decision(
        StrategyDecisionInput(
            decision_id="v2-entry-decision",
            evaluated_at=now - timedelta(seconds=2),
            decision_state=STATE_V2_ENTRY_SIGNAL,
            primary_reason="v2_entry_signal",
            app_mode="DRY_RUN",
            strategy_id=V2_STRATEGY_ID,
            market_ticker=market_ticker,
            candidate_side="YES",
            boundary=Decimal("62000"),
            brti_value=Decimal("62010"),
            distance_bps=Decimal("1.61"),
            code_commit_sha="source-commit",
        )
    )
    intents.insert_intent_if_absent(
        StrategyTradeIntentInput(
            intent_id="v2-pending-no-current-candidate",
            strategy_id=V2_STRATEGY_ID,
            decision_id="v2-entry-decision",
            market_ticker=market_ticker,
            side_candidate="YES",
            action="ENTRY",
            created_at=now - timedelta(seconds=2),
            effective_after=now - timedelta(seconds=1),
            expires_at=now + timedelta(seconds=1),
            intended_limit_price=Decimal("0.62"),
            quantity=Decimal("1"),
        )
    )
    OrderbookRepository(session).insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker=market_ticker,
            received_at=fill_time,
            sequence_number=1,
            yes_bid=Decimal("0.60"),
            yes_ask=Decimal("0.61"),
            yes_ask_count=Decimal("1"),
            no_bid=Decimal("0.39"),
            no_ask=Decimal("0.40"),
            no_ask_count=Decimal("1"),
            book_status="ok",
        )
    )

    observer_module._apply_v2_hypothetical_lifecycle(
        config=load_config({}),
        session=session,
        decision=StrategyDecisionInput(
            decision_id="v2-no-current-candidate-decision",
            evaluated_at=now,
            decision_state="V2_HARD_GATE_BLOCKED",
            primary_reason="reference_stale",
            app_mode="DRY_RUN",
            strategy_id=V2_STRATEGY_ID,
            market_ticker=market_ticker,
            candidate_side=None,
            boundary=Decimal("63000"),
            brti_value=Decimal("63010"),
            distance_bps=Decimal("1.59"),
            code_commit_sha="current-commit",
        ),
    )

    intent = intents.get_intent("v2-pending-no-current-candidate")
    assert intent is not None
    assert intent.status == "FILLED"
    assert intent.resolved_at == now.replace(tzinfo=None)
    positions = StrategyDryRunRepository(session)
    position = positions.get_open_position_by_market(
        strategy_id=V2_STRATEGY_ID,
        market_ticker=market_ticker,
    )
    event = positions.get_latest_event(strategy_id=V2_STRATEGY_ID)
    assert position is not None
    assert intent.position_id == position.position_id
    assert position.opened_at == fill_time.replace(tzinfo=None)
    assert position.boundary == Decimal("62000")
    assert position.brti_at_entry == Decimal("62010")
    assert position.distance_bps_at_entry == Decimal("1.61")
    assert position.code_commit_sha == "source-commit"
    assert event is not None
    assert event.occurred_at == fill_time.replace(tzinfo=None)


def test_v2_pending_entry_fill_persists_candidate_tier_hold_windows(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    fill_time = now - timedelta(milliseconds=500)
    market_ticker = "KXBTC15M-CANDIDATE-HOLDS"
    strategy_id = "btc15_momentum_v2_candidate_hold_windows"
    candidate_parameters = {
        "tiers": {
            "normal": {"time_stop": 71, "max_hold": 93},
        }
    }
    StrategyDecisionsRepository(session).insert_decision(
        StrategyDecisionInput(
            decision_id="v2-candidate-entry-decision",
            evaluated_at=now - timedelta(seconds=2),
            decision_state=STATE_V2_ENTRY_SIGNAL,
            primary_reason="v2_entry_signal",
            app_mode="DRY_RUN",
            strategy_id=strategy_id,
            market_ticker=market_ticker,
            candidate_side="YES",
            boundary=Decimal("62000"),
            brti_value=Decimal("62010"),
            measurements={
                "timing_tier": "normal",
                "v2_parameters": candidate_parameters,
            },
        )
    )
    intents = StrategyV2Repository(session)
    intents.insert_intent_if_absent(
        StrategyTradeIntentInput(
            intent_id="v2-candidate-hold-entry",
            strategy_id=strategy_id,
            decision_id="v2-candidate-entry-decision",
            market_ticker=market_ticker,
            side_candidate="YES",
            action="ENTRY",
            created_at=now - timedelta(seconds=2),
            effective_after=now - timedelta(seconds=1),
            expires_at=now + timedelta(seconds=1),
            intended_limit_price=Decimal("0.62"),
            quantity=Decimal("1"),
        )
    )
    OrderbookRepository(session).insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker=market_ticker,
            received_at=fill_time,
            yes_bid=Decimal("0.60"),
            yes_ask=Decimal("0.61"),
            yes_ask_count=Decimal("1"),
            no_bid=Decimal("0.39"),
            no_ask=Decimal("0.40"),
            no_ask_count=Decimal("1"),
            book_status="ok",
        )
    )

    observer_module._apply_v2_hypothetical_lifecycle(
        config=load_config({}),
        session=session,
        decision=StrategyDecisionInput(
            decision_id="v2-current-candidate-decision",
            evaluated_at=now,
            decision_state="V2_HARD_GATE_BLOCKED",
            primary_reason="reference_stale",
            app_mode="DRY_RUN",
            strategy_id=strategy_id,
            market_ticker=market_ticker,
            candidate_side=None,
        ),
    )

    position = StrategyDryRunRepository(session).get_open_position_by_market(
        strategy_id=strategy_id,
        market_ticker=market_ticker,
    )
    assert position is not None
    assert position.entry_time_stop_seconds == 71
    assert position.entry_max_hold_seconds == 93


def test_variants_receive_one_shared_context_per_iteration(session, monkeypatch) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config(
        {
            "APP_MODE": "DRY_RUN",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STRATEGY_DRY_RUN_ENABLED": "true",
            "STRATEGY_CHALLENGER_ENABLED": "true",
            "STRATEGY_V2_ENABLED": "true",
            "STRATEGY_BRTI_LOOKBACK_LONG_SECONDS": "240",
            "STRATEGY_TRADE_CONFIRMATION_LOOKBACK_SECONDS": "45",
            "STRATEGY_CONTRACT_LOOKBACK_SECONDS": "120",
        }
    )
    _seed_observable_context(session, now=now)
    original_loader = observer_module.load_strategy_evaluation_context
    load_count = 0
    contexts = []
    reference_lookbacks = []
    trade_lookbacks = []
    orderbook_lookbacks = []

    def load_once(**kwargs):
        nonlocal load_count
        load_count += 1
        reference_lookbacks.append(kwargs["reference_lookback_seconds"])
        trade_lookbacks.append(kwargs["trade_lookback_seconds"])
        orderbook_lookbacks.append(kwargs["orderbook_lookback_seconds"])
        return original_loader(**kwargs)

    def capture_context(*, config, safety, session, now, shared_context=None):
        del safety, session
        contexts.append(shared_context)
        return StrategyDecisionInput(
            decision_id=f"shared-{config.strategy_id}",
            evaluated_at=now,
            decision_state=STATE_OBSERVE_ONLY_MARKET,
            primary_reason="observer_decision_ledger_only",
            app_mode="DRY_RUN",
            strategy_id=config.strategy_id,
            blockers=[],
            warnings=[],
        )

    monkeypatch.setattr(observer_module, "load_strategy_evaluation_context", load_once)
    monkeypatch.setattr(observer_module, "evaluate_strategy_observer", capture_context)

    results = evaluate_strategy_variants(
        config=config,
        safety=assess_startup_safety(config),
        session=session,
        now=now,
    )

    assert load_count == 1
    assert reference_lookbacks == [240]
    assert trade_lookbacks == [45]
    assert orderbook_lookbacks == [120]
    assert len(contexts) == 2
    assert contexts[0] is contexts[1]
    assert [variant.strategy_id for variant, _ in results] == [
        CONTROL_STRATEGY_ID,
        CHALLENGER_STRATEGY_ID,
        V2_STRATEGY_ID,
    ]


def test_pinned_candidate_decision_uses_candidate_config_code_version(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config(
        {
            "APP_MODE": "DRY_RUN",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STRATEGY_DRY_RUN_ENABLED": "true",
            "STRATEGY_V2_ENABLED": "true",
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )
    _seed_observable_context(session, now=now)

    results = evaluate_strategy_variants(
        config=config,
        safety=assess_startup_safety(config),
        session=session,
        now=now,
        pinned_candidate=PinnedCandidate(
            "btc15_momentum_v2_candidate_fixture",
            "candidate-config",
            V2_PARAMETERS,
            "candidate-commit",
        ),
        pin_resolved=True,
    )

    candidate_decision = next(
        decision
        for variant, decision in results
        if variant.strategy_id == "btc15_momentum_v2_candidate_fixture"
    )
    assert candidate_decision.strategy_config_version_id == "candidate-config"
    assert candidate_decision.code_commit_sha == "candidate-commit"


def test_shared_context_trims_orderbook_history_to_variant_lookback(session, monkeypatch) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({"STRATEGY_TRADE_CONFIRMATION_LOOKBACK_SECONDS": "5"})
    safety = assess_startup_safety(config)
    _seed_observable_context(session, now=now)
    old_snapshot = OrderbookRepository(session).insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker="KXBTC15M-ACTIVE",
            received_at=now - timedelta(seconds=90),
            sequence_number=121,
            yes_bid=Decimal("0.50"),
            yes_ask=Decimal("0.52"),
            no_bid=Decimal("0.48"),
            no_ask=Decimal("0.50"),
            yes_bid_count=Decimal("3"),
            yes_ask_count=Decimal("3"),
            no_bid_count=Decimal("3"),
            no_ask_count=Decimal("3"),
            book_status="ok",
        )
    )
    old_trade = PublicTradesRepository(session).insert_trade(
        PublicTradeInput(
            market_ticker="KXBTC15M-ACTIVE",
            received_at=now - timedelta(seconds=20),
            executed_at=now - timedelta(seconds=20),
            price=Decimal("0.61"),
            trade_count=Decimal("1"),
            side_inferred="YES",
        )
    )
    old_reference_tick = ReferenceTicksRepository(session).insert_tick(
        ReferenceTickInput(
            source="kalshi_cfbenchmarks_brti",
            received_at=now - timedelta(seconds=90),
            source_ts=now - timedelta(seconds=90),
            raw_value="62010",
            parsed_value=Decimal("62010"),
            parse_status="valid",
        )
    )
    shared_context = observer_module.load_strategy_evaluation_context(
        config=config,
        session=session,
        evaluated_at=now,
        reference_lookback_seconds=config.strategy_brti_lookback_long_seconds,
    )
    captured: dict[str, object] = {}
    variant_config = replace(config, strategy_brti_lookback_long_seconds=60)
    original_metrics = observer_module._contract_confirmation_metrics
    original_trade_metrics = observer_module._trade_confirmation_metrics
    original_impulse_metrics = observer_module._brti_impulse_metrics

    def capture_metrics(**kwargs):
        captured["orderbook_history"] = kwargs["orderbook_history"]
        return original_metrics(**kwargs)

    def capture_trade_metrics(**kwargs):
        captured["recent_trades"] = kwargs["trades"]
        return original_trade_metrics(**kwargs)

    def capture_impulse_metrics(**kwargs):
        captured["reference_ticks"] = kwargs["ticks"]
        return original_impulse_metrics(**kwargs)

    monkeypatch.setattr(observer_module, "_contract_confirmation_metrics", capture_metrics)
    monkeypatch.setattr(observer_module, "_trade_confirmation_metrics", capture_trade_metrics)
    monkeypatch.setattr(observer_module, "_brti_impulse_metrics", capture_impulse_metrics)

    evaluate_strategy_observer(
        config=variant_config,
        safety=safety,
        session=session,
        now=now,
        shared_context=shared_context,
    )

    orderbook_history = captured["orderbook_history"]
    assert isinstance(orderbook_history, list)
    assert old_snapshot.id not in {snapshot.id for snapshot in orderbook_history}
    assert all(
        observer_module._as_utc(snapshot.received_at)
        >= now - timedelta(seconds=config.strategy_contract_lookback_seconds)
        for snapshot in orderbook_history
    )
    recent_trades = captured["recent_trades"]
    assert isinstance(recent_trades, list)
    assert old_trade.id not in {trade.id for trade in recent_trades}
    assert all(
        observer_module._as_utc(trade.received_at)
        >= now - timedelta(seconds=config.strategy_trade_confirmation_lookback_seconds)
        for trade in recent_trades
    )
    reference_ticks = captured["reference_ticks"]
    assert isinstance(reference_ticks, list)
    assert old_reference_tick.id not in {tick.id for tick in reference_ticks}
    assert all(
        observer_module._as_utc(tick.received_at)
        >= now - timedelta(seconds=variant_config.strategy_brti_lookback_long_seconds)
        for tick in reference_ticks
    )


def test_v2_causal_exit_fill_uses_the_first_in_window_book_and_persists_outcome(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    market_ticker = "KXBTC15M-V2-EXIT"
    positions = StrategyDryRunRepository(session)
    positions.insert_position_if_absent(
        StrategyDryRunPositionInput(
            position_id="v2-open-exit",
            strategy_id=V2_STRATEGY_ID,
            market_ticker=market_ticker,
            decision_id="v2-entry",
            side_candidate="YES",
            economic_side="YES",
            opened_at=now - timedelta(seconds=10),
            open_price=Decimal("0.63"),
            contract_count=1,
            entry_reason="v2_causal_hypothetical_fill",
            status="OPEN",
            lifecycle_version="momentum_v2_lifecycle_v2",
            entry_time_stop_seconds=30,
            entry_max_hold_seconds=60,
        )
    )
    OrderbookRepository(session).insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker=market_ticker,
            received_at=now,
            yes_bid=Decimal("0.74"),
            yes_ask=Decimal("0.75"),
            yes_bid_count=Decimal("1"),
            yes_ask_count=Decimal("1"),
            no_bid=Decimal("0.25"),
            no_ask=Decimal("0.26"),
            no_bid_count=Decimal("1"),
            no_ask_count=Decimal("1"),
            book_status="ok",
        )
    )
    decision = StrategyDecisionInput(
        decision_id="v2-profit-signal",
        evaluated_at=now,
        decision_state="V2_HARD_GATE_BLOCKED",
        primary_reason="v2_test",
        app_mode="DRY_RUN",
        strategy_id=V2_STRATEGY_ID,
        market_ticker=market_ticker,
        candidate_side="NO",
        boundary=Decimal("62000"),
        brti_value=Decimal("62010"),
        seconds_left=300,
        measurements={
            "features": {"return_5s": "0", "return_15s": "0"},
            "score": {"total": "80"},
            "edge": {"lower_bound_cents": "2"},
        },
    )
    observer_module._apply_v2_hypothetical_lifecycle(
        config=load_config({}), session=session, decision=decision
    )
    intents = StrategyV2Repository(session)
    exit_intent = intents.get_pending_exit_intent(position_id="v2-open-exit")
    assert exit_intent is not None
    assert exit_intent.action == "EXIT"
    assert exit_intent.status == "PENDING"
    marks_before_exit = intents.list_marks_for_position(position_id="v2-open-exit")
    assert [mark.executable_bid for mark in marks_before_exit] == [Decimal("0.74")]

    first_future_book = OrderbookRepository(session).insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker=market_ticker,
            received_at=now + timedelta(milliseconds=750),
            yes_bid=Decimal("0.74"),
            yes_ask=Decimal("0.75"),
            yes_bid_count=Decimal("1"),
            yes_ask_count=Decimal("1"),
            no_bid=Decimal("0.27"),
            no_ask=Decimal("0.28"),
            no_bid_count=Decimal("1"),
            no_ask_count=Decimal("1"),
            book_status="ok",
        )
    )
    second_future_book = OrderbookRepository(session).insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker=market_ticker,
            received_at=now + timedelta(seconds=1),
            yes_bid=Decimal("0.75"),
            yes_ask=Decimal("0.76"),
            yes_bid_count=Decimal("1"),
            yes_ask_count=Decimal("1"),
            no_bid=Decimal("0.24"),
            no_ask=Decimal("0.25"),
            no_bid_count=Decimal("1"),
            no_ask_count=Decimal("1"),
            book_status="ok",
        )
    )
    OrderbookRepository(session).insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker=market_ticker,
            received_at=now + timedelta(milliseconds=1500),
            yes_bid=Decimal("0.90"),
            yes_ask=Decimal("0.91"),
            yes_bid_count=Decimal("1"),
            yes_ask_count=Decimal("1"),
            no_bid=Decimal("0.09"),
            no_ask=Decimal("0.10"),
            no_bid_count=Decimal("1"),
            no_ask_count=Decimal("1"),
            book_status="ok",
        )
    )
    observer_module._apply_v2_hypothetical_lifecycle(
        config=load_config({}),
        session=session,
        decision=replace(decision, evaluated_at=now + timedelta(milliseconds=1500)),
    )

    position = positions.get_position_by_id("v2-open-exit")
    assert position is not None
    assert position.status == "CLOSED"
    assert position.close_price == Decimal("0.74")
    assert position.realized_pnl_cents == Decimal("11")
    resolved_exit_intent = next(
        intent
        for intent in intents.list_recent_intents(
            strategy_id=V2_STRATEGY_ID,
            limit=10,
        )
        if intent.intent_id == exit_intent.intent_id
    )
    assert resolved_exit_intent.fill_snapshot_id == first_future_book.id
    assert resolved_exit_intent.fill_snapshot_id != second_future_book.id
    assert resolved_exit_intent.fill_timestamp == first_future_book.received_at
    assert resolved_exit_intent.simulated_fill_price == Decimal("0.74")
    assert resolved_exit_intent.simulated_fill_size == Decimal("1")
    outcomes = intents.list_recent_outcomes(strategy_id=V2_STRATEGY_ID, limit=10)
    assert len(outcomes) == 1
    assert outcomes[0].exit_intent_id == exit_intent.intent_id
    assert outcomes[0].mfe_cents == Decimal("11")
    assert outcomes[0].mae_cents == Decimal("0")
    assert outcomes[0].time_to_mfe_ms == 10000


def test_v2_exit_price_failure_does_not_use_later_qualifying_book(session) -> None:
    now = datetime(2026, 7, 11, 12, 10, tzinfo=UTC)
    market_ticker = "KXBTC15M-EXIT-FIRST-PRICE"
    positions = StrategyDryRunRepository(session)
    positions.insert_position_if_absent(
        StrategyDryRunPositionInput(
            position_id="v2-first-price-open",
            strategy_id=V2_STRATEGY_ID,
            market_ticker=market_ticker,
            decision_id="v2-first-price-entry",
            side_candidate="YES",
            economic_side="YES",
            opened_at=now - timedelta(seconds=10),
            open_price=Decimal("0.70"),
            contract_count=1,
            entry_reason="v2_causal_hypothetical_fill",
            status="OPEN",
        )
    )
    intents = StrategyV2Repository(session)
    pending = intents.insert_intent_if_absent(
        StrategyTradeIntentInput(
            intent_id="v2-first-price-exit",
            strategy_id=V2_STRATEGY_ID,
            decision_id="v2-first-price-decision",
            position_id="v2-first-price-open",
            market_ticker=market_ticker,
            side_candidate="YES",
            action="EXIT",
            created_at=now - timedelta(seconds=4),
            effective_after=now - timedelta(seconds=3),
            expires_at=now - timedelta(seconds=1),
            intended_limit_price=Decimal("0.72"),
            quantity=Decimal("1"),
        )
    )
    orderbooks = OrderbookRepository(session)
    orderbooks.insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker=market_ticker,
            received_at=now - timedelta(seconds=2),
            yes_bid=Decimal("0.71"),
            yes_bid_count=Decimal("1"),
            yes_ask=Decimal("0.72"),
            yes_ask_count=Decimal("1"),
            book_status="ok",
        )
    )
    orderbooks.insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker=market_ticker,
            received_at=now - timedelta(milliseconds=1500),
            yes_bid=Decimal("0.75"),
            yes_bid_count=Decimal("1"),
            yes_ask=Decimal("0.76"),
            yes_ask_count=Decimal("1"),
            book_status="ok",
        )
    )

    result, _ = observer_module._resolve_v2_pending_exit(
        session=session,
        intents=intents,
        positions=positions,
        pending=pending,
        resolved_at=now,
        decision=None,
    )

    assert result == "V2_EXIT_NO_FILL"
    assert pending.status == "NO_FILL"
    assert positions.get_position_by_id("v2-first-price-open").status == "OPEN"
    assert intents.list_recent_outcomes(strategy_id=V2_STRATEGY_ID, limit=10) == []
    assert all(
        event.event_type != "V2_EXIT_FILLED"
        for event in positions.list_recent_events(strategy_id=V2_STRATEGY_ID, limit=10)
    )


def test_v2_exit_depth_failure_does_not_use_later_qualifying_book(session) -> None:
    now = datetime(2026, 7, 11, 12, 10, tzinfo=UTC)
    market_ticker = "KXBTC15M-EXIT-FIRST-DEPTH"
    positions = StrategyDryRunRepository(session)
    positions.insert_position_if_absent(
        StrategyDryRunPositionInput(
            position_id="v2-first-depth-open",
            strategy_id=V2_STRATEGY_ID,
            market_ticker=market_ticker,
            decision_id="v2-first-depth-entry",
            side_candidate="YES",
            economic_side="YES",
            opened_at=now - timedelta(seconds=10),
            open_price=Decimal("0.70"),
            contract_count=1,
            entry_reason="v2_causal_hypothetical_fill",
            status="OPEN",
        )
    )
    intents = StrategyV2Repository(session)
    pending = intents.insert_intent_if_absent(
        StrategyTradeIntentInput(
            intent_id="v2-first-depth-exit",
            strategy_id=V2_STRATEGY_ID,
            decision_id="v2-first-depth-decision",
            position_id="v2-first-depth-open",
            market_ticker=market_ticker,
            side_candidate="YES",
            action="EXIT",
            created_at=now - timedelta(seconds=4),
            effective_after=now - timedelta(seconds=3),
            expires_at=now - timedelta(seconds=1),
            intended_limit_price=Decimal("0.72"),
            quantity=Decimal("1"),
        )
    )
    orderbooks = OrderbookRepository(session)
    orderbooks.insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker=market_ticker,
            received_at=now - timedelta(seconds=2),
            yes_bid=Decimal("0.75"),
            yes_bid_count=Decimal("0"),
            yes_ask=Decimal("0.76"),
            yes_ask_count=Decimal("1"),
            book_status="ok",
        )
    )
    orderbooks.insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker=market_ticker,
            received_at=now - timedelta(milliseconds=1500),
            yes_bid=Decimal("0.75"),
            yes_bid_count=Decimal("1"),
            yes_ask=Decimal("0.76"),
            yes_ask_count=Decimal("1"),
            book_status="ok",
        )
    )

    result, _ = observer_module._resolve_v2_pending_exit(
        session=session,
        intents=intents,
        positions=positions,
        pending=pending,
        resolved_at=now,
        decision=None,
    )

    assert result == "V2_EXIT_NO_FILL"
    assert pending.status == "NO_FILL"
    assert positions.get_position_by_id("v2-first-depth-open").status == "OPEN"
    assert intents.list_recent_outcomes(strategy_id=V2_STRATEGY_ID, limit=10) == []


def test_v2_exit_without_a_book_expires_and_keeps_position_open(session) -> None:
    now = datetime(2026, 7, 11, 12, 10, tzinfo=UTC)
    market_ticker = "KXBTC15M-EXIT-NO-BOOK"
    positions = StrategyDryRunRepository(session)
    positions.insert_position_if_absent(
        StrategyDryRunPositionInput(
            position_id="v2-no-book-open",
            strategy_id=V2_STRATEGY_ID,
            market_ticker=market_ticker,
            decision_id="v2-no-book-entry",
            side_candidate="YES",
            economic_side="YES",
            opened_at=now - timedelta(seconds=10),
            open_price=Decimal("0.70"),
            contract_count=1,
            entry_reason="v2_causal_hypothetical_fill",
            status="OPEN",
        )
    )
    intents = StrategyV2Repository(session)
    pending = intents.insert_intent_if_absent(
        StrategyTradeIntentInput(
            intent_id="v2-no-book-exit",
            strategy_id=V2_STRATEGY_ID,
            decision_id="v2-no-book-decision",
            position_id="v2-no-book-open",
            market_ticker=market_ticker,
            side_candidate="YES",
            action="EXIT",
            created_at=now - timedelta(seconds=4),
            effective_after=now - timedelta(seconds=3),
            expires_at=now - timedelta(seconds=1),
            intended_limit_price=Decimal("0.72"),
            quantity=Decimal("1"),
        )
    )

    result, _ = observer_module._resolve_v2_pending_exit(
        session=session,
        intents=intents,
        positions=positions,
        pending=pending,
        resolved_at=now,
        decision=None,
    )

    assert result == "V2_EXIT_EXPIRED"
    assert pending.status == "EXPIRED"
    assert positions.get_position_by_id("v2-no-book-open").status == "OPEN"
    assert intents.list_recent_outcomes(strategy_id=V2_STRATEGY_ID, limit=10) == []


def test_v2_exit_retries_apply_first_book_only_independently(session) -> None:
    now = datetime(2026, 7, 11, 12, 10, tzinfo=UTC)
    market_ticker = "KXBTC15M-EXIT-RETRY"
    positions = StrategyDryRunRepository(session)
    positions.insert_position_if_absent(
        StrategyDryRunPositionInput(
            position_id="v2-retry-open",
            strategy_id=V2_STRATEGY_ID,
            market_ticker=market_ticker,
            decision_id="v2-retry-entry",
            side_candidate="YES",
            economic_side="YES",
            opened_at=now - timedelta(seconds=10),
            open_price=Decimal("0.70"),
            contract_count=1,
            entry_reason="v2_causal_hypothetical_fill",
            status="OPEN",
        )
    )
    intents = StrategyV2Repository(session)
    first_attempt = intents.insert_intent_if_absent(
        StrategyTradeIntentInput(
            intent_id="v2-retry-one",
            strategy_id=V2_STRATEGY_ID,
            decision_id="v2-retry-one-decision",
            position_id="v2-retry-open",
            market_ticker=market_ticker,
            side_candidate="YES",
            action="EXIT",
            created_at=now - timedelta(seconds=8),
            effective_after=now - timedelta(seconds=7),
            expires_at=now - timedelta(seconds=5),
            intended_limit_price=Decimal("0.72"),
            quantity=Decimal("1"),
        )
    )
    second_attempt = intents.insert_intent_if_absent(
        StrategyTradeIntentInput(
            intent_id="v2-retry-two",
            strategy_id=V2_STRATEGY_ID,
            decision_id="v2-retry-two-decision",
            position_id="v2-retry-open",
            market_ticker=market_ticker,
            side_candidate="YES",
            action="EXIT",
            created_at=now - timedelta(seconds=4),
            effective_after=now - timedelta(seconds=3),
            expires_at=now - timedelta(seconds=1),
            intended_limit_price=Decimal("0.72"),
            quantity=Decimal("1"),
        )
    )
    orderbooks = OrderbookRepository(session)
    for received_at, bid in (
        (now - timedelta(seconds=6), Decimal("0.71")),
        (now - timedelta(milliseconds=5500), Decimal("0.75")),
        (now - timedelta(seconds=2), Decimal("0.71")),
        (now - timedelta(milliseconds=1500), Decimal("0.75")),
    ):
        orderbooks.insert_snapshot(
            OrderbookSnapshotInput(
                market_ticker=market_ticker,
                received_at=received_at,
                yes_bid=bid,
                yes_bid_count=Decimal("1"),
                yes_ask=bid + Decimal("0.01"),
                yes_ask_count=Decimal("1"),
                book_status="ok",
            )
        )

    first_result, _ = observer_module._resolve_v2_pending_exit(
        session=session,
        intents=intents,
        positions=positions,
        pending=first_attempt,
        resolved_at=now,
        decision=None,
    )
    second_result, _ = observer_module._resolve_v2_pending_exit(
        session=session,
        intents=intents,
        positions=positions,
        pending=second_attempt,
        resolved_at=now,
        decision=None,
    )

    assert first_result == "V2_EXIT_NO_FILL"
    assert second_result == "V2_EXIT_NO_FILL"
    assert first_attempt.status == "NO_FILL"
    assert second_attempt.status == "NO_FILL"
    assert positions.get_position_by_id("v2-retry-open").status == "OPEN"


def test_v2_intent_uses_recorded_timing_parameters(session, monkeypatch) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    market_ticker = "KXBTC15M-TIMING"
    monkeypatch.setattr(
        observer_module,
        "V2_PARAMETERS",
        {
            **observer_module.V2_PARAMETERS,
            "decision_to_book_latency_ms": 750,
            "intent_expiry_seconds": 3,
        },
    )

    observer_module._apply_v2_hypothetical_lifecycle(
        config=load_config({}),
        session=session,
        decision=StrategyDecisionInput(
            decision_id="v2-recorded-timing-decision",
            evaluated_at=now,
            decision_state=STATE_V2_ENTRY_SIGNAL,
            primary_reason="v2_entry_signal",
            app_mode="DRY_RUN",
            strategy_id=V2_STRATEGY_ID,
            market_ticker=market_ticker,
            candidate_side="YES",
            measurements={
                "intended_entry_price": "0.62",
                "features": {"desired_ask": "0.61"},
            },
        ),
    )

    intent = StrategyV2Repository(session).list_recent_intents(
        strategy_id=V2_STRATEGY_ID,
        limit=1,
    )[0]
    expected_effective_after = now + timedelta(milliseconds=750)
    assert intent.effective_after == expected_effective_after.replace(tzinfo=None)
    assert intent.expires_at == (expected_effective_after + timedelta(seconds=3)).replace(
        tzinfo=None
    )


def test_v2_intent_respects_open_position_cap_across_markets(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    StrategyDryRunRepository(session).insert_position_if_absent(
        StrategyDryRunPositionInput(
            position_id="v2-open-prior-market",
            strategy_id=V2_STRATEGY_ID,
            market_ticker="KXBTC15M-PRIOR",
            decision_id="v2-prior-entry",
            side_candidate="YES",
            economic_side="YES",
            opened_at=now - timedelta(minutes=1),
            open_price=Decimal("0.62"),
            contract_count=1,
            entry_reason="v2_causal_hypothetical_fill",
            status="OPEN",
        )
    )

    observer_module._apply_v2_hypothetical_lifecycle(
        config=load_config({}),
        session=session,
        decision=StrategyDecisionInput(
            decision_id="v2-new-market-entry",
            evaluated_at=now,
            decision_state=STATE_V2_ENTRY_SIGNAL,
            primary_reason="v2_entry_signal",
            app_mode="DRY_RUN",
            strategy_id=V2_STRATEGY_ID,
            market_ticker="KXBTC15M-NEXT",
            candidate_side="YES",
            measurements={"intended_entry_price": "0.62"},
        ),
    )

    assert (
        StrategyV2Repository(session).list_recent_intents(
            strategy_id=V2_STRATEGY_ID,
            limit=10,
        )
        == []
    )


def test_v2_sweeps_expired_intents_without_an_active_market(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    intents = StrategyV2Repository(session)
    for intent_id, market_ticker in (
        ("v2-expired-after-rollover", "KXBTC15M-EXPIRED"),
        ("v2-no-fill-after-rollover", "KXBTC15M-NO-FILL"),
    ):
        intents.insert_intent_if_absent(
            StrategyTradeIntentInput(
                intent_id=intent_id,
                strategy_id=V2_STRATEGY_ID,
                decision_id=f"{intent_id}-decision",
                market_ticker=market_ticker,
                side_candidate="YES",
                action="ENTRY",
                created_at=now - timedelta(seconds=4),
                effective_after=now - timedelta(seconds=3),
                expires_at=now - timedelta(seconds=1),
                intended_limit_price=Decimal("0.62"),
                quantity=Decimal("1"),
            )
        )
    OrderbookRepository(session).insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker="KXBTC15M-NO-FILL",
            received_at=now - timedelta(seconds=2),
            sequence_number=1,
            yes_bid=Decimal("0.64"),
            yes_ask=Decimal("0.65"),
            yes_ask_count=Decimal("1"),
            book_status="ok",
        )
    )

    observer_module._apply_v2_hypothetical_lifecycle(
        config=load_config({}),
        session=session,
        decision=StrategyDecisionInput(
            decision_id="v2-no-active-market-decision",
            evaluated_at=now,
            decision_state="V2_HARD_GATE_BLOCKED",
            primary_reason="no_active_market",
            app_mode="DRY_RUN",
            strategy_id=V2_STRATEGY_ID,
            market_ticker=None,
            candidate_side=None,
        ),
    )

    expired = intents.get_intent("v2-expired-after-rollover")
    no_fill = intents.get_intent("v2-no-fill-after-rollover")
    assert expired is not None
    assert expired.status == "EXPIRED"
    assert no_fill is not None
    assert no_fill.status == "NO_FILL"


def test_v2_manages_open_positions_without_an_active_market(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    market_ticker = "KXBTC15M-NO-ACTIVE-MARKET"
    positions = StrategyDryRunRepository(session)
    positions.insert_position_if_absent(
        StrategyDryRunPositionInput(
            position_id="v2-no-market-open-position",
            strategy_id=V2_STRATEGY_ID,
            market_ticker=market_ticker,
            decision_id="v2-no-market-entry",
            side_candidate="YES",
            economic_side="YES",
            opened_at=now - timedelta(seconds=10),
            open_price=Decimal("0.62"),
            contract_count=1,
            entry_reason="v2_causal_hypothetical_fill",
            status="OPEN",
        )
    )
    orderbooks = OrderbookRepository(session)
    orderbooks.insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker=market_ticker,
            received_at=now,
            yes_bid=Decimal("0.65"),
            yes_ask=Decimal("0.66"),
            yes_bid_count=Decimal("1"),
            yes_ask_count=Decimal("1"),
            no_bid=Decimal("0.34"),
            no_ask=Decimal("0.35"),
            no_bid_count=Decimal("1"),
            no_ask_count=Decimal("1"),
            book_status="ok",
        )
    )
    no_market_decision = StrategyDecisionInput(
        decision_id="v2-no-active-market-management",
        evaluated_at=now,
        decision_state="V2_HARD_GATE_BLOCKED",
        primary_reason="no_active_market",
        app_mode="DRY_RUN",
        strategy_id=V2_STRATEGY_ID,
        market_ticker=None,
        candidate_side=None,
    )

    observer_module._apply_v2_hypothetical_lifecycle(
        config=load_config({}),
        session=session,
        decision=no_market_decision,
    )

    intents = StrategyV2Repository(session)
    pending_exit = intents.get_pending_exit_intent(position_id="v2-no-market-open-position")
    assert pending_exit is not None
    assert pending_exit.trigger == "v2_force_market_lifecycle_failure"
    assert intents.list_marks_for_position(position_id="v2-no-market-open-position")

    orderbooks.insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker=market_ticker,
            received_at=now + timedelta(milliseconds=750),
            yes_bid=Decimal("0.65"),
            yes_ask=Decimal("0.66"),
            yes_bid_count=Decimal("1"),
            yes_ask_count=Decimal("1"),
            no_bid=Decimal("0.34"),
            no_ask=Decimal("0.35"),
            no_bid_count=Decimal("1"),
            no_ask_count=Decimal("1"),
            book_status="ok",
        )
    )
    result = observer_module._apply_v2_hypothetical_lifecycle(
        config=load_config({}),
        session=session,
        decision=replace(no_market_decision, evaluated_at=now + timedelta(seconds=1)),
    )

    position = positions.get_position_by_id("v2-no-market-open-position")
    assert position is not None
    assert position.status == "FORCE_CLOSED"
    assert position.close_price == Decimal("0.65")
    assert result.latest_event_type == "V2_EXIT_FILLED"


def test_v2_resolves_only_the_first_post_delay_orderbook(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    market_ticker = "KXBTC15M-FIRST-BOOK"
    intents = StrategyV2Repository(session)
    intents.insert_intent_if_absent(
        StrategyTradeIntentInput(
            intent_id="v2-first-book-only",
            strategy_id=V2_STRATEGY_ID,
            decision_id="v2-first-book-decision",
            market_ticker=market_ticker,
            side_candidate="YES",
            action="ENTRY",
            created_at=now - timedelta(seconds=4),
            effective_after=now - timedelta(seconds=3),
            expires_at=now - timedelta(seconds=1),
            intended_limit_price=Decimal("0.62"),
            quantity=Decimal("1"),
        )
    )
    orderbooks = OrderbookRepository(session)
    orderbooks.insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker=market_ticker,
            received_at=now - timedelta(milliseconds=2500),
            sequence_number=1,
            yes_bid=Decimal("0.64"),
            yes_ask=Decimal("0.65"),
            yes_ask_count=Decimal("1"),
            book_status="ok",
        )
    )
    orderbooks.insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker=market_ticker,
            received_at=now - timedelta(seconds=2),
            sequence_number=2,
            yes_bid=Decimal("0.60"),
            yes_ask=Decimal("0.61"),
            yes_ask_count=Decimal("1"),
            book_status="ok",
        )
    )

    observer_module._apply_v2_hypothetical_lifecycle(
        config=load_config({}),
        session=session,
        decision=StrategyDecisionInput(
            decision_id="v2-first-book-resolution",
            evaluated_at=now,
            decision_state="V2_HARD_GATE_BLOCKED",
            primary_reason="reference_stale",
            app_mode="DRY_RUN",
            strategy_id=V2_STRATEGY_ID,
            market_ticker=market_ticker,
            candidate_side=None,
        ),
    )

    intent = intents.get_intent("v2-first-book-only")
    assert intent is not None
    assert intent.status == "NO_FILL"
    assert intent.position_id is None
    assert StrategyDryRunRepository(session).count_open_positions(strategy_id=V2_STRATEGY_ID) == 0


def test_v2_position_mark_uses_the_held_side_bid(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    market_ticker = "KXBTC15M-ACTIVE"
    StrategyDryRunRepository(session).insert_position_if_absent(
        StrategyDryRunPositionInput(
            position_id="v2-held-yes-position",
            strategy_id=V2_STRATEGY_ID,
            market_ticker=market_ticker,
            decision_id="v2-entry-decision",
            side_candidate="YES",
            economic_side="YES",
            opened_at=now - timedelta(seconds=1),
            open_price=Decimal("0.62"),
            contract_count=1,
            entry_reason="v2_causal_hypothetical_fill",
            status="OPEN",
        )
    )
    OrderbookRepository(session).insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker=market_ticker,
            received_at=now,
            sequence_number=1,
            yes_bid=Decimal("0.64"),
            yes_ask=Decimal("0.66"),
            no_bid=Decimal("0.34"),
            no_ask=Decimal("0.36"),
            book_status="ok",
        )
    )

    observer_module._apply_v2_hypothetical_lifecycle(
        config=load_config({}),
        session=session,
        decision=StrategyDecisionInput(
            decision_id="v2-current-no-candidate-decision",
            evaluated_at=now,
            decision_state="V2_HARD_GATE_BLOCKED",
            primary_reason="reference_stale",
            app_mode="DRY_RUN",
            strategy_id=V2_STRATEGY_ID,
            market_ticker=market_ticker,
            candidate_side="NO",
            measurements={"features": {"desired_bid": "0.34"}},
        ),
    )

    marks = StrategyV2Repository(session).list_recent_marks(
        strategy_id=V2_STRATEGY_ID,
        limit=1,
    )
    assert marks[0].position_id == "v2-held-yes-position"
    assert marks[0].executable_bid == Decimal("0.64")


def test_dry_run_status_reports_disabled_challenger_without_worker_metadata(
    tmp_path,
) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_strategy_challenger_status.sqlite'}"
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
    try:
        status = build_strategy_dry_run_status(
            config,
            strategy_id=CHALLENGER_STRATEGY_ID,
        )

        assert status.enabled is False
        assert status.worker_observed_enabled is None
    finally:
        engine.dispose()


def test_strategy_variant_metadata_reports_disabled_when_dry_run_is_off(tmp_path) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_strategy_disabled.sqlite'}"
    config = load_config(
        {
            "DATABASE_URL": database_url,
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STRATEGY_DRY_RUN_ENABLED": "false",
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
        observer.evaluate_once()

        with session_factory() as session:
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")

        assert heartbeat is not None
        assert heartbeat.metadata_["strategy"]["variants"][CONTROL_STRATEGY_ID]["enabled"] is False
    finally:
        engine.dispose()


def test_dry_run_comparison_excludes_old_closed_positions_from_window(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    repository = StrategyDryRunRepository(session)
    strategy_id = CONTROL_STRATEGY_ID
    for position_id, status, closed_at in (
        ("old-closed-position", "CLOSED", now - timedelta(days=1)),
        ("old-open-position", "OPEN", None),
    ):
        repository.insert_position_if_absent(
            StrategyDryRunPositionInput(
                position_id=position_id,
                strategy_id=strategy_id,
                market_ticker="KXBTC15M-ACTIVE",
                decision_id=f"decision-{position_id}",
                side_candidate="YES",
                economic_side="YES",
                opened_at=now - timedelta(days=2),
                open_price=Decimal("0.62"),
                contract_count=1,
                entry_reason="dry_run_entry_signal",
                status=status,
                closed_at=closed_at,
                close_price=Decimal("0.63") if closed_at else None,
                close_reason="test_close" if closed_at else None,
                realized_pnl_cents=Decimal("0.01") if closed_at else None,
            )
        )

    repository.insert_event_if_absent(
        StrategyDryRunEventInput(
            event_id="old-dry-run-event",
            strategy_id=strategy_id,
            event_type="ENTER_DRY_RUN",
            occurred_at=now - timedelta(days=1),
        )
    )

    summary = repository.comparison_summary_since(
        strategy_id=strategy_id,
        since=now - timedelta(hours=1),
    )

    assert summary["opened_positions"] == 0
    assert summary["closed_positions"] == 0
    assert summary["current_open_positions"] == 1
    assert summary["latest_position_opened_at"] is None
    assert summary["latest_position_closed_at"] is None
    assert summary["latest_event_at"] is None


def test_strategy_entry_ask_at_max_is_eligible_and_intended_price_is_clamped(session) -> None:
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

    assert decision.decision_state == STATE_ENTER_DRY_RUN
    assert decision.measurements["dry_run_intended_entry_price"] == "0.78"
    assert decision.measurements["gate_results"]["entry_price"]["status"] == "pass"
    assert decision.measurements["gate_results"]["contract_confirmation"]["status"] != "block"


def test_strategy_entry_ask_below_minimum_is_blocked_even_if_offset_reaches_minimum(
    session,
) -> None:
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

    assert decision.decision_state == STATE_CONTRACT_NOT_CONFIRMED
    assert decision.primary_reason == "dry_run_intended_entry_price_too_low"
    assert decision.measurements["dry_run_intended_entry_price"] == "0.56"
    trace = decision.measurements["gate_trace"]
    assert trace["canonical_primary_gate"] == "raw_entry_ask"
    assert trace["gates"]["raw_entry_ask"]["status"] == "block"
    assert "impulse" in trace["gates"]


def test_strategy_entry_ask_above_maximum_is_blocked_before_offset(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config(
        {
            "APP_MODE": "DRY_RUN",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STRATEGY_DRY_RUN_ENABLED": "true",
        }
    )
    _seed_observable_context(
        session,
        now=now,
        yes_bid=Decimal("0.761"),
        yes_ask=Decimal("0.781"),
        yes_spread=Decimal("0.02"),
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=assess_startup_safety(config),
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_CONTRACT_NOT_CONFIRMED
    assert decision.primary_reason == "dry_run_intended_entry_price_too_high"
    assert decision.measurements["dry_run_intended_entry_price"] == "0.78"


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
    assert decision.measurements["gate_trace"]["gates"]["trade_confirmation"]["status"] == "warn"


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


def test_strategy_recomputes_brti_valid_message_age_from_timestamp(session) -> None:
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
        brti_valid_message_age_ms=1_000,
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_REFERENCE_STALE
    assert decision.primary_reason == "brti_reference_stream_stale"
    assert decision.measurements["brti_reference_stream_age_ms"] == 4000


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
    _record_feed_heartbeat(
        session,
        now=now,
        orderbook_stream_last_message_at=now - timedelta(seconds=5),
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state != STATE_KALSHI_STALE
    assert "kalshi_orderbook_data_quiet_carried_forward" in decision.warnings
    assert decision.measurements["orderbook_carry_forward_allowed"] is True
    assert decision.measurements["orderbook_snapshot_source"] == "carried_forward"
    assert decision.measurements["market_feed_transport_state"] == "healthy"
    assert decision.measurements["market_feed_subscription_state"] == "subscribed"
    assert decision.measurements["market_feed_snapshot_state"] == "initialized"
    assert decision.measurements["market_feed_sequence_state"] == "clean"
    assert decision.measurements["market_data_quiet"] is True
    assert decision.measurements["orderbook_recovery_action"] == "request_snapshot"
    assert decision.measurements["gate_results"]["book"]["status"] == "warn"
    assert (
        decision.measurements["gate_results"]["book"]["reason"]
        == "kalshi_orderbook_data_quiet_carried_forward"
    )


def test_strategy_keeps_background_snapshot_refresh_warning_only(session) -> None:
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
        orderbook_stream_last_message_at=now - timedelta(seconds=5),
        orderbook_snapshot_source="carried_forward",
        orderbook_recovery_action="request_snapshot",
        market_recovery_attempt_in_progress=True,
        market_subscription_recovery_last_reason="market_data_quiet",
        market_subscription_recovery_last_action="get_snapshot",
        market_subscription_recovery_last_result="requested",
        market_recovery_attempt_age_ms=500,
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state != STATE_KALSHI_STALE
    assert "kalshi_orderbook_data_quiet_carried_forward" in decision.warnings
    assert decision.primary_reason != "kalshi_orderbook_snapshot_resync_pending"
    assert decision.measurements["orderbook_carry_forward_allowed"] is True
    assert decision.measurements["orderbook_snapshot_source"] == "carried_forward"
    assert decision.measurements["market_recovery_attempt_in_progress"] is True
    assert decision.measurements["gate_results"]["book"]["status"] == "warn"


def test_strategy_carries_forward_with_legacy_fresh_stream_heartbeat(session) -> None:
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
        include_orderbook_transport_fields=False,
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state != STATE_KALSHI_STALE
    assert decision.measurements["market_feed_transport_state"] == "unknown"
    assert decision.measurements["orderbook_carry_forward_allowed"] is True
    assert decision.measurements["orderbook_snapshot_source"] == "carried_forward"
    assert "kalshi_orderbook_data_quiet_carried_forward" in decision.warnings


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
        orderbook_transport_alive=False,
        orderbook_transport_state="stale",
        orderbook_transport_last_pong_at=now - timedelta(seconds=40),
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_KALSHI_STALE
    assert decision.primary_reason == "kalshi_orderbook_transport_stale"
    assert decision.measurements["gate_results"]["book"]["status"] == "block"
    assert decision.measurements["gate_results"]["book"]["transport_state"] == "stale"


def test_strategy_reports_subscription_recovery_pending(session) -> None:
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
        orderbook_connection_state="connecting",
        orderbook_initialized=False,
        market_feed_state="RECOVERING_SUBSCRIPTION",
        market_recovery_attempt_in_progress=True,
        market_subscription_recovery_last_reason="orderbook_sid_pending",
        market_subscription_recovery_last_action="wait_for_subscription_ack",
        market_subscription_recovery_last_result="waiting",
        market_recovery_attempt_age_ms=500,
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_KALSHI_STALE
    assert decision.primary_reason == "kalshi_orderbook_subscription_recovery_pending"
    assert decision.measurements["market_feed_state"] == "RECOVERING_SUBSCRIPTION"
    assert decision.measurements["market_recovery_attempt_in_progress"] is True
    assert decision.measurements["market_recovery_attempt_age_ms"] == 500


def test_strategy_reports_subscription_recovery_failed(session) -> None:
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
        orderbook_connection_state="reconnect_pending",
        orderbook_initialized=False,
        market_feed_state="BLOCKED_UNRECOVERED",
        market_subscription_recovery_last_reason=("kalshi_orderbook_subscription_ack_timeout"),
        market_subscription_recovery_last_action="reconnect",
        market_subscription_recovery_last_result="failed",
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_KALSHI_STALE
    assert decision.primary_reason == "kalshi_orderbook_subscription_recovery_failed"
    assert decision.measurements["market_feed_state"] == "BLOCKED_UNRECOVERED"
    assert decision.measurements["market_unrecovered_blocker_count"] == 1


def test_strategy_allows_fresh_orderbook_when_stream_live_requirement_disabled(
    session,
) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config(
        {
            "STRATEGY_KALSHI_BOOK_MAX_AGE_MS": "2000",
            "STRATEGY_KALSHI_BOOK_REQUIRE_STREAM_LIVE": "false",
        }
    )
    safety = assess_startup_safety(config)
    _seed_observable_context(
        session,
        now=now,
        latest_orderbook_received_at=now - timedelta(milliseconds=500),
    )
    _record_feed_heartbeat(
        session,
        now=now,
        orderbook_connection_state="connecting",
        orderbook_initialized=None,
        orderbook_transport_alive=False,
        orderbook_transport_state="stale",
        orderbook_transport_last_pong_at=now - timedelta(seconds=40),
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state != STATE_KALSHI_STALE
    assert decision.primary_reason != "kalshi_orderbook_subscription_inactive"
    assert decision.primary_reason != "kalshi_orderbook_uninitialized"
    assert decision.measurements["orderbook_age_ms"] == 500


def test_strategy_recomputes_stale_transport_from_old_pong(session) -> None:
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
        orderbook_transport_alive=True,
        orderbook_transport_state="healthy",
        orderbook_transport_last_pong_at=now - timedelta(seconds=40),
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_KALSHI_STALE
    assert decision.primary_reason == "kalshi_orderbook_transport_stale"
    assert decision.measurements["market_feed_transport_state"] == "stale"
    assert decision.measurements["gate_results"]["book"]["transport_state"] == "stale"


def test_strategy_uses_fresh_orderbook_when_stream_heartbeat_is_stale(
    session,
) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config(
        {
            "STRATEGY_KALSHI_BOOK_MAX_AGE_MS": "2000",
            "STRATEGY_KALSHI_BOOK_STREAM_MAX_AGE_MS": "3000",
        }
    )
    safety = assess_startup_safety(config)
    _seed_observable_context(
        session,
        now=now,
        latest_orderbook_received_at=now - timedelta(milliseconds=500),
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

    assert decision.decision_state != STATE_KALSHI_STALE
    assert decision.primary_reason != "kalshi_orderbook_stream_stale"
    assert decision.measurements["orderbook_age_ms"] == 500
    assert decision.measurements["orderbook_stream_age_ms"] == 4000
    assert (
        decision.measurements["orderbook_liveness_reason"]
        == "kalshi_orderbook_data_quiet_carried_forward"
    )
    assert decision.measurements["gate_results"]["book"]["status"] == "warn"


def test_strategy_prefers_component_liveness_over_stale_aggregate(session) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config(
        {
            "STRATEGY_KALSHI_BOOK_MAX_AGE_MS": "2000",
            "STRATEGY_KALSHI_BOOK_STREAM_MAX_AGE_MS": "3000",
        }
    )
    safety = assess_startup_safety(config)
    _seed_observable_context(
        session,
        now=now,
        latest_orderbook_received_at=now - timedelta(milliseconds=500),
    )
    _record_feed_heartbeat(
        session,
        now=now,
        orderbook_stream_last_message_at=now - timedelta(milliseconds=500),
        brti_last_valid_message_at=now - timedelta(milliseconds=500),
    )
    WorkerHeartbeatRepository(session).record_heartbeat(
        WorkerHeartbeatInput(
            service_name=WORKER_SERVICE_AGGREGATE,
            started_at=now - timedelta(minutes=1),
            heartbeat_at=now,
            app_mode="OBSERVER",
            is_safe=True,
            metadata={
                "mode": "strategy_observer",
                "ws": {
                    "enabled": True,
                    "connection_state": "subscribed",
                    "active_market_ticker": "KXBTC15M-ACTIVE",
                    "last_message_at": (now - timedelta(seconds=30)).isoformat(),
                    "last_ticker_at": (now - timedelta(seconds=30)).isoformat(),
                    "last_trade_at": (now - timedelta(seconds=30)).isoformat(),
                    "last_orderbook_at": (now - timedelta(seconds=30)).isoformat(),
                    "orderbook_initialized": True,
                    "warnings": [],
                    "blockers": [],
                },
                "reference": {
                    "brti": {
                        "enabled": True,
                        "connection_state": "subscribed",
                        "last_message_at": (now - timedelta(seconds=30)).isoformat(),
                        "last_valid_message_at": (now - timedelta(seconds=30)).isoformat(),
                        "warnings": ["brti_reference_transport_stale"],
                        "blockers": [],
                    }
                },
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

    assert decision.decision_state != STATE_KALSHI_STALE
    assert decision.decision_state != STATE_REFERENCE_STALE
    assert decision.measurements["market_liveness_source"] == "component"
    assert decision.measurements["reference_liveness_source"] == "component"
    assert decision.measurements["orderbook_stream_age_ms"] == 500
    assert decision.measurements["brti_reference_stream_age_ms"] == 500


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


def test_strategy_blocks_old_orderbook_when_invalid_update_warning_active(
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
        orderbook_warnings=["invalid_orderbook_delta_delta_fp"],
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_KALSHI_STALE
    assert decision.primary_reason == "kalshi_orderbook_invalid_update"


def test_strategy_blocks_old_orderbook_when_snapshot_resync_failed(session) -> None:
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
        orderbook_warnings=["kalshi_orderbook_snapshot_resync_failed"],
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_KALSHI_STALE
    assert decision.primary_reason == "kalshi_orderbook_snapshot_resync_failed"
    assert decision.measurements["gate_results"]["book"]["status"] == "block"


def test_strategy_blocks_old_orderbook_without_initialized_proof(session) -> None:
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
        orderbook_initialized=None,
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=safety,
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_KALSHI_STALE
    assert decision.primary_reason == "kalshi_orderbook_uninitialized"


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
    assert decision.measurements["orderbook_recovery_action"] == "request_snapshot"


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
    assert decision.measurements["gate_results"]["contract_confirmation"]["status"] == "block"
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
    assert decision.measurements["gate_results"]["contract_confirmation"]["status"] == "block"
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
    assert decision.measurements["gate_results"]["boundary"]["status"] == "not_evaluated"


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
    assert decision.measurements["gate_results"]["boundary"]["status"] == "not_evaluated"


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
            open_positions = dry_run_repository.list_open_positions(strategy_id=config.strategy_id)
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
    assert decision.measurements["managed_position_id"] == ("dryrun-only-stale-position")


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
    assert decision.measurements["managed_position_id"] == ("dryrun-older-profit-position")


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
    assert decision.measurements["managed_position_id"] == ("dryrun-stale-book-position")


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
    assert decision.measurements["gate_trace"]["canonical_primary_gate"] == "impulse"
    assert (
        decision.measurements["gate_trace"]["gates"]["impulse"]["affects_canonical_decision"]
        is True
    )


def test_strategy_gate_trace_attributes_emitted_chop_block(session, monkeypatch) -> None:
    now = datetime(2026, 7, 5, 12, 10, tzinfo=UTC)
    config = load_config({})
    _seed_observable_context(session, now=now)
    monkeypatch.setattr(
        observer_module,
        "_brti_chop_metrics",
        lambda **_: {
            "boundary_cross_count": 0,
            "retrace_fraction": None,
            "reason": "short_move_opposes_medium_move",
        },
    )

    decision = evaluate_strategy_observer(
        config=config,
        safety=assess_startup_safety(config),
        session=session,
        now=now,
    )

    assert decision.decision_state == STATE_CHOP_FILTER_BLOCKED
    assert decision.primary_reason == "short_move_opposes_medium_move"
    assert decision.measurements["gate_trace"]["canonical_primary_gate"] == "chop"
    assert (
        decision.measurements["gate_trace"]["gates"]["chop"]["affects_canonical_decision"] is True
    )


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
        decision.measurements["brti_reference_stale_reason"] == "brti_reference_first_tick_timeout"
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
                            "last_message_at": (now - timedelta(seconds=1)).isoformat(),
                            "last_orderbook_at": (now - timedelta(milliseconds=500)).isoformat(),
                            "orderbook_initialized": True,
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
            received_at=latest_orderbook_received_at or now - timedelta(milliseconds=500),
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
    orderbook_connection_state: str = "subscribed",
    orderbook_stream_last_message_at: datetime | None = None,
    orderbook_active_market_ticker: str = "KXBTC15M-ACTIVE",
    orderbook_warnings: list[str] | None = None,
    orderbook_initialized: bool | None = True,
    include_orderbook_transport_fields: bool = True,
    orderbook_transport_alive: bool = True,
    orderbook_transport_last_pong_at: datetime | None = None,
    orderbook_transport_state: str = "healthy",
    market_feed_state: str = "LIVE",
    orderbook_snapshot_source: str = "fresh_update",
    orderbook_recovery_action: str = "none",
    market_recovery_attempt_in_progress: bool = False,
    market_subscription_recovery_last_reason: str | None = None,
    market_subscription_recovery_last_action: str | None = None,
    market_subscription_recovery_last_result: str | None = None,
    market_recovery_attempt_age_ms: int | None = None,
    brti_last_valid_message_at: datetime | None = None,
    brti_last_valid_message_source_ts: datetime | None = None,
    brti_last_valid_message_value: str | None = "62110",
    brti_carried_forward: bool = False,
    brti_valid_message_age_ms: int | None = None,
) -> None:
    stream_at = orderbook_stream_last_message_at or now - timedelta(seconds=1)
    transport_at = orderbook_transport_last_pong_at or now - timedelta(milliseconds=500)
    brti_message_at = brti_last_valid_message_at or now - timedelta(seconds=1)
    ws_metadata: dict[str, object] = {
        "enabled": True,
        "connection_state": orderbook_connection_state,
        "active_market_ticker": orderbook_active_market_ticker,
        "last_message_at": stream_at.isoformat(),
        "last_market_data_message_at": stream_at.isoformat(),
        "market_data_message_age_ms": int((now - stream_at).total_seconds() * 1000),
        "market_feed_subscription_state": "subscribed",
        "market_feed_snapshot_state": ("initialized" if orderbook_initialized else "missing"),
        "market_feed_active_ticker_state": (
            "match" if orderbook_active_market_ticker == "KXBTC15M-ACTIVE" else "mismatch"
        ),
        "market_feed_sequence_state": (
            "gap"
            if orderbook_warnings
            and any("sequence_gap" in warning for warning in orderbook_warnings)
            else "clean"
            if orderbook_initialized
            else "unknown"
        ),
        "market_data_quiet": (int((now - stream_at).total_seconds() * 1000) > 3000),
        "market_data_quiet_age_ms": (
            int((now - stream_at).total_seconds() * 1000)
            if int((now - stream_at).total_seconds() * 1000) > 3000
            else None
        ),
        "orderbook_snapshot_source": orderbook_snapshot_source,
        "orderbook_recovery_action": orderbook_recovery_action,
        "market_feed_state": market_feed_state,
        "market_subscription_recovery_count": 0,
        "market_subscription_recovery_last_reason": (market_subscription_recovery_last_reason),
        "market_subscription_recovery_last_action": (market_subscription_recovery_last_action),
        "market_subscription_recovery_last_result": (market_subscription_recovery_last_result),
        "market_subscription_recovery_last_at": (
            stream_at.isoformat() if market_subscription_recovery_last_reason is not None else None
        ),
        "market_snapshot_resync_count": 0,
        "market_snapshot_resync_last_result": None,
        "market_rollover_recovery_count": 0,
        "market_transport_reconnect_count": 0,
        "market_unrecovered_blocker_count": (
            1 if market_subscription_recovery_last_result == "failed" else 0
        ),
        "market_recovery_attempt_in_progress": market_recovery_attempt_in_progress,
        "market_recovery_attempt_age_ms": market_recovery_attempt_age_ms,
        "last_ticker_at": stream_at.isoformat(),
        "last_trade_at": stream_at.isoformat(),
        "last_orderbook_at": (now - timedelta(seconds=5)).isoformat(),
        "orderbook_sequence_number": 123,
        "warnings": orderbook_warnings or [],
        "blockers": [],
    }
    if include_orderbook_transport_fields:
        ws_metadata.update(
            {
                "transport_alive": orderbook_transport_alive,
                "transport_last_pong_at": transport_at.isoformat(),
                "transport_age_ms": int((now - transport_at).total_seconds() * 1000),
                "transport_liveness_reason": (
                    None
                    if orderbook_transport_state == "healthy"
                    else "kalshi_orderbook_transport_stale"
                ),
                "market_feed_transport_state": orderbook_transport_state,
            }
        )
    if orderbook_initialized is not None:
        ws_metadata["orderbook_initialized"] = orderbook_initialized
    reference_metadata = {
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
        "valid_message_age_ms": (
            brti_valid_message_age_ms
            if brti_valid_message_age_ms is not None
            else int((now - brti_message_at).total_seconds() * 1000)
        ),
        "valid_message_carried_forward": brti_carried_forward,
        "reference_stream_live": ((now - brti_message_at).total_seconds() <= 3),
        "warnings": [],
        "blockers": [],
    }
    repository = WorkerHeartbeatRepository(session)
    heartbeat_at = now - timedelta(milliseconds=250)
    repository.record_heartbeat(
        WorkerHeartbeatInput(
            service_name=WORKER_SERVICE_MARKET_WS,
            started_at=now - timedelta(minutes=1),
            heartbeat_at=heartbeat_at,
            app_mode="OBSERVER",
            is_safe=True,
            metadata={"mode": "market_ws", "ws": ws_metadata},
        )
    )
    repository.record_heartbeat(
        WorkerHeartbeatInput(
            service_name=WORKER_SERVICE_REFERENCE_BRTI,
            started_at=now - timedelta(minutes=1),
            heartbeat_at=heartbeat_at,
            app_mode="OBSERVER",
            is_safe=True,
            metadata={
                "mode": "reference_brti",
                "reference": {"brti": reference_metadata},
            },
        )
    )
    repository.record_heartbeat(
        WorkerHeartbeatInput(
            service_name=WORKER_SERVICE_AGGREGATE,
            started_at=now - timedelta(minutes=1),
            heartbeat_at=heartbeat_at,
            app_mode="OBSERVER",
            is_safe=True,
            metadata={
                "mode": "kalshi_ws",
                "ws": ws_metadata,
                "reference": {"brti": reference_metadata},
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
