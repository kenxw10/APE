from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.dialects import postgresql

from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.inputs import (
    KalshiWsProtocolEventInput,
    MarketInput,
    OrderbookSnapshotInput,
    PublicTradeInput,
    ReferenceTickInput,
    StrategyDecisionInput,
    StrategyDryRunEventInput,
    StrategyDryRunPositionInput,
    WorkerHeartbeatInput,
)
from ape.repositories.kalshi_ws_protocol import KalshiWsProtocolEventRepository
from ape.repositories.markets import MarketsRepository
from ape.repositories.orderbook import OrderbookRepository
from ape.repositories.public_trades import PublicTradesRepository, _recent_trades_statement
from ape.repositories.reference_ticks import ReferenceTicksRepository
from ape.repositories.strategy_decisions import StrategyDecisionsRepository
from ape.repositories.strategy_dry_run import StrategyDryRunRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository


@pytest.fixture
def session(tmp_path):
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_repositories.sqlite'}"
    engine = create_engine_from_config(load_config({"DATABASE_URL": database_url}))
    run_migrations(engine)
    session_factory = create_session_factory(engine)

    try:
        with session_factory() as db_session:
            yield db_session
    finally:
        engine.dispose()


def test_markets_repository_upserts_and_lists_market(session) -> None:
    repository = MarketsRepository(session)

    created = repository.upsert_market(
        MarketInput(
            market_ticker="KXBTC-TEST-001",
            title="Initial title",
            functional_strike=Decimal("50000.00"),
            price_ranges=[{"min": 0, "max": 100}],
        )
    )
    updated = repository.upsert_market(
        MarketInput(
            market_ticker="KXBTC-TEST-001",
            title="Updated title",
            functional_strike=Decimal("51000.00"),
            resolver_decision_reason="test-upsert",
        )
    )

    assert updated.id == created.id
    assert repository.get_market_by_ticker("KXBTC-TEST-001").title == "Updated title"
    assert repository.list_recent_markets(limit=1)[0].market_ticker == "KXBTC-TEST-001"


def test_reference_ticks_repository_inserts_and_reads_recent_ticks(session) -> None:
    repository = ReferenceTicksRepository(session)
    now = datetime.now(UTC)

    repository.insert_tick(
        ReferenceTickInput(
            source="BRTI",
            received_at=now,
            parse_status="parsed",
            raw_value="50000.25",
            parsed_value=Decimal("50000.25"),
            raw_payload={"value": "50000.25"},
        )
    )

    recent = repository.get_recent_ticks("BRTI", limit=1)
    assert len(recent) == 1
    assert recent[0].source == "BRTI"
    assert recent[0].parse_status == "parsed"
    assert repository.get_latest_tick("BRTI").parse_status == "parsed"


def test_reference_ticks_repository_reads_latest_non_null_source_ts(session) -> None:
    repository = ReferenceTicksRepository(session)
    now = datetime.now(UTC)
    source_ts = now - timedelta(seconds=5)

    repository.insert_tick(
        ReferenceTickInput(
            source="BRTI",
            received_at=now - timedelta(seconds=2),
            source_ts=source_ts,
            parse_status="valid",
        )
    )
    repository.insert_tick(
        ReferenceTickInput(
            source="BRTI",
            received_at=now,
            source_ts=None,
            parse_status="malformed_value",
        )
    )

    assert repository.get_latest_tick("BRTI").parse_status == "malformed_value"
    latest_with_source_ts = repository.get_latest_tick_with_source_ts("BRTI")
    assert latest_with_source_ts is not None
    latest_source_ts = latest_with_source_ts.source_ts
    assert latest_source_ts is not None
    if latest_source_ts.tzinfo is None:
        latest_source_ts = latest_source_ts.replace(tzinfo=UTC)
    assert latest_source_ts == source_ts


def test_orderbook_repository_inserts_and_reads_latest_snapshot(session) -> None:
    repository = OrderbookRepository(session)
    now = datetime.now(UTC)

    repository.insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker="KXBTC-TEST-001",
            received_at=now - timedelta(seconds=2),
            yes_bid=Decimal("48"),
            book_status="ok",
        )
    )
    repository.insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker="KXBTC-TEST-001",
            received_at=now,
            yes_bid=Decimal("49"),
            yes_ask=Decimal("51"),
            yes_bid_count=Decimal("12.50"),
            yes_ask_count=Decimal("8.25"),
            raw_payload={"top": "book"},
        )
    )

    latest = repository.get_latest_snapshot("KXBTC-TEST-001")
    assert latest is not None
    assert latest.yes_bid == Decimal("49.00000000")
    assert latest.yes_bid_count == Decimal("12.50000000")
    assert latest.yes_ask_count == Decimal("8.25000000")
    assert repository.get_latest_snapshot_any().market_ticker == "KXBTC-TEST-001"


def test_orderbook_repository_limits_to_newest_snapshots_since(session) -> None:
    repository = OrderbookRepository(session)
    now = datetime.now(UTC)
    for offset_seconds, bid in ((3, "47"), (2, "48"), (1, "49"), (0, "50")):
        repository.insert_snapshot(
            OrderbookSnapshotInput(
                market_ticker="KXBTC-TEST-001",
                received_at=now - timedelta(seconds=offset_seconds),
                yes_bid=Decimal(bid),
                book_status="ok",
            )
        )

    rows = repository.get_snapshots_since(
        "KXBTC-TEST-001",
        now - timedelta(seconds=10),
        limit=2,
    )

    assert [row.yes_bid for row in rows] == [
        Decimal("49.00000000"),
        Decimal("50.00000000"),
    ]
    assert rows[0].received_at <= rows[1].received_at


def test_public_trades_repository_inserts_and_reads_recent_trades(session) -> None:
    repository = PublicTradesRepository(session)
    now = datetime.now(UTC)

    repository.insert_trade(
        PublicTradeInput(
            market_ticker="KXBTC-TEST-001",
            trade_id="trade-1",
            received_at=now,
            executed_at=now,
            price=Decimal("52"),
            trade_count=Decimal("3.50"),
            taker_side="yes",
        )
    )

    recent = repository.get_recent_trades("KXBTC-TEST-001", limit=1)
    assert len(recent) == 1
    assert recent[0].trade_id == "trade-1"
    assert recent[0].count is None
    assert recent[0].trade_count == Decimal("3.50000000")
    assert repository.get_latest_trade("KXBTC-TEST-001").trade_id == "trade-1"
    assert repository.get_latest_trade().market_ticker == "KXBTC-TEST-001"


def test_recent_public_trades_query_orders_null_executed_at_last_for_postgres() -> None:
    compiled_sql = str(
        _recent_trades_statement(market_ticker="KXBTC-TEST-001", limit=10).compile(
            dialect=postgresql.dialect()
        )
    )

    assert "public_trades.executed_at DESC NULLS LAST" in compiled_sql


def test_strategy_decisions_repository_inserts_and_reads_decision(session) -> None:
    repository = StrategyDecisionsRepository(session)
    now = datetime.now(UTC)

    repository.insert_decision(
        StrategyDecisionInput(
            decision_id="decision-1",
            evaluated_at=now,
            decision_state="skip",
            primary_reason="observer-only",
            app_mode="OBSERVER",
            market_ticker="KXBTC-TEST-001",
            measurements={"distance_bps": 12.5},
            blockers=["no-strategy-in-pr-2"],
            warnings=[],
        )
    )

    decision = repository.get_decision_by_id("decision-1")
    assert decision is not None
    assert decision.decision_state == "skip"
    assert repository.list_recent_decisions(limit=1)[0].decision_id == "decision-1"


def test_strategy_dry_run_repository_is_idempotent_and_closes_position(session) -> None:
    repository = StrategyDryRunRepository(session)
    now = datetime.now(UTC)
    position = StrategyDryRunPositionInput(
        position_id="dryrun-position-1",
        strategy_id="btc15_momentum_v1",
        market_ticker="KXBTC-TEST-001",
        decision_id="decision-enter",
        side_candidate="YES",
        economic_side="YES",
        opened_at=now,
        open_price=Decimal("0.63"),
        contract_count=1,
        boundary=Decimal("62000"),
        brti_at_entry=Decimal("62100"),
        distance_bps_at_entry=Decimal("16.1"),
        entry_reason="dry_run_entry_signal",
        status="OPEN",
        measurements={"desired_side_ask": "0.62"},
    )

    created = repository.insert_position_if_absent(position)
    duplicate = repository.insert_position_if_absent(position)
    repository.insert_event_if_absent(
        StrategyDryRunEventInput(
            event_id="dryrun-event-enter-1",
            event_type="ENTER_DRY_RUN",
            occurred_at=now,
            position_id=position.position_id,
            decision_id=position.decision_id,
            market_ticker=position.market_ticker,
            side_candidate="YES",
            price=Decimal("0.63"),
            contract_count=1,
            reason="dry_run_entry_signal",
            measurements={"desired_side_ask": "0.62"},
        )
    )
    repository.insert_event_if_absent(
        StrategyDryRunEventInput(
            event_id="dryrun-event-enter-1",
            event_type="ENTER_DRY_RUN",
            occurred_at=now,
        )
    )
    closed = repository.close_position(
        position_id=position.position_id,
        closed_at=now + timedelta(seconds=30),
        close_price=Decimal("0.73"),
        close_reason="dry_run_profit_target_reached",
        status="CLOSED",
        realized_pnl_cents=Decimal("10"),
        measurements={"desired_side_bid": "0.73"},
    )

    assert duplicate.id == created.id
    assert repository.count_open_positions(strategy_id="btc15_momentum_v1") == 0
    assert closed is not None
    assert closed.status == "CLOSED"
    assert repository.list_recent_events(limit=10)[0].event_id == "dryrun-event-enter-1"


def test_strategy_dry_run_events_remain_queryable_after_position_delete(session) -> None:
    repository = StrategyDryRunRepository(session)
    now = datetime.now(UTC)
    position = repository.insert_position_if_absent(
        StrategyDryRunPositionInput(
            position_id="dryrun-retained-event-position",
            strategy_id="btc15_momentum_v1",
            market_ticker="KXBTC-TEST-001",
            decision_id="decision-enter",
            side_candidate="YES",
            economic_side="YES",
            opened_at=now,
            open_price=Decimal("0.63"),
            contract_count=1,
            boundary=Decimal("62000"),
            brti_at_entry=Decimal("62100"),
            distance_bps_at_entry=Decimal("16.1"),
            entry_reason="dry_run_entry_signal",
            status="CLOSED",
            closed_at=now + timedelta(seconds=30),
            close_price=Decimal("0.73"),
            close_reason="dry_run_profit_target_reached",
            realized_pnl_cents=Decimal("10"),
            measurements={"desired_side_ask": "0.62"},
        )
    )
    repository.insert_event_if_absent(
        StrategyDryRunEventInput(
            event_id="dryrun-retained-event-enter",
            event_type="ENTER_DRY_RUN",
            occurred_at=now,
            position_id=position.position_id,
            decision_id=position.decision_id,
            market_ticker=position.market_ticker,
            side_candidate="YES",
            price=Decimal("0.63"),
            contract_count=1,
            reason="dry_run_entry_signal",
            measurements={"desired_side_ask": "0.62"},
        )
    )
    session.delete(position)
    session.flush()

    latest_event = repository.get_latest_event(strategy_id="btc15_momentum_v1")
    latest_enter_id = repository.get_latest_enter_decision_id(
        strategy_id="btc15_momentum_v1"
    )
    recent_events = repository.list_recent_events(
        limit=10,
        strategy_id="btc15_momentum_v1",
    )

    assert latest_event is not None
    assert latest_event.event_id == "dryrun-retained-event-enter"
    assert latest_event.strategy_id == "btc15_momentum_v1"
    assert latest_enter_id == "decision-enter"
    assert [event.event_id for event in recent_events] == [
        "dryrun-retained-event-enter"
    ]


def test_worker_heartbeat_repository_records_and_reads_latest(session) -> None:
    repository = WorkerHeartbeatRepository(session)
    now = datetime.now(UTC)

    repository.record_heartbeat(
        WorkerHeartbeatInput(
            service_name="ape-worker",
            started_at=now - timedelta(minutes=1),
            heartbeat_at=now,
            app_mode="OBSERVER",
            is_safe=True,
            metadata={"mode": "idle"},
        )
    )

    heartbeat = repository.get_latest_heartbeat("ape-worker")
    assert heartbeat is not None
    assert heartbeat.service_name == "ape-worker"
    assert heartbeat.is_safe is True
    assert heartbeat.metadata_ == {"mode": "idle"}


def test_kalshi_ws_protocol_repository_records_recent_and_summary(session) -> None:
    repository = KalshiWsProtocolEventRepository(session)
    now = datetime(2026, 7, 5, 12, 0, tzinfo=UTC)

    repository.insert_event(
        KalshiWsProtocolEventInput(
            event_type="subscribe_sent",
            created_at=now,
            worker_service="ape-worker.market_data",
            worker_role="market-data",
            connection_id="conn-1",
            channel="orderbook_delta",
            command_id=1,
            command_type="subscribe",
        )
    )
    repository.insert_event(
        KalshiWsProtocolEventInput(
            event_type="websocket_error",
            created_at=now + timedelta(seconds=1),
            worker_service="ape-worker.market_data",
            worker_role="market-data",
            connection_id="conn-1",
            raw_code="400",
            raw_message="Already subscribed",
        )
    )
    session.commit()

    recent = repository.list_recent(limit=10)
    summary = repository.summary_since(since=now - timedelta(seconds=5))

    assert [event.event_type for event in recent] == [
        "websocket_error",
        "subscribe_sent",
    ]
    assert summary["total"] == 2
    assert summary["error_count"] == 1
    assert summary["by_event_type"] == {
        "subscribe_sent": 1,
        "websocket_error": 1,
    }
