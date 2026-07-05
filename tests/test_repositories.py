from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.dialects import postgresql

from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.inputs import (
    MarketInput,
    OrderbookSnapshotInput,
    PublicTradeInput,
    ReferenceTickInput,
    StrategyDecisionInput,
    WorkerHeartbeatInput,
)
from ape.repositories.markets import MarketsRepository
from ape.repositories.orderbook import OrderbookRepository
from ape.repositories.public_trades import PublicTradesRepository, _recent_trades_statement
from ape.repositories.reference_ticks import ReferenceTicksRepository
from ape.repositories.strategy_decisions import StrategyDecisionsRepository
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
            raw_payload={"top": "book"},
        )
    )

    latest = repository.get_latest_snapshot("KXBTC-TEST-001")
    assert latest is not None
    assert latest.yes_bid == Decimal("49.00000000")
    assert repository.get_latest_snapshot_any().market_ticker == "KXBTC-TEST-001"


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
            count=3,
            taker_side="yes",
        )
    )

    recent = repository.get_recent_trades("KXBTC-TEST-001", limit=1)
    assert len(recent) == 1
    assert recent[0].trade_id == "trade-1"
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
