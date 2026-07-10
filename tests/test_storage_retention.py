from __future__ import annotations

import inspect
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.models import (
    KalshiWsProtocolEvent,
    Market,
    OrderbookSnapshot,
    PublicTrade,
    ReferenceTick,
    StorageRetentionRun,
    StrategyDecision,
    StrategyDryRunPosition,
    WorkerHeartbeat,
)
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.inputs import (
    KalshiWsProtocolEventInput,
    MarketInput,
    OrderbookSnapshotInput,
    PublicTradeInput,
    ReferenceTickInput,
    StrategyDecisionInput,
    StrategyDryRunPositionInput,
    WorkerHeartbeatInput,
)
from ape.repositories.kalshi_ws_protocol import KalshiWsProtocolEventRepository
from ape.repositories.markets import MarketsRepository
from ape.repositories.orderbook import OrderbookRepository
from ape.repositories.public_trades import PublicTradesRepository
from ape.repositories.reference_ticks import ReferenceTicksRepository
from ape.repositories.storage_retention import StorageRetentionRepository
from ape.repositories.strategy_decisions import StrategyDecisionsRepository
from ape.repositories.strategy_dry_run import StrategyDryRunRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.safety import assess_startup_safety
from ape.storage import retention as retention_module
from ape.storage.retention import (
    RETENTION_SUCCESS,
    RETENTION_SUCCESS_PARTIAL,
    StorageRetentionWorker,
    run_storage_retention_once,
)


@pytest.fixture
def retention_db(tmp_path):
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_storage_retention.sqlite'}"
    config = load_config({"DATABASE_URL": database_url})
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    try:
        yield database_url, session_factory
    finally:
        engine.dispose()


def test_storage_retention_deletes_old_rows_in_chunks(retention_db) -> None:
    database_url, session_factory = retention_db
    now = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    config = _retention_config(database_url, batch_size=1)

    with session_factory() as session:
        _insert_all_retained_tables(session, now)
        session.commit()

    result = run_storage_retention_once(config, session_factory, now=lambda: now)

    assert result.status == RETENTION_SUCCESS
    assert result.deleted_rows["orderbook_snapshots"] == 1
    assert result.deleted_rows["public_trades"] == 1
    assert result.deleted_rows["reference_ticks"] == 1
    assert result.deleted_rows["worker_heartbeats"] == 1
    assert result.deleted_rows["strategy_decisions"] == 1
    assert result.deleted_rows["kalshi_ws_protocol_events"] == 1
    assert result.deleted_rows["markets"] == 2

    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(OrderbookSnapshot)) == 1
        assert session.scalar(select(func.count()).select_from(PublicTrade)) == 1
        assert session.scalar(select(func.count()).select_from(ReferenceTick)) == 1
        assert session.scalar(select(func.count()).select_from(WorkerHeartbeat)) == 1
        assert session.scalar(select(func.count()).select_from(StrategyDecision)) == 1
        assert session.scalar(select(func.count()).select_from(KalshiWsProtocolEvent)) == 1
        assert session.scalar(select(func.count()).select_from(Market)) == 1
        audit_row = session.scalar(
            select(StorageRetentionRun).where(StorageRetentionRun.run_id == result.run_id)
        )
        assert audit_row is not None
        assert audit_row.status == RETENTION_SUCCESS
        assert audit_row.deleted_rows["orderbook_snapshots"] == 1
        assert audit_row.deleted_rows["kalshi_ws_protocol_events"] == 1
        assert audit_row.deleted_rows["markets"] == 2


def test_storage_retention_keeps_open_dry_run_positions(retention_db) -> None:
    database_url, session_factory = retention_db
    now = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    config = _retention_config(database_url, row_seconds=60)

    with session_factory() as session:
        repository = StrategyDryRunRepository(session)
        repository.insert_position_if_absent(
            StrategyDryRunPositionInput(
                position_id="dryrun-open-old",
                strategy_id="btc15_momentum_v1",
                market_ticker="KXBTC15M-OPEN",
                decision_id="decision-open",
                side_candidate="YES",
                economic_side="YES",
                opened_at=now - timedelta(seconds=120),
                open_price=Decimal("0.63"),
                contract_count=1,
                entry_reason="dry_run_entry_signal",
                status="OPEN",
            )
        )
        repository.insert_position_if_absent(
            StrategyDryRunPositionInput(
                position_id="dryrun-closed-old",
                strategy_id="btc15_momentum_v1",
                market_ticker="KXBTC15M-CLOSED",
                decision_id="decision-closed",
                side_candidate="YES",
                economic_side="YES",
                opened_at=now - timedelta(seconds=180),
                open_price=Decimal("0.63"),
                contract_count=1,
                entry_reason="dry_run_entry_signal",
                status="CLOSED",
                closed_at=now - timedelta(seconds=120),
                close_price=Decimal("0.73"),
                close_reason="dry_run_profit_target_reached",
                realized_pnl_cents=Decimal("10"),
            )
        )
        session.commit()

    result = run_storage_retention_once(config, session_factory, now=lambda: now)

    assert result.deleted_rows["strategy_dry_run_positions"] == 1
    with session_factory() as session:
        positions = list(session.scalars(select(StrategyDryRunPosition)))
        assert len(positions) == 1
        assert positions[0].position_id == "dryrun-open-old"
        assert positions[0].status == "OPEN"


def test_storage_retention_strips_raw_payload_without_losing_normalized_fields(
    retention_db,
) -> None:
    database_url, session_factory = retention_db
    now = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    config = _retention_config(database_url, row_seconds=300, raw_seconds=30)

    with session_factory() as session:
        OrderbookRepository(session).insert_snapshot(
            OrderbookSnapshotInput(
                market_ticker="KXBTC15M-RAW",
                received_at=now - timedelta(seconds=60),
                yes_bid=Decimal("48"),
                raw_payload_hash="hash-raw",
                raw_payload={"book": "large"},
            )
        )
        session.commit()

    result = run_storage_retention_once(config, session_factory, now=lambda: now)

    assert result.raw_payload_stripped_rows["orderbook_snapshots"] == 1
    with session_factory() as session:
        snapshot = session.scalar(select(OrderbookSnapshot))
        assert snapshot is not None
        assert snapshot.yes_bid == Decimal("48.00000000")
        assert snapshot.raw_payload_hash == "hash-raw"
        assert snapshot.raw_payload is None
        audit_row = StorageRetentionRepository(session).get_latest_run()
        assert audit_row is not None
        assert audit_row.raw_payload_stripped_rows["orderbook_snapshots"] == 1


def test_storage_retention_dry_run_reports_counts_without_mutating(retention_db) -> None:
    database_url, session_factory = retention_db
    now = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    config = _retention_config(database_url, dry_run=True)

    with session_factory() as session:
        OrderbookRepository(session).insert_snapshot(
            OrderbookSnapshotInput(
                market_ticker="KXBTC15M-OLD",
                received_at=now - timedelta(seconds=120),
                raw_payload={"book": "old"},
            )
        )
        session.commit()

    result = run_storage_retention_once(config, session_factory, now=lambda: now)

    assert result.dry_run is True
    assert result.deleted_rows["orderbook_snapshots"] == 1
    assert result.raw_payload_stripped_rows["orderbook_snapshots"] == 1
    with session_factory() as session:
        snapshot = session.scalar(select(OrderbookSnapshot))
        assert snapshot is not None
        assert snapshot.raw_payload == {"book": "old"}


def test_storage_retention_max_duration_stops_before_chunks(
    retention_db,
    monkeypatch,
) -> None:
    database_url, session_factory = retention_db
    now = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    config = _retention_config(database_url, max_run_seconds=1)
    monotonic_values = iter([0.0, 2.0, 2.0, 2.0])
    monkeypatch.setattr(retention_module, "monotonic", lambda: next(monotonic_values))

    with session_factory() as session:
        OrderbookRepository(session).insert_snapshot(
            OrderbookSnapshotInput(
                market_ticker="KXBTC15M-OLD",
                received_at=now - timedelta(seconds=120),
            )
        )
        session.commit()

    result = run_storage_retention_once(config, session_factory, now=lambda: now)

    assert result.status == RETENTION_SUCCESS_PARTIAL
    assert result.budget_exhausted is True
    assert "storage_retention_max_run_seconds_reached" in result.warnings
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(OrderbookSnapshot)) == 1


def test_storage_retention_limits_tables_per_run(retention_db) -> None:
    database_url, session_factory = retention_db
    now = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    config = _retention_config(database_url, max_tables_per_run=1)

    with session_factory() as session:
        _insert_all_retained_tables(session, now)
        session.commit()

    result = run_storage_retention_once(config, session_factory, now=lambda: now)

    assert result.status == RETENTION_SUCCESS_PARTIAL
    assert result.tables_processed == ["orderbook_snapshots"]
    assert "public_trades" in result.tables_skipped
    assert "storage_retention_max_tables_per_run_reached" in result.warnings
    with session_factory() as session:
        latest = StorageRetentionRepository(session).get_latest_run()
        assert latest is not None
        assert list(latest.table_row_counts_before.keys()) == ["orderbook_snapshots"]


def test_storage_retention_caps_delete_rows_per_table(retention_db) -> None:
    database_url, session_factory = retention_db
    now = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    config = _retention_config(
        database_url,
        batch_size=5,
        max_delete_rows_per_table=1,
        raw_seconds=1,
    )

    with session_factory() as session:
        for index in range(3):
            OrderbookRepository(session).insert_snapshot(
                OrderbookSnapshotInput(
                    market_ticker=f"KXBTC15M-OLD-{index}",
                    received_at=now - timedelta(seconds=120),
                    raw_payload={"book": "old"},
                )
            )
        session.commit()

    result = run_storage_retention_once(config, session_factory, now=lambda: now)

    assert result.deleted_rows["orderbook_snapshots"] == 1
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(OrderbookSnapshot)) == 2


def test_storage_retention_uses_configured_smoothing_sleeps(
    retention_db,
    monkeypatch,
) -> None:
    database_url, session_factory = retention_db
    now = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    config = _retention_config(
        database_url,
        batch_size=1,
        inter_table_sleep_ms=20,
        batch_sleep_ms=10,
    )
    sleeps: list[float] = []
    monkeypatch.setattr(retention_module, "sleep", lambda seconds: sleeps.append(seconds))

    with session_factory() as session:
        OrderbookRepository(session).insert_snapshot(
            OrderbookSnapshotInput(
                market_ticker="KXBTC15M-OLD",
                received_at=now - timedelta(seconds=120),
            )
        )
        session.commit()

    result = run_storage_retention_once(config, session_factory, now=lambda: now)

    assert result.status == RETENTION_SUCCESS
    assert 0.01 in sleeps
    assert 0.02 in sleeps


def test_failed_storage_retention_run_records_safe_error(
    retention_db,
    monkeypatch,
) -> None:
    database_url, session_factory = retention_db
    now = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    config = _retention_config(database_url)

    def fail_row_counts(self, table_names):
        raise RuntimeError("synthetic failure with no secrets")

    monkeypatch.setattr(StorageRetentionRepository, "row_counts", fail_row_counts)

    result = run_storage_retention_once(config, session_factory, now=lambda: now)

    assert result.status == "failed"
    assert result.error_type == "RuntimeError"
    with session_factory() as session:
        latest = StorageRetentionRepository(session).get_latest_run()
        assert latest is not None
        assert latest.status == "failed"
        assert latest.error_message == "synthetic failure with no secrets"


def test_storage_retention_repository_uses_bounded_cte_mutations() -> None:
    delete_source = inspect.getsource(StorageRetentionRepository.delete_batch)
    update_source = inspect.getsource(StorageRetentionRepository.strip_raw_payload_batch)

    assert "WITH doomed AS" in delete_source
    assert "LIMIT :batch_size" in delete_source
    assert "RETURNING id" in delete_source
    assert "WITH targets AS" in update_source
    assert "LIMIT :batch_size" in update_source
    assert "RETURNING id" in update_source


def test_postgres_raw_payload_count_uses_catalog_estimate() -> None:
    session = _RecordingPostgresSession(value=42)
    repository = StorageRetentionRepository(session)  # type: ignore[arg-type]

    assert repository.raw_payload_non_null_count("orderbook_snapshots") == 42
    sql = " ".join(session.statements)
    assert "pg_stats" in sql
    assert "reltuples" in sql
    assert "COUNT(*)" not in sql
    assert "raw_payload IS NOT NULL" not in sql


def test_storage_retention_worker_records_heartbeat_metadata(retention_db) -> None:
    database_url, session_factory = retention_db
    now = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    config = _retention_config(database_url)
    worker = StorageRetentionWorker(
        config=config,
        safety=assess_startup_safety(config),
        session_factory=session_factory,
        started_at=now,
        now=lambda: now,
    )
    stop_event = threading_event()

    import asyncio

    asyncio.run(worker.run(stop_event=stop_event, max_iterations=1))

    with session_factory() as session:
        heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat("ape-worker")
        assert heartbeat is not None
        assert heartbeat.metadata_["storage"]["retention"]["enabled"] is True
        assert heartbeat.metadata_["storage"]["retention"]["worker_role"] == "maintenance"
        assert heartbeat.metadata_["storage"]["retention"]["last_status"] == "success"
        component = WorkerHeartbeatRepository(session).get_latest_heartbeat(
            "ape-worker.maintenance"
        )
        assert component is not None
        assert component.metadata_["mode"] == "storage_retention"
        assert component.metadata_["storage"]["retention"]["enabled"] is True


def test_storage_retention_worker_offloads_run_from_event_loop(
    retention_db,
    monkeypatch,
) -> None:
    database_url, session_factory = retention_db
    now = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    config = _retention_config(database_url)
    called: dict[str, bool] = {}

    async def fake_to_thread(func, *args, **kwargs):
        called["used"] = True
        return func(*args, **kwargs)

    monkeypatch.setattr(retention_module.asyncio, "to_thread", fake_to_thread)
    worker = StorageRetentionWorker(
        config=config,
        safety=assess_startup_safety(config),
        session_factory=session_factory,
        started_at=now,
        now=lambda: now,
    )

    import asyncio

    asyncio.run(worker.run(stop_event=threading_event(), max_iterations=1))

    assert called["used"] is True


def _insert_all_retained_tables(session, now: datetime) -> None:
    OrderbookRepository(session).insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker="KXBTC15M-OLD",
            received_at=now - timedelta(seconds=120),
            raw_payload={"book": "old"},
        )
    )
    OrderbookRepository(session).insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker="KXBTC15M-RECENT",
            received_at=now - timedelta(seconds=10),
            raw_payload={"book": "recent"},
        )
    )
    PublicTradesRepository(session).insert_trade(
        PublicTradeInput(
            market_ticker="KXBTC15M-OLD",
            received_at=now - timedelta(seconds=120),
            executed_at=now - timedelta(seconds=119),
            raw_payload={"trade": "old"},
        )
    )
    PublicTradesRepository(session).insert_trade(
        PublicTradeInput(
            market_ticker="KXBTC15M-RECENT",
            received_at=now - timedelta(seconds=10),
            executed_at=now - timedelta(seconds=9),
            raw_payload={"trade": "recent"},
        )
    )
    ReferenceTicksRepository(session).insert_tick(
        ReferenceTickInput(
            source="kalshi_cfbenchmarks_brti",
            received_at=now - timedelta(seconds=120),
            parse_status="valid",
            raw_payload={"value": "old"},
        )
    )
    ReferenceTicksRepository(session).insert_tick(
        ReferenceTickInput(
            source="kalshi_cfbenchmarks_brti",
            received_at=now - timedelta(seconds=10),
            parse_status="valid",
            raw_payload={"value": "recent"},
        )
    )
    WorkerHeartbeatRepository(session).record_heartbeat(
        WorkerHeartbeatInput(
            service_name="ape-worker",
            heartbeat_at=now - timedelta(seconds=120),
            app_mode="OBSERVER",
            is_safe=True,
        )
    )
    WorkerHeartbeatRepository(session).record_heartbeat(
        WorkerHeartbeatInput(
            service_name="ape-worker",
            heartbeat_at=now - timedelta(seconds=10),
            app_mode="OBSERVER",
            is_safe=True,
        )
    )
    StrategyDecisionsRepository(session).insert_decision(
        StrategyDecisionInput(
            decision_id="old-decision",
            evaluated_at=now - timedelta(seconds=120),
            decision_state="OBSERVE_ONLY_MARKET",
            primary_reason="old",
            app_mode="OBSERVER",
        )
    )
    StrategyDecisionsRepository(session).insert_decision(
        StrategyDecisionInput(
            decision_id="recent-decision",
            evaluated_at=now - timedelta(seconds=10),
            decision_state="OBSERVE_ONLY_MARKET",
            primary_reason="recent",
            app_mode="OBSERVER",
        )
    )
    protocol_repository = KalshiWsProtocolEventRepository(session)
    protocol_repository.insert_event(
        KalshiWsProtocolEventInput(
            created_at=now - timedelta(seconds=120),
            event_type="orderbook_delta_received",
            worker_service="ape-worker.market_data",
            worker_role="market-data",
        )
    )
    protocol_repository.insert_event(
        KalshiWsProtocolEventInput(
            created_at=now - timedelta(seconds=10),
            event_type="orderbook_delta_received",
            worker_service="ape-worker.market_data",
            worker_role="market-data",
        )
    )
    MarketsRepository(session).upsert_market(
        MarketInput(
            market_ticker="KXBTC15M-OLD",
            open_time=now - timedelta(seconds=240),
            close_time=now - timedelta(seconds=120),
        )
    )
    MarketsRepository(session).upsert_market(
        MarketInput(
            market_ticker="KXBTC15M-ACTIVE",
            open_time=now - timedelta(seconds=60),
            close_time=now + timedelta(seconds=60),
        )
    )
    MarketsRepository(session).upsert_market(
        MarketInput(
            market_ticker="KXBTC15M-NO-CLOSE",
            open_time=now - timedelta(seconds=240),
            close_time=None,
        )
    )
    no_close_market = session.scalar(
        select(Market).where(Market.market_ticker == "KXBTC15M-NO-CLOSE")
    )
    no_close_market.updated_at = now - timedelta(seconds=120)


def _retention_config(
    database_url: str,
    *,
    batch_size: int = 50,
    row_seconds: int = 60,
    raw_seconds: int = 30,
    dry_run: bool = False,
    max_run_seconds: int = 20,
    inter_table_sleep_ms: int = 0,
    batch_sleep_ms: int = 0,
    max_tables_per_run: int | None = None,
    max_delete_rows_per_table: int | None = None,
):
    env = {
        "DATABASE_URL": database_url,
        "STORAGE_RETENTION_ENABLED": "true",
        "STORAGE_RETENTION_BATCH_SIZE": str(batch_size),
        "STORAGE_RETENTION_MAX_RUN_SECONDS": str(max_run_seconds),
        "STORAGE_RETENTION_DRY_RUN": str(dry_run).lower(),
        "STORAGE_RETENTION_INTER_TABLE_SLEEP_MS": str(inter_table_sleep_ms),
        "STORAGE_RETENTION_BATCH_SLEEP_MS": str(batch_sleep_ms),
        "STORAGE_RETENTION_ORDERBOOK_SECONDS": str(row_seconds),
        "STORAGE_RETENTION_PUBLIC_TRADES_SECONDS": str(row_seconds),
        "STORAGE_RETENTION_REFERENCE_TICKS_SECONDS": str(row_seconds),
        "STORAGE_RETENTION_WORKER_HEARTBEATS_SECONDS": str(row_seconds),
        "STORAGE_RETENTION_STRATEGY_DECISIONS_SECONDS": str(row_seconds),
        "STORAGE_RETENTION_KALSHI_WS_PROTOCOL_EVENTS_SECONDS": str(row_seconds),
        "STORAGE_RETENTION_DRY_RUN_POSITIONS_SECONDS": str(row_seconds),
        "STORAGE_RETENTION_DRY_RUN_EVENTS_SECONDS": str(row_seconds),
        "STORAGE_RETENTION_MARKETS_SECONDS": str(row_seconds),
        "STORAGE_RETENTION_RAW_PAYLOAD_ORDERBOOK_SECONDS": str(raw_seconds),
        "STORAGE_RETENTION_RAW_PAYLOAD_PUBLIC_TRADES_SECONDS": str(raw_seconds),
        "STORAGE_RETENTION_RAW_PAYLOAD_REFERENCE_TICKS_SECONDS": str(raw_seconds),
    }
    if max_tables_per_run is not None:
        env["STORAGE_RETENTION_MAX_TABLES_PER_RUN"] = str(max_tables_per_run)
    if max_delete_rows_per_table is not None:
        env["STORAGE_RETENTION_MAX_DELETE_ROWS_PER_TABLE"] = str(
            max_delete_rows_per_table
        )
    return load_config(env)


def threading_event():
    import threading

    return threading.Event()


class _RecordingPostgresSession:
    bind = type(
        "Bind",
        (),
        {"dialect": type("Dialect", (), {"name": "postgresql"})()},
    )()

    def __init__(self, value: int) -> None:
        self.value = value
        self.statements: list[str] = []

    def scalar(self, statement, parameters=None):
        self.statements.append(str(statement))
        return self.value
