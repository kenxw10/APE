from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import event, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from ape.config import load_config
from ape.db.migrations import CURRENT_SCHEMA_VERSION, run_migrations
from ape.db.models import (
    Market,
    OrderbookSnapshot,
    PublicTrade,
    ReferenceTick,
    ResearchArchiveCursor,
    ResearchReplayEvent,
    StrategyFeatureSnapshot,
    StrategyPositionOutcome,
    StrategyTradeIntent,
)
from ape.db.session import create_engine_from_config, create_session_factory
from ape.research import archive as archive_module
from ape.research import service as research_service
from ape.research.archive import (
    ARCHIVE_BATCH_SIZE,
    ARCHIVE_BOOTSTRAP_WINDOW_SPAN,
    ARCHIVE_MAX_BATCHES_PER_CYCLE,
    ARCHIVE_SOURCE_STAGES,
    archive_research_batch,
    archive_research_source_pending,
)
from ape.research.service import ResearchWorker
from ape.research.status import build_research_status
from ape.safety import assess_startup_safety

AT = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
APPEND_ONLY_STAGES = (
    "reference_ticks",
    "orderbook_snapshots",
    "public_trades",
    "strategy_feature_snapshots",
    "strategy_trade_intents",
    "strategy_position_outcomes",
)


def _factory(tmp_path):
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'pr11c.sqlite'}",
            "APP_MODE": "DRY_RUN",
            "RESEARCH_ENABLED": "true",
            "CALIBRATION_ENABLED": "false",
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    return config, engine, create_session_factory(engine)


def _reference(*, row_id: int, received_at: datetime = AT) -> ReferenceTick:
    return ReferenceTick(
        id=row_id,
        source="kalshi_cfbenchmarks_brti",
        received_at=received_at,
        parsed_value=Decimal("62000"),
        parse_status="valid",
    )


def _archive_event_for_reference(row: ReferenceTick) -> ResearchReplayEvent:
    return ResearchReplayEvent(
        event_id=f"existing-reference-{row.id}",
        market_ticker=None,
        event_type="REFERENCE",
        event_time=row.received_at,
        received_at=row.received_at,
        source_table="reference_ticks",
        source_row_id=str(row.id),
        source_hash=f"existing-{row.id}",
        replay_schema_version="momentum_v2_replay_v1",
        payload={"parsed_value": "62000"},
        event_hash=f"existing-hash-{row.id}",
        replay_readiness="FULL",
        blockers=[],
    )


def _capture_sql(engine):
    statements: list[str] = []

    def capture(_connection, _cursor, statement, _parameters, _context, _executemany):
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", capture)
    return statements, capture


def test_append_only_production_selection_never_calls_the_legacy_selector(tmp_path, monkeypatch):
    _config, engine, factory = _factory(tmp_path)
    try:
        with factory() as session:
            session.add(_reference(row_id=1))
            session.commit()

            def forbidden(*_args, **_kwargs):
                raise AssertionError("append-only sources must use the cursor selector")

            monkeypatch.setattr(archive_module, "_unarchived_rows", forbidden)
            batch = archive_research_batch(session, source_stage="reference_ticks")

            assert batch.source_rows == 1
            assert batch.selector_mode == "BOOTSTRAP_VERIFY"
    finally:
        engine.dispose()


def test_bootstrap_anti_join_is_bounded_by_ids_and_250_rows(tmp_path):
    _config, engine, factory = _factory(tmp_path)
    statements, capture = _capture_sql(engine)
    try:
        with factory() as session:
            session.add(_reference(row_id=1))
            session.commit()
            statements.clear()
            batch = archive_research_batch(session, source_stage="reference_ticks")
            session.commit()

            bootstrap_sql = next(
                statement
                for statement in statements
                if "EXISTS" in statement and "reference_ticks" in statement
            )
            assert "reference_ticks.id >=" in bootstrap_sql
            assert "reference_ticks.id <=" in bootstrap_sql
            assert "LIMIT" in bootstrap_sql
            assert batch.source_rows == ARCHIVE_BATCH_SIZE or batch.source_rows == 1
    finally:
        event.remove(engine, "before_cursor_execute", capture)
        engine.dispose()


def test_tail_selection_is_keyset_bounded_and_has_no_anti_join(tmp_path):
    _config, engine, factory = _factory(tmp_path)
    statements, capture = _capture_sql(engine)
    try:
        with factory() as session:
            session.add(_reference(row_id=1))
            session.commit()
            archive_research_batch(session, source_stage="reference_ticks")
            session.commit()
            archive_research_batch(session, source_stage="reference_ticks")
            session.commit()
            session.add(_reference(row_id=2, received_at=AT + timedelta(seconds=1)))
            session.commit()

            statements.clear()
            batch = archive_research_batch(session, source_stage="reference_ticks")
            session.commit()
            tail_sql = next(
                statement
                for statement in statements
                if "FROM reference_ticks" in statement and "reference_ticks.id >" in statement
            )
            assert "NOT EXISTS" not in tail_sql
            assert "reference_ticks.id >" in tail_sql
            assert "ORDER BY reference_ticks.id ASC" in tail_sql
            assert "LIMIT" in tail_sql
            assert batch.selector_mode == "TAIL"
            assert batch.source_rows == 1
    finally:
        event.remove(engine, "before_cursor_execute", capture)
        engine.dispose()


def test_pending_probe_is_bounded_id_only_and_does_not_materialize_source_rows(
    tmp_path,
):
    _config, engine, factory = _factory(tmp_path)
    statements, capture = _capture_sql(engine)
    try:
        with factory() as session:
            session.add(_reference(row_id=1))
            session.commit()
            archive_research_batch(session, source_stage="reference_ticks")
            session.commit()
            archive_research_batch(session, source_stage="reference_ticks")
            session.commit()
            statements.clear()

            assert archive_research_source_pending(session, source_stage="reference_ticks") is False
            pending_sql = next(
                statement
                for statement in statements
                if "FROM reference_ticks" in statement
            )
            assert "SELECT reference_ticks.id" in pending_sql
            assert "NOT EXISTS" not in pending_sql
            assert "raw_payload" not in pending_sql
            assert "LIMIT" in pending_sql
    finally:
        event.remove(engine, "before_cursor_execute", capture)
        engine.dispose()


def test_bootstrap_repairs_gaps_below_high_archived_id_before_tail(tmp_path):
    _config, engine, factory = _factory(tmp_path)
    try:
        with factory() as session:
            rows = [_reference(row_id=row_id) for row_id in (1, 2, 3, 100)]
            session.add_all(rows)
            session.add(_archive_event_for_reference(rows[-1]))
            session.commit()

            first = archive_research_batch(session, source_stage="reference_ticks")
            session.commit()
            assert first.source_cursor == 3
            assert first.bootstrap_target == 100
            assert first.bootstrap_complete is False

            second = archive_research_batch(session, source_stage="reference_ticks")
            session.commit()
            assert second.source_rows == 0
            assert second.operation_performed is True
            cursor = session.get(ResearchArchiveCursor, "reference_ticks")
            assert cursor is not None
            assert cursor.selector_mode == "TAIL"
            assert cursor.source_cursor == 100
            assert set(session.scalars(
                select(ResearchReplayEvent.source_row_id).where(
                    ResearchReplayEvent.source_table == "reference_ticks"
                )
            ).all()) == {"100", "1", "2", "3"}
    finally:
        engine.dispose()


def test_rows_above_frozen_bootstrap_target_are_archived_by_tail(tmp_path):
    _config, engine, factory = _factory(tmp_path)
    try:
        with factory() as session:
            session.add(_reference(row_id=1))
            session.commit()
            first = archive_research_batch(session, source_stage="reference_ticks")
            session.commit()
            assert first.bootstrap_target == 1
            archive_research_batch(session, source_stage="reference_ticks")
            session.commit()
            session.add(_reference(row_id=2, received_at=AT + timedelta(seconds=1)))
            session.commit()
            tail = archive_research_batch(session, source_stage="reference_ticks")
            session.commit()
            assert tail.selector_mode == "TAIL"
            assert tail.source_rows == 1
            assert tail.source_cursor == 2
    finally:
        engine.dispose()


def test_empty_bootstrap_window_is_durable_work_and_counts_against_budget(
    tmp_path, monkeypatch
):
    config, engine, factory = _factory(tmp_path)
    monkeypatch.setattr(archive_module, "ARCHIVE_BOOTSTRAP_WINDOW_SPAN", 2)
    monkeypatch.setattr(research_service, "ARCHIVE_MAX_BATCHES_PER_CYCLE", 1)
    try:
        with factory() as session:
            session.add_all([_reference(row_id=1), _reference(row_id=3)])
            session.commit()
        worker = ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=AT,
        )
        first = worker.run_once()
        assert first["status"] == "partial"
        assert first["completed_archive_batches"] == 1
        assert first["archive_selector_mode"] == "BOOTSTRAP_VERIFY"
        assert first["archive_bootstrap_complete"] is False
    finally:
        engine.dispose()


def test_rollback_keeps_archive_rows_and_cursor_atomic_then_restart_resumes(tmp_path):
    _config, engine, factory = _factory(tmp_path)
    try:
        with factory() as session:
            session.add(_reference(row_id=1))
            session.commit()

        with factory() as session:
            archive_research_batch(session, source_stage="reference_ticks")
            session.rollback()

        with factory() as session:
            cursor = session.get(ResearchArchiveCursor, "reference_ticks")
            assert cursor is not None
            assert cursor.selector_mode == "UNINITIALIZED"
            assert session.scalar(select(ResearchReplayEvent.id)) is None

            committed = archive_research_batch(session, source_stage="reference_ticks")
            session.commit()
            assert committed.source_rows == 1

        with factory() as session:
            resumed = archive_research_batch(session, source_stage="reference_ticks")
            session.commit()
            cursor = session.get(ResearchArchiveCursor, "reference_ticks")
            assert resumed.operation_performed is True
            assert cursor is not None
            assert cursor.source_cursor == 1
            assert cursor.selector_mode == "TAIL"
    finally:
        engine.dispose()


def test_duplicate_retry_does_not_double_advance_cursor(tmp_path, monkeypatch):
    config, engine, factory = _factory(tmp_path)
    original = research_service.archive_research_batch
    attempts = 0
    try:
        with factory() as session:
            session.add(_reference(row_id=1))
            session.commit()

        def duplicate_once(session, *, source_stage: str):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise IntegrityError(
                    "INSERT research_replay_events",
                    {},
                    Exception("UNIQUE constraint failed: research_replay_events.source_table"),
                )
            return original(session, source_stage=source_stage)

        monkeypatch.setattr(research_service, "archive_research_batch", duplicate_once)
        monkeypatch.setattr(research_service.time, "sleep", lambda _seconds: None)
        worker = ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=AT,
        )
        batch = worker._archive_batch_with_retry("reference_ticks")
        with factory() as session:
            cursor = session.get(ResearchArchiveCursor, "reference_ticks")
            event_count = session.scalar(select(ResearchReplayEvent.id))
        assert attempts == 2
        assert batch.source_rows == 1
        assert cursor is not None
        assert cursor.source_cursor == 1
        assert event_count is not None
    finally:
        engine.dispose()


@pytest.mark.parametrize("source_stage", APPEND_ONLY_STAGES)
def test_all_append_only_sources_use_cursors_and_markets_remain_separate(tmp_path, source_stage):
    _config, engine, factory = _factory(tmp_path)
    try:
        with factory() as session:
            if source_stage == "reference_ticks":
                row = _reference(row_id=1)
            elif source_stage == "orderbook_snapshots":
                row = OrderbookSnapshot(id=1, market_ticker="M", received_at=AT)
            elif source_stage == "public_trades":
                row = PublicTrade(id=1, market_ticker="M", received_at=AT)
            elif source_stage == "strategy_feature_snapshots":
                row = StrategyFeatureSnapshot(
                    id=1,
                    feature_snapshot_id="feature-1",
                    evaluated_at=AT,
                    feature_schema_version="v1",
                    context_hash="context-1",
                )
            elif source_stage == "strategy_trade_intents":
                row = StrategyTradeIntent(
                    id=1,
                    intent_id="intent-1",
                    strategy_id="strategy",
                    decision_id="decision-1",
                    market_ticker="M",
                    side_candidate="YES",
                    action="ENTRY",
                    created_at=AT,
                    effective_after=AT,
                    expires_at=AT + timedelta(seconds=1),
                    intended_limit_price=Decimal("0.50"),
                    quantity=Decimal("1"),
                    status="PENDING",
                )
            else:
                row = StrategyPositionOutcome(
                    id=1,
                    outcome_id="outcome-1",
                    position_id="position-1",
                    strategy_id="strategy",
                    market_ticker="M",
                    held_side="YES",
                    lifecycle_version="v1",
                    opened_at=AT,
                    closed_at=AT + timedelta(seconds=1),
                    holding_duration_ms=1000,
                    quantity=Decimal("1"),
                    entry_price=Decimal("0.50"),
                    exit_price=Decimal("0.55"),
                    realized_pnl_cents=Decimal("5"),
                )
            session.add(row)
            session.commit()
            batch = archive_research_batch(session, source_stage=source_stage)
            session.commit()
            cursor = session.get(ResearchArchiveCursor, source_stage)
            assert batch.source_rows == 1
            assert cursor is not None
            assert cursor.selector_mode == "BOOTSTRAP_VERIFY"

            session.add(Market(id=100, market_ticker="M", series_ticker="KXBTC15M"))
            session.commit()
            market_batch = archive_research_batch(session, source_stage="markets")
            session.commit()
            assert market_batch.source_rows == 1
            assert session.get(ResearchArchiveCursor, "markets") is None
    finally:
        engine.dispose()


def test_timeout_is_sanitized_and_next_cycle_can_resume_cursor_archive(tmp_path, monkeypatch):
    config, engine, factory = _factory(tmp_path)
    original = research_service.archive_research_batch
    failed = False
    try:
        with factory() as session:
            session.add(_reference(row_id=1))
            session.commit()

        def fail_once(session, *, source_stage: str):
            nonlocal failed
            if source_stage == "reference_ticks" and not failed:
                failed = True
                raise SQLAlchemyError("canceling statement due to statement timeout")
            return original(session, source_stage=source_stage)

        monkeypatch.setattr(research_service, "archive_research_batch", fail_once)
        worker = ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=AT,
        )
        first = worker.run_once()
        status_after_failure = build_research_status(config)
        assert first["status"] == "error"
        assert status_after_failure["statement_timeout_detected"] is True

        second = worker.run_once()
        assert second["status"] == "completed"
        with factory() as session:
            assert session.scalar(select(ResearchReplayEvent.id)) is not None
            assert session.get(ResearchArchiveCursor, "reference_ticks") is not None
    finally:
        engine.dispose()


def test_pr11b_orchestration_and_operational_boundaries_remain_unchanged():
    assert ARCHIVE_SOURCE_STAGES == (
        "markets",
        *APPEND_ONLY_STAGES,
    )
    assert ARCHIVE_BATCH_SIZE == 250
    assert ARCHIVE_MAX_BATCHES_PER_CYCLE == 20
    assert ARCHIVE_BOOTSTRAP_WINDOW_SPAN == 10_000
    assert CURRENT_SCHEMA_VERSION == "0011_research_archive_cursors"
    assert "strategy" not in ARCHIVE_SOURCE_STAGES
