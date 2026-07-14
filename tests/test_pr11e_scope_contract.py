from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.models import (
    Market,
    PublicTrade,
    ResearchArchiveCursor,
    ResearchMarketOutcome,
    ResearchReplayEvent,
    StrategyFeatureSnapshot,
    StrategyPositionOutcome,
    StrategyTradeIntent,
)
from ape.db.session import create_engine_from_config, create_session_factory
from ape.research import service as research_service
from ape.research.archive import (
    APPEND_ONLY_ARCHIVE_SOURCE_STAGES,
    ARCHIVE_BATCH_SIZE,
    ARCHIVE_CURSOR_SCHEMA_VERSION,
    ARCHIVE_MAX_BATCHES_PER_CYCLE,
    ARCHIVE_SOURCE_STAGES,
    ArchiveBatchResult,
    ArchiveResult,
    LabelRefreshResult,
    ReferenceAssociationResult,
    archive_bootstrap_required,
    archive_research_source_pending,
)
from ape.research.repository import ResearchRepository
from ape.research.status import build_research_status
from ape.safety import assess_startup_safety


def _config(tmp_path):
    return load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'pr11e.sqlite'}",
            "APP_MODE": "DRY_RUN",
            "RESEARCH_ENABLED": "true",
            "CALIBRATION_ENABLED": "false",
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )


def _factory(tmp_path):
    config = _config(tmp_path)
    engine = create_engine_from_config(config)
    run_migrations(engine)
    return config, engine, create_session_factory(engine)


def _seed_tail_cursors(session) -> None:
    for source_stage in APPEND_ONLY_ARCHIVE_SOURCE_STAGES:
        cursor = session.get(ResearchArchiveCursor, source_stage)
        assert cursor is not None
        cursor.selector_mode = "TAIL"
        cursor.source_cursor = 0
        cursor.schema_version = "research_archive_cursor_v1"
        cursor.bootstrap_complete = True
    session.commit()


def _batch(source_stage: str, *, source_rows: int = ARCHIVE_BATCH_SIZE) -> ArchiveBatchResult:
    event_type = {
        "markets": "MARKET",
        "reference_ticks": "REFERENCE",
        "orderbook_snapshots": "ORDERBOOK",
        "public_trades": "TRADE",
        "strategy_feature_snapshots": "FEATURE_SNAPSHOT",
        "strategy_trade_intents": "MARKET_LIFECYCLE",
        "strategy_position_outcomes": "MARKET_LIFECYCLE",
    }[source_stage]
    return ArchiveBatchResult(
        source_stage=source_stage,
        source_rows=source_rows,
        archived_events=source_rows,
        archived_by_type={event_type: source_rows},
        operation_performed=True,
        state_changed=True,
        selector_mode="TAIL",
        source_cursor=source_rows,
        bootstrap_complete=True,
    )


def _insert_public_trades(session, *, count: int, at: datetime, prefix: str) -> None:
    session.add_all(
        [
            PublicTrade(
                market_ticker="KXBTC15M-PR11E-INGEST",
                trade_id=f"{prefix}-{index}",
                received_at=at + timedelta(microseconds=index),
                executed_at=at + timedelta(microseconds=index),
                price=Decimal("0.50"),
                count=1,
                trade_count=Decimal("1"),
                taker_side="yes",
            )
            for index in range(count)
        ]
    )
    session.commit()


def _insert_label_and_lifecycle_rows(
    session,
    *,
    count: int,
    at: datetime,
    prefix: str,
) -> None:
    markets: list[Market] = []
    outcomes: list[ResearchMarketOutcome] = []
    features: list[StrategyFeatureSnapshot] = []
    intents: list[StrategyTradeIntent] = []
    position_outcomes: list[StrategyPositionOutcome] = []
    for index in range(count):
        ticker = f"KXBTC15M-{prefix}-{index:04d}"
        open_at = at - timedelta(minutes=30) + timedelta(seconds=index)
        close_at = at - timedelta(minutes=15) + timedelta(seconds=index)
        markets.append(
            Market(
                market_ticker=ticker,
                series_ticker="KXBTC15M",
                open_time=open_at,
                close_time=close_at,
                updated_at=at,
            )
        )
        outcomes.append(
            ResearchMarketOutcome(
                outcome_id=f"research-{prefix}-{index}",
                market_ticker=ticker,
                market_open_at=open_at,
                market_close_at=close_at,
                outcome_status="RESOLVED",
                result_side="YES",
                resolved_at=at,
            )
        )
        features.append(
            StrategyFeatureSnapshot(
                feature_snapshot_id=f"feature-{prefix}-{index}",
                market_ticker=ticker,
                evaluated_at=open_at,
                feature_schema_version="momentum_v2_features_v2",
                candidate_side="YES",
                candidate_mode="CONTINUATION",
                boundary=Decimal("1"),
                current_brti=Decimal("1"),
                seconds_since_open=600,
                seconds_left=300,
                context_hash=f"context-{prefix}-{index}",
                complete_feature_vector={"candidate_side": "YES"},
                replay_readiness="FULL",
            )
        )
        intents.append(
            StrategyTradeIntent(
                intent_id=f"intent-{prefix}-{index}",
                strategy_id="btc15_momentum_v1",
                strategy_config_version_id="config-test",
                feature_snapshot_id=f"feature-{prefix}-{index}",
                decision_id=f"decision-{prefix}-{index}",
                market_ticker=ticker,
                side_candidate="YES",
                action="ENTRY",
                created_at=open_at,
                effective_after=open_at,
                expires_at=close_at,
                intended_limit_price=Decimal("0.50"),
                quantity=Decimal("1"),
                status="PENDING",
            )
        )
        position_outcomes.append(
            StrategyPositionOutcome(
                outcome_id=f"position-outcome-{prefix}-{index}",
                position_id=f"position-{prefix}-{index}",
                strategy_id="btc15_momentum_v1",
                market_ticker=ticker,
                held_side="YES",
                lifecycle_version="v2",
                opened_at=open_at,
                closed_at=close_at,
                holding_duration_ms=1_000,
                quantity=Decimal("1"),
                entry_price=Decimal("0.50"),
                exit_price=Decimal("0.60"),
                realized_pnl_cents=Decimal("10"),
            )
        )
    session.add_all([*markets, *outcomes, *features, *intents, *position_outcomes])
    session.commit()


def _label_progress(session) -> int:
    return int(
        session.scalar(
            select(func.count())
            .select_from(ResearchMarketOutcome)
            .where(ResearchMarketOutcome.quality_flags.is_not(None))
        )
        or 0
    )


def _patch_fast_post_archive(monkeypatch, *, coverage_snapshots=None, replay_snapshots=None):
    coverage_snapshots = [] if coverage_snapshots is None else coverage_snapshots
    replay_snapshots = [] if replay_snapshots is None else replay_snapshots

    def coverage(_session, *, snapshot, **_kwargs):
        coverage_snapshots.append(snapshot)
        return {
            "frozen_snapshot": {
                "watermark_id": snapshot.watermark_id,
                "total_events": snapshot.event_count,
                "min_event_time": snapshot.min_event_time,
                "max_event_time": snapshot.max_event_time,
                "partition_count": snapshot.partition_count,
            }
        }

    def replay(*_args, **kwargs):
        replay_snapshots.append(kwargs["replay_snapshot"])
        return {"status": "completed"}

    monkeypatch.setattr(research_service, "archive_research_coverage", coverage)
    monkeypatch.setattr(research_service, "run_research_cycle", replay)
    return coverage_snapshots, replay_snapshots


def test_bootstrap_gate_requires_all_six_valid_tail_cursors(tmp_path):
    config, engine, factory = _factory(tmp_path)
    del config
    try:
        with factory() as session:
            assert archive_bootstrap_required(session) is True
            _seed_tail_cursors(session)
            assert archive_bootstrap_required(session) is False
            cursor = session.get(
                ResearchArchiveCursor, "strategy_trade_intents"
            )
            assert cursor is not None
            cursor.selector_mode = "BOOTSTRAP_VERIFY"
            assert archive_bootstrap_required(session) is True
            cursor.selector_mode = "TAIL"
            cursor.bootstrap_complete = False
            assert archive_bootstrap_required(session) is True
    finally:
        engine.dispose()


def test_bootstrap_gate_requires_exact_cursor_schema_version_across_restart(
    tmp_path,
):
    config, engine, factory = _factory(tmp_path)
    at = datetime(2026, 7, 14, tzinfo=UTC)
    try:
        with factory() as session:
            _seed_tail_cursors(session)
            cursor = session.get(ResearchArchiveCursor, "public_trades")
            assert cursor is not None
            cursor.schema_version = ""
            session.commit()

        worker = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        )
        assert worker._bootstrap_required() is True
        strict = worker._archive_stage(
            checked_at=at,
            cycle_started_at=at,
            progress_callback=lambda *_args, **_kwargs: None,
        )
        assert strict.scheduling_mode == "BOOTSTRAP_STRICT"
        assert strict.post_archive_allowed is False
        assert strict.post_archive_deferred_reason == "bootstrap_incomplete"

        with factory() as session:
            cursor = session.get(ResearchArchiveCursor, "public_trades")
            assert cursor is not None
            cursor.schema_version = "research_archive_cursor_v0"
            session.commit()

        fresh_worker = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        )
        assert fresh_worker._bootstrap_required() is True
        with factory() as session:
            cursor = session.get(ResearchArchiveCursor, "public_trades")
            assert cursor is not None
            cursor.schema_version = ARCHIVE_CURSOR_SCHEMA_VERSION
            session.commit()

        corrected_worker = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        )
        assert corrected_worker._bootstrap_required() is False
        fair = corrected_worker._archive_stage(
            checked_at=at,
            cycle_started_at=at,
            progress_callback=lambda *_args, **_kwargs: None,
        )
        assert fair.scheduling_mode == "TAIL_FAIR"
        assert fair.post_archive_allowed is True

        with factory() as session:
            cursor = session.get(ResearchArchiveCursor, "public_trades")
            assert cursor is not None
            cursor.schema_version = ""
            session.commit()
        deferred = worker.run_once()
        status = build_research_status(config)
        assert deferred["status"] == "partial"
        assert deferred["archive_scheduling_mode"] == "BOOTSTRAP_STRICT"
        assert deferred["post_archive_allowed"] is False
        assert deferred["post_archive_deferred_reason"] == "bootstrap_incomplete"
        assert "research_archive_bootstrap_incomplete" in deferred["warnings"]
        assert status["archive_scheduling_mode"] == "BOOTSTRAP_STRICT"
        assert status["post_archive_allowed"] is False
        assert status["post_archive_deferred_reason"] == "bootstrap_incomplete"
        assert "research_archive_bootstrap_incomplete" in status["warnings"]
        assert len(status["archive_sources_served"]) <= len(ARCHIVE_SOURCE_STAGES)
    finally:
        engine.dispose()


def test_bootstrap_strict_budget_gates_every_post_archive_stage(tmp_path, monkeypatch):
    config, engine, factory = _factory(tmp_path)
    try:
        worker = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=datetime(2026, 7, 14, tzinfo=UTC),
        )
        monkeypatch.setattr(worker, "_bootstrap_required", lambda: True)
        calls: list[str] = []

        def bounded_bootstrap_batch(source_stage: str):
            calls.append(source_stage)
            return _batch(source_stage)

        monkeypatch.setattr(worker, "_archive_batch_with_retry", bounded_bootstrap_batch)
        schedule = worker._archive_stage(
            checked_at=datetime.now(UTC),
            cycle_started_at=datetime.now(UTC),
            progress_callback=lambda *_args, **_kwargs: None,
        )

        assert schedule.scheduling_mode == "BOOTSTRAP_STRICT"
        assert schedule.budget_exhausted is True
        assert schedule.bootstrap_pending_after_budget is True
        assert schedule.post_archive_allowed is False
        assert schedule.post_archive_deferred_reason == "bootstrap_pending_after_budget"
        assert len(calls) == 20
        assert calls == ["markets"] * 20

        forbidden = {
            "association": "refresh_research_reference_associations",
            "labels": "refresh_research_archive_labels",
        }
        for name, attribute in forbidden.items():
            monkeypatch.setattr(
                research_service,
                attribute,
                lambda _session, _name=name: pytest.fail(
                    f"{_name} must remain gated during bootstrap"
                ),
            )
        result = worker.run_once()
        assert result["status"] == "partial"
        assert result["archive_scheduling_mode"] == "BOOTSTRAP_STRICT"
        assert result["post_archive_allowed"] is False
        assert "research_archive_bootstrap_budget_exhausted" in result["warnings"]
    finally:
        engine.dispose()


def test_tail_fair_scheduler_is_canonical_bounded_and_uses_remaining_budget(
    tmp_path, monkeypatch
):
    config, engine, factory = _factory(tmp_path)
    try:
        worker = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=datetime(2026, 7, 14, tzinfo=UTC),
        )
        monkeypatch.setattr(worker, "_bootstrap_required", lambda: False)
        remaining = {source_stage: 0 for source_stage in ARCHIVE_SOURCE_STAGES}
        remaining.update(
            {
                "markets": 1,
                "reference_ticks": 1,
                "orderbook_snapshots": 1,
                "public_trades": 20,
                "strategy_feature_snapshots": 2,
                "strategy_trade_intents": 1,
                "strategy_position_outcomes": 1,
            }
        )
        calls: list[tuple[str, int]] = []

        def fair_batch(source_stage: str):
            if remaining[source_stage] <= 0:
                return ArchiveBatchResult(
                    source_stage=source_stage,
                    source_rows=0,
                    archived_events=0,
                    archived_by_type={},
                )
            remaining[source_stage] -= 1
            calls.append((source_stage, ARCHIVE_BATCH_SIZE))
            return _batch(source_stage)

        monkeypatch.setattr(worker, "_archive_batch_with_retry", fair_batch)
        monkeypatch.setattr(
            research_service,
            "archive_research_source_pending",
            lambda _session, *, source_stage: remaining[source_stage] > 0,
        )
        schedule = worker._archive_stage(
            checked_at=datetime.now(UTC),
            cycle_started_at=datetime.now(UTC),
            progress_callback=lambda *_args, **_kwargs: None,
        )

        assert schedule.scheduling_mode == "TAIL_FAIR"
        assert schedule.post_archive_allowed is True
        assert schedule.tail_pending_after_budget is True
        assert schedule.budget_exhausted is True
        assert len(calls) == 20
        assert calls[:7] == [
            (source_stage, ARCHIVE_BATCH_SIZE)
            for source_stage in ARCHIVE_SOURCE_STAGES
        ]
        assert all(source_rows <= ARCHIVE_BATCH_SIZE for _, source_rows in calls)
        assert schedule.operations_by_source["public_trades"] > 1
        assert all(
            schedule.operations_by_source[source_stage] > 0
            for source_stage in ARCHIVE_SOURCE_STAGES
            if source_stage in {
                "markets",
                "reference_ticks",
                "orderbook_snapshots",
                "public_trades",
                "strategy_feature_snapshots",
                "strategy_trade_intents",
                "strategy_position_outcomes",
            }
        )
        assert list(schedule.sources_served) == list(ARCHIVE_SOURCE_STAGES)
    finally:
        engine.dispose()


def test_tail_budget_allows_post_archive_work_and_status_is_truthful(tmp_path, monkeypatch):
    config, engine, factory = _factory(tmp_path)
    try:
        worker = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=datetime(2026, 7, 14, tzinfo=UTC),
        )
        calls: list[str] = []
        schedule = research_service.ArchiveSchedulingResult(
            archive=ArchiveResult(0, {}, 0, {}),
            budget_exhausted=True,
            scheduling_mode="TAIL_FAIR",
            bootstrap_pending_after_budget=False,
            tail_pending_after_budget=True,
            sources_served=("public_trades", "strategy_feature_snapshots"),
            operations_by_source={source_stage: 0 for source_stage in ARCHIVE_SOURCE_STAGES},
            post_archive_allowed=True,
            post_archive_deferred_reason=None,
        )
        monkeypatch.setattr(worker, "_archive_stage", lambda **_kwargs: schedule)
        monkeypatch.setattr(
            research_service,
            "refresh_research_reference_associations",
            lambda _session: calls.append("association") or ReferenceAssociationResult(0, 0),
        )
        monkeypatch.setattr(
            research_service,
            "refresh_research_archive_labels",
            lambda _session: calls.append("labels") or LabelRefreshResult(1, 0, 0),
        )
        monkeypatch.setattr(
            research_service,
            "archive_research_coverage",
            lambda *_args, **_kwargs: {"frozen_snapshot": {"watermark_id": 0}},
        )
        monkeypatch.setattr(
            research_service,
            "run_research_cycle",
            lambda *_args, **_kwargs: calls.append("replay") or {"status": "completed"},
        )

        result = worker.run_once()
        status = build_research_status(config)

        assert calls == ["association", "labels", "replay"]
        assert result["status"] == "partial"
        assert result["current_stage"] == "complete"
        assert result["post_archive_allowed"] is True
        assert result["archive_tail_pending_after_budget"] is True
        assert "research_archive_tail_budget_exhausted" in result["warnings"]
        assert status["archive_scheduling_mode"] == "TAIL_FAIR"
        assert status["archive_tail_pending_after_budget"] is True
        assert status["post_archive_allowed"] is True
        assert "research_archive_tail_budget_exhausted" in status["warnings"]
        assert "research_archive_tail_budget_exhausted" not in status["blockers"]
    finally:
        engine.dispose()


def test_six_tail_cycles_continue_label_progress_under_continuous_ingest(
    tmp_path, monkeypatch
):
    config, engine, factory = _factory(tmp_path)
    try:
        worker = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=datetime(2026, 7, 14, tzinfo=UTC),
        )
        schedule = research_service.ArchiveSchedulingResult(
            archive=ArchiveResult(0, {}, 0, {}),
            budget_exhausted=True,
            scheduling_mode="TAIL_FAIR",
            bootstrap_pending_after_budget=False,
            tail_pending_after_budget=True,
            sources_served=("public_trades",),
            operations_by_source={source_stage: 1 for source_stage in ARCHIVE_SOURCE_STAGES},
            post_archive_allowed=True,
            post_archive_deferred_reason=None,
        )
        monkeypatch.setattr(worker, "_archive_stage", lambda **_kwargs: schedule)
        association_calls = 0
        label_calls = 0
        remaining = iter((5, 4, 3, 2, 1, 0))

        def association(_session):
            nonlocal association_calls
            association_calls += 1
            return ReferenceAssociationResult(1, 0)

        def labels(_session):
            nonlocal label_calls
            label_calls += 1
            remaining_markets = next(remaining)
            return LabelRefreshResult(1, remaining_markets, 0)

        monkeypatch.setattr(
            research_service,
            "refresh_research_reference_associations",
            association,
        )
        monkeypatch.setattr(research_service, "refresh_research_archive_labels", labels)
        monkeypatch.setattr(
            research_service,
            "archive_research_coverage",
            lambda *_args, **_kwargs: {"frozen_snapshot": {"watermark_id": 0}},
        )
        monkeypatch.setattr(
            research_service,
            "run_research_cycle",
            lambda *_args, **_kwargs: {"status": "completed"},
        )

        results = [worker.run_once() for _ in range(6)]

        assert association_calls == 6
        assert label_calls == 6
        assert all(result["post_archive_allowed"] is True for result in results)
        assert all(result["archive_tail_pending_after_budget"] is True for result in results)
        assert all(result["status"] == "partial" for result in results)
    finally:
        engine.dispose()


def test_real_worker_tail_fairness_preserves_labels_across_continuous_ingest(
    tmp_path, monkeypatch
):
    config, engine, factory = _factory(tmp_path)
    at = datetime(2026, 7, 14, tzinfo=UTC)
    original_batch = research_service.archive_research_batch
    archive_calls: list[tuple[int, str, int]] = []
    current_cycle = -1
    progress: list[int] = []
    post_archive_calls: list[str] = []
    try:
        with factory() as session:
            _seed_tail_cursors(session)
            _insert_label_and_lifecycle_rows(
                session,
                count=25,
                at=at,
                prefix="fair",
            )
            _insert_public_trades(
                session,
                count=ARCHIVE_BATCH_SIZE * ARCHIVE_MAX_BATCHES_PER_CYCLE * 6,
                at=at,
                prefix="initial",
            )

        def record_batch(session, *, source_stage: str):
            batch = original_batch(session, source_stage=source_stage)
            archive_calls.append((current_cycle, source_stage, batch.source_rows))
            return batch

        monkeypatch.setattr(research_service, "archive_research_batch", record_batch)
        coverage_snapshots, replay_snapshots = _patch_fast_post_archive(monkeypatch)
        original_association = research_service.refresh_research_reference_associations
        original_labels = research_service.refresh_research_archive_labels

        def record_association(session):
            post_archive_calls.append("association")
            return original_association(session)

        def record_labels(session):
            post_archive_calls.append("labels")
            return original_labels(session)

        monkeypatch.setattr(
            research_service,
            "refresh_research_reference_associations",
            record_association,
        )
        monkeypatch.setattr(research_service, "refresh_research_archive_labels", record_labels)

        worker = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        )
        results = []
        for current_cycle in range(6):
            with factory() as session:
                if current_cycle > 0:
                    _insert_label_and_lifecycle_rows(
                        session,
                        count=1,
                        at=at + timedelta(minutes=current_cycle + 1),
                        prefix=f"cycle-{current_cycle}",
                    )
                _insert_public_trades(
                    session,
                    count=1,
                    at=at + timedelta(minutes=current_cycle + 1),
                    prefix=f"cycle-{current_cycle}",
                )
            if current_cycle == 3:
                worker = research_service.ResearchWorker(
                    config=config,
                    safety=assess_startup_safety(config),
                    session_factory=factory,
                    started_at=at,
                )
            result = worker.run_once()
            results.append(result)
            operations = result["archive_operations_by_source"]
            assert sum(operations.values()) <= ARCHIVE_MAX_BATCHES_PER_CYCLE
            assert result["archive_scheduling_mode"] == "TAIL_FAIR"
            assert result["post_archive_allowed"] is True
            assert result["current_stage"] == "complete"
            with factory() as session:
                progress.append(_label_progress(session))
                assert archive_research_source_pending(
                    session, source_stage="public_trades"
                ) is True

        first_cycle_calls = [call for call in archive_calls if call[0] == 0]
        assert [stage for _, stage, _ in first_cycle_calls[: len(ARCHIVE_SOURCE_STAGES)]] == list(
            ARCHIVE_SOURCE_STAGES
        )
        assert sum(stage == "public_trades" for _, stage, _ in first_cycle_calls) > 1
        assert all(source_rows <= ARCHIVE_BATCH_SIZE for _, _, source_rows in archive_calls)
        assert results[0]["archive_operations_by_source"]["strategy_feature_snapshots"] > 0
        assert results[0]["archive_operations_by_source"]["strategy_trade_intents"] > 0
        assert results[0]["archive_operations_by_source"]["strategy_position_outcomes"] > 0
        assert all(result["archive_operations_by_source"] for result in results)
        assert post_archive_calls == [
            stage for _ in range(6) for stage in ("association", "labels")
        ]
        assert len(coverage_snapshots) == 6
        assert len(replay_snapshots) == 6
        assert progress == [25, 26, 27, 28, 29, 30]

        with factory() as session:
            source_models = {
                "markets": Market,
                "strategy_feature_snapshots": StrategyFeatureSnapshot,
                "strategy_trade_intents": StrategyTradeIntent,
                "strategy_position_outcomes": StrategyPositionOutcome,
            }
            for source_stage, model in source_models.items():
                event_ids = list(
                    session.scalars(
                        select(ResearchReplayEvent.source_row_id)
                        .where(ResearchReplayEvent.source_table == source_stage)
                        .order_by(ResearchReplayEvent.id.asc())
                    )
                )
                assert len(event_ids) == len(set(event_ids))
                source_count = session.scalar(select(func.count()).select_from(model))
                assert len(event_ids) == source_count
            public_event_ids = list(
                session.scalars(
                    select(ResearchReplayEvent.source_row_id).where(
                        ResearchReplayEvent.source_table == "public_trades"
                    )
                )
            )
            public_count = session.scalar(select(func.count()).select_from(PublicTrade))
            assert len(public_event_ids) == len(set(public_event_ids))
            assert 0 < len(public_event_ids) < public_count
    finally:
        engine.dispose()


def test_frozen_snapshot_excludes_events_inserted_after_watermark(tmp_path):
    config, engine, factory = _factory(tmp_path)
    del config
    at = datetime(2026, 7, 14, tzinfo=UTC)
    try:
        with factory() as session:
            session.add_all(
                [
                    ResearchReplayEvent(
                        event_id=f"event-{index}",
                        event_type="MARKET",
                        event_time=at + timedelta(seconds=index),
                        source_table="markets",
                        source_row_id=str(index),
                        replay_schema_version="research_replay_v1",
                        payload={},
                        event_hash=f"hash-{index}",
                        replay_readiness="FULL",
                    )
                    for index in (1, 2)
                ]
            )
            session.commit()
            repository = ResearchRepository(session)
            snapshot = repository.replay_event_snapshot()
            session.add(
                ResearchReplayEvent(
                    event_id="event-3",
                    event_type="MARKET",
                    event_time=at,
                    source_table="markets",
                    source_row_id="3",
                    replay_schema_version="research_replay_v1",
                    payload={},
                    event_hash="hash-3",
                    replay_readiness="FULL",
                )
            )
            session.commit()
            pages = list(repository.frozen_replay_event_reader(snapshot).iter_pages())

        records = [record for page in pages for record in page]
        assert snapshot.watermark_id == 2
        assert [record.source_row_id for record in records] == ["1", "2"]
    finally:
        engine.dispose()


def test_worker_freezes_current_cycle_snapshot_before_late_source_rows(
    tmp_path, monkeypatch
):
    config, engine, factory = _factory(tmp_path)
    at = datetime(2026, 7, 14, tzinfo=UTC)
    coverage_snapshots = []
    replay_snapshots = []
    replay_records: list[list[str]] = []
    late_ticker = "KXBTC15M-PR11E-LATE"
    late_inserted = False
    try:
        with factory() as session:
            _seed_tail_cursors(session)
            session.add(
                Market(
                    market_ticker="KXBTC15M-PR11E-FIRST",
                    series_ticker="KXBTC15M",
                    open_time=at - timedelta(minutes=30),
                    close_time=at - timedelta(minutes=15),
                    updated_at=at,
                )
            )
            session.commit()

        def coverage(session, *, snapshot, **_kwargs):
            nonlocal late_inserted
            coverage_snapshots.append(snapshot)
            if not late_inserted:
                session.add(
                    Market(
                        market_ticker=late_ticker,
                        series_ticker="KXBTC15M",
                        open_time=at - timedelta(minutes=30),
                        close_time=at - timedelta(minutes=15),
                        updated_at=at,
                    )
                )
                session.flush()
                late_inserted = True
            return {
                "frozen_snapshot": {
                    "watermark_id": snapshot.watermark_id,
                    "total_events": snapshot.event_count,
                    "min_event_time": snapshot.min_event_time,
                    "max_event_time": snapshot.max_event_time,
                    "partition_count": snapshot.partition_count,
                }
            }

        def replay(*args, **kwargs):
            snapshot = kwargs["replay_snapshot"]
            replay_snapshots.append(snapshot)
            session = args[1]
            records = [
                record
                for page in ResearchRepository(session)
                .frozen_replay_event_reader(snapshot)
                .iter_pages()
                for record in page
            ]
            replay_records.append([record.source_row_id for record in records])
            return {"status": "completed"}

        monkeypatch.setattr(research_service, "archive_research_coverage", coverage)
        monkeypatch.setattr(research_service, "run_research_cycle", replay)
        worker = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        )

        first = worker.run_once()
        second = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        ).run_once()

        assert first["post_archive_allowed"] is True
        assert second["post_archive_allowed"] is True
        assert coverage_snapshots == replay_snapshots
        assert [snapshot.watermark_id for snapshot in coverage_snapshots] == [1, 2]
        assert [snapshot.event_count for snapshot in coverage_snapshots] == [1, 2]
        assert all(
            snapshot.watermark_id == snapshot.event_count for snapshot in coverage_snapshots
        )
        assert replay_records[0] == ["1"]
        assert replay_records[1] == ["1", "2"]
        with factory() as session:
            late_market = session.scalar(
                select(Market).where(Market.market_ticker == late_ticker)
            )
            assert late_market is not None
            events = list(
                session.scalars(
                    select(ResearchReplayEvent)
                    .where(ResearchReplayEvent.source_table == "markets")
                    .order_by(ResearchReplayEvent.id.asc())
                )
            )
            assert [event.source_row_id for event in events] == ["1", "2"]
            assert archive_research_source_pending(
                session, source_stage="markets"
            ) is False
    finally:
        engine.dispose()


def test_post_archive_failure_preserves_committed_tail_cursor_and_event(
    tmp_path, monkeypatch
):
    config, engine, factory = _factory(tmp_path)
    at = datetime(2026, 7, 14, tzinfo=UTC)
    try:
        with factory() as session:
            _seed_tail_cursors(session)
            session.add(
                Market(
                    market_ticker="KXBTC15M-PR11E-FAILURE",
                    series_ticker="KXBTC15M",
                    open_time=at - timedelta(minutes=30),
                    close_time=at - timedelta(minutes=15),
                    updated_at=at,
                )
            )
            session.commit()
            _insert_public_trades(session, count=1, at=at, prefix="failure")

        _patch_fast_post_archive(monkeypatch)
        original_association = research_service.refresh_research_reference_associations

        def fail_association(_session):
            raise SQLAlchemyError("reference association stage failed")

        monkeypatch.setattr(
            research_service,
            "refresh_research_reference_associations",
            fail_association,
        )
        worker = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        )
        failed = worker.run_once()
        assert failed["status"] == "error"
        assert failed["blockers"] == ["research_database_error"]

        with factory() as session:
            events = list(
                session.scalars(
                    select(ResearchReplayEvent).where(
                        ResearchReplayEvent.source_table == "public_trades"
                    )
                )
            )
            cursor = session.get(ResearchArchiveCursor, "public_trades")
            assert cursor is not None
            assert len(events) == 1
            assert events[0].source_row_id == "1"
            assert cursor.source_cursor == 1

        monkeypatch.setattr(
            research_service,
            "refresh_research_reference_associations",
            original_association,
        )
        resumed = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        ).run_once()
        assert resumed["status"] == "completed"
        with factory() as session:
            events = list(
                session.scalars(
                    select(ResearchReplayEvent).where(
                        ResearchReplayEvent.source_table == "public_trades"
                    )
                )
            )
            assert len(events) == 1
            assert len({event.source_row_id for event in events}) == 1
    finally:
        engine.dispose()


def test_label_failure_preserves_prior_progress_and_resumes_remaining_batch(
    tmp_path, monkeypatch
):
    config, engine, factory = _factory(tmp_path)
    at = datetime(2026, 7, 14, tzinfo=UTC)
    try:
        with factory() as session:
            _seed_tail_cursors(session)
            _insert_label_and_lifecycle_rows(
                session,
                count=26,
                at=at,
                prefix="label-failure",
            )
            _insert_public_trades(
                session,
                count=1,
                at=at,
                prefix="label-first",
            )

        _patch_fast_post_archive(monkeypatch)
        worker = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        )
        first = worker.run_once()
        assert first["status"] == "partial"
        with factory() as session:
            prior_label_progress = _label_progress(session)
            prior_event_count = session.scalar(
                select(func.count())
                .select_from(ResearchReplayEvent)
                .where(ResearchReplayEvent.source_table == "public_trades")
            )
        assert prior_label_progress == 25
        assert prior_event_count == 1

        with factory() as session:
            _insert_public_trades(
                session,
                count=1,
                at=at + timedelta(minutes=1),
                prefix="label-second",
            )

        original_labels = research_service.refresh_research_archive_labels

        def fail_after_label_flush(session):
            original_labels(session)
            raise SQLAlchemyError("label transaction failed")

        monkeypatch.setattr(
            research_service,
            "refresh_research_archive_labels",
            fail_after_label_flush,
        )
        failed = worker.run_once()
        assert failed["status"] == "error"
        assert failed["blockers"] == ["research_database_error"]
        with factory() as session:
            assert _label_progress(session) == prior_label_progress
            failed_outcome = session.scalar(
                select(ResearchMarketOutcome)
                .where(ResearchMarketOutcome.quality_flags.is_(None))
                .order_by(ResearchMarketOutcome.id.desc())
            )
            assert failed_outcome is not None
            event_count_after_failure = session.scalar(
                select(func.count())
                .select_from(ResearchReplayEvent)
                .where(ResearchReplayEvent.source_table == "public_trades")
            )
            assert event_count_after_failure == 2

        monkeypatch.setattr(
            research_service,
            "refresh_research_archive_labels",
            original_labels,
        )
        resumed = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        ).run_once()
        assert resumed["status"] == "completed"
        with factory() as session:
            assert _label_progress(session) == 26
            public_events = list(
                session.scalars(
                    select(ResearchReplayEvent.source_row_id).where(
                        ResearchReplayEvent.source_table == "public_trades"
                    )
                )
            )
            assert len(public_events) == len(set(public_events)) == 2
    finally:
        engine.dispose()


def test_pr11e_scope_contract_keeps_runtime_constants_and_safety_flags():
    archive_source = open("src/ape/research/archive.py", encoding="utf-8").read()
    config_source = open("src/ape/config.py", encoding="utf-8").read()
    assert "ARCHIVE_BATCH_SIZE = 250" in archive_source
    assert "ARCHIVE_MAX_BATCHES_PER_CYCLE = 20" in archive_source
    assert "0012" not in archive_source
    assert "APP_MODE=DRY_RUN" not in config_source
    assert APPEND_ONLY_ARCHIVE_SOURCE_STAGES == (
        "reference_ticks",
        "orderbook_snapshots",
        "public_trades",
        "strategy_feature_snapshots",
        "strategy_trade_intents",
        "strategy_position_outcomes",
    )
