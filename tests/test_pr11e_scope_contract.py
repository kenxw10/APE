from datetime import UTC, datetime, timedelta

import pytest

from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.models import ResearchArchiveCursor, ResearchReplayEvent
from ape.db.session import create_engine_from_config, create_session_factory
from ape.research import service as research_service
from ape.research.archive import (
    APPEND_ONLY_ARCHIVE_SOURCE_STAGES,
    ARCHIVE_BATCH_SIZE,
    ARCHIVE_SOURCE_STAGES,
    ArchiveBatchResult,
    ArchiveResult,
    LabelRefreshResult,
    ReferenceAssociationResult,
    archive_bootstrap_required,
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
