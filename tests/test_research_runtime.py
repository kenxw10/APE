from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from threading import Event, Thread
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.models import (
    Market,
    ResearchArchiveCursor,
    ResearchReplayEvent,
    ResearchReplayRun,
    WorkerHeartbeat,
)
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.inputs import WorkerHeartbeatInput
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.research import service as research_service
from ape.research.archive import (
    ARCHIVE_BATCH_SIZE,
    ARCHIVE_SOURCE_STAGES,
    archive_research_batch,
)
from ape.research.repository import (
    REPLAY_EVENT_PAGE_SIZE,
    FrozenReplayEventReader,
    ResearchRepository,
)
from ape.research.status import build_research_status
from ape.safety import assess_startup_safety
from ape.worker.services import WORKER_SERVICE_RESEARCH


def _runtime_config(tmp_path, *, calibration_enabled: bool = False):
    return load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'research-runtime.sqlite'}",
            "APP_MODE": "DRY_RUN",
            "RESEARCH_ENABLED": "true",
            "CALIBRATION_ENABLED": str(calibration_enabled).lower(),
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )


def _insert_markets(session, *, count: int, at: datetime) -> None:
    session.add_all(
        [
            Market(
                market_ticker=f"KXBTC15M-RUNTIME-{index:04d}",
                series_ticker="KXBTC15M",
                updated_at=at + timedelta(microseconds=index),
            )
            for index in range(count)
        ]
    )
    session.commit()


def test_archive_batches_are_bounded_and_resume_without_duplicates(tmp_path) -> None:
    config = _runtime_config(tmp_path)
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    try:
        with factory() as session:
            _insert_markets(session, count=ARCHIVE_BATCH_SIZE + 1, at=at)

        with factory() as session:
            first = archive_research_batch(session, source_stage="markets")
            session.commit()
        with factory() as session:
            second = archive_research_batch(session, source_stage="markets")
            session.commit()
        with factory() as session:
            empty = archive_research_batch(session, source_stage="markets")
            stored = session.scalar(
                select(func.count())
                .select_from(ResearchReplayEvent)
                .where(ResearchReplayEvent.source_table == "markets")
            )

        assert first.source_rows == ARCHIVE_BATCH_SIZE
        assert second.source_rows == 1
        assert empty.source_rows == 0
        assert stored == ARCHIVE_BATCH_SIZE + 1
    finally:
        engine.dispose()


def test_research_worker_archives_1001_rows_in_exact_resumable_batches(
    tmp_path, monkeypatch
) -> None:
    config = _runtime_config(tmp_path)
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    original_batch = research_service.archive_research_batch
    batches: list[int] = []
    try:
        with factory() as session:
            _insert_markets(session, count=1001, at=at)

        def record_market_batches(session, *, source_stage: str):
            batch = original_batch(session, source_stage=source_stage)
            if source_stage == "markets" and batch.source_rows:
                batches.append(batch.source_rows)
            return batch

        monkeypatch.setattr(research_service, "archive_research_batch", record_market_batches)
        result = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        ).run_once()

        with factory() as session:
            stored = session.scalar(
                select(func.count())
                .select_from(ResearchReplayEvent)
                .where(ResearchReplayEvent.source_table == "markets")
            )
            distinct_rows = session.scalar(
                select(func.count(func.distinct(ResearchReplayEvent.source_row_id))).where(
                    ResearchReplayEvent.source_table == "markets"
                )
            )

        assert result["status"] == "completed"
        assert batches == [250, 250, 250, 250, 1]
        assert stored == 1001
        assert distinct_rows == 1001
    finally:
        engine.dispose()


def test_research_archive_uses_one_bulk_flush_per_bounded_batch(tmp_path, monkeypatch) -> None:
    config = _runtime_config(tmp_path)
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    original_archive_events_batch = ResearchRepository.archive_events_batch
    persisted_batch_sizes: list[int] = []
    try:
        with factory() as session:
            _insert_markets(session, count=1001, at=at)

        def record_bulk_flush(self, values):
            if values and values[0]["source_table"] == "markets":
                persisted_batch_sizes.append(len(values))
            return original_archive_events_batch(self, values)

        monkeypatch.setattr(
            ResearchRepository,
            "archive_events_batch",
            record_bulk_flush,
        )
        research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        ).run_once()

        assert persisted_batch_sizes == [250, 250, 250, 250, 1]
    finally:
        engine.dispose()


def test_research_worker_commits_archive_progress_before_a_later_stage_fails(
    tmp_path, monkeypatch
) -> None:
    config = _runtime_config(tmp_path)
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    original_batch = research_service.archive_research_batch
    try:
        with factory() as session:
            _insert_markets(session, count=1, at=at)

        def fail_after_market_batch(session, *, source_stage: str):
            if source_stage == "reference_ticks":
                raise SQLAlchemyError("canceling statement due to statement timeout")
            return original_batch(session, source_stage=source_stage)

        monkeypatch.setattr(research_service, "archive_research_batch", fail_after_market_batch)
        worker = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        )

        result = worker.run_once()

        with factory() as session:
            archived_market = session.scalar(
                select(ResearchReplayEvent).where(ResearchReplayEvent.source_table == "markets")
            )
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat(
                WORKER_SERVICE_RESEARCH
            )

        assert archived_market is not None
        assert result["status"] == "error"
        assert heartbeat is not None
        details = heartbeat.metadata_["research"]
        assert details["worker_state"] == "error"
        assert details["current_stage"] == "archive"
        assert details["statement_timeout_detected"] is True
        assert details["last_error"] == {
            "type": "SQLAlchemyError",
            "code": "research_statement_timeout",
            "statement_timeout_detected": True,
        }
    finally:
        engine.dispose()


def test_research_archive_timeout_preserves_committed_batch_progress_and_resumes(
    tmp_path, monkeypatch
) -> None:
    config = _runtime_config(tmp_path)
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    original_batch = research_service.archive_research_batch
    market_attempts = 0
    try:
        with factory() as session:
            _insert_markets(session, count=1001, at=at)

        def fail_third_market_batch(session, *, source_stage: str):
            nonlocal market_attempts
            if source_stage == "markets":
                market_attempts += 1
                if market_attempts == 3:
                    raise SQLAlchemyError("canceling statement due to statement timeout")
            return original_batch(session, source_stage=source_stage)

        monkeypatch.setattr(research_service, "archive_research_batch", fail_third_market_batch)
        worker = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        )

        first = worker.run_once()
        with factory() as session:
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat(
                WORKER_SERVICE_RESEARCH
            )

        assert first["status"] == "error"
        assert heartbeat is not None
        details = heartbeat.metadata_["research"]
        assert details["current_stage"] == "archive"
        assert details["current_source_table"] == "markets"
        assert details["completed_archive_batches"] == 2
        assert details["archive_event_count"] == 500
        assert details["archived_counts_by_type"] == {"MARKET": 500}
        assert details["last_archive_batch"] == {
            "source_stage": "markets",
            "source_rows": 250,
            "archived_events": 250,
            "batch_count": 2,
        }
        assert details["failed_stage"] == "archive"
        status = build_research_status(config)
        assert status["worker_observed_enabled"] is True
        assert status["effective_enabled"] is True
        assert status["heartbeat_fresh"] is True
        assert status["healthy"] is False
        assert status["current_stage"] == "archive"
        assert status["current_source_table"] == "markets"
        assert status["completed_archive_batches"] == 2

        monkeypatch.setattr(research_service, "archive_research_batch", original_batch)
        second = worker.run_once()
        with factory() as session:
            stored = session.scalar(
                select(func.count())
                .select_from(ResearchReplayEvent)
                .where(ResearchReplayEvent.source_table == "markets")
            )

        assert second["status"] == "completed"
        assert stored == 1001
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("stage", "calibration_enabled"),
    [
        ("association_labels", False),
        ("coverage", False),
        ("baseline_replay", False),
        ("calibration", True),
    ],
)
def test_research_worker_heartbeats_stay_fresh_during_long_stages(
    tmp_path, monkeypatch, stage, calibration_enabled
) -> None:
    config = _runtime_config(tmp_path, calibration_enabled=calibration_enabled)
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    entered = Event()
    release = Event()
    outcome: dict[str, object] = {}
    try:
        if stage == "association_labels":
            original = research_service.refresh_research_archive_labels

            def pause_labels(session):
                entered.set()
                assert release.wait(timeout=5)
                return original(session)

            monkeypatch.setattr(research_service, "refresh_research_archive_labels", pause_labels)
        elif stage == "coverage":
            original = research_service.archive_research_coverage

            def pause_coverage(*args, **kwargs):
                entered.set()
                assert release.wait(timeout=5)
                return original(*args, **kwargs)

            monkeypatch.setattr(research_service, "archive_research_coverage", pause_coverage)
        elif stage == "baseline_replay":
            original = research_service.DeterministicReplayEngine.replay_ordered_pages

            def pause_replay(instance, *args, **kwargs):
                entered.set()
                assert release.wait(timeout=5)
                return original(instance, *args, **kwargs)

            monkeypatch.setattr(
                research_service.DeterministicReplayEngine,
                "replay_ordered_pages",
                pause_replay,
            )
        else:
            original = research_service.run_governed_calibration

            def pause_calibration(*args, **kwargs):
                entered.set()
                assert release.wait(timeout=5)
                return original(*args, **kwargs)

            monkeypatch.setattr(
                research_service, "run_governed_calibration", pause_calibration
            )

        worker = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
            heartbeat_interval_seconds=0.02,
        )
        thread = Thread(target=lambda: outcome.setdefault("result", worker.run_once()))
        thread.start()
        assert entered.wait(timeout=5)
        time.sleep(0.07)

        with factory() as session:
            heartbeats = list(
                session.scalars(
                    select(WorkerHeartbeat)
                    .where(WorkerHeartbeat.service_name == WORKER_SERVICE_RESEARCH)
                    .order_by(WorkerHeartbeat.id.asc())
                )
            )
        running = [
            heartbeat
            for heartbeat in heartbeats
            if heartbeat.metadata_["research"].get("worker_state") == "running"
            and heartbeat.metadata_["research"].get("current_stage") == stage
        ]
        status = build_research_status(config)

        assert len(running) > 1
        assert running[-1].heartbeat_at > running[0].heartbeat_at
        assert status["worker_observed_enabled"] is True
        assert status["effective_enabled"] is True
        assert status["heartbeat_fresh"] is True
        assert status["worker_state"] == "running"
        assert status["current_stage"] == stage
        assert status["cycle_running"] is True

        release.set()
        thread.join(timeout=10)
        assert not thread.is_alive()
        assert "result" in outcome
        with factory() as session:
            completed_heartbeats = session.scalar(
                select(func.count()).select_from(WorkerHeartbeat)
            )
        time.sleep(0.06)
        with factory() as session:
            assert (
                session.scalar(
                    select(func.count()).select_from(WorkerHeartbeat)
                )
                == completed_heartbeats
            )
    finally:
        release.set()
        engine.dispose()


@pytest.mark.parametrize(
    ("reader_index", "stage", "counter_key"),
    [
        (0, "coverage", "coverage_events_scanned"),
        (1, "baseline_replay", "replay_events_scanned"),
    ],
)
def test_research_scan_heartbeats_publish_advancing_page_progress(
    tmp_path, monkeypatch, reader_index, stage, counter_key
) -> None:
    config = _runtime_config(tmp_path)
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    page_ready = Event()
    release_page = Event()
    outcome: dict[str, object] = {}
    readers: list[FrozenReplayEventReader] = []
    paused_pages: list[int] = []
    try:
        with factory() as session:
            session.add_all(
                [
                    ResearchReplayEvent(
                        event_id=f"progress-{index}",
                        market_ticker="KXBTC15M-PROGRESS",
                        event_type="MARKET",
                        event_time=at + timedelta(microseconds=index),
                        received_at=at + timedelta(microseconds=index),
                        source_table="progress-fixture",
                        source_row_id=str(index),
                        source_hash=str(index),
                        replay_schema_version="momentum_v2_replay_v1",
                        payload={},
                        event_hash=f"progress-hash-{index}",
                        replay_readiness="FULL",
                        blockers=[],
                    )
                    for index in range(REPLAY_EVENT_PAGE_SIZE * 2 + 1)
                ]
            )
            session.commit()

        original_reader = ResearchRepository.frozen_replay_event_reader
        original_report = FrozenReplayEventReader._report_progress

        def track_reader(self, snapshot, *, page_size=REPLAY_EVENT_PAGE_SIZE):
            reader = original_reader(self, snapshot, page_size=page_size)
            readers.append(reader)
            return reader

        def pause_after_page(self, callback, *, phase):
            original_report(self, callback, phase=phase)
            if (
                phase == "page"
                and len(readers) > reader_index
                and self is readers[reader_index]
                and self.pages_scanned <= 2
            ):
                paused_pages.append(self.events_scanned)
                page_ready.set()
                assert release_page.wait(timeout=5)
                release_page.clear()
                page_ready.clear()

        monkeypatch.setattr(ResearchRepository, "frozen_replay_event_reader", track_reader)
        monkeypatch.setattr(FrozenReplayEventReader, "_report_progress", pause_after_page)
        worker = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
            heartbeat_interval_seconds=0.02,
        )
        thread = Thread(target=lambda: outcome.setdefault("result", worker.run_once()))
        thread.start()

        observed_counters: list[int] = []
        for expected in (REPLAY_EVENT_PAGE_SIZE, REPLAY_EVENT_PAGE_SIZE * 2):
            page_deadline = time.monotonic() + 5
            while not (
                page_ready.wait(timeout=0.05)
                and paused_pages
                and paused_pages[-1] == expected
            ):
                assert time.monotonic() < page_deadline
            current_values: list[int] = []
            heartbeat_deadline = time.monotonic() + 5
            while time.monotonic() < heartbeat_deadline:
                with factory() as session:
                    heartbeats = list(
                        session.scalars(
                            select(WorkerHeartbeat)
                            .where(WorkerHeartbeat.service_name == WORKER_SERVICE_RESEARCH)
                            .order_by(WorkerHeartbeat.id.asc())
                        )
                    )
                current_values = [
                    heartbeat.metadata_["research"].get(counter_key)
                    for heartbeat in heartbeats
                    if heartbeat.metadata_["research"].get("worker_state") == "running"
                    and heartbeat.metadata_["research"].get("current_stage") == stage
                    and heartbeat.metadata_["research"].get(counter_key) is not None
                ]
                if current_values and max(current_values) >= expected:
                    break
                time.sleep(0.02)
            assert current_values and max(current_values) >= expected
            observed_counters.append(max(current_values))
            status = build_research_status(config)
            assert status[counter_key] >= expected
            assert status["heartbeat_fresh"] is True
            release_page.set()

        thread.join(timeout=10)
        assert not thread.is_alive()
        assert outcome["result"]["status"] == "completed"
        assert paused_pages == [REPLAY_EVENT_PAGE_SIZE, REPLAY_EVENT_PAGE_SIZE * 2]
        assert observed_counters[0] < observed_counters[1]
        with factory() as session:
            terminal_count = session.scalar(select(func.count()).select_from(WorkerHeartbeat))
        time.sleep(0.06)
        with factory() as session:
            assert (
                session.scalar(select(func.count()).select_from(WorkerHeartbeat))
                == terminal_count
            )
    finally:
        release_page.set()
        engine.dispose()


def test_archive_duplicate_identity_retries_with_a_fresh_session_and_keeps_one_row(
    tmp_path, monkeypatch
) -> None:
    config = _runtime_config(tmp_path)
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    original_batch = research_service.archive_research_batch
    sessions = []
    try:
        with factory() as session:
            _insert_markets(session, count=1, at=at)

        def duplicate_once(session, *, source_stage: str):
            sessions.append(session)
            if len(sessions) == 1:
                raise IntegrityError(
                    "INSERT research_replay_events",
                    {},
                    Exception(
                        "UNIQUE constraint failed: research_replay_events.source_table, "
                        "research_replay_events.source_row_id"
                    ),
                )
            return original_batch(session, source_stage=source_stage)

        monkeypatch.setattr(research_service, "archive_research_batch", duplicate_once)
        monkeypatch.setattr(research_service.time, "sleep", lambda _seconds: None)
        worker = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        )

        batch = worker._archive_batch_with_retry("markets")
        with factory() as session:
            stored = session.scalar(
                select(func.count())
                .select_from(ResearchReplayEvent)
                .where(ResearchReplayEvent.source_table == "markets")
            )

        assert batch.source_rows == 1
        assert len(sessions) == 2
        assert sessions[0] is not sessions[1]
        assert stored == 1

        def unrelated_error(session, *, source_stage: str):
            raise IntegrityError(
                "INSERT research_replay_events",
                {},
                Exception("NOT NULL constraint failed: research_replay_events.event_type"),
            )

        monkeypatch.setattr(research_service, "archive_research_batch", unrelated_error)
        with pytest.raises(IntegrityError):
            worker._archive_batch_with_retry("markets")
    finally:
        engine.dispose()


def test_archive_postgres_lock_precedes_selection_and_sqlite_skips_it() -> None:
    class ArchiveSession:
        def __init__(self, dialect_name: str) -> None:
            self.dialect_name = dialect_name
            self.locked = False
            self.executed: list[tuple[str, dict[str, str]]] = []

        def get_bind(self):
            return SimpleNamespace(dialect=SimpleNamespace(name=self.dialect_name))

        def execute(self, statement, params):
            self.locked = True
            self.executed.append((str(statement), params))

        def scalars(self, _statement):
            if self.dialect_name == "postgresql":
                assert self.locked
            return []

    postgres = ArchiveSession("postgresql")
    sqlite = ArchiveSession("sqlite")

    assert archive_research_batch(postgres, source_stage="markets").source_rows == 0
    assert postgres.executed == [
        (
            "SELECT pg_advisory_xact_lock(hashtext(:lock_key))",
            {"lock_key": "ape:research_archive:markets"},
        )
    ]
    assert archive_research_batch(sqlite, source_stage="markets").source_rows == 0
    assert sqlite.executed == []


def test_research_worker_resumes_after_a_bounded_archive_budget(tmp_path, monkeypatch) -> None:
    config = _runtime_config(tmp_path)
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    try:
        with factory() as session:
            _insert_markets(session, count=ARCHIVE_BATCH_SIZE + 1, at=at)
            for source_stage in ARCHIVE_SOURCE_STAGES:
                if source_stage == "markets":
                    continue
                cursor = session.get(ResearchArchiveCursor, source_stage)
                assert cursor is not None
                cursor.selector_mode = "TAIL"
                cursor.source_cursor = 0
                cursor.schema_version = "research_archive_cursor_v1"
                cursor.bootstrap_complete = True
            session.commit()
        monkeypatch.setattr(research_service, "ARCHIVE_MAX_BATCHES_PER_CYCLE", 1)
        worker = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        )

        first = worker.run_once()
        second = worker.run_once()

        with factory() as session:
            stored = session.scalar(
                select(func.count())
                .select_from(ResearchReplayEvent)
                .where(ResearchReplayEvent.source_table == "markets")
            )
        assert first["status"] == "partial"
        assert second["status"] == "completed"
        assert stored == ARCHIVE_BATCH_SIZE + 1
    finally:
        engine.dispose()


def test_replay_evidence_survives_a_calibration_failure(tmp_path, monkeypatch) -> None:
    config = _runtime_config(tmp_path, calibration_enabled=True)
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    try:
        def fail_calibration(*_args, **_kwargs):
            raise RuntimeError("calibration fixture failure")

        monkeypatch.setattr(
            research_service, "run_governed_calibration", fail_calibration
        )
        worker = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        )

        result = worker.run_once()

        with factory() as session:
            replay_run = session.scalar(select(ResearchReplayRun))
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat(
                WORKER_SERVICE_RESEARCH
            )
        assert replay_run is not None
        assert result["status"] == "error"
        assert heartbeat.metadata_["research"]["current_stage"] == "calibration"
        assert heartbeat.metadata_["research"]["last_successful_stage"] == "baseline_replay"
    finally:
        engine.dispose()


def test_research_status_exposes_sanitized_worker_failure_from_dedicated_worker(tmp_path) -> None:
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'research-status.sqlite'}",
            "RESEARCH_ENABLED": "false",
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    try:
        with factory() as session:
            WorkerHeartbeatRepository(session).record_heartbeat(
                WorkerHeartbeatInput(
                    service_name=WORKER_SERVICE_RESEARCH,
                    heartbeat_at=at,
                    app_mode="DRY_RUN",
                    is_safe=True,
                    metadata={
                        "research": {
                            "enabled": True,
                            "calibration_enabled": False,
                            "poll_seconds": 60,
                            "worker_role": "research",
                            "worker_state": "error",
                            "cycle_state": "error",
                            "current_stage": "archive",
                            "last_successful_stage": "startup",
                            "last_error": {
                                "type": "OperationalError",
                                "code": "research_statement_timeout",
                                "statement_timeout_detected": True,
                                "raw_message": "postgres://private-host/password",
                            },
                        }
                    },
                )
            )
            session.commit()

        status = build_research_status(config, now=at + timedelta(seconds=10))

        assert status["api_local_configuration"]["research_enabled"] is False
        assert status["worker_observed_enabled"] is True
        assert status["effective_enabled"] is True
        assert status["healthy"] is False
        assert status["current_stage"] == "archive"
        assert status["statement_timeout_detected"] is True
        assert "raw_message" not in status["last_error"]
        assert "private-host" not in str(status["worker_heartbeat"])
    finally:
        engine.dispose()
