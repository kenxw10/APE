from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.models import Market, ResearchReplayEvent, ResearchReplayRun
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.inputs import WorkerHeartbeatInput
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.research import service as research_service
from ape.research.archive import ARCHIVE_BATCH_SIZE, archive_research_batch
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


def test_research_worker_resumes_after_a_bounded_archive_budget(tmp_path, monkeypatch) -> None:
    config = _runtime_config(tmp_path)
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    try:
        with factory() as session:
            _insert_markets(session, count=ARCHIVE_BATCH_SIZE + 1, at=at)
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
        def fail_calibration(**_kwargs):
            raise RuntimeError("calibration fixture failure")

        monkeypatch.setattr(research_service, "run_bounded_calibration", fail_calibration)
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
