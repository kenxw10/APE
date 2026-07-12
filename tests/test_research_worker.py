from __future__ import annotations

from sqlalchemy import select

from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.models import ResearchReplayRun, WorkerHeartbeat
from ape.db.session import create_engine_from_config, create_session_factory
from ape.research import service as research_service
from ape.research.calibration import CalibrationResult
from ape.research.service import run_research_cycle
from ape.worker.main import WORKER_ROLE_RESEARCH, run_worker
from ape.worker.services import WORKER_SERVICE_RESEARCH


def test_research_cycle_archives_and_records_isolated_heartbeat(tmp_path) -> None:
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'research.sqlite'}",
            "APP_MODE": "DRY_RUN",
            "RESEARCH_ENABLED": "true",
            "CALIBRATION_ENABLED": "true",
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    try:
        with factory() as session:
            result = run_research_cycle(config, session)
            session.commit()
            assert result["status"] == "completed"
            assert result["calibration_status"] == "INSUFFICIENT_DATA"
        run_worker(config, worker_role=WORKER_ROLE_RESEARCH, max_iterations=1)
        with factory() as session:
            assert session.scalar(select(ResearchReplayRun)) is not None
            heartbeat = session.scalar(
                select(WorkerHeartbeat).where(
                    WorkerHeartbeat.service_name == WORKER_SERVICE_RESEARCH
                )
            )
            assert heartbeat is not None
            assert heartbeat.metadata_["research"]["worker_role"] == "research"
    finally:
        engine.dispose()


def test_research_cycle_does_not_reuse_a_frozen_holdout(tmp_path, monkeypatch) -> None:
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'holdout.sqlite'}",
            "APP_MODE": "DRY_RUN",
            "CALIBRATION_ENABLED": "true",
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    calls = 0

    def fake_calibration(**_kwargs):
        nonlocal calls
        calls += 1
        return CalibrationResult(
            "COMPLETED",
            {"holdout_hash": "fixture-holdout"},
            (),
            {"fixture-selected": {"holdout": {"net_pnl_per_market": "1"}}},
            "fixture-selected",
            (),
            (),
        )

    monkeypatch.setattr(research_service, "run_bounded_calibration", fake_calibration)
    try:
        with factory() as session:
            run_research_cycle(config, session)
            session.commit()
        with factory() as session:
            run_research_cycle(config, session)
            session.commit()
        assert calls == 1
    finally:
        engine.dispose()
