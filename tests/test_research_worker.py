from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import select

from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.models import ResearchReplayRun, ResearchReplayTrade, WorkerHeartbeat
from ape.db.session import create_engine_from_config, create_session_factory
from ape.research import service as research_service
from ape.research.calibration import CalibrationResult, CandidateSpec
from ape.research.replay import ReplayTrade
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


def test_market_outcome_reconciler_uses_public_rest_configuration(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakePublicClient:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(research_service, "KalshiRestClient", FakePublicClient)
    config = load_config(
        {
            "KALSHI_API_BASE_URL": "https://public.example.test/trade-api/v2",
            "KALSHI_REST_TIMEOUT_SECONDS": "17.5",
            "KALSHI_API_KEY_ID": "must-not-be-used",
            "KALSHI_PRIVATE_KEY": "must-not-be-used",
        }
    )

    reconciler = research_service.MarketOutcomeReconciler(
        config=config,
        safety=None,
        session_factory=None,
        started_at=datetime.now(UTC),
    )

    assert reconciler.market_client is not None
    assert captured == {
        "base_url": config.kalshi_api_base_url,
        "timeout_seconds": config.kalshi_rest_timeout_seconds,
    }


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


def test_research_cycle_persists_calibration_candidate_replay_trades(
    tmp_path, monkeypatch
) -> None:
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'candidate-replay.sqlite'}",
            "APP_MODE": "DRY_RUN",
            "CALIBRATION_ENABLED": "true",
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)
    candidate = CandidateSpec(
        candidate_id="candidate-fixture",
        generated_strategy_id="btc15_momentum_v2_candidate_fixture",
        model_type="WEIGHTED_HEURISTIC",
        parameters={"fixture": "candidate"},
    )
    trade = ReplayTrade(
        trade_id="candidate-trade",
        market_ticker="FIXTURE-MARKET",
        side="YES",
        entry_decision_at=at,
        entry_fill_at=at + timedelta(milliseconds=500),
        entry_limit=Decimal("0.60"),
        entry_fill_price=Decimal("0.60"),
        entry_fill_event_id="entry-book",
        exit_trigger_at=at + timedelta(seconds=5),
        exit_fill_at=at + timedelta(seconds=6),
        exit_fill_price=Decimal("0.65"),
        exit_fill_event_id="exit-book",
        status="CLOSED",
        gross_pnl_cents=Decimal("5"),
        fee_cents=Decimal("1"),
        net_pnl_cents=Decimal("4"),
        holding_duration_ms=5500,
        mfe_cents=Decimal("5"),
        mae_cents=Decimal("0"),
        time_to_mfe_ms=5500,
        time_to_mae_ms=0,
        entry_reason="fixture-entry",
        exit_reason="fixture-exit",
        timing_tier="normal",
        measurements={"fixture": True},
    )

    def fake_calibration(**_kwargs):
        return CalibrationResult(
            "COMPLETED",
            {"holdout_hash": "fixture-holdout"},
            (candidate,),
            {candidate.candidate_id: {"status": "EVALUATED"}},
            candidate.candidate_id,
            (),
            (),
            {candidate.candidate_id: (trade,)},
        )

    monkeypatch.setattr(research_service, "run_bounded_calibration", fake_calibration)
    try:
        with factory() as session:
            run_research_cycle(config, session, checked_at=at)
            session.commit()
        with factory() as session:
            stored = session.scalar(
                select(ResearchReplayTrade).where(
                    ResearchReplayTrade.candidate_id == candidate.candidate_id
                )
            )
            assert stored is not None
            assert stored.trade_id.endswith("candidate-fixture-candidate-trade")
            assert stored.strategy_config_version_id == "research-candidate-fixture"
            assert stored.market_ticker == trade.market_ticker
            assert stored.net_pnl_cents == Decimal("4")
    finally:
        engine.dispose()
