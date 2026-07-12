from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import select

from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.models import (
    ResearchMarketOutcome,
    ResearchReplayEvent,
    ResearchReplayRun,
    ResearchReplayTrade,
    WorkerHeartbeat,
)
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.inputs import StrategyConfigVersionInput
from ape.repositories.strategy_v2 import StrategyV2Repository
from ape.research import service as research_service
from ape.research.calibration import CalibrationResult, CandidateSpec
from ape.research.fees import verified_kalshi_taker_fee_model
from ape.research.replay import ReplayTrade
from ape.research.repository import ResearchRepository
from ape.research.service import run_research_cycle
from ape.strategy.momentum_v2 import (
    REPLAY_SCHEMA_VERSION,
    V2_ARCHITECTURE_VERSION,
    V2_FEATURE_SCHEMA_VERSION,
    V2_PARAMETERS,
)
from ape.worker.main import WORKER_ROLE_RESEARCH, run_worker
from ape.worker.services import WORKER_SERVICE_RESEARCH


def _persist_governance_coverage_fixture(session, at: datetime, count: int) -> None:
    events: list[ResearchReplayEvent] = []
    outcomes: list[ResearchMarketOutcome] = []
    for index in range(count):
        market = f"M{index}"
        opened_at = at + timedelta(minutes=15 * index)
        feature_id = f"governance-feature-{index}"
        events.extend(
            (
                ResearchReplayEvent(
                    event_id=f"governance-market-{index}",
                    market_ticker=market,
                    event_type="MARKET",
                    event_time=opened_at,
                    received_at=opened_at,
                    source_table="markets",
                    source_row_id=f"governance-market-{index}",
                    replay_schema_version=REPLAY_SCHEMA_VERSION,
                    payload={},
                    event_hash=f"governance-market-{index}",
                    replay_readiness="FULL",
                ),
                ResearchReplayEvent(
                    event_id=f"governance-reference-{index}",
                    market_ticker=market,
                    event_type="REFERENCE",
                    event_time=opened_at + timedelta(milliseconds=100),
                    received_at=opened_at + timedelta(milliseconds=100),
                    source_table="reference_ticks",
                    source_row_id=f"governance-reference-{index}",
                    replay_schema_version=REPLAY_SCHEMA_VERSION,
                    payload={},
                    event_hash=f"governance-reference-{index}",
                    replay_readiness="FULL",
                ),
                ResearchReplayEvent(
                    event_id=f"governance-lifecycle-{index}",
                    market_ticker=market,
                    event_type="MARKET_LIFECYCLE",
                    event_time=opened_at + timedelta(milliseconds=200),
                    received_at=opened_at + timedelta(milliseconds=200),
                    source_table="market_lifecycle",
                    source_row_id=f"governance-lifecycle-{index}",
                    replay_schema_version=REPLAY_SCHEMA_VERSION,
                    payload={},
                    event_hash=f"governance-lifecycle-{index}",
                    replay_readiness="FULL",
                ),
                ResearchReplayEvent(
                    event_id=feature_id,
                    market_ticker=market,
                    event_type="FEATURE_SNAPSHOT",
                    event_time=opened_at + timedelta(milliseconds=300),
                    received_at=opened_at + timedelta(milliseconds=300),
                    source_table="strategy_feature_snapshots",
                    source_row_id=feature_id,
                    feature_snapshot_id=feature_id,
                    replay_schema_version=REPLAY_SCHEMA_VERSION,
                    payload={"feature_vector": {"candidate_side": "YES"}},
                    event_hash=feature_id,
                    replay_readiness="FULL",
                ),
                ResearchReplayEvent(
                    event_id=f"governance-book-{index}",
                    market_ticker=market,
                    event_type="ORDERBOOK",
                    event_time=opened_at + timedelta(milliseconds=900),
                    received_at=opened_at + timedelta(milliseconds=900),
                    source_table="orderbook_snapshots",
                    source_row_id=f"governance-book-{index}",
                    replay_schema_version=REPLAY_SCHEMA_VERSION,
                    payload={},
                    event_hash=f"governance-book-{index}",
                    replay_readiness="FULL",
                ),
            )
        )
        outcomes.append(
            ResearchMarketOutcome(
                outcome_id=f"governance-outcome-{index}",
                market_ticker=market,
                market_open_at=opened_at,
                market_close_at=opened_at + timedelta(minutes=15),
                outcome_status="RESOLVED",
                outcome_source="fixture",
                quality_flags={
                    "counterfactual_labels": {
                        feature_id: {"net_markout_30s_cents": "1"}
                    }
                },
            )
        )
    session.add_all([*events, *outcomes])
    session.flush()


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


def test_research_cycle_loads_the_complete_archive(tmp_path, monkeypatch) -> None:
    config = load_config(
        {"DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'complete-archive.sqlite'}"}
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    captured_limits: list[int | None] = []
    original_list_events = ResearchRepository.list_events

    def list_events(self, *, market_ticker=None, limit=500):
        captured_limits.append(limit)
        return original_list_events(self, market_ticker=market_ticker, limit=limit)

    monkeypatch.setattr(ResearchRepository, "list_events", list_events)
    try:
        with factory() as session:
            run_research_cycle(config, session)
            session.commit()

        assert captured_limits == [None]
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


def test_research_cycle_replays_again_when_resolved_outcome_changes(tmp_path) -> None:
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'outcome-identity.sqlite'}",
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
    try:
        with factory() as session:
            outcome = ResearchMarketOutcome(
                outcome_id="outcome-identity",
                market_ticker="KXBTC15M-IDENTITY",
                market_open_at=at,
                market_close_at=at + timedelta(minutes=15),
                outcome_status="RESOLVED",
                result_side="YES",
                resolved_at=at + timedelta(minutes=15),
                quality_flags={"counterfactual_labels": {"feature": {"label": "YES"}}},
            )
            session.add(outcome)
            first = run_research_cycle(config, session, checked_at=at + timedelta(minutes=16))
            session.commit()

        with factory() as session:
            outcome = session.scalar(
                select(ResearchMarketOutcome).where(
                    ResearchMarketOutcome.outcome_id == "outcome-identity"
                )
            )
            assert outcome is not None
            outcome.result_side = "NO"
            outcome.quality_flags = {
                "counterfactual_labels": {"feature": {"label": "NO"}}
            }
            second = run_research_cycle(config, session, checked_at=at + timedelta(minutes=17))
            session.commit()
            assert second["replay_run_id"] != first["replay_run_id"]
            assert second["calibration_run_id"] != first["calibration_run_id"]

        with factory() as session:
            runs = list(session.scalars(select(ResearchReplayRun)))
            assert len(runs) == 2
            assert len({run.raw_metrics["outcome_input_hash"] for run in runs}) == 2
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
        exit_intent_at=at + timedelta(seconds=5, milliseconds=500),
        exit_limit=Decimal("0.64"),
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
            assert stored.trade_id.endswith(
                "candidate-fixture-search_development-candidate-trade"
            )
            assert stored.measurements["evidence_partition"] == "search_development"
            assert stored.strategy_config_version_id == "research-candidate-fixture"
            assert stored.market_ticker == trade.market_ticker
            assert stored.net_pnl_cents == Decimal("4")
            assert stored.exit_trigger_at is not None
            assert stored.exit_trigger_at.replace(tzinfo=UTC) == trade.exit_trigger_at
            assert stored.exit_intent_at is not None
            assert stored.exit_intent_at.replace(tzinfo=UTC) == trade.exit_intent_at
            assert stored.exit_limit == trade.exit_limit
            assert stored.exit_fill_at is not None
            assert stored.exit_fill_at.replace(tzinfo=UTC) == trade.exit_fill_at
            assert stored.exit_fill_price == trade.exit_fill_price
    finally:
        engine.dispose()


def test_research_cycle_advances_only_the_selected_candidate(tmp_path, monkeypatch) -> None:
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'selected-candidate.sqlite'}",
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
    selected = CandidateSpec(
        candidate_id="candidate-selected",
        generated_strategy_id="btc15_momentum_v2_candidate_selected",
        model_type="WEIGHTED_HEURISTIC",
        parameters={"fixture": "selected"},
    )
    unselected = CandidateSpec(
        candidate_id="candidate-unselected",
        generated_strategy_id="btc15_momentum_v2_candidate_unselected",
        model_type="WEIGHTED_HEURISTIC",
        parameters={"fixture": "unselected"},
    )

    def fake_calibration(**_kwargs):
        return CalibrationResult(
            "COMPLETED",
            {"holdout_hash": "fixture-holdout"},
            (selected, unselected),
            {
                selected.candidate_id: {"status": "EVALUATED"},
                unselected.candidate_id: {"status": "EVALUATED"},
            },
            selected.candidate_id,
            (),
            (),
        )

    advanced: list[tuple[str, str]] = []

    def capture_advance(self, *, candidate_id: str, actor: str):
        del self
        advanced.append((candidate_id, actor))
        return []

    monkeypatch.setattr(research_service, "run_bounded_calibration", fake_calibration)
    monkeypatch.setattr(
        research_service.ResearchRepository,
        "advance_candidate_governance",
        capture_advance,
    )
    try:
        with factory() as session:
            run_research_cycle(config, session, checked_at=at)
            session.commit()
        with factory() as session:
            repository = ResearchRepository(session)
            assert repository.get_candidate(selected.candidate_id) is not None
            assert repository.get_candidate(unselected.candidate_id) is not None

        assert advanced == [(selected.candidate_id, "ape-research-worker")]
    finally:
        engine.dispose()


def test_automatic_governance_uses_persisted_candidate_evidence(tmp_path) -> None:
    config = load_config({"DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'governance.sqlite'}"})
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)
    metrics = {
        "status": "EVALUATED",
        "closed_trade_count": 50,
        "entry_frequency_per_100_markets": "10",
        "signal_to_fill_rate": "0.5",
        "volatility_regime_coverage": 2,
        "liquidity_regime_coverage": 2,
        "timing_tier_coverage": 2,
        "dominant_regime_entry_share": "0.5",
        "maximum_drawdown_cents": "10",
        "net_pnl_per_market": "1",
        "zero_entry_report": {
            "unique_market_rates_per_100": {"qualified_setups": "5"}
        },
        "penalties": {"adjusted_lower_confidence_expectancy": "1"},
    }
    try:
        with factory() as session:
            repository = ResearchRepository(session)
            repository.create_replay_run(
                {
                    "replay_run_id": "replay-governance",
                    "status": "COMPLETED",
                    "replay_engine_version": REPLAY_SCHEMA_VERSION,
                    "label_schema_version": "labels",
                    "code_commit_sha": "fixture",
                    "dataset_hash": "fixture",
                    "unique_market_count": 500,
                    "event_count": 500,
                        "cost_model": verified_kalshi_taker_fee_model().metadata(),
                        "raw_metrics": {
                            "archive_coverage": {
                                "event_count": 500,
                                "complete_markets": 500,
                                "minimum_coverage": "1",
                                "per_market_coverage": {
                                    f"M{index}": {} for index in range(500)
                                },
                            }
                        },
                    "started_at": at,
                }
            )
            repository.create_calibration_run(
                {
                    "calibration_run_id": "calibration-governance",
                    "status": "COMPLETED",
                    "calibration_schema_version": "fixture",
                    "replay_run_id": "replay-governance",
                    "dataset_hash": "fixture",
                    "code_commit_sha": "fixture",
                    "random_seed": 1,
                    "partition_manifest": {
                        "ordered_market_tickers": [f"M{index}" for index in range(500)],
                        "governance_trade_partitions": ["frozen_holdout"],
                    },
                    "validation_metrics": {
                        "candidate-baseline-v2": {
                            "net_pnl_per_market": "0",
                            "penalties": {"adjusted_lower_confidence_expectancy": "0"},
                        }
                    },
                    "started_at": at,
                }
            )
            StrategyV2Repository(session).ensure_config_version(
                StrategyConfigVersionInput(
                    strategy_config_version_id="config-baseline",
                    strategy_id="btc15_momentum_v2",
                    architecture_version=V2_ARCHITECTURE_VERSION,
                    feature_schema_version=V2_FEATURE_SCHEMA_VERSION,
                    parameter_snapshot=V2_PARAMETERS,
                    parameter_hash="fixture-baseline",
                    code_commit_sha="fixture",
                    source="RESEARCH",
                    lifecycle_state="DRAFT",
                )
            )
            StrategyV2Repository(session).ensure_config_version(
                StrategyConfigVersionInput(
                    strategy_config_version_id="config-governance",
                    strategy_id="btc15_momentum_v2_candidate_governance",
                    architecture_version=V2_ARCHITECTURE_VERSION,
                    feature_schema_version=V2_FEATURE_SCHEMA_VERSION,
                    parameter_snapshot=V2_PARAMETERS,
                    parameter_hash="fixture",
                    code_commit_sha="fixture",
                    source="RESEARCH",
                    parent_config_version_id="config-baseline",
                    lifecycle_state="DRAFT",
                    candidate_id="candidate-governance",
                )
            )
            repository.create_candidate(
                {
                    "candidate_id": "candidate-governance",
                    "strategy_config_version_id": "config-governance",
                        "calibration_run_id": "calibration-governance",
                        "parent_strategy_config_version_id": "config-baseline",
                    "generated_strategy_id": "btc15_momentum_v2_candidate_governance",
                    "architecture_version": V2_ARCHITECTURE_VERSION,
                    "feature_schema_version": V2_FEATURE_SCHEMA_VERSION,
                    "replay_schema_version": REPLAY_SCHEMA_VERSION,
                    "model_type": "WEIGHTED_HEURISTIC",
                    "parameter_snapshot": V2_PARAMETERS,
                    "model_artifact_checksum": "fixture",
                    "validation_metrics": metrics,
                    "holdout_metrics": {
                        "net_pnl_per_market": "1",
                        "closed_trade_count": 50,
                        "bootstrap": {"net_pnl_per_market": {"lower": "1"}},
                    },
                    "lifecycle_state": "DRAFT",
                    "eligibility_status": "RESEARCH_ONLY",
                }
            )
            for index in range(50):
                repository.insert_replay_trade(
                    {
                        "trade_id": f"governance-trade-{index}",
                        "replay_run_id": "replay-governance",
                        "candidate_id": "candidate-governance",
                        "strategy_config_version_id": "config-governance",
                        "market_ticker": f"M{index}",
                        "side": "YES",
                        "status": "CLOSED",
                        "entry_fill_event_id": f"governance-entry-{index}",
                        "measurements": {
                            "evidence_partition": "frozen_holdout",
                            "source_decision_id": f"governance-decision-{index}",
                        },
                    }
                )

            _persist_governance_coverage_fixture(session, at, 500)

            transitions = repository.advance_candidate_governance(
                candidate_id="candidate-governance", actor="test"
            )
            assert [event.to_state for event in transitions] == [
                "BACKTESTED",
                "SHADOW",
                "DRY_RUN_CHALLENGER",
            ]
            assert repository.get_candidate("candidate-governance").lifecycle_state == (
                "DRY_RUN_CHALLENGER"
            )
            assert StrategyV2Repository(session).get_config_version(
                "config-governance"
            ).lifecycle_state == "DRY_RUN_CHALLENGER"
            assert repository.advance_candidate_governance(
                candidate_id="candidate-governance", actor="test"
            ) == []
    finally:
        engine.dispose()
