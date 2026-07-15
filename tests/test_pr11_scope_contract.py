from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import inspect, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import CreateTable
from tests.test_research_helpers import at_base, feature_event, orderbook_event, valid_vector

import ape.worker.main as worker_main
from ape.api.main import create_app
from ape.config import load_config
from ape.db.migrations import CURRENT_SCHEMA_VERSION, SCHEMA_VERSIONS, run_migrations
from ape.db.models import (
    Base,
    Market,
    OrderbookSnapshot,
    ReferenceTick,
    ResearchMarketOutcome,
    ResearchReplayEvent,
    SchemaMigration,
    StrategyFeatureSnapshot,
)
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.inputs import StrategyDecisionInput
from ape.repositories.storage_retention import (
    ALLOWED_RETENTION_TABLES,
    ALLOWED_STATUS_READ_TABLES,
    StorageRetentionRepository,
)
from ape.research import service as research_service
from ape.research.archive import (
    _hydrate_persisted_feature_vector,
    archive_research_events,
    reconcile_market_outcomes,
)
from ape.research.calibration import (
    LIFECYCLE_BACKTESTED,
    LIFECYCLE_DRAFT,
    LIFECYCLE_DRY_RUN_CHALLENGER,
    LIFECYCLE_PAPER_CANDIDATE,
    LIFECYCLE_SHADOW,
    GovernanceError,
    adjusted_lower_confidence_expectancy,
    bounded_candidate_specs,
    build_partition_manifest,
    candidate_parameter_grids,
    complete_search_space_snapshot,
    fit_l2_logistic,
    market_bootstrap,
    replay_metrics,
    transition_candidate,
)
from ape.research.fees import verified_kalshi_taker_fee_model
from ape.research.fixtures import (
    synthetic_btc15_fixture_dataset,
    synthetic_btc15_fixture_markets,
)
from ape.research.pin import PinnedCandidate
from ape.research.replay import DeterministicReplayEngine, zero_entry_audit
from ape.safety import assess_startup_safety
from ape.storage.retention import RETENTION_POLICIES, STATUS_TABLES
from ape.strategy import observer as observer_module
from ape.strategy.momentum_v2 import (
    CALIBRATION_SCHEMA_VERSION,
    GOVERNANCE_SCHEMA_VERSION,
    REPLAY_SCHEMA_VERSION,
    RESEARCH_LABEL_SCHEMA_VERSION,
    V2_FEATURE_SCHEMA_VERSION,
    evaluate_momentum_v2_feature_vector,
    evaluate_momentum_v2_lifecycle,
    feature_vector_hash,
)
from ape.strategy.observer import DryRunLedgerResult, StrategyObserver

ROOT = Path(__file__).resolve().parents[1]


def test_r1_single_research_migration_and_schema_contract(tmp_path) -> None:
    assert "0010_research_replay_calibration" in SCHEMA_VERSIONS
    assert CURRENT_SCHEMA_VERSION == "0011_research_archive_cursors"
    required_tables = {
        "research_replay_events",
        "research_market_outcomes",
        "research_replay_runs",
        "research_replay_trades",
        "calibration_runs",
        "research_candidates",
        "research_governance_events",
    }
    assert required_tables <= set(Base.metadata.tables)
    engine = create_engine_from_config(
        load_config({"DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'r1.sqlite'}"})
    )
    try:
        run_migrations(engine)
        run_migrations(engine)
        inspector = inspect(engine)
        assert required_tables <= set(inspector.get_table_names())
        assert {
            "event_id",
            "source_table",
            "source_row_id",
            "event_time",
            "replay_readiness",
        } <= {column["name"] for column in inspector.get_columns("research_replay_events")}
        assert {
            "complete_feature_vector",
            "feature_vector_hash",
            "architecture_version",
            "replay_schema_version",
            "replay_readiness",
            "replay_blockers",
        } <= {column["name"] for column in inspector.get_columns("strategy_feature_snapshots")}
        assert {
            "parent_config_version_id",
            "calibration_run_id",
            "lifecycle_state",
            "approval_state",
            "model_type",
            "model_artifact_checksum",
            "candidate_id",
        } <= {column["name"] for column in inspector.get_columns("strategy_config_versions")}
        assert {
            "ix_research_replay_events_market_time",
        } <= {index["name"] for index in inspector.get_indexes("research_replay_events")}
        factory = create_session_factory(engine)
        with factory() as session:
            session.add(_replay_event("r1-first"))
            session.commit()
            session.add(_replay_event("r1-first", event_id="r1-second"))
            with pytest.raises(IntegrityError):
                session.flush()
            session.rollback()
            session.add(_replay_event("r1-first"))
            with pytest.raises(IntegrityError):
                session.flush()
            session.rollback()
            assert session.scalar(
                select(SchemaMigration).where(
                    SchemaMigration.version == "0010_research_replay_calibration"
                )
            ) is not None
        for table_name in required_tables:
            # PostgreSQL DDL compilation catches nonportable metadata before deploy.
            assert str(
                CreateTable(Base.metadata.tables[table_name]).compile(
                    dialect=postgresql.dialect()
                )
            )
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("name", "changes", "state", "reason"),
    (
        (
            "missing prerequisite",
            {"quality_state": {"market_ready": False, "reference_ready": True, "book_ready": True}},
            "V2_FEATURES_NOT_READY",
            "v2_prerequisite_data_missing_or_stale",
        ),
        (
            "first 120 seconds",
            {"seconds_since_open": 119},
            "V2_HARD_GATE_BLOCKED",
            "v2_first_120_seconds",
        ),
        (
            "final 60 seconds",
            {"seconds_left": 60},
            "V2_HARD_GATE_BLOCKED",
            "v2_final_60_seconds",
        ),
        (
            "severe reversal",
            {"reversal_beyond_origin": True},
            "V2_HARD_GATE_BLOCKED",
            "v2_severe_path_reversal",
        ),
        (
            "ask above tier cap",
            {"desired_ask": Decimal("0.79")},
            "V2_HARD_GATE_BLOCKED",
            "v2_desired_ask_above_tier_cap",
        ),
        (
            "insufficient depth",
            {"desired_ask_depth": Decimal("0")},
            "V2_HARD_GATE_BLOCKED",
            "v2_executable_depth_below_two_contracts",
        ),
        (
            "score below threshold",
            {
                "return_5s": Decimal("0"),
                "return_15s": Decimal("0"),
                "return_30s": Decimal("0"),
                "impulse_hold_seconds": 0,
                "fast_impulse_active": True,
            },
            "V2_SCORE_BELOW_THRESHOLD",
            "v2_score_below_threshold",
        ),
        (
            "edge below threshold",
            {"response_residual_cents": Decimal("3.5")},
            "V2_EDGE_BELOW_THRESHOLD",
            "v2_edge_below_threshold",
        ),
        ("valid continuation entry", {}, "DRY_RUN_ENTRY_SIGNAL", "v2_entry_signal"),
        (
            "BOUNDARY_CROSS_HOLD research-only",
            {"candidate_mode": "BOUNDARY_CROSS_HOLD"},
            "V2_HARD_GATE_BLOCKED",
            "v2_candidate_mode_not_enabled",
        ),
    ),
)
def test_r2_live_and_json_persisted_vectors_have_exact_evaluator_parity(
    name, changes, state, reason
) -> None:
    live = valid_vector()
    live.update(changes)
    assert feature_vector_hash(live) == feature_vector_hash(dict(live)), name

    live_result = evaluate_momentum_v2_feature_vector(live)
    persisted_result = evaluate_momentum_v2_feature_vector(
        _hydrate_persisted_feature_vector(_json_safe_vector(live))
    )

    assert _evaluation_contract(persisted_result) == _evaluation_contract(live_result), name
    assert (live_result.state, live_result.reason) == (state, reason), name


def test_r3_worker_roles_keep_research_isolated_and_market_data_owns_reconciliation(
    monkeypatch,
) -> None:
    calls: list[str] = []

    class ResearchOnly:
        def __init__(self, **_kwargs) -> None:
            calls.append("research_init")

        async def run(self, **_kwargs) -> None:
            calls.append("research")

    class MarketOnly:
        def __init__(self, **_kwargs) -> None:
            calls.append("market_init")

        async def run_market_data(self, **_kwargs) -> None:
            calls.append("market")

    class PublicReconciler:
        def __init__(self, **_kwargs) -> None:
            calls.append("reconciler_init")

        async def run(self, **_kwargs) -> None:
            calls.append("reconciler")

    class ForbiddenService:
        def __init__(self, **_kwargs) -> None:
            raise AssertionError("unowned service started")

    monkeypatch.setattr(worker_main, "ResearchWorker", ResearchOnly)
    monkeypatch.setattr(worker_main, "KalshiWsCollector", ForbiddenService)
    monkeypatch.setattr(worker_main, "MarketOutcomeReconciler", ForbiddenService)
    monkeypatch.setattr(worker_main, "StrategyObserver", ForbiddenService)
    monkeypatch.setattr(worker_main, "StorageRetentionWorker", ForbiddenService)
    worker_main.run_worker(
        load_config({"RESEARCH_ENABLED": "true"}), max_iterations=1, worker_role="research"
    )
    assert calls == ["research_init", "research"]

    calls.clear()
    monkeypatch.setattr(worker_main, "KalshiWsCollector", MarketOnly)
    monkeypatch.setattr(worker_main, "MarketOutcomeReconciler", PublicReconciler)
    worker_main.run_worker(
        load_config({"KALSHI_WS_ENABLED": "true"}),
        max_iterations=1,
        worker_role="market-data",
    )
    assert calls == ["market_init", "reconciler_init", "market", "reconciler"]


def test_r3_reconciler_is_public_only_and_official_result_wins(tmp_path, monkeypatch) -> None:
    captured: dict[str, object] = {}

    class PublicClient:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(research_service, "KalshiRestClient", PublicClient)
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'r3.sqlite'}",
            "KALSHI_API_BASE_URL": "https://public.example.test/trade-api/v2",
            "KALSHI_REST_TIMEOUT_SECONDS": "17.5",
            "KALSHI_API_KEY_ID": "must-not-be-used",
            "KALSHI_PRIVATE_KEY": "must-not-be-used",
        }
    )
    reconciler = research_service.MarketOutcomeReconciler(
        config=config, safety=None, session_factory=None, started_at=datetime.now(UTC)
    )
    assert reconciler.market_client is not None
    assert captured == {
        "base_url": config.kalshi_api_base_url,
        "timeout_seconds": config.kalshi_rest_timeout_seconds,
    }

    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)
    try:
        with factory() as session:
            session.add_all(
                (
                    Market(
                        market_ticker="R3-OFFICIAL",
                        series_ticker="KXBTC15M",
                        close_time=at - timedelta(minutes=1),
                    ),
                    ReferenceTick(
                        source="kalshi_cfbenchmarks_brti",
                        received_at=at,
                        parsed_value=Decimal("61900"),
                        parse_status="valid",
                    ),
                )
            )

            class OfficialClient:
                def get_market(self, _ticker: str) -> dict[str, object]:
                    return {"market": {"result": "yes", "status": "settled"}}

            assert reconcile_market_outcomes(session, client=OfficialClient(), now=at) == 1
            outcome = session.scalar(
                select(ResearchMarketOutcome).where(
                    ResearchMarketOutcome.market_ticker == "R3-OFFICIAL"
                )
            )
            assert outcome is not None
            assert outcome.result_side == "YES"
            assert outcome.outcome_source == "kalshi_public_market_detail"
    finally:
        engine.dispose()


def test_r4_archive_is_idempotent_and_recovers_new_and_out_of_order_source_rows(tmp_path) -> None:
    engine = create_engine_from_config(
        load_config({"DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'r4.sqlite'}"})
    )
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)
    try:
        with factory() as session:
            for ticker, readiness in (
                ("R4-FULL", "FULL"),
                ("R4-PARTIAL", "PARTIAL"),
                ("R4-UNUSABLE", "UNUSABLE"),
            ):
                session.add_all(
                    (
                        Market(
                            market_ticker=ticker,
                            series_ticker="KXBTC15M",
                            open_time=at - timedelta(minutes=15),
                            close_time=at,
                            functional_strike=Decimal("62000"),
                        ),
                        StrategyFeatureSnapshot(
                            feature_snapshot_id=f"{ticker}-feature",
                            market_ticker=ticker,
                            evaluated_at=at - timedelta(seconds=5),
                            feature_schema_version="momentum_v2_features_v3",
                            context_hash=ticker,
                            candidate_side="YES",
                            boundary=Decimal("62000"),
                            complete_feature_vector=_json_safe_vector(valid_vector()),
                            replay_readiness=readiness,
                            replay_blockers=[] if readiness == "FULL" else ["fixture"],
                        ),
                        OrderbookSnapshot(
                            market_ticker=ticker,
                            received_at=at - timedelta(seconds=4),
                            yes_ask=Decimal("0.60"),
                            yes_bid=Decimal("0.58"),
                            yes_ask_count=Decimal("1"),
                            yes_bid_count=Decimal("1"),
                        ),
                    )
                )
            session.flush()
            first = archive_research_events(session, now=at)
            second = archive_research_events(session, now=at)
            assert first.archived_events > 0
            assert second.archived_events == 0

            # A source row that arrives after the initial pass may have an older event time.
            session.add(
                Market(
                    market_ticker="R4-OUT-OF-ORDER",
                    series_ticker="KXBTC15M",
                    open_time=at - timedelta(days=1),
                    close_time=at - timedelta(hours=23, minutes=45),
                    functional_strike=Decimal("62000"),
                )
            )
            recovered = archive_research_events(session, now=at + timedelta(seconds=1))
            assert recovered.archived_events >= 1
            events = list(session.scalars(select(ResearchReplayEvent)))
            readiness = {
                event.market_ticker: event.replay_readiness
                for event in events
                if event.event_type == "FEATURE_SNAPSHOT"
            }
            assert readiness == {
                "R4-FULL": "FULL",
                "R4-PARTIAL": "PARTIAL",
                "R4-UNUSABLE": "UNUSABLE",
            }
            assert any(event.market_ticker == "R4-OUT-OF-ORDER" for event in events)
            assert recovered.coverage["source_retention_limitations"] == [
                "archive_contains_only_source_rows_available_before_archive_retention"
            ]
            assert any(event.event_type == "COVERAGE_REPORT" for event in events)
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("fills", "market_count", "expected"),
    (
        (0, 100, "ZERO_ENTRY_UNVALIDATABLE"),
        (1, 200, "TOO_RARE_UNVALIDATABLE"),
        (2, 100, "ECONOMICALLY_INADEQUATE"),
        (5, 100, "PREFERRED_OPERATING_RANGE"),
        (12, 100, "ABOVE_PREFERRED_RANGE"),
        (16, 100, "EXCESSIVE_FREQUENCY"),
    ),
)
def test_r5_funnel_frequency_classifications_are_explicit(
    fills, market_count, expected
) -> None:
    report = zero_entry_audit(
        {
            "all_samples": 100,
            "prerequisites_ready": 100,
            "timing": 100,
            "side": 100,
            "continuation": 100,
            "hard_gates": 100,
            "score": 100,
            "edge": 100,
            "signal": fills,
            "intent": fills,
            "fill": fills,
            "opened": fills,
            "exit_intent": fills,
            "exit_fill": fills,
            "exit": fills,
            "closed": fills,
        },
        market_count=market_count,
    )
    assert report["frequency_classification"] == expected
    assert set(report["pipeline"]) == {
        "all_samples",
        "prerequisites_ready",
        "timing",
        "side",
        "continuation",
        "hard_gates",
        "score",
        "edge",
        "signal",
        "intent",
        "fill",
        "opened",
        "exit_intent",
        "exit_fill",
        "exit",
        "closed",
    }


@pytest.mark.parametrize(
    ("price", "contracts", "expected_fee_cents"),
    (
        (Decimal("0.01"), Decimal("1"), Decimal("1.00")),
        (Decimal("0.50"), Decimal("1"), Decimal("2.00")),
        (Decimal("0.60"), Decimal("1"), Decimal("2.00")),
        (Decimal("0.99"), Decimal("1"), Decimal("1.00")),
        (Decimal("0.50"), Decimal("2"), Decimal("4.00")),
    ),
)
def test_r6_verified_taker_fee_examples_and_metadata(
    price, contracts, expected_fee_cents
) -> None:
    fee = verified_kalshi_taker_fee_model()
    assert fee.fee_cents(price=price, contracts=contracts) == expected_fee_cents
    metadata = fee.metadata()
    assert metadata["schedule_version"] == "2026-07-07"
    assert metadata["settlement_fee"] == "0"
    assert metadata["taker_formula"] == "round_up(M * 0.07 * C * P * (1-P))"


def test_r7_ordered_replay_uses_first_book_without_future_rescue() -> None:
    at = at_base()
    result = DeterministicReplayEngine().replay(
        [
            feature_event(at=at),
            orderbook_event(at=at + timedelta(milliseconds=600), event_id="first", yes_ask="0.79"),
            orderbook_event(at=at + timedelta(milliseconds=800), event_id="later", yes_ask="0.60"),
        ]
    )
    assert [trade.status for trade in result.trades] == ["ENTRY_NO_FILL"]


@pytest.mark.parametrize(
    ("changes", "expected_trigger"),
    (
        ({"held_bid": Decimal("0.48")}, "v2_hard_loss"),
        (
            {"held_bid": Decimal("0.51"), "return_5s": Decimal("-1")},
            "v2_soft_loss_with_weakening",
        ),
        ({"current_brti": Decimal("61980")}, "v2_adverse_boundary_cross"),
        ({"reversal_beyond_origin": True}, "v2_reversal_beyond_impulse_origin"),
        ({"return_5s": Decimal("-2")}, "v2_held_side_return_5s"),
        ({"return_15s": Decimal("-2")}, "v2_held_side_return_15s"),
        ({"persistent_adverse_microstructure": True}, "v2_persistent_adverse_microstructure"),
        ({"edge_lower_bound_cents": Decimal("-1")}, "v2_edge_lower_bound_nonpositive"),
        (
            {"response_residual_cents": Decimal("0.5"), "held_bid": Decimal("0.61")},
            "v2_underreaction_resolved",
        ),
        ({"held_bid": Decimal("0.71")}, "v2_profit_target"),
        ({"held_bid": Decimal("0.88")}, "v2_high_bid_target"),
        (
            {"age_seconds": 30, "score": Decimal("60")},
            "v2_tier_time_stop",
        ),
        ({"age_seconds": 60}, "v2_absolute_max_hold"),
        ({"seconds_left": 20}, "v2_final_twenty_seconds"),
        ({"market_matches": False}, "v2_force_market_lifecycle_failure"),
    ),
)
def test_r7_shared_lifecycle_helper_covers_exit_trigger_order(changes, expected_trigger) -> None:
    inputs = {
        "candidate_side": "YES",
        "boundary": Decimal("62000"),
        "current_brti": Decimal("62008"),
        "seconds_left": 360,
        "return_5s": Decimal("2"),
        "return_15s": Decimal("3"),
        "reversal_beyond_origin": False,
        "persistent_adverse_microstructure": False,
        "response_residual_cents": Decimal("6"),
        "desired_bid": Decimal("0.58"),
        "desired_bid_depth": Decimal("3"),
        "timing_tier": "normal",
        "market_matches": True,
        "entry_price": Decimal("0.60"),
        "entry_boundary": Decimal("62000"),
        "entry_side": "YES",
        "entry_score_threshold": Decimal("70"),
        "entry_time_stop_seconds": 30,
        "entry_max_hold_seconds": 60,
        "age_seconds": 1,
        "score": Decimal("88"),
        "edge_lower_bound_cents": Decimal("2"),
        "held_bid": Decimal("0.60"),
    }
    inputs.update(changes)
    parameters = (
        {"calibration_overrides": {"profit_target": "50"}}
        if expected_trigger == "v2_high_bid_target"
        else None
    )
    assert evaluate_momentum_v2_lifecycle(inputs, parameters).trigger == expected_trigger


def test_r8_chronological_partitions_are_disjoint_and_holdout_is_immutable() -> None:
    at = at_base()
    outcomes = [
        ResearchMarketOutcome(
            outcome_id=f"r8-outcome-{index:03d}",
            market_ticker=f"KXBTC15M-R8-{index:03d}",
            market_open_at=at + timedelta(minutes=15 * index),
            market_close_at=at + timedelta(minutes=15 * (index + 1)),
            resolved_at=at + timedelta(minutes=15 * (index + 1)),
            outcome_status="RESOLVED",
        )
        for index in range(100)
    ]

    manifest = build_partition_manifest(outcomes)
    repeated = build_partition_manifest(list(reversed(outcomes)))

    assert manifest["statistical_unit"] == "unique_btc15_market"
    assert len(manifest["development"]) == 80
    assert len(manifest["search_development"]) == 64
    assert len(manifest["development_test"]) == 16
    assert len(manifest["holdout"]) == 20
    assert set(manifest["search_development"]).isdisjoint(manifest["development_test"])
    assert set(manifest["development"]).isdisjoint(manifest["holdout"])
    assert manifest["holdout"] == repeated["holdout"]
    assert manifest["holdout_hash"] == repeated["holdout_hash"]
    for fold in manifest["folds"]:
        assert set(fold["train"]).isdisjoint(fold["validation"])
        assert all(
            manifest["ordered_market_tickers"].index(train)
            < manifest["ordered_market_tickers"].index(validation)
            for train in fold["train"]
            for validation in fold["validation"]
        )


def test_r9_bounded_search_and_logistic_artifacts_are_deterministic() -> None:
    candidates = bounded_candidate_specs("r9-contract")
    repeated = bounded_candidate_specs("r9-contract")
    snapshot = complete_search_space_snapshot("r9-contract", candidates)

    assert len(candidates) == 256
    assert [candidate.candidate_id for candidate in candidates] == [
        candidate.candidate_id for candidate in repeated
    ]
    assert {candidate.model_type for candidate in candidates} >= {
        "BASELINE",
        "WEIGHTED_HEURISTIC",
        "L2_LOGISTIC",
    }
    assert snapshot["maximum_candidate_count"] == 256
    assert snapshot["snapshot_sha256"] == complete_search_space_snapshot(
        "r9-contract", repeated
    )["snapshot_sha256"]
    assert candidate_parameter_grids()["normal_max_ask"] == [
        "0.70",
        "0.72",
        "0.74",
        "0.76",
        "0.78",
    ]
    rows = [
        {"return_5s": Decimal(str(index)), "timing_tier": "normal"}
        for index in range(8)
    ]
    first = fit_l2_logistic(rows, [0, 0, 0, 0, 1, 1, 1, 1], l2=1.0)
    second = fit_l2_logistic(rows, [0, 0, 0, 0, 1, 1, 1, 1], l2=1.0)
    assert first == second
    assert first["feature_columns"]
    assert first["checksum"]


def test_r10_market_normalization_bootstrap_and_penalties_are_explicit() -> None:
    dataset = synthetic_btc15_fixture_dataset()
    by_market: dict[str, list[ResearchReplayEvent]] = {}
    outcomes = {outcome.market_ticker: outcome for outcome in dataset.outcomes}
    for event in dataset.events:
        by_market.setdefault(event.market_ticker or "", []).append(event)
    closed = []
    for market_ticker in ("FIXTURE-00", "FIXTURE-03"):
        closed.extend(
            DeterministicReplayEngine()
            .replay(by_market[market_ticker], outcomes=[outcomes[market_ticker]])
            .trades
        )

    metrics = replay_metrics(
        closed,
        market_count=4,
        market_tickers=(
            "FIXTURE-00",
            "FIXTURE-03",
            "zero-market-one",
            "zero-market-two",
        ),
        calibration_run_id="r10-contract",
    )
    bootstrap = market_bootstrap({"one": Decimal("1"), "two": Decimal("-1")}, "r10-contract")
    penalties = adjusted_lower_confidence_expectancy(
        bootstrap_lower=Decimal("2"),
        changed_parameter_count=2,
        normalized_l1_weight_drift=Decimal("1"),
        fold_net_pnl_per_market=[Decimal("1"), Decimal("3")],
        dominant_regime_entry_share=Decimal("0.75"),
        mean_net_pnl_per_market=Decimal("1"),
        entries_per_100_markets=Decimal("12"),
    )

    assert metrics["closed_trade_count"] == 2
    assert Decimal(metrics["net_pnl_per_market"]) == sum(
        (trade.net_pnl_cents or Decimal("0")) for trade in closed
    ) / Decimal("4")
    assert {"zero-market-one", "zero-market-two"} <= set(metrics["market_net_pnl"])
    assert bootstrap["resamples"] == "2000"
    assert penalties["adjusted_lower_confidence_expectancy"] < Decimal("2")


def test_r11_only_qualified_candidates_can_reach_dry_run_challenger() -> None:
    qualified = {
        "complete_unique_markets": 500,
        "closed_simulated_trades": 50,
        "entry_frequency_per_100_markets_min": "3",
        "entry_frequency_per_100_markets": "5",
        "signal_to_fill_rate": "0.75",
        "complete_replay_coverage": "0.99",
        "volatility_regimes": 3,
        "liquidity_regimes": 3,
        "timing_tiers": 3,
        "holdout_mean_net_pnl_per_market": "1",
        "holdout_lower_95": "0.5",
        "adjusted_lower_confidence_expectancy": "0.25",
        "dominant_regime_entry_share": "0.5",
        "max_drawdown_per_100_markets": "10",
        "verified_fee_model": True,
        "beats_baseline": True,
        "forbidden_parameter_changed": False,
        "safety_or_data_quality_gate_changed": False,
    }

    assert transition_candidate(
        from_state=LIFECYCLE_DRAFT, to_state=LIFECYCLE_BACKTESTED, evidence={}
    )[0] == LIFECYCLE_BACKTESTED
    assert transition_candidate(
        from_state=LIFECYCLE_BACKTESTED, to_state=LIFECYCLE_SHADOW, evidence={}
    )[0] == LIFECYCLE_SHADOW
    assert transition_candidate(
        from_state=LIFECYCLE_SHADOW,
        to_state=LIFECYCLE_DRY_RUN_CHALLENGER,
        evidence=qualified,
    )[0] == LIFECYCLE_DRY_RUN_CHALLENGER
    with pytest.raises(GovernanceError, match="closed_simulated_trades"):
        transition_candidate(
            from_state=LIFECYCLE_SHADOW,
            to_state=LIFECYCLE_DRY_RUN_CHALLENGER,
            evidence={**qualified, "closed_simulated_trades": 49},
        )
    for prohibited in (LIFECYCLE_PAPER_CANDIDATE, "LIVE_CANDIDATE"):
        with pytest.raises(GovernanceError, match="paper or live"):
            transition_candidate(
                from_state=LIFECYCLE_DRAFT, to_state=prohibited, evidence=qualified
            )


def test_r12_candidate_pin_resolves_once_per_observer_lifetime(
    tmp_path, monkeypatch
) -> None:
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'r12.sqlite'}",
            "APP_MODE": "DRY_RUN",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STRATEGY_V2_CANDIDATE_CONFIG_VERSION_ID": "candidate-config",
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    at = at_base()
    first = PinnedCandidate("candidate-first", "candidate-config", {}, "first")
    second = PinnedCandidate("candidate-second", "candidate-config", {}, "second")
    resolved: list[tuple[PinnedCandidate | None, str | None]] = [(first, None)]
    resolver_calls: list[tuple[PinnedCandidate | None, str | None]] = []
    observed: list[tuple[PinnedCandidate | None, str | None]] = []

    def fake_resolve(*_args):
        resolver_calls.append(resolved[0])
        return resolved[0]

    def fake_variants(**kwargs):
        observed.append((kwargs["pinned_candidate"], kwargs["pin_blocker"]))
        return [
            (
                kwargs["config"],
                StrategyDecisionInput(
                    decision_id=f"r12-{len(observed)}",
                    evaluated_at=at,
                    decision_state="OBSERVE_ONLY_MARKET",
                    primary_reason="fixture",
                    app_mode="DRY_RUN",
                    strategy_id="btc15_momentum_v1",
                ),
            )
        ]

    monkeypatch.setattr(observer_module, "resolve_pinned_candidate", fake_resolve)
    monkeypatch.setattr(observer_module, "evaluate_strategy_variants", fake_variants)
    monkeypatch.setattr(
        observer_module, "_apply_dry_run_ledger", lambda **_kwargs: DryRunLedgerResult()
    )
    try:
        observer = StrategyObserver(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=session_factory,
            started_at=at,
            now=lambda: at,
        )
        observer.evaluate_once()
        resolved[0] = (second, None)
        observer.evaluate_once()
        resolved[0] = (None, "candidate_pin_missing")
        observer.evaluate_once()

        assert resolver_calls == [(first, None)]
        assert observed == [(first, None), (first, None), (first, None)]

        restarted_observer = StrategyObserver(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=session_factory,
            started_at=at,
            now=lambda: at,
        )
        restarted_observer.evaluate_once()
        assert resolver_calls == [(first, None), (None, "candidate_pin_missing")]
        assert observed[-1] == (None, "candidate_pin_missing")
    finally:
        engine.dispose()


def test_r13_research_api_surface_is_read_only_and_bounded(tmp_path) -> None:
    config = load_config({"DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'r13.sqlite'}"})
    engine = create_engine_from_config(config)
    run_migrations(engine)
    try:
        app = create_app(config)
        routes = [
            route for route in app.routes if getattr(route, "path", "").startswith("/research/")
        ]
        assert len(routes) == 10
        assert all(route.methods == {"GET"} for route in routes)
        with TestClient(app) as client:
            assert client.get("/research/status").status_code == 200
            assert client.get("/research/cohorts/latest").status_code == 200
            assert (
                client.get("/research/calibration/frontier/latest?limit=21").status_code
                == 422
            )
            assert client.get("/research/replay/runs/recent?limit=501").status_code == 422
            assert (
                client.get("/research/replay/trades/recent?candidate_id=bad%20id").status_code
                == 422
            )
    finally:
        engine.dispose()


def test_r14_retention_and_durable_status_tables_are_separate(tmp_path) -> None:
    retained = {policy.table_name for policy in RETENTION_POLICIES}
    status = {table.table_name for table in STATUS_TABLES}
    assert {"research_replay_events", "research_replay_trades"} <= retained
    assert {
        "research_market_outcomes",
        "research_replay_runs",
        "calibration_runs",
        "research_candidates",
        "research_governance_events",
    } <= status
    assert "research_market_outcomes" not in ALLOWED_RETENTION_TABLES
    assert "research_market_outcomes" in ALLOWED_STATUS_READ_TABLES
    engine = create_engine_from_config(
        load_config({"DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'r14.sqlite'}"})
    )
    run_migrations(engine)
    factory = create_session_factory(engine)
    try:
        with factory() as session:
            repository = StorageRetentionRepository(session)
            for method, kwargs in (
                (
                    repository.count_matching,
                    {"condition_sql": "closed_at < :cutoff", "parameters": {"cutoff": at_base()}},
                ),
                (
                    repository.has_matching,
                    {"condition_sql": "closed_at < :cutoff", "parameters": {"cutoff": at_base()}},
                ),
                (
                    repository.delete_batch,
                    {
                        "condition_sql": "closed_at < :cutoff",
                        "parameters": {"cutoff": at_base()},
                        "batch_size": 1,
                    },
                ),
                (
                    repository.strip_raw_payload_batch,
                    {
                        "condition_sql": "closed_at < :cutoff",
                        "parameters": {"cutoff": at_base()},
                        "batch_size": 1,
                    },
                ),
            ):
                with pytest.raises(ValueError, match="Unsupported retention table"):
                    method(table_name="strategy_position_outcomes", **kwargs)
            with pytest.raises(ValueError, match="Unsupported raw payload storage table"):
                repository.raw_payload_non_null_count("strategy_position_outcomes")
    finally:
        engine.dispose()


def test_r15_documentation_versions_and_safety_contract_are_present() -> None:
    compliance = (ROOT / "docs/PR11_COMPLIANCE.md").read_text()
    assert "# PR 11 Compliance Matrix" in compliance
    assert all(f"| R{requirement} " in compliance for requirement in range(1, 16))
    assert "Promotion evidence is derived from persisted source events" in compliance
    assert "Regenerated raw logs, JUnit XML, result JSON" in compliance
    assert "does not add paper trading" in compliance
    assert (ROOT / "docs/RESEARCH_AND_CALIBRATION.md").exists()
    assert "ape-research-worker" in (ROOT / "docs/RAILWAY.md").read_text()
    assert (
        REPLAY_SCHEMA_VERSION,
        RESEARCH_LABEL_SCHEMA_VERSION,
        CALIBRATION_SCHEMA_VERSION,
        GOVERNANCE_SCHEMA_VERSION,
        V2_FEATURE_SCHEMA_VERSION,
    ) == (
        "momentum_v2_replay_v1",
        "momentum_research_labels_v1",
        "momentum_calibration_v1",
        "momentum_governance_v1",
        "momentum_v2_features_v3",
    )
    fixtures = synthetic_btc15_fixture_markets()
    assert len(fixtures) >= 18
    assert {row.quality_flags["volatility_regime"] for row in fixtures} == {
        "low",
        "medium",
        "high",
    }


def test_r15_eighteen_market_fixture_has_real_event_time_sources_and_labels() -> None:
    dataset = synthetic_btc15_fixture_dataset()
    expected_types = {
        "MARKET",
        "REFERENCE",
        "ORDERBOOK",
        "PUBLIC_TRADE",
        "FEATURE_SNAPSHOT",
        "MARKET_LIFECYCLE",
    }
    by_market = {}
    for event in dataset.events:
        by_market.setdefault(event.market_ticker, []).append(event)

    assert len(dataset.outcomes) == 18
    assert set(by_market) == {outcome.market_ticker for outcome in dataset.outcomes}
    assert all(
        {event.event_type for event in events} >= expected_types
        for events in by_market.values()
    )
    assert all(
        outcome.quality_flags["counterfactual_labels"] for outcome in dataset.outcomes
    )
    assert {
        outcome.quality_flags["scenario"] for outcome in dataset.outcomes
    } >= {
        "continuation_entry",
        "boundary_cross_research_only",
        "later_book_non_rescue",
        "exit_retry_exhaustion",
        "partial_coverage_frozen_holdout",
    }


def test_r7_r15_fixture_scenarios_trigger_real_replay_outcomes() -> None:
    dataset = synthetic_btc15_fixture_dataset()
    by_market: dict[str, list[ResearchReplayEvent]] = {}
    outcomes = {outcome.market_ticker: outcome for outcome in dataset.outcomes}
    for event in dataset.events:
        by_market.setdefault(event.market_ticker or "", []).append(event)

    observed_exit_reasons: set[str] = set()
    for market_ticker, expectation in dataset.scenario_expectations.items():
        result = DeterministicReplayEngine().replay(
            by_market[market_ticker], outcomes=[outcomes[market_ticker]]
        )
        decision = result.decisions[0] if result.decisions else None
        assert (decision.state if decision is not None else None) == expectation["decision_state"]
        if "decision_reason" in expectation:
            assert decision is not None
            assert decision.reason == expectation["decision_reason"]
        assert [trade.status for trade in result.trades] == expectation["trade_statuses"]
        if "exit_reason" in expectation:
            assert result.trades[0].exit_reason == expectation["exit_reason"]
            observed_exit_reasons.add(result.trades[0].exit_reason or "")
        if "exit_intents" in expectation:
            assert (
                result.zero_entry_report["pipeline"]["exit_intent"]
                == expectation["exit_intents"]
            )
        if expectation.get("later_book_cannot_rescue"):
            assert result.trades[0].entry_fill_at is None
        if expectation.get("position_open_after_exhaustion"):
            assert result.zero_entry_report["pipeline"]["closed"] == 0
        if expectation.get("official_settlement"):
            assert result.trades[0].exit_fill_event_id is None

    assert {
        "v2_hard_loss",
        "v2_soft_loss_with_weakening",
        "v2_profit_target",
        "v2_tier_time_stop",
        "v2_absolute_max_hold",
        "v2_final_twenty_seconds",
        "v2_adverse_boundary_cross",
        "v2_reversal_beyond_impulse_origin",
        "v2_persistent_adverse_microstructure",
        "SETTLEMENT",
    } <= observed_exit_reasons


def _replay_event(
    source_row_id: str,
    *,
    event_id: str | None = None,
) -> ResearchReplayEvent:
    at = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)
    return ResearchReplayEvent(
        event_id=event_id or source_row_id,
        market_ticker="R1",
        event_type="MARKET",
        event_time=at,
        received_at=at,
        source_table="r1_source",
        source_row_id=source_row_id,
        replay_schema_version=REPLAY_SCHEMA_VERSION,
        payload={},
        event_hash=source_row_id,
        replay_readiness="FULL",
    )


def _json_safe_vector(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe_vector(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe_vector(item) for item in value]
    return value


def _evaluation_contract(result) -> tuple[object, ...]:
    return (
        result.state,
        result.reason,
        result.blockers,
        result.warnings,
        result.candidate_side,
        result.candidate_mode,
        result.timing_tier,
        result.score,
        result.score_threshold,
        result.edge_lower_bound_cents,
        result.intended_entry_price,
    )
