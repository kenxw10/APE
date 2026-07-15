from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import func, select
from tests.test_research_helpers import json_vector, valid_vector

import ape.research.governed_calibration as governed_calibration
from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.models import (
    ResearchCandidate,
    ResearchMarketOutcome,
    ResearchReplayEvent,
    ResearchReplayTrade,
)
from ape.db.session import create_engine_from_config, create_session_factory
from ape.research import REPLAY_SCHEMA_VERSION, RESEARCH_LABEL_SCHEMA_VERSION
from ape.research.calibration import (
    CalibrationResult,
    CandidateSpec,
    bounded_candidate_specs,
    build_partition_manifest,
)
from ape.research.cohort import build_clean_calibration_cohort
from ape.research.governed_calibration import (
    CALIBRATION_CANDIDATE_BATCH_SIZE,
    build_candidate_frontier,
    classify_calibration_result,
    run_governed_calibration,
)
from ape.research.replay import ReplayTrade
from ape.research.repository import ResearchRepository
from ape.strategy.momentum_v2 import (
    V2_ARCHITECTURE_VERSION,
    V2_FEATURE_SCHEMA_VERSION,
    V2_PARAMETERS,
)


def _factory(tmp_path, name: str):
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / name}",
            "APP_MODE": "DRY_RUN",
            "RESEARCH_ENABLED": "true",
            "CALIBRATION_ENABLED": "true",
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    return engine, create_session_factory(engine)


def _event(
    *,
    event_id: str,
    market: str | None,
    kind: str,
    at: datetime,
    feature_schema: str | None = None,
    architecture: str | None = None,
    replay_schema: str = REPLAY_SCHEMA_VERSION,
    readiness: str = "FULL",
    payload: dict[str, Any] | None = None,
    feature_snapshot_id: str | None = None,
) -> ResearchReplayEvent:
    return ResearchReplayEvent(
        event_id=event_id,
        market_ticker=market,
        event_type=kind,
        event_time=at,
        received_at=at,
        source_table=f"fixture_{kind.lower()}",
        source_row_id=event_id,
        source_hash=f"source-{event_id}",
        sequence_number=None,
        feature_snapshot_id=feature_snapshot_id,
        feature_schema_version=feature_schema,
        architecture_version=architecture,
        replay_schema_version=replay_schema,
        payload=payload or {},
        event_hash=f"hash-{event_id}",
        replay_readiness=readiness,
        blockers=[],
    )


def seed_clean_market(
    session,
    *,
    index: int,
    at: datetime,
    ticker: str | None = None,
    include: frozenset[str] = frozenset(
        {"MARKET", "REFERENCE", "FEATURE_SNAPSHOT", "ORDERBOOK"}
    ),
    outcome_status: str = "RESOLVED",
    architecture: str = V2_ARCHITECTURE_VERSION,
    feature_schema: str = V2_FEATURE_SCHEMA_VERSION,
    replay_schema: str = REPLAY_SCHEMA_VERSION,
    readiness: str = "FULL",
    vector: dict[str, Any] | None = None,
    mature_label: bool = True,
    book_delay_ms: int = 600,
) -> tuple[str, str]:
    ticker = ticker or f"KXBTC15M-CLEAN-{index:04d}"
    feature_id = f"feature-{ticker}"
    feature_at = at + timedelta(seconds=10)
    book_at = feature_at + timedelta(milliseconds=book_delay_ms)
    label = {
        "entry_fillable": True,
        "entry_at": book_at.isoformat(),
        "net_markout_30s_cents": "1",
    }
    quality_flags = {
        "label_schema_version": RESEARCH_LABEL_SCHEMA_VERSION,
        "counterfactual_labels": {feature_id: label if mature_label else {}},
    }
    session.add(
        ResearchMarketOutcome(
            outcome_id=f"outcome-{ticker}",
            market_ticker=ticker,
            market_open_at=at,
            market_close_at=at + timedelta(minutes=15),
            expiration_at=at + timedelta(minutes=15),
            boundary=Decimal("62000"),
            result_side="YES" if outcome_status == "RESOLVED" else None,
            settlement_value=Decimal("1") if outcome_status == "RESOLVED" else None,
            final_reference_value=Decimal("62100"),
            final_minute_reference_average=Decimal("62100"),
            outcome_status=outcome_status,
            outcome_source="fixture",
            source_payload_hash=f"outcome-hash-{ticker}",
            resolved_at=at + timedelta(minutes=15)
            if outcome_status == "RESOLVED"
            else None,
            expected_frame_count=4,
            actual_frame_count=len(include),
            coverage_percentage=Decimal("1"),
            maximum_event_gap_seconds=10,
            quality_flags=quality_flags,
        )
    )
    events = {
        "MARKET": _event(
            event_id=f"market-{ticker}", market=ticker, kind="MARKET", at=at
        ),
        "REFERENCE": _event(
            event_id=f"reference-{ticker}",
            market=ticker,
            kind="REFERENCE",
            at=feature_at - timedelta(seconds=1),
            payload={"parsed_value": "62100"},
        ),
        "FEATURE_SNAPSHOT": _event(
            event_id=feature_id,
            market=ticker,
            kind="FEATURE_SNAPSHOT",
            at=feature_at,
            feature_schema=feature_schema,
            architecture=architecture,
            replay_schema=replay_schema,
            readiness=readiness,
            payload={"feature_vector": json_vector(vector or valid_vector())},
            feature_snapshot_id=feature_id,
        ),
        "ORDERBOOK": _event(
            event_id=f"book-{ticker}",
            market=ticker,
            kind="ORDERBOOK",
            at=book_at,
            payload={
                "yes_ask": "0.60",
                "yes_bid": "0.58",
                "yes_ask_size": "3",
                "yes_bid_size": "3",
            },
        ),
    }
    session.add_all(events[kind] for kind in include)
    return ticker, feature_id


def _candidate(candidate_id: str, *, baseline: bool = False) -> CandidateSpec:
    parameters = deepcopy(V2_PARAMETERS)
    if not baseline:
        parameters["edge_threshold_cents"] = "1.00"
    return CandidateSpec(
        candidate_id,
        f"strategy-{candidate_id}",
        "BASELINE" if baseline else "WEIGHTED_HEURISTIC",
        parameters,
    )


def fake_candidate_evaluator(
    *,
    calibration_run_id: str,
    events,
    outcomes,
    candidate_specs,
    evaluate_finalist: bool = True,
    progress_callback=None,
) -> CalibrationResult:
    del events
    candidates = tuple(candidate_specs)
    manifest = build_partition_manifest(outcomes)
    metrics: dict[str, dict[str, Any]] = {}
    for index, candidate in enumerate(candidates):
        if progress_callback is not None:
            progress_callback(
                {
                    "candidate_index": index,
                    "current_candidate_id": candidate.candidate_id,
                    "current_partition": "search_walk_forward",
                }
            )
        signal_count = 0 if candidate.model_type == "BASELINE" else 1
        metrics[candidate.candidate_id] = {
            "status": "EVALUATED",
            "candidate_id": candidate.candidate_id,
            "model_type": candidate.model_type,
            "entry_signal_count": signal_count,
            "entry_intent_count": signal_count,
            "executable_entry_fill_count": signal_count,
            "closed_position_count": signal_count,
            "net_pnl_cents": str(signal_count),
            "net_pnl_per_market": str(Decimal(signal_count) / Decimal(50)),
            "entry_frequency_per_100_markets": str(signal_count * 2),
            "signal_to_fill_rate": str(signal_count),
            "bootstrap": {
                "net_pnl_per_market": {
                    "lower": str(signal_count),
                    "upper": str(signal_count),
                    "mean": str(signal_count),
                }
            },
            "penalties": {
                "adjusted_lower_confidence_expectancy": str(signal_count)
            },
            "walk_forward_validation": {"fold_count": 5},
            "development_test": None,
            "holdout": None,
        }
        if evaluate_finalist:
            metrics[candidate.candidate_id]["development_test"] = {
                "net_pnl_per_market": "1"
            }
            metrics[candidate.candidate_id]["holdout"] = {
                "net_pnl_per_market": "1",
                "bootstrap": {
                    "net_pnl_per_market": {"lower": "1", "upper": "1", "mean": "1"}
                },
            }
    selected = sorted(
        candidates,
        key=lambda item: (item.model_type == "BASELINE", item.candidate_id),
    )[0].candidate_id
    return CalibrationResult(
        "COMPLETED",
        manifest,
        candidates,
        metrics,
        selected,
        (),
        (),
        {candidate.candidate_id: () for candidate in candidates},
        {
            candidate.candidate_id: {"search_development": ()}
            for candidate in candidates
        },
    )


def test_clean_cohort_excludes_incompatible_evidence_with_explicit_reasons(
    tmp_path,
) -> None:
    engine, factory = _factory(tmp_path, "cohort-reasons.sqlite")
    at = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    try:
        with factory() as session:
            valid, _ = seed_clean_market(session, index=0, at=at)
            seed_clean_market(
                session, index=1, at=at + timedelta(minutes=15), include=frozenset({"MARKET"})
            )
            seed_clean_market(
                session,
                index=2,
                at=at + timedelta(minutes=30),
                include=frozenset({"MARKET", "FEATURE_SNAPSHOT", "ORDERBOOK"}),
            )
            seed_clean_market(
                session,
                index=3,
                at=at + timedelta(minutes=45),
                architecture="old-architecture",
            )
            seed_clean_market(
                session,
                index=4,
                at=at + timedelta(minutes=60),
                feature_schema="old-feature-schema",
            )
            seed_clean_market(
                session,
                index=5,
                at=at + timedelta(minutes=75),
                replay_schema="old-replay-schema",
            )
            seed_clean_market(
                session,
                index=6,
                at=at + timedelta(minutes=90),
                outcome_status="PENDING",
            )
            seed_clean_market(
                session,
                index=7,
                at=at + timedelta(minutes=105),
                mature_label=False,
            )
            seed_clean_market(
                session,
                index=8,
                at=at + timedelta(minutes=120),
                book_delay_ms=3000,
            )
            seed_clean_market(
                session,
                index=9,
                at=at + timedelta(minutes=135),
                readiness="PARTIAL",
            )
            unusable, _ = seed_clean_market(
                session, index=10, at=at + timedelta(minutes=150)
            )
            unusable_feature = session.scalar(
                select(ResearchReplayEvent).where(
                    ResearchReplayEvent.event_id == f"feature-{unusable}"
                )
            )
            unusable_feature.payload = {"feature_vector": {}}
            session.add(
                _event(
                    event_id="unassociated-feature",
                    market=None,
                    kind="FEATURE_SNAPSHOT",
                    at=at,
                    feature_schema=V2_FEATURE_SCHEMA_VERSION,
                    architecture=V2_ARCHITECTURE_VERSION,
                    payload={"feature_vector": json_vector(valid_vector())},
                    feature_snapshot_id="unassociated-feature",
                )
            )
            session.commit()
            repository = ResearchRepository(session)
            snapshot = repository.replay_event_snapshot()
            cohort = build_clean_calibration_cohort(
                session,
                snapshot=snapshot,
                baseline_config_version_id="baseline",
                code_commit_sha="code",
            )
            repeated = build_clean_calibration_cohort(
                session,
                snapshot=snapshot,
                baseline_config_version_id="baseline",
                code_commit_sha="code",
            )

        assert cohort.manifest == repeated.manifest
        assert cohort.manifest["ordered_eligible_market_tickers"] == [valid]
        exclusions = cohort.manifest["exclusion_counts_by_reason"]
        for reason in (
            "unassociated_feature_rows",
            "wrong_architecture_versions",
            "wrong_feature_schema_versions",
            "wrong_replay_schema_versions",
            "unresolved_feature_markets",
            "immature_labels",
            "missing_first_book_windows",
            "partial_feature_vectors",
            "unusable_feature_vectors",
        ):
            assert exclusions[reason] >= 1
        excluded_markets = cohort.manifest["excluded_market_counts_by_reason"]
        assert excluded_markets["market_only_history"] == 1
        assert excluded_markets["missing_required_sources"] >= 1
        assert cohort.manifest["cohort_hash"]
    finally:
        engine.dispose()


def test_governed_epochs_resume_batches_and_consume_holdout_once(tmp_path) -> None:
    engine, factory = _factory(tmp_path, "epoch-resume.sqlite")
    at = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    candidates = tuple(
        [_candidate("candidate-baseline-v2", baseline=True)]
        + [_candidate(f"candidate-{index:02d}") for index in range(1, 10)]
    )
    calls: list[tuple[str, ...]] = []
    fail_second_batch = True

    def evaluator(**kwargs):
        nonlocal fail_second_batch
        ids = tuple(candidate.candidate_id for candidate in kwargs["candidate_specs"])
        calls.append(ids)
        if fail_second_batch and len(calls) == 2:
            raise RuntimeError("simulated batch interruption")
        return fake_candidate_evaluator(**kwargs)

    try:
        with factory() as session:
            for index in range(50):
                seed_clean_market(
                    session,
                    index=index,
                    at=at + timedelta(minutes=15 * index),
                )
            session.commit()
            snapshot = ResearchRepository(session).replay_event_snapshot()
            try:
                run_governed_calibration(
                    session,
                    snapshot=snapshot,
                    replay_run_id="baseline-replay",
                    baseline_config_version_id="baseline-config",
                    code_commit_sha="code",
                    checked_at=at,
                    candidate_evaluator=evaluator,
                    candidate_specs=candidates,
                )
            except RuntimeError as exc:
                assert str(exc) == "simulated batch interruption"
            run = ResearchRepository(session).latest_calibration_run()
            assert run is not None
            assert run.evaluated_candidate_count == CALIBRATION_CANDIDATE_BATCH_SIZE
            assert session.scalar(select(func.count()).select_from(ResearchCandidate)) == 7
            session.commit()

        fail_second_batch = False
        with factory() as session:
            snapshot = ResearchRepository(session).replay_event_snapshot()
            completed = run_governed_calibration(
                session,
                snapshot=snapshot,
                replay_run_id="baseline-replay",
                baseline_config_version_id="baseline-config",
                code_commit_sha="code",
                checked_at=at + timedelta(hours=1),
                candidate_evaluator=evaluator,
                candidate_specs=candidates,
            )
            assert completed.candidates_completed == len(candidates)
            assert completed.classification == "POSITIVE_RESEARCH_CANDIDATE"
            completed_run = ResearchRepository(session).get_calibration_run(completed.run_id)
            assert completed_run is not None
            holdout_used_at = completed_run.holdout_used_at
            assert holdout_used_at is not None
            candidate_rows = list(session.scalars(select(ResearchCandidate)))
            assert all(row.lifecycle_state == "DRAFT" for row in candidate_rows)
            assert all(row.eligibility_status == "RESEARCH_ONLY" for row in candidate_rows)
            assert session.scalar(select(func.count()).select_from(ResearchReplayTrade)) == 0

        calls_before_reuse = len(calls)
        with factory() as session:
            snapshot = ResearchRepository(session).replay_event_snapshot()
            reused = run_governed_calibration(
                session,
                snapshot=snapshot,
                replay_run_id="new-tail-baseline",
                baseline_config_version_id="baseline-config",
                code_commit_sha="code",
                checked_at=at + timedelta(hours=2),
                candidate_evaluator=evaluator,
                candidate_specs=candidates,
            )
            assert reused.reused_existing_run is True
            assert len(calls) == calls_before_reuse
            run = ResearchRepository(session).get_calibration_run(reused.run_id)
            assert run is not None
            assert run.holdout_used_at == holdout_used_at
    finally:
        engine.dispose()


def test_candidate_trades_are_partitioned_and_idempotent_across_reuse(tmp_path) -> None:
    engine, factory = _factory(tmp_path, "trade-evidence.sqlite")
    at = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    candidates = (
        _candidate("candidate-baseline-v2", baseline=True),
        _candidate("candidate-trade-evidence"),
    )
    trade = ReplayTrade(
        trade_id="closed-trade",
        market_ticker="KXBTC15M-CLEAN-0000",
        side="YES",
        entry_decision_at=at,
        entry_fill_at=at + timedelta(milliseconds=600),
        entry_limit=Decimal("0.60"),
        entry_fill_price=Decimal("0.60"),
        entry_fill_event_id="entry-book",
        exit_trigger_at=at + timedelta(seconds=10),
        exit_intent_at=at + timedelta(seconds=10, milliseconds=500),
        exit_limit=Decimal("0.64"),
        exit_fill_at=at + timedelta(seconds=11),
        exit_fill_price=Decimal("0.65"),
        exit_fill_event_id="exit-book",
        status="CLOSED",
        gross_pnl_cents=Decimal("5"),
        fee_cents=Decimal("1"),
        net_pnl_cents=Decimal("4"),
        holding_duration_ms=10400,
        mfe_cents=Decimal("5"),
        mae_cents=Decimal("0"),
        time_to_mfe_ms=10400,
        time_to_mae_ms=0,
        entry_reason="fixture",
        exit_reason="fixture",
        timing_tier="normal",
        measurements={"volatility_regime": "medium", "liquidity_regime": "deep"},
    )

    def evaluator(**kwargs):
        result = fake_candidate_evaluator(**kwargs)
        partitioned = {}
        retained = {}
        for candidate in result.candidates:
            rows = (trade,) if candidate.model_type != "BASELINE" else ()
            retained[candidate.candidate_id] = rows
            partitioned[candidate.candidate_id] = {"search_development": rows}
            if kwargs.get("evaluate_finalist", True) and rows:
                partitioned[candidate.candidate_id].update(
                    {"development_test": rows, "frozen_holdout": rows}
                )
        return CalibrationResult(
            result.status,
            result.partition_manifest,
            result.candidates,
            result.candidate_metrics,
            result.selected_candidate_id,
            result.warnings,
            result.blockers,
            retained,
            partitioned,
        )

    try:
        with factory() as session:
            for index in range(50):
                seed_clean_market(
                    session, index=index, at=at + timedelta(minutes=15 * index)
                )
            session.commit()
            snapshot = ResearchRepository(session).replay_event_snapshot()
            first = run_governed_calibration(
                session,
                snapshot=snapshot,
                replay_run_id="baseline-trades",
                baseline_config_version_id="baseline",
                code_commit_sha="code",
                checked_at=at,
                candidate_evaluator=evaluator,
                candidate_specs=candidates,
            )
            before = session.scalar(select(func.count()).select_from(ResearchReplayTrade))
            reused = run_governed_calibration(
                session,
                snapshot=snapshot,
                replay_run_id="baseline-trades",
                baseline_config_version_id="baseline",
                code_commit_sha="code",
                checked_at=at + timedelta(hours=1),
                candidate_evaluator=evaluator,
                candidate_specs=candidates,
            )
            after = session.scalar(select(func.count()).select_from(ResearchReplayTrade))
            rows = list(session.scalars(select(ResearchReplayTrade)))
        assert first.classification == "POSITIVE_RESEARCH_CANDIDATE"
        assert reused.reused_existing_run is True
        assert before == after == 3
        assert {row.measurements["evidence_partition"] for row in rows} == {
            "search_development",
            "development_test",
            "frozen_holdout",
        }
        assert all(row.fee_cents == Decimal("1") for row in rows)
    finally:
        engine.dispose()


def test_finalist_evidence_recovers_after_fault_without_duplicate_evaluation_or_trades(
    tmp_path, monkeypatch
) -> None:
    at = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    candidates = (
        _candidate("candidate-baseline-v2", baseline=True),
        _candidate("candidate-finalist-recovery"),
    )
    trade = ReplayTrade(
        trade_id="finalist-recovery-trade",
        market_ticker="KXBTC15M-CLEAN-0000",
        side="YES",
        entry_decision_at=at,
        entry_fill_at=at + timedelta(milliseconds=600),
        entry_limit=Decimal("0.60"),
        entry_fill_price=Decimal("0.60"),
        entry_fill_event_id="entry-book",
        exit_trigger_at=at + timedelta(seconds=10),
        exit_intent_at=at + timedelta(seconds=10, milliseconds=500),
        exit_limit=Decimal("0.64"),
        exit_fill_at=at + timedelta(seconds=11),
        exit_fill_price=Decimal("0.65"),
        exit_fill_event_id="exit-book",
        status="CLOSED",
        gross_pnl_cents=Decimal("5"),
        fee_cents=Decimal("1"),
        net_pnl_cents=Decimal("4"),
        holding_duration_ms=10400,
        mfe_cents=Decimal("5"),
        mae_cents=Decimal("0"),
        time_to_mfe_ms=10400,
        time_to_mae_ms=0,
        entry_reason="fixture",
        exit_reason="fixture",
        timing_tier="normal",
        measurements={"volatility_regime": "medium", "liquidity_regime": "deep"},
    )
    evaluator_modes: list[bool] = []

    def evaluator(**kwargs):
        evaluate_finalist = bool(kwargs.get("evaluate_finalist", True))
        evaluator_modes.append(evaluate_finalist)
        result = fake_candidate_evaluator(**kwargs)
        retained = {}
        partitioned = {}
        for candidate in result.candidates:
            rows = (trade,) if candidate.model_type != "BASELINE" else ()
            retained[candidate.candidate_id] = rows
            partitioned[candidate.candidate_id] = {"search_development": rows}
            if evaluate_finalist and rows:
                partitioned[candidate.candidate_id].update(
                    {"development_test": rows, "frozen_holdout": rows}
                )
        return CalibrationResult(
            result.status,
            result.partition_manifest,
            result.candidates,
            result.candidate_metrics,
            result.selected_candidate_id,
            result.warnings,
            result.blockers,
            retained,
            partitioned,
        )

    frontier_failure = True
    original_frontier = governed_calibration.build_candidate_frontier

    def fail_once(*args, **kwargs):
        nonlocal frontier_failure
        if frontier_failure:
            frontier_failure = False
            raise RuntimeError("simulated finalist finalization failure")
        return original_frontier(*args, **kwargs)

    monkeypatch.setattr(governed_calibration, "build_candidate_frontier", fail_once)

    def seed_and_interrupt(factory, replay_run_id: str) -> None:
        with factory() as session:
            for index in range(50):
                seed_clean_market(
                    session, index=index, at=at + timedelta(minutes=15 * index)
                )
            session.commit()
            with pytest.raises(RuntimeError, match="simulated finalist finalization failure"):
                run_governed_calibration(
                    session,
                    snapshot=ResearchRepository(session).replay_event_snapshot(),
                    replay_run_id=replay_run_id,
                    baseline_config_version_id="baseline",
                    code_commit_sha="code",
                    checked_at=at,
                    candidate_evaluator=evaluator,
                    candidate_specs=candidates,
                )

    engine, factory = _factory(tmp_path, "finalist-recovery.sqlite")
    try:
        seed_and_interrupt(factory, "finalist-recovery")
        frontier_failure = False
        with factory() as session:
            repository = ResearchRepository(session)
            interrupted = repository.latest_calibration_run()
            assert interrupted is not None
            holdout_used_at = interrupted.holdout_used_at
            frozen_holdout_hash = interrupted.frozen_holdout_hash
            selected_candidate_id = interrupted.selected_candidate_id
            validation_metrics = deepcopy(interrupted.validation_metrics)
            trade_count_before = session.scalar(
                select(func.count()).select_from(ResearchReplayTrade)
            )

        with factory() as session:
            resumed = run_governed_calibration(
                session,
                snapshot=ResearchRepository(session).replay_event_snapshot(),
                replay_run_id="finalist-recovery-retry",
                baseline_config_version_id="baseline",
                code_commit_sha="code",
                checked_at=at + timedelta(hours=1),
                candidate_evaluator=evaluator,
                candidate_specs=candidates,
            )
            recovered = ResearchRepository(session).get_calibration_run(resumed.run_id)
            assert recovered is not None
            trade_count_after = session.scalar(
                select(func.count()).select_from(ResearchReplayTrade)
            )

        assert resumed.reused_existing_run is True
        assert recovered.status == "POSITIVE_RESEARCH_CANDIDATE"
        assert recovered.finished_at is not None
        assert recovered.holdout_used_at == holdout_used_at
        assert recovered.frozen_holdout_hash == frozen_holdout_hash
        assert recovered.selected_candidate_id == selected_candidate_id
        assert recovered.validation_metrics == validation_metrics
        assert trade_count_after == trade_count_before == 3
        assert evaluator_modes == [False, True]
    finally:
        engine.dispose()

    frontier_failure = True
    engine, factory = _factory(tmp_path, "finalist-recovery-incomplete.sqlite")
    try:
        seed_and_interrupt(factory, "finalist-recovery-incomplete")
        with factory() as session:
            interrupted = ResearchRepository(session).latest_calibration_run()
            assert interrupted is not None
            interrupted.validation_metrics = {}
            session.commit()
        modes_before_recovery = len(evaluator_modes)
        with factory() as session:
            failed = run_governed_calibration(
                session,
                snapshot=ResearchRepository(session).replay_event_snapshot(),
                replay_run_id="finalist-recovery-incomplete-retry",
                baseline_config_version_id="baseline",
                code_commit_sha="code",
                checked_at=at + timedelta(hours=1),
                candidate_evaluator=evaluator,
                candidate_specs=candidates,
            )
            recovered = ResearchRepository(session).get_calibration_run(failed.run_id)
            assert recovered is not None
        assert failed.reused_existing_run is True
        assert failed.status == "CALIBRATION_FAILED"
        assert recovered.finished_at is not None
        assert recovered.blockers == ["finalist_evidence_incomplete_after_holdout"]
        assert len(evaluator_modes) == modes_before_recovery
    finally:
        engine.dispose()


def test_failed_calibration_retry_resets_state_and_reuses_run_replay_id(tmp_path, monkeypatch):
    at = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    candidates = (
        _candidate("candidate-baseline-v2", baseline=True),
        _candidate("candidate-retry"),
    )
    engine, factory = _factory(tmp_path, "calibration-retry-reset.sqlite")
    try:
        with factory() as session:
            for index in range(50):
                seed_clean_market(
                    session, index=index, at=at + timedelta(minutes=15 * index)
                )
            session.commit()

            def incomplete_evaluator(**kwargs):
                result = fake_candidate_evaluator(**kwargs)
                return CalibrationResult(
                    "INCOMPLETE",
                    result.partition_manifest,
                    result.candidates,
                    result.candidate_metrics,
                    result.selected_candidate_id,
                    result.warnings,
                    result.blockers,
                    result.candidate_replay_trades,
                    result.candidate_partition_replay_trades,
                )

            first = run_governed_calibration(
                session,
                snapshot=ResearchRepository(session).replay_event_snapshot(),
                replay_run_id="retry-original-replay",
                baseline_config_version_id="baseline",
                code_commit_sha="code",
                checked_at=at,
                candidate_evaluator=incomplete_evaluator,
                candidate_specs=candidates,
            )
            assert first.status == "CALIBRATION_BLOCKED"
            failed = ResearchRepository(session).get_calibration_run(first.run_id)
            assert failed is not None
            failed.evaluated_candidate_count = len(candidates)
            failed.validation_metrics = {"stale": {"status": "EVALUATED"}}
            failed.holdout_used_at = at
            session.commit()

            replay_ids: list[str] = []
            original_persist = governed_calibration._persist_partition_trades

            def capture_persist(*args, **kwargs):
                replay_ids.append(kwargs["replay_run_id"])
                return original_persist(*args, **kwargs)

            monkeypatch.setattr(
                governed_calibration,
                "_persist_partition_trades",
                capture_persist,
            )
            retried = run_governed_calibration(
                session,
                snapshot=ResearchRepository(session).replay_event_snapshot(),
                replay_run_id="retry-new-caller-replay",
                baseline_config_version_id="baseline",
                code_commit_sha="code",
                checked_at=at + timedelta(hours=1),
                candidate_evaluator=fake_candidate_evaluator,
                candidate_specs=candidates,
            )
            recovered = ResearchRepository(session).get_calibration_run(retried.run_id)
            assert recovered is not None

        assert retried.status == "POSITIVE_RESEARCH_CANDIDATE"
        assert recovered.replay_run_id == "retry-original-replay"
        assert recovered.evaluated_candidate_count == len(candidates)
        assert recovered.holdout_used_at is not None
        assert recovered.validation_metrics != {"stale": {"status": "EVALUATED"}}
        assert replay_ids
        assert set(replay_ids) == {"retry-original-replay"}
    finally:
        engine.dispose()


def test_result_classification_and_frontier_are_deterministic_and_bounded() -> None:
    baseline = {
        "status": "EVALUATED",
        "model_type": "BASELINE",
        "entry_signal_count": 0,
        "executable_entry_fill_count": 0,
        "closed_position_count": 0,
        "penalties": {"adjusted_lower_confidence_expectancy": "0"},
    }
    assert (
        classify_calibration_result({"candidate-baseline-v2": baseline}, None)
        == "NO_CANDIDATE_SIGNALS"
    )
    signal = {**baseline, "entry_signal_count": 1}
    assert (
        classify_calibration_result({"candidate": signal}, "candidate")
        == "SIGNALS_WITHOUT_EXECUTABLE_FILLS"
    )
    fill = {**signal, "executable_entry_fill_count": 1}
    assert (
        classify_calibration_result({"candidate": fill}, "candidate")
        == "FILLS_WITHOUT_CLOSED_TRADES"
    )
    closed = {
        **fill,
        "closed_position_count": 1,
        "holdout": {
            "net_pnl_per_market": "-1",
            "bootstrap": {"net_pnl_per_market": {"lower": "-2"}},
        },
    }
    assert (
        classify_calibration_result({"candidate": closed}, "candidate")
        == "CLOSED_TRADES_WITHOUT_POSITIVE_HOLDOUT"
    )
    positive = deepcopy(closed)
    positive["holdout"] = {
        "net_pnl_per_market": "1",
        "bootstrap": {"net_pnl_per_market": {"lower": "0.1"}},
    }
    positive["penalties"] = {"adjusted_lower_confidence_expectancy": "0.1"}
    assert (
        classify_calibration_result({"candidate": positive}, "candidate")
        == "POSITIVE_RESEARCH_CANDIDATE"
    )
    metrics = {
        "candidate-baseline-v2": baseline,
        **{
            f"candidate-{index:02d}": {
                **positive,
                "candidate_id": f"candidate-{index:02d}",
                "penalties": {
                    "adjusted_lower_confidence_expectancy": str(index)
                },
            }
            for index in range(30)
        },
    }
    first = build_candidate_frontier(metrics, selected_id="candidate-00", limit=20)
    second = build_candidate_frontier(metrics, selected_id="candidate-00", limit=20)
    assert first == second
    assert len(first) <= 22
    assert {row["candidate_id"] for row in first} >= {
        "candidate-baseline-v2",
        "candidate-00",
    }


def test_existing_search_contract_remains_exactly_256_candidates() -> None:
    candidates = bounded_candidate_specs("pr11f-search-contract")
    assert len(candidates) == 256
    assert candidates[0].candidate_id == "candidate-baseline-v2"
    assert sum(candidate.model_type == "WEIGHTED_HEURISTIC" for candidate in candidates) == 252
    assert sum(candidate.model_type == "L2_LOGISTIC" for candidate in candidates) == 3
