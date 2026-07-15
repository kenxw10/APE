from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import insert, select
from tests.test_research_calibration import (
    _candidate,
    _factory,
    fake_candidate_evaluator,
    seed_clean_market,
)
from tests.test_research_helpers import feature_event, orderbook_event

from ape.api.main import create_app
from ape.config import load_config
from ape.db.migrations import CURRENT_SCHEMA_VERSION, run_migrations
from ape.db.models import CalibrationRun, ResearchReplayEvent, ResearchReplayRun
from ape.db.session import create_engine_from_config, create_session_factory
from ape.research.archive import ArchiveResult
from ape.research.calibration import (
    CalibrationResult,
    _candidate_parameter_evidence,
    bounded_candidate_specs,
    build_partition_manifest,
    candidate_parameter_grids,
)
from ape.research.cohort import (
    build_clean_calibration_cohort,
    completed_epoch_size,
    extract_compact_calibration_events,
)
from ape.research.fees import verified_kalshi_taker_fee_model
from ape.research.governed_calibration import (
    CALIBRATION_CANDIDATE_BATCH_SIZE,
    CALIBRATION_RESULT_CLASSIFICATIONS,
    run_governed_calibration,
)
from ape.research.replay import DeterministicReplayEngine, _event_key
from ape.research.repository import (
    REPLAY_EVENT_PAGE_SIZE,
    ResearchRepository,
    _config_diff_evidence,
)
from ape.research.service import run_research_cycle
from ape.research.status import (
    build_latest_calibration_cohort,
    build_latest_calibration_frontier,
    build_research_status,
)
from ape.strategy.momentum_v2 import V2_PARAMETERS

ROOT = Path(__file__).resolve().parents[1]


def test_r1_full_history_baseline_remains_diagnostic_and_causally_unchanged(
    tmp_path,
) -> None:
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'baseline.sqlite'}",
            "APP_MODE": "DRY_RUN",
            "RESEARCH_ENABLED": "true",
            "CALIBRATION_ENABLED": "false",
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    try:
        with factory() as session:
            session.add_all(
                [
                    feature_event(at=at, event_id="baseline-feature"),
                    orderbook_event(
                        at=at + timedelta(milliseconds=600),
                        event_id="baseline-first-book",
                        yes_ask="0.99",
                    ),
                    orderbook_event(
                        at=at + timedelta(milliseconds=700),
                        event_id="baseline-later-book",
                        yes_ask="0.60",
                    ),
                ]
            )
            session.commit()
            result = run_research_cycle(
                config,
                session,
                checked_at=at,
                archive_result=ArchiveResult(0, {}, 0, {}),
            )
            run = ResearchRepository(session).get_replay_run(result["replay_run_id"])
            assert run is not None
            assert run.status == "COMPLETED"
            assert run.partition_manifest["watermark_id"] == 3
            assert run.cost_model == verified_kalshi_taker_fee_model().metadata()
            assert result["calibration_status"] == "DISABLED"
    finally:
        engine.dispose()


def test_r2_and_r4_watermark_cohort_and_compact_reader_are_deterministic(tmp_path) -> None:
    engine, factory = _factory(tmp_path, "watermark.sqlite")
    at = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    try:
        with factory() as session:
            first, _ = seed_clean_market(session, index=0, at=at)
            session.commit()
            repository = ResearchRepository(session)
            frozen = repository.replay_event_snapshot()
            second, _ = seed_clean_market(
                session, index=1, at=at + timedelta(minutes=15)
            )
            session.commit()
            active = build_clean_calibration_cohort(
                session,
                snapshot=frozen,
                baseline_config_version_id="baseline",
                code_commit_sha="code",
            )
            future = build_clean_calibration_cohort(
                session,
                snapshot=repository.replay_event_snapshot(),
                baseline_config_version_id="baseline",
                code_commit_sha="code",
            )
        assert active.manifest["ordered_eligible_market_tickers"] == [first]
        assert future.manifest["ordered_eligible_market_tickers"] == [first, second]
        assert active.manifest["frozen_replay_watermark"] == frozen.watermark_id
        assert active.manifest["reader_progress"]["maximum_page_size"] <= 250
    finally:
        engine.dispose()


def test_r4_filtered_full_page_continues_to_later_compact_events(tmp_path) -> None:
    engine, factory = _factory(tmp_path, "filtered-page.sqlite")
    at = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    try:
        with factory() as session:
            session.add_all(
                [
                    feature_event(at=at, event_id="excluded-feature"),
                    orderbook_event(
                        at=at + timedelta(milliseconds=1),
                        event_id="first-book",
                    ),
                    orderbook_event(
                        at=at + timedelta(milliseconds=2),
                        event_id="later-book",
                    ),
                ]
            )
            session.commit()
            repository = ResearchRepository(session)
            reader = repository.calibration_replay_event_reader(
                repository.replay_event_snapshot(),
                market_tickers=("M1",),
                feature_snapshot_ids=frozenset({"eligible-feature"}),
                page_size=2,
            )
            events = [event for page in reader.iter_pages() for event in page]

        assert [event.event_id for event in events] == ["first-book", "later-book"]
        assert reader.pages_scanned == 2
        assert reader.events_scanned == 3
    finally:
        engine.dispose()


def test_r3_completed_epochs_only_advance_at_fifty_market_boundaries(tmp_path) -> None:
    engine, factory = _factory(tmp_path, "epoch-boundaries.sqlite")
    at = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    baseline = (_candidate("candidate-baseline-v2", baseline=True),)
    try:
        with factory() as session:
            for index in range(50):
                seed_clean_market(
                    session, index=index, at=at + timedelta(minutes=15 * index)
                )
            session.commit()
            first = run_governed_calibration(
                session,
                snapshot=ResearchRepository(session).replay_event_snapshot(),
                replay_run_id="baseline-50",
                baseline_config_version_id="baseline",
                code_commit_sha="code",
                checked_at=at,
                candidate_evaluator=fake_candidate_evaluator,
                candidate_specs=baseline,
            )
            assert first.completed_epoch_size == 50
            assert first.next_epoch_market_count == 100
            assert first.status == "NO_CANDIDATE_SIGNALS"
            first_id = first.run_id

            for index in range(50, 99):
                seed_clean_market(
                    session, index=index, at=at + timedelta(minutes=15 * index)
                )
            session.commit()
            between = run_governed_calibration(
                session,
                snapshot=ResearchRepository(session).replay_event_snapshot(),
                replay_run_id="baseline-99",
                baseline_config_version_id="baseline",
                code_commit_sha="code",
                checked_at=at + timedelta(days=2),
                candidate_evaluator=fake_candidate_evaluator,
                candidate_specs=baseline,
            )
            assert between.run_id == first_id
            assert between.reused_existing_run is True

            seed_clean_market(
                session, index=99, at=at + timedelta(minutes=15 * 99)
            )
            session.commit()
            second = run_governed_calibration(
                session,
                snapshot=ResearchRepository(session).replay_event_snapshot(),
                replay_run_id="baseline-100",
                baseline_config_version_id="baseline",
                code_commit_sha="code",
                checked_at=at + timedelta(days=3),
                candidate_evaluator=fake_candidate_evaluator,
                candidate_specs=baseline,
            )
            assert second.completed_epoch_size == 100
            assert second.run_id != first_id
            assert completed_epoch_size(149) == 100
            assert completed_epoch_size(150) == 150

            changed_code = run_governed_calibration(
                session,
                snapshot=ResearchRepository(session).replay_event_snapshot(),
                replay_run_id="baseline-100-new-code",
                baseline_config_version_id="baseline",
                code_commit_sha="different-code",
                checked_at=at + timedelta(days=4),
                candidate_evaluator=fake_candidate_evaluator,
                candidate_specs=baseline,
            )
            assert changed_code.run_id != second.run_id
            assert changed_code.completed_epoch_size == 50
            assert changed_code.next_epoch_market_count == 100
            changed_run = ResearchRepository(session).get_calibration_run(
                changed_code.run_id
            )
            assert changed_run is not None
            assert changed_run.code_commit_sha == "different-code"
    finally:
        engine.dispose()


def test_r4_more_than_twenty_thousand_archive_events_use_250_row_pages(tmp_path) -> None:
    engine, factory = _factory(tmp_path, "large-archive.sqlite")
    at = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    try:
        with factory() as session:
            for index in range(50):
                seed_clean_market(
                    session, index=index, at=at + timedelta(minutes=15 * index)
                )
            session.flush()
            rows = [
                {
                    "event_id": f"irrelevant-market-{index}",
                    "market_ticker": f"IRRELEVANT-{index}",
                    "event_type": "MARKET",
                    "event_time": at + timedelta(microseconds=index),
                    "received_at": at + timedelta(microseconds=index),
                    "source_table": "large_fixture",
                    "source_row_id": str(index),
                    "source_hash": str(index),
                    "sequence_number": None,
                    "feature_snapshot_id": None,
                    "feature_schema_version": None,
                    "architecture_version": None,
                    "replay_schema_version": "momentum_v2_replay_v1",
                    "payload": {},
                    "event_hash": f"irrelevant-hash-{index}",
                    "replay_readiness": "FULL",
                    "blockers": [],
                }
                for index in range(20_001)
            ]
            session.execute(insert(ResearchReplayEvent), rows)
            session.commit()
            repository = ResearchRepository(session)
            snapshot = repository.replay_event_snapshot()
            cohort = build_clean_calibration_cohort(
                session,
                snapshot=snapshot,
                baseline_config_version_id="baseline",
                code_commit_sha="code",
            )
            epoch = cohort.epoch_manifest(50)
            events, progress = extract_compact_calibration_events(
                session,
                snapshot=snapshot,
                cohort=cohort,
                epoch_manifest=epoch,
            )
            governed = run_governed_calibration(
                session,
                snapshot=snapshot,
                replay_run_id="baseline-large-archive",
                baseline_config_version_id="baseline",
                code_commit_sha="code",
                checked_at=at,
                candidate_evaluator=fake_candidate_evaluator,
                candidate_specs=(_candidate("candidate-baseline-v2", baseline=True),),
            )
        assert snapshot.event_count > 20_000
        assert progress["events_scanned"] == 100
        assert progress["maximum_page_size"] <= REPLAY_EVENT_PAGE_SIZE
        assert len(events) == 100
        assert {event.event_type for event in events} == {
            "FEATURE_SNAPSHOT",
            "ORDERBOOK",
        }
        assert governed.status == "NO_CANDIDATE_SIGNALS"
        assert governed.candidates_completed == 1
    finally:
        engine.dispose()


def test_r3_initial_100_market_cohort_starts_at_epoch_50(tmp_path) -> None:
    engine, factory = _factory(tmp_path, "epoch-initial-100.sqlite")
    at = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    baseline = (_candidate("candidate-baseline-v2", baseline=True),)
    try:
        with factory() as session:
            for index in range(100):
                seed_clean_market(
                    session, index=index, at=at + timedelta(minutes=15 * index)
                )
            session.commit()
            result = run_governed_calibration(
                session,
                snapshot=ResearchRepository(session).replay_event_snapshot(),
                replay_run_id="epoch-initial-100",
                baseline_config_version_id="baseline",
                code_commit_sha="code",
                checked_at=at,
                candidate_evaluator=fake_candidate_evaluator,
                candidate_specs=baseline,
            )
        assert result.completed_epoch_size == 50
        assert result.next_epoch_market_count == 100
        assert result.calibration_due is False
    finally:
        engine.dispose()


def test_r3_158_market_cohort_advances_one_epoch_per_cycle_and_reuses_latest(
    tmp_path,
) -> None:
    engine, factory = _factory(tmp_path, "epoch-158.sqlite")
    at = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    baseline = (_candidate("candidate-baseline-v2", baseline=True),)
    evaluated_market_counts: list[int] = []

    def evaluator(**kwargs):
        evaluated_market_counts.append(
            len(
                {
                    event.market_ticker
                    for event in kwargs["events"]
                    if event.market_ticker is not None
                }
            )
        )
        return fake_candidate_evaluator(**kwargs)

    try:
        with factory() as session:
            for index in range(158):
                seed_clean_market(
                    session, index=index, at=at + timedelta(minutes=15 * index)
                )
            session.commit()
            results = []
            for cycle in range(4):
                results.append(
                    run_governed_calibration(
                        session,
                        snapshot=ResearchRepository(session).replay_event_snapshot(),
                        replay_run_id=f"epoch-158-cycle-{cycle}",
                        baseline_config_version_id="baseline",
                        code_commit_sha="code",
                        checked_at=at + timedelta(days=cycle),
                        candidate_evaluator=evaluator,
                        candidate_specs=baseline,
                    )
                )
        assert [result.completed_epoch_size for result in results] == [50, 100, 150, 150]
        assert [result.next_epoch_market_count for result in results] == [100, 150, 200, 200]
        assert [result.reused_existing_run for result in results] == [False, False, False, True]
        assert evaluated_market_counts == [50, 100, 150]
        assert len({result.run_id for result in results[:3]}) == 3
        assert results[3].run_id == results[2].run_id
    finally:
        engine.dispose()


def test_r3_in_progress_earliest_epoch_resumes_before_larger_due_epoch(tmp_path) -> None:
    engine, factory = _factory(tmp_path, "epoch-resume-before-growth.sqlite")
    at = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    baseline = (_candidate("candidate-baseline-v2", baseline=True),)

    def interrupted_evaluator(**kwargs):
        raise RuntimeError("simulated epoch interruption")

    try:
        with factory() as session:
            for index in range(50):
                seed_clean_market(
                    session, index=index, at=at + timedelta(minutes=15 * index)
                )
            session.commit()
            with pytest.raises(RuntimeError, match="simulated epoch interruption"):
                run_governed_calibration(
                    session,
                    snapshot=ResearchRepository(session).replay_event_snapshot(),
                    replay_run_id="epoch-resume-before-growth",
                    baseline_config_version_id="baseline",
                    code_commit_sha="code",
                    checked_at=at,
                    candidate_evaluator=interrupted_evaluator,
                    candidate_specs=baseline,
                )

        with factory() as session:
            for index in range(50, 100):
                seed_clean_market(
                    session, index=index, at=at + timedelta(minutes=15 * index)
                )
            session.commit()
            resumed = run_governed_calibration(
                session,
                snapshot=ResearchRepository(session).replay_event_snapshot(),
                replay_run_id="epoch-resume-before-growth-retry",
                baseline_config_version_id="baseline",
                code_commit_sha="code",
                checked_at=at + timedelta(hours=1),
                candidate_evaluator=fake_candidate_evaluator,
                candidate_specs=baseline,
            )
        assert resumed.completed_epoch_size == 50
        assert resumed.next_epoch_market_count == 100
        assert resumed.reused_existing_run is False
    finally:
        engine.dispose()


def test_r4_reader_ordering_and_first_book_semantics_are_unchanged() -> None:
    at = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    events = [
        feature_event(at=at, event_id="feature"),
        orderbook_event(
            at=at + timedelta(milliseconds=600),
            event_id="10",
            yes_ask="0.99",
        ),
        orderbook_event(
            at=at + timedelta(milliseconds=600),
            event_id="2",
            yes_ask="0.99",
        ),
        orderbook_event(
            at=at + timedelta(milliseconds=700),
            event_id="later-rescue",
            yes_ask="0.60",
        ),
    ]
    ordered = [event.event_id for event in sorted(events, key=_event_key)]
    replay = DeterministicReplayEngine().replay(events)
    assert ordered == ["feature", "2", "10", "later-rescue"]
    assert replay.trades
    assert replay.trades[0].status == "ENTRY_NO_FILL"
    assert replay.trades[0].entry_fill_event_id is None
    assert replay.trades[0].entry_fill_at is None


def test_r5_search_space_and_protected_gate_contract_are_unchanged() -> None:
    candidates = bounded_candidate_specs("pr11f-contract")
    assert len(candidates) == 256
    assert len(candidate_parameter_grids()) == 18
    assert sum(candidate.model_type == "WEIGHTED_HEURISTIC" for candidate in candidates) == 252
    assert sum(candidate.model_type == "L2_LOGISTIC" for candidate in candidates) == 3
    changed = dict(V2_PARAMETERS)
    changed["decision_to_book_latency_ms"] = 0
    evidence = _config_diff_evidence(V2_PARAMETERS, changed)
    assert evidence["forbidden_parameter_changed"] is True
    assert evidence["safety_or_data_quality_gate_changed"] is True


def test_r5_partitions_are_chronological_purged_and_holdout_isolated() -> None:
    at = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    outcomes = []
    from ape.db.models import ResearchMarketOutcome

    for index in range(50):
        outcomes.append(
            ResearchMarketOutcome(
                outcome_id=f"outcome-{index}",
                market_ticker=f"M{index:02d}",
                market_open_at=at + timedelta(minutes=15 * index),
                market_close_at=at + timedelta(minutes=15 * (index + 1)),
                outcome_status="RESOLVED",
                result_side="YES",
                resolved_at=at + timedelta(minutes=15 * (index + 1)),
                quality_flags={},
            )
        )
    manifest = build_partition_manifest(outcomes)
    assert len(manifest["folds"]) == 5
    assert set(manifest["search_development"]).isdisjoint(manifest["development_test"])
    assert set(manifest["development"]).isdisjoint(manifest["holdout"])
    for fold in manifest["folds"]:
        assert set(fold["train"]).isdisjoint(fold["validation"])
        if fold["train"] and fold["validation"]:
            assert max(fold["train"]) < min(fold["validation"])


def test_r6_fee_and_economic_evidence_fields_are_persisted(tmp_path) -> None:
    engine, factory = _factory(tmp_path, "economic.sqlite")
    at = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    candidates = (
        _candidate("candidate-baseline-v2", baseline=True),
        _candidate("candidate-economic"),
    )
    try:
        with factory() as session:
            for index in range(50):
                seed_clean_market(
                    session, index=index, at=at + timedelta(minutes=15 * index)
                )
            session.commit()
            result = run_governed_calibration(
                session,
                snapshot=ResearchRepository(session).replay_event_snapshot(),
                replay_run_id="baseline-economic",
                baseline_config_version_id="baseline",
                code_commit_sha="code",
                checked_at=at,
                candidate_evaluator=fake_candidate_evaluator,
                candidate_specs=candidates,
            )
            run = ResearchRepository(session).get_calibration_run(result.run_id)
            assert run is not None
            assert result.classification == "POSITIVE_RESEARCH_CANDIDATE"
            metrics = run.validation_metrics["candidate-economic"]
            for key in (
                "entry_signal_count",
                "entry_intent_count",
                "executable_entry_fill_count",
                "closed_position_count",
                "net_pnl_cents",
                "net_pnl_per_market",
                "signal_to_fill_rate",
                "bootstrap",
                "penalties",
            ):
                assert key in metrics
            fee_metadata = verified_kalshi_taker_fee_model().metadata()
            assert fee_metadata["taker_formula"]
            assert fee_metadata["settlement_fee"] == "0"
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("case", "signal_count", "fill_count", "closed_count", "expected"),
    [
        ("zero-signal", 0, 0, 0, "NO_CANDIDATE_SIGNALS"),
        ("signal-no-fill", 1, 0, 0, "SIGNALS_WITHOUT_EXECUTABLE_FILLS"),
        ("fill-no-close", 1, 1, 0, "FILLS_WITHOUT_CLOSED_TRADES"),
        (
            "negative-holdout",
            1,
            1,
            1,
            "CLOSED_TRADES_WITHOUT_POSITIVE_HOLDOUT",
        ),
    ],
)
def test_r7_economic_classifications_are_persisted_from_governed_runs(
    tmp_path,
    case: str,
    signal_count: int,
    fill_count: int,
    closed_count: int,
    expected: str,
) -> None:
    engine, factory = _factory(tmp_path, f"classification-{case}.sqlite")
    at = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    candidates = (
        _candidate("candidate-baseline-v2", baseline=True),
        _candidate(f"candidate-{case}"),
    )

    def evaluator(**kwargs) -> CalibrationResult:
        result = fake_candidate_evaluator(**kwargs)
        metrics = deepcopy(result.candidate_metrics)
        for candidate_id, values in metrics.items():
            is_candidate = candidate_id != "candidate-baseline-v2"
            candidate = next(
                item for item in result.candidates if item.candidate_id == candidate_id
            )
            values.update(_candidate_parameter_evidence(candidate))
            values["entry_signal_count"] = signal_count if is_candidate else 0
            values["entry_intent_count"] = signal_count if is_candidate else 0
            values["executable_entry_fill_count"] = fill_count if is_candidate else 0
            values["closed_position_count"] = closed_count if is_candidate else 0
            values["penalties"] = {
                "adjusted_lower_confidence_expectancy": "-1"
                if closed_count
                else "1"
            }
            if kwargs.get("evaluate_finalist") and is_candidate:
                values["holdout"] = {
                    "net_pnl_per_market": "-1",
                    "bootstrap": {
                        "net_pnl_per_market": {
                            "lower": "-2",
                            "upper": "0",
                            "mean": "-1",
                        }
                    },
                }
        return CalibrationResult(
            result.status,
            result.partition_manifest,
            result.candidates,
            metrics,
            result.selected_candidate_id,
            result.warnings,
            result.blockers,
            result.candidate_replay_trades,
            result.candidate_partition_replay_trades,
        )

    try:
        with factory() as session:
            for index in range(50):
                seed_clean_market(
                    session, index=index, at=at + timedelta(minutes=15 * index)
                )
            session.commit()
            result = run_governed_calibration(
                session,
                snapshot=ResearchRepository(session).replay_event_snapshot(),
                replay_run_id=f"baseline-{case}",
                baseline_config_version_id="baseline",
                code_commit_sha=case,
                checked_at=at,
                candidate_evaluator=evaluator,
                candidate_specs=candidates,
            )
            persisted = ResearchRepository(session).get_calibration_run(result.run_id)
            assert persisted is not None
            assert persisted.status == expected
            assert persisted.test_metrics["classification"] == expected
            if signal_count:
                candidate_metrics = persisted.validation_metrics[f"candidate-{case}"]
                assert candidate_metrics["protected_parameter_changed"] is False
                assert candidate_metrics["safety_data_quality_gate_changed"] is False
                assert candidate_metrics["changed_parameter_count"] > 0
    finally:
        engine.dispose()


def test_r7_classifications_are_exact_and_frontier_is_compact() -> None:
    assert CALIBRATION_RESULT_CLASSIFICATIONS == {
        "INSUFFICIENT_CLEAN_DATA",
        "NO_CANDIDATE_SIGNALS",
        "SIGNALS_WITHOUT_EXECUTABLE_FILLS",
        "FILLS_WITHOUT_CLOSED_TRADES",
        "CLOSED_TRADES_WITHOUT_POSITIVE_HOLDOUT",
        "POSITIVE_RESEARCH_CANDIDATE",
        "CALIBRATION_BLOCKED",
        "CALIBRATION_FAILED",
    }
    assert CALIBRATION_CANDIDATE_BATCH_SIZE == 8


def test_r8_calibration_failure_cannot_roll_back_completed_baseline(tmp_path, monkeypatch) -> None:
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'failure.sqlite'}",
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

    def fail_calibration(*_args, **_kwargs):
        raise RuntimeError("calibration failed after baseline")

    from ape.research import service as research_service

    monkeypatch.setattr(research_service, "run_governed_calibration", fail_calibration)
    try:
        with factory() as session:
            with pytest.raises(RuntimeError, match="calibration failed after baseline"):
                run_research_cycle(
                    config,
                    session,
                    archive_result=ArchiveResult(0, {}, 0, {}),
                )
        with factory() as session:
            run = session.scalar(select(ResearchReplayRun))
            assert run is not None
            assert run.status == "COMPLETED"
    finally:
        engine.dispose()


def test_r8_disabled_calibration_preserves_worker_behavior(tmp_path, monkeypatch) -> None:
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'disabled.sqlite'}",
            "APP_MODE": "DRY_RUN",
            "CALIBRATION_ENABLED": "false",
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("disabled calibration must not start")

    from ape.research import service as research_service

    monkeypatch.setattr(research_service, "run_governed_calibration", forbidden)
    try:
        with factory() as session:
            result = run_research_cycle(
                config, session, archive_result=ArchiveResult(0, {}, 0, {})
            )
        assert result["calibration_status"] == "DISABLED"
    finally:
        engine.dispose()


def test_r9_candidates_remain_research_only_and_no_promotion_call_exists() -> None:
    source = (ROOT / "src" / "ape" / "research" / "service.py").read_text(
        encoding="utf-8"
    )
    runner = (
        ROOT / "src" / "ape" / "research" / "governed_calibration.py"
    ).read_text(encoding="utf-8")
    assert "advance_candidate_governance(" not in source
    assert "advance_candidate_governance(" not in runner
    assert '"lifecycle_state": LIFECYCLE_DRAFT' in runner
    assert '"eligibility_status": "RESEARCH_ONLY"' in runner


def test_r10_research_api_is_read_only_bounded_and_omits_raw_payloads(tmp_path) -> None:
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'api.sqlite'}",
            "APP_MODE": "DRY_RUN",
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    try:
        with factory() as session:
            session.add(
                CalibrationRun(
                    calibration_run_id="calibration-api",
                    status="NO_CANDIDATE_SIGNALS",
                    calibration_schema_version="schema",
                    replay_run_id=None,
                    dataset_hash="dataset",
                    code_commit_sha="code",
                    random_seed=1,
                    search_space_snapshot={"snapshot_sha256": "search"},
                    partition_manifest={
                        "epoch_hash": "epoch",
                        "epoch_size": 50,
                        "next_epoch_market_count": 100,
                        "ordered_market_tickers": ["M1"],
                        "cohort_manifest": {
                            "cohort_schema_version": "clean_calibration_cohort_v1",
                            "cohort_hash": "cohort",
                            "frozen_replay_watermark": 10,
                            "eligible_market_count": 50,
                        },
                    },
                    evaluated_candidate_count=256,
                    test_metrics={
                        "classification": "NO_CANDIDATE_SIGNALS",
                        "baseline_candidate_id": "candidate-baseline-v2",
                        "selected_finalist_id": None,
                        "next_experiment": "STRUCTURAL_TRIGGER_EXPERIMENT_REQUIRED",
                        "frontier": [
                            {
                                "candidate_id": "candidate-baseline-v2",
                                "net_pnl_cents": "0",
                            }
                        ],
                    },
                    warnings=[],
                    blockers=[],
                    started_at=at,
                    finished_at=at,
                )
            )
            session.commit()
        with TestClient(create_app(config)) as client:
            cohort = client.get("/research/cohorts/latest")
            frontier = client.get("/research/calibration/frontier/latest?limit=20")
            assert cohort.status_code == 200
            assert frontier.status_code == 200
            assert client.post("/research/cohorts/latest").status_code == 405
            payload = str({"cohort": cohort.json(), "frontier": frontier.json()})
            assert "raw_payload" not in payload
            assert "score_margin_distribution" not in payload
            assert len(frontier.json()["frontier"]) <= 22
        assert build_latest_calibration_cohort(config)["cohort"]["cohort_hash"] == "cohort"
        assert build_latest_calibration_frontier(config)["frontier"]
        assert "calibration_candidate_count" in build_research_status(config)
    finally:
        engine.dispose()


def test_r11_and_r12_scope_safety_and_deployment_boundaries_are_unchanged() -> None:
    config = load_config(
        {
            "APP_MODE": "DRY_RUN",
            "CALIBRATION_ENABLED": "false",
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )
    assert config.app_mode.value == "DRY_RUN"
    assert config.calibration_enabled is False
    assert config.trading_enabled is False
    assert config.execute is False
    assert CURRENT_SCHEMA_VERSION == "0011_research_archive_cursors"
    assert REPLAY_EVENT_PAGE_SIZE == 250
    source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (
            ROOT / "src" / "ape" / "research" / "cohort.py",
            ROOT / "src" / "ape" / "research" / "governed_calibration.py",
        )
    )
    for prohibited in (
        "place_order",
        "cancel_order",
        "private_websocket",
        "account_balance",
        "paper_trading",
    ):
        assert prohibited not in source.lower()
    assert not list((ROOT / "src" / "ape" / "db" / "migrations").glob("0012*"))
