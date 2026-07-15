from __future__ import annotations

import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from tests.test_research_helpers import (
    at_base,
    feature_event,
    json_vector,
    orderbook_event,
    valid_vector,
)

from ape.config import load_config
from ape.db.migrations import CURRENT_SCHEMA_VERSION, SCHEMA_VERSIONS, run_migrations
from ape.db.models import (
    Market,
    OrderbookSnapshot,
    ReferenceTick,
    ResearchMarketOutcome,
    ResearchReplayEvent,
    StrategyFeatureSnapshot,
)
from ape.db.session import create_engine_from_config, create_session_factory
from ape.kalshi.reference_messages import BRTI_SOURCE
from ape.research import archive as archive_module
from ape.research import replay as replay_module
from ape.research.archive import (
    ARCHIVE_BATCH_SIZE,
    LABEL_MARKETS_PER_CYCLE,
    ArchiveResult,
    archive_research_coverage,
    refresh_research_archive_labels,
)
from ape.research.replay import DeterministicReplayEngine, _event_key
from ape.research.repository import REPLAY_EVENT_PAGE_SIZE, ResearchRepository
from ape.research.service import ResearchWorker, run_research_cycle
from ape.research.status import build_research_status
from ape.safety import assess_startup_safety

ROOT = Path(__file__).resolve().parents[1]
GOLDEN_REPLAY_FIXTURE = ROOT / "tests" / "fixtures" / "pr11a_replay_golden.json"


def _factory(tmp_path, name: str, *, calibration_enabled: bool = False):
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / name}",
            "APP_MODE": "DRY_RUN",
            "RESEARCH_ENABLED": "true",
            "CALIBRATION_ENABLED": str(calibration_enabled).lower(),
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    return config, engine, create_session_factory(engine)


def _archive_event(*, event_id: str, at: datetime, source_id: str) -> ResearchReplayEvent:
    return ResearchReplayEvent(
        event_id=event_id,
        market_ticker="M1",
        event_type="MARKET",
        event_time=at,
        received_at=at,
        source_table="pr11b-fixture",
        source_row_id=source_id,
        source_hash=event_id,
        replay_schema_version="momentum_v2_replay_v1",
        payload={},
        event_hash=f"hash-{event_id}",
        replay_readiness="FULL",
        blockers=[],
    )


def _reference_archive_event(*, event_id: str, at: datetime) -> ResearchReplayEvent:
    return ResearchReplayEvent(
        event_id=event_id,
        market_ticker=None,
        event_type="REFERENCE",
        event_time=at,
        received_at=at,
        source_table="reference_ticks",
        source_row_id=event_id,
        source_hash=event_id,
        replay_schema_version="momentum_v2_replay_v1",
        payload={"parsed_value": "62000"},
        event_hash=f"hash-{event_id}",
        replay_readiness="FULL",
        blockers=[],
    )


def test_r1_frozen_reader_uses_watermark_keyset_pages_and_never_list_events_none(
    tmp_path, monkeypatch
) -> None:
    config, engine, factory = _factory(tmp_path, "r1.sqlite")
    at = at_base()
    try:
        with factory() as session:
            session.add_all(
                [
                    feature_event(at=at, event_id="feature"),
                    orderbook_event(at=at + timedelta(milliseconds=600), event_id="2"),
                    orderbook_event(at=at + timedelta(milliseconds=600), event_id="10"),
                    orderbook_event(
                        at=at + timedelta(hours=2), event_id="partition-boundary"
                    ),
                ]
            )
            session.commit()
            repository = ResearchRepository(session)
            snapshot = repository.replay_event_snapshot()
            session.add(
                orderbook_event(at=at + timedelta(seconds=1), event_id="late-after-watermark")
            )
            session.flush()
            reader = repository.frozen_replay_event_reader(snapshot, page_size=1)
            pages = list(reader.iter_pages())
            observed = [event.event_id for page in pages for event in page]
            assert observed == ["feature", "2", "10", "partition-boundary"]
            assert reader.partitions_completed >= 2

        def forbidden_list_events(*_args, **kwargs):
            assert kwargs.get("limit") is not None
            raise AssertionError("production replay must use FrozenReplayEventReader")

        monkeypatch.setattr(ResearchRepository, "list_events", forbidden_list_events)
        with factory() as session:
            result = run_research_cycle(
                config,
                session,
                archive_result=ArchiveResult(0, {}, 0, {}),
            )
            assert result["status"] == "completed"
    finally:
        engine.dispose()


def test_r2_incremental_replay_matches_small_fixture_api_for_pages_and_ties() -> None:
    at = at_base()
    events = [
        feature_event(at=at, event_id="feature"),
        orderbook_event(at=at + timedelta(milliseconds=600), event_id="10", yes_ask="0.79"),
        orderbook_event(at=at + timedelta(milliseconds=600), event_id="2", yes_ask="0.60"),
        orderbook_event(at=at + timedelta(seconds=61), event_id="exit", yes_bid="0.65"),
    ]
    expected = DeterministicReplayEngine().replay(events)
    ordered = sorted(events, key=_event_key)
    for page_size in (1, 2, 17, REPLAY_EVENT_PAGE_SIZE):
        pages = [ordered[index : index + page_size] for index in range(0, len(ordered), page_size)]
        actual = DeterministicReplayEngine().replay_ordered_pages(
            pages, retain_decisions=True
        )
        assert actual == expected
        assert actual.dataset_hash == expected.dataset_hash
        assert actual.decision_count == len(expected.decisions)


def _pr11a_golden_events() -> tuple[list[ResearchReplayEvent], ResearchMarketOutcome]:
    at = at_base()
    nofill_feature = feature_event(
        at=at,
        event_id="nofill-feature",
        market="M-NOFILL",
    )
    nofill_book = orderbook_event(
        at=at + timedelta(milliseconds=600),
        event_id="nofill-book",
        yes_ask="0.99",
        market="M-NOFILL",
    )
    normal_feature = feature_event(
        at=at + timedelta(seconds=1),
        event_id="normal-feature",
        market="M-NORMAL",
    )
    normal_book = orderbook_event(
        at=at + timedelta(seconds=1, milliseconds=600),
        event_id="normal-book",
        market="M-NORMAL",
    )
    # Same timestamp as normal-book; source-id ordering must keep this before it.
    book_only = orderbook_event(
        at=at + timedelta(seconds=1, milliseconds=600),
        event_id="book-only",
        market="M-BOOK-ONLY",
    )
    partial = feature_event(
        at=at + timedelta(seconds=2),
        event_id="partial-feature",
        market="M-PARTIAL",
    )
    partial.replay_readiness = "PARTIAL"
    missing = feature_event(
        at=at + timedelta(seconds=3),
        event_id="missing-feature",
        market="M-MISSING",
    )
    missing.payload = {"feature_vector": {}}
    outcome = ResearchMarketOutcome(
        outcome_id="normal-outcome",
        market_ticker="M-NORMAL",
        market_close_at=at + timedelta(minutes=15),
        outcome_status="RESOLVED",
        result_side="YES",
        resolved_at=at + timedelta(minutes=15),
    )
    return (
        [
            normal_book,
            partial,
            nofill_book,
            missing,
            normal_feature,
            book_only,
            nofill_feature,
        ],
        outcome,
    )


def _golden_projection(result) -> dict[str, object]:
    report = result.zero_entry_report
    return {
        "event_count": result.event_count,
        "unique_market_count": result.unique_market_count,
        "decision_states": [decision.state for decision in result.decisions],
        "trades": [
            [
                trade.market_ticker,
                trade.status,
                str(trade.entry_fill_price) if trade.entry_fill_price is not None else None,
                str(trade.exit_fill_price) if trade.exit_fill_price is not None else None,
            ]
            for trade in result.trades
        ],
        "pipeline": report["pipeline"],
        "market_count": report["market_count"],
        "markets_without_eligible_continuation": report[
            "markets_without_eligible_continuation"
        ],
        "markets_with_signal_but_no_fill": report["markets_with_signal_but_no_fill"],
        "unique_market_rates_per_100": report["unique_market_rates_per_100"],
    }


def test_f2_pr11a_golden_replay_semantics_match_small_and_database_bounded_paths(
    tmp_path, monkeypatch
) -> None:
    golden = json.loads(GOLDEN_REPLAY_FIXTURE.read_text(encoding="utf-8"))
    events, outcome = _pr11a_golden_events()
    expected = golden["expected"]
    small = DeterministicReplayEngine().replay(events, outcomes=[outcome])
    small_projection = _golden_projection(small)
    # F2 deliberately broadens this one zero-entry field to the full frozen
    # dataset market set. Every remaining result value stays byte-for-byte
    # compatible with the captured PR 11a fixture.
    assert {
        key: value
        for key, value in small_projection.items()
        if key != "markets_without_eligible_continuation"
    } == {
        key: value
        for key, value in expected.items()
        if key != "markets_without_eligible_continuation"
    }
    assert small_projection["markets_without_eligible_continuation"] == [
        "M-BOOK-ONLY",
        "M-MISSING",
        "M-PARTIAL",
    ]
    assert [event.event_id for event in sorted(events, key=_event_key)] == golden[
        "fixture_event_ids"
    ]

    _config, engine, factory = _factory(tmp_path, "f2-golden.sqlite")
    try:
        with factory() as session:
            stored_events, stored_outcome = _pr11a_golden_events()
            session.add_all(stored_events)
            session.commit()
            for partition_seconds in (1, 3_600):
                monkeypatch.setattr(
                    "ape.research.repository.REPLAY_EVENT_PARTITION_SECONDS",
                    partition_seconds,
                )
                snapshot = ResearchRepository(session).replay_event_snapshot()
                for page_size in (1, 2, 17, REPLAY_EVENT_PAGE_SIZE):
                    reader = ResearchRepository(session).frozen_replay_event_reader(
                        snapshot,
                        page_size=page_size,
                    )
                    bounded = DeterministicReplayEngine().replay_ordered_pages(
                        reader.iter_pages(),
                        outcomes=[stored_outcome],
                        retain_decisions=True,
                    )
                    assert _golden_projection(bounded) == small_projection
                    assert reader.events_scanned == snapshot.event_count
                    assert reader.max_page_size <= page_size
    finally:
        engine.dispose()


def test_r3_large_incremental_replay_keeps_decisions_and_distributions_bounded() -> None:
    at = at_base()
    events = [
        _archive_event(
            event_id=f"event-{index}",
            at=at + timedelta(seconds=index),
            source_id=str(index),
        )
        for index in range(REPLAY_EVENT_PAGE_SIZE * 3 + 1)
    ]
    pages = [
        events[index : index + REPLAY_EVENT_PAGE_SIZE]
        for index in range(0, len(events), REPLAY_EVENT_PAGE_SIZE)
    ]
    result = DeterministicReplayEngine().replay_ordered_pages(pages, retain_decisions=False)
    assert result.event_count == len(events)
    assert result.decisions == ()
    assert result.decision_count == 0
    assert result.zero_entry_report["distribution_sampling"]["cap"] == 2_000


def test_f5_zero_entry_sampling_is_exact_below_cap_and_deterministic_above_cap(
    monkeypatch
) -> None:
    monkeypatch.setattr(replay_module, "ZERO_ENTRY_DISTRIBUTION_CAP", 3)
    at = at_base()
    events: list[ResearchReplayEvent] = []
    expected_below_cap: list[str] = []
    for index in range(6):
        vector = feature_event(
            at=at + timedelta(seconds=index),
            event_id=f"sampling-{index}",
            market=f"M-SAMPLING-{index}",
        )
        vector.payload["feature_vector"]["desired_ask"] = str(
            Decimal("0.50") + Decimal(index) / 100
        )
        events.append(vector)
        if index < 3:
            expected_below_cap.append(str(Decimal("0.50") + Decimal(index) / 100))

    exact = DeterministicReplayEngine().replay(events[:3])
    exact_report = exact.zero_entry_report
    assert exact_report["zero_entry_report_schema_version"] == "zero_entry_report_v1"
    assert exact_report["desired_ask_distribution"] == expected_below_cap
    exact_metadata = exact_report["distribution_sampling"]["distributions"][
        "desired_ask_distribution"
    ]
    assert exact_metadata == {
        "total_observation_count": 3,
        "sample_limit": 3,
        "sampling_method": "input_order_exact",
        "sampling_method_version": "pr11b-v2",
        "truncated": False,
    }

    ordered = sorted(events, key=_event_key)
    reports = []
    for page_size in (1, 2, 17):
        pages = [ordered[index : index + page_size] for index in range(0, len(ordered), page_size)]
        reports.append(
            DeterministicReplayEngine()
            .replay_ordered_pages(pages)
            .zero_entry_report
        )
    assert all(
        report["desired_ask_distribution"] == reports[0]["desired_ask_distribution"]
        for report in reports[1:]
    )
    sampled_metadata = reports[0]["distribution_sampling"]["distributions"][
        "desired_ask_distribution"
    ]
    assert sampled_metadata == {
        "total_observation_count": 6,
        "sample_limit": 3,
        "sampling_method": "stable_hash_top_k",
        "sampling_method_version": "pr11b-v2",
        "truncated": True,
    }


def test_f6_database_reader_orders_full_ties_uses_bigints_and_stays_page_bounded(
    tmp_path
) -> None:
    _config, engine, factory = _factory(tmp_path, "f6-reader.sqlite")
    at = at_base()
    try:
        with factory() as session:
            def add(
                event_id: str,
                *,
                received_at: datetime,
                sequence_number: int | None,
                source_id: str,
                source_table: str,
            ) -> None:
                event = _archive_event(event_id=event_id, at=at, source_id=source_id)
                event.received_at = received_at
                event.sequence_number = sequence_number
                event.source_table = source_table
                session.add(event)

            add(
                "received-first",
                received_at=at,
                sequence_number=99,
                source_id="999",
                source_table="tie-received",
            )
            add(
                "sequence-first",
                received_at=at + timedelta(milliseconds=1),
                sequence_number=0,
                source_id="10",
                source_table="tie-sequence",
            )
            add(
                "numeric-leading-zero",
                received_at=at + timedelta(milliseconds=1),
                sequence_number=1,
                source_id="02",
                source_table="tie-leading-zero",
            )
            add(
                "numeric-two-a",
                received_at=at + timedelta(milliseconds=1),
                sequence_number=1,
                source_id="2",
                source_table="tie-two-a",
            )
            add(
                "numeric-two-z",
                received_at=at + timedelta(milliseconds=1),
                sequence_number=1,
                source_id="2",
                source_table="tie-two-z",
            )
            add(
                "numeric-ten",
                received_at=at + timedelta(milliseconds=1),
                sequence_number=1,
                source_id="10",
                source_table="tie-ten",
            )
            add(
                "numeric-bigint",
                received_at=at + timedelta(milliseconds=1),
                sequence_number=1,
                source_id="2147483648",
                source_table="tie-bigint",
            )
            add(
                "source-text",
                received_at=at + timedelta(milliseconds=1),
                sequence_number=1,
                source_id="alpha",
                source_table="tie-text",
            )
            session.commit()
            snapshot = ResearchRepository(session).replay_event_snapshot()
            session.add(
                _archive_event(
                    event_id="later-after-watermark",
                    at=at + timedelta(seconds=1),
                    source_id="later",
                )
            )
            session.flush()
            reader = ResearchRepository(session).frozen_replay_event_reader(snapshot, page_size=1)
            observed = [event.event_id for page in reader.iter_pages() for event in page]

            assert observed == [
                "received-first",
                "sequence-first",
                "numeric-leading-zero",
                "numeric-two-a",
                "numeric-two-z",
                "numeric-ten",
                "numeric-bigint",
                "source-text",
            ]
            assert "later-after-watermark" not in observed
            assert reader.max_page_size == 1

        with factory() as session:
            session.add_all(
                [
                    _archive_event(
                        event_id=f"large-{index}",
                        at=at + timedelta(seconds=10, microseconds=index),
                        source_id=f"large-{index}",
                    )
                    for index in range(REPLAY_EVENT_PAGE_SIZE * 3 + 1)
                ]
            )
            session.commit()

        with factory() as session:
            repository = ResearchRepository(session)
            snapshot = repository.replay_event_snapshot()
            reader = repository.frozen_replay_event_reader(snapshot, page_size=17)
            replay = DeterministicReplayEngine().replay_ordered_pages(
                reader.iter_pages(),
                retain_decisions=False,
            )
            assert replay.decisions == ()
            assert replay.event_count == snapshot.event_count
            assert reader.max_page_size <= 17
            assert len(session.identity_map) == 0

            coverage = archive_research_coverage(session, now=at, snapshot=snapshot)
            assert coverage["event_count"] == snapshot.event_count
            assert coverage["frozen_snapshot"]["max_page_size"] <= REPLAY_EVENT_PAGE_SIZE
    finally:
        engine.dispose()


def test_r4_labels_are_bounded_resumable_and_mark_current_schema(tmp_path) -> None:
    _config, engine, factory = _factory(tmp_path, "r4.sqlite")
    at = at_base()
    try:
        with factory() as session:
            for index in range(LABEL_MARKETS_PER_CYCLE + 1):
                ticker = f"M{index:03d}"
                session.add(
                    Market(
                        market_ticker=ticker,
                        series_ticker="KXBTC15M",
                        open_time=at,
                        close_time=at + timedelta(minutes=15),
                    )
                )
                session.add(
                    ResearchMarketOutcome(
                        outcome_id=f"outcome-{ticker}",
                        market_ticker=ticker,
                        market_open_at=at,
                        market_close_at=at + timedelta(minutes=15),
                        outcome_status="RESOLVED",
                        result_side="YES",
                        resolved_at=at + timedelta(minutes=15),
                    )
                )
            session.commit()
            first = refresh_research_archive_labels(session)
            session.commit()
            second = refresh_research_archive_labels(session)
            session.commit()
            labeled = list(session.scalars(select(ResearchMarketOutcome)))
            assert (first.processed_markets, first.remaining_markets) == (
                LABEL_MARKETS_PER_CYCLE,
                1,
            )
            assert (second.processed_markets, second.remaining_markets) == (1, 0)
            assert all(
                row.quality_flags and row.quality_flags["label_schema_version"]
                for row in labeled
            )
    finally:
        engine.dispose()


def test_r5_coverage_uses_the_same_frozen_snapshot_and_excludes_later_rows(tmp_path) -> None:
    _config, engine, factory = _factory(tmp_path, "r5.sqlite")
    at = at_base()
    try:
        with factory() as session:
            session.add_all(
                [
                    _archive_event(event_id="one", at=at, source_id="1"),
                    _archive_event(event_id="two", at=at + timedelta(hours=2), source_id="2"),
                ]
            )
            session.commit()
            repository = ResearchRepository(session)
            snapshot = repository.replay_event_snapshot()
            session.add(_archive_event(event_id="later", at=at + timedelta(hours=3), source_id="3"))
            session.flush()
            coverage = archive_research_coverage(session, now=at, snapshot=snapshot)
            assert coverage["event_count"] == 2
            assert coverage["frozen_snapshot"]["watermark_id"] == snapshot.watermark_id
            assert coverage["frozen_snapshot"]["events_scanned"] == 2
            assert coverage["frozen_snapshot"]["partitions_completed"] >= 2
    finally:
        engine.dispose()


def test_r6_error_after_a_label_commit_preserves_the_resumable_label_progress(
    tmp_path, monkeypatch
) -> None:
    config, engine, factory = _factory(tmp_path, "r6.sqlite")
    at = at_base()
    try:
        with factory() as session:
            session.add(
                Market(
                    market_ticker="LABEL-COMMIT",
                    series_ticker="KXBTC15M",
                    open_time=at,
                    close_time=at + timedelta(minutes=15),
                )
            )
            session.add(
                ResearchMarketOutcome(
                    outcome_id="label-commit",
                    market_ticker="LABEL-COMMIT",
                    market_open_at=at,
                    market_close_at=at + timedelta(minutes=15),
                    outcome_status="RESOLVED",
                    result_side="YES",
                    resolved_at=at + timedelta(minutes=15),
                )
            )
            session.commit()

        def fail_coverage(*_args, **_kwargs):
            raise SQLAlchemyError("coverage failure after labels")

        from ape.research import service as research_service

        monkeypatch.setattr(research_service, "archive_research_coverage", fail_coverage)
        result = research_service.ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        ).run_once()
        with factory() as session:
            outcome = session.scalar(
                select(ResearchMarketOutcome).where(
                    ResearchMarketOutcome.market_ticker == "LABEL-COMMIT"
                )
            )
            assert outcome is not None
            assert outcome.quality_flags["label_schema_version"]
        assert result["status"] == "error"
        assert result["last_successful_stage"] == "association_labels"
    finally:
        engine.dispose()


def test_f1_association_batches_commit_resume_and_gate_labels_coverage_and_replay(
    tmp_path, monkeypatch
) -> None:
    config, engine, factory = _factory(tmp_path, "f1-association.sqlite")
    at = at_base()
    coverage_calls: list[object] = []
    replay_calls: list[object] = []
    try:
        with factory() as session:
            session.add(
                Market(
                    market_ticker="KXBTC15M-ASSOCIATION",
                    series_ticker="KXBTC15M",
                    open_time=at - timedelta(minutes=1),
                    close_time=at + timedelta(minutes=15),
                )
            )
            session.add_all(
                [
                    _reference_archive_event(event_id=f"reference-{index}", at=at)
                    for index in range(ARCHIVE_BATCH_SIZE * 2 + 1)
                ]
            )
            session.commit()

        from ape.research import service as research_service

        original_coverage = research_service.archive_research_coverage
        original_replay = research_service.DeterministicReplayEngine.replay_ordered_pages

        def forbidden_coverage(*args, **kwargs):
            coverage_calls.append((args, kwargs))
            raise AssertionError("coverage must wait for all association batches")

        def forbidden_replay(*args, **kwargs):
            replay_calls.append((args, kwargs))
            raise AssertionError("replay must wait for all association batches")

        monkeypatch.setattr(research_service, "archive_research_coverage", forbidden_coverage)
        monkeypatch.setattr(
            research_service.DeterministicReplayEngine,
            "replay_ordered_pages",
            forbidden_replay,
        )
        worker = ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        )

        first = worker.run_once()
        second = worker.run_once()
        with factory() as session:
            associated = session.scalar(
                select(func.count())
                .select_from(ResearchReplayEvent)
                .where(
                    ResearchReplayEvent.event_type == "REFERENCE",
                    ResearchReplayEvent.market_ticker.is_not(None),
                )
            )

        assert first["status"] == "partial"
        assert first["association_rows_processed"] == ARCHIVE_BATCH_SIZE
        assert first["association_rows_remaining"] == ARCHIVE_BATCH_SIZE + 1
        assert second["status"] == "partial"
        assert second["association_rows_processed"] == ARCHIVE_BATCH_SIZE
        assert second["association_rows_remaining"] == 1
        assert associated == ARCHIVE_BATCH_SIZE * 2
        assert coverage_calls == []
        assert replay_calls == []

        monkeypatch.setattr(research_service, "archive_research_coverage", original_coverage)
        monkeypatch.setattr(
            research_service.DeterministicReplayEngine,
            "replay_ordered_pages",
            original_replay,
        )
        third = worker.run_once()
        with factory() as session:
            associated = session.scalar(
                select(func.count())
                .select_from(ResearchReplayEvent)
                .where(
                    ResearchReplayEvent.event_type == "REFERENCE",
                    ResearchReplayEvent.market_ticker.is_not(None),
                )
            )

        assert third["status"] == "completed"
        assert third["association_rows_processed"] == 1
        assert third["association_rows_remaining"] == 0
        assert associated == ARCHIVE_BATCH_SIZE * 2 + 1
    finally:
        engine.dispose()


def test_f1_committed_association_progress_survives_a_later_label_failure(
    tmp_path, monkeypatch
) -> None:
    config, engine, factory = _factory(tmp_path, "f1-association-failure.sqlite")
    at = at_base()
    try:
        with factory() as session:
            session.add(
                Market(
                    market_ticker="KXBTC15M-ASSOCIATION-FAILURE",
                    series_ticker="KXBTC15M",
                    open_time=at - timedelta(minutes=1),
                    close_time=at + timedelta(minutes=15),
                )
            )
            session.add(_reference_archive_event(event_id="reference-failure", at=at))
            session.commit()

        from ape.research import service as research_service

        def fail_labels(_session):
            raise RuntimeError("label failure after association commit")

        monkeypatch.setattr(research_service, "refresh_research_archive_labels", fail_labels)
        result = ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        ).run_once()
        with factory() as session:
            event = session.scalar(
                select(ResearchReplayEvent).where(
                    ResearchReplayEvent.event_id == "reference-failure"
                )
            )

        assert result["status"] == "error"
        assert result["failed_stage"] == "association_labels"
        assert result["association_rows_processed"] == 1
        assert event is not None
        assert event.market_ticker == "KXBTC15M-ASSOCIATION-FAILURE"
    finally:
        engine.dispose()


def test_f4_unmatched_outcomes_block_downstream_work_and_resume_when_market_arrives(
    tmp_path, monkeypatch
) -> None:
    config, engine, factory = _factory(tmp_path, "f4-unmatched-outcome.sqlite")
    at = at_base()
    calls: list[str] = []
    try:
        with factory() as session:
            session.add(
                ResearchMarketOutcome(
                    outcome_id="unmatched-outcome",
                    market_ticker="KXBTC15M-MISSING",
                    market_open_at=at,
                    market_close_at=at + timedelta(minutes=15),
                    outcome_status="RESOLVED",
                    result_side="YES",
                    resolved_at=at + timedelta(minutes=15),
                )
            )
            session.commit()

        from ape.research import service as research_service

        original_coverage = research_service.archive_research_coverage
        original_replay = research_service.DeterministicReplayEngine.replay_ordered_pages

        def forbidden_coverage(*_args, **_kwargs):
            calls.append("coverage")
            raise AssertionError("unmatched outcome must defer coverage")

        def forbidden_replay(*_args, **_kwargs):
            calls.append("replay")
            raise AssertionError("unmatched outcome must defer replay")

        monkeypatch.setattr(research_service, "archive_research_coverage", forbidden_coverage)
        monkeypatch.setattr(
            research_service.DeterministicReplayEngine,
            "replay_ordered_pages",
            forbidden_replay,
        )
        worker = ResearchWorker(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=at,
        )
        blocked = worker.run_once()
        status = build_research_status(config)

        assert blocked["status"] == "partial"
        assert blocked["label_markets_processed"] == 0
        assert blocked["label_markets_remaining"] == 1
        assert blocked["label_markets_blocked_missing_market"] == 1
        assert "research_label_market_missing" in blocked["blockers"]
        assert "research_label_markets_blocked_missing_market" in blocked["warnings"]
        assert status["label_markets_blocked_missing_market"] == 1
        assert "research_label_market_missing" in status["blockers"]
        assert calls == []

        with factory() as session:
            session.add(
                Market(
                    market_ticker="KXBTC15M-MISSING",
                    series_ticker="KXBTC15M",
                    open_time=at,
                    close_time=at + timedelta(minutes=15),
                )
            )
            session.commit()
        monkeypatch.setattr(research_service, "archive_research_coverage", original_coverage)
        monkeypatch.setattr(
            research_service.DeterministicReplayEngine,
            "replay_ordered_pages",
            original_replay,
        )
        resumed = worker.run_once()
        with factory() as session:
            outcome = session.scalar(
                select(ResearchMarketOutcome).where(
                    ResearchMarketOutcome.outcome_id == "unmatched-outcome"
                )
            )

        assert resumed["status"] == "completed"
        assert outcome is not None
        assert outcome.quality_flags["label_schema_version"]
    finally:
        engine.dispose()


def test_f6_label_queries_exclude_rows_outside_the_market_and_label_horizon(
    tmp_path, monkeypatch
) -> None:
    _config, engine, factory = _factory(tmp_path, "f6-label-interval-bounds.sqlite")
    at = at_base()
    market_ticker = "KXBTC15M-LABEL-BOUNDS"
    close_at = at + timedelta(minutes=15)
    # The 60-second label has a bounded five-second observation window.
    label_end = close_at + timedelta(seconds=65)
    captured_ticks: list[list[ReferenceTick]] = []
    try:
        with factory() as session:
            session.add_all(
                [
                    Market(
                        market_ticker=market_ticker,
                        series_ticker="KXBTC15M",
                        open_time=at,
                        close_time=close_at,
                    ),
                    ResearchMarketOutcome(
                        outcome_id="label-bounds-outcome",
                        market_ticker=market_ticker,
                        market_open_at=at,
                        market_close_at=close_at,
                        outcome_status="RESOLVED",
                        result_side="YES",
                        resolved_at=close_at,
                    ),
                    StrategyFeatureSnapshot(
                        feature_snapshot_id="feature-before-interval",
                        market_ticker=market_ticker,
                        evaluated_at=at - timedelta(seconds=1),
                        feature_schema_version="momentum_v2_features_v3",
                        candidate_side="YES",
                        context_hash="feature-before-interval",
                        complete_feature_vector=json_vector(valid_vector()),
                        replay_readiness="FULL",
                    ),
                    StrategyFeatureSnapshot(
                        feature_snapshot_id="feature-in-interval",
                        market_ticker=market_ticker,
                        evaluated_at=at + timedelta(seconds=1),
                        feature_schema_version="momentum_v2_features_v3",
                        candidate_side="YES",
                        context_hash="feature-in-interval",
                        complete_feature_vector=json_vector(valid_vector()),
                        replay_readiness="FULL",
                    ),
                    StrategyFeatureSnapshot(
                        feature_snapshot_id="feature-after-label-horizon",
                        market_ticker=market_ticker,
                        evaluated_at=label_end + timedelta(seconds=1),
                        feature_schema_version="momentum_v2_features_v3",
                        candidate_side="YES",
                        context_hash="feature-after-label-horizon",
                        complete_feature_vector=json_vector(valid_vector()),
                        replay_readiness="FULL",
                    ),
                    OrderbookSnapshot(
                        market_ticker=market_ticker,
                        received_at=at - timedelta(seconds=1),
                        yes_ask=Decimal("0.01"),
                    ),
                    OrderbookSnapshot(
                        market_ticker=market_ticker,
                        received_at=at + timedelta(seconds=1, milliseconds=600),
                        yes_bid=Decimal("0.58"),
                        yes_ask=Decimal("0.60"),
                        yes_bid_count=Decimal("3"),
                        yes_ask_count=Decimal("3"),
                    ),
                    OrderbookSnapshot(
                        market_ticker=market_ticker,
                        received_at=label_end + timedelta(seconds=1),
                        yes_ask=Decimal("0.01"),
                    ),
                    ReferenceTick(
                        source=BRTI_SOURCE,
                        received_at=at - timedelta(seconds=1),
                        parsed_value=Decimal("61999"),
                        parse_status="valid",
                    ),
                    ReferenceTick(
                        source=BRTI_SOURCE,
                        received_at=at + timedelta(seconds=6),
                        parsed_value=Decimal("62000"),
                        parse_status="valid",
                    ),
                    ReferenceTick(
                        source=BRTI_SOURCE,
                        received_at=label_end + timedelta(seconds=1),
                        parsed_value=Decimal("62001"),
                        parse_status="valid",
                    ),
                ]
            )
            session.commit()

        original_labeler = archive_module._labels_for_market
        original_counterfactual = archive_module._counterfactual_label

        def capture_labeler(session, market, ticks, outcome, *, start, end):
            captured_ticks.append(list(ticks))
            return original_labeler(session, market, ticks, outcome, start=start, end=end)

        def assert_bounded_sources(feature, books, ticks, market, outcome, **kwargs):
            observed_book_times = [archive_module._utc(book.received_at) for book in books]
            observed_tick_times = [archive_module._utc(tick.received_at) for tick in ticks]
            assert all(
                at <= value <= label_end for value in observed_book_times
            ), observed_book_times
            assert all(
                at <= value <= label_end for value in observed_tick_times
            ), observed_tick_times
            return original_counterfactual(feature, books, ticks, market, outcome, **kwargs)

        monkeypatch.setattr(archive_module, "_labels_for_market", capture_labeler)
        monkeypatch.setattr(
            archive_module, "_counterfactual_label", assert_bounded_sources
        )
        with factory() as session:
            result = refresh_research_archive_labels(session)
            session.commit()
            outcome = session.scalar(
                select(ResearchMarketOutcome).where(
                    ResearchMarketOutcome.outcome_id == "label-bounds-outcome"
                )
            )

        assert result.processed_markets == 1
        assert result.remaining_markets == 0
        assert captured_ticks
        assert [tick.parsed_value for tick in captured_ticks[0]] == [Decimal("62000")]
        assert outcome is not None
        labels = outcome.quality_flags["counterfactual_labels"]
        assert set(labels) == {"feature-in-interval"}
    finally:
        engine.dispose()


def test_f6_coverage_matches_the_frozen_pre_pr11b_payload_except_progress_metadata(
    tmp_path,
) -> None:
    _config, engine, factory = _factory(tmp_path, "f6-coverage-golden.sqlite")
    at = at_base()

    def event(event_id: str, event_type: str, offset_seconds: int) -> ResearchReplayEvent:
        event_at = at + timedelta(seconds=offset_seconds)
        return ResearchReplayEvent(
            event_id=event_id,
            market_ticker="KXBTC15M-COVERAGE-GOLDEN",
            event_type=event_type,
            event_time=event_at,
            received_at=event_at,
            source_table=f"coverage-golden-{event_type.lower()}",
            source_row_id=event_id,
            source_hash=event_id,
            replay_schema_version="momentum_v2_replay_v1",
            payload={},
            event_hash=f"coverage-golden-hash-{event_id}",
            replay_readiness="FULL",
            blockers=[],
        )

    try:
        with factory() as session:
            session.add_all(
                [
                    event("coverage-market", "MARKET", 0),
                    event("coverage-book", "ORDERBOOK", 1),
                    event("coverage-feature", "FEATURE_SNAPSHOT", 2),
                ]
            )
            session.commit()
            snapshot = ResearchRepository(session).replay_event_snapshot()
            coverage = archive_research_coverage(session, now=at, snapshot=snapshot)

        frozen = coverage.pop("frozen_snapshot")
        assert coverage == {
            "coverage_schema_version": "research_coverage_v1",
            "data_cutoff": at.isoformat(),
            "complete_markets": 0,
            "event_count": 3,
            "earliest_event_time": at.isoformat(),
            "latest_event_time": (at + timedelta(seconds=2)).isoformat(),
            "unique_markets": 1,
            "event_counts_by_type": {
                "MARKET": 1,
                "ORDERBOOK": 1,
                "FEATURE_SNAPSHOT": 1,
            },
            "complete_frame_count": 1,
            "partial_frame_count": 0,
            "unusable_frame_count": 0,
            "source_retention_limitations": [
                "archive_contains_only_source_rows_available_before_archive_retention"
            ],
            "replay_eligibility_blockers": [
                "partial_feature_vectors_excluded_from_executable_calibration"
            ],
            "minimum_coverage": 0.0,
            "missing_source_counts": {"KXBTC15M-COVERAGE-GOLDEN": 0},
            "per_market_coverage": {
                "KXBTC15M-COVERAGE-GOLDEN": {
                    "event_count": 3,
                    "event_counts_by_type": {
                        "MARKET": 1,
                        "ORDERBOOK": 1,
                        "FEATURE_SNAPSHOT": 1,
                    },
                    "maximum_event_gap_seconds": 1,
                    "missing_source_count": 0,
                    "coverage_percentage": "1",
                }
            },
        }
        assert frozen == {
            "watermark_id": snapshot.watermark_id,
            "total_events": 3,
            "events_scanned": 3,
            "pages_completed": 1,
            "partitions_completed": snapshot.partition_count,
            "partitions_total": snapshot.partition_count,
            "max_page_size": 3,
        }
    finally:
        engine.dispose()


def test_f6_enabled_calibration_requires_a_clean_epoch_without_materializing(
    tmp_path, monkeypatch
) -> None:
    config, engine, factory = _factory(
        tmp_path, "f6-calibration-limit.sqlite", calibration_enabled=True
    )
    at = at_base()
    reader_calls = 0
    calibration_called = False
    try:
        with factory() as session:
            session.add_all(
                [
                    _archive_event(event_id="oversize-1", at=at, source_id="1"),
                    _archive_event(
                        event_id="oversize-2", at=at + timedelta(seconds=1), source_id="2"
                    ),
                ]
            )
            session.commit()

        from ape.research import service as research_service

        original_reader = ResearchRepository.frozen_replay_event_reader

        def track_reader(self, *args, **kwargs):
            nonlocal reader_calls
            reader_calls += 1
            return original_reader(self, *args, **kwargs)

        def forbidden_calibration(**_kwargs):
            nonlocal calibration_called
            calibration_called = True
            raise AssertionError("oversized replay must not enter calibration")

        monkeypatch.setattr(ResearchRepository, "frozen_replay_event_reader", track_reader)
        monkeypatch.setattr(research_service, "run_bounded_calibration", forbidden_calibration)
        with factory() as session:
            result = run_research_cycle(
                config,
                session,
                archive_result=ArchiveResult(0, {}, 0, {}),
            )
            session.commit()
            calibration = ResearchRepository(session).get_calibration_run(
                result["calibration_run_id"]
            )

        assert result["calibration_status"] == "INSUFFICIENT_CLEAN_DATA"
        assert calibration is not None
        assert calibration.status == "INSUFFICIENT_CLEAN_DATA"
        assert calibration.blockers == ["insufficient_clean_calibration_markets"]
        assert reader_calls == 1
        assert calibration_called is False
    finally:
        engine.dispose()


def test_r7_disabled_calibration_cannot_run_or_materialize_replay_events(
    tmp_path, monkeypatch
) -> None:
    config, engine, factory = _factory(tmp_path, "r7.sqlite")
    at = at_base()
    called = False
    try:
        with factory() as session:
            session.add_all(
                [
                    feature_event(at=at, event_id="r7-feature"),
                    orderbook_event(at=at + timedelta(milliseconds=600), event_id="r7-book"),
                ]
            )
            session.commit()

        def forbidden_calibration(**_kwargs):
            nonlocal called
            called = True
            raise AssertionError("disabled calibration must not run")

        from ape.research import service as research_service

        monkeypatch.setattr(research_service, "run_bounded_calibration", forbidden_calibration)
        with factory() as session:
            result = run_research_cycle(
                config,
                session,
                archive_result=ArchiveResult(0, {}, 0, {}),
            )
            assert result["calibration_status"] == "DISABLED"
        assert called is False
    finally:
        engine.dispose()


def test_r8_and_r9_keep_calibration_disabled_and_scope_boundaries_static() -> None:
    service_source = (ROOT / "src" / "ape" / "research" / "service.py").read_text(
        encoding="utf-8"
    )
    repository_source = (ROOT / "src" / "ape" / "research" / "repository.py").read_text(
        encoding="utf-8"
    )
    assert "list_events(limit=None)" not in service_source
    assert "CALIBRATION_MATERIALIZE_EVENT_LIMIT" not in service_source
    assert "run_governed_calibration" in service_source
    assert "REPLAY_EVENT_PAGE_SIZE = 250" in repository_source
    assert "0010_research_replay_calibration" in SCHEMA_VERSIONS
    assert CURRENT_SCHEMA_VERSION == "0011_research_archive_cursors"
    assert not list((ROOT / "src" / "ape" / "db" / "migrations").glob("0011*"))
