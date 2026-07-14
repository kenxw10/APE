from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from tests.test_research_helpers import at_base, feature_event, orderbook_event

from ape.config import load_config
from ape.db.migrations import CURRENT_SCHEMA_VERSION, run_migrations
from ape.db.models import Market, ResearchMarketOutcome, ResearchReplayEvent
from ape.db.session import create_engine_from_config, create_session_factory
from ape.research.archive import (
    LABEL_MARKETS_PER_CYCLE,
    ArchiveResult,
    archive_research_coverage,
    refresh_research_archive_labels,
)
from ape.research.replay import DeterministicReplayEngine, _event_key
from ape.research.repository import REPLAY_EVENT_PAGE_SIZE, ResearchRepository
from ape.research.service import run_research_cycle
from ape.safety import assess_startup_safety

ROOT = Path(__file__).resolve().parents[1]


def _factory(tmp_path, name: str):
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / name}",
            "APP_MODE": "DRY_RUN",
            "RESEARCH_ENABLED": "true",
            "CALIBRATION_ENABLED": "false",
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
    assert "CALIBRATION_MATERIALIZE_EVENT_LIMIT" in service_source
    assert "REPLAY_EVENT_PAGE_SIZE = 250" in repository_source
    assert CURRENT_SCHEMA_VERSION == "0010_research_replay_calibration"
    assert not list((ROOT / "src" / "ape" / "db" / "migrations").glob("0011*"))
