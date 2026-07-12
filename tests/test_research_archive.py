from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import func, select

from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.models import (
    Market,
    OrderbookSnapshot,
    ReferenceTick,
    ResearchMarketOutcome,
    ResearchReplayEvent,
    StrategyDecision,
    StrategyFeatureSnapshot,
)
from ape.db.session import create_engine_from_config, create_session_factory
from ape.research.archive import (
    _counterfactual_label,
    _coverage,
    _feature_event,
    _labels_for_market,
    archive_research_events,
    reconcile_market_outcomes,
)
from ape.research.fixtures import replayable_feature_vector
from ape.research.replay import _dataset_hash


def test_archive_is_idempotent_and_keeps_only_normalized_market_values(tmp_path) -> None:
    engine = create_engine_from_config(
        load_config({"DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'archive.sqlite'}"})
    )
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)
    try:
        with factory() as session:
            session.add(
                Market(
                    market_ticker="KXBTC15M-ARCHIVE",
                    open_time=at,
                    close_time=at + timedelta(minutes=15),
                    functional_strike=Decimal("62000"),
                )
            )
            session.commit()
            first = archive_research_events(session, now=at)
            second = archive_research_events(session, now=at)
            session.commit()
            events = list(session.scalars(select(ResearchReplayEvent)))
            assert first.archived_events == 1
            assert second.archived_events == 0
            assert session.scalar(select(func.count()).select_from(ResearchReplayEvent)) == 2
            assert {event.event_type for event in events} == {"MARKET", "COVERAGE_REPORT"}
            market_event = next(event for event in events if event.event_type == "MARKET")
            assert "raw_payload" not in market_event.payload
    finally:
        engine.dispose()


def test_archive_refreshes_mutable_market_payload_when_source_version_advances(tmp_path) -> None:
    engine = create_engine_from_config(
        load_config({"DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'market-refresh.sqlite'}"})
    )
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)
    try:
        with factory() as session:
            market = Market(
                market_ticker="KXBTC15M-MARKET-REFRESH",
                open_time=at,
                series_ticker="KXBTC15M",
            )
            session.add(market)
            session.flush()
            first = archive_research_events(session, now=at)
            event = session.scalar(
                select(ResearchReplayEvent).where(
                    ResearchReplayEvent.source_table == "markets",
                    ResearchReplayEvent.source_row_id == str(market.id),
                )
            )
            assert event is not None
            original_hash = event.source_hash

            market.close_time = at + timedelta(minutes=15)
            market.functional_strike = Decimal("62000")
            market.updated_at = event.event_time + timedelta(seconds=1)
            session.flush()
            second = archive_research_events(session, now=at + timedelta(seconds=1))
            third = archive_research_events(session, now=at + timedelta(seconds=2))
            session.commit()

            refreshed = session.scalar(
                select(ResearchReplayEvent).where(
                    ResearchReplayEvent.source_table == "markets",
                    ResearchReplayEvent.source_row_id == str(market.id),
                )
            )
            assert first.archived_by_type == {"MARKET": 1}
            assert second.archived_by_type == {"MARKET": 1}
            assert third.archived_events == 0
            assert refreshed is not None
            assert refreshed.source_hash != original_hash
            assert refreshed.payload["close_time"] == (at + timedelta(minutes=15)).isoformat()
            assert refreshed.payload["boundary"] == "62000"
            assert (
                session.scalar(
                    select(func.count()).select_from(ResearchReplayEvent).where(
                        ResearchReplayEvent.source_table == "markets",
                        ResearchReplayEvent.source_row_id == str(market.id),
                    )
                )
                == 1
            )
    finally:
        engine.dispose()


def test_archive_advances_mutable_market_cursor_when_payload_is_unchanged(tmp_path) -> None:
    engine = create_engine_from_config(
        load_config(
            {"DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'market-cursor.sqlite'}"}
        )
    )
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)
    try:
        with factory() as session:
            market = Market(
                market_ticker="KXBTC15M-CURSOR",
                open_time=at,
                series_ticker="KXBTC15M",
            )
            session.add(market)
            session.flush()
            archive_research_events(session, now=at)
            event = session.scalar(
                select(ResearchReplayEvent).where(
                    ResearchReplayEvent.source_table == "markets",
                    ResearchReplayEvent.source_row_id == str(market.id),
                )
            )
            assert event is not None
            original_dataset_hash = _dataset_hash((event,))
            original_event_hash = event.event_hash

            advanced_at = event.event_time + timedelta(seconds=1)
            market.updated_at = advanced_at
            session.flush()
            second = archive_research_events(session, now=advanced_at)
            third = archive_research_events(session, now=advanced_at + timedelta(seconds=1))
            session.commit()

            refreshed = session.scalar(
                select(ResearchReplayEvent).where(
                    ResearchReplayEvent.source_table == "markets",
                    ResearchReplayEvent.source_row_id == str(market.id),
                )
            )
            assert second.archived_events == 0
            assert third.archived_events == 0
            assert refreshed is not None
            assert refreshed.event_time == advanced_at.replace(tzinfo=UTC)
            assert refreshed.event_hash == original_event_hash
            assert _dataset_hash((refreshed,)) == original_dataset_hash
    finally:
        engine.dispose()


def test_archive_coverage_counts_readiness_only_for_feature_snapshots(tmp_path) -> None:
    engine = create_engine_from_config(
        load_config(
            {
                "DATABASE_URL": (
                    f"sqlite+pysqlite:///{tmp_path / 'coverage-readiness.sqlite'}"
                )
            }
        )
    )
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)

    def event(event_id: str, event_type: str, readiness: str) -> ResearchReplayEvent:
        return ResearchReplayEvent(
            event_id=event_id,
            market_ticker="KXBTC15M-COVERAGE",
            event_type=event_type,
            event_time=at,
            received_at=at,
            source_table=f"fixture-{event_type.lower()}",
            source_row_id=event_id,
            source_hash=event_id,
            replay_schema_version="momentum_v2_replay_v1",
            payload={},
            event_hash=f"hash-{event_id}",
            replay_readiness=readiness,
            blockers=[],
        )

    try:
        with factory() as session:
            session.add_all(
                (
                    event("market", "MARKET", "FULL"),
                    event("reference", "REFERENCE", "FULL"),
                    event("orderbook", "ORDERBOOK", "FULL"),
                    event("feature-full", "FEATURE_SNAPSHOT", "FULL"),
                    event("feature-partial", "FEATURE_SNAPSHOT", "PARTIAL"),
                    event("feature-unusable", "FEATURE_SNAPSHOT", "UNUSABLE"),
                )
            )
            session.flush()

            coverage = _coverage(session, data_cutoff=at)

            assert coverage["complete_frame_count"] == 1
            assert coverage["partial_frame_count"] == 1
            assert coverage["unusable_frame_count"] == 1
    finally:
        engine.dispose()


def test_archive_assigns_reference_ticks_to_the_active_btc15_market(tmp_path) -> None:
    engine = create_engine_from_config(
        load_config({"DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'reference-market.sqlite'}"})
    )
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)
    try:
        with factory() as session:
            session.add_all(
                (
                    Market(
                        market_ticker="KXBTC15M-REFERENCE-MARKET",
                        series_ticker="KXBTC15M",
                        open_time=at,
                        close_time=at + timedelta(minutes=15),
                        functional_strike=Decimal("62000"),
                    ),
                    ReferenceTick(
                        source="kalshi_cfbenchmarks_brti",
                        received_at=at + timedelta(seconds=5),
                        parsed_value=Decimal("62001"),
                        parse_status="valid",
                    ),
                    ReferenceTick(
                        source="kalshi_cfbenchmarks_brti",
                        received_at=at + timedelta(minutes=15),
                        parsed_value=Decimal("62002"),
                        parse_status="valid",
                    ),
                )
            )
            session.commit()

            archive_research_events(session, now=at + timedelta(minutes=15))
            session.commit()

            reference_events = list(
                session.scalars(
                    select(ResearchReplayEvent)
                    .where(ResearchReplayEvent.event_type == "REFERENCE")
                    .order_by(ResearchReplayEvent.event_time.asc())
                )
            )
            assert [event.market_ticker for event in reference_events] == [
                "KXBTC15M-REFERENCE-MARKET",
                None,
            ]
    finally:
        engine.dispose()


def test_archive_backfills_legacy_global_reference_events(tmp_path) -> None:
    engine = create_engine_from_config(
        load_config({"DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'legacy-reference.sqlite'}"})
    )
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)
    try:
        with factory() as session:
            session.add_all(
                (
                    Market(
                        market_ticker="KXBTC15M-LEGACY-REFERENCE",
                        series_ticker="KXBTC15M",
                        open_time=at,
                        close_time=at + timedelta(minutes=15),
                        functional_strike=Decimal("62000"),
                    ),
                    ResearchReplayEvent(
                        event_id="legacy-global-reference",
                        market_ticker=None,
                        event_type="REFERENCE",
                        event_time=at + timedelta(seconds=5),
                        received_at=at + timedelta(seconds=5),
                        source_table="reference_ticks",
                        source_row_id="legacy-reference",
                        source_hash="fixture",
                        replay_schema_version="momentum_v2_replay_v1",
                        payload={"parsed_value": "62001"},
                        event_hash="legacy-global-reference",
                        replay_readiness="FULL",
                        blockers=[],
                    ),
                )
            )
            session.commit()

            event = session.scalar(
                select(ResearchReplayEvent).where(
                    ResearchReplayEvent.event_id == "legacy-global-reference"
                )
            )
            assert event is not None
            original_dataset_hash = _dataset_hash((event,))

            archive_research_events(session, now=at + timedelta(minutes=15))
            session.commit()

            event = session.scalar(
                select(ResearchReplayEvent).where(
                    ResearchReplayEvent.event_id == "legacy-global-reference"
                )
            )
            assert event is not None
            assert event.market_ticker == "KXBTC15M-LEGACY-REFERENCE"
            assert event.event_hash != "legacy-global-reference"
            assert _dataset_hash((event,)) != original_dataset_hash
    finally:
        engine.dispose()


def test_counterfactual_label_uses_the_first_in_window_executable_book() -> None:
    at = datetime(2026, 7, 11, 12, 5, tzinfo=UTC)
    market = Market(
        market_ticker="KXBTC15M-LABEL",
        open_time=at - timedelta(minutes=5),
        close_time=at + timedelta(minutes=10),
        functional_strike=Decimal("62000"),
    )
    feature = StrategyFeatureSnapshot(
        feature_snapshot_id="feature-label",
        evaluated_at=at,
        feature_schema_version="momentum_v2_features_v3",
        context_hash="context",
        candidate_side="YES",
        boundary=Decimal("62000"),
        complete_feature_vector=replayable_feature_vector(),
    )
    books = [
        OrderbookSnapshot(
            market_ticker=market.market_ticker,
            received_at=at + timedelta(milliseconds=600),
            yes_ask=Decimal("0.60"),
            yes_ask_count=Decimal("1"),
            yes_bid=Decimal("0.58"),
            yes_bid_count=Decimal("1"),
        ),
        OrderbookSnapshot(
            market_ticker=market.market_ticker,
            received_at=at + timedelta(seconds=5),
            yes_ask=Decimal("0.55"),
            yes_ask_count=Decimal("10"),
            yes_bid=Decimal("0.65"),
            yes_bid_count=Decimal("2"),
        ),
    ]
    ticks = [
        ReferenceTick(
            source="kalshi_cfbenchmarks_brti",
            received_at=at + timedelta(seconds=5),
            parsed_value=Decimal("62010"),
            parse_status="valid",
        )
    ]

    label = _counterfactual_label(feature, books, ticks, market)

    assert label["entry_fillable"] is True
    assert label["entry_price"] == "0.60"
    assert label["depth_5s"] == "2"
    assert label["gross_markout_5s_cents"] == "5.00"
    assert label["net_markout_5s_cents"] is not None


def test_legacy_null_replay_readiness_snapshot_remains_labelable(tmp_path) -> None:
    engine = create_engine_from_config(
        load_config({"DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'legacy-label.sqlite'}"})
    )
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 11, 12, 5, tzinfo=UTC)
    try:
        with factory() as session:
            market = Market(
                market_ticker="KXBTC15M-LEGACY-LABEL",
                open_time=at - timedelta(minutes=5),
                close_time=at + timedelta(minutes=10),
                functional_strike=Decimal("62000"),
            )
            snapshot = StrategyFeatureSnapshot(
                feature_snapshot_id="legacy-feature-label",
                market_ticker=market.market_ticker,
                evaluated_at=at,
                feature_schema_version="momentum_v2_features_v3",
                context_hash="legacy-context",
                candidate_side="YES",
                boundary=Decimal("62000"),
                complete_feature_vector={
                    key: str(value) if isinstance(value, Decimal) else value
                    for key, value in replayable_feature_vector().items()
                },
                replay_readiness=None,
            )
            session.add_all(
                (
                    market,
                    snapshot,
                    OrderbookSnapshot(
                        market_ticker=market.market_ticker,
                        received_at=at + timedelta(milliseconds=600),
                        yes_ask=Decimal("0.60"),
                        yes_ask_count=Decimal("1"),
                        yes_bid=Decimal("0.58"),
                        yes_bid_count=Decimal("1"),
                    ),
                )
            )
            session.flush()

            labels = _labels_for_market(session, market, [], None)

            label = labels["counterfactual_labels"]["legacy-feature-label"]
            assert label["entry_fillable"] is True
            assert label["entry_label_readiness"] == "FULL"
    finally:
        engine.dispose()


def test_legacy_v2_snapshot_recovers_decision_vector_for_labels(tmp_path) -> None:
    engine = create_engine_from_config(
        load_config({"DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'legacy-vector.sqlite'}"})
    )
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 11, 12, 5, tzinfo=UTC)
    vector = {
        key: str(value) if isinstance(value, Decimal) else value
        for key, value in replayable_feature_vector().items()
    }
    for key in (
        "candidate_side",
        "candidate_mode",
        "boundary",
        "current_brti",
        "seconds_since_open",
        "seconds_left",
        "timing_tier",
        "quality_state",
    ):
        vector.pop(key)
    try:
        with factory() as session:
            market = Market(
                market_ticker="KXBTC15M-LEGACY-VECTOR",
                open_time=at - timedelta(minutes=5),
                close_time=at + timedelta(minutes=10),
                functional_strike=Decimal("62000"),
            )
            snapshot = StrategyFeatureSnapshot(
                feature_snapshot_id="legacy-vector-label",
                market_ticker=market.market_ticker,
                evaluated_at=at,
                feature_schema_version="momentum_v2_features_v2",
                context_hash="legacy-vector-context",
                candidate_side="YES",
                candidate_mode="CONTINUATION",
                boundary=Decimal("62000"),
                current_brti=Decimal("62010"),
                seconds_since_open=360,
                seconds_left=360,
                complete_feature_vector=None,
                replay_readiness=None,
                quality_state={
                    "market_ready": True,
                    "reference_ready": True,
                    "book_ready": True,
                    "canonical_market_ready": True,
                    "canonical_reference_ready": True,
                },
            )
            decision = StrategyDecision(
                decision_id="legacy-vector-decision",
                strategy_id="btc15_momentum_v2",
                market_ticker=market.market_ticker,
                evaluated_at=at,
                decision_state="DRY_RUN_ENTRY_SIGNAL",
                primary_reason="fixture",
                app_mode="DRY_RUN",
                feature_snapshot_id=snapshot.feature_snapshot_id,
                measurements={"features": vector},
            )
            book = OrderbookSnapshot(
                market_ticker=market.market_ticker,
                received_at=at + timedelta(milliseconds=600),
                yes_ask=Decimal("0.60"),
                yes_ask_count=Decimal("1"),
                yes_bid=Decimal("0.58"),
                yes_bid_count=Decimal("1"),
            )
            session.add_all((market, snapshot, decision, book))
            session.flush()

            archived = _feature_event(session, snapshot)
            labels = _labels_for_market(session, market, [], None)

            assert archived["replay_readiness"] == "FULL"
            assert archived["payload"]["feature_vector"]["boundary"] == "62000"
            assert archived["payload"]["feature_vector"]["seconds_since_open"] == 360
            assert archived["payload"]["feature_vector"]["seconds_left"] == 360
            assert archived["payload"]["feature_vector"]["timing_tier"] == "normal"
            label = labels["counterfactual_labels"][snapshot.feature_snapshot_id]
            assert label["entry_fillable"] is True
            assert label["entry_label_readiness"] == "FULL"
    finally:
        engine.dispose()


def test_outcome_reconciliation_pages_past_resolved_newest_markets(tmp_path) -> None:
    engine = create_engine_from_config(
        load_config({"DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'outcomes.sqlite'}"})
    )
    run_migrations(engine)
    factory = create_session_factory(engine)
    at = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)

    class FakePublicClient:
        def __init__(self) -> None:
            self.requested_tickers: list[str] = []

        def get_market(self, market_ticker: str) -> dict[str, object]:
            self.requested_tickers.append(market_ticker)
            return {
                "market": {
                    "result": "yes",
                    "status": "settled",
                    "settlement_value": "62000",
                }
            }

    client = FakePublicClient()
    oldest_ticker = "KXBTC15M-BACKLOG-000"
    try:
        with factory() as session:
            markets = [
                Market(
                    market_ticker=f"KXBTC15M-BACKLOG-{index:03d}",
                    series_ticker="KXBTC15M",
                    open_time=at + timedelta(minutes=15 * index - 15),
                    close_time=at + timedelta(minutes=15 * index),
                    functional_strike=Decimal("62000"),
                )
                for index in range(501)
            ]
            session.add_all(markets)
            session.add_all(
                ResearchMarketOutcome(
                    outcome_id=f"outcome-{market.market_ticker}",
                    market_ticker=market.market_ticker,
                    outcome_status="RESOLVED",
                )
                for market in markets[1:]
            )
            session.flush()

            changed = reconcile_market_outcomes(
                session, client=client, now=at + timedelta(days=7)
            )

            assert changed == 1
            assert client.requested_tickers == [oldest_ticker]
            assert session.scalar(select(func.count()).select_from(ResearchMarketOutcome)) == 501
    finally:
        engine.dispose()
