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
    ResearchReplayEvent,
    StrategyFeatureSnapshot,
)
from ape.db.session import create_engine_from_config, create_session_factory
from ape.research.archive import _counterfactual_label, archive_research_events
from ape.research.fixtures import replayable_feature_vector


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
            yes_ask_count=Decimal("1"),
            yes_bid=Decimal("0.65"),
            yes_bid_count=Decimal("1"),
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
    assert label["gross_markout_5s_cents"] == "5.00"
    assert label["net_markout_5s_cents"] is not None
