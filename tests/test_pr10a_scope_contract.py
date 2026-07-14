from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from ape.api.main import create_app
from ape.config import load_config
from ape.db.migrations import CURRENT_SCHEMA_VERSION, SCHEMA_VERSIONS
from ape.db.models import OrderbookSnapshot, StrategyPositionOutcome
from ape.kalshi.ws_messages import parse_ws_payload
from ape.kalshi.ws_state import OrderbookState
from ape.strategy import momentum_v2
from ape.strategy.context import StrategyEvaluationContext
from ape.strategy.momentum_v2 import (
    V2_ARCHITECTURE_VERSION,
    V2_FEATURE_SCHEMA_VERSION,
    V2_LIFECYCLE_SCHEMA_VERSION,
)
from ape.strategy.observer import (
    STATE_V2_EXIT_ATTEMPT_LIMIT,
    STATE_V2_EXIT_PENDING,
    STATE_V2_EXIT_SIGNAL,
    STATE_V2_FORCE_EXIT,
    STATE_V2_MANAGE_POSITION,
)


def test_pr10a_versions_states_schema_and_endpoint_contract() -> None:
    assert "0010_research_replay_calibration" in SCHEMA_VERSIONS
    assert CURRENT_SCHEMA_VERSION == "0011_research_archive_cursors"
    assert V2_ARCHITECTURE_VERSION == "momentum_v2_heuristic_v3"
    assert V2_FEATURE_SCHEMA_VERSION == "momentum_v2_features_v3"
    assert V2_LIFECYCLE_SCHEMA_VERSION == "momentum_v2_lifecycle_v2"
    assert StrategyPositionOutcome.__tablename__ == "strategy_position_outcomes"
    assert {
        STATE_V2_MANAGE_POSITION,
        STATE_V2_EXIT_SIGNAL,
        STATE_V2_FORCE_EXIT,
        STATE_V2_EXIT_PENDING,
        STATE_V2_EXIT_ATTEMPT_LIMIT,
    } == {
        "V2_MANAGE_POSITION",
        "V2_EXIT_SIGNAL",
        "V2_FORCE_EXIT",
        "V2_EXIT_PENDING",
        "V2_EXIT_ATTEMPT_LIMIT",
    }
    paths = {route.path for route in create_app(load_config({})).routes}
    assert "/strategy/dry-run/outcomes/recent" in paths
    assert "/strategy/dry-run/intents/recent" in paths


def test_pr10a_executable_ladders_are_complementary_precise_and_bounded() -> None:
    now = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)
    message = parse_ws_payload(
        {
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "yes_dollars_fp": [
                    ["0.63", "1.25"],
                    ["0.61", "2.50"],
                    ["0.59", "3.75"],
                    ["0.57", "4.25"],
                    ["0.55", "5.50"],
                    ["0.53", "6.75"],
                ],
                "no_dollars_fp": [
                    ["0.70", "7.12"],
                    ["0.72", "8.25"],
                    ["0.74", "9.50"],
                    ["0.76", "10.75"],
                    ["0.78", "11.00"],
                    ["0.80", "12.25"],
                ],
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=now,
    )
    state = OrderbookState("KXBTC15M-TEST")
    state.apply_snapshot(message)
    snapshot = state.snapshot_input(
        received_at=now,
        sequence_number=1,
        raw_payload_hash=None,
        raw_payload=None,
    )

    assert snapshot.ladder_schema_version == "kalshi_executable_ladders_v2"
    assert snapshot.yes_bid_ladder == [
        {"price": "0.63", "count": "1.25"},
        {"price": "0.61", "count": "2.50"},
        {"price": "0.59", "count": "3.75"},
        {"price": "0.57", "count": "4.25"},
        {"price": "0.55", "count": "5.50"},
    ]
    assert snapshot.yes_ask_ladder[0] == {"price": "0.70", "count": "7.12"}
    assert snapshot.no_bid_ladder[0] == {"price": "0.30", "count": "7.12"}
    assert snapshot.no_ask_ladder[0] == {"price": "0.37", "count": "1.25"}
    assert all(
        len(ladder) == 5
        for ladder in (
            snapshot.yes_bid_ladder,
            snapshot.yes_ask_ladder,
            snapshot.no_bid_ladder,
            snapshot.no_ask_ladder,
        )
    )
    assert [Decimal(level["price"]) for level in snapshot.yes_ask_ladder] == sorted(
        Decimal(level["price"]) for level in snapshot.yes_ask_ladder
    )
    assert [Decimal(level["price"]) for level in snapshot.no_bid_ladder] == sorted(
        (Decimal(level["price"]) for level in snapshot.no_bid_ladder), reverse=True
    )


def test_pr10a_multilevel_microstructure_uses_ladders_and_independent_windows() -> None:
    now = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)
    earlier = _v2_book(
        now - timedelta(seconds=14),
        bid=[("0.62", "2"), ("0.61", "2"), ("0.60", "2")],
        ask=[("0.65", "8"), ("0.66", "8"), ("0.67", "8")],
    )
    recent = _v2_book(
        now - timedelta(seconds=4),
        bid=[("0.61", "8"), ("0.60", "8"), ("0.59", "8")],
        ask=[("0.66", "2"), ("0.67", "2"), ("0.68", "2")],
    )
    current = _v2_book(
        now,
        bid=[("0.62", "10"), ("0.61", "9"), ("0.60", "8")],
        ask=[("0.67", "1"), ("0.68", "2"), ("0.69", "3")],
    )
    context = StrategyEvaluationContext(
        evaluated_at=now,
        market=None,
        boundary=None,
        boundary_source=None,
        reference_tick=None,
        orderbook=current,
        latest_trade=None,
        reference_ticks=(),
        orderbook_history=(earlier, recent, current),
        recent_trades=(),
    )

    micro = momentum_v2._microstructure(context, "YES")

    assert micro["bid_depth_1"] == Decimal("10")
    assert micro["bid_depth_3"] == Decimal("27")
    assert micro["ask_depth_3"] == Decimal("6")
    assert micro["imbalance_3"] == Decimal("21") / Decimal("33")
    assert micro["order_flow_5s"] != micro["order_flow_15s"]
    assert micro["top5_imbalance"] > 0


def _v2_book(
    received_at: datetime,
    *,
    bid: list[tuple[str, str]],
    ask: list[tuple[str, str]],
) -> OrderbookSnapshot:
    return OrderbookSnapshot(
        market_ticker="KXBTC15M-TEST",
        received_at=received_at,
        ladder_schema_version="kalshi_executable_ladders_v2",
        yes_bid_ladder=[{"price": price, "count": count} for price, count in bid],
        yes_ask_ladder=[{"price": price, "count": count} for price, count in ask],
        no_bid_ladder=[],
        no_ask_ladder=[],
    )
