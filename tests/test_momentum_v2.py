from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from ape.config import load_config
from ape.db.models import Market, OrderbookSnapshot, PublicTrade, ReferenceTick
from ape.strategy import momentum_v2
from ape.strategy.context import StrategyEvaluationContext


def test_low_edge_uses_dedicated_v2_edge_state(monkeypatch) -> None:
    evaluated_at = datetime(2026, 7, 11, 12, 10, tzinfo=UTC)
    context = StrategyEvaluationContext(
        evaluated_at=evaluated_at,
        market=Market(
            market_ticker="KXBTC15M-TEST",
            open_time=evaluated_at - timedelta(minutes=5),
            close_time=evaluated_at + timedelta(minutes=10),
        ),
        boundary=Decimal("62000"),
        boundary_source=None,
        reference_tick=None,
        orderbook=None,
        latest_trade=None,
        reference_ticks=(),
        orderbook_history=(),
        recent_trades=(),
    )
    features = {
        "candidate_side": "YES",
        "candidate_mode": "CONTINUATION",
        "quality_state": {
            "market_ready": True,
            "reference_ready": True,
            "book_ready": True,
        },
        "distance_bps": Decimal("2"),
        "fast_impulse_active": True,
        "retrace_fraction": Decimal("0.10"),
        "reversal_beyond_origin": False,
        "boundary_crosses_90s": 0,
        "return_60s": Decimal("0"),
        "return_120s": Decimal("0"),
        "contract_move_15s_cents": Decimal("0"),
        "contract_move_30s_cents": Decimal("0"),
        "persistent_adverse_microstructure": False,
        "desired_ask": Decimal("0.60"),
        "desired_spread_cents": Decimal("2"),
        "desired_ask_depth": Decimal("2"),
    }
    monkeypatch.setattr(momentum_v2, "_features", lambda _context, *, config: features)
    monkeypatch.setattr(
        momentum_v2,
        "_score",
        lambda _features, _tier: (Decimal("90"), {}),
    )
    monkeypatch.setattr(momentum_v2, "_edge", lambda _features: Decimal("1.49"))
    monkeypatch.setattr(momentum_v2, "_timing_tier", lambda _open, _left: "normal")

    result = momentum_v2.evaluate_momentum_v2(context, config=load_config({}))

    assert result.state == momentum_v2.STATE_V2_EDGE_BELOW_THRESHOLD
    assert result.reason == "v2_edge_below_threshold"
    assert result.blockers == []


def test_v2_blocks_stale_persisted_reference_and_orderbook() -> None:
    evaluated_at = datetime(2026, 7, 11, 12, 10, tzinfo=UTC)
    stale_at = evaluated_at - timedelta(milliseconds=2_001)
    market = Market(
        market_ticker="KXBTC15M-TEST",
        open_time=evaluated_at - timedelta(minutes=5),
        close_time=evaluated_at + timedelta(minutes=10),
    )
    reference_tick = ReferenceTick(
        source="kalshi_cfbenchmarks_brti",
        received_at=stale_at,
        source_ts=stale_at,
        parsed_value=Decimal("62010"),
        parse_status="valid",
    )
    orderbook = OrderbookSnapshot(
        market_ticker=market.market_ticker,
        received_at=stale_at,
        yes_bid=Decimal("0.60"),
        yes_ask=Decimal("0.62"),
        no_bid=Decimal("0.38"),
        no_ask=Decimal("0.40"),
        book_status="ok",
    )
    context = StrategyEvaluationContext(
        evaluated_at=evaluated_at,
        market=market,
        boundary=Decimal("62000"),
        boundary_source=None,
        reference_tick=reference_tick,
        orderbook=orderbook,
        latest_trade=None,
        reference_ticks=(reference_tick,),
        orderbook_history=(orderbook,),
        recent_trades=(),
    )

    result = momentum_v2.evaluate_momentum_v2(context, config=load_config({}))

    assert result.state == momentum_v2.STATE_V2_FEATURES_NOT_READY
    assert "v2_prerequisite_data_missing_or_stale" in result.blockers


def test_v2_trade_flow_uses_inferred_trade_sides() -> None:
    now = datetime(2026, 7, 11, 12, 10, tzinfo=UTC)
    context = SimpleNamespace(
        recent_trades=(
            PublicTrade(
                market_ticker="KXBTC15M-TEST",
                received_at=now,
                trade_count=Decimal("3"),
                side_inferred="YES",
                taker_side="NO",
            ),
            PublicTrade(
                market_ticker="KXBTC15M-TEST",
                received_at=now,
                trade_count=Decimal("1"),
                side_inferred="NO",
                taker_side="YES",
            ),
        )
    )

    ratio, count = momentum_v2._trade_flow(context, "YES")

    assert ratio == Decimal("0.75")
    assert count == 2


def test_builtin_config_version_changes_with_code_revision(monkeypatch) -> None:
    monkeypatch.setattr(momentum_v2, "resolve_code_version", lambda: "revision-a")
    first = momentum_v2.built_in_config_version("btc15_momentum_v2", {"alpha": 1})
    monkeypatch.setattr(momentum_v2, "resolve_code_version", lambda: "revision-b")
    second = momentum_v2.built_in_config_version("btc15_momentum_v2", {"alpha": 1})

    assert first.parameter_hash == second.parameter_hash
    assert first.strategy_config_version_id != second.strategy_config_version_id
    assert second.code_commit_sha == "revision-b"
