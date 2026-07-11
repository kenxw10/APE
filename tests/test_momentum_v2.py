from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from ape.db.models import Market
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
    monkeypatch.setattr(momentum_v2, "_features", lambda _context: features)
    monkeypatch.setattr(
        momentum_v2,
        "_score",
        lambda _features, _tier: (Decimal("90"), {}),
    )
    monkeypatch.setattr(momentum_v2, "_edge", lambda _features: Decimal("1.49"))
    monkeypatch.setattr(momentum_v2, "_timing_tier", lambda _open, _left: "normal")

    result = momentum_v2.evaluate_momentum_v2(context)

    assert result.state == momentum_v2.STATE_V2_EDGE_BELOW_THRESHOLD
    assert result.reason == "v2_edge_below_threshold"
    assert result.blockers == []
