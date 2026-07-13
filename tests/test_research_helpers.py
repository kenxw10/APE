from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from ape.db.models import ResearchReplayEvent


def valid_vector() -> dict[str, Any]:
    return {
        "candidate_side": "YES",
        "candidate_mode": "CONTINUATION",
        "boundary": Decimal("62000"),
        "quality_state": {
            "market_ready": True,
            "reference_ready": True,
            "book_ready": True,
            "canonical_market_ready": True,
            "canonical_reference_ready": True,
        },
        "seconds_since_open": 360,
        "seconds_left": 360,
        "timing_tier": "normal",
        "distance_bps": Decimal("4"),
        "fast_impulse_active": True,
        "retrace_fraction": Decimal("0.10"),
        "reversal_beyond_origin": False,
        "boundary_crosses_90s": 0,
        "return_5s": Decimal("2"),
        "return_15s": Decimal("3"),
        "return_30s": Decimal("4"),
        "return_60s": Decimal("5"),
        "return_120s": Decimal("6"),
        "impulse_hold_seconds": 10,
        "directional_efficiency_30s": Decimal("0.80"),
        "directional_tick_ratio_30s": Decimal("0.80"),
        "contract_move_5s_cents": Decimal("1"),
        "contract_move_15s_cents": Decimal("2"),
        "contract_move_30s_cents": Decimal("2"),
        "persistent_adverse_microstructure": False,
        "desired_ask": Decimal("0.60"),
        "desired_bid": Decimal("0.58"),
        "desired_spread_cents": Decimal("2"),
        "desired_ask_depth": Decimal("3"),
        "desired_bid_depth": Decimal("3"),
        "response_residual_cents": Decimal("6"),
        "expected_contract_move_cents": Decimal("2"),
        "standardized_distance_120s": Decimal("1"),
        "top5_imbalance": Decimal("0.2"),
        "level1_imbalance": Decimal("0.1"),
        "level3_imbalance": Decimal("0.1"),
        "order_flow_5s": Decimal("0.2"),
        "order_flow_15s": Decimal("0.2"),
        "desired_bid_replenishment": Decimal("0.1"),
        "opposing_ask_depletion": Decimal("0.1"),
        "depth_withdrawal_pressure": Decimal("0.1"),
        "trade_count": 4,
        "trade_ratio": Decimal("0.8"),
        "response_ratio_15s": Decimal("1"),
        "response_ratio_30s": Decimal("1"),
        "volatility_regime": "medium",
        "liquidity_regime": "deep",
        "architecture_version": "momentum_v2_heuristic_v3",
        "feature_schema_version": "momentum_v2_features_v3",
        "replay_schema_version": "momentum_v2_replay_v1",
        "replay_readiness": "FULL",
        "replay_blockers": [],
    }


def json_vector(vector: dict[str, Any]) -> dict[str, Any]:
    return {
        key: str(value) if isinstance(value, Decimal) else value for key, value in vector.items()
    }


def feature_event(
    *,
    at: datetime,
    event_id: str = "feature-1",
    vector: dict[str, Any] | None = None,
    market: str = "M1",
) -> ResearchReplayEvent:
    return ResearchReplayEvent(
        event_id=event_id,
        market_ticker=market,
        event_type="FEATURE_SNAPSHOT",
        event_time=at,
        received_at=at,
        source_table="strategy_feature_snapshots",
        source_row_id=event_id,
        source_hash=event_id,
        feature_snapshot_id=event_id,
        feature_schema_version="momentum_v2_features_v3",
        architecture_version="momentum_v2_heuristic_v3",
        replay_schema_version="momentum_v2_replay_v1",
        payload={"feature_vector": json_vector(vector or valid_vector())},
        event_hash=f"hash-{event_id}",
        replay_readiness="FULL",
        blockers=[],
    )


def orderbook_event(
    *,
    at: datetime,
    event_id: str,
    yes_ask: str = "0.60",
    yes_bid: str = "0.58",
    yes_ask_size: str = "3",
    yes_bid_size: str = "3",
    market: str = "M1",
) -> ResearchReplayEvent:
    return ResearchReplayEvent(
        event_id=event_id,
        market_ticker=market,
        event_type="ORDERBOOK",
        event_time=at,
        received_at=at,
        source_table="orderbook_snapshots",
        source_row_id=event_id,
        source_hash=event_id,
        sequence_number=None,
        feature_snapshot_id=None,
        feature_schema_version=None,
        architecture_version=None,
        replay_schema_version="momentum_v2_replay_v1",
        payload={
            "yes_ask": yes_ask,
            "yes_bid": yes_bid,
            "yes_ask_size": yes_ask_size,
            "yes_bid_size": yes_bid_size,
        },
        event_hash=f"hash-{event_id}",
        replay_readiness="FULL",
        blockers=[],
    )


def at_base() -> datetime:
    return datetime(2026, 7, 11, 12, 0, tzinfo=UTC)
