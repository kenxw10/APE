from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from ape.db.models import OrderbookSnapshot
from ape.repositories.inputs import (
    JsonPayload,
    StrategyConfigVersionInput,
    StrategyFeatureSnapshotInput,
)
from ape.strategy.context import StrategyEvaluationContext

V2_STRATEGY_ID = "btc15_momentum_v2"
V2_ARCHITECTURE_VERSION = "momentum_v2_heuristic_v1"
V2_FEATURE_SCHEMA_VERSION = "momentum_v2_features_v1"
V2_EDGE_MODEL_VERSION = "heuristic_edge_v1"

STATE_V2_FEATURES_NOT_READY = "V2_FEATURES_NOT_READY"
STATE_V2_HARD_GATE_BLOCKED = "V2_HARD_GATE_BLOCKED"
STATE_V2_SCORE_BELOW_THRESHOLD = "V2_SCORE_BELOW_THRESHOLD"
STATE_V2_EDGE_BELOW_THRESHOLD = "V2_EDGE_BELOW_THRESHOLD"
STATE_V2_ENTRY_SIGNAL = "DRY_RUN_ENTRY_SIGNAL"

V2_PARAMETERS: dict[str, Any] = {
    "feature_windows_seconds": [3, 5, 10, 15, 30, 60, 120],
    "decision_to_book_latency_ms": 500,
    "intent_expiry_seconds": 2,
    "entry_attempts_per_market": 1,
    "tiers": {
        "early": {
            "start": 120,
            "end": 300,
            "score": 80,
            "max_ask": "0.72",
            "time_stop": 30,
            "max_hold": 60,
            "time_multiplier": "0.85",
        },
        "normal": {
            "start": 300,
            "end": 780,
            "score": 70,
            "max_ask": "0.78",
            "time_stop": 30,
            "max_hold": 60,
            "time_multiplier": "1.00",
        },
        "late": {
            "start": 780,
            "end": 840,
            "score": 75,
            "max_ask": "0.74",
            "time_stop": 15,
            "max_hold": 30,
            "time_multiplier": "0.80",
        },
    },
}


@dataclass(frozen=True)
class V2Evaluation:
    state: str
    reason: str
    blockers: list[str]
    warnings: list[str]
    candidate_side: str | None
    candidate_mode: str | None
    timing_tier: str | None
    intended_entry_price: Decimal | None
    score: Decimal | None
    score_threshold: Decimal | None
    edge_lower_bound_cents: Decimal | None
    feature_snapshot: StrategyFeatureSnapshotInput
    measurements: dict[str, JsonPayload]


def built_in_config_version(
    strategy_id: str, parameters: dict[str, Any]
) -> StrategyConfigVersionInput:
    payload = _json_safe(parameters)
    parameter_hash = _hash(payload)
    code_version = resolve_code_version()
    version_hash = _hash({"parameters": parameter_hash, "code": code_version})
    version_id = f"config-{strategy_id}-{version_hash[:20]}"
    return StrategyConfigVersionInput(
        strategy_config_version_id=version_id,
        strategy_id=strategy_id,
        architecture_version=(
            V2_ARCHITECTURE_VERSION if strategy_id == V2_STRATEGY_ID else "momentum_v1_legacy"
        ),
        feature_schema_version=V2_FEATURE_SCHEMA_VERSION,
        parameter_snapshot=payload,
        parameter_hash=parameter_hash,
        code_commit_sha=code_version,
    )


def resolve_code_version() -> str:
    for key in ("RAILWAY_GIT_COMMIT_SHA", "GITHUB_SHA", "SOURCE_VERSION", "COMMIT_SHA"):
        value = os.environ.get(key, "").strip()
        if value:
            return value[:128]
    return "unknown-local"


def evaluate_momentum_v2(context: StrategyEvaluationContext) -> V2Evaluation:
    features = _features(context)
    snapshot = _feature_snapshot(context, features)
    candidate_side = features["candidate_side"]
    mode = features["candidate_mode"]
    tier = _timing_tier(context.seconds_since_open, context.seconds_left)
    quality = features["quality_state"]
    hard_gates: list[str] = []
    warnings: list[str] = []

    if not quality["reference_ready"] or not quality["book_ready"] or not quality["market_ready"]:
        hard_gates.append("v2_prerequisite_data_missing_or_stale")
    if context.seconds_since_open is None or context.seconds_since_open < 120:
        hard_gates.append("v2_first_120_seconds")
    if context.seconds_left is None or context.seconds_left <= 60:
        hard_gates.append("v2_final_60_seconds")
    if candidate_side is None or context.boundary is None:
        hard_gates.append("v2_candidate_or_boundary_missing")
    if tier is None:
        hard_gates.append("v2_timing_tier_unavailable")
    if features["distance_bps"] is None or features["distance_bps"] < Decimal("1.5"):
        hard_gates.append("v2_boundary_distance_below_1_5_bps")
    if not features["fast_impulse_active"]:
        hard_gates.append("v2_fast_impulse_not_active")
    if (
        features["reversal_beyond_origin"]
        or features["retrace_fraction"] > Decimal("0.70")
        or features["boundary_crosses_90s"] > 2
    ):
        hard_gates.append("v2_severe_path_reversal")
    if (
        features["return_60s"] is not None
        and features["return_120s"] is not None
        and features["return_60s"] <= Decimal("-3")
        and features["return_120s"] <= Decimal("-5")
    ):
        hard_gates.append("v2_severe_long_context_opposition")
    if (
        features["contract_move_15s_cents"] is not None
        and features["contract_move_30s_cents"] is not None
        and features["contract_move_15s_cents"] <= Decimal("-2")
        and features["contract_move_30s_cents"] <= Decimal("-2")
    ):
        hard_gates.append("v2_material_contract_divergence")
    if features["persistent_adverse_microstructure"]:
        hard_gates.append("v2_persistent_adverse_microstructure")

    desired_ask = features["desired_ask"]
    desired_spread_cents = features["desired_spread_cents"]
    if desired_ask is None:
        hard_gates.append("v2_desired_ask_missing")
    elif desired_ask < Decimal("0.56"):
        hard_gates.append("v2_desired_ask_below_hard_range")
    elif tier is not None and desired_ask > _tier_value(tier, "max_ask"):
        hard_gates.append("v2_desired_ask_above_tier_cap")
    if desired_spread_cents is None or desired_spread_cents > Decimal("4"):
        hard_gates.append("v2_spread_above_4_cents")
    if features["desired_ask_depth"] is None or features["desired_ask_depth"] < Decimal("2"):
        hard_gates.append("v2_executable_depth_below_two_contracts")

    score, components = _score(features, tier)
    edge = _edge(features)
    threshold = _tier_value(tier, "score") if tier is not None else None
    edge_threshold = Decimal("1.5")
    measurements: dict[str, JsonPayload] = {
        "architecture_version": V2_ARCHITECTURE_VERSION,
        "feature_schema_version": V2_FEATURE_SCHEMA_VERSION,
        "edge_model_version": V2_EDGE_MODEL_VERSION,
        "candidate_mode": mode,
        "timing_tier": tier,
        "hard_gates": hard_gates,
        "features": _json_safe(features),
        "score": {
            "components": _json_safe(components),
            "total": str(score),
            "threshold": str(threshold) if threshold is not None else None,
            "margin": str(score - threshold) if threshold is not None else None,
        },
        "edge": {"lower_bound_cents": str(edge), "threshold_cents": str(edge_threshold)},
    }
    snapshot = _feature_snapshot(context, features, execution=measurements["edge"])
    if hard_gates:
        state = (
            STATE_V2_FEATURES_NOT_READY
            if hard_gates[0].startswith("v2_prerequisite")
            else STATE_V2_HARD_GATE_BLOCKED
        )
        return V2Evaluation(
            state,
            hard_gates[0],
            hard_gates,
            warnings,
            candidate_side,
            mode,
            tier,
            None,
            score,
            threshold,
            edge,
            snapshot,
            measurements,
        )
    if mode != "CONTINUATION":
        return V2Evaluation(
            STATE_V2_HARD_GATE_BLOCKED,
            "v2_candidate_mode_not_enabled",
            ["v2_candidate_mode_not_enabled"],
            warnings,
            candidate_side,
            mode,
            tier,
            None,
            score,
            threshold,
            edge,
            snapshot,
            measurements,
        )
    if threshold is None or score < threshold:
        return V2Evaluation(
            STATE_V2_SCORE_BELOW_THRESHOLD,
            "v2_score_below_threshold",
            [],
            warnings,
            candidate_side,
            mode,
            tier,
            None,
            score,
            threshold,
            edge,
            snapshot,
            measurements,
        )
    if edge < edge_threshold:
        return V2Evaluation(
            STATE_V2_EDGE_BELOW_THRESHOLD,
            "v2_edge_below_threshold",
            [],
            warnings,
            candidate_side,
            mode,
            tier,
            None,
            score,
            threshold,
            edge,
            snapshot,
            measurements,
        )
    intended = min((desired_ask or Decimal("0")) + Decimal("0.01"), _tier_value(tier, "max_ask"))
    measurements["intended_entry_price"] = str(intended)
    return V2Evaluation(
        STATE_V2_ENTRY_SIGNAL,
        "v2_entry_signal",
        [],
        warnings,
        candidate_side,
        mode,
        tier,
        intended,
        score,
        threshold,
        edge,
        snapshot,
        measurements,
    )


def clip01(value: Decimal | float) -> Decimal:
    return max(Decimal("0"), min(Decimal("1"), Decimal(str(value))))


def _features(context: StrategyEvaluationContext) -> dict[str, Any]:
    latest = context.brti_value
    side = context.candidate_side
    returns = {
        f"return_{window}s": _oriented_return(context, window, side)
        for window in V2_PARAMETERS["feature_windows_seconds"]
    }
    changes = _one_second_changes(context, side)
    values = [change for _, change in changes]
    net = sum(values, Decimal("0"))
    total_abs = sum((abs(value) for value in values), Decimal("0"))
    efficiency = Decimal("0") if total_abs == 0 else min(Decimal("1"), abs(net) / total_abs)
    directional = [value for value in values if value > 0]
    ratio = Decimal(len(directional)) / Decimal(len(values)) if values else Decimal("0")
    peak = max(
        [
            Decimal("0"),
            *(_oriented_return(context, window, side) or Decimal("0") for window in (5, 15, 30)),
        ]
    )
    current = returns["return_30s"] or Decimal("0")
    retrace = Decimal("0") if peak <= 0 else clip01((peak - current) / peak)
    reversal = current < 0
    volatility = _sample_stddev_bps(context)
    distance_bps = _distance_bps(latest, context.boundary)
    expected_120 = volatility * Decimal(str(math.sqrt(min(context.seconds_left or 0, 120))))
    expected_remaining = volatility * Decimal(str(math.sqrt(max(context.seconds_left or 0, 1))))
    desired_bid, desired_ask, desired_spread, bid_depth, ask_depth = _desired_book(
        context.orderbook, side
    )
    desired_mid = (
        None
        if desired_bid is None or desired_ask is None
        else (desired_bid + desired_ask) / Decimal("2")
    )
    contract_moves = {
        f"contract_move_{window}s_cents": _contract_move(context, side, window, desired_mid)
        for window in (5, 15, 30, 60)
    }
    impulse = max(
        Decimal("0"), returns["return_15s"] or Decimal("0"), returns["return_30s"] or Decimal("0")
    )
    sensitivity = (
        Decimal("0.5")
        if desired_mid is None
        else max(
            Decimal("0.5"),
            min(Decimal("1"), Decimal("4") * desired_mid * (Decimal("1") - desired_mid)),
        )
    )
    multiplier = (
        Decimal("1")
        if (context.seconds_since_open or 0) >= 300 and (context.seconds_left or 0) > 60
        else Decimal("0.85")
        if (context.seconds_since_open or 0) < 300
        else Decimal("0.80")
    )
    expected_move = min(Decimal("12"), Decimal("0.90") * impulse * sensitivity * multiplier)
    observed_move = max(
        Decimal("0"),
        contract_moves["contract_move_15s_cents"] or Decimal("0"),
        contract_moves["contract_move_30s_cents"] or Decimal("0"),
    )
    residual = expected_move - observed_move
    micro = _microstructure(context, side)
    trade_ratio, trade_count = _trade_flow(context, side)
    fast_active = (
        (returns["return_15s"] or Decimal("-999")) >= Decimal("1.25")
        or (returns["return_30s"] or Decimal("-999")) >= Decimal("2")
    ) and (returns["return_5s"] or Decimal("-999")) > Decimal("-0.5")
    return {
        "candidate_side": side,
        "candidate_mode": "CONTINUATION" if fast_active else "UNSTABLE",
        "current_brti": latest,
        **returns,
        "velocity_bps": returns["return_5s"],
        "acceleration_bps": (returns["return_5s"] or Decimal("0"))
        - (returns["return_15s"] or Decimal("0")) / Decimal("3"),
        "point_count": len(context.reference_ticks),
        "directional_tick_ratio_30s": ratio,
        "directional_efficiency_30s": efficiency,
        "sign_change_count": _sign_changes(values),
        "longest_counter_run": _longest_counter_run(values),
        "impulse_hold_seconds": _impulse_hold(values),
        "retrace_fraction": retrace,
        "reversal_beyond_origin": reversal,
        "boundary_crosses_30s": _boundary_crosses(context, 30),
        "boundary_crosses_60s": _boundary_crosses(context, 60),
        "boundary_crosses_90s": _boundary_crosses(context, 90),
        "sigma_1s_bps": volatility,
        "distance_bps": distance_bps,
        "expected_move_120s_bps": expected_120,
        "expected_move_remaining_bps": expected_remaining,
        "standardized_distance_120s": None
        if distance_bps is None
        else distance_bps / max(expected_120, Decimal("0.5")),
        "standardized_distance_remaining": None
        if distance_bps is None
        else distance_bps / max(expected_remaining, Decimal("0.5")),
        "desired_bid": desired_bid,
        "desired_ask": desired_ask,
        "desired_spread_cents": None if desired_spread is None else desired_spread * Decimal("100"),
        "desired_mid": desired_mid,
        "desired_bid_depth": bid_depth,
        "desired_ask_depth": ask_depth,
        **contract_moves,
        "expected_contract_move_cents": expected_move,
        "response_residual_cents": residual,
        **micro,
        "trade_ratio": trade_ratio,
        "trade_count": trade_count,
        "fast_impulse_active": fast_active,
        "persistent_adverse_microstructure": micro["top5_imbalance_support_fraction"]
        <= Decimal("0.30")
        and micro["top5_imbalance"] <= Decimal("-0.60")
        and trade_count >= 3
        and trade_ratio <= Decimal("0.35"),
        "quality_state": {
            "market_ready": context.market is not None and context.boundary is not None,
            "reference_ready": latest is not None,
            "book_ready": desired_bid is not None
            and desired_ask is not None
            and desired_bid < desired_ask,
        },
    }


def _feature_snapshot(
    context: StrategyEvaluationContext,
    features: dict[str, Any],
    execution: JsonPayload | None = None,
) -> StrategyFeatureSnapshotInput:
    key = {
        "market": getattr(context.market, "market_ticker", None),
        "at": int(context.evaluated_at.timestamp()),
        "schema": V2_FEATURE_SCHEMA_VERSION,
        "context": _context_hash(context),
    }
    return StrategyFeatureSnapshotInput(
        feature_snapshot_id=f"feature-{_hash(key)[:28]}",
        evaluated_at=context.evaluated_at,
        feature_schema_version=V2_FEATURE_SCHEMA_VERSION,
        context_hash=_context_hash(context),
        market_ticker=getattr(context.market, "market_ticker", None),
        candidate_side=features["candidate_side"],
        candidate_mode=features["candidate_mode"],
        boundary=context.boundary,
        current_brti=context.brti_value,
        seconds_since_open=context.seconds_since_open,
        seconds_left=context.seconds_left,
        reference_tick_id=getattr(context.reference_tick, "id", None),
        orderbook_snapshot_id=getattr(context.orderbook, "id", None),
        public_trade_id=getattr(context.latest_trade, "id", None),
        quality_state=_json_safe(features["quality_state"]),
        reference_features=_json_safe(
            {
                key: value
                for key, value in features.items()
                if key.startswith("return_")
                or key
                in {
                    "sigma_1s_bps",
                    "retrace_fraction",
                    "reversal_beyond_origin",
                    "directional_efficiency_30s",
                    "directional_tick_ratio_30s",
                }
            }
        ),
        contract_features=_json_safe(
            {
                key: value
                for key, value in features.items()
                if key.startswith("contract_")
                or key
                in {
                    "desired_ask",
                    "desired_bid",
                    "desired_mid",
                    "response_residual_cents",
                    "expected_contract_move_cents",
                }
            }
        ),
        microstructure_features=_json_safe(
            {
                key: value
                for key, value in features.items()
                if "imbalance" in key or key in {"trade_ratio", "trade_count"}
            }
        ),
        execution_features=execution,
    )


def _score(features: dict[str, Any], tier: str | None) -> tuple[Decimal, dict[str, Decimal]]:
    confirmation = _confirmation_score(features["contract_move_15s_cents"])
    components = {
        "fast_impulse": Decimal("5")
        * clip01((features["return_5s"] or Decimal("0")) / Decimal("1.25"))
        + Decimal("8") * clip01((features["return_15s"] or Decimal("0")) / Decimal("2.5"))
        + Decimal("8") * clip01((features["return_30s"] or Decimal("0")) / Decimal("4"))
        + Decimal("4") * clip01(Decimal(features["impulse_hold_seconds"]) / Decimal("10")),
        "path_quality": Decimal("10")
        * clip01((features["directional_efficiency_30s"] - Decimal("0.25")) / Decimal("0.5"))
        + Decimal("6") * clip01((Decimal("0.70") - features["retrace_fraction"]) / Decimal("0.70"))
        + Decimal("4")
        * clip01((features["directional_tick_ratio_30s"] - Decimal("0.50")) / Decimal("0.30")),
        "underreaction": Decimal("12") * clip01(features["response_residual_cents"] / Decimal("6"))
        + confirmation
        + Decimal("4")
        * clip01((Decimal("0.78") - (features["desired_ask"] or Decimal("1"))) / Decimal("0.12")),
        "boundary_regime": Decimal("5")
        * clip01(((features["distance_bps"] or Decimal("0")) - Decimal("1.5")) / Decimal("5"))
        + Decimal("7")
        * clip01((features["standardized_distance_120s"] or Decimal("0")) / Decimal("1.5"))
        + _context_alignment(features),
        "microstructure": Decimal("4")
        * clip01((features["top5_imbalance"] + Decimal("0.5")) / Decimal("1"))
        + Decimal("3") * clip01((features["order_flow_5s"] + Decimal("0.5")) / Decimal("1"))
        + (
            Decimal("1.5")
            if features["trade_count"] < 3
            else Decimal("3")
            * clip01((features["trade_ratio"] - Decimal("0.35")) / Decimal("0.45"))
        ),
        "timing_economics": _price_quality(features["desired_ask"], tier)
        + (Decimal("2") if tier == "normal" else Decimal("1"))
        + Decimal("6") * clip01(_edge(features) / Decimal("6")),
    }
    total = sum(components.values(), Decimal("0"))
    return total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), {
        key: value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        for key, value in components.items()
    }


def _edge(features: dict[str, Any]) -> Decimal:
    spread = features["desired_spread_cents"] or Decimal("99")
    uncertainty = max(Decimal("0.75"), Decimal("0.25") * features["expected_contract_move_cents"])
    return (features["response_residual_cents"] - (spread + Decimal("1")) - uncertainty).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )


def _oriented_return(
    context: StrategyEvaluationContext, window: int, side: str | None
) -> Decimal | None:
    latest = context.brti_value
    if latest is None or side is None:
        return None
    before = context.evaluated_at - timedelta(seconds=window)
    anchor = next(
        (
            row
            for row in context.reference_ticks
            if _utc(row.received_at) >= before and row.parsed_value is not None
        ),
        None,
    )
    if anchor is None or Decimal(anchor.parsed_value) == 0:
        return None
    raw = (latest - Decimal(anchor.parsed_value)) / Decimal(anchor.parsed_value) * Decimal("10000")
    return raw if side == "YES" else -raw


def _desired_book(
    book: OrderbookSnapshot | None, side: str | None
) -> tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None]:
    if book is None or side not in {"YES", "NO"}:
        return None, None, None, None, None
    if side == "YES":
        bid, ask, bid_depth, ask_depth = (
            book.yes_bid,
            book.yes_ask,
            book.yes_bid_count,
            book.yes_ask_count,
        )
    else:
        bid, ask, bid_depth, ask_depth = (
            book.no_bid,
            book.no_ask,
            book.no_bid_count,
            book.no_ask_count,
        )
    bid_value = Decimal(bid) if bid is not None else None
    ask_value = Decimal(ask) if ask is not None else None
    return (
        bid_value,
        ask_value,
        None if bid_value is None or ask_value is None else ask_value - bid_value,
        Decimal(bid_depth) if bid_depth is not None else None,
        Decimal(ask_depth) if ask_depth is not None else None,
    )


def _contract_move(
    context: StrategyEvaluationContext, side: str | None, window: int, current_mid: Decimal | None
) -> Decimal | None:
    if current_mid is None:
        return None
    cutoff = context.evaluated_at - timedelta(seconds=window)
    prior = next(
        (row for row in context.orderbook_history if _utc(row.received_at) >= cutoff),
        None,
    )
    bid, ask, _, _, _ = _desired_book(prior, side)
    if bid is None or ask is None:
        return None
    return (current_mid - (bid + ask) / Decimal("2")) * Decimal("100")


def _one_second_changes(
    context: StrategyEvaluationContext, side: str | None
) -> list[tuple[int, Decimal]]:
    rows = [
        row
        for row in context.reference_ticks
        if row.parsed_value is not None
        and _utc(row.received_at) >= context.evaluated_at - timedelta(seconds=60)
    ]
    changes: list[tuple[int, Decimal]] = []
    for prior, current in zip(rows, rows[1:], strict=False):
        previous = Decimal(prior.parsed_value)
        if previous == 0:
            continue
        value = (Decimal(current.parsed_value) - previous) / previous * Decimal("10000")
        changes.append(
            (
                int((_utc(current.received_at) - _utc(prior.received_at)).total_seconds()),
                value if side == "YES" else -value,
            )
        )
    return changes


def _sample_stddev_bps(context: StrategyEvaluationContext) -> Decimal:
    values = [value for _, value in _one_second_changes(context, "YES")]
    if len(values) < 2:
        return Decimal("0")
    mean = sum(values, Decimal("0")) / Decimal(len(values))
    return (
        sum(((value - mean) ** 2 for value in values), Decimal("0")) / Decimal(len(values) - 1)
    ).sqrt()


def _distance_bps(value: Decimal | None, boundary: Decimal | None) -> Decimal | None:
    if value is None or boundary is None or boundary == 0:
        return None
    return abs(value - boundary) / abs(boundary) * Decimal("10000")


def _boundary_crosses(context: StrategyEvaluationContext, window: int) -> int:
    if context.boundary is None:
        return 0
    signs = [
        Decimal(row.parsed_value) > context.boundary
        for row in context.reference_ticks
        if row.parsed_value is not None
        and _utc(row.received_at) >= context.evaluated_at - timedelta(seconds=window)
    ]
    return sum(left != right for left, right in zip(signs, signs[1:], strict=False))


def _microstructure(context: StrategyEvaluationContext, side: str | None) -> dict[str, Decimal]:
    rows = context.orderbook_history[-5:]
    imbalances: list[Decimal] = []
    for row in rows:
        bid, ask, _, bid_depth, ask_depth = _desired_book(row, side)
        del bid, ask
        if bid_depth is not None and ask_depth is not None and bid_depth + ask_depth > 0:
            imbalances.append((bid_depth - ask_depth) / (bid_depth + ask_depth))
    top5 = sum(imbalances, Decimal("0")) / Decimal(len(imbalances)) if imbalances else Decimal("0")
    support = (
        Decimal(sum(value > 0 for value in imbalances)) / Decimal(len(imbalances))
        if imbalances
        else Decimal("0")
    )
    flow = Decimal("0")
    if len(rows) >= 2:
        _, _, _, first_bid, first_ask = _desired_book(rows[0], side)
        _, _, _, last_bid, last_ask = _desired_book(rows[-1], side)
        if None not in (first_bid, first_ask, last_bid, last_ask):
            delta = (last_bid - first_bid) - (last_ask - first_ask)
            denominator = abs(last_bid - first_bid) + abs(last_ask - first_ask)
            flow = Decimal("0") if denominator == 0 else delta / denominator
    return {
        "top5_imbalance": top5,
        "top5_imbalance_support_fraction": support,
        "order_flow_5s": flow,
        "order_flow_15s": flow,
    }


def _trade_flow(context: StrategyEvaluationContext, side: str | None) -> tuple[Decimal, int]:
    usable = [row for row in context.recent_trades if row.taker_side in {"yes", "no", "YES", "NO"}]
    total = sum((Decimal(row.trade_count or row.count or 1) for row in usable), Decimal("0"))
    if not usable or total == 0 or side is None:
        return Decimal("0.5"), 0
    desired = sum(
        (
            Decimal(row.trade_count or row.count or 1)
            for row in usable
            if str(row.taker_side).upper() == side
        ),
        Decimal("0"),
    )
    return desired / total, len(usable)


def _timing_tier(since_open: int | None, seconds_left: int | None) -> str | None:
    if since_open is None or seconds_left is None or since_open < 120 or seconds_left <= 60:
        return None
    if since_open < 300:
        return "early"
    if since_open < 780:
        return "normal"
    if since_open < 840:
        return "late"
    return None


def _tier_value(tier: str, field: str) -> Decimal:
    return Decimal(str(V2_PARAMETERS["tiers"][tier][field]))


def _confirmation_score(move: Decimal | None) -> Decimal:
    if move is None or move < 0:
        return Decimal("0")
    if move <= 1:
        return Decimal("2")
    if move <= 5:
        return Decimal("4")
    if move <= 10:
        return Decimal("4") - (move - Decimal("5")) * Decimal("0.6")
    return Decimal("0")


def _context_alignment(features: dict[str, Any]) -> Decimal:
    aligned = sum((features[f"return_{window}s"] or Decimal("0")) > 0 for window in (60, 120))
    return Decimal("3") if aligned == 2 else Decimal("1.5") if aligned == 1 else Decimal("0")


def _price_quality(ask: Decimal | None, tier: str | None) -> Decimal:
    if ask is None or tier is None:
        return Decimal("0")
    if Decimal("0.58") <= ask <= Decimal("0.72"):
        return Decimal("2")
    limit = _tier_value(tier, "max_ask")
    if ask < Decimal("0.58"):
        return Decimal("2") * clip01((ask - Decimal("0.56")) / Decimal("0.02"))
    return Decimal("2") * clip01((limit - ask) / max(limit - Decimal("0.72"), Decimal("0.01")))


def _sign_changes(values: list[Decimal]) -> int:
    signs = [value > 0 for value in values if value != 0]
    return sum(left != right for left, right in zip(signs, signs[1:], strict=False))


def _longest_counter_run(values: list[Decimal]) -> int:
    longest = current = 0
    for value in values:
        current = current + 1 if value < 0 else 0
        longest = max(longest, current)
    return longest


def _impulse_hold(values: list[Decimal]) -> int:
    hold = 0
    for value in reversed(values):
        if value < 0:
            break
        hold += 1
    return hold


def _context_hash(context: StrategyEvaluationContext) -> str:
    return _hash(
        {
            "market": getattr(context.market, "market_ticker", None),
            "reference": getattr(context.reference_tick, "id", None),
            "book": getattr(context.orderbook, "id", None),
            "trade": getattr(context.latest_trade, "id", None),
            "at": int(context.evaluated_at.timestamp()),
        }
    )


def _hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(_json_safe(value), sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
