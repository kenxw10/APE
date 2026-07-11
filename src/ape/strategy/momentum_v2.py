from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from ape.config import AppConfig
from ape.db.models import OrderbookSnapshot, PublicTrade
from ape.repositories.inputs import (
    JsonPayload,
    StrategyConfigVersionInput,
    StrategyFeatureSnapshotInput,
)
from ape.strategy.context import StrategyEvaluationContext

V2_STRATEGY_ID = "btc15_momentum_v2"
V2_ARCHITECTURE_VERSION = "momentum_v2_heuristic_v2"
V2_FEATURE_SCHEMA_VERSION = "momentum_v2_features_v2"
V2_LIFECYCLE_SCHEMA_VERSION = "momentum_v2_lifecycle_v1"
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
    architecture_version = (
        V2_ARCHITECTURE_VERSION
        if strategy_id == V2_STRATEGY_ID
        else "momentum_v1_legacy"
    )
    feature_schema_version = (
        V2_FEATURE_SCHEMA_VERSION
        if strategy_id == V2_STRATEGY_ID
        else "momentum_v1_legacy"
    )
    version_hash = _hash(
        {
            "parameters": parameter_hash,
            "code": code_version,
            "architecture": architecture_version,
            "feature_schema": feature_schema_version,
            "lifecycle": (
                V2_LIFECYCLE_SCHEMA_VERSION if strategy_id == V2_STRATEGY_ID else None
            ),
        }
    )
    version_id = f"config-{strategy_id}-{version_hash[:20]}"
    return StrategyConfigVersionInput(
        strategy_config_version_id=version_id,
        strategy_id=strategy_id,
        architecture_version=architecture_version,
        feature_schema_version=feature_schema_version,
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


def evaluate_momentum_v2(
    context: StrategyEvaluationContext,
    *,
    config: AppConfig,
) -> V2Evaluation:
    features = _features(context, config=config)
    snapshot = _feature_snapshot(context, features)
    candidate_side = features["candidate_side"]
    mode = features["candidate_mode"]
    tier = _timing_tier(context.seconds_since_open, context.seconds_left)
    quality = features["quality_state"]
    hard_gates: list[str] = []
    warnings: list[str] = []

    if (
        not quality["reference_ready"]
        or not quality["book_ready"]
        or not quality["market_ready"]
        or not quality.get("canonical_market_ready", True)
        or not quality.get("canonical_reference_ready", True)
    ):
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
    if mode not in {"BOUNDARY_CROSS_HOLD", "CONTINUATION"}:
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


def _features(context: StrategyEvaluationContext, *, config: AppConfig) -> dict[str, Any]:
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
    last_cross_at, cross_hold_seconds = _boundary_cross_hold(context, side)
    candidate_mode = (
        "BOUNDARY_CROSS_HOLD"
        if last_cross_at is not None and cross_hold_seconds is not None and cross_hold_seconds >= 5
        else "CONTINUATION"
        if fast_active
        else "UNSTABLE"
    )
    return {
        "candidate_side": side,
        "candidate_mode": candidate_mode,
        "last_boundary_cross_at": last_cross_at,
        "boundary_cross_hold_seconds": cross_hold_seconds,
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
        "impulse_retention_fraction": Decimal("1") - retrace,
        "reversal_overshoot_bps": max(Decimal("0"), -current),
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
        "contract_response_velocity_5s": _velocity(contract_moves["contract_move_5s_cents"], 5),
        "contract_response_velocity_15s": _velocity(contract_moves["contract_move_15s_cents"], 15),
        "contract_response_velocity_30s": _velocity(contract_moves["contract_move_30s_cents"], 30),
        "fast_medium_response_acceleration": _velocity(contract_moves["contract_move_5s_cents"], 5)
        - _velocity(contract_moves["contract_move_15s_cents"], 15),
        "medium_slow_response_acceleration": _velocity(
            contract_moves["contract_move_15s_cents"], 15
        )
        - _velocity(contract_moves["contract_move_30s_cents"], 30),
        "contract_brti_response_ratio_15s": _response_ratio(
            contract_moves["contract_move_15s_cents"], returns["return_15s"]
        ),
        "contract_brti_response_ratio_30s": _response_ratio(
            contract_moves["contract_move_30s_cents"], returns["return_30s"]
        ),
        "expected_contract_move_cents": expected_move,
        "response_residual_cents": residual,
        "full_repricing_state": observed_move >= expected_move,
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
            "reference_ready": latest is not None
            and _timestamp_within_age(
                getattr(context.reference_tick, "received_at", None),
                context.evaluated_at,
                config.strategy_reference_max_age_ms,
            )
            and (
                getattr(context.reference_tick, "source_age_ms", None) is None
                or context.reference_tick.source_age_ms
                <= config.strategy_reference_source_max_age_ms
            ),
            "book_ready": desired_bid is not None
            and desired_ask is not None
            and desired_bid < desired_ask
            and _timestamp_within_age(
                getattr(context.orderbook, "received_at", None),
                context.evaluated_at,
                config.strategy_kalshi_book_max_age_ms,
            ),
            "canonical_market_ready": _canonical_market_ready(context),
            "canonical_reference_ready": _canonical_reference_ready(context),
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
                if "imbalance" in key
                or key
                in {
                    "trade_ratio",
                    "trade_count",
                    "order_flow_5s",
                    "order_flow_15s",
                    "desired_bid_replenishment",
                    "opposing_ask_depletion",
                    "depth_withdrawal_pressure",
                }
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
    bid_ladder, ask_ladder = _executable_ladders(book, side)
    if not bid_ladder or not ask_ladder:
        return None, None, None, None, None
    bid_value, bid_depth = bid_ladder[0]
    ask_value, ask_depth = ask_ladder[0]
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
    rows = tuple(context.orderbook_history)
    current_bid, current_ask = _executable_ladders(context.orderbook, side)
    if not current_bid or not current_ask:
        return _empty_microstructure()
    depths = {
        f"bid_depth_{level}": _cumulative_depth(current_bid, level) for level in (1, 3, 5)
    } | {f"ask_depth_{level}": _cumulative_depth(current_ask, level) for level in (1, 3, 5)}
    imbalances = {
        f"imbalance_{level}": _imbalance(depths[f"bid_depth_{level}"], depths[f"ask_depth_{level}"])
        for level in (1, 3, 5)
    }
    five_rows = _rows_since(rows, context.evaluated_at, 5)
    fifteen_rows = _rows_since(rows, context.evaluated_at, 15)
    history = [
        _imbalance(_cumulative_depth(bid, 5), _cumulative_depth(ask, 5))
        for row in fifteen_rows
        if (ladders := _executable_ladders(row, side))
        and (bid := ladders[0])
        and (ask := ladders[1])
    ]
    replenishment, depletion, withdrawal = _depth_pressures(fifteen_rows, side)
    return {
        **depths,
        **imbalances,
        "top5_imbalance": imbalances["imbalance_5"],
        "top5_imbalance_support_fraction": _positive_fraction(history),
        "order_flow_5s": _order_flow_imbalance(five_rows, side),
        "order_flow_15s": _order_flow_imbalance(fifteen_rows, side),
        "desired_bid_replenishment": replenishment,
        "opposing_ask_depletion": depletion,
        "depth_withdrawal_pressure": withdrawal,
        "imbalance_support_persistence": _positive_fraction(history),
        "adverse_pressure_persistence": _positive_fraction([-value for value in history]),
        "replenishment_persistence": _positive_fraction(
            _pressure_series(fifteen_rows, side, "replenishment")
        ),
        "depletion_persistence": _positive_fraction(
            _pressure_series(fifteen_rows, side, "depletion")
        ),
        "depth_withdrawal_persistence": _positive_fraction(
            _pressure_series(fifteen_rows, side, "withdrawal")
        ),
        "adverse_contract_divergence_persistence_5s": _adverse_divergence_persistence(
            five_rows, side
        ),
        "persistent_adverse_microstructure": imbalances["imbalance_5"] <= Decimal("-0.60"),
    }


def _executable_ladders(
    book: OrderbookSnapshot | None, side: str | None
) -> tuple[list[tuple[Decimal, Decimal]], list[tuple[Decimal, Decimal]]]:
    if (
        book is None
        or side not in {"YES", "NO"}
        or book.ladder_schema_version != "kalshi_executable_ladders_v2"
    ):
        return [], []
    bid_raw = book.yes_bid_ladder if side == "YES" else book.no_bid_ladder
    ask_raw = book.yes_ask_ladder if side == "YES" else book.no_ask_ladder
    return _parse_ladder(bid_raw), _parse_ladder(ask_raw)


def _parse_ladder(value: Any) -> list[tuple[Decimal, Decimal]]:
    if not isinstance(value, list):
        return []
    levels: list[tuple[Decimal, Decimal]] = []
    for level in value[:5]:
        if not isinstance(level, dict):
            return []
        try:
            price = Decimal(str(level["price"]))
            count = Decimal(str(level["count"]))
        except (KeyError, ArithmeticError, ValueError):
            return []
        if price < 0 or price > 1 or count <= 0:
            return []
        levels.append((price, count))
    return levels


def _cumulative_depth(levels: list[tuple[Decimal, Decimal]], count: int) -> Decimal:
    return sum((size for _, size in levels[:count]), Decimal("0"))


def _imbalance(bid_depth: Decimal, ask_depth: Decimal) -> Decimal:
    total = bid_depth + ask_depth
    return Decimal("0") if total <= 0 else (bid_depth - ask_depth) / total


def _rows_since(
    rows: tuple[OrderbookSnapshot, ...], evaluated_at: datetime, seconds: int
) -> tuple[OrderbookSnapshot, ...]:
    cutoff = _utc(evaluated_at) - timedelta(seconds=seconds)
    return tuple(row for row in rows if _utc(row.received_at) >= cutoff)


def _order_flow_imbalance(rows: tuple[OrderbookSnapshot, ...], side: str | None) -> Decimal:
    signed = Decimal("0")
    magnitude = Decimal("0")
    for prior, current in zip(rows, rows[1:], strict=False):
        prior_bid, prior_ask = _executable_ladders(prior, side)
        current_bid, current_ask = _executable_ladders(current, side)
        if not prior_bid or not prior_ask or not current_bid or not current_ask:
            continue
        prior_bid_price, current_bid_price = prior_bid[0][0], current_bid[0][0]
        prior_ask_price, current_ask_price = prior_ask[0][0], current_ask[0][0]
        prior_bid_depth = _cumulative_depth(prior_bid, 5)
        current_bid_depth = _cumulative_depth(current_bid, 5)
        prior_ask_depth = _cumulative_depth(prior_ask, 5)
        current_ask_depth = _cumulative_depth(current_ask, 5)
        bid_pressure = (
            current_bid_depth
            if current_bid_price > prior_bid_price
            else -prior_bid_depth
            if current_bid_price < prior_bid_price
            else current_bid_depth - prior_bid_depth
        )
        ask_pressure = (
            current_ask_depth
            if current_ask_price > prior_ask_price
            else -prior_ask_depth
            if current_ask_price < prior_ask_price
            else prior_ask_depth - current_ask_depth
        )
        signed += bid_pressure + ask_pressure
        magnitude += abs(bid_pressure) + abs(ask_pressure)
    return max(Decimal("-1"), min(Decimal("1"), signed / magnitude)) if magnitude else Decimal("0")


def _depth_pressures(
    rows: tuple[OrderbookSnapshot, ...], side: str | None
) -> tuple[Decimal, Decimal, Decimal]:
    series = _pressure_series(rows, side, "all")
    if not series:
        return Decimal("0"), Decimal("0"), Decimal("0")
    replenishment = sum((item[0] for item in series), Decimal("0"))
    depletion = sum((item[1] for item in series), Decimal("0"))
    withdrawal = sum((item[2] for item in series), Decimal("0"))
    scale = max(
        Decimal("1"),
        sum((abs(value) for triplet in series for value in triplet), Decimal("0")),
    )
    return replenishment / scale, depletion / scale, withdrawal / scale


def _pressure_series(rows: tuple[OrderbookSnapshot, ...], side: str | None, kind: str) -> list[Any]:
    values: list[tuple[Decimal, Decimal, Decimal]] = []
    for prior, current in zip(rows, rows[1:], strict=False):
        prior_bid, prior_ask = _executable_ladders(prior, side)
        current_bid, current_ask = _executable_ladders(current, side)
        if not prior_bid or not prior_ask or not current_bid or not current_ask:
            continue
        bid_delta = _cumulative_depth(current_bid, 5) - _cumulative_depth(prior_bid, 5)
        ask_delta = _cumulative_depth(current_ask, 5) - _cumulative_depth(prior_ask, 5)
        values.append(
            (
                max(bid_delta, Decimal("0")),
                max(-ask_delta, Decimal("0")),
                max(-bid_delta, Decimal("0")),
            )
        )
    if kind == "all":
        return values
    index = {"replenishment": 0, "depletion": 1, "withdrawal": 2}[kind]
    return [triplet[index] for triplet in values]


def _positive_fraction(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    return Decimal(sum(value > 0 for value in values)) / Decimal(len(values))


def _adverse_divergence_persistence(
    rows: tuple[OrderbookSnapshot, ...], side: str | None
) -> Decimal:
    return _positive_fraction([-_order_flow_imbalance(rows, side)])


def _empty_microstructure() -> dict[str, Decimal]:
    return {
        **{f"bid_depth_{level}": Decimal("0") for level in (1, 3, 5)},
        **{f"ask_depth_{level}": Decimal("0") for level in (1, 3, 5)},
        **{f"imbalance_{level}": Decimal("0") for level in (1, 3, 5)},
        "top5_imbalance": Decimal("0"),
        "top5_imbalance_support_fraction": Decimal("0"),
        "order_flow_5s": Decimal("0"),
        "order_flow_15s": Decimal("0"),
        "desired_bid_replenishment": Decimal("0"),
        "opposing_ask_depletion": Decimal("0"),
        "depth_withdrawal_pressure": Decimal("0"),
        "imbalance_support_persistence": Decimal("0"),
        "adverse_pressure_persistence": Decimal("0"),
        "replenishment_persistence": Decimal("0"),
        "depletion_persistence": Decimal("0"),
        "depth_withdrawal_persistence": Decimal("0"),
        "adverse_contract_divergence_persistence_5s": Decimal("0"),
        "persistent_adverse_microstructure": Decimal("0"),
    }


def _boundary_cross_hold(
    context: StrategyEvaluationContext, side: str | None
) -> tuple[datetime | None, int | None]:
    if context.boundary is None or side not in {"YES", "NO"}:
        return None, None
    rows = [
        row
        for row in context.reference_ticks
        if row.parsed_value is not None
        and _utc(row.received_at) >= _utc(context.evaluated_at) - timedelta(seconds=30)
    ]
    cross_at: datetime | None = None
    for previous, current in zip(rows, rows[1:], strict=False):
        previous_side = "YES" if Decimal(previous.parsed_value) > context.boundary else "NO"
        current_side = "YES" if Decimal(current.parsed_value) > context.boundary else "NO"
        if previous_side != current_side and current_side == side:
            cross_at = _utc(current.received_at)
    if cross_at is None:
        return None, None
    after_cross = [
        row for row in rows if _utc(row.received_at) >= cross_at and row.parsed_value is not None
    ]
    if not after_cross or any(
        ("YES" if Decimal(row.parsed_value) > context.boundary else "NO") != side
        for row in after_cross
    ):
        return None, None
    return cross_at, max(0, int((_utc(context.evaluated_at) - cross_at).total_seconds()))


def _velocity(value: Decimal | None, seconds: int) -> Decimal:
    return Decimal("0") if value is None else value / Decimal(seconds)


def _response_ratio(contract_cents: Decimal | None, brti_bps: Decimal | None) -> Decimal:
    if contract_cents is None or brti_bps is None or brti_bps == 0:
        return Decimal("0")
    return contract_cents / brti_bps


def _canonical_market_ready(context: StrategyEvaluationContext) -> bool:
    liveness = getattr(context, "market_liveness", None)
    if liveness is None:
        return True
    if liveness.market_feed_transport_state not in {"healthy", "unknown"}:
        return False
    if liveness.market_feed_subscription_state not in {"subscribed", "unknown"}:
        return False
    if liveness.market_feed_snapshot_state in {"missing", "resync_pending", "stale_cap_exceeded"}:
        return False
    if liveness.market_feed_active_ticker_state == "mismatch":
        return False
    if liveness.market_feed_sequence_state in {"gap", "reset"}:
        return False
    return not liveness.market_recovery_attempt_in_progress


def _canonical_reference_ready(context: StrategyEvaluationContext) -> bool:
    liveness = getattr(context, "reference_liveness", None)
    if liveness is None:
        return True
    metadata = liveness.metadata or {}
    blockers = metadata.get("blockers") if isinstance(metadata.get("blockers"), list) else []
    warnings = metadata.get("warnings") if isinstance(metadata.get("warnings"), list) else []
    unsafe = {
        "brti_reference_transport_stale",
        "brti_reference_persistence_stale",
        "brti_reference_worker_heartbeat_stale",
        "brti_reference_first_tick_timeout",
        "brti_reference_no_valid_tick_timeout",
    }
    return not any(str(value) in unsafe for value in [*blockers, *warnings])


def _trade_flow(context: StrategyEvaluationContext, side: str | None) -> tuple[Decimal, int]:
    usable = [
        (row, trade_side)
        for row in context.recent_trades
        if (trade_side := _trade_side(row)) is not None
    ]
    total = sum(
        (Decimal(row.trade_count or row.count or 1) for row, _ in usable),
        Decimal("0"),
    )
    if not usable or total == 0 or side is None:
        return Decimal("0.5"), 0
    desired = sum(
        (
            Decimal(row.trade_count or row.count or 1)
            for row, trade_side in usable
            if trade_side == side
        ),
        Decimal("0"),
    )
    return desired / total, len(usable)


def _trade_side(trade: PublicTrade) -> str | None:
    for value in (trade.side_inferred, trade.taker_side):
        if value is None:
            continue
        normalized = str(value).strip().upper()
        if normalized in {"YES", "NO"}:
            return normalized
    return None


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
    if isinstance(value, datetime):
        return _utc(value).isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _timestamp_within_age(
    value: datetime | None,
    evaluated_at: datetime,
    max_age_ms: int,
) -> bool:
    if value is None:
        return False
    return (_utc(evaluated_at) - _utc(value)).total_seconds() * 1000 <= max_age_ms
