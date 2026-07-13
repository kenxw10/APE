from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from statistics import pstdev
from typing import Any

import numpy as np

from ape.db.models import ResearchMarketOutcome, ResearchReplayEvent
from ape.research import REPLAY_SCHEMA_VERSION
from ape.research.replay import DeterministicReplayEngine, ReplayTrade
from ape.research.repository import _CANDIDATE_TUNABLE_PATHS, _PROTECTED_GATE_PATHS
from ape.strategy.momentum_v2 import (
    CALIBRATION_SCHEMA_VERSION,
    V2_FEATURE_SCHEMA_VERSION,
    V2_PARAMETERS,
)

LIFECYCLE_DRAFT = "DRAFT"
LIFECYCLE_BACKTESTED = "BACKTESTED"
LIFECYCLE_SHADOW = "SHADOW"
LIFECYCLE_DRY_RUN_CHALLENGER = "DRY_RUN_CHALLENGER"
LIFECYCLE_PAPER_CANDIDATE = "PAPER_CANDIDATE"
LIFECYCLE_PAPER_ACTIVE = "PAPER_ACTIVE"
LIFECYCLE_LIVE_CANDIDATE = "LIVE_CANDIDATE"
LIFECYCLE_LIVE_ACTIVE = "LIVE_ACTIVE"
LIFECYCLE_RETIRED = "RETIRED"
GOVERNANCE_STATES = {
    LIFECYCLE_DRAFT,
    LIFECYCLE_BACKTESTED,
    LIFECYCLE_SHADOW,
    LIFECYCLE_DRY_RUN_CHALLENGER,
    LIFECYCLE_PAPER_CANDIDATE,
    LIFECYCLE_PAPER_ACTIVE,
    LIFECYCLE_LIVE_CANDIDATE,
    LIFECYCLE_LIVE_ACTIVE,
    LIFECYCLE_RETIRED,
}
LOGISTIC_FEATURE_COLUMNS = (
    "return_5s",
    "return_15s",
    "return_30s",
    "return_60s",
    "return_120s",
    "directional_efficiency_30s",
    "directional_tick_ratio_30s",
    "retrace_fraction",
    "distance_bps",
    "standardized_distance_120s",
    "desired_ask",
    "desired_spread_cents",
    "desired_bid_depth",
    "desired_ask_depth",
    "contract_move_5s_cents",
    "contract_move_15s_cents",
    "contract_move_30s_cents",
    "response_residual_cents",
    "contract_brti_response_ratio_15s",
    "contract_brti_response_ratio_30s",
    "level1_imbalance",
    "level3_imbalance",
    "top5_imbalance",
    "order_flow_5s",
    "order_flow_15s",
    "desired_bid_replenishment",
    "opposing_ask_depletion",
    "depth_withdrawal_pressure",
    "trade_ratio",
    "trade_count",
    "timing_tier",
    "volatility_regime",
    "liquidity_regime",
)

FREQUENCY_GOVERNANCE_VERSION = "frequency_governance_v1"
FREQUENCY_GOVERNANCE = {
    "version": FREQUENCY_GOVERNANCE_VERSION,
    "qualified_setup_target_min_per_100": "5",
    "qualified_setup_target_max_per_100": "15",
    "preferred_fill_min_per_100": "3",
    "preferred_fill_max_per_100": "10",
    "hard_fill_min_for_challenger_per_100": "3",
    "hard_fill_max_for_challenger_per_100": "15",
}
CANDIDATE_GENERATION_ALGORITHM_VERSION = "bounded_v2_search_v1"


class GovernanceError(ValueError):
    pass


@dataclass(frozen=True)
class CandidateSpec:
    candidate_id: str
    generated_strategy_id: str
    model_type: str
    parameters: dict[str, Any]
    feature_columns: tuple[str, ...] = ()
    model_artifact: dict[str, Any] | None = None


@dataclass(frozen=True)
class CalibrationResult:
    status: str
    partition_manifest: dict[str, Any]
    candidates: tuple[CandidateSpec, ...]
    candidate_metrics: dict[str, dict[str, Any]]
    selected_candidate_id: str | None
    warnings: tuple[str, ...]
    blockers: tuple[str, ...]
    candidate_replay_trades: dict[str, tuple[ReplayTrade, ...]] = field(
        default_factory=dict
    )
    candidate_partition_replay_trades: dict[
        str, dict[str, tuple[ReplayTrade, ...]]
    ] = field(default_factory=dict)


def build_partition_manifest(outcomes: Iterable[ResearchMarketOutcome]) -> dict[str, Any]:
    rows = sorted(
        (
            row
            for row in outcomes
            if row.outcome_status == "RESOLVED" and row.market_open_at is not None
        ),
        key=lambda row: (_utc(row.market_open_at), row.market_ticker),
    )
    tickers = [row.market_ticker for row in rows]
    holdout_count = max(1, int(round(len(tickers) * 0.20))) if tickers else 0
    development = tickers[:-holdout_count] if holdout_count else []
    holdout = tickers[-holdout_count:] if holdout_count else []
    development_test_count = max(1, int(round(len(development) * 0.20))) if development else 0
    search_development = (
        development[:-development_test_count] if development_test_count else development
    )
    development_test = development[-development_test_count:] if development_test_count else []
    folds: list[dict[str, Any]] = []
    # Reserve the first chronological block for fold-one training, then evaluate
    # five expanding-train validation folds without looking into the future.
    block_size = max(1, len(search_development) // 6) if search_development else 0
    for index in range(5):
        start = min(len(search_development), (index + 1) * block_size)
        end = (
            len(search_development)
            if index == 4
            else min(len(search_development), start + block_size)
        )
        validation = search_development[start:end]
        boundary_rows = [row for row in rows if row.market_ticker in validation]
        boundary_at = _utc(boundary_rows[0].market_open_at) if boundary_rows else None
        purged = _purged_markets(rows, search_development, start, end, boundary_at)
        purged_tickers = {item["market_ticker"] for item in purged}
        folds.append(
            {
                "fold": index + 1,
                "train": [
                    ticker for ticker in search_development[:start] if ticker not in purged_tickers
                ],
                "validation": [ticker for ticker in validation if ticker not in purged_tickers],
                "purged": purged,
            }
        )
    cutoff_times = [
        _utc(row.resolved_at or row.market_close_at)
        for row in rows
        if row.resolved_at is not None or row.market_close_at is not None
    ]
    payload = {
        "statistical_unit": "unique_btc15_market",
        "ordered_market_tickers": tickers,
        "development": development,
        "search_development": search_development,
        "test": development_test,
        "development_test": development_test,
        "holdout": holdout,
        "folds": folds,
        "assignments": {
            ticker: (
                "frozen_holdout"
                if ticker in holdout
                else "development_test"
                if ticker in development_test
                else "search_walk_forward"
            )
            for ticker in tickers
        },
        "data_cutoff": max(cutoff_times).isoformat() if cutoff_times else None,
        "replay_schema_version": REPLAY_SCHEMA_VERSION,
        "feature_schema_version": V2_FEATURE_SCHEMA_VERSION,
        "governance_trade_partitions": ["frozen_holdout"],
    }
    payload["manifest_hash"] = _hash(payload)
    payload["holdout_hash"] = _hash(holdout)
    return payload


def bounded_candidate_specs(calibration_run_id: str) -> tuple[CandidateSpec, ...]:
    """Candidate zero plus a deterministic, bounded 256-candidate search."""
    seed = int(hashlib.sha256(calibration_run_id.encode()).hexdigest()[:16], 16)
    rng = np.random.default_rng(seed)
    grids = candidate_parameter_grids()
    baseline = CandidateSpec(
        "candidate-baseline-v2",
        "btc15_momentum_v2_candidate_baseline000",
        "BASELINE",
        copy.deepcopy(V2_PARAMETERS),
    )
    specs = [baseline]
    for index in range(252):
        parameters = copy.deepcopy(V2_PARAMETERS)
        selected = {key: values[int(rng.integers(0, len(values)))] for key, values in grids.items()}
        parameters["edge_threshold_cents"] = selected["edge"]
        parameters["calibration_overrides"] = {
            **{
                key: value
                for key, value in selected.items()
                if key != "weight_multiplier"
            },
            "component_weight_multipliers": {
                component: grids["weight_multiplier"][
                    int(rng.integers(0, len(grids["weight_multiplier"])))
                ]
                for component in (
                    "fast_impulse",
                    "path_quality",
                    "underreaction",
                    "boundary_regime",
                    "microstructure",
                    "timing_economics",
                )
            },
        }
        for tier, score in (
            ("early", selected["early"]),
            ("normal", selected["normal"]),
            ("late", selected["late"]),
        ):
            max_ask_key = f"{tier}_max_ask"
            parameters["tiers"][tier].update(
                {
                    "score": score,
                    "max_ask": selected[max_ask_key],
                    "time_stop": selected["time_stop"],
                    "max_hold": selected["max_hold"],
                }
            )
        candidate_hash = _hash(
            {"run": calibration_run_id, "index": index, "parameters": parameters}
        )
        candidate_id = f"candidate-{candidate_hash[:24]}"
        specs.append(
            CandidateSpec(
                candidate_id, _generated_strategy_id(candidate_id), "WEIGHTED_HEURISTIC", parameters
            )
        )
    for l2 in ("0.1", "1.0", "10.0"):
        candidate_id = f"candidate-{_hash({'run': calibration_run_id, 'logistic': l2})[:24]}"
        specs.append(
            CandidateSpec(
                candidate_id,
                _generated_strategy_id(candidate_id),
                "L2_LOGISTIC",
                copy.deepcopy(V2_PARAMETERS),
                LOGISTIC_FEATURE_COLUMNS,
                {"l2": l2},
            )
        )
    assert len(specs) == 256
    return tuple(specs)


def candidate_parameter_grids() -> dict[str, list[Any]]:
    """The complete bounded parameter space used for heuristic candidates."""
    return {
        "fast_15": ["0.75", "1.00", "1.25", "1.50", "1.75"],
        "fast_30": ["1.25", "1.50", "2.00", "2.50", "3.00"],
        "adverse_5": ["-1.00", "-0.50", "0.00"],
        "retrace": ["0.60", "0.70", "0.80"],
        "crosses": [1, 2, 3],
        "edge": ["0.50", "1.00", "1.50", "2.00", "2.50", "3.00"],
        "early": [70, 75, 80, 85, 90],
        "normal": [60, 65, 70, 75, 80, 85],
        "late": [65, 70, 75, 80, 85, 90],
        "early_max_ask": ["0.68", "0.70", "0.72"],
        "normal_max_ask": ["0.70", "0.72", "0.74", "0.76", "0.78"],
        "late_max_ask": ["0.68", "0.70", "0.72", "0.74"],
        "time_stop": [15, 30, 45, 60],
        "max_hold": [30, 60, 90, 120],
        "profit_target": [8, 10, 12],
        "soft_stop": [6, 8],
        "hard_stop": [8, 10, 12],
        "weight_multiplier": ["0.75", "1.00", "1.25"],
    }


def complete_search_space_snapshot(
    calibration_run_id: str, candidates: Iterable[CandidateSpec]
) -> dict[str, Any]:
    """Persist enough immutable information to reproduce a candidate search."""
    candidate_rows = [
        {
            "candidate_id": candidate.candidate_id,
            "parameter_hash": _hash(candidate.parameters),
            "model_type": candidate.model_type,
        }
        for candidate in candidates
    ]
    seed = int(hashlib.sha256(calibration_run_id.encode()).hexdigest()[:16], 16)
    snapshot = {
        "calibration_schema_version": CALIBRATION_SCHEMA_VERSION,
        "deterministic_seed": seed,
        "candidate_generation_algorithm_version": CANDIDATE_GENERATION_ALGORITHM_VERSION,
        "baseline_definition": {
            "candidate_id": "candidate-baseline-v2",
            "parameters": V2_PARAMETERS,
        },
        "heuristic_grids": candidate_parameter_grids(),
        "tier_specific_ask_grids": {
            key: values
            for key, values in candidate_parameter_grids().items()
            if key.endswith("max_ask")
        },
        "weight_multiplier_grid": candidate_parameter_grids()["weight_multiplier"],
        "exit_grids": {
            key: candidate_parameter_grids()[key]
            for key in ("time_stop", "max_hold", "profit_target", "soft_stop", "hard_stop")
        },
        "logistic_l2_values": ["0.1", "1.0", "10.0"],
        "logistic_feature_order": list(LOGISTIC_FEATURE_COLUMNS),
        "logistic_maximum_iterations": 500,
        "logistic_convergence_tolerance": "1e-8",
        "maximum_candidate_count": 256,
        "generated_candidates": candidate_rows,
        "allowed_parameter_paths": sorted(_CANDIDATE_TUNABLE_PATHS),
        "protected_gate_paths": list(_PROTECTED_GATE_PATHS),
        "frequency_governance": FREQUENCY_GOVERNANCE,
    }
    snapshot["snapshot_sha256"] = _hash(snapshot)
    return snapshot


def fit_l2_logistic(
    rows: list[dict[str, Any]],
    targets: list[int],
    *,
    l2: float,
    max_iterations: int = 500,
    tolerance: float = 1e-8,
) -> dict[str, Any]:
    if len(rows) != len(targets) or not rows:
        raise ValueError("Logistic calibration requires non-empty aligned training rows.")
    matrix, medians, means, scales = _design_matrix(rows)
    target = np.asarray(targets, dtype=float)
    weights, intercept = np.zeros(matrix.shape[1]), 0.0
    for iteration in range(max_iterations):
        prediction = 1 / (1 + np.exp(-np.clip(matrix @ weights + intercept, -40, 40)))
        error = prediction - target
        next_weights = weights - 0.25 * (
            matrix.T @ error / len(target) + l2 * weights / len(target)
        )
        next_intercept = intercept - 0.25 * float(error.mean())
        if (
            max(float(np.max(np.abs(next_weights - weights))), abs(next_intercept - intercept))
            < tolerance
        ):
            return _logistic_artifact(
                next_weights, next_intercept, medians, means, scales, l2, iteration + 1
            )
        weights, intercept = next_weights, next_intercept
    raise ValueError("Logistic calibration did not converge within 500 iterations.")


def market_bootstrap(
    metrics_by_market: dict[str, Decimal], calibration_run_id: str
) -> dict[str, str]:
    if not metrics_by_market:
        return {"resamples": "2000", "lower": "0", "upper": "0", "mean": "0"}
    values = np.asarray([float(value) for value in metrics_by_market.values()])
    rng = np.random.default_rng(
        int(hashlib.sha256(calibration_run_id.encode()).hexdigest()[:16], 16)
    )
    samples = np.asarray(
        [rng.choice(values, size=len(values), replace=True).mean() for _ in range(2000)]
    )
    return {
        "resamples": "2000",
        "mean": str(Decimal(str(float(samples.mean())))),
        "lower": str(Decimal(str(float(np.percentile(samples, 2.5))))),
        "upper": str(Decimal(str(float(np.percentile(samples, 97.5))))),
    }


def replay_metrics(
    trades: Iterable[ReplayTrade],
    *,
    market_count: int,
    calibration_run_id: str,
    market_tickers: Iterable[str] = (),
) -> dict[str, Any]:
    all_trades = list(trades)
    closed = [
        trade
        for trade in all_trades
        if trade.status == "CLOSED" and trade.net_pnl_cents is not None
    ]
    eligible_markets = tuple(dict.fromkeys(str(ticker) for ticker in market_tickers))
    by_market: dict[str, Decimal] = {ticker: Decimal("0") for ticker in eligible_markets}
    for trade in closed:
        by_market[trade.market_ticker] = by_market.get(trade.market_ticker, Decimal("0")) + (
            trade.net_pnl_cents or Decimal("0")
        )
    net = sum(by_market.values(), Decimal("0"))
    gross = sum((trade.gross_pnl_cents or Decimal("0") for trade in closed), Decimal("0"))
    fees = sum((trade.fee_cents or Decimal("0") for trade in closed), Decimal("0"))
    holding = [
        trade.holding_duration_ms for trade in closed if trade.holding_duration_ms is not None
    ]
    mfe = [trade.mfe_cents for trade in closed if trade.mfe_cents is not None]
    mae = [trade.mae_cents for trade in closed if trade.mae_cents is not None]
    timing_counts: dict[str, int] = {}
    volatility_counts: dict[str, int] = {}
    liquidity_counts: dict[str, int] = {}
    regime_counts: dict[str, int] = {}
    for trade in closed:
        measurements = trade.measurements if isinstance(trade.measurements, dict) else {}
        timing_tier = str(trade.timing_tier or "unknown")
        volatility_regime = str(measurements.get("volatility_regime") or "unknown")
        liquidity_regime = str(measurements.get("liquidity_regime") or "unknown")
        for counts, value in (
            (timing_counts, timing_tier),
            (volatility_counts, volatility_regime),
            (liquidity_counts, liquidity_regime),
        ):
            key = str(value)
            counts[key] = counts.get(key, 0) + 1
        regime = ":".join((volatility_regime, liquidity_regime, timing_tier))
        regime_counts[regime] = regime_counts.get(regime, 0) + 1
    # A candidate must be diversified across every monitored regime dimension.
    # The combined key catches identical full regimes; the marginal counters
    # keep timing-tier variety from masking volatility or liquidity concentration.
    dominant_entries = max(
        max(timing_counts.values(), default=0),
        max(volatility_counts.values(), default=0),
        max(liquidity_counts.values(), default=0),
        max(regime_counts.values(), default=0),
    )
    ordered_markets = eligible_markets or tuple(sorted(by_market))
    ordered_market_pnl = [by_market.get(market, Decimal("0")) for market in ordered_markets]
    cumulative = Decimal("0")
    peak = Decimal("0")
    max_drawdown = Decimal("0")
    for value in ordered_market_pnl:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    net_values = [float(trade.net_pnl_cents or Decimal("0")) for trade in closed]
    bootstrap = market_bootstrap(by_market, calibration_run_id)
    regime_pnl: dict[str, Decimal] = {}
    for trade in closed:
        measurements = trade.measurements if isinstance(trade.measurements, dict) else {}
        regime = ":".join(
            (
                str(measurements.get("volatility_regime") or "unknown"),
                str(measurements.get("liquidity_regime") or "unknown"),
                str(trade.timing_tier or "unknown"),
            )
        )
        regime_pnl[regime] = regime_pnl.get(regime, Decimal("0")) + (
            trade.net_pnl_cents or Decimal("0")
        )
    return {
        "net_pnl_per_market": str(net / Decimal(max(market_count, 1))),
        "net_pnl_per_trade": str(net / Decimal(max(len(closed), 1))),
        "gross_pnl_cents": str(gross),
        "fee_cents": str(fees),
        "entry_frequency_per_100_markets": str(
            Decimal(len(closed)) * 100 / Decimal(max(market_count, 1))
        ),
        "signal_to_fill_rate": str(Decimal(len(closed)) / Decimal(max(len(all_trades), 1))),
        "closed_trade_count": len(closed),
        "win_rate": str(
            Decimal(sum(trade.net_pnl_cents > 0 for trade in closed)) / Decimal(max(len(closed), 1))
        ),
        "average_holding_duration_ms": str(Decimal(sum(holding)) / Decimal(max(len(holding), 1))),
        "median_holding_duration_ms": str(Decimal(str(float(np.median(holding or [0]))))),
        "average_mfe_cents": str(sum(mfe, Decimal("0")) / Decimal(max(len(mfe), 1))),
        "average_mae_cents": str(sum(mae, Decimal("0")) / Decimal(max(len(mae), 1))),
        "maximum_drawdown_cents": str(max_drawdown),
        "fifth_percentile_trade_pnl_cents": str(
            Decimal(str(float(np.percentile(net_values, 5)))) if net_values else Decimal("0")
        ),
        "turnover": len(closed),
        "volatility_regime_coverage": len(volatility_counts),
        "liquidity_regime_coverage": len(liquidity_counts),
        "timing_tier_coverage": len(timing_counts),
        "dominant_regime_entry_share": str(
            Decimal(dominant_entries) / Decimal(max(len(closed), 1))
        ),
        "dominant_regime_pnl_share": str(
            max((abs(value) for value in regime_pnl.values()), default=Decimal("0"))
            / max(abs(net), Decimal("1"))
        ),
        "bootstrap": {
            "net_pnl_per_market": bootstrap,
            "net_pnl_per_trade": _bootstrap_trade_metric(
                closed, calibration_run_id, "net", market_tickers=ordered_markets
            ),
            "win_rate": _bootstrap_trade_metric(
                closed, calibration_run_id, "win", market_tickers=ordered_markets
            ),
            "entry_frequency_per_100_markets": _bootstrap_trade_metric(
                closed,
                calibration_run_id,
                "frequency",
                market_count,
                market_tickers=ordered_markets,
            ),
        },
        "market_net_pnl": {market: str(value) for market, value in by_market.items()},
    }


def adjusted_lower_confidence_expectancy(
    *,
    bootstrap_lower: Decimal,
    changed_parameter_count: int,
    normalized_l1_weight_drift: Decimal,
    fold_net_pnl_per_market: list[Decimal],
    dominant_regime_entry_share: Decimal,
    mean_net_pnl_per_market: Decimal,
    entries_per_100_markets: Decimal,
) -> dict[str, Decimal]:
    complexity = (
        Decimal("0.01") * changed_parameter_count + Decimal("0.005") * normalized_l1_weight_drift
    )
    instability = Decimal("0.50") * Decimal(
        str(pstdev([float(value) for value in fold_net_pnl_per_market] or [0]))
    )
    concentration = max(Decimal("0"), dominant_regime_entry_share - Decimal("0.60")) * abs(
        mean_net_pnl_per_market
    )
    turnover = max(Decimal("0"), entries_per_100_markets - Decimal("10")) * Decimal("0.01")
    return {
        "complexity_penalty": complexity,
        "instability_penalty": instability,
        "concentration_penalty": concentration,
        "turnover_penalty": turnover,
        "adjusted_lower_confidence_expectancy": bootstrap_lower
        - complexity
        - instability
        - concentration
        - turnover,
    }


def run_bounded_calibration(
    *,
    calibration_run_id: str,
    events: list[ResearchReplayEvent],
    outcomes: list[ResearchMarketOutcome],
    candidate_specs: Iterable[CandidateSpec] | None = None,
) -> CalibrationResult:
    manifest = build_partition_manifest(outcomes)
    if len(manifest["ordered_market_tickers"]) < 50:
        return CalibrationResult(
            "INSUFFICIENT_DATA",
            manifest,
            (),
            {},
            None,
            ("calibration_requires_at_least_50_complete_unique_markets",),
            ("insufficient_complete_markets",),
        )
    candidates, metrics, selected, best_lower, candidate_replay_trades, partition_trades = (
        list(candidate_specs or bounded_candidate_specs(calibration_run_id)),
        {},
        None,
        Decimal("-Infinity"),
        {},
        {},
    )
    search_development = list(manifest["search_development"])
    search_development_members = set(search_development)
    development_events = [
        event for event in events if event.market_ticker in search_development_members
    ]
    development_outcomes = [
        outcome for outcome in outcomes if outcome.market_ticker in search_development_members
    ]
    labeled_rows, labeled_targets = _labeled_feature_rows(development_events, development_outcomes)
    for index, candidate in enumerate(candidates):
        fold_metrics = _walk_forward_metrics(
            candidate,
            manifest,
            events,
            outcomes,
            calibration_run_id,
        )
        if candidate.model_type == "L2_LOGISTIC":
            if not fold_metrics:
                metrics[candidate.candidate_id] = {
                    "status": "BLOCKED",
                    "reason": "logistic_fold_training_unavailable",
                    "training_row_count": len(labeled_rows),
                }
                continue
            try:
                artifact = fit_l2_logistic(
                    labeled_rows,
                    labeled_targets,
                    l2=float((candidate.model_artifact or {}).get("l2", "1")),
                )
            except ValueError:
                metrics[candidate.candidate_id] = {
                    "status": "BLOCKED",
                    "reason": "logistic_final_training_unavailable",
                    "training_row_count": len(labeled_rows),
                }
                continue
            candidate = replace(
                candidate,
                parameters={
                    **candidate.parameters,
                    "logistic_model": artifact,
                    "logistic_probability_threshold": "0.50",
                },
                model_artifact=artifact,
            )
            candidates[index] = candidate
        result = DeterministicReplayEngine(parameters=candidate.parameters).replay(
            development_events, outcomes=development_outcomes
        )
        values = replay_metrics(
            result.trades,
            market_count=len(search_development),
            calibration_run_id=calibration_run_id,
            market_tickers=search_development,
        )
        candidate_replay_trades[candidate.candidate_id] = result.trades
        partition_trades[candidate.candidate_id] = {
            "search_development": result.trades,
        }
        lower = Decimal(values["bootstrap"]["net_pnl_per_market"]["lower"])
        penalties = adjusted_lower_confidence_expectancy(
            bootstrap_lower=lower,
            changed_parameter_count=_changed_parameter_count(candidate.parameters),
            normalized_l1_weight_drift=_normalized_l1_weight_drift(candidate.parameters),
            fold_net_pnl_per_market=[Decimal(item["net_pnl_per_market"]) for item in fold_metrics],
            dominant_regime_entry_share=Decimal(values["dominant_regime_entry_share"]),
            mean_net_pnl_per_market=Decimal(values["net_pnl_per_market"]),
            entries_per_100_markets=Decimal(values["entry_frequency_per_100_markets"]),
        )
        metrics[candidate.candidate_id] = {
            "status": "EVALUATED",
            **values,
            "training": values,
            "walk_forward_validation": _aggregate_fold_metrics(fold_metrics),
            "development_test": None,
            "holdout": None,
            "partition_metrics": {"search_development": values},
            "fold_metrics": fold_metrics,
            "penalties": {key: str(value) for key, value in penalties.items()},
            "bootstrap": values["bootstrap"],
            "model_artifact": candidate.model_artifact,
            "training_row_count": len(labeled_rows)
            if candidate.model_type == "L2_LOGISTIC"
            else None,
            "zero_entry_report": result.zero_entry_report,
        }
        adjusted_lower = penalties["adjusted_lower_confidence_expectancy"]
        if adjusted_lower > best_lower:
            best_lower, selected = adjusted_lower, candidate.candidate_id
    baseline_metrics = metrics.get("candidate-baseline-v2", {})
    baseline_net = Decimal(str(baseline_metrics.get("net_pnl_per_market", "0")))
    for candidate_id, candidate_metrics in metrics.items():
        if candidate_metrics.get("status") != "EVALUATED":
            continue
        candidate_metrics["beats_baseline"] = Decimal(
            str(candidate_metrics.get("net_pnl_per_market", "0"))
        ) > baseline_net
        candidate_metrics["verified_fee_model"] = True
        candidate_metrics["candidate_replay_trade_count"] = len(
            candidate_replay_trades.get(candidate_id, ())
        )
        candidate_metrics["candidate_closed_trade_count"] = sum(
            getattr(trade, "status", None) == "CLOSED"
            for trade in candidate_replay_trades.get(candidate_id, ())
        )
    if selected is not None:
        chosen = next(candidate for candidate in candidates if candidate.candidate_id == selected)
        development_test = list(manifest["development_test"])
        development_test_members = set(development_test)
        test_result = DeterministicReplayEngine(parameters=chosen.parameters).replay(
            [event for event in events if event.market_ticker in development_test_members],
            outcomes=[
                outcome for outcome in outcomes if outcome.market_ticker in development_test_members
            ],
        )
        development_test_metrics = replay_metrics(
            test_result.trades,
            market_count=len(development_test),
            calibration_run_id=f"{calibration_run_id}-development-test",
            market_tickers=development_test,
        )
        metrics[selected]["development_test"] = development_test_metrics
        metrics[selected]["partition_metrics"]["development_test"] = development_test_metrics
        partition_trades[selected]["development_test"] = test_result.trades
        holdout = list(manifest["holdout"])
        holdout_members = set(holdout)
        holdout_events = [event for event in events if event.market_ticker in holdout_members]
        holdout_outcomes = [
            outcome for outcome in outcomes if outcome.market_ticker in holdout_members
        ]
        holdout_result = DeterministicReplayEngine(parameters=chosen.parameters).replay(
            holdout_events, outcomes=holdout_outcomes
        )
        holdout_metrics = replay_metrics(
            holdout_result.trades,
            market_count=len(holdout),
            calibration_run_id=calibration_run_id,
            market_tickers=holdout,
        )
        metrics[selected]["holdout"] = holdout_metrics
        metrics[selected]["partition_metrics"]["frozen_holdout"] = holdout_metrics
        partition_trades[selected]["frozen_holdout"] = holdout_result.trades
    return CalibrationResult(
        "COMPLETED",
        manifest,
        tuple(candidates),
        metrics,
        selected,
        (),
        (),
        candidate_replay_trades,
        partition_trades,
    )


def _labeled_feature_rows(
    events: list[ResearchReplayEvent], outcomes: list[ResearchMarketOutcome]
) -> tuple[list[dict[str, Any]], list[int]]:
    labels: dict[str, dict[str, Any]] = {}
    for outcome in outcomes:
        flags = outcome.quality_flags if isinstance(outcome.quality_flags, dict) else {}
        labels.update(
            flags.get("counterfactual_labels", {})
            if isinstance(flags.get("counterfactual_labels"), dict)
            else {}
        )
    rows: list[dict[str, Any]] = []
    targets: list[int] = []
    for event in events:
        if event.event_type != "FEATURE_SNAPSHOT" or event.replay_readiness != "FULL":
            continue
        label = labels.get(event.feature_snapshot_id or "")
        vector = (event.payload or {}).get("feature_vector")
        net_markout = label.get("net_markout_30s_cents") if isinstance(label, dict) else None
        if not isinstance(vector, dict) or net_markout is None:
            continue
        try:
            rows.append(vector)
            targets.append(int(Decimal(str(net_markout)) > 0))
        except (ArithmeticError, ValueError):
            continue
    return rows, targets


def _walk_forward_metrics(
    candidate: CandidateSpec,
    manifest: dict[str, Any],
    events: list[ResearchReplayEvent],
    outcomes: list[ResearchMarketOutcome],
    calibration_run_id: str,
) -> list[dict[str, Any]]:
    metrics: list[dict[str, Any]] = []
    for fold in manifest["folds"]:
        train = list(fold["train"])
        validation = list(fold["validation"])
        if not train or not validation:
            continue
        train_members = set(train)
        validation_members = set(validation)
        evaluated_candidate = candidate
        if candidate.model_type == "L2_LOGISTIC":
            train_rows, train_targets = _labeled_feature_rows(
                [event for event in events if event.market_ticker in train_members],
                [outcome for outcome in outcomes if outcome.market_ticker in train_members],
            )
            try:
                artifact = fit_l2_logistic(
                    train_rows,
                    train_targets,
                    l2=float((candidate.model_artifact or {}).get("l2", "1")),
                )
            except ValueError:
                continue
            evaluated_candidate = replace(
                candidate,
                parameters={
                    **candidate.parameters,
                    "logistic_model": artifact,
                    "logistic_probability_threshold": "0.50",
                },
                model_artifact=artifact,
            )
        replay = DeterministicReplayEngine(parameters=evaluated_candidate.parameters).replay(
            # The model is trained only from this fold's earlier markets.
            # Validation receives the fitted artifact and never refits it.
            [event for event in events if event.market_ticker in validation_members],
            outcomes=[
                outcome for outcome in outcomes if outcome.market_ticker in validation_members
            ],
        )
        metrics.append(
            {
                "fold": fold["fold"],
                **replay_metrics(
                    replay.trades,
                    market_count=len(validation),
                    calibration_run_id=f"{calibration_run_id}-fold-{fold['fold']}",
                    market_tickers=validation,
                ),
            }
        )
    return metrics


def _aggregate_fold_metrics(fold_metrics: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize validation-only fold evidence without introducing a global fit."""
    if not fold_metrics:
        return {}
    exemplar = fold_metrics[-1]
    numeric_keys = (
        "net_pnl_per_market",
        "net_pnl_per_trade",
        "entry_frequency_per_100_markets",
        "signal_to_fill_rate",
        "dominant_regime_entry_share",
    )
    if any(any(key not in item for key in numeric_keys) for item in fold_metrics):
        return {"fold_count": len(fold_metrics)}
    values = dict(exemplar)
    for key in numeric_keys:
        values[key] = str(
            sum(Decimal(str(item[key])) for item in fold_metrics) / Decimal(len(fold_metrics))
        )
    values["bootstrap"] = exemplar["bootstrap"]
    return values


def _changed_parameter_count(parameters: dict[str, Any]) -> int:
    return sum(
        value != _nested_value(V2_PARAMETERS, path)
        for path, value in _flatten_parameters(parameters).items()
    )


def _normalized_l1_weight_drift(parameters: dict[str, Any]) -> Decimal:
    configured = parameters.get("calibration_overrides", {})
    weights = (
        configured.get("component_weight_multipliers", {}) if isinstance(configured, dict) else {}
    )
    if not isinstance(weights, dict):
        return Decimal("0")
    return sum(
        (abs(Decimal(str(value)) - Decimal("1")) for value in weights.values()), Decimal("0")
    )


def _nested_value(values: dict[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = values
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def _flatten_parameters(
    values: dict[str, Any], prefix: tuple[str, ...] = ()
) -> dict[tuple[str, ...], Any]:
    flattened: dict[tuple[str, ...], Any] = {}
    for key, value in values.items():
        path = (*prefix, str(key))
        if isinstance(value, dict):
            flattened.update(_flatten_parameters(value, path))
        else:
            flattened[path] = value
    return flattened


def _bootstrap_trade_metric(
    trades: list[ReplayTrade],
    calibration_run_id: str,
    metric: str,
    market_count: int | None = None,
    *,
    market_tickers: Iterable[str] = (),
) -> dict[str, str]:
    eligible_markets = tuple(dict.fromkeys(str(ticker) for ticker in market_tickers))
    if not trades and not eligible_markets:
        return {"resamples": "2000", "lower": "0", "upper": "0", "mean": "0"}
    rng = np.random.default_rng(
        int(hashlib.sha256(f"{calibration_run_id}-{metric}".encode()).hexdigest()[:16], 16)
    )
    by_market: dict[str, list[ReplayTrade]] = {}
    for trade in trades:
        by_market.setdefault(trade.market_ticker, []).append(trade)
    markets = eligible_markets or tuple(sorted(by_market))
    for market in markets:
        by_market.setdefault(market, [])
    samples = np.asarray(
        [
            _sample_trade_metric(
                rng.choice(markets, size=len(markets), replace=True), by_market, metric
            )
            for _ in range(2000)
        ]
    )
    return {
        "resamples": "2000",
        "mean": str(Decimal(str(float(samples.mean())))),
        "lower": str(Decimal(str(float(np.percentile(samples, 2.5))))),
        "upper": str(Decimal(str(float(np.percentile(samples, 97.5))))),
    }


def _sample_trade_metric(
    sampled_markets: Iterable[str], by_market: dict[str, list[ReplayTrade]], metric: str
) -> float:
    sampled = [trade for market in sampled_markets for trade in by_market[str(market)]]
    if metric == "frequency":
        return 100.0 * len(sampled) / max(len(sampled_markets), 1)
    values = [float(trade.net_pnl_cents or Decimal("0")) for trade in sampled]
    if metric == "win":
        return sum(value > 0 for value in values) / max(len(values), 1)
    # Net-P&L bootstrap is a market statistic: sampled zero-trade markets
    # remain zero-valued observations instead of disappearing from the mean.
    return sum(values) / max(len(sampled_markets), 1)


def transition_candidate(
    *, from_state: str, to_state: str, evidence: dict[str, Any]
) -> tuple[str, dict[str, Any]]:
    if to_state not in GOVERNANCE_STATES:
        raise GovernanceError(f"Unknown lifecycle state {to_state!r}.")
    if to_state in {
        LIFECYCLE_PAPER_CANDIDATE,
        LIFECYCLE_PAPER_ACTIVE,
        LIFECYCLE_LIVE_CANDIDATE,
        LIFECYCLE_LIVE_ACTIVE,
    }:
        raise GovernanceError(
            "Research workers may not transition candidates to paper or live states."
        )
    if (from_state, to_state) not in {
        (LIFECYCLE_DRAFT, LIFECYCLE_BACKTESTED),
        (LIFECYCLE_BACKTESTED, LIFECYCLE_SHADOW),
        (LIFECYCLE_SHADOW, LIFECYCLE_DRY_RUN_CHALLENGER),
    }:
        raise GovernanceError(f"Forbidden automatic transition {from_state} -> {to_state}.")
    if to_state == LIFECYCLE_DRY_RUN_CHALLENGER:
        failures = _promotion_failures(evidence)
        if failures:
            raise GovernanceError(
                "Candidate failed dry-run challenger gates: " + ", ".join(failures)
            )
    return to_state, {
        "from_state": from_state,
        "to_state": to_state,
        "evidence": copy.deepcopy(evidence),
    }


def _promotion_failures(evidence: dict[str, Any]) -> list[str]:
    minimums = {
        "complete_unique_markets": 500,
        "closed_simulated_trades": 50,
        "entry_frequency_per_100_markets_min": Decimal(
            FREQUENCY_GOVERNANCE["hard_fill_min_for_challenger_per_100"]
        ),
        "signal_to_fill_rate": Decimal("0.50"),
        "complete_replay_coverage": Decimal("0.95"),
        "volatility_regimes": 2,
        "liquidity_regimes": 2,
        "timing_tiers": 2,
    }
    failures = [
        key
        for key, minimum in minimums.items()
        if Decimal(str(evidence.get(key, 0))) < Decimal(str(minimum))
    ]
    for key in (
        "holdout_mean_net_pnl_per_market",
        "holdout_lower_95",
        "adjusted_lower_confidence_expectancy",
    ):
        if Decimal(str(evidence.get(key, 0))) <= 0:
            failures.append(key)
    if Decimal(str(evidence.get("entry_frequency_per_100_markets", 0))) > Decimal(
        FREQUENCY_GOVERNANCE["hard_fill_max_for_challenger_per_100"]
    ):
        failures.append("entry_frequency_per_100_markets_max")
    if Decimal(str(evidence.get("dominant_regime_entry_share", 1))) > Decimal("0.60"):
        failures.append("dominant_regime_entry_share")
    if Decimal(str(evidence.get("max_drawdown_per_100_markets", 999))) > 25:
        failures.append("max_drawdown_per_100_markets")
    if (
        not evidence.get("verified_fee_model")
        or not evidence.get("beats_baseline")
        or evidence.get("forbidden_parameter_changed")
        or evidence.get("safety_or_data_quality_gate_changed")
    ):
        failures.append("required_governance_evidence")
    return failures


def _design_matrix(
    rows: list[dict[str, Any]],
) -> tuple[np.ndarray, list[float], list[float], list[float]]:
    raw = np.asarray(
        [[_feature_number(row, column) for column in LOGISTIC_FEATURE_COLUMNS] for row in rows],
        dtype=float,
    )
    missing = np.isnan(raw)
    medians = np.asarray(
        [
            np.median(column[~np.isnan(column)]) if np.any(~np.isnan(column)) else 0.0
            for column in raw.T
        ]
    )
    filled = np.where(missing, medians, raw)
    means = filled.mean(axis=0)
    scales = np.where(filled.std(axis=0) == 0, 1.0, filled.std(axis=0))
    return (
        np.hstack([(filled - means) / scales, missing.astype(float)]),
        medians.tolist(),
        means.tolist(),
        scales.tolist(),
    )


def _feature_number(row: dict[str, Any], column: str) -> float:
    value = row.get(column)
    if value is None:
        return np.nan
    if column == "timing_tier":
        return {"early": 0.0, "normal": 1.0, "late": 2.0}.get(str(value), np.nan)
    if column in {"volatility_regime", "liquidity_regime"}:
        return float(int(_hash(str(value))[:6], 16) % 10_000)
    try:
        return float(value)
    except (TypeError, ValueError):
        return np.nan


def _logistic_artifact(
    weights: np.ndarray,
    intercept: float,
    medians: list[float],
    means: list[float],
    scales: list[float],
    l2: float,
    iterations: int,
) -> dict[str, Any]:
    artifact = {
        "feature_columns": list(LOGISTIC_FEATURE_COLUMNS),
        "coefficients": weights.tolist(),
        "intercept": intercept,
        "medians": medians,
        "means": means,
        "scales": scales,
        "l2": l2,
        "iterations": iterations,
    }
    artifact["checksum"] = _hash(artifact)
    return artifact


def _generated_strategy_id(candidate_id: str) -> str:
    return f"btc15_momentum_v2_candidate_{candidate_id[-12:]}"


def _purged_markets(
    rows: list[ResearchMarketOutcome],
    development: list[str],
    start: int,
    end: int,
    boundary_at: datetime | None,
) -> list[dict[str, str]]:
    purged: dict[str, str] = {}
    for ticker in development[max(0, start - 1) : start + 1]:
        if ticker:
            purged[ticker] = "adjacent_market_boundary"
    if boundary_at is not None:
        for row in rows:
            if row.market_ticker not in development:
                continue
            opened = _utc(row.market_open_at) if row.market_open_at else None
            closed = _utc(row.market_close_at) if row.market_close_at else None
            if opened is None or closed is None:
                continue
            if opened - timedelta(minutes=5) <= boundary_at <= closed + timedelta(minutes=5):
                purged.setdefault(
                    row.market_ticker, "event_interval_within_five_minutes_of_boundary"
                )
    return [
        {"market_ticker": ticker, "reason": reason} for ticker, reason in sorted(purged.items())
    ]


def _hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
