from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from ape.db.models import ResearchMarketOutcome, ResearchReplayEvent
from ape.research.fees import FeeModel, verified_kalshi_taker_fee_model
from ape.strategy.momentum_v2 import (
    REPLAY_SCHEMA_VERSION,
    V2_LIFECYCLE_SCHEMA_VERSION,
    V2_PARAMETERS,
    V2FeatureEvaluation,
    evaluate_momentum_v2_feature_vector,
)


@dataclass(frozen=True)
class ReplayTrade:
    trade_id: str
    market_ticker: str
    side: str
    entry_decision_at: datetime
    entry_fill_at: datetime | None
    entry_limit: Decimal
    entry_fill_price: Decimal | None
    entry_fill_event_id: str | None
    exit_trigger_at: datetime | None
    exit_fill_at: datetime | None
    exit_fill_price: Decimal | None
    exit_fill_event_id: str | None
    status: str
    gross_pnl_cents: Decimal | None
    fee_cents: Decimal | None
    net_pnl_cents: Decimal | None
    holding_duration_ms: int | None
    mfe_cents: Decimal | None
    mae_cents: Decimal | None
    time_to_mfe_ms: int | None
    time_to_mae_ms: int | None
    entry_reason: str
    exit_reason: str | None
    timing_tier: str | None
    measurements: dict[str, Any]


@dataclass(frozen=True)
class ReplayResult:
    decisions: tuple[V2FeatureEvaluation, ...]
    trades: tuple[ReplayTrade, ...]
    zero_entry_report: dict[str, Any]
    blocker_funnel: dict[str, int]
    event_count: int
    dataset_hash: str
    cost_model: dict[str, str]


@dataclass
class _PendingEntry:
    evaluation: V2FeatureEvaluation
    event_id: str
    decision_at: datetime
    effective_after: datetime
    expires_at: datetime


@dataclass
class _OpenPosition:
    pending: _PendingEntry
    entry_at: datetime
    entry_price: Decimal
    entry_event_id: str
    best_bid: Decimal
    worst_bid: Decimal
    best_at: datetime
    worst_at: datetime
    exit_checked: bool = False


class DeterministicReplayEngine:
    """Strictly ordered replay of the same pure V2 evaluator used in production."""

    def __init__(
        self,
        *,
        parameters: dict[str, Any] | None = None,
        fee_model: FeeModel | None = None,
    ) -> None:
        self.parameters = parameters or V2_PARAMETERS
        self.fee_model = fee_model or verified_kalshi_taker_fee_model()

    def replay(
        self,
        events: Iterable[ResearchReplayEvent],
        *,
        outcomes: Iterable[ResearchMarketOutcome] = (),
    ) -> ReplayResult:
        ordered = tuple(sorted(events, key=_event_key))
        outcome_by_market = {outcome.market_ticker: outcome for outcome in outcomes}
        decisions: list[V2FeatureEvaluation] = []
        trades: list[ReplayTrade] = []
        samples: list[tuple[str, V2FeatureEvaluation]] = []
        pending: dict[str, _PendingEntry] = {}
        open_positions: dict[str, _OpenPosition] = {}
        funnel = _new_funnel()
        blockers: dict[str, int] = {}

        for event in ordered:
            at = _utc(event.event_time)
            market = event.market_ticker
            if event.event_type == "FEATURE_SNAPSHOT" and market is not None:
                if event.replay_readiness != "FULL":
                    funnel["all_samples"] += 1
                    blockers["partial_feature_vector"] = (
                        blockers.get("partial_feature_vector", 0) + 1
                    )
                    continue
                vector = _hydrate_vector((event.payload or {}).get("feature_vector"))
                if not vector:
                    funnel["all_samples"] += 1
                    blockers["feature_vector_missing"] = (
                        blockers.get("feature_vector_missing", 0) + 1
                    )
                    continue
                evaluation = evaluate_momentum_v2_feature_vector(vector, self.parameters)
                decisions.append(evaluation)
                samples.append((market, evaluation))
                _add_funnel(funnel, evaluation)
                if evaluation.blockers:
                    for blocker in evaluation.blockers:
                        blockers[blocker] = blockers.get(blocker, 0) + 1
                if (
                    evaluation.state == "DRY_RUN_ENTRY_SIGNAL"
                    and market not in pending
                    and market not in open_positions
                ):
                    pending[market] = _PendingEntry(
                        evaluation=evaluation,
                        event_id=event.event_id,
                        decision_at=at,
                        effective_after=at
                        + timedelta(
                            milliseconds=int(
                                self.parameters.get("decision_to_book_latency_ms", 500)
                            )
                        ),
                        expires_at=at
                        + timedelta(seconds=float(self.parameters.get("intent_expiry_seconds", 2))),
                    )
                    funnel["intent"] += 1
                continue

            if event.event_type != "ORDERBOOK" or market is None:
                continue
            payload = event.payload or {}
            pending_entry = pending.get(market)
            if pending_entry is not None:
                if at > pending_entry.expires_at:
                    trades.append(_no_fill_trade(pending_entry, market, "ENTRY_EXPIRED"))
                    del pending[market]
                elif at >= pending_entry.effective_after:
                    # Causal first-book semantics: this one book is the only fill attempt.
                    side = pending_entry.evaluation.candidate_side or "YES"
                    ask, ask_size = _ask(payload, side)
                    if (
                        ask is not None
                        and ask_size >= Decimal("1")
                        and ask <= pending_entry.evaluation.intended_entry_price
                    ):
                        bid, _ = _bid(payload, side)
                        open_positions[market] = _OpenPosition(
                            pending=pending_entry,
                            entry_at=at,
                            entry_price=ask,
                            entry_event_id=event.event_id,
                            best_bid=bid or ask,
                            worst_bid=bid or ask,
                            best_at=at,
                            worst_at=at,
                        )
                        funnel["fill"] += 1
                        funnel["opened"] += 1
                    else:
                        trades.append(_no_fill_trade(pending_entry, market, "ENTRY_NO_FILL"))
                    del pending[market]

            position = open_positions.get(market)
            if position is None:
                continue
            side = position.pending.evaluation.candidate_side or "YES"
            bid, bid_size = _bid(payload, side)
            if bid is not None:
                if bid > position.best_bid:
                    position.best_bid, position.best_at = bid, at
                if bid < position.worst_bid:
                    position.worst_bid, position.worst_at = bid, at
            max_hold = _max_hold(self.parameters, position.pending.evaluation.timing_tier)
            if not position.exit_checked and at >= position.entry_at + timedelta(seconds=max_hold):
                position.exit_checked = True
                funnel["exit_intent"] += 1
                # Same first-book rule for exits. A thin/invalid first book cannot be rescued later.
                if bid is not None and bid_size >= Decimal("1"):
                    trades.append(
                        _closed_trade(
                            position, market, at, event.event_id, bid, self.fee_model, "TIME_STOP"
                        )
                    )
                    funnel["exit"] += 1
                    funnel["exit_fill"] += 1
                    funnel["closed"] += 1
                    del open_positions[market]

        for market, entry in pending.items():
            trades.append(_no_fill_trade(entry, market, "ENTRY_EXPIRED"))
        for market, position in open_positions.items():
            outcome = outcome_by_market.get(market)
            if outcome and outcome.outcome_status == "RESOLVED" and outcome.result_side:
                exit_price = (
                    Decimal("1")
                    if outcome.result_side == position.pending.evaluation.candidate_side
                    else Decimal("0")
                )
                closed_at = _utc(
                    outcome.resolved_at or outcome.market_close_at or position.entry_at
                )
                trades.append(
                    _closed_trade(
                        position, market, closed_at, None, exit_price, self.fee_model, "SETTLEMENT"
                    )
                )
                funnel["exit"] += 1
                funnel["exit_intent"] += 1
                funnel["exit_fill"] += 1
                funnel["closed"] += 1

        report = zero_entry_audit(
            funnel,
            market_count=len({event.market_ticker for event in ordered if event.market_ticker}),
            samples=samples,
            trades=trades,
        )
        return ReplayResult(
            decisions=tuple(decisions),
            trades=tuple(trades),
            zero_entry_report=report,
            blocker_funnel=blockers,
            event_count=len(ordered),
            dataset_hash=_dataset_hash(ordered),
            cost_model=self.fee_model.metadata(),
        )


def zero_entry_audit(
    funnel: dict[str, int],
    *,
    market_count: int,
    samples: Iterable[tuple[str, V2FeatureEvaluation]] = (),
    trades: Iterable[ReplayTrade] = (),
) -> dict[str, Any]:
    """Describe strategy frequency as an evidence gap, never as selectivity."""
    observed_samples = list(samples)
    observed_trades = list(trades)
    entries_per_100 = (
        Decimal("0")
        if market_count == 0
        else Decimal(funnel.get("opened", 0)) * 100 / Decimal(market_count)
    )
    if entries_per_100 == 0:
        classification = "ZERO_ENTRY_UNVALIDATABLE"
    elif entries_per_100 < 1:
        classification = "TOO_RARE_UNVALIDATABLE"
    elif entries_per_100 < 2:
        classification = "BELOW_RESEARCH_TARGET"
    elif entries_per_100 <= 10:
        classification = "RESEARCH_TARGET_RANGE"
    elif entries_per_100 <= 15:
        classification = "ABOVE_RESEARCH_TARGET"
    else:
        classification = "EXCESSIVE_FREQUENCY"
    denominator = max(funnel.get("all_samples", 0), 1)
    first_blockers: dict[str, int] = {}
    blocker_combinations: dict[str, int] = {}
    candidate_modes: dict[str, int] = {}
    timing_tiers: dict[str, int] = {}
    score_margins: list[str] = []
    edge_margins: list[str] = []
    desired_asks: list[str] = []
    spreads: list[str] = []
    depths: list[str] = []
    gates_away = {"zero": 0, "one": 0, "two": 0, "three_or_more": 0}
    maxima: dict[str, dict[str, str]] = {}
    near_misses: list[dict[str, Any]] = []
    continuation_markets: set[str] = set()
    for market, evaluation in observed_samples:
        features = evaluation.measurements.get("features") or {}
        mode = str(evaluation.candidate_mode or "missing")
        tier = str(evaluation.timing_tier or "missing")
        candidate_modes[mode] = candidate_modes.get(mode, 0) + 1
        timing_tiers[tier] = timing_tiers.get(tier, 0) + 1
        if evaluation.candidate_mode == "CONTINUATION":
            continuation_markets.add(market)
        blockers = list(evaluation.blockers)
        if blockers:
            first_blockers[blockers[0]] = first_blockers.get(blockers[0], 0) + 1
            combination = "|".join(blockers)
            blocker_combinations[combination] = blocker_combinations.get(combination, 0) + 1
        count = len(blockers)
        gates_away[
            "zero"
            if count == 0
            else "one"
            if count == 1
            else "two"
            if count == 2
            else "three_or_more"
        ] += 1
        if evaluation.score is not None and evaluation.score_threshold is not None:
            score_margins.append(str(evaluation.score - evaluation.score_threshold))
        if evaluation.edge_lower_bound_cents is not None:
            edge_margins.append(
                str(
                    evaluation.edge_lower_bound_cents
                    - Decimal(str(features.get("edge_threshold_cents", "1.5")))
                )
            )
        for key, target in (
            ("desired_ask", desired_asks),
            ("desired_spread_cents", spreads),
            ("desired_ask_depth", depths),
        ):
            if features.get(key) is not None:
                target.append(str(features[key]))
        current = maxima.setdefault(market, {})
        if evaluation.score is not None and Decimal(str(evaluation.score)) > Decimal(
            current.get("maximum_score", "-Infinity")
        ):
            current["maximum_score"] = str(evaluation.score)
        if evaluation.edge_lower_bound_cents is not None and Decimal(
            str(evaluation.edge_lower_bound_cents)
        ) > Decimal(current.get("maximum_edge", "-Infinity")):
            current["maximum_edge"] = str(evaluation.edge_lower_bound_cents)
        if evaluation.candidate_mode == "CONTINUATION" and (
            "best_continuation_score" not in current
            or evaluation.score is not None
            and Decimal(str(evaluation.score)) > Decimal(current["best_continuation_score"])
        ):
            current["best_continuation_score"] = str(evaluation.score or Decimal("0"))
        if 1 <= count <= 3:
            near_misses.append(
                {
                    "market_ticker": market,
                    "state": evaluation.state,
                    "blockers": blockers,
                    "score_margin": score_margins[-1] if score_margins else None,
                    "edge_margin": edge_margins[-1] if edge_margins else None,
                }
            )
    return {
        "pipeline": dict(funnel),
        "pipeline_percentages": {
            stage: str(
                (Decimal(count) * Decimal("100") / Decimal(denominator)).quantize(Decimal("0.01"))
            )
            for stage, count in funnel.items()
        },
        "one_second_row_statistics": {"sample_count": len(observed_samples)},
        "unique_market_statistics": {"market_count": market_count},
        "decision_states": _count_values(evaluation.state for _, evaluation in observed_samples),
        "first_blockers": first_blockers,
        "all_blocker_combinations": blocker_combinations,
        "hard_gate_count_distance": gates_away,
        "score_margin_distribution": score_margins,
        "edge_margin_distribution": edge_margins,
        "desired_ask_distribution": desired_asks,
        "spread_distribution": spreads,
        "depth_distribution": depths,
        "candidate_mode_distribution": candidate_modes,
        "timing_tier_distribution": timing_tiers,
        "per_market_maxima": maxima,
        "top_near_miss_samples": near_misses[:25],
        "markets_with_signal_but_no_fill": sorted(
            {trade.market_ticker for trade in observed_trades if trade.status == "ENTRY_NO_FILL"}
        ),
        "markets_without_eligible_continuation": sorted(
            {market for market, _ in observed_samples} - continuation_markets
        ),
        "market_count": market_count,
        "entries_per_100_markets": str(entries_per_100.quantize(Decimal("0.01"))),
        "frequency_classification": classification,
        "validation_status": "UNVALIDATABLE"
        if classification == "ZERO_ENTRY_UNVALIDATABLE"
        else "REQUIRES_REVIEW",
    }


def _count_values(values: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return counts


def _new_funnel() -> dict[str, int]:
    return {
        "all_samples": 0,
        "prerequisites_ready": 0,
        "timing": 0,
        "side": 0,
        "continuation": 0,
        "hard_gates": 0,
        "score": 0,
        "edge": 0,
        "signal": 0,
        "intent": 0,
        "fill": 0,
        "opened": 0,
        "exit_intent": 0,
        "exit_fill": 0,
        "exit": 0,
        "closed": 0,
    }


def _add_funnel(funnel: dict[str, int], evaluation: V2FeatureEvaluation) -> None:
    funnel["all_samples"] += 1
    features = evaluation.measurements.get("features") or {}
    quality = features.get("quality_state") if isinstance(features, dict) else {}
    if isinstance(quality, dict) and all(
        quality.get(key) for key in ("market_ready", "reference_ready", "book_ready")
    ):
        funnel["prerequisites_ready"] += 1
    if evaluation.timing_tier is not None:
        funnel["timing"] += 1
    if evaluation.candidate_side is not None:
        funnel["side"] += 1
    if evaluation.candidate_mode == "CONTINUATION":
        funnel["continuation"] += 1
    if not evaluation.blockers:
        funnel["hard_gates"] += 1
    if evaluation.state not in {"V2_FEATURES_NOT_READY", "V2_HARD_GATE_BLOCKED"}:
        funnel["score"] += 1
    if evaluation.state not in {
        "V2_FEATURES_NOT_READY",
        "V2_HARD_GATE_BLOCKED",
        "V2_SCORE_BELOW_THRESHOLD",
    }:
        funnel["edge"] += 1
    if evaluation.state == "DRY_RUN_ENTRY_SIGNAL":
        funnel["signal"] += 1


def _no_fill_trade(pending: _PendingEntry, market: str, reason: str) -> ReplayTrade:
    return ReplayTrade(
        trade_id=_trade_id(pending, market, reason),
        market_ticker=market,
        side=pending.evaluation.candidate_side or "YES",
        entry_decision_at=pending.decision_at,
        entry_fill_at=None,
        entry_limit=pending.evaluation.intended_entry_price or Decimal("0"),
        entry_fill_price=None,
        entry_fill_event_id=None,
        exit_trigger_at=None,
        exit_fill_at=None,
        exit_fill_price=None,
        exit_fill_event_id=None,
        status=reason,
        gross_pnl_cents=None,
        fee_cents=None,
        net_pnl_cents=None,
        holding_duration_ms=None,
        mfe_cents=None,
        mae_cents=None,
        time_to_mfe_ms=None,
        time_to_mae_ms=None,
        entry_reason=pending.evaluation.reason,
        exit_reason=None,
        timing_tier=pending.evaluation.timing_tier,
        measurements={"replay_schema_version": REPLAY_SCHEMA_VERSION},
    )


def _closed_trade(
    position: _OpenPosition,
    market: str,
    exit_at: datetime,
    exit_event_id: str | None,
    exit_price: Decimal,
    fee_model: FeeModel,
    reason: str,
) -> ReplayTrade:
    gross = (exit_price - position.entry_price) * Decimal("100")
    fees = fee_model.fee_cents(price=position.entry_price) + fee_model.fee_cents(price=exit_price)
    best = (position.best_bid - position.entry_price) * Decimal("100")
    worst = (position.worst_bid - position.entry_price) * Decimal("100")
    return ReplayTrade(
        trade_id=_trade_id(position.pending, market, f"closed-{exit_at.isoformat()}"),
        market_ticker=market,
        side=position.pending.evaluation.candidate_side or "YES",
        entry_decision_at=position.pending.decision_at,
        entry_fill_at=position.entry_at,
        entry_limit=position.pending.evaluation.intended_entry_price or Decimal("0"),
        entry_fill_price=position.entry_price,
        entry_fill_event_id=position.entry_event_id,
        exit_trigger_at=exit_at,
        exit_fill_at=exit_at,
        exit_fill_price=exit_price,
        exit_fill_event_id=exit_event_id,
        status="CLOSED",
        gross_pnl_cents=gross,
        fee_cents=fees,
        net_pnl_cents=gross - fees,
        holding_duration_ms=int((exit_at - position.entry_at).total_seconds() * 1000),
        mfe_cents=max(Decimal("0"), best, gross),
        mae_cents=min(Decimal("0"), worst, gross),
        time_to_mfe_ms=int((position.best_at - position.entry_at).total_seconds() * 1000),
        time_to_mae_ms=int((position.worst_at - position.entry_at).total_seconds() * 1000),
        entry_reason=position.pending.evaluation.reason,
        exit_reason=reason,
        timing_tier=position.pending.evaluation.timing_tier,
        measurements={
            "replay_schema_version": REPLAY_SCHEMA_VERSION,
            "lifecycle_version": V2_LIFECYCLE_SCHEMA_VERSION,
        },
    )


def _ask(payload: dict[str, Any], side: str) -> tuple[Decimal | None, Decimal]:
    key = "yes" if side == "YES" else "no"
    return _decimal(payload.get(f"{key}_ask")), _decimal(payload.get(f"{key}_ask_size")) or Decimal(
        "0"
    )


def _bid(payload: dict[str, Any], side: str) -> tuple[Decimal | None, Decimal]:
    key = "yes" if side == "YES" else "no"
    return _decimal(payload.get(f"{key}_bid")), _decimal(payload.get(f"{key}_bid_size")) or Decimal(
        "0"
    )


def _max_hold(parameters: dict[str, Any], tier: str | None) -> int:
    if tier is None:
        return 30
    return int(parameters["tiers"][tier]["max_hold"])


def _hydrate_vector(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return _decimalize(value)


def _decimalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _decimalize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_decimalize(item) for item in value]
    if isinstance(value, str):
        try:
            return Decimal(value)
        except Exception:
            return value
    return value


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _event_key(event: ResearchReplayEvent) -> tuple[datetime, datetime, str, str]:
    return (
        _utc(event.event_time),
        _utc(event.received_at or event.event_time),
        str(event.source_row_id),
        event.event_id,
    )


def _dataset_hash(events: tuple[ResearchReplayEvent, ...]) -> str:
    return hashlib.sha256("|".join(event.event_hash for event in events).encode()).hexdigest()


def _trade_id(pending: _PendingEntry, market: str, suffix: str) -> str:
    return (
        "replay-trade-"
        + hashlib.sha256(f"{pending.event_id}|{market}|{suffix}".encode()).hexdigest()[:24]
    )


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
