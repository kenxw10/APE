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
    evaluate_momentum_v2_lifecycle,
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
    exit_intent_at: datetime | None
    exit_limit: Decimal | None
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
    decision_count: int
    unique_market_count: int
    dataset_hash: str
    cost_model: dict[str, str]


@dataclass
class _PendingEntry:
    evaluation: V2FeatureEvaluation
    market_ticker: str
    event_id: str
    feature_snapshot_id: str | None
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
    exit_attempts: int = 0


@dataclass
class _PendingExit:
    position: _OpenPosition
    market_ticker: str
    trigger: str
    classification: str
    triggered_at: datetime
    effective_after: datetime
    expires_at: datetime
    intended_limit: Decimal


ZERO_ENTRY_DISTRIBUTION_CAP = 2_000
ZERO_ENTRY_SAMPLE_SCHEMA_VERSION = "pr11b-v1"


class _ZeroEntryAccumulator:
    """Exact counters plus explicitly bounded distribution evidence."""

    def __init__(self) -> None:
        self.sample_count = 0
        self.observed_markets: set[str] = set()
        self.continuation_markets: set[str] = set()
        self.qualified_markets: set[str] = set()
        self.signal_markets: set[str] = set()
        self.first_blockers: dict[str, int] = {}
        self.blocker_combinations: dict[str, int] = {}
        self.candidate_modes: dict[str, int] = {}
        self.timing_tiers: dict[str, int] = {}
        self.decision_states: dict[str, int] = {}
        self.gates_away = {"zero": 0, "one": 0, "two": 0, "three_or_more": 0}
        self.maxima: dict[str, dict[str, str]] = {}
        self.near_misses: list[dict[str, Any]] = []
        self.distributions = {
            "score_margin_distribution": _BoundedDistribution(),
            "edge_margin_distribution": _BoundedDistribution(),
            "desired_ask_distribution": _BoundedDistribution(),
            "spread_distribution": _BoundedDistribution(),
            "depth_distribution": _BoundedDistribution(),
        }

    def add(self, market: str, evaluation: V2FeatureEvaluation) -> None:
        self.sample_count += 1
        self.observed_markets.add(market)
        self.decision_states[evaluation.state] = self.decision_states.get(evaluation.state, 0) + 1
        features = evaluation.measurements.get("features") or {}
        mode = str(evaluation.candidate_mode or "missing")
        tier = str(evaluation.timing_tier or "missing")
        self.candidate_modes[mode] = self.candidate_modes.get(mode, 0) + 1
        self.timing_tiers[tier] = self.timing_tiers.get(tier, 0) + 1
        if evaluation.candidate_mode == "CONTINUATION":
            self.continuation_markets.add(market)
        if evaluation.state in {
            "V2_SCORE_BELOW_THRESHOLD",
            "V2_EDGE_BELOW_THRESHOLD",
            "DRY_RUN_ENTRY_SIGNAL",
        }:
            self.qualified_markets.add(market)
        if evaluation.state == "DRY_RUN_ENTRY_SIGNAL":
            self.signal_markets.add(market)
        blockers = list(evaluation.blockers)
        if blockers:
            self.first_blockers[blockers[0]] = self.first_blockers.get(blockers[0], 0) + 1
            combination = "|".join(blockers)
            self.blocker_combinations[combination] = (
                self.blocker_combinations.get(combination, 0) + 1
            )
        count = len(blockers)
        self.gates_away[
            "zero"
            if count == 0
            else "one"
            if count == 1
            else "two"
            if count == 2
            else "three_or_more"
        ] += 1
        score_margin: str | None = None
        edge_margin: str | None = None
        if evaluation.score is not None and evaluation.score_threshold is not None:
            score_margin = str(evaluation.score - evaluation.score_threshold)
            self.distributions["score_margin_distribution"].add(score_margin)
        edge = evaluation.measurements.get("edge")
        edge_threshold = edge.get("threshold_cents") if isinstance(edge, dict) else None
        if evaluation.edge_lower_bound_cents is not None and edge_threshold is not None:
            edge_margin = str(evaluation.edge_lower_bound_cents - Decimal(str(edge_threshold)))
            self.distributions["edge_margin_distribution"].add(edge_margin)
        for key, distribution in (
            ("desired_ask", "desired_ask_distribution"),
            ("desired_spread_cents", "spread_distribution"),
            ("desired_ask_depth", "depth_distribution"),
        ):
            if features.get(key) is not None:
                self.distributions[distribution].add(str(features[key]))
        current = self.maxima.setdefault(market, {})
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
        if 1 <= count <= 3 and len(self.near_misses) < 25:
            self.near_misses.append(
                {
                    "market_ticker": market,
                    "state": evaluation.state,
                    "blockers": blockers,
                    "score_margin": score_margin,
                    "edge_margin": edge_margin,
                }
            )

    def report(self, funnel: dict[str, int], trades: Iterable[ReplayTrade]) -> dict[str, Any]:
        market_count = len(self.observed_markets)
        fills_per_100 = (
            Decimal("0")
            if market_count == 0
            else Decimal(funnel.get("fill", 0)) * 100 / Decimal(market_count)
        )
        classification = _fill_frequency_classification(fills_per_100)
        denominator = max(funnel.get("all_samples", 0), 1)
        no_fill_markets = {
            trade.market_ticker
            for trade in trades
            if trade.status in {"ENTRY_NO_FILL", "ENTRY_EXPIRED"}
        }
        report = {
            "pipeline": dict(funnel),
            "pipeline_percentages": {
                stage: str((Decimal(count) * 100 / Decimal(denominator)).quantize(Decimal("0.01")))
                for stage, count in funnel.items()
            },
            "one_second_row_statistics": {"sample_count": self.sample_count},
            "unique_market_statistics": {"market_count": market_count},
            "decision_states": self.decision_states,
            "first_blockers": self.first_blockers,
            "all_blocker_combinations": self.blocker_combinations,
            "hard_gate_count_distance": self.gates_away,
            "candidate_mode_distribution": self.candidate_modes,
            "timing_tier_distribution": self.timing_tiers,
            "per_market_maxima": self.maxima,
            "top_near_miss_samples": self.near_misses,
            "markets_with_signal_but_no_fill": sorted(no_fill_markets),
            "markets_without_eligible_continuation": sorted(
                self.observed_markets - self.continuation_markets
            ),
            "market_count": market_count,
            "unique_market_rates_per_100": {
                "qualified_setups": _per_100(len(self.qualified_markets), market_count),
                "entry_signals": _per_100(len(self.signal_markets), market_count),
                "entry_intents": _per_100(funnel.get("intent", 0), market_count),
                "executable_fills": _per_100(funnel.get("fill", 0), market_count),
                "closed_positions": _per_100(funnel.get("closed", 0), market_count),
            },
            "qualified_setup_frequency_classification": _setup_frequency_classification(
                Decimal(_per_100(len(self.qualified_markets), market_count))
            ),
            "fill_frequency_classification": classification,
            "entries_per_100_markets": str(fills_per_100.quantize(Decimal("0.01"))),
            "frequency_classification": classification,
            "validation_status": "UNVALIDATABLE"
            if classification == "ZERO_ENTRY_UNVALIDATABLE"
            else "REQUIRES_REVIEW",
        }
        distribution_metadata: dict[str, Any] = {}
        for key, distribution in self.distributions.items():
            report[key] = distribution.values()
            distribution_metadata[key] = distribution.metadata()
        report["distribution_sampling"] = {
            "schema_version": ZERO_ENTRY_SAMPLE_SCHEMA_VERSION,
            "cap": ZERO_ENTRY_DISTRIBUTION_CAP,
            "distributions": distribution_metadata,
        }
        return report


class _BoundedDistribution:
    def __init__(self) -> None:
        self.count = 0
        self._all: list[str] = []
        self._sample: list[tuple[str, str]] = []

    def add(self, value: str) -> None:
        self.count += 1
        if len(self._all) < ZERO_ENTRY_DISTRIBUTION_CAP:
            self._all.append(value)
            return
        if self._all:
            self._sample = [
                (_stable_sample_key(item, index), item)
                for index, item in enumerate(self._all)
            ]
            self._all = []
        candidate = (_stable_sample_key(value, self.count), value)
        self._sample.append(candidate)
        self._sample.sort()
        del self._sample[ZERO_ENTRY_DISTRIBUTION_CAP:]

    def values(self) -> list[str]:
        if self.count <= ZERO_ENTRY_DISTRIBUTION_CAP:
            return list(self._all)
        return [value for _, value in self._sample]

    def metadata(self) -> dict[str, Any]:
        return {
            "observed_count": self.count,
            "sampled": self.count > ZERO_ENTRY_DISTRIBUTION_CAP,
        }


def _stable_sample_key(value: str, index: int) -> str:
    return hashlib.sha256(f"{index}|{value}".encode()).hexdigest()


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
        state = self.start_incremental(outcomes=outcomes, retain_decisions=True)
        for event in ordered:
            state.consume(event)
        return state.finalize()

    def start_incremental(
        self,
        *,
        outcomes: Iterable[ResearchMarketOutcome] = (),
        retain_decisions: bool = False,
    ) -> _IncrementalReplayState:
        return _IncrementalReplayState(
            self,
            outcomes=outcomes,
            retain_decisions=retain_decisions,
        )

    def replay_ordered_pages(
        self,
        pages: Iterable[Iterable[ResearchReplayEvent]],
        *,
        outcomes: Iterable[ResearchMarketOutcome] = (),
        retain_decisions: bool = False,
    ) -> ReplayResult:
        """Consume a pre-ordered bounded stream without retaining its input events."""
        state = self.start_incremental(outcomes=outcomes, retain_decisions=retain_decisions)
        for page in pages:
            for event in page:
                state.consume(event)
        return state.finalize()


class _IncrementalReplayState:
    """Replay state that retains lifecycle state, not the source-event archive."""

    def __init__(
        self,
        engine: DeterministicReplayEngine,
        *,
        outcomes: Iterable[ResearchMarketOutcome],
        retain_decisions: bool,
    ) -> None:
        self.engine = engine
        self.outcomes = {outcome.market_ticker: outcome for outcome in outcomes}
        self.retain_decisions = retain_decisions
        self.decisions: list[V2FeatureEvaluation] = []
        self.trades: list[ReplayTrade] = []
        self.audit = _ZeroEntryAccumulator()
        self.pending: dict[str, _PendingEntry] = {}
        self.entry_attempted_markets: set[str] = set()
        self.open_position: _OpenPosition | None = None
        self.pending_exit: _PendingExit | None = None
        self.latest_books: dict[str, tuple[dict[str, Any], str, datetime]] = {}
        self.funnel = _new_funnel()
        self.blockers: dict[str, int] = {}
        self.event_count = 0
        self.decision_count = 0
        self.market_tickers: set[str] = set()
        self._hash = hashlib.sha256()
        self._first_hash = True

    def consume(self, event: ResearchReplayEvent) -> None:
        self.event_count += 1
        if not self._first_hash:
            self._hash.update(b"|")
        self._hash.update(event.event_hash.encode())
        self._first_hash = False
        at = _utc(event.event_time)
        market = event.market_ticker
        if market is not None:
            self.market_tickers.add(market)
        if event.event_type == "FEATURE_SNAPSHOT" and market is not None:
            self._consume_feature(event, market, at)
            return
        if event.event_type == "ORDERBOOK" and market is not None:
            self._consume_orderbook(event, market, at)

    def _consume_feature(
        self, event: ResearchReplayEvent, market: str, at: datetime
    ) -> None:
        if event.replay_readiness != "FULL":
            self.funnel["all_samples"] += 1
            self._add_blocker("partial_feature_vector")
            return
        vector = _hydrate_vector((event.payload or {}).get("feature_vector"))
        if not vector:
            self.funnel["all_samples"] += 1
            self._add_blocker("feature_vector_missing")
            return
        evaluation = evaluate_momentum_v2_feature_vector(vector, self.engine.parameters)
        self.decision_count += 1
        if self.retain_decisions:
            self.decisions.append(evaluation)
        self.audit.add(market, evaluation)
        _add_funnel(self.funnel, evaluation)
        for blocker in evaluation.blockers:
            self._add_blocker(blocker)
        if (
            evaluation.state == "DRY_RUN_ENTRY_SIGNAL"
            and not self.pending
            and market not in self.pending
            and market not in self.entry_attempted_markets
            and self.open_position is None
        ):
            self.entry_attempted_markets.add(market)
            latency = timedelta(
                milliseconds=int(self.engine.parameters.get("decision_to_book_latency_ms", 500))
            )
            self.pending[market] = _PendingEntry(
                evaluation=evaluation,
                market_ticker=market,
                event_id=event.event_id,
                feature_snapshot_id=event.feature_snapshot_id or event.event_id,
                decision_at=at,
                effective_after=at + latency,
                expires_at=at
                + latency
                + timedelta(seconds=float(self.engine.parameters.get("intent_expiry_seconds", 2))),
            )
            self.funnel["intent"] += 1
        if self.open_position is not None and self.pending_exit is None:
            held_payload = self.latest_books.get(
                self.open_position.pending.market_ticker, ({}, "", at)
            )[0]
            side = self.open_position.pending.evaluation.candidate_side or "YES"
            held_bid, _ = _bid(held_payload or {}, side)
            lifecycle = evaluate_momentum_v2_lifecycle(
                _lifecycle_inputs(
                    position=self.open_position,
                    evaluation=evaluation,
                    features=vector,
                    held_bid=held_bid,
                    market_matches=market == self.open_position.pending.market_ticker,
                    evaluated_at=at,
                    parameters=self.engine.parameters,
                ),
                self.engine.parameters,
            )
            if lifecycle.trigger is not None and held_bid is not None:
                if self.open_position.exit_attempts < 3:
                    latency = timedelta(
                        milliseconds=int(
                            self.engine.parameters.get("decision_to_book_latency_ms", 500)
                        )
                    )
                    self.pending_exit = _PendingExit(
                        position=self.open_position,
                        market_ticker=self.open_position.pending.market_ticker,
                        trigger=lifecycle.trigger,
                        classification=lifecycle.classification or "SIGNAL",
                        triggered_at=at,
                        effective_after=at + latency,
                        expires_at=at
                        + latency
                        + timedelta(
                            seconds=float(self.engine.parameters.get("intent_expiry_seconds", 2))
                        ),
                        intended_limit=max(held_bid - Decimal("0.01"), Decimal("0.01")),
                    )
                    self.funnel["exit_intent"] += 1

    def _consume_orderbook(
        self, event: ResearchReplayEvent, market: str, at: datetime
    ) -> None:
        payload = event.payload or {}
        self.latest_books[market] = (payload, event.event_id, at)
        pending_entry = self.pending.get(market)
        if pending_entry is not None:
            if at > pending_entry.expires_at:
                self.trades.append(_no_fill_trade(pending_entry, market, "ENTRY_EXPIRED"))
                del self.pending[market]
            elif at >= pending_entry.effective_after:
                side = pending_entry.evaluation.candidate_side or "YES"
                ask, ask_size = _ask(payload, side)
                if (
                    ask is not None
                    and ask_size >= Decimal("1")
                    and ask <= pending_entry.evaluation.intended_entry_price
                ):
                    bid, _ = _bid(payload, side)
                    self.open_position = _OpenPosition(
                        pending=pending_entry,
                        entry_at=at,
                        entry_price=ask,
                        entry_event_id=event.event_id,
                        best_bid=bid if bid is not None else ask,
                        worst_bid=bid if bid is not None else ask,
                        best_at=at,
                        worst_at=at,
                    )
                    self.funnel["fill"] += 1
                    self.funnel["opened"] += 1
                else:
                    self.trades.append(_no_fill_trade(pending_entry, market, "ENTRY_NO_FILL"))
                del self.pending[market]
        if self.pending_exit is not None and self.pending_exit.market_ticker == market:
            if at > self.pending_exit.expires_at:
                self.pending_exit.position.exit_attempts += 1
                self.pending_exit = None
            elif at >= self.pending_exit.effective_after:
                side = self.pending_exit.position.pending.evaluation.candidate_side or "YES"
                bid, bid_size = _bid(payload, side)
                if (
                    bid is not None
                    and bid_size >= Decimal("1")
                    and bid >= self.pending_exit.intended_limit
                ):
                    self.trades.append(
                        _closed_trade(
                            self.pending_exit.position,
                            market,
                            at,
                            event.event_id,
                            bid,
                            self.engine.fee_model,
                            self.pending_exit.trigger,
                            pending_exit=self.pending_exit,
                        )
                    )
                    self.funnel["exit"] += 1
                    self.funnel["exit_fill"] += 1
                    self.funnel["closed"] += 1
                    self.open_position = None
                else:
                    self.pending_exit.position.exit_attempts += 1
                self.pending_exit = None
        position = (
            self.open_position
            if self.open_position and self.open_position.pending.market_ticker == market
            else None
        )
        if position is not None:
            side = position.pending.evaluation.candidate_side or "YES"
            bid, _ = _bid(payload, side)
            if bid is not None:
                if bid > position.best_bid:
                    position.best_bid, position.best_at = bid, at
                if bid < position.worst_bid:
                    position.worst_bid, position.worst_at = bid, at

    def _add_blocker(self, blocker: str) -> None:
        self.blockers[blocker] = self.blockers.get(blocker, 0) + 1

    def finalize(self) -> ReplayResult:
        for market, entry in self.pending.items():
            self.trades.append(_no_fill_trade(entry, market, "ENTRY_EXPIRED"))
        if self.open_position is not None:
            market = self.open_position.pending.market_ticker
            outcome = self.outcomes.get(market)
            if outcome and outcome.outcome_status == "RESOLVED" and outcome.result_side:
                exit_price = (
                    Decimal("1")
                    if outcome.result_side == self.open_position.pending.evaluation.candidate_side
                    else Decimal("0")
                )
                closed_at = _utc(
                    outcome.resolved_at or outcome.market_close_at or self.open_position.entry_at
                )
                self.trades.append(
                    _closed_trade(
                        self.open_position,
                        market,
                        closed_at,
                        None,
                        exit_price,
                        self.engine.fee_model,
                        "SETTLEMENT",
                    )
                )
                self.funnel["exit"] += 1
                self.funnel["exit_intent"] += 1
                self.funnel["exit_fill"] += 1
                self.funnel["closed"] += 1
        return ReplayResult(
            decisions=tuple(self.decisions),
            trades=tuple(self.trades),
            zero_entry_report=self.audit.report(self.funnel, self.trades),
            blocker_funnel=self.blockers,
            event_count=self.event_count,
            decision_count=self.decision_count,
            unique_market_count=len(self.market_tickers),
            dataset_hash=self._hash.hexdigest(),
            cost_model=self.engine.fee_model.metadata(),
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
    fills_per_100 = (
        Decimal("0")
        if market_count == 0
        else Decimal(funnel.get("fill", 0)) * 100 / Decimal(market_count)
    )
    classification = _fill_frequency_classification(fills_per_100)
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
    qualified_markets: set[str] = set()
    signal_markets: set[str] = set()
    for market, evaluation in observed_samples:
        features = evaluation.measurements.get("features") or {}
        score_margin: str | None = None
        edge_margin: str | None = None
        mode = str(evaluation.candidate_mode or "missing")
        tier = str(evaluation.timing_tier or "missing")
        candidate_modes[mode] = candidate_modes.get(mode, 0) + 1
        timing_tiers[tier] = timing_tiers.get(tier, 0) + 1
        if evaluation.candidate_mode == "CONTINUATION":
            continuation_markets.add(market)
        if evaluation.state in {
            "V2_SCORE_BELOW_THRESHOLD",
            "V2_EDGE_BELOW_THRESHOLD",
            "DRY_RUN_ENTRY_SIGNAL",
        }:
            qualified_markets.add(market)
        if evaluation.state == "DRY_RUN_ENTRY_SIGNAL":
            signal_markets.add(market)
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
            score_margin = str(evaluation.score - evaluation.score_threshold)
            score_margins.append(score_margin)
        edge = evaluation.measurements.get("edge")
        edge_threshold = edge.get("threshold_cents") if isinstance(edge, dict) else None
        if evaluation.edge_lower_bound_cents is not None and edge_threshold is not None:
            edge_margin = str(
                evaluation.edge_lower_bound_cents - Decimal(str(edge_threshold))
            )
            edge_margins.append(edge_margin)
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
                    "score_margin": score_margin,
                    "edge_margin": edge_margin,
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
            {
                trade.market_ticker
                for trade in observed_trades
                if trade.status in {"ENTRY_NO_FILL", "ENTRY_EXPIRED"}
            }
        ),
        "markets_without_eligible_continuation": sorted(
            {market for market, _ in observed_samples} - continuation_markets
        ),
        "market_count": market_count,
        "unique_market_rates_per_100": {
            "qualified_setups": _per_100(len(qualified_markets), market_count),
            "entry_signals": _per_100(len(signal_markets), market_count),
            "entry_intents": _per_100(funnel.get("intent", 0), market_count),
            "executable_fills": _per_100(funnel.get("fill", 0), market_count),
            "closed_positions": _per_100(funnel.get("closed", 0), market_count),
        },
        "qualified_setup_frequency_classification": _setup_frequency_classification(
            Decimal(_per_100(len(qualified_markets), market_count))
        ),
        "fill_frequency_classification": classification,
        # Retained for consumers of the previous endpoint shape.
        "entries_per_100_markets": str(fills_per_100.quantize(Decimal("0.01"))),
        "frequency_classification": classification,
        "validation_status": "UNVALIDATABLE"
        if classification == "ZERO_ENTRY_UNVALIDATABLE"
        else "REQUIRES_REVIEW",
    }


def _per_100(count: int, market_count: int) -> str:
    if market_count <= 0:
        return "0.00"
    return str((Decimal(count) * 100 / Decimal(market_count)).quantize(Decimal("0.01")))


def _fill_frequency_classification(fills_per_100: Decimal) -> str:
    if fills_per_100 == 0:
        return "ZERO_ENTRY_UNVALIDATABLE"
    if fills_per_100 < 1:
        return "TOO_RARE_UNVALIDATABLE"
    if fills_per_100 < 3:
        return "ECONOMICALLY_INADEQUATE"
    if fills_per_100 <= 10:
        return "PREFERRED_OPERATING_RANGE"
    if fills_per_100 <= 15:
        return "ABOVE_PREFERRED_RANGE"
    return "EXCESSIVE_FREQUENCY"


def _setup_frequency_classification(setups_per_100: Decimal) -> str:
    if setups_per_100 < 5:
        return "BELOW_TARGET_RANGE"
    if setups_per_100 <= 15:
        return "WITHIN_TARGET_RANGE"
    return "ABOVE_TARGET_RANGE"


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
        "V2_EDGE_BELOW_THRESHOLD",
    }:
        funnel["edge"] += 1
    if evaluation.state == "DRY_RUN_ENTRY_SIGNAL":
        funnel["signal"] += 1


def _lifecycle_inputs(
    *,
    position: _OpenPosition,
    evaluation: V2FeatureEvaluation,
    features: dict[str, Any],
    held_bid: Decimal | None,
    market_matches: bool,
    evaluated_at: datetime,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    entry_features = position.pending.evaluation.measurements.get("features") or {}
    tier = position.pending.evaluation.timing_tier
    tier_parameters = parameters["tiers"].get(tier or "", {})
    return {
        "candidate_side": evaluation.candidate_side,
        "boundary": features.get("boundary"),
        "current_brti": features.get("current_brti"),
        "seconds_left": features.get("seconds_left"),
        "return_5s": features.get("return_5s"),
        "return_15s": features.get("return_15s"),
        "reversal_beyond_origin": bool(features.get("reversal_beyond_origin")),
        "persistent_adverse_microstructure": bool(
            features.get("persistent_adverse_microstructure")
        ),
        "response_residual_cents": features.get("response_residual_cents"),
        "desired_bid": features.get("desired_bid"),
        "desired_bid_depth": features.get("desired_bid_depth"),
        "timing_tier": evaluation.timing_tier,
        "market_matches": market_matches,
        "entry_price": position.entry_price,
        "entry_boundary": entry_features.get("boundary"),
        "entry_side": position.pending.evaluation.candidate_side,
        "entry_score_threshold": position.pending.evaluation.score_threshold,
        "entry_time_stop_seconds": tier_parameters.get("time_stop"),
        "entry_max_hold_seconds": tier_parameters.get("max_hold"),
        "age_seconds": max(0, int((evaluated_at - position.entry_at).total_seconds())),
        "score": evaluation.score,
        "edge_lower_bound_cents": evaluation.edge_lower_bound_cents,
        "held_bid": held_bid,
    }


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
        exit_intent_at=None,
        exit_limit=None,
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
    *,
    pending_exit: _PendingExit | None = None,
) -> ReplayTrade:
    entry_features = position.pending.evaluation.measurements.get("features") or {}
    gross = (exit_price - position.entry_price) * Decimal("100")
    fees = fee_model.fee_cents(price=position.entry_price) + fee_model.fee_cents(price=exit_price)
    best = (position.best_bid - position.entry_price) * Decimal("100")
    worst = (position.worst_bid - position.entry_price) * Decimal("100")
    mfe_cents = max(Decimal("0"), best, gross)
    mae_cents = min(Decimal("0"), worst, gross)
    time_to_mfe_ms = int(
        (
            (exit_at if gross > max(Decimal("0"), best) else position.best_at)
            - position.entry_at
        ).total_seconds()
        * 1000
    )
    time_to_mae_ms = int(
        (
            (exit_at if gross < min(Decimal("0"), worst) else position.worst_at)
            - position.entry_at
        ).total_seconds()
        * 1000
    )
    return ReplayTrade(
        trade_id=_trade_id(position.pending, market, f"closed-{exit_at.isoformat()}"),
        market_ticker=market,
        side=position.pending.evaluation.candidate_side or "YES",
        entry_decision_at=position.pending.decision_at,
        entry_fill_at=position.entry_at,
        entry_limit=position.pending.evaluation.intended_entry_price or Decimal("0"),
        entry_fill_price=position.entry_price,
        entry_fill_event_id=position.entry_event_id,
        exit_trigger_at=pending_exit.triggered_at if pending_exit is not None else exit_at,
        exit_intent_at=pending_exit.effective_after if pending_exit is not None else exit_at,
        exit_limit=pending_exit.intended_limit if pending_exit is not None else exit_price,
        exit_fill_at=exit_at,
        exit_fill_price=exit_price,
        exit_fill_event_id=exit_event_id,
        status="CLOSED",
        gross_pnl_cents=gross,
        fee_cents=fees,
        net_pnl_cents=gross - fees,
        holding_duration_ms=int((exit_at - position.entry_at).total_seconds() * 1000),
        mfe_cents=mfe_cents,
        mae_cents=mae_cents,
        time_to_mfe_ms=time_to_mfe_ms,
        time_to_mae_ms=time_to_mae_ms,
        entry_reason=position.pending.evaluation.reason,
        exit_reason=reason,
        timing_tier=position.pending.evaluation.timing_tier,
        measurements={
            "replay_schema_version": REPLAY_SCHEMA_VERSION,
            "lifecycle_version": V2_LIFECYCLE_SCHEMA_VERSION,
            "volatility_regime": entry_features.get("volatility_regime"),
            "liquidity_regime": entry_features.get("liquidity_regime"),
            "timing_tier": position.pending.evaluation.timing_tier,
            "entry_feature_snapshot_id": position.pending.feature_snapshot_id,
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


def _event_key(
    event: ResearchReplayEvent,
) -> tuple[datetime, datetime, int, tuple[int, int, str], str]:
    return (
        _utc(event.event_time),
        _utc(event.received_at or event.event_time),
        event.sequence_number if event.sequence_number is not None else 0,
        _source_row_key(event.source_row_id),
        event.event_id,
    )


def _source_row_key(source_row_id: str) -> tuple[int, int, str]:
    try:
        return (0, int(source_row_id), source_row_id)
    except (TypeError, ValueError):
        return (1, 0, str(source_row_id))


def _dataset_hash(events: tuple[ResearchReplayEvent, ...]) -> str:
    return hashlib.sha256("|".join(event.event_hash for event in events).encode()).hexdigest()


def _trade_id(pending: _PendingEntry, market: str, suffix: str) -> str:
    return (
        "replay-trade-"
        + hashlib.sha256(f"{pending.event_id}|{market}|{suffix}".encode()).hexdigest()[:24]
    )


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
