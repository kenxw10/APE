from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from ape.db.models import ResearchMarketOutcome, ResearchReplayEvent


@dataclass(frozen=True)
class ResearchFixtureDataset:
    """A deterministic archive/replay dataset, not descriptive outcome-only flags."""

    events: tuple[ResearchReplayEvent, ...]
    outcomes: tuple[ResearchMarketOutcome, ...]
    scenario_expectations: dict[str, dict[str, Any]]


def fixture_time() -> datetime:
    return datetime(2026, 7, 11, 12, 0, tzinfo=UTC)


def replayable_feature_vector() -> dict[str, Any]:
    return {
        "candidate_side": "YES",
        "candidate_mode": "CONTINUATION",
        "boundary": Decimal("62000"),
        "current_brti": Decimal("62008"),
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
        "contract_brti_response_ratio_15s": Decimal("1"),
        "contract_brti_response_ratio_30s": Decimal("1"),
        "volatility_regime": "medium",
        "liquidity_regime": "deep",
        "architecture_version": "momentum_v2_heuristic_v3",
        "feature_schema_version": "momentum_v2_features_v3",
        "replay_schema_version": "momentum_v2_replay_v1",
        "replay_readiness": "FULL",
        "replay_blockers": [],
    }


def fixture_event(*, at: datetime, market_ticker: str = "M1") -> ResearchReplayEvent:
    vector = _json_vector(replayable_feature_vector())
    return ResearchReplayEvent(
        event_id=f"feature-{market_ticker}",
        market_ticker=market_ticker,
        event_type="FEATURE_SNAPSHOT",
        event_time=at,
        received_at=at,
        source_table="strategy_feature_snapshots",
        source_row_id=f"feature-{market_ticker}",
        source_hash="fixture",
        feature_snapshot_id=f"feature-{market_ticker}",
        feature_schema_version="momentum_v2_features_v3",
        architecture_version="momentum_v2_heuristic_v3",
        replay_schema_version="momentum_v2_replay_v1",
        payload={"feature_vector": vector},
        event_hash=f"fixture-{market_ticker}",
        replay_readiness="FULL",
        blockers=[],
    )


def synthetic_btc15_fixture_markets(count: int = 18) -> list[ResearchMarketOutcome]:
    """Deterministic markets spanning timing, volatility, liquidity, fills, losses, and gaps."""
    start = fixture_time()
    rows: list[ResearchMarketOutcome] = []
    for index in range(count):
        opened = start + timedelta(minutes=15 * index)
        flags = {
            "volatility_regime": ("low", "medium", "high")[index % 3],
            "liquidity_regime": ("thin", "medium", "deep")[index % 3],
            "timing_tier": ("early", "normal", "late")[index % 3],
            "eligible_fill": index % 3 != 0,
            "profitable": index % 2 == 0,
            "boundary_cross_research_sample": index % 5 == 0,
            "stale_prerequisite": index % 7 == 0,
            "market_gap": index % 11 == 0,
            "frozen_holdout": index >= int(count * 0.8),
        }
        rows.append(
            ResearchMarketOutcome(
                outcome_id=f"fixture-outcome-{index}",
                market_ticker=f"FIXTURE-{index:02d}",
                market_open_at=opened,
                market_close_at=opened + timedelta(minutes=15),
                expiration_at=opened + timedelta(minutes=15),
                boundary=Decimal("62000"),
                result_side="YES" if flags["profitable"] else "NO",
                settlement_value=Decimal("62001") if flags["profitable"] else Decimal("61999"),
                final_reference_value=Decimal("62001") if flags["profitable"] else Decimal("61999"),
                final_minute_reference_average=Decimal("62000"),
                outcome_status="RESOLVED",
                outcome_source="fixture",
                source_payload_hash=f"fixture-{index}",
                resolved_at=opened + timedelta(minutes=15),
                expected_frame_count=900,
                actual_frame_count=900 - (30 if flags["market_gap"] else 0),
                coverage_percentage=Decimal("0.966") if flags["market_gap"] else Decimal("1"),
                maximum_event_gap_seconds=31 if flags["market_gap"] else 1,
                quality_flags=flags,
            )
        )
    return rows


def synthetic_btc15_fixture_dataset(count: int = 18) -> ResearchFixtureDataset:
    """Build concrete event-time fixtures for archive, replay, and calibration tests.

    Scenario labels are only an index into this deterministic source data.  Each
    case changes the vector or the subsequent executable book so callers must
    prove the replay outcome rather than trusting a descriptive flag.
    """
    scenarios = (
        "continuation_entry",
        "boundary_cross_research_only",
        "stale_prerequisites",
        "reversal_beyond_origin",
        "entry_no_fill",
        "entry_expiry",
        "later_book_non_rescue",
        "hard_stop",
        "soft_stop_weakening",
        "profit_target",
        "time_stop",
        "maximum_hold",
        "final_twenty_second_force_exit",
        "adverse_boundary_cross",
        "persistent_adverse_microstructure",
        "exit_retry_success",
        "exit_retry_exhaustion",
        "partial_coverage_frozen_holdout",
    )
    outcomes = synthetic_btc15_fixture_markets(count)
    events: list[ResearchReplayEvent] = []
    expectations: dict[str, dict[str, Any]] = {}
    for index, outcome in enumerate(outcomes):
        market = outcome.market_ticker
        opened_at = outcome.market_open_at or fixture_time()
        scenario = scenarios[index % len(scenarios)]
        plan = _scenario_plan(scenario)
        # Every archive fixture carries a concrete book source.  Blocked cases
        # place it before the causal entry window, so it cannot manufacture a fill.
        if not plan["books"]:
            plan["books"] = [_book(100, ask="0.60", bid="0.58")]
        vector = plan["entry_vector"]
        vector.update(
            {
                "volatility_regime": ("low", "medium", "high")[index % 3],
                "liquidity_regime": ("thin", "medium", "deep")[index % 3],
            }
        )
        readiness = plan["readiness"]
        feature_at = opened_at + timedelta(minutes=6)
        feature_id = f"fixture-feature-{index}"
        base = f"fixture-{index}"
        market_events: list[ResearchReplayEvent] = [
                _source_event(
                    event_id=f"{base}-market",
                    market_ticker=market,
                    event_type="MARKET",
                    event_time=opened_at,
                    source_table="markets",
                    payload={"opened_at": opened_at.isoformat()},
                ),
                _source_event(
                    event_id=f"{base}-reference",
                    market_ticker=market,
                    event_type="REFERENCE",
                    event_time=feature_at - timedelta(milliseconds=100),
                    source_table="reference_ticks",
                    payload={"value": str(Decimal("62008") + index)},
                ),
                _source_event(
                    event_id=f"{base}-trade",
                    market_ticker=market,
                    event_type="PUBLIC_TRADE",
                    event_time=feature_at + timedelta(milliseconds=100),
                    source_table="public_trades",
                    payload={"taker_side": "YES", "count": 4},
                ),
                ResearchReplayEvent(
                    event_id=feature_id,
                    market_ticker=market,
                    event_type="FEATURE_SNAPSHOT",
                    event_time=feature_at,
                    received_at=feature_at,
                    source_table="strategy_feature_snapshots",
                    source_row_id=feature_id,
                    source_hash=feature_id,
                    feature_snapshot_id=feature_id,
                    feature_schema_version="momentum_v2_features_v3",
                    architecture_version="momentum_v2_heuristic_v3",
                    replay_schema_version="momentum_v2_replay_v1",
                    payload={"feature_vector": _json_vector(vector)},
                    event_hash=feature_id,
                    replay_readiness=readiness,
                    blockers=[] if readiness == "FULL" else ["fixture_partial_coverage"],
                ),
        ]
        for offset_ms, payload in plan["books"]:
            market_events.append(
                _source_event(
                    event_id=f"{base}-book-{offset_ms}",
                    market_ticker=market,
                    event_type="ORDERBOOK",
                    event_time=feature_at + timedelta(milliseconds=offset_ms),
                    source_table="orderbook_snapshots",
                    payload=payload,
                )
            )
        for offset_ms, lifecycle_vector in plan["management_vectors"]:
            lifecycle_id = f"{base}-lifecycle-{offset_ms}"
            market_events.append(
                ResearchReplayEvent(
                    event_id=lifecycle_id,
                    market_ticker=market,
                    event_type="FEATURE_SNAPSHOT",
                    event_time=feature_at + timedelta(milliseconds=offset_ms),
                    received_at=feature_at + timedelta(milliseconds=offset_ms),
                    source_table="strategy_feature_snapshots",
                    source_row_id=lifecycle_id,
                    source_hash=lifecycle_id,
                    feature_snapshot_id=lifecycle_id,
                    feature_schema_version="momentum_v2_features_v3",
                    architecture_version="momentum_v2_heuristic_v3",
                    replay_schema_version="momentum_v2_replay_v1",
                    payload={"feature_vector": _json_vector(lifecycle_vector)},
                    event_hash=lifecycle_id,
                    replay_readiness="FULL",
                    blockers=[],
                )
            )
        market_events.append(
            _source_event(
                event_id=f"{base}-lifecycle",
                market_ticker=market,
                event_type="MARKET_LIFECYCLE",
                event_time=opened_at + timedelta(minutes=15),
                source_table="market_lifecycle",
                payload={"status": "settled"},
            )
        )
        events.extend(market_events)
        flags = dict(outcome.quality_flags or {})
        flags.update(
            {
                "scenario": scenario,
                "counterfactual_labels": {
                    feature_id: {"net_markout_30s_cents": "1"}
                },
            }
        )
        outcome.quality_flags = flags
        if plan["outcome_status"] != "RESOLVED":
            outcome.outcome_status = plan["outcome_status"]
            outcome.result_side = None
        expectations[market] = dict(plan["expectation"])
        expectations[market]["scenario"] = scenario
    return ResearchFixtureDataset(tuple(events), tuple(outcomes), expectations)


def synthetic_governance_fixture_dataset(count: int = 500) -> ResearchFixtureDataset:
    """Return 500 complete markets with causal closes for governance smoke tests.

    Ten search-development entries yield a 3.125 per-hundred candidate rate;
    one is marginal and only the bounded candidate accepts it.  Fifty distinct
    frozen-holdout entries then provide the required causal closed-trade
    evidence without changing search-time selection metrics.
    """
    if count < 500:
        raise ValueError("The governance fixture requires at least 500 markets.")
    events: list[ResearchReplayEvent] = []
    outcomes: list[ResearchMarketOutcome] = []
    expectations: dict[str, dict[str, Any]] = {}
    start = fixture_time()
    search_candidate_indices = set(range(0, 320, 32))
    holdout_candidate_indices = set(range(400, 500, 2))
    marginal_indices = {32}
    for index in range(count):
        market = f"GOVERNANCE-{index:03d}"
        opened_at = start + timedelta(minutes=15 * index)
        feature_at = opened_at + timedelta(minutes=6)
        feature_id = f"governance-feature-{index}"
        candidate_market = index in (search_candidate_indices | holdout_candidate_indices)
        marginal = index in marginal_indices
        vector = replayable_feature_vector()
        if not candidate_market:
            vector["quality_state"] = {
                "market_ready": True,
                "reference_ready": False,
                "book_ready": True,
            }
        elif marginal:
            vector.update(
                {
                    "return_5s": Decimal("0"),
                    "return_15s": Decimal("0"),
                    "return_30s": Decimal("0"),
                    "impulse_hold_seconds": 0,
                    "fast_impulse_active": True,
                }
            )
        vector.update(
            {
                "volatility_regime": ("low", "medium", "high")[index % 3],
                "liquidity_regime": ("thin", "medium", "deep")[index % 3],
            }
        )
        if candidate_market:
            vector["timing_tier"] = (
                "normal" if marginal else ("early", "normal", "late")[index % 3]
            )
        labels = {feature_id: {"net_markout_30s_cents": "1"}}
        if candidate_market:
            labels[f"governance-manage-{index}"] = {"net_markout_30s_cents": "1"}
        outcomes.append(
            ResearchMarketOutcome(
                outcome_id=f"governance-outcome-{index}",
                market_ticker=market,
                market_open_at=opened_at,
                market_close_at=opened_at + timedelta(minutes=15),
                expiration_at=opened_at + timedelta(minutes=15),
                boundary=Decimal("62000"),
                result_side="YES",
                settlement_value=Decimal("62001"),
                final_reference_value=Decimal("62001"),
                final_minute_reference_average=Decimal("62000"),
                outcome_status="RESOLVED",
                outcome_source="fixture",
                source_payload_hash=f"governance-{index}",
                resolved_at=opened_at + timedelta(minutes=15),
                expected_frame_count=900,
                actual_frame_count=900,
                coverage_percentage=Decimal("1"),
                maximum_event_gap_seconds=1,
                quality_flags={
                    "counterfactual_labels": labels,
                    "volatility_regime": vector["volatility_regime"],
                    "liquidity_regime": vector["liquidity_regime"],
                    "timing_tier": "normal",
                },
            )
        )
        events.extend(
            (
                _source_event(
                    event_id=f"governance-market-{index}",
                    market_ticker=market,
                    event_type="MARKET",
                    event_time=opened_at,
                    source_table="markets",
                    payload={"open_time": opened_at.isoformat()},
                ),
                _source_event(
                    event_id=f"governance-reference-{index}",
                    market_ticker=market,
                    event_type="REFERENCE",
                    event_time=feature_at - timedelta(milliseconds=100),
                    source_table="reference_ticks",
                    payload={"value": "62008"},
                ),
                ResearchReplayEvent(
                    event_id=feature_id,
                    market_ticker=market,
                    event_type="FEATURE_SNAPSHOT",
                    event_time=feature_at,
                    received_at=feature_at,
                    source_table="strategy_feature_snapshots",
                    source_row_id=feature_id,
                    source_hash=feature_id,
                    feature_snapshot_id=feature_id,
                    feature_schema_version="momentum_v2_features_v3",
                    architecture_version="momentum_v2_heuristic_v3",
                    replay_schema_version="momentum_v2_replay_v1",
                    payload={"feature_vector": _json_vector(vector)},
                    event_hash=feature_id,
                    replay_readiness="FULL",
                    blockers=[],
                ),
                _source_event(
                    event_id=f"governance-book-entry-{index}",
                    market_ticker=market,
                    event_type="ORDERBOOK",
                    event_time=feature_at + timedelta(milliseconds=600),
                    source_table="orderbook_snapshots",
                    payload=_book_payload(ask="0.60", bid="0.58"),
                ),
                _source_event(
                    event_id=f"governance-lifecycle-{index}",
                    market_ticker=market,
                    event_type="MARKET_LIFECYCLE",
                    event_time=opened_at + timedelta(minutes=15),
                    source_table="market_lifecycle",
                    payload={"status": "settled"},
                ),
            )
        )
        if candidate_market:
            management = dict(vector) if marginal else replayable_feature_vector()
            management["timing_tier"] = vector["timing_tier"]
            events.extend(
                (
                    _source_event(
                        event_id=f"governance-book-mark-{index}",
                        market_ticker=market,
                        event_type="ORDERBOOK",
                        event_time=feature_at + timedelta(milliseconds=4800),
                        source_table="orderbook_snapshots",
                        payload=_book_payload(ask="0.60", bid="0.71"),
                    ),
                    ResearchReplayEvent(
                        event_id=f"governance-manage-{index}",
                        market_ticker=market,
                        event_type="FEATURE_SNAPSHOT",
                        event_time=feature_at + timedelta(milliseconds=5000),
                        received_at=feature_at + timedelta(milliseconds=5000),
                        source_table="strategy_feature_snapshots",
                        source_row_id=f"governance-manage-{index}",
                        source_hash=f"governance-manage-{index}",
                        feature_snapshot_id=f"governance-manage-{index}",
                        feature_schema_version="momentum_v2_features_v3",
                        architecture_version="momentum_v2_heuristic_v3",
                        replay_schema_version="momentum_v2_replay_v1",
                        payload={"feature_vector": _json_vector(management)},
                        event_hash=f"governance-manage-{index}",
                        replay_readiness="FULL",
                        blockers=[],
                    ),
                    _source_event(
                        event_id=f"governance-book-exit-{index}",
                        market_ticker=market,
                        event_type="ORDERBOOK",
                        event_time=feature_at + timedelta(milliseconds=5600),
                        source_table="orderbook_snapshots",
                        payload=_book_payload(ask="0.60", bid="0.71"),
                    ),
                )
            )
        expectations[market] = {
            "candidate_market": candidate_market,
            "marginal_candidate_market": marginal,
        }
    return ResearchFixtureDataset(tuple(events), tuple(outcomes), expectations)


def _scenario_plan(scenario: str) -> dict[str, Any]:
    """Return executable books and feature values for one causal replay case."""
    entry = replayable_feature_vector()
    books: list[tuple[int, dict[str, Any]]] = [_book(600, ask="0.60", bid="0.58")]
    management_vectors: list[tuple[int, dict[str, Any]]] = []
    expectation: dict[str, Any] = {"decision_state": "DRY_RUN_ENTRY_SIGNAL"}
    readiness = "FULL"
    outcome_status = "RESOLVED"

    if scenario == "boundary_cross_research_only":
        entry["candidate_mode"] = "BOUNDARY_CROSS_HOLD"
        books = []
        expectation.update(
            decision_state="V2_HARD_GATE_BLOCKED",
            decision_reason="v2_candidate_mode_not_enabled",
            trade_statuses=[],
        )
    elif scenario == "stale_prerequisites":
        entry["quality_state"] = {
            "market_ready": True,
            "reference_ready": False,
            "book_ready": True,
        }
        books = []
        expectation.update(
            decision_state="V2_FEATURES_NOT_READY",
            decision_reason="v2_prerequisite_data_missing_or_stale",
            trade_statuses=[],
        )
    elif scenario == "entry_no_fill":
        books = [_book(600, ask="0.90", bid="0.58"), _book(900, ask="0.60", bid="0.58")]
        expectation["trade_statuses"] = ["ENTRY_NO_FILL"]
    elif scenario == "entry_expiry":
        books = [_book(2700, ask="0.60", bid="0.58")]
        expectation["trade_statuses"] = ["ENTRY_EXPIRED"]
    elif scenario == "later_book_non_rescue":
        books = [_book(600, ask="0.90", bid="0.58"), _book(900, ask="0.60", bid="0.58")]
        expectation.update(trade_statuses=["ENTRY_NO_FILL"], later_book_cannot_rescue=True)
    elif scenario == "partial_coverage_frozen_holdout":
        readiness = "PARTIAL"
        books = []
        expectation.update(decision_state=None, trade_statuses=[], partial_coverage=True)
    elif scenario == "exit_retry_exhaustion":
        # Three real feature-triggered exit attempts all receive a thin/unusable bid.
        for _index, offset in enumerate((5000, 8000, 11000), start=1):
            trigger = replayable_feature_vector()
            trigger["return_5s"] = Decimal("-2")
            books.extend(
                [
                    _book(offset - 200, ask="0.60", bid="0.58"),
                    _book(offset + 600, ask="0.60", bid="0.01", bid_size="0"),
                ]
            )
            management_vectors.append((offset, trigger))
        outcome_status = "UNRESOLVED"
        expectation.update(trade_statuses=[], exit_intents=3, position_open_after_exhaustion=True)
    else:
        trigger = replayable_feature_vector()
        trigger_offset = 5000
        mark_bid = "0.58"
        exit_bid = "0.58"
        reason = "SETTLEMENT"
        if scenario == "profit_target":
            mark_bid = exit_bid = "0.71"
            reason = "v2_profit_target"
        elif scenario == "hard_stop":
            mark_bid = exit_bid = "0.45"
            reason = "v2_hard_loss"
        elif scenario == "soft_stop_weakening":
            mark_bid = exit_bid = "0.51"
            trigger["return_5s"] = Decimal("-1")
            reason = "v2_soft_loss_with_weakening"
        elif scenario == "time_stop":
            trigger_offset = 31000
            trigger["return_5s"] = Decimal("0")
            trigger["return_15s"] = Decimal("0")
            trigger["return_30s"] = Decimal("0")
            trigger["impulse_hold_seconds"] = 0
            trigger["fast_impulse_active"] = True
            reason = "v2_tier_time_stop"
        elif scenario == "maximum_hold":
            trigger_offset = 61000
            reason = "v2_absolute_max_hold"
        elif scenario == "final_twenty_second_force_exit":
            trigger["seconds_left"] = 20
            reason = "v2_final_twenty_seconds"
        elif scenario == "adverse_boundary_cross":
            trigger["current_brti"] = Decimal("61980")
            reason = "v2_adverse_boundary_cross"
        elif scenario == "reversal_beyond_origin":
            trigger["reversal_beyond_origin"] = True
            reason = "v2_reversal_beyond_impulse_origin"
        elif scenario == "persistent_adverse_microstructure":
            trigger["persistent_adverse_microstructure"] = True
            reason = "v2_persistent_adverse_microstructure"
        elif scenario == "exit_retry_success":
            mark_bid = "0.71"
            books = [
                _book(600, ask="0.60", bid="0.58"),
                _book(4800, ask="0.60", bid="0.71"),
                _book(5600, ask="0.60", bid="0.01", bid_size="0"),
                _book(6800, ask="0.60", bid="0.71"),
                _book(7600, ask="0.60", bid="0.71"),
            ]
            management_vectors.extend(
                [(5000, trigger), (7000, replayable_feature_vector())]
            )
            expectation.update(
                trade_statuses=["CLOSED"],
                exit_intents=2,
                exit_reason="v2_profit_target",
            )
            return {
                "entry_vector": entry,
                "management_vectors": management_vectors,
                "books": books,
                "readiness": readiness,
                "outcome_status": outcome_status,
                "expectation": expectation,
            }
        elif scenario == "continuation_entry":
            # No in-window trigger: the resolved official outcome settles the fill.
            expectation.update(
                trade_statuses=["CLOSED"],
                exit_reason="SETTLEMENT",
                official_settlement=True,
            )
            return {
                "entry_vector": entry,
                "management_vectors": [],
                "books": books,
                "readiness": readiness,
                "outcome_status": outcome_status,
                "expectation": expectation,
            }
        else:
            raise ValueError(f"Unsupported fixture scenario: {scenario}")
        books.extend(
            [
                _book(trigger_offset - 200, ask="0.60", bid=mark_bid),
                _book(trigger_offset + 600, ask="0.60", bid=exit_bid),
            ]
        )
        management_vectors.append((trigger_offset, trigger))
        expectation.update(trade_statuses=["CLOSED"], exit_reason=reason)

    return {
        "entry_vector": entry,
        "management_vectors": management_vectors,
        "books": books,
        "readiness": readiness,
        "outcome_status": outcome_status,
        "expectation": expectation,
    }


def _book(
    offset_ms: int,
    *,
    ask: str,
    bid: str,
    bid_size: str = "3",
) -> tuple[int, dict[str, Any]]:
    return (
        offset_ms,
        _book_payload(ask=ask, bid=bid, bid_size=bid_size),
    )


def _book_payload(*, ask: str, bid: str, bid_size: str = "3") -> dict[str, Any]:
    return {
        "yes_ask": ask,
        "yes_bid": bid,
        "yes_ask_size": "3",
        "yes_bid_size": bid_size,
    }


def _source_event(
    *,
    event_id: str,
    market_ticker: str,
    event_type: str,
    event_time: datetime,
    source_table: str,
    payload: dict[str, Any],
) -> ResearchReplayEvent:
    return ResearchReplayEvent(
        event_id=event_id,
        market_ticker=market_ticker,
        event_type=event_type,
        event_time=event_time,
        received_at=event_time,
        source_table=source_table,
        source_row_id=event_id,
        source_hash=event_id,
        replay_schema_version="momentum_v2_replay_v1",
        payload=payload,
        event_hash=event_id,
        replay_readiness="FULL",
        blockers=[],
    )


def _json_vector(vector: dict[str, Any]) -> dict[str, Any]:
    return {
        key: str(value) if isinstance(value, Decimal) else value for key, value in vector.items()
    }
