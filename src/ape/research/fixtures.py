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


def fixture_time() -> datetime:
    return datetime(2026, 7, 11, 12, 0, tzinfo=UTC)


def replayable_feature_vector() -> dict[str, Any]:
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
    """Build complete event-time fixtures for archive, replay, and calibration tests."""
    scenarios = (
        "continuation_entry",
        "boundary_cross_research_only",
        "stale_prerequisites",
        "entry_fill",
        "entry_no_fill",
        "entry_expiry",
        "later_book_non_rescue",
        "profitable_exit",
        "losing_exit",
        "hard_stop",
        "soft_stop_weakening",
        "profit_target",
        "time_stop",
        "maximum_hold",
        "final_twenty_second_force_exit",
        "adverse_boundary_cross",
        "exit_retry_exhaustion",
        "partial_coverage_frozen_holdout",
    )
    outcomes = synthetic_btc15_fixture_markets(count)
    events: list[ResearchReplayEvent] = []
    for index, outcome in enumerate(outcomes):
        market = outcome.market_ticker
        opened_at = outcome.market_open_at or fixture_time()
        scenario = scenarios[index % len(scenarios)]
        vector = replayable_feature_vector()
        vector.update(
            {
                "timing_tier": ("early", "normal", "late")[index % 3],
                "volatility_regime": ("low", "medium", "high")[index % 3],
                "liquidity_regime": ("thin", "medium", "deep")[index % 3],
                "candidate_mode": (
                    "BOUNDARY_CROSS_HOLD"
                    if scenario == "boundary_cross_research_only"
                    else "CONTINUATION"
                ),
            }
        )
        readiness = "PARTIAL" if scenario == "partial_coverage_frozen_holdout" else "FULL"
        feature_at = opened_at + timedelta(minutes=6)
        feature_id = f"fixture-feature-{index}"
        base = f"fixture-{index}"
        events.extend(
            (
                _source_event(
                    event_id=f"{base}-market",
                    market_ticker=market,
                    event_type="MARKET",
                    event_time=opened_at,
                    source_table="markets",
                    payload={"scenario": scenario},
                ),
                _source_event(
                    event_id=f"{base}-reference",
                    market_ticker=market,
                    event_type="REFERENCE",
                    event_time=feature_at - timedelta(milliseconds=100),
                    source_table="reference_ticks",
                    payload={"value": str(Decimal("62000") + index), "scenario": scenario},
                ),
                _source_event(
                    event_id=f"{base}-trade",
                    market_ticker=market,
                    event_type="PUBLIC_TRADE",
                    event_time=feature_at + timedelta(milliseconds=100),
                    source_table="public_trades",
                    payload={"taker_side": "YES", "scenario": scenario},
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
                    payload={"feature_vector": _json_vector(vector), "scenario": scenario},
                    event_hash=feature_id,
                    replay_readiness=readiness,
                    blockers=[] if readiness == "FULL" else ["fixture_partial_coverage"],
                ),
                _source_event(
                    event_id=f"{base}-book-first",
                    market_ticker=market,
                    event_type="ORDERBOOK",
                    event_time=feature_at + timedelta(milliseconds=600),
                    source_table="orderbook_snapshots",
                    payload={
                        "yes_ask": (
                            "0.90"
                            if scenario in {"entry_no_fill", "later_book_non_rescue"}
                            else "0.60"
                        ),
                        "yes_bid": "0.58",
                        "yes_ask_size": "3",
                        "yes_bid_size": "3",
                        "scenario": scenario,
                    },
                ),
                _source_event(
                    event_id=f"{base}-book-later",
                    market_ticker=market,
                    event_type="ORDERBOOK",
                    event_time=feature_at + timedelta(milliseconds=900),
                    source_table="orderbook_snapshots",
                    payload={
                        "yes_ask": "0.60",
                        "yes_bid": "0.58",
                        "yes_ask_size": "3",
                        "yes_bid_size": "3",
                        "scenario": scenario,
                    },
                ),
                _source_event(
                    event_id=f"{base}-lifecycle",
                    market_ticker=market,
                    event_type="MARKET_LIFECYCLE",
                    event_time=opened_at + timedelta(minutes=15),
                    source_table="market_lifecycle",
                    payload={"status": "settled", "scenario": scenario},
                ),
            )
        )
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
    return ResearchFixtureDataset(tuple(events), tuple(outcomes))


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
