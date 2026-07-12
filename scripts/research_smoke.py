from __future__ import annotations

import json
from datetime import timedelta
from decimal import Decimal

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from ape.db.models import (
    Base,
    Market,
    OrderbookSnapshot,
    ReferenceTick,
    ResearchMarketOutcome,
    ResearchReplayEvent,
    StrategyFeatureSnapshot,
)
from ape.research.archive import archive_research_events
from ape.research.calibration import (
    LIFECYCLE_DRAFT,
    LIFECYCLE_PAPER_CANDIDATE,
    LIFECYCLE_SHADOW,
    GovernanceError,
    bounded_candidate_specs,
    build_partition_manifest,
    market_bootstrap,
    run_bounded_calibration,
    transition_candidate,
)
from ape.research.fixtures import fixture_event, fixture_time, replayable_feature_vector
from ape.research.replay import DeterministicReplayEngine


def main() -> int:
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    with session_factory() as session:
        at = fixture_time()
        session.add_all(
            [
                Market(
                    market_ticker="SMOKE-ARCHIVE",
                    open_time=at - timedelta(minutes=5),
                    close_time=at + timedelta(minutes=10),
                    expiration_time=at + timedelta(minutes=10),
                    functional_strike=Decimal("62000"),
                ),
                StrategyFeatureSnapshot(
                    feature_snapshot_id="smoke-feature",
                    market_ticker="SMOKE-ARCHIVE",
                    evaluated_at=at,
                    feature_schema_version="momentum_v2_features_v3",
                    context_hash="smoke",
                    candidate_side="YES",
                    boundary=Decimal("62000"),
                    complete_feature_vector={"candidate_side": "YES", "boundary": "62000"},
                    replay_readiness="FULL",
                    replay_blockers=[],
                ),
                OrderbookSnapshot(
                    market_ticker="SMOKE-ARCHIVE",
                    received_at=at + timedelta(milliseconds=600),
                    yes_ask=Decimal("0.60"),
                    yes_bid=Decimal("0.58"),
                    yes_ask_count=Decimal("1"),
                    yes_bid_count=Decimal("1"),
                ),
                OrderbookSnapshot(
                    market_ticker="SMOKE-ARCHIVE",
                    received_at=at + timedelta(seconds=5),
                    yes_bid=Decimal("0.65"),
                    yes_bid_count=Decimal("1"),
                ),
                ReferenceTick(
                    source="kalshi_cfbenchmarks_brti",
                    received_at=at + timedelta(seconds=5),
                    source_ts=at + timedelta(seconds=5),
                    parsed_value=Decimal("62010"),
                    parse_status="valid",
                ),
            ]
        )
        session.flush()
        archive = archive_research_events(session, now=at + timedelta(minutes=11))
        archived_outcome = session.scalar(
            select(ResearchMarketOutcome).where(
                ResearchMarketOutcome.market_ticker == "SMOKE-ARCHIVE"
            )
        )
        baseline_vector = replayable_feature_vector()
        baseline_vector["candidate_mode"] = "BOUNDARY_CROSS_HOLD"
        baseline = DeterministicReplayEngine().replay(
            [_feature_event(at=fixture_time(), vector=baseline_vector)]
        )
        candidate = DeterministicReplayEngine().replay(
            [
                fixture_event(at=fixture_time()),
                _orderbook_event(at=fixture_time() + timedelta(milliseconds=600), event_id="entry"),
                _orderbook_event(at=fixture_time() + timedelta(seconds=61), event_id="exit"),
            ]
        )
        under_sampled = run_bounded_calibration(calibration_run_id="smoke", events=[], outcomes=[])
        outcomes = [
            ResearchMarketOutcome(
                outcome_id=f"outcome-{index}",
                market_ticker=f"M{index}",
                market_open_at=fixture_time() + timedelta(minutes=15 * index),
                market_close_at=fixture_time() + timedelta(minutes=15 * (index + 1)),
                expiration_at=fixture_time(),
                boundary=Decimal("1"),
                result_side="YES",
                settlement_value=Decimal("1"),
                final_reference_value=Decimal("1"),
                final_minute_reference_average=Decimal("1"),
                outcome_status="RESOLVED",
                outcome_source="fixture",
                source_payload_hash="fixture",
                resolved_at=fixture_time(),
                expected_frame_count=1,
                actual_frame_count=1,
                coverage_percentage=Decimal("1"),
                maximum_event_gap_seconds=1,
                quality_flags={},
            )
            for index in range(50)
        ]
        manifest = build_partition_manifest(outcomes)
        candidates = bounded_candidate_specs("smoke")
        bootstrap = market_bootstrap({"M1": Decimal("1"), "M2": Decimal("-1")}, "smoke")
        evidence = {
            "complete_unique_markets": 500,
            "closed_simulated_trades": 50,
            "entry_frequency_per_100_markets_min": 1,
            "signal_to_fill_rate": "0.50",
            "complete_replay_coverage": "0.95",
            "volatility_regimes": 2,
            "liquidity_regimes": 2,
            "timing_tiers": 2,
            "holdout_mean_net_pnl_per_market": "0.01",
            "holdout_lower_95": "0.01",
            "adjusted_lower_confidence_expectancy": "0.01",
            "entry_frequency_per_100_markets": 2,
            "dominant_regime_entry_share": "0.5",
            "max_drawdown_per_100_markets": 10,
            "verified_fee_model": True,
            "beats_baseline": True,
        }
        promoted = transition_candidate(
            from_state=LIFECYCLE_SHADOW,
            to_state="DRY_RUN_CHALLENGER",
            evidence=evidence,
        )[0]
        paper_live_failed = []
        for target in (LIFECYCLE_PAPER_CANDIDATE, "LIVE_CANDIDATE"):
            try:
                transition_candidate(from_state=LIFECYCLE_DRAFT, to_state=target, evidence={})
            except GovernanceError:
                paper_live_failed.append(target)
        payload = {
            "archive": archive.coverage,
            "labels": (
                archived_outcome.quality_flags.get("counterfactual_labels", {})
                if archived_outcome is not None
                else {}
            ),
            "baseline_replay": baseline.zero_entry_report,
            "candidate_closed_trades": len(
                [trade for trade in candidate.trades if trade.status == "CLOSED"]
            ),
            "under_sampled_calibration": under_sampled.status,
            "bounded_candidate_count": len(candidates),
            "partition_manifest_hash": manifest["manifest_hash"],
            "bootstrap": bootstrap,
            "governance_promoted_to": promoted,
            "paper_live_transitions_failed": paper_live_failed,
        }
        print(json.dumps(payload, sort_keys=True, default=str, indent=2))
    engine.dispose()
    return 0


def _feature_event(*, at, vector) -> ResearchReplayEvent:
    event = fixture_event(at=at)
    event.payload = {
        "feature_vector": {
            key: str(value) if isinstance(value, Decimal) else value
            for key, value in vector.items()
        }
    }
    return event


def _orderbook_event(*, at, event_id: str) -> ResearchReplayEvent:
    return ResearchReplayEvent(
        event_id=event_id,
        market_ticker="M1",
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
        payload={"yes_ask": "0.60", "yes_bid": "0.58", "yes_ask_size": "3", "yes_bid_size": "3"},
        event_hash=event_id,
        replay_readiness="FULL",
        blockers=[],
    )


if __name__ == "__main__":
    raise SystemExit(main())
