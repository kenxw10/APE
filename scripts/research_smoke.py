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
    StrategyFeatureSnapshot,
)
from ape.research.archive import archive_research_events, reconcile_market_outcomes
from ape.research.calibration import (
    LIFECYCLE_DRAFT,
    LIFECYCLE_PAPER_CANDIDATE,
    GovernanceError,
    bounded_candidate_specs,
    build_partition_manifest,
    market_bootstrap,
    run_bounded_calibration,
)
from ape.research.fixtures import (
    fixture_time,
    replayable_feature_vector,
    synthetic_btc15_fixture_dataset,
)
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
                    market_ticker="KXBTC15M-SMOKE-ARCHIVE",
                    series_ticker="KXBTC15M",
                    open_time=at - timedelta(minutes=5),
                    close_time=at + timedelta(minutes=10),
                    expiration_time=at + timedelta(minutes=10),
                    functional_strike=Decimal("62000"),
                ),
                StrategyFeatureSnapshot(
                    feature_snapshot_id="smoke-feature",
                    market_ticker="KXBTC15M-SMOKE-ARCHIVE",
                    evaluated_at=at,
                    feature_schema_version="momentum_v2_features_v3",
                    context_hash="smoke",
                    candidate_side="YES",
                    boundary=Decimal("62000"),
                    complete_feature_vector={
                        key: str(value) if isinstance(value, Decimal) else value
                        for key, value in replayable_feature_vector().items()
                    },
                    replay_readiness="FULL",
                    replay_blockers=[],
                ),
                OrderbookSnapshot(
                    market_ticker="KXBTC15M-SMOKE-ARCHIVE",
                    received_at=at + timedelta(milliseconds=600),
                    yes_ask=Decimal("0.60"),
                    yes_bid=Decimal("0.58"),
                    yes_ask_count=Decimal("1"),
                    yes_bid_count=Decimal("1"),
                ),
                OrderbookSnapshot(
                    market_ticker="KXBTC15M-SMOKE-ARCHIVE",
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
        initial_archive = archive_research_events(session, now=at + timedelta(minutes=11))

        class PublicOutcomeClient:
            def get_market(self, _market_ticker: str) -> dict[str, object]:
                return {
                    "market": {
                        "result": "yes",
                        "status": "settled",
                        "settlement_value": "62010",
                    }
                }

        reconciled = reconcile_market_outcomes(
            session,
            client=PublicOutcomeClient(),
            now=at + timedelta(minutes=11),
        )
        archive = archive_research_events(session, now=at + timedelta(minutes=11))
        archived_outcome = session.scalar(
            select(ResearchMarketOutcome).where(
                ResearchMarketOutcome.market_ticker == "KXBTC15M-SMOKE-ARCHIVE"
            )
        )
        fixture = synthetic_btc15_fixture_dataset(50)
        baseline = DeterministicReplayEngine().replay(
            list(fixture.events), outcomes=list(fixture.outcomes)
        )
        all_candidates = bounded_candidate_specs("smoke")
        smoke_candidates = (
            all_candidates[0],
            next(
                candidate
                for candidate in all_candidates
                if candidate.model_type == "WEIGHTED_HEURISTIC"
            ),
            next(
                candidate
                for candidate in all_candidates
                if candidate.model_type == "L2_LOGISTIC"
            ),
        )
        calibration = run_bounded_calibration(
            calibration_run_id="smoke",
            events=list(fixture.events),
            outcomes=list(fixture.outcomes),
            candidate_specs=smoke_candidates,
        )
        manifest = build_partition_manifest(fixture.outcomes)
        bootstrap = market_bootstrap({"M1": Decimal("1"), "M2": Decimal("-1")}, "smoke")
        paper_live_failed = []
        for target in (LIFECYCLE_PAPER_CANDIDATE, "LIVE_CANDIDATE"):
            try:
                from ape.research.calibration import transition_candidate

                transition_candidate(from_state=LIFECYCLE_DRAFT, to_state=target, evidence={})
            except GovernanceError:
                paper_live_failed.append(target)
        payload = {
            "archive": {
                "archived_events": initial_archive.archived_events + archive.archived_events,
                "event_counts_by_type": archive.coverage["event_counts_by_type"],
                "complete_markets": archive.coverage["complete_markets"],
            },
            "labels": _label_summary(archived_outcome),
            "baseline_replay": {
                "decision_states": baseline.zero_entry_report["decision_states"],
                "pipeline": baseline.zero_entry_report["pipeline"],
                "frequency_classification": baseline.zero_entry_report[
                    "frequency_classification"
                ],
            },
            "official_outcomes_reconciled": reconciled,
            "fixture_market_count": len(fixture.outcomes),
            "fixture_event_count": len(fixture.events),
            "candidate_partitions": {
                candidate_id: sorted(partitions)
                for candidate_id, partitions in (
                    calibration.candidate_partition_replay_trades.items()
                )
            },
            "calibration_status": calibration.status,
            "smoke_candidate_count": len(smoke_candidates),
            "partition_manifest_hash": manifest["manifest_hash"],
            "bootstrap": bootstrap,
            "governance_transition": transition_candidate(
                from_state=LIFECYCLE_DRAFT,
                to_state="BACKTESTED",
                evidence={"source": "fixture-driven-smoke"},
            )[0],
            "paper_live_transitions_failed": paper_live_failed,
        }
        print(json.dumps(payload, sort_keys=True, default=str, indent=2))
    engine.dispose()
    return 0


def _label_summary(outcome: ResearchMarketOutcome | None) -> dict[str, object]:
    flags = outcome.quality_flags if outcome is not None else {}
    labels = flags.get("counterfactual_labels", {}) if isinstance(flags, dict) else {}
    values = list(labels.values()) if isinstance(labels, dict) else []
    return {
        "label_count": len(values),
        "entry_label_ready": all(value.get("entry_label_readiness") == "FULL" for value in values),
        "official_settlement_ready": all(
            value.get("settlement_label_readiness") == "FULL" for value in values
        ),
    }


if __name__ == "__main__":
    raise SystemExit(main())
