from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest
from tests.test_research_helpers import at_base

import ape.research.calibration as calibration
from ape.db.models import ResearchMarketOutcome
from ape.research.calibration import (
    LIFECYCLE_BACKTESTED,
    LIFECYCLE_DRAFT,
    LIFECYCLE_PAPER_CANDIDATE,
    CandidateSpec,
    GovernanceError,
    bounded_candidate_specs,
    build_partition_manifest,
    fit_l2_logistic,
    market_bootstrap,
    transition_candidate,
)


def _outcomes(count: int) -> list[ResearchMarketOutcome]:
    at = at_base()
    return [
        ResearchMarketOutcome(
            outcome_id=f"outcome-{index}",
            market_ticker=f"M{index}",
            market_open_at=at + timedelta(minutes=15 * index),
            market_close_at=at + timedelta(minutes=15 * (index + 1)),
            expiration_at=at,
            boundary=Decimal("1"),
            result_side="YES",
            settlement_value=Decimal("1"),
            final_reference_value=Decimal("1"),
            final_minute_reference_average=Decimal("1"),
            outcome_status="RESOLVED",
            outcome_source="fixture",
            source_payload_hash="x",
            resolved_at=at,
            expected_frame_count=1,
            actual_frame_count=1,
            coverage_percentage=Decimal("1"),
            maximum_event_gap_seconds=1,
            quality_flags={},
        )
        for index in range(count)
    ]


def test_market_partitions_keep_markets_whole_and_freeze_holdout() -> None:
    manifest = build_partition_manifest(_outcomes(50))
    assert set(manifest["development"]).isdisjoint(manifest["holdout"])
    assert len(manifest["holdout"]) == 10
    assert all(set(fold["train"]).isdisjoint(fold["validation"]) for fold in manifest["folds"])


def test_search_is_bounded_and_deterministic() -> None:
    first = bounded_candidate_specs("calibration-1")
    second = bounded_candidate_specs("calibration-1")
    assert len(first) == 256
    assert [candidate.candidate_id for candidate in first] == [
        candidate.candidate_id for candidate in second
    ]
    assert first[0].model_type == "BASELINE"


def test_logistic_is_reproducible_and_training_only() -> None:
    rows = [{"return_5s": index, "timing_tier": "normal"} for index in range(8)]
    first = fit_l2_logistic(rows, [0, 0, 0, 0, 1, 1, 1, 1], l2=1.0)
    second = fit_l2_logistic(rows, [0, 0, 0, 0, 1, 1, 1, 1], l2=1.0)
    assert first["checksum"] == second["checksum"]
    assert first["iterations"] <= 500


def test_bootstrap_uses_exactly_two_thousand_deterministic_resamples() -> None:
    first = market_bootstrap({"M1": Decimal("1"), "M2": Decimal("-1")}, "run")
    assert first == market_bootstrap({"M1": Decimal("1"), "M2": Decimal("-1")}, "run")
    assert first["resamples"] == "2000"


def test_walk_forward_metrics_preserves_manifest_validation_order(monkeypatch) -> None:
    captured: list[tuple[str, ...]] = []

    class FakeReplayEngine:
        def __init__(self, *, parameters) -> None:
            del parameters

        def replay(self, *_args, **_kwargs):
            return type("Replay", (), {"trades": ()})()

    def fake_metrics(*_args, market_tickers, **_kwargs):
        captured.append(tuple(market_tickers))
        return {"net_pnl_per_market": "0"}

    monkeypatch.setattr(calibration, "DeterministicReplayEngine", FakeReplayEngine)
    monkeypatch.setattr(calibration, "replay_metrics", fake_metrics)

    calibration._walk_forward_metrics(
        CandidateSpec("candidate", "strategy", "BASELINE", {}),
        {"folds": [{"fold": 1, "train": ["M2", "M1"], "validation": ["M4", "M3"]}]},
        [],
        [],
        "run",
    )

    assert captured == [("M4", "M3")]


def test_governance_rejects_paper_live_transitions() -> None:
    with pytest.raises(GovernanceError):
        transition_candidate(
            from_state=LIFECYCLE_DRAFT, to_state=LIFECYCLE_PAPER_CANDIDATE, evidence={}
        )
    assert (
        transition_candidate(
            from_state=LIFECYCLE_DRAFT, to_state=LIFECYCLE_BACKTESTED, evidence={}
        )[0]
        == LIFECYCLE_BACKTESTED
    )
