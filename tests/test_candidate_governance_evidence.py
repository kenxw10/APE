from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from ape.research.repository import (
    _CANDIDATE_TUNABLE_PATHS,
    _config_diff_evidence,
    _eligible_feature_coverage,
    _governance_trade_evidence,
)
from ape.strategy.momentum_v2 import V2_PARAMETERS

EXPECTED_TUNABLE_PATHS = {
    "edge_threshold_cents",
    "tiers.early.score",
    "tiers.early.max_ask",
    "tiers.early.time_stop",
    "tiers.early.max_hold",
    "tiers.normal.score",
    "tiers.normal.max_ask",
    "tiers.normal.time_stop",
    "tiers.normal.max_hold",
    "tiers.late.score",
    "tiers.late.max_ask",
    "tiers.late.time_stop",
    "tiers.late.max_hold",
    "logistic_model",
    "logistic_probability_threshold",
    "calibration_overrides.fast_15",
    "calibration_overrides.fast_30",
    "calibration_overrides.adverse_5",
    "calibration_overrides.retrace",
    "calibration_overrides.crosses",
    "calibration_overrides.edge",
    "calibration_overrides.early",
    "calibration_overrides.normal",
    "calibration_overrides.late",
    "calibration_overrides.early_max_ask",
    "calibration_overrides.normal_max_ask",
    "calibration_overrides.late_max_ask",
    "calibration_overrides.time_stop",
    "calibration_overrides.max_hold",
    "calibration_overrides.profit_target",
    "calibration_overrides.soft_stop",
    "calibration_overrides.hard_stop",
    "calibration_overrides.component_weight_multipliers.fast_impulse",
    "calibration_overrides.component_weight_multipliers.path_quality",
    "calibration_overrides.component_weight_multipliers.underreaction",
    "calibration_overrides.component_weight_multipliers.boundary_regime",
    "calibration_overrides.component_weight_multipliers.microstructure",
    "calibration_overrides.component_weight_multipliers.timing_economics",
}


def test_candidate_allowlist_exactly_matches_the_approved_search_space() -> None:
    assert _CANDIDATE_TUNABLE_PATHS == EXPECTED_TUNABLE_PATHS


@pytest.mark.parametrize("path", sorted(EXPECTED_TUNABLE_PATHS))
def test_every_approved_candidate_parameter_path_is_permitted(path: str) -> None:
    candidate = deepcopy(V2_PARAMETERS)
    value = (
        {"checksum": "fixture", "feature_columns": []}
        if path == "logistic_model"
        else _changed_value(_value_at(candidate, path))
    )
    _set_path(candidate, path, value)

    evidence = _config_diff_evidence(V2_PARAMETERS, candidate)

    assert path in evidence["changed_parameter_paths"]
    assert path in evidence["allowed_changed_parameter_paths"]
    assert evidence["forbidden_parameter_changed"] is False
    assert evidence["safety_or_data_quality_gate_changed"] is False


def test_unknown_nested_calibration_override_is_forbidden() -> None:
    candidate = deepcopy(V2_PARAMETERS)
    _set_path(candidate, "calibration_overrides.unknown.nested", "1")

    evidence = _config_diff_evidence(V2_PARAMETERS, candidate)

    assert "calibration_overrides.unknown.nested" in evidence["forbidden_changed_parameter_paths"]
    assert evidence["forbidden_parameter_changed"] is True


def test_added_empty_nested_calibration_dict_is_forbidden() -> None:
    candidate = deepcopy(V2_PARAMETERS)
    _set_path(candidate, "calibration_overrides.unapproved", {})

    evidence = _config_diff_evidence(V2_PARAMETERS, candidate)

    assert "calibration_overrides.unapproved" in evidence["forbidden_changed_parameter_paths"]


def test_removed_protected_value_is_reported_independently() -> None:
    candidate = deepcopy(V2_PARAMETERS)
    del candidate["decision_to_book_latency_ms"]

    evidence = _config_diff_evidence(V2_PARAMETERS, candidate)

    assert "decision_to_book_latency_ms" in evidence["changed_protected_gate_paths"]
    assert evidence["forbidden_parameter_changed"] is True
    assert evidence["safety_or_data_quality_gate_changed"] is True


def test_type_only_change_is_detected_without_reclassifying_an_approved_path() -> None:
    candidate = deepcopy(V2_PARAMETERS)
    candidate["tiers"]["early"]["score"] = "80"

    evidence = _config_diff_evidence(V2_PARAMETERS, candidate)

    assert evidence["changed_parameter_paths"] == ["tiers.early.score"]
    assert evidence["forbidden_parameter_changed"] is False


def test_hidden_safety_gate_inside_calibration_metadata_is_rejected() -> None:
    candidate = deepcopy(V2_PARAMETERS)
    _set_path(candidate, "calibration_overrides.maximum_spread", "99")

    evidence = _config_diff_evidence(V2_PARAMETERS, candidate)

    assert "calibration_overrides.maximum_spread" in evidence["forbidden_changed_parameter_paths"]
    assert "calibration_overrides.maximum_spread" in evidence["changed_protected_gate_paths"]
    assert evidence["forbidden_parameter_changed"] is True
    assert evidence["safety_or_data_quality_gate_changed"] is True


def test_feature_coverage_counts_only_eligible_feature_snapshots_and_real_gaps() -> None:
    at = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)
    feature = _event(
        event_id="feature",
        event_type="FEATURE_SNAPSHOT",
        event_time=at,
        source_table="strategy_feature_snapshots",
        payload={"feature_vector": {"candidate_side": "YES"}},
        feature_snapshot_id="feature",
    )
    events = [
        _event("market", "MARKET", at, "markets"),
        _event(
            "reference",
            "REFERENCE",
            at + timedelta(milliseconds=100),
            "reference_ticks",
        ),
        _event(
            "lifecycle",
            "MARKET_LIFECYCLE",
            at + timedelta(milliseconds=200),
            "market_lifecycle",
        ),
        feature,
        _event(
            "book",
            "ORDERBOOK",
            at + timedelta(milliseconds=600),
            "orderbook_snapshots",
        ),
        _event("trade-first", "PUBLIC_TRADE", at, "public_trades"),
        _event("trade-later", "PUBLIC_TRADE", at + timedelta(seconds=60), "public_trades"),
    ]
    outcome = SimpleNamespace(
        market_ticker="M1",
        outcome_status="RESOLVED",
        quality_flags={"counterfactual_labels": {"feature": {"net_markout_30s_cents": "1"}}},
    )

    evidence = _eligible_feature_coverage(events, [outcome])

    assert evidence["total_feature_snapshot_events"] == 1
    assert evidence["full_feature_frames"] == 1
    assert evidence["complete_eligible_unique_markets"] == 1
    assert evidence["missing_source_counts"] == {"M1": 0}
    assert evidence["overall_maximum_event_gap"] == 60
    assert evidence["gap_related_blockers"] == ["maximum_event_gap_exceeds_30_seconds"]


def test_governance_trade_evidence_uses_declared_partitions_and_deduplicates() -> None:
    def closed(trade_id, partition, market, decision) -> SimpleNamespace:
        return SimpleNamespace(
            status="CLOSED",
            candidate_id="candidate",
            market_ticker=market,
            trade_id=trade_id,
            entry_feature_snapshot_id=None,
            entry_fill_event_id="entry",
            measurements={
                "evidence_partition": partition,
                "source_decision_id": decision,
            },
        )

    trades = [
        closed("holdout-one", "frozen_holdout", "M1", "D1"),
        closed("holdout-duplicate", "frozen_holdout", "M1", "D1"),
        closed("development", "search_development", "M2", "D2"),
    ]
    metrics = {"holdout": {"closed_trade_count": 1}}

    evidence = _governance_trade_evidence(
        trades,
        metrics,
        {"governance_trade_partitions": ["frozen_holdout"]},
    )

    assert evidence["unique_closed_governance_trades"] == 1
    assert evidence["excluded_duplicate_rows"] == 1
    assert evidence["partition_specific_closed_counts"] == {"frozen_holdout": 1}
    assert evidence["reported_partition_trade_integrity"] is True


def _value_at(values: dict[str, Any], path: str) -> Any:
    current: Any = values
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _set_path(values: dict[str, Any], path: str, value: Any) -> None:
    current = values
    parts = path.split(".")
    for part in parts[:-1]:
        current = current.setdefault(part, {})
    current[parts[-1]] = value


def _changed_value(value: Any) -> Any:
    if value is None:
        return "fixture"
    if isinstance(value, int):
        return value + 1
    if isinstance(value, str):
        return value + "-fixture"
    if isinstance(value, dict):
        return {"checksum": "fixture"}
    return "fixture"


def _event(
    event_id: str,
    event_type: str,
    event_time: datetime,
    source_table: str,
    *,
    payload: dict[str, Any] | None = None,
    feature_snapshot_id: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        event_id=event_id,
        event_type=event_type,
        event_time=event_time,
        source_table=source_table,
        market_ticker="M1",
        payload=payload or {},
        feature_snapshot_id=feature_snapshot_id,
        replay_readiness="FULL",
    )
