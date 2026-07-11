from __future__ import annotations

from ape.db.migrations import CURRENT_SCHEMA_VERSION
from ape.repositories.storage_retention import (
    ALLOWED_RETENTION_TABLES,
    ALLOWED_STATUS_READ_TABLES,
)
from ape.storage.retention import RETENTION_POLICIES, RETENTION_TABLE_NAMES, STATUS_TABLES
from ape.strategy import momentum_v2


def test_pr10b_versions_preserve_schema_and_revise_v2_semantics(monkeypatch) -> None:
    assert momentum_v2.V2_ARCHITECTURE_VERSION == "momentum_v2_heuristic_v3"
    assert momentum_v2.V2_LIFECYCLE_SCHEMA_VERSION == "momentum_v2_lifecycle_v2"
    assert momentum_v2.V2_FEATURE_SCHEMA_VERSION == "momentum_v2_features_v2"
    assert CURRENT_SCHEMA_VERSION == "0009_momentum_v2_scope_completion"

    monkeypatch.setattr(momentum_v2, "resolve_code_version", lambda: "pr10b-test")
    corrected = momentum_v2.built_in_config_version(
        momentum_v2.V2_STRATEGY_ID,
        momentum_v2.V2_PARAMETERS,
    )
    monkeypatch.setattr(
        momentum_v2,
        "V2_ARCHITECTURE_VERSION",
        "momentum_v2_heuristic_v2",
    )
    monkeypatch.setattr(
        momentum_v2,
        "V2_LIFECYCLE_SCHEMA_VERSION",
        "momentum_v2_lifecycle_v1",
    )
    pr10a = momentum_v2.built_in_config_version(
        momentum_v2.V2_STRATEGY_ID,
        momentum_v2.V2_PARAMETERS,
    )

    assert corrected.strategy_config_version_id != pr10a.strategy_config_version_id


def test_pr10b_outcomes_are_status_visible_but_not_retention_targets() -> None:
    outcome_table = "strategy_position_outcomes"

    assert outcome_table in {table.table_name for table in STATUS_TABLES}
    assert outcome_table not in RETENTION_TABLE_NAMES
    assert outcome_table not in {policy.table_name for policy in RETENTION_POLICIES}
    assert outcome_table not in ALLOWED_RETENTION_TABLES
    assert outcome_table in ALLOWED_STATUS_READ_TABLES
