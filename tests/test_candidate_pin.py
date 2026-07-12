from __future__ import annotations

import hashlib
import json

from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.inputs import StrategyConfigVersionInput
from ape.repositories.strategy_v2 import StrategyV2Repository
from ape.research.pin import resolve_pinned_candidate
from ape.research.repository import ResearchRepository
from ape.strategy.momentum_v2 import (
    REPLAY_SCHEMA_VERSION,
    V2_ARCHITECTURE_VERSION,
    V2_FEATURE_SCHEMA_VERSION,
    V2_PARAMETERS,
)


def test_candidate_pin_requires_immutable_approved_checksum_valid_candidate(tmp_path) -> None:
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'pin.sqlite'}",
            "STRATEGY_V2_CANDIDATE_CONFIG_VERSION_ID": "config-pin",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    artifact = {"coefficients": [1.0]}
    checksum = hashlib.sha256(
        json.dumps(artifact, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    try:
        with factory() as session:
            StrategyV2Repository(session).ensure_config_version(
                StrategyConfigVersionInput(
                    strategy_config_version_id="config-pin",
                    strategy_id="btc15_momentum_v2_candidate_123456789012",
                    architecture_version=V2_ARCHITECTURE_VERSION,
                    feature_schema_version=V2_FEATURE_SCHEMA_VERSION,
                    parameter_snapshot=V2_PARAMETERS,
                    parameter_hash="parameters",
                    code_commit_sha="fixture",
                    source="RESEARCH",
                    lifecycle_state="DRY_RUN_CHALLENGER",
                    candidate_id="candidate-pin",
                )
            )
            ResearchRepository(session).create_candidate(
                {
                    "candidate_id": "candidate-pin",
                    "strategy_config_version_id": "config-pin",
                    "calibration_run_id": "run",
                    "parent_strategy_config_version_id": None,
                    "generated_strategy_id": "btc15_momentum_v2_candidate_123456789012",
                    "architecture_version": V2_ARCHITECTURE_VERSION,
                    "feature_schema_version": V2_FEATURE_SCHEMA_VERSION,
                    "replay_schema_version": REPLAY_SCHEMA_VERSION,
                    "model_type": "WEIGHTED_HEURISTIC",
                    "parameter_snapshot": V2_PARAMETERS,
                    "feature_columns": [],
                    "model_artifact": artifact,
                    "model_artifact_checksum": checksum,
                    "training_metrics": None,
                    "validation_metrics": None,
                    "test_metrics": None,
                    "holdout_metrics": None,
                    "governance_report": None,
                    "lifecycle_state": "DRY_RUN_CHALLENGER",
                    "eligibility_status": "APPROVED",
                }
            )
            session.commit()
        with factory() as session:
            pinned, blocker = resolve_pinned_candidate(config, session)
            assert blocker is None
            assert pinned is not None
            assert pinned.strategy_id.endswith("123456789012")
    finally:
        engine.dispose()


def test_invalid_candidate_pin_fails_closed_without_a_baseline_substitute(tmp_path) -> None:
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'invalid-pin.sqlite'}",
            "STRATEGY_V2_CANDIDATE_CONFIG_VERSION_ID": "missing-config",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    try:
        with factory() as session:
            pinned, blocker = resolve_pinned_candidate(config, session)
            assert pinned is None
            assert blocker == "candidate_pin_missing"
    finally:
        engine.dispose()
