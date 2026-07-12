from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.inputs import StrategyConfigVersionInput, StrategyDecisionInput
from ape.repositories.strategy_v2 import StrategyV2Repository
from ape.research.pin import PinnedCandidate, resolve_pinned_candidate
from ape.research.repository import ResearchRepository
from ape.safety import assess_startup_safety
from ape.strategy import observer as observer_module
from ape.strategy.momentum_v2 import (
    REPLAY_SCHEMA_VERSION,
    V2_ARCHITECTURE_VERSION,
    V2_FEATURE_SCHEMA_VERSION,
    V2_PARAMETERS,
)
from ape.strategy.observer import DryRunLedgerResult, StrategyObserver


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
            assert pinned.code_commit_sha == "fixture"
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


def test_strategy_observer_candidate_pin_is_resolved_once_until_restart(
    tmp_path, monkeypatch
) -> None:
    config = load_config(
        {
            "DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'startup-pin.sqlite'}",
            "APP_MODE": "DRY_RUN",
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STRATEGY_V2_CANDIDATE_CONFIG_VERSION_ID": "config-pin",
            "TRADING_ENABLED": "false",
            "EXECUTE": "false",
        }
    )
    engine = create_engine_from_config(config)
    run_migrations(engine)
    factory = create_session_factory(engine)
    first = PinnedCandidate("candidate-first", "config-pin", V2_PARAMETERS, "first")
    second = PinnedCandidate("candidate-second", "config-pin", V2_PARAMETERS, "second")
    resolved: list[tuple[PinnedCandidate | None, str | None]] = [(first, None)]
    resolver_calls: list[PinnedCandidate | None] = []
    observed: list[tuple[PinnedCandidate | None, str | None]] = []

    def fake_resolve(*_args):
        resolver_calls.append(resolved[0][0])
        return resolved[0]

    def fake_variants(**kwargs):
        observed.append((kwargs["pinned_candidate"], kwargs["pin_blocker"]))
        return [
            (
                kwargs["config"],
                StrategyDecisionInput(
                    decision_id=f"startup-pin-{len(observed)}",
                    evaluated_at=datetime(2026, 7, 12, 12, 0, tzinfo=UTC),
                    decision_state="OBSERVE_ONLY_MARKET",
                    primary_reason="fixture",
                    app_mode="DRY_RUN",
                    strategy_id="btc15_momentum_v1",
                ),
            )
        ]

    monkeypatch.setattr(observer_module, "resolve_pinned_candidate", fake_resolve)
    monkeypatch.setattr(observer_module, "evaluate_strategy_variants", fake_variants)
    monkeypatch.setattr(
        observer_module, "_apply_dry_run_ledger", lambda **_kwargs: DryRunLedgerResult()
    )
    try:
        observer = StrategyObserver(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=datetime(2026, 7, 12, 12, 0, tzinfo=UTC),
            now=lambda: datetime(2026, 7, 12, 12, 0, tzinfo=UTC),
        )
        observer.evaluate_once()
        resolved[0] = (second, None)  # Represents a changed pin row after startup.
        observer.evaluate_once()

        assert resolver_calls == [first]
        assert observed == [(first, None), (first, None)]

        restarted = StrategyObserver(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=datetime(2026, 7, 12, 12, 1, tzinfo=UTC),
            now=lambda: datetime(2026, 7, 12, 12, 1, tzinfo=UTC),
        )
        restarted.evaluate_once()
        assert resolver_calls == [first, second]

        resolved[0] = (None, "candidate_pin_missing")
        invalid_restart = StrategyObserver(
            config=config,
            safety=assess_startup_safety(config),
            session_factory=factory,
            started_at=datetime(2026, 7, 12, 12, 2, tzinfo=UTC),
            now=lambda: datetime(2026, 7, 12, 12, 2, tzinfo=UTC),
        )
        invalid_restart.evaluate_once()
        assert observed[-1] == (None, "candidate_pin_missing")
    finally:
        engine.dispose()
