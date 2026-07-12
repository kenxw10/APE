from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError
from tests.test_research_helpers import at_base, feature_event, orderbook_event, valid_vector

from ape.api.main import create_app
from ape.config import WORKER_ROLES, load_config
from ape.db.migrations import CURRENT_SCHEMA_VERSION, run_migrations
from ape.db.models import Base, ResearchReplayEvent
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.storage_retention import ALLOWED_RETENTION_TABLES, ALLOWED_STATUS_READ_TABLES
from ape.research.archive import _hydrate_persisted_feature_vector
from ape.research.calibration import (
    LIFECYCLE_DRAFT,
    LIFECYCLE_PAPER_CANDIDATE,
    GovernanceError,
    bounded_candidate_specs,
    build_partition_manifest,
    market_bootstrap,
    transition_candidate,
)
from ape.research.fees import verified_kalshi_taker_fee_model
from ape.research.fixtures import (
    synthetic_btc15_fixture_dataset,
    synthetic_btc15_fixture_markets,
)
from ape.research.replay import DeterministicReplayEngine, zero_entry_audit
from ape.storage.retention import RETENTION_POLICIES, STATUS_TABLES
from ape.strategy.momentum_v2 import (
    CALIBRATION_SCHEMA_VERSION,
    GOVERNANCE_SCHEMA_VERSION,
    REPLAY_SCHEMA_VERSION,
    RESEARCH_LABEL_SCHEMA_VERSION,
    V2_FEATURE_SCHEMA_VERSION,
    evaluate_momentum_v2_feature_vector,
    feature_vector_hash,
)

ROOT = Path(__file__).resolve().parents[1]


def test_r1_single_research_migration_and_schema_contract(tmp_path) -> None:
    assert CURRENT_SCHEMA_VERSION == "0010_research_replay_calibration"
    assert {
        "research_replay_events",
        "research_market_outcomes",
        "research_replay_runs",
        "research_replay_trades",
        "calibration_runs",
        "research_candidates",
        "research_governance_events",
    } <= set(Base.metadata.tables)
    engine = create_engine_from_config(
        load_config({"DATABASE_URL": f"sqlite+pysqlite:///{tmp_path / 'r1.sqlite'}"})
    )
    try:
        run_migrations(engine)
        run_migrations(engine)
        factory = create_session_factory(engine)
        with factory() as session:
            session.add_all(
                    (
                        _replay_event("r1-first"),
                        _replay_event("same-source"),
                        _replay_event("same-source", event_id="r1-first"),
                )
            )
            with pytest.raises(IntegrityError):
                session.flush()
            session.rollback()
        indexes = inspect(engine).get_indexes("research_replay_events")
        assert any(index["name"] == "ix_research_replay_events_market_time" for index in indexes)
    finally:
        engine.dispose()


def test_r2_complete_vector_is_hashed_and_boundary_cross_remains_non_enterable() -> None:
    vector = valid_vector()
    assert feature_vector_hash(vector) == feature_vector_hash(dict(vector))
    vector["candidate_mode"] = "BOUNDARY_CROSS_HOLD"
    evaluation = evaluate_momentum_v2_feature_vector(vector)
    assert evaluation.state != "DRY_RUN_ENTRY_SIGNAL"


@pytest.mark.parametrize(
    "changes",
    (
        {},
        {"candidate_mode": "BOUNDARY_CROSS_HOLD"},
        {"quality_state": {"market_ready": False, "reference_ready": True, "book_ready": True}},
    ),
)
def test_r2_live_and_json_persisted_vectors_have_identical_evaluator_results(changes) -> None:
    live = valid_vector()
    live.update(changes)
    persisted = _json_safe_vector(live)

    live_result = evaluate_momentum_v2_feature_vector(live)
    persisted_result = evaluate_momentum_v2_feature_vector(
        _hydrate_persisted_feature_vector(persisted)
    )

    assert (persisted_result.state, persisted_result.reason, persisted_result.blockers) == (
        live_result.state,
        live_result.reason,
        live_result.blockers,
    )


def test_r3_research_role_is_explicit_and_database_only() -> None:
    assert "research" in WORKER_ROLES
    source = (ROOT / "src/ape/research/service.py").read_text()
    assert "KalshiWsCollector" not in source
    assert "ape-worker.research" in (ROOT / "src/ape/worker/services.py").read_text()


def test_r4_archive_uses_normalized_source_types() -> None:
    source = (ROOT / "src/ape/research/archive.py").read_text()
    for event_type in (
        "MARKET",
        "REFERENCE",
        "ORDERBOOK",
        "PUBLIC_TRADE",
        "FEATURE_SNAPSHOT",
        "MARKET_LIFECYCLE",
    ):
        assert event_type in source
    assert "raw_payload" not in source
    assert "ARCHIVE_BATCH_SIZE" in source


def test_r5_zero_entry_audit_is_explicitly_unvalidatable() -> None:
    report = zero_entry_audit({"signal": 0, "intent": 0, "opened": 0, "closed": 0}, market_count=18)
    assert report["frequency_classification"] == "ZERO_ENTRY_UNVALIDATABLE"
    assert {"pipeline_percentages", "first_blockers", "top_near_miss_samples"} <= set(report)


def test_r6_verified_taker_fee_is_nonzero_and_versioned() -> None:
    fee = verified_kalshi_taker_fee_model()
    assert fee.fee_cents(price=Decimal("0.50")) == Decimal("2.00")
    assert fee.metadata()["schedule_version"] == "2026-07-07"


def test_r7_ordered_replay_uses_first_book_without_future_rescue() -> None:
    at = at_base()
    result = DeterministicReplayEngine().replay(
        [
            feature_event(at=at),
            orderbook_event(at=at + timedelta(milliseconds=600), event_id="first", yes_ask="0.79"),
            orderbook_event(at=at + timedelta(milliseconds=800), event_id="later", yes_ask="0.60"),
        ]
    )
    assert [trade.status for trade in result.trades] == ["ENTRY_NO_FILL"]


def test_r8_r9_market_partitions_and_bounded_candidates() -> None:
    candidates = bounded_candidate_specs("contract")
    assert len(candidates) == 256
    assert {candidate.model_type for candidate in candidates} >= {
        "BASELINE",
        "WEIGHTED_HEURISTIC",
        "L2_LOGISTIC",
    }
    assert "statistical_unit" in build_partition_manifest([])


def test_r10_market_bootstrap_is_two_thousand_resamples() -> None:
    result = market_bootstrap({"one": Decimal("1"), "two": Decimal("-1")}, "contract")
    assert result["resamples"] == "2000"


def test_r11_governance_cannot_cross_into_paper_or_live() -> None:
    with pytest.raises(GovernanceError):
        transition_candidate(
            from_state=LIFECYCLE_DRAFT, to_state=LIFECYCLE_PAPER_CANDIDATE, evidence={}
        )


def test_r12_candidate_pin_is_optional_and_defaults_unset() -> None:
    assert load_config({}).strategy_v2_candidate_config_version_id is None
    assert "STRATEGY_V2_CANDIDATE_CONFIG_VERSION_ID" in (ROOT / "src/ape/config.py").read_text()


def test_r13_research_api_surface_is_read_only_and_bounded() -> None:
    app = create_app(load_config({}))
    routes = [route for route in app.routes if getattr(route, "path", "").startswith("/research/")]
    assert len(routes) == 8
    assert all(route.methods == {"GET"} for route in routes)


def test_r14_retention_and_durable_status_tables_are_separate() -> None:
    retained = {policy.table_name for policy in RETENTION_POLICIES}
    status = {table.table_name for table in STATUS_TABLES}
    assert {"research_replay_events", "research_replay_trades"} <= retained
    assert {
        "research_market_outcomes",
        "research_replay_runs",
        "calibration_runs",
        "research_candidates",
        "research_governance_events",
    } <= status
    assert "research_market_outcomes" not in ALLOWED_RETENTION_TABLES
    assert "research_market_outcomes" in ALLOWED_STATUS_READ_TABLES


def test_r15_documentation_versions_and_safety_contract_are_present() -> None:
    compliance = (ROOT / "docs/PR11_COMPLIANCE.md").read_text()
    assert "# PR 11 Compliance Matrix" in compliance
    assert all(f"| R{requirement} " in compliance for requirement in range(1, 16))
    assert "Promotion evidence is derived from persisted source events" in compliance
    assert "Regenerated raw logs, JUnit XML, result JSON" in compliance
    assert "does not add paper trading" in compliance
    assert (ROOT / "docs/RESEARCH_AND_CALIBRATION.md").exists()
    assert "ape-research-worker" in (ROOT / "docs/RAILWAY.md").read_text()
    assert (
        REPLAY_SCHEMA_VERSION,
        RESEARCH_LABEL_SCHEMA_VERSION,
        CALIBRATION_SCHEMA_VERSION,
        GOVERNANCE_SCHEMA_VERSION,
        V2_FEATURE_SCHEMA_VERSION,
    ) == (
        "momentum_v2_replay_v1",
        "momentum_research_labels_v1",
        "momentum_calibration_v1",
        "momentum_governance_v1",
        "momentum_v2_features_v3",
    )
    fixtures = synthetic_btc15_fixture_markets()
    assert len(fixtures) >= 18
    assert {row.quality_flags["volatility_regime"] for row in fixtures} == {
        "low",
        "medium",
        "high",
    }


def test_r15_eighteen_market_fixture_has_real_event_time_sources_and_labels() -> None:
    dataset = synthetic_btc15_fixture_dataset()
    expected_types = {
        "MARKET",
        "REFERENCE",
        "ORDERBOOK",
        "PUBLIC_TRADE",
        "FEATURE_SNAPSHOT",
        "MARKET_LIFECYCLE",
    }
    by_market = {}
    for event in dataset.events:
        by_market.setdefault(event.market_ticker, []).append(event)

    assert len(dataset.outcomes) == 18
    assert set(by_market) == {outcome.market_ticker for outcome in dataset.outcomes}
    assert all(
        {event.event_type for event in events} >= expected_types
        for events in by_market.values()
    )
    assert all(
        outcome.quality_flags["counterfactual_labels"] for outcome in dataset.outcomes
    )
    assert {
        outcome.quality_flags["scenario"] for outcome in dataset.outcomes
    } >= {
        "continuation_entry",
        "boundary_cross_research_only",
        "later_book_non_rescue",
        "exit_retry_exhaustion",
        "partial_coverage_frozen_holdout",
    }


def _replay_event(
    source_row_id: str,
    *,
    event_id: str | None = None,
) -> ResearchReplayEvent:
    at = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)
    return ResearchReplayEvent(
        event_id=event_id or source_row_id,
        market_ticker="R1",
        event_type="MARKET",
        event_time=at,
        received_at=at,
        source_table="r1_source",
        source_row_id=source_row_id,
        replay_schema_version=REPLAY_SCHEMA_VERSION,
        payload={},
        event_hash=source_row_id,
        replay_readiness="FULL",
    )


def _json_safe_vector(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe_vector(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe_vector(item) for item in value]
    return value
