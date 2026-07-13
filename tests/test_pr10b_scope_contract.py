from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from ape.config import load_config
from ape.db.migrations import CURRENT_SCHEMA_VERSION, run_migrations
from ape.db.models import Market
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.inputs import (
    OrderbookSnapshotInput,
    StrategyDecisionInput,
    StrategyDryRunPositionInput,
    StrategyTradeIntentInput,
)
from ape.repositories.orderbook import OrderbookRepository
from ape.repositories.storage_retention import (
    ALLOWED_RAW_PAYLOAD_READ_TABLES,
    ALLOWED_RETENTION_TABLES,
    ALLOWED_STATUS_READ_TABLES,
    StorageRetentionRepository,
)
from ape.repositories.strategy_dry_run import StrategyDryRunRepository
from ape.repositories.strategy_v2 import StrategyV2Repository
from ape.safety import assess_startup_safety
from ape.storage.retention import RETENTION_POLICIES, RETENTION_TABLE_NAMES, STATUS_TABLES
from ape.strategy import momentum_v2
from ape.strategy import observer as observer_module
from ape.strategy.context import StrategyEvaluationContext

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def session(tmp_path):
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_pr10b_contract.sqlite'}"
    engine = create_engine_from_config(load_config({"DATABASE_URL": database_url}))
    run_migrations(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as db_session:
            yield db_session
    finally:
        engine.dispose()


def test_pr10b_r1_boundary_cross_hold_is_research_only_and_cannot_create_entry(
    session,
    monkeypatch,
) -> None:
    evaluated_at = datetime(2026, 7, 11, 12, 10, tzinfo=UTC)
    context = StrategyEvaluationContext(
        evaluated_at=evaluated_at,
        market=Market(
            market_ticker="KXBTC15M-PR10B-R1",
            open_time=evaluated_at - timedelta(minutes=5),
            close_time=evaluated_at + timedelta(minutes=10),
        ),
        boundary=Decimal("62000"),
        boundary_source=None,
        reference_tick=None,
        orderbook=None,
        latest_trade=None,
        reference_ticks=(),
        orderbook_history=(),
        recent_trades=(),
    )
    features = {
        "candidate_side": "YES",
        "candidate_mode": "BOUNDARY_CROSS_HOLD",
        "quality_state": {
            "market_ready": True,
            "reference_ready": True,
            "book_ready": True,
        },
        "distance_bps": Decimal("2"),
        "fast_impulse_active": True,
        "retrace_fraction": Decimal("0.10"),
        "reversal_beyond_origin": False,
        "boundary_crosses_90s": 0,
        "return_60s": Decimal("0"),
        "return_120s": Decimal("0"),
        "contract_move_15s_cents": Decimal("0"),
        "contract_move_30s_cents": Decimal("0"),
        "persistent_adverse_microstructure": False,
        "desired_ask": Decimal("0.60"),
        "desired_spread_cents": Decimal("2"),
        "desired_ask_depth": Decimal("2"),
    }
    monkeypatch.setattr(momentum_v2, "_features", lambda _context, *, config: features)
    monkeypatch.setattr(momentum_v2, "_score", lambda _features, _tier: (Decimal("90"), {}))
    monkeypatch.setattr(momentum_v2, "_edge", lambda _features: Decimal("2"))
    monkeypatch.setattr(momentum_v2, "_timing_tier", lambda _open, _left: "normal")

    result = momentum_v2.evaluate_momentum_v2(context, config=load_config({}))

    assert result.state == momentum_v2.STATE_V2_HARD_GATE_BLOCKED
    assert result.reason == "v2_candidate_mode_not_enabled"
    assert result.blockers == ["v2_candidate_mode_not_enabled"]
    assert result.intended_entry_price is None
    assert result.candidate_mode == "BOUNDARY_CROSS_HOLD"

    features["quality_state"]["reference_ready"] = False
    not_ready_result = momentum_v2.evaluate_momentum_v2(context, config=load_config({}))

    assert not_ready_result.state == momentum_v2.STATE_V2_FEATURES_NOT_READY
    assert not_ready_result.reason == "v2_prerequisite_data_missing_or_stale"
    assert "v2_prerequisite_data_missing_or_stale" in not_ready_result.blockers
    assert not_ready_result.candidate_mode == "BOUNDARY_CROSS_HOLD"

    observer_module._apply_v2_hypothetical_lifecycle(
        config=load_config({}),
        session=session,
        decision=StrategyDecisionInput(
            decision_id="pr10b-r1-boundary-cross",
            evaluated_at=evaluated_at,
            decision_state=result.state,
            primary_reason=result.reason,
            app_mode="DRY_RUN",
            strategy_id=momentum_v2.V2_STRATEGY_ID,
            market_ticker=context.market.market_ticker,
            candidate_side=result.candidate_side,
            measurements=result.measurements,
            blockers=result.blockers,
        ),
    )

    assert (
        StrategyV2Repository(session).list_recent_intents(
            strategy_id=momentum_v2.V2_STRATEGY_ID,
            limit=10,
            action="ENTRY",
        )
        == []
    )


def test_pr10b_r2_exit_attempt_uses_only_the_first_in_window_book(session) -> None:
    now = datetime(2026, 7, 11, 12, 10, tzinfo=UTC)
    market_ticker = "KXBTC15M-PR10B-R2"
    positions = StrategyDryRunRepository(session)
    positions.insert_position_if_absent(
        StrategyDryRunPositionInput(
            position_id="pr10b-r2-open",
            strategy_id=momentum_v2.V2_STRATEGY_ID,
            market_ticker=market_ticker,
            decision_id="pr10b-r2-entry",
            side_candidate="YES",
            economic_side="YES",
            opened_at=now - timedelta(seconds=10),
            open_price=Decimal("0.70"),
            contract_count=1,
            entry_reason="v2_causal_hypothetical_fill",
            status="OPEN",
        )
    )
    intents = StrategyV2Repository(session)
    pending = intents.insert_intent_if_absent(
        StrategyTradeIntentInput(
            intent_id="pr10b-r2-exit",
            strategy_id=momentum_v2.V2_STRATEGY_ID,
            decision_id="pr10b-r2-decision",
            position_id="pr10b-r2-open",
            market_ticker=market_ticker,
            side_candidate="YES",
            action="EXIT",
            created_at=now - timedelta(seconds=4),
            effective_after=now - timedelta(seconds=3),
            expires_at=now - timedelta(seconds=1),
            intended_limit_price=Decimal("0.72"),
            quantity=Decimal("1"),
        )
    )
    orderbooks = OrderbookRepository(session)
    first_book = orderbooks.insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker=market_ticker,
            received_at=now - timedelta(seconds=2),
            yes_bid=Decimal("0.71"),
            yes_bid_count=Decimal("1"),
            yes_ask=Decimal("0.72"),
            yes_ask_count=Decimal("1"),
            book_status="ok",
        )
    )
    later_book = orderbooks.insert_snapshot(
        OrderbookSnapshotInput(
            market_ticker=market_ticker,
            received_at=now - timedelta(milliseconds=1500),
            yes_bid=Decimal("0.75"),
            yes_bid_count=Decimal("1"),
            yes_ask=Decimal("0.76"),
            yes_ask_count=Decimal("1"),
            book_status="ok",
        )
    )

    event_type, _ = observer_module._resolve_v2_pending_exit(
        session=session,
        intents=intents,
        positions=positions,
        pending=pending,
        resolved_at=now,
        decision=None,
    )

    assert first_book.id < later_book.id
    assert event_type == "V2_EXIT_NO_FILL"
    assert pending.status == "NO_FILL"
    assert pending.fill_snapshot_id is None
    assert positions.get_position_by_id("pr10b-r2-open").status == "OPEN"
    assert intents.list_recent_outcomes(strategy_id=momentum_v2.V2_STRATEGY_ID, limit=10) == []


def test_pr10b_r3_status_reads_are_separate_from_all_mutation_paths(session) -> None:
    repository = StorageRetentionRepository(session)
    outcome_table = "strategy_position_outcomes"
    cutoff = datetime(2026, 7, 11, 12, 10, tzinfo=UTC)

    assert repository.approximate_row_count(outcome_table) == 0
    assert repository.table_size(outcome_table)["approximate_total_bytes"] is None
    assert repository.oldest_newest(
        table_name=outcome_table,
        timestamp_expression="closed_at",
    ) == (None, None)

    with pytest.raises(ValueError, match="Unsupported retention table"):
        repository.count_matching(
            table_name=outcome_table,
            condition_sql="closed_at < :cutoff",
            parameters={"cutoff": cutoff},
        )
    with pytest.raises(ValueError, match="Unsupported retention table"):
        repository.has_matching(
            table_name=outcome_table,
            condition_sql="closed_at < :cutoff",
            parameters={"cutoff": cutoff},
        )
    with pytest.raises(ValueError, match="Unsupported retention table"):
        repository.delete_batch(
            table_name=outcome_table,
            condition_sql="closed_at < :cutoff",
            parameters={"cutoff": cutoff},
            batch_size=1,
        )
    with pytest.raises(ValueError, match="Unsupported retention table"):
        repository.strip_raw_payload_batch(
            table_name=outcome_table,
            condition_sql="closed_at < :cutoff",
            parameters={"cutoff": cutoff},
            batch_size=1,
        )
    with pytest.raises(ValueError, match="Unsupported raw payload storage table"):
        repository.raw_payload_non_null_count(outcome_table)

    assert outcome_table in {table.table_name for table in STATUS_TABLES}
    assert outcome_table not in RETENTION_TABLE_NAMES
    assert outcome_table not in {policy.table_name for policy in RETENTION_POLICIES}
    assert outcome_table not in ALLOWED_RETENTION_TABLES
    assert outcome_table in ALLOWED_STATUS_READ_TABLES
    assert outcome_table not in ALLOWED_RAW_PAYLOAD_READ_TABLES


def test_pr10b_r4_versions_preserve_schema_and_revise_v2_semantics(monkeypatch) -> None:
    assert momentum_v2.V2_ARCHITECTURE_VERSION == "momentum_v2_heuristic_v3"
    assert momentum_v2.V2_LIFECYCLE_SCHEMA_VERSION == "momentum_v2_lifecycle_v2"
    assert momentum_v2.V2_FEATURE_SCHEMA_VERSION == "momentum_v2_features_v3"
    assert CURRENT_SCHEMA_VERSION == "0010_research_replay_calibration"

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


def test_pr10b_r5_safety_and_documentation_preserve_deployment_boundaries() -> None:
    config = load_config({})
    safety = assess_startup_safety(config)
    railway = (REPOSITORY_ROOT / "docs" / "RAILWAY.md").read_text(encoding="utf-8")
    runbook = (REPOSITORY_ROOT / "docs" / "PR_RUNBOOK.md").read_text(encoding="utf-8")
    pr10a = (REPOSITORY_ROOT / "docs" / "PR10A_COMPLIANCE.md").read_text(
        encoding="utf-8"
    )
    pr10b = (REPOSITORY_ROOT / "docs" / "PR10B_COMPLIANCE.md").read_text(
        encoding="utf-8"
    )

    assert config.strategy_v2_enabled is False
    assert config.trading_enabled is False
    assert config.execute is False
    assert safety.is_safe is True
    for document in (railway, runbook):
        for step in (
            "1. Keep STRATEGY_V2_ENABLED=false.",
            "2. Redeploy ape-api.",
            "3. Redeploy ape-maintenance-worker.",
            "4. Redeploy ape-strategy-worker.",
            "5. Run initial API/storage/safety preflight.",
            "6. Set STRATEGY_V2_ENABLED=true only on ape-strategy-worker.",
            "7. Redeploy ape-strategy-worker.",
            "8. Run full PR 10b production validation.",
        ):
            assert step in document
    assert "No migration, Railway service, or environment variable is added." in railway
    assert "incomplete and noncompliant" in pr10a
    assert "PR 10b was required" in pr10a
    assert "DRY_RUN-only safety" in pr10b
