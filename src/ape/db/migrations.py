from __future__ import annotations

import logging

from sqlalchemy import inspect, select, text
from sqlalchemy.engine import Connection, Engine

from ape.config import ConfigError, load_config
from ape.db.models import Base, SchemaMigration, utc_now
from ape.db.session import DatabaseConfigError, create_engine_from_config

CURRENT_SCHEMA_VERSION = "0011_research_archive_cursors"
SCHEMA_VERSIONS = (
    "0001_initial_schema",
    "0002_fixed_point_ws_quantities",
    "0003_storage_retention_lifecycle",
    "0004_dry_run_strategy_ledger",
    "0005_strategy_dry_run_event_strategy_id",
    "0006_kalshi_ws_protocol_events",
    "0007_strategy_decision_strategy_id",
    "0008_momentum_v2_feature_architecture",
    "0009_momentum_v2_scope_completion",
    "0010_research_replay_calibration",
    CURRENT_SCHEMA_VERSION,
)
POSTGRES_MIGRATION_LOCK_ID = 4_150_002

LOGGER = logging.getLogger(__name__)


def run_migrations(engine: Engine) -> None:
    with engine.begin() as connection:
        _disable_postgres_migration_statement_timeout(connection)
        _acquire_migration_lock(connection)
        Base.metadata.create_all(connection)
        _ensure_fixed_point_quantity_columns(connection)
        _ensure_strategy_dry_run_event_strategy_id(connection)
        _ensure_strategy_decision_strategy_id(connection)
        _ensure_momentum_v2_columns(connection)
        _ensure_momentum_v2_scope_completion_columns(connection)
        _ensure_research_replay_calibration_columns(connection)
        _ensure_research_archive_cursors_table(connection)
        _record_schema_versions(connection)


def _disable_postgres_migration_statement_timeout(connection: Connection) -> None:
    if connection.dialect.name != "postgresql":
        return

    connection.execute(text("SET LOCAL statement_timeout = 0"))


def _acquire_migration_lock(connection: Connection) -> None:
    if connection.dialect.name != "postgresql":
        return

    connection.execute(
        text("SELECT pg_advisory_xact_lock(:lock_id)"),
        {"lock_id": POSTGRES_MIGRATION_LOCK_ID},
    )


def _ensure_fixed_point_quantity_columns(connection: Connection) -> None:
    inspector = inspect(connection)
    existing_columns = {
        table: {column["name"] for column in inspector.get_columns(table)}
        for table in ("orderbook_snapshots", "public_trades")
        if table in inspector.get_table_names()
    }

    statements: list[str] = []
    for column in (
        "yes_bid_count",
        "yes_ask_count",
        "no_bid_count",
        "no_ask_count",
    ):
        if column not in existing_columns.get("orderbook_snapshots", set()):
            statements.append(
                _add_numeric_column_statement(connection, "orderbook_snapshots", column)
            )

    if "trade_count" not in existing_columns.get("public_trades", set()):
        statements.append(_add_numeric_column_statement(connection, "public_trades", "trade_count"))

    backfill_statements = (
        "UPDATE orderbook_snapshots SET yes_bid_count = yes_bid_size "
        "WHERE yes_bid_count IS NULL AND yes_bid_size IS NOT NULL",
        "UPDATE orderbook_snapshots SET yes_ask_count = yes_ask_size "
        "WHERE yes_ask_count IS NULL AND yes_ask_size IS NOT NULL",
        "UPDATE orderbook_snapshots SET no_bid_count = no_bid_size "
        "WHERE no_bid_count IS NULL AND no_bid_size IS NOT NULL",
        "UPDATE orderbook_snapshots SET no_ask_count = no_ask_size "
        "WHERE no_ask_count IS NULL AND no_ask_size IS NOT NULL",
        'UPDATE public_trades SET trade_count = "count" '
        'WHERE trade_count IS NULL AND "count" IS NOT NULL',
    )

    for statement in statements:
        connection.execute(text(statement))
    for statement in backfill_statements:
        connection.execute(text(statement))


def _add_numeric_column_statement(
    connection: Connection,
    table_name: str,
    column_name: str,
) -> str:
    if connection.dialect.name == "postgresql":
        return f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} NUMERIC(24, 8)"
    return f"ALTER TABLE {table_name} ADD COLUMN {column_name} NUMERIC(24, 8)"


def _ensure_strategy_dry_run_event_strategy_id(connection: Connection) -> None:
    inspector = inspect(connection)
    table_names = set(inspector.get_table_names())
    if "strategy_dry_run_events" not in table_names:
        return

    event_columns = {column["name"] for column in inspector.get_columns("strategy_dry_run_events")}
    if "strategy_id" not in event_columns:
        connection.execute(
            text(
                _add_text_column_statement(
                    connection,
                    "strategy_dry_run_events",
                    "strategy_id",
                )
            )
        )

    if "strategy_dry_run_positions" in table_names:
        connection.execute(
            text(
                """
                UPDATE strategy_dry_run_events
                SET strategy_id = (
                    SELECT strategy_dry_run_positions.strategy_id
                    FROM strategy_dry_run_positions
                    WHERE strategy_dry_run_positions.position_id =
                        strategy_dry_run_events.position_id
                    LIMIT 1
                )
                WHERE strategy_id IS NULL
                    AND position_id IS NOT NULL
                """
            )
        )

    _ensure_index(
        connection,
        table_name="strategy_dry_run_events",
        index_name="ix_strategy_dry_run_events_strategy_id",
        column_name="strategy_id",
    )


def _ensure_strategy_decision_strategy_id(connection: Connection) -> None:
    inspector = inspect(connection)
    if "strategy_decisions" not in set(inspector.get_table_names()):
        return

    decision_columns = {column["name"] for column in inspector.get_columns("strategy_decisions")}
    if "strategy_id" not in decision_columns:
        connection.execute(
            text(
                _add_text_column_statement(
                    connection,
                    "strategy_decisions",
                    "strategy_id",
                )
            )
        )

    connection.execute(
        text(
            """
            UPDATE strategy_decisions
            SET strategy_id = 'btc15_momentum_v1'
            WHERE strategy_id IS NULL OR TRIM(strategy_id) = ''
            """
        )
    )
    _ensure_composite_index(
        connection,
        table_name="strategy_decisions",
        index_name="ix_strategy_decisions_strategy_id_evaluated",
        column_names=("strategy_id", "evaluated_at"),
    )


def _ensure_momentum_v2_columns(connection: Connection) -> None:
    columns = {
        "orderbook_snapshots": {
            "ladder_schema_version": "VARCHAR(64)",
            "yes_bid_ladder": "JSON",
            "no_bid_ladder": "JSON",
        },
        "strategy_decisions": {
            "feature_snapshot_id": "VARCHAR(128)",
            "strategy_config_version_id": "VARCHAR(128)",
            "code_commit_sha": "VARCHAR(128)",
            "calibration_run_id": "VARCHAR(128)",
        },
        "strategy_dry_run_positions": {
            "feature_snapshot_id": "VARCHAR(128)",
            "strategy_config_version_id": "VARCHAR(128)",
            "code_commit_sha": "VARCHAR(128)",
        },
        "strategy_dry_run_events": {
            "feature_snapshot_id": "VARCHAR(128)",
            "strategy_config_version_id": "VARCHAR(128)",
            "code_commit_sha": "VARCHAR(128)",
        },
    }
    inspector = inspect(connection)
    table_names = set(inspector.get_table_names())
    for table_name, expected in columns.items():
        if table_name not in table_names:
            continue
        existing = {column["name"] for column in inspector.get_columns(table_name)}
        for column_name, type_sql in expected.items():
            if column_name not in existing:
                connection.execute(
                    text(_add_column_statement(connection, table_name, column_name, type_sql))
                )

    _ensure_composite_index(
        connection,
        table_name="strategy_feature_snapshots",
        index_name="ix_strategy_feature_snapshots_market_evaluated",
        column_names=("market_ticker", "evaluated_at"),
    )


def _ensure_momentum_v2_scope_completion_columns(connection: Connection) -> None:
    columns = {
        "orderbook_snapshots": {
            "yes_ask_ladder": "JSON",
            "no_ask_ladder": "JSON",
        },
        "strategy_dry_run_positions": {
            "entry_intent_id": "VARCHAR(128)",
            "exit_intent_id": "VARCHAR(128)",
            "lifecycle_version": "VARCHAR(128)",
            "entry_timing_tier": "VARCHAR(64)",
            "entry_score_threshold": "NUMERIC(24, 8)",
            "entry_time_stop_seconds": "INTEGER",
            "entry_max_hold_seconds": "INTEGER",
            "entry_score": "NUMERIC(24, 8)",
            "entry_edge_lower_bound_cents": "NUMERIC(24, 8)",
            "entry_response_residual_cents": "NUMERIC(24, 8)",
            "entry_boundary": "NUMERIC(24, 8)",
            "entry_standardized_distance": "NUMERIC(24, 8)",
        },
        "strategy_trade_intents": {
            "architecture_version": "VARCHAR(128)",
            "code_commit_sha": "VARCHAR(128)",
            "lifecycle_version": "VARCHAR(128)",
            "trigger": "VARCHAR(128)",
            "trigger_classification": "VARCHAR(32)",
            "attempt_number": "INTEGER",
            "decision_time_executable_bid": "NUMERIC(24, 8)",
            "fill_timestamp": "TIMESTAMP",
        },
    }
    inspector = inspect(connection)
    table_names = set(inspector.get_table_names())
    for table_name, expected in columns.items():
        if table_name not in table_names:
            continue
        existing = {column["name"] for column in inspector.get_columns(table_name)}
        for column_name, type_sql in expected.items():
            if column_name not in existing:
                connection.execute(
                    text(_add_column_statement(connection, table_name, column_name, type_sql))
                )

    _ensure_composite_index(
        connection,
        table_name="strategy_trade_intents",
        index_name="ix_strategy_trade_intents_position_status",
        column_names=("position_id", "status"),
    )
    _ensure_composite_index(
        connection,
        table_name="strategy_trade_intents",
        index_name="ix_strategy_trade_intents_strategy_market_created",
        column_names=("strategy_id", "market_ticker", "created_at"),
    )


def _ensure_research_replay_calibration_columns(connection: Connection) -> None:
    """Make the single research migration safe on both new and existing databases."""
    columns = {
        "strategy_config_versions": {
            "parent_config_version_id": "VARCHAR(128)",
            "calibration_run_id": "VARCHAR(128)",
            "lifecycle_state": "VARCHAR(64)",
            "approval_state": "VARCHAR(64)",
            "model_type": "VARCHAR(64)",
            "model_artifact_checksum": "VARCHAR(128)",
            "data_cutoff": "TIMESTAMP",
            "candidate_id": "VARCHAR(128)",
        },
        "strategy_feature_snapshots": {
            "complete_feature_vector": "JSON",
            "feature_vector_hash": "VARCHAR(128)",
            "architecture_version": "VARCHAR(128)",
            "replay_schema_version": "VARCHAR(128)",
            "replay_readiness": "VARCHAR(32)",
            "replay_blockers": "JSON",
        },
    }
    inspector = inspect(connection)
    table_names = set(inspector.get_table_names())
    for table_name, expected in columns.items():
        if table_name not in table_names:
            continue
        existing = {column["name"] for column in inspector.get_columns(table_name)}
        for column_name, type_sql in expected.items():
            if column_name not in existing:
                connection.execute(
                    text(_add_column_statement(connection, table_name, column_name, type_sql))
                )

    _ensure_composite_index(
        connection,
        table_name="research_replay_events",
        index_name="ix_research_replay_events_market_time",
        column_names=("market_ticker", "event_time"),
    )
    _ensure_composite_index(
        connection,
        table_name="research_replay_trades",
        index_name="ix_research_replay_trades_run_market_config",
        column_names=("replay_run_id", "market_ticker", "strategy_config_version_id"),
    )
    _ensure_composite_index(
        connection,
        table_name="research_candidates",
        index_name="ix_research_candidates_architecture_state",
        column_names=("architecture_version", "lifecycle_state"),
    )


def _ensure_research_archive_cursors_table(connection: Connection) -> None:
    """Create only the small durable cursor table for append-only research sources."""
    connection.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS research_archive_cursors (
                source_table VARCHAR(128) PRIMARY KEY,
                selector_mode VARCHAR(32) NOT NULL,
                source_cursor INTEGER NOT NULL,
                frozen_bootstrap_target INTEGER,
                verification_window_start INTEGER,
                verification_window_end INTEGER,
                schema_version VARCHAR(64) NOT NULL,
                bootstrap_complete BOOLEAN NOT NULL,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
            """
        )
    )
    sources = (
        "reference_ticks",
        "orderbook_snapshots",
        "public_trades",
        "strategy_feature_snapshots",
        "strategy_trade_intents",
        "strategy_position_outcomes",
    )
    value_rows = ", ".join(
        f"(:source_{index}, 'UNINITIALIZED', 0, 'research_archive_cursor_v1', 0, "
        "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
        for index in range(len(sources))
    )
    parameters = {f"source_{index}": source_table for index, source_table in enumerate(sources)}
    insert = "INSERT OR IGNORE" if connection.dialect.name == "sqlite" else "INSERT"
    conflict = (
        " ON CONFLICT (source_table) DO NOTHING"
        if connection.dialect.name == "postgresql"
        else ""
    )
    connection.execute(
        text(
            f"""
            {insert} INTO research_archive_cursors (
                source_table,
                selector_mode,
                source_cursor,
                schema_version,
                bootstrap_complete,
                created_at,
                updated_at
            )
            VALUES {value_rows}{conflict}
            """
        ),
        parameters,
    )


def _add_column_statement(
    connection: Connection,
    table_name: str,
    column_name: str,
    type_sql: str,
) -> str:
    if connection.dialect.name == "postgresql":
        return f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {type_sql}"
    return f"ALTER TABLE {table_name} ADD COLUMN {column_name} {type_sql}"


def _add_text_column_statement(
    connection: Connection,
    table_name: str,
    column_name: str,
) -> str:
    if connection.dialect.name == "postgresql":
        return f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} VARCHAR(128)"
    return f"ALTER TABLE {table_name} ADD COLUMN {column_name} VARCHAR(128)"


def _ensure_index(
    connection: Connection,
    *,
    table_name: str,
    index_name: str,
    column_name: str,
) -> None:
    inspector = inspect(connection)
    existing_indexes = {index["name"] for index in inspector.get_indexes(table_name)}
    if index_name in existing_indexes:
        return

    connection.execute(
        text(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column_name})")
    )


def _ensure_composite_index(
    connection: Connection,
    *,
    table_name: str,
    index_name: str,
    column_names: tuple[str, ...],
) -> None:
    inspector = inspect(connection)
    existing_indexes = {index["name"] for index in inspector.get_indexes(table_name)}
    if index_name in existing_indexes:
        return

    columns = ", ".join(column_names)
    connection.execute(text(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({columns})"))


def _record_schema_versions(connection: Connection) -> None:
    for version in SCHEMA_VERSIONS:
        _record_schema_version(connection, version)


def _record_schema_version(connection: Connection, version: str) -> None:
    if connection.dialect.name == "postgresql":
        connection.execute(
            text(
                """
                INSERT INTO schema_migrations (version, applied_at)
                VALUES (:version, CURRENT_TIMESTAMP)
                ON CONFLICT (version) DO NOTHING
                """
            ),
            {"version": version},
        )
        return

    if connection.dialect.name == "sqlite":
        connection.execute(
            text(
                """
                INSERT OR IGNORE INTO schema_migrations (version, applied_at)
                VALUES (:version, CURRENT_TIMESTAMP)
                """
            ),
            {"version": version},
        )
        return

    applied_at = utc_now()
    existing = connection.scalar(
        select(SchemaMigration.version).where(SchemaMigration.version == version)
    )
    if existing is None:
        connection.execute(
            SchemaMigration.__table__.insert().values(
                version=version,
                applied_at=applied_at,
            )
        )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")

    try:
        config = load_config()
        engine = create_engine_from_config(config)
        try:
            run_migrations(engine)
        finally:
            engine.dispose()
    except (ConfigError, DatabaseConfigError) as exc:
        LOGGER.error("Database migration configuration error: %s", exc)
        return 1

    LOGGER.info("Database schema is current at version %s.", CURRENT_SCHEMA_VERSION)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
