from __future__ import annotations

import logging

from sqlalchemy import inspect, select, text
from sqlalchemy.engine import Connection, Engine

from ape.config import ConfigError, load_config
from ape.db.models import Base, SchemaMigration, utc_now
from ape.db.session import DatabaseConfigError, create_engine_from_config

CURRENT_SCHEMA_VERSION = "0002_fixed_point_ws_quantities"
SCHEMA_VERSIONS = ("0001_initial_schema", CURRENT_SCHEMA_VERSION)
POSTGRES_MIGRATION_LOCK_ID = 4_150_002

LOGGER = logging.getLogger(__name__)


def run_migrations(engine: Engine) -> None:
    with engine.begin() as connection:
        _disable_postgres_migration_statement_timeout(connection)
        _acquire_migration_lock(connection)
        Base.metadata.create_all(connection)
        _ensure_fixed_point_quantity_columns(connection)
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
        statements.append(
            _add_numeric_column_statement(connection, "public_trades", "trade_count")
        )

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
        return (
            f"ALTER TABLE {table_name} "
            f"ADD COLUMN IF NOT EXISTS {column_name} NUMERIC(24, 8)"
        )
    return f"ALTER TABLE {table_name} ADD COLUMN {column_name} NUMERIC(24, 8)"


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
