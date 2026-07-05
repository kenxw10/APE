from __future__ import annotations

import logging

from sqlalchemy import inspect, select, text
from sqlalchemy.engine import Engine

from ape.config import ConfigError, load_config
from ape.db.models import Base, SchemaMigration
from ape.db.session import DatabaseConfigError, create_engine_from_config, create_session_factory

CURRENT_SCHEMA_VERSION = "0002_fixed_point_ws_quantities"
SCHEMA_VERSIONS = ("0001_initial_schema", CURRENT_SCHEMA_VERSION)

LOGGER = logging.getLogger(__name__)


def run_migrations(engine: Engine) -> None:
    Base.metadata.create_all(engine)
    _ensure_fixed_point_quantity_columns(engine)

    session_factory = create_session_factory(engine)
    with session_factory() as session:
        for version in SCHEMA_VERSIONS:
            existing = session.scalar(
                select(SchemaMigration).where(SchemaMigration.version == version)
            )
            if existing is None:
                session.add(SchemaMigration(version=version))
        session.commit()


def _ensure_fixed_point_quantity_columns(engine: Engine) -> None:
    inspector = inspect(engine)
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
                f"ALTER TABLE orderbook_snapshots ADD COLUMN {column} NUMERIC(24, 8)"
            )

    if "trade_count" not in existing_columns.get("public_trades", set()):
        statements.append("ALTER TABLE public_trades ADD COLUMN trade_count NUMERIC(24, 8)")

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

    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
        for statement in backfill_statements:
            connection.execute(text(statement))


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
