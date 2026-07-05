from __future__ import annotations

from sqlalchemy import inspect, select, text

from ape.config import load_config
from ape.db.migrations import CURRENT_SCHEMA_VERSION, run_migrations
from ape.db.models import SchemaMigration
from ape.db.session import create_engine_from_config, create_session_factory


def test_schema_can_be_created_in_local_sqlite_database(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_schema.sqlite'}"
    engine = create_engine_from_config(load_config({"DATABASE_URL": database_url}))

    try:
        run_migrations(engine)

        inspector = inspect(engine)
        assert set(inspector.get_table_names()) >= {
            "schema_migrations",
            "markets",
            "reference_ticks",
            "orderbook_snapshots",
            "public_trades",
            "strategy_decisions",
            "worker_heartbeats",
        }

        session_factory = create_session_factory(engine)
        with session_factory() as session:
            migration = session.scalar(
                select(SchemaMigration).where(SchemaMigration.version == CURRENT_SCHEMA_VERSION)
            )
            assert migration is not None

        orderbook_columns = {
            column["name"] for column in inspector.get_columns("orderbook_snapshots")
        }
        trade_columns = {column["name"] for column in inspector.get_columns("public_trades")}
        assert orderbook_columns >= {
            "yes_bid_count",
            "yes_ask_count",
            "no_bid_count",
            "no_ask_count",
        }
        assert "trade_count" in trade_columns
    finally:
        engine.dispose()


def test_migration_adds_fixed_point_quantity_columns_to_existing_tables(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_schema_existing.sqlite'}"
    engine = create_engine_from_config(load_config({"DATABASE_URL": database_url}))

    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE orderbook_snapshots (
                        id INTEGER PRIMARY KEY,
                        yes_bid_size INTEGER,
                        yes_ask_size INTEGER,
                        no_bid_size INTEGER,
                        no_ask_size INTEGER
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    CREATE TABLE public_trades (
                        id INTEGER PRIMARY KEY,
                        "count" INTEGER
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    INSERT INTO orderbook_snapshots (
                        id,
                        yes_bid_size,
                        yes_ask_size,
                        no_bid_size,
                        no_ask_size
                    )
                    VALUES (1, 10, 11, 12, 13)
                    """
                )
            )
            connection.execute(
                text('INSERT INTO public_trades (id, "count") VALUES (1, 3)')
            )

        run_migrations(engine)

        inspector = inspect(engine)
        orderbook_columns = {
            column["name"] for column in inspector.get_columns("orderbook_snapshots")
        }
        trade_columns = {column["name"] for column in inspector.get_columns("public_trades")}
        assert orderbook_columns >= {
            "yes_bid_count",
            "yes_ask_count",
            "no_bid_count",
            "no_ask_count",
        }
        assert "trade_count" in trade_columns

        with engine.connect() as connection:
            orderbook_row = connection.execute(
                text(
                    """
                    SELECT
                        yes_bid_count,
                        yes_ask_count,
                        no_bid_count,
                        no_ask_count
                    FROM orderbook_snapshots
                    WHERE id = 1
                    """
                )
            ).one()
            trade_row = connection.execute(
                text("SELECT trade_count FROM public_trades WHERE id = 1")
            ).one()

        assert tuple(orderbook_row) == (10, 11, 12, 13)
        assert trade_row.trade_count == 3
    finally:
        engine.dispose()
