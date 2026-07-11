from __future__ import annotations

from sqlalchemy import func, inspect, select, text

from ape.config import load_config
from ape.db.migrations import (
    CURRENT_SCHEMA_VERSION,
    POSTGRES_MIGRATION_LOCK_ID,
    _acquire_migration_lock,
    _disable_postgres_migration_statement_timeout,
    run_migrations,
)
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
            "kalshi_ws_protocol_events",
            "strategy_decisions",
            "strategy_dry_run_events",
            "strategy_dry_run_positions",
            "worker_heartbeats",
            "storage_retention_runs",
        }

        session_factory = create_session_factory(engine)
        with session_factory() as session:
            migration = session.scalar(
                select(SchemaMigration).where(SchemaMigration.version == CURRENT_SCHEMA_VERSION)
            )
            assert migration is not None
            assert session.scalar(select(func.count()).select_from(SchemaMigration)) == 7

        orderbook_columns = {
            column["name"] for column in inspector.get_columns("orderbook_snapshots")
        }
        trade_columns = {column["name"] for column in inspector.get_columns("public_trades")}
        event_columns = {
            column["name"] for column in inspector.get_columns("strategy_dry_run_events")
        }
        assert orderbook_columns >= {
            "yes_bid_count",
            "yes_ask_count",
            "no_bid_count",
            "no_ask_count",
        }
        assert "trade_count" in trade_columns
        assert "strategy_id" in event_columns
        decision_columns = {
            column["name"] for column in inspector.get_columns("strategy_decisions")
        }
        decision_indexes = {
            index["name"] for index in inspector.get_indexes("strategy_decisions")
        }
        assert "strategy_id" in decision_columns
        assert "ix_strategy_decisions_strategy_id_evaluated" in decision_indexes
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


def test_migration_adds_strategy_id_to_existing_dry_run_events(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_schema_events.sqlite'}"
    engine = create_engine_from_config(load_config({"DATABASE_URL": database_url}))

    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE strategy_dry_run_positions (
                        id INTEGER PRIMARY KEY,
                        position_id VARCHAR(128) NOT NULL,
                        strategy_id VARCHAR(128) NOT NULL
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    CREATE TABLE strategy_dry_run_events (
                        id INTEGER PRIMARY KEY,
                        event_id VARCHAR(128) NOT NULL,
                        position_id VARCHAR(128),
                        event_type VARCHAR(64) NOT NULL,
                        occurred_at DATETIME NOT NULL
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    INSERT INTO strategy_dry_run_positions (
                        id,
                        position_id,
                        strategy_id
                    )
                    VALUES (1, 'dryrun-position-1', 'btc15_momentum_v1')
                    """
                )
            )
            connection.execute(
                text(
                    """
                    INSERT INTO strategy_dry_run_events (
                        id,
                        event_id,
                        position_id,
                        event_type,
                        occurred_at
                    )
                    VALUES (
                        1,
                        'dryrun-event-1',
                        'dryrun-position-1',
                        'ENTER_DRY_RUN',
                        CURRENT_TIMESTAMP
                    )
                    """
                )
            )

        run_migrations(engine)
        run_migrations(engine)

        inspector = inspect(engine)
        event_columns = {
            column["name"] for column in inspector.get_columns("strategy_dry_run_events")
        }
        event_indexes = {
            index["name"] for index in inspector.get_indexes("strategy_dry_run_events")
        }
        assert "strategy_id" in event_columns
        assert "ix_strategy_dry_run_events_strategy_id" in event_indexes

        with engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT strategy_id
                    FROM strategy_dry_run_events
                    WHERE event_id = 'dryrun-event-1'
                    """
                )
            ).one()

        assert row.strategy_id == "btc15_momentum_v1"
    finally:
        engine.dispose()


def test_migration_backfills_strategy_id_on_existing_decisions(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_schema_decisions.sqlite'}"
    engine = create_engine_from_config(load_config({"DATABASE_URL": database_url}))

    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE strategy_decisions (
                        id INTEGER PRIMARY KEY,
                        decision_id VARCHAR(128) NOT NULL,
                        evaluated_at DATETIME NOT NULL
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    INSERT INTO strategy_decisions (id, decision_id, evaluated_at)
                    VALUES (1, 'strategy-legacy-1', CURRENT_TIMESTAMP)
                    """
                )
            )

        run_migrations(engine)
        run_migrations(engine)

        inspector = inspect(engine)
        columns = {
            column["name"] for column in inspector.get_columns("strategy_decisions")
        }
        indexes = {
            index["name"] for index in inspector.get_indexes("strategy_decisions")
        }
        assert "strategy_id" in columns
        assert "ix_strategy_decisions_strategy_id_evaluated" in indexes

        with engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT strategy_id
                    FROM strategy_decisions
                    WHERE decision_id = 'strategy-legacy-1'
                    """
                )
            ).one()

        assert row.strategy_id == "btc15_momentum_v1"
    finally:
        engine.dispose()


def test_postgres_migrations_disable_statement_timeout_before_lock() -> None:
    connection = _RecordingPostgresConnection()

    _disable_postgres_migration_statement_timeout(connection)
    _acquire_migration_lock(connection)

    assert connection.executed == [
        ("SET LOCAL statement_timeout = 0", None),
        (
            "SELECT pg_advisory_xact_lock(:lock_id)",
            {"lock_id": POSTGRES_MIGRATION_LOCK_ID},
        ),
    ]


class _RecordingPostgresConnection:
    dialect = type("Dialect", (), {"name": "postgresql"})()

    def __init__(self) -> None:
        self.executed: list[tuple[str, object]] = []

    def execute(self, statement, parameters=None) -> None:
        self.executed.append((str(statement), parameters))
