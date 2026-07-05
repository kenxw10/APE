from __future__ import annotations

from sqlalchemy import inspect, select

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
    finally:
        engine.dispose()
