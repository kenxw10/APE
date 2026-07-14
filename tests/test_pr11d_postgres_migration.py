from __future__ import annotations

import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ape.config import load_config
from ape.db import migrations as migrations_module
from ape.db.migrations import CURRENT_SCHEMA_VERSION, run_migrations
from ape.db.session import create_engine_from_config


def _engine(database_url: str) -> Engine:
    return create_engine_from_config(load_config({"DATABASE_URL": database_url}))


@pytest.fixture
def postgres_url() -> str:
    database_url = os.environ.get("TEST_POSTGRES_URL")
    if not database_url:
        pytest.skip("TEST_POSTGRES_URL is not configured")
    return database_url


@pytest.fixture
def postgres_engine(postgres_url: str) -> Iterator[Engine]:
    engine = _engine(postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
            connection.execute(text("CREATE SCHEMA public AUTHORIZATION CURRENT_USER"))
        yield engine
    finally:
        engine.dispose()


def _table_exists(engine: Engine, table_name: str) -> bool:
    with engine.connect() as connection:
        return bool(
            connection.scalar(
                text(
                    """
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = :table_name
                    """
                ),
                {"table_name": table_name},
            )
        )


def _cursor_count(engine: Engine) -> int:
    with engine.connect() as connection:
        return int(
            connection.scalar(text("SELECT COUNT(*) FROM research_archive_cursors"))
        )


def _run_migrations_in_new_engine(database_url: str) -> None:
    engine = _engine(database_url)
    try:
        run_migrations(engine)
    finally:
        engine.dispose()


def test_postgres_migration_is_typed_idempotent_and_concurrent(
    postgres_engine: Engine, postgres_url: str
) -> None:
    run_migrations(postgres_engine)

    with postgres_engine.connect() as connection:
        column = connection.execute(
            text(
                """
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'research_archive_cursors'
                  AND column_name = 'bootstrap_complete'
                """
            )
        ).one()
        assert column.data_type == "boolean"
        values = connection.execute(
            text(
                """
                SELECT bootstrap_complete, pg_typeof(bootstrap_complete)::text AS pg_type
                FROM research_archive_cursors
                ORDER BY source_table
                """
            )
        ).all()
        assert len(values) == 6
        assert all(row.bootstrap_complete is False for row in values)
        assert all(row.pg_type == "boolean" for row in values)
        assert connection.scalar(
            text("SELECT COUNT(*) FROM schema_migrations WHERE version = :version"),
            {"version": CURRENT_SCHEMA_VERSION},
        ) == 1

    assert _table_exists(postgres_engine, "research_archive_cursors")
    run_migrations(postgres_engine)
    assert _cursor_count(postgres_engine) == 6

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(_run_migrations_in_new_engine, postgres_url)
            for _ in range(2)
        ]
        for future in futures:
            future.result()

    with postgres_engine.connect() as connection:
        assert connection.scalar(
            text("SELECT COUNT(*) FROM schema_migrations WHERE version = :version"),
            {"version": CURRENT_SCHEMA_VERSION},
        ) == 1
        assert connection.scalar(text("SELECT COUNT(*) FROM research_archive_cursors")) == 6


def test_postgres_migration_transaction_and_seed_recovery(
    postgres_engine: Engine, monkeypatch
) -> None:
    original_record_schema_versions = migrations_module._record_schema_versions

    def fail_before_schema_record(_connection) -> None:
        raise RuntimeError("intentional migration failure")

    monkeypatch.setattr(
        migrations_module,
        "_record_schema_versions",
        fail_before_schema_record,
    )
    with pytest.raises(RuntimeError, match="intentional migration failure"):
        run_migrations(postgres_engine)

    monkeypatch.setattr(
        migrations_module,
        "_record_schema_versions",
        original_record_schema_versions,
    )
    assert _table_exists(postgres_engine, "research_archive_cursors") is False
    assert _table_exists(postgres_engine, "schema_migrations") is False

    run_migrations(postgres_engine)
    with postgres_engine.begin() as connection:
        connection.execute(text("DELETE FROM research_archive_cursors"))
    run_migrations(postgres_engine)
    assert _cursor_count(postgres_engine) == 6

    with postgres_engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE research_archive_cursors
                SET selector_mode = 'TAIL',
                    source_cursor = 42,
                    bootstrap_complete = TRUE,
                    updated_at = CURRENT_TIMESTAMP
                WHERE source_table = 'reference_ticks'
                """
            )
        )

    run_migrations(postgres_engine)
    with postgres_engine.connect() as connection:
        preserved = connection.execute(
            text(
                """
                SELECT selector_mode, source_cursor, bootstrap_complete
                FROM research_archive_cursors
                WHERE source_table = 'reference_ticks'
                """
            )
        ).one()
        assert (preserved.selector_mode, preserved.source_cursor) == ("TAIL", 42)
        assert preserved.bootstrap_complete is True
        assert connection.scalar(text("SELECT COUNT(*) FROM research_archive_cursors")) == 6
        assert connection.scalar(
            text(
                """
                SELECT COUNT(*)
                FROM research_archive_cursors
                WHERE bootstrap_complete IS FALSE
                """
            )
        ) == 5

    run_migrations(postgres_engine)
    with postgres_engine.connect() as connection:
        preserved_again = connection.execute(
            text(
                """
                SELECT selector_mode, source_cursor, bootstrap_complete
                FROM research_archive_cursors
                WHERE source_table = 'reference_ticks'
                """
            )
        ).one()
        assert (preserved_again.selector_mode, preserved_again.source_cursor) == ("TAIL", 42)
        assert preserved_again.bootstrap_complete is True
