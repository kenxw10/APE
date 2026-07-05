from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.engine import Engine

from ape.config import ConfigError, load_config
from ape.db.models import Base, SchemaMigration
from ape.db.session import DatabaseConfigError, create_engine_from_config, create_session_factory

CURRENT_SCHEMA_VERSION = "0001_initial_schema"

LOGGER = logging.getLogger(__name__)


def run_migrations(engine: Engine) -> None:
    Base.metadata.create_all(engine)

    session_factory = create_session_factory(engine)
    with session_factory() as session:
        existing = session.scalar(
            select(SchemaMigration).where(SchemaMigration.version == CURRENT_SCHEMA_VERSION)
        )
        if existing is None:
            session.add(SchemaMigration(version=CURRENT_SCHEMA_VERSION))
            session.commit()


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

