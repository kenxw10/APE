from __future__ import annotations

from ape.config import load_config
from ape.db.session import create_engine_from_config


def test_plain_postgresql_url_uses_psycopg_driver_without_connecting() -> None:
    engine = create_engine_from_config(
        load_config({"DATABASE_URL": "postgresql://user:password@localhost:5432/ape"})
    )

    try:
        assert engine.url.drivername == "postgresql+psycopg"
    finally:
        engine.dispose()
