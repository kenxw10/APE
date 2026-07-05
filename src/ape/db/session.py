from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import Session, sessionmaker

from ape.config import AppConfig


class DatabaseConfigError(ValueError):
    """Raised when database access is requested without valid database config."""


def create_engine_from_config(config: AppConfig) -> Engine:
    if not config.database_url:
        raise DatabaseConfigError("DATABASE_URL is not configured.")

    url = make_url(config.database_url)
    if url.drivername in {"postgres", "postgresql"}:
        url = url.set(drivername="postgresql+psycopg")
    engine_kwargs: dict[str, object] = {"echo": config.db_echo, "future": True}
    backend = url.get_backend_name()

    if backend == "sqlite":
        engine_kwargs["connect_args"] = {"check_same_thread": False}
    else:
        engine_kwargs["pool_size"] = config.db_pool_size
        engine_kwargs["max_overflow"] = config.db_max_overflow

        if backend.startswith("postgresql"):
            engine_kwargs["connect_args"] = {
                "options": f"-c statement_timeout={config.db_statement_timeout_ms}"
            }

    return create_engine(url, **engine_kwargs)


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)


def check_database_connection(engine: Engine) -> None:
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
