from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from urllib.parse import urlsplit

from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

DEFAULT_KALSHI_API_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
DEFAULT_KALSHI_BTC15_SERIES_TICKER = "KXBTC15M"
DEFAULT_KALSHI_RESOLVER_PARSER_VERSION = "btc15_resolver_v1"


class ConfigError(ValueError):
    """Raised when environment configuration is invalid."""


class AppMode(StrEnum):
    OBSERVER = "OBSERVER"
    DRY_RUN = "DRY_RUN"
    PAPER = "PAPER"
    LIVE = "LIVE"


@dataclass(frozen=True)
class AppConfig:
    app_mode: AppMode = AppMode.OBSERVER
    trading_enabled: bool = False
    execute: bool = False
    log_level: str = "INFO"
    env: str = "local"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    worker_poll_seconds: float = 1.0
    database_url: str | None = None
    db_echo: bool = False
    db_pool_size: int = 5
    db_max_overflow: int = 10
    db_statement_timeout_ms: int = 5000
    kalshi_api_base_url: str = DEFAULT_KALSHI_API_BASE_URL
    kalshi_api_key_id: str | None = None
    kalshi_private_key: str | None = None
    kalshi_env: str = "prod"
    kalshi_btc15_series_ticker: str = DEFAULT_KALSHI_BTC15_SERIES_TICKER
    kalshi_rest_timeout_seconds: float = 10.0
    kalshi_resolver_parser_version: str = DEFAULT_KALSHI_RESOLVER_PARSER_VERSION


TRUE_VALUES = {"1", "true", "t", "yes", "y", "on"}
FALSE_VALUES = {"0", "false", "f", "no", "n", "off", ""}


def load_config(env: Mapping[str, str] | None = None) -> AppConfig:
    source = os.environ if env is None else env

    return AppConfig(
        app_mode=_parse_mode(_get(source, "APP_MODE", "OBSERVER")),
        trading_enabled=_parse_bool("TRADING_ENABLED", _get(source, "TRADING_ENABLED", "false")),
        execute=_parse_bool("EXECUTE", _get(source, "EXECUTE", "false")),
        log_level=_get(source, "LOG_LEVEL", "INFO").upper(),
        env=_get(source, "ENV", "local"),
        api_host=_get(source, "API_HOST", "0.0.0.0"),
        api_port=_parse_api_port(source),
        worker_poll_seconds=_parse_float(
            "WORKER_POLL_SECONDS",
            _get(source, "WORKER_POLL_SECONDS", "1.0"),
        ),
        database_url=_optional_database_url(source.get("DATABASE_URL")),
        db_echo=_parse_bool("DB_ECHO", _get(source, "DB_ECHO", "false")),
        db_pool_size=_parse_int("DB_POOL_SIZE", _get(source, "DB_POOL_SIZE", "5")),
        db_max_overflow=_parse_non_negative_int(
            "DB_MAX_OVERFLOW",
            _get(source, "DB_MAX_OVERFLOW", "10"),
        ),
        db_statement_timeout_ms=_parse_int(
            "DB_STATEMENT_TIMEOUT_MS",
            _get(source, "DB_STATEMENT_TIMEOUT_MS", "5000"),
        ),
        kalshi_api_base_url=_parse_url(
            "KALSHI_API_BASE_URL",
            _get(source, "KALSHI_API_BASE_URL", DEFAULT_KALSHI_API_BASE_URL),
        ),
        kalshi_api_key_id=_optional(source.get("KALSHI_API_KEY_ID")),
        kalshi_private_key=_optional(source.get("KALSHI_PRIVATE_KEY")),
        kalshi_env=_get(source, "KALSHI_ENV", "prod").strip().lower() or "prod",
        kalshi_btc15_series_ticker=_get(
            source,
            "KALSHI_BTC15_SERIES_TICKER",
            DEFAULT_KALSHI_BTC15_SERIES_TICKER,
        ).strip()
        or DEFAULT_KALSHI_BTC15_SERIES_TICKER,
        kalshi_rest_timeout_seconds=_parse_float(
            "KALSHI_REST_TIMEOUT_SECONDS",
            _get(source, "KALSHI_REST_TIMEOUT_SECONDS", "10"),
        ),
        kalshi_resolver_parser_version=_get(
            source,
            "KALSHI_RESOLVER_PARSER_VERSION",
            DEFAULT_KALSHI_RESOLVER_PARSER_VERSION,
        ).strip()
        or DEFAULT_KALSHI_RESOLVER_PARSER_VERSION,
    )


def _get(env: Mapping[str, str], key: str, default: str) -> str:
    value = env.get(key)
    if value is None:
        return default
    return value


def _optional(value: str | None) -> str | None:
    if value is None or value == "":
        return None
    return value


def _optional_database_url(value: str | None) -> str | None:
    database_url = _optional(value)
    if database_url is None:
        return None

    try:
        make_url(database_url)
    except ArgumentError as exc:
        raise ConfigError("Invalid DATABASE_URL. Expected a SQLAlchemy database URL.") from exc

    return database_url


def _parse_url(name: str, raw_value: str) -> str:
    value = raw_value.strip().rstrip("/")
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConfigError(f"{name} must use http or https.")

    return value


def _parse_api_port(env: Mapping[str, str]) -> int:
    explicit_api_port = _optional(env.get("API_PORT"))
    if explicit_api_port is not None:
        return _parse_int("API_PORT", explicit_api_port)

    railway_port = _optional(env.get("PORT"))
    if railway_port is not None:
        return _parse_int("PORT", railway_port)

    return 8000


def _parse_mode(raw_value: str) -> AppMode:
    normalized = raw_value.strip().upper()
    try:
        return AppMode(normalized)
    except ValueError as exc:
        allowed = ", ".join(mode.value for mode in AppMode)
        raise ConfigError(
            f"Invalid APP_MODE {raw_value!r}. Expected one of: {allowed}."
        ) from exc


def _parse_bool(name: str, raw_value: str) -> bool:
    normalized = raw_value.strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    raise ConfigError(
        f"Invalid boolean for {name}: {raw_value!r}. "
        "Use true/false, yes/no, on/off, or 1/0."
    )


def _parse_int(name: str, raw_value: str) -> int:
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ConfigError(f"Invalid integer for {name}: {raw_value!r}.") from exc
    if value <= 0:
        raise ConfigError(f"{name} must be greater than 0.")
    return value


def _parse_non_negative_int(name: str, raw_value: str) -> int:
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ConfigError(f"Invalid integer for {name}: {raw_value!r}.") from exc
    if value < 0:
        raise ConfigError(f"{name} must be greater than or equal to 0.")
    return value


def _parse_float(name: str, raw_value: str) -> float:
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ConfigError(f"Invalid number for {name}: {raw_value!r}.") from exc
    if value <= 0:
        raise ConfigError(f"{name} must be greater than 0.")
    return value
