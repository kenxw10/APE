from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum


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
    kalshi_api_key_id: str | None = None
    kalshi_private_key: str | None = None


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
        api_port=_parse_int("API_PORT", _get(source, "API_PORT", "8000")),
        worker_poll_seconds=_parse_float(
            "WORKER_POLL_SECONDS",
            _get(source, "WORKER_POLL_SECONDS", "1.0"),
        ),
        database_url=_optional(source.get("DATABASE_URL")),
        kalshi_api_key_id=_optional(source.get("KALSHI_API_KEY_ID")),
        kalshi_private_key=_optional(source.get("KALSHI_PRIVATE_KEY")),
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


def _parse_float(name: str, raw_value: str) -> float:
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ConfigError(f"Invalid number for {name}: {raw_value!r}.") from exc
    if value <= 0:
        raise ConfigError(f"{name} must be greater than 0.")
    return value
