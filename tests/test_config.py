from __future__ import annotations

import pytest

from ape.config import AppMode, ConfigError, load_config


def test_default_config_is_observer_only() -> None:
    config = load_config({})

    assert config.app_mode is AppMode.OBSERVER
    assert config.trading_enabled is False
    assert config.execute is False
    assert config.database_url is None
    assert config.db_echo is False
    assert config.db_pool_size == 5
    assert config.db_max_overflow == 10
    assert config.db_statement_timeout_ms == 5000


def test_kalshi_credentials_are_not_required() -> None:
    config = load_config({})

    assert config.kalshi_api_key_id is None
    assert config.kalshi_private_key is None


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("true", True),
        ("1", True),
        ("yes", True),
        ("on", True),
        ("false", False),
        ("0", False),
        ("no", False),
        ("off", False),
    ],
)
def test_common_boolean_strings_parse_safely(raw_value: str, expected: bool) -> None:
    config = load_config({"TRADING_ENABLED": raw_value})

    assert config.trading_enabled is expected


def test_invalid_boolean_raises_clear_config_error() -> None:
    with pytest.raises(ConfigError, match="Invalid boolean"):
        load_config({"EXECUTE": "maybe"})


def test_invalid_db_echo_raises_clear_config_error() -> None:
    with pytest.raises(ConfigError, match="Invalid boolean"):
        load_config({"DB_ECHO": "sometimes"})


def test_invalid_database_url_raises_clear_config_error() -> None:
    with pytest.raises(ConfigError, match="Invalid DATABASE_URL"):
        load_config({"DATABASE_URL": "not a database url"})


@pytest.mark.parametrize(
    "env",
    [
        {"DB_POOL_SIZE": "0"},
        {"DB_MAX_OVERFLOW": "-1"},
        {"DB_STATEMENT_TIMEOUT_MS": "0"},
    ],
)
def test_invalid_db_numeric_config_raises_clear_config_error(env: dict[str, str]) -> None:
    with pytest.raises(ConfigError):
        load_config(env)


def test_invalid_app_mode_raises_clear_config_error() -> None:
    with pytest.raises(ConfigError, match="Invalid APP_MODE"):
        load_config({"APP_MODE": "UNKNOWN"})


@pytest.mark.parametrize("mode", ["OBSERVER", "DRY_RUN", "PAPER", "LIVE"])
def test_supported_app_modes_parse(mode: str) -> None:
    config = load_config({"APP_MODE": mode})

    assert config.app_mode.value == mode
