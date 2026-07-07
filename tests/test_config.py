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
    assert config.kalshi_api_base_url == "https://external-api.kalshi.com/trade-api/v2"
    assert config.kalshi_env == "prod"
    assert config.kalshi_btc15_series_ticker == "KXBTC15M"
    assert config.kalshi_rest_timeout_seconds == 10
    assert config.kalshi_resolver_parser_version == "btc15_resolver_v1"
    assert config.kalshi_ws_base_url == "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
    assert config.kalshi_ws_enabled is False
    assert config.kalshi_ws_connect_timeout_seconds == 10
    assert config.kalshi_ws_heartbeat_timeout_seconds == 30
    assert config.kalshi_ws_reconnect_seconds == 5
    assert config.kalshi_ws_max_reconnect_seconds == 60
    assert config.kalshi_ws_subscribe_orderbook is True
    assert config.kalshi_ws_subscribe_ticker is True
    assert config.kalshi_ws_subscribe_trades is True
    assert config.kalshi_cfbenchmarks_enabled is False
    assert config.kalshi_cfbenchmarks_index_ids == ("BRTI",)
    assert config.kalshi_cfbenchmarks_stale_after_seconds == 3
    assert config.kalshi_cfbenchmarks_max_source_age_ms == 3000
    assert config.kalshi_cfbenchmarks_subscribe_on_worker is True
    assert config.kalshi_cfbenchmarks_persist_raw_payload is True
    assert config.kalshi_cfbenchmarks_dedicated_connection is True
    assert config.kalshi_cfbenchmarks_transport_stale_after_seconds == 5
    assert config.kalshi_cfbenchmarks_persistence_stale_after_seconds == 5
    assert config.kalshi_cfbenchmarks_source_age_warn_ms == 45000
    assert config.kalshi_cfbenchmarks_kalshi_received_warn_ms == 10000
    assert config.kalshi_cfbenchmarks_trade_fresh_ms == 2000
    assert config.kalshi_cfbenchmarks_first_tick_timeout_seconds == 15
    assert config.kalshi_cfbenchmarks_no_valid_tick_reconnect_seconds == 15
    assert config.kalshi_cfbenchmarks_max_consecutive_stale_before_reconnect == 2
    assert config.kalshi_cfbenchmarks_heartbeat_stale_after_seconds == 15
    assert config.kalshi_cfbenchmarks_status_grace_seconds == 3
    assert config.kalshi_cfbenchmarks_recovery_required_fresh_ticks == 2
    assert config.strategy_observer_enabled is False
    assert config.strategy_observer_poll_seconds == 1.0
    assert config.strategy_observer_decision_ttl_seconds == 5
    assert config.strategy_min_boundary_distance_bps == 3.5
    assert config.strategy_reference_max_age_ms == 2000
    assert config.strategy_kalshi_book_max_age_ms == 2000
    assert config.strategy_no_entry_first_seconds == 300
    assert config.strategy_no_entry_last_seconds == 60
    assert config.strategy_min_entry_ask == 0.56
    assert config.strategy_max_entry_ask == 0.78
    assert config.strategy_max_spread_cents == 4
    assert config.storage_retention_enabled is False
    assert config.storage_retention_interval_seconds == 300
    assert config.storage_retention_batch_size == 5000
    assert config.storage_retention_max_run_seconds == 20
    assert config.storage_retention_dry_run is False
    assert config.storage_retention_orderbook_seconds == 7200
    assert config.storage_retention_public_trades_seconds == 86400
    assert config.storage_retention_reference_ticks_seconds == 86400
    assert config.storage_retention_worker_heartbeats_seconds == 21600
    assert config.storage_retention_strategy_decisions_seconds == 1209600
    assert config.storage_retention_markets_seconds == 2592000
    assert config.storage_retention_raw_payload_orderbook_seconds == 900
    assert config.storage_retention_raw_payload_public_trades_seconds == 3600
    assert config.storage_retention_raw_payload_reference_ticks_seconds == 3600
    assert config.storage_retention_status_warn_bytes == 40_000_000_000
    assert config.storage_retention_status_critical_bytes == 47_500_000_000


def test_kalshi_credentials_are_not_required() -> None:
    config = load_config({})

    assert config.kalshi_api_key_id is None
    assert config.kalshi_private_key is None


def test_kalshi_env_vars_parse_without_requiring_credentials() -> None:
    config = load_config(
        {
            "KALSHI_API_BASE_URL": "https://external-api.demo.kalshi.co/trade-api/v2/",
            "KALSHI_API_KEY_ID": "key-id",
            "KALSHI_PRIVATE_KEY": "private-key",
            "KALSHI_ENV": "demo",
            "KALSHI_BTC15_SERIES_TICKER": "KXBTC15M",
            "KALSHI_REST_TIMEOUT_SECONDS": "7.5",
            "KALSHI_RESOLVER_PARSER_VERSION": "btc15_resolver_test",
        }
    )

    assert config.kalshi_api_base_url == "https://external-api.demo.kalshi.co/trade-api/v2"
    assert config.kalshi_api_key_id == "key-id"
    assert config.kalshi_private_key == "private-key"
    assert config.kalshi_env == "demo"
    assert config.kalshi_btc15_series_ticker == "KXBTC15M"
    assert config.kalshi_rest_timeout_seconds == 7.5
    assert config.kalshi_resolver_parser_version == "btc15_resolver_test"


def test_kalshi_websocket_env_vars_parse_safely() -> None:
    config = load_config(
        {
            "KALSHI_WS_BASE_URL": "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2/",
            "KALSHI_WS_ENABLED": "true",
            "KALSHI_WS_CONNECT_TIMEOUT_SECONDS": "8",
            "KALSHI_WS_HEARTBEAT_TIMEOUT_SECONDS": "25",
            "KALSHI_WS_RECONNECT_SECONDS": "3",
            "KALSHI_WS_MAX_RECONNECT_SECONDS": "45",
            "KALSHI_WS_SUBSCRIBE_ORDERBOOK": "false",
            "KALSHI_WS_SUBSCRIBE_TICKER": "true",
            "KALSHI_WS_SUBSCRIBE_TRADES": "false",
        }
    )

    assert config.kalshi_ws_base_url == "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"
    assert config.kalshi_ws_enabled is True
    assert config.kalshi_ws_connect_timeout_seconds == 8
    assert config.kalshi_ws_heartbeat_timeout_seconds == 25
    assert config.kalshi_ws_reconnect_seconds == 3
    assert config.kalshi_ws_max_reconnect_seconds == 45
    assert config.kalshi_ws_subscribe_orderbook is False
    assert config.kalshi_ws_subscribe_ticker is True
    assert config.kalshi_ws_subscribe_trades is False


def test_kalshi_cfbenchmarks_env_vars_parse_safely() -> None:
    config = load_config(
        {
            "KALSHI_CFBENCHMARKS_ENABLED": "true",
            "KALSHI_CFBENCHMARKS_INDEX_IDS": "BRTI",
            "KALSHI_CFBENCHMARKS_STALE_AFTER_SECONDS": "4",
            "KALSHI_CFBENCHMARKS_MAX_SOURCE_AGE_MS": "4000",
            "KALSHI_CFBENCHMARKS_SUBSCRIBE_ON_WORKER": "false",
            "KALSHI_CFBENCHMARKS_PERSIST_RAW_PAYLOAD": "false",
            "KALSHI_CFBENCHMARKS_DEDICATED_CONNECTION": "false",
            "KALSHI_CFBENCHMARKS_TRANSPORT_STALE_AFTER_SECONDS": "6",
            "KALSHI_CFBENCHMARKS_PERSISTENCE_STALE_AFTER_SECONDS": "7",
            "KALSHI_CFBENCHMARKS_SOURCE_AGE_WARN_MS": "45001",
            "KALSHI_CFBENCHMARKS_KALSHI_RECEIVED_WARN_MS": "10001",
            "KALSHI_CFBENCHMARKS_TRADE_FRESH_MS": "2001",
            "KALSHI_CFBENCHMARKS_FIRST_TICK_TIMEOUT_SECONDS": "16",
            "KALSHI_CFBENCHMARKS_NO_VALID_TICK_RECONNECT_SECONDS": "17",
            "KALSHI_CFBENCHMARKS_MAX_CONSECUTIVE_STALE_BEFORE_RECONNECT": "3",
            "KALSHI_CFBENCHMARKS_HEARTBEAT_STALE_AFTER_SECONDS": "18",
            "KALSHI_CFBENCHMARKS_STATUS_GRACE_SECONDS": "4",
            "KALSHI_CFBENCHMARKS_RECOVERY_REQUIRED_FRESH_TICKS": "5",
        }
    )

    assert config.kalshi_cfbenchmarks_enabled is True
    assert config.kalshi_cfbenchmarks_index_ids == ("BRTI",)
    assert config.kalshi_cfbenchmarks_stale_after_seconds == 4
    assert config.kalshi_cfbenchmarks_max_source_age_ms == 4000
    assert config.kalshi_cfbenchmarks_subscribe_on_worker is False
    assert config.kalshi_cfbenchmarks_persist_raw_payload is False
    assert config.kalshi_cfbenchmarks_dedicated_connection is False
    assert config.kalshi_cfbenchmarks_transport_stale_after_seconds == 6
    assert config.kalshi_cfbenchmarks_persistence_stale_after_seconds == 7
    assert config.kalshi_cfbenchmarks_source_age_warn_ms == 45001
    assert config.kalshi_cfbenchmarks_kalshi_received_warn_ms == 10001
    assert config.kalshi_cfbenchmarks_trade_fresh_ms == 2001
    assert config.kalshi_cfbenchmarks_first_tick_timeout_seconds == 16
    assert config.kalshi_cfbenchmarks_no_valid_tick_reconnect_seconds == 17
    assert config.kalshi_cfbenchmarks_max_consecutive_stale_before_reconnect == 3
    assert config.kalshi_cfbenchmarks_heartbeat_stale_after_seconds == 18
    assert config.kalshi_cfbenchmarks_status_grace_seconds == 4
    assert config.kalshi_cfbenchmarks_recovery_required_fresh_ticks == 5


def test_invalid_kalshi_cfbenchmarks_index_ids_raise_clear_config_error() -> None:
    with pytest.raises(ConfigError, match="KALSHI_CFBENCHMARKS_INDEX_IDS"):
        load_config({"KALSHI_CFBENCHMARKS_INDEX_IDS": " , "})


def test_strategy_observer_env_vars_parse_safely() -> None:
    config = load_config(
        {
            "STRATEGY_OBSERVER_ENABLED": "true",
            "STRATEGY_OBSERVER_POLL_SECONDS": "2",
            "STRATEGY_OBSERVER_DECISION_TTL_SECONDS": "6",
            "STRATEGY_MIN_BOUNDARY_DISTANCE_BPS": "4.5",
            "STRATEGY_REFERENCE_MAX_AGE_MS": "2500",
            "STRATEGY_KALSHI_BOOK_MAX_AGE_MS": "2600",
            "STRATEGY_NO_ENTRY_FIRST_SECONDS": "301",
            "STRATEGY_NO_ENTRY_LAST_SECONDS": "61",
            "STRATEGY_MIN_ENTRY_ASK": "0.57",
            "STRATEGY_MAX_ENTRY_ASK": "0.79",
            "STRATEGY_MAX_SPREAD_CENTS": "5",
        }
    )

    assert config.strategy_observer_enabled is True
    assert config.strategy_observer_poll_seconds == 2
    assert config.strategy_observer_decision_ttl_seconds == 6
    assert config.strategy_min_boundary_distance_bps == 4.5
    assert config.strategy_reference_max_age_ms == 2500
    assert config.strategy_kalshi_book_max_age_ms == 2600
    assert config.strategy_no_entry_first_seconds == 301
    assert config.strategy_no_entry_last_seconds == 61
    assert config.strategy_min_entry_ask == 0.57
    assert config.strategy_max_entry_ask == 0.79
    assert config.strategy_max_spread_cents == 5


def test_storage_retention_env_vars_parse_safely() -> None:
    config = load_config(
        {
            "STORAGE_RETENTION_ENABLED": "true",
            "STORAGE_RETENTION_INTERVAL_SECONDS": "301",
            "STORAGE_RETENTION_BATCH_SIZE": "123",
            "STORAGE_RETENTION_MAX_RUN_SECONDS": "21",
            "STORAGE_RETENTION_DRY_RUN": "true",
            "STORAGE_RETENTION_ORDERBOOK_SECONDS": "7201",
            "STORAGE_RETENTION_PUBLIC_TRADES_SECONDS": "86401",
            "STORAGE_RETENTION_REFERENCE_TICKS_SECONDS": "86402",
            "STORAGE_RETENTION_WORKER_HEARTBEATS_SECONDS": "21601",
            "STORAGE_RETENTION_STRATEGY_DECISIONS_SECONDS": "1209601",
            "STORAGE_RETENTION_MARKETS_SECONDS": "2592001",
            "STORAGE_RETENTION_RAW_PAYLOAD_ORDERBOOK_SECONDS": "901",
            "STORAGE_RETENTION_RAW_PAYLOAD_PUBLIC_TRADES_SECONDS": "3601",
            "STORAGE_RETENTION_RAW_PAYLOAD_REFERENCE_TICKS_SECONDS": "3602",
            "STORAGE_RETENTION_STATUS_WARN_BYTES": "40000000001",
            "STORAGE_RETENTION_STATUS_CRITICAL_BYTES": "47500000001",
        }
    )

    assert config.storage_retention_enabled is True
    assert config.storage_retention_interval_seconds == 301
    assert config.storage_retention_batch_size == 123
    assert config.storage_retention_max_run_seconds == 21
    assert config.storage_retention_dry_run is True
    assert config.storage_retention_orderbook_seconds == 7201
    assert config.storage_retention_public_trades_seconds == 86401
    assert config.storage_retention_reference_ticks_seconds == 86402
    assert config.storage_retention_worker_heartbeats_seconds == 21601
    assert config.storage_retention_strategy_decisions_seconds == 1209601
    assert config.storage_retention_markets_seconds == 2592001
    assert config.storage_retention_raw_payload_orderbook_seconds == 901
    assert config.storage_retention_raw_payload_public_trades_seconds == 3601
    assert config.storage_retention_raw_payload_reference_ticks_seconds == 3602
    assert config.storage_retention_status_warn_bytes == 40_000_000_001
    assert config.storage_retention_status_critical_bytes == 47_500_000_001


def test_invalid_kalshi_api_base_url_raises_clear_config_error() -> None:
    with pytest.raises(ConfigError, match="KALSHI_API_BASE_URL"):
        load_config({"KALSHI_API_BASE_URL": "not-a-url"})


def test_kalshi_api_base_url_rejects_plaintext_http() -> None:
    with pytest.raises(ConfigError, match="must use https"):
        load_config({"KALSHI_API_BASE_URL": "http://external-api.kalshi.com/trade-api/v2"})


def test_kalshi_websocket_base_url_rejects_plaintext_ws() -> None:
    with pytest.raises(ConfigError, match="must use wss"):
        load_config({"KALSHI_WS_BASE_URL": "ws://external-api-ws.kalshi.com/trade-api/ws/v2"})


def test_api_port_defaults_to_8000() -> None:
    config = load_config({})

    assert config.api_port == 8000


def test_port_is_used_when_api_port_is_unset() -> None:
    config = load_config({"PORT": "9000"})

    assert config.api_port == 9000


def test_api_port_overrides_port_when_both_are_set() -> None:
    config = load_config({"PORT": "9000", "API_PORT": "7000"})

    assert config.api_port == 7000


def test_invalid_port_raises_clear_config_error_when_api_port_unset() -> None:
    with pytest.raises(ConfigError, match="Invalid integer for PORT"):
        load_config({"PORT": "not-a-port"})


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
        {"STORAGE_RETENTION_INTERVAL_SECONDS": "0"},
        {"STORAGE_RETENTION_BATCH_SIZE": "0"},
        {"STORAGE_RETENTION_MAX_RUN_SECONDS": "-1"},
        {"STORAGE_RETENTION_ORDERBOOK_SECONDS": "0"},
        {"STORAGE_RETENTION_STATUS_WARN_BYTES": "0"},
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
