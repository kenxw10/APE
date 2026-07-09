from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from urllib.parse import urlsplit

from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

DEFAULT_KALSHI_API_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
DEFAULT_KALSHI_WS_BASE_URL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
DEFAULT_KALSHI_BTC15_SERIES_TICKER = "KXBTC15M"
DEFAULT_KALSHI_RESOLVER_PARSER_VERSION = "btc15_resolver_v1"
DEFAULT_KALSHI_CFBENCHMARKS_INDEX_IDS = ("BRTI",)
WORKER_ROLES = {"all", "market-data", "reference-brti", "strategy", "maintenance"}


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
    ape_worker_role: str = "all"
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
    kalshi_ws_base_url: str = DEFAULT_KALSHI_WS_BASE_URL
    kalshi_ws_enabled: bool = False
    kalshi_ws_connect_timeout_seconds: float = 10.0
    kalshi_ws_heartbeat_timeout_seconds: float = 30.0
    kalshi_ws_reconnect_seconds: float = 5.0
    kalshi_ws_max_reconnect_seconds: float = 60.0
    kalshi_ws_snapshot_min_interval_seconds: float = 1.0
    kalshi_ws_snapshot_timeout_seconds: float = 10.0
    kalshi_ws_db_writer_queue_max_size: int = 1000
    kalshi_ws_db_slow_write_ms: int = 500
    kalshi_ws_subscribe_orderbook: bool = True
    kalshi_ws_subscribe_ticker: bool = True
    kalshi_ws_subscribe_trades: bool = True
    kalshi_cfbenchmarks_enabled: bool = False
    kalshi_cfbenchmarks_index_ids: tuple[str, ...] = DEFAULT_KALSHI_CFBENCHMARKS_INDEX_IDS
    kalshi_cfbenchmarks_stale_after_seconds: float = 3.0
    kalshi_cfbenchmarks_max_source_age_ms: int = 3000
    kalshi_cfbenchmarks_subscribe_on_worker: bool = True
    kalshi_cfbenchmarks_persist_raw_payload: bool = True
    kalshi_cfbenchmarks_dedicated_connection: bool = True
    kalshi_cfbenchmarks_transport_stale_after_seconds: float = 5.0
    kalshi_cfbenchmarks_persistence_stale_after_seconds: float = 5.0
    kalshi_cfbenchmarks_source_age_warn_ms: int = 45_000
    kalshi_cfbenchmarks_kalshi_received_warn_ms: int = 10_000
    kalshi_cfbenchmarks_trade_fresh_ms: int = 2_000
    kalshi_cfbenchmarks_first_tick_timeout_seconds: float = 15.0
    kalshi_cfbenchmarks_no_valid_tick_reconnect_seconds: float = 15.0
    kalshi_cfbenchmarks_max_consecutive_stale_before_reconnect: int = 2
    kalshi_cfbenchmarks_heartbeat_stale_after_seconds: float = 15.0
    kalshi_cfbenchmarks_status_grace_seconds: float = 3.0
    kalshi_cfbenchmarks_recovery_required_fresh_ticks: int = 2
    strategy_observer_enabled: bool = False
    strategy_observer_poll_seconds: float = 1.0
    strategy_observer_decision_ttl_seconds: float = 5.0
    strategy_dry_run_enabled: bool = False
    strategy_id: str = "btc15_momentum_v1"
    strategy_dry_run_max_open_positions: int = 1
    strategy_dry_run_one_entry_per_market: bool = True
    strategy_dry_run_position_size_contracts: int = 1
    strategy_dry_run_entry_price_offset_cents: int = 1
    strategy_dry_run_min_seconds_between_decisions: float = 1.0
    strategy_brti_lookback_short_seconds: int = 30
    strategy_brti_lookback_medium_seconds: int = 90
    strategy_brti_lookback_long_seconds: int = 180
    strategy_brti_min_move_short_bps: float = 2.0
    strategy_brti_min_move_medium_bps: float = 4.5
    strategy_brti_min_move_long_bps: float = 6.0
    strategy_brti_directional_tick_ratio_min: float = 0.62
    strategy_brti_max_boundary_crosses_90s: int = 1
    strategy_brti_max_retrace_fraction: float = 0.40
    strategy_contract_lookback_seconds: int = 45
    strategy_contract_min_mid_move_cents: int = 4
    strategy_contract_ask_pullback_lookback_seconds: int = 15
    strategy_contract_max_ask_pullback_cents: int = 2
    strategy_trade_confirmation_lookback_seconds: int = 30
    strategy_trade_confirmation_min_ratio: float = 0.60
    strategy_trade_confirmation_min_trades: int = 3
    strategy_min_top_book_size_contracts: int = 2
    strategy_dry_run_max_entry_price: float = 0.78
    strategy_dry_run_min_entry_price: float = 0.56
    strategy_min_boundary_distance_bps: float = 3.5
    strategy_reference_max_age_ms: int = 2_000
    strategy_reference_source_max_age_ms: int = 45_000
    strategy_reference_source_warn_ms: int = 10_000
    strategy_reference_require_trade_ready_fresh: bool = True
    strategy_reference_stream_max_age_ms: int = 3_000
    strategy_reference_carry_forward_max_age_ms: int = 15_000
    strategy_reference_allow_duplicate_source_ts_carry_forward: bool = True
    strategy_kalshi_book_max_age_ms: int = 2_000
    strategy_kalshi_book_stream_max_age_ms: int = 3_000
    strategy_kalshi_book_carry_forward_max_age_ms: int = 30_000
    strategy_kalshi_book_require_stream_live: bool = True
    strategy_no_entry_first_seconds: int = 300
    strategy_no_entry_last_seconds: int = 60
    strategy_min_entry_ask: float = 0.56
    strategy_max_entry_ask: float = 0.78
    strategy_max_spread_cents: int = 4
    storage_retention_enabled: bool = False
    storage_retention_interval_seconds: float = 300.0
    storage_retention_batch_size: int = 5000
    storage_retention_max_run_seconds: float = 20.0
    storage_retention_dry_run: bool = False
    storage_retention_orderbook_seconds: int = 7200
    storage_retention_public_trades_seconds: int = 86400
    storage_retention_reference_ticks_seconds: int = 86400
    storage_retention_worker_heartbeats_seconds: int = 21600
    storage_retention_strategy_decisions_seconds: int = 1209600
    storage_retention_dry_run_positions_seconds: int = 2592000
    storage_retention_dry_run_events_seconds: int = 2592000
    storage_retention_markets_seconds: int = 2592000
    storage_retention_raw_payload_orderbook_seconds: int = 900
    storage_retention_raw_payload_public_trades_seconds: int = 3600
    storage_retention_raw_payload_reference_ticks_seconds: int = 3600
    storage_retention_status_warn_bytes: int = 40_000_000_000
    storage_retention_status_critical_bytes: int = 47_500_000_000


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
        ape_worker_role=_parse_worker_role(_get(source, "APE_WORKER_ROLE", "all")),
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
        kalshi_ws_base_url=_parse_ws_url(
            "KALSHI_WS_BASE_URL",
            _get(source, "KALSHI_WS_BASE_URL", DEFAULT_KALSHI_WS_BASE_URL),
        ),
        kalshi_ws_enabled=_parse_bool(
            "KALSHI_WS_ENABLED",
            _get(source, "KALSHI_WS_ENABLED", "false"),
        ),
        kalshi_ws_connect_timeout_seconds=_parse_float(
            "KALSHI_WS_CONNECT_TIMEOUT_SECONDS",
            _get(source, "KALSHI_WS_CONNECT_TIMEOUT_SECONDS", "10"),
        ),
        kalshi_ws_heartbeat_timeout_seconds=_parse_float(
            "KALSHI_WS_HEARTBEAT_TIMEOUT_SECONDS",
            _get(source, "KALSHI_WS_HEARTBEAT_TIMEOUT_SECONDS", "30"),
        ),
        kalshi_ws_reconnect_seconds=_parse_float(
            "KALSHI_WS_RECONNECT_SECONDS",
            _get(source, "KALSHI_WS_RECONNECT_SECONDS", "5"),
        ),
        kalshi_ws_max_reconnect_seconds=_parse_float(
            "KALSHI_WS_MAX_RECONNECT_SECONDS",
            _get(source, "KALSHI_WS_MAX_RECONNECT_SECONDS", "60"),
        ),
        kalshi_ws_snapshot_min_interval_seconds=_parse_float(
            "KALSHI_WS_SNAPSHOT_MIN_INTERVAL_SECONDS",
            _get(source, "KALSHI_WS_SNAPSHOT_MIN_INTERVAL_SECONDS", "1"),
        ),
        kalshi_ws_snapshot_timeout_seconds=_parse_float(
            "KALSHI_WS_SNAPSHOT_TIMEOUT_SECONDS",
            _get(source, "KALSHI_WS_SNAPSHOT_TIMEOUT_SECONDS", "10"),
        ),
        kalshi_ws_db_writer_queue_max_size=_parse_int(
            "KALSHI_WS_DB_WRITER_QUEUE_MAX_SIZE",
            _get(source, "KALSHI_WS_DB_WRITER_QUEUE_MAX_SIZE", "1000"),
        ),
        kalshi_ws_db_slow_write_ms=_parse_int(
            "KALSHI_WS_DB_SLOW_WRITE_MS",
            _get(source, "KALSHI_WS_DB_SLOW_WRITE_MS", "500"),
        ),
        kalshi_ws_subscribe_orderbook=_parse_bool(
            "KALSHI_WS_SUBSCRIBE_ORDERBOOK",
            _get(source, "KALSHI_WS_SUBSCRIBE_ORDERBOOK", "true"),
        ),
        kalshi_ws_subscribe_ticker=_parse_bool(
            "KALSHI_WS_SUBSCRIBE_TICKER",
            _get(source, "KALSHI_WS_SUBSCRIBE_TICKER", "true"),
        ),
        kalshi_ws_subscribe_trades=_parse_bool(
            "KALSHI_WS_SUBSCRIBE_TRADES",
            _get(source, "KALSHI_WS_SUBSCRIBE_TRADES", "true"),
        ),
        kalshi_cfbenchmarks_enabled=_parse_bool(
            "KALSHI_CFBENCHMARKS_ENABLED",
            _get(source, "KALSHI_CFBENCHMARKS_ENABLED", "false"),
        ),
        kalshi_cfbenchmarks_index_ids=_parse_csv_values(
            "KALSHI_CFBENCHMARKS_INDEX_IDS",
            _get(source, "KALSHI_CFBENCHMARKS_INDEX_IDS", "BRTI"),
        ),
        kalshi_cfbenchmarks_stale_after_seconds=_parse_float(
            "KALSHI_CFBENCHMARKS_STALE_AFTER_SECONDS",
            _get(source, "KALSHI_CFBENCHMARKS_STALE_AFTER_SECONDS", "3"),
        ),
        kalshi_cfbenchmarks_max_source_age_ms=_parse_int(
            "KALSHI_CFBENCHMARKS_MAX_SOURCE_AGE_MS",
            _get(source, "KALSHI_CFBENCHMARKS_MAX_SOURCE_AGE_MS", "3000"),
        ),
        kalshi_cfbenchmarks_subscribe_on_worker=_parse_bool(
            "KALSHI_CFBENCHMARKS_SUBSCRIBE_ON_WORKER",
            _get(source, "KALSHI_CFBENCHMARKS_SUBSCRIBE_ON_WORKER", "true"),
        ),
        kalshi_cfbenchmarks_persist_raw_payload=_parse_bool(
            "KALSHI_CFBENCHMARKS_PERSIST_RAW_PAYLOAD",
            _get(source, "KALSHI_CFBENCHMARKS_PERSIST_RAW_PAYLOAD", "true"),
        ),
        kalshi_cfbenchmarks_dedicated_connection=_parse_bool(
            "KALSHI_CFBENCHMARKS_DEDICATED_CONNECTION",
            _get(source, "KALSHI_CFBENCHMARKS_DEDICATED_CONNECTION", "true"),
        ),
        kalshi_cfbenchmarks_transport_stale_after_seconds=_parse_float(
            "KALSHI_CFBENCHMARKS_TRANSPORT_STALE_AFTER_SECONDS",
            _get(source, "KALSHI_CFBENCHMARKS_TRANSPORT_STALE_AFTER_SECONDS", "5"),
        ),
        kalshi_cfbenchmarks_persistence_stale_after_seconds=_parse_float(
            "KALSHI_CFBENCHMARKS_PERSISTENCE_STALE_AFTER_SECONDS",
            _get(source, "KALSHI_CFBENCHMARKS_PERSISTENCE_STALE_AFTER_SECONDS", "5"),
        ),
        kalshi_cfbenchmarks_source_age_warn_ms=_parse_int(
            "KALSHI_CFBENCHMARKS_SOURCE_AGE_WARN_MS",
            _get(source, "KALSHI_CFBENCHMARKS_SOURCE_AGE_WARN_MS", "45000"),
        ),
        kalshi_cfbenchmarks_kalshi_received_warn_ms=_parse_int(
            "KALSHI_CFBENCHMARKS_KALSHI_RECEIVED_WARN_MS",
            _get(source, "KALSHI_CFBENCHMARKS_KALSHI_RECEIVED_WARN_MS", "10000"),
        ),
        kalshi_cfbenchmarks_trade_fresh_ms=_parse_int(
            "KALSHI_CFBENCHMARKS_TRADE_FRESH_MS",
            _get(source, "KALSHI_CFBENCHMARKS_TRADE_FRESH_MS", "2000"),
        ),
        kalshi_cfbenchmarks_first_tick_timeout_seconds=_parse_float(
            "KALSHI_CFBENCHMARKS_FIRST_TICK_TIMEOUT_SECONDS",
            _get(source, "KALSHI_CFBENCHMARKS_FIRST_TICK_TIMEOUT_SECONDS", "15"),
        ),
        kalshi_cfbenchmarks_no_valid_tick_reconnect_seconds=_parse_float(
            "KALSHI_CFBENCHMARKS_NO_VALID_TICK_RECONNECT_SECONDS",
            _get(source, "KALSHI_CFBENCHMARKS_NO_VALID_TICK_RECONNECT_SECONDS", "15"),
        ),
        kalshi_cfbenchmarks_max_consecutive_stale_before_reconnect=(
            _parse_non_negative_int(
                "KALSHI_CFBENCHMARKS_MAX_CONSECUTIVE_STALE_BEFORE_RECONNECT",
                _get(
                    source,
                    "KALSHI_CFBENCHMARKS_MAX_CONSECUTIVE_STALE_BEFORE_RECONNECT",
                    "2",
                ),
            )
        ),
        kalshi_cfbenchmarks_heartbeat_stale_after_seconds=_parse_float(
            "KALSHI_CFBENCHMARKS_HEARTBEAT_STALE_AFTER_SECONDS",
            _get(source, "KALSHI_CFBENCHMARKS_HEARTBEAT_STALE_AFTER_SECONDS", "15"),
        ),
        kalshi_cfbenchmarks_status_grace_seconds=_parse_float(
            "KALSHI_CFBENCHMARKS_STATUS_GRACE_SECONDS",
            _get(source, "KALSHI_CFBENCHMARKS_STATUS_GRACE_SECONDS", "3"),
        ),
        kalshi_cfbenchmarks_recovery_required_fresh_ticks=_parse_non_negative_int(
            "KALSHI_CFBENCHMARKS_RECOVERY_REQUIRED_FRESH_TICKS",
            _get(source, "KALSHI_CFBENCHMARKS_RECOVERY_REQUIRED_FRESH_TICKS", "2"),
        ),
        strategy_observer_enabled=_parse_bool(
            "STRATEGY_OBSERVER_ENABLED",
            _get(source, "STRATEGY_OBSERVER_ENABLED", "false"),
        ),
        strategy_observer_poll_seconds=_parse_float(
            "STRATEGY_OBSERVER_POLL_SECONDS",
            _get(source, "STRATEGY_OBSERVER_POLL_SECONDS", "1.0"),
        ),
        strategy_observer_decision_ttl_seconds=_parse_float(
            "STRATEGY_OBSERVER_DECISION_TTL_SECONDS",
            _get(source, "STRATEGY_OBSERVER_DECISION_TTL_SECONDS", "5"),
        ),
        strategy_dry_run_enabled=_parse_bool(
            "STRATEGY_DRY_RUN_ENABLED",
            _get(source, "STRATEGY_DRY_RUN_ENABLED", "false"),
        ),
        strategy_id=_parse_required_text(
            "STRATEGY_ID",
            _get(source, "STRATEGY_ID", "btc15_momentum_v1"),
        ),
        strategy_dry_run_max_open_positions=_parse_int(
            "STRATEGY_DRY_RUN_MAX_OPEN_POSITIONS",
            _get(source, "STRATEGY_DRY_RUN_MAX_OPEN_POSITIONS", "1"),
        ),
        strategy_dry_run_one_entry_per_market=_parse_bool(
            "STRATEGY_DRY_RUN_ONE_ENTRY_PER_MARKET",
            _get(source, "STRATEGY_DRY_RUN_ONE_ENTRY_PER_MARKET", "true"),
        ),
        strategy_dry_run_position_size_contracts=_parse_int(
            "STRATEGY_DRY_RUN_POSITION_SIZE_CONTRACTS",
            _get(source, "STRATEGY_DRY_RUN_POSITION_SIZE_CONTRACTS", "1"),
        ),
        strategy_dry_run_entry_price_offset_cents=_parse_non_negative_int(
            "STRATEGY_DRY_RUN_ENTRY_PRICE_OFFSET_CENTS",
            _get(source, "STRATEGY_DRY_RUN_ENTRY_PRICE_OFFSET_CENTS", "1"),
        ),
        strategy_dry_run_min_seconds_between_decisions=_parse_float(
            "STRATEGY_DRY_RUN_MIN_SECONDS_BETWEEN_DECISIONS",
            _get(source, "STRATEGY_DRY_RUN_MIN_SECONDS_BETWEEN_DECISIONS", "1"),
        ),
        strategy_brti_lookback_short_seconds=_parse_int(
            "STRATEGY_BRTI_LOOKBACK_SHORT_SECONDS",
            _get(source, "STRATEGY_BRTI_LOOKBACK_SHORT_SECONDS", "30"),
        ),
        strategy_brti_lookback_medium_seconds=_parse_int(
            "STRATEGY_BRTI_LOOKBACK_MEDIUM_SECONDS",
            _get(source, "STRATEGY_BRTI_LOOKBACK_MEDIUM_SECONDS", "90"),
        ),
        strategy_brti_lookback_long_seconds=_parse_int(
            "STRATEGY_BRTI_LOOKBACK_LONG_SECONDS",
            _get(source, "STRATEGY_BRTI_LOOKBACK_LONG_SECONDS", "180"),
        ),
        strategy_brti_min_move_short_bps=_parse_float(
            "STRATEGY_BRTI_MIN_MOVE_SHORT_BPS",
            _get(source, "STRATEGY_BRTI_MIN_MOVE_SHORT_BPS", "2.0"),
        ),
        strategy_brti_min_move_medium_bps=_parse_float(
            "STRATEGY_BRTI_MIN_MOVE_MEDIUM_BPS",
            _get(source, "STRATEGY_BRTI_MIN_MOVE_MEDIUM_BPS", "4.5"),
        ),
        strategy_brti_min_move_long_bps=_parse_float(
            "STRATEGY_BRTI_MIN_MOVE_LONG_BPS",
            _get(source, "STRATEGY_BRTI_MIN_MOVE_LONG_BPS", "6.0"),
        ),
        strategy_brti_directional_tick_ratio_min=_parse_float(
            "STRATEGY_BRTI_DIRECTIONAL_TICK_RATIO_MIN",
            _get(source, "STRATEGY_BRTI_DIRECTIONAL_TICK_RATIO_MIN", "0.62"),
        ),
        strategy_brti_max_boundary_crosses_90s=_parse_non_negative_int(
            "STRATEGY_BRTI_MAX_BOUNDARY_CROSSES_90S",
            _get(source, "STRATEGY_BRTI_MAX_BOUNDARY_CROSSES_90S", "1"),
        ),
        strategy_brti_max_retrace_fraction=_parse_float(
            "STRATEGY_BRTI_MAX_RETRACE_FRACTION",
            _get(source, "STRATEGY_BRTI_MAX_RETRACE_FRACTION", "0.40"),
        ),
        strategy_contract_lookback_seconds=_parse_int(
            "STRATEGY_CONTRACT_LOOKBACK_SECONDS",
            _get(source, "STRATEGY_CONTRACT_LOOKBACK_SECONDS", "45"),
        ),
        strategy_contract_min_mid_move_cents=_parse_int(
            "STRATEGY_CONTRACT_MIN_MID_MOVE_CENTS",
            _get(source, "STRATEGY_CONTRACT_MIN_MID_MOVE_CENTS", "4"),
        ),
        strategy_contract_ask_pullback_lookback_seconds=_parse_int(
            "STRATEGY_CONTRACT_ASK_PULLBACK_LOOKBACK_SECONDS",
            _get(source, "STRATEGY_CONTRACT_ASK_PULLBACK_LOOKBACK_SECONDS", "15"),
        ),
        strategy_contract_max_ask_pullback_cents=_parse_non_negative_int(
            "STRATEGY_CONTRACT_MAX_ASK_PULLBACK_CENTS",
            _get(source, "STRATEGY_CONTRACT_MAX_ASK_PULLBACK_CENTS", "2"),
        ),
        strategy_trade_confirmation_lookback_seconds=_parse_int(
            "STRATEGY_TRADE_CONFIRMATION_LOOKBACK_SECONDS",
            _get(source, "STRATEGY_TRADE_CONFIRMATION_LOOKBACK_SECONDS", "30"),
        ),
        strategy_trade_confirmation_min_ratio=_parse_float(
            "STRATEGY_TRADE_CONFIRMATION_MIN_RATIO",
            _get(source, "STRATEGY_TRADE_CONFIRMATION_MIN_RATIO", "0.60"),
        ),
        strategy_trade_confirmation_min_trades=_parse_int(
            "STRATEGY_TRADE_CONFIRMATION_MIN_TRADES",
            _get(source, "STRATEGY_TRADE_CONFIRMATION_MIN_TRADES", "3"),
        ),
        strategy_min_top_book_size_contracts=_parse_int(
            "STRATEGY_MIN_TOP_BOOK_SIZE_CONTRACTS",
            _get(source, "STRATEGY_MIN_TOP_BOOK_SIZE_CONTRACTS", "2"),
        ),
        strategy_dry_run_max_entry_price=_parse_float(
            "STRATEGY_DRY_RUN_MAX_ENTRY_PRICE",
            _get(source, "STRATEGY_DRY_RUN_MAX_ENTRY_PRICE", "0.78"),
        ),
        strategy_dry_run_min_entry_price=_parse_float(
            "STRATEGY_DRY_RUN_MIN_ENTRY_PRICE",
            _get(source, "STRATEGY_DRY_RUN_MIN_ENTRY_PRICE", "0.56"),
        ),
        strategy_min_boundary_distance_bps=_parse_float(
            "STRATEGY_MIN_BOUNDARY_DISTANCE_BPS",
            _get(source, "STRATEGY_MIN_BOUNDARY_DISTANCE_BPS", "3.5"),
        ),
        strategy_reference_max_age_ms=_parse_int(
            "STRATEGY_REFERENCE_MAX_AGE_MS",
            _get(source, "STRATEGY_REFERENCE_MAX_AGE_MS", "2000"),
        ),
        strategy_reference_source_max_age_ms=_parse_int(
            "STRATEGY_REFERENCE_SOURCE_MAX_AGE_MS",
            _get(source, "STRATEGY_REFERENCE_SOURCE_MAX_AGE_MS", "45000"),
        ),
        strategy_reference_source_warn_ms=_parse_int(
            "STRATEGY_REFERENCE_SOURCE_WARN_MS",
            _get(source, "STRATEGY_REFERENCE_SOURCE_WARN_MS", "10000"),
        ),
        strategy_reference_require_trade_ready_fresh=_parse_bool(
            "STRATEGY_REFERENCE_REQUIRE_TRADE_READY_FRESH",
            _get(source, "STRATEGY_REFERENCE_REQUIRE_TRADE_READY_FRESH", "true"),
        ),
        strategy_reference_stream_max_age_ms=_parse_int(
            "STRATEGY_REFERENCE_STREAM_MAX_AGE_MS",
            _get(source, "STRATEGY_REFERENCE_STREAM_MAX_AGE_MS", "3000"),
        ),
        strategy_reference_carry_forward_max_age_ms=_parse_int(
            "STRATEGY_REFERENCE_CARRY_FORWARD_MAX_AGE_MS",
            _get(source, "STRATEGY_REFERENCE_CARRY_FORWARD_MAX_AGE_MS", "15000"),
        ),
        strategy_reference_allow_duplicate_source_ts_carry_forward=_parse_bool(
            "STRATEGY_REFERENCE_ALLOW_DUPLICATE_SOURCE_TS_CARRY_FORWARD",
            _get(
                source,
                "STRATEGY_REFERENCE_ALLOW_DUPLICATE_SOURCE_TS_CARRY_FORWARD",
                "true",
            ),
        ),
        strategy_kalshi_book_max_age_ms=_parse_int(
            "STRATEGY_KALSHI_BOOK_MAX_AGE_MS",
            _get(source, "STRATEGY_KALSHI_BOOK_MAX_AGE_MS", "2000"),
        ),
        strategy_kalshi_book_stream_max_age_ms=_parse_int(
            "STRATEGY_KALSHI_BOOK_STREAM_MAX_AGE_MS",
            _get(source, "STRATEGY_KALSHI_BOOK_STREAM_MAX_AGE_MS", "3000"),
        ),
        strategy_kalshi_book_carry_forward_max_age_ms=_parse_int(
            "STRATEGY_KALSHI_BOOK_CARRY_FORWARD_MAX_AGE_MS",
            _get(source, "STRATEGY_KALSHI_BOOK_CARRY_FORWARD_MAX_AGE_MS", "30000"),
        ),
        strategy_kalshi_book_require_stream_live=_parse_bool(
            "STRATEGY_KALSHI_BOOK_REQUIRE_STREAM_LIVE",
            _get(source, "STRATEGY_KALSHI_BOOK_REQUIRE_STREAM_LIVE", "true"),
        ),
        strategy_no_entry_first_seconds=_parse_int(
            "STRATEGY_NO_ENTRY_FIRST_SECONDS",
            _get(source, "STRATEGY_NO_ENTRY_FIRST_SECONDS", "300"),
        ),
        strategy_no_entry_last_seconds=_parse_int(
            "STRATEGY_NO_ENTRY_LAST_SECONDS",
            _get(source, "STRATEGY_NO_ENTRY_LAST_SECONDS", "60"),
        ),
        strategy_min_entry_ask=_parse_float(
            "STRATEGY_MIN_ENTRY_ASK",
            _get(source, "STRATEGY_MIN_ENTRY_ASK", "0.56"),
        ),
        strategy_max_entry_ask=_parse_float(
            "STRATEGY_MAX_ENTRY_ASK",
            _get(source, "STRATEGY_MAX_ENTRY_ASK", "0.78"),
        ),
        strategy_max_spread_cents=_parse_int(
            "STRATEGY_MAX_SPREAD_CENTS",
            _get(source, "STRATEGY_MAX_SPREAD_CENTS", "4"),
        ),
        storage_retention_enabled=_parse_bool(
            "STORAGE_RETENTION_ENABLED",
            _get(source, "STORAGE_RETENTION_ENABLED", "false"),
        ),
        storage_retention_interval_seconds=_parse_float(
            "STORAGE_RETENTION_INTERVAL_SECONDS",
            _get(source, "STORAGE_RETENTION_INTERVAL_SECONDS", "300"),
        ),
        storage_retention_batch_size=_parse_int(
            "STORAGE_RETENTION_BATCH_SIZE",
            _get(source, "STORAGE_RETENTION_BATCH_SIZE", "5000"),
        ),
        storage_retention_max_run_seconds=_parse_float(
            "STORAGE_RETENTION_MAX_RUN_SECONDS",
            _get(source, "STORAGE_RETENTION_MAX_RUN_SECONDS", "20"),
        ),
        storage_retention_dry_run=_parse_bool(
            "STORAGE_RETENTION_DRY_RUN",
            _get(source, "STORAGE_RETENTION_DRY_RUN", "false"),
        ),
        storage_retention_orderbook_seconds=_parse_int(
            "STORAGE_RETENTION_ORDERBOOK_SECONDS",
            _get(source, "STORAGE_RETENTION_ORDERBOOK_SECONDS", "7200"),
        ),
        storage_retention_public_trades_seconds=_parse_int(
            "STORAGE_RETENTION_PUBLIC_TRADES_SECONDS",
            _get(source, "STORAGE_RETENTION_PUBLIC_TRADES_SECONDS", "86400"),
        ),
        storage_retention_reference_ticks_seconds=_parse_int(
            "STORAGE_RETENTION_REFERENCE_TICKS_SECONDS",
            _get(source, "STORAGE_RETENTION_REFERENCE_TICKS_SECONDS", "86400"),
        ),
        storage_retention_worker_heartbeats_seconds=_parse_int(
            "STORAGE_RETENTION_WORKER_HEARTBEATS_SECONDS",
            _get(source, "STORAGE_RETENTION_WORKER_HEARTBEATS_SECONDS", "21600"),
        ),
        storage_retention_strategy_decisions_seconds=_parse_int(
            "STORAGE_RETENTION_STRATEGY_DECISIONS_SECONDS",
            _get(source, "STORAGE_RETENTION_STRATEGY_DECISIONS_SECONDS", "1209600"),
        ),
        storage_retention_dry_run_positions_seconds=_parse_int(
            "STORAGE_RETENTION_DRY_RUN_POSITIONS_SECONDS",
            _get(source, "STORAGE_RETENTION_DRY_RUN_POSITIONS_SECONDS", "2592000"),
        ),
        storage_retention_dry_run_events_seconds=_parse_int(
            "STORAGE_RETENTION_DRY_RUN_EVENTS_SECONDS",
            _get(source, "STORAGE_RETENTION_DRY_RUN_EVENTS_SECONDS", "2592000"),
        ),
        storage_retention_markets_seconds=_parse_int(
            "STORAGE_RETENTION_MARKETS_SECONDS",
            _get(source, "STORAGE_RETENTION_MARKETS_SECONDS", "2592000"),
        ),
        storage_retention_raw_payload_orderbook_seconds=_parse_int(
            "STORAGE_RETENTION_RAW_PAYLOAD_ORDERBOOK_SECONDS",
            _get(source, "STORAGE_RETENTION_RAW_PAYLOAD_ORDERBOOK_SECONDS", "900"),
        ),
        storage_retention_raw_payload_public_trades_seconds=_parse_int(
            "STORAGE_RETENTION_RAW_PAYLOAD_PUBLIC_TRADES_SECONDS",
            _get(source, "STORAGE_RETENTION_RAW_PAYLOAD_PUBLIC_TRADES_SECONDS", "3600"),
        ),
        storage_retention_raw_payload_reference_ticks_seconds=_parse_int(
            "STORAGE_RETENTION_RAW_PAYLOAD_REFERENCE_TICKS_SECONDS",
            _get(source, "STORAGE_RETENTION_RAW_PAYLOAD_REFERENCE_TICKS_SECONDS", "3600"),
        ),
        storage_retention_status_warn_bytes=_parse_int(
            "STORAGE_RETENTION_STATUS_WARN_BYTES",
            _get(source, "STORAGE_RETENTION_STATUS_WARN_BYTES", "40000000000"),
        ),
        storage_retention_status_critical_bytes=_parse_int(
            "STORAGE_RETENTION_STATUS_CRITICAL_BYTES",
            _get(source, "STORAGE_RETENTION_STATUS_CRITICAL_BYTES", "47500000000"),
        ),
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
    if parsed.scheme != "https" or not parsed.netloc:
        raise ConfigError(f"{name} must use https.")

    return value


def _parse_ws_url(name: str, raw_value: str) -> str:
    value = raw_value.strip().rstrip("/")
    parsed = urlsplit(value)
    if parsed.scheme != "wss" or not parsed.netloc:
        raise ConfigError(f"{name} must use wss.")

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


def _parse_worker_role(raw_value: str) -> str:
    normalized = raw_value.strip().lower().replace("_", "-")
    if normalized in WORKER_ROLES:
        return normalized
    allowed = ", ".join(sorted(WORKER_ROLES))
    raise ConfigError(
        f"Invalid APE_WORKER_ROLE {raw_value!r}. Expected one of: {allowed}."
    )


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


def _parse_required_text(name: str, raw_value: str) -> str:
    value = raw_value.strip()
    if not value:
        raise ConfigError(f"{name} must not be empty.")
    return value


def _parse_float(name: str, raw_value: str) -> float:
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ConfigError(f"Invalid number for {name}: {raw_value!r}.") from exc
    if value <= 0:
        raise ConfigError(f"{name} must be greater than 0.")
    return value


def _parse_csv_values(name: str, raw_value: str) -> tuple[str, ...]:
    values = tuple(item.strip().upper() for item in raw_value.split(",") if item.strip())
    if not values:
        raise ConfigError(f"{name} must contain at least one value.")
    return values
