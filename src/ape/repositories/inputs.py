from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

JsonPayload = dict[str, Any] | list[Any] | str | int | float | bool


@dataclass(frozen=True)
class MarketInput:
    market_ticker: str
    event_ticker: str | None = None
    series_ticker: str | None = None
    title: str | None = None
    subtitle: str | None = None
    yes_sub_title: str | None = None
    no_sub_title: str | None = None
    open_time: datetime | None = None
    close_time: datetime | None = None
    expected_expiration_time: datetime | None = None
    expiration_time: datetime | None = None
    latest_expiration_time: datetime | None = None
    settlement_timer_seconds: int | None = None
    rules_primary: str | None = None
    rules_secondary: str | None = None
    functional_strike: Decimal | None = None
    floor_strike: Decimal | None = None
    cap_strike: Decimal | None = None
    custom_strike: Decimal | None = None
    price_level_structure: JsonPayload | None = None
    price_ranges: JsonPayload | None = None
    liquidity_dollars: Decimal | None = None
    raw_payload_hash: str | None = None
    parser_version: str | None = None
    resolver_decision_reason: str | None = None


@dataclass(frozen=True)
class ReferenceTickInput:
    source: str
    received_at: datetime
    parse_status: str
    source_ts: datetime | None = None
    kalshi_received_at: datetime | None = None
    raw_value: str | None = None
    parsed_value: Decimal | None = None
    trailing_60s_avg: Decimal | None = None
    trailing_60s_window_size: int | None = None
    last_60s_windowed_average_15min: Decimal | None = None
    final_minute_average_window_size: int | None = None
    final_minute_average_status: str | None = None
    sequence_number: int | None = None
    subscription_id: str | None = None
    source_age_ms: int | None = None
    raw_payload_hash: str | None = None
    raw_payload: JsonPayload | None = None


@dataclass(frozen=True)
class OrderbookSnapshotInput:
    market_ticker: str
    received_at: datetime
    sequence_number: int | None = None
    yes_bid: Decimal | None = None
    yes_ask: Decimal | None = None
    no_bid: Decimal | None = None
    no_ask: Decimal | None = None
    yes_spread: Decimal | None = None
    no_spread: Decimal | None = None
    yes_bid_size: int | None = None
    yes_ask_size: int | None = None
    no_bid_size: int | None = None
    no_ask_size: int | None = None
    yes_bid_count: Decimal | None = None
    yes_ask_count: Decimal | None = None
    no_bid_count: Decimal | None = None
    no_ask_count: Decimal | None = None
    book_status: str | None = None
    raw_payload_hash: str | None = None
    raw_payload: JsonPayload | None = None


@dataclass(frozen=True)
class PublicTradeInput:
    market_ticker: str
    received_at: datetime
    trade_id: str | None = None
    executed_at: datetime | None = None
    price: Decimal | None = None
    count: int | None = None
    trade_count: Decimal | None = None
    taker_side: str | None = None
    side_inferred: str | None = None
    raw_payload_hash: str | None = None
    raw_payload: JsonPayload | None = None


@dataclass(frozen=True)
class KalshiWsProtocolEventInput:
    event_type: str
    created_at: datetime
    worker_service: str | None = None
    worker_role: str | None = None
    connection_id: str | None = None
    channel: str | None = None
    active_market_ticker: str | None = None
    command_id: int | None = None
    command_type: str | None = None
    command_action: str | None = None
    sid: int | None = None
    expected_sid: int | None = None
    seq: int | None = None
    event_subtype: str | None = None
    raw_code: str | None = None
    raw_message: str | None = None
    close_code: int | None = None
    close_reason: str | None = None
    exception_type: str | None = None
    exception_message: str | None = None
    latency_ms: int | None = None
    round_trip_ms: int | None = None
    ping_sent_at: datetime | None = None
    pong_received_at: datetime | None = None
    server_ping_received_at: datetime | None = None
    client_pong_sent_at: datetime | None = None
    subscription_state_before: str | None = None
    subscription_state_after: str | None = None
    recovery_action: str | None = None
    recovery_result: str | None = None
    raw_payload_hash: str | None = None
    payload_summary_json: JsonPayload | None = None


@dataclass(frozen=True)
class StrategyDecisionInput:
    decision_id: str
    evaluated_at: datetime
    decision_state: str
    primary_reason: str
    app_mode: str
    market_ticker: str | None = None
    candidate_side: str | None = None
    boundary: Decimal | None = None
    brti_value: Decimal | None = None
    distance_bps: Decimal | None = None
    seconds_left: int | None = None
    measurements: JsonPayload | None = None
    blockers: JsonPayload | None = None
    warnings: JsonPayload | None = None
    raw_context_hash: str | None = None


@dataclass(frozen=True)
class StrategyDryRunPositionInput:
    position_id: str
    strategy_id: str
    market_ticker: str
    decision_id: str
    side_candidate: str
    economic_side: str
    opened_at: datetime
    open_price: Decimal
    contract_count: int
    entry_reason: str
    status: str
    boundary: Decimal | None = None
    brti_at_entry: Decimal | None = None
    distance_bps_at_entry: Decimal | None = None
    closed_at: datetime | None = None
    close_price: Decimal | None = None
    close_reason: str | None = None
    realized_pnl_cents: Decimal | None = None
    measurements: JsonPayload | None = None


@dataclass(frozen=True)
class StrategyDryRunEventInput:
    event_id: str
    event_type: str
    occurred_at: datetime
    strategy_id: str | None = None
    position_id: str | None = None
    decision_id: str | None = None
    market_ticker: str | None = None
    side_candidate: str | None = None
    price: Decimal | None = None
    contract_count: int | None = None
    reason: str | None = None
    measurements: JsonPayload | None = None


@dataclass(frozen=True)
class WorkerHeartbeatInput:
    service_name: str
    heartbeat_at: datetime
    app_mode: str
    is_safe: bool
    started_at: datetime | None = None
    metadata: JsonPayload | None = None


@dataclass(frozen=True)
class StorageRetentionRunInput:
    run_id: str
    started_at: datetime
    status: str
    dry_run: bool
    finished_at: datetime | None = None
    duration_ms: int | None = None
    deleted_rows: JsonPayload | None = None
    raw_payload_stripped_rows: JsonPayload | None = None
    table_row_counts_before: JsonPayload | None = None
    table_row_counts_after: JsonPayload | None = None
    table_sizes_before: JsonPayload | None = None
    table_sizes_after: JsonPayload | None = None
    warnings: JsonPayload | None = None
    blockers: JsonPayload | None = None
    error_type: str | None = None
    error_message: str | None = None
