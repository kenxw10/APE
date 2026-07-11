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
    ladder_schema_version: str | None = None
    yes_bid_ladder: JsonPayload | None = None
    yes_ask_ladder: JsonPayload | None = None
    no_bid_ladder: JsonPayload | None = None
    no_ask_ladder: JsonPayload | None = None
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
    strategy_id: str = "btc15_momentum_v1"
    market_ticker: str | None = None
    candidate_side: str | None = None
    boundary: Decimal | None = None
    brti_value: Decimal | None = None
    distance_bps: Decimal | None = None
    seconds_left: int | None = None
    feature_snapshot_id: str | None = None
    strategy_config_version_id: str | None = None
    code_commit_sha: str | None = None
    calibration_run_id: str | None = None
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
    feature_snapshot_id: str | None = None
    strategy_config_version_id: str | None = None
    code_commit_sha: str | None = None
    entry_intent_id: str | None = None
    exit_intent_id: str | None = None
    lifecycle_version: str | None = None
    entry_timing_tier: str | None = None
    entry_score_threshold: Decimal | None = None
    entry_time_stop_seconds: int | None = None
    entry_max_hold_seconds: int | None = None
    entry_score: Decimal | None = None
    entry_edge_lower_bound_cents: Decimal | None = None
    entry_response_residual_cents: Decimal | None = None
    entry_boundary: Decimal | None = None
    entry_standardized_distance: Decimal | None = None
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
    feature_snapshot_id: str | None = None
    strategy_config_version_id: str | None = None
    code_commit_sha: str | None = None
    measurements: JsonPayload | None = None


@dataclass(frozen=True)
class StrategyFeatureSnapshotInput:
    feature_snapshot_id: str
    evaluated_at: datetime
    feature_schema_version: str
    context_hash: str
    market_ticker: str | None = None
    candidate_side: str | None = None
    candidate_mode: str | None = None
    boundary: Decimal | None = None
    current_brti: Decimal | None = None
    seconds_since_open: int | None = None
    seconds_left: int | None = None
    reference_tick_id: int | None = None
    orderbook_snapshot_id: int | None = None
    public_trade_id: int | None = None
    quality_state: JsonPayload | None = None
    reference_features: JsonPayload | None = None
    contract_features: JsonPayload | None = None
    microstructure_features: JsonPayload | None = None
    execution_features: JsonPayload | None = None


@dataclass(frozen=True)
class StrategyConfigVersionInput:
    strategy_config_version_id: str
    strategy_id: str
    architecture_version: str
    feature_schema_version: str
    parameter_snapshot: JsonPayload
    parameter_hash: str
    code_commit_sha: str
    source: str = "BUILT_IN"


@dataclass(frozen=True)
class StrategyTradeIntentInput:
    intent_id: str
    strategy_id: str
    decision_id: str
    market_ticker: str
    side_candidate: str
    action: str
    created_at: datetime
    effective_after: datetime
    expires_at: datetime
    intended_limit_price: Decimal
    quantity: Decimal
    status: str = "PENDING"
    strategy_config_version_id: str | None = None
    feature_snapshot_id: str | None = None
    position_id: str | None = None
    optimistic_price: Decimal | None = None
    optimistic_snapshot_id: int | None = None
    architecture_version: str | None = None
    code_commit_sha: str | None = None
    lifecycle_version: str | None = None
    trigger: str | None = None
    trigger_classification: str | None = None
    attempt_number: int | None = None
    decision_time_executable_bid: Decimal | None = None
    resolved_at: datetime | None = None
    fill_snapshot_id: int | None = None
    simulated_fill_price: Decimal | None = None
    simulated_fill_size: Decimal | None = None
    fill_timestamp: datetime | None = None
    resolution_reason: str | None = None
    measurements: JsonPayload | None = None


@dataclass(frozen=True)
class StrategyPositionMarkInput:
    mark_id: str
    strategy_id: str
    position_id: str
    market_ticker: str
    marked_at: datetime
    strategy_config_version_id: str | None = None
    feature_snapshot_id: str | None = None
    executable_bid: Decimal | None = None
    score: Decimal | None = None
    edge_lower_bound_cents: Decimal | None = None
    boundary_state: JsonPayload | None = None
    management_reason: str | None = None
    measurements: JsonPayload | None = None


@dataclass(frozen=True)
class StrategyPositionOutcomeInput:
    outcome_id: str
    position_id: str
    strategy_id: str
    market_ticker: str
    held_side: str
    lifecycle_version: str
    opened_at: datetime
    closed_at: datetime
    holding_duration_ms: int
    quantity: Decimal
    entry_price: Decimal
    exit_price: Decimal
    realized_pnl_cents: Decimal
    architecture_version: str | None = None
    strategy_config_version_id: str | None = None
    code_commit_sha: str | None = None
    entry_decision_id: str | None = None
    exit_decision_id: str | None = None
    entry_intent_id: str | None = None
    exit_intent_id: str | None = None
    entry_feature_snapshot_id: str | None = None
    exit_feature_snapshot_id: str | None = None
    mfe_cents: Decimal | None = None
    mae_cents: Decimal | None = None
    time_to_mfe_ms: int | None = None
    time_to_mae_ms: int | None = None
    optimistic_entry_delta: Decimal | None = None
    decision_to_filled_exit_delta: Decimal | None = None
    close_trigger: str | None = None
    close_reason: str | None = None
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
