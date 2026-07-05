from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

JsonPayload = dict[str, Any] | list[Any]


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
    taker_side: str | None = None
    side_inferred: str | None = None
    raw_payload_hash: str | None = None
    raw_payload: JsonPayload | None = None


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
class WorkerHeartbeatInput:
    service_name: str
    heartbeat_at: datetime
    app_mode: str
    is_safe: bool
    started_at: datetime | None = None
    metadata: JsonPayload | None = None

