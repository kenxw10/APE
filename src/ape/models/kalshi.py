from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel

from ape.kalshi.diagnostics import KalshiConfigDiagnostic
from ape.kalshi.resolver import ResolverResult
from ape.kalshi.ws_status import KalshiWsStatusSnapshot
from ape.repositories.inputs import JsonPayload, MarketInput


class KalshiStatusResponse(BaseModel):
    configured: bool
    signer_ready: bool
    base_url_host: str
    api_key_configured: bool
    private_key_configured: bool
    private_key_parseable: bool
    kalshi_env: str
    series_ticker: str
    timeout_seconds: float
    parser_version: str


class MarketBoundaryResponse(BaseModel):
    functional_strike: Decimal | None
    floor_strike: Decimal | None
    cap_strike: Decimal | None
    custom_strike: Decimal | None
    source: str
    parse_status: str
    blockers: list[str]
    warnings: list[str]


class ActiveMarketMetadataResponse(BaseModel):
    market_ticker: str
    event_ticker: str | None
    series_ticker: str | None
    title: str | None
    subtitle: str | None
    yes_sub_title: str | None
    no_sub_title: str | None
    open_time: datetime | None
    close_time: datetime | None
    expected_expiration_time: datetime | None
    expiration_time: datetime | None
    latest_expiration_time: datetime | None
    settlement_timer_seconds: int | None
    rules_primary: str | None
    rules_secondary: str | None
    functional_strike: Decimal | None
    floor_strike: Decimal | None
    cap_strike: Decimal | None
    custom_strike: Decimal | None
    price_level_structure: JsonPayload | None
    price_ranges: JsonPayload | None
    liquidity_dollars: Decimal | None
    raw_payload_hash: str | None
    parser_version: str | None
    resolver_decision_reason: str | None


class ActiveMarketResponse(BaseModel):
    state: str
    configured: bool
    signer_ready: bool
    series_ticker: str
    query_scope: dict[str, Any]
    market: ActiveMarketMetadataResponse | None
    boundary: MarketBoundaryResponse | None
    blockers: list[str]
    warnings: list[str]
    resolver_decision_reason: str
    parser_version: str
    raw_payload_hash: str | None
    persisted: bool
    resolved_at: datetime


class KalshiWsStatusResponse(BaseModel):
    configured: bool
    enabled: bool
    signer_ready: bool
    endpoint_host: str
    endpoint_path: str
    connection_state: str
    active_market_ticker: str | None
    liveness_source: str
    worker_heartbeat_at: datetime | None
    worker_heartbeat_age_ms: int | None
    worker_started_at: datetime | None
    component_heartbeat_at: datetime | None
    component_heartbeat_age_ms: int | None
    latest_aggregate_heartbeat_mode: str | None
    latest_component_heartbeat_mode: str | None
    liveness_source_mismatch: bool
    subscribed_channels: list[str]
    subscription_ids: dict[str, int]
    last_connected_at: datetime | None
    last_message_at: datetime | None
    last_ticker_at: datetime | None
    last_orderbook_at: datetime | None
    last_trade_at: datetime | None
    latest_orderbook_received_at: datetime | None
    latest_trade_received_at: datetime | None
    orderbook_stream_age_ms: int | None
    orderbook_liveness_reason: str | None
    transport_alive: bool
    transport_last_pong_at: datetime | None
    transport_age_ms: int | None
    transport_liveness_reason: str | None
    last_market_data_message_at: datetime | None
    market_data_message_age_ms: int | None
    market_feed_transport_state: str
    market_feed_subscription_state: str
    market_feed_snapshot_state: str
    market_feed_active_ticker_state: str
    market_feed_sequence_state: str
    market_data_quiet: bool
    market_data_quiet_age_ms: int | None
    orderbook_snapshot_age_ms: int | None
    orderbook_snapshot_source: str | None
    orderbook_recovery_action: str | None
    market_feed_state: str | None
    market_subscription_recovery_count: int
    market_subscription_recovery_last_reason: str | None
    market_subscription_recovery_last_action: str | None
    market_subscription_recovery_last_result: str | None
    market_subscription_recovery_last_at: datetime | None
    market_snapshot_resync_count: int
    market_snapshot_resync_last_result: str | None
    market_rollover_recovery_count: int
    market_transport_reconnect_count: int
    market_unrecovered_blocker_count: int
    market_recovery_attempt_in_progress: bool
    market_recovery_attempt_age_ms: int | None
    reconnect_count: int
    last_error_type: str | None
    last_error_message: str | None
    warnings: list[str]
    blockers: list[str]
    diagnostic_samples: list[dict[str, Any]]
    stale: bool
    checked_at: datetime


def kalshi_status_response(diagnostic: KalshiConfigDiagnostic) -> KalshiStatusResponse:
    return KalshiStatusResponse(**diagnostic.__dict__)


def active_market_response(result: ResolverResult) -> ActiveMarketResponse:
    return ActiveMarketResponse(
        state=result.state.value,
        configured=result.configured,
        signer_ready=result.signer_ready,
        series_ticker=result.series_ticker,
        query_scope=result.query_scope,
        market=_market_response(result.market),
        boundary=_boundary_response(result.boundary),
        blockers=result.blockers,
        warnings=result.warnings,
        resolver_decision_reason=result.resolver_decision_reason,
        parser_version=result.parser_version,
        raw_payload_hash=result.raw_payload_hash,
        persisted=result.persisted,
        resolved_at=result.resolved_at,
    )


def kalshi_ws_status_response(snapshot: KalshiWsStatusSnapshot) -> KalshiWsStatusResponse:
    return KalshiWsStatusResponse(**snapshot.__dict__)


def _boundary_response(boundary) -> MarketBoundaryResponse | None:
    if boundary is None:
        return None
    return MarketBoundaryResponse(**boundary.__dict__)


def _market_response(market: MarketInput | None) -> ActiveMarketMetadataResponse | None:
    if market is None:
        return None
    return ActiveMarketMetadataResponse(**market.__dict__)
