from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel

from ape.kalshi.diagnostics import KalshiConfigDiagnostic
from ape.kalshi.resolver import ResolverResult
from ape.repositories.inputs import MarketInput


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
    price_level_structure: dict[str, Any] | list[Any] | None
    price_ranges: dict[str, Any] | list[Any] | None
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


def _boundary_response(boundary) -> MarketBoundaryResponse | None:
    if boundary is None:
        return None
    return MarketBoundaryResponse(**boundary.__dict__)


def _market_response(market: MarketInput | None) -> ActiveMarketMetadataResponse | None:
    if market is None:
        return None
    return ActiveMarketMetadataResponse(**market.__dict__)

