from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from ape.config import AppConfig
from ape.db.session import create_engine_from_config, create_session_factory
from ape.kalshi.boundary import ParsedBoundary, parse_market_boundary
from ape.kalshi.client import KalshiRestClient
from ape.kalshi.diagnostics import KalshiConfigDiagnostic, build_kalshi_config_diagnostic
from ape.kalshi.errors import KalshiAuthError, KalshiRequestError, KalshiUnreachableError
from ape.kalshi.types import KalshiMarketPayload
from ape.repositories.inputs import JsonPayload, MarketInput
from ape.repositories.markets import MarketsRepository


class ResolverState(StrEnum):
    NOT_CONFIGURED = "not_configured"
    AUTH_ERROR = "auth_error"
    KALSHI_UNREACHABLE = "kalshi_unreachable"
    NO_ACTIVE_MARKET = "no_active_market"
    AMBIGUOUS_MARKET = "ambiguous_market"
    MARKET_NOT_PARSEABLE = "market_not_parseable"
    RESOLVED_OBSERVER_ONLY = "resolved_observer_only"


TRADABLE_MARKET_STATUSES = {"open", "active"}
MARKETS_PAGE_LIMIT = 100
MARKETS_MAX_PAGES = 10


@dataclass(frozen=True)
class ResolverResult:
    state: ResolverState
    configured: bool
    signer_ready: bool
    series_ticker: str
    query_scope: dict[str, Any]
    market: MarketInput | None
    boundary: ParsedBoundary | None
    blockers: list[str]
    warnings: list[str]
    resolver_decision_reason: str
    parser_version: str
    raw_payload_hash: str | None
    persisted: bool
    resolved_at: datetime


@dataclass(frozen=True)
class MarketsPageResult:
    markets: list[Any]
    page_count: int
    truncated: bool


def resolve_active_btc15_market(
    *,
    config: AppConfig,
    client: KalshiRestClient | None = None,
    session: Session | None = None,
    now: datetime | None = None,
) -> ResolverResult:
    resolved_at = now or datetime.now(UTC)
    diagnostic = build_kalshi_config_diagnostic(config)
    query_scope = _query_scope(config)

    if not diagnostic.configured:
        return _result(
            state=ResolverState.NOT_CONFIGURED,
            diagnostic=diagnostic,
            query_scope=query_scope,
            resolved_at=resolved_at,
            reason="kalshi_credentials_not_configured_or_not_parseable",
            blockers=["kalshi_credentials_not_configured"],
        )

    if not diagnostic.signer_ready:
        return _result(
            state=ResolverState.AUTH_ERROR,
            diagnostic=diagnostic,
            query_scope=query_scope,
            resolved_at=resolved_at,
            reason="kalshi_credentials_present_but_signer_not_ready",
            blockers=["kalshi_signer_not_ready"],
        )

    rest_client = client or KalshiRestClient(
        base_url=config.kalshi_api_base_url,
        api_key_id=config.kalshi_api_key_id,
        private_key_pem=config.kalshi_private_key,
        timeout_seconds=config.kalshi_rest_timeout_seconds,
    )

    try:
        page_result = _fetch_open_market_pages(rest_client, config)
    except KalshiAuthError:
        return _result(
            state=ResolverState.AUTH_ERROR,
            diagnostic=diagnostic,
            query_scope=query_scope,
            resolved_at=resolved_at,
            reason="kalshi_auth_signing_failed",
            blockers=["kalshi_auth_error"],
        )
    except KalshiRequestError as exc:
        state = (
            ResolverState.AUTH_ERROR
            if exc.status_code in {401, 403}
            else ResolverState.KALSHI_UNREACHABLE
        )
        return _result(
            state=state,
            diagnostic=diagnostic,
            query_scope=query_scope,
            resolved_at=resolved_at,
            reason=f"kalshi_request_failed_status_{exc.status_code}",
            blockers=[
                "kalshi_auth_error"
                if state is ResolverState.AUTH_ERROR
                else "kalshi_request_failed"
            ],
        )
    except KalshiUnreachableError:
        return _result(
            state=ResolverState.KALSHI_UNREACHABLE,
            diagnostic=diagnostic,
            query_scope=query_scope,
            resolved_at=resolved_at,
            reason="kalshi_unreachable_or_timeout",
            blockers=["kalshi_unreachable"],
        )

    markets = page_result.markets
    if not all(isinstance(markets_page_item, dict) for markets_page_item in markets):
        return _result(
            state=ResolverState.KALSHI_UNREACHABLE,
            diagnostic=diagnostic,
            query_scope=query_scope,
            resolved_at=resolved_at,
            reason="kalshi_markets_response_missing_markets",
            blockers=["kalshi_response_unexpected_shape"],
        )

    candidates = [
        market
        for market in markets
        if isinstance(market, dict) and _is_btc15_open_market(market, config)
    ]
    pagination_metadata = {
        "returned_market_count": len(markets),
        "fetched_page_count": page_result.page_count,
        "pagination_truncated": page_result.truncated,
    }
    selected, state, reason = _select_market(candidates, resolved_at)
    if selected is None:
        return _result(
            state=state,
            diagnostic=diagnostic,
            query_scope={**query_scope, **pagination_metadata},
            resolved_at=resolved_at,
            reason=reason,
            blockers=[state.value],
            warnings=_pagination_warnings(page_result),
        )

    boundary = parse_market_boundary(selected)
    raw_hash = raw_payload_hash(selected)
    market_input = market_input_from_payload(
        selected,
        series_ticker=config.kalshi_btc15_series_ticker,
        boundary=boundary,
        parser_version=config.kalshi_resolver_parser_version,
        raw_hash=raw_hash,
        decision_reason=reason,
    )

    if not boundary.is_parseable:
        persisted, persist_warning = _persist_market(
            config=config,
            session=session,
            market=market_input,
        )
        warnings = list(boundary.warnings)
        if persist_warning:
            warnings.append(persist_warning)
        return _result(
            state=ResolverState.MARKET_NOT_PARSEABLE,
            diagnostic=diagnostic,
            query_scope={**query_scope, **pagination_metadata},
            resolved_at=resolved_at,
            market=market_input,
            boundary=boundary,
            raw_payload_hash=raw_hash,
            reason="market_boundary_not_parseable",
            blockers=boundary.blockers,
            warnings=[*warnings, *_pagination_warnings(page_result)],
            persisted=persisted,
        )

    persisted, persist_warning = _persist_market(
        config=config,
        session=session,
        market=market_input,
    )
    warnings = list(boundary.warnings)
    if persist_warning:
        warnings.append(persist_warning)

    return _result(
        state=ResolverState.RESOLVED_OBSERVER_ONLY,
        diagnostic=diagnostic,
        query_scope={**query_scope, **pagination_metadata},
        resolved_at=resolved_at,
        market=market_input,
        boundary=boundary,
        raw_payload_hash=raw_hash,
        reason=reason,
        warnings=[*warnings, *_pagination_warnings(page_result)],
        persisted=persisted,
    )


def _fetch_open_market_pages(
    rest_client: KalshiRestClient,
    config: AppConfig,
) -> MarketsPageResult:
    markets: list[Any] = []
    cursor: str | None = None
    page_count = 0

    while page_count < MARKETS_MAX_PAGES:
        response = rest_client.get_markets(
            series_ticker=config.kalshi_btc15_series_ticker,
            status="open",
            limit=MARKETS_PAGE_LIMIT,
            cursor=cursor,
        )
        page_count += 1

        page_markets = response.get("markets")
        if not isinstance(page_markets, list):
            return MarketsPageResult(markets=[None], page_count=page_count, truncated=False)

        markets.extend(page_markets)
        cursor = _cursor_or_none(response.get("cursor"))
        if cursor is None:
            break

    return MarketsPageResult(
        markets=markets,
        page_count=page_count,
        truncated=cursor is not None,
    )


def _cursor_or_none(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _pagination_warnings(page_result: MarketsPageResult) -> list[str]:
    if not page_result.truncated:
        return []
    return ["kalshi_markets_pagination_truncated"]


def raw_payload_hash(payload: KalshiMarketPayload) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def market_input_from_payload(
    payload: KalshiMarketPayload,
    *,
    series_ticker: str,
    boundary: ParsedBoundary,
    parser_version: str,
    raw_hash: str,
    decision_reason: str,
) -> MarketInput:
    return MarketInput(
        market_ticker=str(payload.get("ticker") or payload.get("market_ticker")),
        event_ticker=_str_or_none(payload.get("event_ticker")),
        series_ticker=_str_or_none(payload.get("series_ticker")) or series_ticker,
        title=_str_or_none(payload.get("title")),
        subtitle=_str_or_none(payload.get("subtitle")),
        yes_sub_title=_str_or_none(payload.get("yes_sub_title")),
        no_sub_title=_str_or_none(payload.get("no_sub_title")),
        open_time=_datetime_or_none(payload.get("open_time")),
        close_time=_datetime_or_none(payload.get("close_time")),
        expected_expiration_time=_datetime_or_none(payload.get("expected_expiration_time")),
        expiration_time=_datetime_or_none(payload.get("expiration_time")),
        latest_expiration_time=_datetime_or_none(payload.get("latest_expiration_time")),
        settlement_timer_seconds=_int_or_none(payload.get("settlement_timer_seconds")),
        rules_primary=_str_or_none(payload.get("rules_primary")),
        rules_secondary=_str_or_none(payload.get("rules_secondary")),
        functional_strike=boundary.functional_strike,
        floor_strike=boundary.floor_strike,
        cap_strike=boundary.cap_strike,
        custom_strike=boundary.custom_strike,
        price_level_structure=_json_payload_or_none(payload.get("price_level_structure")),
        price_ranges=_json_payload_or_none(payload.get("price_ranges")),
        liquidity_dollars=_decimal_or_none(payload.get("liquidity_dollars")),
        raw_payload_hash=raw_hash,
        parser_version=parser_version,
        resolver_decision_reason=decision_reason,
    )


def _is_btc15_open_market(market: KalshiMarketPayload, config: AppConfig) -> bool:
    series_ticker = _str_or_none(market.get("series_ticker"))
    if series_ticker is not None and series_ticker != config.kalshi_btc15_series_ticker:
        return False

    status = str(market.get("status") or "").strip().lower()
    return status in TRADABLE_MARKET_STATUSES


def _select_market(
    markets: list[KalshiMarketPayload],
    now: datetime,
) -> tuple[KalshiMarketPayload | None, ResolverState, str]:
    containing = [
        market
        for market in markets
        if _contains_now(
            _datetime_or_none(market.get("open_time")),
            _datetime_or_none(market.get("close_time")),
            now,
        )
    ]
    if len(containing) == 1:
        return containing[0], ResolverState.RESOLVED_OBSERVER_ONLY, "market_interval_contains_now"
    if len(containing) > 1:
        return None, ResolverState.AMBIGUOUS_MARKET, "multiple_open_markets_contain_now"

    timed_markets = [
        market
        for market in markets
        if _datetime_or_none(market.get("open_time")) is not None
        and _datetime_or_none(market.get("close_time")) is not None
    ]
    if not timed_markets:
        return None, ResolverState.NO_ACTIVE_MARKET, "no_open_market_with_parseable_timing"

    return None, ResolverState.NO_ACTIVE_MARKET, "no_open_market_contains_now"


def _contains_now(open_time: datetime | None, close_time: datetime | None, now: datetime) -> bool:
    if open_time is None or close_time is None:
        return False
    return open_time <= now < close_time


def _persist_market(
    *,
    config: AppConfig,
    session: Session | None,
    market: MarketInput,
) -> tuple[bool, str | None]:
    if session is not None:
        repository = MarketsRepository(session)
        repository.upsert_market(market)
        session.commit()
        return True, None

    if not config.database_url:
        return False, "database_not_configured_for_market_persistence"

    try:
        engine = create_engine_from_config(config)
        try:
            session_factory = create_session_factory(engine)
            with session_factory() as db_session:
                repository = MarketsRepository(db_session)
                repository.upsert_market(market)
                db_session.commit()
        finally:
            engine.dispose()
    except SQLAlchemyError:
        return False, "market_persistence_failed"

    return True, None


def _result(
    *,
    state: ResolverState,
    diagnostic: KalshiConfigDiagnostic,
    query_scope: dict[str, Any],
    resolved_at: datetime,
    reason: str,
    market: MarketInput | None = None,
    boundary: ParsedBoundary | None = None,
    blockers: list[str] | None = None,
    warnings: list[str] | None = None,
    raw_payload_hash: str | None = None,
    persisted: bool = False,
) -> ResolverResult:
    return ResolverResult(
        state=state,
        configured=diagnostic.configured,
        signer_ready=diagnostic.signer_ready,
        series_ticker=diagnostic.series_ticker,
        query_scope=query_scope,
        market=market,
        boundary=boundary,
        blockers=blockers or [],
        warnings=warnings or [],
        resolver_decision_reason=reason,
        parser_version=diagnostic.parser_version,
        raw_payload_hash=raw_payload_hash,
        persisted=persisted,
        resolved_at=resolved_at,
    )


def _query_scope(config: AppConfig) -> dict[str, Any]:
    return {
        "endpoint": "/markets",
        "series_ticker": config.kalshi_btc15_series_ticker,
        "status": "open",
        "limit": MARKETS_PAGE_LIMIT,
        "max_pages": MARKETS_MAX_PAGES,
    }


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _datetime_or_none(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value).replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _json_payload_or_none(value: Any) -> JsonPayload | None:
    if isinstance(value, dict | list | str | int | float | bool):
        return value
    return None
