from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from ape.config import load_config
from ape.db.migrations import run_migrations
from ape.db.session import create_engine_from_config, create_session_factory
from ape.kalshi.boundary import parse_market_boundary
from ape.kalshi.resolver import ResolverState, resolve_active_btc15_market
from ape.repositories.markets import MarketsRepository

NOW = datetime(2026, 7, 5, 12, 5, tzinfo=UTC)


class FakeKalshiClient:
    def __init__(
        self,
        markets: list[dict[str, Any]] | None = None,
        pages: list[dict[str, Any]] | None = None,
    ) -> None:
        self.markets = markets or []
        self.pages = pages
        self.calls: list[dict[str, Any]] = []

    def get_markets(self, **kwargs):
        self.calls.append(kwargs)
        if self.pages is not None:
            return self.pages[len(self.calls) - 1]
        return {"markets": self.markets, "cursor": ""}


def test_resolver_handles_missing_credentials_as_not_configured() -> None:
    client = FakeKalshiClient([_market_payload()])

    result = resolve_active_btc15_market(config=load_config({}), client=client, now=NOW)

    assert result.state is ResolverState.NOT_CONFIGURED
    assert result.configured is False
    assert client.calls == []


def test_resolver_selects_active_btc15_market_and_uses_bounded_query() -> None:
    client = FakeKalshiClient(
        [
            _market_payload(
                ticker="KXBTC15M-OLDER",
                open_time="2026-07-05T11:30:00Z",
                close_time="2026-07-05T11:45:00Z",
            ),
            _market_payload(ticker="KXBTC15M-ACTIVE", include_series_ticker=False),
        ]
    )

    result = resolve_active_btc15_market(
        config=_configured(),
        client=client,
        now=NOW,
    )

    assert result.state is ResolverState.RESOLVED_OBSERVER_ONLY
    assert result.market is not None
    assert result.market.market_ticker == "KXBTC15M-ACTIVE"
    assert result.market.series_ticker == "KXBTC15M"
    assert result.market.functional_strike == Decimal("62000")
    assert result.market.price_level_structure == "binary"
    assert result.query_scope["series_ticker"] == "KXBTC15M"
    assert result.query_scope["fetched_page_count"] == 1
    assert client.calls == [
        {"series_ticker": "KXBTC15M", "status": "open", "limit": 100, "cursor": None}
    ]


def test_resolver_paginates_before_selecting_active_market() -> None:
    client = FakeKalshiClient(
        pages=[
            {
                "markets": [
                    _market_payload(
                        ticker="KXBTC15M-OLDER",
                        open_time="2026-07-05T11:30:00Z",
                        close_time="2026-07-05T11:45:00Z",
                    )
                ],
                "cursor": "next-page",
            },
            {
                "markets": [_market_payload(ticker="KXBTC15M-ACTIVE")],
                "cursor": "",
            },
        ]
    )

    result = resolve_active_btc15_market(config=_configured(), client=client, now=NOW)

    assert result.state is ResolverState.RESOLVED_OBSERVER_ONLY
    assert result.market is not None
    assert result.market.market_ticker == "KXBTC15M-ACTIVE"
    assert result.query_scope["returned_market_count"] == 2
    assert result.query_scope["fetched_page_count"] == 2
    assert result.query_scope["pagination_truncated"] is False
    assert client.calls == [
        {"series_ticker": "KXBTC15M", "status": "open", "limit": 100, "cursor": None},
        {"series_ticker": "KXBTC15M", "status": "open", "limit": 100, "cursor": "next-page"},
    ]


def test_resolver_rejects_explicit_series_mismatch() -> None:
    client = FakeKalshiClient([_market_payload(series_ticker="KXMISMATCH")])

    result = resolve_active_btc15_market(config=_configured(), client=client, now=NOW)

    assert result.state is ResolverState.NO_ACTIVE_MARKET
    assert result.market is None


def test_resolver_does_not_select_nearest_inactive_market() -> None:
    client = FakeKalshiClient(
        [
            _market_payload(
                ticker="KXBTC15M-FUTURE",
                open_time="2026-07-05T12:15:00Z",
                close_time="2026-07-05T12:30:00Z",
            )
        ]
    )

    result = resolve_active_btc15_market(config=_configured(), client=client, now=NOW)

    assert result.state is ResolverState.NO_ACTIVE_MARKET
    assert result.market is None
    assert result.resolver_decision_reason == "no_open_market_contains_now"


def test_resolver_rejects_ambiguous_active_markets() -> None:
    client = FakeKalshiClient(
        [
            _market_payload(ticker="KXBTC15M-A"),
            _market_payload(ticker="KXBTC15M-B"),
        ]
    )

    result = resolve_active_btc15_market(config=_configured(), client=client, now=NOW)

    assert result.state is ResolverState.AMBIGUOUS_MARKET
    assert result.market is None
    assert "ambiguous_market" in result.blockers


def test_boundary_parser_uses_text_fallback_when_structured_fields_are_absent() -> None:
    boundary = parse_market_boundary(
        {
            "title": "Bitcoin price above $62,000 at settlement?",
            "rules_primary": "Settlement source only.",
        }
    )

    assert boundary.is_parseable is True
    assert boundary.functional_strike == Decimal("62000")
    assert boundary.source == "text_fallback"


def test_boundary_parser_rejects_structured_text_disagreement() -> None:
    boundary = parse_market_boundary(
        {
            "functional_strike": "62000",
            "title": "Bitcoin price above $63,000 at settlement?",
        }
    )

    assert boundary.is_parseable is False
    assert "structured_boundary_disagrees_with_text" in boundary.blockers


def test_resolver_rejects_unparseable_boundary_but_persists_metadata(tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'ape_kalshi_resolver.sqlite'}"
    engine = create_engine_from_config(load_config({"DATABASE_URL": database_url}))
    run_migrations(engine)
    session_factory = create_session_factory(engine)

    try:
        with session_factory() as session:
            result = resolve_active_btc15_market(
                config=_configured({"DATABASE_URL": database_url}),
                client=FakeKalshiClient(
                    [
                        _market_payload(
                            functional_strike=None,
                            title="No price",
                            yes_sub_title="No boundary",
                            no_sub_title="No boundary",
                        )
                    ]
                ),
                session=session,
                now=NOW,
            )

            assert result.state is ResolverState.MARKET_NOT_PARSEABLE
            assert result.persisted is True
            stored = MarketsRepository(session).get_market_by_ticker("KXBTC15M-ACTIVE")
            assert stored is not None
            assert stored.raw_payload_hash == result.raw_payload_hash
            assert stored.parser_version == "btc15_resolver_v1"
            assert stored.price_level_structure == "binary"
    finally:
        engine.dispose()


def _configured(extra: dict[str, str] | None = None):
    env = {
        "KALSHI_API_KEY_ID": "key-id",
        "KALSHI_PRIVATE_KEY": _test_private_key_pem(),
    }
    if extra:
        env.update(extra)
    return load_config(env)


def _market_payload(
    *,
    ticker: str = "KXBTC15M-ACTIVE",
    open_time: str = "2026-07-05T12:00:00Z",
    close_time: str = "2026-07-05T12:15:00Z",
    functional_strike: str | None = "62000",
    title: str = "Bitcoin price above $62,000 at settlement?",
    yes_sub_title: str = "Above $62,000",
    no_sub_title: str = "At or below $62,000",
    series_ticker: str | None = "KXBTC15M",
    include_series_ticker: bool = True,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ticker": ticker,
        "event_ticker": "KXBTC15M-26JUL051200",
        "status": "open",
        "title": title,
        "subtitle": "BTC 15-minute market",
        "yes_sub_title": yes_sub_title,
        "no_sub_title": no_sub_title,
        "open_time": open_time,
        "close_time": close_time,
        "expected_expiration_time": close_time,
        "expiration_time": close_time,
        "latest_expiration_time": close_time,
        "settlement_timer_seconds": 60,
        "rules_primary": "Uses CF Benchmarks settlement.",
        "rules_secondary": "Observer metadata only.",
        "price_level_structure": "binary",
        "price_ranges": [{"min": "62000"}],
        "liquidity_dollars": "123.45",
    }
    if include_series_ticker:
        payload["series_ticker"] = series_ticker
    if functional_strike is not None:
        payload["functional_strike"] = functional_strike
    return payload


def _test_private_key_pem() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
