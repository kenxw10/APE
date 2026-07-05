from __future__ import annotations

import httpx
import pytest

from ape.kalshi.client import KalshiRestClient
from ape.kalshi.errors import KalshiRequestError


def test_client_signs_safe_get_market_request_without_query_in_signature() -> None:
    observed: dict[str, str] = {}

    def signer(_private_key_pem: str, payload: str) -> str:
        observed["payload"] = payload
        return "mock-signature"

    def handler(request: httpx.Request) -> httpx.Response:
        observed["url"] = str(request.url)
        observed["key"] = request.headers["KALSHI-ACCESS-KEY"]
        observed["signature"] = request.headers["KALSHI-ACCESS-SIGNATURE"]
        observed["timestamp"] = request.headers["KALSHI-ACCESS-TIMESTAMP"]
        return httpx.Response(200, json={"markets": [], "cursor": ""})

    client = KalshiRestClient(
        base_url="https://external-api.kalshi.com/trade-api/v2",
        api_key_id="key-id",
        private_key_pem="private-key",
        timeout_seconds=5,
        now_ms=lambda: 123,
        signer=signer,
        transport=httpx.MockTransport(handler),
    )

    assert client.get_markets(series_ticker="KXBTC15M", status="open", limit=100) == {
        "markets": [],
        "cursor": "",
    }
    assert observed["url"].startswith(
        "https://external-api.kalshi.com/trade-api/v2/markets?"
    )
    assert observed["key"] == "key-id"
    assert observed["signature"] == "mock-signature"
    assert observed["timestamp"] == "123"
    assert observed["payload"] == "123GET/trade-api/v2/markets"


def test_client_errors_redact_key_and_signature_from_response_context() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            401,
            text="key-id mock-signature should not be returned",
            headers={"x-request-id": "request-123"},
        )

    client = KalshiRestClient(
        base_url="https://external-api.kalshi.com/trade-api/v2",
        api_key_id="key-id",
        private_key_pem="private-key",
        now_ms=lambda: 123,
        signer=lambda _key, _payload: "mock-signature",
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(KalshiRequestError) as exc_info:
        client.get_exchange_status()

    error = exc_info.value
    assert error.status_code == 401
    assert error.request_id == "request-123"
    assert "key-id" not in (error.body_preview or "")
    assert "mock-signature" not in (error.body_preview or "")


def test_client_exposes_only_observer_read_methods() -> None:
    client = KalshiRestClient(base_url="https://external-api.kalshi.com/trade-api/v2")

    assert hasattr(client, "get_exchange_status")
    assert hasattr(client, "get_markets")
    assert hasattr(client, "get_market")
    assert hasattr(client, "get_market_orderbook")
    assert not hasattr(client, "create_order")
    assert not hasattr(client, "cancel_order")
    assert not hasattr(client, "get_fills")

