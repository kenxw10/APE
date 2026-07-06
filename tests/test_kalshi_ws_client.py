from __future__ import annotations

from ape.kalshi.ws_client import (
    build_cfbenchmarks_subscribe_message,
    build_subscribe_message,
    create_websocket_auth_headers,
    websocket_signature_path,
)


def test_websocket_headers_use_existing_auth_without_exposing_private_key() -> None:
    seen: dict[str, str] = {}

    def signer(private_key: str, payload: str) -> str:
        seen["private_key"] = private_key
        seen["payload"] = payload
        return "signed-value"

    headers = create_websocket_auth_headers(
        endpoint="wss://external-api-ws.kalshi.com/trade-api/ws/v2",
        api_key_id="key-id",
        private_key_pem="PRIVATE KEY VALUE",
        timestamp_ms=123,
        signer=signer,
    )

    assert websocket_signature_path("wss://external-api-ws.kalshi.com/trade-api/ws/v2") == (
        "/trade-api/ws/v2"
    )
    assert seen["payload"] == "123GET/trade-api/ws/v2"
    assert headers == {
        "KALSHI-ACCESS-KEY": "key-id",
        "KALSHI-ACCESS-TIMESTAMP": "123",
        "KALSHI-ACCESS-SIGNATURE": "signed-value",
    }
    assert "PRIVATE KEY" not in str(headers)


def test_subscribe_command_payloads_for_public_channels() -> None:
    orderbook = build_subscribe_message(
        request_id=1,
        channels=["orderbook_delta"],
        market_ticker="KXBTC15M-TEST",
        use_yes_price=True,
    )
    ticker_trade = build_subscribe_message(
        request_id=2,
        channels=["ticker", "trade"],
        market_ticker="KXBTC15M-TEST",
    )

    assert orderbook == {
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": ["orderbook_delta"],
            "market_ticker": "KXBTC15M-TEST",
            "use_yes_price": True,
        },
    }
    assert ticker_trade == {
        "id": 2,
        "cmd": "subscribe",
        "params": {"channels": ["ticker", "trade"], "market_ticker": "KXBTC15M-TEST"},
    }


def test_cfbenchmarks_subscribe_command_uses_index_ids_without_market_ticker() -> None:
    message = build_cfbenchmarks_subscribe_message(request_id=3, index_ids=["BRTI"])

    assert message == {
        "id": 3,
        "cmd": "subscribe",
        "params": {
            "channels": ["cfbenchmarks_value"],
            "index_ids": ["BRTI"],
        },
    }
    assert "market_ticker" not in message["params"]
    assert "market_tickers" not in message["params"]
    assert "market_id" not in message["params"]
    assert "market_ids" not in message["params"]
