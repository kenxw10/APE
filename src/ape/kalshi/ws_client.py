from __future__ import annotations

from collections.abc import Callable
from typing import Any
from urllib.parse import urlsplit

from ape.kalshi.auth import KalshiSigner, create_auth_headers, sign_payload
from ape.kalshi.errors import KalshiUnreachableError

SafeHeaders = dict[str, str]


def websocket_signature_path(endpoint: str) -> str:
    return urlsplit(endpoint).path


def create_websocket_auth_headers(
    *,
    endpoint: str,
    api_key_id: str | None,
    private_key_pem: str | None,
    timestamp_ms: int | None = None,
    signer: KalshiSigner = sign_payload,
) -> SafeHeaders:
    return create_auth_headers(
        api_key_id=api_key_id,
        private_key_pem=private_key_pem,
        method="GET",
        request_path=websocket_signature_path(endpoint),
        timestamp_ms=timestamp_ms,
        signer=signer,
    )


def build_subscribe_message(
    *,
    request_id: int,
    channels: list[str],
    market_ticker: str,
    use_yes_price: bool = False,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "channels": channels,
        "market_ticker": market_ticker,
    }
    if use_yes_price:
        params["use_yes_price"] = True

    return {
        "id": request_id,
        "cmd": "subscribe",
        "params": params,
    }


def build_cfbenchmarks_subscribe_message(
    *,
    request_id: int,
    index_ids: list[str],
) -> dict[str, Any]:
    return {
        "id": request_id,
        "cmd": "subscribe",
        "params": {
            "channels": ["cfbenchmarks_value"],
            "index_ids": index_ids,
        },
    }


def build_update_subscription_message(
    *,
    request_id: int,
    subscription_id: int,
    market_ticker: str,
    action: str = "get_snapshot",
) -> dict[str, Any]:
    return {
        "id": request_id,
        "cmd": "update_subscription",
        "params": {
            "sids": [subscription_id],
            "market_tickers": [market_ticker],
            "action": action,
        },
    }


def build_list_subscriptions_message(*, request_id: int) -> dict[str, Any]:
    return {
        "id": request_id,
        "cmd": "list_subscriptions",
    }


async def connect_websocket(
    *,
    endpoint: str,
    headers: SafeHeaders,
    connect_timeout_seconds: float,
    heartbeat_timeout_seconds: float,
    websocket_connect: Callable[..., Any] | None = None,
) -> Any:
    if websocket_connect is None:
        try:
            import websockets
        except ImportError as exc:
            raise KalshiUnreachableError("websockets dependency is not installed.") from exc

        websocket_connect = websockets.connect

    try:
        return await websocket_connect(
            endpoint,
            additional_headers=headers,
            open_timeout=connect_timeout_seconds,
            ping_timeout=heartbeat_timeout_seconds,
        )
    except TypeError:
        return await websocket_connect(
            endpoint,
            extra_headers=headers,
            open_timeout=connect_timeout_seconds,
            ping_timeout=heartbeat_timeout_seconds,
        )
    except Exception as exc:
        raise KalshiUnreachableError("Kalshi WebSocket connection failed.") from exc
