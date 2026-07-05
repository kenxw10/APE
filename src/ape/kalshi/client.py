from __future__ import annotations

from collections.abc import Callable
from urllib.parse import quote, urlencode, urlsplit

import httpx

from ape.kalshi.auth import KalshiSigner, create_auth_headers, sign_payload
from ape.kalshi.errors import KalshiRequestError, KalshiUnreachableError
from ape.kalshi.types import KalshiJson

SafeHeaders = dict[str, str]


class KalshiRestClient:
    """Minimal observer-only Kalshi REST client.

    This client intentionally exposes only read methods needed for diagnostics and market
    resolution. It has no order, cancel, portfolio, fill, WebSocket, or trading methods.
    """

    def __init__(
        self,
        *,
        base_url: str,
        api_key_id: str | None = None,
        private_key_pem: str | None = None,
        timeout_seconds: float = 10.0,
        now_ms: Callable[[], int] | None = None,
        signer: KalshiSigner | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key_id = api_key_id
        self.private_key_pem = private_key_pem
        self.timeout_seconds = timeout_seconds
        self.now_ms = now_ms
        self.signer = signer
        self.transport = transport

    def get_exchange_status(self) -> KalshiJson:
        return self._request_json("/exchange/status")

    def get_markets(
        self,
        *,
        series_ticker: str,
        status: str | None = "open",
        limit: int = 100,
        cursor: str | None = None,
    ) -> KalshiJson:
        params: dict[str, str] = {
            "series_ticker": series_ticker,
            "limit": str(limit),
        }
        if status:
            params["status"] = status
        if cursor:
            params["cursor"] = cursor

        return self._request_json(f"/markets?{urlencode(params)}")

    def get_market(self, ticker: str) -> KalshiJson:
        return self._request_json(f"/markets/{quote(ticker, safe='')}")

    def get_market_orderbook(self, ticker: str) -> KalshiJson:
        return self._request_json(f"/markets/{quote(ticker, safe='')}/orderbook")

    def _request_json(self, path: str) -> KalshiJson:
        headers = self._headers_for_path(path)
        full_url = f"{self.base_url}{path}"
        signature = headers.get("KALSHI-ACCESS-SIGNATURE")

        try:
            with httpx.Client(
                timeout=self.timeout_seconds,
                transport=self.transport,
                follow_redirects=False,
            ) as client:
                response = client.get(full_url, headers=headers)
        except httpx.RequestError as exc:
            raise KalshiUnreachableError("Kalshi REST request failed or timed out.") from exc

        if response.status_code >= 400:
            request_id = response.headers.get("x-request-id") or response.headers.get(
                "X-Request-Id"
            )
            raise KalshiRequestError(
                status_code=response.status_code,
                path=path,
                request_id=request_id,
                body_preview=_redact_text(
                    response.text[:500],
                    values=[self.api_key_id, signature],
                ),
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise KalshiRequestError(
                status_code=response.status_code,
                path=path,
                request_id=response.headers.get("x-request-id"),
                body_preview="non-json response",
            ) from exc

        if not isinstance(payload, dict):
            raise KalshiRequestError(
                status_code=response.status_code,
                path=path,
                request_id=response.headers.get("x-request-id"),
                body_preview="unexpected non-object response",
            )

        return payload

    def _headers_for_path(self, path: str) -> SafeHeaders:
        if not self.api_key_id or not self.private_key_pem:
            return {}

        full_url = f"{self.base_url}{path}"
        request_path = urlsplit(full_url).path
        timestamp_ms = self.now_ms() if self.now_ms else None
        signer = self.signer if self.signer else None

        return create_auth_headers(
            api_key_id=self.api_key_id,
            private_key_pem=self.private_key_pem,
            method="GET",
            request_path=request_path,
            timestamp_ms=timestamp_ms,
            signer=signer or sign_payload,
        )


def _redact_text(text: str, values: list[str | None]) -> str:
    redacted = text
    for value in values:
        if value:
            redacted = redacted.replace(value, "[redacted]")
    return redacted
