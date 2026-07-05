from __future__ import annotations


class KalshiError(RuntimeError):
    """Base error for observer-only Kalshi REST helpers."""


class KalshiAuthError(KalshiError):
    """Raised when Kalshi credentials cannot sign a safe read request."""


class KalshiConfigurationError(KalshiError):
    """Raised when Kalshi configuration is incomplete or invalid."""


class KalshiUnreachableError(KalshiError):
    """Raised when Kalshi cannot be reached within the configured timeout."""


class KalshiRequestError(KalshiError):
    """Raised for non-success Kalshi REST responses with redacted context."""

    def __init__(
        self,
        *,
        status_code: int,
        path: str,
        request_id: str | None = None,
        body_preview: str | None = None,
    ) -> None:
        super().__init__(f"Kalshi request failed with status {status_code} for {path}.")
        self.status_code = status_code
        self.path = path
        self.request_id = request_id
        self.body_preview = body_preview

