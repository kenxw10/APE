from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit

from ape.config import AppConfig
from ape.kalshi.auth import private_key_parseable


@dataclass(frozen=True)
class KalshiConfigDiagnostic:
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


def build_kalshi_config_diagnostic(config: AppConfig) -> KalshiConfigDiagnostic:
    api_key_configured = bool(config.kalshi_api_key_id)
    private_key_configured = bool(config.kalshi_private_key)
    parseable = private_key_parseable(config.kalshi_private_key)

    return KalshiConfigDiagnostic(
        configured=api_key_configured and private_key_configured,
        signer_ready=api_key_configured and private_key_configured and parseable,
        base_url_host=urlsplit(config.kalshi_api_base_url).netloc,
        api_key_configured=api_key_configured,
        private_key_configured=private_key_configured,
        private_key_parseable=parseable,
        kalshi_env=config.kalshi_env,
        series_ticker=config.kalshi_btc15_series_ticker,
        timeout_seconds=config.kalshi_rest_timeout_seconds,
        parser_version=config.kalshi_resolver_parser_version,
    )
