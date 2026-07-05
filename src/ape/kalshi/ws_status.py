from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy.exc import SQLAlchemyError

from ape.config import AppConfig
from ape.db.session import create_engine_from_config, create_session_factory
from ape.kalshi.diagnostics import build_kalshi_config_diagnostic
from ape.repositories.orderbook import OrderbookRepository
from ape.repositories.public_trades import PublicTradesRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository

WORKER_SERVICE_NAME = "ape-worker"


@dataclass(frozen=True)
class KalshiWsStatusSnapshot:
    configured: bool
    enabled: bool
    signer_ready: bool
    endpoint_host: str
    endpoint_path: str
    connection_state: str
    active_market_ticker: str | None
    subscribed_channels: list[str]
    subscription_ids: dict[str, int]
    last_connected_at: datetime | None
    last_message_at: datetime | None
    last_ticker_at: datetime | None
    last_orderbook_at: datetime | None
    last_trade_at: datetime | None
    latest_orderbook_received_at: datetime | None
    latest_trade_received_at: datetime | None
    reconnect_count: int
    last_error_type: str | None
    last_error_message: str | None
    warnings: list[str]
    blockers: list[str]
    diagnostic_samples: list[dict[str, Any]]
    stale: bool
    checked_at: datetime


def build_kalshi_ws_status(
    config: AppConfig,
    *,
    now: datetime | None = None,
) -> KalshiWsStatusSnapshot:
    checked_at = now or datetime.now(UTC)
    diagnostic = build_kalshi_config_diagnostic(config)
    parsed_endpoint = urlsplit(config.kalshi_ws_base_url)
    warnings: list[str] = []
    blockers: list[str] = []
    heartbeat_metadata: dict[str, Any] = {}
    active_market_ticker: str | None = None
    latest_orderbook_at: datetime | None = None
    latest_trade_at: datetime | None = None

    if not config.kalshi_ws_enabled:
        connection_state = "disabled"
    elif not diagnostic.signer_ready:
        connection_state = "not_configured"
        blockers.append("kalshi_ws_credentials_not_configured_or_not_parseable")
    else:
        connection_state = "waiting_for_worker"

    if config.database_url:
        try:
            engine = create_engine_from_config(config)
            try:
                session_factory = create_session_factory(engine)
                with session_factory() as session:
                    heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat(
                        WORKER_SERVICE_NAME
                    )
                    if heartbeat is not None and isinstance(heartbeat.metadata_, dict):
                        heartbeat_metadata = _dict_or_empty(heartbeat.metadata_.get("ws"))

                    active_market_ticker = _str_or_none(
                        heartbeat_metadata.get("active_market_ticker")
                    )
                    latest_orderbook = (
                        OrderbookRepository(session).get_latest_snapshot(active_market_ticker)
                        if active_market_ticker
                        else OrderbookRepository(session).get_latest_snapshot_any()
                    )
                    latest_trade = PublicTradesRepository(session).get_latest_trade(
                        active_market_ticker
                    )
                    latest_orderbook_at = (
                        latest_orderbook.received_at if latest_orderbook else None
                    )
                    latest_trade_at = latest_trade.received_at if latest_trade else None
            finally:
                engine.dispose()
        except SQLAlchemyError:
            blockers.append("database_unavailable_for_ws_diagnostics")
            if config.kalshi_ws_enabled:
                connection_state = "diagnostics_unavailable"
    elif config.kalshi_ws_enabled:
        blockers.append("database_not_configured_for_ws_diagnostics")

    if heartbeat_metadata:
        connection_state = (
            _str_or_none(heartbeat_metadata.get("connection_state")) or connection_state
        )
        warnings.extend(_string_list(heartbeat_metadata.get("warnings")))
        blockers.extend(_string_list(heartbeat_metadata.get("blockers")))

    enabled = _bool_or_none(heartbeat_metadata.get("enabled"))
    effective_enabled = config.kalshi_ws_enabled if enabled is None else enabled
    configured = _bool_or_none(heartbeat_metadata.get("configured"))
    effective_configured = diagnostic.configured if configured is None else configured
    signer_ready = _bool_or_none(heartbeat_metadata.get("signer_ready"))
    effective_signer_ready = diagnostic.signer_ready if signer_ready is None else signer_ready

    last_message_at = _datetime_or_none(heartbeat_metadata.get("last_message_at"))
    if last_message_at is None:
        last_message_at = _latest_datetime(latest_orderbook_at, latest_trade_at)

    stale = _is_stale(
        enabled=effective_enabled,
        last_message_at=last_message_at,
        checked_at=checked_at,
        stale_after_seconds=config.kalshi_ws_heartbeat_timeout_seconds,
    )
    if stale:
        warnings.append("kalshi_ws_data_stale")

    final_warnings = sorted(set(warnings))
    final_blockers = sorted(set(blockers))
    last_error_type = _str_or_none(heartbeat_metadata.get("last_error_type"))
    last_error_message = _str_or_none(heartbeat_metadata.get("last_error_message"))
    if _healthy_stream_recovered_error(
        connection_state=connection_state,
        stale=stale,
        warnings=final_warnings,
        blockers=final_blockers,
        last_message_at=last_message_at,
        latest_orderbook_at=latest_orderbook_at,
        latest_trade_at=latest_trade_at,
    ):
        last_error_type = None
        last_error_message = None

    return KalshiWsStatusSnapshot(
        configured=effective_configured,
        enabled=effective_enabled,
        signer_ready=effective_signer_ready,
        endpoint_host=parsed_endpoint.netloc,
        endpoint_path=parsed_endpoint.path,
        connection_state=connection_state,
        active_market_ticker=active_market_ticker,
        subscribed_channels=_string_list(heartbeat_metadata.get("subscribed_channels")),
        subscription_ids=_int_dict(heartbeat_metadata.get("subscription_ids")),
        last_connected_at=_datetime_or_none(heartbeat_metadata.get("last_connected_at")),
        last_message_at=last_message_at,
        last_ticker_at=_datetime_or_none(heartbeat_metadata.get("last_ticker_at")),
        last_orderbook_at=_latest_datetime(
            _datetime_or_none(heartbeat_metadata.get("last_orderbook_at")),
            latest_orderbook_at,
        ),
        last_trade_at=_latest_datetime(
            _datetime_or_none(heartbeat_metadata.get("last_trade_at")),
            latest_trade_at,
        ),
        latest_orderbook_received_at=latest_orderbook_at,
        latest_trade_received_at=latest_trade_at,
        reconnect_count=_int_or_zero(heartbeat_metadata.get("reconnect_count")),
        last_error_type=last_error_type,
        last_error_message=last_error_message,
        warnings=final_warnings,
        blockers=final_blockers,
        diagnostic_samples=_diagnostic_samples(
            heartbeat_metadata.get("diagnostic_samples")
        ),
        stale=stale,
        checked_at=checked_at,
    )


def _healthy_stream_recovered_error(
    *,
    connection_state: str,
    stale: bool,
    warnings: list[str],
    blockers: list[str],
    last_message_at: datetime | None,
    latest_orderbook_at: datetime | None,
    latest_trade_at: datetime | None,
) -> bool:
    latest_persisted_at = _latest_datetime(latest_orderbook_at, latest_trade_at)
    return (
        connection_state == "subscribed"
        and not stale
        and not warnings
        and not blockers
        and last_message_at is not None
        and latest_persisted_at is not None
        and latest_persisted_at >= _as_utc(last_message_at)
    )


def _is_stale(
    *,
    enabled: bool,
    last_message_at: datetime | None,
    checked_at: datetime,
    stale_after_seconds: float,
) -> bool:
    if not enabled:
        return False
    if last_message_at is None:
        return True
    return (checked_at - last_message_at).total_seconds() > stale_after_seconds


def _latest_datetime(*values: datetime | None) -> datetime | None:
    present = [_as_utc(value) for value in values if value is not None]
    return max(present) if present else None


def _dict_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item is not None]


def _int_dict(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    parsed: dict[str, int] = {}
    for key, item in value.items():
        try:
            parsed[str(key)] = int(item)
        except (TypeError, ValueError):
            continue
    return parsed


def _diagnostic_samples(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)][:3]


def _datetime_or_none(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _as_utc(value)
    if not isinstance(value, str) or not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_or_zero(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _bool_or_none(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    return None
