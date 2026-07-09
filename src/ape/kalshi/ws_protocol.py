from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from ape.config import AppConfig
from ape.db.models import KalshiWsProtocolEvent
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.kalshi_ws_protocol import KalshiWsProtocolEventRepository


@dataclass(frozen=True)
class KalshiWsProtocolEventSnapshot:
    id: int
    created_at: datetime
    worker_service: str | None
    worker_role: str | None
    connection_id: str | None
    channel: str | None
    active_market_ticker: str | None
    command_id: int | None
    command_type: str | None
    command_action: str | None
    sid: int | None
    expected_sid: int | None
    seq: int | None
    event_type: str
    event_subtype: str | None
    raw_code: str | None
    raw_message: str | None
    close_code: int | None
    close_reason: str | None
    exception_type: str | None
    exception_message: str | None
    latency_ms: int | None
    round_trip_ms: int | None
    ping_sent_at: datetime | None
    pong_received_at: datetime | None
    server_ping_received_at: datetime | None
    client_pong_sent_at: datetime | None
    subscription_state_before: str | None
    subscription_state_after: str | None
    recovery_action: str | None
    recovery_result: str | None
    raw_payload_hash: str | None
    payload_summary_json: dict[str, Any] | list[Any] | str | int | float | bool | None


@dataclass(frozen=True)
class KalshiWsProtocolRecentSnapshot:
    limit: int
    count: int
    events: list[KalshiWsProtocolEventSnapshot]
    checked_at: datetime
    warnings: list[str]


@dataclass(frozen=True)
class KalshiWsProtocolSummarySnapshot:
    window_seconds: int
    checked_at: datetime
    since: datetime
    total: int
    error_count: int
    close_count: int
    reconnect_count: int
    by_event_type: dict[str, int]
    latest_event_at: datetime | None
    warnings: list[str]


def build_kalshi_ws_protocol_recent(
    config: AppConfig,
    *,
    limit: int = 200,
    now: datetime | None = None,
) -> KalshiWsProtocolRecentSnapshot:
    checked_at = _as_utc(now or datetime.now(UTC))
    capped_limit = min(max(limit, 1), 500)
    warnings: list[str] = []
    events: list[KalshiWsProtocolEventSnapshot] = []
    if not config.database_url:
        warnings.append("database_not_configured_for_ws_protocol")
        return KalshiWsProtocolRecentSnapshot(
            limit=capped_limit,
            count=0,
            events=[],
            checked_at=checked_at,
            warnings=warnings,
        )

    try:
        engine = create_engine_from_config(config)
        try:
            session_factory = create_session_factory(engine)
            with session_factory() as session:
                rows = KalshiWsProtocolEventRepository(session).list_recent(
                    limit=capped_limit
                )
                events = [_event_snapshot(row) for row in rows]
        finally:
            engine.dispose()
    except SQLAlchemyError:
        warnings.append("database_unavailable_for_ws_protocol")

    return KalshiWsProtocolRecentSnapshot(
        limit=capped_limit,
        count=len(events),
        events=events,
        checked_at=checked_at,
        warnings=warnings,
    )


def build_kalshi_ws_protocol_summary(
    config: AppConfig,
    *,
    window_seconds: int = 1800,
    now: datetime | None = None,
) -> KalshiWsProtocolSummarySnapshot:
    checked_at = _as_utc(now or datetime.now(UTC))
    capped_window = min(max(window_seconds, 1), 86_400)
    since = checked_at - timedelta(seconds=capped_window)
    warnings: list[str] = []
    summary: dict[str, Any] = {
        "total": 0,
        "error_count": 0,
        "close_count": 0,
        "reconnect_count": 0,
        "by_event_type": {},
        "latest_event_at": None,
    }
    if not config.database_url:
        warnings.append("database_not_configured_for_ws_protocol")
    else:
        try:
            engine = create_engine_from_config(config)
            try:
                session_factory = create_session_factory(engine)
                with session_factory() as session:
                    summary = KalshiWsProtocolEventRepository(session).summary_since(
                        since=since
                    )
            finally:
                engine.dispose()
        except SQLAlchemyError:
            warnings.append("database_unavailable_for_ws_protocol")

    return KalshiWsProtocolSummarySnapshot(
        window_seconds=capped_window,
        checked_at=checked_at,
        since=since,
        total=int(summary["total"]),
        error_count=int(summary["error_count"]),
        close_count=int(summary["close_count"]),
        reconnect_count=int(summary["reconnect_count"]),
        by_event_type={
            str(key): int(value)
            for key, value in dict(summary["by_event_type"]).items()
        },
        latest_event_at=summary["latest_event_at"],
        warnings=warnings,
    )


def _event_snapshot(row: KalshiWsProtocolEvent) -> KalshiWsProtocolEventSnapshot:
    return KalshiWsProtocolEventSnapshot(
        id=row.id,
        created_at=_as_utc(row.created_at),
        worker_service=row.worker_service,
        worker_role=row.worker_role,
        connection_id=row.connection_id,
        channel=row.channel,
        active_market_ticker=row.active_market_ticker,
        command_id=row.command_id,
        command_type=row.command_type,
        command_action=row.command_action,
        sid=row.sid,
        expected_sid=row.expected_sid,
        seq=row.seq,
        event_type=row.event_type,
        event_subtype=row.event_subtype,
        raw_code=row.raw_code,
        raw_message=row.raw_message,
        close_code=row.close_code,
        close_reason=row.close_reason,
        exception_type=row.exception_type,
        exception_message=row.exception_message,
        latency_ms=row.latency_ms,
        round_trip_ms=row.round_trip_ms,
        ping_sent_at=_as_utc_or_none(row.ping_sent_at),
        pong_received_at=_as_utc_or_none(row.pong_received_at),
        server_ping_received_at=_as_utc_or_none(row.server_ping_received_at),
        client_pong_sent_at=_as_utc_or_none(row.client_pong_sent_at),
        subscription_state_before=row.subscription_state_before,
        subscription_state_after=row.subscription_state_after,
        recovery_action=row.recovery_action,
        recovery_result=row.recovery_result,
        raw_payload_hash=row.raw_payload_hash,
        payload_summary_json=row.payload_summary_json,
    )


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _as_utc_or_none(value: datetime | None) -> datetime | None:
    return _as_utc(value) if value is not None else None
