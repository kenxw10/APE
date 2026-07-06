from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from ape.config import AppConfig
from ape.db.models import ReferenceTick
from ape.db.session import create_engine_from_config, create_session_factory
from ape.kalshi.diagnostics import build_kalshi_config_diagnostic
from ape.kalshi.reference_messages import BRTI_SOURCE
from ape.repositories.reference_ticks import ReferenceTicksRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository

WORKER_SERVICE_NAME = "ape-worker"


@dataclass(frozen=True)
class BrtiReferenceStatusSnapshot:
    configured: bool
    enabled: bool
    signer_ready: bool
    source: str
    index_ids: list[str]
    subscription_id: int | None
    connection_state: str
    latest_tick_received_at: datetime | None
    latest_source_ts: datetime | None
    latest_parsed_value: Decimal | None
    latest_trailing_60s_avg: Decimal | None
    latest_trailing_60s_window_size: int | None
    latest_final_minute_average: Decimal | None
    final_minute_average_status: str | None
    source_age_ms: int | None
    stale: bool
    last_message_at: datetime | None
    last_persisted_at: datetime | None
    last_error_type: str | None
    last_error_message: str | None
    warnings: list[str]
    blockers: list[str]
    checked_at: datetime


@dataclass(frozen=True)
class BrtiReferenceLatestSnapshot:
    found: bool
    source: str
    received_at: datetime | None
    source_ts: datetime | None
    kalshi_received_at: datetime | None
    parsed_value: Decimal | None
    trailing_60s_avg: Decimal | None
    trailing_60s_window_size: int | None
    last_60s_windowed_average_15min: Decimal | None
    final_minute_average_window_size: int | None
    final_minute_average_status: str | None
    source_age_ms: int | None
    parse_status: str | None
    sequence_number: int | None
    subscription_id: str | None
    raw_payload_hash: str | None


def build_brti_reference_status(
    config: AppConfig,
    *,
    now: datetime | None = None,
) -> BrtiReferenceStatusSnapshot:
    checked_at = now or datetime.now(UTC)
    diagnostic = build_kalshi_config_diagnostic(config)
    warnings: list[str] = []
    blockers: list[str] = []
    heartbeat_metadata: dict[str, Any] = {}
    latest_tick: ReferenceTick | None = None

    config_enabled = config.kalshi_cfbenchmarks_enabled
    if not config_enabled:
        connection_state = "disabled"
    elif not diagnostic.signer_ready:
        connection_state = "not_configured"
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
                        reference = _dict_or_empty(heartbeat.metadata_.get("reference"))
                        heartbeat_metadata = _dict_or_empty(reference.get("brti"))
                    latest_tick = ReferenceTicksRepository(session).get_latest_tick(BRTI_SOURCE)
            finally:
                engine.dispose()
        except SQLAlchemyError:
            blockers.append("database_unavailable_for_brti_diagnostics")
            if config_enabled:
                connection_state = "diagnostics_unavailable"
    elif config_enabled:
        blockers.append("database_not_configured_for_brti_diagnostics")

    if heartbeat_metadata:
        connection_state = (
            _str_or_none(heartbeat_metadata.get("connection_state")) or connection_state
        )
        warnings.extend(_string_list(heartbeat_metadata.get("warnings")))
        blockers.extend(_string_list(heartbeat_metadata.get("blockers")))

    enabled = _bool_or_none(heartbeat_metadata.get("enabled"))
    effective_enabled = config_enabled if enabled is None else enabled
    configured = _bool_or_none(heartbeat_metadata.get("configured"))
    effective_configured = diagnostic.configured if configured is None else configured
    signer_ready = _bool_or_none(heartbeat_metadata.get("signer_ready"))
    effective_signer_ready = (
        diagnostic.signer_ready if signer_ready is None else signer_ready
    )
    if effective_enabled and not effective_signer_ready:
        blockers.append("kalshi_cfbenchmarks_credentials_not_configured_or_not_parseable")

    last_message_at = _datetime_or_none(heartbeat_metadata.get("last_message_at"))
    latest_received_at = latest_tick.received_at if latest_tick else None
    last_persisted_at = _latest_datetime(
        _datetime_or_none(heartbeat_metadata.get("last_persisted_at")),
        latest_received_at,
    )
    latest_source_age_ms = latest_tick.source_age_ms if latest_tick else None
    stale_receipt = _is_stale(
        enabled=effective_enabled and effective_signer_ready and not blockers,
        latest_tick_received_at=last_persisted_at,
        checked_at=checked_at,
        stale_after_seconds=config.kalshi_cfbenchmarks_stale_after_seconds,
    )
    stale_source_age = _is_source_age_stale(
        enabled=effective_enabled and effective_signer_ready and not blockers,
        source_age_ms=latest_source_age_ms,
        max_source_age_ms=config.kalshi_cfbenchmarks_max_source_age_ms,
    )
    stale = stale_receipt or stale_source_age
    if stale_receipt:
        warnings.append("brti_reference_stale")
    if stale_source_age:
        warnings.append("brti_reference_source_age_stale")

    return BrtiReferenceStatusSnapshot(
        configured=effective_configured,
        enabled=effective_enabled,
        signer_ready=effective_signer_ready,
        source=BRTI_SOURCE,
        index_ids=(
            _string_list(heartbeat_metadata.get("index_ids"))
            or list(config.kalshi_cfbenchmarks_index_ids)
        ),
        subscription_id=_int_or_none(heartbeat_metadata.get("subscription_id")),
        connection_state=connection_state,
        latest_tick_received_at=latest_received_at,
        latest_source_ts=latest_tick.source_ts if latest_tick else None,
        latest_parsed_value=latest_tick.parsed_value if latest_tick else None,
        latest_trailing_60s_avg=latest_tick.trailing_60s_avg if latest_tick else None,
        latest_trailing_60s_window_size=(
            latest_tick.trailing_60s_window_size if latest_tick else None
        ),
        latest_final_minute_average=(
            latest_tick.last_60s_windowed_average_15min if latest_tick else None
        ),
        final_minute_average_status=(
            latest_tick.final_minute_average_status if latest_tick else None
        ),
        source_age_ms=latest_source_age_ms,
        stale=stale,
        last_message_at=last_message_at,
        last_persisted_at=last_persisted_at,
        last_error_type=_str_or_none(heartbeat_metadata.get("last_error_type")),
        last_error_message=_str_or_none(heartbeat_metadata.get("last_error_message")),
        warnings=sorted(set(warnings)),
        blockers=sorted(set(blockers)),
        checked_at=checked_at,
    )


def build_brti_reference_latest(config: AppConfig) -> BrtiReferenceLatestSnapshot:
    latest_tick: ReferenceTick | None = None
    if config.database_url:
        try:
            engine = create_engine_from_config(config)
            try:
                session_factory = create_session_factory(engine)
                with session_factory() as session:
                    latest_tick = ReferenceTicksRepository(session).get_latest_tick(
                        BRTI_SOURCE
                    )
            finally:
                engine.dispose()
        except SQLAlchemyError:
            latest_tick = None

    if latest_tick is None:
        return BrtiReferenceLatestSnapshot(
            found=False,
            source=BRTI_SOURCE,
            received_at=None,
            source_ts=None,
            kalshi_received_at=None,
            parsed_value=None,
            trailing_60s_avg=None,
            trailing_60s_window_size=None,
            last_60s_windowed_average_15min=None,
            final_minute_average_window_size=None,
            final_minute_average_status=None,
            source_age_ms=None,
            parse_status=None,
            sequence_number=None,
            subscription_id=None,
            raw_payload_hash=None,
        )

    return BrtiReferenceLatestSnapshot(
        found=True,
        source=latest_tick.source,
        received_at=latest_tick.received_at,
        source_ts=latest_tick.source_ts,
        kalshi_received_at=latest_tick.kalshi_received_at,
        parsed_value=latest_tick.parsed_value,
        trailing_60s_avg=latest_tick.trailing_60s_avg,
        trailing_60s_window_size=latest_tick.trailing_60s_window_size,
        last_60s_windowed_average_15min=latest_tick.last_60s_windowed_average_15min,
        final_minute_average_window_size=latest_tick.final_minute_average_window_size,
        final_minute_average_status=latest_tick.final_minute_average_status,
        source_age_ms=latest_tick.source_age_ms,
        parse_status=latest_tick.parse_status,
        sequence_number=latest_tick.sequence_number,
        subscription_id=latest_tick.subscription_id,
        raw_payload_hash=latest_tick.raw_payload_hash,
    )


def _is_stale(
    *,
    enabled: bool,
    latest_tick_received_at: datetime | None,
    checked_at: datetime,
    stale_after_seconds: float,
) -> bool:
    if not enabled:
        return False
    if latest_tick_received_at is None:
        return True
    return (checked_at - latest_tick_received_at).total_seconds() > stale_after_seconds


def _is_source_age_stale(
    *,
    enabled: bool,
    source_age_ms: int | None,
    max_source_age_ms: int,
) -> bool:
    if not enabled or source_age_ms is None:
        return False
    return source_age_ms > max_source_age_ms


def _dict_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item is not None]


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


def _latest_datetime(*values: datetime | None) -> datetime | None:
    present = [_as_utc(value) for value in values if value is not None]
    return max(present) if present else None


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bool_or_none(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    return None
