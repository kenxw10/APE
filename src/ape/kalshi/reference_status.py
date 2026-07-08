from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from ape.config import AppConfig
from ape.db.models import ReferenceTick
from ape.db.session import create_engine_from_config, create_session_factory
from ape.kalshi.diagnostics import build_kalshi_config_diagnostic
from ape.kalshi.reference_messages import BRTI_SOURCE
from ape.repositories.reference_ticks import ReferenceTicksRepository
from ape.worker.feed_liveness import load_reference_feed_liveness

BRTI_SERIES_MAX_WINDOW_SECONDS = 900
BRTI_SERIES_MAX_POINTS = 16_000


@dataclass(frozen=True)
class BrtiReferenceStatusSnapshot:
    configured: bool
    enabled: bool
    signer_ready: bool
    source: str
    index_ids: list[str]
    subscription_id: int | None
    subscription_request_id: int | None
    subscribed_channels: list[str]
    connection_state: str
    status_category: str
    connection_state_detail: str | None
    liveness_source: str
    worker_heartbeat_at: datetime | None
    worker_heartbeat_age_ms: int | None
    worker_started_at: datetime | None
    component_heartbeat_at: datetime | None
    component_heartbeat_age_ms: int | None
    latest_aggregate_heartbeat_mode: str | None
    latest_component_heartbeat_mode: str | None
    liveness_source_mismatch: bool
    worker_heartbeat_stale: bool
    last_connected_at: datetime | None
    last_successful_subscribe_at: datetime | None
    last_subscription_ack_at: datetime | None
    latest_tick_received_at: datetime | None
    last_valid_tick_at: datetime | None
    last_healthy_at: datetime | None
    last_recovered_at: datetime | None
    latest_source_ts: datetime | None
    latest_parsed_value: Decimal | None
    latest_trailing_60s_avg: Decimal | None
    latest_trailing_60s_window_size: int | None
    latest_final_minute_average: Decimal | None
    final_minute_average_status: str | None
    source_age_ms: int | None
    kalshi_age_ms: int | None
    upstream_to_kalshi_lag_ms: int | None
    backend_transport_lag_ms: int | None
    inter_arrival_ms: int | None
    source_gap_ms: int | None
    duplicate_source_ts_count: int
    out_of_order_source_ts_count: int
    skipped_tick_count: int
    last_skipped_reason: str | None
    last_skipped_at: datetime | None
    transport_stale: bool
    source_stale: bool
    kalshi_received_stale: bool
    persistence_stale: bool
    trade_ready_fresh: bool
    stale: bool
    stale_reason: str | None
    stale_age_ms: int | None
    stale_since: datetime | None
    last_message_at: datetime | None
    last_persisted_at: datetime | None
    time_since_last_message_ms: int | None
    time_since_last_persisted_ms: int | None
    time_since_last_valid_tick_ms: int | None
    last_error_type: str | None
    last_error_message: str | None
    reconnect_count: int
    recovery_state: str | None
    consecutive_stale_count: int
    consecutive_reconnect_count: int
    consecutive_fresh_tick_count: int
    recommended_action: str | None
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


@dataclass(frozen=True)
class BrtiReferenceSeriesPointSnapshot:
    received_at: datetime
    source_ts: datetime | None
    kalshi_received_at: datetime | None
    parsed_value: Decimal | None
    trailing_60s_avg: Decimal | None
    last_60s_windowed_average_15min: Decimal | None
    final_minute_average_status: str | None
    source_age_ms: int | None
    parse_status: str | None
    sequence_number: int | None
    raw_payload_hash: str | None


@dataclass(frozen=True)
class BrtiReferenceSeriesSnapshot:
    source: str
    window_seconds: int
    max_points: int
    point_count: int
    generated_at: datetime
    points: list[BrtiReferenceSeriesPointSnapshot]


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
    liveness_source = "missing"
    worker_heartbeat_at: datetime | None = None
    worker_heartbeat_age_ms: int | None = None
    worker_started_at: datetime | None = None
    component_heartbeat_at: datetime | None = None
    component_heartbeat_age_ms: int | None = None
    latest_aggregate_heartbeat_mode: str | None = None
    latest_component_heartbeat_mode: str | None = None
    liveness_source_mismatch = False
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
                    liveness = load_reference_feed_liveness(
                        session,
                        config,
                        checked_at=checked_at,
                    )
                    heartbeat_metadata = liveness.metadata or {}
                    warnings.extend(liveness.warnings)
                    liveness_source = liveness.source
                    worker_heartbeat_at = liveness.heartbeat_at
                    worker_heartbeat_age_ms = liveness.heartbeat_age_ms
                    worker_started_at = liveness.started_at
                    component_heartbeat_at = liveness.component_heartbeat_at
                    component_heartbeat_age_ms = liveness.component_heartbeat_age_ms
                    latest_aggregate_heartbeat_mode = (
                        liveness.latest_aggregate_heartbeat_mode
                    )
                    latest_component_heartbeat_mode = (
                        liveness.latest_component_heartbeat_mode
                    )
                    liveness_source_mismatch = liveness.liveness_source_mismatch
                    latest_tick = liveness.latest_tick
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

    worker_heartbeat_stale = _is_stale(
        enabled=effective_enabled and effective_signer_ready and not blockers,
        latest_tick_received_at=worker_heartbeat_at,
        checked_at=checked_at,
        stale_after_seconds=config.kalshi_cfbenchmarks_heartbeat_stale_after_seconds,
    )
    if worker_heartbeat_stale:
        warnings.append("brti_reference_worker_heartbeat_stale")

    last_message_at = _datetime_or_none(heartbeat_metadata.get("last_message_at"))
    latest_received_at = latest_tick.received_at if latest_tick else None
    last_persisted_at = _latest_datetime(
        _datetime_or_none(heartbeat_metadata.get("last_persisted_at")),
        latest_received_at,
    )
    last_valid_message_at = _datetime_or_none(
        heartbeat_metadata.get("last_valid_message_at")
    )
    valid_message_carried_forward = (
        _bool_or_none(heartbeat_metadata.get("valid_message_carried_forward")) is True
    )
    persistence_fresh_at = (
        _latest_datetime(last_persisted_at, last_valid_message_at)
        if valid_message_carried_forward
        else last_persisted_at
    )
    last_valid_tick_at = _latest_datetime(
        _datetime_or_none(heartbeat_metadata.get("last_valid_tick_at")),
        latest_received_at if _reference_tick_valid(latest_tick) else None,
    )
    latest_source_age_ms = latest_tick.source_age_ms if latest_tick else None
    latest_kalshi_age_ms = (
        _age_ms(checked_at, latest_tick.kalshi_received_at) if latest_tick else None
    )
    upstream_to_kalshi_lag_ms = (
        _lag_ms(latest_tick.kalshi_received_at, latest_tick.source_ts)
        if latest_tick
        else None
    )
    backend_transport_lag_ms = _age_ms(checked_at, last_message_at)
    time_since_last_message_ms = backend_transport_lag_ms
    time_since_last_persisted_ms = _age_ms(checked_at, last_persisted_at)
    time_since_last_valid_tick_ms = _age_ms(checked_at, last_valid_tick_at)
    transport_stale = _is_stale(
        enabled=effective_enabled and effective_signer_ready and not blockers,
        latest_tick_received_at=last_message_at,
        checked_at=checked_at,
        stale_after_seconds=config.kalshi_cfbenchmarks_transport_stale_after_seconds,
    )
    persistence_stale = _is_stale(
        enabled=effective_enabled and effective_signer_ready and not blockers,
        latest_tick_received_at=persistence_fresh_at,
        checked_at=checked_at,
        stale_after_seconds=config.kalshi_cfbenchmarks_persistence_stale_after_seconds,
    )
    source_stale = _is_source_age_stale(
        enabled=effective_enabled and effective_signer_ready and not blockers,
        source_age_ms=latest_source_age_ms,
        max_source_age_ms=config.kalshi_cfbenchmarks_source_age_warn_ms,
    )
    kalshi_received_stale = _is_source_age_stale(
        enabled=effective_enabled and effective_signer_ready and not blockers,
        source_age_ms=latest_kalshi_age_ms,
        max_source_age_ms=config.kalshi_cfbenchmarks_kalshi_received_warn_ms,
    )
    trade_ready_fresh = _is_trade_ready_fresh(
        enabled=effective_enabled and effective_signer_ready and not blockers,
        transport_stale=transport_stale,
        persistence_stale=persistence_stale,
        source_age_ms=latest_source_age_ms,
        kalshi_age_ms=latest_kalshi_age_ms,
        trade_fresh_ms=config.kalshi_cfbenchmarks_trade_fresh_ms,
    )
    metadata_warnings = _string_list(heartbeat_metadata.get("warnings"))
    worker_timeout_stale = _worker_timeout_reason(metadata_warnings) is not None
    stale = (
        worker_heartbeat_stale
        or transport_stale
        or persistence_stale
        or worker_timeout_stale
    )
    if transport_stale:
        warnings.append("brti_reference_transport_stale")
    if persistence_stale:
        warnings.append("brti_reference_persistence_stale")
    if source_stale:
        warnings.append("brti_reference_source_age_stale")
    if kalshi_received_stale:
        warnings.append("brti_reference_kalshi_received_stale")

    stale_reason, stale_age_ms, stale_since = _stale_details(
        checked_at=checked_at,
        worker_heartbeat_stale=worker_heartbeat_stale,
        worker_heartbeat_at=worker_heartbeat_at,
        worker_heartbeat_stale_after_seconds=(
            config.kalshi_cfbenchmarks_heartbeat_stale_after_seconds
        ),
        transport_stale=transport_stale,
        last_message_at=last_message_at,
        transport_stale_after_seconds=(
            config.kalshi_cfbenchmarks_transport_stale_after_seconds
        ),
        persistence_stale=persistence_stale,
        last_persisted_at=persistence_fresh_at,
        persistence_stale_after_seconds=(
            config.kalshi_cfbenchmarks_persistence_stale_after_seconds
        ),
        source_stale=source_stale,
        source_age_ms=latest_source_age_ms,
        source_age_warn_ms=config.kalshi_cfbenchmarks_source_age_warn_ms,
        kalshi_received_stale=kalshi_received_stale,
        kalshi_age_ms=latest_kalshi_age_ms,
        kalshi_received_warn_ms=(
            config.kalshi_cfbenchmarks_kalshi_received_warn_ms
        ),
        metadata_stale_since=_datetime_or_none(heartbeat_metadata.get("stale_since")),
        metadata_warnings=metadata_warnings,
    )
    recovery_state = _str_or_none(heartbeat_metadata.get("recovery_state"))
    last_connected_at = _datetime_or_none(heartbeat_metadata.get("last_connected_at"))
    last_successful_subscribe_at = _datetime_or_none(
        heartbeat_metadata.get("last_successful_subscribe_at")
    )
    last_subscription_ack_at = _datetime_or_none(
        heartbeat_metadata.get("last_subscription_ack_at")
    )
    current_subscription_at = _latest_datetime(
        last_connected_at,
        last_successful_subscribe_at,
        last_subscription_ack_at,
    )
    status_category = _status_category(
        enabled=effective_enabled,
        signer_ready=effective_signer_ready,
        blockers=blockers,
        connection_state=connection_state,
        latest_tick=latest_tick,
        last_valid_tick_at=last_valid_tick_at,
        current_subscription_at=current_subscription_at,
        recovery_state=recovery_state,
        worker_heartbeat_stale=worker_heartbeat_stale,
        transport_stale=transport_stale,
        persistence_stale=persistence_stale,
        source_stale=source_stale,
        kalshi_received_stale=kalshi_received_stale,
        warnings=warnings,
        last_error_type=_str_or_none(heartbeat_metadata.get("last_error_type")),
    )
    recommended_action = _recommended_action(
        status_category=status_category,
        stale_reason=stale_reason,
        blockers=blockers,
        last_error_type=_str_or_none(heartbeat_metadata.get("last_error_type")),
    )

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
        subscription_request_id=_int_or_none(
            heartbeat_metadata.get("subscription_request_id")
        ),
        subscribed_channels=_string_list(heartbeat_metadata.get("subscribed_channels")),
        connection_state=connection_state,
        status_category=status_category,
        connection_state_detail=_connection_state_detail(
            connection_state=connection_state,
            recovery_state=recovery_state,
            stale_reason=stale_reason,
        ),
        liveness_source=liveness_source,
        worker_heartbeat_at=worker_heartbeat_at,
        worker_heartbeat_age_ms=worker_heartbeat_age_ms,
        worker_started_at=worker_started_at,
        component_heartbeat_at=component_heartbeat_at,
        component_heartbeat_age_ms=component_heartbeat_age_ms,
        latest_aggregate_heartbeat_mode=latest_aggregate_heartbeat_mode,
        latest_component_heartbeat_mode=latest_component_heartbeat_mode,
        liveness_source_mismatch=liveness_source_mismatch,
        worker_heartbeat_stale=worker_heartbeat_stale,
        last_connected_at=last_connected_at,
        last_successful_subscribe_at=last_successful_subscribe_at,
        last_subscription_ack_at=last_subscription_ack_at,
        latest_tick_received_at=latest_received_at,
        last_valid_tick_at=last_valid_tick_at,
        last_healthy_at=_datetime_or_none(heartbeat_metadata.get("last_healthy_at")),
        last_recovered_at=_datetime_or_none(heartbeat_metadata.get("last_recovered_at")),
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
        kalshi_age_ms=latest_kalshi_age_ms,
        upstream_to_kalshi_lag_ms=upstream_to_kalshi_lag_ms,
        backend_transport_lag_ms=backend_transport_lag_ms,
        inter_arrival_ms=_int_or_none(heartbeat_metadata.get("inter_arrival_ms")),
        source_gap_ms=_int_or_none(heartbeat_metadata.get("source_gap_ms")),
        duplicate_source_ts_count=_int_or_zero(
            heartbeat_metadata.get("duplicate_source_ts_count")
        ),
        out_of_order_source_ts_count=_int_or_zero(
            heartbeat_metadata.get("out_of_order_source_ts_count")
        ),
        skipped_tick_count=_int_or_zero(heartbeat_metadata.get("skipped_tick_count")),
        last_skipped_reason=_str_or_none(heartbeat_metadata.get("last_skipped_reason")),
        last_skipped_at=_datetime_or_none(heartbeat_metadata.get("last_skipped_at")),
        transport_stale=transport_stale,
        source_stale=source_stale,
        kalshi_received_stale=kalshi_received_stale,
        persistence_stale=persistence_stale,
        trade_ready_fresh=trade_ready_fresh,
        stale=stale,
        stale_reason=stale_reason,
        stale_age_ms=stale_age_ms,
        stale_since=stale_since,
        last_message_at=last_message_at,
        last_persisted_at=last_persisted_at,
        time_since_last_message_ms=time_since_last_message_ms,
        time_since_last_persisted_ms=time_since_last_persisted_ms,
        time_since_last_valid_tick_ms=time_since_last_valid_tick_ms,
        last_error_type=_str_or_none(heartbeat_metadata.get("last_error_type")),
        last_error_message=_str_or_none(heartbeat_metadata.get("last_error_message")),
        reconnect_count=_int_or_zero(heartbeat_metadata.get("reconnect_count")),
        recovery_state=recovery_state,
        consecutive_stale_count=_int_or_zero(
            heartbeat_metadata.get("consecutive_stale_count")
        ),
        consecutive_reconnect_count=_int_or_zero(
            heartbeat_metadata.get("consecutive_reconnect_count")
        ),
        consecutive_fresh_tick_count=_int_or_zero(
            heartbeat_metadata.get("consecutive_fresh_tick_count")
        ),
        recommended_action=recommended_action,
        warnings=sorted(set(warnings)),
        blockers=sorted(set(blockers)),
        checked_at=checked_at,
    )


def build_brti_reference_series(
    config: AppConfig,
    *,
    window_seconds: int = BRTI_SERIES_MAX_WINDOW_SECONDS,
    max_points: int = BRTI_SERIES_MAX_POINTS,
    since: datetime | None = None,
    include_final_minute: bool = False,
    now: datetime | None = None,
) -> BrtiReferenceSeriesSnapshot:
    generated_at = now or datetime.now(UTC)
    bounded_window_seconds = min(max(1, window_seconds), BRTI_SERIES_MAX_WINDOW_SECONDS)
    bounded_max_points = min(max(1, max_points), BRTI_SERIES_MAX_POINTS)
    window_start = generated_at - timedelta(seconds=bounded_window_seconds)
    if since is not None:
        since_utc = _as_utc(since)
        if since_utc > window_start:
            window_start = since_utc

    rows: list[ReferenceTick] = []
    if config.database_url:
        try:
            engine = create_engine_from_config(config)
            try:
                session_factory = create_session_factory(engine)
                with session_factory() as session:
                    rows = ReferenceTicksRepository(session).get_ticks_since(
                        BRTI_SOURCE,
                        window_start,
                        limit=bounded_max_points,
                    )
            finally:
                engine.dispose()
        except SQLAlchemyError:
            rows = []

    points = [
        BrtiReferenceSeriesPointSnapshot(
            received_at=row.received_at,
            source_ts=row.source_ts,
            kalshi_received_at=row.kalshi_received_at,
            parsed_value=row.parsed_value,
            trailing_60s_avg=row.trailing_60s_avg,
            last_60s_windowed_average_15min=(
                row.last_60s_windowed_average_15min if include_final_minute else None
            ),
            final_minute_average_status=row.final_minute_average_status,
            source_age_ms=row.source_age_ms,
            parse_status=row.parse_status,
            sequence_number=row.sequence_number,
            raw_payload_hash=row.raw_payload_hash,
        )
        for row in rows
    ]
    return BrtiReferenceSeriesSnapshot(
        source=BRTI_SOURCE,
        window_seconds=bounded_window_seconds,
        max_points=bounded_max_points,
        point_count=len(points),
        generated_at=generated_at,
        points=points,
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


def _reference_tick_valid(tick: ReferenceTick | None) -> bool:
    return (
        tick is not None
        and tick.parse_status == "valid"
        and tick.received_at is not None
        and tick.parsed_value is not None
    )


def _stale_details(
    *,
    checked_at: datetime,
    worker_heartbeat_stale: bool,
    worker_heartbeat_at: datetime | None,
    worker_heartbeat_stale_after_seconds: float,
    transport_stale: bool,
    last_message_at: datetime | None,
    transport_stale_after_seconds: float,
    persistence_stale: bool,
    last_persisted_at: datetime | None,
    persistence_stale_after_seconds: float,
    source_stale: bool,
    source_age_ms: int | None,
    source_age_warn_ms: int,
    kalshi_received_stale: bool,
    kalshi_age_ms: int | None,
    kalshi_received_warn_ms: int,
    metadata_stale_since: datetime | None,
    metadata_warnings: list[str],
) -> tuple[str | None, int | None, datetime | None]:
    if worker_heartbeat_stale:
        return _time_stale_details(
            "brti_reference_worker_heartbeat_stale",
            checked_at=checked_at,
            value_at=worker_heartbeat_at,
            stale_after_seconds=worker_heartbeat_stale_after_seconds,
            metadata_stale_since=metadata_stale_since,
        )
    worker_timeout_reason = _worker_timeout_reason(metadata_warnings)
    if worker_timeout_reason is not None:
        stale_age_ms = _age_ms(checked_at, metadata_stale_since)
        return worker_timeout_reason, stale_age_ms, metadata_stale_since
    if transport_stale:
        return _time_stale_details(
            "brti_reference_transport_stale",
            checked_at=checked_at,
            value_at=last_message_at,
            stale_after_seconds=transport_stale_after_seconds,
            metadata_stale_since=metadata_stale_since,
        )
    if persistence_stale:
        return _time_stale_details(
            "brti_reference_persistence_stale",
            checked_at=checked_at,
            value_at=last_persisted_at,
            stale_after_seconds=persistence_stale_after_seconds,
            metadata_stale_since=metadata_stale_since,
        )
    if source_stale:
        stale_age_ms = None
        if source_age_ms is not None:
            stale_age_ms = max(0, source_age_ms - source_age_warn_ms)
        return "brti_reference_source_age_stale", stale_age_ms, metadata_stale_since
    if kalshi_received_stale:
        stale_age_ms = None
        if kalshi_age_ms is not None:
            stale_age_ms = max(0, kalshi_age_ms - kalshi_received_warn_ms)
        return (
            "brti_reference_kalshi_received_stale",
            stale_age_ms,
            metadata_stale_since,
        )
    return None, None, None


def _worker_timeout_reason(warnings: list[str]) -> str | None:
    for warning in (
        "brti_reference_first_tick_timeout",
        "brti_reference_no_valid_tick_timeout",
    ):
        if warning in warnings:
            return warning
    return None


def _time_stale_details(
    reason: str,
    *,
    checked_at: datetime,
    value_at: datetime | None,
    stale_after_seconds: float,
    metadata_stale_since: datetime | None,
) -> tuple[str, int | None, datetime | None]:
    if value_at is None:
        return reason, None, metadata_stale_since
    threshold_at = _as_utc(value_at) + timedelta(seconds=stale_after_seconds)
    stale_age_ms = max(0, int((_as_utc(checked_at) - threshold_at).total_seconds() * 1000))
    return reason, stale_age_ms, metadata_stale_since or threshold_at


def _status_category(
    *,
    enabled: bool,
    signer_ready: bool,
    blockers: list[str],
    connection_state: str,
    latest_tick: ReferenceTick | None,
    last_valid_tick_at: datetime | None,
    current_subscription_at: datetime | None,
    recovery_state: str | None,
    worker_heartbeat_stale: bool,
    transport_stale: bool,
    persistence_stale: bool,
    source_stale: bool,
    kalshi_received_stale: bool,
    warnings: list[str],
    last_error_type: str | None,
) -> str:
    if not enabled:
        return "disabled"
    if not signer_ready or blockers:
        return "waiting"
    if worker_heartbeat_stale:
        return "worker_stale"
    if "brti_persistence_failed" in warnings:
        return "persistence_error"
    if _worker_timeout_reason(warnings) is not None or connection_state in {
        "stale",
        "reconnect_pending",
    }:
        return "stale_transport"
    if connection_state == "error" or last_error_type is not None:
        return "stale_transport"
    if transport_stale:
        return "stale_transport"
    if persistence_stale:
        return "stale_persistence"
    if source_stale or kalshi_received_stale:
        return "upstream_lag"
    if recovery_state in {"connecting", "waiting_for_fresh_tick", "recovering"}:
        return "waiting"
    if current_subscription_at is not None and (
        last_valid_tick_at is None or _as_utc(last_valid_tick_at) < current_subscription_at
    ):
        return "waiting"
    if latest_tick is None or not _reference_tick_valid(latest_tick) or connection_state in {
        "waiting_for_worker",
        "waiting_for_fresh_tick",
        "connected",
    }:
        return "waiting"
    return "healthy"


def _recommended_action(
    *,
    status_category: str,
    stale_reason: str | None,
    blockers: list[str],
    last_error_type: str | None,
) -> str | None:
    if status_category == "disabled":
        return None
    if blockers:
        if "kalshi_cfbenchmarks_subscription_error" in blockers:
            return "check_kalshi_cfbenchmarks_entitlement_or_index_id"
        if "kalshi_cfbenchmarks_credentials_not_configured_or_not_parseable" in blockers:
            return "check_worker_kalshi_credentials"
        if "database_not_configured_for_brti_diagnostics" in blockers:
            return "configure_database_url_for_reference_diagnostics"
        return "inspect_reference_blockers"
    if status_category == "worker_stale":
        return "restart_or_inspect_railway_worker"
    if status_category == "persistence_error" or last_error_type:
        return "inspect_database_and_worker_logs"
    if stale_reason in {
        "brti_reference_transport_stale",
        "brti_reference_first_tick_timeout",
        "brti_reference_no_valid_tick_timeout",
    }:
        return "inspect_cfbenchmarks_websocket_subscription"
    if status_category == "stale_persistence":
        return "inspect_reference_tick_persistence"
    if status_category == "upstream_lag":
        return "monitor_cfbenchmarks_source_age"
    if status_category == "waiting":
        return "wait_for_worker_subscription_and_first_tick"
    return None


def _connection_state_detail(
    *,
    connection_state: str,
    recovery_state: str | None,
    stale_reason: str | None,
) -> str | None:
    details = [connection_state]
    if recovery_state:
        details.append(recovery_state)
    if stale_reason:
        details.append(stale_reason)
    return ":".join(details) if details else None


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


def _is_trade_ready_fresh(
    *,
    enabled: bool,
    transport_stale: bool,
    persistence_stale: bool,
    source_age_ms: int | None,
    kalshi_age_ms: int | None,
    trade_fresh_ms: int,
) -> bool:
    if not enabled or transport_stale or persistence_stale:
        return False
    if source_age_ms is None or kalshi_age_ms is None:
        return False
    return source_age_ms <= trade_fresh_ms and kalshi_age_ms <= trade_fresh_ms


def _age_ms(checked_at: datetime, value: datetime | None) -> int | None:
    if value is None:
        return None
    return max(0, int((_as_utc(checked_at) - _as_utc(value)).total_seconds() * 1000))


def _lag_ms(end: datetime | None, start: datetime | None) -> int | None:
    if end is None or start is None:
        return None
    return max(0, int((_as_utc(end) - _as_utc(start)).total_seconds() * 1000))


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


def _int_or_zero(value: Any) -> int:
    parsed = _int_or_none(value)
    return parsed if parsed is not None else 0


def _bool_or_none(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    return None
