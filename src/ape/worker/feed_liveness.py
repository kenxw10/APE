from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from ape.config import AppConfig
from ape.db.models import OrderbookSnapshot, PublicTrade, ReferenceTick, WorkerHeartbeat
from ape.kalshi.reference_messages import BRTI_SOURCE
from ape.repositories.orderbook import OrderbookRepository
from ape.repositories.public_trades import PublicTradesRepository
from ape.repositories.reference_ticks import ReferenceTicksRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.worker.services import (
    WORKER_SERVICE_AGGREGATE,
    WORKER_SERVICE_MARKET_WS,
    WORKER_SERVICE_REFERENCE_BRTI,
)

FEED_LIVENESS_LEGACY_FALLBACK_WARNING = "feed_liveness_legacy_aggregate_fallback"
LIVENESS_SOURCE_COMPONENT = "component"
LIVENESS_SOURCE_LEGACY_FALLBACK = "legacy_aggregate_fallback"
LIVENESS_SOURCE_MISSING = "missing"


@dataclass(frozen=True)
class MarketFeedLiveness:
    source: str
    metadata: dict[str, Any] | None
    heartbeat_at: datetime | None
    heartbeat_age_ms: int | None
    started_at: datetime | None
    component_heartbeat_at: datetime | None
    component_heartbeat_age_ms: int | None
    latest_aggregate_heartbeat_mode: str | None
    latest_component_heartbeat_mode: str | None
    liveness_source_mismatch: bool
    warnings: list[str]
    active_market_ticker: str | None
    latest_orderbook: OrderbookSnapshot | None
    latest_trade: PublicTrade | None
    latest_orderbook_received_at: datetime | None
    latest_trade_received_at: datetime | None
    stream_last_message_at: datetime | None
    stream_age_ms: int | None


@dataclass(frozen=True)
class ReferenceFeedLiveness:
    source: str
    metadata: dict[str, Any] | None
    heartbeat_at: datetime | None
    heartbeat_age_ms: int | None
    started_at: datetime | None
    component_heartbeat_at: datetime | None
    component_heartbeat_age_ms: int | None
    latest_aggregate_heartbeat_mode: str | None
    latest_component_heartbeat_mode: str | None
    liveness_source_mismatch: bool
    warnings: list[str]
    latest_tick: ReferenceTick | None
    latest_valid_tick: ReferenceTick | None
    stream_last_valid_message_at: datetime | None
    stream_age_ms: int | None


def load_market_feed_liveness(
    session: Session,
    config: AppConfig,
    *,
    checked_at: datetime,
    active_market_ticker: str | None = None,
) -> MarketFeedLiveness:
    repository = WorkerHeartbeatRepository(session)
    component = repository.get_latest_heartbeat(WORKER_SERVICE_MARKET_WS)
    aggregate = repository.get_latest_heartbeat(WORKER_SERVICE_AGGREGATE)
    component_metadata = _ws_metadata(component)
    aggregate_metadata = _ws_metadata(aggregate)
    selected, metadata, source, warnings = _select_component_metadata(
        component=component,
        component_metadata=component_metadata,
        aggregate=aggregate,
        aggregate_metadata=aggregate_metadata,
    )
    component_heartbeat_at = _heartbeat_at(component) if component_metadata else None
    heartbeat_at = _heartbeat_at(selected)
    active_ticker = (
        active_market_ticker
        or _str_or_none((metadata or {}).get("active_market_ticker"))
    )
    orderbook_repository = OrderbookRepository(session)
    latest_orderbook = (
        orderbook_repository.get_latest_snapshot(active_ticker)
        if active_ticker
        else orderbook_repository.get_latest_snapshot_any()
    )
    latest_trade = PublicTradesRepository(session).get_latest_trade(active_ticker)
    stream_last_message_at = _latest_datetime(
        _datetime_or_none((metadata or {}).get("last_message_at")),
        _datetime_or_none((metadata or {}).get("last_ticker_at")),
        _datetime_or_none((metadata or {}).get("last_trade_at")),
        _datetime_or_none((metadata or {}).get("last_orderbook_at")),
    )
    return MarketFeedLiveness(
        source=source,
        metadata=metadata,
        heartbeat_at=heartbeat_at,
        heartbeat_age_ms=_age_ms(heartbeat_at, checked_at),
        started_at=_started_at(selected),
        component_heartbeat_at=component_heartbeat_at,
        component_heartbeat_age_ms=_age_ms(component_heartbeat_at, checked_at),
        latest_aggregate_heartbeat_mode=_metadata_mode(aggregate),
        latest_component_heartbeat_mode=_metadata_mode(component),
        liveness_source_mismatch=_liveness_source_mismatch(
            source=source,
            component=component,
            aggregate=aggregate,
        ),
        warnings=warnings,
        active_market_ticker=active_ticker,
        latest_orderbook=latest_orderbook,
        latest_trade=latest_trade,
        latest_orderbook_received_at=(
            _as_utc(latest_orderbook.received_at) if latest_orderbook else None
        ),
        latest_trade_received_at=(
            _as_utc(latest_trade.received_at) if latest_trade else None
        ),
        stream_last_message_at=stream_last_message_at,
        stream_age_ms=_age_ms(stream_last_message_at, checked_at),
    )


def load_reference_feed_liveness(
    session: Session,
    config: AppConfig,
    *,
    checked_at: datetime,
) -> ReferenceFeedLiveness:
    del config
    repository = WorkerHeartbeatRepository(session)
    component = repository.get_latest_heartbeat(WORKER_SERVICE_REFERENCE_BRTI)
    aggregate = repository.get_latest_heartbeat(WORKER_SERVICE_AGGREGATE)
    component_metadata = _reference_metadata(component)
    aggregate_metadata = _reference_metadata(aggregate)
    selected, metadata, source, warnings = _select_component_metadata(
        component=component,
        component_metadata=component_metadata,
        aggregate=aggregate,
        aggregate_metadata=aggregate_metadata,
    )
    component_heartbeat_at = _heartbeat_at(component) if component_metadata else None
    heartbeat_at = _heartbeat_at(selected)
    reference_repository = ReferenceTicksRepository(session)
    latest_tick = reference_repository.get_latest_tick(BRTI_SOURCE)
    latest_valid_tick = reference_repository.get_latest_valid_tick(BRTI_SOURCE)
    stream_last_valid_message_at = _datetime_or_none(
        (metadata or {}).get("last_valid_message_at")
    )
    return ReferenceFeedLiveness(
        source=source,
        metadata=metadata,
        heartbeat_at=heartbeat_at,
        heartbeat_age_ms=_age_ms(heartbeat_at, checked_at),
        started_at=_started_at(selected),
        component_heartbeat_at=component_heartbeat_at,
        component_heartbeat_age_ms=_age_ms(component_heartbeat_at, checked_at),
        latest_aggregate_heartbeat_mode=_metadata_mode(aggregate),
        latest_component_heartbeat_mode=_metadata_mode(component),
        liveness_source_mismatch=_liveness_source_mismatch(
            source=source,
            component=component,
            aggregate=aggregate,
        ),
        warnings=warnings,
        latest_tick=latest_tick,
        latest_valid_tick=latest_valid_tick,
        stream_last_valid_message_at=stream_last_valid_message_at,
        stream_age_ms=_age_ms(stream_last_valid_message_at, checked_at),
    )


def _select_component_metadata(
    *,
    component: WorkerHeartbeat | None,
    component_metadata: dict[str, Any] | None,
    aggregate: WorkerHeartbeat | None,
    aggregate_metadata: dict[str, Any] | None,
) -> tuple[WorkerHeartbeat | None, dict[str, Any] | None, str, list[str]]:
    if component is not None and component_metadata is not None:
        return component, component_metadata, LIVENESS_SOURCE_COMPONENT, []
    if aggregate_metadata is not None:
        return (
            aggregate,
            aggregate_metadata,
            LIVENESS_SOURCE_LEGACY_FALLBACK,
            [FEED_LIVENESS_LEGACY_FALLBACK_WARNING],
        )
    return None, None, LIVENESS_SOURCE_MISSING, []


def _ws_metadata(heartbeat: WorkerHeartbeat | None) -> dict[str, Any] | None:
    if heartbeat is None or not isinstance(heartbeat.metadata_, dict):
        return None
    ws_metadata = heartbeat.metadata_.get("ws")
    return ws_metadata if isinstance(ws_metadata, dict) else None


def _reference_metadata(heartbeat: WorkerHeartbeat | None) -> dict[str, Any] | None:
    if heartbeat is None or not isinstance(heartbeat.metadata_, dict):
        return None
    reference = heartbeat.metadata_.get("reference")
    if not isinstance(reference, dict):
        return None
    brti = reference.get("brti")
    return brti if isinstance(brti, dict) else None


def _metadata_mode(heartbeat: WorkerHeartbeat | None) -> str | None:
    if heartbeat is None or not isinstance(heartbeat.metadata_, dict):
        return None
    return _str_or_none(heartbeat.metadata_.get("mode"))


def _heartbeat_at(heartbeat: WorkerHeartbeat | None) -> datetime | None:
    return _as_utc(heartbeat.heartbeat_at) if heartbeat is not None else None


def _started_at(heartbeat: WorkerHeartbeat | None) -> datetime | None:
    return _as_utc(heartbeat.started_at) if heartbeat is not None else None


def _liveness_source_mismatch(
    *,
    source: str,
    component: WorkerHeartbeat | None,
    aggregate: WorkerHeartbeat | None,
) -> bool:
    del component, aggregate
    return source == LIVENESS_SOURCE_LEGACY_FALLBACK


def _age_ms(value_at: datetime | None, checked_at: datetime) -> int | None:
    if value_at is None:
        return None
    return max(0, int((_as_utc(checked_at) - _as_utc(value_at)).total_seconds() * 1000))


def _latest_datetime(*values: datetime | None) -> datetime | None:
    present = [_as_utc(value) for value in values if value is not None]
    return max(present) if present else None


def _datetime_or_none(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _as_utc(value)
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
