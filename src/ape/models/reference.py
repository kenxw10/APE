from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel

from ape.kalshi.reference_status import (
    BrtiReferenceLatestSnapshot,
    BrtiReferenceSeriesPointSnapshot,
    BrtiReferenceSeriesSnapshot,
    BrtiReferenceStatusSnapshot,
)


class BrtiReferenceStatusResponse(BaseModel):
    configured: bool
    enabled: bool
    signer_ready: bool
    source: str
    index_ids: list[str]
    subscription_id: int | None
    subscription_request_id: int | None
    subscribed_channels: list[str]
    connection_state: str
    last_connected_at: datetime | None
    latest_tick_received_at: datetime | None
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
    last_message_at: datetime | None
    last_persisted_at: datetime | None
    last_error_type: str | None
    last_error_message: str | None
    reconnect_count: int
    warnings: list[str]
    blockers: list[str]
    checked_at: datetime


class BrtiReferenceLatestResponse(BaseModel):
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


class BrtiReferenceSeriesPointResponse(BaseModel):
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


class BrtiReferenceSeriesResponse(BaseModel):
    source: str
    window_seconds: int
    max_points: int
    point_count: int
    generated_at: datetime
    points: list[BrtiReferenceSeriesPointResponse]


def brti_reference_status_response(
    snapshot: BrtiReferenceStatusSnapshot,
) -> BrtiReferenceStatusResponse:
    return BrtiReferenceStatusResponse(**snapshot.__dict__)


def brti_reference_latest_response(
    snapshot: BrtiReferenceLatestSnapshot,
) -> BrtiReferenceLatestResponse:
    return BrtiReferenceLatestResponse(**snapshot.__dict__)


def brti_reference_series_point_response(
    snapshot: BrtiReferenceSeriesPointSnapshot,
) -> BrtiReferenceSeriesPointResponse:
    return BrtiReferenceSeriesPointResponse(**snapshot.__dict__)


def brti_reference_series_response(
    snapshot: BrtiReferenceSeriesSnapshot,
) -> BrtiReferenceSeriesResponse:
    return BrtiReferenceSeriesResponse(
        source=snapshot.source,
        window_seconds=snapshot.window_seconds,
        max_points=snapshot.max_points,
        point_count=snapshot.point_count,
        generated_at=snapshot.generated_at,
        points=[
            brti_reference_series_point_response(point) for point in snapshot.points
        ],
    )
