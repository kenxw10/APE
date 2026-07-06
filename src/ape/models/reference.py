from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel

from ape.kalshi.reference_status import (
    BrtiReferenceLatestSnapshot,
    BrtiReferenceStatusSnapshot,
)


class BrtiReferenceStatusResponse(BaseModel):
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


def brti_reference_status_response(
    snapshot: BrtiReferenceStatusSnapshot,
) -> BrtiReferenceStatusResponse:
    return BrtiReferenceStatusResponse(**snapshot.__dict__)


def brti_reference_latest_response(
    snapshot: BrtiReferenceLatestSnapshot,
) -> BrtiReferenceLatestResponse:
    return BrtiReferenceLatestResponse(**snapshot.__dict__)
