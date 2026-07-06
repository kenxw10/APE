from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from ape.kalshi.ws_messages import parse_decimal, raw_payload_hash
from ape.repositories.inputs import JsonPayload, ReferenceTickInput

BRTI_SOURCE = "kalshi_cfbenchmarks_brti"
BRTI_INDEX_ID = "BRTI"


@dataclass(frozen=True)
class ParsedReferenceMessage:
    kind: str
    index_id: str | None = None
    tick: ReferenceTickInput | None = None
    reason: str | None = None
    warning: str | None = None
    raw_payload_hash: str | None = None


def is_cfbenchmarks_value_payload(payload: Any) -> bool:
    return isinstance(payload, dict) and payload.get("type") == "cfbenchmarks_value"


def parse_cfbenchmarks_value_message(
    payload: Any,
    *,
    received_at: datetime,
    allowed_index_ids: tuple[str, ...],
    persist_raw_payload: bool,
) -> ParsedReferenceMessage:
    raw_hash = raw_payload_hash(payload)
    raw_payload = _json_payload(payload) if persist_raw_payload else None
    if not isinstance(payload, dict):
        return ParsedReferenceMessage(
            kind="invalid",
            reason="cfbenchmarks_message_not_object",
            raw_payload_hash=raw_hash,
        )

    sid = _int_or_none(payload.get("sid"))
    seq = _int_or_none(payload.get("seq"))
    msg = payload.get("msg")
    if not isinstance(msg, dict):
        return ParsedReferenceMessage(
            kind="invalid",
            reason="cfbenchmarks_payload_missing",
            raw_payload_hash=raw_hash,
        )

    index_id = _str_or_none(msg.get("index_id"))
    if index_id not in allowed_index_ids:
        return ParsedReferenceMessage(
            kind="ignored",
            index_id=index_id,
            reason="non_target_cfbenchmarks_index",
            raw_payload_hash=raw_hash,
        )
    if index_id != BRTI_INDEX_ID:
        return ParsedReferenceMessage(
            kind="ignored",
            index_id=index_id,
            reason="non_brti_cfbenchmarks_index",
            raw_payload_hash=raw_hash,
        )

    data = _data_payload(msg.get("data"))
    source_ts = _timestamp_or_none(data.get("time")) if isinstance(data, dict) else None
    kalshi_received_at = _timestamp_or_none(msg.get("received_at"))
    raw_value = _str_or_none(data.get("value")) if isinstance(data, dict) else None
    parsed_value: Decimal | None = None
    parse_status = "valid"
    warning: str | None = None

    try:
        parsed_value = parse_decimal(raw_value)
    except ValueError:
        parse_status = "malformed_value"
        warning = "brti_malformed_value"

    avg_value: Decimal | None = None
    avg_window_size: int | None = None
    if isinstance(msg.get("avg_60s_data"), dict):
        avg_payload = msg["avg_60s_data"]
        avg_window_size = _int_or_none(avg_payload.get("window_size"))
        try:
            avg_value = parse_decimal(avg_payload.get("value"))
        except ValueError:
            if parse_status == "valid":
                parse_status = "malformed_avg_60s"
                warning = "brti_malformed_avg_60s"
    else:
        parse_status = "malformed_avg_60s" if parse_status == "valid" else parse_status
        warning = warning or "brti_malformed_avg_60s"

    final_value: Decimal | None = None
    final_window_size: int | None = None
    final_status = "absent"
    final_payload = msg.get("last_60s_windowed_average_15min")
    if final_payload is not None:
        final_status = "malformed"
        if isinstance(final_payload, dict):
            final_window_size = _int_or_none(final_payload.get("window_size"))
            try:
                final_value = parse_decimal(final_payload.get("value"))
                final_status = "present"
            except ValueError:
                pass
        if final_status == "malformed" and parse_status == "valid":
            parse_status = "malformed_final_minute_average"
            warning = "brti_malformed_final_minute_average"

    source_age_ms = _source_age_ms(received_at, source_ts)
    return ParsedReferenceMessage(
        kind="tick",
        index_id=index_id,
        tick=ReferenceTickInput(
            source=BRTI_SOURCE,
            received_at=received_at,
            parse_status=parse_status,
            source_ts=source_ts,
            kalshi_received_at=kalshi_received_at,
            raw_value=raw_value,
            parsed_value=parsed_value,
            trailing_60s_avg=avg_value,
            trailing_60s_window_size=avg_window_size,
            last_60s_windowed_average_15min=final_value,
            final_minute_average_window_size=final_window_size,
            final_minute_average_status=final_status,
            sequence_number=seq,
            subscription_id=str(sid) if sid is not None else None,
            source_age_ms=source_age_ms,
            raw_payload_hash=raw_hash,
            raw_payload=raw_payload,
        ),
        warning=warning,
        raw_payload_hash=raw_hash,
    )


def _data_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _source_age_ms(received_at: datetime, source_ts: datetime | None) -> int | None:
    if source_ts is None:
        return None
    elapsed_seconds = (
        received_at.astimezone(UTC) - source_ts.astimezone(UTC)
    ).total_seconds()
    return max(0, int(elapsed_seconds * 1000))


def _timestamp_or_none(value: Any) -> datetime | None:
    if isinstance(value, int | float):
        divisor = 1000 if value > 10_000_000_000 else 1
        return datetime.fromtimestamp(value / divisor, tz=UTC)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            numeric = float(text)
        except ValueError:
            normalized = text.replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(normalized)
            except ValueError:
                return None
            return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        return _timestamp_or_none(numeric)
    return None


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _json_payload(value: Any) -> JsonPayload:
    if isinstance(value, dict | list | str | int | float | bool):
        return value
    return str(value)
