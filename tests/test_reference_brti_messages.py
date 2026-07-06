from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

from ape.kalshi.reference_messages import (
    BRTI_SOURCE,
    parse_cfbenchmarks_value_message,
)

RECEIVED_AT = datetime(2026, 7, 5, 12, 0, 2, tzinfo=UTC)
SOURCE_TS = datetime(2026, 7, 5, 12, 0, 0, tzinfo=UTC)
SOURCE_TS_MS = int(SOURCE_TS.timestamp() * 1000)


def test_parse_valid_brti_payload_with_json_string_data() -> None:
    result = parse_cfbenchmarks_value_message(
        _payload(),
        received_at=RECEIVED_AT,
        allowed_index_ids=("BRTI",),
        persist_raw_payload=True,
    )

    assert result.kind == "tick"
    assert result.index_id == "BRTI"
    assert result.tick is not None
    assert result.tick.source == BRTI_SOURCE
    assert result.tick.source_ts == SOURCE_TS
    assert result.tick.kalshi_received_at == datetime(2026, 7, 5, 12, 0, 1, tzinfo=UTC)
    assert result.tick.raw_value == "68000.12"
    assert result.tick.parsed_value == Decimal("68000.12")
    assert result.tick.trailing_60s_avg == Decimal("67999.50")
    assert result.tick.trailing_60s_window_size == 60
    assert result.tick.last_60s_windowed_average_15min is None
    assert result.tick.final_minute_average_status == "absent"
    assert result.tick.sequence_number == 7
    assert result.tick.subscription_id == "3"
    assert result.tick.source_age_ms == 2000
    assert result.tick.parse_status == "valid"
    assert result.tick.raw_payload_hash
    assert result.tick.raw_payload is not None


def test_parse_valid_brti_payload_with_final_minute_average() -> None:
    payload = _payload(
        last_60s_windowed_average_15min={
            "value": "68001.25",
            "window_size": 60,
            "window_start_ts_ms": SOURCE_TS_MS,
            "window_end_ts_exclusive": SOURCE_TS_MS + 60_000,
        }
    )

    result = parse_cfbenchmarks_value_message(
        payload,
        received_at=RECEIVED_AT,
        allowed_index_ids=("BRTI",),
        persist_raw_payload=False,
    )

    assert result.tick is not None
    assert result.tick.last_60s_windowed_average_15min == Decimal("68001.25")
    assert result.tick.final_minute_average_window_size == 60
    assert result.tick.final_minute_average_status == "present"
    assert result.tick.raw_payload is None


def test_parse_malformed_brti_value_persists_malformed_status() -> None:
    payload = _payload(data={"type": "value", "id": "BRTI", "time": SOURCE_TS_MS, "value": "bad"})

    result = parse_cfbenchmarks_value_message(
        payload,
        received_at=RECEIVED_AT,
        allowed_index_ids=("BRTI",),
        persist_raw_payload=True,
    )

    assert result.tick is not None
    assert result.tick.parse_status == "malformed_value"
    assert result.tick.parsed_value is None
    assert result.warning == "brti_malformed_value"


def test_parse_malformed_avg_60s_value_flags_status() -> None:
    payload = _payload(avg_60s_data={"value": "bad", "window_size": 60})

    result = parse_cfbenchmarks_value_message(
        payload,
        received_at=RECEIVED_AT,
        allowed_index_ids=("BRTI",),
        persist_raw_payload=True,
    )

    assert result.tick is not None
    assert result.tick.parse_status == "malformed_avg_60s"
    assert result.tick.trailing_60s_avg is None
    assert result.warning == "brti_malformed_avg_60s"


def test_parse_malformed_final_minute_average_flags_status() -> None:
    payload = _payload(last_60s_windowed_average_15min={"value": "bad", "window_size": 60})

    result = parse_cfbenchmarks_value_message(
        payload,
        received_at=RECEIVED_AT,
        allowed_index_ids=("BRTI",),
        persist_raw_payload=True,
    )

    assert result.tick is not None
    assert result.tick.parse_status == "malformed_final_minute_average"
    assert result.tick.final_minute_average_status == "malformed"
    assert result.warning == "brti_malformed_final_minute_average"


def test_non_target_index_is_ignored() -> None:
    payload = _payload(index_id="OTHER")

    result = parse_cfbenchmarks_value_message(
        payload,
        received_at=RECEIVED_AT,
        allowed_index_ids=("BRTI",),
        persist_raw_payload=True,
    )

    assert result.kind == "ignored"
    assert result.reason == "non_target_cfbenchmarks_index"
    assert result.tick is None


def test_allowed_non_brti_index_is_ignored() -> None:
    payload = _payload(index_id="OTHER")

    result = parse_cfbenchmarks_value_message(
        payload,
        received_at=RECEIVED_AT,
        allowed_index_ids=("BRTI", "OTHER"),
        persist_raw_payload=True,
    )

    assert result.kind == "ignored"
    assert result.reason == "non_brti_cfbenchmarks_index"
    assert result.tick is None


def _payload(**overrides):
    msg = {
        "index_id": overrides.pop("index_id", "BRTI"),
        "received_at": "2026-07-05T12:00:01Z",
        "data": json.dumps(
            overrides.pop(
                "data",
                {
                    "type": "value",
                    "id": "BRTI",
                    "time": SOURCE_TS_MS,
                    "value": "68000.12",
                },
            )
        ),
        "avg_60s_data": overrides.pop(
            "avg_60s_data",
            {
                "value": "67999.50",
                "window_size": 60,
                "window_start_ts_ms": SOURCE_TS_MS - 60_000,
                "window_end_ts_exclusive": SOURCE_TS_MS,
            },
        ),
    }
    msg.update(overrides)
    return {
        "type": "cfbenchmarks_value",
        "sid": 3,
        "seq": 7,
        "msg": msg,
    }
