from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from ape.repositories.inputs import JsonPayload, PublicTradeInput

ONE_DOLLAR = Decimal("1")
CONTRACT_COUNT_QUANTUM = Decimal("0.01")


class WsMessageParseError(ValueError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class PriceLevel:
    price: Decimal
    size: Decimal


@dataclass(frozen=True)
class ParsedWsMessage:
    kind: str
    sid: int | None = None
    seq: int | None = None
    market_ticker: str | None = None
    source_ts: datetime | None = None
    yes_levels: list[PriceLevel] | None = None
    no_levels: list[PriceLevel] | None = None
    delta_side: str | None = None
    delta_price: Decimal | None = None
    delta_size: Decimal | None = None
    trade: PublicTradeInput | None = None
    control_type: str | None = None
    reason: str | None = None
    warning: str | None = None
    raw_payload_hash: str | None = None
    raw_payload: JsonPayload | None = None


def parse_ws_payload(
    payload: Any,
    *,
    target_market_ticker: str,
    received_at: datetime | None = None,
) -> ParsedWsMessage:
    received = received_at or datetime.now(UTC)
    if not isinstance(payload, dict):
        return ParsedWsMessage(kind="invalid", reason="message_not_object")

    message_type = _str_or_none(payload.get("type"))
    sid = _int_or_none(payload.get("sid"))
    seq = _int_or_none(payload.get("seq"))
    message = payload.get("msg")
    raw_hash = raw_payload_hash(payload)
    raw_payload = _json_payload(payload)

    if message_type in {"subscribed", "ok", "unsubscribed"}:
        return ParsedWsMessage(
            kind="control",
            sid=sid,
            seq=seq,
            control_type=message_type,
            raw_payload_hash=raw_hash,
            raw_payload=raw_payload,
        )

    if message_type == "error":
        reason = "kalshi_websocket_error"
        if isinstance(message, dict) and _int_or_none(message.get("code")) == 25:
            reason = "kalshi_websocket_buffer_overflow"
        return ParsedWsMessage(
            kind="invalid",
            sid=sid,
            seq=seq,
            reason=reason,
            raw_payload_hash=raw_hash,
            raw_payload=raw_payload,
        )

    if not isinstance(message, dict):
        return ParsedWsMessage(
            kind="invalid",
            sid=sid,
            seq=seq,
            reason="message_payload_missing",
            raw_payload_hash=raw_hash,
            raw_payload=raw_payload,
        )

    market_ticker = _str_or_none(message.get("market_ticker"))
    if market_ticker != target_market_ticker:
        return ParsedWsMessage(
            kind="ignored",
            sid=sid,
            seq=seq,
            market_ticker=market_ticker,
            reason="non_target_market",
            raw_payload_hash=raw_hash,
            raw_payload=raw_payload,
        )

    if message_type == "ticker":
        return ParsedWsMessage(
            kind="ticker",
            sid=sid,
            seq=seq,
            market_ticker=market_ticker,
            source_ts=_timestamp_or_none(message),
            raw_payload_hash=raw_hash,
            raw_payload=raw_payload,
        )

    if message_type == "orderbook_snapshot":
        try:
            yes_levels = _levels_from_payload(
                message.get("yes_dollars_fp"),
                reason_prefix="invalid_orderbook_snapshot_yes",
            )
            no_levels = _levels_from_payload(
                message.get("no_dollars_fp"),
                reason_prefix="invalid_orderbook_snapshot_no",
            )
        except WsMessageParseError as exc:
            return ParsedWsMessage(
                kind="invalid",
                sid=sid,
                seq=seq,
                market_ticker=market_ticker,
                reason=exc.reason,
                raw_payload_hash=raw_hash,
                raw_payload=raw_payload,
            )

        return ParsedWsMessage(
            kind="orderbook_snapshot",
            sid=sid,
            seq=seq,
            market_ticker=market_ticker,
            source_ts=_timestamp_or_none(message),
            yes_levels=yes_levels,
            no_levels=no_levels,
            raw_payload_hash=raw_hash,
            raw_payload=raw_payload,
        )

    if message_type == "orderbook_delta":
        side = _str_or_none(message.get("side"))
        if side not in {"yes", "no"}:
            return ParsedWsMessage(
                kind="invalid",
                sid=sid,
                seq=seq,
                market_ticker=market_ticker,
                reason="invalid_orderbook_delta_side",
                raw_payload_hash=raw_hash,
                raw_payload=raw_payload,
            )

        try:
            price = parse_decimal(message.get("price_dollars"))
        except ValueError:
            return ParsedWsMessage(
                kind="invalid",
                sid=sid,
                seq=seq,
                market_ticker=market_ticker,
                reason="invalid_orderbook_delta_price_dollars",
                raw_payload_hash=raw_hash,
                raw_payload=raw_payload,
            )

        try:
            delta_size = parse_fixed_point_contract_count(
                message.get("delta_fp"),
                allow_negative=True,
            )
        except ValueError:
            return ParsedWsMessage(
                kind="invalid",
                sid=sid,
                seq=seq,
                market_ticker=market_ticker,
                reason="invalid_orderbook_delta_delta_fp",
                raw_payload_hash=raw_hash,
                raw_payload=raw_payload,
            )

        return ParsedWsMessage(
            kind="orderbook_delta",
            sid=sid,
            seq=seq,
            market_ticker=market_ticker,
            source_ts=_timestamp_or_none(message),
            delta_side=side,
            delta_price=price,
            delta_size=delta_size,
            raw_payload_hash=raw_hash,
            raw_payload=raw_payload,
        )

    if message_type == "trade":
        trade, warning = _trade_from_payload(
            message,
            received_at=received,
            raw_payload_hash=raw_hash,
            raw_payload=raw_payload,
        )
        if trade is None:
            return ParsedWsMessage(
                kind="invalid",
                sid=sid,
                seq=seq,
                market_ticker=market_ticker,
                reason=warning or "invalid_trade_message",
                raw_payload_hash=raw_hash,
                raw_payload=raw_payload,
            )

        return ParsedWsMessage(
            kind="trade",
            sid=sid,
            seq=seq,
            market_ticker=market_ticker,
            source_ts=trade.executed_at,
            trade=trade,
            warning=warning,
            raw_payload_hash=raw_hash,
            raw_payload=raw_payload,
        )

    return ParsedWsMessage(
        kind="ignored",
        sid=sid,
        seq=seq,
        market_ticker=market_ticker,
        reason=message_type or "missing_message_type",
        raw_payload_hash=raw_hash,
        raw_payload=raw_payload,
    )


def parse_decimal(value: Any) -> Decimal:
    if value is None or value == "" or isinstance(value, bool):
        raise ValueError("missing decimal")
    try:
        parsed = Decimal(_decimal_text(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("invalid decimal") from exc
    if not parsed.is_finite():
        raise ValueError("invalid decimal")
    return parsed


def parse_fixed_point_contract_count(
    value: Any,
    *,
    allow_negative: bool = False,
    allow_zero: bool = True,
) -> Decimal:
    count = parse_decimal(value)
    if not allow_negative and count < 0:
        raise ValueError("count must not be negative")
    if not allow_zero and count == 0:
        raise ValueError("count must be positive")

    try:
        normalized = count.quantize(CONTRACT_COUNT_QUANTUM)
    except InvalidOperation as exc:
        raise ValueError("invalid count precision") from exc
    if count != normalized:
        raise ValueError("count has more than two decimal places")
    return normalized


def raw_payload_hash(payload: Any) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _levels_from_payload(value: Any, *, reason_prefix: str) -> list[PriceLevel]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise WsMessageParseError(f"{reason_prefix}_levels_not_array")

    levels: list[PriceLevel] = []
    for raw_level in value:
        if not isinstance(raw_level, list | tuple) or len(raw_level) != 2:
            raise WsMessageParseError(f"{reason_prefix}_level_shape")
        try:
            price = parse_decimal(raw_level[0])
        except ValueError as exc:
            raise WsMessageParseError(f"{reason_prefix}_level_price") from exc
        try:
            size = parse_fixed_point_contract_count(raw_level[1])
        except ValueError as exc:
            raise WsMessageParseError(f"{reason_prefix}_level_size") from exc
        levels.append(PriceLevel(price=price, size=size))

    return levels


def _trade_from_payload(
    payload: dict[str, Any],
    *,
    received_at: datetime,
    raw_payload_hash: str,
    raw_payload: JsonPayload,
) -> tuple[PublicTradeInput | None, str | None]:
    try:
        price = _trade_yes_price(payload)
    except ValueError:
        return None, "invalid_trade_price"

    try:
        trade_count = parse_fixed_point_contract_count(
            _first_present(payload, ("count", "count_fp")),
            allow_zero=False,
        )
    except ValueError:
        return None, "invalid_trade_count_fp"

    taker_side = _safe_trade_side(payload)
    warning = None if taker_side is not None else "trade_side_not_inferred"

    return (
        PublicTradeInput(
            market_ticker=str(payload["market_ticker"]),
            trade_id=_str_or_none(payload.get("trade_id")),
            received_at=received_at,
            executed_at=_timestamp_or_none(payload),
            price=price,
            count=_legacy_int_count(trade_count),
            trade_count=trade_count,
            taker_side=taker_side,
            side_inferred="provided" if taker_side else "unknown",
            raw_payload_hash=raw_payload_hash,
            raw_payload=raw_payload,
        ),
        warning,
    )


def _trade_yes_price(payload: dict[str, Any]) -> Decimal:
    if _is_present(payload.get("price_dollars")):
        return parse_decimal(payload.get("price_dollars"))
    if _is_present(payload.get("yes_price_dollars")):
        return parse_decimal(payload.get("yes_price_dollars"))
    if _is_present(payload.get("no_price_dollars")):
        return ONE_DOLLAR - parse_decimal(payload.get("no_price_dollars"))
    raise ValueError("missing trade price")


def _safe_trade_side(payload: dict[str, Any]) -> str | None:
    for key in ("taker_outcome_side", "taker_side"):
        value = _str_or_none(payload.get(key))
        if value in {"yes", "no"}:
            return value

    book_side = _str_or_none(payload.get("taker_book_side"))
    if book_side == "bid":
        return "yes"
    if book_side == "ask":
        return "no"

    return None


def _decimal_text(value: Any) -> str:
    if isinstance(value, int | float):
        return str(value)
    if isinstance(value, str):
        text = value.strip().replace(",", "")
        if text:
            return text
    raise ValueError("invalid decimal")


def _first_present(payload: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = payload.get(key)
        if _is_present(value):
            return value
    return None


def _is_present(value: Any) -> bool:
    return value is not None and value != ""


def _legacy_int_count(value: Decimal) -> int | None:
    integral_value = value.to_integral_value()
    return int(integral_value) if value == integral_value else None


def _timestamp_or_none(payload: dict[str, Any]) -> datetime | None:
    ts_ms = payload.get("ts_ms")
    if isinstance(ts_ms, int | float):
        return datetime.fromtimestamp(ts_ms / 1000, tz=UTC)

    ts = payload.get("ts")
    if isinstance(ts, int | float):
        return datetime.fromtimestamp(ts, tz=UTC)
    if isinstance(ts, str):
        return _datetime_or_none(ts)

    time_value = payload.get("time")
    if isinstance(time_value, str):
        return _datetime_or_none(time_value)

    return None


def _datetime_or_none(value: str) -> datetime | None:
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


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
