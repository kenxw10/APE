from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from ape.kalshi.ws_messages import parse_ws_payload
from ape.kalshi.ws_state import OrderbookState

NOW = datetime(2026, 7, 5, 14, 35, tzinfo=UTC)


def test_orderbook_snapshot_normalizes_yes_price_top_of_book() -> None:
    message = parse_ws_payload(
        {
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 10,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "yes_dollars_fp": [["0.63", "12"], ["0.61", "9"]],
                "no_dollars_fp": [["0.70", "5"], ["0.72", "3"]],
                "ts_ms": 1780000000000,
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )
    state = OrderbookState("KXBTC15M-TEST")
    state.apply_snapshot(message)

    snapshot = state.snapshot_input(
        received_at=NOW,
        sequence_number=message.seq,
        raw_payload_hash=message.raw_payload_hash,
        raw_payload=message.raw_payload,
    )

    assert snapshot.yes_bid == Decimal("0.63")
    assert snapshot.yes_ask == Decimal("0.70")
    assert snapshot.no_bid == Decimal("0.30")
    assert snapshot.no_ask == Decimal("0.37")
    assert snapshot.yes_spread == Decimal("0.07")
    assert snapshot.no_spread == Decimal("0.07")
    assert snapshot.yes_ask_size == 5
    assert snapshot.no_bid_size == 5
    assert snapshot.book_status == "ok"


def test_orderbook_delta_updates_book_after_snapshot() -> None:
    state = OrderbookState("KXBTC15M-TEST")
    snapshot_message = parse_ws_payload(
        {
            "type": "orderbook_snapshot",
            "seq": 1,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "yes_dollars_fp": [["0.60", "10"]],
                "no_dollars_fp": [["0.65", "8"]],
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )
    state.apply_snapshot(snapshot_message)
    delta_message = parse_ws_payload(
        {
            "type": "orderbook_delta",
            "seq": 2,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "side": "yes",
                "price_dollars": "0.60",
                "delta_fp": "-4",
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    state.apply_delta(delta_message)
    snapshot = state.snapshot_input(
        received_at=NOW,
        sequence_number=delta_message.seq,
        raw_payload_hash=delta_message.raw_payload_hash,
        raw_payload=delta_message.raw_payload,
    )

    assert snapshot.yes_bid == Decimal("0.60")
    assert snapshot.yes_bid_size == 6
    assert snapshot.yes_ask == Decimal("0.65")
    assert snapshot.no_bid == Decimal("0.35")


def test_no_side_delta_uses_yes_price_scale() -> None:
    state = OrderbookState("KXBTC15M-TEST")
    snapshot_message = parse_ws_payload(
        {
            "type": "orderbook_snapshot",
            "seq": 1,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "yes_dollars_fp": [["0.60", "10"]],
                "no_dollars_fp": [["0.70", "8"]],
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )
    state.apply_snapshot(snapshot_message)
    delta_message = parse_ws_payload(
        {
            "type": "orderbook_delta",
            "seq": 2,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "side": "no",
                "price_dollars": "0.64",
                "delta_fp": "4",
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    state.apply_delta(delta_message)
    snapshot = state.snapshot_input(
        received_at=NOW,
        sequence_number=delta_message.seq,
        raw_payload_hash=delta_message.raw_payload_hash,
        raw_payload=delta_message.raw_payload,
    )

    assert snapshot.yes_ask == Decimal("0.64")
    assert snapshot.no_bid == Decimal("0.36")
    assert snapshot.yes_ask_size == 4
    assert snapshot.no_bid_size == 4


def test_orderbook_sequence_gap_detection_resets_state() -> None:
    state = OrderbookState("KXBTC15M-TEST")
    snapshot_message = parse_ws_payload(
        {
            "type": "orderbook_snapshot",
            "seq": 1,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "yes_dollars_fp": [["0.60", "10"]],
                "no_dollars_fp": [["0.65", "8"]],
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )
    state.apply_snapshot(snapshot_message)

    assert state.has_sequence_gap(3)

    state.reset()

    assert not state.initialized
    assert state.last_sequence_number is None
    assert state.yes_levels == {}
    assert state.no_levels == {}


def test_websocket_buffer_overflow_error_is_distinguishable() -> None:
    message = parse_ws_payload(
        {
            "type": "error",
            "sid": 1,
            "seq": 2,
            "msg": {"code": 25, "msg": "Subscription buffer overflow"},
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    assert message.kind == "invalid"
    assert message.reason == "kalshi_websocket_buffer_overflow"


def test_crossed_and_missing_book_warnings() -> None:
    state = OrderbookState("KXBTC15M-TEST")

    snapshot = state.snapshot_input(
        received_at=NOW,
        sequence_number=None,
        raw_payload_hash=None,
        raw_payload=None,
    )

    assert snapshot.book_status == "missing_or_null_book"


def test_public_trade_parsing_allows_unknown_side_without_faking_it() -> None:
    message = parse_ws_payload(
        {
            "type": "trade",
            "sid": 3,
            "seq": 4,
            "msg": {
                "trade_id": "trade-1",
                "market_ticker": "KXBTC15M-TEST",
                "yes_price_dollars": "0.64",
                "count_fp": "2",
                "ts_ms": 1780000000123,
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    assert message.kind == "trade"
    assert message.trade is not None
    assert message.trade.price == Decimal("0.64")
    assert message.trade.count == 2
    assert message.trade.taker_side is None
    assert message.trade.side_inferred == "unknown"
    assert message.warning == "trade_side_not_inferred"
