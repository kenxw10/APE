from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from ape.kalshi.ws_messages import parse_fixed_point_contract_count, parse_ws_payload
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
                "yes_dollars_fp": [["0.6300", "12.00"], ["0.6100", "9.00"]],
                "no_dollars_fp": [["0.7000", "5.00"], ["0.7200", "3.00"]],
                "ts_ms": 1780000000000,
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    assert message.kind == "orderbook_snapshot"
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
    assert snapshot.yes_ask_count == Decimal("5.00")
    assert snapshot.no_bid_size == 5
    assert snapshot.no_bid_count == Decimal("5.00")
    assert snapshot.book_status == "ok"


def test_orderbook_snapshot_allows_missing_yes_side() -> None:
    message = parse_ws_payload(
        {
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 10,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "no_dollars_fp": [["0.7000", "5.00"]],
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    assert message.kind == "orderbook_snapshot"
    assert message.yes_levels == []
    assert message.no_levels is not None
    assert message.no_levels[0].price == Decimal("0.7000")
    assert message.no_levels[0].size == Decimal("5.00")


def test_orderbook_snapshot_allows_missing_no_side() -> None:
    message = parse_ws_payload(
        {
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 10,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "yes_dollars_fp": [[0.63, 12.0]],
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    assert message.kind == "orderbook_snapshot"
    assert message.yes_levels is not None
    assert message.yes_levels[0].price == Decimal("0.63")
    assert message.yes_levels[0].size == Decimal("12.00")
    assert message.no_levels == []


def test_orderbook_snapshot_allows_empty_sides() -> None:
    message = parse_ws_payload(
        {
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 10,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "yes_dollars_fp": [],
                "no_dollars_fp": [],
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    assert message.kind == "orderbook_snapshot"
    assert message.yes_levels == []
    assert message.no_levels == []


def test_orderbook_snapshot_accepts_comma_decimal_contract_counts() -> None:
    message = parse_ws_payload(
        {
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 10,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "yes_dollars_fp": [["0.6300", "1,200.00"]],
                "no_dollars_fp": [["0.7000", "2,500.0000"]],
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    assert message.kind == "orderbook_snapshot"
    assert message.yes_levels is not None
    assert message.no_levels is not None
    assert message.yes_levels[0].size == Decimal("1200.00")
    assert message.no_levels[0].size == Decimal("2500.00")


def test_fixed_point_contract_count_preserves_fractional_precision() -> None:
    assert parse_fixed_point_contract_count(" 1,234.50 ") == Decimal("1234.50")
    assert parse_fixed_point_contract_count("-4.25", allow_negative=True) == Decimal(
        "-4.25"
    )
    assert parse_fixed_point_contract_count("0.00", allow_negative=True) == Decimal("0.00")


def test_orderbook_delta_updates_book_after_snapshot() -> None:
    state = OrderbookState("KXBTC15M-TEST")
    snapshot_message = parse_ws_payload(
        {
            "type": "orderbook_snapshot",
            "seq": 1,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "yes_dollars_fp": [["0.60", "10.00"]],
                "no_dollars_fp": [["0.65", "8.00"]],
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
                "delta_fp": "-4.00",
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    assert delta_message.kind == "orderbook_delta"
    state.apply_delta(delta_message)
    snapshot = state.snapshot_input(
        received_at=NOW,
        sequence_number=delta_message.seq,
        raw_payload_hash=delta_message.raw_payload_hash,
        raw_payload=delta_message.raw_payload,
    )

    assert snapshot.yes_bid == Decimal("0.60")
    assert snapshot.yes_bid_size == 6
    assert snapshot.yes_bid_count == Decimal("6.00")
    assert snapshot.yes_ask == Decimal("0.65")
    assert snapshot.no_bid == Decimal("0.35")


def test_orderbook_delta_accepts_positive_fixed_point_size() -> None:
    message = parse_ws_payload(
        {
            "type": "orderbook_delta",
            "seq": 2,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "side": "yes",
                "price_dollars": "0.9600",
                "delta_fp": "54.25",
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    assert message.kind == "orderbook_delta"
    assert message.delta_price == Decimal("0.9600")
    assert message.delta_size == Decimal("54.25")


def test_orderbook_delta_accepts_negative_fixed_point_size() -> None:
    message = parse_ws_payload(
        {
            "type": "orderbook_delta",
            "seq": 2,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "side": "yes",
                "price_dollars": 0.96,
                "delta_fp": -54.0,
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    assert message.kind == "orderbook_delta"
    assert message.delta_price == Decimal("0.96")
    assert message.delta_size == Decimal("-54.00")


def test_orderbook_delta_accepts_comma_decimal_and_zero_sizes() -> None:
    negative_message = parse_ws_payload(
        {
            "type": "orderbook_delta",
            "seq": 2,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "side": "yes",
                "price_dollars": "0.9600",
                "delta_fp": "-1,200.00",
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )
    zero_message = parse_ws_payload(
        {
            "type": "orderbook_delta",
            "seq": 3,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "side": "yes",
                "price_dollars": "0.9600",
                "delta_fp": "0.00",
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    assert negative_message.kind == "orderbook_delta"
    assert negative_message.delta_size == Decimal("-1200.00")
    assert zero_message.kind == "orderbook_delta"
    assert zero_message.delta_size == Decimal("0.00")


def test_no_side_delta_uses_yes_price_scale() -> None:
    state = OrderbookState("KXBTC15M-TEST")
    snapshot_message = parse_ws_payload(
        {
            "type": "orderbook_snapshot",
            "seq": 1,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "yes_dollars_fp": [["0.60", "10.00"]],
                "no_dollars_fp": [["0.70", "8.00"]],
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
                "delta_fp": "4.00",
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
    assert snapshot.yes_ask_count == Decimal("4.00")
    assert snapshot.no_bid_size == 4
    assert snapshot.no_bid_count == Decimal("4.00")


def test_malformed_snapshot_level_reports_precise_warning() -> None:
    message = parse_ws_payload(
        {
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 10,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "yes_dollars_fp": [["0.6300", "12.501"]],
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    assert message.kind == "invalid"
    assert message.reason == "invalid_orderbook_snapshot_yes_level_size"


def test_malformed_snapshot_no_level_reports_precise_price_warning() -> None:
    message = parse_ws_payload(
        {
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 10,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "no_dollars_fp": [[{"price": "0.7000"}, "12.00"]],
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    assert message.kind == "invalid"
    assert message.reason == "invalid_orderbook_snapshot_no_level_price"


def test_malformed_delta_reports_precise_warning() -> None:
    message = parse_ws_payload(
        {
            "type": "orderbook_delta",
            "seq": 2,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "side": "yes",
                "price_dollars": "0.9600",
                "delta_fp": "1.255",
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    assert message.kind == "invalid"
    assert message.reason == "invalid_orderbook_delta_delta_fp"


def test_orderbook_sequence_gap_detection_resets_state() -> None:
    state = OrderbookState("KXBTC15M-TEST")
    snapshot_message = parse_ws_payload(
        {
            "type": "orderbook_snapshot",
            "seq": 1,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "yes_dollars_fp": [["0.60", "10.00"]],
                "no_dollars_fp": [["0.65", "8.00"]],
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


def test_ticker_parsing_accepts_live_shape() -> None:
    message = parse_ws_payload(
        {
            "type": "ticker",
            "sid": 2,
            "seq": 3,
            "msg": {
                "market_ticker": "KXBTC15M-TEST",
                "yes_bid_dollars": "0.6100",
                "yes_ask_dollars": "0.6400",
                "yes_bid_size_fp": "25.00",
                "yes_ask_size_fp": "30.00",
                "ts_ms": 1780000000123,
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    assert message.kind == "ticker"
    assert message.source_ts is not None


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
                "count_fp": "2.00",
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
    assert message.trade.trade_count == Decimal("2.00")
    assert message.trade.taker_side is None
    assert message.trade.side_inferred == "unknown"
    assert message.warning == "trade_side_not_inferred"


def test_public_trade_parsing_accepts_comma_decimal_count() -> None:
    message = parse_ws_payload(
        {
            "type": "trade",
            "sid": 3,
            "seq": 4,
            "msg": {
                "trade_id": "trade-1",
                "market_ticker": "KXBTC15M-TEST",
                "yes_price_dollars": "0.64",
                "count_fp": "1,200.00",
                "taker_side": "yes",
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    assert message.kind == "trade"
    assert message.trade is not None
    assert message.trade.count == 1200
    assert message.trade.trade_count == Decimal("1200.00")


def test_public_trade_parsing_accepts_fractional_fixed_point_count() -> None:
    message = parse_ws_payload(
        {
            "type": "trade",
            "sid": 3,
            "seq": 4,
            "msg": {
                "trade_id": "trade-1",
                "market_ticker": "KXBTC15M-TEST",
                "yes_price_dollars": "0.64",
                "count_fp": "2.50",
                "taker_side": "yes",
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    assert message.kind == "trade"
    assert message.trade is not None
    assert message.trade.count is None
    assert message.trade.trade_count == Decimal("2.50")


def test_public_trade_parsing_reports_precise_price_warning() -> None:
    message = parse_ws_payload(
        {
            "type": "trade",
            "sid": 3,
            "seq": 4,
            "msg": {
                "trade_id": "trade-1",
                "market_ticker": "KXBTC15M-TEST",
                "yes_price_dollars": {"value": "0.64"},
                "count_fp": "2.00",
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    assert message.kind == "invalid"
    assert message.reason == "invalid_trade_price"


def test_public_trade_parsing_reports_precise_count_warning() -> None:
    message = parse_ws_payload(
        {
            "type": "trade",
            "sid": 3,
            "seq": 4,
            "msg": {
                "trade_id": "trade-1",
                "market_ticker": "KXBTC15M-TEST",
                "yes_price_dollars": "0.64",
                "count_fp": "2.345",
            },
        },
        target_market_ticker="KXBTC15M-TEST",
        received_at=NOW,
    )

    assert message.kind == "invalid"
    assert message.reason == "invalid_trade_count_fp"
