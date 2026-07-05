from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from ape.kalshi.ws_messages import ONE_DOLLAR, ParsedWsMessage, PriceLevel
from ape.repositories.inputs import JsonPayload, OrderbookSnapshotInput


@dataclass
class OrderbookState:
    market_ticker: str
    yes_levels: dict[Decimal, int] = field(default_factory=dict)
    no_levels: dict[Decimal, int] = field(default_factory=dict)
    initialized: bool = False
    last_sequence_number: int | None = None

    def apply_snapshot(self, message: ParsedWsMessage) -> None:
        self.yes_levels = _level_map(message.yes_levels or [])
        self.no_levels = _level_map(message.no_levels or [])
        self.initialized = True
        self.last_sequence_number = message.seq

    def apply_delta(self, message: ParsedWsMessage) -> None:
        if message.delta_side not in {"yes", "no"}:
            return
        if message.delta_price is None or message.delta_size is None:
            return

        levels = self.yes_levels if message.delta_side == "yes" else self.no_levels
        next_size = levels.get(message.delta_price, 0) + message.delta_size
        if next_size <= 0:
            levels.pop(message.delta_price, None)
        else:
            levels[message.delta_price] = next_size
        if message.seq is not None:
            self.last_sequence_number = message.seq

    def has_sequence_gap(self, sequence_number: int | None) -> bool:
        if self.last_sequence_number is None or sequence_number is None:
            return False
        return sequence_number != self.last_sequence_number + 1

    def reset(self) -> None:
        self.yes_levels = {}
        self.no_levels = {}
        self.initialized = False
        self.last_sequence_number = None

    def snapshot_input(
        self,
        *,
        received_at: datetime,
        sequence_number: int | None,
        raw_payload_hash: str | None,
        raw_payload: JsonPayload | None,
    ) -> OrderbookSnapshotInput:
        yes_bid = _best_bid(self.yes_levels)
        # With use_yes_price=True, Kalshi sends NO-side book levels in YES price scale.
        yes_ask_level = _best_ask(self.no_levels)
        yes_ask = yes_ask_level.price if yes_ask_level else None
        no_bid = ONE_DOLLAR - yes_ask_level.price if yes_ask_level else None
        no_ask = ONE_DOLLAR - yes_bid.price if yes_bid else None
        yes_spread = yes_ask - yes_bid.price if yes_ask is not None and yes_bid else None
        no_spread = no_ask - no_bid if no_ask is not None and no_bid is not None else None
        warnings = _book_warnings(
            yes_bid=yes_bid.price if yes_bid else None,
            yes_ask=yes_ask,
            no_bid=no_bid,
            no_ask=no_ask,
            yes_spread=yes_spread,
            no_spread=no_spread,
        )

        return OrderbookSnapshotInput(
            market_ticker=self.market_ticker,
            received_at=received_at,
            sequence_number=sequence_number,
            yes_bid=yes_bid.price if yes_bid else None,
            yes_ask=yes_ask,
            no_bid=no_bid,
            no_ask=no_ask,
            yes_spread=yes_spread,
            no_spread=no_spread,
            yes_bid_size=yes_bid.size if yes_bid else None,
            yes_ask_size=yes_ask_level.size if yes_ask_level else None,
            no_bid_size=yes_ask_level.size if yes_ask_level else None,
            no_ask_size=yes_bid.size if yes_bid else None,
            book_status="ok" if not warnings else "|".join(warnings),
            raw_payload_hash=raw_payload_hash,
            raw_payload=raw_payload,
        )


@dataclass(frozen=True)
class BestLevel:
    price: Decimal
    size: int


def _level_map(levels: list[PriceLevel]) -> dict[Decimal, int]:
    return {level.price: level.size for level in levels if level.size > 0}


def _best_bid(levels: dict[Decimal, int]) -> BestLevel | None:
    executable = [(price, size) for price, size in levels.items() if size > 0]
    if not executable:
        return None

    price, size = max(executable, key=lambda item: item[0])
    return BestLevel(price=price, size=size)


def _best_ask(levels: dict[Decimal, int]) -> BestLevel | None:
    executable = [(price, size) for price, size in levels.items() if size > 0]
    if not executable:
        return None

    price, size = min(executable, key=lambda item: item[0])
    return BestLevel(price=price, size=size)


def _book_warnings(
    *,
    yes_bid: Decimal | None,
    yes_ask: Decimal | None,
    no_bid: Decimal | None,
    no_ask: Decimal | None,
    yes_spread: Decimal | None,
    no_spread: Decimal | None,
) -> list[str]:
    warnings: list[str] = []

    if yes_bid is None and no_bid is None:
        warnings.append("missing_or_null_book")
    elif yes_bid is None or no_bid is None or yes_ask is None or no_ask is None:
        warnings.append("one_sided_book")

    if (yes_spread is not None and yes_spread < 0) or (
        no_spread is not None and no_spread < 0
    ):
        warnings.append("crossed_book")

    return warnings
