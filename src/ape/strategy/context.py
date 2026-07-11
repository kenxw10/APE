from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy.orm import Session

from ape.config import AppConfig
from ape.db.models import Market, OrderbookSnapshot, PublicTrade, ReferenceTick
from ape.kalshi.reference_messages import BRTI_SOURCE
from ape.repositories.markets import MarketsRepository
from ape.repositories.orderbook import OrderbookRepository
from ape.repositories.public_trades import PublicTradesRepository
from ape.repositories.reference_ticks import ReferenceTicksRepository


@dataclass(frozen=True)
class StrategyEvaluationContext:
    """One immutable database view shared by all strategy variants per tick."""

    evaluated_at: datetime
    market: Market | None
    boundary: Decimal | None
    boundary_source: str | None
    reference_tick: ReferenceTick | None
    orderbook: OrderbookSnapshot | None
    latest_trade: PublicTrade | None
    reference_ticks: tuple[ReferenceTick, ...]
    orderbook_history: tuple[OrderbookSnapshot, ...]
    recent_trades: tuple[PublicTrade, ...]

    @property
    def brti_value(self) -> Decimal | None:
        return (
            Decimal(self.reference_tick.parsed_value)
            if self.reference_tick and self.reference_tick.parsed_value is not None
            else None
        )

    @property
    def candidate_side(self) -> str | None:
        if self.brti_value is None or self.boundary is None:
            return None
        return "YES" if self.brti_value > self.boundary else "NO"

    @property
    def seconds_since_open(self) -> int | None:
        if self.market is None or self.market.open_time is None:
            return None
        return max(0, int((self.evaluated_at - _utc(self.market.open_time)).total_seconds()))

    @property
    def seconds_left(self) -> int | None:
        if self.market is None or self.market.close_time is None:
            return None
        return max(0, int((_utc(self.market.close_time) - self.evaluated_at).total_seconds()))


def load_strategy_evaluation_context(
    *, config: AppConfig, session: Session, evaluated_at: datetime
) -> StrategyEvaluationContext:
    market = MarketsRepository(session).get_active_market(
        now=evaluated_at,
        series_ticker=config.kalshi_btc15_series_ticker,
    )
    reference_repository = ReferenceTicksRepository(session)
    orderbook_repository = OrderbookRepository(session)
    trades_repository = PublicTradesRepository(session)
    reference_tick = reference_repository.get_latest_valid_tick(BRTI_SOURCE)
    boundary, boundary_source = _market_boundary(market)
    if market is None:
        return StrategyEvaluationContext(
            evaluated_at=evaluated_at,
            market=None,
            boundary=boundary,
            boundary_source=boundary_source,
            reference_tick=reference_tick,
            orderbook=None,
            latest_trade=None,
            reference_ticks=(),
            orderbook_history=(),
            recent_trades=(),
        )

    reference_ticks = tuple(
        reference_repository.get_ticks_since(
            BRTI_SOURCE,
            evaluated_at - timedelta(seconds=130),
            limit=512,
        )
    )
    orderbook = orderbook_repository.get_latest_snapshot(market.market_ticker)
    orderbook_history = tuple(
        orderbook_repository.get_snapshots_since(
            market.market_ticker,
            evaluated_at - timedelta(seconds=95),
            limit=512,
        )
    )
    recent_trades = tuple(
        trades_repository.get_trades_since(
            market.market_ticker,
            evaluated_at - timedelta(seconds=30),
            limit=512,
        )
    )
    return StrategyEvaluationContext(
        evaluated_at=evaluated_at,
        market=market,
        boundary=boundary,
        boundary_source=boundary_source,
        reference_tick=reference_tick,
        orderbook=orderbook,
        latest_trade=trades_repository.get_latest_trade(market.market_ticker),
        reference_ticks=reference_ticks,
        orderbook_history=orderbook_history,
        recent_trades=recent_trades,
    )


def _market_boundary(market: Market | None) -> tuple[Decimal | None, str | None]:
    if market is None:
        return None, None
    if market.functional_strike is not None:
        return Decimal(market.functional_strike), "functional_strike"
    if market.floor_strike is not None:
        return Decimal(market.floor_strike), "floor_strike"
    return None, None


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
