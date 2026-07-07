from __future__ import annotations

from datetime import datetime

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ape.db.models import PublicTrade
from ape.repositories.inputs import PublicTradeInput


class PublicTradesRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def insert_trade(self, trade: PublicTradeInput) -> PublicTrade:
        row = PublicTrade(**trade.__dict__)
        self.session.add(row)
        self.session.flush()
        return row

    def get_recent_trades(self, market_ticker: str, limit: int = 100) -> list[PublicTrade]:
        return list(
            self.session.scalars(
                _recent_trades_statement(market_ticker=market_ticker, limit=limit)
            )
        )

    def get_latest_trade(self, market_ticker: str | None = None) -> PublicTrade | None:
        statement = select(PublicTrade)
        if market_ticker is not None:
            statement = statement.where(PublicTrade.market_ticker == market_ticker)

        return self.session.scalar(
            statement.order_by(
                desc(PublicTrade.executed_at).nulls_last(),
                desc(PublicTrade.received_at),
                desc(PublicTrade.id),
            ).limit(1)
        )

    def get_trades_since(
        self,
        market_ticker: str,
        since: datetime,
        *,
        limit: int,
    ) -> list[PublicTrade]:
        rows = list(
            self.session.scalars(
                select(PublicTrade)
                .where(
                    PublicTrade.market_ticker == market_ticker,
                    PublicTrade.received_at >= since,
                )
                .order_by(
                    desc(PublicTrade.executed_at).nulls_last(),
                    desc(PublicTrade.received_at),
                    desc(PublicTrade.id),
                )
                .limit(limit)
            )
        )
        return list(reversed(rows))


def _recent_trades_statement(market_ticker: str, limit: int):
    return (
        select(PublicTrade)
        .where(PublicTrade.market_ticker == market_ticker)
        .order_by(
            desc(PublicTrade.executed_at).nulls_last(),
            desc(PublicTrade.received_at),
            desc(PublicTrade.id),
        )
        .limit(limit)
    )
