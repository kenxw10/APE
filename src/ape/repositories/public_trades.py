from __future__ import annotations

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
                select(PublicTrade)
                .where(PublicTrade.market_ticker == market_ticker)
                .order_by(
                    desc(PublicTrade.executed_at),
                    desc(PublicTrade.received_at),
                    desc(PublicTrade.id),
                )
                .limit(limit)
            )
        )

