from __future__ import annotations

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ape.db.models import Market
from ape.repositories.inputs import MarketInput


class MarketsRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def upsert_market(self, market: MarketInput) -> Market:
        existing = self.get_market_by_ticker(market.market_ticker)
        if existing is None:
            row = Market(**market.__dict__)
            self.session.add(row)
            self.session.flush()
            return row

        for key, value in market.__dict__.items():
            setattr(existing, key, value)
        self.session.flush()
        return existing

    def get_market_by_ticker(self, market_ticker: str) -> Market | None:
        return self.session.scalar(select(Market).where(Market.market_ticker == market_ticker))

    def list_recent_markets(self, limit: int = 50) -> list[Market]:
        return list(
            self.session.scalars(select(Market).order_by(desc(Market.created_at)).limit(limit))
        )

