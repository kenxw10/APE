from __future__ import annotations

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ape.db.models import OrderbookSnapshot
from ape.repositories.inputs import OrderbookSnapshotInput


class OrderbookRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def insert_snapshot(self, snapshot: OrderbookSnapshotInput) -> OrderbookSnapshot:
        row = OrderbookSnapshot(**snapshot.__dict__)
        self.session.add(row)
        self.session.flush()
        return row

    def get_latest_snapshot(self, market_ticker: str) -> OrderbookSnapshot | None:
        return self.session.scalar(
            select(OrderbookSnapshot)
            .where(OrderbookSnapshot.market_ticker == market_ticker)
            .order_by(desc(OrderbookSnapshot.received_at), desc(OrderbookSnapshot.id))
            .limit(1)
        )

