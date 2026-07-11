from __future__ import annotations

from datetime import datetime

from sqlalchemy import desc, insert, select
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

    def insert_snapshots(self, snapshots: list[OrderbookSnapshotInput]) -> None:
        if not snapshots:
            return
        self.session.execute(
            insert(OrderbookSnapshot),
            [snapshot.__dict__ for snapshot in snapshots],
        )

    def get_latest_snapshot(self, market_ticker: str) -> OrderbookSnapshot | None:
        return self.session.scalar(
            select(OrderbookSnapshot)
            .where(OrderbookSnapshot.market_ticker == market_ticker)
            .order_by(desc(OrderbookSnapshot.received_at), desc(OrderbookSnapshot.id))
            .limit(1)
        )

    def get_latest_snapshot_any(self) -> OrderbookSnapshot | None:
        return self.session.scalar(
            select(OrderbookSnapshot)
            .order_by(desc(OrderbookSnapshot.received_at), desc(OrderbookSnapshot.id))
            .limit(1)
        )

    def get_first_snapshot_between(
        self,
        market_ticker: str,
        *,
        start: datetime,
        end: datetime,
    ) -> OrderbookSnapshot | None:
        return self.session.scalar(
            select(OrderbookSnapshot)
            .where(
                OrderbookSnapshot.market_ticker == market_ticker,
                OrderbookSnapshot.received_at >= start,
                OrderbookSnapshot.received_at <= end,
            )
            .order_by(OrderbookSnapshot.received_at.asc(), OrderbookSnapshot.id.asc())
            .limit(1)
        )

    def get_snapshots_since(
        self,
        market_ticker: str,
        since: datetime,
        *,
        limit: int,
    ) -> list[OrderbookSnapshot]:
        rows = list(
            self.session.scalars(
                select(OrderbookSnapshot)
                .where(
                    OrderbookSnapshot.market_ticker == market_ticker,
                    OrderbookSnapshot.received_at >= since,
                )
                .order_by(desc(OrderbookSnapshot.received_at), desc(OrderbookSnapshot.id))
                .limit(limit)
            )
        )
        return list(reversed(rows))
