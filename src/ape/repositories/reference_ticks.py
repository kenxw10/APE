from __future__ import annotations

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ape.db.models import ReferenceTick
from ape.repositories.inputs import ReferenceTickInput


class ReferenceTicksRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def insert_tick(self, tick: ReferenceTickInput) -> ReferenceTick:
        row = ReferenceTick(**tick.__dict__)
        self.session.add(row)
        self.session.flush()
        return row

    def get_recent_ticks(self, source: str, limit: int = 100) -> list[ReferenceTick]:
        return list(
            self.session.scalars(
                select(ReferenceTick)
                .where(ReferenceTick.source == source)
                .order_by(desc(ReferenceTick.received_at), desc(ReferenceTick.id))
                .limit(limit)
            )
        )

    def get_latest_tick(self, source: str) -> ReferenceTick | None:
        return self.session.scalar(
            select(ReferenceTick)
            .where(ReferenceTick.source == source)
            .order_by(desc(ReferenceTick.received_at), desc(ReferenceTick.id))
            .limit(1)
        )

    def get_latest_tick_with_source_ts(self, source: str) -> ReferenceTick | None:
        return self.session.scalar(
            select(ReferenceTick)
            .where(ReferenceTick.source == source, ReferenceTick.source_ts.is_not(None))
            .order_by(desc(ReferenceTick.source_ts), desc(ReferenceTick.id))
            .limit(1)
        )
