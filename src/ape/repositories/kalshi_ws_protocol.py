from __future__ import annotations

from datetime import datetime

from sqlalchemy import desc, func, insert, or_, select
from sqlalchemy.orm import Session

from ape.db.models import KalshiWsProtocolEvent
from ape.kalshi.protocol_events import PROTOCOL_ERROR_EVENTS, PROTOCOL_NORMAL_CLOSE_CODES
from ape.repositories.inputs import KalshiWsProtocolEventInput


class KalshiWsProtocolEventRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def insert_event(
        self,
        event: KalshiWsProtocolEventInput,
    ) -> KalshiWsProtocolEvent:
        row = KalshiWsProtocolEvent(**event.__dict__)
        self.session.add(row)
        self.session.flush()
        return row

    def insert_events(self, events: list[KalshiWsProtocolEventInput]) -> None:
        if not events:
            return
        self.session.execute(
            insert(KalshiWsProtocolEvent),
            [event.__dict__ for event in events],
        )

    def list_recent(self, *, limit: int = 200) -> list[KalshiWsProtocolEvent]:
        capped_limit = min(max(limit, 1), 500)
        return list(
            self.session.scalars(
                select(KalshiWsProtocolEvent)
                .order_by(
                    desc(KalshiWsProtocolEvent.created_at),
                    desc(KalshiWsProtocolEvent.id),
                )
                .limit(capped_limit)
            )
        )

    def count_recent_errors(self, *, since: datetime) -> int:
        value = self.session.scalar(
            select(func.count())
            .select_from(KalshiWsProtocolEvent)
            .where(
                KalshiWsProtocolEvent.created_at >= since,
                _protocol_error_expression(),
            )
        )
        return int(value or 0)

    def summary_since(self, *, since: datetime) -> dict[str, object]:
        rows = list(
            self.session.execute(
                select(
                    KalshiWsProtocolEvent.event_type,
                    func.count().label("count"),
                )
                .where(KalshiWsProtocolEvent.created_at >= since)
                .group_by(KalshiWsProtocolEvent.event_type)
            )
        )
        by_event_type = {str(event_type): int(count) for event_type, count in rows}
        latest_at = self.session.scalar(
            select(func.max(KalshiWsProtocolEvent.created_at)).where(
                KalshiWsProtocolEvent.created_at >= since
            )
        )
        total = sum(by_event_type.values())
        abnormal_close_count = self.session.scalar(
            select(func.count())
            .select_from(KalshiWsProtocolEvent)
            .where(
                KalshiWsProtocolEvent.created_at >= since,
                _abnormal_close_expression(),
            )
        )
        error_count = sum(
            by_event_type.get(event_type, 0) for event_type in PROTOCOL_ERROR_EVENTS
        ) + int(abnormal_close_count or 0)
        close_count = by_event_type.get("websocket_close", 0)
        reconnect_count = sum(
            by_event_type.get(event_type, 0)
            for event_type in (
                "reconnect_scheduled",
                "reconnect_started",
                "reconnect_completed",
                "reconnect_failed",
            )
        )
        return {
            "total": total,
            "error_count": error_count,
            "close_count": close_count,
            "reconnect_count": reconnect_count,
            "by_event_type": by_event_type,
            "latest_event_at": latest_at,
        }


def _protocol_error_expression():
    return or_(
        KalshiWsProtocolEvent.event_type.in_(tuple(PROTOCOL_ERROR_EVENTS)),
        _abnormal_close_expression(),
    )


def _abnormal_close_expression():
    return (
        (KalshiWsProtocolEvent.event_type == "websocket_close")
        & KalshiWsProtocolEvent.close_code.is_not(None)
        & KalshiWsProtocolEvent.close_code.not_in(tuple(PROTOCOL_NORMAL_CLOSE_CODES))
    )
