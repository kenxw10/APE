from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from ape.db.models import StrategyDryRunEvent, StrategyDryRunPosition
from ape.repositories.inputs import (
    StrategyDryRunEventInput,
    StrategyDryRunPositionInput,
)

OPEN_POSITION_STATUS = "OPEN"
CLOSED_POSITION_STATUSES = {"CLOSED", "FORCE_CLOSED"}


class StrategyDryRunRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get_position_by_id(self, position_id: str) -> StrategyDryRunPosition | None:
        return self.session.scalar(
            select(StrategyDryRunPosition)
            .where(StrategyDryRunPosition.position_id == position_id)
            .limit(1)
        )

    def get_open_position_by_market(
        self,
        *,
        strategy_id: str,
        market_ticker: str,
    ) -> StrategyDryRunPosition | None:
        return self.session.scalar(
            select(StrategyDryRunPosition)
            .where(
                StrategyDryRunPosition.strategy_id == strategy_id,
                StrategyDryRunPosition.market_ticker == market_ticker,
                StrategyDryRunPosition.status == OPEN_POSITION_STATUS,
            )
            .order_by(desc(StrategyDryRunPosition.opened_at), desc(StrategyDryRunPosition.id))
            .limit(1)
        )

    def get_latest_open_position(
        self,
        *,
        strategy_id: str,
    ) -> StrategyDryRunPosition | None:
        return self.session.scalar(
            select(StrategyDryRunPosition)
            .where(
                StrategyDryRunPosition.strategy_id == strategy_id,
                StrategyDryRunPosition.status == OPEN_POSITION_STATUS,
            )
            .order_by(desc(StrategyDryRunPosition.opened_at), desc(StrategyDryRunPosition.id))
            .limit(1)
        )

    def count_open_positions(self, *, strategy_id: str) -> int:
        rows = self.session.scalars(
            select(StrategyDryRunPosition.id).where(
                StrategyDryRunPosition.strategy_id == strategy_id,
                StrategyDryRunPosition.status == OPEN_POSITION_STATUS,
            )
        )
        return len(list(rows))

    def has_any_position_for_market(
        self,
        *,
        strategy_id: str,
        market_ticker: str,
    ) -> bool:
        return (
            self.session.scalar(
                select(StrategyDryRunPosition.id)
                .where(
                    StrategyDryRunPosition.strategy_id == strategy_id,
                    StrategyDryRunPosition.market_ticker == market_ticker,
                )
                .limit(1)
            )
            is not None
        )

    def insert_position_if_absent(
        self,
        position: StrategyDryRunPositionInput,
    ) -> StrategyDryRunPosition:
        existing = self.get_position_by_id(position.position_id)
        if existing is not None:
            return existing

        row = StrategyDryRunPosition(**_position_values(position))
        self.session.add(row)
        self.session.flush()
        return row

    def close_position(
        self,
        *,
        position_id: str,
        closed_at: datetime,
        close_price: Decimal | None,
        close_reason: str,
        status: str,
        realized_pnl_cents: Decimal | None,
        measurements: dict[str, Any] | None,
    ) -> StrategyDryRunPosition | None:
        if status not in CLOSED_POSITION_STATUSES:
            raise ValueError(f"Unsupported dry-run close status: {status}")

        row = self.get_position_by_id(position_id)
        if row is None:
            return None
        if row.status != OPEN_POSITION_STATUS:
            return row

        row.status = status
        row.closed_at = closed_at
        row.close_price = close_price
        row.close_reason = close_reason
        row.realized_pnl_cents = realized_pnl_cents
        row.measurements = deepcopy(measurements)
        flag_modified(row, "measurements")
        self.session.flush()
        return row

    def insert_event_if_absent(self, event: StrategyDryRunEventInput) -> StrategyDryRunEvent:
        existing = self.get_event_by_id(event.event_id)
        if existing is not None:
            return existing

        row = StrategyDryRunEvent(**_event_values(event))
        self.session.add(row)
        self.session.flush()
        return row

    def get_event_by_id(self, event_id: str) -> StrategyDryRunEvent | None:
        return self.session.scalar(
            select(StrategyDryRunEvent)
            .where(StrategyDryRunEvent.event_id == event_id)
            .limit(1)
        )

    def get_latest_event(
        self,
        *,
        strategy_id: str | None = None,
    ) -> StrategyDryRunEvent | None:
        statement = select(StrategyDryRunEvent)
        if strategy_id is not None:
            statement = statement.join(
                StrategyDryRunPosition,
                StrategyDryRunEvent.position_id == StrategyDryRunPosition.position_id,
            ).where(StrategyDryRunPosition.strategy_id == strategy_id)
        return self.session.scalar(
            statement.order_by(
                desc(StrategyDryRunEvent.occurred_at),
                desc(StrategyDryRunEvent.id),
            ).limit(1)
        )

    def get_latest_enter_decision_id(self, *, strategy_id: str | None = None) -> str | None:
        statement = select(StrategyDryRunEvent).where(
            StrategyDryRunEvent.event_type == "ENTER_DRY_RUN"
        )
        if strategy_id is not None:
            statement = statement.join(
                StrategyDryRunPosition,
                StrategyDryRunEvent.position_id == StrategyDryRunPosition.position_id,
            ).where(StrategyDryRunPosition.strategy_id == strategy_id)
        row = self.session.scalar(
            statement.order_by(
                desc(StrategyDryRunEvent.occurred_at),
                desc(StrategyDryRunEvent.id),
            ).limit(1)
        )
        return row.decision_id if row is not None else None

    def list_open_positions(
        self,
        *,
        strategy_id: str | None = None,
    ) -> list[StrategyDryRunPosition]:
        statement = select(StrategyDryRunPosition).where(
            StrategyDryRunPosition.status == OPEN_POSITION_STATUS
        )
        if strategy_id is not None:
            statement = statement.where(StrategyDryRunPosition.strategy_id == strategy_id)
        return list(
            self.session.scalars(
                statement.order_by(
                    desc(StrategyDryRunPosition.opened_at),
                    desc(StrategyDryRunPosition.id),
                )
            )
        )

    def list_recent_positions(
        self,
        limit: int = 100,
        *,
        strategy_id: str | None = None,
    ) -> list[StrategyDryRunPosition]:
        statement = select(StrategyDryRunPosition)
        if strategy_id is not None:
            statement = statement.where(StrategyDryRunPosition.strategy_id == strategy_id)
        return list(
            self.session.scalars(
                statement.order_by(
                    desc(StrategyDryRunPosition.opened_at),
                    desc(StrategyDryRunPosition.id),
                ).limit(limit)
            )
        )

    def list_recent_events(
        self,
        limit: int = 100,
        *,
        strategy_id: str | None = None,
    ) -> list[StrategyDryRunEvent]:
        statement = select(StrategyDryRunEvent)
        if strategy_id is not None:
            statement = statement.join(
                StrategyDryRunPosition,
                StrategyDryRunEvent.position_id == StrategyDryRunPosition.position_id,
            ).where(StrategyDryRunPosition.strategy_id == strategy_id)
        return list(
            self.session.scalars(
                statement.order_by(
                    desc(StrategyDryRunEvent.occurred_at),
                    desc(StrategyDryRunEvent.id),
                ).limit(limit)
            )
        )


def _position_values(position: StrategyDryRunPositionInput) -> dict[str, Any]:
    values = position.__dict__.copy()
    values["measurements"] = deepcopy(values.get("measurements"))
    return values


def _event_values(event: StrategyDryRunEventInput) -> dict[str, Any]:
    values = event.__dict__.copy()
    values["measurements"] = deepcopy(values.get("measurements"))
    return values
