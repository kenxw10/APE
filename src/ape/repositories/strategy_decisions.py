from __future__ import annotations

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ape.db.models import StrategyDecision
from ape.repositories.inputs import StrategyDecisionInput


class StrategyDecisionsRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def insert_decision(self, decision: StrategyDecisionInput) -> StrategyDecision:
        row = StrategyDecision(**decision.__dict__)
        self.session.add(row)
        self.session.flush()
        return row

    def get_decision_by_id(self, decision_id: str) -> StrategyDecision | None:
        return self.session.scalar(
            select(StrategyDecision).where(StrategyDecision.decision_id == decision_id)
        )

    def get_latest_decision(self) -> StrategyDecision | None:
        return self.session.scalar(
            select(StrategyDecision)
            .order_by(desc(StrategyDecision.evaluated_at), desc(StrategyDecision.id))
            .limit(1)
        )

    def list_recent_decisions(self, limit: int = 100) -> list[StrategyDecision]:
        return list(
            self.session.scalars(
                select(StrategyDecision)
                .order_by(desc(StrategyDecision.evaluated_at), desc(StrategyDecision.id))
                .limit(limit)
            )
        )
