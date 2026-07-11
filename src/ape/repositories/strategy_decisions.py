from __future__ import annotations

from datetime import datetime

from sqlalchemy import case, desc, distinct, func, select
from sqlalchemy.orm import Session

from ape.db.models import StrategyDecision
from ape.repositories.inputs import StrategyDecisionInput


class StrategyDecisionsRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def insert_decision(self, decision: StrategyDecisionInput) -> StrategyDecision:
        if not decision.strategy_id.strip():
            raise ValueError("Strategy decision strategy_id must not be empty.")
        row = StrategyDecision(**decision.__dict__)
        self.session.add(row)
        self.session.flush()
        return row

    def get_decision_by_id(self, decision_id: str) -> StrategyDecision | None:
        return self.session.scalar(
            select(StrategyDecision).where(StrategyDecision.decision_id == decision_id)
        )

    def get_latest_decision(
        self,
        *,
        strategy_id: str | None = None,
    ) -> StrategyDecision | None:
        statement = select(StrategyDecision)
        if strategy_id is not None:
            statement = statement.where(StrategyDecision.strategy_id == strategy_id)
        return self.session.scalar(
            statement
            .order_by(desc(StrategyDecision.evaluated_at), desc(StrategyDecision.id))
            .limit(1)
        )

    def list_recent_decisions(
        self,
        limit: int = 100,
        *,
        strategy_id: str | None = None,
    ) -> list[StrategyDecision]:
        statement = select(StrategyDecision)
        if strategy_id is not None:
            statement = statement.where(StrategyDecision.strategy_id == strategy_id)
        return list(
            self.session.scalars(
                statement
                .order_by(desc(StrategyDecision.evaluated_at), desc(StrategyDecision.id))
                .limit(limit)
            )
        )

    def comparison_summary_since(
        self,
        *,
        strategy_id: str,
        since: datetime,
    ) -> dict[str, object]:
        statement = select(StrategyDecision).where(
            StrategyDecision.strategy_id == strategy_id,
            StrategyDecision.evaluated_at >= since,
        )
        total_decisions = self.session.scalar(
            select(func.count()).select_from(statement.subquery())
        ) or 0
        state_counts = dict(
            self.session.execute(
                select(StrategyDecision.decision_state, func.count())
                .where(
                    StrategyDecision.strategy_id == strategy_id,
                    StrategyDecision.evaluated_at >= since,
                )
                .group_by(StrategyDecision.decision_state)
            ).all()
        )
        reason_counts = dict(
            self.session.execute(
                select(StrategyDecision.primary_reason, func.count())
                .where(
                    StrategyDecision.strategy_id == strategy_id,
                    StrategyDecision.evaluated_at >= since,
                )
                .group_by(StrategyDecision.primary_reason)
            ).all()
        )
        aggregates = self.session.execute(
            select(
                func.count(distinct(StrategyDecision.market_ticker)),
                func.sum(
                    case(
                        (StrategyDecision.decision_state == "ENTER_DRY_RUN", 1),
                        else_=0,
                    )
                ),
                func.max(StrategyDecision.evaluated_at),
            ).where(
                StrategyDecision.strategy_id == strategy_id,
                StrategyDecision.evaluated_at >= since,
            )
        ).one()
        return {
            "total_decisions": int(total_decisions),
            "state_counts": state_counts,
            "reason_counts": reason_counts,
            "unique_markets": int(aggregates[0] or 0),
            "enter_decisions": int(aggregates[1] or 0),
            "latest_decision_at": aggregates[2],
        }
