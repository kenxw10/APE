from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from decimal import Decimal

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from ape.db.models import (
    StrategyConfigVersion,
    StrategyFeatureSnapshot,
    StrategyPositionMark,
    StrategyTradeIntent,
)
from ape.repositories.inputs import (
    StrategyConfigVersionInput,
    StrategyFeatureSnapshotInput,
    StrategyPositionMarkInput,
    StrategyTradeIntentInput,
)


class StrategyV2Repository:
    """Idempotent persistence for immutable v2 research records."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def ensure_config_version(self, value: StrategyConfigVersionInput) -> StrategyConfigVersion:
        existing = self.session.scalar(
            select(StrategyConfigVersion).where(
                StrategyConfigVersion.strategy_config_version_id == value.strategy_config_version_id
            )
        )
        if existing is not None:
            return existing
        row = StrategyConfigVersion(**_values(value))
        self.session.add(row)
        self.session.flush()
        return row

    def ensure_feature_snapshot(
        self, value: StrategyFeatureSnapshotInput
    ) -> StrategyFeatureSnapshot:
        existing = self.get_feature_snapshot(value.feature_snapshot_id)
        if existing is not None:
            return existing
        row = StrategyFeatureSnapshot(**_values(value))
        self.session.add(row)
        self.session.flush()
        return row

    def get_feature_snapshot(self, feature_snapshot_id: str) -> StrategyFeatureSnapshot | None:
        return self.session.scalar(
            select(StrategyFeatureSnapshot)
            .where(StrategyFeatureSnapshot.feature_snapshot_id == feature_snapshot_id)
            .limit(1)
        )

    def get_latest_feature_snapshot(self) -> StrategyFeatureSnapshot | None:
        return self.session.scalar(
            select(StrategyFeatureSnapshot)
            .order_by(desc(StrategyFeatureSnapshot.evaluated_at), desc(StrategyFeatureSnapshot.id))
            .limit(1)
        )

    def list_recent_feature_snapshots(self, *, limit: int) -> list[StrategyFeatureSnapshot]:
        return list(
            self.session.scalars(
                select(StrategyFeatureSnapshot)
                .order_by(
                    desc(StrategyFeatureSnapshot.evaluated_at), desc(StrategyFeatureSnapshot.id)
                )
                .limit(limit)
            )
        )

    def insert_intent_if_absent(self, value: StrategyTradeIntentInput) -> StrategyTradeIntent:
        existing = self.get_intent(value.intent_id)
        if existing is not None:
            return existing
        row = StrategyTradeIntent(**_values(value))
        self.session.add(row)
        self.session.flush()
        return row

    def get_intent(self, intent_id: str) -> StrategyTradeIntent | None:
        return self.session.scalar(
            select(StrategyTradeIntent).where(StrategyTradeIntent.intent_id == intent_id).limit(1)
        )

    def get_pending_intent(
        self, *, strategy_id: str, market_ticker: str
    ) -> StrategyTradeIntent | None:
        return self.session.scalar(
            select(StrategyTradeIntent)
            .where(
                StrategyTradeIntent.strategy_id == strategy_id,
                StrategyTradeIntent.market_ticker == market_ticker,
                StrategyTradeIntent.status == "PENDING",
            )
            .order_by(desc(StrategyTradeIntent.created_at), desc(StrategyTradeIntent.id))
            .limit(1)
        )

    def list_expired_pending_intents(
        self,
        *,
        strategy_id: str,
        before: datetime,
        limit: int,
    ) -> list[StrategyTradeIntent]:
        return list(
            self.session.scalars(
                select(StrategyTradeIntent)
                .where(
                    StrategyTradeIntent.strategy_id == strategy_id,
                    StrategyTradeIntent.status == "PENDING",
                    StrategyTradeIntent.expires_at < before,
                )
                .order_by(StrategyTradeIntent.expires_at.asc(), StrategyTradeIntent.id.asc())
                .limit(limit)
            )
        )

    def has_entry_intent_for_market(self, *, strategy_id: str, market_ticker: str) -> bool:
        return (
            self.session.scalar(
                select(StrategyTradeIntent.id)
                .where(
                    StrategyTradeIntent.strategy_id == strategy_id,
                    StrategyTradeIntent.market_ticker == market_ticker,
                    StrategyTradeIntent.action == "ENTRY",
                )
                .limit(1)
            )
            is not None
        )

    def list_recent_intents(
        self, *, strategy_id: str | None, limit: int
    ) -> list[StrategyTradeIntent]:
        statement = select(StrategyTradeIntent)
        if strategy_id is not None:
            statement = statement.where(StrategyTradeIntent.strategy_id == strategy_id)
        return list(
            self.session.scalars(
                statement.order_by(
                    desc(StrategyTradeIntent.created_at), desc(StrategyTradeIntent.id)
                ).limit(limit)
            )
        )

    def resolve_intent(
        self,
        intent: StrategyTradeIntent,
        *,
        status: str,
        resolved_at: datetime,
        reason: str,
        fill_snapshot_id: int | None = None,
        fill_price: Decimal | None = None,
        fill_size: Decimal | None = None,
    ) -> StrategyTradeIntent:
        if intent.status != "PENDING":
            return intent
        intent.status = status
        intent.resolved_at = resolved_at
        intent.resolution_reason = reason
        intent.fill_snapshot_id = fill_snapshot_id
        intent.simulated_fill_price = fill_price
        intent.simulated_fill_size = fill_size
        self.session.flush()
        return intent

    def insert_mark_if_absent(self, value: StrategyPositionMarkInput) -> StrategyPositionMark:
        existing = self.session.scalar(
            select(StrategyPositionMark)
            .where(StrategyPositionMark.mark_id == value.mark_id)
            .limit(1)
        )
        if existing is not None:
            return existing
        row = StrategyPositionMark(**_values(value))
        self.session.add(row)
        self.session.flush()
        return row

    def list_recent_marks(
        self, *, strategy_id: str | None, limit: int
    ) -> list[StrategyPositionMark]:
        statement = select(StrategyPositionMark)
        if strategy_id is not None:
            statement = statement.where(StrategyPositionMark.strategy_id == strategy_id)
        return list(
            self.session.scalars(
                statement.order_by(
                    desc(StrategyPositionMark.marked_at), desc(StrategyPositionMark.id)
                ).limit(limit)
            )
        )

    def comparison_summary_since(self, *, strategy_id: str, since: datetime) -> dict[str, object]:
        intent_counts = dict(
            self.session.execute(
                select(StrategyTradeIntent.status, func.count())
                .where(
                    StrategyTradeIntent.strategy_id == strategy_id,
                    StrategyTradeIntent.created_at >= since,
                )
                .group_by(StrategyTradeIntent.status)
            ).all()
        )
        signal_stats = self.session.execute(
            select(
                func.avg(StrategyTradeIntent.measurements["score"].as_float()),
                func.avg(StrategyTradeIntent.measurements["edge_lower_bound_cents"].as_float()),
            ).where(
                StrategyTradeIntent.strategy_id == strategy_id,
                StrategyTradeIntent.action == "ENTRY",
                StrategyTradeIntent.created_at >= since,
            )
        ).one()
        return {
            "intent_counts": intent_counts,
            "entry_intents": int(sum(intent_counts.values())),
            "fills": int(intent_counts.get("FILLED", 0)),
            "no_fills": int(intent_counts.get("NO_FILL", 0)),
            "expiries": int(intent_counts.get("EXPIRED", 0)),
            "average_score_for_signals": signal_stats[0],
            "average_edge_lower_bound_for_signals": signal_stats[1],
        }


def _values(value: object) -> dict[str, object]:
    result = dict(value.__dict__)
    for key in (
        "parameter_snapshot",
        "quality_state",
        "reference_features",
        "contract_features",
        "microstructure_features",
        "execution_features",
        "measurements",
        "boundary_state",
    ):
        if key in result:
            result[key] = deepcopy(result[key])
    return result
