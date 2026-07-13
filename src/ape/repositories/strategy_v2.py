from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from decimal import Decimal

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from ape.db.models import (
    StrategyConfigVersion,
    StrategyDecision,
    StrategyDryRunEvent,
    StrategyDryRunPosition,
    StrategyFeatureSnapshot,
    StrategyPositionMark,
    StrategyPositionOutcome,
    StrategyTradeIntent,
)
from ape.repositories.inputs import (
    StrategyConfigVersionInput,
    StrategyFeatureSnapshotInput,
    StrategyPositionMarkInput,
    StrategyPositionOutcomeInput,
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

    def get_config_version(self, strategy_config_version_id: str) -> StrategyConfigVersion | None:
        return self.session.scalar(
            select(StrategyConfigVersion)
            .where(StrategyConfigVersion.strategy_config_version_id == strategy_config_version_id)
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
        self, *, strategy_id: str, market_ticker: str, action: str | None = None
    ) -> StrategyTradeIntent | None:
        statement = select(StrategyTradeIntent).where(
            StrategyTradeIntent.strategy_id == strategy_id,
            StrategyTradeIntent.market_ticker == market_ticker,
            StrategyTradeIntent.status == "PENDING",
        )
        if action is not None:
            statement = statement.where(StrategyTradeIntent.action == action)
        return self.session.scalar(
            statement.order_by(
                desc(StrategyTradeIntent.created_at), desc(StrategyTradeIntent.id)
            ).limit(1)
        )

    def get_pending_exit_intent(self, *, position_id: str) -> StrategyTradeIntent | None:
        return self.session.scalar(
            select(StrategyTradeIntent)
            .where(
                StrategyTradeIntent.position_id == position_id,
                StrategyTradeIntent.action == "EXIT",
                StrategyTradeIntent.status == "PENDING",
            )
            .order_by(desc(StrategyTradeIntent.created_at), desc(StrategyTradeIntent.id))
            .limit(1)
        )

    def has_pending_intents(self, *, strategy_id: str) -> bool:
        return (
            self.session.scalar(
                select(StrategyTradeIntent.id)
                .where(
                    StrategyTradeIntent.strategy_id == strategy_id,
                    StrategyTradeIntent.status == "PENDING",
                )
                .limit(1)
            )
            is not None
        )

    def list_pending_intents(self) -> list[StrategyTradeIntent]:
        return list(
            self.session.scalars(
                select(StrategyTradeIntent)
                .where(StrategyTradeIntent.status == "PENDING")
                .order_by(StrategyTradeIntent.created_at.asc(), StrategyTradeIntent.id.asc())
            )
        )

    def expire_pending_entry_intents(
        self,
        *,
        strategy_id: str,
        resolved_at: datetime,
        reason: str,
    ) -> list[StrategyTradeIntent]:
        pending = list(
            self.session.scalars(
                select(StrategyTradeIntent)
                .where(
                    StrategyTradeIntent.strategy_id == strategy_id,
                    StrategyTradeIntent.action == "ENTRY",
                    StrategyTradeIntent.status == "PENDING",
                )
                .order_by(StrategyTradeIntent.created_at.asc(), StrategyTradeIntent.id.asc())
            )
        )
        for intent in pending:
            self.resolve_intent(
                intent,
                status="EXPIRED",
                resolved_at=resolved_at,
                reason=reason,
            )
        return pending

    def count_exit_attempts(self, *, position_id: str) -> int:
        return int(
            self.session.scalar(
                select(func.count()).where(
                    StrategyTradeIntent.position_id == position_id,
                    StrategyTradeIntent.action == "EXIT",
                )
            )
            or 0
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
        self, *, strategy_id: str | None, limit: int, action: str | None = None
    ) -> list[StrategyTradeIntent]:
        statement = select(StrategyTradeIntent)
        if strategy_id is not None:
            statement = statement.where(StrategyTradeIntent.strategy_id == strategy_id)
        if action is not None:
            statement = statement.where(StrategyTradeIntent.action == action)
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
        position_id: str | None = None,
        fill_snapshot_id: int | None = None,
        fill_price: Decimal | None = None,
        fill_size: Decimal | None = None,
        fill_timestamp: datetime | None = None,
    ) -> StrategyTradeIntent:
        if intent.status != "PENDING":
            return intent
        intent.status = status
        intent.resolved_at = resolved_at
        intent.resolution_reason = reason
        intent.position_id = position_id
        intent.fill_snapshot_id = fill_snapshot_id
        intent.simulated_fill_price = fill_price
        intent.simulated_fill_size = fill_size
        intent.fill_timestamp = fill_timestamp if fill_price is not None else None
        self.session.flush()
        return intent

    def insert_outcome_if_absent(
        self, value: StrategyPositionOutcomeInput
    ) -> StrategyPositionOutcome:
        existing = self.session.scalar(
            select(StrategyPositionOutcome)
            .where(StrategyPositionOutcome.position_id == value.position_id)
            .limit(1)
        )
        if existing is not None:
            return existing
        row = StrategyPositionOutcome(**_values(value))
        self.session.add(row)
        self.session.flush()
        return row

    def list_recent_outcomes(
        self, *, strategy_id: str | None, limit: int
    ) -> list[StrategyPositionOutcome]:
        statement = select(StrategyPositionOutcome)
        if strategy_id is not None:
            statement = statement.where(StrategyPositionOutcome.strategy_id == strategy_id)
        return list(
            self.session.scalars(
                statement.order_by(
                    desc(StrategyPositionOutcome.closed_at), desc(StrategyPositionOutcome.id)
                ).limit(limit)
            )
        )

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

    def list_marks_for_position(
        self, *, position_id: str, limit: int = 512
    ) -> list[StrategyPositionMark]:
        return list(
            self.session.scalars(
                select(StrategyPositionMark)
                .where(StrategyPositionMark.position_id == position_id)
                .order_by(StrategyPositionMark.marked_at.asc(), StrategyPositionMark.id.asc())
                .limit(limit)
            )
        )

    def comparison_summary_since(self, *, strategy_id: str, since: datetime) -> dict[str, object]:
        intent_rows = self.session.execute(
            select(StrategyTradeIntent.action, StrategyTradeIntent.status, func.count())
            .where(
                StrategyTradeIntent.strategy_id == strategy_id,
                StrategyTradeIntent.created_at >= since,
            )
            .group_by(StrategyTradeIntent.action, StrategyTradeIntent.status)
        ).all()
        counts = {f"{action}_{status}": int(count) for action, status, count in intent_rows}
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
        outcome_stats = self.session.execute(
            select(
                func.avg(StrategyPositionOutcome.holding_duration_ms),
                func.avg(StrategyPositionOutcome.realized_pnl_cents),
                func.avg(StrategyPositionOutcome.mfe_cents),
                func.avg(StrategyPositionOutcome.mae_cents),
            ).where(
                StrategyPositionOutcome.strategy_id == strategy_id,
                StrategyPositionOutcome.closed_at >= since,
            )
        ).one()
        entry_signals = (
            self.session.scalar(
                select(func.count()).where(
                    StrategyDecision.strategy_id == strategy_id,
                    StrategyDecision.evaluated_at >= since,
                    StrategyDecision.decision_state == "DRY_RUN_ENTRY_SIGNAL",
                )
            )
            or 0
        )
        open_positions = (
            self.session.scalar(
                select(func.count()).where(
                    StrategyDryRunPosition.strategy_id == strategy_id,
                    StrategyDryRunPosition.status == "OPEN",
                )
            )
            or 0
        )
        closed_positions = (
            self.session.scalar(
                select(func.count()).where(
                    StrategyDryRunPosition.strategy_id == strategy_id,
                    StrategyDryRunPosition.closed_at >= since,
                )
            )
            or 0
        )
        attempt_limit = (
            self.session.scalar(
                select(func.count()).where(
                    StrategyDryRunEvent.strategy_id == strategy_id,
                    StrategyDryRunEvent.occurred_at >= since,
                    StrategyDryRunEvent.event_type == "V2_EXIT_ATTEMPT_LIMIT",
                )
            )
            or 0
        )
        return {
            "intent_counts": counts,
            "entry_signals": int(entry_signals),
            "entry_intents": counts.get("ENTRY_PENDING", 0)
            + counts.get("ENTRY_FILLED", 0)
            + counts.get("ENTRY_NO_FILL", 0)
            + counts.get("ENTRY_EXPIRED", 0),
            "entry_fills": counts.get("ENTRY_FILLED", 0),
            "entry_no_fills": counts.get("ENTRY_NO_FILL", 0),
            "entry_expiries": counts.get("ENTRY_EXPIRED", 0),
            "exit_signals": counts.get("EXIT_PENDING", 0)
            + counts.get("EXIT_FILLED", 0)
            + counts.get("EXIT_NO_FILL", 0)
            + counts.get("EXIT_EXPIRED", 0),
            "exit_intents": counts.get("EXIT_PENDING", 0)
            + counts.get("EXIT_FILLED", 0)
            + counts.get("EXIT_NO_FILL", 0)
            + counts.get("EXIT_EXPIRED", 0),
            "exit_fills": counts.get("EXIT_FILLED", 0),
            "exit_no_fills": counts.get("EXIT_NO_FILL", 0),
            "exit_expiries": counts.get("EXIT_EXPIRED", 0),
            "open_positions": int(open_positions),
            "closed_positions": int(closed_positions),
            "unresolved_open_positions": int(open_positions),
            "exit_attempt_limit_count": int(attempt_limit),
            "average_holding_duration_ms": outcome_stats[0],
            "average_realized_pnl_cents": outcome_stats[1],
            "average_mfe_cents": outcome_stats[2],
            "average_mae_cents": outcome_stats[3],
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
        "complete_feature_vector",
        "replay_blockers",
        "model_artifact",
        "measurements",
        "boundary_state",
    ):
        if key in result:
            result[key] = deepcopy(result[key])
    return result
