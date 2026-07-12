from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from ape.db.models import (
    CalibrationRun,
    ResearchCandidate,
    ResearchGovernanceEvent,
    ResearchMarketOutcome,
    ResearchReplayEvent,
    ResearchReplayRun,
    ResearchReplayTrade,
    StrategyConfigVersion,
)


class ResearchRepository:
    """Idempotent persistence and bounded reads for the research-only subsystem."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def archive_event(self, values: dict[str, Any]) -> ResearchReplayEvent:
        existing = self.get_event_by_source(
            source_table=values["source_table"], source_row_id=str(values["source_row_id"])
        )
        if existing is not None:
            return existing
        row = ResearchReplayEvent(**_values(values))
        self.session.add(row)
        self.session.flush()
        return row

    def get_event_by_source(
        self, *, source_table: str, source_row_id: str
    ) -> ResearchReplayEvent | None:
        return self.session.scalar(
            select(ResearchReplayEvent).where(
                ResearchReplayEvent.source_table == source_table,
                ResearchReplayEvent.source_row_id == source_row_id,
            )
        )

    def latest_archived_source_row_id(self, source_table: str) -> int | None:
        """Return the latest numeric source primary key archived for one source table."""
        values = self.session.scalars(
            select(ResearchReplayEvent.source_row_id).where(
                ResearchReplayEvent.source_table == source_table
            )
        )
        numeric = []
        for value in values:
            try:
                numeric.append(int(value))
            except (TypeError, ValueError):
                continue
        return max(numeric, default=None)

    def latest_coverage_report(self) -> dict[str, Any] | None:
        event = self.session.scalar(
            select(ResearchReplayEvent)
            .where(ResearchReplayEvent.event_type == "COVERAGE_REPORT")
            .order_by(desc(ResearchReplayEvent.event_time), desc(ResearchReplayEvent.id))
            .limit(1)
        )
        if event is None or not isinstance(event.payload, dict):
            return None
        return deepcopy(event.payload)

    def active_challenger_count(self, architecture_version: str) -> int:
        return int(
            self.session.scalar(
                select(func.count()).select_from(ResearchCandidate).where(
                    ResearchCandidate.architecture_version == architecture_version,
                    ResearchCandidate.lifecycle_state == "DRY_RUN_CHALLENGER",
                )
            )
            or 0
        )

    def upsert_market_outcome(self, values: dict[str, Any]) -> ResearchMarketOutcome:
        row = self.session.scalar(
            select(ResearchMarketOutcome).where(
                ResearchMarketOutcome.market_ticker == values["market_ticker"]
            )
        )
        if row is None:
            row = ResearchMarketOutcome(**_values(values))
            self.session.add(row)
        else:
            for key, value in _values(values).items():
                if key not in {"id", "created_at", "outcome_id"}:
                    setattr(row, key, value)
        self.session.flush()
        return row

    def create_replay_run(self, values: dict[str, Any]) -> ResearchReplayRun:
        existing = self.get_replay_run(values["replay_run_id"])
        if existing is not None:
            return existing
        row = ResearchReplayRun(**_values(values))
        self.session.add(row)
        self.session.flush()
        return row

    def finish_replay_run(self, run: ResearchReplayRun, **values: Any) -> ResearchReplayRun:
        for key, value in _values(values).items():
            setattr(run, key, value)
        self.session.flush()
        return run

    def insert_replay_trade(self, values: dict[str, Any]) -> ResearchReplayTrade:
        existing = self.session.scalar(
            select(ResearchReplayTrade).where(ResearchReplayTrade.trade_id == values["trade_id"])
        )
        if existing is not None:
            return existing
        row = ResearchReplayTrade(**_values(values))
        self.session.add(row)
        self.session.flush()
        return row

    def create_calibration_run(self, values: dict[str, Any]) -> CalibrationRun:
        existing = self.get_calibration_run(values["calibration_run_id"])
        if existing is not None:
            return existing
        row = CalibrationRun(**_values(values))
        self.session.add(row)
        self.session.flush()
        return row

    def finish_calibration_run(self, run: CalibrationRun, **values: Any) -> CalibrationRun:
        for key, value in _values(values).items():
            setattr(run, key, value)
        self.session.flush()
        return run

    def create_candidate(self, values: dict[str, Any]) -> ResearchCandidate:
        existing = self.get_candidate(values["candidate_id"])
        if existing is not None:
            return existing
        row = ResearchCandidate(**_values(values))
        self.session.add(row)
        self.session.flush()
        return row

    def record_governance_event(self, values: dict[str, Any]) -> ResearchGovernanceEvent:
        existing = self.session.scalar(
            select(ResearchGovernanceEvent).where(
                ResearchGovernanceEvent.governance_event_id == values["governance_event_id"]
            )
        )
        if existing is not None:
            return existing
        row = ResearchGovernanceEvent(**_values(values))
        self.session.add(row)
        self.session.flush()
        return row

    def transition_candidate_state(
        self,
        *,
        candidate_id: str,
        to_state: str,
        actor: str,
        reason: str,
        evidence: dict[str, Any],
    ) -> ResearchGovernanceEvent:
        """Apply only a governed database-state transition and preserve immutable evidence."""
        candidate = self.get_candidate(candidate_id)
        if candidate is None:
            raise ValueError(f"Unknown research candidate: {candidate_id}")
        from ape.research.calibration import transition_candidate

        if (
            to_state == "DRY_RUN_CHALLENGER"
            and self.active_challenger_count(candidate.architecture_version) > 0
        ):
            raise ValueError(
                "Only one non-retired DRY_RUN_CHALLENGER is allowed per architecture."
            )

        next_state, transition = transition_candidate(
            from_state=candidate.lifecycle_state,
            to_state=to_state,
            evidence=evidence,
        )
        event_id = (
            "governance-"
            + hashlib.sha256(
                json.dumps(
                    {
                        "candidate": candidate_id,
                        "from": candidate.lifecycle_state,
                        "to": next_state,
                        "reason": reason,
                        "evidence": transition,
                    },
                    sort_keys=True,
                    default=str,
                ).encode()
            ).hexdigest()[:24]
        )
        event = self.record_governance_event(
            {
                "governance_event_id": event_id,
                "candidate_id": candidate_id,
                "from_state": candidate.lifecycle_state,
                "to_state": next_state,
                "actor": actor,
                "reason": reason,
                "evidence": transition,
            }
        )
        candidate.lifecycle_state = next_state
        candidate.governance_report = deepcopy(transition)
        version = self.session.scalar(
            select(StrategyConfigVersion).where(
                StrategyConfigVersion.strategy_config_version_id
                == candidate.strategy_config_version_id
            )
        )
        if version is not None:
            version.lifecycle_state = next_state
        self.session.flush()
        return event

    def get_replay_run(self, replay_run_id: str) -> ResearchReplayRun | None:
        return self.session.scalar(
            select(ResearchReplayRun).where(ResearchReplayRun.replay_run_id == replay_run_id)
        )

    def get_calibration_run(self, calibration_run_id: str) -> CalibrationRun | None:
        return self.session.scalar(
            select(CalibrationRun).where(CalibrationRun.calibration_run_id == calibration_run_id)
        )

    def get_candidate(self, candidate_id: str) -> ResearchCandidate | None:
        return self.session.scalar(
            select(ResearchCandidate).where(ResearchCandidate.candidate_id == candidate_id)
        )

    def get_candidate_by_config_version(self, config_version_id: str) -> ResearchCandidate | None:
        return self.session.scalar(
            select(ResearchCandidate).where(
                ResearchCandidate.strategy_config_version_id == config_version_id
            )
        )

    def list_events(
        self, *, market_ticker: str | None = None, limit: int = 500
    ) -> list[ResearchReplayEvent]:
        statement = select(ResearchReplayEvent)
        statement = statement.where(ResearchReplayEvent.event_type != "COVERAGE_REPORT")
        if market_ticker is not None:
            statement = statement.where(ResearchReplayEvent.market_ticker == market_ticker)
        return list(
            self.session.scalars(
                statement.order_by(
                    ResearchReplayEvent.event_time.asc(),
                    ResearchReplayEvent.received_at.asc(),
                    ResearchReplayEvent.source_row_id.asc(),
                    ResearchReplayEvent.event_id.asc(),
                ).limit(limit)
            )
        )

    def list_events_for_markets(self, market_tickers: list[str]) -> list[ResearchReplayEvent]:
        if not market_tickers:
            return []
        return list(
            self.session.scalars(
                select(ResearchReplayEvent)
                .where(ResearchReplayEvent.market_ticker.in_(market_tickers))
                .order_by(
                    ResearchReplayEvent.event_time.asc(),
                    ResearchReplayEvent.received_at.asc(),
                    ResearchReplayEvent.source_row_id.asc(),
                    ResearchReplayEvent.event_id.asc(),
                )
            )
        )

    def list_complete_outcomes(self) -> list[ResearchMarketOutcome]:
        return list(
            self.session.scalars(
                select(ResearchMarketOutcome)
                .where(ResearchMarketOutcome.outcome_status == "RESOLVED")
                .order_by(
                    ResearchMarketOutcome.market_open_at.asc(), ResearchMarketOutcome.id.asc()
                )
            )
        )

    def latest_event(self) -> ResearchReplayEvent | None:
        return self.session.scalar(
            select(ResearchReplayEvent)
            .order_by(desc(ResearchReplayEvent.event_time), desc(ResearchReplayEvent.id))
            .limit(1)
        )

    def latest_replay_run(self) -> ResearchReplayRun | None:
        return self.session.scalar(
            select(ResearchReplayRun)
            .order_by(desc(ResearchReplayRun.started_at), desc(ResearchReplayRun.id))
            .limit(1)
        )

    def latest_calibration_run(self) -> CalibrationRun | None:
        return self.session.scalar(
            select(CalibrationRun)
            .order_by(desc(CalibrationRun.started_at), desc(CalibrationRun.id))
            .limit(1)
        )

    def latest_zero_entry_report(self) -> dict[str, Any] | None:
        run = self.latest_replay_run()
        return deepcopy(run.zero_entry_report) if run and run.zero_entry_report else None

    def list_recent_replay_runs(self, limit: int) -> list[ResearchReplayRun]:
        return list(
            self.session.scalars(
                select(ResearchReplayRun)
                .order_by(desc(ResearchReplayRun.started_at), desc(ResearchReplayRun.id))
                .limit(limit)
            )
        )

    def list_recent_replay_trades(
        self, limit: int, candidate_id: str | None = None
    ) -> list[ResearchReplayTrade]:
        statement = select(ResearchReplayTrade)
        if candidate_id is not None:
            statement = statement.where(ResearchReplayTrade.candidate_id == candidate_id)
        return list(
            self.session.scalars(
                statement.order_by(
                    desc(ResearchReplayTrade.created_at), desc(ResearchReplayTrade.id)
                ).limit(limit)
            )
        )

    def list_recent_calibration_runs(self, limit: int) -> list[CalibrationRun]:
        return list(
            self.session.scalars(
                select(CalibrationRun)
                .order_by(desc(CalibrationRun.started_at), desc(CalibrationRun.id))
                .limit(limit)
            )
        )

    def list_recent_candidates(self, limit: int) -> list[ResearchCandidate]:
        return list(
            self.session.scalars(
                select(ResearchCandidate)
                .order_by(desc(ResearchCandidate.created_at), desc(ResearchCandidate.id))
                .limit(limit)
            )
        )

    def list_recent_governance_events(self, limit: int) -> list[ResearchGovernanceEvent]:
        return list(
            self.session.scalars(
                select(ResearchGovernanceEvent)
                .order_by(
                    desc(ResearchGovernanceEvent.created_at), desc(ResearchGovernanceEvent.id)
                )
                .limit(limit)
            )
        )

    def candidate_state_counts(self) -> dict[str, int]:
        rows = self.session.execute(
            select(ResearchCandidate.lifecycle_state, func.count()).group_by(
                ResearchCandidate.lifecycle_state
            )
        ).all()
        return {str(state): int(count) for state, count in rows}


def _values(values: dict[str, Any]) -> dict[str, Any]:
    copied = deepcopy(values)
    for key, value in list(copied.items()):
        if isinstance(value, datetime) and value.tzinfo is None:
            copied[key] = value.replace(tzinfo=UTC)
    return copied
