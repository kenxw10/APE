from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import desc, func, select, text
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
from ape.research.fees import verified_kalshi_taker_fee_model


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

    def _lock_challenger_architecture(self, architecture_version: str) -> None:
        """Serialize challenger admission without introducing a schema migration.

        PostgreSQL advisory transaction locks exist even before any challenger row
        does, so concurrent transitions cannot both observe an empty active set.
        SQLite ignores this production-only lock in local tests.
        """
        if self.session.get_bind().dialect.name != "postgresql":
            return
        self.session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:architecture_version))"),
            {"architecture_version": architecture_version},
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

        if to_state == "DRY_RUN_CHALLENGER":
            self._lock_challenger_architecture(candidate.architecture_version)
            if self.active_challenger_count(candidate.architecture_version) > 0:
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

    def advance_candidate_governance(
        self, *, candidate_id: str, actor: str
    ) -> list[ResearchGovernanceEvent]:
        """Advance a candidate only from immutable calibration and replay evidence."""
        candidate = self.get_candidate(candidate_id)
        if candidate is None:
            raise ValueError(f"Unknown research candidate: {candidate_id}")
        evidence, blockers = self._candidate_governance_evidence(candidate)
        if blockers:
            candidate.governance_report = {"evidence": evidence, "blockers": blockers}
            self.session.flush()
            return []

        from ape.research.calibration import (
            LIFECYCLE_BACKTESTED,
            LIFECYCLE_DRAFT,
            LIFECYCLE_DRY_RUN_CHALLENGER,
            LIFECYCLE_SHADOW,
            _promotion_failures,
        )

        transitions: list[ResearchGovernanceEvent] = []
        for from_state, to_state in (
            (LIFECYCLE_DRAFT, LIFECYCLE_BACKTESTED),
            (LIFECYCLE_BACKTESTED, LIFECYCLE_SHADOW),
            (LIFECYCLE_SHADOW, LIFECYCLE_DRY_RUN_CHALLENGER),
        ):
            candidate = self.get_candidate(candidate_id)
            if candidate is None or candidate.lifecycle_state != from_state:
                continue
            if to_state == LIFECYCLE_DRY_RUN_CHALLENGER:
                promotion_blockers = _promotion_failures(evidence)
                if promotion_blockers:
                    candidate.governance_report = {
                        "evidence": evidence,
                        "blockers": promotion_blockers,
                    }
                    self.session.flush()
                    break
            transitions.append(
                self.transition_candidate_state(
                    candidate_id=candidate_id,
                    to_state=to_state,
                    actor=actor,
                    reason="automatic_persisted_research_evidence",
                    evidence=evidence,
                )
            )
        return transitions

    def _candidate_governance_evidence(
        self, candidate: ResearchCandidate
    ) -> tuple[dict[str, Any], list[str]]:
        calibration = self.get_calibration_run(candidate.calibration_run_id or "")
        metrics = (
            candidate.validation_metrics if isinstance(candidate.validation_metrics, dict) else {}
        )
        heldout = candidate.holdout_metrics if isinstance(candidate.holdout_metrics, dict) else {}
        replay_run = self.get_replay_run(calibration.replay_run_id) if calibration else None
        trades = self.list_recent_replay_trades(
            limit=500_000,
            candidate_id=candidate.candidate_id,
            replay_run_id=calibration.replay_run_id if calibration else None,
        )
        manifest = calibration.partition_manifest if calibration else {}
        manifest = manifest if isinstance(manifest, dict) else {}
        manifest_markets = [
            str(market) for market in manifest.get("ordered_market_tickers", [])
        ]
        coverage_evidence = _eligible_feature_coverage(
            self.list_events_for_markets(manifest_markets),
            self.list_complete_outcomes(),
        )
        partition_evidence = _governance_trade_evidence(
            trades,
            {**metrics, "holdout": heldout},
            manifest,
        )
        market_count = coverage_evidence["complete_eligible_unique_markets"]
        reported_closed = partition_evidence["reported_closed_governance_trades"]
        replay_integrity = partition_evidence["reported_partition_trade_integrity"]
        frequency = _decimal_or_zero(metrics.get("entry_frequency_per_100_markets"))
        zero_entry = metrics.get("zero_entry_report") if isinstance(metrics, dict) else {}
        unique_rates = (
            zero_entry.get("unique_market_rates_per_100", {})
            if isinstance(zero_entry, dict)
            else {}
        )
        qualified_setups = _decimal_or_zero(
            unique_rates.get("qualified_setups") if isinstance(unique_rates, dict) else None
        )
        baseline_metrics = (
            (calibration.validation_metrics or {}).get("candidate-baseline-v2", {})
            if calibration and isinstance(calibration.validation_metrics, dict)
            else {}
        )
        baseline_version = self.session.scalar(
            select(StrategyConfigVersion).where(
                StrategyConfigVersion.strategy_config_version_id
                == candidate.parent_strategy_config_version_id
            )
        )
        config_diff = _config_diff_evidence(
            baseline_version.parameter_snapshot if baseline_version is not None else {},
            candidate.parameter_snapshot,
        )
        expected_fee = verified_kalshi_taker_fee_model().metadata()
        actual_fee = (
            replay_run.cost_model
            if replay_run is not None and isinstance(replay_run.cost_model, dict)
            else {}
        )
        fee_keys = (
            "effective_date",
            "kxbtc15m_taker_multiplier",
            "taker_formula",
            "rounding_rule_source_text",
            "settlement_fee",
            "parameter_snapshot_sha256",
        )
        fee_mismatches = {
            key: {"expected": expected_fee.get(key), "actual": actual_fee.get(key)}
            for key in fee_keys
            if expected_fee.get(key) != actual_fee.get(key)
        }
        evidence = {
            "source": "persisted_candidate_calibration_and_replay",
            "candidate_id": candidate.candidate_id,
            "calibration_run_id": candidate.calibration_run_id,
            "replay_run_id": calibration.replay_run_id if calibration else None,
            "manifest_unique_markets": len(set(manifest_markets)),
            "complete_unique_markets": market_count,
            "complete_eligible_unique_markets": market_count,
            "closed_simulated_trades": partition_evidence[
                "unique_closed_governance_trades"
            ],
            "reported_closed_trade_count": reported_closed,
            "candidate_replay_integrity": replay_integrity,
            "entry_frequency_per_100_markets": str(frequency),
            "entry_frequency_per_100_markets_min": str(frequency),
            "preferred_fill_range_3_to_10_per_100": Decimal("3") <= frequency <= Decimal("10"),
            "qualified_setup_target_5_to_15_per_100": (
                Decimal("5") <= qualified_setups <= Decimal("15")
            ),
            "signal_to_fill_rate": metrics.get("signal_to_fill_rate", "0"),
            **coverage_evidence,
            **partition_evidence,
            "complete_replay_coverage": coverage_evidence["frame_coverage_ratio"],
            "volatility_regimes": metrics.get("volatility_regime_coverage", 0),
            "liquidity_regimes": metrics.get("liquidity_regime_coverage", 0),
            "timing_tiers": metrics.get("timing_tier_coverage", 0),
            "holdout_mean_net_pnl_per_market": heldout.get(
                "net_pnl_per_market", metrics.get("net_pnl_per_market", "0")
            ),
            "holdout_lower_95": (
                (heldout.get("bootstrap") or {}).get("net_pnl_per_market", {}).get("lower", "0")
                if isinstance(heldout, dict)
                else "0"
            ),
            "adjusted_lower_confidence_expectancy": (
                (metrics.get("penalties") or {}).get("adjusted_lower_confidence_expectancy", "0")
                if isinstance(metrics, dict)
                else "0"
            ),
            "dominant_regime_entry_share": metrics.get("dominant_regime_entry_share", "1"),
            "max_drawdown_per_100_markets": str(
                _decimal_or_zero(metrics.get("maximum_drawdown_cents"))
                * Decimal("100")
                / Decimal(max(market_count, 1))
            ),
            "fee_metadata_expected": {key: expected_fee.get(key) for key in fee_keys},
            "fee_metadata_mismatches": fee_mismatches,
            "verified_fee_model": not fee_mismatches,
            "candidate_adjusted_lower_confidence_expectancy": (
                (metrics.get("penalties") or {}).get("adjusted_lower_confidence_expectancy", "0")
            ),
            "baseline_adjusted_lower_confidence_expectancy": (
                (baseline_metrics.get("penalties") or {}).get(
                    "adjusted_lower_confidence_expectancy", "0"
                )
            ),
            "beats_baseline": _decimal_or_zero(
                (metrics.get("penalties") or {}).get("adjusted_lower_confidence_expectancy")
            )
            > _decimal_or_zero(
                (baseline_metrics.get("penalties") or {}).get(
                    "adjusted_lower_confidence_expectancy"
                )
            ),
            **config_diff,
        }
        blockers = []
        if metrics.get("status") != "EVALUATED":
            blockers.append("candidate_metrics_not_evaluated")
        if not replay_integrity:
            blockers.append("candidate_partition_trade_count_mismatch")
        if reported_closed and not partition_evidence["unique_closed_governance_trades"]:
            blockers.append("candidate_metrics_with_fills_missing_replay_evidence")
        if config_diff["forbidden_parameter_changed"]:
            blockers.append("candidate_forbidden_parameter_change")
        if config_diff["safety_or_data_quality_gate_changed"]:
            blockers.append("candidate_protected_gate_change")
        if _decimal_or_zero(coverage_evidence["frame_coverage_ratio"]) < Decimal("0.95"):
            blockers.append("candidate_replay_coverage_below_threshold")
        blockers.extend(coverage_evidence["gap_related_blockers"])
        if fee_mismatches:
            blockers.append("candidate_fee_metadata_mismatch")
        return evidence, list(dict.fromkeys(blockers))

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
            .where(ResearchReplayEvent.event_type != "COVERAGE_REPORT")
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

    def list_recent_replay_runs(
        self, limit: int, *, replay_run_id: str | None = None, status: str | None = None
    ) -> list[ResearchReplayRun]:
        statement = select(ResearchReplayRun)
        if replay_run_id is not None:
            statement = statement.where(ResearchReplayRun.replay_run_id == replay_run_id)
        if status is not None:
            statement = statement.where(ResearchReplayRun.status == status)
        return list(
            self.session.scalars(
                statement.order_by(
                    desc(ResearchReplayRun.started_at), desc(ResearchReplayRun.id)
                ).limit(limit)
            )
        )

    def list_recent_replay_trades(
        self,
        limit: int,
        candidate_id: str | None = None,
        *,
        replay_run_id: str | None = None,
        market_ticker: str | None = None,
        status: str | None = None,
    ) -> list[ResearchReplayTrade]:
        statement = select(ResearchReplayTrade)
        if candidate_id is not None:
            statement = statement.where(ResearchReplayTrade.candidate_id == candidate_id)
        if replay_run_id is not None:
            statement = statement.where(ResearchReplayTrade.replay_run_id == replay_run_id)
        if market_ticker is not None:
            statement = statement.where(ResearchReplayTrade.market_ticker == market_ticker)
        if status is not None:
            statement = statement.where(ResearchReplayTrade.status == status)
        return list(
            self.session.scalars(
                statement.order_by(
                    desc(ResearchReplayTrade.created_at), desc(ResearchReplayTrade.id)
                ).limit(limit)
            )
        )

    def list_recent_calibration_runs(
        self,
        limit: int,
        *,
        calibration_run_id: str | None = None,
        replay_run_id: str | None = None,
        status: str | None = None,
    ) -> list[CalibrationRun]:
        statement = select(CalibrationRun)
        if calibration_run_id is not None:
            statement = statement.where(CalibrationRun.calibration_run_id == calibration_run_id)
        if replay_run_id is not None:
            statement = statement.where(CalibrationRun.replay_run_id == replay_run_id)
        if status is not None:
            statement = statement.where(CalibrationRun.status == status)
        return list(
            self.session.scalars(
                statement.order_by(desc(CalibrationRun.started_at), desc(CalibrationRun.id)).limit(
                    limit
                )
            )
        )

    def list_recent_candidates(
        self,
        limit: int,
        *,
        candidate_id: str | None = None,
        calibration_run_id: str | None = None,
        lifecycle_state: str | None = None,
    ) -> list[ResearchCandidate]:
        statement = select(ResearchCandidate)
        if candidate_id is not None:
            statement = statement.where(ResearchCandidate.candidate_id == candidate_id)
        if calibration_run_id is not None:
            statement = statement.where(ResearchCandidate.calibration_run_id == calibration_run_id)
        if lifecycle_state is not None:
            statement = statement.where(ResearchCandidate.lifecycle_state == lifecycle_state)
        return list(
            self.session.scalars(
                statement.order_by(
                    desc(ResearchCandidate.created_at), desc(ResearchCandidate.id)
                ).limit(limit)
            )
        )

    def list_recent_governance_events(
        self,
        limit: int,
        *,
        candidate_id: str | None = None,
        lifecycle_state: str | None = None,
    ) -> list[ResearchGovernanceEvent]:
        statement = select(ResearchGovernanceEvent)
        if candidate_id is not None:
            statement = statement.where(ResearchGovernanceEvent.candidate_id == candidate_id)
        if lifecycle_state is not None:
            statement = statement.where(ResearchGovernanceEvent.to_state == lifecycle_state)
        return list(
            self.session.scalars(
                statement.order_by(
                    desc(ResearchGovernanceEvent.created_at),
                    desc(ResearchGovernanceEvent.id),
                ).limit(limit)
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


def _decimal_or_zero(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (ArithmeticError, ValueError):
        return Decimal("0")


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


_REQUIRED_ELIGIBILITY_EVENT_TYPES = frozenset(
    {"MARKET", "REFERENCE", "ORDERBOOK", "FEATURE_SNAPSHOT", "MARKET_LIFECYCLE"}
)
_GOVERNANCE_PARTITIONS = ("development_test", "frozen_holdout")


def _eligible_feature_coverage(
    events: list[ResearchReplayEvent], outcomes: list[ResearchMarketOutcome]
) -> dict[str, Any]:
    """Derive promotion coverage from archived source events, never run totals."""
    outcomes_by_market = {
        outcome.market_ticker: outcome
        for outcome in outcomes
        if outcome.outcome_status == "RESOLVED"
    }
    events_by_market: dict[str, list[ResearchReplayEvent]] = {}
    for event in events:
        if event.market_ticker is not None:
            events_by_market.setdefault(event.market_ticker, []).append(event)

    total_feature_snapshots = 0
    mature_candidate_frames = 0
    full_frames = 0
    partial_frames = 0
    unusable_frames = 0
    complete_markets: set[str] = set()
    eligible_markets: set[str] = set()
    missing_source_counts: dict[str, int] = {}
    gaps_by_market_and_source: dict[str, dict[str, int]] = {}
    gap_blockers: list[str] = []

    for market_ticker, market_events in sorted(events_by_market.items()):
        ordered = sorted(market_events, key=lambda row: (_utc(row.event_time), row.event_id))
        types = {row.event_type for row in ordered}
        missing = _REQUIRED_ELIGIBILITY_EVENT_TYPES - types
        missing_source_counts[market_ticker] = len(missing)
        if missing:
            gap_blockers.append(f"missing_required_event_sources:{market_ticker}")

        source_gaps: dict[str, int] = {}
        by_source: dict[str, list[ResearchReplayEvent]] = {}
        for event in ordered:
            by_source.setdefault(event.source_table, []).append(event)
        for source, source_events in sorted(by_source.items()):
            source_events.sort(key=lambda row: (_utc(row.event_time), row.event_id))
            maximum_gap = max(
                (
                    max(
                        0,
                        int(
                            (_utc(current.event_time) - _utc(previous.event_time)).total_seconds()
                        ),
                    )
                    for previous, current in zip(source_events, source_events[1:], strict=False)
                ),
                default=0,
            )
            source_gaps[source] = maximum_gap
        gaps_by_market_and_source[market_ticker] = source_gaps

        market_frames = [row for row in ordered if row.event_type == "FEATURE_SNAPSHOT"]
        total_feature_snapshots += len(market_frames)
        full_for_market = 0
        candidate_for_market = 0
        for feature in market_frames:
            vector = _feature_vector(feature)
            side = vector.get("candidate_side") if isinstance(vector, dict) else None
            if side not in {"YES", "NO"}:
                unusable_frames += 1
                continue
            candidate_for_market += 1
            labels = _outcome_labels(outcomes_by_market.get(market_ticker))
            label = labels.get(feature.feature_snapshot_id or "")
            mature = _label_is_mature(label)
            if mature:
                mature_candidate_frames += 1
            first_book = _first_book_in_window(ordered, feature.event_time)
            full = (
                feature.replay_readiness == "FULL"
                and mature
                and first_book is not None
                and market_ticker in outcomes_by_market
            )
            if full:
                full_frames += 1
                full_for_market += 1
            elif feature.replay_readiness == "UNUSABLE" or not mature:
                unusable_frames += 1
            else:
                partial_frames += 1
        if candidate_for_market:
            eligible_markets.add(market_ticker)
            # A complete market has no candidate-side frame that failed the
            # full executable-label contract.
            if full_for_market == candidate_for_market and candidate_for_market > 0:
                complete_markets.add(market_ticker)

    overall_gap = max(
        (gap for sources in gaps_by_market_and_source.values() for gap in sources.values()),
        default=0,
    )
    if overall_gap > 30:
        gap_blockers.append("maximum_event_gap_exceeds_30_seconds")
    eligible_frame_count = full_frames + partial_frames + unusable_frames
    frame_coverage = Decimal(full_frames) / Decimal(max(eligible_frame_count, 1))
    market_coverage = Decimal(len(complete_markets)) / Decimal(max(len(eligible_markets), 1))
    return {
        "total_feature_snapshot_events": total_feature_snapshots,
        "mature_candidate_side_feature_frames": mature_candidate_frames,
        "eligible_feature_frames": eligible_frame_count,
        "full_feature_frames": full_frames,
        "partial_feature_frames": partial_frames,
        "unusable_feature_frames": unusable_frames,
        "complete_eligible_markets": len(complete_markets),
        "total_eligible_markets": len(eligible_markets),
        "complete_eligible_unique_markets": len(complete_markets),
        "frame_coverage_ratio": str(frame_coverage),
        "market_coverage_ratio": str(market_coverage),
        "frame_coverage": str(frame_coverage),
        "market_coverage": str(market_coverage),
        "missing_source_counts": missing_source_counts,
        "maximum_event_gap_by_market_and_source": gaps_by_market_and_source,
        "overall_maximum_event_gap": overall_gap,
        "gap_related_blockers": sorted(set(gap_blockers)),
        "coverage_blockers": sorted(set(gap_blockers)),
    }


def _feature_vector(event: ResearchReplayEvent) -> dict[str, Any]:
    payload = event.payload if isinstance(event.payload, dict) else {}
    vector = payload.get("feature_vector") if isinstance(payload, dict) else {}
    return vector if isinstance(vector, dict) else {}


def _outcome_labels(outcome: ResearchMarketOutcome | None) -> dict[str, Any]:
    flags = (
        outcome.quality_flags
        if outcome is not None and isinstance(outcome.quality_flags, dict)
        else {}
    )
    labels = flags.get("counterfactual_labels") if isinstance(flags, dict) else {}
    return labels if isinstance(labels, dict) else {}


def _label_is_mature(label: Any) -> bool:
    return isinstance(label, dict) and label.get("net_markout_30s_cents") is not None


def _first_book_in_window(
    events: list[ResearchReplayEvent], feature_at: datetime
) -> ResearchReplayEvent | None:
    effective_after = _utc(feature_at) + timedelta(milliseconds=500)
    expires_at = effective_after + timedelta(seconds=2)
    return next(
        (
            event
            for event in events
            if event.event_type == "ORDERBOOK"
            and effective_after <= _utc(event.event_time) <= expires_at
        ),
        None,
    )


def _governance_trade_evidence(
    trades: list[ResearchReplayTrade], metrics: dict[str, Any], manifest: dict[str, Any]
) -> dict[str, Any]:
    declared = manifest.get("governance_trade_partitions", _GOVERNANCE_PARTITIONS)
    partitions = tuple(dict.fromkeys(str(value) for value in declared if isinstance(value, str)))
    partitions = partitions or _GOVERNANCE_PARTITIONS
    metrics_by_partition = _partition_metrics(metrics)
    unique: dict[tuple[str, str, str, str, str], ResearchReplayTrade] = {}
    excluded_duplicates = 0
    for trade in trades:
        if trade.status != "CLOSED":
            continue
        measurements = trade.measurements if isinstance(trade.measurements, dict) else {}
        partition = measurements.get("evidence_partition")
        if partition not in partitions:
            continue
        source_decision = str(
            measurements.get("source_decision_id")
            or measurements.get("entry_decision_id")
            or trade.entry_feature_snapshot_id
            or trade.trade_id
        )
        entry_event = str(trade.entry_fill_event_id or trade.trade_id)
        key = (
            str(trade.candidate_id or ""),
            trade.market_ticker,
            source_decision,
            entry_event,
            str(partition),
        )
        if key in unique:
            excluded_duplicates += 1
            continue
        unique[key] = trade
    closed_by_partition = {
        partition: sum(1 for key in unique if key[-1] == partition) for partition in partitions
    }
    reported_by_partition = {
        partition: _int_or_none(metrics_by_partition.get(partition, {}).get("closed_trade_count"))
        for partition in partitions
    }
    reported_values = list(reported_by_partition.values())
    reported_closed = (
        sum(value for value in reported_values if value is not None)
        if all(value is not None for value in reported_values)
        else None
    )
    return {
        "governance_trade_partitions": list(partitions),
        "unique_closed_governance_trades": len(unique),
        "unique_governance_markets": len({trade.market_ticker for trade in unique.values()}),
        "excluded_duplicate_rows": excluded_duplicates,
        "partition_specific_closed_counts": closed_by_partition,
        "partition_specific_reported_closed_counts": reported_by_partition,
        "reported_closed_governance_trades": reported_closed,
        "reported_partition_trade_integrity": (
            reported_closed is not None
            and all(
                reported_by_partition[partition] == closed_by_partition[partition]
                for partition in partitions
            )
        ),
    }


def _partition_metrics(metrics: dict[str, Any]) -> dict[str, dict[str, Any]]:
    explicit = metrics.get("partition_metrics")
    result = explicit if isinstance(explicit, dict) else {}
    values = {
        str(partition): value for partition, value in result.items() if isinstance(value, dict)
    }
    if "development_test" not in values and isinstance(metrics.get("development_test"), dict):
        values["development_test"] = metrics["development_test"]
    if "frozen_holdout" not in values and isinstance(metrics.get("holdout"), dict):
        values["frozen_holdout"] = metrics["holdout"]
    return values


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


_CANDIDATE_TUNABLE_PATHS = {
    "edge_threshold_cents",
    "tiers.early.score",
    "tiers.early.max_ask",
    "tiers.early.time_stop",
    "tiers.early.max_hold",
    "tiers.normal.score",
    "tiers.normal.max_ask",
    "tiers.normal.time_stop",
    "tiers.normal.max_hold",
    "tiers.late.score",
    "tiers.late.max_ask",
    "tiers.late.time_stop",
    "tiers.late.max_hold",
    "logistic_model",
    "logistic_probability_threshold",
    "calibration_overrides.fast_15",
    "calibration_overrides.fast_30",
    "calibration_overrides.adverse_5",
    "calibration_overrides.retrace",
    "calibration_overrides.crosses",
    "calibration_overrides.edge",
    "calibration_overrides.early",
    "calibration_overrides.normal",
    "calibration_overrides.late",
    "calibration_overrides.early_max_ask",
    "calibration_overrides.normal_max_ask",
    "calibration_overrides.late_max_ask",
    "calibration_overrides.time_stop",
    "calibration_overrides.max_hold",
    "calibration_overrides.profit_target",
    "calibration_overrides.soft_stop",
    "calibration_overrides.hard_stop",
    "calibration_overrides.component_weight_multipliers.fast_impulse",
    "calibration_overrides.component_weight_multipliers.path_quality",
    "calibration_overrides.component_weight_multipliers.underreaction",
    "calibration_overrides.component_weight_multipliers.boundary_regime",
    "calibration_overrides.component_weight_multipliers.microstructure",
    "calibration_overrides.component_weight_multipliers.timing_economics",
}

# These are the persisted V2 safeguards.  Runtime AppConfig safeguards are not
# part of a candidate parameter snapshot, but are reported here as immutable
# protected paths so the governance record makes that boundary explicit.
_PROTECTED_GATE_PATHS = (
    "app_mode",
    "trading_enabled",
    "execute",
    "market_identity_readiness",
    "reference_freshness",
    "reference_source_freshness",
    "book_freshness",
    "sequence_integrity",
    "canonical_market_readiness",
    "canonical_reference_readiness",
    "maximum_spread",
    "minimum_executable_depth",
    "decision_to_book_latency_ms",
    "intent_expiry_seconds",
    "one_entry_per_market",
    "one_global_position",
    "entry_attempts_per_market",
    "boundary_cross_research_only",
    "global_maximum_ask",
    "maximum_hard_loss",
    "feature_windows_seconds[0]",
    "feature_windows_seconds[1]",
    "feature_windows_seconds[2]",
    "feature_windows_seconds[3]",
    "feature_windows_seconds[4]",
    "feature_windows_seconds[5]",
    "feature_windows_seconds[6]",
    "tiers.early.start",
    "tiers.early.end",
    "tiers.early.time_multiplier",
    "tiers.normal.start",
    "tiers.normal.end",
    "tiers.normal.time_multiplier",
    "tiers.late.start",
    "tiers.late.end",
    "tiers.late.time_multiplier",
)


def _config_diff_evidence(baseline: Any, candidate: Any) -> dict[str, Any]:
    before = _flatten_payload(baseline)
    after = _flatten_payload(candidate)
    changed = sorted(
        path for path in set(before) | set(after) if before.get(path) != after.get(path)
    )
    allowed = [path for path in changed if _allowed_candidate_path(path)]
    forbidden = [path for path in changed if path not in allowed]
    protected = list(_PROTECTED_GATE_PATHS)
    changed_protected = [path for path in changed if _protected_candidate_path(path)]
    unchanged_protected = [path for path in protected if path not in changed_protected]
    return {
        "changed_parameter_paths": changed,
        "allowed_changed_parameter_paths": allowed,
        "forbidden_changed_parameter_paths": forbidden,
        "protected_gate_paths": protected,
        "changed_protected_gate_paths": changed_protected,
        "unchanged_protected_gate_paths": unchanged_protected,
        "forbidden_parameter_changed": bool(forbidden),
        "safety_or_data_quality_gate_changed": bool(changed_protected),
    }


def _allowed_candidate_path(path: str) -> bool:
    return path in _CANDIDATE_TUNABLE_PATHS


def _protected_candidate_path(path: str) -> bool:
    if path in _PROTECTED_GATE_PATHS:
        return True
    # A candidate must not smuggle a production safety/data-quality control
    # into otherwise tunable calibration metadata.
    return path.rsplit(".", 1)[-1] in {
        "app_mode",
        "trading_enabled",
        "execute",
        "market_identity_readiness",
        "reference_freshness",
        "reference_source_freshness",
        "book_freshness",
        "sequence_integrity",
        "canonical_market_readiness",
        "canonical_reference_readiness",
        "maximum_spread",
        "minimum_executable_depth",
        "decision_to_book_latency_ms",
        "intent_expiry_seconds",
        "one_entry_per_market",
        "one_global_position",
        "entry_attempts_per_market",
        "boundary_cross_research_only",
        "global_maximum_ask",
        "maximum_hard_loss",
    }


def _flatten_payload(value: Any, prefix: str = "") -> dict[str, Any]:
    # The fitted logistic artifact is an immutable model payload.  It is an
    # approved single candidate parameter, not a free-form calibration branch.
    if prefix == "logistic_model":
        return {prefix: (type(value).__name__, value)}
    if isinstance(value, dict):
        if not value:
            return {prefix: ("dict", "<empty>")}
        flattened: dict[str, Any] = {}
        for key, item in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            flattened.update(_flatten_payload(item, path))
        return flattened
    if isinstance(value, list):
        if not value:
            return {prefix: ("list", "<empty>")}
        flattened: dict[str, Any] = {}
        for index, item in enumerate(value):
            flattened.update(_flatten_payload(item, f"{prefix}[{index}]"))
        return flattened
    return {prefix: (type(value).__name__, value)}
