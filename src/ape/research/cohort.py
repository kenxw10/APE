from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from ape.db.models import ResearchMarketOutcome
from ape.research import REPLAY_SCHEMA_VERSION, RESEARCH_LABEL_SCHEMA_VERSION
from ape.research.repository import (
    REPLAY_EVENT_PAGE_SIZE,
    FrozenReplayProgress,
    ReplayEventRecord,
    ReplayEventSnapshot,
    ResearchRepository,
)
from ape.strategy.momentum_v2 import (
    V2_ARCHITECTURE_VERSION,
    V2_FEATURE_SCHEMA_VERSION,
    V2_LIFECYCLE_SCHEMA_VERSION,
)

CALIBRATION_COHORT_SCHEMA_VERSION = "clean_calibration_cohort_v1"
CALIBRATION_EPOCH_MARKET_COUNT = 50
CALIBRATION_COMPACT_EVENT_LIMIT = 250_000
CALIBRATION_RELEVANT_EVENT_TYPES = (
    "MARKET",
    "REFERENCE",
    "ORDERBOOK",
    "FEATURE_SNAPSHOT",
)
CALIBRATION_REPLAY_EVENT_TYPES = ("FEATURE_SNAPSHOT", "ORDERBOOK")


class CalibrationInputLimitError(RuntimeError):
    pass


@dataclass
class _MarketEvidence:
    market_ticker: str
    outcome: ResearchMarketOutcome | None = None
    event_counts: Counter[str] = field(default_factory=Counter)
    feature_reasons: Counter[str] = field(default_factory=Counter)
    eligible_feature_ids: list[str] = field(default_factory=list)
    first_event_at: datetime | None = None
    last_event_at: datetime | None = None
    maximum_event_gap_seconds: int = 0
    exclusion_reason: str | None = None


@dataclass(frozen=True)
class CleanCalibrationCohort:
    manifest: dict[str, Any]
    eligible_feature_ids_by_market: dict[str, tuple[str, ...]]
    outcomes_by_market: dict[str, ResearchMarketOutcome]
    market_summaries: dict[str, dict[str, Any]]

    def epoch_manifest(self, epoch_size: int) -> dict[str, Any]:
        if epoch_size < CALIBRATION_EPOCH_MARKET_COUNT:
            raise ValueError("A calibration epoch requires at least 50 eligible markets.")
        ordered = tuple(self.manifest["ordered_eligible_market_tickers"][:epoch_size])
        if len(ordered) != epoch_size:
            raise ValueError("The requested calibration epoch is not complete.")
        included_counts: Counter[str] = Counter()
        feature_count = 0
        first_market_at: datetime | None = None
        last_market_at: datetime | None = None
        earliest_event_at: datetime | None = None
        latest_event_at: datetime | None = None
        for market_ticker in ordered:
            summary = self.market_summaries[market_ticker]
            included_counts.update(summary["event_counts_by_type"])
            feature_count += len(self.eligible_feature_ids_by_market[market_ticker])
            market_at = _datetime_or_none(summary.get("market_open_at"))
            event_start = _datetime_or_none(summary.get("first_event_at"))
            event_end = _datetime_or_none(summary.get("last_event_at"))
            first_market_at = _minimum_time(first_market_at, market_at)
            last_market_at = _maximum_time(last_market_at, market_at)
            earliest_event_at = _minimum_time(earliest_event_at, event_start)
            latest_event_at = _maximum_time(latest_event_at, event_end)
        outcome_hash = _hash(
            [
                _outcome_identity(self.outcomes_by_market[market_ticker])
                for market_ticker in ordered
            ]
        )
        identity = {
            "cohort_schema_version": CALIBRATION_COHORT_SCHEMA_VERSION,
            "epoch_size": epoch_size,
            "ordered_market_tickers": list(ordered),
            "input_outcome_hash": outcome_hash,
            "architecture_version": V2_ARCHITECTURE_VERSION,
            "feature_schema_version": V2_FEATURE_SCHEMA_VERSION,
            "replay_schema_version": REPLAY_SCHEMA_VERSION,
            "label_schema_version": RESEARCH_LABEL_SCHEMA_VERSION,
            "lifecycle_schema_version": V2_LIFECYCLE_SCHEMA_VERSION,
        }
        return {
            **identity,
            "epoch_hash": _hash(identity),
            "frozen_replay_watermark": self.manifest["frozen_replay_watermark"],
            "cohort_hash_at_creation": self.manifest["cohort_hash"],
            "eligible_market_count": epoch_size,
            "eligible_feature_frame_count": feature_count,
            "included_event_count_by_type": dict(sorted(included_counts.items())),
            "earliest_market_time": _iso(first_market_at),
            "latest_market_time": _iso(last_market_at),
            "earliest_event_time": _iso(earliest_event_at),
            "latest_event_time": _iso(latest_event_at),
            "current_baseline_config_version": self.manifest[
                "current_baseline_config_version"
            ],
            "code_commit_sha": self.manifest["code_commit_sha"],
            "eligible_feature_snapshot_ids_by_market": {
                market_ticker: list(self.eligible_feature_ids_by_market[market_ticker])
                for market_ticker in ordered
            },
        }


def build_clean_calibration_cohort(
    session: Session,
    *,
    snapshot: ReplayEventSnapshot,
    baseline_config_version_id: str,
    code_commit_sha: str,
    progress_callback: Callable[[FrozenReplayProgress], None] | None = None,
) -> CleanCalibrationCohort:
    """Derive one strict cohort from archived evidence under a frozen watermark."""
    outcomes = list(
        session.scalars(
            select(ResearchMarketOutcome).order_by(
                ResearchMarketOutcome.market_open_at.asc(),
                ResearchMarketOutcome.market_ticker.asc(),
                ResearchMarketOutcome.id.asc(),
            )
        )
    )
    outcomes_by_market = {row.market_ticker: row for row in outcomes}
    states = {
        row.market_ticker: _MarketEvidence(row.market_ticker, outcome=row)
        for row in outcomes
    }
    pending: dict[str, list[dict[str, Any]]] = {}
    feature_exclusions: Counter[str] = Counter()
    architecture_versions: Counter[str] = Counter()
    feature_schema_versions: Counter[str] = Counter()
    replay_schema_versions: Counter[str] = Counter()
    repository = ResearchRepository(session)
    reader = repository.calibration_replay_event_reader(
        snapshot,
        event_types=CALIBRATION_RELEVANT_EVENT_TYPES,
        page_size=REPLAY_EVENT_PAGE_SIZE,
    )
    for page in reader.iter_pages(progress_callback=progress_callback):
        for event in page:
            market_ticker = event.market_ticker
            if market_ticker is None:
                if event.event_type == "FEATURE_SNAPSHOT":
                    feature_exclusions["unassociated_feature_rows"] += 1
                continue
            state = states.setdefault(market_ticker, _MarketEvidence(market_ticker))
            state.event_counts[event.event_type] += 1
            if state.first_event_at is None:
                state.first_event_at = event.event_time
            if state.last_event_at is not None:
                state.maximum_event_gap_seconds = max(
                    state.maximum_event_gap_seconds,
                    max(0, int((event.event_time - state.last_event_at).total_seconds())),
                )
            state.last_event_at = event.event_time
            if event.event_type == "FEATURE_SNAPSHOT":
                _consider_feature(
                    event,
                    state=state,
                    outcome=outcomes_by_market.get(market_ticker),
                    pending=pending,
                    feature_exclusions=feature_exclusions,
                    architecture_versions=architecture_versions,
                    feature_schema_versions=feature_schema_versions,
                    replay_schema_versions=replay_schema_versions,
                )
            elif event.event_type == "ORDERBOOK":
                _resolve_pending_features(
                    event,
                    state=state,
                    pending=pending,
                    feature_exclusions=feature_exclusions,
                )
    for market_ticker, rows in pending.items():
        state = states[market_ticker]
        for _row in rows:
            _exclude_feature(state, feature_exclusions, "missing_first_book_windows")

    eligible_states: list[_MarketEvidence] = []
    excluded_markets: Counter[str] = Counter()
    source_complete_markets = 0
    missing_source_counts: Counter[str] = Counter()
    for state in states.values():
        missing_sources = sorted(set(CALIBRATION_RELEVANT_EVENT_TYPES) - state.event_counts.keys())
        for source in missing_sources:
            missing_source_counts[source] += 1
        if not missing_sources:
            source_complete_markets += 1
        state.exclusion_reason = _market_exclusion_reason(state, missing_sources)
        if state.exclusion_reason is None:
            eligible_states.append(state)
        else:
            excluded_markets[state.exclusion_reason] += 1

    eligible_states.sort(key=_market_sort_key)
    ordered_eligible = [state.market_ticker for state in eligible_states]
    included_counts: Counter[str] = Counter()
    eligible_feature_count = 0
    maximum_gap = 0
    market_summaries: dict[str, dict[str, Any]] = {}
    for state in states.values():
        outcome = state.outcome
        market_summaries[state.market_ticker] = {
            "event_counts_by_type": dict(sorted(state.event_counts.items())),
            "feature_exclusion_counts": dict(sorted(state.feature_reasons.items())),
            "eligible_feature_frame_count": len(state.eligible_feature_ids),
            "maximum_event_gap_seconds": state.maximum_event_gap_seconds,
            "first_event_at": _iso(state.first_event_at),
            "last_event_at": _iso(state.last_event_at),
            "market_open_at": _iso(outcome.market_open_at) if outcome else None,
            "exclusion_reason": state.exclusion_reason,
        }
        if state.exclusion_reason is None:
            included_counts.update(state.event_counts)
            eligible_feature_count += len(state.eligible_feature_ids)
            maximum_gap = max(maximum_gap, state.maximum_event_gap_seconds)

    eligible_outcomes = [
        outcomes_by_market[ticker]
        for ticker in ordered_eligible
        if ticker in outcomes_by_market
    ]
    outcome_hash = _hash([_outcome_identity(row) for row in eligible_outcomes])
    core_manifest = {
        "cohort_schema_version": CALIBRATION_COHORT_SCHEMA_VERSION,
        "frozen_replay_watermark": snapshot.watermark_id,
        "frozen_replay_event_count": snapshot.event_count,
        "ordered_eligible_market_tickers": ordered_eligible,
        "eligible_market_count": len(ordered_eligible),
        "eligible_feature_frame_count": eligible_feature_count,
        "included_event_count_by_type": dict(sorted(included_counts.items())),
        "architecture_version_distribution": dict(sorted(architecture_versions.items())),
        "feature_schema_version_distribution": dict(sorted(feature_schema_versions.items())),
        "replay_schema_version_distribution": dict(sorted(replay_schema_versions.items())),
        "exclusion_counts_by_reason": dict(sorted(feature_exclusions.items())),
        "excluded_market_counts_by_reason": dict(sorted(excluded_markets.items())),
        "maximum_relevant_event_gap_seconds": maximum_gap,
        "source_completeness": {
            "observed_market_count": len(states),
            "source_complete_market_count": source_complete_markets,
            "eligible_source_complete_market_count": len(eligible_states),
            "missing_source_market_counts": dict(sorted(missing_source_counts.items())),
            "unassociated_feature_row_count": feature_exclusions.get(
                "unassociated_feature_rows", 0
            ),
        },
        "earliest_market_time": _iso(
            min(
                (_utc(row.market_open_at) for row in eligible_outcomes if row.market_open_at),
                default=None,
            )
        ),
        "latest_market_time": _iso(
            max(
                (_utc(row.market_open_at) for row in eligible_outcomes if row.market_open_at),
                default=None,
            )
        ),
        "earliest_event_time": _iso(
            min(
                (
                    state.first_event_at
                    for state in eligible_states
                    if state.first_event_at
                ),
                default=None,
            )
        ),
        "latest_event_time": _iso(
            max(
                (
                    state.last_event_at
                    for state in eligible_states
                    if state.last_event_at
                ),
                default=None,
            )
        ),
        "input_outcome_hash": outcome_hash,
        "current_baseline_config_version": baseline_config_version_id,
        "code_commit_sha": code_commit_sha,
        "compatibility": {
            "architecture_version": V2_ARCHITECTURE_VERSION,
            "feature_schema_version": V2_FEATURE_SCHEMA_VERSION,
            "replay_schema_version": REPLAY_SCHEMA_VERSION,
            "label_schema_version": RESEARCH_LABEL_SCHEMA_VERSION,
            "lifecycle_schema_version": V2_LIFECYCLE_SCHEMA_VERSION,
        },
        "reader_progress": {
            "pages_scanned": reader.pages_scanned,
            "events_scanned": reader.events_scanned,
            "partitions_scanned": reader.partitions_completed,
            "maximum_page_size": reader.max_page_size,
        },
    }
    manifest = {**core_manifest, "cohort_hash": _hash(core_manifest)}
    return CleanCalibrationCohort(
        manifest=manifest,
        eligible_feature_ids_by_market={
            state.market_ticker: tuple(state.eligible_feature_ids)
            for state in eligible_states
        },
        outcomes_by_market={row.market_ticker: row for row in eligible_outcomes},
        market_summaries=market_summaries,
    )


def completed_epoch_size(eligible_market_count: int) -> int:
    return (
        eligible_market_count // CALIBRATION_EPOCH_MARKET_COUNT
    ) * CALIBRATION_EPOCH_MARKET_COUNT


def next_epoch_market_count(eligible_market_count: int) -> int:
    return completed_epoch_size(eligible_market_count) + CALIBRATION_EPOCH_MARKET_COUNT


def extract_compact_calibration_events(
    session: Session,
    *,
    snapshot: ReplayEventSnapshot,
    cohort: CleanCalibrationCohort,
    epoch_manifest: dict[str, Any],
    progress_callback: Callable[[FrozenReplayProgress], None] | None = None,
) -> tuple[list[ReplayEventRecord], dict[str, int]]:
    market_tickers = tuple(epoch_manifest["ordered_market_tickers"])
    epoch_feature_ids = epoch_manifest.get("eligible_feature_snapshot_ids_by_market")
    feature_ids = frozenset(
        feature_id
        for market_ticker in market_tickers
        for feature_id in (
            epoch_feature_ids.get(market_ticker, [])
            if isinstance(epoch_feature_ids, dict)
            else cohort.eligible_feature_ids_by_market[market_ticker]
        )
    )
    repository = ResearchRepository(session)
    reader = repository.calibration_replay_event_reader(
        snapshot,
        market_tickers=market_tickers,
        event_types=CALIBRATION_REPLAY_EVENT_TYPES,
        feature_snapshot_ids=feature_ids,
        page_size=REPLAY_EVENT_PAGE_SIZE,
    )
    events: list[ReplayEventRecord] = []
    for page in reader.iter_pages(progress_callback=progress_callback):
        events.extend(page)
        if len(events) > CALIBRATION_COMPACT_EVENT_LIMIT:
            raise CalibrationInputLimitError(
                "Compact calibration evidence exceeded the fixed code-level cap."
            )
    return events, {
        "pages_scanned": reader.pages_scanned,
        "events_scanned": reader.events_scanned,
        "partitions_scanned": reader.partitions_completed,
        "maximum_page_size": reader.max_page_size,
        "compact_event_count": len(events),
    }


def _consider_feature(
    event: ReplayEventRecord,
    *,
    state: _MarketEvidence,
    outcome: ResearchMarketOutcome | None,
    pending: dict[str, list[dict[str, Any]]],
    feature_exclusions: Counter[str],
    architecture_versions: Counter[str],
    feature_schema_versions: Counter[str],
    replay_schema_versions: Counter[str],
) -> None:
    architecture_versions[str(event.architecture_version or "unknown")] += 1
    feature_schema_versions[str(event.feature_schema_version or "unknown")] += 1
    replay_schema_versions[str(event.replay_schema_version or "unknown")] += 1
    vector = (event.payload or {}).get("feature_vector")
    if not isinstance(vector, dict) or not vector:
        _exclude_feature(state, feature_exclusions, "unusable_feature_vectors")
        return
    if vector.get("candidate_side") not in {"YES", "NO"}:
        _exclude_feature(state, feature_exclusions, "missing_candidate_side")
        return
    if event.architecture_version != V2_ARCHITECTURE_VERSION:
        _exclude_feature(state, feature_exclusions, "wrong_architecture_versions")
        return
    if event.feature_schema_version != V2_FEATURE_SCHEMA_VERSION:
        _exclude_feature(state, feature_exclusions, "wrong_feature_schema_versions")
        return
    if event.replay_schema_version != REPLAY_SCHEMA_VERSION:
        _exclude_feature(state, feature_exclusions, "wrong_replay_schema_versions")
        return
    if event.replay_readiness != "FULL":
        _exclude_feature(state, feature_exclusions, "partial_feature_vectors")
        return
    if outcome is None or outcome.outcome_status != "RESOLVED":
        _exclude_feature(state, feature_exclusions, "unresolved_feature_markets")
        return
    flags = outcome.quality_flags if isinstance(outcome.quality_flags, dict) else {}
    labels = flags.get("counterfactual_labels") if isinstance(flags, dict) else None
    label = labels.get(event.feature_snapshot_id or "") if isinstance(labels, dict) else None
    if (
        flags.get("label_schema_version") != RESEARCH_LABEL_SCHEMA_VERSION
        or not isinstance(label, dict)
        or label.get("net_markout_30s_cents") is None
    ):
        _exclude_feature(state, feature_exclusions, "immature_labels")
        return
    entry_at = _datetime_or_none(label.get("entry_at"))
    if not label.get("entry_fillable") or entry_at is None:
        _exclude_feature(state, feature_exclusions, "missing_first_book_windows")
        return
    pending.setdefault(state.market_ticker, []).append(
        {
            "feature_snapshot_id": event.feature_snapshot_id,
            "effective_after": event.event_time + timedelta(milliseconds=500),
            "expires_at": event.event_time + timedelta(milliseconds=2500),
            "label_entry_at": entry_at,
        }
    )


def _resolve_pending_features(
    event: ReplayEventRecord,
    *,
    state: _MarketEvidence,
    pending: dict[str, list[dict[str, Any]]],
    feature_exclusions: Counter[str],
) -> None:
    rows = pending.get(state.market_ticker)
    if not rows:
        return
    remaining: list[dict[str, Any]] = []
    for row in rows:
        if event.event_time < row["effective_after"]:
            remaining.append(row)
            continue
        if event.event_time > row["expires_at"]:
            _exclude_feature(state, feature_exclusions, "missing_first_book_windows")
            continue
        if event.event_time == row["label_entry_at"]:
            feature_snapshot_id = row["feature_snapshot_id"]
            if feature_snapshot_id is not None:
                state.eligible_feature_ids.append(feature_snapshot_id)
            continue
        _exclude_feature(state, feature_exclusions, "missing_first_book_windows")
    pending[state.market_ticker] = remaining


def _exclude_feature(
    state: _MarketEvidence, feature_exclusions: Counter[str], reason: str
) -> None:
    state.feature_reasons[reason] += 1
    feature_exclusions[reason] += 1


def _market_exclusion_reason(
    state: _MarketEvidence, missing_sources: list[str]
) -> str | None:
    event_types = set(state.event_counts)
    if event_types == {"MARKET"}:
        return "market_only_history"
    if state.outcome is None or state.outcome.outcome_status != "RESOLVED":
        return "unresolved_markets"
    if missing_sources:
        return "missing_required_sources"
    if state.eligible_feature_ids:
        return None
    for reason in (
        "missing_candidate_side",
        "wrong_architecture_versions",
        "wrong_feature_schema_versions",
        "wrong_replay_schema_versions",
        "partial_feature_vectors",
        "immature_labels",
        "missing_first_book_windows",
        "unusable_feature_vectors",
    ):
        if state.feature_reasons.get(reason):
            return reason
    return "no_eligible_feature_frames"


def _market_sort_key(state: _MarketEvidence) -> tuple[datetime, str]:
    market_open_at = state.outcome.market_open_at if state.outcome else None
    return (_utc(market_open_at or datetime.min.replace(tzinfo=UTC)), state.market_ticker)


def _outcome_identity(row: ResearchMarketOutcome) -> dict[str, Any]:
    return {
        "market_ticker": row.market_ticker,
        "market_open_at": _iso(row.market_open_at),
        "market_close_at": _iso(row.market_close_at),
        "outcome_status": row.outcome_status,
        "result_side": row.result_side,
        "resolved_at": _iso(row.resolved_at),
        "source_payload_hash": row.source_payload_hash,
    }


def _hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _datetime_or_none(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if isinstance(value, str):
        try:
            return _utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
        except ValueError:
            return None
    return None


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _iso(value: datetime | None) -> str | None:
    return _utc(value).isoformat() if value is not None else None


def _minimum_time(current: datetime | None, value: datetime | None) -> datetime | None:
    if value is None:
        return current
    return value if current is None else min(current, value)


def _maximum_time(current: datetime | None, value: datetime | None) -> datetime | None:
    if value is None:
        return current
    return value if current is None else max(current, value)
