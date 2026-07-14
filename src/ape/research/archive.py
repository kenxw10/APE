from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import String, and_, cast, desc, exists, func, or_, select, text
from sqlalchemy.orm import Session

from ape.db.models import (
    Market,
    OrderbookSnapshot,
    PublicTrade,
    ReferenceTick,
    ResearchMarketOutcome,
    ResearchReplayEvent,
    StrategyDecision,
    StrategyFeatureSnapshot,
    StrategyPositionOutcome,
    StrategyTradeIntent,
)
from ape.kalshi.errors import KalshiError
from ape.kalshi.reference_messages import BRTI_SOURCE
from ape.research import REPLAY_SCHEMA_VERSION, RESEARCH_LABEL_SCHEMA_VERSION
from ape.research.fees import verified_kalshi_taker_fee_model
from ape.research.repository import ReplayEventSnapshot, ResearchRepository

ARCHIVE_BATCH_SIZE = 250
ARCHIVE_MAX_BATCHES_PER_CYCLE = 20
LABEL_MARKETS_PER_CYCLE = 25
LABEL_MAX_HORIZON_SECONDS = 65
ARCHIVE_SOURCE_STAGES = (
    "markets",
    "reference_ticks",
    "orderbook_snapshots",
    "public_trades",
    "strategy_feature_snapshots",
    "strategy_trade_intents",
    "strategy_position_outcomes",
)
_ARCHIVE_SOURCE_DETAILS = {
    "reference_ticks": (ReferenceTick, "REFERENCE"),
    "orderbook_snapshots": (OrderbookSnapshot, "ORDERBOOK"),
    "public_trades": (PublicTrade, "TRADE"),
    "strategy_feature_snapshots": (StrategyFeatureSnapshot, "FEATURE_SNAPSHOT"),
    "strategy_trade_intents": (StrategyTradeIntent, "MARKET_LIFECYCLE"),
    "strategy_position_outcomes": (StrategyPositionOutcome, "MARKET_LIFECYCLE"),
}
_REPLAY_EVENT_HASH_FIELDS = (
    "event_id",
    "market_ticker",
    "event_type",
    "event_time",
    "received_at",
    "source_table",
    "source_row_id",
    "source_hash",
    "sequence_number",
    "feature_snapshot_id",
    "feature_schema_version",
    "architecture_version",
    "replay_schema_version",
    "payload",
    "replay_readiness",
    "blockers",
)


@dataclass(frozen=True)
class ArchiveResult:
    archived_events: int
    archived_by_type: dict[str, int]
    outcomes_reconciled: int
    coverage: dict[str, Any]


@dataclass(frozen=True)
class ArchiveBatchResult:
    source_stage: str
    source_rows: int
    archived_events: int
    archived_by_type: dict[str, int]


@dataclass(frozen=True)
class LabelRefreshResult:
    processed_markets: int
    remaining_markets: int


def archive_research_events(session: Session, *, now: datetime | None = None) -> ArchiveResult:
    """Incrementally archive normalized, replayable source data without raw payload copies."""
    checked_at = _utc(now or datetime.now(UTC))
    counts: dict[str, int] = {}
    for source_stage in ARCHIVE_SOURCE_STAGES:
        while True:
            batch = archive_research_batch(session, source_stage=source_stage)
            _merge_archive_counts(counts, batch.archived_by_type)
            if batch.source_rows == 0:
                break
    refresh_research_archive_labels(session)
    coverage = archive_research_coverage(session, now=checked_at)
    return ArchiveResult(sum(counts.values()), counts, 0, coverage)


def archive_research_batch(session: Session, *, source_stage: str) -> ArchiveBatchResult:
    """Archive at most ``ARCHIVE_BATCH_SIZE`` rows from one deterministic source."""
    _acquire_archive_source_lock(session, _archive_source_table(source_stage))
    repository = ResearchRepository(session)
    rows, source_table, event_type = _archive_source_rows(session, repository, source_stage)
    if not rows:
        return ArchiveBatchResult(source_stage, 0, 0, {})
    events = [_archive_source_event(session, source_stage, row) for row in rows]
    changed = repository.archive_events_batch(events)
    return ArchiveBatchResult(
        source_stage=source_stage,
        source_rows=len(rows),
        archived_events=changed,
        archived_by_type={event_type: changed} if changed else {},
    )


def archive_research_source_pending(session: Session, *, source_stage: str) -> bool:
    """Read-only backlog check used when a runtime batch budget is exhausted."""
    rows, _, _ = _archive_source_rows(session, ResearchRepository(session), source_stage)
    return bool(rows)


def refresh_research_archive_labels(session: Session) -> LabelRefreshResult:
    """Persist reference association and mature labels independently of replay work."""
    repository = ResearchRepository(session)
    _associate_unassigned_reference_events(session)
    result = _refresh_mature_labels(session, repository)
    session.flush()
    return result


def archive_research_coverage(
    session: Session,
    *,
    now: datetime | None = None,
    snapshot: ReplayEventSnapshot | None = None,
) -> dict[str, Any]:
    checked_at = _utc(now or datetime.now(UTC))
    repository = ResearchRepository(session)
    frozen_snapshot = snapshot or repository.replay_event_snapshot()
    coverage = _coverage(session, data_cutoff=checked_at, snapshot=frozen_snapshot)
    repository.archive_event(_coverage_event(coverage, checked_at))
    session.flush()
    return coverage


def _archive_source_rows(session: Session, repository: ResearchRepository, source_stage: str):
    if source_stage == "markets":
        rows = list(_unarchived_rows(session, repository, Market, "markets"))
        if not rows:
            rows = list(_updated_archived_rows(session, Market, "markets"))
        return rows, "markets", "MARKET"
    try:
        model, event_type = _ARCHIVE_SOURCE_DETAILS[source_stage]
    except KeyError as error:
        raise ValueError(f"Unsupported archive source stage: {source_stage}") from error
    return (
        list(_unarchived_rows(session, repository, model, source_stage)),
        source_stage,
        event_type,
    )


def _archive_source_table(source_stage: str) -> str:
    if source_stage == "markets":
        return "markets"
    if source_stage not in _ARCHIVE_SOURCE_DETAILS:
        raise ValueError(f"Unsupported archive source stage: {source_stage}")
    return source_stage


def _acquire_archive_source_lock(session: Session, source_table: str) -> None:
    """Serialize mutable archive selection on PostgreSQL; SQLite stays single-process safe."""
    bind = session.get_bind()
    if bind.dialect.name != "postgresql":
        return
    session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
        {"lock_key": f"ape:research_archive:{source_table}"},
    )


def _archive_source_event(session: Session, source_stage: str, row: Any) -> dict[str, Any]:
    if source_stage == "markets":
        return _market_event(row)
    if source_stage == "reference_ticks":
        return _reference_event(session, row)
    if source_stage == "orderbook_snapshots":
        return _orderbook_event(row)
    if source_stage == "public_trades":
        return _trade_event(row)
    if source_stage == "strategy_feature_snapshots":
        return _feature_event(session, row)
    return _lifecycle_event(row, source_stage)


def _merge_archive_counts(target: dict[str, int], incoming: dict[str, int]) -> None:
    for event_type, count in incoming.items():
        target[event_type] = target.get(event_type, 0) + count


def _unarchived_rows(session: Session, repository: ResearchRepository, model, source_table: str):
    # A cursor based only on the last inserted archive row can skip an interrupted
    # or out-of-order source row. The anti-join makes every source primary key
    # independently idempotent while repository.latest_archived_source_row_id()
    # remains a numeric-only diagnostic cursor.
    del repository
    archived = exists(
        select(1).where(
            ResearchReplayEvent.source_table == source_table,
            ResearchReplayEvent.source_row_id == cast(model.id, String),
        )
    )
    statement = select(model).where(~archived).order_by(model.id.asc()).limit(ARCHIVE_BATCH_SIZE)
    return session.scalars(statement)


def _updated_archived_rows(session: Session, model, source_table: str):
    """Return mutable source rows whose normalized archive needs a refresh."""
    statement = (
        select(model)
        .join(
            ResearchReplayEvent,
            and_(
                ResearchReplayEvent.source_table == source_table,
                ResearchReplayEvent.source_row_id == cast(model.id, String),
            ),
        )
        .where(model.updated_at > ResearchReplayEvent.event_time)
        .order_by(model.updated_at.asc(), model.id.asc())
        .limit(ARCHIVE_BATCH_SIZE)
    )
    return session.scalars(statement)


def reconcile_market_outcomes(session: Session, *, client, now: datetime | None = None) -> int:
    """Public-data-only market outcome reconciliation owned by the market-data service."""
    checked_at = _utc(now or datetime.now(UTC))
    repository = ResearchRepository(session)
    changed = 0
    for market in session.scalars(
        select(Market)
        .outerjoin(
            ResearchMarketOutcome,
            ResearchMarketOutcome.market_ticker == Market.market_ticker,
        )
        .where(
            Market.close_time.is_not(None),
            Market.close_time <= checked_at,
            or_(
                Market.series_ticker == "KXBTC15M",
                Market.market_ticker.ilike("KXBTC15M%"),
            ),
            or_(
                ResearchMarketOutcome.id.is_(None),
                ResearchMarketOutcome.outcome_status != "RESOLVED",
            ),
        )
        .order_by(Market.close_time.asc(), Market.id.asc())
        .limit(500)
    ):
        if not _is_btc15_market(market):
            continue
        if market.close_time is None:
            continue
        close_at = _utc(market.close_time)
        expiration = _utc(
            market.expiration_time or market.latest_expiration_time or market.close_time
        )
        ticks = list(
            session.scalars(
                select(ReferenceTick)
                .where(
                    ReferenceTick.source == BRTI_SOURCE,
                    ReferenceTick.received_at >= _utc(market.open_time or close_at),
                    ReferenceTick.received_at <= max(expiration, close_at) + timedelta(minutes=1),
                    ReferenceTick.parsed_value.is_not(None),
                )
                .order_by(ReferenceTick.received_at.asc(), ReferenceTick.id.asc())
            )
        )
        final_tick = ticks[-1] if ticks else None
        boundary = market.functional_strike or market.floor_strike
        try:
            response = client.get_market(market.market_ticker)
        except KalshiError:
            response = {}
        official = response.get("market") if isinstance(response.get("market"), dict) else response
        official = official if isinstance(official, dict) else {}
        result_side = _official_result_side(official)
        official_status = str(official.get("status") or "").strip().lower()
        resolved = result_side is not None and _official_is_settled(official, official_status)
        status = "RESOLVED" if resolved else "PENDING" if official else "UNAVAILABLE"
        final_value = (
            Decimal(final_tick.parsed_value)
            if final_tick and final_tick.parsed_value is not None
            else None
        )
        final_minute = [
            Decimal(tick.parsed_value)
            for tick in ticks
            if tick.parsed_value is not None
            and close_at - timedelta(seconds=60) <= _utc(tick.received_at) <= close_at
        ]
        expected = max(1, int((close_at - _utc(market.open_time or close_at)).total_seconds()))
        actual = len(ticks)
        gaps = [
            int((_utc(right.received_at) - _utc(left.received_at)).total_seconds())
            for left, right in zip(ticks, ticks[1:], strict=False)
        ]
        repository.upsert_market_outcome(
            {
                "outcome_id": _identifier("outcome", market.market_ticker),
                "market_ticker": market.market_ticker,
                "market_open_at": market.open_time,
                "market_close_at": market.close_time,
                "expiration_at": market.expiration_time or market.latest_expiration_time,
                "boundary": Decimal(boundary) if boundary is not None else None,
                "result_side": result_side if resolved else None,
                "settlement_value": _official_settlement_value(official) if resolved else None,
                "final_reference_value": final_value,
                "final_minute_reference_average": sum(final_minute, Decimal("0"))
                / len(final_minute)
                if final_minute
                else None,
                "outcome_status": status,
                "outcome_source": "kalshi_public_market_detail",
                "source_payload_hash": _hash(official),
                "resolved_at": checked_at if resolved else None,
                "expected_frame_count": expected,
                "actual_frame_count": actual,
                "coverage_percentage": Decimal(actual) / Decimal(expected),
                "maximum_event_gap_seconds": max(gaps, default=None),
                "quality_flags": {
                    "official_outcome_available": resolved,
                    "official_status": official_status or None,
                },
            }
        )
        changed += 1
    return changed


def _is_btc15_market(market: Market) -> bool:
    return bool(
        market.series_ticker == "KXBTC15M"
        or (market.market_ticker or "").upper().startswith("KXBTC15M")
    )


def _official_result_side(payload: dict[str, Any]) -> str | None:
    value = str(payload.get("result") or payload.get("result_side") or "").strip().upper()
    if value in {"YES", "NO"}:
        return value
    for key in ("settlement_value_dollars", "settlement_value", "settlement_price"):
        value = payload.get(key)
        if value is None:
            continue
        try:
            settlement = Decimal(str(value))
        except (ArithmeticError, ValueError):
            continue
        if settlement == Decimal("1"):
            return "YES"
        if settlement == Decimal("0"):
            return "NO"
    return None


def _official_is_settled(payload: dict[str, Any], official_status: str) -> bool:
    return official_status in {"settled", "resolved", "finalized", "closed"} or bool(
        payload.get("settlement_ts")
    )


def _official_settlement_value(payload: dict[str, Any]) -> Decimal | None:
    for key in (
        "expiration_value",
        "settlement_value",
        "settlement_value_dollars",
        "settlement_price",
        "result_value",
    ):
        value = payload.get(key)
        if value is None:
            continue
        try:
            return Decimal(str(value))
        except (ArithmeticError, ValueError):
            continue
    return None


def _archive(repository: ResearchRepository, event: dict[str, Any], counts: dict[str, int]) -> None:
    existing = repository.get_event_by_source(
        source_table=event["source_table"], source_row_id=event["source_row_id"]
    )
    if existing is not None and existing.source_hash == event["source_hash"]:
        # Mutable sources use event_time as the incremental archive cursor.
        existing.event_time = event["event_time"]
        existing.received_at = event["received_at"]
        existing.event_hash = _normalized_event_hash(existing)
        return
    repository.archive_event(event)
    counts[event["event_type"]] = counts.get(event["event_type"], 0) + 1


def _market_event(row: Market) -> dict[str, Any]:
    return _event(
        row=row,
        source_table="markets",
        event_type="MARKET",
        market_ticker=row.market_ticker,
        event_time=row.updated_at,
        received_at=row.updated_at,
        payload={
            "open_time": row.open_time,
            "close_time": row.close_time,
            "expiration_time": row.expiration_time or row.latest_expiration_time,
            "boundary": row.functional_strike or row.floor_strike,
            "series_ticker": row.series_ticker,
        },
    )


def _reference_event(session: Session, row: ReferenceTick) -> dict[str, Any]:
    event_time = _utc(row.source_ts or row.received_at)
    return _event(
        row=row,
        source_table="reference_ticks",
        event_type="REFERENCE",
        market_ticker=_active_btc15_market_ticker(session, event_time),
        event_time=event_time,
        received_at=row.received_at,
        sequence_number=row.sequence_number,
        payload={
            "source": row.source,
            "parsed_value": row.parsed_value,
            "parse_status": row.parse_status,
            "source_ts": row.source_ts,
            "source_age_ms": row.source_age_ms,
        },
    )


def _active_btc15_market_ticker(session: Session, event_time: datetime) -> str | None:
    """Associate a reference tick with exactly one active BTC15 market interval."""
    return session.scalar(
        select(Market.market_ticker)
        .where(
            Market.open_time.is_not(None),
            Market.close_time.is_not(None),
            Market.open_time <= event_time,
            Market.close_time > event_time,
            or_(
                Market.series_ticker == "KXBTC15M",
                Market.market_ticker.ilike("KXBTC15M%"),
            ),
        )
        .order_by(Market.open_time.desc(), Market.id.desc())
        .limit(1)
    )


def _associate_unassigned_reference_events(session: Session) -> None:
    """Backfill legacy global reference events when their BTC15 window is known."""
    active_market_exists = exists(
        select(1).where(
            Market.open_time.is_not(None),
            Market.close_time.is_not(None),
            Market.open_time <= ResearchReplayEvent.event_time,
            Market.close_time > ResearchReplayEvent.event_time,
            or_(
                Market.series_ticker == "KXBTC15M",
                Market.market_ticker.ilike("KXBTC15M%"),
            ),
        )
    )
    rows = session.scalars(
        select(ResearchReplayEvent)
        .where(
            ResearchReplayEvent.event_type == "REFERENCE",
            ResearchReplayEvent.market_ticker.is_(None),
            active_market_exists,
        )
        .order_by(ResearchReplayEvent.id.asc())
        .limit(ARCHIVE_BATCH_SIZE)
    )
    for event in rows:
        event.market_ticker = _active_btc15_market_ticker(session, _utc(event.event_time))
        event.event_hash = _normalized_event_hash(event)


def _orderbook_event(row: OrderbookSnapshot) -> dict[str, Any]:
    return _event(
        row=row,
        source_table="orderbook_snapshots",
        event_type="ORDERBOOK",
        market_ticker=row.market_ticker,
        event_time=row.received_at,
        received_at=row.received_at,
        sequence_number=row.sequence_number,
        payload={
            "yes_bid": row.yes_bid,
            "yes_ask": row.yes_ask,
            "no_bid": row.no_bid,
            "no_ask": row.no_ask,
            "yes_bid_size": _fixed_count_or_legacy_size(row.yes_bid_count, row.yes_bid_size),
            "yes_ask_size": _fixed_count_or_legacy_size(row.yes_ask_count, row.yes_ask_size),
            "no_bid_size": _fixed_count_or_legacy_size(row.no_bid_count, row.no_bid_size),
            "no_ask_size": _fixed_count_or_legacy_size(row.no_ask_count, row.no_ask_size),
            "yes_bid_ladder": _top_ladder(row.yes_bid_ladder),
            "yes_ask_ladder": _top_ladder(row.yes_ask_ladder),
            "no_bid_ladder": _top_ladder(row.no_bid_ladder),
            "no_ask_ladder": _top_ladder(row.no_ask_ladder),
            "book_status": row.book_status,
        },
    )


def _fixed_count_or_legacy_size(
    fixed_count: Decimal | None, legacy_size: int | None
) -> Decimal | int | None:
    return fixed_count if fixed_count is not None else legacy_size


def _trade_event(row: PublicTrade) -> dict[str, Any]:
    return _event(
        row=row,
        source_table="public_trades",
        event_type="PUBLIC_TRADE",
        market_ticker=row.market_ticker,
        event_time=row.executed_at or row.received_at,
        received_at=row.received_at,
        payload={
            "price": row.price,
            "count": row.trade_count or row.count,
            "taker_side": row.taker_side or row.side_inferred,
            "trade_id": row.trade_id,
        },
    )


def _feature_event(session: Session, row: StrategyFeatureSnapshot) -> dict[str, Any]:
    vector = _feature_vector_for_snapshot(session, row)
    readiness = row.replay_readiness or "FULL"
    blockers = list(row.replay_blockers or [])
    if not vector:
        vector = {}
        readiness = "PARTIAL"
        if row.feature_schema_version == "momentum_v2_features_v2":
            blockers.append("v2_feature_vector_unrecoverable")
        blockers.append("feature_vector_missing")
    return _event(
        row=row,
        source_table="strategy_feature_snapshots",
        event_type="FEATURE_SNAPSHOT",
        market_ticker=row.market_ticker,
        event_time=row.evaluated_at,
        received_at=row.evaluated_at,
        feature_snapshot_id=row.feature_snapshot_id,
        feature_schema_version=row.feature_schema_version,
        architecture_version=row.architecture_version,
        replay_readiness=readiness,
        blockers=blockers,
        payload={
            "feature_vector": vector,
            "feature_vector_hash": row.feature_vector_hash,
            "context_hash": row.context_hash,
        },
    )


def _feature_vector_for_snapshot(
    session: Session, row: StrategyFeatureSnapshot
) -> dict[str, Any] | None:
    vector = row.complete_feature_vector
    if vector or row.feature_schema_version != "momentum_v2_features_v2":
        return vector if isinstance(vector, dict) else None
    decision = session.scalar(
        select(StrategyDecision)
        .where(StrategyDecision.feature_snapshot_id == row.feature_snapshot_id)
        .order_by(desc(StrategyDecision.id))
        .limit(1)
    )
    measurements = decision.measurements if decision else None
    recovered = measurements.get("features") if isinstance(measurements, dict) else None
    if not isinstance(recovered, dict):
        return None
    return _hydrate_legacy_v2_feature_vector(row, recovered)


def _hydrate_legacy_v2_feature_vector(
    row: StrategyFeatureSnapshot, recovered: dict[str, Any]
) -> dict[str, Any] | None:
    """Complete pre-PR11 decision features from their immutable snapshot context."""
    vector = dict(recovered)
    for key, value in {
        "candidate_side": row.candidate_side,
        "candidate_mode": row.candidate_mode,
        "boundary": row.boundary,
        "current_brti": row.current_brti,
        "seconds_since_open": row.seconds_since_open,
        "seconds_left": row.seconds_left,
        "quality_state": row.quality_state,
        "architecture_version": row.architecture_version,
        "feature_schema_version": row.feature_schema_version,
        "replay_schema_version": row.replay_schema_version,
    }.items():
        if value is not None:
            vector[key] = value
    if row.seconds_since_open is not None and row.seconds_left is not None:
        from ape.strategy.momentum_v2 import _timing_tier

        vector["timing_tier"] = _timing_tier(
            row.seconds_since_open, row.seconds_left
        )
    if any(
        vector.get(key) is None
        for key in (
            "candidate_side",
            "candidate_mode",
            "boundary",
            "seconds_since_open",
            "seconds_left",
            "timing_tier",
        )
    ):
        return None
    return vector


def _lifecycle_event(row: Any, source_table: str) -> dict[str, Any]:
    at = getattr(row, "closed_at", None) or getattr(row, "created_at", None) or row.opened_at
    return _event(
        row=row,
        source_table=source_table,
        event_type="MARKET_LIFECYCLE",
        market_ticker=row.market_ticker,
        event_time=at,
        received_at=at,
        feature_snapshot_id=getattr(row, "feature_snapshot_id", None),
        payload={
            "status": getattr(row, "status", None),
            "position_id": getattr(row, "position_id", None),
            "intent_id": getattr(row, "intent_id", None),
            "reason": getattr(row, "resolution_reason", None) or getattr(row, "close_reason", None),
        },
    )


def _event(
    *,
    row: Any,
    source_table: str,
    event_type: str,
    market_ticker: str | None,
    event_time: datetime,
    received_at: datetime | None,
    payload: dict[str, Any],
    sequence_number: int | None = None,
    feature_snapshot_id: str | None = None,
    feature_schema_version: str | None = None,
    architecture_version: str | None = None,
    replay_readiness: str = "FULL",
    blockers: list[str] | None = None,
) -> dict[str, Any]:
    source_row_id = str(row.id)
    source_hash = _hash(payload)
    event_id = _identifier("replay-event", source_table, source_row_id, source_hash)
    event = {
        "event_id": event_id,
        "market_ticker": market_ticker,
        "event_type": event_type,
        "event_time": _utc(event_time),
        "received_at": _utc(received_at) if received_at else None,
        "source_table": source_table,
        "source_row_id": source_row_id,
        "source_hash": source_hash,
        "sequence_number": sequence_number,
        "feature_snapshot_id": feature_snapshot_id,
        "feature_schema_version": feature_schema_version,
        "architecture_version": architecture_version,
        "replay_schema_version": REPLAY_SCHEMA_VERSION,
        "payload": _json_safe(payload),
        "replay_readiness": replay_readiness,
        "blockers": list(dict.fromkeys(blockers or [])),
    }
    event["event_hash"] = _normalized_event_hash(event)
    return event


def _coverage(
    session: Session,
    *,
    data_cutoff: datetime,
    snapshot: ReplayEventSnapshot | None = None,
) -> dict[str, Any]:
    repository = ResearchRepository(session)
    frozen_snapshot = snapshot or repository.replay_event_snapshot()
    outcomes = repository.list_complete_outcomes()
    reader = repository.frozen_replay_event_reader(frozen_snapshot)
    event_count = 0
    by_type: dict[str, int] = {}
    readiness: dict[str, int] = {}
    per_market: dict[str, dict[str, Any]] = {}
    for page in reader.iter_pages():
        for event in page:
            event_count += 1
            by_type[event.event_type] = by_type.get(event.event_type, 0) + 1
            if event.event_type == "FEATURE_SNAPSHOT":
                readiness[event.replay_readiness] = readiness.get(event.replay_readiness, 0) + 1
            market_ticker = event.market_ticker
            event_type = event.event_type
            event_time = event.event_time
            if market_ticker is None:
                continue
            current = per_market.setdefault(
                market_ticker,
                {"event_count": 0, "event_counts_by_type": {}, "maximum_event_gap_seconds": 0},
            )
            current["event_count"] += 1
            counts = current["event_counts_by_type"]
            counts[str(event_type)] = int(counts.get(str(event_type), 0)) + 1
            previous = current.get("last_event_time")
            if previous is not None:
                current["maximum_event_gap_seconds"] = max(
                    int(current["maximum_event_gap_seconds"]),
                    max(0, int((_utc(event_time) - _utc(previous)).total_seconds())),
                )
            current["last_event_time"] = event_time
    if event_count != frozen_snapshot.event_count:
        raise RuntimeError(
            "Frozen coverage scan was incomplete: "
            f"expected {frozen_snapshot.event_count}, scanned {event_count}."
        )
    for values in per_market.values():
        values.pop("last_event_time", None)
        source_types = set(values["event_counts_by_type"])
        values["missing_source_count"] = len(
            {"MARKET", "ORDERBOOK", "FEATURE_SNAPSHOT"} - source_types
        )
        values["coverage_percentage"] = str(
            Decimal(values["event_count"]) / Decimal(max(event_count, 1))
        )
    return {
        "coverage_schema_version": "research_coverage_v1",
        "data_cutoff": _utc(data_cutoff).isoformat(),
        "complete_markets": len(outcomes),
        "event_count": int(event_count),
        "earliest_event_time": _utc(frozen_snapshot.min_event_time).isoformat()
        if frozen_snapshot.min_event_time
        else None,
        "latest_event_time": _utc(frozen_snapshot.max_event_time).isoformat()
        if frozen_snapshot.max_event_time
        else None,
        "unique_markets": len(per_market),
        "event_counts_by_type": {str(key): int(value) for key, value in by_type.items()},
        "complete_frame_count": int(readiness.get("FULL", 0)),
        "partial_frame_count": int(readiness.get("PARTIAL", 0)),
        "unusable_frame_count": int(readiness.get("UNUSABLE", 0)),
        "source_retention_limitations": [
            "archive_contains_only_source_rows_available_before_archive_retention"
        ],
        "replay_eligibility_blockers": [
            "partial_feature_vectors_excluded_from_executable_calibration"
        ],
        "minimum_coverage": min(
            (float(row.coverage_percentage or 0) for row in outcomes), default=0.0
        ),
        "missing_source_counts": {
            market: int(values["missing_source_count"])
            for market, values in sorted(per_market.items())
        },
        "per_market_coverage": per_market,
        "frozen_snapshot": {
            "watermark_id": frozen_snapshot.watermark_id,
            "total_events": frozen_snapshot.event_count,
            "events_scanned": reader.events_scanned,
            "pages_completed": reader.pages_scanned,
            "partitions_completed": reader.partitions_completed,
        },
    }


def _coverage_event(coverage: dict[str, Any], checked_at: datetime) -> dict[str, Any]:
    checksum = _hash(coverage)
    return {
        "event_id": _identifier("coverage", checksum),
        "market_ticker": None,
        "event_type": "COVERAGE_REPORT",
        "event_time": checked_at,
        "received_at": checked_at,
        "source_table": "research_coverage_reports",
        "source_row_id": checksum,
        "source_hash": checksum,
        "sequence_number": None,
        "feature_snapshot_id": None,
        "feature_schema_version": None,
        "architecture_version": None,
        "replay_schema_version": REPLAY_SCHEMA_VERSION,
        "payload": _json_safe(coverage),
        "event_hash": checksum,
        "replay_readiness": "FULL",
        "blockers": [],
    }


def _refresh_mature_labels(
    session: Session, repository: ResearchRepository
) -> LabelRefreshResult:
    label_schema = ResearchMarketOutcome.quality_flags["label_schema_version"].as_string()
    pending_filter = (
        ResearchMarketOutcome.outcome_status == "RESOLVED",
        or_(
            ResearchMarketOutcome.quality_flags.is_(None),
            label_schema.is_(None),
            label_schema != RESEARCH_LABEL_SCHEMA_VERSION,
        ),
    )
    remaining_total = int(
        session.scalar(
            select(func.count()).select_from(ResearchMarketOutcome).where(*pending_filter)
        )
        or 0
    )
    outcomes = list(
        session.scalars(
            select(ResearchMarketOutcome)
            .where(*pending_filter)
            .order_by(ResearchMarketOutcome.updated_at.asc(), ResearchMarketOutcome.id.asc())
            .limit(LABEL_MARKETS_PER_CYCLE)
        )
    )
    for outcome in outcomes:
        market = session.scalar(
            select(Market).where(Market.market_ticker == outcome.market_ticker)
        )
        if market is None:
            continue
        start = _utc(
            market.open_time
            or outcome.market_open_at
            or market.close_time
            or outcome.market_close_at
        )
        end = _utc(
            market.close_time
            or outcome.market_close_at
            or outcome.expiration_at
            or start
        ) + timedelta(seconds=LABEL_MAX_HORIZON_SECONDS)
        ticks = list(
            session.scalars(
                select(ReferenceTick)
                .where(
                    ReferenceTick.source == BRTI_SOURCE,
                    ReferenceTick.received_at >= start,
                    ReferenceTick.received_at <= end,
                )
                .order_by(ReferenceTick.received_at.asc(), ReferenceTick.id.asc())
            )
        )
        outcome.quality_flags = {
            **(outcome.quality_flags if isinstance(outcome.quality_flags, dict) else {}),
            **_labels_for_market(session, market, ticks, outcome, start=start, end=end),
        }
    session.flush()
    return LabelRefreshResult(
        processed_markets=len(outcomes),
        remaining_markets=max(0, remaining_total - len(outcomes)),
    )


def _labels_for_market(
    session: Session,
    market: Market,
    ticks: list[ReferenceTick],
    outcome: ResearchMarketOutcome | None = None,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
) -> dict[str, Any]:
    """Generate labels only after archival; replay never consumes future values."""
    book_filters = [OrderbookSnapshot.market_ticker == market.market_ticker]
    feature_filters = [StrategyFeatureSnapshot.market_ticker == market.market_ticker]
    if start is not None and end is not None:
        book_filters.extend(
            [
                OrderbookSnapshot.received_at >= start,
                OrderbookSnapshot.received_at <= end,
            ]
        )
        feature_filters.extend(
            [
                StrategyFeatureSnapshot.evaluated_at >= start,
                StrategyFeatureSnapshot.evaluated_at <= end,
            ]
        )
    books = list(
        session.scalars(
            select(OrderbookSnapshot)
            .where(*book_filters)
            .order_by(OrderbookSnapshot.received_at.asc(), OrderbookSnapshot.id.asc())
        )
    )
    labels: dict[str, Any] = {}
    for event in session.scalars(
        select(StrategyFeatureSnapshot)
        .where(*feature_filters)
        .order_by(StrategyFeatureSnapshot.evaluated_at.asc(), StrategyFeatureSnapshot.id.asc())
    ):
        vector = _feature_vector_for_snapshot(session, event) or {}
        side = vector.get("candidate_side") or event.candidate_side
        if (event.replay_readiness or "FULL") == "FULL" and side in {"YES", "NO"}:
            labels[event.feature_snapshot_id] = _counterfactual_label(
                event, books, ticks, market, outcome, feature_vector=vector
            )
    return {"label_schema_version": RESEARCH_LABEL_SCHEMA_VERSION, "counterfactual_labels": labels}


def _counterfactual_label(
    feature: StrategyFeatureSnapshot,
    books: list[OrderbookSnapshot],
    ticks: list[ReferenceTick],
    market: Market,
    outcome: ResearchMarketOutcome | None = None,
    *,
    feature_vector: dict[str, Any] | None = None,
) -> dict[str, Any]:
    at = _utc(feature.evaluated_at)
    vector = _hydrate_persisted_feature_vector(
        feature_vector if feature_vector is not None else feature.complete_feature_vector
    )
    side = vector.get("candidate_side") or feature.candidate_side
    effective_after = at + timedelta(milliseconds=500)
    expires_at = at + timedelta(milliseconds=2500)
    entry = next(
        (book for book in books if effective_after <= _utc(book.received_at) <= expires_at),
        None,
    )
    first_ask = _ask(entry, side) if entry else None
    entry_depth = _ask_depth(entry, side) if entry else None
    from ape.strategy.momentum_v2 import evaluate_momentum_v2_feature_vector

    try:
        evaluation = evaluate_momentum_v2_feature_vector(vector)
    except (KeyError, TypeError, ValueError):
        return _json_safe(
            {
                "entry_fillable": False,
                "entry_label_readiness": "UNAVAILABLE",
                "entry_label_blockers": ["feature_vector_incomplete"],
                "settlement_label_readiness": "UNAVAILABLE",
                "settlement_label_blockers": ["executable_label_not_available"],
            }
        )
    intended_limit = evaluation.intended_entry_price
    entry_price = (
        first_ask
        if first_ask is not None
        and entry_depth is not None
        and entry_depth >= Decimal("1")
        and intended_limit is not None
        and first_ask <= intended_limit
        else None
    )
    fee_model = verified_kalshi_taker_fee_model()
    values: dict[str, Any] = {
        "entry_fillable": entry_price is not None,
        "entry_at": entry.received_at if entry else None,
        "first_book_ask": first_ask,
        "first_book_ask_depth": entry_depth,
        "entry_price": entry_price,
        "entry_intended_limit": intended_limit,
        "entry_label_readiness": "FULL" if entry_price is not None else "UNAVAILABLE",
        "entry_label_blockers": [] if entry_price is not None else ["first_book_not_executable"],
        "fee_model": fee_model.metadata(),
    }
    for seconds in (5, 15, 30, 60):
        label_at = at + timedelta(seconds=seconds)
        tick = next(
            (
                row
                for row in ticks
                if label_at <= _utc(row.received_at) <= label_at + timedelta(seconds=5)
            ),
            None,
        )
        mark_book = next(
            (
                book
                for book in books
                if label_at <= _utc(book.received_at) <= label_at + timedelta(seconds=5)
            ),
            None,
        )
        mark = _bid(mark_book, side)
        values[f"brti_{seconds}s"] = tick.parsed_value if tick else None
        values[f"target_timestamp_{seconds}s"] = label_at
        values[f"selected_event_id_{seconds}s"] = mark_book.id if mark_book else None
        values[f"selected_event_timestamp_{seconds}s"] = (
            mark_book.received_at if mark_book else None
        )
        values[f"event_age_ms_{seconds}s"] = (
            int((_utc(mark_book.received_at) - label_at).total_seconds() * 1000)
            if mark_book
            else None
        )
        values[f"executable_bid_{seconds}s"] = mark
        values[f"depth_{seconds}s"] = _bid_depth(mark_book, side)
        values[f"gross_markout_{seconds}s_cents"] = (
            (mark - entry_price) * Decimal("100")
            if mark is not None and entry_price is not None
            else None
        )
        values[f"net_markout_{seconds}s_cents"] = (
            (mark - entry_price) * Decimal("100")
            - fee_model.fee_cents(price=entry_price)
            - fee_model.fee_cents(price=mark)
            if mark is not None and entry_price is not None
            else None
        )
        values[f"label_readiness_{seconds}s"] = "FULL" if mark is not None else "UNAVAILABLE"
        values[f"label_blockers_{seconds}s"] = [] if mark is not None else ["bounded_mark_missing"]
    held_bids = [
        (_bid(book, side), _utc(book.received_at))
        for book in books
        if entry is not None and _utc(book.received_at) >= _utc(entry.received_at)
    ]
    executable_bids = [(bid, bid_at) for bid, bid_at in held_bids if bid is not None]
    best_bid = max(executable_bids, default=(None, None), key=lambda item: item[0])
    worst_bid = min(executable_bids, default=(None, None), key=lambda item: item[0])
    values["mfe_cents"] = (
        (best_bid[0] - entry_price) * Decimal("100")
        if best_bid[0] is not None and entry_price is not None
        else None
    )
    values["mae_cents"] = (
        (worst_bid[0] - entry_price) * Decimal("100")
        if worst_bid[0] is not None and entry_price is not None
        else None
    )
    values["time_to_mfe_ms"] = (
        int((best_bid[1] - _utc(entry.received_at)).total_seconds() * 1000)
        if best_bid[1] is not None and entry is not None
        else None
    )
    values["time_to_mae_ms"] = (
        int((worst_bid[1] - _utc(entry.received_at)).total_seconds() * 1000)
        if worst_bid[1] is not None and entry is not None
        else None
    )
    close_at = _utc(market.close_time) if market.close_time else None
    final_minute_bids = [
        _bid(book, side)
        for book in books
        if close_at is not None
        and close_at - timedelta(seconds=60) <= _utc(book.received_at) <= close_at
        and _bid(book, side) is not None
    ]
    values["final_minute_mark"] = (
        sum(final_minute_bids, Decimal("0")) / len(final_minute_bids) if final_minute_bids else None
    )
    values["settlement_payout"] = (
        (Decimal("1") if outcome.result_side == side else Decimal("0"))
        if outcome is not None and outcome.result_side is not None
        else None
    )
    values["settlement_source"] = outcome.outcome_source if outcome is not None else None
    values["settlement_status"] = outcome.outcome_status if outcome is not None else None
    values["settlement_label_readiness"] = "FULL" if outcome is not None else "UNAVAILABLE"
    values["settlement_label_blockers"] = (
        [] if outcome is not None else ["official_outcome_missing"]
    )
    return _json_safe(values)


def _hydrate_persisted_feature_vector(value: Any) -> dict[str, Any]:
    """Restore JSON-encoded Decimal values before replaying archived features."""
    if not isinstance(value, dict):
        return {}
    return _hydrate_json_value(value)


def _hydrate_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _hydrate_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_hydrate_json_value(item) for item in value]
    if isinstance(value, str):
        try:
            return Decimal(value)
        except (ArithmeticError, ValueError):
            return value
    return value


def _ask(book: OrderbookSnapshot | None, side: str | None) -> Decimal | None:
    if book is None:
        return None
    value = book.yes_ask if side == "YES" else book.no_ask if side == "NO" else None
    return Decimal(value) if value is not None else None


def _ask_depth(book: OrderbookSnapshot | None, side: str | None) -> Decimal | None:
    if book is None:
        return None
    value = (
        _fixed_count_or_legacy_size(book.yes_ask_count, book.yes_ask_size)
        if side == "YES"
        else _fixed_count_or_legacy_size(book.no_ask_count, book.no_ask_size)
        if side == "NO"
        else None
    )
    return Decimal(value) if value is not None else None


def _bid(book: OrderbookSnapshot | None, side: str | None) -> Decimal | None:
    if book is None:
        return None
    value = book.yes_bid if side == "YES" else book.no_bid if side == "NO" else None
    return Decimal(value) if value is not None else None


def _bid_depth(book: OrderbookSnapshot | None, side: str | None) -> Decimal | None:
    if book is None:
        return None
    value = (
        _fixed_count_or_legacy_size(book.yes_bid_count, book.yes_bid_size)
        if side == "YES"
        else _fixed_count_or_legacy_size(book.no_bid_count, book.no_bid_size)
        if side == "NO"
        else None
    )
    return Decimal(value) if value is not None else None


def _top_ladder(value: Any) -> list[Any] | None:
    return list(value[:5]) if isinstance(value, list) else None


def _identifier(*parts: str) -> str:
    return "-".join((parts[0], _hash(parts[1:])[:24]))


def _hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(_json_safe(value), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _normalized_event_hash(event: dict[str, Any] | ResearchReplayEvent) -> str:
    if isinstance(event, dict):
        envelope = {field: event[field] for field in _REPLAY_EVENT_HASH_FIELDS}
    else:
        envelope = {field: getattr(event, field) for field in _REPLAY_EVENT_HASH_FIELDS}
    return _hash(envelope)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return _utc(value).isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
