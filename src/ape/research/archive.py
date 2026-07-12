from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from ape.db.models import (
    Market,
    OrderbookSnapshot,
    PublicTrade,
    ReferenceTick,
    ResearchReplayEvent,
    StrategyDecision,
    StrategyFeatureSnapshot,
    StrategyPositionOutcome,
    StrategyTradeIntent,
)
from ape.kalshi.reference_messages import BRTI_SOURCE
from ape.research import REPLAY_SCHEMA_VERSION, RESEARCH_LABEL_SCHEMA_VERSION
from ape.research.fees import verified_kalshi_taker_fee_model
from ape.research.repository import ResearchRepository

ARCHIVE_BATCH_SIZE = 5_000


@dataclass(frozen=True)
class ArchiveResult:
    archived_events: int
    archived_by_type: dict[str, int]
    outcomes_reconciled: int
    coverage: dict[str, Any]


def archive_research_events(session: Session, *, now: datetime | None = None) -> ArchiveResult:
    """Incrementally archive normalized, replayable source data without raw payload copies."""
    checked_at = _utc(now or datetime.now(UTC))
    repository = ResearchRepository(session)
    counts: dict[str, int] = {}
    for row in _unarchived_rows(session, repository, Market, "markets"):
        _archive(repository, _market_event(row), counts)
    for row in _unarchived_rows(session, repository, ReferenceTick, "reference_ticks"):
        _archive(repository, _reference_event(row), counts)
    for row in _unarchived_rows(session, repository, OrderbookSnapshot, "orderbook_snapshots"):
        _archive(repository, _orderbook_event(row), counts)
    for row in _unarchived_rows(session, repository, PublicTrade, "public_trades"):
        _archive(repository, _trade_event(row), counts)
    for row in _unarchived_rows(
        session, repository, StrategyFeatureSnapshot, "strategy_feature_snapshots"
    ):
        _archive(repository, _feature_event(session, row), counts)
    for row in _unarchived_rows(session, repository, StrategyTradeIntent, "strategy_trade_intents"):
        _archive(repository, _lifecycle_event(row, "strategy_trade_intents"), counts)
    for row in _unarchived_rows(
        session, repository, StrategyPositionOutcome, "strategy_position_outcomes"
    ):
        _archive(repository, _lifecycle_event(row, "strategy_position_outcomes"), counts)
    reconciled = reconcile_market_outcomes(session, now=checked_at)
    coverage = _coverage(session)
    return ArchiveResult(sum(counts.values()), counts, reconciled, coverage)


def _unarchived_rows(session: Session, repository: ResearchRepository, model, source_table: str):
    last_id = repository.latest_archived_source_row_id(source_table)
    statement = select(model).order_by(model.id.asc()).limit(ARCHIVE_BATCH_SIZE)
    if last_id is not None:
        statement = statement.where(model.id > last_id)
    return session.scalars(statement)


def reconcile_market_outcomes(session: Session, *, now: datetime | None = None) -> int:
    """Public-data-only market outcome reconciliation owned by the market-data service."""
    checked_at = _utc(now or datetime.now(UTC))
    repository = ResearchRepository(session)
    changed = 0
    for market in session.scalars(select(Market).order_by(Market.id.asc())):
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
        resolved = (
            expiration <= checked_at
            and final_tick is not None
            and final_tick.parsed_value is not None
            and boundary is not None
        )
        status = (
            "RESOLVED"
            if resolved
            else "UNAVAILABLE"
            if expiration + timedelta(hours=1) < checked_at
            else "PENDING"
        )
        final_value = (
            Decimal(final_tick.parsed_value)
            if final_tick and final_tick.parsed_value is not None
            else None
        )
        final_minute = [
            Decimal(tick.parsed_value)
            for tick in ticks
            if tick.parsed_value is not None
            and _utc(tick.received_at) >= close_at - timedelta(seconds=60)
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
                "result_side": "YES"
                if resolved and final_value >= Decimal(boundary)
                else "NO"
                if resolved
                else None,
                "settlement_value": final_value if resolved else None,
                "final_reference_value": final_value,
                "final_minute_reference_average": sum(final_minute, Decimal("0"))
                / len(final_minute)
                if final_minute
                else None,
                "outcome_status": status,
                "outcome_source": "public_reference_ticks",
                "source_payload_hash": _hash(
                    {"market": market.market_ticker, "tick": getattr(final_tick, "id", None)}
                ),
                "resolved_at": checked_at if resolved else None,
                "expected_frame_count": expected,
                "actual_frame_count": actual,
                "coverage_percentage": Decimal(actual) / Decimal(expected),
                "maximum_event_gap_seconds": max(gaps, default=None),
                "quality_flags": _labels_for_market(session, market, ticks),
            }
        )
        changed += 1
    return changed


def _archive(repository: ResearchRepository, event: dict[str, Any], counts: dict[str, int]) -> None:
    existing = repository.get_event_by_source(
        source_table=event["source_table"], source_row_id=event["source_row_id"]
    )
    if existing is None:
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


def _reference_event(row: ReferenceTick) -> dict[str, Any]:
    return _event(
        row=row,
        source_table="reference_ticks",
        event_type="REFERENCE",
        market_ticker=None,
        event_time=row.source_ts or row.received_at,
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
            "yes_bid_size": row.yes_bid_count or row.yes_bid_size,
            "yes_ask_size": row.yes_ask_count or row.yes_ask_size,
            "no_bid_size": row.no_bid_count or row.no_bid_size,
            "no_ask_size": row.no_ask_count or row.no_ask_size,
            "yes_bid_ladder": _top_ladder(row.yes_bid_ladder),
            "yes_ask_ladder": _top_ladder(row.yes_ask_ladder),
            "no_bid_ladder": _top_ladder(row.no_bid_ladder),
            "no_ask_ladder": _top_ladder(row.no_ask_ladder),
            "book_status": row.book_status,
        },
    )


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
    vector = row.complete_feature_vector
    readiness = row.replay_readiness or "FULL"
    blockers = list(row.replay_blockers or [])
    if not vector and row.feature_schema_version == "momentum_v2_features_v2":
        decision = session.scalar(
            select(StrategyDecision)
            .where(StrategyDecision.feature_snapshot_id == row.feature_snapshot_id)
            .order_by(desc(StrategyDecision.id))
            .limit(1)
        )
        measurements = decision.measurements if decision else None
        vector = measurements.get("features") if isinstance(measurements, dict) else None
        if vector is None:
            readiness = "PARTIAL"
            blockers.append("v2_feature_vector_unrecoverable")
    if not vector:
        vector = {}
        readiness = "PARTIAL"
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
    return {
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
        "event_hash": _hash({"event": event_id, "payload": payload}),
        "replay_readiness": replay_readiness,
        "blockers": list(dict.fromkeys(blockers or [])),
    }


def _coverage(session: Session) -> dict[str, Any]:
    repository = ResearchRepository(session)
    outcomes = repository.list_complete_outcomes()
    event_count = session.scalar(select(func.count()).select_from(ResearchReplayEvent)) or 0
    by_type = dict(
        session.execute(
            select(ResearchReplayEvent.event_type, func.count()).group_by(
                ResearchReplayEvent.event_type
            )
        ).all()
    )
    earliest, latest = session.execute(
        select(func.min(ResearchReplayEvent.event_time), func.max(ResearchReplayEvent.event_time))
    ).one()
    readiness = dict(
        session.execute(
            select(ResearchReplayEvent.replay_readiness, func.count()).group_by(
                ResearchReplayEvent.replay_readiness
            )
        ).all()
    )
    return {
        "complete_markets": len(outcomes),
        "event_count": int(event_count),
        "earliest_event_time": _utc(earliest).isoformat() if earliest else None,
        "latest_event_time": _utc(latest).isoformat() if latest else None,
        "unique_markets": session.scalar(
            select(func.count(func.distinct(ResearchReplayEvent.market_ticker))).where(
                ResearchReplayEvent.market_ticker.is_not(None)
            )
        )
        or 0,
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
    }


def _labels_for_market(
    session: Session, market: Market, ticks: list[ReferenceTick]
) -> dict[str, Any]:
    """Generate labels only after archival; replay never consumes future values."""
    books = list(
        session.scalars(
            select(OrderbookSnapshot)
            .where(OrderbookSnapshot.market_ticker == market.market_ticker)
            .order_by(OrderbookSnapshot.received_at.asc(), OrderbookSnapshot.id.asc())
        )
    )
    labels: dict[str, Any] = {}
    for event in session.scalars(
        select(StrategyFeatureSnapshot)
        .where(StrategyFeatureSnapshot.market_ticker == market.market_ticker)
        .order_by(StrategyFeatureSnapshot.evaluated_at.asc(), StrategyFeatureSnapshot.id.asc())
    ):
        vector = event.complete_feature_vector or {}
        side = vector.get("candidate_side") or event.candidate_side
        if side in {"YES", "NO"}:
            labels[event.feature_snapshot_id] = _counterfactual_label(event, books, ticks, market)
    return {"label_schema_version": RESEARCH_LABEL_SCHEMA_VERSION, "counterfactual_labels": labels}


def _counterfactual_label(
    feature: StrategyFeatureSnapshot,
    books: list[OrderbookSnapshot],
    ticks: list[ReferenceTick],
    market: Market,
) -> dict[str, Any]:
    at = _utc(feature.evaluated_at)
    vector = feature.complete_feature_vector or {}
    side = vector.get("candidate_side") or feature.candidate_side
    effective_after = at + timedelta(milliseconds=500)
    expires_at = at + timedelta(milliseconds=2500)
    entry = next(
        (book for book in books if effective_after <= _utc(book.received_at) <= expires_at),
        None,
    )
    first_ask = _ask(entry, side) if entry else None
    entry_depth = _ask_depth(entry, side) if entry else None
    entry_price = (
        first_ask
        if first_ask is not None
        and entry_depth is not None
        and entry_depth >= Decimal("1")
        and first_ask <= Decimal("0.78")
        else None
    )
    fee_model = verified_kalshi_taker_fee_model()
    values: dict[str, Any] = {
        "entry_fillable": entry_price is not None,
        "entry_at": entry.received_at if entry else None,
        "first_book_ask": first_ask,
        "first_book_ask_depth": entry_depth,
        "entry_price": entry_price,
        "fee_model": fee_model.metadata(),
    }
    for seconds in (5, 15, 30, 60):
        label_at = at + timedelta(seconds=seconds)
        tick = next((row for row in ticks if _utc(row.received_at) >= label_at), None)
        mark_book = next((book for book in books if _utc(book.received_at) >= label_at), None)
        mark = _bid(mark_book, side)
        values[f"brti_{seconds}s"] = tick.parsed_value if tick else None
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
    final_tick = ticks[-1] if ticks else None
    boundary = vector.get("boundary") or feature.boundary
    values["final_minute_mark"] = (
        sum(final_minute_bids, Decimal("0")) / len(final_minute_bids) if final_minute_bids else None
    )
    values["settlement_payout"] = (
        Decimal("1")
        if final_tick is not None
        and final_tick.parsed_value is not None
        and boundary is not None
        and (
            (side == "YES" and Decimal(final_tick.parsed_value) >= Decimal(str(boundary)))
            or (side == "NO" and Decimal(final_tick.parsed_value) < Decimal(str(boundary)))
        )
        else Decimal("0")
        if final_tick is not None and final_tick.parsed_value is not None and boundary is not None
        else None
    )
    return _json_safe(values)


def _ask(book: OrderbookSnapshot | None, side: str | None) -> Decimal | None:
    if book is None:
        return None
    value = book.yes_ask if side == "YES" else book.no_ask if side == "NO" else None
    return Decimal(value) if value is not None else None


def _ask_depth(book: OrderbookSnapshot | None, side: str | None) -> Decimal | None:
    if book is None:
        return None
    value = (
        book.yes_ask_count or book.yes_ask_size
        if side == "YES"
        else book.no_ask_count or book.no_ask_size
        if side == "NO"
        else None
    )
    return Decimal(value) if value is not None else None


def _bid(book: OrderbookSnapshot | None, side: str | None) -> Decimal | None:
    if book is None:
        return None
    value = book.yes_bid if side == "YES" else book.no_bid if side == "NO" else None
    return Decimal(value) if value is not None else None


def _top_ladder(value: Any) -> list[Any] | None:
    return list(value[:5]) if isinstance(value, list) else None


def _identifier(*parts: str) -> str:
    return "-".join((parts[0], _hash(parts[1:])[:24]))


def _hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(_json_safe(value), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


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
