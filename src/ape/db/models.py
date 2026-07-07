from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utc_now() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class SchemaMigration(Base):
    __tablename__ = "schema_migrations"

    version: Mapped[str] = mapped_column(String(64), primary_key=True)
    applied_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )


class Market(Base):
    __tablename__ = "markets"
    __table_args__ = (UniqueConstraint("market_ticker", name="uq_markets_market_ticker"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market_ticker: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    event_ticker: Mapped[str | None] = mapped_column(String(128))
    series_ticker: Mapped[str | None] = mapped_column(String(128))
    title: Mapped[str | None] = mapped_column(Text)
    subtitle: Mapped[str | None] = mapped_column(Text)
    yes_sub_title: Mapped[str | None] = mapped_column(Text)
    no_sub_title: Mapped[str | None] = mapped_column(Text)
    open_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    close_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expected_expiration_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expiration_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    latest_expiration_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    settlement_timer_seconds: Mapped[int | None] = mapped_column(Integer)
    rules_primary: Mapped[str | None] = mapped_column(Text)
    rules_secondary: Mapped[str | None] = mapped_column(Text)
    functional_strike: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    floor_strike: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    cap_strike: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    custom_strike: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    price_level_structure: Mapped[Any | None] = mapped_column(JSON)
    price_ranges: Mapped[Any | None] = mapped_column(JSON)
    liquidity_dollars: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    raw_payload_hash: Mapped[str | None] = mapped_column(String(128))
    parser_version: Mapped[str | None] = mapped_column(String(64))
    resolver_decision_reason: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )


class ReferenceTick(Base):
    __tablename__ = "reference_ticks"
    __table_args__ = (Index("ix_reference_ticks_source_received_at", "source", "received_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
    )
    source_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    kalshi_received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    raw_value: Mapped[str | None] = mapped_column(Text)
    parsed_value: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    trailing_60s_avg: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    trailing_60s_window_size: Mapped[int | None] = mapped_column(Integer)
    last_60s_windowed_average_15min: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    final_minute_average_window_size: Mapped[int | None] = mapped_column(Integer)
    final_minute_average_status: Mapped[str | None] = mapped_column(String(64))
    sequence_number: Mapped[int | None] = mapped_column(Integer)
    subscription_id: Mapped[str | None] = mapped_column(String(128))
    source_age_ms: Mapped[int | None] = mapped_column(Integer)
    parse_status: Mapped[str] = mapped_column(String(64), nullable=False)
    raw_payload_hash: Mapped[str | None] = mapped_column(String(128))
    raw_payload: Mapped[Any | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )


class OrderbookSnapshot(Base):
    __tablename__ = "orderbook_snapshots"
    __table_args__ = (
        Index("ix_orderbook_snapshots_market_received", "market_ticker", "received_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market_ticker: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
    )
    sequence_number: Mapped[int | None] = mapped_column(Integer)
    yes_bid: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    yes_ask: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    no_bid: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    no_ask: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    yes_spread: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    no_spread: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    yes_bid_size: Mapped[int | None] = mapped_column(Integer)
    yes_ask_size: Mapped[int | None] = mapped_column(Integer)
    no_bid_size: Mapped[int | None] = mapped_column(Integer)
    no_ask_size: Mapped[int | None] = mapped_column(Integer)
    yes_bid_count: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    yes_ask_count: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    no_bid_count: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    no_ask_count: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    book_status: Mapped[str | None] = mapped_column(String(64))
    raw_payload_hash: Mapped[str | None] = mapped_column(String(128))
    raw_payload: Mapped[Any | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )


class PublicTrade(Base):
    __tablename__ = "public_trades"
    __table_args__ = (
        Index("ix_public_trades_market_executed", "market_ticker", "executed_at"),
        Index("ix_public_trades_market_received", "market_ticker", "received_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market_ticker: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    trade_id: Mapped[str | None] = mapped_column(String(128))
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
    )
    executed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    price: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    count: Mapped[int | None] = mapped_column(Integer)
    trade_count: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    taker_side: Mapped[str | None] = mapped_column(String(32))
    side_inferred: Mapped[str | None] = mapped_column(String(32))
    raw_payload_hash: Mapped[str | None] = mapped_column(String(128))
    raw_payload: Mapped[Any | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )


class StrategyDecision(Base):
    __tablename__ = "strategy_decisions"
    __table_args__ = (
        UniqueConstraint("decision_id", name="uq_strategy_decisions_decision_id"),
        Index("ix_strategy_decisions_state_evaluated", "decision_state", "evaluated_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    decision_id: Mapped[str] = mapped_column(String(128), nullable=False)
    market_ticker: Mapped[str | None] = mapped_column(String(128), index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    evaluated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
    )
    decision_state: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    primary_reason: Mapped[str] = mapped_column(Text, nullable=False)
    app_mode: Mapped[str] = mapped_column(String(32), nullable=False)
    candidate_side: Mapped[str | None] = mapped_column(String(32))
    boundary: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    brti_value: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    distance_bps: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    seconds_left: Mapped[int | None] = mapped_column(Integer)
    measurements: Mapped[Any | None] = mapped_column(JSON)
    blockers: Mapped[Any | None] = mapped_column(JSON)
    warnings: Mapped[Any | None] = mapped_column(JSON)
    raw_context_hash: Mapped[str | None] = mapped_column(String(128))


class StrategyDryRunPosition(Base):
    __tablename__ = "strategy_dry_run_positions"
    __table_args__ = (
        UniqueConstraint(
            "position_id",
            name="uq_strategy_dry_run_positions_position_id",
        ),
        Index(
            "ix_strategy_dry_run_positions_status_opened",
            "status",
            "opened_at",
        ),
        Index(
            "ix_strategy_dry_run_positions_market_status",
            "market_ticker",
            "status",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    position_id: Mapped[str] = mapped_column(String(128), nullable=False)
    strategy_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    market_ticker: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    decision_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    side_candidate: Mapped[str] = mapped_column(String(32), nullable=False)
    economic_side: Mapped[str] = mapped_column(String(32), nullable=False)
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    open_price: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    contract_count: Mapped[int] = mapped_column(Integer, nullable=False)
    boundary: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    brti_at_entry: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    distance_bps_at_entry: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    entry_reason: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    close_price: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    close_reason: Mapped[str | None] = mapped_column(Text)
    realized_pnl_cents: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    measurements: Mapped[Any | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )


class StrategyDryRunEvent(Base):
    __tablename__ = "strategy_dry_run_events"
    __table_args__ = (
        UniqueConstraint("event_id", name="uq_strategy_dry_run_events_event_id"),
        Index("ix_strategy_dry_run_events_occurred_at", "occurred_at"),
        Index("ix_strategy_dry_run_events_market_occurred", "market_ticker", "occurred_at"),
        Index("ix_strategy_dry_run_events_type_occurred", "event_type", "occurred_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    strategy_id: Mapped[str | None] = mapped_column(String(128), index=True)
    position_id: Mapped[str | None] = mapped_column(String(128), index=True)
    decision_id: Mapped[str | None] = mapped_column(String(128), index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    market_ticker: Mapped[str | None] = mapped_column(String(128), index=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    side_candidate: Mapped[str | None] = mapped_column(String(32))
    price: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    contract_count: Mapped[int | None] = mapped_column(Integer)
    reason: Mapped[str | None] = mapped_column(Text)
    measurements: Mapped[Any | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )


class WorkerHeartbeat(Base):
    __tablename__ = "worker_heartbeats"
    __table_args__ = (
        Index("ix_worker_heartbeats_service_heartbeat", "service_name", "heartbeat_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    service_name: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    heartbeat_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
    )
    app_mode: Mapped[str] = mapped_column(String(32), nullable=False)
    is_safe: Mapped[bool] = mapped_column(Boolean, nullable=False)
    metadata_: Mapped[Any | None] = mapped_column("metadata", JSON)


class StorageRetentionRun(Base):
    __tablename__ = "storage_retention_runs"
    __table_args__ = (
        UniqueConstraint("run_id", name="uq_storage_retention_runs_run_id"),
        Index("ix_storage_retention_runs_started_at", "started_at"),
        Index("ix_storage_retention_runs_status_started", "status", "started_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(128), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    dry_run: Mapped[bool] = mapped_column(Boolean, nullable=False)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    deleted_rows: Mapped[Any | None] = mapped_column(JSON)
    raw_payload_stripped_rows: Mapped[Any | None] = mapped_column(JSON)
    table_row_counts_before: Mapped[Any | None] = mapped_column(JSON)
    table_row_counts_after: Mapped[Any | None] = mapped_column(JSON)
    table_sizes_before: Mapped[Any | None] = mapped_column(JSON)
    table_sizes_after: Mapped[Any | None] = mapped_column(JSON)
    warnings: Mapped[Any | None] = mapped_column(JSON)
    blockers: Mapped[Any | None] = mapped_column(JSON)
    error_type: Mapped[str | None] = mapped_column(String(128))
    error_message: Mapped[str | None] = mapped_column(Text)
