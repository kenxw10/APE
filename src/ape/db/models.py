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
    ladder_schema_version: Mapped[str | None] = mapped_column(String(64))
    yes_bid_ladder: Mapped[Any | None] = mapped_column(JSON)
    yes_ask_ladder: Mapped[Any | None] = mapped_column(JSON)
    no_bid_ladder: Mapped[Any | None] = mapped_column(JSON)
    no_ask_ladder: Mapped[Any | None] = mapped_column(JSON)
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


class KalshiWsProtocolEvent(Base):
    __tablename__ = "kalshi_ws_protocol_events"
    __table_args__ = (
        Index("ix_kalshi_ws_protocol_events_created", "created_at"),
        Index(
            "ix_kalshi_ws_protocol_events_worker_created",
            "worker_service",
            "created_at",
        ),
        Index(
            "ix_kalshi_ws_protocol_events_type_created",
            "event_type",
            "created_at",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
        index=True,
    )
    worker_service: Mapped[str | None] = mapped_column(String(128), index=True)
    worker_role: Mapped[str | None] = mapped_column(String(64), index=True)
    connection_id: Mapped[str | None] = mapped_column(String(128), index=True)
    channel: Mapped[str | None] = mapped_column(String(64), index=True)
    active_market_ticker: Mapped[str | None] = mapped_column(String(128), index=True)
    command_id: Mapped[int | None] = mapped_column(Integer, index=True)
    command_type: Mapped[str | None] = mapped_column(String(64))
    command_action: Mapped[str | None] = mapped_column(String(64))
    sid: Mapped[int | None] = mapped_column(Integer, index=True)
    expected_sid: Mapped[int | None] = mapped_column(Integer)
    seq: Mapped[int | None] = mapped_column(Integer)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    event_subtype: Mapped[str | None] = mapped_column(String(128))
    raw_code: Mapped[str | None] = mapped_column(String(128))
    raw_message: Mapped[str | None] = mapped_column(Text)
    close_code: Mapped[int | None] = mapped_column(Integer)
    close_reason: Mapped[str | None] = mapped_column(Text)
    exception_type: Mapped[str | None] = mapped_column(String(128))
    exception_message: Mapped[str | None] = mapped_column(Text)
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    round_trip_ms: Mapped[int | None] = mapped_column(Integer)
    ping_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    pong_received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    server_ping_received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    client_pong_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    subscription_state_before: Mapped[str | None] = mapped_column(String(128))
    subscription_state_after: Mapped[str | None] = mapped_column(String(128))
    recovery_action: Mapped[str | None] = mapped_column(String(128))
    recovery_result: Mapped[str | None] = mapped_column(String(128))
    raw_payload_hash: Mapped[str | None] = mapped_column(String(128))
    payload_summary_json: Mapped[Any | None] = mapped_column(JSON)


class StrategyDecision(Base):
    __tablename__ = "strategy_decisions"
    __table_args__ = (
        UniqueConstraint("decision_id", name="uq_strategy_decisions_decision_id"),
        Index("ix_strategy_decisions_state_evaluated", "decision_state", "evaluated_at"),
        Index("ix_strategy_decisions_strategy_id_evaluated", "strategy_id", "evaluated_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    decision_id: Mapped[str] = mapped_column(String(128), nullable=False)
    strategy_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
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
    feature_snapshot_id: Mapped[str | None] = mapped_column(String(128), index=True)
    strategy_config_version_id: Mapped[str | None] = mapped_column(String(128), index=True)
    code_commit_sha: Mapped[str | None] = mapped_column(String(128))
    calibration_run_id: Mapped[str | None] = mapped_column(String(128))
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
    feature_snapshot_id: Mapped[str | None] = mapped_column(String(128), index=True)
    strategy_config_version_id: Mapped[str | None] = mapped_column(String(128), index=True)
    code_commit_sha: Mapped[str | None] = mapped_column(String(128))
    entry_intent_id: Mapped[str | None] = mapped_column(String(128), index=True)
    exit_intent_id: Mapped[str | None] = mapped_column(String(128), index=True)
    lifecycle_version: Mapped[str | None] = mapped_column(String(128))
    entry_timing_tier: Mapped[str | None] = mapped_column(String(64))
    entry_score_threshold: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    entry_time_stop_seconds: Mapped[int | None] = mapped_column(Integer)
    entry_max_hold_seconds: Mapped[int | None] = mapped_column(Integer)
    entry_score: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    entry_edge_lower_bound_cents: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    entry_response_residual_cents: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    entry_boundary: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    entry_standardized_distance: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
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
    feature_snapshot_id: Mapped[str | None] = mapped_column(String(128), index=True)
    strategy_config_version_id: Mapped[str | None] = mapped_column(String(128), index=True)
    code_commit_sha: Mapped[str | None] = mapped_column(String(128))
    measurements: Mapped[Any | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )


class StrategyFeatureSnapshot(Base):
    __tablename__ = "strategy_feature_snapshots"
    __table_args__ = (
        UniqueConstraint("feature_snapshot_id", name="uq_strategy_feature_snapshots_id"),
        Index(
            "ix_strategy_feature_snapshots_market_evaluated",
            "market_ticker",
            "evaluated_at",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    feature_snapshot_id: Mapped[str] = mapped_column(String(128), nullable=False)
    market_ticker: Mapped[str | None] = mapped_column(String(128), index=True)
    evaluated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    feature_schema_version: Mapped[str] = mapped_column(String(64), nullable=False)
    candidate_side: Mapped[str | None] = mapped_column(String(32))
    candidate_mode: Mapped[str | None] = mapped_column(String(64))
    boundary: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    current_brti: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    seconds_since_open: Mapped[int | None] = mapped_column(Integer)
    seconds_left: Mapped[int | None] = mapped_column(Integer)
    reference_tick_id: Mapped[int | None] = mapped_column(Integer)
    orderbook_snapshot_id: Mapped[int | None] = mapped_column(Integer)
    public_trade_id: Mapped[int | None] = mapped_column(Integer)
    context_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    quality_state: Mapped[Any | None] = mapped_column(JSON)
    reference_features: Mapped[Any | None] = mapped_column(JSON)
    contract_features: Mapped[Any | None] = mapped_column(JSON)
    microstructure_features: Mapped[Any | None] = mapped_column(JSON)
    execution_features: Mapped[Any | None] = mapped_column(JSON)
    complete_feature_vector: Mapped[Any | None] = mapped_column(JSON)
    feature_vector_hash: Mapped[str | None] = mapped_column(String(128), index=True)
    architecture_version: Mapped[str | None] = mapped_column(String(128))
    replay_schema_version: Mapped[str | None] = mapped_column(String(128))
    replay_readiness: Mapped[str | None] = mapped_column(String(32))
    replay_blockers: Mapped[Any | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )


class StrategyConfigVersion(Base):
    __tablename__ = "strategy_config_versions"
    __table_args__ = (
        UniqueConstraint("strategy_config_version_id", name="uq_strategy_config_versions_id"),
        Index("ix_strategy_config_versions_strategy_created", "strategy_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_config_version_id: Mapped[str] = mapped_column(String(128), nullable=False)
    strategy_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    architecture_version: Mapped[str] = mapped_column(String(128), nullable=False)
    feature_schema_version: Mapped[str] = mapped_column(String(128), nullable=False)
    parameter_snapshot: Mapped[Any] = mapped_column(JSON, nullable=False)
    parameter_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    code_commit_sha: Mapped[str] = mapped_column(String(128), nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    parent_config_version_id: Mapped[str | None] = mapped_column(String(128), index=True)
    calibration_run_id: Mapped[str | None] = mapped_column(String(128), index=True)
    lifecycle_state: Mapped[str | None] = mapped_column(String(64), index=True)
    approval_state: Mapped[str | None] = mapped_column(String(64))
    model_type: Mapped[str | None] = mapped_column(String(64))
    model_artifact_checksum: Mapped[str | None] = mapped_column(String(128))
    data_cutoff: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    candidate_id: Mapped[str | None] = mapped_column(String(128), index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )


class StrategyTradeIntent(Base):
    __tablename__ = "strategy_trade_intents"
    __table_args__ = (
        UniqueConstraint("intent_id", name="uq_strategy_trade_intents_id"),
        Index(
            "ix_strategy_trade_intents_strategy_market_created",
            "strategy_id",
            "market_ticker",
            "created_at",
        ),
        Index("ix_strategy_trade_intents_status_effective", "status", "effective_after"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    intent_id: Mapped[str] = mapped_column(String(128), nullable=False)
    strategy_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    strategy_config_version_id: Mapped[str | None] = mapped_column(String(128), index=True)
    feature_snapshot_id: Mapped[str | None] = mapped_column(String(128), index=True)
    decision_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    position_id: Mapped[str | None] = mapped_column(String(128), index=True)
    market_ticker: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    side_candidate: Mapped[str] = mapped_column(String(32), nullable=False)
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    effective_after: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    intended_limit_price: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    optimistic_price: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    optimistic_snapshot_id: Mapped[int | None] = mapped_column(Integer)
    architecture_version: Mapped[str | None] = mapped_column(String(128))
    code_commit_sha: Mapped[str | None] = mapped_column(String(128))
    lifecycle_version: Mapped[str | None] = mapped_column(String(128))
    trigger: Mapped[str | None] = mapped_column(String(128))
    trigger_classification: Mapped[str | None] = mapped_column(String(32))
    attempt_number: Mapped[int | None] = mapped_column(Integer)
    decision_time_executable_bid: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    fill_snapshot_id: Mapped[int | None] = mapped_column(Integer)
    simulated_fill_price: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    simulated_fill_size: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    fill_timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    resolution_reason: Mapped[str | None] = mapped_column(Text)
    measurements: Mapped[Any | None] = mapped_column(JSON)


class StrategyPositionMark(Base):
    __tablename__ = "strategy_position_marks"
    __table_args__ = (
        UniqueConstraint("mark_id", name="uq_strategy_position_marks_id"),
        Index("ix_strategy_position_marks_position_marked", "position_id", "marked_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    mark_id: Mapped[str] = mapped_column(String(128), nullable=False)
    strategy_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    position_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    market_ticker: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    marked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    feature_snapshot_id: Mapped[str | None] = mapped_column(String(128), index=True)
    strategy_config_version_id: Mapped[str | None] = mapped_column(String(128), index=True)
    executable_bid: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    score: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    edge_lower_bound_cents: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    boundary_state: Mapped[Any | None] = mapped_column(JSON)
    management_reason: Mapped[str | None] = mapped_column(Text)
    measurements: Mapped[Any | None] = mapped_column(JSON)


class StrategyPositionOutcome(Base):
    __tablename__ = "strategy_position_outcomes"
    __table_args__ = (
        UniqueConstraint("outcome_id", name="uq_strategy_position_outcomes_outcome_id"),
        UniqueConstraint("position_id", name="uq_strategy_position_outcomes_position_id"),
        Index("ix_strategy_position_outcomes_strategy_closed", "strategy_id", "closed_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    outcome_id: Mapped[str] = mapped_column(String(128), nullable=False)
    position_id: Mapped[str] = mapped_column(String(128), nullable=False)
    strategy_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    market_ticker: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    held_side: Mapped[str] = mapped_column(String(32), nullable=False)
    lifecycle_version: Mapped[str] = mapped_column(String(128), nullable=False)
    architecture_version: Mapped[str | None] = mapped_column(String(128))
    strategy_config_version_id: Mapped[str | None] = mapped_column(String(128), index=True)
    code_commit_sha: Mapped[str | None] = mapped_column(String(128))
    entry_decision_id: Mapped[str | None] = mapped_column(String(128), index=True)
    exit_decision_id: Mapped[str | None] = mapped_column(String(128), index=True)
    entry_intent_id: Mapped[str | None] = mapped_column(String(128), index=True)
    exit_intent_id: Mapped[str | None] = mapped_column(String(128), index=True)
    entry_feature_snapshot_id: Mapped[str | None] = mapped_column(String(128), index=True)
    exit_feature_snapshot_id: Mapped[str | None] = mapped_column(String(128), index=True)
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    closed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    holding_duration_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    entry_price: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    exit_price: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    realized_pnl_cents: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    mfe_cents: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    mae_cents: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    time_to_mfe_ms: Mapped[int | None] = mapped_column(Integer)
    time_to_mae_ms: Mapped[int | None] = mapped_column(Integer)
    optimistic_entry_delta: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    decision_to_filled_exit_delta: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    close_trigger: Mapped[str | None] = mapped_column(String(128))
    close_reason: Mapped[str | None] = mapped_column(Text)
    measurements: Mapped[Any | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
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


class ResearchReplayEvent(Base):
    __tablename__ = "research_replay_events"
    __table_args__ = (
        UniqueConstraint("event_id", name="uq_research_replay_events_event_id"),
        UniqueConstraint("source_table", "source_row_id", name="uq_research_replay_events_source"),
        Index("ix_research_replay_events_market_time", "market_ticker", "event_time"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    market_ticker: Mapped[str | None] = mapped_column(String(128), index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    event_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    source_table: Mapped[str] = mapped_column(String(128), nullable=False)
    source_row_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_hash: Mapped[str | None] = mapped_column(String(128))
    sequence_number: Mapped[int | None] = mapped_column(Integer)
    feature_snapshot_id: Mapped[str | None] = mapped_column(String(128), index=True)
    feature_schema_version: Mapped[str | None] = mapped_column(String(128))
    architecture_version: Mapped[str | None] = mapped_column(String(128))
    replay_schema_version: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[Any] = mapped_column(JSON, nullable=False)
    event_hash: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    replay_readiness: Mapped[str] = mapped_column(String(32), nullable=False)
    blockers: Mapped[Any | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )


class ResearchMarketOutcome(Base):
    __tablename__ = "research_market_outcomes"
    __table_args__ = (
        UniqueConstraint("outcome_id", name="uq_research_market_outcomes_outcome_id"),
        UniqueConstraint("market_ticker", name="uq_research_market_outcomes_market"),
        Index("ix_research_market_outcomes_status_resolved", "outcome_status", "resolved_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    outcome_id: Mapped[str] = mapped_column(String(128), nullable=False)
    market_ticker: Mapped[str] = mapped_column(String(128), nullable=False)
    market_open_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    market_close_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expiration_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    boundary: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    result_side: Mapped[str | None] = mapped_column(String(32))
    settlement_value: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    final_reference_value: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    final_minute_reference_average: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    outcome_status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    outcome_source: Mapped[str | None] = mapped_column(String(128))
    source_payload_hash: Mapped[str | None] = mapped_column(String(128))
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expected_frame_count: Mapped[int | None] = mapped_column(Integer)
    actual_frame_count: Mapped[int | None] = mapped_column(Integer)
    coverage_percentage: Mapped[Decimal | None] = mapped_column(Numeric(12, 8))
    maximum_event_gap_seconds: Mapped[int | None] = mapped_column(Integer)
    quality_flags: Mapped[Any | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )


class ResearchReplayRun(Base):
    __tablename__ = "research_replay_runs"
    __table_args__ = (
        UniqueConstraint("replay_run_id", name="uq_research_replay_runs_id"),
        Index("ix_research_replay_runs_dataset_started", "dataset_hash", "started_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    replay_run_id: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    replay_engine_version: Mapped[str] = mapped_column(String(128), nullable=False)
    label_schema_version: Mapped[str] = mapped_column(String(128), nullable=False)
    code_commit_sha: Mapped[str] = mapped_column(String(128), nullable=False)
    baseline_strategy_config_version_id: Mapped[str | None] = mapped_column(String(128), index=True)
    dataset_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    data_cutoff: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    start_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    end_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    unique_market_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    event_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    partition_manifest: Mapped[Any | None] = mapped_column(JSON)
    cost_model: Mapped[Any | None] = mapped_column(JSON)
    zero_entry_report: Mapped[Any | None] = mapped_column(JSON)
    blocker_funnel: Mapped[Any | None] = mapped_column(JSON)
    raw_metrics: Mapped[Any | None] = mapped_column(JSON)
    adjusted_metrics: Mapped[Any | None] = mapped_column(JSON)
    warnings: Mapped[Any | None] = mapped_column(JSON)
    blockers: Mapped[Any | None] = mapped_column(JSON)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ResearchReplayTrade(Base):
    __tablename__ = "research_replay_trades"
    __table_args__ = (
        UniqueConstraint("trade_id", name="uq_research_replay_trades_id"),
        Index(
            "ix_research_replay_trades_run_market_config",
            "replay_run_id",
            "market_ticker",
            "strategy_config_version_id",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_id: Mapped[str] = mapped_column(String(128), nullable=False)
    replay_run_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    candidate_id: Mapped[str | None] = mapped_column(String(128), index=True)
    strategy_config_version_id: Mapped[str | None] = mapped_column(String(128), index=True)
    market_ticker: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(32), nullable=False)
    entry_decision_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    entry_fill_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    entry_limit: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    entry_fill_price: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    entry_fill_event_id: Mapped[str | None] = mapped_column(String(128))
    exit_trigger_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    exit_intent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    exit_fill_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    exit_limit: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    exit_fill_price: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    exit_fill_event_id: Mapped[str | None] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    gross_pnl_cents: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    fee_cents: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    net_pnl_cents: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    holding_duration_ms: Mapped[int | None] = mapped_column(Integer)
    mfe_cents: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    mae_cents: Mapped[Decimal | None] = mapped_column(Numeric(24, 8))
    time_to_mfe_ms: Mapped[int | None] = mapped_column(Integer)
    time_to_mae_ms: Mapped[int | None] = mapped_column(Integer)
    entry_reason: Mapped[str | None] = mapped_column(Text)
    exit_reason: Mapped[str | None] = mapped_column(Text)
    timing_tier: Mapped[str | None] = mapped_column(String(64))
    volatility_regime: Mapped[str | None] = mapped_column(String(64))
    liquidity_regime: Mapped[str | None] = mapped_column(String(64))
    entry_feature_snapshot_id: Mapped[str | None] = mapped_column(String(128))
    exit_feature_snapshot_id: Mapped[str | None] = mapped_column(String(128))
    lifecycle_version: Mapped[str | None] = mapped_column(String(128))
    measurements: Mapped[Any | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )


class CalibrationRun(Base):
    __tablename__ = "calibration_runs"
    __table_args__ = (
        UniqueConstraint("calibration_run_id", name="uq_calibration_runs_id"),
        Index("ix_calibration_runs_replay_started", "replay_run_id", "started_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    calibration_run_id: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    calibration_schema_version: Mapped[str] = mapped_column(String(128), nullable=False)
    replay_run_id: Mapped[str | None] = mapped_column(String(128), index=True)
    dataset_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    code_commit_sha: Mapped[str] = mapped_column(String(128), nullable=False)
    random_seed: Mapped[int] = mapped_column(Integer, nullable=False)
    search_space_snapshot: Mapped[Any | None] = mapped_column(JSON)
    partition_manifest: Mapped[Any | None] = mapped_column(JSON)
    frozen_holdout_hash: Mapped[str | None] = mapped_column(String(128))
    holdout_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    evaluated_candidate_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    selected_candidate_id: Mapped[str | None] = mapped_column(String(128), index=True)
    training_metrics: Mapped[Any | None] = mapped_column(JSON)
    validation_metrics: Mapped[Any | None] = mapped_column(JSON)
    test_metrics: Mapped[Any | None] = mapped_column(JSON)
    holdout_metrics: Mapped[Any | None] = mapped_column(JSON)
    bootstrap_metrics: Mapped[Any | None] = mapped_column(JSON)
    penalties: Mapped[Any | None] = mapped_column(JSON)
    warnings: Mapped[Any | None] = mapped_column(JSON)
    blockers: Mapped[Any | None] = mapped_column(JSON)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ResearchCandidate(Base):
    __tablename__ = "research_candidates"
    __table_args__ = (
        UniqueConstraint("candidate_id", name="uq_research_candidates_id"),
        UniqueConstraint("strategy_config_version_id", name="uq_research_candidates_config"),
        Index(
            "ix_research_candidates_architecture_state", "architecture_version", "lifecycle_state"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    candidate_id: Mapped[str] = mapped_column(String(128), nullable=False)
    strategy_config_version_id: Mapped[str] = mapped_column(String(128), nullable=False)
    calibration_run_id: Mapped[str | None] = mapped_column(String(128), index=True)
    parent_strategy_config_version_id: Mapped[str | None] = mapped_column(String(128))
    generated_strategy_id: Mapped[str] = mapped_column(String(128), nullable=False)
    architecture_version: Mapped[str] = mapped_column(String(128), nullable=False)
    feature_schema_version: Mapped[str] = mapped_column(String(128), nullable=False)
    replay_schema_version: Mapped[str] = mapped_column(String(128), nullable=False)
    model_type: Mapped[str] = mapped_column(String(64), nullable=False)
    parameter_snapshot: Mapped[Any] = mapped_column(JSON, nullable=False)
    feature_columns: Mapped[Any | None] = mapped_column(JSON)
    model_artifact: Mapped[Any | None] = mapped_column(JSON)
    model_artifact_checksum: Mapped[str] = mapped_column(String(128), nullable=False)
    training_metrics: Mapped[Any | None] = mapped_column(JSON)
    validation_metrics: Mapped[Any | None] = mapped_column(JSON)
    test_metrics: Mapped[Any | None] = mapped_column(JSON)
    holdout_metrics: Mapped[Any | None] = mapped_column(JSON)
    governance_report: Mapped[Any | None] = mapped_column(JSON)
    lifecycle_state: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    eligibility_status: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )


class ResearchGovernanceEvent(Base):
    __tablename__ = "research_governance_events"
    __table_args__ = (
        UniqueConstraint("governance_event_id", name="uq_research_governance_events_id"),
        Index("ix_research_governance_events_candidate_created", "candidate_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    governance_event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    candidate_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    from_state: Mapped[str | None] = mapped_column(String(64))
    to_state: Mapped[str] = mapped_column(String(64), nullable=False)
    actor: Mapped[str] = mapped_column(String(128), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    evidence: Mapped[Any | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
