from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel

from ape.repositories.inputs import JsonPayload
from ape.strategy.observer import (
    StrategyDecisionSnapshot,
    StrategyDryRunEventSnapshot,
    StrategyDryRunEventsSnapshot,
    StrategyDryRunPositionSnapshot,
    StrategyDryRunPositionsSnapshot,
    StrategyDryRunStatusSnapshot,
    StrategyGateSummarySnapshot,
    StrategyRecentDecisionsSnapshot,
    StrategyStatusSnapshot,
)


class StrategyDecisionResponse(BaseModel):
    found: bool
    decision_id: str | None
    evaluated_at: datetime | None
    decision_state: str | None
    primary_reason: str | None
    app_mode: str | None
    market_ticker: str | None
    candidate_side: str | None
    boundary: Decimal | None
    brti_value: Decimal | None
    distance_bps: Decimal | None
    seconds_left: int | None
    measurements: JsonPayload | None
    blockers: JsonPayload | None
    warnings: JsonPayload | None
    raw_context_hash: str | None


class StrategyRecentDecisionsResponse(BaseModel):
    limit: int
    count: int
    decisions: list[StrategyDecisionResponse]
    checked_at: datetime


class StrategyStatusResponse(BaseModel):
    enabled: bool
    worker_observed_enabled: bool | None
    connection_state: str
    app_mode: str
    trading_enabled: bool
    execute: bool
    is_safe: bool
    latest_decision_id: str | None
    latest_evaluated_at: datetime | None
    latest_decision_state: str | None
    latest_primary_reason: str | None
    market_ticker: str | None
    candidate_side: str | None
    boundary: Decimal | None
    brti_value: Decimal | None
    distance_bps: Decimal | None
    seconds_left: int | None
    latest_measurements_summary: JsonPayload | None
    gate_results_summary: JsonPayload | None
    decision_age_seconds: float | None
    stale: bool
    warnings: list[str]
    blockers: list[str]
    checked_at: datetime


class StrategyGateSummaryResponse(BaseModel):
    limit: int
    count: int
    checked_at: datetime
    by_state: JsonPayload
    by_reason: JsonPayload
    by_gate: JsonPayload
    latest_decision: StrategyDecisionResponse
    latest_enter_dry_run: StrategyDecisionResponse
    latest_blockers: list[str]
    current_open_position_count: int


class StrategyDryRunPositionResponse(BaseModel):
    found: bool
    position_id: str | None
    market_ticker: str | None
    strategy_id: str | None
    side_candidate: str | None
    status: str | None
    opened_at: datetime | None
    open_price: Decimal | None
    contract_count: int | None
    boundary: Decimal | None
    brti_at_entry: Decimal | None
    distance_bps_at_entry: Decimal | None
    decision_id: str | None
    closed_at: datetime | None
    close_price: Decimal | None
    close_reason: str | None
    realized_pnl_cents: Decimal | None
    measurements_summary: JsonPayload | None


class StrategyDryRunEventResponse(BaseModel):
    found: bool
    event_id: str | None
    position_id: str | None
    decision_id: str | None
    event_type: str | None
    market_ticker: str | None
    occurred_at: datetime | None
    side_candidate: str | None
    price: Decimal | None
    contract_count: int | None
    reason: str | None
    measurements_summary: JsonPayload | None


class StrategyDryRunPositionsResponse(BaseModel):
    limit: int
    count: int
    positions: list[StrategyDryRunPositionResponse]
    checked_at: datetime


class StrategyDryRunEventsResponse(BaseModel):
    limit: int
    count: int
    events: list[StrategyDryRunEventResponse]
    checked_at: datetime


class StrategyDryRunStatusResponse(BaseModel):
    enabled: bool
    worker_observed_enabled: bool | None
    app_mode: str
    trading_enabled: bool
    execute: bool
    is_safe: bool
    open_position_count: int
    max_open_positions: int
    latest_event: StrategyDryRunEventResponse
    latest_enter_decision: StrategyDecisionResponse
    last_evaluated_at: datetime | None
    warnings: list[str]
    blockers: list[str]
    checked_at: datetime


def strategy_decision_response(
    snapshot: StrategyDecisionSnapshot,
) -> StrategyDecisionResponse:
    return StrategyDecisionResponse(**snapshot.__dict__)


def strategy_recent_decisions_response(
    snapshot: StrategyRecentDecisionsSnapshot,
) -> StrategyRecentDecisionsResponse:
    return StrategyRecentDecisionsResponse(
        limit=snapshot.limit,
        count=snapshot.count,
        decisions=[strategy_decision_response(decision) for decision in snapshot.decisions],
        checked_at=snapshot.checked_at,
    )


def strategy_status_response(snapshot: StrategyStatusSnapshot) -> StrategyStatusResponse:
    return StrategyStatusResponse(**snapshot.__dict__)


def strategy_gate_summary_response(
    snapshot: StrategyGateSummarySnapshot,
) -> StrategyGateSummaryResponse:
    return StrategyGateSummaryResponse(
        limit=snapshot.limit,
        count=snapshot.count,
        checked_at=snapshot.checked_at,
        by_state=snapshot.by_state,
        by_reason=snapshot.by_reason,
        by_gate=snapshot.by_gate,
        latest_decision=strategy_decision_response(snapshot.latest_decision),
        latest_enter_dry_run=strategy_decision_response(snapshot.latest_enter_dry_run),
        latest_blockers=snapshot.latest_blockers,
        current_open_position_count=snapshot.current_open_position_count,
    )


def strategy_dry_run_position_response(
    snapshot: StrategyDryRunPositionSnapshot,
) -> StrategyDryRunPositionResponse:
    return StrategyDryRunPositionResponse(**snapshot.__dict__)


def strategy_dry_run_event_response(
    snapshot: StrategyDryRunEventSnapshot,
) -> StrategyDryRunEventResponse:
    return StrategyDryRunEventResponse(**snapshot.__dict__)


def strategy_dry_run_positions_response(
    snapshot: StrategyDryRunPositionsSnapshot,
) -> StrategyDryRunPositionsResponse:
    return StrategyDryRunPositionsResponse(
        limit=snapshot.limit,
        count=snapshot.count,
        positions=[
            strategy_dry_run_position_response(position)
            for position in snapshot.positions
        ],
        checked_at=snapshot.checked_at,
    )


def strategy_dry_run_events_response(
    snapshot: StrategyDryRunEventsSnapshot,
) -> StrategyDryRunEventsResponse:
    return StrategyDryRunEventsResponse(
        limit=snapshot.limit,
        count=snapshot.count,
        events=[strategy_dry_run_event_response(event) for event in snapshot.events],
        checked_at=snapshot.checked_at,
    )


def strategy_dry_run_status_response(
    snapshot: StrategyDryRunStatusSnapshot,
) -> StrategyDryRunStatusResponse:
    return StrategyDryRunStatusResponse(
        enabled=snapshot.enabled,
        worker_observed_enabled=snapshot.worker_observed_enabled,
        app_mode=snapshot.app_mode,
        trading_enabled=snapshot.trading_enabled,
        execute=snapshot.execute,
        is_safe=snapshot.is_safe,
        open_position_count=snapshot.open_position_count,
        max_open_positions=snapshot.max_open_positions,
        latest_event=strategy_dry_run_event_response(snapshot.latest_event),
        latest_enter_decision=strategy_decision_response(snapshot.latest_enter_decision),
        last_evaluated_at=snapshot.last_evaluated_at,
        warnings=snapshot.warnings,
        blockers=snapshot.blockers,
        checked_at=snapshot.checked_at,
    )
