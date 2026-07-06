from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel

from ape.repositories.inputs import JsonPayload
from ape.strategy.observer import (
    StrategyDecisionSnapshot,
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
    decision_age_seconds: float | None
    stale: bool
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
