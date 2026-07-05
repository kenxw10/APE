from __future__ import annotations

from pydantic import BaseModel


class SafetyResponse(BaseModel):
    mode: str
    trading_enabled: bool
    execute: bool
    is_safe: bool
    blockers: list[str]
    warnings: list[str]


class HealthResponse(BaseModel):
    status: str
    service: str
    environment: str
    app_mode: str
    safety: SafetyResponse
    version: str | None


class DatabaseStatusResponse(BaseModel):
    status: str
    configured: bool


class ReadinessResponse(BaseModel):
    status: str
    ready: bool
    safety: SafetyResponse
    database: DatabaseStatusResponse
