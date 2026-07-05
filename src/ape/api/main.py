from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from ape import __version__
from ape.config import AppConfig, load_config
from ape.db.session import check_database_connection, create_engine_from_config
from ape.kalshi.diagnostics import build_kalshi_config_diagnostic
from ape.kalshi.resolver import resolve_active_btc15_market
from ape.models.health import (
    DatabaseStatusResponse,
    HealthResponse,
    ReadinessResponse,
    SafetyResponse,
)
from ape.models.kalshi import (
    ActiveMarketResponse,
    KalshiStatusResponse,
    active_market_response,
    kalshi_status_response,
)
from ape.safety import SafetyAssessment, assert_startup_safe, assess_startup_safety

LOGGER = logging.getLogger(__name__)


def create_app(config: AppConfig | None = None) -> FastAPI:
    settings = config or load_config()
    safety = assess_startup_safety(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        assert_startup_safe(safety)
        yield

    app = FastAPI(
        title="APE Observer API",
        version=__version__,
        lifespan=lifespan,
    )
    app.state.config = settings
    app.state.safety = safety
    app.state.kalshi_client_factory = None

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        current_safety: SafetyAssessment = app.state.safety
        return HealthResponse(
            status="ok" if current_safety.is_safe else "blocked",
            service="ape-api",
            environment=settings.env,
            app_mode=settings.app_mode.value,
            safety=_safety_response(current_safety),
            version=__version__,
        )

    @app.get("/safety", response_model=SafetyResponse)
    def safety_status() -> SafetyResponse:
        return _safety_response(app.state.safety)

    @app.get("/db/status", response_model=DatabaseStatusResponse)
    def database_status() -> DatabaseStatusResponse:
        return _database_status(settings)

    @app.get("/kalshi/status", response_model=KalshiStatusResponse)
    def kalshi_status() -> KalshiStatusResponse:
        return kalshi_status_response(build_kalshi_config_diagnostic(settings))

    @app.get("/markets/active", response_model=ActiveMarketResponse)
    def active_market() -> ActiveMarketResponse:
        client_factory = getattr(app.state, "kalshi_client_factory", None)
        client = client_factory(settings) if client_factory else None
        result = resolve_active_btc15_market(config=settings, client=client)
        return active_market_response(result)

    @app.get("/ready", response_model=ReadinessResponse)
    def readiness() -> ReadinessResponse:
        current_safety: SafetyAssessment = app.state.safety
        current_database_status = _database_status(settings)

        if not current_safety.is_safe:
            status = "blocked"
        elif current_database_status.status == "ok":
            status = "ready"
        else:
            status = "not_ready"

        return ReadinessResponse(
            status=status,
            ready=status == "ready",
            safety=_safety_response(current_safety),
            database=current_database_status,
        )

    return app


def _safety_response(assessment: SafetyAssessment) -> SafetyResponse:
    return SafetyResponse(**assessment.to_dict())


def _database_status(settings: AppConfig) -> DatabaseStatusResponse:
    if not settings.database_url:
        return DatabaseStatusResponse(status="not_configured", configured=False)

    try:
        engine = create_engine_from_config(settings)
        try:
            check_database_connection(engine)
        finally:
            engine.dispose()
    except Exception:
        return DatabaseStatusResponse(status="error", configured=True)

    return DatabaseStatusResponse(status="ok", configured=True)


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        force=True,
    )


def main() -> None:
    settings = load_config()
    configure_logging(settings.log_level)
    safety = assess_startup_safety(settings)
    LOGGER.info(
        "Starting ape-api env=%s app_mode=%s safety=%s db_configured=%s",
        settings.env,
        settings.app_mode.value,
        "safe" if safety.is_safe else "blocked",
        bool(settings.database_url),
    )
    app = create_app(settings)
    uvicorn.run(
        app,
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
