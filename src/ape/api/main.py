from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from ape import __version__
from ape.config import AppConfig, load_config
from ape.models.health import HealthResponse, SafetyResponse
from ape.safety import SafetyAssessment, assert_startup_safe, assess_startup_safety


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

    return app


def _safety_response(assessment: SafetyAssessment) -> SafetyResponse:
    return SafetyResponse(**assessment.to_dict())


def main() -> None:
    settings = load_config()
    app = create_app(settings)
    uvicorn.run(
        app,
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
