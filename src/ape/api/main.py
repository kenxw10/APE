from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Literal

import uvicorn
from fastapi import FastAPI, Query

from ape import __version__
from ape.config import AppConfig, load_config
from ape.db.session import check_database_connection, create_engine_from_config
from ape.kalshi.diagnostics import build_kalshi_config_diagnostic
from ape.kalshi.reference_status import (
    build_brti_reference_latest,
    build_brti_reference_series,
    build_brti_reference_status,
)
from ape.kalshi.resolver import resolve_active_btc15_market
from ape.kalshi.ws_protocol import (
    build_kalshi_ws_protocol_recent,
    build_kalshi_ws_protocol_summary,
)
from ape.kalshi.ws_status import build_kalshi_ws_status
from ape.models.health import (
    DatabaseStatusResponse,
    HealthResponse,
    ReadinessResponse,
    SafetyResponse,
)
from ape.models.kalshi import (
    ActiveMarketResponse,
    KalshiStatusResponse,
    KalshiWsProtocolRecentResponse,
    KalshiWsProtocolSummaryResponse,
    KalshiWsStatusResponse,
    active_market_response,
    kalshi_status_response,
    kalshi_ws_protocol_recent_response,
    kalshi_ws_protocol_summary_response,
    kalshi_ws_status_response,
)
from ape.models.reference import (
    BrtiReferenceLatestResponse,
    BrtiReferenceSeriesResponse,
    BrtiReferenceStatusResponse,
    brti_reference_latest_response,
    brti_reference_series_response,
    brti_reference_status_response,
)
from ape.models.storage import StorageStatusResponse, storage_status_response
from ape.models.strategy import (
    StrategyDecisionResponse,
    StrategyDryRunEventsResponse,
    StrategyDryRunPositionsResponse,
    StrategyDryRunStatusResponse,
    StrategyGateSummaryResponse,
    StrategyRecentDecisionsResponse,
    StrategyStatusResponse,
    StrategyVariantsComparisonResponse,
    strategy_decision_response,
    strategy_dry_run_events_response,
    strategy_dry_run_positions_response,
    strategy_dry_run_status_response,
    strategy_gate_summary_response,
    strategy_recent_decisions_response,
    strategy_status_response,
    strategy_variants_comparison_response,
)
from ape.research.status import (
    build_latest_calibration_cohort,
    build_latest_calibration_frontier,
    build_latest_zero_entry,
    build_research_records,
    build_research_status,
)
from ape.safety import SafetyAssessment, assert_startup_safe, assess_startup_safety
from ape.storage.retention import build_storage_status
from ape.strategy.observer import (
    build_latest_strategy_decision,
    build_open_strategy_dry_run_positions,
    build_recent_strategy_decisions,
    build_recent_strategy_dry_run_events,
    build_recent_strategy_dry_run_positions,
    build_recent_strategy_gate_summary,
    build_strategy_dry_run_status,
    build_strategy_status,
    build_strategy_variants_comparison,
    build_v2_feature_records,
    build_v2_intent_records,
    build_v2_mark_records,
    build_v2_outcome_records,
)

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

    @app.get("/ws/status", response_model=KalshiWsStatusResponse)
    def websocket_status() -> KalshiWsStatusResponse:
        return kalshi_ws_status_response(build_kalshi_ws_status(settings))

    @app.get("/ws/protocol/recent", response_model=KalshiWsProtocolRecentResponse)
    def websocket_protocol_recent(
        limit: int = Query(default=200, ge=1, le=500),
    ) -> KalshiWsProtocolRecentResponse:
        return kalshi_ws_protocol_recent_response(
            build_kalshi_ws_protocol_recent(settings, limit=limit)
        )

    @app.get("/ws/protocol/summary", response_model=KalshiWsProtocolSummaryResponse)
    def websocket_protocol_summary(
        window_seconds: int = Query(default=1800, ge=1, le=86_400),
    ) -> KalshiWsProtocolSummaryResponse:
        return kalshi_ws_protocol_summary_response(
            build_kalshi_ws_protocol_summary(
                settings,
                window_seconds=window_seconds,
            )
        )

    @app.get("/reference/brti/status", response_model=BrtiReferenceStatusResponse)
    def brti_reference_status() -> BrtiReferenceStatusResponse:
        return brti_reference_status_response(build_brti_reference_status(settings))

    @app.get("/reference/brti/latest", response_model=BrtiReferenceLatestResponse)
    def brti_reference_latest() -> BrtiReferenceLatestResponse:
        return brti_reference_latest_response(build_brti_reference_latest(settings))

    @app.get("/reference/brti/series", response_model=BrtiReferenceSeriesResponse)
    def brti_reference_series(
        window_seconds: int = Query(default=900, ge=1),
        max_points: int = Query(default=16_000, ge=1),
        since: datetime | None = None,
        include_final_minute: bool = False,
    ) -> BrtiReferenceSeriesResponse:
        return brti_reference_series_response(
            build_brti_reference_series(
                settings,
                window_seconds=window_seconds,
                max_points=max_points,
                since=since,
                include_final_minute=include_final_minute,
            )
        )

    @app.get("/strategy/status", response_model=StrategyStatusResponse)
    def strategy_status() -> StrategyStatusResponse:
        return strategy_status_response(build_strategy_status(settings))

    @app.get("/strategy/decisions/latest", response_model=StrategyDecisionResponse)
    def strategy_decision_latest(
        strategy_id: str | None = None,
    ) -> StrategyDecisionResponse:
        return strategy_decision_response(
            build_latest_strategy_decision(settings, strategy_id=strategy_id)
        )

    @app.get("/strategy/decisions/recent", response_model=StrategyRecentDecisionsResponse)
    def strategy_decisions_recent(
        limit: int = Query(default=100, ge=1, le=500),
        strategy_id: str | None = None,
    ) -> StrategyRecentDecisionsResponse:
        return strategy_recent_decisions_response(
            build_recent_strategy_decisions(
                settings,
                limit=limit,
                strategy_id=strategy_id,
            )
        )

    @app.get("/strategy/gates/recent", response_model=StrategyGateSummaryResponse)
    def strategy_gates_recent(
        limit: int = Query(default=100, ge=1, le=500),
        strategy_id: str | None = None,
    ) -> StrategyGateSummaryResponse:
        return strategy_gate_summary_response(
            build_recent_strategy_gate_summary(
                settings,
                limit=limit,
                strategy_id=strategy_id,
            )
        )

    @app.get("/strategy/dry-run/status", response_model=StrategyDryRunStatusResponse)
    def strategy_dry_run_status(
        strategy_id: str | None = None,
    ) -> StrategyDryRunStatusResponse:
        return strategy_dry_run_status_response(
            build_strategy_dry_run_status(settings, strategy_id=strategy_id)
        )

    @app.get(
        "/strategy/variants/comparison",
        response_model=StrategyVariantsComparisonResponse,
    )
    def strategy_variants_comparison(
        window_seconds: int = Query(default=3600, ge=1, le=604800),
    ) -> StrategyVariantsComparisonResponse:
        return strategy_variants_comparison_response(
            build_strategy_variants_comparison(
                settings,
                window_seconds=window_seconds,
            )
        )

    @app.get(
        "/strategy/dry-run/positions/open",
        response_model=StrategyDryRunPositionsResponse,
    )
    def strategy_dry_run_positions_open(
        strategy_id: str | None = None,
    ) -> StrategyDryRunPositionsResponse:
        return strategy_dry_run_positions_response(
            build_open_strategy_dry_run_positions(settings, strategy_id=strategy_id)
        )

    @app.get(
        "/strategy/dry-run/positions/recent",
        response_model=StrategyDryRunPositionsResponse,
    )
    def strategy_dry_run_positions_recent(
        limit: int = Query(default=100, ge=1, le=500),
        strategy_id: str | None = None,
    ) -> StrategyDryRunPositionsResponse:
        return strategy_dry_run_positions_response(
            build_recent_strategy_dry_run_positions(
                settings,
                limit=limit,
                strategy_id=strategy_id,
            )
        )

    @app.get(
        "/strategy/dry-run/events/recent",
        response_model=StrategyDryRunEventsResponse,
    )
    def strategy_dry_run_events_recent(
        limit: int = Query(default=100, ge=1, le=500),
        strategy_id: str | None = None,
    ) -> StrategyDryRunEventsResponse:
        return strategy_dry_run_events_response(
            build_recent_strategy_dry_run_events(
                settings,
                limit=limit,
                strategy_id=strategy_id,
            )
        )

    @app.get("/strategy/features/latest")
    def strategy_features_latest() -> dict[str, Any]:
        return build_v2_feature_records(settings, limit=1)

    @app.get("/strategy/features/recent")
    def strategy_features_recent(
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict[str, Any]:
        return build_v2_feature_records(settings, limit=limit)

    @app.get("/strategy/dry-run/intents/recent")
    def strategy_dry_run_intents_recent(
        limit: int = Query(default=100, ge=1, le=500),
        strategy_id: str = "btc15_momentum_v2",
        action: str | None = Query(default=None, pattern="^(ENTRY|EXIT)$"),
    ) -> dict[str, Any]:
        return build_v2_intent_records(
            settings, limit=limit, strategy_id=strategy_id, action=action
        )

    @app.get("/strategy/dry-run/position-marks/recent")
    def strategy_dry_run_position_marks_recent(
        limit: int = Query(default=100, ge=1, le=500),
        strategy_id: str = "btc15_momentum_v2",
    ) -> dict[str, Any]:
        return build_v2_mark_records(settings, limit=limit, strategy_id=strategy_id)

    @app.get("/strategy/dry-run/outcomes/recent")
    def strategy_dry_run_outcomes_recent(
        limit: int = Query(default=100, ge=1, le=500),
        strategy_id: str = "btc15_momentum_v2",
    ) -> dict[str, Any]:
        return build_v2_outcome_records(settings, limit=limit, strategy_id=strategy_id)

    @app.get("/storage/status", response_model=StorageStatusResponse)
    def storage_status() -> StorageStatusResponse:
        return storage_status_response(build_storage_status(settings))

    @app.get("/research/status")
    def research_status() -> dict[str, Any]:
        return build_research_status(settings)

    @app.get("/research/coverage/latest")
    def research_coverage_latest() -> dict[str, Any]:
        return build_research_records(settings, kind="coverage", limit=1)

    @app.get("/research/zero-entry/latest")
    def research_zero_entry_latest() -> dict[str, Any]:
        return build_latest_zero_entry(settings)

    @app.get("/research/replay/runs/recent")
    def research_replay_runs_recent(
        limit: int = Query(default=100, ge=1, le=500),
        replay_run_id: str | None = Query(default=None, pattern=r"^[A-Za-z0-9_.:-]{1,128}$"),
        status: Literal["RUNNING", "COMPLETED", "FAILED"] | None = None,
    ) -> dict[str, Any]:
        return build_research_records(
            settings,
            kind="replay_runs",
            limit=limit,
            filters={"replay_run_id": replay_run_id, "status": status},
        )

    @app.get("/research/replay/trades/recent")
    def research_replay_trades_recent(
        limit: int = Query(default=100, ge=1, le=500),
        replay_run_id: str | None = Query(default=None, pattern=r"^[A-Za-z0-9_.:-]{1,128}$"),
        candidate_id: str | None = Query(default=None, pattern=r"^[A-Za-z0-9_.:-]{1,128}$"),
        market_ticker: str | None = Query(default=None, pattern=r"^[A-Za-z0-9_.:-]{1,128}$"),
        status: Literal["CLOSED", "ENTRY_EXPIRED", "ENTRY_NO_FILL"] | None = None,
    ) -> dict[str, Any]:
        return build_research_records(
            settings,
            kind="replay_trades",
            limit=limit,
            filters={
                "replay_run_id": replay_run_id,
                "candidate_id": candidate_id,
                "market_ticker": market_ticker,
                "status": status,
            },
        )

    @app.get("/research/calibration/runs/recent")
    def research_calibration_runs_recent(
        limit: int = Query(default=100, ge=1, le=500),
        calibration_run_id: str | None = Query(default=None, pattern=r"^[A-Za-z0-9_.:-]{1,128}$"),
        replay_run_id: str | None = Query(default=None, pattern=r"^[A-Za-z0-9_.:-]{1,128}$"),
        status: (
            Literal[
                "RUNNING",
                "COMPLETED",
                "INSUFFICIENT_DATA",
                "BLOCKED",
                "FAILED",
                "INSUFFICIENT_CLEAN_DATA",
                "NO_CANDIDATE_SIGNALS",
                "SIGNALS_WITHOUT_EXECUTABLE_FILLS",
                "FILLS_WITHOUT_CLOSED_TRADES",
                "CLOSED_TRADES_WITHOUT_POSITIVE_HOLDOUT",
                "POSITIVE_RESEARCH_CANDIDATE",
                "CALIBRATION_BLOCKED",
                "CALIBRATION_FAILED",
            ]
            | None
        ) = None,
    ) -> dict[str, Any]:
        return build_research_records(
            settings,
            kind="calibration_runs",
            limit=limit,
            filters={
                "calibration_run_id": calibration_run_id,
                "replay_run_id": replay_run_id,
                "status": status,
            },
        )

    @app.get("/research/cohorts/latest")
    def research_cohort_latest() -> dict[str, Any]:
        return build_latest_calibration_cohort(settings)

    @app.get("/research/calibration/frontier/latest")
    def research_calibration_frontier_latest(
        limit: int = Query(default=20, ge=1, le=20),
    ) -> dict[str, Any]:
        return build_latest_calibration_frontier(settings, limit=limit)

    @app.get("/research/candidates/recent")
    def research_candidates_recent(
        limit: int = Query(default=100, ge=1, le=500),
        candidate_id: str | None = Query(default=None, pattern=r"^[A-Za-z0-9_.:-]{1,128}$"),
        calibration_run_id: str | None = Query(default=None, pattern=r"^[A-Za-z0-9_.:-]{1,128}$"),
        lifecycle_state: Literal[
            "DRAFT", "BACKTESTED", "SHADOW", "DRY_RUN_CHALLENGER", "RETIRED"
        ] | None = None,
    ) -> dict[str, Any]:
        return build_research_records(
            settings,
            kind="candidates",
            limit=limit,
            filters={
                "candidate_id": candidate_id,
                "calibration_run_id": calibration_run_id,
                "lifecycle_state": lifecycle_state,
            },
        )

    @app.get("/research/governance/events/recent")
    def research_governance_events_recent(
        limit: int = Query(default=100, ge=1, le=500),
        candidate_id: str | None = Query(default=None, pattern=r"^[A-Za-z0-9_.:-]{1,128}$"),
        lifecycle_state: Literal[
            "DRAFT", "BACKTESTED", "SHADOW", "DRY_RUN_CHALLENGER", "RETIRED"
        ] | None = None,
    ) -> dict[str, Any]:
        return build_research_records(
            settings,
            kind="governance_events",
            limit=limit,
            filters={"candidate_id": candidate_id, "lifecycle_state": lifecycle_state},
        )

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
