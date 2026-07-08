from __future__ import annotations

import asyncio
import logging
import threading
from datetime import UTC, datetime

from sqlalchemy.exc import SQLAlchemyError

from ape.config import AppConfig, ConfigError, load_config
from ape.db.session import create_engine_from_config, create_session_factory
from ape.kalshi.ws_collector import KalshiWsCollector, heartbeat_interval_seconds
from ape.repositories.inputs import WorkerHeartbeatInput
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.safety import SafetyError, assert_startup_safe, assess_startup_safety
from ape.storage.retention import StorageRetentionWorker
from ape.strategy.observer import StrategyObserver
from ape.worker.services import (
    WORKER_SERVICE_AGGREGATE,
    WORKER_SERVICE_MARKET_WS,
    WORKER_SERVICE_REFERENCE_BRTI,
    WORKER_SERVICE_STORAGE_RETENTION,
    WORKER_SERVICE_STRATEGY,
)

LOGGER = logging.getLogger(__name__)


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        force=True,
    )


def run_worker(
    config: AppConfig,
    stop_event: threading.Event | None = None,
    max_iterations: int | None = None,
) -> None:
    safety = assess_startup_safety(config)
    assert_startup_safe(safety)
    started_at = datetime.now(UTC)
    session_factory = None
    engine = create_engine_from_config(config) if config.database_url else None
    if engine is not None:
        session_factory = create_session_factory(engine)

    try:
        LOGGER.info(
            "Starting ape-worker env=%s app_mode=%s safety=%s db_configured=%s "
            "ws_enabled=%s brti_enabled=%s strategy_observer_enabled=%s "
            "strategy_dry_run_enabled=%s storage_retention_enabled=%s",
            config.env,
            config.app_mode.value,
            "safe" if safety.is_safe else "blocked",
            bool(config.database_url),
            config.kalshi_ws_enabled,
            config.kalshi_cfbenchmarks_enabled,
            config.strategy_observer_enabled,
            config.strategy_dry_run_enabled,
            config.storage_retention_enabled,
        )

        event = stop_event or threading.Event()
        if (
            config.kalshi_ws_enabled
            or _reference_worker_enabled(config)
            or config.strategy_observer_enabled
            or config.storage_retention_enabled
        ):
            LOGGER.info("APE worker running enabled observer services.")
            tasks = []
            if config.kalshi_ws_enabled or _reference_worker_enabled(config):
                collector = KalshiWsCollector(
                    config=config,
                    safety=safety,
                    session_factory=session_factory,
                    started_at=started_at,
                )
                tasks.append(collector.run(stop_event=event, max_cycles=max_iterations))
            if config.strategy_observer_enabled:
                observer = StrategyObserver(
                    config=config,
                    safety=safety,
                    session_factory=session_factory,
                    started_at=started_at,
                )
                tasks.append(observer.run(stop_event=event, max_iterations=max_iterations))
            if config.storage_retention_enabled:
                retention = StorageRetentionWorker(
                    config=config,
                    safety=safety,
                    session_factory=session_factory,
                    started_at=started_at,
                )
                tasks.append(retention.run(stop_event=event, max_iterations=max_iterations))
            asyncio.run(
                _run_enabled_observer_tasks(
                    *tasks,
                )
            )
            return

        LOGGER.info("APE worker running in OBSERVER mode; idle heartbeat only.")
        iterations = 0
        last_idle_heartbeat_at: datetime | None = None

        while not event.is_set():
            heartbeat_at = datetime.now(UTC)
            if _idle_heartbeat_due(config, last_idle_heartbeat_at, heartbeat_at):
                LOGGER.debug("APE worker heartbeat: observer idle.")
                last_idle_heartbeat_at = _record_idle_heartbeat(
                    config,
                    safety,
                    session_factory,
                    started_at,
                    heartbeat_at,
                )
            iterations += 1

            if max_iterations is not None and iterations >= max_iterations:
                return

            event.wait(config.worker_poll_seconds)
    finally:
        if engine is not None:
            engine.dispose()


def _record_idle_heartbeat(
    config: AppConfig,
    safety,
    session_factory,
    started_at: datetime,
    heartbeat_at: datetime,
) -> datetime:
    if session_factory is None:
        return heartbeat_at

    try:
        with session_factory() as session:
            repository = WorkerHeartbeatRepository(session)
            metadata = {
                "mode": "idle",
                "ws": {
                    "enabled": False,
                    "connection_state": "disabled",
                    "warnings": ["kalshi_ws_disabled"],
                    "blockers": [],
                },
                "reference": {
                    "brti": {
                        "enabled": config.kalshi_cfbenchmarks_enabled,
                        "source": "kalshi_cfbenchmarks_brti",
                        "index_ids": list(config.kalshi_cfbenchmarks_index_ids),
                        "connection_state": "disabled",
                        "warnings": [],
                        "blockers": [],
                    }
                },
                "strategy": {
                    "observer": {
                        "enabled": config.strategy_observer_enabled,
                        "connection_state": "disabled",
                        "last_evaluated_at": None,
                        "last_decision_state": None,
                        "last_primary_reason": None,
                        "last_decision_id": None,
                        "warnings": ["strategy_observer_disabled"],
                        "blockers": [],
                    },
                    "dry_run": {
                        "enabled": False,
                        "open_position_count": 0,
                        "latest_event_type": None,
                        "latest_position_id": None,
                        "warnings": ["strategy_dry_run_disabled"],
                        "blockers": [],
                    }
                },
                "storage": {
                    "retention": {
                        "enabled": config.storage_retention_enabled,
                        "connection_state": "disabled",
                        "last_run_id": None,
                        "last_started_at": None,
                        "last_finished_at": None,
                        "last_status": None,
                        "last_deleted_rows": {},
                        "last_raw_payload_stripped_rows": {},
                        "warnings": ["storage_retention_disabled"],
                        "blockers": [],
                    }
                },
            }
            component_metadata = (
                (WORKER_SERVICE_MARKET_WS, {"mode": "market_ws", "ws": metadata["ws"]}),
                (
                    WORKER_SERVICE_REFERENCE_BRTI,
                    {"mode": "reference_brti", "reference": metadata["reference"]},
                ),
                (
                    WORKER_SERVICE_STRATEGY,
                    {"mode": "strategy_observer", "strategy": metadata["strategy"]},
                ),
                (
                    WORKER_SERVICE_STORAGE_RETENTION,
                    {"mode": "storage_retention", "storage": metadata["storage"]},
                ),
            )
            for service_name, service_metadata in component_metadata:
                repository.record_heartbeat(
                    WorkerHeartbeatInput(
                        service_name=service_name,
                        started_at=started_at,
                        heartbeat_at=heartbeat_at,
                        app_mode=config.app_mode.value,
                        is_safe=safety.is_safe,
                        metadata=service_metadata,
                    )
                )
            repository.record_heartbeat(
                WorkerHeartbeatInput(
                    service_name=WORKER_SERVICE_AGGREGATE,
                    started_at=started_at,
                    heartbeat_at=heartbeat_at,
                    app_mode=config.app_mode.value,
                    is_safe=safety.is_safe,
                    metadata=metadata,
                )
            )
            session.commit()
    except SQLAlchemyError:
        LOGGER.warning("Idle worker heartbeat persistence failed.", exc_info=True)

    return heartbeat_at


def _idle_heartbeat_due(
    config: AppConfig,
    last_heartbeat_at: datetime | None,
    heartbeat_at: datetime,
) -> bool:
    if last_heartbeat_at is None:
        return True
    elapsed = (heartbeat_at.astimezone(UTC) - last_heartbeat_at.astimezone(UTC)).total_seconds()
    return elapsed >= heartbeat_interval_seconds(config)


def _reference_worker_enabled(config: AppConfig) -> bool:
    return (
        config.kalshi_cfbenchmarks_enabled
        and config.kalshi_cfbenchmarks_subscribe_on_worker
    )


async def _run_enabled_observer_tasks(*tasks) -> None:
    await asyncio.gather(*tasks)


def main() -> int:
    try:
        config = load_config()
        configure_logging(config.log_level)
        run_worker(config)
    except ConfigError as exc:
        LOGGER.error("Configuration error: %s", exc)
        return 1
    except SafetyError as exc:
        LOGGER.error("Safety blocked startup: %s", exc)
        return 1
    except KeyboardInterrupt:
        LOGGER.info("APE worker stopped.")
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
