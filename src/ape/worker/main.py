from __future__ import annotations

import asyncio
import logging
import threading
from datetime import UTC, datetime

from ape.config import AppConfig, ConfigError, load_config
from ape.db.session import create_engine_from_config, create_session_factory
from ape.kalshi.ws_collector import KalshiWsCollector
from ape.repositories.inputs import WorkerHeartbeatInput
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.safety import SafetyError, assert_startup_safe, assess_startup_safety

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
            "Starting ape-worker env=%s app_mode=%s safety=%s db_configured=%s ws_enabled=%s",
            config.env,
            config.app_mode.value,
            "safe" if safety.is_safe else "blocked",
            bool(config.database_url),
            config.kalshi_ws_enabled,
        )

        event = stop_event or threading.Event()
        if config.kalshi_ws_enabled:
            LOGGER.info("APE worker running Kalshi WebSocket collector in OBSERVER mode.")
            collector = KalshiWsCollector(
                config=config,
                safety=safety,
                session_factory=session_factory,
                started_at=started_at,
            )
            asyncio.run(collector.run(stop_event=event, max_cycles=max_iterations))
            return

        LOGGER.info("APE worker running in OBSERVER mode; idle heartbeat only.")
        iterations = 0

        while not event.is_set():
            LOGGER.debug("APE worker heartbeat: observer idle.")
            _record_idle_heartbeat(config, safety, session_factory, started_at)
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
) -> None:
    if session_factory is None:
        return

    with session_factory() as session:
        WorkerHeartbeatRepository(session).record_heartbeat(
            WorkerHeartbeatInput(
                service_name="ape-worker",
                started_at=started_at,
                heartbeat_at=datetime.now(UTC),
                app_mode=config.app_mode.value,
                is_safe=safety.is_safe,
                metadata={
                    "mode": "idle",
                    "ws": {
                        "enabled": False,
                        "connection_state": "disabled",
                        "warnings": ["kalshi_ws_disabled"],
                        "blockers": [],
                    },
                },
            )
        )
        session.commit()


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
