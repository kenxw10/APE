from __future__ import annotations

import logging
import threading

from ape.config import AppConfig, ConfigError, load_config
from ape.safety import SafetyError, assert_startup_safe, assess_startup_safety

LOGGER = logging.getLogger(__name__)


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def run_worker(
    config: AppConfig,
    stop_event: threading.Event | None = None,
    max_iterations: int | None = None,
) -> None:
    safety = assess_startup_safety(config)
    assert_startup_safe(safety)

    LOGGER.info("APE worker started in OBSERVER mode; idle heartbeat only.")

    event = stop_event or threading.Event()
    iterations = 0

    while not event.is_set():
        LOGGER.debug("APE worker heartbeat: observer idle.")
        iterations += 1

        if max_iterations is not None and iterations >= max_iterations:
            return

        event.wait(config.worker_poll_seconds)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")

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

