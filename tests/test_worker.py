from __future__ import annotations

import logging

from ape.worker.main import configure_logging


def test_configure_logging_reapplies_requested_level() -> None:
    root_logger = logging.getLogger()
    previous_level = root_logger.level
    previous_handlers = list(root_logger.handlers)

    try:
        logging.basicConfig(level=logging.INFO, force=True)

        configure_logging("DEBUG")

        assert root_logger.isEnabledFor(logging.DEBUG)
    finally:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
        for handler in previous_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(previous_level)
