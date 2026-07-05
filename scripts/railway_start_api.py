from __future__ import annotations

import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

STARTUP_STEPS = ("python -m ape.db.migrations", "python -m ape.api.main")

LOGGER = logging.getLogger(__name__)


def migrations_main() -> int:
    from ape.db.migrations import main

    return main()


def api_main() -> None:
    from ape.api.main import main

    main()


def run() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    LOGGER.info("Railway API startup: running database migrations before API start.")

    migration_result = migrations_main()
    if migration_result != 0:
        return migration_result

    api_main()
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
