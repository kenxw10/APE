import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))


def run() -> int:
    from ape.db.migrations import main

    return main()

if __name__ == "__main__":
    raise SystemExit(run())
