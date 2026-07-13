from __future__ import annotations

from ape.config import WORKER_ROLES, load_config
from ape.worker.main import WORKER_ROLE_RESEARCH, _normalize_worker_role


def test_research_worker_role_is_configured_and_normalized() -> None:
    assert WORKER_ROLE_RESEARCH in WORKER_ROLES
    assert _normalize_worker_role("research") == WORKER_ROLE_RESEARCH
    assert load_config({"APE_WORKER_ROLE": "research"}).ape_worker_role == WORKER_ROLE_RESEARCH
