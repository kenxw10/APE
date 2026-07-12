from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from ape.config import AppConfig
from ape.repositories.strategy_v2 import StrategyV2Repository
from ape.research.calibration import LIFECYCLE_DRY_RUN_CHALLENGER
from ape.research.repository import ResearchRepository
from ape.strategy.momentum_v2 import (
    REPLAY_SCHEMA_VERSION,
    V2_ARCHITECTURE_VERSION,
    V2_FEATURE_SCHEMA_VERSION,
)


@dataclass(frozen=True)
class PinnedCandidate:
    strategy_id: str
    strategy_config_version_id: str
    parameters: dict[str, Any]


def resolve_pinned_candidate(
    config: AppConfig, session: Session
) -> tuple[PinnedCandidate | None, str | None]:
    config_version_id = config.strategy_v2_candidate_config_version_id
    if not config_version_id:
        return None, None
    candidate = ResearchRepository(session).get_candidate_by_config_version(config_version_id)
    version = StrategyV2Repository(session).get_config_version(config_version_id)
    if candidate is None or version is None:
        return None, "candidate_pin_missing"
    if candidate.lifecycle_state != LIFECYCLE_DRY_RUN_CHALLENGER:
        return None, "candidate_pin_not_dry_run_challenger"
    if candidate.architecture_version != V2_ARCHITECTURE_VERSION:
        return None, "candidate_pin_architecture_mismatch"
    if candidate.feature_schema_version != V2_FEATURE_SCHEMA_VERSION:
        return None, "candidate_pin_feature_schema_mismatch"
    if candidate.replay_schema_version != REPLAY_SCHEMA_VERSION:
        return None, "candidate_pin_replay_schema_mismatch"
    if candidate.model_artifact_checksum != _hash(candidate.model_artifact or {}):
        return None, "candidate_pin_artifact_checksum_invalid"
    if version.candidate_id != candidate.candidate_id:
        return None, "candidate_pin_config_association_invalid"
    if version.lifecycle_state != LIFECYCLE_DRY_RUN_CHALLENGER:
        return None, "candidate_pin_config_not_approved"
    return PinnedCandidate(
        strategy_id=candidate.generated_strategy_id,
        strategy_config_version_id=config_version_id,
        parameters=dict(candidate.parameter_snapshot),
    ), None


def _hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()
