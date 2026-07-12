from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from ape.config import AppConfig
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.research.repository import ResearchRepository
from ape.worker.services import WORKER_SERVICE_RESEARCH


def build_research_status(config: AppConfig, *, now: datetime | None = None) -> dict[str, Any]:
    checked_at = _as_utc(now or datetime.now(UTC))
    base = {
        "configured_enabled": config.research_enabled,
        "configured_calibration_enabled": config.calibration_enabled,
        "api_local_configuration": {
            "research_enabled": config.research_enabled,
            "calibration_enabled": config.calibration_enabled,
        },
        "worker_observed_configuration": None,
        "worker_observed_safety": None,
        "worker_observed_enabled": None,
        "heartbeat_fresh": False,
        "effective_enabled": False,
        "worker_role": "research",
        "worker_heartbeat": None,
        "last_archive_run": None,
        "last_outcome_label_run": None,
        "last_zero_entry_audit": None,
        "last_replay_run": None,
        "last_calibration_run": None,
        "candidate_counts_by_state": {},
        "data_coverage": {},
        "event_lag_seconds": None,
        "warnings": [],
        "blockers": [],
        "safety_state": "DRY_RUN_ONLY"
        if config.app_mode.value == "DRY_RUN" and not config.trading_enabled and not config.execute
        else "BLOCKED",
    }
    if not config.database_url:
        base["blockers"].append("research_database_not_configured")
        return base
    engine = create_engine_from_config(config)
    try:
        factory = create_session_factory(engine)
        with factory() as session:
            repository = ResearchRepository(session)
            heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat(
                WORKER_SERVICE_RESEARCH
            )
            metadata = (
                heartbeat.metadata_ if heartbeat and isinstance(heartbeat.metadata_, dict) else {}
            )
            details = metadata.get("research") if isinstance(metadata.get("research"), dict) else {}
            heartbeat_at = _as_utc(heartbeat.heartbeat_at) if heartbeat is not None else None
            heartbeat_fresh = bool(
                heartbeat_at is not None
                and (checked_at - heartbeat_at).total_seconds()
                <= max(config.research_poll_seconds * 2, 60.0)
            )
            latest_replay = repository.latest_replay_run()
            latest_calibration = repository.latest_calibration_run()
            outcomes = repository.list_complete_outcomes()
            latest_event = repository.latest_event()
            base.update(
                effective_enabled=bool(details.get("enabled", False))
                and bool(heartbeat.is_safe if heartbeat is not None else False)
                and heartbeat_fresh,
                worker_observed_configuration={
                    "research_enabled": details.get("enabled"),
                    "calibration_enabled": details.get("calibration_enabled"),
                    "worker_role": details.get("worker_role"),
                }
                if heartbeat
                else None,
                worker_observed_safety=heartbeat.is_safe if heartbeat is not None else None,
                worker_observed_enabled=details.get("enabled") if heartbeat else None,
                heartbeat_fresh=heartbeat_fresh,
                worker_heartbeat={
                    "at": heartbeat_at.isoformat() if heartbeat_at else None,
                    "metadata": details,
                }
                if heartbeat
                else None,
                last_archive_run=details.get("last_archive_run"),
                last_outcome_label_run=details.get("last_archive_run"),
                last_zero_entry_audit=repository.latest_zero_entry_report(),
                last_replay_run=_row(latest_replay),
                last_calibration_run=_row(latest_calibration),
                candidate_counts_by_state=repository.candidate_state_counts(),
                data_coverage=(latest_replay.raw_metrics or {}).get(
                    "archive_coverage", {"complete_markets": len(outcomes)}
                )
                if latest_replay
                else {"complete_markets": len(outcomes)},
                event_lag_seconds=(checked_at - _as_utc(latest_event.event_time)).total_seconds()
                if latest_event
                else None,
            )
            if heartbeat is not None and not heartbeat_fresh:
                base["warnings"].append("research_worker_heartbeat_stale")
            if not base["effective_enabled"] and config.research_enabled:
                base["warnings"].append("research_worker_waiting_for_heartbeat")
    except SQLAlchemyError:
        base["blockers"].append("research_database_error")
    finally:
        engine.dispose()
    return base


def build_research_records(
    config: AppConfig,
    *,
    kind: str,
    limit: int,
    filters: dict[str, str | None] | None = None,
) -> dict[str, Any]:
    filters = {key: value for key, value in (filters or {}).items() if value is not None}
    if not config.database_url:
        return {"items": [], "configured": False, "kind": kind}
    engine = create_engine_from_config(config)
    try:
        factory = create_session_factory(engine)
        with factory() as session:
            repository = ResearchRepository(session)
            if kind == "coverage":
                return {
                    "kind": kind,
                    "configured": True,
                    "coverage": repository.latest_coverage_report(),
                }
            loaders = {
                "replay_runs": lambda: repository.list_recent_replay_runs(limit, **filters),
                "replay_trades": lambda: repository.list_recent_replay_trades(limit, **filters),
                "calibration_runs": lambda: repository.list_recent_calibration_runs(
                    limit, **filters
                ),
                "candidates": lambda: repository.list_recent_candidates(limit, **filters),
                "governance_events": lambda: repository.list_recent_governance_events(
                    limit, **filters
                ),
            }
            return {
                "kind": kind,
                "configured": True,
                "items": [_row(row) for row in loaders[kind]()],
            }
    except SQLAlchemyError:
        return {"kind": kind, "configured": True, "items": [], "error": "research_database_error"}
    finally:
        engine.dispose()


def build_latest_zero_entry(config: AppConfig) -> dict[str, Any]:
    if not config.database_url:
        return {"configured": False, "report": None}
    engine = create_engine_from_config(config)
    try:
        with create_session_factory(engine)() as session:
            return {
                "configured": True,
                "report": ResearchRepository(session).latest_zero_entry_report(),
            }
    finally:
        engine.dispose()


def _row(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    result: dict[str, Any] = {}
    for column in value.__table__.columns:
        item = getattr(value, column.key)
        result[column.key] = item.isoformat() if isinstance(item, datetime) else item
    return result


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
