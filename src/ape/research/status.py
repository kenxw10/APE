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
        "healthy": False,
        "worker_state": "waiting_for_worker",
        "cycle_state": "waiting_for_worker",
        "current_stage": None,
        "post_archive_substage": None,
        "last_successful_stage": None,
        "cycle_id": None,
        "cycle_running": False,
        "last_error": None,
        "statement_timeout_detected": False,
        "cycle_started_at": None,
        "cycle_finished_at": None,
        "current_source_table": None,
        "completed_archive_batches": None,
        "archive_event_count": None,
        "archived_counts_by_type": {},
        "last_progress_at": None,
        "failed_stage": None,
        "last_archive_batch": None,
        "labels_processed": None,
        "labels_remaining": None,
        "association_rows_processed": None,
        "association_rows_remaining": None,
        "label_markets_processed": None,
        "label_markets_remaining": None,
        "label_markets_blocked_missing_market": None,
        "replay_dataset_watermark": None,
        "replay_total_events": None,
        "replay_events_scanned": None,
        "replay_pages_completed": None,
        "replay_partitions_completed": None,
        "replay_partitions_total": None,
        "replay_max_page_size": None,
        "coverage_dataset_watermark": None,
        "coverage_total_events": None,
        "coverage_events_scanned": None,
        "coverage_pages_completed": None,
        "coverage_partitions_completed": None,
        "coverage_partitions_total": None,
        "coverage_max_page_size": None,
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
            heartbeat_interval_seconds = _positive_float(
                details.get("poll_seconds"), config.research_poll_seconds
            )
            heartbeat_fresh = bool(
                heartbeat_at is not None
                and (checked_at - heartbeat_at).total_seconds()
                <= max(heartbeat_interval_seconds * 2, 60.0)
            )
            worker_state = str(details.get("worker_state") or "waiting_for_worker")
            last_error = _safe_error(details.get("last_error"))
            latest_replay = repository.latest_replay_run()
            latest_calibration = repository.latest_calibration_run()
            outcomes = repository.list_complete_outcomes()
            latest_event = repository.latest_event()
            base.update(
                effective_enabled=bool(details.get("enabled", False))
                and bool(heartbeat.is_safe if heartbeat is not None else False)
                and heartbeat_fresh,
                healthy=bool(details.get("enabled", False))
                and bool(heartbeat.is_safe if heartbeat is not None else False)
                and heartbeat_fresh
                and worker_state == "healthy",
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
                    "metadata": _safe_worker_metadata(details),
                }
                if heartbeat
                else None,
                last_archive_run=details.get("last_archive_run"),
                last_archive_batch=details.get("last_archive_batch"),
                labels_processed=details.get("labels_processed"),
                labels_remaining=details.get("labels_remaining"),
                association_rows_processed=details.get("association_rows_processed"),
                association_rows_remaining=details.get("association_rows_remaining"),
                label_markets_processed=details.get("label_markets_processed"),
                label_markets_remaining=details.get("label_markets_remaining"),
                label_markets_blocked_missing_market=details.get(
                    "label_markets_blocked_missing_market"
                ),
                replay_dataset_watermark=details.get("replay_dataset_watermark"),
                replay_total_events=details.get("replay_total_events"),
                replay_events_scanned=details.get("replay_events_scanned"),
                replay_pages_completed=details.get("replay_pages_completed"),
                replay_partitions_completed=details.get("replay_partitions_completed"),
                replay_partitions_total=details.get("replay_partitions_total"),
                replay_max_page_size=details.get("replay_max_page_size"),
                coverage_dataset_watermark=details.get("coverage_dataset_watermark"),
                coverage_total_events=details.get("coverage_total_events"),
                coverage_events_scanned=details.get("coverage_events_scanned"),
                coverage_pages_completed=details.get("coverage_pages_completed"),
                coverage_partitions_completed=details.get(
                    "coverage_partitions_completed"
                ),
                coverage_partitions_total=details.get("coverage_partitions_total"),
                coverage_max_page_size=details.get("coverage_max_page_size"),
                worker_state=worker_state,
                cycle_state=str(details.get("cycle_state") or worker_state),
                current_stage=details.get("current_stage"),
                post_archive_substage=details.get("post_archive_substage"),
                last_successful_stage=details.get("last_successful_stage"),
                cycle_id=details.get("cycle_id"),
                cycle_running=bool(details.get("cycle_running")),
                last_error=last_error,
                statement_timeout_detected=bool(
                    details.get("statement_timeout_detected")
                    or (last_error or {}).get("statement_timeout_detected")
                ),
                cycle_started_at=details.get("cycle_started_at"),
                cycle_finished_at=details.get("cycle_finished_at"),
                current_source_table=details.get("current_source_table"),
                completed_archive_batches=details.get("completed_archive_batches"),
                archive_event_count=details.get("archive_event_count"),
                archived_counts_by_type=details.get("archived_counts_by_type")
                if isinstance(details.get("archived_counts_by_type"), dict)
                else {},
                last_progress_at=details.get("last_progress_at"),
                failed_stage=details.get("failed_stage"),
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
            for warning in _bounded_strings(details.get("warnings")):
                if warning not in base["warnings"]:
                    base["warnings"].append(warning)
            for blocker in _bounded_strings(details.get("blockers")):
                if blocker not in base["blockers"]:
                    base["blockers"].append(blocker)
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
    except SQLAlchemyError:
        return {
            "configured": True,
            "report": None,
            "error": "research_database_error",
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


def _positive_float(value: Any, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _safe_error(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    return {
        "type": str(value.get("type") or "ResearchError")[:80],
        "code": str(value.get("code") or "research_stage_failed")[:80],
        "statement_timeout_detected": bool(value.get("statement_timeout_detected")),
    }


def _safe_worker_metadata(details: dict[str, Any]) -> dict[str, Any]:
    return {
        "enabled": details.get("enabled"),
        "calibration_enabled": details.get("calibration_enabled"),
        "worker_role": details.get("worker_role"),
        "worker_state": details.get("worker_state"),
        "cycle_state": details.get("cycle_state"),
        "current_stage": details.get("current_stage"),
        "post_archive_substage": details.get("post_archive_substage"),
        "last_successful_stage": details.get("last_successful_stage"),
        "cycle_id": details.get("cycle_id"),
        "cycle_running": bool(details.get("cycle_running")),
        "cycle_started_at": details.get("cycle_started_at"),
        "cycle_finished_at": details.get("cycle_finished_at"),
        "current_source_table": details.get("current_source_table"),
        "completed_archive_batches": details.get("completed_archive_batches"),
        "archive_event_count": details.get("archive_event_count"),
        "archived_counts_by_type": details.get("archived_counts_by_type")
        if isinstance(details.get("archived_counts_by_type"), dict)
        else {},
        "last_progress_at": details.get("last_progress_at"),
        "failed_stage": details.get("failed_stage"),
        "last_archive_batch": details.get("last_archive_batch"),
        "association_rows_processed": details.get("association_rows_processed"),
        "association_rows_remaining": details.get("association_rows_remaining"),
        "label_markets_processed": details.get("label_markets_processed"),
        "label_markets_remaining": details.get("label_markets_remaining"),
        "label_markets_blocked_missing_market": details.get(
            "label_markets_blocked_missing_market"
        ),
        "replay_dataset_watermark": details.get("replay_dataset_watermark"),
        "replay_total_events": details.get("replay_total_events"),
        "replay_events_scanned": details.get("replay_events_scanned"),
        "replay_pages_completed": details.get("replay_pages_completed"),
        "replay_partitions_completed": details.get("replay_partitions_completed"),
        "replay_partitions_total": details.get("replay_partitions_total"),
        "coverage_dataset_watermark": details.get("coverage_dataset_watermark"),
        "coverage_total_events": details.get("coverage_total_events"),
        "coverage_events_scanned": details.get("coverage_events_scanned"),
        "coverage_pages_completed": details.get("coverage_pages_completed"),
        "coverage_partitions_completed": details.get("coverage_partitions_completed"),
        "coverage_partitions_total": details.get("coverage_partitions_total"),
        "last_error": _safe_error(details.get("last_error")),
        "statement_timeout_detected": bool(details.get("statement_timeout_detected")),
        "warnings": _bounded_strings(details.get("warnings")),
        "blockers": _bounded_strings(details.get("blockers")),
    }


def _bounded_strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item)[:128] for item in value[:8]]
