from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Any
from uuid import uuid4

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from ape.config import AppConfig
from ape.db.models import StorageRetentionRun
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.inputs import StorageRetentionRunInput, WorkerHeartbeatInput
from ape.repositories.storage_retention import StorageRetentionRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.safety import SafetyAssessment

LOGGER = logging.getLogger(__name__)

RETENTION_SUCCESS = "success"
RETENTION_FAILED = "failed"
RETENTION_RUNNING = "running"
RETENTION_TIMED_OUT = "timed_out"


@dataclass(frozen=True)
class RetentionPolicy:
    table_name: str
    retention_config_key: str
    timestamp_expression: str
    delete_condition_sql: str
    raw_payload_retention_config_key: str | None = None
    raw_payload_condition_sql: str | None = None


RETENTION_POLICIES: tuple[RetentionPolicy, ...] = (
    RetentionPolicy(
        table_name="orderbook_snapshots",
        retention_config_key="storage_retention_orderbook_seconds",
        timestamp_expression="received_at",
        delete_condition_sql="received_at < :cutoff",
        raw_payload_retention_config_key="storage_retention_raw_payload_orderbook_seconds",
        raw_payload_condition_sql="raw_payload IS NOT NULL AND received_at < :cutoff",
    ),
    RetentionPolicy(
        table_name="public_trades",
        retention_config_key="storage_retention_public_trades_seconds",
        timestamp_expression="received_at",
        delete_condition_sql="received_at < :cutoff",
        raw_payload_retention_config_key="storage_retention_raw_payload_public_trades_seconds",
        raw_payload_condition_sql="raw_payload IS NOT NULL AND received_at < :cutoff",
    ),
    RetentionPolicy(
        table_name="reference_ticks",
        retention_config_key="storage_retention_reference_ticks_seconds",
        timestamp_expression="received_at",
        delete_condition_sql="received_at < :cutoff",
        raw_payload_retention_config_key="storage_retention_raw_payload_reference_ticks_seconds",
        raw_payload_condition_sql="raw_payload IS NOT NULL AND received_at < :cutoff",
    ),
    RetentionPolicy(
        table_name="worker_heartbeats",
        retention_config_key="storage_retention_worker_heartbeats_seconds",
        timestamp_expression="heartbeat_at",
        delete_condition_sql="heartbeat_at < :cutoff",
    ),
    RetentionPolicy(
        table_name="strategy_decisions",
        retention_config_key="storage_retention_strategy_decisions_seconds",
        timestamp_expression="evaluated_at",
        delete_condition_sql="evaluated_at < :cutoff",
    ),
    RetentionPolicy(
        table_name="strategy_dry_run_positions",
        retention_config_key="storage_retention_dry_run_positions_seconds",
        timestamp_expression="closed_at",
        delete_condition_sql="closed_at IS NOT NULL AND closed_at < :cutoff",
    ),
    RetentionPolicy(
        table_name="strategy_dry_run_events",
        retention_config_key="storage_retention_dry_run_events_seconds",
        timestamp_expression="occurred_at",
        delete_condition_sql="occurred_at < :cutoff",
    ),
    RetentionPolicy(
        table_name="markets",
        retention_config_key="storage_retention_markets_seconds",
        timestamp_expression="COALESCE(close_time, updated_at, created_at)",
        delete_condition_sql=(
            "(close_time IS NOT NULL AND close_time < :cutoff) "
            "OR (close_time IS NULL AND updated_at < :cutoff)"
        ),
    ),
)

RETENTION_TABLE_NAMES = tuple(policy.table_name for policy in RETENTION_POLICIES)


@dataclass(frozen=True)
class StorageRetentionResult:
    run_id: str
    started_at: datetime
    finished_at: datetime | None
    status: str
    dry_run: bool
    duration_ms: int | None
    deleted_rows: dict[str, int]
    raw_payload_stripped_rows: dict[str, int]
    table_row_counts_before: dict[str, int | None]
    table_row_counts_after: dict[str, int | None]
    table_sizes_before: dict[str, dict[str, int | None]]
    table_sizes_after: dict[str, dict[str, int | None]]
    warnings: list[str]
    blockers: list[str]
    error_type: str | None = None
    error_message: str | None = None

    def as_metadata(self) -> dict[str, Any]:
        return {
            "last_run_id": self.run_id,
            "last_started_at": _isoformat_or_none(self.started_at),
            "last_finished_at": _isoformat_or_none(self.finished_at),
            "last_status": self.status,
            "last_deleted_rows": self.deleted_rows,
            "last_raw_payload_stripped_rows": self.raw_payload_stripped_rows,
            "warnings": self.warnings,
            "blockers": self.blockers,
        }


@dataclass
class StorageRetentionRuntimeStatus:
    enabled: bool
    connection_state: str = "disabled"
    last_run_id: str | None = None
    last_started_at: datetime | None = None
    last_finished_at: datetime | None = None
    last_status: str | None = None
    last_deleted_rows: dict[str, int] = field(default_factory=dict)
    last_raw_payload_stripped_rows: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)

    def apply_result(self, result: StorageRetentionResult) -> None:
        self.connection_state = "idle" if result.status == RETENTION_SUCCESS else "error"
        self.last_run_id = result.run_id
        self.last_started_at = result.started_at
        self.last_finished_at = result.finished_at
        self.last_status = result.status
        self.last_deleted_rows = result.deleted_rows
        self.last_raw_payload_stripped_rows = result.raw_payload_stripped_rows
        self.warnings = result.warnings
        self.blockers = result.blockers

    def as_metadata(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "connection_state": self.connection_state,
            "last_run_id": self.last_run_id,
            "last_started_at": _isoformat_or_none(self.last_started_at),
            "last_finished_at": _isoformat_or_none(self.last_finished_at),
            "last_status": self.last_status,
            "last_deleted_rows": self.last_deleted_rows,
            "last_raw_payload_stripped_rows": self.last_raw_payload_stripped_rows,
            "warnings": self.warnings,
            "blockers": self.blockers,
        }


@dataclass(frozen=True)
class StorageTableStats:
    table_name: str
    row_count: int | None
    approximate_total_bytes: int | None
    approximate_table_bytes: int | None
    approximate_index_bytes: int | None
    approximate_toast_bytes: int | None
    approximate_total_pretty: str | None
    oldest_row_at: datetime | None
    newest_row_at: datetime | None
    raw_payload_non_null_count: int | None
    retention_seconds: int
    raw_payload_retention_seconds: int | None


@dataclass(frozen=True)
class StorageStatusSnapshot:
    enabled: bool
    worker_observed_enabled: bool | None
    connection_state: str
    checked_at: datetime
    database_configured: bool
    retention_config: dict[str, Any]
    latest_run_found: bool
    latest_run_id: str | None
    latest_run_status: str | None
    latest_run_started_at: datetime | None
    latest_run_finished_at: datetime | None
    latest_run_duration_ms: int | None
    latest_deleted_rows: dict[str, int]
    latest_raw_payload_stripped_rows: dict[str, int]
    latest_warnings: list[str]
    latest_blockers: list[str]
    table_stats: list[StorageTableStats]
    warnings: list[str]
    blockers: list[str]


class StorageRetentionWorker:
    def __init__(
        self,
        *,
        config: AppConfig,
        safety: SafetyAssessment,
        session_factory: sessionmaker[Session] | None,
        started_at: datetime,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config
        self.safety = safety
        self.session_factory = session_factory
        self.started_at = started_at
        self.now = now or (lambda: datetime.now(UTC))
        self.status = StorageRetentionRuntimeStatus(
            enabled=config.storage_retention_enabled
        )

    async def run(
        self,
        *,
        stop_event: threading.Event,
        max_iterations: int | None = None,
    ) -> None:
        if not self.config.storage_retention_enabled:
            self.status.connection_state = "disabled"
            self.status.warnings = ["storage_retention_disabled"]
            self.record_heartbeat()
            return

        iterations = 0
        while not stop_event.is_set():
            iterations += 1
            self.status.connection_state = "running"
            self.record_heartbeat()
            result = await asyncio.to_thread(
                run_storage_retention_once,
                self.config,
                self.session_factory,
                now=self.now,
            )
            self.status.apply_result(result)
            self.record_heartbeat()

            if max_iterations is not None and iterations >= max_iterations:
                return

            await _sleep_or_stop(
                stop_event,
                self.config.storage_retention_interval_seconds,
            )

    def record_heartbeat(self) -> None:
        if self.session_factory is None:
            return

        try:
            with self.session_factory() as session:
                repository = WorkerHeartbeatRepository(session)
                metadata = {
                    "mode": "storage_retention",
                    "storage": {"retention": self.status.as_metadata()},
                }
                latest_heartbeat = repository.get_latest_heartbeat("ape-worker")
                metadata_keys = _enabled_non_storage_metadata_keys(self.config)
                if latest_heartbeat is not None and metadata_keys:
                    _preserve_existing_worker_metadata(
                        metadata,
                        latest_heartbeat.metadata_,
                        keys=metadata_keys,
                    )
                repository.record_heartbeat(
                    WorkerHeartbeatInput(
                        service_name="ape-worker",
                        started_at=self.started_at,
                        heartbeat_at=self.now(),
                        app_mode=self.config.app_mode.value,
                        is_safe=self.safety.is_safe,
                        metadata=metadata,
                    )
                )
                session.commit()
        except SQLAlchemyError:
            LOGGER.warning("Storage retention heartbeat persistence failed.", exc_info=True)


def run_storage_retention_once(
    config: AppConfig,
    session_factory: sessionmaker[Session] | None,
    *,
    now: Callable[[], datetime] | None = None,
) -> StorageRetentionResult:
    clock = now or (lambda: datetime.now(UTC))
    started_at = _as_utc(clock())
    run_id = _run_id(started_at)
    empty_counts = _zero_counts()

    if session_factory is None:
        finished_at = _as_utc(clock())
        return StorageRetentionResult(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            status=RETENTION_FAILED,
            dry_run=config.storage_retention_dry_run,
            duration_ms=_duration_ms(started_at, finished_at),
            deleted_rows=empty_counts,
            raw_payload_stripped_rows=empty_counts.copy(),
            table_row_counts_before={},
            table_row_counts_after={},
            table_sizes_before={},
            table_sizes_after={},
            warnings=[],
            blockers=["database_not_configured_for_storage_retention"],
            error_type="DatabaseNotConfigured",
            error_message="DATABASE_URL is not configured.",
        )

    deleted_rows = _zero_counts()
    raw_payload_stripped_rows = _zero_counts()
    table_row_counts_before: dict[str, int | None] = {}
    table_row_counts_after: dict[str, int | None] = {}
    table_sizes_before: dict[str, dict[str, int | None]] = {}
    table_sizes_after: dict[str, dict[str, int | None]] = {}
    warnings: list[str] = []
    blockers: list[str] = []
    started_monotonic = monotonic()

    try:
        with session_factory() as session:
            repository = StorageRetentionRepository(session)
            timed_out = repository.mark_stale_running_runs(
                cutoff=started_at
                - timedelta(seconds=max(config.storage_retention_max_run_seconds, 1.0)),
                finished_at=started_at,
            )
            if timed_out:
                warnings.append("stale_running_retention_runs_marked_timed_out")
            repository.start_run(
                StorageRetentionRunInput(
                    run_id=run_id,
                    started_at=started_at,
                    status=RETENTION_RUNNING,
                    dry_run=config.storage_retention_dry_run,
                    deleted_rows=deleted_rows,
                    raw_payload_stripped_rows=raw_payload_stripped_rows,
                    warnings=warnings,
                    blockers=blockers,
                )
            )
            session.commit()

            try:
                table_row_counts_before = repository.row_counts(RETENTION_TABLE_NAMES)
                table_sizes_before = repository.table_sizes(RETENTION_TABLE_NAMES)
                _apply_row_retention(
                    config=config,
                    repository=repository,
                    deleted_rows=deleted_rows,
                    started_at=started_at,
                    started_monotonic=started_monotonic,
                    warnings=warnings,
                )
                _apply_raw_payload_retention(
                    config=config,
                    repository=repository,
                    raw_payload_stripped_rows=raw_payload_stripped_rows,
                    started_at=started_at,
                    started_monotonic=started_monotonic,
                    warnings=warnings,
                )
                table_row_counts_after = repository.row_counts(RETENTION_TABLE_NAMES)
                table_sizes_after = repository.table_sizes(RETENTION_TABLE_NAMES)
                finished_at = _as_utc(clock())
                result = StorageRetentionResult(
                    run_id=run_id,
                    started_at=started_at,
                    finished_at=finished_at,
                    status=RETENTION_SUCCESS,
                    dry_run=config.storage_retention_dry_run,
                    duration_ms=_duration_ms(started_at, finished_at),
                    deleted_rows=deleted_rows,
                    raw_payload_stripped_rows=raw_payload_stripped_rows,
                    table_row_counts_before=table_row_counts_before,
                    table_row_counts_after=table_row_counts_after,
                    table_sizes_before=table_sizes_before,
                    table_sizes_after=table_sizes_after,
                    warnings=_unique_strings(warnings),
                    blockers=blockers,
                )
            except Exception as exc:
                session.rollback()
                finished_at = _as_utc(clock())
                result = StorageRetentionResult(
                    run_id=run_id,
                    started_at=started_at,
                    finished_at=finished_at,
                    status=RETENTION_FAILED,
                    dry_run=config.storage_retention_dry_run,
                    duration_ms=_duration_ms(started_at, finished_at),
                    deleted_rows=deleted_rows,
                    raw_payload_stripped_rows=raw_payload_stripped_rows,
                    table_row_counts_before=table_row_counts_before,
                    table_row_counts_after=table_row_counts_after,
                    table_sizes_before=table_sizes_before,
                    table_sizes_after=table_sizes_after,
                    warnings=_unique_strings(warnings),
                    blockers=["storage_retention_run_failed"],
                    error_type=exc.__class__.__name__,
                    error_message=_safe_error_message(exc),
                )

            repository.finish_run(run_id, _result_to_input(result))
            session.commit()
            return result
    except Exception as exc:
        finished_at = _as_utc(clock())
        LOGGER.warning("Storage retention run failed before audit could persist.", exc_info=True)
        return StorageRetentionResult(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            status=RETENTION_FAILED,
            dry_run=config.storage_retention_dry_run,
            duration_ms=_duration_ms(started_at, finished_at),
            deleted_rows=deleted_rows,
            raw_payload_stripped_rows=raw_payload_stripped_rows,
            table_row_counts_before=table_row_counts_before,
            table_row_counts_after=table_row_counts_after,
            table_sizes_before=table_sizes_before,
            table_sizes_after=table_sizes_after,
            warnings=_unique_strings(warnings),
            blockers=["storage_retention_audit_persistence_failed"],
            error_type=exc.__class__.__name__,
            error_message=_safe_error_message(exc),
        )


def build_storage_status(
    config: AppConfig,
    *,
    now: datetime | None = None,
) -> StorageStatusSnapshot:
    checked_at = _as_utc(now or datetime.now(UTC))
    retention_config = storage_retention_config_summary(config)
    warnings: list[str] = []
    blockers: list[str] = []
    latest_run: StorageRetentionRun | None = None
    worker_metadata: dict[str, Any] | None = None
    table_stats: list[StorageTableStats] = []
    database_total_bytes: int | None = None

    if not config.database_url:
        return _storage_status_snapshot(
            config=config,
            checked_at=checked_at,
            database_configured=False,
            retention_config=retention_config,
            latest_run=None,
            worker_metadata=None,
            table_stats=[],
            database_total_bytes=None,
            warnings=warnings,
            blockers=blockers,
        )

    try:
        engine = create_engine_from_config(config)
        try:
            session_factory = create_session_factory(engine)
            with session_factory() as session:
                retention_repository = StorageRetentionRepository(session)
                heartbeat = WorkerHeartbeatRepository(session).get_latest_heartbeat(
                    "ape-worker"
                )
                worker_metadata = _storage_worker_metadata(
                    heartbeat.metadata_ if heartbeat else None
                )
                latest_run = retention_repository.get_latest_run()
                table_stats = [
                    _table_stats_for_policy(
                        config=config,
                        repository=retention_repository,
                        policy=policy,
                        warnings=warnings,
                    )
                    for policy in RETENTION_POLICIES
                ]
                database_total_bytes = retention_repository.database_total_bytes()
        finally:
            engine.dispose()
    except SQLAlchemyError:
        warnings.append("storage_status_database_error")

    return _storage_status_snapshot(
        config=config,
        checked_at=checked_at,
        database_configured=True,
        retention_config=retention_config,
        latest_run=latest_run,
        worker_metadata=worker_metadata,
        table_stats=table_stats,
        database_total_bytes=database_total_bytes,
        warnings=warnings,
        blockers=blockers,
    )


def storage_retention_config_summary(config: AppConfig) -> dict[str, Any]:
    return {
        "enabled": config.storage_retention_enabled,
        "interval_seconds": config.storage_retention_interval_seconds,
        "batch_size": config.storage_retention_batch_size,
        "max_run_seconds": config.storage_retention_max_run_seconds,
        "dry_run": config.storage_retention_dry_run,
        "retention_seconds": {
            policy.table_name: _policy_retention_seconds(config, policy)
            for policy in RETENTION_POLICIES
        },
        "raw_payload_retention_seconds": {
            policy.table_name: _policy_raw_payload_retention_seconds(config, policy)
            for policy in RETENTION_POLICIES
            if policy.raw_payload_retention_config_key is not None
        },
        "status_warn_bytes": config.storage_retention_status_warn_bytes,
        "status_critical_bytes": config.storage_retention_status_critical_bytes,
    }


def _apply_row_retention(
    *,
    config: AppConfig,
    repository: StorageRetentionRepository,
    deleted_rows: dict[str, int],
    started_at: datetime,
    started_monotonic: float,
    warnings: list[str],
) -> None:
    for policy in RETENTION_POLICIES:
        if _max_run_seconds_reached(config, started_monotonic, warnings):
            return
        cutoff = started_at - timedelta(seconds=_policy_retention_seconds(config, policy))
        if config.storage_retention_dry_run:
            deleted_rows[policy.table_name] = repository.count_matching(
                table_name=policy.table_name,
                condition_sql=policy.delete_condition_sql,
                parameters={"cutoff": cutoff},
            )
            continue

        while not _max_run_seconds_reached(config, started_monotonic, warnings):
            deleted = repository.delete_batch(
                table_name=policy.table_name,
                condition_sql=policy.delete_condition_sql,
                parameters={"cutoff": cutoff},
                batch_size=config.storage_retention_batch_size,
            )
            if deleted <= 0:
                break
            deleted_rows[policy.table_name] += deleted
            repository.session.commit()


def _apply_raw_payload_retention(
    *,
    config: AppConfig,
    repository: StorageRetentionRepository,
    raw_payload_stripped_rows: dict[str, int],
    started_at: datetime,
    started_monotonic: float,
    warnings: list[str],
) -> None:
    for policy in RETENTION_POLICIES:
        if (
            policy.raw_payload_retention_config_key is None
            or policy.raw_payload_condition_sql is None
        ):
            continue
        if _max_run_seconds_reached(config, started_monotonic, warnings):
            return
        cutoff = started_at - timedelta(
            seconds=_policy_raw_payload_retention_seconds(config, policy) or 0
        )
        if config.storage_retention_dry_run:
            raw_payload_stripped_rows[policy.table_name] = repository.count_matching(
                table_name=policy.table_name,
                condition_sql=policy.raw_payload_condition_sql,
                parameters={"cutoff": cutoff},
            )
            continue

        while not _max_run_seconds_reached(config, started_monotonic, warnings):
            updated = repository.strip_raw_payload_batch(
                table_name=policy.table_name,
                condition_sql=policy.raw_payload_condition_sql,
                parameters={"cutoff": cutoff},
                batch_size=config.storage_retention_batch_size,
            )
            if updated <= 0:
                break
            raw_payload_stripped_rows[policy.table_name] += updated
            repository.session.commit()


def _storage_status_snapshot(
    *,
    config: AppConfig,
    checked_at: datetime,
    database_configured: bool,
    retention_config: dict[str, Any],
    latest_run: StorageRetentionRun | None,
    worker_metadata: dict[str, Any] | None,
    table_stats: list[StorageTableStats],
    database_total_bytes: int | None,
    warnings: list[str],
    blockers: list[str],
) -> StorageStatusSnapshot:
    worker_observed_enabled = (
        None if worker_metadata is None else bool(worker_metadata.get("enabled"))
    )
    enabled = (
        config.storage_retention_enabled
        if worker_observed_enabled is None
        else worker_observed_enabled
    )
    worker_warnings = _string_list(worker_metadata.get("warnings") if worker_metadata else [])
    worker_blockers = _string_list(worker_metadata.get("blockers") if worker_metadata else [])
    warnings = [*warnings, *worker_warnings]
    blockers = [*blockers, *worker_blockers]

    if worker_metadata is not None:
        connection_state = str(worker_metadata.get("connection_state") or "unknown")
    elif not enabled:
        connection_state = "disabled"
    elif not database_configured:
        connection_state = "not_configured"
    else:
        connection_state = "unknown"

    if database_total_bytes is not None:
        if database_total_bytes >= config.storage_retention_status_critical_bytes:
            blockers.append("database_size_critical")
        elif database_total_bytes >= config.storage_retention_status_warn_bytes:
            warnings.append("database_size_warning")

    if latest_run is not None and latest_run.status in {RETENTION_FAILED, RETENTION_TIMED_OUT}:
        warnings.append("latest_storage_retention_run_failed")
    if enabled and database_configured:
        if latest_run is None:
            warnings.append("storage_retention_has_not_run")
        elif latest_run.finished_at is None and latest_run.status == RETENTION_RUNNING:
            age_seconds = (
                checked_at - _as_utc(latest_run.started_at)
            ).total_seconds()
            stale_running_after_seconds = max(config.storage_retention_max_run_seconds, 1.0)
            if age_seconds > stale_running_after_seconds:
                warnings.append("storage_retention_running_run_stale")
        elif latest_run.finished_at is not None:
            age_seconds = (
                checked_at - _as_utc(latest_run.finished_at)
            ).total_seconds()
            stale_after_seconds = max(config.storage_retention_interval_seconds * 2, 1.0)
            if age_seconds > stale_after_seconds:
                warnings.append("storage_retention_latest_run_stale")

    if table_stats and any(stat.row_count is None for stat in table_stats):
        warnings.append("storage_table_stats_incomplete")

    latest_warnings = _string_list(latest_run.warnings if latest_run else [])
    latest_blockers = _string_list(latest_run.blockers if latest_run else [])

    return StorageStatusSnapshot(
        enabled=enabled,
        worker_observed_enabled=worker_observed_enabled,
        connection_state=connection_state,
        checked_at=checked_at,
        database_configured=database_configured,
        retention_config=retention_config,
        latest_run_found=latest_run is not None,
        latest_run_id=latest_run.run_id if latest_run else None,
        latest_run_status=latest_run.status if latest_run else None,
        latest_run_started_at=latest_run.started_at if latest_run else None,
        latest_run_finished_at=latest_run.finished_at if latest_run else None,
        latest_run_duration_ms=latest_run.duration_ms if latest_run else None,
        latest_deleted_rows=_int_dict(latest_run.deleted_rows if latest_run else None),
        latest_raw_payload_stripped_rows=_int_dict(
            latest_run.raw_payload_stripped_rows if latest_run else None
        ),
        latest_warnings=latest_warnings,
        latest_blockers=latest_blockers,
        table_stats=table_stats,
        warnings=_unique_strings(warnings),
        blockers=_unique_strings(blockers),
    )


def _table_stats_for_policy(
    *,
    config: AppConfig,
    repository: StorageRetentionRepository,
    policy: RetentionPolicy,
    warnings: list[str],
) -> StorageTableStats:
    try:
        size = repository.table_size(policy.table_name)
        oldest_row_at, newest_row_at = repository.oldest_newest(
            table_name=policy.table_name,
            timestamp_expression=policy.timestamp_expression,
        )
        raw_payload_non_null_count = (
            repository.raw_payload_non_null_count(policy.table_name)
            if policy.raw_payload_retention_config_key is not None
            else None
        )
        row_count = repository.approximate_row_count(policy.table_name)
    except SQLAlchemyError:
        warnings.append(f"{policy.table_name}_stats_unavailable")
        size = {}
        oldest_row_at = None
        newest_row_at = None
        raw_payload_non_null_count = None
        row_count = None

    total_bytes = size.get("approximate_total_bytes")
    return StorageTableStats(
        table_name=policy.table_name,
        row_count=row_count,
        approximate_total_bytes=total_bytes,
        approximate_table_bytes=size.get("approximate_table_bytes"),
        approximate_index_bytes=size.get("approximate_index_bytes"),
        approximate_toast_bytes=size.get("approximate_toast_bytes"),
        approximate_total_pretty=_pretty_bytes(total_bytes),
        oldest_row_at=oldest_row_at,
        newest_row_at=newest_row_at,
        raw_payload_non_null_count=raw_payload_non_null_count,
        retention_seconds=_policy_retention_seconds(config, policy),
        raw_payload_retention_seconds=_policy_raw_payload_retention_seconds(config, policy),
    )


def _policy_retention_seconds(config: AppConfig, policy: RetentionPolicy) -> int:
    return int(getattr(config, policy.retention_config_key))


def _policy_raw_payload_retention_seconds(
    config: AppConfig,
    policy: RetentionPolicy,
) -> int | None:
    if policy.raw_payload_retention_config_key is None:
        return None
    return int(getattr(config, policy.raw_payload_retention_config_key))


def _max_run_seconds_reached(
    config: AppConfig,
    started_monotonic: float,
    warnings: list[str],
) -> bool:
    reached = (monotonic() - started_monotonic) >= config.storage_retention_max_run_seconds
    if reached and "storage_retention_max_run_seconds_reached" not in warnings:
        warnings.append("storage_retention_max_run_seconds_reached")
    return reached


def _result_to_input(result: StorageRetentionResult) -> StorageRetentionRunInput:
    return StorageRetentionRunInput(
        run_id=result.run_id,
        started_at=result.started_at,
        finished_at=result.finished_at,
        status=result.status,
        dry_run=result.dry_run,
        duration_ms=result.duration_ms,
        deleted_rows=result.deleted_rows,
        raw_payload_stripped_rows=result.raw_payload_stripped_rows,
        table_row_counts_before=result.table_row_counts_before,
        table_row_counts_after=result.table_row_counts_after,
        table_sizes_before=result.table_sizes_before,
        table_sizes_after=result.table_sizes_after,
        warnings=result.warnings,
        blockers=result.blockers,
        error_type=result.error_type,
        error_message=result.error_message,
    )


def _run_id(started_at: datetime) -> str:
    timestamp = started_at.strftime("%Y%m%dT%H%M%S%fZ")
    return f"storage-retention-{timestamp}-{uuid4().hex[:8]}"


def _zero_counts() -> dict[str, int]:
    return {table_name: 0 for table_name in RETENTION_TABLE_NAMES}


def _duration_ms(started_at: datetime, finished_at: datetime) -> int:
    return max(0, int((_as_utc(finished_at) - _as_utc(started_at)).total_seconds() * 1000))


def _storage_worker_metadata(metadata: Any) -> dict[str, Any] | None:
    if not isinstance(metadata, dict):
        return None
    storage_metadata = metadata.get("storage")
    if not isinstance(storage_metadata, dict):
        return None
    retention_metadata = storage_metadata.get("retention")
    return retention_metadata if isinstance(retention_metadata, dict) else None


def _enabled_non_storage_metadata_keys(config: AppConfig) -> tuple[str, ...]:
    keys: list[str] = []
    if config.kalshi_ws_enabled:
        keys.append("ws")
    if (
        config.kalshi_cfbenchmarks_enabled
        and config.kalshi_cfbenchmarks_subscribe_on_worker
    ):
        keys.append("reference")
    if config.strategy_observer_enabled:
        keys.append("strategy")
    return tuple(keys)


def _preserve_existing_worker_metadata(
    metadata: dict[str, Any],
    existing_metadata: Any,
    *,
    keys: tuple[str, ...],
) -> None:
    if not isinstance(existing_metadata, dict):
        return
    for key in keys:
        if key not in metadata and isinstance(existing_metadata.get(key), dict):
            metadata[key] = existing_metadata[key]


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _int_dict(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    return {
        str(key): int(item)
        for key, item in value.items()
        if isinstance(item, int | float)
    }


def _unique_strings(values: list[str]) -> list[str]:
    unique: list[str] = []
    for value in values:
        if value not in unique:
            unique.append(value)
    return unique


def _pretty_bytes(value: int | None) -> str | None:
    if value is None:
        return None
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    amount = float(max(0, value))
    unit = units[0]
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            break
        amount /= 1024
    if unit == "B":
        return f"{int(amount)} B"
    return f"{amount:.2f} {unit}"


def _safe_error_message(exc: Exception) -> str:
    message = str(exc).replace("\n", " ").strip()
    return message[:500]


async def _sleep_or_stop(stop_event: threading.Event, seconds: float) -> None:
    deadline = datetime.now(UTC).timestamp() + seconds
    while not stop_event.is_set() and datetime.now(UTC).timestamp() < deadline:
        await asyncio.sleep(min(0.1, seconds))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _isoformat_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _as_utc(value).isoformat().replace("+00:00", "Z")
