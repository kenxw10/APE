from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from math import isfinite
from time import monotonic, sleep
from typing import Any
from uuid import uuid4

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from ape.config import AppConfig
from ape.db.models import StorageRetentionRun, WorkerHeartbeat
from ape.db.session import create_engine_from_config, create_session_factory
from ape.repositories.inputs import StorageRetentionRunInput, WorkerHeartbeatInput
from ape.repositories.storage_retention import StorageRetentionRepository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.safety import SafetyAssessment
from ape.worker.services import (
    WORKER_SERVICE_AGGREGATE,
    WORKER_SERVICE_STORAGE_RETENTION,
    WORKER_SERVICE_STORAGE_RETENTION_LEGACY,
)

LOGGER = logging.getLogger(__name__)

RETENTION_SUCCESS = "success"
RETENTION_SUCCESS_PARTIAL = "success_partial"
RETENTION_FAILED = "failed"
RETENTION_RUNNING = "running"
RETENTION_TIMED_OUT = "timed_out"
RETENTION_SUCCESS_STATES = {RETENTION_SUCCESS, RETENTION_SUCCESS_PARTIAL}
RETENTION_ROW_CAP_REACHED_WARNING = "storage_retention_max_delete_rows_per_table_reached"
STORAGE_LIVENESS_SOURCE_COMPONENT = "component"
STORAGE_LIVENESS_SOURCE_LEGACY_FALLBACK = "legacy_aggregate_fallback"
STORAGE_LIVENESS_SOURCE_MISSING = "missing"
STORAGE_LIVENESS_LEGACY_FALLBACK_WARNING = "feed_liveness_legacy_aggregate_fallback"
STORAGE_WORKER_ROLE_MAINTENANCE = "maintenance"


@dataclass(frozen=True)
class RetentionPolicy:
    table_name: str
    retention_config_key: str
    timestamp_expression: str
    delete_condition_sql: str
    raw_payload_retention_config_key: str | None = None
    raw_payload_condition_sql: str | None = None


@dataclass(frozen=True)
class StorageStatusTable:
    table_name: str
    timestamp_expression: str
    retention_config_key: str | None = None
    raw_payload_retention_config_key: str | None = None


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
        table_name="kalshi_ws_protocol_events",
        retention_config_key="storage_retention_kalshi_ws_protocol_events_seconds",
        timestamp_expression="created_at",
        delete_condition_sql="created_at < :cutoff",
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
        table_name="strategy_feature_snapshots",
        retention_config_key="storage_retention_strategy_feature_snapshots_seconds",
        timestamp_expression="evaluated_at",
        delete_condition_sql="evaluated_at < :cutoff",
    ),
    RetentionPolicy(
        table_name="strategy_trade_intents",
        retention_config_key="storage_retention_strategy_trade_intents_seconds",
        timestamp_expression="COALESCE(resolved_at, expires_at)",
        delete_condition_sql=(
            "(resolved_at IS NOT NULL AND resolved_at < :cutoff) "
            "OR (resolved_at IS NULL AND expires_at < :cutoff)"
        ),
    ),
    RetentionPolicy(
        table_name="strategy_position_marks",
        retention_config_key="storage_retention_strategy_position_marks_seconds",
        timestamp_expression="marked_at",
        delete_condition_sql="marked_at < :cutoff",
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
    RetentionPolicy(
        table_name="research_replay_events",
        retention_config_key="storage_retention_research_replay_events_seconds",
        timestamp_expression="event_time",
        delete_condition_sql="event_time < :cutoff",
    ),
    RetentionPolicy(
        table_name="research_replay_trades",
        retention_config_key="storage_retention_research_replay_trades_seconds",
        timestamp_expression="created_at",
        delete_condition_sql="created_at < :cutoff",
    ),
)

RETENTION_TABLE_NAMES = tuple(policy.table_name for policy in RETENTION_POLICIES)
STATUS_TABLES: tuple[StorageStatusTable, ...] = tuple(
    StorageStatusTable(
        table_name=policy.table_name,
        timestamp_expression=policy.timestamp_expression,
        retention_config_key=policy.retention_config_key,
        raw_payload_retention_config_key=policy.raw_payload_retention_config_key,
    )
    for policy in RETENTION_POLICIES
) + (
    StorageStatusTable(
        table_name="strategy_position_outcomes",
        timestamp_expression="closed_at",
    ),
    StorageStatusTable(table_name="research_market_outcomes", timestamp_expression="updated_at"),
    StorageStatusTable(table_name="research_replay_runs", timestamp_expression="started_at"),
    StorageStatusTable(table_name="calibration_runs", timestamp_expression="started_at"),
    StorageStatusTable(table_name="research_candidates", timestamp_expression="updated_at"),
    StorageStatusTable(table_name="research_governance_events", timestamp_expression="created_at"),
)


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
    budget_exhausted: bool = False
    tables_processed: list[str] = field(default_factory=list)
    tables_skipped: list[str] = field(default_factory=list)

    def as_metadata(self) -> dict[str, Any]:
        return {
            "last_run_id": self.run_id,
            "last_started_at": _isoformat_or_none(self.started_at),
            "last_finished_at": _isoformat_or_none(self.finished_at),
            "last_status": self.status,
            "last_deleted_rows": self.deleted_rows,
            "last_raw_payload_stripped_rows": self.raw_payload_stripped_rows,
            "last_budget_exhausted": self.budget_exhausted,
            "last_tables_processed": self.tables_processed,
            "last_tables_skipped": self.tables_skipped,
            "warnings": self.warnings,
            "blockers": self.blockers,
        }


@dataclass
class StorageRetentionRuntimeStatus:
    enabled: bool
    interval_seconds: float | None = None
    connection_state: str = "disabled"
    last_run_id: str | None = None
    last_started_at: datetime | None = None
    last_finished_at: datetime | None = None
    last_status: str | None = None
    last_deleted_rows: dict[str, int] = field(default_factory=dict)
    last_raw_payload_stripped_rows: dict[str, int] = field(default_factory=dict)
    last_budget_exhausted: bool = False
    last_tables_processed: list[str] = field(default_factory=list)
    last_tables_skipped: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)

    def apply_result(self, result: StorageRetentionResult) -> None:
        self.connection_state = "idle" if result.status in RETENTION_SUCCESS_STATES else "error"
        self.last_run_id = result.run_id
        self.last_started_at = result.started_at
        self.last_finished_at = result.finished_at
        self.last_status = result.status
        self.last_deleted_rows = result.deleted_rows
        self.last_raw_payload_stripped_rows = result.raw_payload_stripped_rows
        self.last_budget_exhausted = result.budget_exhausted
        self.last_tables_processed = result.tables_processed
        self.last_tables_skipped = result.tables_skipped
        self.warnings = result.warnings
        self.blockers = result.blockers

    def as_metadata(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "interval_seconds": self.interval_seconds,
            "worker_role": STORAGE_WORKER_ROLE_MAINTENANCE,
            "connection_state": self.connection_state,
            "last_run_id": self.last_run_id,
            "last_started_at": _isoformat_or_none(self.last_started_at),
            "last_finished_at": _isoformat_or_none(self.last_finished_at),
            "last_status": self.last_status,
            "last_deleted_rows": self.last_deleted_rows,
            "last_raw_payload_stripped_rows": self.last_raw_payload_stripped_rows,
            "last_budget_exhausted": self.last_budget_exhausted,
            "last_tables_processed": self.last_tables_processed,
            "last_tables_skipped": self.last_tables_skipped,
            "warnings": self.warnings,
            "blockers": self.blockers,
        }


@dataclass(frozen=True)
class StorageTableStats:
    table_name: str
    timestamp_basis: str
    row_count: int | None
    approximate_total_bytes: int | None
    approximate_table_bytes: int | None
    approximate_index_bytes: int | None
    approximate_toast_bytes: int | None
    approximate_total_pretty: str | None
    oldest_row_at: datetime | None
    newest_row_at: datetime | None
    raw_payload_non_null_count: int | None
    retention_seconds: int | None
    raw_payload_retention_seconds: int | None


@dataclass(frozen=True)
class StorageRetentionLiveness:
    source: str
    metadata: dict[str, Any] | None
    heartbeat_at: datetime | None
    heartbeat_age_ms: int | None
    started_at: datetime | None
    component_heartbeat_at: datetime | None
    component_heartbeat_age_ms: int | None
    latest_component_heartbeat_mode: str | None
    latest_aggregate_heartbeat_mode: str | None
    liveness_source_mismatch: bool
    warnings: list[str]
    worker_role: str | None


@dataclass(frozen=True)
class StorageStatusSnapshot:
    enabled: bool
    configured_enabled: bool
    worker_observed_enabled: bool | None
    effective_enabled: bool
    dry_run: bool
    connection_state: str
    checked_at: datetime
    database_configured: bool
    liveness_source: str
    worker_role: str | None
    worker_heartbeat_at: datetime | None
    worker_heartbeat_age_ms: int | None
    worker_started_at: datetime | None
    component_heartbeat_at: datetime | None
    component_heartbeat_age_ms: int | None
    latest_component_heartbeat_mode: str | None
    latest_aggregate_heartbeat_mode: str | None
    liveness_source_mismatch: bool
    worker_heartbeat_stale: bool
    retention_config: dict[str, Any]
    latest_run_found: bool
    latest_run_id: str | None
    latest_run_status: str | None
    latest_run_started_at: datetime | None
    latest_run_finished_at: datetime | None
    latest_run_duration_ms: int | None
    latest_deleted_rows: dict[str, int]
    latest_raw_payload_stripped_rows: dict[str, int]
    latest_run_budget_exhausted: bool
    latest_tables_processed: list[str]
    latest_tables_skipped: list[str]
    latest_total_deleted_rows: int
    latest_total_raw_payload_stripped_rows: int
    latest_db_statement_timeout_count: int
    latest_db_lock_timeout_count: int
    latest_db_error_count: int
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
            enabled=config.storage_retention_enabled,
            interval_seconds=config.storage_retention_interval_seconds,
        )
        self._last_storage_heartbeat_at: datetime | None = None

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
                heartbeat_at = self.now()
                repository.record_heartbeat(
                    WorkerHeartbeatInput(
                        service_name=WORKER_SERVICE_STORAGE_RETENTION,
                        started_at=self.started_at,
                        heartbeat_at=heartbeat_at,
                        app_mode=self.config.app_mode.value,
                        is_safe=self.safety.is_safe,
                        metadata=metadata,
                    )
                )
                latest_heartbeat = repository.get_latest_heartbeat(WORKER_SERVICE_AGGREGATE)
                metadata_keys = _enabled_non_storage_metadata_keys(self.config)
                if latest_heartbeat is not None and metadata_keys:
                    _preserve_existing_worker_metadata(
                        metadata,
                        latest_heartbeat.metadata_,
                        keys=metadata_keys,
                    )
                repository.record_heartbeat(
                    WorkerHeartbeatInput(
                        service_name=WORKER_SERVICE_AGGREGATE,
                        started_at=self.started_at,
                        heartbeat_at=heartbeat_at,
                        app_mode=self.config.app_mode.value,
                        is_safe=self.safety.is_safe,
                        metadata=metadata,
                    )
                )
                session.commit()
                self._last_storage_heartbeat_at = heartbeat_at
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
            budget_exhausted=False,
            tables_processed=[],
            tables_skipped=list(RETENTION_TABLE_NAMES),
        )

    deleted_rows = _zero_counts()
    raw_payload_stripped_rows = _zero_counts()
    mutated_rows_by_table = _zero_counts()
    table_row_counts_before: dict[str, int | None] = {}
    table_row_counts_after: dict[str, int | None] = {}
    table_sizes_before: dict[str, dict[str, int | None]] = {}
    table_sizes_after: dict[str, dict[str, int | None]] = {}
    warnings: list[str] = []
    blockers: list[str] = []
    started_monotonic = monotonic()
    selected_policies: tuple[RetentionPolicy, ...] = ()
    processed_table_names: list[str] = []

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
            previous_run = repository.get_latest_run()
            selected_policies, _ = _selected_retention_policies(
                config=config,
                warnings=warnings,
                previous_run=previous_run,
            )
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

                def record_policy_started(policy: RetentionPolicy) -> None:
                    table_name = policy.table_name
                    if table_name in processed_table_names:
                        return
                    table_row_counts_before.update(repository.row_counts((table_name,)))
                    table_sizes_before.update(repository.table_sizes((table_name,)))
                    processed_table_names.append(table_name)

                _apply_row_retention(
                    config=config,
                    repository=repository,
                    policies=selected_policies,
                    deleted_rows=deleted_rows,
                    mutated_rows_by_table=mutated_rows_by_table,
                    started_at=started_at,
                    started_monotonic=started_monotonic,
                    warnings=warnings,
                    on_policy_started=record_policy_started,
                )
                _apply_raw_payload_retention(
                    config=config,
                    repository=repository,
                    policies=selected_policies,
                    raw_payload_stripped_rows=raw_payload_stripped_rows,
                    mutated_rows_by_table=mutated_rows_by_table,
                    started_at=started_at,
                    started_monotonic=started_monotonic,
                    warnings=warnings,
                    on_policy_started=record_policy_started,
                )
                processed_table_names_tuple = tuple(processed_table_names)
                table_row_counts_after = repository.row_counts(processed_table_names_tuple)
                table_sizes_after = repository.table_sizes(processed_table_names_tuple)
                finished_at = _as_utc(clock())
                budget_exhausted = _retention_budget_exhausted(warnings)
                result = StorageRetentionResult(
                    run_id=run_id,
                    started_at=started_at,
                    finished_at=finished_at,
                    status=(RETENTION_SUCCESS_PARTIAL if budget_exhausted else RETENTION_SUCCESS),
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
                    budget_exhausted=budget_exhausted,
                    tables_processed=list(processed_table_names),
                    tables_skipped=_retention_tables_skipped(processed_table_names),
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
                    budget_exhausted=_retention_budget_exhausted(warnings),
                    tables_processed=list(processed_table_names),
                    tables_skipped=_retention_tables_skipped(processed_table_names),
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
            budget_exhausted=_retention_budget_exhausted(warnings),
            tables_processed=list(processed_table_names),
            tables_skipped=_retention_tables_skipped(processed_table_names),
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
    liveness = _empty_storage_liveness(checked_at)
    table_stats: list[StorageTableStats] = []
    database_total_bytes: int | None = None

    if not config.database_url:
        return _storage_status_snapshot(
            config=config,
            checked_at=checked_at,
            database_configured=False,
            retention_config=retention_config,
            latest_run=None,
            liveness=liveness,
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
                heartbeat_repository = WorkerHeartbeatRepository(session)
                liveness = _load_storage_liveness(
                    heartbeat_repository,
                    checked_at=checked_at,
                )
                warnings.extend(liveness.warnings)
                latest_run = retention_repository.get_latest_run()
                table_stats = [
                    _table_stats_for_policy(
                        config=config,
                        repository=retention_repository,
                        policy=policy,
                        warnings=warnings,
                    )
                    for policy in STATUS_TABLES
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
        liveness=liveness,
        table_stats=table_stats,
        database_total_bytes=database_total_bytes,
        warnings=warnings,
        blockers=blockers,
    )


def storage_retention_config_summary(config: AppConfig) -> dict[str, Any]:
    return {
        "enabled": config.storage_retention_enabled,
        "configured_enabled": config.storage_retention_enabled,
        "worker_observed_enabled": None,
        "effective_enabled": config.storage_retention_enabled,
        "interval_seconds": config.storage_retention_interval_seconds,
        "batch_size": config.storage_retention_batch_size,
        "max_run_seconds": config.storage_retention_max_run_seconds,
        "dry_run": config.storage_retention_dry_run,
        "inter_table_sleep_ms": config.storage_retention_inter_table_sleep_ms,
        "batch_sleep_ms": config.storage_retention_batch_sleep_ms,
        "max_tables_per_run": config.storage_retention_max_tables_per_run,
        "max_delete_rows_per_table": config.storage_retention_max_delete_rows_per_table,
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
    policies: tuple[RetentionPolicy, ...],
    deleted_rows: dict[str, int],
    mutated_rows_by_table: dict[str, int],
    started_at: datetime,
    started_monotonic: float,
    warnings: list[str],
    on_policy_started: Callable[[RetentionPolicy], None],
) -> None:
    for table_index, policy in enumerate(policies):
        if _max_run_seconds_reached(config, started_monotonic, warnings):
            return
        on_policy_started(policy)
        cutoff = started_at - timedelta(seconds=_policy_retention_seconds(config, policy))
        if config.storage_retention_dry_run:
            deleted_rows[policy.table_name] = repository.count_matching(
                table_name=policy.table_name,
                condition_sql=policy.delete_condition_sql,
                parameters={"cutoff": cutoff},
            )
            _sleep_between_retention_tables(config, table_index, policies)
            continue

        while not _max_run_seconds_reached(config, started_monotonic, warnings):
            batch_size = _retention_batch_size(
                config,
                mutated_rows_by_table[policy.table_name],
            )
            if batch_size <= 0:
                break
            deleted = repository.delete_batch(
                table_name=policy.table_name,
                condition_sql=policy.delete_condition_sql,
                parameters={"cutoff": cutoff},
                batch_size=batch_size,
            )
            if deleted <= 0:
                break
            deleted_rows[policy.table_name] += deleted
            mutated_rows_by_table[policy.table_name] += deleted
            repository.session.commit()
            _retention_sleep_ms(config.storage_retention_batch_sleep_ms)
        _mark_row_cap_exhausted_if_needed(
            config=config,
            repository=repository,
            policy=policy,
            condition_sql=policy.delete_condition_sql,
            parameters={"cutoff": cutoff},
            rows_processed=mutated_rows_by_table[policy.table_name],
            warnings=warnings,
        )
        _sleep_between_retention_tables(config, table_index, policies)


def _apply_raw_payload_retention(
    *,
    config: AppConfig,
    repository: StorageRetentionRepository,
    policies: tuple[RetentionPolicy, ...],
    raw_payload_stripped_rows: dict[str, int],
    mutated_rows_by_table: dict[str, int],
    started_at: datetime,
    started_monotonic: float,
    warnings: list[str],
    on_policy_started: Callable[[RetentionPolicy], None],
) -> None:
    for table_index, policy in enumerate(policies):
        if (
            policy.raw_payload_retention_config_key is None
            or policy.raw_payload_condition_sql is None
        ):
            _sleep_between_retention_tables(config, table_index, policies)
            continue
        if _max_run_seconds_reached(config, started_monotonic, warnings):
            return
        on_policy_started(policy)
        cutoff = started_at - timedelta(
            seconds=_policy_raw_payload_retention_seconds(config, policy) or 0
        )
        if config.storage_retention_dry_run:
            raw_payload_stripped_rows[policy.table_name] = repository.count_matching(
                table_name=policy.table_name,
                condition_sql=policy.raw_payload_condition_sql,
                parameters={"cutoff": cutoff},
            )
            _sleep_between_retention_tables(config, table_index, policies)
            continue

        while not _max_run_seconds_reached(config, started_monotonic, warnings):
            batch_size = _retention_batch_size(
                config,
                mutated_rows_by_table[policy.table_name],
            )
            if batch_size <= 0:
                break
            updated = repository.strip_raw_payload_batch(
                table_name=policy.table_name,
                condition_sql=policy.raw_payload_condition_sql,
                parameters={"cutoff": cutoff},
                batch_size=batch_size,
            )
            if updated <= 0:
                break
            raw_payload_stripped_rows[policy.table_name] += updated
            mutated_rows_by_table[policy.table_name] += updated
            repository.session.commit()
            _retention_sleep_ms(config.storage_retention_batch_sleep_ms)
        _mark_row_cap_exhausted_if_needed(
            config=config,
            repository=repository,
            policy=policy,
            condition_sql=policy.raw_payload_condition_sql,
            parameters={"cutoff": cutoff},
            rows_processed=mutated_rows_by_table[policy.table_name],
            warnings=warnings,
        )
        _sleep_between_retention_tables(config, table_index, policies)


def _storage_status_snapshot(
    *,
    config: AppConfig,
    checked_at: datetime,
    database_configured: bool,
    retention_config: dict[str, Any],
    latest_run: StorageRetentionRun | None,
    liveness: StorageRetentionLiveness,
    table_stats: list[StorageTableStats],
    database_total_bytes: int | None,
    warnings: list[str],
    blockers: list[str],
) -> StorageStatusSnapshot:
    worker_metadata = liveness.metadata
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
    worker_heartbeat_stale = _is_storage_worker_heartbeat_stale(
        enabled=enabled and database_configured,
        heartbeat_at=liveness.heartbeat_at,
        checked_at=checked_at,
        stale_after_seconds=_worker_heartbeat_stale_after_seconds(
            config,
            worker_metadata,
        ),
    )
    if worker_heartbeat_stale:
        warnings.append("storage_retention_worker_heartbeat_stale")

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
            age_seconds = (checked_at - _as_utc(latest_run.started_at)).total_seconds()
            stale_running_after_seconds = max(config.storage_retention_max_run_seconds, 1.0)
            if age_seconds > stale_running_after_seconds:
                warnings.append("storage_retention_running_run_stale")
        elif latest_run.finished_at is not None:
            age_seconds = (checked_at - _as_utc(latest_run.finished_at)).total_seconds()
            stale_after_seconds = max(config.storage_retention_interval_seconds * 2, 1.0)
            if age_seconds > stale_after_seconds:
                warnings.append("storage_retention_latest_run_stale")

    if table_stats and any(stat.row_count is None for stat in table_stats):
        warnings.append("storage_table_stats_incomplete")

    latest_warnings = _string_list(latest_run.warnings if latest_run else [])
    latest_blockers = _string_list(latest_run.blockers if latest_run else [])
    latest_deleted_rows = _int_dict(latest_run.deleted_rows if latest_run else None)
    latest_raw_payload_stripped_rows = _int_dict(
        latest_run.raw_payload_stripped_rows if latest_run else None
    )
    latest_tables_processed = _latest_tables_processed(latest_run)
    latest_tables_skipped = _latest_tables_skipped(latest_tables_processed)
    latest_run_budget_exhausted = _latest_run_budget_exhausted(
        latest_run,
        latest_warnings,
    )
    timeout_counts = _latest_db_timeout_counts(latest_run)
    effective_retention_config = {
        **retention_config,
        "enabled": enabled,
        "configured_enabled": config.storage_retention_enabled,
        "worker_observed_enabled": worker_observed_enabled,
        "effective_enabled": enabled,
    }

    return StorageStatusSnapshot(
        enabled=enabled,
        configured_enabled=config.storage_retention_enabled,
        worker_observed_enabled=worker_observed_enabled,
        effective_enabled=enabled,
        dry_run=config.storage_retention_dry_run,
        connection_state=connection_state,
        checked_at=checked_at,
        database_configured=database_configured,
        liveness_source=liveness.source,
        worker_role=liveness.worker_role,
        worker_heartbeat_at=liveness.heartbeat_at,
        worker_heartbeat_age_ms=liveness.heartbeat_age_ms,
        worker_started_at=liveness.started_at,
        component_heartbeat_at=liveness.component_heartbeat_at,
        component_heartbeat_age_ms=liveness.component_heartbeat_age_ms,
        latest_component_heartbeat_mode=liveness.latest_component_heartbeat_mode,
        latest_aggregate_heartbeat_mode=liveness.latest_aggregate_heartbeat_mode,
        liveness_source_mismatch=liveness.liveness_source_mismatch,
        worker_heartbeat_stale=worker_heartbeat_stale,
        retention_config=effective_retention_config,
        latest_run_found=latest_run is not None,
        latest_run_id=latest_run.run_id if latest_run else None,
        latest_run_status=latest_run.status if latest_run else None,
        latest_run_started_at=latest_run.started_at if latest_run else None,
        latest_run_finished_at=latest_run.finished_at if latest_run else None,
        latest_run_duration_ms=latest_run.duration_ms if latest_run else None,
        latest_deleted_rows=latest_deleted_rows,
        latest_raw_payload_stripped_rows=latest_raw_payload_stripped_rows,
        latest_run_budget_exhausted=latest_run_budget_exhausted,
        latest_tables_processed=latest_tables_processed,
        latest_tables_skipped=latest_tables_skipped,
        latest_total_deleted_rows=sum(latest_deleted_rows.values()),
        latest_total_raw_payload_stripped_rows=sum(latest_raw_payload_stripped_rows.values()),
        latest_db_statement_timeout_count=timeout_counts["statement_timeout"],
        latest_db_lock_timeout_count=timeout_counts["lock_timeout"],
        latest_db_error_count=timeout_counts["db_error"],
        latest_warnings=latest_warnings,
        latest_blockers=latest_blockers,
        table_stats=table_stats,
        warnings=_unique_strings(warnings),
        blockers=_unique_strings(blockers),
    )


def _empty_storage_liveness(checked_at: datetime) -> StorageRetentionLiveness:
    del checked_at
    return StorageRetentionLiveness(
        source=STORAGE_LIVENESS_SOURCE_MISSING,
        metadata=None,
        heartbeat_at=None,
        heartbeat_age_ms=None,
        started_at=None,
        component_heartbeat_at=None,
        component_heartbeat_age_ms=None,
        latest_component_heartbeat_mode=None,
        latest_aggregate_heartbeat_mode=None,
        liveness_source_mismatch=False,
        warnings=[],
        worker_role=None,
    )


def _load_storage_liveness(
    repository: WorkerHeartbeatRepository,
    *,
    checked_at: datetime,
) -> StorageRetentionLiveness:
    component = repository.get_latest_heartbeat(WORKER_SERVICE_STORAGE_RETENTION)
    component_metadata = _storage_worker_metadata(component.metadata_ if component else None)
    if component_metadata is None:
        legacy_component = repository.get_latest_heartbeat(WORKER_SERVICE_STORAGE_RETENTION_LEGACY)
        legacy_component_metadata = _storage_worker_metadata(
            legacy_component.metadata_ if legacy_component else None
        )
        if legacy_component_metadata is not None:
            component = legacy_component
            component_metadata = legacy_component_metadata

    aggregate = repository.get_latest_heartbeat(WORKER_SERVICE_AGGREGATE)
    aggregate_metadata = _storage_worker_metadata(aggregate.metadata_ if aggregate else None)
    selected, metadata, source, warnings = _select_storage_liveness(
        component=component,
        component_metadata=component_metadata,
        aggregate=aggregate,
        aggregate_metadata=aggregate_metadata,
    )
    component_heartbeat_at = _heartbeat_at(component) if component_metadata else None
    heartbeat_at = _heartbeat_at(selected)
    return StorageRetentionLiveness(
        source=source,
        metadata=metadata,
        heartbeat_at=heartbeat_at,
        heartbeat_age_ms=_age_ms(heartbeat_at, checked_at),
        started_at=_started_at(selected),
        component_heartbeat_at=component_heartbeat_at,
        component_heartbeat_age_ms=_age_ms(component_heartbeat_at, checked_at),
        latest_component_heartbeat_mode=_metadata_mode(component),
        latest_aggregate_heartbeat_mode=_metadata_mode(aggregate),
        liveness_source_mismatch=source == STORAGE_LIVENESS_SOURCE_LEGACY_FALLBACK,
        warnings=warnings,
        worker_role=_str_or_none((metadata or {}).get("worker_role")),
    )


def _select_storage_liveness(
    *,
    component: WorkerHeartbeat | None,
    component_metadata: dict[str, Any] | None,
    aggregate: WorkerHeartbeat | None,
    aggregate_metadata: dict[str, Any] | None,
) -> tuple[WorkerHeartbeat | None, dict[str, Any] | None, str, list[str]]:
    if component is not None and component_metadata is not None:
        return component, component_metadata, STORAGE_LIVENESS_SOURCE_COMPONENT, []
    if aggregate_metadata is not None:
        return (
            aggregate,
            aggregate_metadata,
            STORAGE_LIVENESS_SOURCE_LEGACY_FALLBACK,
            [STORAGE_LIVENESS_LEGACY_FALLBACK_WARNING],
        )
    return None, None, STORAGE_LIVENESS_SOURCE_MISSING, []


def _heartbeat_at(heartbeat: WorkerHeartbeat | None) -> datetime | None:
    return _as_utc(heartbeat.heartbeat_at) if heartbeat is not None else None


def _started_at(heartbeat: WorkerHeartbeat | None) -> datetime | None:
    if heartbeat is None or heartbeat.started_at is None:
        return None
    return _as_utc(heartbeat.started_at)


def _metadata_mode(heartbeat: WorkerHeartbeat | None) -> str | None:
    if heartbeat is None or not isinstance(heartbeat.metadata_, dict):
        return None
    return _str_or_none(heartbeat.metadata_.get("mode"))


def _age_ms(value_at: datetime | None, checked_at: datetime) -> int | None:
    if value_at is None:
        return None
    return max(0, int((_as_utc(checked_at) - _as_utc(value_at)).total_seconds() * 1000))


def _is_storage_worker_heartbeat_stale(
    *,
    enabled: bool,
    heartbeat_at: datetime | None,
    checked_at: datetime,
    stale_after_seconds: float,
) -> bool:
    if not enabled:
        return False
    if heartbeat_at is None:
        return True
    return (_as_utc(checked_at) - _as_utc(heartbeat_at)).total_seconds() > stale_after_seconds


def _worker_heartbeat_stale_after_seconds(
    config: AppConfig,
    worker_metadata: dict[str, Any] | None,
) -> float:
    worker_interval_seconds = _positive_seconds_or_none(
        (worker_metadata or {}).get("interval_seconds")
    )
    interval_seconds = worker_interval_seconds or config.storage_retention_interval_seconds
    return max(interval_seconds * 2, 1.0)


def _positive_seconds_or_none(value: Any) -> float | None:
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return seconds if isfinite(seconds) and seconds > 0 else None


def _latest_tables_processed(latest_run: StorageRetentionRun | None) -> list[str]:
    if latest_run is None:
        return []
    table_names = _string_dict_keys(latest_run.table_row_counts_before)
    if table_names:
        return table_names
    return [name for name, value in _int_dict(latest_run.deleted_rows).items() if value > 0]


def _latest_tables_skipped(latest_tables_processed: list[str]) -> list[str]:
    return _retention_tables_skipped(latest_tables_processed)


def _retention_tables_skipped(processed_table_names: list[str]) -> list[str]:
    processed = set(processed_table_names)
    return [table_name for table_name in RETENTION_TABLE_NAMES if table_name not in processed]


def _latest_run_budget_exhausted(
    latest_run: StorageRetentionRun | None,
    latest_warnings: list[str],
) -> bool:
    if latest_run is None:
        return False
    return (
        latest_run.status == RETENTION_SUCCESS_PARTIAL
        or "storage_retention_max_run_seconds_reached" in latest_warnings
        or "storage_retention_max_tables_per_run_reached" in latest_warnings
        or RETENTION_ROW_CAP_REACHED_WARNING in latest_warnings
    )


def _latest_db_timeout_counts(latest_run: StorageRetentionRun | None) -> dict[str, int]:
    if latest_run is None:
        return {"statement_timeout": 0, "lock_timeout": 0, "db_error": 0}
    error_text = " ".join(
        str(value or "")
        for value in (
            latest_run.error_type,
            latest_run.error_message,
        )
    ).lower()
    has_db_error = latest_run.status in {RETENTION_FAILED, RETENTION_TIMED_OUT}
    return {
        "statement_timeout": 1 if "statement timeout" in error_text else 0,
        "lock_timeout": 1 if "lock timeout" in error_text else 0,
        "db_error": 1 if has_db_error and latest_run.error_type else 0,
    }


def _string_dict_keys(value: Any) -> list[str]:
    if not isinstance(value, dict):
        return []
    return [str(key) for key in value if str(key) in RETENTION_TABLE_NAMES]


def _table_stats_for_policy(
    *,
    config: AppConfig,
    repository: StorageRetentionRepository,
    policy: StorageStatusTable,
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
        timestamp_basis=policy.timestamp_expression,
        row_count=row_count,
        approximate_total_bytes=total_bytes,
        approximate_table_bytes=size.get("approximate_table_bytes"),
        approximate_index_bytes=size.get("approximate_index_bytes"),
        approximate_toast_bytes=size.get("approximate_toast_bytes"),
        approximate_total_pretty=_pretty_bytes(total_bytes),
        oldest_row_at=oldest_row_at,
        newest_row_at=newest_row_at,
        raw_payload_non_null_count=raw_payload_non_null_count,
        retention_seconds=(
            int(getattr(config, policy.retention_config_key))
            if policy.retention_config_key is not None
            else None
        ),
        raw_payload_retention_seconds=(
            int(getattr(config, policy.raw_payload_retention_config_key))
            if policy.raw_payload_retention_config_key is not None
            else None
        ),
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


def _selected_retention_policies(
    *,
    config: AppConfig,
    warnings: list[str],
    previous_run: StorageRetentionRun | None,
) -> tuple[tuple[RetentionPolicy, ...], tuple[RetentionPolicy, ...]]:
    max_tables = config.storage_retention_max_tables_per_run
    if max_tables is None or max_tables >= len(RETENTION_POLICIES):
        return RETENTION_POLICIES, ()

    if "storage_retention_max_tables_per_run_reached" not in warnings:
        warnings.append("storage_retention_max_tables_per_run_reached")
    start_index = _next_retention_policy_index(previous_run)
    ordered_policies = RETENTION_POLICIES[start_index:] + RETENTION_POLICIES[:start_index]
    return ordered_policies[:max_tables], ordered_policies[max_tables:]


def _next_retention_policy_index(previous_run: StorageRetentionRun | None) -> int:
    if previous_run is None or previous_run.status not in RETENTION_SUCCESS_STATES:
        return 0
    processed_table_names = _latest_tables_processed(previous_run)
    if not processed_table_names:
        return 0
    try:
        last_index = RETENTION_TABLE_NAMES.index(processed_table_names[-1])
    except ValueError:
        return 0
    return (last_index + 1) % len(RETENTION_POLICIES)


def _retention_batch_size(config: AppConfig, rows_processed_for_table: int) -> int:
    max_rows = config.storage_retention_max_delete_rows_per_table
    if max_rows is None:
        return config.storage_retention_batch_size

    remaining = max_rows - rows_processed_for_table
    return max(0, min(config.storage_retention_batch_size, remaining))


def _mark_row_cap_exhausted_if_needed(
    *,
    config: AppConfig,
    repository: StorageRetentionRepository,
    policy: RetentionPolicy,
    condition_sql: str,
    parameters: dict[str, Any],
    rows_processed: int,
    warnings: list[str],
) -> None:
    max_rows = config.storage_retention_max_delete_rows_per_table
    if max_rows is None or rows_processed < max_rows:
        return
    if (
        repository.has_matching(
            table_name=policy.table_name,
            condition_sql=condition_sql,
            parameters=parameters,
        )
        and RETENTION_ROW_CAP_REACHED_WARNING not in warnings
    ):
        warnings.append(RETENTION_ROW_CAP_REACHED_WARNING)


def _sleep_between_retention_tables(
    config: AppConfig,
    table_index: int,
    policies: tuple[RetentionPolicy, ...],
) -> None:
    if table_index < len(policies) - 1:
        _retention_sleep_ms(config.storage_retention_inter_table_sleep_ms)


def _retention_sleep_ms(duration_ms: int) -> None:
    if duration_ms > 0:
        sleep(duration_ms / 1000)


def _retention_budget_exhausted(warnings: list[str]) -> bool:
    return (
        "storage_retention_max_run_seconds_reached" in warnings
        or "storage_retention_max_tables_per_run_reached" in warnings
        or RETENTION_ROW_CAP_REACHED_WARNING in warnings
    )


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
    if config.kalshi_cfbenchmarks_enabled and config.kalshi_cfbenchmarks_subscribe_on_worker:
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
    return {str(key): int(item) for key, item in value.items() if isinstance(item, int | float)}


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


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


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
