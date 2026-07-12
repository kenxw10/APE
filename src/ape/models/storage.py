from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel

from ape.storage.retention import StorageStatusSnapshot, StorageTableStats


class StorageTableStatsResponse(BaseModel):
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


class StorageStatusResponse(BaseModel):
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
    table_stats: list[StorageTableStatsResponse]
    warnings: list[str]
    blockers: list[str]


def storage_status_response(snapshot: StorageStatusSnapshot) -> StorageStatusResponse:
    return StorageStatusResponse(
        **{
            **snapshot.__dict__,
            "table_stats": [
                storage_table_stats_response(stat) for stat in snapshot.table_stats
            ],
        }
    )


def storage_table_stats_response(stat: StorageTableStats) -> StorageTableStatsResponse:
    return StorageTableStatsResponse(**stat.__dict__)
