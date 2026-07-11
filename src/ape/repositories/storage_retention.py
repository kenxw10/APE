from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import desc, select, text, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from ape.db.models import StorageRetentionRun
from ape.repositories.inputs import StorageRetentionRunInput

ALLOWED_RETENTION_TABLES = {
    "kalshi_ws_protocol_events",
    "markets",
    "orderbook_snapshots",
    "public_trades",
    "reference_ticks",
    "strategy_decisions",
    "strategy_dry_run_events",
    "strategy_dry_run_positions",
    "strategy_feature_snapshots",
    "strategy_position_marks",
    "strategy_trade_intents",
    "worker_heartbeats",
}
ALLOWED_STATUS_READ_TABLES = ALLOWED_RETENTION_TABLES | {
    "strategy_position_outcomes",
}
JSON_FIELDS = {
    "deleted_rows",
    "raw_payload_stripped_rows",
    "table_row_counts_before",
    "table_row_counts_after",
    "table_sizes_before",
    "table_sizes_after",
    "warnings",
    "blockers",
}


class StorageRetentionRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def start_run(self, run: StorageRetentionRunInput) -> StorageRetentionRun:
        row = StorageRetentionRun(**_run_values(run))
        self.session.add(row)
        self.session.flush()
        return row

    def finish_run(self, run_id: str, run: StorageRetentionRunInput) -> StorageRetentionRun:
        row = self.get_run_by_run_id(run_id)
        if row is None:
            row = StorageRetentionRun(**_run_values(run))
            self.session.add(row)
            self.session.flush()
            return row

        values = _run_values(run)
        values.pop("run_id", None)
        values.pop("started_at", None)
        for key, value in values.items():
            setattr(row, key, value)
            if key in JSON_FIELDS:
                flag_modified(row, key)
        self.session.flush()
        return row

    def mark_stale_running_runs(self, *, cutoff: datetime, finished_at: datetime) -> int:
        statement = (
            update(StorageRetentionRun)
            .where(
                StorageRetentionRun.status == "running",
                StorageRetentionRun.started_at < cutoff,
            )
            .values(
                status="timed_out",
                finished_at=finished_at,
                warnings=["retention_run_marked_timed_out_on_next_start"],
                error_type="RetentionRunTimedOut",
                error_message="Previous retention run did not finish before the next run.",
            )
        )
        result = self.session.execute(statement)
        return int(result.rowcount or 0)

    def get_run_by_run_id(self, run_id: str) -> StorageRetentionRun | None:
        return self.session.scalar(
            select(StorageRetentionRun).where(StorageRetentionRun.run_id == run_id).limit(1)
        )

    def get_latest_run(self) -> StorageRetentionRun | None:
        return self.session.scalar(
            select(StorageRetentionRun)
            .order_by(desc(StorageRetentionRun.started_at), desc(StorageRetentionRun.id))
            .limit(1)
        )

    def count_matching(
        self,
        *,
        table_name: str,
        condition_sql: str,
        parameters: dict[str, Any],
    ) -> int:
        _validate_retention_table(table_name)
        row_count = self.session.scalar(
            text(f"SELECT COUNT(*) FROM {table_name} WHERE {condition_sql}"),
            parameters,
        )
        return int(row_count or 0)

    def has_matching(
        self,
        *,
        table_name: str,
        condition_sql: str,
        parameters: dict[str, Any],
    ) -> bool:
        _validate_retention_table(table_name)
        value = self.session.scalar(
            text(f"SELECT 1 FROM {table_name} WHERE {condition_sql} LIMIT 1"),
            parameters,
        )
        return value is not None

    def delete_batch(
        self,
        *,
        table_name: str,
        condition_sql: str,
        parameters: dict[str, Any],
        batch_size: int,
    ) -> int:
        _validate_retention_table(table_name)
        rows = self.session.execute(
            text(
                f"""
                WITH doomed AS (
                    SELECT id
                    FROM {table_name}
                    WHERE {condition_sql}
                    ORDER BY id
                    LIMIT :batch_size
                )
                DELETE FROM {table_name}
                WHERE id IN (SELECT id FROM doomed)
                RETURNING id
                """
            ),
            {**parameters, "batch_size": batch_size},
        ).all()
        return len(rows)

    def strip_raw_payload_batch(
        self,
        *,
        table_name: str,
        condition_sql: str,
        parameters: dict[str, Any],
        batch_size: int,
    ) -> int:
        _validate_retention_table(table_name)
        rows = self.session.execute(
            text(
                f"""
                WITH targets AS (
                    SELECT id
                    FROM {table_name}
                    WHERE {condition_sql}
                    ORDER BY id
                    LIMIT :batch_size
                )
                UPDATE {table_name}
                SET raw_payload = NULL
                WHERE id IN (SELECT id FROM targets)
                RETURNING id
                """
            ),
            {**parameters, "batch_size": batch_size},
        ).all()
        return len(rows)

    def row_counts(self, table_names: tuple[str, ...]) -> dict[str, int | None]:
        return {table_name: self.approximate_row_count(table_name) for table_name in table_names}

    def table_sizes(self, table_names: tuple[str, ...]) -> dict[str, dict[str, int | None]]:
        return {table_name: self.table_size(table_name) for table_name in table_names}

    def approximate_row_count(self, table_name: str) -> int | None:
        _validate_status_read_table(table_name)
        if self.session.bind is not None and self.session.bind.dialect.name == "postgresql":
            value = self.session.scalar(
                text(
                    """
                    SELECT n_live_tup
                    FROM pg_stat_all_tables
                    WHERE schemaname = current_schema()
                      AND relname = :table_name
                    """
                ),
                {"table_name": table_name},
            )
            return None if value is None else int(value)

        value = self.session.scalar(text(f"SELECT COUNT(*) FROM {table_name}"))
        return int(value or 0)

    def table_size(self, table_name: str) -> dict[str, int | None]:
        _validate_status_read_table(table_name)
        if self.session.bind is None or self.session.bind.dialect.name != "postgresql":
            return {
                "approximate_total_bytes": None,
                "approximate_table_bytes": None,
                "approximate_index_bytes": None,
                "approximate_toast_bytes": None,
            }

        row = self.session.execute(
            text(
                """
                SELECT
                    pg_total_relation_size(to_regclass(:table_name)) AS total_bytes,
                    pg_relation_size(to_regclass(:table_name)) AS table_bytes,
                    pg_indexes_size(to_regclass(:table_name)) AS index_bytes
                """
            ),
            {"table_name": table_name},
        ).one()
        return {
            "approximate_total_bytes": _int_or_none(row.total_bytes),
            "approximate_table_bytes": _int_or_none(row.table_bytes),
            "approximate_index_bytes": _int_or_none(row.index_bytes),
            "approximate_toast_bytes": None,
        }

    def database_total_bytes(self) -> int | None:
        if self.session.bind is None or self.session.bind.dialect.name != "postgresql":
            return None

        value = self.session.scalar(text("SELECT pg_database_size(current_database())"))
        return _int_or_none(value)

    def oldest_newest(
        self,
        *,
        table_name: str,
        timestamp_expression: str,
    ) -> tuple[datetime | None, datetime | None]:
        _validate_status_read_table(table_name)
        row = self.session.execute(
            text(
                f"""
                SELECT
                    MIN({timestamp_expression}) AS oldest_row_at,
                    MAX({timestamp_expression}) AS newest_row_at
                FROM {table_name}
                """
            )
        ).one()
        return _datetime_or_none(row.oldest_row_at), _datetime_or_none(row.newest_row_at)

    def raw_payload_non_null_count(self, table_name: str) -> int | None:
        _validate_status_read_table(table_name)
        if self.session.bind is not None and self.session.bind.dialect.name == "postgresql":
            value = self.session.scalar(
                text(
                    """
                    SELECT
                        CASE
                            WHEN stats.null_frac IS NULL OR cls.reltuples < 0
                                THEN NULL
                            ELSE GREATEST(
                                0,
                                ROUND(cls.reltuples * (1.0 - stats.null_frac))
                            )::bigint
                        END AS approximate_count
                    FROM pg_class cls
                    JOIN pg_namespace ns ON ns.oid = cls.relnamespace
                    LEFT JOIN pg_stats stats
                      ON stats.schemaname = ns.nspname
                     AND stats.tablename = cls.relname
                     AND stats.attname = 'raw_payload'
                    WHERE ns.nspname = current_schema()
                      AND cls.relname = :table_name
                    LIMIT 1
                    """
                ),
                {"table_name": table_name},
            )
            return None if value is None else int(value)

        value = self.session.scalar(
            text(f"SELECT COUNT(*) FROM {table_name} WHERE raw_payload IS NOT NULL")
        )
        return int(value or 0)


def _validate_retention_table(table_name: str) -> None:
    if table_name not in ALLOWED_RETENTION_TABLES:
        raise ValueError(f"Unsupported retention table: {table_name}")


def _validate_status_read_table(table_name: str) -> None:
    if table_name not in ALLOWED_STATUS_READ_TABLES:
        raise ValueError(f"Unsupported storage status table: {table_name}")


def _run_values(run: StorageRetentionRunInput) -> dict[str, Any]:
    values = run.__dict__.copy()
    for field_name in JSON_FIELDS:
        values[field_name] = deepcopy(values.get(field_name))
    return values


def _int_or_none(value: Any) -> int | None:
    return None if value is None else int(value)


def _datetime_or_none(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise SQLAlchemyError(f"Could not parse stored timestamp {value!r}") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
