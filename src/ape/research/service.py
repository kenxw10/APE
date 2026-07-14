from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Event, Lock, Thread
from typing import Any

from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from ape.config import AppConfig
from ape.kalshi.client import KalshiRestClient
from ape.repositories.inputs import StrategyConfigVersionInput, WorkerHeartbeatInput
from ape.repositories.strategy_v2 import StrategyV2Repository
from ape.repositories.worker_heartbeats import WorkerHeartbeatRepository
from ape.research import (
    CALIBRATION_SCHEMA_VERSION,
    REPLAY_SCHEMA_VERSION,
    RESEARCH_LABEL_SCHEMA_VERSION,
)
from ape.research.archive import (
    ARCHIVE_MAX_BATCHES_PER_CYCLE,
    ARCHIVE_SOURCE_STAGES,
    ArchiveResult,
    archive_bootstrap_required,
    archive_research_batch,
    archive_research_coverage,
    archive_research_events,
    archive_research_source_pending,
    reconcile_market_outcomes,
    refresh_research_archive_labels,
    refresh_research_reference_associations,
)
from ape.research.calibration import (
    LIFECYCLE_DRAFT,
    complete_search_space_snapshot,
    run_bounded_calibration,
)
from ape.research.replay import DeterministicReplayEngine, ReplayTrade
from ape.research.repository import FrozenReplayProgress, ResearchRepository
from ape.strategy.momentum_v2 import (
    V2_ARCHITECTURE_VERSION,
    V2_FEATURE_SCHEMA_VERSION,
    V2_PARAMETERS,
    built_in_config_version,
    resolve_code_version,
)
from ape.worker.services import WORKER_SERVICE_RESEARCH

LOGGER = logging.getLogger(__name__)

RESEARCH_HEARTBEAT_INTERVAL_SECONDS = 30.0
ARCHIVE_DUPLICATE_RETRY_LIMIT = 3
ARCHIVE_DUPLICATE_RETRY_DELAY_SECONDS = 0.05
CALIBRATION_MATERIALIZE_EVENT_LIMIT = 20_000


@dataclass(frozen=True)
class ArchiveSchedulingResult:
    archive: ArchiveResult
    budget_exhausted: bool
    scheduling_mode: str
    bootstrap_pending_after_budget: bool
    tail_pending_after_budget: bool
    sources_served: tuple[str, ...]
    operations_by_source: dict[str, int]
    post_archive_allowed: bool
    post_archive_deferred_reason: str | None

    def progress_metadata(self) -> dict[str, Any]:
        return {
            "archive_scheduling_mode": self.scheduling_mode,
            "archive_bootstrap_pending_after_budget": self.bootstrap_pending_after_budget,
            "archive_tail_pending_after_budget": self.tail_pending_after_budget,
            "archive_sources_served": list(self.sources_served),
            "archive_operations_by_source": dict(self.operations_by_source),
            "post_archive_allowed": self.post_archive_allowed,
            "post_archive_deferred_reason": self.post_archive_deferred_reason,
        }


class ResearchWorker:
    """Database-only research worker. It owns no websocket, trading, or retention loop."""

    def __init__(
        self,
        *,
        config: AppConfig,
        safety,
        session_factory,
        started_at: datetime,
        heartbeat_interval_seconds: float = RESEARCH_HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        self.config = config
        self.safety = safety
        self.session_factory = session_factory
        self.started_at = started_at
        self.heartbeat_interval_seconds = min(
            max(float(heartbeat_interval_seconds), 0.01),
            RESEARCH_HEARTBEAT_INTERVAL_SECONDS,
        )
        self._progress_lock = Lock()
        self._current_progress: dict[str, Any] = {}
        self._heartbeat_stop: Event | None = None
        self._heartbeat_thread: Thread | None = None

    async def run(self, *, stop_event, max_iterations: int | None = None) -> None:
        iterations = 0
        while not stop_event.is_set():
            await asyncio.to_thread(self.run_once)
            iterations += 1
            if max_iterations is not None and iterations >= max_iterations:
                return
            await asyncio.to_thread(stop_event.wait, self.config.research_poll_seconds)

    def run_once(self) -> dict[str, Any]:
        checked_at = datetime.now(UTC)
        if self.session_factory is None:
            return {"status": "blocked", "blockers": ["research_database_not_configured"]}
        self._stop_stage_heartbeat_ticker()
        cycle_started_at = checked_at
        result = _cycle_progress(
            state="running",
            stage="startup",
            cycle_started_at=cycle_started_at,
            cycle_id=_cycle_id(cycle_started_at),
        )
        self._publish_progress(result, heartbeat_at=checked_at)
        LOGGER.info("Research cycle starting.")
        try:
            result = _cycle_progress(
                previous=result,
                state="running",
                stage="archive",
                cycle_started_at=cycle_started_at,
            )
            self._publish_progress(result)

            def report_archive_progress(
                archive_progress: ArchiveResult,
                *,
                source_stage: str,
                completed_batches: int,
                last_archive_batch: dict[str, Any] | None,
                archive_metadata: dict[str, Any] | None,
            ) -> None:
                nonlocal result
                result = _cycle_progress(
                    previous=result,
                    state="running",
                    stage="archive",
                    cycle_started_at=cycle_started_at,
                    last_successful_stage=(
                        "archive"
                        if completed_batches > 0
                        else result.get("last_successful_stage")
                    ),
                    archive=archive_progress,
                    current_source_table=source_stage,
                    completed_archive_batches=completed_batches,
                    archived_counts_by_type=dict(archive_progress.archived_by_type),
                    **(archive_metadata if archive_metadata is not None else {}),
                    **(
                        {"last_archive_batch": last_archive_batch}
                        if last_archive_batch is not None
                        else {}
                    ),
                )
                self._publish_progress(result)

            archive_schedule = self._archive_stage(
                checked_at=checked_at,
                cycle_started_at=cycle_started_at,
                progress_callback=report_archive_progress,
            )
            archive = archive_schedule.archive
            archive_metadata = archive_schedule.progress_metadata()
            if not archive_schedule.post_archive_allowed:
                bootstrap_warning = (
                    "research_archive_bootstrap_budget_exhausted"
                    if archive_schedule.budget_exhausted
                    else "research_archive_bootstrap_incomplete"
                )
                warnings = [bootstrap_warning]
                if archive_schedule.budget_exhausted:
                    warnings.insert(0, "research_archive_batch_budget_exhausted")
                result = _cycle_progress(
                    previous=result,
                    state="partial",
                    stage="archive",
                    cycle_started_at=cycle_started_at,
                    last_successful_stage="archive",
                    archive=archive,
                    archived_counts_by_type=dict(archive.archived_by_type),
                    warnings=warnings,
                    status="partial",
                    **archive_metadata,
                )
                self._publish_progress(result)
                LOGGER.warning(
                    "Research archive bootstrap remains incomplete; post-archive work deferred."
                )
                return result

            with self.session_factory() as session:
                archive_snapshot = ResearchRepository(session).replay_event_snapshot()

            result = _cycle_progress(
                previous=result,
                state="running",
                stage="association_labels",
                cycle_started_at=cycle_started_at,
                last_successful_stage="archive",
                archive=archive,
                post_archive_substage="reference_association",
                **archive_metadata,
            )
            self._publish_progress(result)
            self._start_stage_heartbeat_ticker()
            try:
                with self.session_factory() as session:
                    association_result = refresh_research_reference_associations(session)
            finally:
                self._stop_stage_heartbeat_ticker()
            tail_warnings = (
                ["research_archive_tail_budget_exhausted"]
                if archive_schedule.tail_pending_after_budget
                else []
            )
            result = _cycle_progress(
                previous=result,
                state="running",
                stage="association_labels",
                cycle_started_at=cycle_started_at,
                last_successful_stage="reference_association",
                archive=archive,
                post_archive_substage="reference_association",
                association_rows_processed=association_result.processed_rows,
                association_rows_remaining=association_result.remaining_rows,
                **archive_metadata,
            )
            self._publish_progress(result)
            if association_result.remaining_rows > 0:
                result = _cycle_progress(
                    previous=result,
                    state="partial",
                    stage="association_labels",
                    cycle_started_at=cycle_started_at,
                    last_successful_stage="reference_association",
                    archive=archive,
                    post_archive_substage="reference_association",
                    association_rows_processed=association_result.processed_rows,
                    association_rows_remaining=association_result.remaining_rows,
                    warnings=[
                        *tail_warnings,
                        "research_reference_association_batch_remaining",
                    ],
                    status="partial",
                    **archive_metadata,
                )
                self._publish_progress(result)
                LOGGER.info("Reference association remains; labels, coverage, and replay deferred.")
                return result

            result = _cycle_progress(
                previous=result,
                state="running",
                stage="association_labels",
                cycle_started_at=cycle_started_at,
                last_successful_stage="reference_association",
                archive=archive,
                post_archive_substage="labels",
                association_rows_processed=association_result.processed_rows,
                association_rows_remaining=association_result.remaining_rows,
                **archive_metadata,
            )
            self._publish_progress(result)
            self._start_stage_heartbeat_ticker()
            try:
                with self.session_factory() as session:
                    label_result = refresh_research_archive_labels(session)
                    session.commit()
            finally:
                self._stop_stage_heartbeat_ticker()
            if label_result.remaining_markets > 0:
                label_warnings = [
                    *tail_warnings,
                    "research_label_batch_budget_exhausted",
                ]
                label_blockers: list[str] = []
                if label_result.blocked_missing_market_count > 0:
                    label_warnings.append("research_label_markets_blocked_missing_market")
                    label_blockers.append("research_label_market_missing")
                result = _cycle_progress(
                    previous=result,
                    state="partial",
                    stage="association_labels",
                    cycle_started_at=cycle_started_at,
                    last_successful_stage="reference_association",
                    archive=archive,
                    post_archive_substage="labels",
                    association_rows_processed=association_result.processed_rows,
                    association_rows_remaining=association_result.remaining_rows,
                    label_markets_processed=label_result.processed_markets,
                    label_markets_remaining=label_result.remaining_markets,
                    label_markets_blocked_missing_market=label_result.blocked_missing_market_count,
                    labels_processed=label_result.processed_markets,
                    labels_remaining=label_result.remaining_markets,
                    warnings=label_warnings,
                    blockers=label_blockers,
                    status="partial",
                    **archive_metadata,
                )
                self._publish_progress(result)
                LOGGER.info("Research labels remain; coverage and replay deferred.")
                return result
            result = _cycle_progress(
                previous=result,
                state="running",
                stage="association_labels",
                cycle_started_at=cycle_started_at,
                last_successful_stage="association_labels",
                archive=archive,
                post_archive_substage="labels",
                association_rows_processed=association_result.processed_rows,
                association_rows_remaining=association_result.remaining_rows,
                label_markets_processed=label_result.processed_markets,
                label_markets_remaining=label_result.remaining_markets,
                label_markets_blocked_missing_market=label_result.blocked_missing_market_count,
                labels_processed=label_result.processed_markets,
                labels_remaining=label_result.remaining_markets,
                **archive_metadata,
            )
            self._publish_progress(result)

            def report_coverage_progress(progress: FrozenReplayProgress) -> None:
                nonlocal result
                result = _cycle_progress(
                    previous=result,
                    state="running",
                    stage="coverage",
                    cycle_started_at=cycle_started_at,
                    last_successful_stage="association_labels",
                    archive=archive,
                    post_archive_substage="coverage_scan",
                    coverage_dataset_watermark=progress.watermark_id,
                    coverage_total_events=progress.total_events,
                    coverage_events_scanned=progress.events_scanned,
                    coverage_pages_completed=progress.pages_completed,
                    coverage_partitions_completed=progress.partitions_completed,
                    coverage_partitions_total=progress.partitions_total,
                    coverage_max_page_size=progress.max_page_size,
                )
                self._update_progress(result)

            with self.session_factory() as session:
                snapshot = archive_snapshot
                result = _cycle_progress(
                    previous=result,
                    state="running",
                    stage="coverage",
                    cycle_started_at=cycle_started_at,
                    last_successful_stage="association_labels",
                    archive=archive,
                    post_archive_substage="coverage_scan",
                    coverage_dataset_watermark=snapshot.watermark_id,
                    coverage_total_events=snapshot.event_count,
                    coverage_events_scanned=0,
                    coverage_pages_completed=0,
                    coverage_partitions_completed=0,
                    coverage_partitions_total=snapshot.partition_count,
                    coverage_max_page_size=0,
                )
                self._publish_progress(result)
                self._start_stage_heartbeat_ticker()
                try:
                    coverage = archive_research_coverage(
                        session,
                        now=checked_at,
                        snapshot=snapshot,
                        progress_callback=report_coverage_progress,
                    )
                    session.commit()
                finally:
                    self._stop_stage_heartbeat_ticker()
            archive = ArchiveResult(
                archived_events=archive.archived_events,
                archived_by_type=archive.archived_by_type,
                outcomes_reconciled=archive.outcomes_reconciled,
                coverage=coverage,
            )
            frozen_coverage = coverage.get("frozen_snapshot", {})
            result = _cycle_progress(
                previous=result,
                state="running",
                stage="coverage",
                cycle_started_at=cycle_started_at,
                last_successful_stage="coverage",
                archive=archive,
                post_archive_substage="coverage",
                coverage_dataset_watermark=frozen_coverage.get("watermark_id"),
                coverage_total_events=frozen_coverage.get("total_events"),
                coverage_events_scanned=frozen_coverage.get("events_scanned"),
                coverage_pages_completed=frozen_coverage.get("pages_completed"),
                coverage_partitions_completed=frozen_coverage.get("partitions_completed"),
                coverage_partitions_total=frozen_coverage.get("partitions_total"),
                coverage_max_page_size=frozen_coverage.get("max_page_size"),
            )
            self._publish_progress(result)

            def report_replay_progress(stage: str, details: dict[str, Any]) -> None:
                nonlocal result
                progress_details = dict(details)
                last_successful_stage = progress_details.pop(
                    "last_successful_stage", "coverage"
                )
                if stage.endswith("_started"):
                    current_stage = stage.removesuffix("_started")
                elif stage.endswith("_completed"):
                    current_stage = stage.removesuffix("_completed")
                elif stage == "baseline_replay_progress":
                    # Page updates are part of the same bounded replay stage;
                    # keeping a stable stage name lets status consumers observe
                    # the counters advancing before completion.
                    current_stage = "baseline_replay"
                else:
                    current_stage = stage
                result = _cycle_progress(
                    previous=result,
                    state="running",
                    stage=current_stage,
                    cycle_started_at=cycle_started_at,
                    last_successful_stage=last_successful_stage,
                    archive=archive,
                    **progress_details,
                )
                if stage == "baseline_replay_progress":
                    # The capped ticker owns persistence while the bounded
                    # reader advances counters page by page.
                    self._update_progress(result)
                    return
                self._publish_progress(result)
                if stage.endswith("_started"):
                    self._start_stage_heartbeat_ticker()
                elif stage.endswith("_completed"):
                    self._stop_stage_heartbeat_ticker()

            with self.session_factory() as session:
                cycle_result = run_research_cycle(
                    self.config,
                    session,
                    checked_at=checked_at,
                    archive_result=archive,
                    replay_snapshot=snapshot,
                    progress_callback=report_replay_progress,
                )
                session.commit()
            self._stop_stage_heartbeat_ticker()
            final_tail_pending = archive_schedule.tail_pending_after_budget
            final_status = cycle_result["status"]
            final_state = "healthy" if final_status == "completed" else "degraded"
            if final_tail_pending and final_status == "completed":
                final_status = "partial"
                final_state = "partial"
            result = _cycle_progress(
                previous={**result, **cycle_result},
                state=final_state,
                stage="complete",
                cycle_started_at=cycle_started_at,
                last_successful_stage="calibration"
                if self.config.calibration_enabled
                else "baseline_replay",
                archive=archive,
                warnings=tail_warnings,
                status=final_status,
                **archive_metadata,
            )
            self._publish_progress(result)
            LOGGER.info(
                "Research cycle completed archive_events=%s replay_run=%s calibration=%s.",
                archive.archived_events,
                result.get("replay_run_id"),
                result.get("calibration_status"),
            )
            return result
        except SQLAlchemyError as error:
            self._stop_stage_heartbeat_ticker()
            LOGGER.exception("Research cycle database stage failed.")
            result = _cycle_progress(
                previous=self._current_progress_copy(),
                state="error",
                stage=result.get("current_stage", "unknown"),
                cycle_started_at=cycle_started_at,
                last_successful_stage=result.get("last_successful_stage"),
                blockers=["research_database_error"],
                error=error,
                status="error",
                failed_stage=result.get("current_stage", "unknown"),
            )
            self._publish_progress(result)
            return result
        except Exception as error:
            self._stop_stage_heartbeat_ticker()
            LOGGER.exception("Research cycle unexpected stage failed.")
            result = _cycle_progress(
                previous=self._current_progress_copy(),
                state="error",
                stage=result.get("current_stage", "unknown"),
                cycle_started_at=cycle_started_at,
                last_successful_stage=result.get("last_successful_stage"),
                blockers=["research_cycle_unexpected_error"],
                error=error,
                status="error",
                failed_stage=result.get("current_stage", "unknown"),
            )
            self._publish_progress(result)
            return result
        finally:
            self._stop_stage_heartbeat_ticker()

    def _archive_stage(
        self,
        *,
        checked_at: datetime,
        cycle_started_at: datetime,
        progress_callback: Callable[..., None],
    ) -> ArchiveSchedulingResult:
        del checked_at, cycle_started_at
        counts: dict[str, int] = {}
        archived_events = 0
        batch_count = 0
        operations_by_source = {source_stage: 0 for source_stage in ARCHIVE_SOURCE_STAGES}
        sources_served: list[str] = []
        scheduling_mode = (
            "BOOTSTRAP_STRICT"
            if self._bootstrap_required()
            else "TAIL_FAIR"
        )

        def scheduling_metadata() -> dict[str, Any]:
            return {
                "archive_scheduling_mode": scheduling_mode,
                "archive_bootstrap_pending_after_budget": False,
                "archive_tail_pending_after_budget": False,
                "archive_sources_served": list(sources_served),
                "archive_operations_by_source": dict(operations_by_source),
                "post_archive_allowed": scheduling_mode == "TAIL_FAIR",
                "post_archive_deferred_reason": None,
            }

        def run_one(source_stage: str) -> bool:
            nonlocal archived_events, batch_count
            progress_callback(
                ArchiveResult(archived_events, dict(counts), 0, {}),
                source_stage=source_stage,
                completed_batches=batch_count,
                last_archive_batch=None,
                archive_metadata=scheduling_metadata(),
            )
            batch = self._archive_batch_with_retry(source_stage)
            if not batch.operation_performed:
                return False
            batch_count += 1
            archived_events += batch.archived_events
            operations_by_source[source_stage] += 1
            if source_stage not in sources_served:
                sources_served.append(source_stage)
            for event_type, count in batch.archived_by_type.items():
                counts[event_type] = counts.get(event_type, 0) + count
            metadata = scheduling_metadata()
            progress_callback(
                ArchiveResult(archived_events, dict(counts), 0, {}),
                source_stage=source_stage,
                completed_batches=batch_count,
                last_archive_batch={
                    "source_stage": batch.source_stage,
                    "source_rows": batch.source_rows,
                    "archived_events": batch.archived_events,
                    "batch_count": batch_count,
                },
                archive_metadata={
                    **metadata,
                    "archive_selector_mode": batch.selector_mode,
                    "archive_source_cursor": batch.source_cursor,
                    "archive_bootstrap_target": batch.bootstrap_target,
                    "archive_verification_window_start": batch.verification_window_start,
                    "archive_verification_window_end": batch.verification_window_end,
                    "archive_missing_rows_archived": batch.missing_rows_archived,
                    "archive_bootstrap_complete": batch.bootstrap_complete,
                },
            )
            return True

        if scheduling_mode == "BOOTSTRAP_STRICT":
            for source_stage in ARCHIVE_SOURCE_STAGES:
                while batch_count < ARCHIVE_MAX_BATCHES_PER_CYCLE:
                    if not run_one(source_stage):
                        break
                if batch_count >= ARCHIVE_MAX_BATCHES_PER_CYCLE:
                    break
            bootstrap_pending = self._bootstrap_required()
            if bootstrap_pending:
                budget_exhausted = batch_count >= ARCHIVE_MAX_BATCHES_PER_CYCLE
                return ArchiveSchedulingResult(
                    archive=ArchiveResult(archived_events, counts, 0, {}),
                    budget_exhausted=budget_exhausted,
                    scheduling_mode="BOOTSTRAP_STRICT",
                    bootstrap_pending_after_budget=budget_exhausted,
                    tail_pending_after_budget=False,
                    sources_served=tuple(sources_served),
                    operations_by_source=operations_by_source,
                    post_archive_allowed=False,
                    post_archive_deferred_reason=(
                        "bootstrap_pending_after_budget"
                        if budget_exhausted
                        else "bootstrap_incomplete"
                    ),
                )
            scheduling_mode = "TAIL_FAIR"

        while batch_count < ARCHIVE_MAX_BATCHES_PER_CYCLE:
            pass_served = False
            for source_stage in ARCHIVE_SOURCE_STAGES:
                if batch_count >= ARCHIVE_MAX_BATCHES_PER_CYCLE:
                    break
                if run_one(source_stage):
                    pass_served = True
            if not pass_served:
                break

        budget_exhausted = batch_count >= ARCHIVE_MAX_BATCHES_PER_CYCLE
        with self.session_factory() as session:
            tail_pending = any(
                archive_research_source_pending(session, source_stage=source_stage)
                for source_stage in ARCHIVE_SOURCE_STAGES
            )
        tail_pending_after_budget = budget_exhausted and tail_pending
        return ArchiveSchedulingResult(
            archive=ArchiveResult(archived_events, counts, 0, {}),
            budget_exhausted=budget_exhausted,
            scheduling_mode="TAIL_FAIR",
            bootstrap_pending_after_budget=False,
            tail_pending_after_budget=tail_pending_after_budget,
            sources_served=tuple(sources_served),
            operations_by_source=operations_by_source,
            post_archive_allowed=True,
            post_archive_deferred_reason=None,
        )

    def _bootstrap_required(self) -> bool:
        with self.session_factory() as session:
            return archive_bootstrap_required(session)

    def _archive_batch_with_retry(self, source_stage: str):
        for attempt in range(ARCHIVE_DUPLICATE_RETRY_LIMIT):
            with self.session_factory() as session:
                try:
                    batch = archive_research_batch(session, source_stage=source_stage)
                    if batch.state_changed:
                        session.commit()
                    return batch
                except IntegrityError as error:
                    session.rollback()
                    if (
                        not _is_archive_duplicate_identity_error(error)
                        or attempt + 1 >= ARCHIVE_DUPLICATE_RETRY_LIMIT
                    ):
                        raise
            time.sleep(ARCHIVE_DUPLICATE_RETRY_DELAY_SECONDS * (attempt + 1))
        raise AssertionError("Archive retry loop exhausted without returning or raising.")

    def _publish_progress(
        self, result: dict[str, Any], *, heartbeat_at: datetime | None = None
    ) -> None:
        with self._progress_lock:
            self._current_progress = dict(result)
        self._write_heartbeat(heartbeat_at or datetime.now(UTC), self._progress_snapshot())

    def _update_progress(self, result: dict[str, Any]) -> None:
        """Update bounded scan counters without adding a heartbeat write per page."""
        with self._progress_lock:
            self._current_progress = dict(result)

    def _progress_snapshot(self) -> dict[str, Any]:
        with self._progress_lock:
            snapshot = dict(self._current_progress)
            snapshot["last_progress_at"] = datetime.now(UTC).isoformat()
            self._current_progress = dict(snapshot)
            return snapshot

    def _current_progress_copy(self) -> dict[str, Any]:
        with self._progress_lock:
            return dict(self._current_progress)

    def _start_stage_heartbeat_ticker(self) -> None:
        self._stop_stage_heartbeat_ticker()
        stop = Event()

        def tick() -> None:
            while not stop.wait(self.heartbeat_interval_seconds):
                snapshot = self._progress_snapshot()
                if snapshot.get("worker_state") != "running":
                    return
                self._write_heartbeat(datetime.now(UTC), snapshot)

        thread = Thread(target=tick, name="ape-research-heartbeat", daemon=True)
        self._heartbeat_stop = stop
        self._heartbeat_thread = thread
        thread.start()

    def _stop_stage_heartbeat_ticker(self) -> None:
        stop = self._heartbeat_stop
        thread = self._heartbeat_thread
        self._heartbeat_stop = None
        self._heartbeat_thread = None
        if stop is not None:
            stop.set()
        if thread is not None and thread.is_alive():
            thread.join()

    def _write_heartbeat(self, heartbeat_at: datetime, result: dict[str, Any]) -> None:
        """Persist liveness from a fresh transaction so failed work cannot erase it."""
        try:
            with self.session_factory() as session:
                _record_research_heartbeat(
                    session,
                    self.config,
                    self.safety,
                    self.started_at,
                    heartbeat_at,
                    result,
                )
                session.commit()
        except SQLAlchemyError:
            LOGGER.warning("Research heartbeat persistence failed.", exc_info=True)


class MarketOutcomeReconciler:
    """Public-data-only reconciler run by the market-data worker, never research credentials."""

    def __init__(
        self,
        *,
        config: AppConfig,
        safety,
        session_factory,
        started_at: datetime,
        market_client: KalshiRestClient | None = None,
    ) -> None:
        self.config = config
        self.safety = safety
        self.session_factory = session_factory
        self.started_at = started_at
        # Deliberately omit credentials: reconciliation owns only public market detail.
        self.market_client = market_client or KalshiRestClient(
            base_url=config.kalshi_api_base_url,
            timeout_seconds=config.kalshi_rest_timeout_seconds,
        )

    async def run(self, *, stop_event, max_iterations: int | None = None) -> None:
        iterations = 0
        while not stop_event.is_set():
            await asyncio.to_thread(self.run_once)
            iterations += 1
            if max_iterations is not None and iterations >= max_iterations:
                return
            await asyncio.to_thread(stop_event.wait, max(self.config.research_poll_seconds, 60.0))

    def run_once(self) -> None:
        if self.session_factory is None:
            return
        try:
            with self.session_factory() as session:
                reconcile_market_outcomes(session, client=self.market_client)
                session.commit()
        except SQLAlchemyError:
            LOGGER.warning("Market outcome reconciliation failed.", exc_info=True)


def run_research_cycle(
    config: AppConfig,
    session,
    *,
    checked_at: datetime | None = None,
    archive_result: ArchiveResult | None = None,
    replay_snapshot=None,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Execute archive -> labels -> baseline replay -> optional bounded calibration."""
    checked_at = checked_at or datetime.now(UTC)
    archive = archive_result or archive_research_events(session, now=checked_at)
    repository = ResearchRepository(session)
    snapshot = replay_snapshot or repository.replay_event_snapshot()
    outcomes = repository.list_complete_outcomes()
    baseline = StrategyV2Repository(session).ensure_config_version(
        built_in_config_version("btc15_momentum_v2", V2_PARAMETERS)
    )
    if progress_callback is not None:
        # Release the config-version write before the fresh-session heartbeat
        # ticker starts, especially for SQLite-backed local validation.
        session.commit()
        progress_callback(
            "baseline_replay_started",
            {
                "last_successful_stage": "coverage",
                "post_archive_substage": "baseline_replay_scan",
                "replay_dataset_watermark": snapshot.watermark_id,
                "replay_total_events": snapshot.event_count,
                "replay_events_scanned": 0,
                "replay_pages_completed": 0,
                "replay_partitions_completed": 0,
                "replay_partitions_total": snapshot.partition_count,
                "replay_max_page_size": 0,
            },
        )
    reader = repository.frozen_replay_event_reader(snapshot)

    def report_replay_page(progress: FrozenReplayProgress) -> None:
        if progress_callback is None:
            return
        progress_callback(
            "baseline_replay_progress",
            {
                "last_successful_stage": "coverage",
                "post_archive_substage": "baseline_replay_scan",
                "replay_dataset_watermark": progress.watermark_id,
                "replay_total_events": progress.total_events,
                "replay_events_scanned": progress.events_scanned,
                "replay_pages_completed": progress.pages_completed,
                "replay_partitions_completed": progress.partitions_completed,
                "replay_partitions_total": progress.partitions_total,
                "replay_max_page_size": progress.max_page_size,
            },
        )

    replay = DeterministicReplayEngine().replay_ordered_pages(
        reader.iter_pages(progress_callback=report_replay_page),
        outcomes=outcomes,
        retain_decisions=False,
    )
    if reader.events_scanned != snapshot.event_count:
        raise RuntimeError(
            "Frozen replay scan was incomplete: "
            f"expected {snapshot.event_count}, scanned {reader.events_scanned}."
        )
    outcome_input_hash = _replay_outcome_input_hash(outcomes)
    run_id = (
        "replay-"
        + _hash(
            {
                "dataset": replay.dataset_hash,
                "outcomes": outcome_input_hash,
                "baseline": baseline.strategy_config_version_id,
            }
        )[:24]
    )
    replay_run = repository.create_replay_run(
        {
            "replay_run_id": run_id,
            "status": "RUNNING",
            "replay_engine_version": REPLAY_SCHEMA_VERSION,
            "label_schema_version": RESEARCH_LABEL_SCHEMA_VERSION,
            "code_commit_sha": resolve_code_version(),
            "baseline_strategy_config_version_id": baseline.strategy_config_version_id,
            "dataset_hash": replay.dataset_hash,
            "data_cutoff": checked_at,
            "start_at": snapshot.min_event_time,
            "end_at": snapshot.max_event_time,
            "unique_market_count": replay.unique_market_count,
            "event_count": replay.event_count,
            "partition_manifest": {
                "watermark_id": snapshot.watermark_id,
                "total_events": snapshot.event_count,
                "events_scanned": reader.events_scanned,
                "pages_completed": reader.pages_scanned,
                "partitions_completed": reader.partitions_completed,
                "partitions_total": snapshot.partition_count,
            },
            "cost_model": replay.cost_model,
            "zero_entry_report": replay.zero_entry_report,
            "blocker_funnel": replay.blocker_funnel,
            "raw_metrics": {
                "decision_count": replay.decision_count,
                "trade_count": len(replay.trades),
                "archive_coverage": archive.coverage,
                "outcome_input_hash": outcome_input_hash,
            },
            "adjusted_metrics": None,
            "warnings": [],
            "blockers": [],
            "started_at": checked_at,
        }
    )
    for trade in replay.trades:
        repository.insert_replay_trade(
            _replay_trade_values(
                replay_run_id=run_id,
                trade=trade,
                candidate_id=None,
                strategy_config_version_id=baseline.strategy_config_version_id,
                evidence_partition="full_dataset_baseline",
            )
        )
    repository.finish_replay_run(replay_run, status="COMPLETED", finished_at=checked_at)
    if progress_callback is not None:
        # Commit replay evidence before calibration so a later calibration failure
        # cannot roll back a completed archive/replay stage.
        session.commit()
        progress_callback(
            "baseline_replay_completed",
            {
                "last_successful_stage": "baseline_replay",
                "post_archive_substage": "baseline_replay",
                "replay_run_id": run_id,
                "zero_entry_report": replay.zero_entry_report,
                "replay_dataset_watermark": snapshot.watermark_id,
                "replay_total_events": snapshot.event_count,
                "replay_events_scanned": reader.events_scanned,
                "replay_pages_completed": reader.pages_scanned,
                "replay_partitions_completed": reader.partitions_completed,
                "replay_partitions_total": snapshot.partition_count,
                "replay_max_page_size": reader.max_page_size,
            },
        )
    calibration_status = "DISABLED"
    calibration_run_id = None
    if config.calibration_enabled:
        if progress_callback is not None:
            progress_callback(
                "calibration_started",
                {
                    "last_successful_stage": "baseline_replay",
                    "post_archive_substage": "calibration",
                    "replay_run_id": run_id,
                    "zero_entry_report": replay.zero_entry_report,
                },
            )
        calibration_run_id = (
            "calibration-" + _hash({"replay": run_id, "dataset": replay.dataset_hash})[:24]
        )
        existing_calibration = repository.get_calibration_run(calibration_run_id)
        if existing_calibration is not None and existing_calibration.holdout_used_at is not None:
            calibration_status = existing_calibration.status
        elif snapshot.event_count > CALIBRATION_MATERIALIZE_EVENT_LIMIT:
            calibration_status = "BLOCKED_REPLAY_EVENT_LIMIT"
            calibration_run = repository.create_calibration_run(
                {
                    "calibration_run_id": calibration_run_id,
                    "status": calibration_status,
                    "calibration_schema_version": CALIBRATION_SCHEMA_VERSION,
                    "replay_run_id": run_id,
                    "dataset_hash": replay.dataset_hash,
                    "code_commit_sha": resolve_code_version(),
                    "random_seed": int(_hash(calibration_run_id)[:8], 16),
                    "search_space_snapshot": None,
                    "partition_manifest": {"event_limit": CALIBRATION_MATERIALIZE_EVENT_LIMIT},
                    "frozen_holdout_hash": None,
                    "evaluated_candidate_count": 0,
                    "selected_candidate_id": None,
                    "training_metrics": None,
                    "validation_metrics": None,
                    "test_metrics": None,
                    "holdout_metrics": None,
                    "bootstrap_metrics": None,
                    "penalties": None,
                    "warnings": [],
                    "blockers": ["calibration_replay_event_limit_exceeded"],
                    "started_at": checked_at,
                    "finished_at": checked_at,
                    "holdout_used_at": None,
                }
            )
            del calibration_run
        else:
            calibration_events = [
                event
                for page in repository.frozen_replay_event_reader(snapshot).iter_pages()
                for event in page
            ]
            calibration = run_bounded_calibration(
                calibration_run_id=calibration_run_id,
                events=calibration_events,
                outcomes=outcomes,
            )
            calibration_status = calibration.status
            selected_metrics = calibration.candidate_metrics.get(
                calibration.selected_candidate_id or "", {}
            )
            calibration_run = repository.create_calibration_run(
                {
                    "calibration_run_id": calibration_run_id,
                    "status": calibration.status,
                    "calibration_schema_version": CALIBRATION_SCHEMA_VERSION,
                    "replay_run_id": run_id,
                    "dataset_hash": replay.dataset_hash,
                    "code_commit_sha": resolve_code_version(),
                    "random_seed": int(_hash(calibration_run_id)[:8], 16),
                    "search_space_snapshot": complete_search_space_snapshot(
                        calibration_run_id,
                        calibration.candidates,
                    ),
                    "partition_manifest": calibration.partition_manifest,
                    "frozen_holdout_hash": calibration.partition_manifest.get("holdout_hash"),
                    "evaluated_candidate_count": len(calibration.candidates),
                    "selected_candidate_id": calibration.selected_candidate_id,
                    "training_metrics": None,
                    "validation_metrics": calibration.candidate_metrics,
                    "test_metrics": None,
                    "holdout_metrics": selected_metrics.get("holdout"),
                    "bootstrap_metrics": selected_metrics.get("bootstrap"),
                    "penalties": selected_metrics.get("penalties"),
                    "warnings": list(calibration.warnings),
                    "blockers": list(calibration.blockers),
                    "started_at": checked_at,
                    "finished_at": checked_at,
                    "holdout_used_at": checked_at if calibration.selected_candidate_id else None,
                }
            )
            if calibration.status == "COMPLETED":
                for candidate in calibration.candidates:
                    candidate_metrics = calibration.candidate_metrics.get(
                        candidate.candidate_id,
                        {},
                    )
                    candidate_partition_trades = (
                        calibration.candidate_partition_replay_trades.get(
                            candidate.candidate_id,
                            {
                                "search_development": calibration.candidate_replay_trades.get(
                                    candidate.candidate_id,
                                    (),
                                )
                            },
                        )
                    )
                    if candidate.model_type == "BASELINE":
                        for partition, partition_trades in candidate_partition_trades.items():
                            for trade in partition_trades:
                                repository.insert_replay_trade(
                                    _replay_trade_values(
                                        replay_run_id=run_id,
                                        trade=trade,
                                        candidate_id=candidate.candidate_id,
                                        strategy_config_version_id=(
                                            baseline.strategy_config_version_id
                                        ),
                                        evidence_partition=partition,
                                    )
                                )
                        continue
                    artifact = candidate.model_artifact or {}
                    config_version_id = f"research-{candidate.candidate_id}"
                    StrategyV2Repository(session).ensure_config_version(
                        StrategyConfigVersionInput(
                            strategy_config_version_id=config_version_id,
                            strategy_id=candidate.generated_strategy_id,
                            architecture_version=V2_ARCHITECTURE_VERSION,
                            feature_schema_version=V2_FEATURE_SCHEMA_VERSION,
                            parameter_snapshot=candidate.parameters,
                            parameter_hash=_hash(candidate.parameters),
                            code_commit_sha=resolve_code_version(),
                            source="RESEARCH_CALIBRATION",
                            parent_config_version_id=baseline.strategy_config_version_id,
                            calibration_run_id=calibration_run.calibration_run_id,
                            lifecycle_state=LIFECYCLE_DRAFT,
                            approval_state="RESEARCH_ONLY",
                            model_type=candidate.model_type,
                            model_artifact_checksum=_hash(artifact),
                            data_cutoff=checked_at,
                            candidate_id=candidate.candidate_id,
                        )
                    )
                    repository.create_candidate(
                        {
                            "candidate_id": candidate.candidate_id,
                            "strategy_config_version_id": config_version_id,
                            "calibration_run_id": calibration_run.calibration_run_id,
                            "parent_strategy_config_version_id": (
                                baseline.strategy_config_version_id
                            ),
                            "generated_strategy_id": candidate.generated_strategy_id,
                            "architecture_version": V2_ARCHITECTURE_VERSION,
                            "feature_schema_version": V2_FEATURE_SCHEMA_VERSION,
                            "replay_schema_version": REPLAY_SCHEMA_VERSION,
                            "model_type": candidate.model_type,
                            "parameter_snapshot": candidate.parameters,
                            "feature_columns": list(candidate.feature_columns),
                            "model_artifact": artifact,
                            "model_artifact_checksum": _hash(artifact),
                            "training_metrics": candidate_metrics.get("training"),
                            "validation_metrics": candidate_metrics,
                            "test_metrics": candidate_metrics.get("development_test"),
                            "holdout_metrics": candidate_metrics.get("holdout"),
                            "governance_report": None,
                            "lifecycle_state": LIFECYCLE_DRAFT,
                            "eligibility_status": "RESEARCH_ONLY",
                        }
                    )
                    for partition, partition_trades in candidate_partition_trades.items():
                        for trade in partition_trades:
                            repository.insert_replay_trade(
                                _replay_trade_values(
                                    replay_run_id=run_id,
                                    trade=trade,
                                    candidate_id=candidate.candidate_id,
                                    strategy_config_version_id=config_version_id,
                                    evidence_partition=partition,
                                )
                            )
                    if candidate.candidate_id == calibration.selected_candidate_id:
                        repository.advance_candidate_governance(
                            candidate_id=candidate.candidate_id,
                            actor="ape-research-worker",
                        )
        if progress_callback is not None:
            progress_callback(
                "calibration_completed",
                {
                    "last_successful_stage": "calibration",
                    "replay_run_id": run_id,
                    "calibration_run_id": calibration_run_id,
                    "zero_entry_report": replay.zero_entry_report,
                },
            )
    return {
        "status": "completed",
        "archive": archive.coverage,
        "archive_event_count": archive.archived_events,
        "replay_run_id": run_id,
        "zero_entry_report": replay.zero_entry_report,
        "calibration_status": calibration_status,
        "calibration_run_id": calibration_run_id,
        "warnings": [],
        "blockers": [],
    }


def _replay_trade_values(
    *,
    replay_run_id: str,
    trade: ReplayTrade,
    candidate_id: str | None,
    strategy_config_version_id: str,
    evidence_partition: str,
) -> dict[str, Any]:
    trade_prefix = replay_run_id if candidate_id is None else f"{replay_run_id}-{candidate_id}"
    measurements = {
        **(trade.measurements if isinstance(trade.measurements, dict) else {}),
        "evidence_partition": evidence_partition,
    }
    return {
        "trade_id": f"{trade_prefix}-{evidence_partition}-{trade.trade_id}",
        "replay_run_id": replay_run_id,
        "candidate_id": candidate_id,
        "strategy_config_version_id": strategy_config_version_id,
        "market_ticker": trade.market_ticker,
        "side": trade.side,
        "entry_decision_at": trade.entry_decision_at,
        "entry_fill_at": trade.entry_fill_at,
        "entry_limit": trade.entry_limit,
        "entry_fill_price": trade.entry_fill_price,
        "entry_fill_event_id": trade.entry_fill_event_id,
        "exit_trigger_at": trade.exit_trigger_at,
        "exit_intent_at": trade.exit_intent_at,
        "exit_fill_at": trade.exit_fill_at,
        "exit_limit": trade.exit_limit,
        "exit_fill_price": trade.exit_fill_price,
        "exit_fill_event_id": trade.exit_fill_event_id,
        "status": trade.status,
        "gross_pnl_cents": trade.gross_pnl_cents,
        "fee_cents": trade.fee_cents,
        "net_pnl_cents": trade.net_pnl_cents,
        "holding_duration_ms": trade.holding_duration_ms,
        "mfe_cents": trade.mfe_cents,
        "mae_cents": trade.mae_cents,
        "time_to_mfe_ms": trade.time_to_mfe_ms,
        "time_to_mae_ms": trade.time_to_mae_ms,
        "entry_reason": trade.entry_reason,
        "exit_reason": trade.exit_reason,
        "timing_tier": trade.timing_tier,
        "volatility_regime": measurements.get("volatility_regime"),
        "liquidity_regime": measurements.get("liquidity_regime"),
        "entry_feature_snapshot_id": measurements.get("entry_feature_snapshot_id"),
        "exit_feature_snapshot_id": measurements.get("exit_feature_snapshot_id"),
        "lifecycle_version": measurements.get(
            "lifecycle_version", "momentum_v2_lifecycle_v2"
        ),
        "measurements": measurements,
    }


def _record_research_heartbeat(
    session,
    config: AppConfig,
    safety,
    started_at: datetime,
    heartbeat_at: datetime,
    result: dict[str, Any],
) -> None:
    archive = result.get("archive")
    last_error = result.get("last_error")
    WorkerHeartbeatRepository(session).record_heartbeat(
        WorkerHeartbeatInput(
            service_name=WORKER_SERVICE_RESEARCH,
            started_at=started_at,
            heartbeat_at=heartbeat_at,
            app_mode=config.app_mode.value,
            is_safe=safety.is_safe,
            metadata={
                "mode": "research",
                "research": {
                    "enabled": config.research_enabled,
                    "calibration_enabled": config.calibration_enabled,
                    "poll_seconds": config.research_poll_seconds,
                    "worker_role": "research",
                    "worker_state": result.get("worker_state", "healthy"),
                    "cycle_state": result.get("cycle_state", "healthy"),
                    "current_stage": result.get("current_stage"),
                    "post_archive_substage": result.get("post_archive_substage"),
                    "last_successful_stage": result.get("last_successful_stage"),
                    "cycle_id": result.get("cycle_id"),
                    "cycle_running": result.get("worker_state") == "running",
                    "cycle_started_at": result.get("cycle_started_at"),
                    "cycle_finished_at": result.get("cycle_finished_at"),
                    "current_source_table": result.get("current_source_table"),
                    "completed_archive_batches": result.get("completed_archive_batches"),
                    "archive_event_count": result.get("archive_event_count"),
                    "archived_counts_by_type": result.get("archived_counts_by_type"),
                    "last_progress_at": result.get("last_progress_at"),
                    "failed_stage": result.get("failed_stage"),
                    "last_archive_run": archive,
                    "last_archive_batch": result.get("last_archive_batch"),
                    "archive_selector_mode": result.get("archive_selector_mode"),
                    "archive_source_cursor": result.get("archive_source_cursor"),
                    "archive_bootstrap_target": result.get("archive_bootstrap_target"),
                    "archive_verification_window_start": result.get(
                        "archive_verification_window_start"
                    ),
                    "archive_verification_window_end": result.get(
                        "archive_verification_window_end"
                    ),
                    "archive_missing_rows_archived": result.get(
                        "archive_missing_rows_archived", 0
                    ),
                    "archive_bootstrap_complete": result.get("archive_bootstrap_complete"),
                    "archive_scheduling_mode": result.get("archive_scheduling_mode"),
                    "archive_bootstrap_pending_after_budget": result.get(
                        "archive_bootstrap_pending_after_budget"
                    ),
                    "archive_tail_pending_after_budget": result.get(
                        "archive_tail_pending_after_budget"
                    ),
                    "archive_sources_served": _bounded_strings(
                        result.get("archive_sources_served")
                    ),
                    "archive_operations_by_source": result.get(
                        "archive_operations_by_source"
                    ),
                    "post_archive_allowed": result.get("post_archive_allowed"),
                    "post_archive_deferred_reason": result.get(
                        "post_archive_deferred_reason"
                    ),
                    "labels_processed": result.get("labels_processed"),
                    "labels_remaining": result.get("labels_remaining"),
                    "association_rows_processed": result.get("association_rows_processed"),
                    "association_rows_remaining": result.get("association_rows_remaining"),
                    "label_markets_processed": result.get("label_markets_processed"),
                    "label_markets_remaining": result.get("label_markets_remaining"),
                    "label_markets_blocked_missing_market": result.get(
                        "label_markets_blocked_missing_market"
                    ),
                    "replay_dataset_watermark": result.get("replay_dataset_watermark"),
                    "replay_total_events": result.get("replay_total_events"),
                    "replay_events_scanned": result.get("replay_events_scanned"),
                    "replay_pages_completed": result.get("replay_pages_completed"),
                    "replay_partitions_completed": result.get(
                        "replay_partitions_completed"
                    ),
                    "replay_partitions_total": result.get("replay_partitions_total"),
                    "replay_max_page_size": result.get("replay_max_page_size"),
                    "coverage_dataset_watermark": result.get("coverage_dataset_watermark"),
                    "coverage_total_events": result.get("coverage_total_events"),
                    "coverage_events_scanned": result.get("coverage_events_scanned"),
                    "coverage_pages_completed": result.get("coverage_pages_completed"),
                    "coverage_partitions_completed": result.get(
                        "coverage_partitions_completed"
                    ),
                    "coverage_partitions_total": result.get("coverage_partitions_total"),
                    "coverage_max_page_size": result.get("coverage_max_page_size"),
                    "last_replay_run": result.get("replay_run_id"),
                    "last_calibration_run": result.get("calibration_run_id"),
                    "zero_entry_report": result.get("zero_entry_report"),
                    "last_error": last_error,
                    "statement_timeout_detected": bool(
                        isinstance(last_error, dict)
                        and last_error.get("statement_timeout_detected")
                    ),
                    "warnings": _bounded_strings(result.get("warnings")),
                    "blockers": _bounded_strings(result.get("blockers")),
                },
            },
        )
    )


def _cycle_progress(
    *,
    previous: dict[str, Any] | None = None,
    state: str,
    stage: str,
    cycle_started_at: datetime,
    last_successful_stage: str | None = None,
    archive: ArchiveResult | None = None,
    warnings: list[str] | None = None,
    blockers: list[str] | None = None,
    error: Exception | None = None,
    **details: Any,
) -> dict[str, Any]:
    result = dict(previous or {})
    result.update(
        {
        "worker_state": state,
        "cycle_state": state,
        "current_stage": stage,
        "last_successful_stage": (
            last_successful_stage
            if last_successful_stage is not None
            else result.get("last_successful_stage")
        ),
        "cycle_started_at": cycle_started_at.isoformat(),
        "cycle_finished_at": datetime.now(UTC).isoformat()
        if state in {"healthy", "partial", "error", "degraded"}
        else None,
        "last_error": _sanitized_error(error) if error is not None else None,
        "warnings": _bounded_strings(warnings),
        "blockers": _bounded_strings(blockers),
        **details,
        }
    )
    if archive is not None:
        result["archive"] = archive.coverage
        result["archive_event_count"] = archive.archived_events
    return result


def _cycle_id(cycle_started_at: datetime) -> str:
    return "research-" + _hash({"started_at": cycle_started_at.isoformat()})[:24]


def _is_archive_duplicate_identity_error(error: IntegrityError) -> bool:
    message = str(error).lower()
    has_duplicate_marker = "unique" in message or "duplicate" in message
    return has_duplicate_marker and "research_replay_events" in message


def _sanitized_error(error: Exception) -> dict[str, Any]:
    statement_timeout = _is_statement_timeout(error)
    return {
        "type": error.__class__.__name__[:80],
        "code": "research_statement_timeout"
        if statement_timeout
        else "research_stage_failed",
        "statement_timeout_detected": statement_timeout,
    }


def _is_statement_timeout(error: Exception) -> bool:
    message = str(error).lower()
    return any(
        marker in message
        for marker in (
            "statement timeout",
            "query canceled",
            "query_cancelled",
            "canceling statement",
        )
    )


def _bounded_strings(value: Any, *, limit: int = 8) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item)[:128] for item in value[:limit]]


def _hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _replay_outcome_input_hash(outcomes) -> str:
    """Hash every resolved-outcome field that can change replay or label evidence."""
    return _hash(
        [
            {
                "outcome_id": outcome.outcome_id,
                "market_ticker": outcome.market_ticker,
                "market_open_at": outcome.market_open_at,
                "market_close_at": outcome.market_close_at,
                "expiration_at": outcome.expiration_at,
                "boundary": outcome.boundary,
                "result_side": outcome.result_side,
                "settlement_value": outcome.settlement_value,
                "final_reference_value": outcome.final_reference_value,
                "final_minute_reference_average": outcome.final_minute_reference_average,
                "outcome_status": outcome.outcome_status,
                "outcome_source": outcome.outcome_source,
                "source_payload_hash": outcome.source_payload_hash,
                "resolved_at": outcome.resolved_at,
                "expected_frame_count": outcome.expected_frame_count,
                "actual_frame_count": outcome.actual_frame_count,
                "coverage_percentage": outcome.coverage_percentage,
                "maximum_event_gap_seconds": outcome.maximum_event_gap_seconds,
                "quality_flags": outcome.quality_flags,
            }
            for outcome in outcomes
        ]
    )
