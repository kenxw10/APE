# PR 11c Compliance Matrix

PR 11c is a bounded fix-forward for the existing database-only research worker.
It replaces the repeated append-only archive prefix anti-join with a durable,
restart-safe cursor and bounded bootstrap verification. Mutable market refreshes
remain separate. Calibration remains disabled during rollout.

## R1-R8 Matrix

| Requirement | Implementation | Direct evidence |
| --- | --- | --- |
| R1 durable cursor migration | `src/ape/db/migrations.py` adds `0011_research_archive_cursors`; `src/ape/db/models.py` defines one small row per append-only source with selector mode, numeric cursor, frozen target, verification window, schema version, bootstrap state, and timestamps. Existing PostgreSQL migration advisory locking makes concurrent redeploys idempotent. | `test_pr11c_scope_contract.py::test_all_append_only_sources_use_cursors_and_markets_remain_separate`; `tests/test_db_schema.py`. |
| R2 bounded gap-safe bootstrap | `src/ape/research/archive.py::_append_only_source_rows` snapshots min/max, freezes the target, uses `ARCHIVE_BOOTSTRAP_WINDOW_SPAN`, bounds every anti-join by source ID, repairs missing rows before window advancement, and switches to tail only after the frozen target. | `test_bootstrap_anti_join_is_bounded_by_ids_and_250_rows`; `test_bootstrap_repairs_gaps_below_high_archived_id_before_tail`; `test_rows_above_frozen_bootstrap_target_are_archived_by_tail`. |
| R3 indexed steady-state tail | `src/ape/research/archive.py` selects `id > source_cursor ORDER BY id ASC LIMIT 250` without an anti-join and updates the cursor in the same transaction as archive persistence. | `test_tail_selection_is_keyset_bounded_and_has_no_anti_join`; `test_rollback_keeps_archive_rows_and_cursor_atomic_then_restart_resumes`. |
| R4 bounded pending probe | `archive_research_source_pending` uses cursor state plus an ID-only `LIMIT 1` query for append-only sources and does not call `_archive_source_rows` or load raw payloads. Market refresh pending remains a separate bounded ID probe. | `test_pending_probe_is_bounded_id_only_and_does_not_materialize_source_rows`; `test_append_only_production_selection_never_calls_the_legacy_selector`. |
| R5 orchestration preservation | `src/ape/research/service.py` preserves source order, 250-row batches, the 20-operation budget, one operation per transaction, advisory locks, fresh-session duplicate retry, heartbeats, and PR 11b downstream gating. Cursor fields are carried through research heartbeats and `/research/status`. | `test_empty_bootstrap_window_is_durable_work_and_counts_against_budget`; `tests/test_research_runtime.py`; `tests/test_pr11b_scope_contract.py`. |
| R6 failure and restart semantics | Cursor and archive writes share the commit boundary; rollback leaves both unchanged, duplicate retry uses a fresh session, retention gaps do not regress the cursor, and a sanitized timeout heartbeat is followed by resumable progress. | `test_rollback_keeps_archive_rows_and_cursor_atomic_then_restart_resumes`; `test_duplicate_retry_does_not_double_advance_cursor`; `test_timeout_is_sanitized_and_next_cycle_can_resume_cursor_archive`. |
| R7 direct behavioral coverage | The PR-specific suite exercises production SQL shape, bootstrap windows, tailing, pending reads, deliberate gaps, frozen-target tail rows, operation-budget accounting, rollback, restart, duplicate retry, timeout recovery, all six cursor sources, and the unchanged market boundary. | `tests/test_pr11c_scope_contract.py` (17 direct behavioral tests). |
| R8 documentation and rollout | This document, the Railway runbook, and the PR body describe bounded bootstrap, keyset tailing, status fields, migration, validation, and the unchanged DRY_RUN rollout boundary. | `docs/PR11C_COMPLIANCE.md`; `docs/RAILWAY.md`; `docs/PR_RUNBOOK.md`. |

## Exact Migration

`0011_research_archive_cursors` creates only `research_archive_cursors`:

- `source_table` primary key;
- `selector_mode`;
- `source_cursor`;
- `frozen_bootstrap_target`;
- `verification_window_start` and `verification_window_end`;
- `schema_version` and `bootstrap_complete`;
- `created_at` and `updated_at`.

There are no indexes on source tables or `research_replay_events` in this PR.
The existing PostgreSQL migration transaction lock remains the concurrency
boundary for auto-redeploys.

## Operational Boundary

- New required environment variables: none.
- New Railway services: none.
- `DB_STATEMENT_TIMEOUT_MS`: unchanged.
- `RESEARCH_POLL_SECONDS`: unchanged.
- Archive batch size: unchanged at 250.
- Archive cycle budget: unchanged at 20 operations.
- Label, replay, coverage, retention, strategy, lifecycle, fee, threshold, and governance semantics: unchanged.
- `APP_MODE=DRY_RUN`, `CALIBRATION_ENABLED=false`, `TRADING_ENABLED=false`, and `EXECUTE=false` remain required for rollout.
- No paper/live trading, orders, cancels, private channels, account reads, credentials, or execution behavior were added.

## Validation

The final local validation completed before publication:

- `python -m pytest tests/test_pr11c_scope_contract.py tests/test_research_archive.py tests/test_research_runtime.py tests/test_research_worker.py tests/test_pr11b_scope_contract.py -q`: 74 passed.
- `python -m pytest tests/test_replay_engine.py tests/test_research_api.py tests/test_pr11_scope_contract.py tests/test_storage_retention.py tests/test_storage_api.py tests/test_worker_roles.py -q`: 104 passed.
- `python -m pytest`: 577 passed, 284 warnings in 326.86 seconds.
- `python -m ruff check .`: passed.
- `python -m compileall src scripts`: passed.
- `python -m pip check`: passed with no broken requirements.
- `git diff --check`: passed.
- `python scripts/research_smoke.py`: passed; migration idempotency, archive/labels/coverage, official outcomes, read APIs, governance invariants, and no-execution invariants were all true.

The existing `.github/workflows/pr-validation.yml` runs the exact unsharded
`python -m pytest` command on pull requests, followed by Ruff, compileall, and
pip check. The implementation-head run passed:

- `validation` passed in 2m14s on commit `dc98dfa`: [GitHub Actions run 29304346734](https://github.com/kenxw10/APE/actions/runs/29304346734).

The documentation-only follow-up commit is also validated by the same workflow
before final reporting.

## Literal Prompt-to-Diff Self-Audit

- R1 PASS: one migration creates only the small `research_archive_cursors` table and seeds one durable row for each append-only source.
- R2 PASS: bootstrap uses a frozen numeric-ID target, bounded verification windows, and repairs gaps before tailing.
- R3 PASS: steady state uses numeric keyset selection with `id > source_cursor`, ascending order, and `LIMIT 250`.
- R4 PASS: pending checks are ID-only and bounded; mutable market refresh remains on its separate path.
- R5 PASS: source order, batch size, 20-operation budget, transaction boundaries, advisory locks, retry behavior, heartbeat fields, and PR 11b downstream behavior are preserved.
- R6 PASS: cursor and archive writes commit together; rollback, duplicate retry, timeout recovery, and restart resume are directly tested.
- R7 PASS: the PR-specific suite contains 17 direct behavioral contract tests plus the focused regression suites.
- R8 PASS: rollout documentation, status visibility, migration confirmation, unchanged safety boundary, and validation evidence are included; no new environment variable or service is introduced.

R1-R8 were not reduced, deferred, substituted, or relabeled.
