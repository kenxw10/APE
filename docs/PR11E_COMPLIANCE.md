# PR 11e Compliance Matrix

PR 11e is a bounded fix-forward for post-bootstrap research scheduling. It keeps
historical bootstrap strict and fail-closed, then gives completed TAIL cursors
deterministic fair service without changing the existing archive batch or cycle
budgets.

## R1-R9 Matrix

| Requirement | Implementation | Direct evidence |
| --- | --- | --- |
| R1 strict bootstrap gating | `archive_bootstrap_required()` validates every append-only cursor is a valid TAIL cursor with `bootstrap_complete=true`; `_archive_stage()` keeps canonical strict scheduling and defers all post-archive work while incomplete. | `test_bootstrap_gate_requires_all_six_valid_tail_cursors`; `test_bootstrap_strict_budget_gates_every_post_archive_stage`; existing PR 11c cursor tests. |
| R2 deterministic fair TAIL scheduling | `ResearchWorker._archive_stage()` performs a canonical one-operation fairness pass followed by deterministic canonical round-robin passes, with 250-row source batches and the existing 20-operation cap. | `test_tail_fair_scheduler_is_canonical_bounded_and_uses_remaining_budget`. |
| R3 tail backlog does not gate downstream work | `ArchiveSchedulingResult.post_archive_allowed` remains true in `TAIL_FAIR`; tail budget exhaustion is recorded and association/labels/coverage/replay continue. | `test_tail_budget_allows_post_archive_work_and_status_is_truthful`; `test_six_tail_cycles_continue_label_progress_under_continuous_ingest`. |
| R4 frozen snapshot consistency | The worker captures `ResearchRepository.replay_event_snapshot()` immediately after the committed archive slice and reuses it for coverage and replay. | `test_frozen_snapshot_excludes_events_inserted_after_watermark`; existing PR 11b frozen-reader tests. |
| R5 bounded labels and resumability | Existing 25-market label cap, association-zero gate, per-stage commits, and durable progress are unchanged; fair tail work does not reset label progress. | `test_six_tail_cycles_continue_label_progress_under_continuous_ingest`; existing research archive/runtime tests. |
| R6 status and warning semantics | Heartbeats and `/research/status` expose `archive_scheduling_mode`, bootstrap/tail budget flags, served sources, per-source operation counts, `post_archive_allowed`, and deferred bootstrap reason. | `test_tail_budget_allows_post_archive_work_and_status_is_truthful`; strict metadata assertions. |
| R7 failure and transaction safety | Existing per-source advisory locks, one-operation commits, fresh-session duplicate retry, rollback-safe cursors, timeout recovery, and independent post-archive transactions remain unchanged. | Existing `tests/test_pr11c_scope_contract.py`, `tests/test_research_runtime.py`, and `tests/test_research_worker.py`. |
| R8 direct acceptance coverage | The new scope-contract tests cover strict blocking, fair service, starvation prevention, operation and batch bounds, deterministic order, tail continuation, repeated label progress, frozen watermarks, and safety boundaries. | `tests/test_pr11e_scope_contract.py`. |
| R9 documentation and release | Research and Railway rollout docs explain strict versus fair modes, canonical fairness, tail continuation, status fields, rollout, and unchanged deployment/safety boundaries. | `docs/RESEARCH_AND_CALIBRATION.md`; `docs/RAILWAY.md`; this matrix and PR body. |

## Runtime Invariants

- Migration: none.
- New required production environment variables: none.
- New Railway services: none.
- `ARCHIVE_BATCH_SIZE` remains `250`.
- `ARCHIVE_MAX_BATCHES_PER_CYCLE` remains `20`.
- `DB_STATEMENT_TIMEOUT_MS` remains unchanged.
- `RESEARCH_POLL_SECONDS` remains unchanged.
- Retention, source normalization, event identity, label schema/horizons, coverage,
  replay, calibration search, strategy thresholds/features/timing/scoring/fees/
  lifecycle, candidates, governance, and safety behavior remain unchanged.
- Safety remains `APP_MODE=DRY_RUN`, `CALIBRATION_ENABLED=false`,
  `TRADING_ENABLED=false`, and `EXECUTE=false`.
- No paper/live trading, orders, cancels, private feeds, credentials, account reads,
  balances, fills, or execution behavior are added.

## Scheduling Contract

`BOOTSTRAP_STRICT` is selected whenever any one of the six append-only cursor rows is
missing, not in TAIL mode, incomplete, or missing a valid cursor. It preserves
canonical source order and blocks all downstream stages after a bounded budget.

`TAIL_FAIR` is selected only after all six cursor rows are valid TAIL cursors. The
fairness pass gives each pending source one operation in this order:

`markets`, `reference_ticks`, `orderbook_snapshots`, `public_trades`,
`strategy_feature_snapshots`, `strategy_trade_intents`,
`strategy_position_outcomes`.

Remaining operations repeat the same order. A continuously pending public-trades tail
can use additional operations, but later sources receive their first operation before
that additional throughput. Tail budget exhaustion is a warning, not a blocker.

## Validation Plan

Required focused commands:

```text
python -m pytest tests/test_pr11e_scope_contract.py tests/test_research_worker.py tests/test_research_archive.py tests/test_research_runtime.py tests/test_pr11c_scope_contract.py tests/test_pr11b_scope_contract.py -q
python -m pytest tests/test_replay_engine.py tests/test_research_api.py tests/test_pr11_scope_contract.py tests/test_storage_retention.py tests/test_storage_api.py tests/test_worker_roles.py -q
python -m pytest
python -m ruff check .
python -m compileall src scripts
python -m pip check
git diff --check
python scripts/research_smoke.py
```

The final PR body will record the exact final head, unsharded workflow run/job,
pytest totals and skips, static checks, and the literal R1-R9 audit. The PR remains
draft until review and hosted validation complete. It will not be merged or deployed
by this change.
