# PR 11 Compliance Matrix

`PR 11 — Deterministic replay, zero-entry audit, and governed calibration`

| Requirement | Exact implementation files | Exact behavioral tests | Status |
| --- | --- | --- | --- |
| R1 immutable research schema and migration | `src/ape/db/models.py`, `src/ape/db/migrations.py` | `tests/test_db_schema.py`, `tests/test_pr11_scope_contract.py` | COMPLIANT |
| R2 canonical feature vector and evaluator parity | `src/ape/strategy/momentum_v2.py`, `src/ape/repositories/inputs.py` | `tests/test_momentum_v2.py`, `tests/test_replay_engine.py`, `tests/test_pr11_scope_contract.py` | COMPLIANT |
| R3 isolated research worker | `src/ape/worker/main.py`, `src/ape/research/service.py`, `src/ape/worker/services.py` | `tests/test_research_worker.py`, `tests/test_pr11_scope_contract.py` | COMPLIANT |
| R4 normalized archival and public outcome reconciliation | `src/ape/research/archive.py`, `src/ape/research/repository.py` | `tests/test_research_archive.py`, `tests/test_research_worker.py`, `tests/test_pr11_scope_contract.py` | COMPLIANT |
| R5 zero-entry audit | `src/ape/research/replay.py` | `tests/test_replay_engine.py`, `tests/test_pr11_scope_contract.py` | COMPLIANT |
| R6 executable labels and verified fee model | `src/ape/research/archive.py`, `src/ape/research/fees.py` | `tests/test_research_archive.py`, `tests/test_replay_engine.py`, `tests/test_pr11_scope_contract.py` | COMPLIANT |
| R7 deterministic no-lookahead replay | `src/ape/research/replay.py` | `tests/test_replay_engine.py`, `tests/test_pr11_scope_contract.py` | COMPLIANT |
| R8 market partitions and frozen holdout | `src/ape/research/calibration.py`, `src/ape/research/service.py` | `tests/test_calibration.py`, `tests/test_research_worker.py`, `tests/test_pr11_scope_contract.py` | COMPLIANT |
| R9 bounded heuristic and NumPy logistic search | `pyproject.toml`, `src/ape/research/calibration.py` | `tests/test_calibration.py`, `tests/test_pr11_scope_contract.py` | COMPLIANT |
| R10 objective, penalties, and market bootstrap | `src/ape/research/calibration.py` | `tests/test_calibration.py`, `tests/test_pr11_scope_contract.py` | COMPLIANT |
| R11 immutable governance limits | `src/ape/research/calibration.py`, `src/ape/research/repository.py` | `tests/test_calibration.py`, `tests/test_pr11_scope_contract.py` | COMPLIANT |
| R12 operator-pinned DRY_RUN challenger | `src/ape/research/pin.py`, `src/ape/strategy/observer.py` | `tests/test_candidate_pin.py`, `tests/test_pr11_scope_contract.py` | COMPLIANT |
| R13 bounded read-only research APIs | `src/ape/api/main.py`, `src/ape/research/status.py` | `tests/test_research_api.py`, `tests/test_pr11_scope_contract.py` | COMPLIANT |
| R14 retention and durable evidence | `src/ape/storage/retention.py`, `src/ape/repositories/storage_retention.py` | `tests/test_storage_retention.py`, `tests/test_storage_api.py`, `tests/test_pr11_scope_contract.py` | COMPLIANT |
| R15 fixture tests, documentation, and deployment | `tests/test_pr11_scope_contract.py`, `docs/RESEARCH_AND_CALIBRATION.md`, `README.md`, `PROJECT_CONTEXT.md`, `docs/RAILWAY.md`, `docs/PR_RUNBOOK.md` | `tests/test_pr11_scope_contract.py` | COMPLIANT |

The single migration is `0010_research_replay_calibration`. Retained tables are
`research_replay_events` (30 days) and `research_replay_trades` (180 days).
Market outcomes, replay-run summaries, calibration runs, candidates, governance
events, and strategy config versions are durable. The verified general Kalshi taker
fee model is `ceil(0.07 * contracts * price * (1 - price), $0.01)` from the
2026-02-05 fee schedule; each replay run stores source metadata and a checksum.

No required scope was deferred, simplified, substituted, broadened, or relabeled.

## Sandbox-Sharded Full-Suite Verification

The hosted sandbox terminates a foreground process at approximately 30 seconds,
so the required exact collection command and a deterministic test sharding
procedure were used for local full-suite coverage. The implementation scope was
not changed to accommodate that infrastructure limit.

- Direct collection command: `python -m pytest --collect-only -q`
- Direct collection output: `docs/validation/pr11/collect-only-output.txt`
- Direct collection summary: `docs/validation/pr11/collect-only-summary.json`
- Complete sorted node-ID manifest: `docs/validation/pr11/collected-nodeids.json`
- Deterministic shard plan and exact commands: `docs/validation/pr11/shard-plan.json`
- Per-shard complete output: `docs/validation/pr11/logs/`
- Per-shard JUnit XML: `docs/validation/pr11/junit/`
- Per-shard exit status and assigned nodes: `docs/validation/pr11/results/`
- Aggregate coverage proof, counts, commands, exit codes, and timings:
  `docs/validation/pr11/shard-report.json`

The direct collection command reported 394 tests, matching the 394-node
manifest. The plan divides that sorted collection into 47 non-overlapping
shards, using whole test files where possible and eight-node sorted shards only
for `tests/test_kalshi_ws_collector.py` and
`tests/test_strategy_observer.py`.

The aggregate verifier result is exact: 394 collected, 394 assigned, 394
executed, 394 unique assigned, 394 unique executed, 0 omitted, 0 unexecuted,
0 duplicate assignments, and 0 duplicate executions. JUnit recorded 394
passed, 0 failed, 0 errors, and 0 skipped. Every shard exited 0.

The longest individual tests were 4.110 seconds
(`tests.test_worker::test_worker_enabled_storage_retention_runs_periodic_task`)
and 3.503 seconds
(`tests.test_kalshi_ws_collector::test_collector_persists_market_liveness_heartbeats_before_stream_gate`).
The largest cumulative files were `tests/test_kalshi_ws_collector.py` at
115.810 seconds and `tests/test_strategy_observer.py` at 99.369 seconds. The
sum of all shard durations was 543.815 seconds. JUnit test-call time totals
319.261 seconds; the remaining 224.554 seconds is the expected repeated
collection, process startup, and fixture cost of 47 isolated pytest processes,
not a claim about unsharded duration. The required 136-test focused command
also completed locally in approximately 108 seconds. This explains the earlier
approximately-18-percent foreground observation as normal suite duration, not
an implementation regression or a hanging test: every isolated shard completed
within the sandbox limit and passed.

Sandbox-sharded full-suite coverage passed; exact unsharded `python -m pytest`
is required in PR CI. R1-R15 were not reduced, deferred, substituted, or
relabeled for the sandbox limitation.
