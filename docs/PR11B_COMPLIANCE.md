# PR 11b Compliance Matrix

PR 11b is a bounded-runtime correction for the existing database-only research
worker. It adds no migration, environment variable, Railway service, dependency,
strategy threshold, fee change, lifecycle change, governance change, paper path,
live path, order path, private account path, or dashboard change.

| Requirement | Implementation | Direct evidence |
| --- | --- | --- |
| R1 frozen replay input | `FrozenReplayEventReader` captures an event-ID watermark, count, time range, and page metrics before a keyset scan. It excludes `COVERAGE_REPORT` rows and orders by the exact replay key. | `test_r1_frozen_reader_uses_watermark_keyset_pages_and_never_list_events_none` proves later rows are excluded and production never calls `list_events(limit=None)`. |
| R2 incremental replay parity | The public `replay()` remains a small-fixture API; production uses `replay_ordered_pages()` and retains only active lifecycle state, trades, bounded samples, and optional decisions. | `test_r2_incremental_replay_matches_small_fixture_api_for_pages_and_ties` compares every result field at page sizes 1, 2, 17, and 250. |
| R3 bounded replay evidence | Production does not retain decision objects. Exact counters and unique-market state are retained; distributions are exact through 2,000 values and then expose deterministic sampled evidence with schema metadata. | `test_r3_large_incremental_replay_keeps_decisions_and_distributions_bounded`. |
| R4 bounded labels | At most 25 current-schema markets are labeled per cycle. Labels query only the market interval plus the 65-second label horizon and commit before later stages. | `test_r4_labels_are_bounded_resumable_and_mark_current_schema`; `test_r6_error_after_a_label_commit_preserves_the_resumable_label_progress`. |
| R5 bounded coverage | Coverage consumes the same frozen reader and persists its report only after its complete scan. It keeps the existing coverage payload and records scan metrics. | `test_r5_coverage_uses_the_same_frozen_snapshot_and_excludes_later_rows`. |
| R6 truthful worker stages | Worker order is archive, labels, full frozen coverage, full frozen baseline replay, and optional calibration. Label backlog returns `partial`; coverage, labels, and replay publish fresh running heartbeats. | `tests/test_research_runtime.py`, including long association-label, coverage, baseline-replay, and calibration stages. |
| R7 calibration guard | Disabled calibration does nothing. Enabled calibration only materializes a second bounded frozen scan when the dataset is at or below the fixed 20,000-event safety limit; otherwise it persists `BLOCKED_REPLAY_EVENT_LIMIT`. | `test_r7_disabled_calibration_cannot_run_or_materialize_replay_events`. |
| R8 acceptance coverage | The PR-specific contract checks reader, parity, page boundaries, later inserts, bounded state, label resumption, coverage, errors after committed progress, and the disabled-calibration boundary. | `tests/test_pr11b_scope_contract.py`. |
| R9 documentation and boundaries | This document, `PR11_COMPLIANCE.md`, research documentation, and Railway notes describe only the bounded runtime correction. | Static scope assertions in `test_r8_and_r9_keep_calibration_disabled_and_scope_boundaries_static`. |

## Runtime Facts

- Archive batches remain 250 rows with the existing 20-batch runtime budget.
- Label work is bounded to 25 markets per cycle and resumes from the durable
  `quality_flags.label_schema_version` marker.
- Replay and coverage share one post-label frozen event snapshot. A later insert
  cannot enter either run.
- Existing small-fixture replay keeps the exact SHA-256 input hash:
  `sha256("|".join(event_hashes))`.
- A worker never reports coverage or replay complete until the corresponding full
  frozen scan is complete.

## Validation

`python -m pytest --collect-only -q -o addopts=` collected 549 tests. All 549
completed locally in deterministic test-file shards; the 63-node collector file
ran in four non-overlapping 16/16/16/15 node shards and the 76-node observer file
ran in four non-overlapping 19-node shards. The desktop sandbox stopped the exact
unsharded `python -m pytest` process at roughly 26 percent without a failure or
exit result, so the existing pull-request workflow remains the authoritative exact
unsharded gate. That workflow runs:

```text
python -m pytest
python -m ruff check .
python -m compileall src scripts
python -m pip check
```

Local focused suites, all local shards, Ruff, `compileall`, `pip check`,
`git diff --check`, and `python scripts/research_smoke.py` passed. The smoke test
confirmed archived labels, replay/calibration evidence, read APIs, and the absence
of paper/live/order/private/account capability.

## Literal Self-Audit

R1-R9 were implemented without reducing, deferring, substituting, or relabeling
their required behavior. This PR contains no migration, environment variable,
service, deployment, strategy-frequency, threshold, or safety-policy change.
The application remains DRY_RUN-only with `TRADING_ENABLED=false` and
`EXECUTE=false`; it has no paper trading, live trading, order, cancel, private
channel, account, or credential capability.
