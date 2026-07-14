# PR 11b Compliance Matrix

PR 11b is a bounded-runtime correction for the existing database-only research
worker. It preserves the existing DRY_RUN-only research boundary. It adds no
migration, required environment variable, Railway service, dependency, strategy
threshold, feature, lifecycle, fee, governance, paper-trading, live-trading,
order, private-channel, account, or dashboard behavior.

## Remediation Matrix

| Finding | Corrected implementation | Direct evidence |
| --- | --- | --- |
| F1 reference association | `src/ape/research/archive.py` returns bounded processed/remaining counts and commits every association batch. `src/ape/research/service.py` makes association a durable `association_labels` substage that gates labels, coverage, and replay. `src/ape/research/status.py` exposes association and label counters. | `test_f1_association_batches_commit_resume_and_gate_labels_coverage_and_replay`; `test_f1_committed_association_progress_survives_a_later_label_failure`. |
| F2 full-dataset replay denominator | `src/ape/research/replay.py` records every non-null market ticker consumed by the frozen dataset, including non-feature rows and incomplete feature rows, and uses that set for market counts, rates, and continuation gaps. | `tests/fixtures/pr11a_replay_golden.json`; `test_f2_pr11a_golden_replay_semantics_match_small_and_database_bounded_paths`. The test compares all unchanged fields to the captured PR 11a output and separately proves the audit-required full-dataset continuation correction. |
| F3 advancing scan progress | `src/ape/research/repository.py` emits bounded page/partition callbacks after each completed page. `src/ape/research/service.py` starts coverage/replay with frozen totals, updates in-memory counters monotonically, and lets the capped ticker persist them. `src/ape/research/status.py` exposes the progress fields and `post_archive_substage`. | `test_research_scan_heartbeats_publish_advancing_page_progress` pauses two coverage and two replay pages and proves fresh persisted/status counters rise before completion and stop after terminal state. |
| F4 unmatched outcomes | `src/ape/research/archive.py` distinguishes persisted labels from unresolved missing-market outcomes. `src/ape/research/service.py` reports a partial cycle with an explicit blocker and defers downstream work until the source market appears. | `test_f4_unmatched_outcomes_block_downstream_work_and_resume_when_market_arrives`. |
| F5 zero-entry schema and sampling | `src/ape/research/replay.py` adds a report schema version plus per-distribution total observations, sample limit, deterministic method, method version, and truncation flag. Below the cap arrays retain input order exactly; above it stable-hash top-k is independent of page and partition boundaries. | `test_f5_zero_entry_sampling_is_exact_below_cap_and_deterministic_across_pages`; `test_f2_pr11a_golden_replay_semantics_match_small_and_database_bounded_paths`. |
| F6 missing behavioral proof | The scoped tests directly exercise database-backed page/partition parity, complete tie ordering, bigint source IDs, watermark exclusion, bounded identity map and decisions, association/label gating, interval-bounded label queries, frozen coverage payload, bounded coverage page size, scan-heartbeat progress, calibration limit blocking, committed-stage failures, and the PR 11a golden fixture. | `tests/test_pr11b_scope_contract.py`; `tests/test_research_runtime.py`; `tests/test_research_archive.py`; `tests/test_replay_engine.py`. |
| F7 truthful documentation | This matrix and the existing draft PR body map the corrective work and the original R1-R9 requirements to implementation and direct tests. | `docs/PR11B_COMPLIANCE.md`; PR 39 body. |

## Original R1-R9 Mapping

| Requirement | Implementation | Direct evidence |
| --- | --- | --- |
| R1 frozen input | `FrozenReplayEventReader` captures an event-ID watermark and scans it with deterministic keyset pages. | `test_r1_frozen_reader_uses_watermark_keyset_pages_and_never_list_events_none`; F6 database reader checks. |
| R2 incremental parity | The small-fixture `replay()` API remains, while production uses `replay_ordered_pages()` with bounded retained state. | `test_r2_incremental_replay_matches_small_fixture_api_for_pages_and_ties`; F2 golden fixture across page sizes 1, 2, 17, and 250 and two partition widths. |
| R3 bounded evidence | Decisions are optional, active lifecycle state/trades are bounded, and zero-entry distributions have explicit bounded sampling metadata. | `test_r3_large_incremental_replay_keeps_decisions_and_distributions_bounded`; F5 sampling tests. |
| R4 bounded labels | Label work is bounded to the existing 25-market limit, durable, resumable, and now blocks correctly on unmatched markets. | `test_r4_labels_are_bounded_resumable_and_mark_current_schema`; F1, F4, and F6 label-bound tests. |
| R5 bounded coverage | Coverage reads the same frozen snapshot and is complete only after its full bounded scan. | `test_r5_coverage_uses_the_same_frozen_snapshot_and_excludes_later_rows`; F3 progress and F6 frozen-payload tests. |
| R6 truthful stages | Stages expose archive, association/labels, coverage, replay, and calibration progress without per-event heartbeats. | `tests/test_research_runtime.py`, including the F3 threaded scan-progress test. |
| R7 calibration guard | Disabled calibration remains inactive; enabled calibration persists a fail-closed result before oversized event materialization. | `test_r7_disabled_calibration_cannot_run_or_materialize_replay_events`; `test_f6_enabled_oversized_calibration_persists_a_fail_closed_run_without_materializing`. |
| R8 acceptance coverage | The PR-specific contract proves ordering, resumption, bounded memory, frozen compatibility, and failure preservation directly against database-backed paths. | `tests/test_pr11b_scope_contract.py`. |
| R9 boundaries and documentation | No migration, environment, deployment, service, trading, execution, or strategy behavior changed. | `test_r8_and_r9_keep_calibration_disabled_and_scope_boundaries_static`; this document and PR 39 body. |

## Runtime Facts

- Archive batches remain 250 rows with the existing 20-batch runtime budget.
- Reference association uses that same 250-row code-level batch bound, commits
  each batch, and reports exact associable rows remaining.
- Labels remain limited to 25 markets per cycle and use the durable
  `quality_flags.label_schema_version` marker.
- Coverage and replay start only after association and label remaining counts are
  both zero. A missing market keeps the cycle partial and visible until it can be
  labeled.
- Coverage and replay publish a frozen watermark and total at stage start and
  only reach terminal state after their scanned count equals that frozen total.
- Existing small-fixture replay retains the original SHA-256 input hash shape.

## Safety and Boundaries

The application remains DRY_RUN-only for research validation with
`TRADING_ENABLED=false` and `EXECUTE=false`. PR 11b has no paper trading, live
trading, order, cancel, private WebSocket, account, balance, fill, credential,
or execution capability. It introduces no migration, required environment
variable, Railway service, dependency, or deployment change.

## Literal Self-Audit

F1-F7 and R1-R9 are implemented as documented above. No requirement was
reduced, deferred, substituted, or relabeled. The final validation commands,
their exact results, the final head SHA, and the exact unsharded GitHub Actions
result are recorded in the existing draft PR before it is marked compliant.
