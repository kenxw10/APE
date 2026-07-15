# PR 11f Compliance

PR 11f introduces clean-cohort governed calibration without changing the existing
full-history diagnostic baseline or any production safety/deployment boundary.

## R1-R12 Matrix

| Requirement | Status | Implementation | Direct evidence |
| --- | --- | --- | --- |
| R1 Full-history baseline | PASS | `src/ape/research/service.py`, `src/ape/research/replay.py` | `test_r1_full_history_baseline_remains_diagnostic_and_causally_unchanged`, `tests/test_replay_engine.py` |
| R2 Strict compatible cohort | PASS | `src/ape/research/cohort.py` | `test_clean_cohort_excludes_incompatible_evidence_with_explicit_reasons`, `test_r2_and_r4_watermark_cohort_and_compact_reader_are_deterministic` |
| R3 Immutable 50-market epochs | PASS | `src/ape/research/cohort.py`, `src/ape/research/governed_calibration.py` | `test_r3_initial_100_market_cohort_starts_at_epoch_50`, `test_r3_158_market_cohort_advances_one_epoch_per_cycle_and_reuses_latest`, `test_r3_in_progress_earliest_epoch_resumes_before_larger_due_epoch`, `test_r3_completed_epochs_only_advance_at_fifty_market_boundaries` |
| R4 Bounded input | PASS | `src/ape/research/repository.py`, `src/ape/research/cohort.py`, `src/ape/research/governed_calibration.py` | `test_r4_more_than_twenty_thousand_archive_events_use_250_row_pages`, `test_r4_reader_ordering_and_first_book_semantics_are_unchanged` |
| R5 Existing governed search | PASS | `src/ape/research/calibration.py` | `test_r5_search_space_and_protected_gate_contract_are_unchanged`, `test_r5_partitions_are_chronological_purged_and_holdout_isolated`, `test_existing_search_contract_remains_exactly_256_candidates` |
| R6 Material evidence | PASS | `src/ape/research/calibration.py`, `src/ape/research/governed_calibration.py` | `test_r6_fee_and_economic_evidence_fields_are_persisted`, `test_candidate_trades_are_partitioned_and_idempotent_across_reuse`, `test_finalist_evidence_recovers_after_fault_without_duplicate_evaluation_or_trades` |
| R7 Frontier/classification | PASS | `src/ape/research/governed_calibration.py` | `test_r7_economic_classifications_are_persisted_from_governed_runs`, `test_result_classification_and_frontier_are_deterministic_and_bounded` |
| R8 Recoverable always-on runner | PASS | `src/ape/research/service.py`, `src/ape/research/governed_calibration.py` | `test_governed_epochs_resume_batches_and_consume_holdout_once`, `test_r8_calibration_failure_cannot_roll_back_completed_baseline`, `test_r8_disabled_calibration_preserves_worker_behavior`, `test_failed_calibration_retry_resets_state_and_reuses_run_replay_id` |
| R9 No automatic promotion | PASS | `src/ape/research/service.py`, `src/ape/research/governed_calibration.py` | `test_r9_candidates_remain_research_only_and_no_promotion_call_exists`, `test_research_cycle_never_auto_advances_a_candidate` |
| R10 Read-only APIs | PASS | `src/ape/api/main.py`, `src/ape/research/status.py` | `test_r10_research_api_is_read_only_bounded_and_omits_raw_payloads`, `tests/test_research_api.py` |
| R11 Direct acceptance tests | PASS | `tests/test_pr11f_scope_contract.py`, `tests/test_research_calibration.py` | Numbered crosswalk below and required focused suite |
| R12 Documentation/compliance | PASS | this file plus README, PROJECT_CONTEXT, RESEARCH_AND_CALIBRATION, RAILWAY, PR_RUNBOOK | `test_r11_and_r12_scope_safety_and_deployment_boundaries_are_unchanged` |

## R11 Acceptance Crosswalk

1. Baseline available/unchanged: `test_r1_full_history_baseline_remains_diagnostic_and_causally_unchanged`.
2. Market-only exclusion: `test_clean_cohort_excludes_incompatible_evidence_with_explicit_reasons`.
3. Partial-source exclusion: same strict-cohort database test.
4. Unassociated feature exclusion: same strict-cohort database test.
5. Wrong architecture exclusion: same strict-cohort database test.
6. Wrong feature schema exclusion: same strict-cohort database test.
7. Wrong replay schema exclusion: same strict-cohort database test.
8. Unresolved outcome exclusion: same strict-cohort database test.
9. Immature 30-second label exclusion: same strict-cohort database test.
10. Missing first-book exclusion: same strict-cohort database test.
11. Valid current-version inclusion: same strict-cohort database test.
12. Deterministic/no hard-coded date: same strict-cohort test and current constants in `src/ape/research/cohort.py`.
13. Stable manifest/hash: same strict-cohort database test.
14. Post-watermark exclusion: `test_r2_and_r4_watermark_cohort_and_compact_reader_are_deterministic`.
15. Later-cycle eligibility: same watermark database test.
16. Under 50 classification: `test_research_cycle_does_not_consume_holdout_without_a_clean_epoch`.
17. Initial 100-market cohort starts at 50: `test_r3_initial_100_market_cohort_starts_at_epoch_50`.
18. Exactly 50 first epoch: `test_r3_completed_epochs_only_advance_at_fifty_market_boundaries`.
19. Add 49 without rerun: same epoch-boundary database test.
20. Reach 100 new epoch: same epoch-boundary database test.
21. 158 markets advance 50 -> 100 -> 150 and reuse 150: `test_r3_158_market_cohort_advances_one_epoch_per_cycle_and_reuses_latest`.
22. Earliest in-progress epoch resumes before a larger due epoch: `test_r3_in_progress_earliest_epoch_resumes_before_larger_due_epoch`.
23. Same identity reuse: same epoch-boundary test and `test_governed_epochs_resume_batches_and_consume_holdout_once`.
24. Holdout once: `test_governed_epochs_resume_batches_and_consume_holdout_once` and `test_finalist_evidence_recovers_after_fault_without_duplicate_evaluation_or_trades`.
25. More than 20,000 events calibrates: `test_r4_more_than_twenty_thousand_archive_events_use_250_row_pages`.
26. Page size at most 250: same large-archive test and PR 11b bounded-reader tests.
27. Deterministic ordering/ties: `test_r4_reader_ordering_and_first_book_semantics_are_unchanged`.
28. 500 ms/first-book semantics: same replay test plus `tests/test_replay_engine.py`.
29. Exact 256 identity: `test_r5_search_space_and_protected_gate_contract_are_unchanged`.
30. Protected changes rejected: same search/protected-gate test.
31. Zero signals: `test_r7_economic_classifications_are_persisted_from_governed_runs[zero-signal...]`.
32. Authorized parameter signal: the same database test persists non-baseline edge-threshold evidence.
33. Signal/no fill: `test_r7_economic_classifications_are_persisted_from_governed_runs[signal-no-fill...]`.
34. Fill/no close: `test_r7_economic_classifications_are_persisted_from_governed_runs[fill-no-close...]`.
35. Negative holdout: `test_r7_economic_classifications_are_persisted_from_governed_runs[negative-holdout...]`.
36. Positive holdout: `test_r6_fee_and_economic_evidence_fields_are_persisted`.
37. Fees: same economic evidence test and partition-trade idempotence test.
38. No future-market training: `test_r5_partitions_are_chronological_purged_and_holdout_isolated`.
39. Development-test isolation: same partition test and finalist runner test.
40. Finalist-only holdout: `test_governed_epochs_resume_batches_and_consume_holdout_once`.
41. Finalist evidence durable across restart: `test_finalist_evidence_recovers_after_fault_without_duplicate_evaluation_or_trades`.
42. Incomplete finalist evidence fails closed: same fault-injection test.
43. Deterministic frontier ties: `test_result_classification_and_frontier_are_deterministic_and_bounded`.
44. Bounded frontier plus baseline/finalist: same frontier test and API test.
45. No raw response artifacts: `test_r10_research_api_is_read_only_bounded_and_omits_raw_payloads`.
46. Durable bounded batch: `test_governed_epochs_resume_batches_and_consume_holdout_once`.
47. Restart resume/no duplicate trades: same resume test and `test_candidate_trades_are_partitioned_and_idempotent_across_reuse`.
47a. Failed/blocked retry resets stale state and preserves durable replay lineage: `test_failed_calibration_retry_resets_state_and_reuses_run_replay_id`.
48. Baseline survives failure: `test_r8_calibration_failure_cannot_roll_back_completed_baseline`.
49. Disabled behavior: `test_r8_disabled_calibration_preserves_worker_behavior`.
50. No promotion/activation: `test_r9_candidates_remain_research_only_and_no_promotion_call_exists`.
51. No trading/private/account capability: `test_r11_and_r12_scope_safety_and_deployment_boundaries_are_unchanged`.
52. Fail-closed flags: same scope/safety test.
53. PR 11b/c/d/e regression coverage: required focused suite, full suite, and PostgreSQL 16 PR CI.

## Runtime Design

- Full-history replay remains the diagnostic continuity path and is committed before optional calibration.
- Cohort discovery is frozen by replay watermark and records explicit source/version/label/book exclusions.
- Calibration epochs are immutable sequential 50-market epochs: 50, then 100, then 150, and so on; one bounded epoch is evaluated per research cycle, in-progress epochs resume before later epochs, and a completed latest epoch is reused until the next threshold is reached.
- Keyset database reads use at most 250 rows per page and release each page transaction.
- Candidate replay receives only compact FEATURE_SNAPSHOT and ORDERBOOK evidence.
- Candidate evaluation commits fixed batches of eight; logistic fitting caps its compact matrix at 100,000 rows.
- The search remains exactly 256 candidates: baseline, 252 weighted heuristic, three L2 logistic.
- Only the selected finalist receives development-test and frozen-holdout evaluation.
- Completed identities are reused; interrupted identities resume from durable candidate progress. Finalist evidence, the finalist phase marker, and `holdout_used_at` are committed atomically before finalization; restart finalizes complete evidence without rerunning the finalist and fails closed on incomplete evidence.
- Candidates remain `DRAFT` / `RESEARCH_ONLY`; there is no automatic promotion or activation.

## Locked Boundaries

- Migration: none; current migration remains `0011_research_archive_cursors`.
- New required production environment variables: none.
- New Railway services: none.
- Archive page size: 250, unchanged.
- Archive operation budget: 20, unchanged.
- `DB_STATEMENT_TIMEOUT_MS`: 5000, unchanged.
- `RESEARCH_POLL_SECONDS`: 60, unchanged.
- Archive batching, retention, strategy thresholds, first-book semantics, latency, expiry, and fee formulas: unchanged.
- `APP_MODE=DRY_RUN`.
- `CALIBRATION_ENABLED=false` pending explicit production-validation instructions.
- `TRADING_ENABLED=false`.
- `EXECUTE=false`.
- No paper/live trading, order placement/cancellation, private WebSocket, account/balance/order/fill read, or dashboard trading control.

## Validation Evidence

- Required focused suite: PASS, 102 tests collected/executed, 102 passed, 0 failed, 0 skipped.
- Full local suite: PASS, 621 passed, 4 PostgreSQL-only skips, 0 failed in 345.89s.
- `python -m ruff check .`: PASS.
- `python -m compileall src scripts`: PASS.
- `python -m pip check`: PASS, no broken requirements.
- `git diff --check`: PASS.
- `python scripts/research_smoke.py`: PASS; all reported invariants true.
- PostgreSQL 16 exact unsharded PR workflow on final head `21fe7cb`: PASS,
  run `29385677643`, job `87258296436`; 625 passed, 0 skipped, 0 failed.
