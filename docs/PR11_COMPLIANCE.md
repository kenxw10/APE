# PR 11 Compliance Matrix

PR 11 is DRY_RUN-only research infrastructure. It does not add paper trading,
live trading, orders, cancels, private channels, account reads, credentials, or
execution capability. Candidate pins resolve once when the strategy worker starts.
The running process does not hot reload candidate configuration. Database or
environment changes require a worker restart.

| Requirement | Implementation evidence | Behavioral evidence |
| --- | --- | --- |
| R1 schema, constraints, indexes, idempotency | `src/ape/db/migrations.py`, `src/ape/db/models.py` | `test_r1_single_research_migration_and_schema_contract`: executes 0010 twice on SQLite, checks research/config/snapshot columns and unique source/event rejection, and compiles research DDL for PostgreSQL. |
| R2 canonical evaluator parity | `src/ape/strategy/momentum_v2.py`, `src/ape/research/archive.py` | `test_r2_live_and_json_persisted_vectors_have_exact_evaluator_parity`: exact live-versus-persisted parity for all ten required vector cases and all returned decision fields. |
| R3 isolated worker and public reconciliation | `src/ape/worker/main.py`, `src/ape/research/service.py` | `test_r3_worker_roles_keep_research_isolated_and_market_data_owns_reconciliation`; `test_r3_reconciler_is_public_only_and_official_result_wins`; `test_market_outcome_reconciler_offloads_blocking_cycle`. |
| R4 archive recovery, cursor, coverage, labels | `src/ape/research/archive.py` | `test_r4_archive_is_idempotent_and_recovers_new_and_out_of_order_source_rows` and `tests/test_research_archive.py`. |
| R5 zero-entry funnel and frequency classes | `src/ape/research/replay.py` | `test_r5_funnel_frequency_classifications_are_explicit` covers all funnel stages and all six fill-frequency classifications; `test_zero_entry_audit_uses_the_evaluated_candidate_edge_threshold` verifies calibrated edge margins use the evaluator threshold. |
| R6 executable labels and verified fees | `src/ape/research/archive.py`, `src/ape/research/fees.py` | `test_r6_verified_taker_fee_examples_and_metadata`, archive tests, and smoke validate fee examples, first-book labels, 5/15/30/60 marks, final-minute mark, settlement, and label readiness. |
| R7 causal lifecycle and retry semantics | `src/ape/research/replay.py`, `src/ape/strategy/observer.py` | `test_r7_ordered_replay_uses_first_book_without_future_rescue`, `test_replay_blocks_a_second_entry_while_the_first_entry_is_pending`, `test_r7_shared_lifecycle_helper_covers_exit_trigger_order`, and `test_r7_r15_fixture_scenarios_trigger_real_replay_outcomes`. |
| R8 chronological folds, purge, test, holdout | `src/ape/research/calibration.py` | `test_r8_chronological_partitions_are_disjoint_and_holdout_is_immutable` verifies ordered 64/16/20 partitions, chronological folds, and immutable holdout assignment. |
| R9 bounded search and fold-specific logistic fitting | `src/ape/research/calibration.py` | `test_r9_bounded_search_and_logistic_artifacts_are_deterministic` verifies the 256-candidate cap, deterministic snapshot hash, tier grids, and reproducible L2 artifact. |
| R10 bootstrap and penalties | `src/ape/research/calibration.py` | `test_r10_market_normalization_bootstrap_and_penalties_are_explicit` verifies zero-trade-market normalization, exact 2,000 resamples, and lower-confidence penalties. |
| R11 governance evidence and transitions | `src/ape/research/repository.py`, `src/ape/research/service.py` | `test_r11_only_qualified_candidates_can_reach_dry_run_challenger` verifies the promotion threshold, under-sampled rejection, and paper/live rejection; `test_automatic_governance_uses_persisted_candidate_evidence` verifies an occupied architecture slot leaves a replacement candidate in `SHADOW` with durable blocker evidence rather than crashing the worker; smoke supplies persisted 500-market evidence. |
| R12 governed candidate pin | `src/ape/strategy/observer.py`, `src/ape/research/pin.py` | `test_r12_candidate_pin_resolves_once_per_observer_lifetime` and `test_strategy_observer_candidate_pin_is_cached_until_restart` verify startup-only resolution, immutable runtime candidate/blocker state, and restart-required changes; `test_restart_with_replacement_candidate_cleans_stale_prior_candidate_state` proves a restarted worker cancels and force-exits stale A state against startup-resolved B; `test_candidate_pin_cleanup_blocks_a_replacement_candidate_entry` prevents B from opening until A's stale state drains while baseline V2, v1, and v1_fast decisions remain unchanged. |
| R13 bounded read-only APIs and status | `src/ape/api/main.py`, `src/ape/research/status.py` | `tests/test_research_api.py`, including `test_zero_entry_route_returns_bounded_database_error`, `test_research_status_uses_worker_observed_enabled_state` for separate archive/label status, `test_r13_research_api_surface_is_read_only_and_bounded`, and smoke's research/storage read-route map. |
| R14 retention and durable evidence | `src/ape/storage/retention.py`, `src/ape/repositories/storage_retention.py` | `test_r14_retention_and_durable_status_tables_are_separate` proves status reads remain separate from all four mutation paths and raw-payload reads. |
| R15 fixtures, smoke, documentation, deployment boundaries | `src/ape/research/fixtures.py`, `scripts/research_smoke.py` | `test_r15_eighteen_market_fixture_has_real_event_time_sources_and_labels`, real fixture replay outcomes, and machine-readable smoke invariants. |

## Acceptance Boundary

R1-R15 were not reduced, deferred, substituted, or relabeled. The scope contract
executes direct behavioral checks for the migration, evaluator parity, worker
ownership, archive recovery, zero-entry funnel, fee model, causal lifecycle,
chronological partitions, deterministic bounded search, market-normalized
metrics, governance, startup-only candidate pin resolution, read APIs, retention boundaries, and
the event-time fixture corpus.

## PR 11b Bounded Runtime Correction

PR 11b preserves R1-R15 semantics while making the existing research runtime safe
for a large archived event table. Replay and coverage capture one immutable
event-ID watermark and scan that snapshot with 250-row keyset pages. Production
does not use `list_events(limit=None)`. Small fixture replay remains available for
parity tests, while production retains only lifecycle state, trades, exact counters,
unique-market sets, and explicitly bounded distribution samples.

Mature labels process 25 markets per cycle from the durable label-schema marker;
they commit before coverage/replay and report a partial cycle when work remains.
The same worker publishes fresh heartbeats during labels, coverage, replay, and
calibration. Calibration remains disabled unless configured and fails closed above
the fixed in-code 20,000-event materialization limit. PR 11b adds no migration,
environment variable, service, deployment, strategy, fee, lifecycle, governance,
paper, live, private-account, or execution capability.

## Governance Evidence

Promotion evidence is derived from persisted source events, resolved official
outcomes, and declared out-of-sample partitions. It records exact changed and
protected parameter paths, candidate-side feature eligibility, per-source event
gaps, complete eligible markets, fee metadata, and partition-specific de-duplicated
closed trades. Search metadata is immutable and includes candidate IDs, parameter
hashes, grids, logistic settings, governance configuration, and a snapshot SHA-256.

Frequency targets are diagnostic governance bounds, not activation controls:

- Qualified setups: 5-15 per 100 markets.
- Preferred fills: 3-10 per 100 markets.
- Challenger hard fill band: 3-15 per 100 markets.

## Validation Evidence

The compact PR 11 collection manifest and shard aggregate report remain under
`docs/validation/pr11/`. Regenerated raw logs, JUnit XML, result JSON, and smoke
output are intentionally ignored. The exact unsharded `python -m pytest` run is
the GitHub Actions gate for this draft PR.

The latest candidate-pin correction cancels every pending challenger ENTRY with
`v2_candidate_pin_invalid_entry_cancelled` before lifecycle fill resolution;
open positions continue through the existing force-exit path. This is a
fail-closed correction only and does not alter R1-R15 scope or thresholds.

## Smoke Invariants

`python scripts/research_smoke.py` creates a temporary migrated database and
drives archive, official public outcome reconciliation, label refresh, baseline
and candidate replay, bounded calibration, durable persistence, and governance.
It emits machine-readable invariants proving source rows were archived, labels
and coverage exist, the 500-market/50-holdout-trade promotion boundary is met
only by the qualifying candidate, under-sampled candidates are blocked, paper
and live transitions are rejected, and every research/storage read API remains
read-only. The smoke path never creates private Kalshi clients, account reads,
orders, cancels, or execution capability.
