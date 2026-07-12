# PR 11 Compliance Matrix

PR 11 is DRY_RUN-only research infrastructure. It does not add paper trading,
live trading, orders, cancels, private channels, account reads, credentials, or
execution capability. Candidate pins remain startup-only: a changed pin requires
a strategy-worker restart and is never hot reloaded.

| Requirement | Implementation evidence | Behavioral evidence |
| --- | --- | --- |
| R1 schema, constraints, indexes, idempotency | `src/ape/db/migrations.py`, `src/ape/db/models.py` | `tests/test_pr11_scope_contract.py::test_r1_single_research_migration_and_schema_contract` |
| R2 canonical evaluator parity | `src/ape/strategy/momentum_v2.py`, `src/ape/research/archive.py` | `tests/test_pr11_scope_contract.py::test_r2_live_and_json_persisted_vectors_have_identical_evaluator_results` |
| R3 isolated worker and public reconciliation | `src/ape/worker/main.py`, `src/ape/research/service.py` | `tests/test_worker.py`, `tests/test_worker_roles.py`, `tests/test_research_worker.py` |
| R4 archive recovery, cursor, coverage, labels | `src/ape/research/archive.py` | `tests/test_research_archive.py` |
| R5 zero-entry funnel and frequency classes | `src/ape/research/replay.py` | `tests/test_pr11_scope_contract.py::test_r5_zero_entry_audit_is_explicitly_unvalidatable` |
| R6 executable labels and verified fees | `src/ape/research/archive.py`, `src/ape/research/fees.py` | `tests/test_research_archive.py`, `tests/test_pr11_scope_contract.py::test_r6_verified_taker_fee_is_nonzero_and_versioned` |
| R7 causal lifecycle and retry semantics | `src/ape/research/replay.py`, `src/ape/strategy/observer.py` | `tests/test_replay_engine.py`, `tests/test_pr11_scope_contract.py::test_r7_ordered_replay_uses_first_book_without_future_rescue` |
| R8 chronological folds, purge, test, holdout | `src/ape/research/calibration.py` | `tests/test_calibration.py` |
| R9 bounded search and fold-specific logistic fitting | `src/ape/research/calibration.py` | `tests/test_calibration.py` |
| R10 bootstrap and penalties | `src/ape/research/calibration.py` | `tests/test_calibration.py`, `tests/test_pr11_scope_contract.py::test_r10_market_bootstrap_is_two_thousand_resamples` |
| R11 governance evidence and transitions | `src/ape/research/repository.py`, `src/ape/research/service.py` | `tests/test_candidate_governance_evidence.py`, `tests/test_research_worker.py::test_automatic_governance_uses_persisted_candidate_evidence` |
| R12 startup-only candidate pin | `src/ape/strategy/observer.py`, `src/ape/research/pin.py` | `tests/test_candidate_pin.py` |
| R13 bounded read-only APIs and status | `src/ape/api/main.py`, `src/ape/research/status.py` | `tests/test_research_api.py`, `tests/test_pr11_scope_contract.py::test_r13_research_api_surface_is_read_only_and_bounded` |
| R14 retention and durable evidence | `src/ape/storage/retention.py`, `src/ape/repositories/storage_retention.py` | `tests/test_storage_retention.py`, `tests/test_storage_api.py` |
| R15 fixtures, smoke, documentation, deployment boundaries | `src/ape/research/fixtures.py`, `scripts/research_smoke.py` | `tests/test_pr11_scope_contract.py::test_r15_eighteen_market_fixture_has_real_event_time_sources_and_labels` |

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
