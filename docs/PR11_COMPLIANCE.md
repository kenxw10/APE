# PR 11 Remediation Status

`WIP — PR 11 remediation in progress; do not merge`

The current branch contains a partial remediation batch. PR 11 is not yet
compliant and must not be merged or deployed.

| Requirement | Current remediation evidence | Status |
| --- | --- | --- |
| R1 immutable research schema and migration | Existing `0010_research_replay_calibration` foundation retained. | IN PROGRESS |
| R2 canonical feature vector and evaluator parity | Complete lifecycle input vector and shared lifecycle helper are being added. The full behavioral parity matrix is still missing. | IN PROGRESS |
| R3 isolated research worker | Archive no longer reconciles outcomes; public outcome reconciliation is being moved to the market-data role. | IN PROGRESS |
| R4 normalized archive, labels, and coverage | Idempotent archive protection, immutable coverage reports, and fail-closed malformed labels are in this batch. | IN PROGRESS |
| R5 zero-entry audit and frequency governance | Funnel and zero-market bootstrap corrections are in this batch; required audit coverage remains incomplete. | IN PROGRESS |
| R6 executable labels and fee model | July 7, 2026 parameter attribution and published fee-table examples are pinned; complete label-horizon evidence remains incomplete. | IN PROGRESS |
| R7 deterministic production-parity replay | Shared lifecycle logic and first-book handling are in progress; complete production/replay parity coverage is missing. | IN PROGRESS |
| R8 chronological partitions and frozen holdout | Development-test and fold corrections are in progress; the required complete leakage-control proof is missing. | IN PROGRESS |
| R9 bounded search and fold-specific logistic preprocessing | Canonical logistic feature names and fold-specific replay usage are in this batch. Full calibration evidence remains incomplete. | IN PROGRESS |
| R10 objective, penalties, and bootstrap | Zero-trade market handling and regime aggregation corrections are in this batch. Full metric fixtures remain incomplete. | IN PROGRESS |
| R11 automatic governance | Candidate-specific persisted replay/calibration evidence now drives DRAFT -> BACKTESTED -> SHADOW -> DRY_RUN_CHALLENGER, with immutable events and database serialization. Final acceptance fixtures remain required. | IN PROGRESS |
| R12 startup-only candidate pin | A configured candidate is resolved once when the strategy worker starts. It is intentionally not hot-reloaded; database or environment changes require a worker restart. | IN PROGRESS |
| R13 bounded read-only research APIs | Validated bounded filters and worker-observed status are implemented. The complete R1-R15 behavioral matrix remains required. | IN PROGRESS |
| R14 retention and durable evidence | Existing retention/status separation is retained. Generated-validation cleanup remains pending. | IN PROGRESS |
| R15 fixtures, documentation, and deployment | The smoke script no longer fabricates successful governance evidence. The full event-time fixture suite and behavioral R1-R15 matrix are missing. | BLOCKED BY REMAINING IMPLEMENTATION |

## July 2026 Fee Attribution

The fee model stores a SHA-256 of the exact canonical **parameter snapshot**, not
a PDF-byte checksum. The Codex environment receives HTTP 429 from the official
PDF URL, so no PDF-byte hash is claimed.

- Source URL: `https://kalshi.com/docs/kalshi-fee-schedule.pdf`
- Document title: `Fee Schedule for July 2026 - 7.7.26 Update`
- Effective date: `2026-07-07`
- Parameter snapshot SHA-256:
  `6d625f01b407d66a8f42c3df193ed750054df489bb075de63fc98608cfe1b823`
- KXBTC15M is not listed as non-standard; its taker multiplier is `1`, maker
  multiplier is `0`, and settlement fee is `0`.

## Remaining Blockers

The following GPT-audit findings remain unresolved and prevent a compliance
claim:

1. Full 18-market event-time fixture suite.
2. Complete behavioral R1-R15 acceptance matrix.
3. Generated validation-log, JUnit, and result-file cleanup.
4. Final compliance and deployment documentation.
5. Any further failures discovered by the next prompt-to-diff audit.

## Accepted Candidate-Pin Boundary

The active review suggestion to revalidate candidate pins on every observer
evaluation is incompatible with the accepted PR 11 architecture. Candidate pins
are immutable for a running strategy-worker process: they resolve at startup,
never hot reload, and require a worker restart after any database or environment
change. Invalid startup pins omit only the candidate and surface a
candidate-specific blocker; they never alter the baseline V2, v1, or v1_fast
variants.

No paper trading, live execution, credentials, private API calls, deployment, or
new migration is included in this remediation batch.
