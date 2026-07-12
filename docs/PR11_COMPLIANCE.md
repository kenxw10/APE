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
| R11 automatic governance | Database-owned challenger checks are in this batch. Automatic candidate progression and immutable transition proof are not complete. | BLOCKED BY REMAINING IMPLEMENTATION |
| R12 startup-only candidate pin | Candidate pin validation has been strengthened. Startup-only runtime and compatibility coverage remains incomplete. | IN PROGRESS |
| R13 bounded read-only research APIs | Worker/API status separation is in progress. Required validated API filters are not implemented. | BLOCKED BY REMAINING IMPLEMENTATION |
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

1. Automatic governance transitions with candidate-specific immutable evidence.
2. Candidate-attributed replay-trade persistence for every evaluated candidate.
3. Validated bounded research API filters.
4. Full event-time market fixtures.
5. Complete behavioral R1-R15 acceptance matrix.
6. Compliance-document cleanup outside this WIP correction.
7. Generated validation-log, JUnit, and result-file cleanup.
8. Any further failures discovered by the next prompt-to-diff audit.

No paper trading, live execution, credentials, private API calls, deployment, or
new migration is included in this remediation batch.
