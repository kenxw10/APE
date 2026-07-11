# PR 10b Compliance Matrix

`PR 10b — Correct v2 mode gating, first-book exit fills, and outcome observability`

| Requirement | Exact implementation files | Exact tests | Status |
| --- | --- | --- | --- |
| R1 boundary-cross research-only mode | `src/ape/strategy/momentum_v2.py`, `src/ape/strategy/observer.py` | `tests/test_pr10b_scope_contract.py`, `tests/test_momentum_v2.py`, `tests/test_strategy_observer.py` | COMPLIANT |
| R2 first-book-only EXIT resolution | `src/ape/strategy/observer.py` | `tests/test_pr10b_scope_contract.py`, `tests/test_strategy_observer.py` | COMPLIANT |
| R3 durable outcome status observability without retention | `src/ape/storage/retention.py`, `src/ape/repositories/storage_retention.py`, `src/ape/models/storage.py` | `tests/test_storage_retention.py`, `tests/test_storage_api.py`, `tests/test_pr10b_scope_contract.py` | COMPLIANT |
| R4 corrected V2 semantic attribution | `src/ape/strategy/momentum_v2.py` | `tests/test_pr10a_scope_contract.py`, `tests/test_pr10b_scope_contract.py` | COMPLIANT |
| R5 safety, regressions, documentation, and delivery | `README.md`, `PROJECT_CONTEXT.md`, `docs/RAILWAY.md`, `docs/PR_RUNBOOK.md`, `docs/PR10A_COMPLIANCE.md`, `docs/PR10B_COMPLIANCE.md` | `tests/test_pr10b_scope_contract.py`, required full validation suite | COMPLIANT |

PR 10b preserves DRY_RUN-only safety. It adds no migration, environment variable,
Railway service, paper execution, live execution, order placement, cancellation,
private channel, account read, balance read, threshold change, timing change, or
new strategy mode. `BOUNDARY_CROSS_HOLD` remains persisted research evidence only;
`CONTINUATION` remains the sole mode that can proceed toward a V2 entry signal.
