# PR 10a Compliance Matrix

`PR 10a — Complete momentum v2 context, microstructure, and causal exit lifecycle`

| Requirement | Implementation | Tests | Status |
| --- | --- | --- | --- |
| R1 scope contract | `docs/PR10A_COMPLIANCE.md`, `tests/test_pr10a_scope_contract.py` | `tests/test_pr10a_scope_contract.py` | COMPLIANT |
| R2 migration and immutable attribution | `src/ape/db/models.py`, `src/ape/db/migrations.py`, `src/ape/repositories/inputs.py`, `src/ape/repositories/strategy_v2.py` | `tests/test_db_schema.py`, `tests/test_pr10a_scope_contract.py` | COMPLIANT |
| R3 shared immutable context | `src/ape/strategy/context.py`, `src/ape/strategy/observer.py` | `tests/test_strategy_observer.py`, `tests/test_pr10a_scope_contract.py` | COMPLIANT |
| R4 executable ladders | `src/ape/kalshi/ws_state.py`, `src/ape/db/models.py` | `tests/test_kalshi_ws_messages.py`, `tests/test_pr10a_scope_contract.py` | COMPLIANT |
| R5 multi-level microstructure | `src/ape/strategy/momentum_v2.py` | `tests/test_momentum_v2.py`, `tests/test_pr10a_scope_contract.py` | COMPLIANT |
| R6 modes, liveness, measurements | `src/ape/strategy/context.py`, `src/ape/strategy/momentum_v2.py` | `tests/test_momentum_v2.py`, `tests/test_strategy_observer.py` | COMPLIANT |
| R7 position management | `src/ape/strategy/observer.py`, `src/ape/db/models.py` | `tests/test_strategy_observer.py` | COMPLIANT |
| R8 causal exit intents and retries | `src/ape/strategy/observer.py`, `src/ape/repositories/strategy_v2.py` | `tests/test_strategy_observer.py` | COMPLIANT |
| R9 outcomes and read-only APIs | `src/ape/strategy/observer.py`, `src/ape/api/main.py`, `src/ape/repositories/strategy_v2.py` | `tests/test_strategy_api.py`, `tests/test_pr10a_scope_contract.py` | COMPLIANT |
| R10 safety and compatibility | `src/ape/strategy/observer.py`, `src/ape/strategy/momentum_v2.py`, `README.md`, `docs/RAILWAY.md`, `docs/PR_RUNBOOK.md` | full `pytest`, `ruff`, `compileall`, `pip check` | COMPLIANT |

This change remains DRY_RUN-only. It adds no execution credentials, private Kalshi channels, account reads, order placement, cancellation, paper trading, live trading, service, environment-variable, position-sizing, or dashboard-control behavior.

## Post-Merge Correction Note

The R6, R8, and R9 rows above described the PR 10a implementation at merge time,
but are not an unqualified assertion of the final V2 semantics. PR 10b remediates
the boundary-cross mode eligibility, first-book-only EXIT fill behavior, and
durable outcome-table status observability required after PR 10a.
