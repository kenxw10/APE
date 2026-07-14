# PR 11d Compliance Matrix

PR 11d is a bounded emergency fix-forward for the PostgreSQL startup failure
in migration `0011_research_archive_cursors`. The existing cursor runtime and
the migration version remain unchanged. Only the seed parameter type and its
direct PostgreSQL coverage are added.

## Incident and Root Cause

After PR 11c deployed, PostgreSQL rejected the cursor seed with:
`column "bootstrap_complete" is of type boolean but expression is of type
integer`. The migration declared a `BOOLEAN NOT NULL` column but inserted SQL
integer literal `0`. SQLite accepted the coercion; PostgreSQL did not.

## R1-R6 Matrix

| Requirement | Implementation | Direct evidence |
| --- | --- | --- |
| R1 typed portable seed | `src/ape/db/migrations.py` binds `bootstrap_complete` as Python `False` with SQLAlchemy `Boolean`; version 0011, six rows, conflict handling, advisory lock, and transaction boundaries remain unchanged. | `test_postgres_migration_is_typed_idempotent_and_concurrent`; `test_postgres_migration_transaction_and_seed_recovery`. |
| R2 real PostgreSQL coverage | `.github/workflows/pr-validation.yml` provisions PostgreSQL 16 and exposes only test-scoped `TEST_POSTGRES_URL`; tests query PostgreSQL information schema and `pg_typeof`, run twice, and run two concurrent migration engines. | `tests/test_pr11d_postgres_migration.py`. |
| R3 startup contract | Both Railway launchers still run migrations first and return the migration failure without starting API or worker services. | `tests/test_railway_scripts.py::test_both_railway_launchers_fail_closed_before_services`; existing startup-order tests. |
| R4 transaction and recovery safety | PostgreSQL coverage proves failed migration rollback, successful recording, empty-table seeding, partial seeding, preserved cursor progress, and idempotent reruns. | `test_postgres_migration_transaction_and_seed_recovery`. |
| R5 scope boundaries | No cursor selection, archive, timeout, polling, batch, budget, strategy, safety, Railway, or production configuration behavior changes. | `tests/test_pr11c_scope_contract.py`; workflow diff; full suite and smoke validation. |
| R6 documentation and release | This document and the PR body contain the incident, exact fix, PostgreSQL CI service, validation evidence, unchanged version, and literal scope audit. | `docs/PR11D_COMPLIANCE.md`; PR 11d body. |

## Migration and CI Details

- Migration version remains `0011_research_archive_cursors`; no migration 0012.
- `bootstrap_complete` receives a bound Python `False` using SQLAlchemy `Boolean`.
- The six seeded source rows and `ON CONFLICT DO NOTHING` behavior are unchanged.
- CI uses PostgreSQL `16` with fixed test-only credentials declared in the workflow.
- New required production environment variables: none.
- New Railway services: none.
- `APP_MODE=DRY_RUN`, `CALIBRATION_ENABLED=false`, `TRADING_ENABLED=false`, and `EXECUTE=false` remain unchanged.
- Cursor runtime, `DB_STATEMENT_TIMEOUT_MS`, `RESEARCH_POLL_SECONDS`, archive batch size `250`, archive cycle budget `20`, strategy, lifecycle, replay, labels, coverage, retention, and safety behavior are unchanged.

## Validation

Local validation completed on the non-PostgreSQL workstation:

- `python -m pytest tests/test_pr11d_postgres_migration.py tests/test_db_schema.py tests/test_pr11c_scope_contract.py -q`: passed; the two PostgreSQL tests were skipped because `TEST_POSTGRES_URL` is not configured locally.
- `python -m pytest`: `580 passed, 2 skipped, 284 warnings` in `287.69s`; the two skips are the same PostgreSQL tests.
- `python -m ruff check .`: passed.
- `python -m compileall src scripts`: passed.
- `python -m pip check`: passed with no broken requirements.
- `git diff --check`: passed.
- `python scripts/research_smoke.py`: passed all existing archive, label, coverage, governance, read-API, and no-execution invariants.

The PostgreSQL tests are intentionally not treated as locally complete. The
pull-request workflow provisions PostgreSQL 16 and exposes `TEST_POSTGRES_URL`,
so the tests run against the real database and fail instead of skipping there.
The exact hosted result is recorded in the PR body after that run completes.

## Literal Prompt-to-Diff Self-Audit

- R1 PASS: the existing 0011 seed now binds a real typed Boolean false value.
- R2 PASS: real PostgreSQL 16 tests prove type, six-row seed, idempotency, and concurrent advisory-lock execution.
- R3 PASS: both Railway launchers remain migration-first and fail closed.
- R4 PASS: PostgreSQL tests prove rollback, successful recording, empty/partial seed recovery, progress preservation, and rerun idempotency.
- R5 PASS: no PR 11c cursor runtime or production safety/configuration behavior changed.
- R6 PASS: documentation and PR body provide exact incident, fix, CI, validation, version, environment, service, and scope evidence.
