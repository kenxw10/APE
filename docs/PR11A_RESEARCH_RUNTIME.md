# PR 11a Research Runtime Hotfix

## Scope

PR 11a corrects the production research worker's archive persistence boundary after
Postgres canceled a long archive transaction at the configured statement timeout.
It does not change V2 strategy evaluation, replay causality, calibration search or
governance thresholds, candidate parameters, candidate-pin behavior, dashboard data,
execution, database schema, migrations, Railway service count, or environment
variables.

## Runtime Contract

- Archive sources run in deterministic order: markets, reference ticks, orderbook
  snapshots, public trades, feature snapshots, trade intents, and position outcomes.
- Each archive batch contains no more than 250 source events and flushes once.
- Every committed archive batch is followed by a separately committed research
  heartbeat. An interrupted run resumes from persisted source identities without
  duplicate replay events.
- The worker records startup, archive, association/label refresh, coverage, baseline
  replay, optional calibration, completion, and failure state. Failure heartbeats are
  attempted in a fresh session and expose only bounded error type/code metadata.
- Baseline replay is committed before calibration starts. A calibration failure leaves
  archive, labels, coverage, and replay evidence intact for the next cycle.

## PR 38 Corrective Runtime Guarantees

- A terminal archive error heartbeat preserves the current cycle id, source stage,
  completed batch count, archived counts, event count, last committed batch, and
  the last successful stage. The error heartbeat is persisted from a new session
  after the failed work transaction is closed or rolled back.
- Association/label refresh, baseline replay, and calibration publish running
  heartbeats from fresh database sessions at most every 30 seconds. The test-only
  interval is injectable. The ticker stops before a terminal heartbeat is written.
- PostgreSQL archive batches acquire a transaction advisory lock keyed by source
  table before selecting or writing source rows. SQLite intentionally skips that
  lock. A duplicate replay-event identity retries from a fresh session at most
  three times; unrelated integrity failures still fail immediately.
- `/research/status` exposes the active cycle, stage, source table, completed
  archive batches, archived counts, progress timestamp, and failure stage. A fresh
  error heartbeat remains enabled and fresh but is never healthy.

## Verification

`tests/test_research_runtime.py` proves the exact 1,001-row `250,250,250,250,1`
archive sequence, one bulk persistence call per batch, third-batch timeout recovery,
long-stage heartbeat freshness and shutdown, duplicate-identity retry behavior, the
PostgreSQL lock boundary, SQLite lock omission, fresh failure heartbeats, calibration
isolation, and worker/API status visibility.
The PR validation workflow continues to run the exact unsharded `python -m pytest`.
