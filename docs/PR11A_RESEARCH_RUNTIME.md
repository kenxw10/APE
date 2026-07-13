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

## Verification

`tests/test_research_runtime.py` proves bounded archive batches, resumable progress,
fresh failure heartbeats, calibration isolation, and worker/API status visibility.
The PR validation workflow continues to run the exact unsharded `python -m pytest`.
