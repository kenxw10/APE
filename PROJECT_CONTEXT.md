# APE Project Context

Canonical repo: https://github.com/kenxw10/APE

APE is a standalone BTC15 Kalshi Momentum Bot project. It is separate from BULL.

## Platform Direction

Planned platform split:

- Railway backend API
- Railway always-on worker
- Railway Postgres
- Vercel dashboard

PR 1 is merged and validated. PR 2 is merged and validated. PR 3 adds Railway backend deployment scaffolding for the API and always-on worker. PR 3a adds Railway runtime dependency packaging. PR 4 adds a Vercel-ready read-only dashboard scaffold. PR 4a adds explicit Vercel build configuration. PR 4b adds dashboard visual polish. PR 5 adds observer-only Kalshi REST auth diagnostics and active BTC15 market resolution. PR 6 adds observer-only Kalshi WebSocket market-data intake for the Railway worker, disabled by default. PR 7 adds observer-only BRTI / CF Benchmarks reference-feed intake for the Railway worker, disabled by default. PR 7a makes BRTI use a dedicated worker WebSocket by default, exposes `/reference/brti/series`, and wires the read-only dashboard Reference Price CF/BRTI chart to live BRTI data when available. PR 8 adds an observer-only strategy decision ledger v0, disabled by default, with read-only strategy status/decision endpoints and dashboard diagnostics. PR 8 also renders the dashboard Reference Price chart as the current fixed Kalshi 15-minute interval instead of a trailing rolling plot. PR 8a adds worker-owned storage retention and read-only `/storage/status` database lifecycle diagnostics; it is disabled by default, strips old raw payload JSON, deletes old observer rows in bounded batches, writes `storage_retention_runs` audit rows, and never exposes a destructive API endpoint. PR 8b hardens BRTI freshness and reconnect diagnostics. PR 9 adds a dry-run-only BTC15 momentum evaluator and hypothetical ledger; it remains disabled by default and never places orders, paper trades, reads balances, or uses private/user channels. PR 9a stabilizes dry-run trade readiness by separating BRTI backend receipt freshness from upstream source age, adding per-gate pass/warn/block summaries, and keeping the new diagnostics read-only. PR 9b/9c make feed liveness component-scoped and scheduler-safe. PR 9d adds market feed-state recovery so quiet market data is not treated as dead transport when the socket, subscription, active ticker, snapshot, and sequence state are healthy. PR 9e hardens subscription recovery, rollover recovery, snapshot resync metrics, and unrecovered market-feed blockers. PR 9f splits production workers into dedicated market-data, reference-BRTI, strategy, and maintenance roles and adds a read-only Kalshi WebSocket protocol ledger for SID/list/snapshot/reconnect proof. PR 9g hardens the market worker persistence path with split critical/diagnostic DB queues, orderbook coalescing, batched writes, and sampled protocol diagnostics after PR 9f showed the WebSocket was live but the single writer queue saturated. PR 9h is maintenance-only: it makes `/storage/status` prefer `ape-worker.maintenance` component liveness, reports effective retention enablement and latest run totals, and smooths retention load with small sleeps and optional caps.

Railway API: https://ape-api-production.up.railway.app

PR 4 dashboard style source:

- HOMERUN font stack and terminal/operator visual language.
- BULL only as a reference for chart baseline/open-line behavior.

## BULL Reference Rule

BULL may be used only as an implementation reference for:

- Kalshi BTC15 market resolution
- Websocket patterns
- CF Benchmarks/BRTI intake
- Dashboard state/SSE patterns
- Storage lifecycle
- Diagnostics
- Retention
- Warning and blocker taxonomy

Do not import BULL's fair-value strategy, model-targeting logic, project thesis, or conceptual direction.

## Strategy Context

The intended strategy direction is selective BTC 15-minute Kalshi momentum.

High-level future ingredients may include:

- Late-window impulse continuation
- BRTI/reference momentum
- Boundary distance
- Anti-chop filters
- Kalshi contract confirmation
- Spread and depth gates
- Dry-run decisions first
- Paper trading later
- Tiny live canary only after evidence

PR 8 implements only a read-only strategy decision ledger. PR 9 adds dry-run-only momentum evaluation and simulated ledger rows from persisted market, BRTI, orderbook, and public trade rows. PR 9a hardens the readiness checks and summaries for that dry-run evaluator. PR 9b separates event-driven feed liveness from persisted value-change timestamps so unchanged but live orderbook/BRTI streams can carry forward within hard caps. PR 9c makes market and BRTI feed liveness use their own worker heartbeat rows so strategy readiness is not affected by strategy/storage scheduler writes to the aggregate row. PR 9d separates market WebSocket transport liveness from market-data quietness, adds explicit transport/subscription/snapshot/ticker/sequence/feed-recovery diagnostics, and keeps BRTI strict for missing valid ticks. PR 9e reduces subscription-inactive and rollover gaps with bounded recovery state, confirmed orderbook SID waits, snapshot resync attempts, and reconnect escalation when recovery fails. PR 9f isolates those loops into role-specific workers and records protocol proof for subscribe/update/list/ping/pong/close/error behavior. PR 9g keeps that market worker observer-only while making critical latest market state persistence higher priority than historical/protocol diagnostics. PR 9h keeps strategy untouched and focuses only on maintenance worker liveness, storage status semantics, and retention load smoothing. It does not implement paper trading, live trading, orders, fills, private channels, account reads, execution, or strategy threshold tuning.

## Safety Defaults

Required safe defaults:

```text
APP_MODE=OBSERVER
TRADING_ENABLED=false
EXECUTE=false
```

Current safety policy blocks startup when:

- `APP_MODE` is not `OBSERVER` or `DRY_RUN`
- `TRADING_ENABLED=true`
- `EXECUTE=true`

No live trading, paper trading, Kalshi order placement, real strategy execution, or trading-capable dashboard behavior is included. The PR 6 ingestion loop is an observer-only Railway worker WebSocket collector for public Kalshi ticker, orderbook, and trade messages. PR 7/7a adds observer-only BRTI reference ticks through Kalshi's authenticated `cfbenchmarks_value` WebSocket channel for `index_ids=["BRTI"]`; it stores diagnostics and a safe read-only series only. PR 8 writes an observer-only decision ledger to `strategy_decisions`. PR 8a is storage lifecycle only; it does not change strategy logic, stale/transport semantics, credentials, or trading safety. PR 8b hardens BRTI freshness diagnostics and worker-side BRTI reconnects without changing formulas, strategy thresholds, execution, credentials, ingestion scope, or storage retention behavior. PR 9 may emit `ENTER_DRY_RUN`, `MANAGE_POSITION`, `EXIT_SIGNAL`, or `FORCE_EXIT` only as hypothetical ledger states when `APP_MODE=DRY_RUN`, `STRATEGY_OBSERVER_ENABLED=true`, `STRATEGY_DRY_RUN_ENABLED=true`, `TRADING_ENABLED=false`, and `EXECUTE=false`. PR 9a adds no paper/live states and no order, balance, private-channel, credential, or execution path. PR 9b/9d/9e/9f/9g keep those safety boundaries and only change feed-readiness semantics, worker isolation, persistence backpressure handling, and read-only protocol diagnostics so stale blockers mean true feed failure rather than quiet unchanged WebSocket state, diagnostic backlog, or a recoverable subscription rollover.

Kalshi REST/WebSocket credentials are optional at startup. When missing, `/kalshi/status`, `/markets/active`, and `/ws/status` return safe diagnostics. If configured, credentials belong only in Railway API/worker environment variables, never in Vercel.

## PR Ladder

This ladder is directional and should be reviewed before each PR.

1. Repo foundation and observer-only skeleton. Completed and validated.
2. Postgres schema and repository foundation. Completed and validated.
3. Railway backend deployment scaffold. Completed and validated.
4. Vercel-ready read-only dashboard scaffold. Completed and validated.
5. Kalshi BTC15 market catalog and contract resolver in observer mode. Completed and validated.
6. Kalshi orderbook, ticker, and public trade WebSocket observer. Completed and validated.
7. BRTI/reference data intake in observer mode. Completed by PR 7.
7a. Dedicated BRTI WebSocket, live BRTI series endpoint, and dashboard Reference Price chart wiring. Completed by PR 7a.
8. Observer-only strategy decision ledger v0. Completed by PR 8.
8a. Storage lifecycle, retention policy, and database lifecycle controls. Completed by PR 8a.
8b. BRTI freshness/recovery diagnostics hardening. Completed by PR 8b.
9. Momentum strategy decision engine, dry-run only. Completed by PR 9.
9a. Dry-run trade-readiness stabilization and gate diagnostics. Completed by PR 9a.
9b. Feed liveness correctness for dry-run readiness. Completed by PR 9b.
9c. Worker feed source-of-truth and scheduler isolation. Completed by PR 9c.
9d. Market feed-state recovery and orderbook snapshot liveness. Completed by PR 9d.
9e. Market subscription recovery hardening and rollover gap reduction. Completed by PR 9e.
9f. Dedicated market data worker isolation and Kalshi WebSocket protocol proof. Completed by PR 9f.
9g. Market worker persistence backpressure hardening. Completed by PR 9g.
9h. Maintenance worker liveness/status and retention load smoothing. Current PR.
10. Local replay fixtures.
11. Deterministic replay harness for captured market/reference data.
12. Spread, depth, liquidity, and anti-chop calibration diagnostics.
13. Paper trading simulator after dry-run evidence review.
14. Calibration and reporting workflow for strategy quality.
15. Paper-trading readiness review with explicit safety approval.
16. Reporting workflow for dry-run and paper-trading evidence.
17. Railway Postgres/Vercel dashboard wiring beyond the backend scaffold.
18. Manual live-canary safety plan with tiny limits and approvals.
19. Post-canary monitoring, rollback, alerting, and hardening.

Next manual checkpoint after PR 9h: keep API, market, reference, and maintenance workers running, but do not deploy or enable strategy until storage validation is clean. Confirm `/storage/status` uses `liveness_source=component` from `ape-worker.maintenance`, shows `worker_role=maintenance`, `latest_component_heartbeat_mode=storage_retention`, `worker_heartbeat_stale=false`, `retention_config.effective_enabled=true`, and latest run status `success` or `success_partial` with no blockers. `success_partial` is acceptable when bounded cleanup made progress and only the configured time/table budget was reached. Market validation may allow `QUIET_CARRY_FORWARD` when transport is healthy, subscriptions are reconciled, there is no unrecovered blocker, and the snapshot is inside the hard carry-forward cap; `BLOCKED_UNRECOVERED`, stale market transport, or BRTI `stale_transport` remain hard regressions. Keep `TRADING_ENABLED=false` and `EXECUTE=false` everywhere. The API and dashboard may stay read-only; Vercel must not receive Kalshi credentials, WebSocket variables, BRTI env vars, strategy env vars, storage retention env vars, dry-run controls, private-channel controls, account reads, or order/execution controls.
