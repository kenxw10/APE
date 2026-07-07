# APE Project Context

Canonical repo: https://github.com/kenxw10/APE

APE is a standalone BTC15 Kalshi Momentum Bot project. It is separate from BULL.

## Platform Direction

Planned platform split:

- Railway backend API
- Railway always-on worker
- Railway Postgres
- Vercel dashboard

PR 1 is merged and validated. PR 2 is merged and validated. PR 3 adds Railway backend deployment scaffolding for the API and always-on worker. PR 3a adds Railway runtime dependency packaging. PR 4 adds a Vercel-ready read-only dashboard scaffold. PR 4a adds explicit Vercel build configuration. PR 4b adds dashboard visual polish. PR 5 adds observer-only Kalshi REST auth diagnostics and active BTC15 market resolution. PR 6 adds observer-only Kalshi WebSocket market-data intake for the Railway worker, disabled by default. PR 7 adds observer-only BRTI / CF Benchmarks reference-feed intake for the Railway worker, disabled by default. PR 7a makes BRTI use a dedicated worker WebSocket by default, exposes `/reference/brti/series`, and wires the read-only dashboard Reference Price CF/BRTI chart to live BRTI data when available. PR 8 adds an observer-only strategy decision ledger v0, disabled by default, with read-only strategy status/decision endpoints and dashboard diagnostics. PR 8 also renders the dashboard Reference Price chart as the current fixed Kalshi 15-minute interval instead of a trailing rolling plot. PR 8a adds worker-owned storage retention and read-only `/storage/status` database lifecycle diagnostics; it is disabled by default, strips old raw payload JSON, deletes old observer rows in bounded batches, writes `storage_retention_runs` audit rows, and never exposes a destructive API endpoint. PR 8b hardens BRTI freshness and reconnect diagnostics. PR 9 adds a dry-run-only BTC15 momentum evaluator and hypothetical ledger; it remains disabled by default and never places orders, paper trades, reads balances, or uses private/user channels.

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

PR 8 implements only a read-only strategy decision ledger. PR 9 adds dry-run-only momentum evaluation and simulated ledger rows from persisted market, BRTI, orderbook, and public trade rows. It does not implement paper trading, live trading, orders, fills, private channels, account reads, or execution.

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

No live trading, paper trading, Kalshi order placement, real strategy execution, or trading-capable dashboard behavior is included. The PR 6 ingestion loop is an observer-only Railway worker WebSocket collector for public Kalshi ticker, orderbook, and trade messages. PR 7/7a adds observer-only BRTI reference ticks through Kalshi's authenticated `cfbenchmarks_value` WebSocket channel for `index_ids=["BRTI"]`; it stores diagnostics and a safe read-only series only. PR 8 writes an observer-only decision ledger to `strategy_decisions`. PR 8a is storage lifecycle only; it does not change strategy logic, stale/transport semantics, credentials, or trading safety. PR 8b hardens BRTI freshness diagnostics and worker-side BRTI reconnects without changing formulas, strategy thresholds, execution, credentials, ingestion scope, or storage retention behavior. PR 9 may emit `ENTER_DRY_RUN`, `MANAGE_POSITION`, `EXIT_SIGNAL`, or `FORCE_EXIT` only as hypothetical ledger states when `APP_MODE=DRY_RUN`, `STRATEGY_OBSERVER_ENABLED=true`, `STRATEGY_DRY_RUN_ENABLED=true`, `TRADING_ENABLED=false`, and `EXECUTE=false`.

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
9. Momentum strategy decision engine, dry-run only. Current PR.
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

Next manual checkpoint after PR 9: keep market WebSocket, BRTI, strategy observer, and storage retention worker settings healthy; set `APP_MODE=DRY_RUN`, `STRATEGY_OBSERVER_ENABLED=true`, `STRATEGY_DRY_RUN_ENABLED=true`, `TRADING_ENABLED=false`, and `EXECUTE=false` on the Railway worker only; redeploy the worker; and validate worker logs, `/strategy/dry-run/status`, `/strategy/dry-run/positions/open`, `/strategy/dry-run/positions/recent`, `/strategy/dry-run/events/recent`, `/strategy/status`, `/strategy/decisions/latest`, `/strategy/decisions/recent`, `/storage/status`, `/ws/status`, `/reference/brti/status`, `/reference/brti/latest`, `/reference/brti/series`, `/health`, `/safety`, `/db/status`, `/ready`, `/kalshi/status`, and `/markets/active`. The API and dashboard may stay read-only; Vercel must not receive Kalshi credentials, WebSocket variables, BRTI env vars, strategy env vars, storage retention env vars, or dry-run controls.
