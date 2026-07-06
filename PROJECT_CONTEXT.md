# APE Project Context

Canonical repo: https://github.com/kenxw10/APE

APE is a standalone BTC15 Kalshi Momentum Bot project. It is separate from BULL.

## Platform Direction

Planned platform split:

- Railway backend API
- Railway always-on worker
- Railway Postgres
- Vercel dashboard

PR 1 is merged and validated. PR 2 is merged and validated. PR 3 adds Railway backend deployment scaffolding for the API and always-on worker. PR 3a adds Railway runtime dependency packaging. PR 4 adds a Vercel-ready read-only dashboard scaffold. PR 4a adds explicit Vercel build configuration. PR 4b adds dashboard visual polish. PR 5 adds observer-only Kalshi REST auth diagnostics and active BTC15 market resolution. PR 6 adds observer-only Kalshi WebSocket market-data intake for the Railway worker, disabled by default. PR 7 adds observer-only BRTI / CF Benchmarks reference-feed intake for the Railway worker, disabled by default.

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

PR 1 did not implement the strategy. PR 2 and PR 3 also do not implement ingestion, strategy decisions, paper trading, live trading, or execution.

## Safety Defaults

Required safe defaults:

```text
APP_MODE=OBSERVER
TRADING_ENABLED=false
EXECUTE=false
```

Current safety policy blocks startup when:

- `APP_MODE` is not `OBSERVER`
- `TRADING_ENABLED=true`
- `EXECUTE=true`

No live trading, paper trading, Kalshi order placement, strategy execution, or trading-capable dashboard behavior is included. The PR 6 ingestion loop is an observer-only Railway worker WebSocket collector for public Kalshi ticker, orderbook, and trade messages. PR 7 adds observer-only BRTI reference ticks through Kalshi's authenticated `cfbenchmarks_value` WebSocket channel for `index_ids=["BRTI"]`; it stores diagnostics only and does not generate strategy decisions.

Kalshi REST/WebSocket credentials are optional at startup. When missing, `/kalshi/status`, `/markets/active`, and `/ws/status` return safe diagnostics. If configured, credentials belong only in Railway API/worker environment variables, never in Vercel.

## PR Ladder

This ladder is directional and should be reviewed before each PR.

1. Repo foundation and observer-only skeleton. Completed and validated.
2. Postgres schema and repository foundation. Completed and validated.
3. Railway backend deployment scaffold. Completed and validated.
4. Vercel-ready read-only dashboard scaffold. Completed and validated.
5. Kalshi BTC15 market catalog and contract resolver in observer mode. Completed and validated.
6. Kalshi orderbook, ticker, and public trade WebSocket observer. Completed and validated.
7. BRTI/reference data intake in observer mode. Current PR.
8. Observer state API, health, safety, and SSE diagnostics.
9. Storage lifecycle, retention policy, and local replay fixtures.
10. Deterministic replay harness for captured market/reference data.
11. Momentum feature calculations without trade decisions.
12. Dry-run decision interface with execution still blocked.
13. Spread, depth, liquidity, and anti-chop gate diagnostics.
14. Paper trading simulator after dry-run evidence review.
15. Calibration and reporting workflow for strategy quality.
16. Railway Postgres/Vercel dashboard wiring beyond the backend scaffold.
17. Manual live-canary safety plan with tiny limits and approvals.
18. Post-canary monitoring, rollback, alerting, and hardening.

Next manual checkpoint after PR 7: keep `KALSHI_WS_ENABLED=true` on the Railway worker, add `KALSHI_CFBENCHMARKS_ENABLED=true` and `KALSHI_CFBENCHMARKS_INDEX_IDS=BRTI` to the Railway worker only, keep API and worker observer-only, redeploy the worker, and validate worker logs, `/ws/status`, `/reference/brti/status`, `/reference/brti/latest`, `reference_ticks`, `/health`, `/safety`, `/db/status`, `/ready`, `/kalshi/status`, and `/markets/active`. Do not add Kalshi credentials, WebSocket variables, or BRTI variables to Vercel.
