# APE Project Context

Canonical repo: https://github.com/kenxw10/APE

APE is a standalone BTC15 Kalshi Momentum Bot project. It is separate from BULL.

## Platform Direction

Planned platform split:

- Railway backend API
- Railway always-on worker
- Railway Postgres
- Vercel dashboard

PR 1 is merged and validated. PR 2 is merged and validated. PR 3 adds Railway backend deployment scaffolding for the API and always-on worker, but still does not add market ingestion, strategy execution, live trading, paper trading, or Vercel dashboard code.

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

No live trading, paper trading, Kalshi order placement, strategy execution, external market data calls, or dashboard behavior is included.

## PR Ladder

This ladder is directional and should be reviewed before each PR.

1. Repo foundation and observer-only skeleton. Completed and validated.
2. Postgres schema and repository foundation. Completed and validated.
3. Railway backend deployment scaffold. Current PR.
4. Kalshi BTC15 market catalog and contract resolver in observer mode.
5. BRTI/reference data intake in observer mode.
6. Kalshi order book and trade websocket observer.
7. Observer state API, health, safety, and SSE diagnostics.
8. Storage lifecycle, retention policy, and local replay fixtures.
9. Deterministic replay harness for captured market/reference data.
10. Momentum feature calculations without trade decisions.
11. Dry-run decision interface with execution still blocked.
12. Spread, depth, liquidity, and anti-chop gate diagnostics.
13. Paper trading simulator after dry-run evidence review.
14. Calibration and reporting workflow for strategy quality.
15. Railway Postgres/Vercel dashboard wiring beyond the backend scaffold.
16. Manual live-canary safety plan with tiny limits and approvals.
17. Post-canary monitoring, rollback, alerting, and hardening.
