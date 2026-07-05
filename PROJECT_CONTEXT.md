# APE Project Context

Canonical repo: https://github.com/kenxw10/APE

APE is a standalone BTC15 Kalshi Momentum Bot project. It is separate from BULL.

## Platform Direction

Planned platform split:

- Railway backend API
- Railway always-on worker
- Railway Postgres
- Vercel dashboard

PR 1 does not add Railway, Postgres, or Vercel configuration.

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

PR 1 does not implement the strategy.

## Safety Defaults

Required safe defaults:

```text
APP_MODE=OBSERVER
TRADING_ENABLED=false
EXECUTE=false
```

PR 1 blocks startup when:

- `APP_MODE` is not `OBSERVER`
- `TRADING_ENABLED=true`
- `EXECUTE=true`

No live trading, paper trading, Kalshi order placement, strategy execution, or database persistence is included in PR 1.

## PR Ladder

This ladder is directional and should be reviewed before each PR.

1. Repo foundation and observer-only skeleton.
2. Kalshi BTC15 market catalog and contract resolver in observer mode.
3. BRTI/reference data intake in observer mode.
4. Kalshi order book and trade websocket observer.
5. Observer state API, health, safety, and SSE diagnostics.
6. Storage lifecycle, retention policy, and local replay fixtures.
7. Deterministic replay harness for captured market/reference data.
8. Momentum feature calculations without trade decisions.
9. Dry-run decision interface with execution still blocked.
10. Spread, depth, liquidity, and anti-chop gate diagnostics.
11. Paper trading simulator after dry-run evidence review.
12. Calibration and reporting workflow for strategy quality.
13. Railway API/worker/Postgres and Vercel dashboard wiring.
14. Manual live-canary safety plan with tiny limits and approvals.
15. Post-canary monitoring, rollback, alerting, and hardening.

