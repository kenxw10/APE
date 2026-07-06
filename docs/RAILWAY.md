# Railway Deployment

PR 3 adds Railway deployment scaffolding for APE in observer-only mode.

APE should be deployed as two Railway services from the same GitHub repo:

- API service
- Always-on worker service

Railway Postgres should be attached to the project and should provide `DATABASE_URL`.

Railway/Railpack installs Python runtime dependencies from the root `requirements.txt`. If deploy logs show missing Python modules such as `sqlalchemy`, confirm `requirements.txt` exists and includes the runtime dependencies from `pyproject.toml`.

## Safety Defaults

Set these environment variables for both Railway services:

```text
APP_MODE=OBSERVER
TRADING_ENABLED=false
EXECUTE=false
ENV=railway
LOG_LEVEL=INFO
```

After PR 5 merges, Kalshi credentials may be added to Railway API and worker env only. Do not enable trading. Do not configure cron. Vercel must not receive Kalshi credentials.

After PR 6 merges, Kalshi WebSocket intake is still disabled by default. Enable it only on the Railway worker service with `KALSHI_WS_ENABLED=true` after API/worker safety and credentials are validated.

After PR 7a merges, BRTI / CF Benchmarks intake is still disabled by default. Enable it only on the Railway worker service with `KALSHI_CFBENCHMARKS_ENABLED=true` and `KALSHI_CFBENCHMARKS_INDEX_IDS=BRTI`. BRTI uses a dedicated worker-owned WebSocket connection by default. Do not add BRTI env vars or Kalshi credentials to Vercel.

After PR 8 merges, the strategy observer ledger is still disabled by default. Enable it only on the Railway worker service with `STRATEGY_OBSERVER_ENABLED=true` after market WebSocket and BRTI intake are healthy. This records observer-only decisions in the database and does not add paper trading, live trading, orders, fills, private channels, or execution.

## Create Railway Project

1. Create a new Railway project for APE.
2. Add Railway Postgres.
3. Create the API service from `https://github.com/kenxw10/APE`.
4. Create the worker service from the same GitHub repo.
5. Link both services to the Railway Postgres variables so each service receives `DATABASE_URL`.

## API Service

Set the API service start command:

```text
python -m scripts.railway_start_api
```

The API helper runs database migrations before starting the API:

```text
python -m ape.db.migrations
python -m ape.api.main
```

Railway provides `PORT`. APE uses `PORT` when `API_PORT` is not set. Leave `API_HOST=0.0.0.0`.

Useful API endpoints:

```text
/health
/safety
/db/status
/ready
/kalshi/status
/markets/active
/ws/status
/reference/brti/status
/reference/brti/latest
/reference/brti/series
/strategy/status
/strategy/decisions/latest
/strategy/decisions/recent
```

`/ready` should return `ready` only when safety is safe and database connectivity works.

## Worker Service

Set the worker service start command:

```text
python -m scripts.railway_start_worker
```

The helper runs:

```text
python -m ape.db.migrations
python -m ape.worker.main
```

The API and worker helpers both run the same idempotent migrations before their service starts. The migration runner takes a PostgreSQL advisory transaction lock and uses idempotent schema/version writes so simultaneous API and worker restarts serialize safely. Prefer redeploying the API first for schema-changing PRs, but the worker is protected if it starts first or restarts during a deploy. If migrations fail, the worker does not start.

The worker is an always-on observer process. Do not configure a Railway cron job for it.

When `KALSHI_WS_ENABLED=false`, the worker records heartbeat-only diagnostics. When `KALSHI_WS_ENABLED=true`, the worker owns the observer-only Kalshi WebSocket collector for the active BTC15 market.

When `KALSHI_CFBENCHMARKS_ENABLED=true`, the worker also subscribes to Kalshi's authenticated `cfbenchmarks_value` channel with `index_ids=["BRTI"]` and stores observer-only reference ticks in `reference_ticks`. By default this runs on a dedicated BRTI WebSocket connection so BTC15 market rollover/resubscribe does not disconnect the reference feed. This does not add strategy, paper trading, live trading, orders, private channels, or execution.

When `STRATEGY_OBSERVER_ENABLED=true`, the worker also evaluates the observer-only strategy ledger from persisted database rows. It reads active market metadata, BRTI ticks, Kalshi orderbook snapshots, and public trades; then writes a diagnostic row to `strategy_decisions`. It does not call Kalshi REST, subscribe to private channels, place orders, paper trade, live trade, or emit enter/fill/execution states.

For `/ws/status`, `last_error_type` and `last_error_message` describe a current unresolved worker error. A successful current orderbook or trade database write clears old recovered errors so stale startup failures do not keep the status page red.

If a manual migration is needed outside API startup, run:

```text
python -m scripts.railway_migrate
```

## Required Environment Variables

Use these for both services:

```text
APP_MODE=OBSERVER
TRADING_ENABLED=false
EXECUTE=false
DATABASE_URL=<provided by Railway Postgres>
ENV=railway
LOG_LEVEL=INFO
```

Do not commit or paste real database URLs into repo files.

## Optional Database Variables

```text
DB_ECHO=false
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=10
DB_STATEMENT_TIMEOUT_MS=5000
```

## Kalshi Credential Checkpoint After PR 5

Only after PR 5 is merged, add these to Railway API and worker services:

```text
KALSHI_API_KEY_ID=<Railway secret>
KALSHI_PRIVATE_KEY=<Railway secret>
KALSHI_API_BASE_URL=https://external-api.kalshi.com/trade-api/v2
KALSHI_ENV=prod
KALSHI_BTC15_SERIES_TICKER=KXBTC15M
KALSHI_REST_TIMEOUT_SECONDS=10
KALSHI_RESOLVER_PARSER_VERSION=btc15_resolver_v1
```

If Railway stores the private key as one line with escaped `\n` characters, APE normalizes it before signing. Do not paste the private key into logs, docs, GitHub, Vercel, or local screenshots.

After setting Railway credentials, redeploy API and worker and validate:

```powershell
Invoke-RestMethod https://ape-api-production.up.railway.app/kalshi/status
Invoke-RestMethod https://ape-api-production.up.railway.app/markets/active
Invoke-RestMethod https://ape-api-production.up.railway.app/health
Invoke-RestMethod https://ape-api-production.up.railway.app/safety
Invoke-RestMethod https://ape-api-production.up.railway.app/db/status
Invoke-RestMethod https://ape-api-production.up.railway.app/ready
```

Expected behavior: safety remains observer-only, `/kalshi/status` reports configured booleans without secrets, and `/markets/active` resolves or returns a safe diagnostic state without placing orders.

## Kalshi WebSocket Checkpoint After PR 6

Only after PR 6 is merged and PR 5 credentials are validated, add these to the Railway worker service:

```text
KALSHI_WS_BASE_URL=wss://external-api-ws.kalshi.com/trade-api/ws/v2
KALSHI_WS_ENABLED=true
KALSHI_WS_CONNECT_TIMEOUT_SECONDS=10
KALSHI_WS_HEARTBEAT_TIMEOUT_SECONDS=30
KALSHI_WS_RECONNECT_SECONDS=5
KALSHI_WS_MAX_RECONNECT_SECONDS=60
KALSHI_WS_SUBSCRIBE_ORDERBOOK=true
KALSHI_WS_SUBSCRIBE_TICKER=true
KALSHI_WS_SUBSCRIBE_TRADES=true
```

The API service may keep `KALSHI_WS_ENABLED=false`; `/ws/status` reads database rows and worker heartbeat metadata. Do not add WebSocket credentials or Kalshi secrets to Vercel.

After enabling the worker collector, redeploy the worker and validate:

```powershell
Invoke-RestMethod https://ape-api-production.up.railway.app/ws/status
Invoke-RestMethod https://ape-api-production.up.railway.app/health
Invoke-RestMethod https://ape-api-production.up.railway.app/safety
Invoke-RestMethod https://ape-api-production.up.railway.app/db/status
Invoke-RestMethod https://ape-api-production.up.railway.app/ready
Invoke-RestMethod https://ape-api-production.up.railway.app/kalshi/status
Invoke-RestMethod https://ape-api-production.up.railway.app/markets/active
```

Expected behavior:

- Worker logs show WebSocket connect/subscribe diagnostics without secrets.
- `/ws/status` shows `enabled=true` from worker metadata, recent orderbook data, or a safe diagnostic state.
- `orderbook_snapshots` rows are written when Kalshi sends snapshots/deltas.
- `public_trades` rows may be sparse, but trade messages persist when received.
- Dashboard remains read-only. Dashboard validation may identify the WebSocket panels as
  `Kalshi WS` and `WS Channels`; direct API `/ws/status` success is also an acceptable
  validation signal.

## BRTI Reference Checkpoint After PR 7a

Only after PR 7 is merged and PR 6d WebSocket validation is healthy, add these to the Railway worker service:

```text
KALSHI_CFBENCHMARKS_ENABLED=true
KALSHI_CFBENCHMARKS_INDEX_IDS=BRTI
KALSHI_CFBENCHMARKS_STALE_AFTER_SECONDS=3
KALSHI_CFBENCHMARKS_MAX_SOURCE_AGE_MS=3000
KALSHI_CFBENCHMARKS_SUBSCRIBE_ON_WORKER=true
KALSHI_CFBENCHMARKS_PERSIST_RAW_PAYLOAD=true
KALSHI_CFBENCHMARKS_DEDICATED_CONNECTION=true
KALSHI_CFBENCHMARKS_TRANSPORT_STALE_AFTER_SECONDS=5
KALSHI_CFBENCHMARKS_PERSISTENCE_STALE_AFTER_SECONDS=5
KALSHI_CFBENCHMARKS_SOURCE_AGE_WARN_MS=45000
KALSHI_CFBENCHMARKS_KALSHI_RECEIVED_WARN_MS=10000
KALSHI_CFBENCHMARKS_TRADE_FRESH_MS=2000
```

Keep:

```text
APP_MODE=OBSERVER
TRADING_ENABLED=false
EXECUTE=false
KALSHI_WS_ENABLED=true
```

The API service may keep `KALSHI_CFBENCHMARKS_ENABLED=false`; `/reference/brti/status` reads database rows and worker heartbeat metadata. Do not add BRTI env vars, WebSocket settings, or Kalshi credentials to Vercel.

After enabling BRTI on the worker, redeploy the worker and validate:

```powershell
Invoke-RestMethod https://ape-api-production.up.railway.app/reference/brti/status
Invoke-RestMethod https://ape-api-production.up.railway.app/reference/brti/latest
Invoke-RestMethod "https://ape-api-production.up.railway.app/reference/brti/series?window_seconds=900&max_points=16000"
Invoke-RestMethod https://ape-api-production.up.railway.app/ws/status
Invoke-RestMethod https://ape-api-production.up.railway.app/health
Invoke-RestMethod https://ape-api-production.up.railway.app/safety
Invoke-RestMethod https://ape-api-production.up.railway.app/db/status
Invoke-RestMethod https://ape-api-production.up.railway.app/ready
```

Expected behavior:

- Worker logs show the BRTI subscription without secrets.
- `/reference/brti/status` shows `enabled=true`, `index_ids=["BRTI"]`, `connection_state=subscribed`, recent transport/persistence timestamps, no blockers, and null `last_error_type` / `last_error_message`.
- Source age may be lagging without making observer status globally stale. Read `source_stale`, `kalshi_received_stale`, and `trade_ready_fresh` separately from `transport_stale` and `persistence_stale`.
- `/reference/brti/latest` returns the latest safe reference tick shape without raw payloads or credentials.
- `/reference/brti/series` returns live BRTI points sorted by `received_at`, capped at a 900-second rolling window and 16,000 points, without raw payloads.
- `reference_ticks` rows are written when Kalshi emits `cfbenchmarks_value` events.
- BRTI final-minute averages are stored when present, but no strategy or position-management logic uses them in PR 7a.
- Dashboard remains read-only and may show live BRTI status plus the current fixed Kalshi 15-minute CF/BRTI chart from the public API only.

## Strategy Observer Checkpoint After PR 8

Only after PR 8 is merged and the PR 7a BRTI/reference validation is healthy, add these to the Railway worker service:

```text
STRATEGY_OBSERVER_ENABLED=true
STRATEGY_OBSERVER_POLL_SECONDS=1.0
STRATEGY_OBSERVER_DECISION_TTL_SECONDS=5
STRATEGY_MIN_BOUNDARY_DISTANCE_BPS=3.5
STRATEGY_REFERENCE_MAX_AGE_MS=2000
STRATEGY_KALSHI_BOOK_MAX_AGE_MS=2000
STRATEGY_NO_ENTRY_FIRST_SECONDS=300
STRATEGY_NO_ENTRY_LAST_SECONDS=60
STRATEGY_MIN_ENTRY_ASK=0.56
STRATEGY_MAX_ENTRY_ASK=0.78
STRATEGY_MAX_SPREAD_CENTS=4
```

Keep:

```text
APP_MODE=OBSERVER
TRADING_ENABLED=false
EXECUTE=false
KALSHI_WS_ENABLED=true
KALSHI_CFBENCHMARKS_ENABLED=true
KALSHI_CFBENCHMARKS_INDEX_IDS=BRTI
```

The API service may keep `STRATEGY_OBSERVER_ENABLED=false`; `/strategy/status` reads latest decision rows and worker heartbeat metadata. Do not add strategy observer env vars, WebSocket settings, BRTI env vars, or Kalshi credentials to Vercel.

After enabling the strategy observer on the worker, redeploy the worker and validate:

```powershell
Invoke-RestMethod https://ape-api-production.up.railway.app/strategy/status
Invoke-RestMethod https://ape-api-production.up.railway.app/strategy/decisions/latest
Invoke-RestMethod "https://ape-api-production.up.railway.app/strategy/decisions/recent?limit=100"
Invoke-RestMethod https://ape-api-production.up.railway.app/ws/status
Invoke-RestMethod https://ape-api-production.up.railway.app/reference/brti/status
Invoke-RestMethod https://ape-api-production.up.railway.app/health
Invoke-RestMethod https://ape-api-production.up.railway.app/safety
Invoke-RestMethod https://ape-api-production.up.railway.app/db/status
Invoke-RestMethod https://ape-api-production.up.railway.app/ready
```

Expected behavior:

- `/strategy/status` shows `enabled=true`, `is_safe=true`, a recent latest decision, and `stale=false` when the worker is evaluating.
- `/strategy/decisions/latest` returns a diagnostic state such as `OBSERVE_ONLY_MARKET`, `REFERENCE_STALE`, `KALSHI_STALE`, `TOO_EARLY`, or `TOO_CLOSE_TO_BOUNDARY`.
- No strategy endpoint returns private keys, signatures, raw Kalshi credentials, enter actions, order actions, fills, paper trades, or live-trading controls.
- `strategy_decisions` rows are written at no more than the configured poll cadence unless the persisted context changes inside the same bucket.
- Dashboard remains read-only and may show Strategy Observer status from the public API only.

## Explicitly Not Included

- Live trading
- Paper trading
- Order placement
- Strategy execution
- Private/user WebSocket subscriptions
- Vercel secrets or trading controls
- Railway cron

Railway production validation should happen after merge using GPT-provided commands.
