# Railway Deployment

PR 3 adds Railway deployment scaffolding for APE in observer-only mode.

APE started as two Railway services from the same GitHub repo:

- API service
- Always-on worker service

After PR 9f, production should use dedicated worker services instead of the
old all-in-one worker:

- `ape-api`
- `ape-market-worker`
- `ape-reference-worker`
- `ape-strategy-worker`
- `ape-maintenance-worker`

The all-in-one worker remains available only for local development and small
test deployments with `python -m ape.worker --role all`.

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

After PR 8a merges, storage retention is still disabled by default. Enable it only on the Railway worker service with `STORAGE_RETENTION_ENABLED=true` after `/storage/status` and migrations are validated. Retention deletes old observer data in bounded batches, strips old raw payload JSON, writes audit rows, and does not add public delete controls or automatic `VACUUM FULL`.

After PR 9 merges, dry-run remains disabled by default. Enable it only on the Railway worker service with `APP_MODE=DRY_RUN`, `STRATEGY_OBSERVER_ENABLED=true`, `STRATEGY_DRY_RUN_ENABLED=true`, `TRADING_ENABLED=false`, and `EXECUTE=false` after market WebSocket, BRTI, strategy observer, and storage retention are healthy. Dry-run writes hypothetical simulated positions/events only; it does not place orders, paper trade, live trade, read balances, or use private/user channels.

After PR 9f merges, production workers are split by role. The role is selected
with `--role` or `APE_WORKER_ROLE`; the CLI flag wins when both are set. A
role-specific worker only starts loops for that role, even if unrelated env
flags are accidentally enabled.

## Create Railway Project

1. Create a new Railway project for APE.
2. Add Railway Postgres.
3. Create the API service from `https://github.com/kenxw10/APE`.
4. Create the four worker services from the same GitHub repo.
5. Link all services to the Railway Postgres variables so each service receives `DATABASE_URL`.

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
/ws/protocol/recent
/ws/protocol/summary
/reference/brti/status
/reference/brti/latest
/reference/brti/series
/strategy/status
/strategy/decisions/latest
/strategy/decisions/recent
/strategy/gates/recent
/strategy/dry-run/status
/strategy/dry-run/positions/open
/strategy/dry-run/positions/recent
/strategy/dry-run/events/recent
/storage/status
```

`/ready` should return `ready` only when safety is safe and database connectivity works.

## Worker Services

Use one dedicated Railway worker service for each production role. The role can
also be supplied with `APE_WORKER_ROLE`, but the start command makes the owner
of each process explicit.

Market data worker:

```text
python -m ape.worker --role market-data
```

Reference BRTI worker:

```text
python -m ape.worker --role reference-brti
```

Strategy worker:

```text
python -m ape.worker --role strategy
```

Maintenance worker:

```text
python -m ape.worker --role maintenance
```

Run database migrations through the API startup helper or `python -m scripts.railway_migrate` before starting role-specific workers on schema-changing deploys. The migration runner takes a PostgreSQL advisory transaction lock and uses idempotent schema/version writes so simultaneous starts serialize safely.

Each worker is an always-on process. Do not configure a Railway cron job for these services.

The market data worker owns only the observer-only Kalshi public market WebSocket collector for the active BTC15 market. It may resolve the active BTC15 market, subscribe to `orderbook_delta`, `ticker`, and `trade`, persist market metadata, orderbook snapshots, public trades, market heartbeats, and Kalshi WebSocket protocol events. It does not run BRTI, strategy evaluation, dry-run ledger logic, or storage retention.

The reference worker owns only Kalshi's authenticated `cfbenchmarks_value` channel with `index_ids=["BRTI"]` and stores observer-only reference ticks in `reference_ticks`. It does not run market WebSockets, strategy evaluation, dry-run ledger logic, or storage retention.

The strategy worker reads persisted market, orderbook, public trade, reference, and component heartbeat rows only. It evaluates strategy diagnostics and dry-run hypothetical ledger rows when enabled. It does not open Kalshi WebSocket connections, call order APIs, read balances, subscribe to private channels, paper trade, or live trade.

The maintenance worker runs storage retention only. It does not run market WebSockets, BRTI, strategy evaluation, or dry-run ledger logic.

When `APP_MODE=DRY_RUN`, `STRATEGY_OBSERVER_ENABLED=true`, and `STRATEGY_DRY_RUN_ENABLED=true`, the strategy worker may write hypothetical rows to `strategy_dry_run_positions` and `strategy_dry_run_events`. This is not paper trading: there are no Kalshi order API calls, no account balance reads, no private/user WebSocket channels, no real fills, and no execution controls.

Worker feed liveness is component-scoped. The market collector writes `ape-worker.market_data`, the BRTI collector writes `ape-worker.reference_brti`, the strategy observer writes `ape-worker.strategy`, and storage retention writes `ape-worker.maintenance`. The legacy `ape-worker` aggregate row and older component aliases remain only for backward compatibility; `/ws/status`, `/reference/brti/status`, and strategy readiness prefer the role-specific component rows.

After PR 9d, market feed readiness separates WebSocket transport liveness from market-data quietness. The market collector proves transport with a client ping/pong, records `last_market_data_message_at` separately, and exposes transport, subscription, active ticker, snapshot, sequence, quiet-data, snapshot-source, and recovery-action fields through `/ws/status` and strategy diagnostics. A quiet public market-data stream may be carried forward as a warning only while the transport, subscription, ticker, snapshot, and sequence state are healthy and the latest book remains inside the hard carry-forward cap. BRTI remains stricter because valid BRTI ticks are expected roughly once per second.

After PR 9e, market feed recovery adds bounded subscription and rollover diagnostics. The worker waits for confirmed Kalshi orderbook subscription SIDs before requesting snapshots, escalates missing or timed-out SIDs to a market WebSocket reconnect, requests snapshots for uninitialized books and quiet-but-live streams, and records recovery fields including `market_feed_state`, subscription recovery count/reason/action/result, snapshot resync count/result, rollover recovery count, transport reconnect count, unrecovered blocker count, and recovery attempt age. These diagnostics remain read-only and do not add paper/live trading, order placement, private channels, account reads, credentials, or dashboard trading controls.

After PR 9f, the market worker also records raw Kalshi WebSocket protocol evidence in `kalshi_ws_protocol_events`. `/ws/status` exposes worker role, connection ID, subscription reconciliation, confirmed SIDs, list-subscription results, in-flight snapshot state, market DB writer queue metrics, recent protocol error count, reconnect reason, and close details. `/ws/protocol/recent` and `/ws/protocol/summary` expose read-only subscribe/update/list/ping/pong/close/error evidence for production validation.

After PR 9g, the market worker persistence path is split into critical market state and noncritical diagnostic/protocol queues. Critical latest market state, orderbook snapshots, public trades, rollover state, and heartbeats are prioritized; routine protocol events are sampled/rate-limited and may be dropped under diagnostic backpressure. `/ws/status` separates critical queue health from diagnostic queue health and reports coalesced orderbook writes, dropped diagnostic events, sampled protocol events, and latest state persistence age/lag. No env change is required before PR 9g unless the optional defaults below need tuning.

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

## Dedicated Worker Environment Matrix After PR 9f

Use these service-specific groups for production validation. Keep Kalshi
credentials and worker-loop variables out of Vercel.

`ape-api`:

```text
DATABASE_URL=<provided by Railway Postgres>
ENV=railway
LOG_LEVEL=INFO
DB_ECHO=false
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=10
DB_STATEMENT_TIMEOUT_MS=5000
APP_MODE=DRY_RUN
TRADING_ENABLED=false
EXECUTE=false
```

Kalshi REST credentials may remain on the API only for read-only
`/kalshi/status` and `/markets/active` diagnostics if that is the current
deployment choice. Do not put `KALSHI_WS_*`, `KALSHI_CFBENCHMARKS_*`, strategy
worker loop settings, or storage retention loop settings on the API unless a
future PR explicitly needs a read-only config display.

`ape-market-worker`:

```text
DATABASE_URL=<provided by Railway Postgres>
ENV=railway
LOG_LEVEL=INFO
DB_ECHO=false
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=10
DB_STATEMENT_TIMEOUT_MS=5000
KALSHI_API_KEY_ID=<Railway secret>
KALSHI_PRIVATE_KEY=<Railway secret>
KALSHI_API_BASE_URL=https://external-api.kalshi.com/trade-api/v2
KALSHI_ENV=prod
KALSHI_BTC15_SERIES_TICKER=KXBTC15M
KALSHI_REST_TIMEOUT_SECONDS=10
KALSHI_RESOLVER_PARSER_VERSION=btc15_resolver_v1
KALSHI_WS_BASE_URL=wss://external-api-ws.kalshi.com/trade-api/ws/v2
KALSHI_WS_ENABLED=true
KALSHI_WS_CONNECT_TIMEOUT_SECONDS=10
KALSHI_WS_HEARTBEAT_TIMEOUT_SECONDS=30
KALSHI_WS_RECONNECT_SECONDS=5
KALSHI_WS_MAX_RECONNECT_SECONDS=60
KALSHI_WS_SUBSCRIBE_ORDERBOOK=true
KALSHI_WS_SUBSCRIBE_TICKER=true
KALSHI_WS_SUBSCRIBE_TRADES=true
MARKET_DB_WRITER_CRITICAL_QUEUE_MAX_SIZE=2000
MARKET_DB_WRITER_DIAGNOSTIC_QUEUE_MAX_SIZE=5000
MARKET_DB_WRITER_FLUSH_INTERVAL_MS=250
MARKET_DB_WRITER_MAX_BATCH_SIZE=500
MARKET_DB_WRITER_MAX_FLUSH_MS=1000
MARKET_ORDERBOOK_SNAPSHOT_MIN_INTERVAL_MS=250
MARKET_PROTOCOL_EVENT_SAMPLE_RATE=0.02
MARKET_PROTOCOL_EVENT_ERROR_SAMPLE_RATE=1.0
MARKET_PROTOCOL_EVENT_MAX_PER_FLUSH=100
MARKET_DB_WRITER_BACKPRESSURE_WARN_DEPTH=750
MARKET_DB_WRITER_BACKPRESSURE_BLOCK_DEPTH=1500
MARKET_DB_WRITER_BACKPRESSURE_MAX_AGE_MS=10000
APE_WORKER_ROLE=market-data
APP_MODE=OBSERVER
TRADING_ENABLED=false
EXECUTE=false
```

`APP_MODE=DRY_RUN` is also safe on `ape-market-worker`; the `market-data` role
still prevents strategy and dry-run loops from starting.

`ape-reference-worker`:

```text
DATABASE_URL=<provided by Railway Postgres>
ENV=railway
LOG_LEVEL=INFO
DB_ECHO=false
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=10
DB_STATEMENT_TIMEOUT_MS=5000
KALSHI_API_KEY_ID=<Railway secret>
KALSHI_PRIVATE_KEY=<Railway secret>
KALSHI_WS_BASE_URL=wss://external-api-ws.kalshi.com/trade-api/ws/v2
KALSHI_CFBENCHMARKS_ENABLED=true
KALSHI_CFBENCHMARKS_INDEX_IDS=BRTI
KALSHI_CFBENCHMARKS_STALE_AFTER_SECONDS=3
KALSHI_CFBENCHMARKS_MAX_SOURCE_AGE_MS=3000
KALSHI_CFBENCHMARKS_SUBSCRIBE_ON_WORKER=true
KALSHI_CFBENCHMARKS_DEDICATED_CONNECTION=true
KALSHI_CFBENCHMARKS_TRANSPORT_STALE_AFTER_SECONDS=5
KALSHI_CFBENCHMARKS_PERSISTENCE_STALE_AFTER_SECONDS=5
KALSHI_CFBENCHMARKS_SOURCE_AGE_WARN_MS=45000
KALSHI_CFBENCHMARKS_KALSHI_RECEIVED_WARN_MS=10000
KALSHI_CFBENCHMARKS_TRADE_FRESH_MS=2000
APE_WORKER_ROLE=reference-brti
TRADING_ENABLED=false
EXECUTE=false
```

`ape-strategy-worker`:

```text
DATABASE_URL=<provided by Railway Postgres>
ENV=railway
LOG_LEVEL=INFO
DB_ECHO=false
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=10
DB_STATEMENT_TIMEOUT_MS=5000
APP_MODE=DRY_RUN
STRATEGY_OBSERVER_ENABLED=true
STRATEGY_DRY_RUN_ENABLED=true
STRATEGY_ID=btc15_momentum_v1
TRADING_ENABLED=false
EXECUTE=false
APE_WORKER_ROLE=strategy
```

Add the remaining `STRATEGY_*` readiness and dry-run variables from the dry-run
checkpoint below. The strategy worker should not need `KALSHI_WS_*` or
`KALSHI_CFBENCHMARKS_*` worker-loop variables to run because it reads persisted
database rows and component heartbeats.

`ape-maintenance-worker`:

```text
DATABASE_URL=<provided by Railway Postgres>
ENV=railway
LOG_LEVEL=INFO
DB_ECHO=false
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=10
DB_STATEMENT_TIMEOUT_MS=5000
STORAGE_RETENTION_ENABLED=true
STORAGE_RETENTION_INTERVAL_SECONDS=300
STORAGE_RETENTION_BATCH_SIZE=5000
STORAGE_RETENTION_MAX_RUN_SECONDS=20
STORAGE_RETENTION_DRY_RUN=false
STORAGE_RETENTION_KALSHI_WS_PROTOCOL_EVENTS_SECONDS=21600
APE_WORKER_ROLE=maintenance
TRADING_ENABLED=false
EXECUTE=false
```

Add the remaining `STORAGE_RETENTION_*` retention windows from the storage
retention checkpoint below.

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
- `/reference/brti/status` shows `enabled=true`, `index_ids=["BRTI"]`, `connection_state=subscribed`, `status_category=healthy`, recent transport/persistence/worker heartbeat timestamps, no blockers, and null `last_error_type` / `last_error_message`.
- If the worker is subscribed but no valid BRTI tick arrives, `/reference/brti/status` reports a `stale_reason` such as `brti_reference_first_tick_timeout` or `brti_reference_no_valid_tick_timeout`, increments recovery counters, and the worker reconnects the BRTI WebSocket without stopping the market collector, strategy observer, or storage retention worker.
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
STRATEGY_REFERENCE_STREAM_MAX_AGE_MS=3000
STRATEGY_REFERENCE_CARRY_FORWARD_MAX_AGE_MS=15000
STRATEGY_REFERENCE_ALLOW_DUPLICATE_SOURCE_TS_CARRY_FORWARD=true
STRATEGY_KALSHI_BOOK_MAX_AGE_MS=2000
STRATEGY_KALSHI_BOOK_STREAM_MAX_AGE_MS=3000
STRATEGY_KALSHI_BOOK_CARRY_FORWARD_MAX_AGE_MS=30000
STRATEGY_KALSHI_BOOK_REQUIRE_STREAM_LIVE=true
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

PR 9b/9c/9d/9f liveness note: orderbook and BRTI feeds are event-driven, but they do not use identical readiness rules. Market orderbook readiness separates transport, subscription, active ticker, snapshot, sequence, and quiet-data state; quiet market data may warn with `kalshi_orderbook_data_quiet_carried_forward` while the latest book is safely inside the carry-forward cap. BRTI is stricter: missing valid BRTI ticks beyond the configured timeout still block with `REFERENCE_STALE` and reconnect diagnostics. A newer strategy or retention heartbeat must not make market/BRTI feed liveness stale because strategy reads `ape-worker.market_data` and `ape-worker.reference_brti` directly.

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
- `/strategy/decisions/latest.measurements` shows `market_liveness_source=component`, `reference_liveness_source=component`, recent component heartbeat ages, market feed-state fields, and no `feed_liveness_legacy_aggregate_fallback` warning after the worker has written component rows.
- Quiet healthy market data appears as `kalshi_orderbook_data_quiet_carried_forward`, not `kalshi_orderbook_stream_stale`; stale transport, inactive subscription, ticker mismatch, missing snapshot, sequence reset/gap, invalid orderbook update, snapshot recovery failure, and carry-forward cap breach still block.
- No strategy endpoint returns private keys, signatures, raw Kalshi credentials, enter actions, order actions, fills, paper trades, or live-trading controls.
- `strategy_decisions` rows are written at no more than the configured poll cadence unless the persisted context changes inside the same bucket.
- Dashboard remains read-only and may show Strategy Observer status from the public API only.

## Storage Retention Checkpoint After PR 8a

Only after PR 8a is merged and the API/worker migrations are healthy, add these to the Railway worker service:

```text
STORAGE_RETENTION_ENABLED=true
STORAGE_RETENTION_INTERVAL_SECONDS=300
STORAGE_RETENTION_BATCH_SIZE=5000
STORAGE_RETENTION_MAX_RUN_SECONDS=20
STORAGE_RETENTION_DRY_RUN=false
STORAGE_RETENTION_ORDERBOOK_SECONDS=7200
STORAGE_RETENTION_PUBLIC_TRADES_SECONDS=86400
STORAGE_RETENTION_REFERENCE_TICKS_SECONDS=86400
STORAGE_RETENTION_WORKER_HEARTBEATS_SECONDS=21600
STORAGE_RETENTION_STRATEGY_DECISIONS_SECONDS=1209600
STORAGE_RETENTION_KALSHI_WS_PROTOCOL_EVENTS_SECONDS=21600
STORAGE_RETENTION_DRY_RUN_POSITIONS_SECONDS=2592000
STORAGE_RETENTION_DRY_RUN_EVENTS_SECONDS=2592000
STORAGE_RETENTION_MARKETS_SECONDS=2592000
STORAGE_RETENTION_RAW_PAYLOAD_ORDERBOOK_SECONDS=900
STORAGE_RETENTION_RAW_PAYLOAD_PUBLIC_TRADES_SECONDS=3600
STORAGE_RETENTION_RAW_PAYLOAD_REFERENCE_TICKS_SECONDS=3600
STORAGE_RETENTION_STATUS_WARN_BYTES=40000000000
STORAGE_RETENTION_STATUS_CRITICAL_BYTES=47500000000
```

Keep:

```text
APP_MODE=OBSERVER
TRADING_ENABLED=false
EXECUTE=false
KALSHI_WS_ENABLED=true
KALSHI_CFBENCHMARKS_ENABLED=true
KALSHI_CFBENCHMARKS_INDEX_IDS=BRTI
STRATEGY_OBSERVER_ENABLED=true
```

The API service may keep `STORAGE_RETENTION_ENABLED=false`; `/storage/status` reads audit rows, table stats, and worker heartbeat metadata. Do not add storage retention env vars, database credentials, Kalshi credentials, WebSocket settings, BRTI env vars, or strategy observer env vars to Vercel.

After enabling storage retention on the worker, redeploy the worker and validate:

```powershell
Invoke-RestMethod https://ape-api-production.up.railway.app/storage/status
Invoke-RestMethod https://ape-api-production.up.railway.app/strategy/status
Invoke-RestMethod https://ape-api-production.up.railway.app/ws/status
Invoke-RestMethod https://ape-api-production.up.railway.app/reference/brti/status
Invoke-RestMethod https://ape-api-production.up.railway.app/health
Invoke-RestMethod https://ape-api-production.up.railway.app/safety
Invoke-RestMethod https://ape-api-production.up.railway.app/db/status
Invoke-RestMethod https://ape-api-production.up.railway.app/ready
```

Expected behavior:

- `/storage/status` shows `enabled=true` from worker metadata after the worker has recorded a heartbeat.
- `latest_run_status` becomes `success` after the first retention pass.
- `table_stats` reports aggregate table counts/sizes without row contents or raw payloads.
- `storage_retention_runs` receives audit rows for retention attempts.
- Old `raw_payload` JSON is stripped before normalized rows expire.
- Old high-frequency rows are deleted in bounded batches.
- No endpoint can trigger deletion on request.
- No `VACUUM FULL` runs automatically. Postgres deletes free space for reuse, but physical disk size may not shrink immediately. Manual `VACUUM FULL` can lock tables and is outside this PR.

## Dry-Run Strategy Checkpoint After PR 9

Only after PR 9a is merged and market WebSocket, BRTI, strategy observer, and storage retention are healthy, update the Railway worker service:

```text
APP_MODE=DRY_RUN
STRATEGY_OBSERVER_ENABLED=true
STRATEGY_DRY_RUN_ENABLED=true
TRADING_ENABLED=false
EXECUTE=false
STRATEGY_ID=btc15_momentum_v1
STRATEGY_DRY_RUN_MAX_OPEN_POSITIONS=1
STRATEGY_DRY_RUN_ONE_ENTRY_PER_MARKET=true
STRATEGY_DRY_RUN_POSITION_SIZE_CONTRACTS=1
STRATEGY_DRY_RUN_ENTRY_PRICE_OFFSET_CENTS=1
STRATEGY_BRTI_LOOKBACK_SHORT_SECONDS=30
STRATEGY_BRTI_LOOKBACK_MEDIUM_SECONDS=90
STRATEGY_BRTI_LOOKBACK_LONG_SECONDS=180
STRATEGY_BRTI_MIN_MOVE_SHORT_BPS=2.0
STRATEGY_BRTI_MIN_MOVE_MEDIUM_BPS=4.5
STRATEGY_BRTI_MIN_MOVE_LONG_BPS=6.0
STRATEGY_BRTI_DIRECTIONAL_TICK_RATIO_MIN=0.62
STRATEGY_BRTI_MAX_BOUNDARY_CROSSES_90S=1
STRATEGY_BRTI_MAX_RETRACE_FRACTION=0.40
STRATEGY_CONTRACT_LOOKBACK_SECONDS=45
STRATEGY_CONTRACT_MIN_MID_MOVE_CENTS=4
STRATEGY_CONTRACT_ASK_PULLBACK_LOOKBACK_SECONDS=15
STRATEGY_CONTRACT_MAX_ASK_PULLBACK_CENTS=2
STRATEGY_TRADE_CONFIRMATION_LOOKBACK_SECONDS=30
STRATEGY_TRADE_CONFIRMATION_MIN_RATIO=0.60
STRATEGY_TRADE_CONFIRMATION_MIN_TRADES=3
STRATEGY_MIN_TOP_BOOK_SIZE_CONTRACTS=2
STRATEGY_DRY_RUN_MAX_ENTRY_PRICE=0.78
STRATEGY_DRY_RUN_MIN_ENTRY_PRICE=0.56
STRATEGY_REFERENCE_MAX_AGE_MS=2000
STRATEGY_REFERENCE_SOURCE_WARN_MS=10000
STRATEGY_REFERENCE_SOURCE_MAX_AGE_MS=45000
STRATEGY_REFERENCE_REQUIRE_TRADE_READY_FRESH=true
STRATEGY_REFERENCE_STREAM_MAX_AGE_MS=3000
STRATEGY_REFERENCE_CARRY_FORWARD_MAX_AGE_MS=15000
STRATEGY_REFERENCE_ALLOW_DUPLICATE_SOURCE_TS_CARRY_FORWARD=true
STRATEGY_KALSHI_BOOK_MAX_AGE_MS=2000
STRATEGY_KALSHI_BOOK_STREAM_MAX_AGE_MS=3000
STRATEGY_KALSHI_BOOK_CARRY_FORWARD_MAX_AGE_MS=30000
STRATEGY_KALSHI_BOOK_REQUIRE_STREAM_LIVE=true
```

To disable dry-run, set:

```text
STRATEGY_DRY_RUN_ENABLED=false
APP_MODE=OBSERVER
```

After enabling dry-run, redeploy the worker and validate:

```powershell
Invoke-RestMethod https://ape-api-production.up.railway.app/strategy/dry-run/status
Invoke-RestMethod https://ape-api-production.up.railway.app/strategy/dry-run/positions/open
Invoke-RestMethod "https://ape-api-production.up.railway.app/strategy/dry-run/positions/recent?limit=100"
Invoke-RestMethod "https://ape-api-production.up.railway.app/strategy/dry-run/events/recent?limit=100"
Invoke-RestMethod https://ape-api-production.up.railway.app/strategy/status
Invoke-RestMethod https://ape-api-production.up.railway.app/strategy/decisions/latest
Invoke-RestMethod "https://ape-api-production.up.railway.app/strategy/decisions/recent?limit=100"
Invoke-RestMethod "https://ape-api-production.up.railway.app/strategy/gates/recent?limit=100"
Invoke-RestMethod https://ape-api-production.up.railway.app/storage/status
Invoke-RestMethod https://ape-api-production.up.railway.app/ws/status
Invoke-RestMethod https://ape-api-production.up.railway.app/reference/brti/status
Invoke-RestMethod https://ape-api-production.up.railway.app/health
Invoke-RestMethod https://ape-api-production.up.railway.app/safety
Invoke-RestMethod https://ape-api-production.up.railway.app/db/status
Invoke-RestMethod https://ape-api-production.up.railway.app/ready
```

Expected behavior:

- `/strategy/dry-run/status` shows `enabled=true`, `app_mode=DRY_RUN`, `trading_enabled=false`, `execute=false`, and no safety blockers.
- `/strategy/gates/recent` summarizes recent decision gate outcomes by state, reason, and gate without raw payloads or execution controls.
- `ENTER_DRY_RUN` appears only when all safety, data-quality, timing, BRTI impulse, anti-chop, contract, and dry-run risk gates pass; too-few public trades are surfaced as a trade-confirmation warning.
- BRTI backend receipt age over `STRATEGY_REFERENCE_MAX_AGE_MS` blocks dry-run readiness; upstream source age over `STRATEGY_REFERENCE_SOURCE_WARN_MS` warns, and over `STRATEGY_REFERENCE_SOURCE_MAX_AGE_MS` blocks.
- Dry-run positions/events are hypothetical ledger rows only and contain no order IDs, client order IDs, account data, credentials, raw payloads, or execution controls.
- `ENTER_PAPER` and `ENTER_LIVE` must not appear.
- If dry-run is disabled or `APP_MODE=OBSERVER`, the evaluator should return observer/diagnostic states rather than `ENTER_DRY_RUN`.

## Explicitly Not Included

- Live trading
- Paper trading
- Order placement
- Real or paper strategy execution beyond dry-run simulation
- Private/user WebSocket subscriptions
- Vercel secrets or trading controls
- Railway cron

Railway production validation should happen after merge using GPT-provided commands.
