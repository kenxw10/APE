# APE

APE is a standalone BTC15 Kalshi Momentum Bot project. It is separate from BULL.

PR 1 created an observer-only Python foundation: configuration, startup safety checks, a FastAPI health API, an idle worker skeleton, tests, and project documentation.

PR 2 adds the database schema and repository foundation for future observer ingestion and dry-run decision storage. It does not ingest Kalshi/BRTI data, execute strategy logic, place orders, add dashboard code, or configure production deployment.

PR 3 adds Railway backend deployment scaffolding for the API and always-on worker. It remains observer-only.

PR 3a adds a root `requirements.txt` so Railway/Railpack installs APE's runtime Python dependencies before starting the API or worker.

PR 4 adds a Vercel-ready read-only dashboard scaffold under `dashboard/`. The dashboard uses the live Railway API for health, safety, database, and readiness state. Portfolio and positions sections remain clearly labeled placeholders until backend endpoints exist; PR 7a wires the CF/BRTI reference chart to the live read-only BRTI series when available.

PR 5 adds observer-only Kalshi REST authentication diagnostics and an active BTC15 market resolver. It can authenticate to Kalshi REST when Railway credentials are configured, resolve the currently active `KXBTC15M` market, store market metadata in the existing `markets` table, and expose safe read-only diagnostics.

PR 6 adds an observer-only Kalshi WebSocket market-data intake foundation for the Railway worker. It is disabled by default, subscribes only to public `ticker`, `orderbook_delta`, and `trade` channels for the active BTC15 market when enabled, stores normalized orderbook/trade data in existing tables, and exposes `/ws/status` diagnostics. It does not add BRTI/CF Benchmarks intake, strategy decisions, paper trading, live trading, or order placement.

PR 7 adds observer-only BRTI / CF Benchmarks reference-feed intake. It is disabled by default, subscribes to Kalshi's authenticated `cfbenchmarks_value` WebSocket channel for `index_ids=["BRTI"]` only on the Railway worker, stores safe reference ticks in the existing `reference_ticks` table, and exposes read-only `/reference/brti/status` and `/reference/brti/latest` diagnostics. PR 7a makes BRTI use a dedicated worker-owned WebSocket connection by default and adds `/reference/brti/series` for the read-only dashboard reference chart. It does not add strategy decisions, paper trading, live trading, orders, fills, private channels, or execution controls.

PR 8 adds an observer-only strategy decision ledger v0. It is disabled by default, runs only from persisted market/BRTI/orderbook/trade rows when enabled on the Railway worker, writes diagnostic rows to the existing `strategy_decisions` table, and exposes read-only `/strategy/status`, `/strategy/decisions/latest`, and `/strategy/decisions/recent` endpoints. It does not place orders, paper trade, live trade, create fills, use private channels, or add execution controls.

PR 8a adds worker-owned storage retention and read-only database lifecycle status. It is disabled by default, deletes old high-volume observer rows in bounded batches when enabled on the Railway worker, strips raw payload JSON before normalized rows expire, writes audit rows to `storage_retention_runs`, and exposes read-only `/storage/status`. It does not add a destructive public API endpoint and does not run `VACUUM FULL`.

PR 9 adds the first dry-run-only BTC15 momentum decision engine. It is disabled by default, runs only on persisted market/BRTI/orderbook/trade rows when `APP_MODE=DRY_RUN`, `STRATEGY_OBSERVER_ENABLED=true`, and `STRATEGY_DRY_RUN_ENABLED=true`, writes hypothetical simulated positions/events to dry-run ledger tables, and exposes read-only dry-run endpoints. It does not place orders, paper trade, live trade, read account balances, subscribe to private/user channels, or add execution controls.

PR 9c makes worker feed liveness component-scoped. Market WebSocket, BRTI, strategy, and storage retention now write separate worker heartbeat service names so strategy readiness no longer depends on whichever service most recently wrote the legacy aggregate `ape-worker` row.

## Safety Defaults

The default configuration is intentionally non-trading:

```powershell
APP_MODE=OBSERVER
TRADING_ENABLED=false
EXECUTE=false
```

Startup is blocked if:

- `APP_MODE` is anything other than `OBSERVER` or `DRY_RUN`
- `TRADING_ENABLED=true`
- `EXECUTE=true`

Kalshi credentials are not required for local health checks or tests.

`DATABASE_URL` is optional. If it is unset, the API and worker still start in observer-only mode.

If Kalshi credentials are missing, `/kalshi/status` and `/markets/active` return safe `not_configured` diagnostics instead of crashing.

`KALSHI_WS_ENABLED=false` by default. The worker only connects to Kalshi WebSocket when this is set to `true` on the Railway worker service.

`KALSHI_CFBENCHMARKS_ENABLED=false` by default. BRTI collection is worker-only and does not change trading safety.

`STRATEGY_OBSERVER_ENABLED=false` by default. The strategy observer is a decision ledger only; it records why the system would keep observing and never emits enter/order/execution actions.

`STRATEGY_DRY_RUN_ENABLED=false` by default. Dry-run simulation requires `APP_MODE=DRY_RUN`, `STRATEGY_OBSERVER_ENABLED=true`, `STRATEGY_DRY_RUN_ENABLED=true`, `TRADING_ENABLED=false`, and `EXECUTE=false`. Dry-run is a hypothetical ledger only and is not paper trading.

`STORAGE_RETENTION_ENABLED=false` by default. Retention is worker-only observer infrastructure; it deletes old persisted diagnostics and raw payload JSON only when explicitly enabled on the Railway worker.

## Local Setup

Run these commands from the repo root in Windows PowerShell:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Successful install output should end without red error text.

## Run Tests

```powershell
python -m pytest
```

Successful output should show all tests passing.

## Run Lint

```powershell
python -m ruff check .
```

Successful output should include `All checks passed!`.

## Run API Locally

```powershell
python -m ape.api.main
```

Then, in a second PowerShell window:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
Invoke-RestMethod http://127.0.0.1:8000/safety
Invoke-RestMethod http://127.0.0.1:8000/db/status
Invoke-RestMethod http://127.0.0.1:8000/ready
Invoke-RestMethod http://127.0.0.1:8000/kalshi/status
Invoke-RestMethod http://127.0.0.1:8000/markets/active
Invoke-RestMethod http://127.0.0.1:8000/ws/status
Invoke-RestMethod http://127.0.0.1:8000/reference/brti/status
Invoke-RestMethod http://127.0.0.1:8000/reference/brti/latest
Invoke-RestMethod "http://127.0.0.1:8000/reference/brti/series?window_seconds=900&max_points=16000"
Invoke-RestMethod http://127.0.0.1:8000/strategy/status
Invoke-RestMethod http://127.0.0.1:8000/strategy/decisions/latest
Invoke-RestMethod "http://127.0.0.1:8000/strategy/decisions/recent?limit=100"
Invoke-RestMethod "http://127.0.0.1:8000/strategy/gates/recent?limit=100"
Invoke-RestMethod http://127.0.0.1:8000/strategy/dry-run/status
Invoke-RestMethod http://127.0.0.1:8000/strategy/dry-run/positions/open
Invoke-RestMethod "http://127.0.0.1:8000/strategy/dry-run/positions/recent?limit=100"
Invoke-RestMethod "http://127.0.0.1:8000/strategy/dry-run/events/recent?limit=100"
Invoke-RestMethod http://127.0.0.1:8000/storage/status
```

Successful health output should report `status` as `ok`, `app_mode` as `OBSERVER`, and `is_safe` as `True`.

When `DATABASE_URL` is unset, `/db/status` should report `status` as `not_configured`.

When `DATABASE_URL` is unset, `/ready` should report `status` as `not_ready`. This is expected locally unless you configure a database.

When Kalshi credentials are unset, `/kalshi/status` should report `configured` as `False`, and `/markets/active` should report `state` as `not_configured`.

When `KALSHI_WS_ENABLED=false`, `/ws/status` should report `connection_state` as `disabled` and `stale` as `False`.

When `KALSHI_CFBENCHMARKS_ENABLED=false`, `/reference/brti/status` should report `connection_state` as `disabled` and `stale` as `False`.

When `STRATEGY_OBSERVER_ENABLED=false`, `/strategy/status` should report `connection_state` as `disabled` and `stale` as `False`.

When `STRATEGY_DRY_RUN_ENABLED=false`, `/strategy/dry-run/status` should report `enabled` as `False` and `open_position_count` as `0`.

When `STORAGE_RETENTION_ENABLED=false`, `/storage/status` should report retention as disabled while still returning read-only table stats if `DATABASE_URL` is configured.

## Kalshi REST Resolver

PR 5 is observer-only. It does not trade, paper trade, place orders, ingest WebSocket data, ingest BRTI, or run a strategy engine.

Optional Railway-only Kalshi settings:

```text
KALSHI_API_BASE_URL=https://external-api.kalshi.com/trade-api/v2
KALSHI_ENV=prod
KALSHI_API_KEY_ID=<Railway secret only>
KALSHI_PRIVATE_KEY=<Railway secret only>
KALSHI_BTC15_SERIES_TICKER=KXBTC15M
KALSHI_REST_TIMEOUT_SECONDS=10
KALSHI_RESOLVER_PARSER_VERSION=btc15_resolver_v1
```

Never put `KALSHI_API_KEY_ID` or `KALSHI_PRIVATE_KEY` in Vercel. The dashboard only needs the public Railway API URL.

## Kalshi WebSocket Collector

PR 6 is observer-only. The collector is owned by the Railway worker and remains disabled unless `KALSHI_WS_ENABLED=true`.

Optional Railway worker settings:

```text
KALSHI_WS_BASE_URL=wss://external-api-ws.kalshi.com/trade-api/ws/v2
KALSHI_WS_ENABLED=false
KALSHI_WS_CONNECT_TIMEOUT_SECONDS=10
KALSHI_WS_HEARTBEAT_TIMEOUT_SECONDS=30
KALSHI_WS_RECONNECT_SECONDS=5
KALSHI_WS_MAX_RECONNECT_SECONDS=60
KALSHI_WS_SUBSCRIBE_ORDERBOOK=true
KALSHI_WS_SUBSCRIBE_TICKER=true
KALSHI_WS_SUBSCRIBE_TRADES=true
```

After PR 6 is merged, enable the collector only on the Railway worker by setting `KALSHI_WS_ENABLED=true`. The API service may keep `KALSHI_WS_ENABLED=false`; `/ws/status` is derived from database rows and worker heartbeat metadata. Do not add these variables or Kalshi credentials to Vercel.

## BRTI Reference Feed

PR 7/7a is observer-only. The BRTI collector is owned by the Railway worker and remains disabled unless `KALSHI_CFBENCHMARKS_ENABLED=true`.

BRTI uses a dedicated authenticated WebSocket connection by default when enabled. The market WebSocket owns BTC15 `orderbook_delta`, `ticker`, and `trade`; the BRTI WebSocket owns only `cfbenchmarks_value` with `index_ids=["BRTI"]`. Market rollover should not disconnect BRTI, and BRTI errors should not kill market collection.

Optional Railway worker settings:

```text
KALSHI_CFBENCHMARKS_ENABLED=false
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
KALSHI_CFBENCHMARKS_FIRST_TICK_TIMEOUT_SECONDS=15
KALSHI_CFBENCHMARKS_NO_VALID_TICK_RECONNECT_SECONDS=15
KALSHI_CFBENCHMARKS_MAX_CONSECUTIVE_STALE_BEFORE_RECONNECT=2
KALSHI_CFBENCHMARKS_HEARTBEAT_STALE_AFTER_SECONDS=15
KALSHI_CFBENCHMARKS_STATUS_GRACE_SECONDS=3
KALSHI_CFBENCHMARKS_RECOVERY_REQUIRED_FRESH_TICKS=2
```

After PR 7a is merged, enable BRTI only on the Railway worker. Do not add Kalshi credentials, WebSocket settings, or BRTI env vars to Vercel. The API remains read-only and the dashboard only reads the public Railway API. `/reference/brti/series` returns BRTI points sorted by `received_at`, capped at 16,000 points, and excludes raw payloads; the dashboard renders those points in the current fixed Kalshi 15-minute interval. Source age is upstream CF timestamp lag; it remains visible but is separate from transport and persistence staleness. PR 8b adds status categories, worker heartbeat age, stale reasons, recovery counters, and bounded BRTI reconnects when the worker is subscribed but no valid tick arrives. `trade_ready_fresh` is a future strategy gate and is not used for trading in PR 7a/8b. If Kalshi sends the final-minute 15-minute average, APE stores it for diagnostics only; no position-management, strategy, or trading logic uses it in PR 7a/8b.

## Strategy Observer Ledger

PR 8 is observer-only. The strategy observer reads only persisted active market metadata, BRTI ticks, Kalshi orderbook snapshots, and public trades from the database. It never calls Kalshi REST, never subscribes to private channels, and never places or simulates orders.

Optional Railway worker settings:

```text
STRATEGY_OBSERVER_ENABLED=false
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

After PR 8 is merged and market/BRTI WebSocket intake is healthy, enable the ledger only on the Railway worker with `STRATEGY_OBSERVER_ENABLED=true`. Keep `APP_MODE=OBSERVER`, `TRADING_ENABLED=false`, and `EXECUTE=false`. Do not add strategy observer env vars to Vercel.

PR 9b separates event-driven feed liveness from value-change persistence. `STRATEGY_REFERENCE_MAX_AGE_MS` and `STRATEGY_KALSHI_BOOK_MAX_AGE_MS` remain preferred fresh-update thresholds. The stream max-age settings prove the WebSocket is still active, and the carry-forward caps bound how long unchanged BRTI/orderbook values may be reused for dry-run readiness. True stream failures, active-ticker mismatches, sequence resets, missing sides, crossed books, persistence failures, or carry-forward cap breaches still block. This does not tune strategy thresholds and does not add paper/live/order/private-channel behavior.

PR 9c changes the liveness source of truth, not the thresholds. The worker writes these component heartbeat names:

```text
ape-worker.market_ws
ape-worker.reference_brti
ape-worker.strategy
ape-worker.storage_retention
```

The legacy `ape-worker` aggregate row remains for backward compatibility. `/ws/status`, `/reference/brti/status`, and strategy readiness prefer the component row and fall back to the aggregate only when no component row exists; fallback responses include `feed_liveness_legacy_aggregate_fallback`. Strategy decision measurements expose `market_liveness_source`, `reference_liveness_source`, component heartbeat ages, stream ages, and liveness reasons so stale dry-run blockers can be tied to the actual feed component.

The read-only endpoints are:

```text
/strategy/status
/strategy/decisions/latest
/strategy/decisions/recent?limit=100
```

Expected behavior: the latest decision state is one of the observer-safe diagnostic states such as `OBSERVE_ONLY_MARKET`, `REFERENCE_STALE`, `KALSHI_STALE`, or `TOO_CLOSE_TO_BOUNDARY`. There are no enter, fill, order, paper-trading, or live-trading states.

## Dry-Run Strategy Engine

PR 9 is dry-run only. It upgrades the strategy observer from a skip ledger to a momentum evaluator that can emit `ENTER_DRY_RUN`, `MANAGE_POSITION`, `EXIT_SIGNAL`, and `FORCE_EXIT` simulation states only when all safety and strategy gates pass. PR 9a stabilizes dry-run trade readiness by separating fresh backend BRTI receipt age from upstream source age, preserving warning-only source lag, and recording per-gate pass/warn/block diagnostics.

Required Railway worker settings for dry-run validation:

```text
APP_MODE=DRY_RUN
STRATEGY_OBSERVER_ENABLED=true
STRATEGY_DRY_RUN_ENABLED=true
TRADING_ENABLED=false
EXECUTE=false
```

Dry-run remains disabled unless both `APP_MODE=DRY_RUN` and `STRATEGY_DRY_RUN_ENABLED=true` are set. To disable it, set:

```text
STRATEGY_DRY_RUN_ENABLED=false
APP_MODE=OBSERVER
```

Dry-run differs from paper trading: APE creates only hypothetical database ledger rows in `strategy_dry_run_positions` and `strategy_dry_run_events`. It does not place orders, read balances, consume private/user streams, create real positions, cancel orders, or call Kalshi order APIs.

Read-only dry-run endpoints:

```text
/strategy/dry-run/status
/strategy/dry-run/positions/open
/strategy/dry-run/positions/recent?limit=100
/strategy/dry-run/events/recent?limit=100
/strategy/gates/recent?limit=100
```

The evaluator checks safety, active market, boundary parsing, BRTI freshness, Kalshi book freshness, entry timing, boundary distance, spread/depth, BRTI impulse, anti-chop, contract confirmation, recent public-trade confirmation, and dry-run risk limits. Backend BRTI receipt age is the hard freshness gate for strategy use; upstream source age above `STRATEGY_REFERENCE_SOURCE_WARN_MS` is a warning until it exceeds `STRATEGY_REFERENCE_SOURCE_MAX_AGE_MS`. Too-few public trades are reported as a trade-confirmation warning instead of silently hiding the rest of the gate result. Observer mode still stops at `OBSERVE_ONLY_MARKET`; PR 9/9a never emits `ENTER_PAPER` or `ENTER_LIVE`.

## Storage Retention

PR 8a is observer infrastructure for Railway Postgres lifecycle control. Retention is disabled by default and should be enabled only on the Railway worker after merge.

Recommended Railway worker settings:

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
STORAGE_RETENTION_DRY_RUN_POSITIONS_SECONDS=2592000
STORAGE_RETENTION_DRY_RUN_EVENTS_SECONDS=2592000
STORAGE_RETENTION_MARKETS_SECONDS=2592000
STORAGE_RETENTION_RAW_PAYLOAD_ORDERBOOK_SECONDS=900
STORAGE_RETENTION_RAW_PAYLOAD_PUBLIC_TRADES_SECONDS=3600
STORAGE_RETENTION_RAW_PAYLOAD_REFERENCE_TICKS_SECONDS=3600
STORAGE_RETENTION_STATUS_WARN_BYTES=40000000000
STORAGE_RETENTION_STATUS_CRITICAL_BYTES=47500000000
```

Retention deletes old `orderbook_snapshots`, `public_trades`, `reference_ticks`, `worker_heartbeats`, `strategy_decisions`, dry-run ledger rows, and old closed `markets` rows in bounded batches. It strips `raw_payload` JSON from orderbook, public trade, and reference tick rows earlier than it deletes the normalized row, while preserving `raw_payload_hash` and parsed fields. Strategy decisions, dry-run ledger rows, and markets are retained longer because they are lower volume and useful for audit.

The read-only endpoint is:

```text
/storage/status
```

It returns aggregate table stats, latest retention-run audit information, warning/critical database-size status, and retention config summary. It does not expose raw payload contents, secrets, or any delete controls.

Postgres deletes do not immediately shrink physical disk usage. Normal autovacuum should make freed table space reusable. A manual `VACUUM FULL` can shrink files but locks tables and is intentionally out of scope; APE never runs `VACUUM FULL` automatically.

## Database Setup

PR 2 uses SQLAlchemy for the schema and repository layer. Railway Postgres is the production direction for a later PR, but local tests use SQLite so you do not need to install Postgres manually.

To create a local SQLite database for development:

```powershell
$env:DATABASE_URL="sqlite+pysqlite:///./local-ape.sqlite"
python -m ape.db.migrations
```

Successful output should say the database schema is current. The command does not print the database URL. PR 8a adds the `storage_retention_runs` audit table. PR 9 adds the `strategy_dry_run_positions` and `strategy_dry_run_events` simulation ledger tables.

## Railway Deployment

PR 3 adds Railway deployment helper scripts and documentation for two Railway services from this repo:

- API service: `python -m scripts.railway_start_api`
- Worker service: `python -m scripts.railway_start_worker`

The API and worker commands run database migrations before startup. PostgreSQL migrations are serialized with an advisory transaction lock so simultaneous Railway restarts do not race on schema changes. Railway Postgres should provide `DATABASE_URL` in deployment. See [docs/RAILWAY.md](docs/RAILWAY.md) before configuring Railway.

Railway/Railpack uses the root `requirements.txt` for runtime dependency installation. If deploy logs show missing modules such as `sqlalchemy`, verify `requirements.txt` includes the runtime dependencies from `pyproject.toml`.

## Vercel Dashboard

PR 4 adds a Next.js dashboard app in `dashboard/`.

Run locally from the dashboard folder:

```powershell
cd dashboard
npm install
$env:NEXT_PUBLIC_API_BASE_URL="https://ape-api-production.up.railway.app"
npm run dev
```

Deploy on Vercel with `dashboard` as the project root/build directory and set:

```text
NEXT_PUBLIC_API_BASE_URL=https://ape-api-production.up.railway.app
```

Do not add `DATABASE_URL`, Kalshi credentials, private keys, or trading secrets to Vercel. See [docs/VERCEL.md](docs/VERCEL.md).

The Reference Price CF/BRTI chart reads `/reference/brti/series` from the public Railway API when live data is available. It remains read-only, renders the current fixed Kalshi 15-minute interval, keeps the interval-open value fixed to the first valid BRTI tick at or after interval start, caps the chart at 16,000 points, and falls back to clearly labeled scaffold data when the backend series is unavailable.

## Run Worker Locally

```powershell
python -m ape.worker.main
```

Successful startup should log that the worker is running in observer mode. Stop it with `Ctrl+C`.

## Intentionally Not Included Yet

- Live trading
- Paper trading
- Kalshi order placement
- Order executor
- Kalshi WebSocket ingestion beyond observer-only public market/reference capture
- Real or paper trading strategy execution beyond dry-run simulation
- CF Benchmarks/BRTI REST intake
- Real dashboard portfolio/ledger endpoints
- Railway cron
- GitHub Actions
- Real secrets
