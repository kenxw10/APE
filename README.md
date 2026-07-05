# APE

APE is a standalone BTC15 Kalshi Momentum Bot project. It is separate from BULL.

PR 1 created an observer-only Python foundation: configuration, startup safety checks, a FastAPI health API, an idle worker skeleton, tests, and project documentation.

PR 2 adds the database schema and repository foundation for future observer ingestion and dry-run decision storage. It does not ingest Kalshi/BRTI data, execute strategy logic, place orders, add dashboard code, or configure production deployment.

PR 3 adds Railway backend deployment scaffolding for the API and always-on worker. It remains observer-only.

PR 3a adds a root `requirements.txt` so Railway/Railpack installs APE's runtime Python dependencies before starting the API or worker.

PR 4 adds a Vercel-ready read-only dashboard scaffold under `dashboard/`. The dashboard uses the live Railway API for health, safety, database, and readiness state. Portfolio, CF/BRTI reference, and positions sections are clearly labeled placeholders until backend endpoints exist.

PR 5 adds observer-only Kalshi REST authentication diagnostics and an active BTC15 market resolver. It can authenticate to Kalshi REST when Railway credentials are configured, resolve the currently active `KXBTC15M` market, store market metadata in the existing `markets` table, and expose safe read-only diagnostics.

## Safety Defaults

The default configuration is intentionally non-trading:

```powershell
APP_MODE=OBSERVER
TRADING_ENABLED=false
EXECUTE=false
```

Startup is blocked if:

- `APP_MODE` is anything other than `OBSERVER`
- `TRADING_ENABLED=true`
- `EXECUTE=true`

Kalshi credentials are not required for local health checks or tests.

`DATABASE_URL` is optional. If it is unset, the API and worker still start in observer-only mode.

If Kalshi credentials are missing, `/kalshi/status` and `/markets/active` return safe `not_configured` diagnostics instead of crashing.

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
```

Successful health output should report `status` as `ok`, `app_mode` as `OBSERVER`, and `is_safe` as `True`.

When `DATABASE_URL` is unset, `/db/status` should report `status` as `not_configured`.

When `DATABASE_URL` is unset, `/ready` should report `status` as `not_ready`. This is expected locally unless you configure a database.

When Kalshi credentials are unset, `/kalshi/status` should report `configured` as `False`, and `/markets/active` should report `state` as `not_configured`.

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

## Database Setup

PR 2 uses SQLAlchemy for the schema and repository layer. Railway Postgres is the production direction for a later PR, but local tests use SQLite so you do not need to install Postgres manually.

To create a local SQLite database for development:

```powershell
$env:DATABASE_URL="sqlite+pysqlite:///./local-ape.sqlite"
python -m ape.db.migrations
```

Successful output should say the database schema is current. The command does not print the database URL.

## Railway Deployment

PR 3 adds Railway deployment helper scripts and documentation for two Railway services from this repo:

- API service: `python -m scripts.railway_start_api`
- Worker service: `python -m scripts.railway_start_worker`

The API command runs database migrations before API startup. The worker command starts the always-on observer worker directly so both services do not race on migrations. Railway Postgres should provide `DATABASE_URL` in deployment. See [docs/RAILWAY.md](docs/RAILWAY.md) before configuring Railway.

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
- Strategy decision engine
- Kalshi WebSocket ingestion
- BRTI/reference ingestion
- Websocket collectors
- Real dashboard portfolio/ledger endpoints
- Real CF/BRTI reference data endpoint
- Railway cron
- GitHub Actions
- Real secrets
