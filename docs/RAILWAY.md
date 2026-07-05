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

The API helper is the automatic migration owner. It runs:

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
```

`/ready` should return `ready` only when safety is safe and database connectivity works.

## Worker Service

Set the worker service start command:

```text
python -m scripts.railway_start_worker
```

The helper runs:

```text
python -m ape.worker.main
```

The worker helper does not run migrations. This avoids API and worker services racing each other on a fresh Railway Postgres database. The worker is an always-on observer process. Do not configure a Railway cron job for it.

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

## Explicitly Not Included

- Live trading
- Paper trading
- Order placement
- Strategy execution
- Market ingestion loops
- BRTI ingestion
- Websocket collectors
- Vercel dashboard
- Railway cron

Railway production validation should happen after merge using GPT-provided commands.
