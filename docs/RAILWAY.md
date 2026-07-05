# Railway Deployment

PR 3 adds Railway deployment scaffolding for APE in observer-only mode.

APE should be deployed as two Railway services from the same GitHub repo:

- API service
- Always-on worker service

Railway Postgres should be attached to the project and should provide `DATABASE_URL`.

## Safety Defaults

Set these environment variables for both Railway services:

```text
APP_MODE=OBSERVER
TRADING_ENABLED=false
EXECUTE=false
ENV=railway
LOG_LEVEL=INFO
```

Do not add Kalshi credentials yet. Do not enable trading. Do not configure cron. Vercel is not part of PR 3.

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

The helper runs:

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

The worker is an always-on observer process. Do not configure a Railway cron job for it.

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

## Explicitly Not Included

- Kalshi credentials
- Live trading
- Paper trading
- Order placement
- Strategy execution
- Market ingestion
- BRTI ingestion
- Websocket collectors
- Vercel dashboard
- Railway cron

Railway production validation should happen after merge using GPT-provided commands.

