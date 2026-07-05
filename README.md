# APE

APE is a standalone BTC15 Kalshi Momentum Bot project. It is separate from BULL.

PR 1 creates an observer-only Python foundation: configuration, startup safety checks, a FastAPI health API, an idle worker skeleton, tests, and project documentation. It does not implement trading, paper trading, strategy execution, database persistence, dashboard code, or production deployment.

## Safety Defaults

The default configuration is intentionally non-trading:

```powershell
APP_MODE=OBSERVER
TRADING_ENABLED=false
EXECUTE=false
```

In PR 1, startup is blocked if:

- `APP_MODE` is anything other than `OBSERVER`
- `TRADING_ENABLED=true`
- `EXECUTE=true`

Kalshi credentials are not required for local health checks or tests.

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
```

Successful health output should report `status` as `ok`, `app_mode` as `OBSERVER`, and `is_safe` as `True`.

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
- Database schema or repositories
- Vercel dashboard
- Railway deployment config
- GitHub Actions
- Real secrets
