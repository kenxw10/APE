# APE Dashboard

This is the Vercel-ready read-only dashboard scaffold for APE.

It reads the live Railway observer API for operational status:

- `/health`
- `/safety`
- `/db/status`
- `/ready`

Portfolio, CF/BRTI reference, and position sections are scaffold placeholders until backend endpoints exist. They are labeled in the UI and are not live trading data.

## Local Development

Run from `dashboard` in Windows PowerShell:

```powershell
npm install
$env:NEXT_PUBLIC_API_BASE_URL="https://ape-api-production.up.railway.app"
npm run dev
```

Successful startup should show a local Next.js URL, usually `http://localhost:3000`.

## Build

```powershell
npm run typecheck
npm run lint
npm run test
npm run build
```

Successful output should end without TypeScript, ESLint, test, or Next.js build errors.

## Environment

Required:

```text
NEXT_PUBLIC_API_BASE_URL=https://ape-api-production.up.railway.app
```

Do not add secrets to the dashboard. Do not add `DATABASE_URL`, Kalshi credentials, or private keys.

## Current Scaffold Limits

- No live trading.
- No paper trading.
- No Kalshi client.
- No BRTI ingestion.
- No websocket collectors.
- No order placement.
- No portfolio ledger endpoint yet.
- No real CF/BRTI reference endpoint yet.
- No real open or closed positions endpoint yet.

The dashboard fetches Railway API status server-side, so browser CORS is not required for PR 4.
