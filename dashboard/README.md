# APE Dashboard

This is the Vercel-ready read-only dashboard scaffold for APE.

It reads the live Railway observer API for operational status:

- `/health`
- `/safety`
- `/db/status`
- `/ready`
- `/ws/status`
- `/reference/brti/status`
- `/reference/brti/series`

Portfolio and position sections are scaffold placeholders until backend endpoints exist. The Reference Price CF/BRTI chart reads live `/reference/brti/series` data when available, polls once per second, shows only the rolling 15-minute window, and caps rendering at 16,000 points. If the backend series is unavailable, it falls back to clearly labeled scaffold data. This is not live trading data.

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

`npm run build` should print this marker before `next build`:

```text
APE_DASHBOARD_BUILD_PATH_CONFIRMED
```

## Environment

Required:

```text
NEXT_PUBLIC_API_BASE_URL=https://ape-api-production.up.railway.app
```

Do not add secrets to the dashboard. Do not add `DATABASE_URL`, Kalshi credentials, private keys, WebSocket settings, or BRTI env vars.
Kalshi WebSocket and BRTI collection are Railway-worker-only; the dashboard only reads public Railway API status and series endpoints.

## Current Scaffold Limits

- No live trading.
- No paper trading.
- No Kalshi client.
- No websocket collectors.
- No order placement.
- No portfolio ledger endpoint yet.
- No raw CF/BRTI payload endpoint.
- No real open or closed positions endpoint yet.

The dashboard fetches Railway API status server-side, so browser CORS is not required for PR 4.

## Vercel Deployment Troubleshooting

This app includes `vercel.json` so Vercel should run the dashboard install and build from the `dashboard` directory.

The repo config sets `outputDirectory` to `null` to clear stale Vercel UI overrides such as `public` and let Vercel auto-detect the Next.js output.

Expected Vercel logs should include:

```text
Running "install" command: npm install
APE_DASHBOARD_BUILD_PATH_CONFIRMED
> @ape/dashboard@0.1.0 build
> next build
Compiled successfully
```

Keep Vercel configured with:

- Root Directory: `dashboard`
- Framework Preset: `Next.js`
- Build Command: `npm run build`
- Install Command: `npm install`
- Output Directory: blank / Next.js default in the Vercel UI

If logs show `Build Completed in /vercel/output [70ms]` without npm install or next build, Vercel did not run the dashboard build path.
