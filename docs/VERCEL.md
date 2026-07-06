# Vercel Dashboard Setup

PR 4 adds a Vercel-ready Next.js dashboard under `dashboard/`.

PR 4a adds `dashboard/vercel.json` so Vercel has repository-controlled instructions for the dashboard build path and clears stale static output-directory overrides.

## Create the Vercel Project

1. Create a new Vercel project from `https://github.com/kenxw10/APE`.
2. Set the project root/build directory to `dashboard`.
3. Framework Preset should be `Next.js`.
4. Build Command should be `npm run build`.
5. Install Command should be `npm install`.
6. Output Directory should stay blank / Next.js default in the Vercel UI. The repo config sets `outputDirectory` to `null` so Vercel auto-detects the Next.js output instead of using any stale static folder override.
7. Set this environment variable:

```text
NEXT_PUBLIC_API_BASE_URL=https://ape-api-production.up.railway.app
```

## Do Not Add Secrets

Do not add these to Vercel:

- `DATABASE_URL`
- Kalshi credentials
- Kalshi private keys
- Railway database credentials
- Any trading or execution secrets

The dashboard is read-only and only needs the public Railway API base URL.

PR 5 Kalshi REST credentials are Railway-only. Do not add `KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY`, or `KALSHI_API_BASE_URL` to Vercel.

PR 6 Kalshi WebSocket intake is Railway-worker-only. Do not add `KALSHI_WS_ENABLED`, `KALSHI_WS_BASE_URL`, Kalshi credentials, or any WebSocket secrets/settings to Vercel. The dashboard only reads the public Railway API `/ws/status` response.

PR 7a BRTI / CF Benchmarks intake is Railway-worker-only. Do not add `KALSHI_CFBENCHMARKS_ENABLED`, `KALSHI_CFBENCHMARKS_INDEX_IDS`, WebSocket settings, Kalshi credentials, or any BRTI-related secrets/settings to Vercel. The dashboard only reads the public Railway API `/reference/brti/status` and `/reference/brti/series` responses.

## Expected Build Logs

The dashboard build includes a marker that proves Vercel is running from the `dashboard` app:

```text
Running "install" command: npm install
APE_DASHBOARD_BUILD_PATH_CONFIRMED
> @ape/dashboard@0.1.0 build
> next build
Compiled successfully
```

If logs show this without `npm install`, `APE_DASHBOARD_BUILD_PATH_CONFIRMED`, or `next build`, Vercel did not run the dashboard build path:

```text
Build Completed in /vercel/output [70ms]
```

In that case, re-check that the Vercel project Root Directory is `dashboard`. Also confirm the deployed commit includes `dashboard/vercel.json` with `outputDirectory` set to `null`.

## CORS

PR 4 fetches the Railway API from the Next.js server side. Browser-side CORS is not required.

If a future PR moves status fetching into browser-side requests, add a minimal Railway backend allowlist such as `CORS_ALLOWED_ORIGINS=<deployed Vercel origin>`. Do not use a wildcard production CORS policy.

## Validate After Deploy

Open the Vercel dashboard and confirm:

- Header shows API connected when Railway is reachable.
- Header shows DB ready when `/db/status` is `ok`.
- Source/Data panels show Kalshi WebSocket status from `/ws/status` when the Railway API is reachable.
- Source/Data panels show BRTI status from `/reference/brti/status` when the Railway API is reachable.
- Reference Price CF/BRTI chart uses live `/reference/brti/series` data when available.
- Reference Price CF/BRTI chart shows only a rolling 15-minute window and a 16,000-point maximum.
- If live BRTI series data is unavailable, the chart is clearly labeled as fallback/scaffold.
- Safety panel shows `Mode: OBSERVER`.
- Safety panel shows `Trading: DISABLED`.
- Safety panel shows `Execute: FALSE`.
- Portfolio and positions sections are labeled as scaffold placeholders.

You can also verify the Railway API directly:

```powershell
Invoke-RestMethod https://ape-api-production.up.railway.app/health
Invoke-RestMethod https://ape-api-production.up.railway.app/safety
Invoke-RestMethod https://ape-api-production.up.railway.app/db/status
Invoke-RestMethod https://ape-api-production.up.railway.app/ready
```

Successful output should show observer-only safety and ready database status.
