# Vercel Dashboard Setup

PR 4 adds a Vercel-ready Next.js dashboard under `dashboard/`.

## Create the Vercel Project

1. Create a new Vercel project from `https://github.com/kenxw10/APE`.
2. Set the project root/build directory to `dashboard`.
3. Use the default Next.js build settings.
4. Set this environment variable:

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

## CORS

PR 4 fetches the Railway API from the Next.js server side. Browser-side CORS is not required.

If a future PR moves status fetching into browser-side requests, add a minimal Railway backend allowlist such as `CORS_ALLOWED_ORIGINS=<deployed Vercel origin>`. Do not use a wildcard production CORS policy.

## Validate After Deploy

Open the Vercel dashboard and confirm:

- Header shows API connected when Railway is reachable.
- Header shows DB ready when `/db/status` is `ok`.
- Safety panel shows `Mode: OBSERVER`.
- Safety panel shows `Trading: DISABLED`.
- Safety panel shows `Execute: FALSE`.
- Portfolio, reference, and positions sections are labeled as scaffold placeholders.

You can also verify the Railway API directly:

```powershell
Invoke-RestMethod https://ape-api-production.up.railway.app/health
Invoke-RestMethod https://ape-api-production.up.railway.app/safety
Invoke-RestMethod https://ape-api-production.up.railway.app/db/status
Invoke-RestMethod https://ape-api-production.up.railway.app/ready
```

Successful output should show observer-only safety and ready database status.
