# PR Runbook

This is the intended GPT/Codex workflow for APE.

1. GPT gives one bounded PR prompt.
2. Codex implements the scoped change and opens/pushes the PR.
3. Kenneth reviews and merges the PR.
4. GPT provides Windows PowerShell validation commands.
5. Kenneth runs the commands and pastes the output.
6. GPT evaluates pass/fail, updates Notion, and only then generates the next PR prompt.

Rules:

- Keep each PR bounded.
- Do not skip validation.
- Do not move to the next PR until the current PR has been reviewed, merged, and validated.
- Do not introduce live trading, paper trading, execution, secrets, external market data calls, or deployment behavior unless that PR explicitly authorizes it.
- Kalshi REST resolver PRs may add read-only authenticated diagnostics only when explicitly scoped; they must not add order placement, paper trading, strategy decisions, WebSocket ingestion, or BRTI ingestion.
- Kalshi WebSocket PRs may add observer-only public ticker/orderbook/trade capture only when explicitly scoped; they must not add BRTI/CF Benchmarks, private user channels, order placement, paper trading, strategy decisions, or execution.
- Database schema/repository changes are allowed only in PRs that explicitly authorize storage work.
- Railway worker services should be always-on processes, not cron jobs, unless a later PR explicitly changes that decision.

PR 5 post-merge checkpoint:

- Add Kalshi credentials only to Railway API and worker env.
- Do not add Kalshi credentials to Vercel.
- Keep `APP_MODE=OBSERVER`, `TRADING_ENABLED=false`, and `EXECUTE=false`.
- Redeploy Railway API and worker.
- Validate `/kalshi/status`, `/markets/active`, `/health`, `/safety`, `/db/status`, and `/ready`.

PR 6 post-merge checkpoint:

- Set `KALSHI_WS_ENABLED=true` on the Railway worker only.
- Keep API and worker `APP_MODE=OBSERVER`, `TRADING_ENABLED=false`, and `EXECUTE=false`.
- Do not add Kalshi credentials or WebSocket settings to Vercel.
- Redeploy the Railway worker.
- Validate worker logs, `/ws/status`, `/health`, `/safety`, `/db/status`, `/ready`, `/kalshi/status`, and `/markets/active`.
- Confirm `orderbook_snapshots` rows are being written. `public_trades` may be sparse.
- PR 6b live WebSocket validation passes when `/ws/status` shows `enabled=true`,
  `connection_state=subscribed`, a non-null recent `latest_orderbook_received_at`,
  no blockers, and no old broad parse warnings such as `invalid_trade_price_or_size`.
- `diagnostic_samples` in `/ws/status` may appear only as bounded shape metadata
  for invalid orderbook/trade payloads; it must not expose credentials, signatures,
  private keys, headers, or full raw WebSocket payloads.
- Dashboard validation should use the `Kalshi WS` / `WS Channels` status or a
  successful `/ws/status` response. Do not require literal browser WebSocket access
  from the Vercel dashboard.
