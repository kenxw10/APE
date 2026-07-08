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
- Kalshi WebSocket PRs may add observer-only public ticker/orderbook/trade capture only when explicitly scoped; they must not add private user channels, order placement, paper trading, strategy decisions, or execution.
- BRTI/CF Benchmarks PRs may add observer-only reference-feed capture only when explicitly scoped; they must not add strategy decisions, paper trading, live trading, order placement, private/user channels, or execution controls.
- Dry-run strategy PRs may add hypothetical ledger simulation only when explicitly scoped; they must not add paper trading, live trading, Kalshi order placement, account reads, private/user channels, or execution controls.
- Database schema/repository changes are allowed only in PRs that explicitly authorize storage or ledger work.
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
- Worker startup runs database migrations before `ape.worker.main`; if migrations
  fail, the worker must not start. API-first deploy order is still preferred for
  schema-changing PRs, but both API and worker startup helpers are migration-safe.
  PostgreSQL migrations are serialized with an advisory transaction lock so API
  and worker restarts do not race on `ALTER TABLE` or schema-version writes.
- `/ws/status.last_error_type` and `/ws/status.last_error_message` mean a current
  unresolved worker error. If current orderbook/trade rows are being written and
  warnings/blockers are empty, old recovered database errors should be null there.
- `diagnostic_samples` in `/ws/status` may appear only as bounded shape metadata
  for invalid orderbook/trade payloads; it must not expose credentials, signatures,
  private keys, headers, or full raw WebSocket payloads.
- Dashboard validation should use the `Kalshi WS` / `WS Channels` status or a
  successful `/ws/status` response. Do not require literal browser WebSocket access
  from the Vercel dashboard.

PR 7a post-merge checkpoint:

- Set `KALSHI_CFBENCHMARKS_ENABLED=true` on the Railway worker only.
- Set `KALSHI_CFBENCHMARKS_INDEX_IDS=BRTI` on the Railway worker only.
- Keep `KALSHI_CFBENCHMARKS_SUBSCRIBE_ON_WORKER=true`.
- Keep `KALSHI_CFBENCHMARKS_DEDICATED_CONNECTION=true`.
- Keep API and worker `APP_MODE=OBSERVER`, `TRADING_ENABLED=false`, and `EXECUTE=false`.
- Do not add BRTI env vars, Kalshi credentials, or WebSocket settings to Vercel.
- Redeploy the Railway worker.
- Validate worker logs, `/reference/brti/status`, `/reference/brti/latest`,
  `/reference/brti/series`, `/ws/status`,
  `/health`, `/safety`, `/db/status`, and `/ready`.
- Confirm `reference_ticks` rows are being written when Kalshi emits BRTI
  `cfbenchmarks_value` events.
- Confirm `/reference/brti/status` reports `status_category=healthy`,
  `transport_stale=false`, `persistence_stale=false`,
  `worker_heartbeat_stale=false`, no blockers, and null unresolved error fields
  when live BRTI rows are arriving. `source_stale` may be true if upstream CF
  timestamps lag; that is visible but not a global collector failure by itself.
- Confirm stale BRTI states include `stale_reason`, `stale_age_ms`,
  `recovery_state`, `recommended_action`, and worker heartbeat age fields.
- Confirm `/reference/brti/series` returns a bounded 900-second, 16,000-point
  maximum series sorted by `received_at` and does not return raw payloads.
- Confirm the dashboard Reference Price CF/BRTI chart uses the current fixed
  Kalshi 15-minute interval, keeps Interval Open fixed to the first valid BRTI
  tick at or after interval start, and resets at the next interval boundary.
- BRTI final-minute averages may be stored when present, but no strategy,
  position-management, paper trading, live trading, order, fill, or decision-ledger
  logic is enabled by PR 7a.

PR 8 post-merge checkpoint:

- Set `STRATEGY_OBSERVER_ENABLED=true` on the Railway worker only after PR 7a
  market/BRTI intake is healthy.
- Keep API and worker `APP_MODE=OBSERVER`, `TRADING_ENABLED=false`, and
  `EXECUTE=false`.
- Do not add strategy observer env vars, Kalshi credentials, WebSocket settings,
  or BRTI env vars to Vercel.
- Redeploy the Railway worker.
- Validate worker logs, `/strategy/status`, `/strategy/decisions/latest`,
  `/strategy/decisions/recent`, `/ws/status`, `/reference/brti/status`,
  `/health`, `/safety`, `/db/status`, and `/ready`.
- Confirm `strategy_decisions` rows are being written when active persisted market,
  BRTI, and orderbook rows are fresh enough for evaluation.
- Confirm latest strategy states are observer-safe diagnostics only. They may
  include `OBSERVE_ONLY_MARKET`, `REFERENCE_STALE`, `KALSHI_STALE`, `TOO_EARLY`,
  `TOO_LATE_FOR_ENTRY`, `TOO_CLOSE_TO_BOUNDARY`, `BOOK_UNUSABLE`, or
  `CONTRACT_NOT_CONFIRMED`; they must not include enter, order, fill, paper, or
  live execution states.
- Confirm the dashboard Engine Status panel shows Strategy Observer state from
  `/strategy/status` and remains read-only.

PR 8a post-merge checkpoint:

- Set `STORAGE_RETENTION_ENABLED=true` on the Railway worker only after
  migrations and `/storage/status` are healthy.
- Keep API and worker `APP_MODE=OBSERVER`, `TRADING_ENABLED=false`, and
  `EXECUTE=false`.
- Do not add storage retention env vars, database credentials, Kalshi
  credentials, WebSocket settings, BRTI env vars, or strategy observer env vars
  to Vercel.
- Use the documented worker retention windows:
  `STORAGE_RETENTION_ORDERBOOK_SECONDS=7200`,
  `STORAGE_RETENTION_PUBLIC_TRADES_SECONDS=86400`,
  `STORAGE_RETENTION_REFERENCE_TICKS_SECONDS=86400`,
  `STORAGE_RETENTION_WORKER_HEARTBEATS_SECONDS=21600`,
  `STORAGE_RETENTION_STRATEGY_DECISIONS_SECONDS=1209600`, and
  `STORAGE_RETENTION_MARKETS_SECONDS=2592000`.
- Use the documented raw-payload windows:
  `STORAGE_RETENTION_RAW_PAYLOAD_ORDERBOOK_SECONDS=900`,
  `STORAGE_RETENTION_RAW_PAYLOAD_PUBLIC_TRADES_SECONDS=3600`, and
  `STORAGE_RETENTION_RAW_PAYLOAD_REFERENCE_TICKS_SECONDS=3600`.
- Redeploy the Railway worker.
- Validate worker logs, `/storage/status`, `/ws/status`,
  `/reference/brti/status`, `/strategy/status`, `/health`, `/safety`,
  `/db/status`, and `/ready`.
- Confirm `storage_retention_runs` rows are written and latest status becomes
  `success`.
- Confirm `/storage/status` returns aggregate table stats and no row contents,
  raw payloads, credentials, signatures, private keys, headers, or delete
  controls.
- Confirm old raw payload JSON is stripped before normalized rows expire.
- Confirm old high-frequency rows are deleted in bounded batches.
- Do not run automatic `VACUUM FULL`. PostgreSQL deletes make space reusable
  but may not immediately reduce Railway physical disk usage; manual
  `VACUUM FULL` can lock tables and is outside PR 8a.

PR 9a post-merge checkpoint:

- Set `APP_MODE=DRY_RUN` on the Railway worker only after market WebSocket,
  BRTI, strategy observer, and storage retention are healthy.
- Set `STRATEGY_OBSERVER_ENABLED=true` and `STRATEGY_DRY_RUN_ENABLED=true`
  on the Railway worker only.
- Keep `TRADING_ENABLED=false` and `EXECUTE=false`.
- Keep API and dashboard read-only; do not add dry-run controls, Kalshi
  credentials, WebSocket settings, BRTI env vars, strategy env vars, or storage
  retention env vars to Vercel.
- Redeploy the Railway worker.
- Validate worker logs, `/strategy/dry-run/status`,
  `/strategy/dry-run/positions/open`,
  `/strategy/dry-run/positions/recent`, `/strategy/dry-run/events/recent`,
  `/strategy/status`, `/strategy/decisions/latest`,
  `/strategy/decisions/recent`, `/strategy/gates/recent`, `/storage/status`, `/ws/status`,
  `/reference/brti/status`, `/reference/brti/latest`,
  `/reference/brti/series`, `/health`, `/safety`, `/db/status`, and `/ready`.
- Confirm dry-run rows are hypothetical only and contain no order IDs, client
  order IDs, account data, credentials, raw payloads, private-channel data, or
  execution controls.
- Confirm `ENTER_DRY_RUN` appears only in explicit DRY_RUN mode with dry-run
  enabled and all gates passed.
- Confirm `/strategy/gates/recent` exposes pass/warn/block summaries for recent
  dry-run readiness checks, including BRTI source-age warnings and public-trade
  sample-size warnings, without raw payloads or execution controls.
- Confirm `ENTER_PAPER` and `ENTER_LIVE` do not appear.

PR 9b post-merge checkpoint:

- Keep `APP_MODE=DRY_RUN`, `STRATEGY_OBSERVER_ENABLED=true`,
  `STRATEGY_DRY_RUN_ENABLED=true`, `TRADING_ENABLED=false`, and `EXECUTE=false`
  on the Railway worker only.
- Keep API and dashboard read-only; do not add dry-run controls, paper/live
  controls, order placement, private channels, account reads, or credentials to
  Vercel.
- Redeploy the Railway worker after setting the PR 9b liveness variables:
  `STRATEGY_REFERENCE_STREAM_MAX_AGE_MS=3000`,
  `STRATEGY_REFERENCE_CARRY_FORWARD_MAX_AGE_MS=15000`,
  `STRATEGY_REFERENCE_ALLOW_DUPLICATE_SOURCE_TS_CARRY_FORWARD=true`,
  `STRATEGY_KALSHI_BOOK_STREAM_MAX_AGE_MS=3000`,
  `STRATEGY_KALSHI_BOOK_CARRY_FORWARD_MAX_AGE_MS=30000`, and
  `STRATEGY_KALSHI_BOOK_REQUIRE_STREAM_LIVE=true`.
- Validate `/strategy/status`, `/strategy/decisions/latest`,
  `/strategy/decisions/recent`, `/strategy/gates/recent`, `/ws/status`, and
  `/reference/brti/status`.
- Confirm unchanged orderbooks and unchanged valid BRTI source timestamps show
  explicit carry-forward warnings only while stream liveness is proven and the
  hard caps are not exceeded.
- Confirm stale blockers such as `kalshi_orderbook_stream_stale`,
  `kalshi_orderbook_sequence_gap_or_reset`,
  `kalshi_orderbook_active_ticker_mismatch`,
  `kalshi_orderbook_carry_forward_age_exceeds_limit`,
  `brti_reference_stream_stale`, `brti_reference_transport_stale`,
  `brti_reference_persistence_stale`,
  `brti_reference_worker_heartbeat_stale`, and
  `brti_reference_carry_forward_age_exceeds_limit` only appear for true
  feed/liveness failures.
- Confirm this PR did not tune strategy thresholds and did not add paper/live,
  order, fill, private-channel, account, executor, or dashboard control
  behavior.

PR 9c post-merge checkpoint:

- Keep `APP_MODE=DRY_RUN`, `STRATEGY_OBSERVER_ENABLED=true`,
  `STRATEGY_DRY_RUN_ENABLED=true`, `TRADING_ENABLED=false`, and `EXECUTE=false`
  on the Railway worker only.
- Keep API and dashboard read-only; do not add dry-run controls, paper/live
  controls, order placement, private channels, account reads, or credentials to
  Vercel.
- Redeploy the Railway worker.
- Validate `/ws/status`, `/reference/brti/status`, `/strategy/status`,
  `/strategy/decisions/latest`, `/strategy/decisions/recent`,
  `/strategy/gates/recent`, `/storage/status`, `/health`, `/safety`,
  `/db/status`, and `/ready`.
- Confirm `/ws/status.liveness_source=component` from `ape-worker.market_ws`
  and `/reference/brti/status.liveness_source=component` from
  `ape-worker.reference_brti` after fresh worker heartbeats are written.
- Confirm `/strategy/decisions/latest.measurements` includes
  `market_liveness_source=component`, `reference_liveness_source=component`,
  recent component heartbeat ages, `orderbook_stream_age_ms`,
  `brti_reference_stream_age_ms`, and no
  `feed_liveness_legacy_aggregate_fallback` warning.
- Confirm stale blockers such as `kalshi_orderbook_stream_stale` and
  `brti_reference_stream_stale` only appear when the corresponding component
  heartbeat or stream timestamp is actually stale, not because strategy/storage
  wrote the latest legacy aggregate heartbeat.
- Confirm this PR did not tune strategy thresholds and did not add paper/live,
  order, fill, private-channel, account, executor, or dashboard control
  behavior.
