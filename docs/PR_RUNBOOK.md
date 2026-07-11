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
  `STORAGE_RETENTION_STRATEGY_DECISIONS_SECONDS=1209600`,
  `STORAGE_RETENTION_KALSHI_WS_PROTOCOL_EVENTS_SECONDS=21600`, and
  `STORAGE_RETENTION_MARKETS_SECONDS=2592000`.
- Use the documented raw-payload windows:
  `STORAGE_RETENTION_RAW_PAYLOAD_ORDERBOOK_SECONDS=900`,
  `STORAGE_RETENTION_RAW_PAYLOAD_PUBLIC_TRADES_SECONDS=3600`, and
  `STORAGE_RETENTION_RAW_PAYLOAD_REFERENCE_TICKS_SECONDS=3600`.
- Redeploy the Railway worker.
- Validate worker logs, `/storage/status`, `/ws/status`,
  `/reference/brti/status`, `/strategy/status`, `/health`, `/safety`,
  `/db/status`, and `/ready`.
- Confirm `/storage/status` reports `liveness_source=component`,
  `worker_role=maintenance`, `latest_component_heartbeat_mode=storage_retention`,
  `worker_heartbeat_stale=false`, `retention_config.effective_enabled=true`, and
  `retention_config.configured_enabled` matching the API process config.
- Confirm `storage_retention_runs` rows are written and latest status becomes
  `success` or `success_partial` with no blockers.
- Treat `success_partial` as acceptable incremental progress only when it is
  caused by the configured time, table, or per-table row budget and row totals continue moving on
  later runs.
- Confirm latest totals, processed/skipped tables, budget-exhausted flag, and
  DB timeout/error counters are present in `/storage/status`.
- Confirm `/storage/status` returns aggregate table stats and no row contents,
  raw payloads, credentials, signatures, private keys, headers, or delete
  controls.
- Confirm old raw payload JSON is stripped before normalized rows expire.
- Confirm old high-frequency rows are deleted in bounded batches.
- Keep default smoothing unless production catch-up needs stricter pacing:
  `STORAGE_RETENTION_INTER_TABLE_SLEEP_MS=100`,
  `STORAGE_RETENTION_BATCH_SLEEP_MS=50`, and optional unset caps for
  `STORAGE_RETENTION_MAX_TABLES_PER_RUN` and
  `STORAGE_RETENTION_MAX_DELETE_ROWS_PER_TABLE`.
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

PR 9i post-merge checkpoint:

- Keep the existing DRY_RUN safety settings on `ape-strategy-worker` only:
  `APP_MODE=DRY_RUN`, `STRATEGY_OBSERVER_ENABLED=true`,
  `STRATEGY_DRY_RUN_ENABLED=true`, `TRADING_ENABLED=false`, and
  `EXECUTE=false`.
- Add only `STRATEGY_CHALLENGER_ENABLED=true` to `ape-strategy-worker` to
  enable the challenger. Do not add it to API, market, reference, maintenance,
  Railway Postgres, or Vercel services.
- Confirm the control remains `btc15_momentum_v1` with 30/90/180-second BRTI
  lookbacks and a 45-second contract lookback. Confirm the challenger is
  `btc15_momentum_v1_fast` with 20/60/120-second BRTI lookbacks and a
  30-second contract lookback; all other thresholds remain the same.
- Validate `/strategy/status` for worker-owned `variants` metadata and
  `/strategy/variants/comparison?window_seconds=3600` for bounded counts.
  Use `strategy_id=btc15_momentum_v1` or
  `strategy_id=btc15_momentum_v1_fast` on decision, gate, and dry-run routes
  to inspect one ledger.
- Verify the raw desired-side ask range remains `$0.56` through `$0.78`; an
  ask of `$0.78` is eligible with intended entry clamped to `$0.78`, while a
  raw ask outside the range blocks. Inspect `measurements.gate_trace` for the
  canonical reason and later analysis-only gate results.
- Confirm both variants write hypothetical rows only. There must be no orders,
  cancels, private/user channels, account reads, balance reads, paper/live
  positions, credentials, or dashboard trading controls.

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
- Confirm `/ws/status.liveness_source=component` from `ape-worker.market_data`
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

PR 9d post-merge checkpoint:

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
- Confirm `/ws/status` and `/strategy/decisions/latest.measurements` expose
  `market_feed_transport_state`, `market_feed_subscription_state`,
  `market_feed_snapshot_state`, `market_feed_active_ticker_state`,
  `market_feed_sequence_state`, `market_data_quiet`,
  `market_data_quiet_age_ms`, `orderbook_snapshot_age_ms`,
  `orderbook_snapshot_source`, and `orderbook_recovery_action`.
- Confirm quiet market data with healthy transport, active subscription,
  initialized snapshot, matching ticker, clean sequence, and in-cap book age
  produces `kalshi_orderbook_data_quiet_carried_forward` as a warning instead
  of `KALSHI_STALE`.
- Confirm true market feed failures still block, including stale transport,
  inactive subscription, active ticker mismatch, missing snapshot, sequence
  gap/reset, invalid orderbook updates, snapshot resync failure, and
  carry-forward cap breach.
- Confirm market rollover records `market_roll_reresolve`, requires a fresh
  snapshot for the new ticker before strategy uses the book, and then recovers
  through a fresh or resynced snapshot.
- Confirm `/reference/brti/status` exposes `brti_reference_transport_alive`,
  `brti_reference_last_valid_message_age_ms`,
  `brti_reference_no_valid_tick_timeout`, and
  `brti_reference_reconnect_requested`. BRTI silence beyond the valid-tick
  timeout should remain `REFERENCE_STALE` until recovered.
- Confirm this PR did not tune strategy thresholds and did not add paper/live,
  order, fill, private-channel, account, executor, or dashboard control
  behavior.

PR 9e post-merge checkpoint:

- Keep `APP_MODE=DRY_RUN`, `STRATEGY_OBSERVER_ENABLED=true`,
  `STRATEGY_DRY_RUN_ENABLED=true`, `TRADING_ENABLED=false`, and `EXECUTE=false`
  on the Railway worker only.
- Redeploy the Railway worker and validate `/ws/status`, `/strategy/status`,
  `/strategy/decisions/latest`, `/strategy/decisions/recent`,
  `/strategy/gates/recent`, `/reference/brti/status`, `/storage/status`,
  `/health`, `/safety`, `/db/status`, and `/ready`.
- Confirm `/ws/status` and strategy decision measurements expose
  `market_feed_state`, `market_subscription_recovery_count`,
  `market_subscription_recovery_last_reason`,
  `market_subscription_recovery_last_action`,
  `market_subscription_recovery_last_result`,
  `market_subscription_recovery_last_at`, `market_snapshot_resync_count`,
  `market_snapshot_resync_last_result`, `market_rollover_recovery_count`,
  `market_transport_reconnect_count`, `market_unrecovered_blocker_count`,
  `market_recovery_attempt_in_progress`, and
  `market_recovery_attempt_age_ms`.
- Confirm transient `kalshi_orderbook_subscription_inactive` conditions become
  bounded recovery states such as
  `kalshi_orderbook_subscription_recovery_pending` or
  `kalshi_orderbook_snapshot_resync_pending` before they become hard blockers.
- Confirm failed subscribe ACK waits, failed snapshot resync sends, or repeated
  unrecovered subscription errors escalate to market WebSocket reconnects and
  explicit blockers such as `kalshi_orderbook_subscription_recovery_failed`.
- Confirm market rollover records a rollover recovery state, clears the old
  runtime book, and requires a fresh or resynced snapshot for the new BTC15
  ticker before the strategy reuses the orderbook.
- PowerShell validation loops must use `${field}` when a variable is followed
  by a colon. Use this form:

```powershell
foreach ($field in $requiredWsFields) {
  $finalWsField = $finalWs.$field
  Write-Host "final_ws_field_${field}: $finalWsField"
}
```

- Confirm this PR did not tune strategy thresholds and did not add paper/live,
  order, fill, private-channel, account, executor, credential, or dashboard
  control behavior.

PR 9f post-merge checkpoint:

- Replace the old production all-in-one worker with dedicated Railway services:
  `ape-market-worker`, `ape-reference-worker`, `ape-strategy-worker`, and
  `ape-maintenance-worker`.
- Keep the API read-only. The API service must not run market WebSocket, BRTI,
  strategy, or storage retention loops.
- Use these start commands:

```text
python -m ape.worker --role market-data
python -m ape.worker --role reference-brti
python -m ape.worker --role strategy
python -m ape.worker --role maintenance
```

- Set matching `APE_WORKER_ROLE` values on the four worker services:
  `market-data`, `reference-brti`, `strategy`, and `maintenance`.
- Keep `TRADING_ENABLED=false` and `EXECUTE=false` on every service.
- Keep `APP_MODE=DRY_RUN`, `STRATEGY_OBSERVER_ENABLED=true`, and
  `STRATEGY_DRY_RUN_ENABLED=true` only on the strategy worker for dry-run
  validation.
- Do not add paper/live controls, order placement, private channels, account
  reads, executor code, Kalshi credentials, or worker-loop env vars to Vercel.
- Run or confirm database migrations before starting the split workers.
- First deploy only `ape-market-worker` and let it run for at least 30 minutes
  before enabling strategy validation. This proves market WebSocket stability
  without BRTI, strategy, or retention sharing the event loop.
- Validate market worker endpoints:

```powershell
Invoke-RestMethod https://ape-api-production.up.railway.app/ws/status
Invoke-RestMethod "https://ape-api-production.up.railway.app/ws/protocol/recent?limit=200"
Invoke-RestMethod "https://ape-api-production.up.railway.app/ws/protocol/summary?window_seconds=1800"
Invoke-RestMethod https://ape-api-production.up.railway.app/markets/active
Invoke-RestMethod https://ape-api-production.up.railway.app/health
Invoke-RestMethod https://ape-api-production.up.railway.app/safety
Invoke-RestMethod https://ape-api-production.up.railway.app/db/status
Invoke-RestMethod https://ape-api-production.up.railway.app/ready
```

- Confirm `/ws/status.worker_role=market-data`,
  `liveness_source=component`, `subscription_reconciled=true`,
  `orderbook_sid_confirmed=true`, and a recent `connection_id`.
- Confirm `/ws/status` exposes `last_list_subscriptions_at`,
  `last_list_subscriptions_result`, in-flight snapshot fields, DB writer queue
  depth/age/flush metrics, recent protocol error count, reconnect reason, and
  close code/reason fields.
- Confirm `/ws/protocol/recent` shows subscribe, list-subscriptions,
  orderbook/ticker/trade, ping/pong, reconnect, close, and error evidence
  without secrets or full raw payloads.
- Confirm `/ws/protocol/summary` has no repeated subscribe/list/get-snapshot
  error pattern, and that `market_feed_state=SUBSCRIBING` or
  `BLOCKED_UNRECOVERED` does not dominate the 30-minute window.
- Confirm `get_snapshot` is not sent until a confirmed active orderbook SID is
  known, and duplicate in-flight snapshot refreshes are suppressed.
- Confirm market rollover creates a new connection/subscription/snapshot proof
  before strategy uses the new ticker.
- Confirm DB writer queue backlog stays bounded; if
  `db_writer_queue_depth` grows or old queue age crosses the configured limit,
  strategy should block instead of using stale market data.
- After market worker validation passes, deploy `ape-reference-worker`, then
  validate `/reference/brti/status`, `/reference/brti/latest`, and
  `/reference/brti/series` as in PR 7a.
- After market and reference workers are healthy, deploy `ape-strategy-worker`
  and validate `/strategy/status`, `/strategy/decisions/latest`,
  `/strategy/decisions/recent`, `/strategy/gates/recent`,
  `/strategy/dry-run/status`, `/strategy/dry-run/positions/open`,
  `/strategy/dry-run/positions/recent`, and
  `/strategy/dry-run/events/recent`.
- Confirm strategy decisions block on missing or stale market worker heartbeat,
  unreconciled subscriptions, unconfirmed orderbook SID, stale snapshot beyond
  the carry-forward cap, timed-out in-flight snapshot requests, DB writer
  backlog, or recent protocol errors.
- Deploy `ape-maintenance-worker` last and validate `/storage/status`.
- Confirm this PR did not tune strategy thresholds and did not add paper/live,
  order, fill, private-channel, account, executor, credential, or dashboard
  control behavior.

PR 9g post-merge checkpoint:

- Keep only `ape-market-worker` enabled until market-worker persistence is
  validated. Do not proceed to reference, strategy, maintenance, replay, paper
  ledger, dashboard polish, or strategy tuning until this checkpoint passes.
- Use the market worker command:

```text
python -m ape.worker --role market-data
```

- Keep the market worker environment market-only:
  `APE_WORKER_ROLE=market-data`, `APP_MODE=OBSERVER`,
  `TRADING_ENABLED=false`, `EXECUTE=false`, database vars, Kalshi REST vars,
  and Kalshi public market WebSocket vars. Do not add BRTI, strategy, storage
  retention, private-channel, account, order, or execution variables.
- No env changes are required before PR 9g unless optional tuning is needed.
  Defaults are:
  `MARKET_DB_WRITER_CRITICAL_QUEUE_MAX_SIZE=2000`,
  `MARKET_DB_WRITER_DIAGNOSTIC_QUEUE_MAX_SIZE=5000`,
  `MARKET_DB_WRITER_FLUSH_INTERVAL_MS=250`,
  `MARKET_DB_WRITER_MAX_BATCH_SIZE=500`,
  `MARKET_DB_WRITER_MAX_FLUSH_MS=1000`,
  `MARKET_ORDERBOOK_SNAPSHOT_MIN_INTERVAL_MS=250`,
  `MARKET_PROTOCOL_EVENT_SAMPLE_RATE=0.02`,
  `MARKET_PROTOCOL_EVENT_ERROR_SAMPLE_RATE=1.0`,
  `MARKET_PROTOCOL_EVENT_MAX_PER_FLUSH=100`,
  `MARKET_DB_WRITER_BACKPRESSURE_WARN_DEPTH=750`,
  `MARKET_DB_WRITER_BACKPRESSURE_BLOCK_DEPTH=1500`, and
  `MARKET_DB_WRITER_BACKPRESSURE_MAX_AGE_MS=10000`.
- Validate market worker endpoints:

```powershell
$ws = Invoke-RestMethod https://ape-api-production.up.railway.app/ws/status
$recent = Invoke-RestMethod "https://ape-api-production.up.railway.app/ws/protocol/recent?limit=200"
$summary = Invoke-RestMethod "https://ape-api-production.up.railway.app/ws/protocol/summary?window_seconds=1800"
$ws
$summary
```

- Pass criteria:
  `/ws/status.worker_role=market-data`,
  `connection_state=subscribed`,
  `market_feed_state=LIVE` except bounded rollover windows,
  `subscription_reconciled=true`,
  `orderbook_sid_confirmed=true`,
  `market_feed_transport_state=healthy`,
  `market_feed_subscription_state=subscribed`,
  critical queue depth below `MARKET_DB_WRITER_BACKPRESSURE_BLOCK_DEPTH`,
  critical queue oldest age below `MARKET_DB_WRITER_BACKPRESSURE_MAX_AGE_MS`,
  and `latest_state_persisted_age_ms` below the critical backpressure age.
- Diagnostic queue backlog, `protocol_events_sampled_out`, and
  `protocol_events_dropped_backpressure` may increase under load, but they must
  not produce readiness blockers.
- `QUIET_CARRY_FORWARD` is acceptable when transport is healthy, the active
  subscription is reconciled, there is no unrecovered blocker, and the snapshot
  remains inside the hard carry-forward cap.
- `BLOCKED_UNRECOVERED`, stale market transport, stale BRTI transport, or
  `/reference/brti/status.status_category=stale_transport` are hard
  regressions.
- `protocol_event_recent_error_count` should stay low unless actual close,
  websocket error, subscription error, list-subscriptions error,
  update-subscription error, get-snapshot error, or reconnect failure events
  occurred.
- There should be no repeated hard blockers named
  `market_db_writer_queue_backpressure`, `orderbook_persistence_pending`,
  `market_critical_persistence_backpressure`, or
  `market_critical_persistence_failed`. If the critical blockers appear, treat
  them as a real persistence failure and do not enable strategy validation.
- `/ws/protocol/recent` and `/ws/protocol/summary` must not expose secrets,
  signatures, private keys, headers, full raw payloads, private-channel data,
  account data, or order/execution controls.
- Confirm this PR did not tune strategy thresholds and did not add paper/live,
  order, fill, private-channel, account, executor, credential, or dashboard
  control behavior.

PR 9h post-merge checkpoint:

- Keep API, `ape-market-worker`, `ape-reference-worker`, and
  `ape-maintenance-worker` running. Do not deploy or enable strategy until
  market, reference, and maintenance validation are clean.
- Use the maintenance worker command:

```text
python -m ape.worker --role maintenance
```

- Keep the maintenance worker environment maintenance-only:
  `APE_WORKER_ROLE=maintenance`, `APP_MODE=OBSERVER`, `TRADING_ENABLED=false`,
  `EXECUTE=false`, database vars, and `STORAGE_RETENTION_ENABLED=true`. Do not
  add Kalshi WebSocket, BRTI, strategy, private-channel, account, order, or
  execution variables.
- Validate:

```powershell
$storage = Invoke-RestMethod https://ape-api-production.up.railway.app/storage/status
$ws = Invoke-RestMethod https://ape-api-production.up.railway.app/ws/status
$brti = Invoke-RestMethod https://ape-api-production.up.railway.app/reference/brti/status
$safety = Invoke-RestMethod https://ape-api-production.up.railway.app/safety
$storage
$ws
$brti
$safety
```

- Pass criteria: `/storage/status.liveness_source=component`,
  `worker_role=maintenance`, `latest_component_heartbeat_mode=storage_retention`,
  `worker_heartbeat_stale=false`, `retention_config.effective_enabled=true`,
  latest run `success` or `success_partial`, no blockers, and no DB statement,
  lock, or generic DB error count.
- Confirm market status remains live or acceptable `QUIET_CARRY_FORWARD` under
  healthy transport/subscription conditions. `BLOCKED_UNRECOVERED`, stale
  market transport, and BRTI `stale_transport` remain hard regressions.
- Confirm `/safety` still reports `trading_enabled=false`, `execute=false`, and
  no unsafe live/paper behavior.
