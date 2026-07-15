# Research and Calibration

PR 11 adds a database-only research path for the existing DRY_RUN V2 strategy.
It archives normalized public observations before short raw-payload retention,
labels complete market evidence, replays events in deterministic event-time order,
and records the exact blocker funnel when no entries occur.

The active baseline is not automatically loosened. Zero entries are a
strategy-frequency warning and are classified as `ZERO_ENTRY_UNVALIDATABLE` when
there are exactly zero replayed entries. This is not healthy selectivity.

Replay uses the same V2 feature-vector evaluator as production. It exposes an
event only after its ordered event time, uses a 500 ms entry delay and two-second
window, and attempts exactly the first in-window book for both entry and exit.
Later books cannot rescue an ineligible first attempt. The replay fee model records
the verified general Kalshi taker schedule source and calculation metadata.

Governed calibration uses a strict current-version clean cohort rather than the
mixed all-history baseline denominator. Eligible markets require resolved outcomes,
archived MARKET/REFERENCE/ORDERBOOK/FEATURE evidence, FULL current-version candidate
features, mature net 30-second labels, and the causal first executable book between
500 ms and 2.5 seconds after the feature. Every exclusion is counted explicitly.

Calibration partitions whole eligible BTC15 markets chronologically. The final 20
percent is one frozen holdout; the development set has five chronological folds with
adjacent market purge. Fewer than 50 eligible markets produces
`INSUFFICIENT_CLEAN_DATA`. Completed immutable epochs use the first 50, 100, 150,
and subsequent groups of 50 eligible markets. Tail growth between boundaries reuses
the completed result, while code or search identity changes create a new run.

The bounded search includes candidate zero, deterministic weighted-heuristic
variants, and NumPy-only L2 logistic candidates. Model selection uses market-level
2,000-resample bootstrap intervals. PR 11f generates candidates only as `DRAFT` /
`RESEARCH_ONLY`. It does not call governance advancement, activate a candidate, or
alter the control strategy. Paper and live transitions remain prohibited.

`STRATEGY_V2_CANDIDATE_CONFIG_VERSION_ID` is optional and unset by default. When
an operator pins an immutable approved candidate, the strategy worker evaluates it
as a separate DRY_RUN-only variant. Missing, retired, incompatible, or checksum-bad
pins fail closed for that candidate only and never replace the control baseline.

All research APIs are read-only and bounded. No research route can promote, activate,
trade, inspect an account, access private feeds, or expose raw payloads.

## Runtime Recovery

PR 11a keeps the existing research semantics but makes archive work resumable. The
research worker commits at most 250 source events per archive batch, writes a fresh
worker heartbeat before work and after each committed batch, and records the current
stage through archive, label refresh, coverage, baseline replay, and calibration.
If a database stage fails, the error heartbeat is committed from a separate session;
already committed archive batches and baseline replay evidence remain available for
the next cycle. Error metadata is bounded and sanitized, including a statement-timeout
indicator, and never includes connection strings, SQL parameters, or raw payloads.

Candidate pins resolve once when the strategy worker starts. The running process does
not hot reload candidate configuration. Database or environment changes require a
worker restart. At startup, the resolved candidate or startup blocker is compared with
persisted candidate state: stale pending ENTRY intents are cancelled, stale open
positions are force-managed, and a replacement candidate cannot enter until stale
state drains. The control variants remain unchanged.

## Bounded Runtime (PR 11b)

The research worker freezes replay input after the archive and label stages by
recording the highest archived event ID, its count, and time bounds. Coverage and
baseline replay scan exactly that snapshot through 250-row keyset pages, so rows
written after the watermark wait for the next cycle. The worker reports the
watermark, scan count, completed pages, partitions, and label progress in its
heartbeat and `/research/status`.

Mature outcome labels process at most 25 current-schema markets per cycle. Each
market reads only its own interval plus the 65-second label horizon. A remaining
label backlog is reported as a partial cycle and coverage/replay are deferred rather
than reported complete. Calibration stays disabled by default. PR 11f replaces
the former 20,000-event full-archive materialization gate with a strict-cohort keyset
reader. Database pages remain capped at 250 and are released between pages. Candidate
replay receives only compact FEATURE_SNAPSHOT and ORDERBOOK evidence for the immutable
epoch under the frozen watermark. Candidate states run in fixed batches of eight; L2
logistic fitting has a fixed 100,000-row compact feature-matrix cap and fails closed
above it.

## Clean-Cohort Governed Calibration (PR 11f)

The all-history baseline remains unchanged and diagnostic. It retains deterministic
event ordering, the frozen watermark, the current feature evaluator, 500 ms latency,
the two-second intent window, first-book-only entry/exit semantics, the verified fee
model, and zero-entry blocker evidence. It is not the governed strategy-frequency
denominator.

The clean-cohort manifest records the frozen watermark and cohort hash, ordered
eligible markets, source/version distributions, explicit feature and market
exclusions, source completeness, event gaps and time bounds, resolved-outcome hash,
baseline config version, and code commit. Each 50-market epoch has its own immutable
epoch hash and complete 256-candidate search-space snapshot/hash.

Candidate batches commit durable progress and partition-attributed replay trades.
An interrupted worker resumes after completed candidates without repeating the
holdout. Only the selected finalist receives development-test and frozen-holdout
evaluation. Results use one exact deterministic classification:
`INSUFFICIENT_CLEAN_DATA`, `NO_CANDIDATE_SIGNALS`,
`SIGNALS_WITHOUT_EXECUTABLE_FILLS`, `FILLS_WITHOUT_CLOSED_TRADES`,
`CLOSED_TRADES_WITHOUT_POSITIVE_HOLDOUT`, `POSITIVE_RESEARCH_CANDIDATE`,
`CALIBRATION_BLOCKED`, or `CALIBRATION_FAILED`.

`/research/cohorts/latest`, `/research/calibration/frontier/latest?limit=20`, and
`/research/status` are bounded and read-only. They expose compact identities,
progress, source/exclusion counts, frontier evidence, economic summaries, and the
next-experiment marker without raw payloads or unbounded distributions.

PR 11f adds no migration, service, required environment variable, timeout, polling,
archive batch-size, archive operation-budget, retention, strategy-control, or safety
change. Keep `APP_MODE=DRY_RUN`, `CALIBRATION_ENABLED=false`,
`TRADING_ENABLED=false`, and `EXECUTE=false` until separate production-validation
instructions explicitly enable calibration.

## Post-Bootstrap Fair Scheduling (PR 11e)

While any append-only cursor is uninitialized, in bootstrap verification, incomplete,
or missing a valid durable state, the worker reports `BOOTSTRAP_STRICT` and keeps the
existing canonical source order and 20-operation gate. Association, labels, coverage,
replay, and calibration remain fail-closed until all six cursors are valid TAIL
cursors. Mutable `markets` remains outside the bootstrap-completion predicate.

After bootstrap, the worker reports `TAIL_FAIR`. Each bounded archive cycle first gives
every pending canonical source one opportunity, then uses remaining operations in
deterministic canonical round-robin passes. Each operation remains capped at 250 rows
and the total remains capped at 20; continuously pending `public_trades` therefore
cannot starve feature, intent, or outcome sources.

Pending TAIL rows after the 20-operation slice are normal backlog, not a bootstrap
blocker. The worker records `archive_tail_pending_after_budget` and continues through
reference association, labels, frozen coverage, baseline replay, and optional
calibration. `/research/status` and the research heartbeat expose the scheduling mode,
bootstrap/tail budget flags, served sources, per-source operation counts,
`post_archive_allowed`, and any deferred bootstrap reason. No migration, environment
variable, Railway service, timeout, polling, batch-size, or operation-budget setting
changes.

## Calibration Evidence

Every bounded calibration run stores the complete immutable search-space snapshot:
the deterministic seed, generation algorithm version, all grids, candidate IDs and
parameter hashes, logistic feature order/L2/convergence settings, exact allowed
candidate paths, protected gates, frequency governance, and snapshot SHA-256.

Each candidate stores its own training, walk-forward validation, bootstrap, and
penalty evidence. Only the selected finalist receives development-test and frozen
holdout metrics. Candidate replay trades are persisted with an explicit partition;
promotion uses the declared immutable out-of-sample partition and de-duplicates by
candidate, market, source decision, entry event, and partition.

Promotion evidence counts only actual `FEATURE_SNAPSHOT` rows that have a candidate
side, mature label horizon, required first-book window, and resolved official outcome.
It records missing sources and real per-market/per-source event gaps separately from
coverage counts. A manifest market is not treated as complete merely because it was
listed in the partition manifest.
