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

Calibration partitions whole BTC15 markets chronologically. The final 20 percent is
one frozen holdout; the development set has five chronological folds with adjacent
market purge. Fewer than 50 complete markets produces `INSUFFICIENT_DATA` and no
candidate can move beyond `DRAFT`.

The bounded search includes candidate zero, deterministic weighted-heuristic
variants, and NumPy-only L2 logistic candidates. Model selection uses market-level
2,000-resample bootstrap intervals. Governance may only advance database state from
`DRAFT` to `BACKTESTED`, `SHADOW`, and then `DRY_RUN_CHALLENGER`; paper and live
transitions raise errors.

`STRATEGY_V2_CANDIDATE_CONFIG_VERSION_ID` is optional and unset by default. When
an operator pins an immutable approved candidate, the strategy worker evaluates it
as a separate DRY_RUN-only variant. Missing, retired, incompatible, or checksum-bad
pins fail closed for that candidate only and never replace the control baseline.

All research APIs are read-only and bounded. No research route can promote, activate,
trade, inspect an account, access private feeds, or expose raw payloads.

Candidate pins are revalidated on every strategy-observer evaluation. Changing,
switching, or retiring a candidate takes effect on the next observer tick without a
worker restart: an invalid or replaced pin cancels pending candidate ENTRY intents
and force-exits existing candidate positions while leaving the newly resolved
candidate and control variants unchanged.

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
