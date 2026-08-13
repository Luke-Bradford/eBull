# Forward strategy outcomes — validated implementation (#2474)

Status: implemented and regression-tested on branch `feature/2474-forward-outcomes`.

## Decision

The shadow track record is a two-stage pipeline:

1. `strategy_signal_scan` records each causal decision and its next-open fill.
2. `strategy_outcome_resolution` revisits current-version, level-based fills and
   records the first terminal result supported by the live fail-closed corpus.

The resolver runs daily at 06:55 UTC, after the 06:45 signal scan and before
07:05 retention. Both jobs share the `strategy_scan` source lock.

## Maturity and ambiguity

`window_truncated` means the future has not happened yet. It is not written to
`strategy_outcomes`; the fill remains pending and is retried after more bars
arrive. A decisive target/stop, expiry, masked required field, series boundary,
or daily both-touch is terminal and is stored once at
`(signal_id, outcome_rule_set_version, quarantine_rule_set_version)`.

A both-touch daily bar remains `ambiguous`, has no synthetic price or return,
and is excluded from resolved-performance counts. The live gate counts only
outcomes carrying `gross_return_pct`, rather than treating any audit row as a
measured result.

## Corpus and causality

The job uses the same `load_masked_bars` path, current quarantine version,
survivor-only universe label and manifest-owned strategy identity as the live
scan. Indicator state and holding windows are bounded by unresolved
`price_series_break` transitions. Brackets are produced from the signal's own
price segment and from values available at signal/fill time; the resolver never
derives strategy parameters.

Only strategies with a manifest-owned `exit_levels` factory are eligible. At
implementation time this is S-4. Signal-pair and calendar-exit strategies need
their own forward position resolver before their shadow returns can be treated
as comparable evidence.

## Bounded storage and work

- Pending selection is capped at 1,000 fills per run and advances through signal
  IDs with a round-robin cursor, so permanently immature/delisted instruments
  cannot starve later resolvable fills.
- Bars are loaded once per instrument in the selected batch.
- No heartbeat or pending-window rows are appended.
- Terminal storage is one immutable row per signal and version pair.
- Re-resolution requires a resolver or quarantine version change, preserving
  the prior row alongside the new interpretation.

Database growth is therefore proportional to fired signals, not to the number
of instruments, bars, or scheduler polls. Cursor state is one mutable row per
strategy/resolver/input-version identity; it does not append on each poll.

## Pinned validation

Automated tests cover:

- an immature window remaining absent and retryable;
- target resolution;
- daily both-touch remaining unpriced and excluded from the live sample;
- quarantine refusal;
- price-scale boundary refusal;
- oldest-first batch limits and invalid-limit rejection;
- shared segmentation for per-series and cross-sectional strategy state; and
- scheduler/runtime registration.

The production path was also exercised against the development database with
the current manifest. With no signals at the new S-4 strategy version it
correctly reported a zero-write success rather than inventing a backfill.

## Opportunity-forecast outcomes (#2553)

The same scheduled job also resolves every immutable opportunity forecast
against the forecast's own `target_barrier_pct`, `stop_barrier_pct`, and
`horizon_market_days`. This is deliberately separate from the generic
strategy outcome: the bracket that ranked and sized a possible order is the
bracket whose probability claim must be tested. Reconstructing a later bracket
from a mutable strategy manifest would test a different decision.

Forecast outcomes use the shared causal OHLC rules (including gap-through,
daily both-touch ambiguity, quarantine masking, next-open timeout, and price
scale boundaries) and map them to `target_first`, `stop_first`, `timeout`,
`ambiguous`, or counted `unresolved`. One row is stored per exact
forecast/resolver/input-version identity. Immature forecasts have no row and a
single round-robin cursor prevents them starving later mature forecasts.

This adds no feature snapshots and copies no bars. Storage grows by at most one
terminal row per fired forecast/version, with one mutable cursor for the whole
resolver. `gross_return_pct` is observed gross price return in the existing
outcome-resolver convention (for example `0.10` means 10%); it is not net P&L
and cannot replace reconciled broker costs. The duplicate lookup index found in
review is dropped by migration 316 because the unique constraint already owns
the same index shape.

These rows are prospective calibration evidence, not capital authority. A
subsequent assessment must compare stated class probabilities with observed
classes over a recent, preregistered window and fail closed on insufficient
sample, calibration drift, or unresolved/ambiguous evidence.

## Explicit non-goals

This closes measurement plumbing; it does not make an unproven strategy safe to
fund. Deployment remains fail-closed until recent historical evidence, forward
resolved observations and the paper-stage controls all pass their existing
gates. No 75% win-rate guarantee is introduced.
