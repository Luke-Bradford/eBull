# Strategy evidence refresh and capital semantics

Status: implementation contract for #2469. This narrows, and does not weaken,
the validation and execution gates in the existing TA specifications.

## Operator promise

The Strategies page separates three facts:

1. live signal observation, which advances with the validated market-data
   frontier;
2. frozen research evidence, currently ending **2026-07-08** under
   `CORPUS_VERSION`; and
3. allocation/execution approval, which remains unavailable until every
   evidence, cost, risk and broker-contract gate passes.

⚠ The validated equity corpus reaches that frontier. Its 16 comparator-only
market/sector series have `instrument_id IS NULL`, stay outside the tradable
universe by construction, and all stop on **2024-09-27**. Historical claims
ending by that date remain computable; any benchmark, beta or regime window that
extends later must fail closed until #2482 supplies compatible source and
adjustment provenance. Missing market context is never interpreted as zero.

“Refresh evidence” means complete the declared recent-regime denominator. It
does not append today's data to the frozen corpus, select arbitrary dates, or
replace an existing result. Advancing the frozen date is a deliberate corpus
re-freeze: the corpus version moves and old results remain attributable to the
old version.

## Recent denominator

The required windows remain code-pinned:

- 2022 onward;
- rolling 36 and 24 months;
- calendar 2022, 2023, 2024 and 2025; and
- 2026 year-to-frozen-date.

One refresh evaluates the full runnable strategy set in each missing window so
Deflated Sharpe uses the registered measured trials. Each completed window is
committed as a restart boundary. A later run skips exact completed result
identities. A partial immutable window is an operator-repair condition, never
an invitation to overwrite the audit trail.

Only compact aggregate results and hold-out access records are retained.
Indicators, positions and returns are recomputed from the existing research
bars while the job runs; no additional per-bar metric store is introduced.

### Bounded refresh checkpoints

A running refresh exposes its active pinned window, phase, strategy, quarantine
arm and series count through transient `job_runs` progress. That heartbeat is
not evidence: before the result transaction commits it may not advance
`row_count`, the completed-window count or any result identity. One completed
window remains the smallest restart/cancellation boundary.

Transient progress uses one bounded, autocommit connection for the refresh,
separate from the evidence transaction. It is throttled and fail-soft; losing
telemetry removes operator visibility but cannot abort, commit or reclassify the
backtest. A post-commit checkpoint may advance the completed count only after
the immutable arm-pair writers have completed and the evidence connection's
commit has returned.

The runner may share immutable calculations only where exact arm equivalence is
proved. In particular, S-4's two daily-OHLC ambiguity arms share series reads,
signals, fill identifiers, causal ATR brackets and the raw resolver verdict;
only genuinely ambiguous rows are projected to best/worst exits. Namespace
books and all mutable counters remain arm-local. S-4's strategy module and
identity are unchanged: the batch adapter lives outside the hashed strategy
source and is tested against the scalar factory. Full history, corpus
membership, dates, parameters, costs, trial membership and result/write order
are not performance controls.

An S-4 bracket is broker-orderable only when both finite levels are positive,
the stop is below entry and take profit is above entry. A signal whose causal
ATR produces any other bracket is recorded as the terminal
`unorderable_exit_levels` refusal in both ambiguity arms. The runner must not
clamp or otherwise invent a level, and one refused signal must not abort the
remaining strategy, window or evidence refresh. The same refusal vocabulary
and rule-set version apply to historical and forward outcome resolution.

## What qualifies a strategy

Win rate is descriptive, not a promotion target. A 75% win rate can be created
by taking tiny profits against rare catastrophic losses and is therefore not a
safety claim. Promotion continues to require, at minimum:

- positive after-cost expectancy whose confidence interval excludes zero;
- declared-trial/Deflated-Sharpe evidence and random-entry control;
- stable recent-window performance, drawdown and tail-loss bounds;
- point-in-time universe, quarantine and ambiguity-arm completeness;
- measured broker costs, financing/borrow where applicable, and executable
  stop/take-profit semantics; and
- current signal scan plus approved paper execution policy.

No rule is enabled merely because the UI can display it or a historical win
rate exceeds a threshold.

## Capital model

The shared pot carries an explicit, audited mode:

- `fixed`: the configured principal is the hard strategy-system ceiling;
  net realised profits do not increase it and net realised losses reduce what
  remains available;
- `compound`: the risk base is configured principal plus reconciled realised
  strategy P&L.

Open marks never increase buying power. In either mode, incomplete realised
P&L reconciliation blocks new entries because an unknown loss must not be
subsidised by the wider broker account.

Each approved strategy carries an explicit, revisioned ticket mode:

- `fixed`: one configured currency amount per accepted signal; or
- `percent`: one configured fraction of its effective allocated base, still
  bounded by the shared remaining pot, per-instrument exposure, drawdown and
  broker cash limits.

These settings configure sizing only. They cannot bypass approval, health,
cost, freshness, ownership or kill-switch gates. Manual portfolio positions
remain outside strategy capital ownership and lifecycle decisions.

## Candidate expansion

The four current manifest rows are permanent `harness_validation` controls, not
a claim that only four trading ideas exist. #2478 makes that purpose immutable;
none can receive capital. S-4's causal ATR bracket plus resolver wiring
establish a backtest/forward-outcome path, not strategy profitability.

Every new hypothesis receives a fixed specification and trial identity
**before** measurement. Its measured trial is retained in the shared register
whatever the outcome. Passing a preregistered candidate spike is what can later
add a separate, disabled `capital_candidate` to the manifest; it never changes
a harness control. A definition-gated ticket is not yet a trial and may not run
until the missing choices below are transcribed into an immutable contract. The
current research programme spans intraday through multi-month horizons:

- the weaker time-series/XBRL SEC earnings-surprise drift (#2476) is
  **definition-gated**. No run may start until one cited seasonal-SUE equation,
  its minimum eight-quarter history and restatement rule are frozen. The
  decision time is SEC acceptance, entry is the next executable regular-session
  open, and the primary outcome is the price-compatible market/sector-adjusted
  return over sessions +2 through +62. The primary portfolio is equal-weight
  long the top SUE decile and short the bottom decile, with overlapping vintages
  scaled to equal gross exposure; a positive-SUE long-only arm is reported but
  cannot inherit the long-short result. Observed/estimated entry and exit spread,
  slippage, short carry and refusal are charged. It must beat matched random
  filing entries and the compatible market/sector comparator with a positive
  clustered lower confidence bound on net expectancy. Any 5/20/40-session
  diagnostic is a separately declared arm, never a best-horizon search;
- opportunistic Form 4 code-`P` purchases (#2480) were measured once and
  **rejected for capital promotion**. The recent 24/36-month spreads were
  negative, the primary confidence interval crossed zero, the timing placebo
  did better, and concentration/tail gates failed; the immutable verdict is in
  `2026-08-10-insider-purchase-result.md`. The frozen trial used exact filing
  acceptance where available and a conservative end-of-filed-date boundary
  otherwise. The
  causal adaptation of the published classifier calls an insider routine only
  after purchases in the same calendar month in each of the preceding three
  years; every other sufficiently observed insider is opportunistic, while
  insufficient history is its own refusal/control cohort. The
  primary test is a purchase-only, monthly rebalanced disclosed-purchase-value-
  weighted portfolio held for one month, against routine purchases, matched
  random entries and a price-compatible market/sector comparator. Sales never
  enter the long signal;
  observed/estimated spread and slippage are charged on the next-open entry and
  monthly exit. Its primary pass metric is the opportunistic-minus-routine net
  monthly return with a positive clustered lower confidence bound; ATR exits or
  other horizons are separately registered trials;
- the discovered >=12% one-day-drop **short continuation** (#2481): next-open
  entry, five-bar hold and 20% gap-through stop. It came from roughly 100
  searched arms, has no clean recent historical holdout, suffered an observed
  -87% trade, and needs decision-time borrow eligibility and CFD financing.
  Positions are equal risk at the declared 20% stop, with a fixed per-trade risk
  budget, per-name and gross-exposure caps and pro-rata refusal when the event
  cohort exceeds available capital. Its event-time portfolio simulation applies
  conservative same-day cash/margin ordering and correlated-gap stress. The
  primary metric is five-bar after-spread/slippage/borrow net expectancy with a
  day-clustered positive lower confidence bound; portfolio drawdown must also
  beat matched random five-bar shorts under identical construction. A genuinely
  new prospective out-of-sample cohort is required before it can be a candidate;
- the published long-short 30-minute midpoint/factor-residual reversal (#2484)
  is **definition- and data-gated**. Before any outcome is read, the primary PDF
  must pin the S&P 500 point-in-time universe, regular-session 30-minute bins,
  bid/ask midpoint, previous-bin residual formation, exact intraday factor
  regression and normalisation, cross-sectional breakpoints/weights, next-bin
  fill, one-bin hold/rebalance, both long and short legs, observed spread and
  CFD costs. The primary metric is the after-cost one-bin return of the complete
  published long-short portfolio, with time-clustered lower confidence bound
  above zero; raw-return reversal, random residual-decile labels and each leg
  alone are controls/attribution, not substitutes. It is forbidden from silently
  degrading to candle closes, raw returns or the weaker long leg; and
- an unlevered, eToro-costed Opening Range Breakout **falsification** (#2485),
  preserving the published Stocks-in-Play selection. It is definition-gated
  until the final paper pins the 5-minute regular-session opening range,
  relative-volume/gap selection, breakout fill, stop and same-session exit.
  Measurement uses observed eToro bid/ask costs, no leverage, a matched/random
  control and a selection-only control, month by month. Positions use the
  paper's stop-distance risk weighting capped at 1% research capital at the
  stop, the top-20 selection limit, total gross exposure at or below 100%, and
  pro-rata scaling rather than leverage when signals overlap. The falsification
  survives only if its after-cost daily portfolio return has a positive
  day-clustered lower confidence bound and beats both controls. It remains a
  filter-level rejection test and never becomes a strategy-menu entry; a pass
  can only motivate a new separately preregistered candidate.

#2477 owns bounded prospective intraday collection. Order-flow imbalance stays
out of scope because the free eToro feed lacks causal size/depth and aggressor
history; missing fields cannot be guessed from candles.

Long/short support also requires borrow availability, financing, gap-through
loss treatment, broker short eligibility and direction-aware signal/outcome/
execution schemas. Leverage remains out of scope until the unlevered demo
lifecycle passes forward observation and kill drills.
