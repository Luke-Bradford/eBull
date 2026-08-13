# Point-in-time SEC filing-drift preregistration

Status: frozen before outcome measurement for #2476. Parent #2469.

## Hypothesis and honest name

This is a **historical-SUE SEC filing-drift** test, not an analyst-surprise
strategy and not a conventional earnings-announcement feed. eBull has no free
point-in-time analyst consensus. SEC facts may also arrive after a separate
issuer press release, so this test can miss part or all of the immediate
announcement response. Neither limitation may be renamed away if the result is
favourable.

The published construction is Jegadeesh and Livnat's rolling seasonal random
walk with drift. For firm `j` and fiscal quarter `t`:

```text
D[j,q]       = X[j,q] - X[j,q-4]
delta[j,t]   = mean(D[j,q]) for q = t-21 .. t-1
sigma[j,t]   = sample_stddev(D[j,q] - delta[j,t]) over the same 21 values
SUE[j,t]     = (X[j,t] - X[j,t-4] - delta[j,t]) / sigma[j,t]
```

Source: Jegadeesh and Livnat, *Post-Earnings-Announcement Drift: The Role of
Revenue Surprises*, equations 1 and 2. The paper estimates quarters `t-21`
through `t-1` and standardises by forecast-error volatility. This adaptation
uses as-filed SEC net income because eBull does not hold Compustat income before
extraordinary items.

## Frozen source construction

- Taxonomy `us-gaap`, concept `NetIncomeLoss`, unit `USD` only.
- Original forms `10-Q` and `10-K` only. An amendment is retained as evidence
  but never becomes a second signal and never rewrites an earlier quarter.
- Every value must be tied to the exact accession that first reported it.
  Later comparative facts, canonical latest-period rows and comma-joined source
  references are forbidden because they leak restatements backwards.
- Q1-Q3 use one unambiguous duration fact of 70-110 days ending on the filing's
  latest reported period end.
- Q4 is the as-known annual duration fact of 300-400 days less the same fiscal
  year's three exact, earlier as-filed quarters. Missing or ambiguous legs
  refuse the quarter.
- A SUE needs the current quarter plus 25 consecutive prior fiscal quarters:
  four seasonal lags plus the 21 prior seasonal differences used to estimate
  drift and dispersion. No shorter history, zero dispersion, non-finite value
  or discontinuous fiscal sequence is imputed.

## Knowledge time and fill

`sec_filing_manifest.accepted_at` is the preferred knowledge timestamp. The
current database retains it for only a small recent subset. When it is absent,
historical evaluation uses the SEC `filed_date` as an end-of-day boundary and
may fill only at the first research-corpus session whose date is strictly later
than `filed_date`. That fallback is deliberately late and causal; it is never
allowed to fill on the filing date. Live observation requires `accepted_at` and
refuses without it.

Entry is the next eligible session open. The primary exit is the close of the
62nd eligible session, counting the entry session as session one. Five-, 20-
and 40-session outcomes use the same counting convention, are declared
diagnostics and cannot replace the primary result after measurement.

## Causal cross-sectional trigger

An event cannot know the final decile of a calendar quarter that is still in
progress. The executable adaptation therefore derives thresholds only from
SUEs in the eight **completed** calendar quarters before the event's calendar
quarter. At least 200 eligible prior events are required. Thresholds use the
nearest-rank 10th and 90th percentiles with no interpolation.

- long arm: current SUE at or above the frozen trailing 90th percentile;
- short arm: current SUE at or below the frozen trailing 10th percentile;
- middle events are observed controls, not signals.

The equal-gross long-short arm is the published-family primary comparison. It
remains research-only because free historical borrow availability and eToro
CFD carry are absent. The long-only arm is reported independently and cannot
inherit a long-short result.

Equal-gross expectancy is one half of the long-arm mean plus one half of the
short-arm mean, regardless of retained event-count imbalance. Its interval
combines separate date-clustered 97.5% marginal intervals with Bonferroni
bounds, providing at least 95% joint coverage without assuming the arms are
independent.

## Universe, prices and costs

- US common-equity research series with an exact `instrument_id`; survivor-only
  is stamped and blocks promotion under the standing gate.
- Entry price at least USD 5 and prior-20-session median dollar volume at least
  USD 10m, computed strictly before entry.
- Equity OHLC uses the immutable primary research corpus through 2026-07-08.
- SPY and the exact SIC-resolved Select Sector SPDR use comparator snapshot
  `etoro-comparators-2026-07-08-v1`. Missing exact sessions or sector mapping
  refuse sector-relative evidence; they are never forward-filled or replaced.
- Raw, SPY-relative and sector-relative price returns are reported. No
  dividend-adjusted claim is made.
- Observed/estimated entry and exit spread plus slippage use the standing cost
  model. Short carry is not zero: it is unavailable and blocks promotion.
- No leverage, stop, target or ratchet is inferred. A bracket is a separate
  preregistered trial after this fixed-horizon effect establishes positive
  after-cost expectancy. Until then there is no executable capital candidate.

## Evidence windows and one-read rule

- Source/feature history may begin before 2022 solely to form the 25-quarter
  SUE history.
- Primary outcome evidence starts 2022-01-01.
- Results report the latest 24 months, latest 36 months, each calendar year and
  the full 2022+ interval separately. Older outcomes are stress evidence only.
- The final recent interval that has a complete 62-session outcome at the
  frozen frontier is sealed before measurement and read once. Any later rule
  change mints a new trial and may not reuse that interval as an unseen holdout.

## Required report and pass gate

The retained effect-screen result includes the complete exclusion census,
event and date-cluster counts, after-cost expectancy with a date-clustered
confidence interval, win rate, profit factor, expected shortfall/worst trade,
holding duration, observed event concurrency, year stability,
market/sector-relative return and a matched random-filing control under
identical fills and costs.

The control is selected before prices are read, one-for-one without replacement
from causally classified middle-SUE filings. It is matched on filing calendar
quarter and fiscal quarter using seed `2476001`, then inherits the paired
signal's long/short direction. The same price, liquidity, quarantine and cost
filters apply. A depleted match cell is counted, never replaced from another
period.

This effect spike intentionally defines no capital sizing path. Portfolio-level
drawdown, exposure, turnover and dollar capacity are therefore `not_measured`,
not inferred from independently compounded overlapping event returns, and each
is a promotion refusal. If the effect screen passes, a separately
preregistered fixed-cap portfolio trial must define those quantities and the
bracket before any paper allocation. This staging prevents a sizing choice made
after seeing event returns from flattering the same trial.

This one declared arm is appended to the global prior-trial register when it is
measured. A negative or inconclusive result remains retained. No threshold,
lookback, horizon or conditioner search follows it under the same identity.

Promotion is refused unless the lower confidence bound on after-cost
expectancy is positive and every standing evidence, universe, cost, tail,
capacity, holdout, execution and accounting gate passes. A win rate above 75%
is neither required nor sufficient.
