# Completed-session market and sector regime mathematics (#2523)

Date: 2026-08-12  
Status: formula and read-only feasibility foundation; no candidate promoted

## Decision

Add one versioned pure calculator for compact market/sector **context** over
1, 3, 5, 10 and 20 completed provider sessions. It receives an aligned,
caller-frozen point-in-time cohort and returns aggregate direction, breadth,
dispersion, prior-trend participation and common-movement state. It neither
selects a trade nor writes an indicator history.

This distinction is load-bearing. Published evidence supports dispersion and
correlation as descriptions of market state and, in some samples, predictors
at longer horizons. It does not establish that combining them with RSI or a
moving average produces a profitable one-to-20-session rule. A 2024
comprehensive replication reports that the Pollet-Wilson average-correlation
predictor lost significance on its broader sample, had negative out-of-sample
R-squared, and produced poor investment performance. These values therefore
remain conditioning/risk variables until a separately preregistered mechanism
passes recent untouched and prospective tests.

## Frozen formulas

For point-in-time cohort member `i`, final completed provider session `t`, and
horizon `h` in `{1,3,5,10,20}`:

```text
R(i,t,h) = close(i,t) / close(i,t-h) - 1
R(m,t,h) = arithmetic mean of valid R(i,t,h)
dispersion(m,t,h) = sample standard deviation of valid R(i,t,h)
breadth(m,t,h) = count(R(i,t,h) > 0) / count(valid R(i,t,h))
```

Every provider-close transition inside `t-h...t` must also be joinable under
the pinned quarantine/series-break state. Endpoint availability alone is not
enough: a return crossing an unresolved split, ADR-ratio change or vendor
rescale is missing. The same rule covers every link used by the prior-trend and
common-movement windows. The calculator accepts only the explicitly named
`quarantine_joinable_vendor_close` basis; a raw unlabelled close panel is
rejected.

The same formulas are applied within the point-in-time provider industry. The
instrument decomposition is exact:

```text
instrument return
  = market return
  + (sector return - market return)
  + (instrument return - sector return)
```

This makes market, sector-relative and idiosyncratic movement auditable without
pretending the coarse eToro industry taxonomy is GICS or an executable sector
ETF hedge.

Prior-trend participation is deliberately causal and descriptive:

```text
above_prior_20(i,t)
  = close(i,t) > mean(close(i,t-20), ..., close(i,t-1))
```

The final close is not allowed to move its own baseline. “Percent above a
moving average” has not been admitted as independent alpha; the measure tells
us whether current direction is broadly established or narrow.

The 20-session common-movement value is:

```text
daily_market_return(d) = mean_i daily_return(i,d)
common_variance_share
  = variance_d(daily_market_return(d))
    / mean_i(variance_d(daily_return(i,d)))
```

It uses only members with a balanced 21-close panel. It is bounded to `[0,1]`
and measures the share of average constituent variance surviving equal-weight
diversification. It is **not** labelled average correlation or first-principal-
component share: unequal constituent volatilities make those different
statistics. A future candidate needing one of those exact quantities must
freeze and implement it separately.

## Knowledge time, missingness and coverage

- The caller supplies the completed-session calendar, cohort version, raw
  source version and point-in-time provider industry. The calculator never
  reads current classifications or later constituents.
- A missing/non-positive/non-finite endpoint excludes the member from that
  measure but remains in the declared coverage denominator. It never becomes a
  zero return, unchanged stock or zero volatility.
- An unresolved or provisional unit-regime transition has the same fail-closed
  treatment. The verification source pins both the quarantine rule version and
  break/adjustment frontiers in a repeatable-read transaction.
- The candidate supplies its coverage threshold before outcomes. Below it, the
  aggregate is refused and values are `NULL`, not a partial number wearing a
  warning.
- Dispersion and common movement require at least two members. Sectors below a
  candidate-supplied minimum size are refused, even at 100% coverage.
- All 21 dates must be strictly increasing and every member must align exactly
  to them. The calculator contains no wall-clock fallback.

## Live-corpus feasibility result

`scripts/verify_2523_regime_context.py` performed a read-only check against the
current NYSE/Nasdaq common-stock classification with a known provider industry,
the current quarantine rule set, and an 80% illustrative feasibility floor.
This threshold is not a candidate threshold and no outcome was accessed.

The first implementation used the union of observed stock dates. It admitted
sparse provider dates contributed by nine names: 3- and 10-session coverage
collapsed to **9 / 4,351 (0.21%)**. That approach is rejected. eToro daily bars
also do not use New York civil dates consistently (a Monday session may carry a
Sunday provider date), so filtering weekdays would introduce a second error.

The corrected check pins the provider SPY series (`instrument_id=3000`, asserted
as the ETF identity) as the completed-session calendar:

| measure | observed result |
|---|---:|
| point-in-time members with provider industry | 4,351 |
| completed source sessions | 21 |
| latest quarantine-complete provider session | 2026-08-05 |
| 1-session endpoint + joinable-unit coverage | 80.602% |
| 3-session endpoint + joinable-unit coverage | 80.602% |
| 5-session endpoint + joinable-unit coverage | 80.487% |
| 10-session endpoint + joinable-unit coverage | 80.487% |
| 20-session endpoint + joinable-unit coverage | 80.464% |
| balanced 21-close/trend/unit coverage | 80.257% |
| provider industries / with at least 20 members | 9 / 8 |
| representative database load / pure calculation | 1.428s / 0.116s |

The 2026-08-05 frontier is intentional: newer raw bars exist, but the current
fail-closed quarantine coverage does not yet admit them. A runtime candidate
must report this staleness and refuse rather than silently reading unchecked
2026-08-11 bars.

The unit-regime mask removed only a small fraction of members but moved the
descriptive common-variance share from 3.45% to 5.60%. Neither value is a trade
signal; the sensitivity demonstrates why corporate-action joins are mandatory
before regime attribution rather than an optional cleanup after it.

## Storage and product impact

This increment adds no table, index, migration, derived row, WAL or UI element.
The 4,351-by-21 panel exists only for the calculation and is released. A later
decision-context migration should copy only:

- five market horizon aggregates;
- the candidate instrument's one sector aggregate at each horizon;
- market and selected-sector breadth/trend/dispersion/common-movement values;
- formula, cohort/source, session frontier, denominator and refusal identity.

It must not persist all nine sectors per instrument, per-bar rolling values, or
routine not-fired scans. This is the measured path that keeps the 63 GB database
warning from becoming an excuse for another feature warehouse.

## What remains blocked

1. A production loader needs a candidate-owned calendar and exact cohort, not
   the verification script's broad feasibility cohort.
2. Only current/prospective provider industry is known. Historical decisions
   before the observation boundary cannot use today's sector.
3. Event-mechanism state from #2507 and exact intraday coverage remain missing.
4. The coarse nine-industry mapping does not complete #2522's sector-ETF hedge
   contract.
5. These aggregates need candidate-specific recent OOS attribution before any
   claim that a condition improves expectancy, loss tails or calibration.

The honest runtime catalogue therefore remains empty.

## Primary research references

- Connolly and Stivers, [Information content and other characteristics of the
  daily cross-sectional dispersion in stock returns](https://doi.org/10.1016/j.jempfin.2005.02.001)
  — defines broad daily return dispersion as cross-sectional standard deviation
  and studies it primarily as volatility information.
- Campbell and Lettau, [Dispersion and Volatility in Stock Returns: An Empirical
  Investigation](https://www.nber.org/papers/w7144) — separates market,
  industry-relative and firm-relative dispersion.
- Pollet and Wilson, [Average correlation and stock market
  returns](https://doi.org/10.1016/j.jfineco.2010.02.011) — motivates common
  movement as distinct from individual variance; its original forecast horizon
  is quarterly, not a one-day entry.
- Goyal, Welch and Zafirov, [A Comprehensive Look at the Empirical Performance
  of Equity Premium Prediction](https://doi.org/10.1093/rfs/hhae060) — broad
  updated replication and the reason correlation is not treated as deployable
  alpha here.
