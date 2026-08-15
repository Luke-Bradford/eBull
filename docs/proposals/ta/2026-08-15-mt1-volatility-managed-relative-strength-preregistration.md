# MT-1 capped volatility-managed long-only relative strength — preregistration

Date: 2026-08-15  
Status: design frozen in source before implementation or outcome access; not yet entered in
`trial_register.py`, not yet frozen in `strategy_preregistration_declarations`, and therefore
not authorised to open outcomes  
Parent: #2437  
Candidate ledger: `2026-08-15-market-technician-derived-candidates.md`

## Purpose and honest name

`mt1-capped-volatility-managed-relative-strength` is a new, separately versioned
`capital_candidate` hypothesis. It is not S-10, does not inherit S-10's evidence, and may
not change S-10's permanent `harness_validation` purpose.

The prior is Moreira and Muir, *Volatility-Managed Portfolios* (*Journal of Finance*,
2017), and Cederburg, O'Doherty, Wang and Yan, *On the Performance of
Volatility-Managed Portfolios* (*Journal of Financial Economics*, 2020). Cederburg et al.
find that direct volatility scaling improves all nine momentum portfolios they test,
significantly for five. Their portfolios are zero-cost long-short factors/anomalies; MT-1
is a long-only ranked leader book. The evidence therefore motivates this experiment but
does not validate it.

MT-1 also differs from the published portfolio in two material ways:

- eBull forbids leverage, so the exposure multiplier is clipped to `[0, 1]`; and
- eBull has no frozen historical risk-free series for this experiment, so the reference
  uses after-cost total returns and residual capital earns zero, not factor excess returns.

Those deviations are part of the candidate identity. The result must be described as a
capped long-only variant, never as a replication of either paper.

## Frozen signal and portfolio clock

The signal and exit legs are an exact copy of the source-level S-10 rule at the commit that
implements MT-1: monthly cross-sectional relative-strength entry, wider retention-band
exit, causal regime input, identical minimum cross-section, universe, fill, ambiguity,
termination, quarantine and cost policies. The implementation must expose a separate
strategy module and identity hash. Calling S-10's helpers is allowed; reading or relabelling
an S-10 result is not.

All target-weight decisions occur on the source rule's existing **first-session monthly
decision dates**: exactly `s10_rebalance_dates`, the first weekday panel bar whose calendar
month differs from the preceding weekday panel bar. Calling this a month-end clock would be
wrong and non-causal: S-10 deliberately waits until the first bar of the new month rather
than assuming on the prior bar that no later session will appear. The overlay may not create
another intramonth rebalance. The unscaled reference is recomputed fresh in the same
invocation and under the same new trial contract.

Pre-outcome correction, 2026-08-15: the first frozen draft called these
"month-end" decisions while also requiring an exact copy of S-10. Source inspection before
the book implementation or any outcome access showed that phrase contradicted
`s10_rebalance_dates`. This paragraph records the correction rather than silently editing
the clock: the binding was and remains the source rule's exact dates; no price outcome was
read and no choice was made between measured variants.

The exposure statistic for that date uses information only through the prior completed
calendar month and is therefore fixed before the decision bar opens. The synthetic sizing
trade executes after that decision bar's close mark, using the holdings-level engine's
existing per-leg half-spread, sell-before-buy ordering and cash cap. The new target applies
only to the following close-to-close return; it never scales the decision bar's return.
This is identified as
`capped_target_exposure_after_decision_close_v1`. Source-strategy entries and exits retain
their original stored open fills. When a source event and an exposure decision share a
date, step 4 performs one closing rebalance over the post-fill holdings rather than two
synthetic rebalances.

## Frozen volatility construction

For each complete calendar month `m`, let `f[m,d]` be the unscaled reference portfolio's
after-cost daily total return on trading day `d`, including zero-return cash where the
reference is not fully invested. Let `J[m]` be its count of usable daily returns.

The realised variance used by Cederburg et al.'s equation (4) is:

```text
v[m] = (22 / J[m]) × Σ_d f[m,d]²
```

It is deliberately not demeaned. A month with no usable daily return or non-finite or
non-positive `v[m]` is unavailable.

For a decision in month `t`, raw inverse-variance reference returns over completed training
months are:

```text
g[m] = f_month[m] / v[m-1]
```

where `f_month[m]` is the compounded unscaled after-cost total return for month `m`. A
training observation exists only when both terms are available before `t`.

Every calendar month from the reference book's first eligible complete month onward remains
in the input history, with exactly one return for every session on the frozen panel calendar.
This includes an all-cash month whose returns and variance are zero. Because S-10 can be
intermittently invested whereas the papers' factors are always defined, a zero `v[m-1]`
cannot divide the next month's return: that pair is unavailable but the calendar month is
not deleted. MT-1 requires at least **120 usable completed training pairs**, matching
Cederburg et al.'s real-time initial-window count. Thereafter every usable pair from the
frozen first eligible month remains in the expanding window; it never rolls. At decision
`t`, compute only from usable training pairs strictly before `t`:

```text
c[t] = sample_stddev(f_month) / sample_stddev(g)
raw_exposure[t] = c[t] / v[t-1]
exposure[t] = min(1, max(0, raw_exposure[t]))
```

Both sample standard deviations use `n-1`. A missing session in any supposedly complete
month, a non-finite or non-positive standard deviation, a non-positive `v[t-1]`, or fewer
than 120 usable pairs refuses that decision. In particular, an all-cash prior month produces
no next-month exposure; it never defaults to 100%. The multiplier is applied once to the
aggregate reference book and uniformly to all selected holdings; residual weight stays in
zero-return cash. Per-name inverse-volatility weighting is a different strategy and is not
permitted under this identity.

## Arms and trial accounting

The evaluator must compute all four pairs unconditionally before reporting any outcome:

1. MT-1 long-only relative strength, capped-scaled;
2. the fresh MT-1 unscaled reference;
3. S-8 range-mean-reversion negative control, capped-scaled; and
4. the fresh S-8 unscaled reference.

"Same overlay" on S-8 means the same aggregate portfolio-level formula, 120-month
expanding training rule and exact S-10 first-session monthly exposure-update clock. S-8
signals and exits may change the underlying reference book between those monthly decisions,
but its exposure multiplier remains fixed until the next one. A per-entry, per-name or
signal-date scaling rule is not this control and must refuse.

Before the first price/outcome read, add two exact trial-register entries: one for the MT-1
scaled/unscaled controlled pair and one for the S-8 scaled/unscaled negative-control pair.
The ambiguity, termination, quarantine, recent-window and regime rows are conjunctive
robustness reports, not selectable trials. Any different training length, variance formula,
cap, clock, signal family or history policy is a new trial and a new strategy version.

## Population, timing and costs

- Full `survivorship_free` research population and its point-in-time termination treatment.
- Raw prices for signals/fills/cost bands; aligned total-return wealth prices for returns.
- Exposure input ends at the prior completed month and the target is fixed before the
  declared first-session monthly decision bar; its synthetic sizing trade uses that bar's
  close and affects only later returns. Underlying signal fills and exits remain exactly as
  the source rule permits. No input return is also an outcome return under the new target.
- Current complete cost-model identity, including spread, carry and FX stamps. A missing
  cost term is a structural refusal, never zero.
- The fixed in-sample/hold-out boundary and all code-pinned recent windows used by the
  strategy evidence platform. Raw operator-supplied dates are forbidden.
- The hold-out remains sealed until the complete in-sample controlled experiment and all
  structural gates pass. A failed in-sample arm ends this version; it is not repaired.

## First-order turnover disqualifier

Before any return, Sharpe, drawdown or expectancy value is exposed, report only structural
counts and traded notional. Refuse outcome access if either scaled book:

- introduces a decision date outside the frozen S-10 first-session monthly clock;
- has annualised turnover above 600% (the repository's 50%-per-month viability bar); or
- cannot reconcile every exposure change to the frozen formula and preceding information.

Passing this gate does not imply equal turnover: scaling can add traded notional on an
existing decision date. Both gross and costed turnover remain outcome-report fields.

## Frozen primary estimand and inference

Collapse daily after-cost returns to one compounded observation per complete calendar
month, expressed in decimal return units (`0.01` means 1%). With risk-aversion coefficient
`γ = 5`, matching Cederburg et al.'s base real-time test, define certainty equivalent:

```text
CER(r) = mean(r) - (γ / 2) × sample_variance(r)
ΔMT1 = CER(MT1_scaled) - CER(MT1_unscaled)
ΔS8  = CER(S8_scaled)  - CER(S8_unscaled)
primary = ΔMT1 - ΔS8
```

The primary gate is the **lower bound of a two-sided 95% paired moving-block-bootstrap
interval for `primary` greater than zero**. Resample the same contiguous month blocks and
indices across all four arms. Block length is 12 months, fixed before outcomes; use 10,000
replicates and integer seed `243715082026`. A percentile interval is used. Missing months are
intersected across all four arms before resampling; fewer than 120 common evaluation months
refuses the experiment.

The following are conjunctive, not alternatives from which a favourable winner may be
selected:

- the 95% paired bootstrap lower bound for `ΔMT1` is greater than zero;
- MT-1 scaled maximum drawdown is strictly smaller than its unscaled reference;
- MT-1 scaled 5% monthly expected shortfall is strictly less negative than its unscaled
  reference;
- the scaled result remains positive after the existing cost, synthetic-control,
  deflated-Sharpe, benchmark, recent-window, ambiguity/quarantine and structural promotion
  gates; and
- the latest code-pinned recent window has positive net expectancy. Pre-2000 history cannot
  rescue it.

No raw-return, Sharpe, drawdown, expected-shortfall, regime or recent-window value may be
used to alter this version after the first look. One failed conjunct kills this version.

## Forward-shadow floor

The minimum economically meaningful prospective improvement is fixed as a standardised
paired monthly effect of `0.5`. At two-sided `α = 0.05` and power `0.8`, using the same
normal approximation as the declaration gate:

```text
n = ceil(((z_0.975 + z_0.8) / 0.5)²)
  = ceil((2.8016 / 0.5)²)
  = 32 independent decision months
```

The floor is raised to **36 independent monthly decision dates** to cover three complete
calendar years. The calendar-duration floor is
`ceil(36 × 365.25 / (12 × 7)) = 157 weeks`. These are floors, not a claim that monthly
returns are independent; prospective inference must still block-bootstrap. A smaller true
effect requires more evidence and cannot lower the frozen floor.

## Authority boundary

This document alone authorises nothing. Before outcome access, the implementation must:

1. land under its separate strategy identity and result-version hash;
2. add the two exact trial-register entries;
3. merge the current structural-refusal policy version;
4. freeze a `capital_candidate` declaration for MT-1 with `survivorship_free`, carry and FX
   stamps that recompute to no structural refusal, plus the 36-date/157-week shadow floor;
5. freeze the S-8 control as `falsification_only`; and
6. pass the declaration/access gate through the paved evaluator path.

Even an evidence pass gives no direct broker authority. Promotion, a calibrated opportunity
forecast, the mandate-driven batch allocator, broker preflight and bounded paper deployment
must each pass separately. Live/real-money activation remains hard-false.
