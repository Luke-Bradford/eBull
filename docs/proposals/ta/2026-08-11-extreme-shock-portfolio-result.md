# C-2 extreme-shock portfolio result — rejected

Date: 2026-08-11  
Issue: #2481  
Trial identity: `extreme-shock-portfolio-sizing-stress-v1`  
Trial register: `trial-register-2026-08-11-r3`

## Verdict

Reject C-2 as a capital candidate and do not start its 60-session prospective
shortability/cost census. The attractive per-event-day mean does not survive the
capital-allocation problem an operator actually faces. Every one of the eight
declared sizing/concentration arms lost money after the existing adverse cost
sensitivity, with maximum drawdowns between 40.68% and 69.05%.

This is a one-way rejection. The 2020–2026 interval was searched and cannot validate
a positive result, but a failure under the frozen rule and declared portfolio
constraints is sufficient to stop spending data, broker quota and research trials on
the family. Do not rescue it by selecting a cap, threshold, hold, stop, era, sector or
event subset from these opened outcomes.

## Frozen input and accounting

The signal was not changed from the searched development lead:

- prior close at least $20 and prior 20-session median dollar volume at least $10m;
- completed close-to-close fall of at least 12%;
- enter short at the next adjusted open;
- exit after five bars, or at a 20% stop first;
- a gap through the stop exits at the adjusted open, never the stop price;
- no leverage at entry;
- 50 bps round trip plus 57.4 bps adverse carry (8.2 bps × seven financed-day
  equivalents).

The simulation funds a single compounding pot. Same-date signals have no frozen
causal ranking feature, so available capital is split symmetrically rather than by
arrival order. Gross exposure is capped at 100% when entries are funded. Four
predeclared per-name cap sensitivities (0.5%, 1%, 2%, 5%) are each run with a 25%
sector cap and without it. Those eight evaluations are now charged to the trial
register; the C-2 family floor is 109 and the repository-wide declared floor is 122.

Sector labels are current and incomplete, not historical point-in-time identities.
Unknown sectors therefore share one conservative bucket in the capped arms. The
uncapped arms show that this imperfect control is not the cause of rejection.

## Reconciliation

The extractor reads 7,709 non-comparator research series and produces the same 8,049
trades as the earlier stop study. The flat trade mean is +136.33 bps gross; giving
each of 1,368 event days equal weight reproduces the previously reported +156.81 bps
gross and +49.41 bps after the adverse cost sensitivity. Median gross return is
+74.06 bps, win rate 52.28%, and worst trade -8,749.18 bps.

This reconciliation matters: the rejection is not a sign error or a changed signal.
It is the difference between a statistical average and capital that is scarce when
signals cluster.

## Portfolio results

`wt trade` is the final net result weighted by the notional the portfolio could
actually assign. `DD + wipe` adds a simultaneous -100% loss on one largest funded
position at peak gross exposure. Caps apply at entry; ratios can drift slightly above
them after losses before positions close.

| name cap | sector cap | funded | total return | annualised | wt trade | max drawdown | worst day | ES 5% | max concurrent | DD + wipe |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0.5% | 25% | 8,045 | -14.81% | -2.47% | -0.43% | -41.02% | -13.93% | -1.74% | 1,209 | -41.14% |
| 0.5% | none | 8,045 | -13.76% | -2.28% | -0.39% | -40.68% | -14.02% | -1.74% | 1,209 | -40.79% |
| 1.0% | 25% | 8,049 | -34.12% | -6.30% | -0.63% | -54.97% | -14.08% | -2.80% | 1,210 | -55.44% |
| 1.0% | none | 8,045 | -34.49% | -6.38% | -0.63% | -55.77% | -14.50% | -2.88% | 1,209 | -55.91% |
| 2.0% | 25% | 8,036 | -38.73% | -7.36% | -0.43% | -59.69% | -14.07% | -3.86% | 1,210 | -60.99% |
| 2.0% | none | 8,047 | -47.72% | -9.62% | -0.55% | -66.86% | -16.87% | -4.17% | 1,210 | -68.24% |
| 5.0% | 25% | 7,924 | -26.28% | -4.64% | -0.15% | -64.07% | -16.24% | -5.20% | 1,210 | -64.07% |
| 5.0% | none | 8,015 | -22.97% | -3.99% | -0.13% | -69.05% | -18.13% | -5.71% | 1,210 | -71.63% |

For the declared 1%/25% diagnostic arm, calendar returns are +0.20% (2020), -11.88%
(2021), -18.53% (2022), +2.17% (2023), +1.09% (2024), +5.09% (2025) and -15.62%
(2026 through the corpus frontier). This is neither reliably recent nor regime
stable.

The mechanism of failure is allocation-weighting. As many as 596 signals enter on
one date and 1,210 overlap. The event-day statistic gives a 596-name crash cluster
the same weight as a quiet one-signal day. A finite pot cannot do that: it divides
capital across the cluster while quieter events receive materially larger positions.
Once returns are weighted by executable notional, every arm is negative. Sector
coverage is 46.7% by series and 85.4% across fired signals, but sector-capped and
uncapped arms both fail.

## Remaining broker/data gaps do not justify continuation

The eToro eligibility and what-if-cost adapters exist, but the measured cost payload
has undocumented units and stale observations. Carry, FX and exact shortability are
therefore fail-closed. Historical point-in-time sector membership and shortability
are also absent. These gaps could only make the observed short result less reliable;
they cannot repair a negative capital-weighted result without selecting a new rule.

Consequently C-2 gets no manifest row, allocation authority, picker, demo order or
prospective high-volume store. Existing bounded quote/cost infrastructure remains
available for a future independently preregistered candidate. No new database table
or retained event dump is introduced by this result.

## Reproduce

```text
PYTHONPATH=. uv run python scripts/verify_2481_extreme_shock_portfolio.py
uv run pytest -q tests/test_extreme_shock_portfolio.py tests/test_trial_register.py
```

The verification script is read-only. Portfolio mechanics are pure functions in
`app/services/extreme_shock_portfolio.py`; tests pin no-leverage input, symmetric
batch allocation, unknown-sector concentration, cost accrual and the structural
single-name loss scenario.
