# #1822 route F — readout 1 (retrospective)

Refs #1822. Spec: `2026-09-28-1822-ranking-ablation.md` (v9).

**Declaration and vintage**
- Declaration 14, frozen 2026-09-28T10:00:12Z from main `d6f2d3b2` (`1822-route-f/freeze-14.json`).
- Terms sidecar `ee86663b…`; register r18 charges 96.
- Vintage `1822-route-f/vintage-2026-09-28-c94b15b0.json`: readout date 2026-09-28, c_k 2026-09-22, same commit. It
  holds every number below, the input identity, and one look.

**Readout, not a verdict.** This is a descriptive readout with no inference:
- six families × two cost bases, so every t is marginal, and no multiplicity correction is applied;
- net only, with between-slot rebalancing uncosted and price-only returns (no dividends);
- the window's prices existed before the freeze, so this readout is **retrospective**;
- the prospective population is **empty** (`empty_grid`), because no run was known after the freeze.

⚠ **Both inference floors fail by an order of magnitude**, reported, not applied. T = 22 and L = `hunt_lag(22, 21)`
= 42, so L ≥ T: T/L = 0.52 against `SHORT_SAMPLE_LAGS` = 10, and `sparse_arm_count` = 2 against 30. The HAC SE is
computed at a lag longer than the sample, so neither the SEs nor the t values below support a claim about any
family. With L ≥ T, the 2L SE comes out *smaller* than the L SE for every family, which is one more sign the
estimate is not usable yet.

## Δ_f = r(arm_full) − r(arm_−f), canonical cell, pooled (annualised ×252, fraction)

| family | mean Δ | SE (L) | t | MDE | SE (2L) | mean \|arm_full △ arm_−f\| |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| quality | +0.0266 | 0.0090 | +2.94 | 0.0253 | 0.0064 | 329.6 |
| value | +0.0612 | 0.0292 | +2.10 | 0.0817 | 0.0207 | 641.9 |
| turnaround | −0.0207 | 0.0109 | −1.89 | 0.0307 | 0.0078 | 260.4 |
| momentum | −0.0171 | 0.0026 | −6.66 | 0.0072 | 0.0018 | 140.7 |
| sentiment | −0.0033 | 0.0005 | −5.98 | 0.0015 | 0.0004 | 39.1 |
| confidence | +0.0002 | 0.0007 | +0.30 | 0.0019 | 0.0005 | 20.6 |

Positive Δ means the full key did better than the key without f in this window. `arm_full − control` = −0.0552/yr.
Across the 16 formations, `arm_full` held 762 names on average and the control about 3,800 (the funnel below).

**Sensitivities** (one assumption each):
- **`t3_excluded` and `as_traded`** move no family's mean by more than 0.0015.
- **The two other termination policies** reproduce the canonical to 4 dp. Three terminating names were held.
- **The verbatim bar rule** (NULL volume fails) changes the population. The control shrinks, `arm_full − control`
  becomes −0.0149, confidence becomes +0.0038 (t 3.81), turnaround −0.0060, quality +0.0364 and value +0.0624. The
  signs of quality, value, momentum and sentiment are the same in every cell.

## Descriptives (canonical cell)

- **Funnel.** 16 active formations.
  - 3,916–3,926 rows were ranked per run; 473 non-stock rows were excluded in total.
  - The control holds 3,796–3,817 names.
  - Bar-rule drops, summed over formations: 274 `high_not_above_low` and 9 `non_positive_ohlc`.
  - Every `arm_full` position was entered.
- **Turnover.** Mean per-formation turnover is 0.08 for every arm book and 0.003 for the control.
- **Deployed share.** It rises from 1/21 to a maximum of 15/21 at G_k. The annualised figures are read against
  that partial exposure.
- **Carried V** (position-sessions with no valid close): 191 for `arm_full`, 191–500 for the ablated arms, and
  3,119 for the control.
- **T3.** 54 entered positions were held across a computed T3 transition, all of them in the control book. None
  was in any arm.
- **Yield tilt** (`arm_full` − `arm_−f`, mean TTM yield in pp, read at readout time): value +0.71 and quality
  −0.67. 851 held names have no yield. The tilt neither signs nor bounds the omitted dividends.
- **Regime.** Every reported session is labelled `bull_quiet`, so there is no per-regime split. All of the above
  is one regime.
- **Implementation markers.** Seven commits touched `scoring.py` between 2026-07-23 and c_k. The readout does not
  split on them.

## Next

- **Readout 2** runs on 2026-11-02 (the first weekday of the month) on the same frozen terms, and appends
  sessions. Its prospective subseries starts with the first run known after 10:00:12Z.
- No weight changes here (spec "Not doing"). Route H is built when its prerequisite 1 (#3386) lands, independent
  of these numbers.
