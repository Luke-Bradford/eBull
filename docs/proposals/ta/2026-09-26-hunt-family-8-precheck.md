# Hunt family 8 pre-check: interactions on held price data — no trial registered

Refs #2437 (queue note 2026-09-26 16:30Z; north-star comment 10:43Z, small-cap variant rule), #3387 (hunt 1
pre-check: Bar A / Bar B), #3385 (harness v1). Programme doc: `2026-09-25-pattern-hunt-programme.md`, family 8.
Previous family pre-check: `2026-09-26-hunt-2-families-4-5-precheck.md`.
Security: none (a research document; no code, broker, auth or order path).

**Status:** v3, after Codex ckpt-1: 60 findings on v1 and 39 on a scoped v2 round. Applied: v1 missed JKX's
time-scale transfer tables (8 and 9), whose equal-weight edges clear Bar B before haircuts; the nonlinear-versus-
linear reading was overstated; the small-cap variant confused price with size; haircuts were asserted without
measurement; v2's tracking-error bound was stated as a harness-level power failure, which it is not. No trial
registered, no research return read, no ingest started. Inputs: hunt 1's bars, harness v1's construction rules, one
paper read in full text, two abstracts. Nothing here enters M.

## Result
**A budget decision, as in hunts 1 and 2, and a discretionary one: register no family-8 trial now.** The one
published price-only nonlinear model with portfolio magnitudes found here (Jiang, Kelly & Xiu 2023, "JKX") has
constructions whose equal-weight long-only edge clears the hurdle **before** any haircut. Two clear it by more
than the tables' rounding: the 5-day model applied to down-sampled 20-day histories (+1.8 to +2.6%/yr at the
$5–20 and $20–100 bands) and to down-sampled 60-day histories (+0.7 to +1.5%/yr at every band ≥ $5). Both come from
an all-stock, equal-weight, same-day-entry population that differs from ours in exactly the ways that are known to
move this kind of edge, and no estimate exists for the matching population. This is hunt 1's 3b again. Screened
out, not tested and rejected; it does not exhaust family 8 ("What is not screened").

## Which published evidence applies
- **Gu, Kelly & Xiu (2020)**, *RFS* 33(5) (`strategy-evidence.md` §2.7), the programme's motivating paper: 94
  characteristics following Green et al. (2017), each interacted with eight aggregate series, plus 74 industry
  dummies, 1957–2016 (journal abstract and data description). Many of those characteristics are accounting-based
  and we hold as-filed fundamentals only from 2011 (programme data table), so the full model is not expressible in
  the 1990–2008 discovery window. No magnitude for a price-only subset of it was retrieved.
- **Jiang, Kelly & Xiu (2023)**, *"(Re-)Imag(in)ing Price Trends"*, *JF* 78(6), 3193–3249 (SSRN 3756587, full
  text): a CNN on 5/20/60-day chart images (OHLC bars, a moving-average line and volume, from CRSP returns
  including distributions) of NYSE/AMEX/NASDAQ stocks, trained once on 1993–2000 with a random 70/30 split and five
  averaged fits, held fixed over a 2001–2019 test (§4.1). §4.1 states no price or size floor. Portfolios are formed
  on the last image day, holding period equal to the forecast horizon, annualised average holding-period returns.
- **Avramov, Cheng & Metzker (2023)**, *Management Science* 69(5), 2587–2619 (abstract): deep-learning profits come
  from difficult-to-arbitrage stocks and high limits-to-arbitrage states; *"excluding microcaps, distressed stocks,
  or episodes of high market volatility considerably attenuates profitability"*; performance *"further deteriorates
  in the presence of reasonable trading costs because of high turnover"*; the signals *"are profitable in long
  positions"*. Direction only, no magnitude, and not specific to JKX's model.
- **Our record:** the residual-confluence candidate (#2499), a small pre-registered interaction of price trend,
  liquidity and volatility motivated by GKX, was rejected (`2026-08-11-portfolio-alpha-viability-plan.md`, "More
  confirming indicators…" row). One construction, not a bound on the family.

## The pre-check table
Harness v1 is **equal-weight only** (`2026-09-26-3385-hunt-harness.md`, spec table `weighting`), and its control is
every eligible scored name under the same construction. JKX's closest analogue to arm − control is **top decile
minus the mean of the ten deciles**, which approximates the equal-weight universe (equal-count deciles; ties,
integer deciles, changing availability, missing-return treatment and JKX's IPO/delisting image exclusions make it
inexact). Inputs are JKX's annualised gross returns (Tables 3, 8, 9); the mapping of that annualisation onto the
harness's arithmetic daily series is not reconciled. Each entry is rounded to 0.01, so each difference carries up to
±0.9%/yr of rounding. JKX's portfolios are rebalanced every h days; the harness runs daily overlapping cohorts with
fixed slot weights. Hunt 1's cost qualifications (fill-price accounting, idle slots, missing entries, uncosted slot
rebalancing, mixed-band books needing separate arm and control cost averages) apply unchanged.

The hurdle is the larger of Bar A and Bar B at the horizon, for a book entirely in one band (hunt 1's formulas,
with g = 0 and k_tracker ≈ 0 as there; the mean must strictly exceed it):

| h | ≥ $100 | $20–100 | $5–20 | < $5 (Bar B is 0) |
| --- | --- | --- | --- | --- |
| 20 | B 14.2 | B 11.9 | B 11.1 | A 19.8 |
| 60 | B 4.7 | B 4.0 | **A 3.9** | A 7.6 |

| JKX construction (image / hold) | EW top − mean, %/yr | VW top − mean | against the hurdle (EW, point estimate) |
| --- | --- | --- | --- |
| I5/R20, I20/R20, I60/R20 (Table 3) | 10.8, 7.9, 3.9 | 2.9, 4.1, 1.7 | below every band (I5/R20 at $5–20 within rounding) |
| I5/R60, I20/R60, I60/R60 (Table 3) | 4.3, 1.5, 2.9 | 2.1, 0.5, 2.1 | I5/R60 clears $20–100 and $5–20 by 0.3–0.4, within rounding |
| **I5/R5 on 20-day history sampled every 4 days, R20** (Table 8 "Transfer") | **13.7** | 4.2 | clears $5–20 (+2.6) and $20–100 (+1.8), beyond rounding |
| Table 8 "Baseline+Transfer" | 12.8 | 5.0 | clears $5–20 (+1.7) and $20–100 (+0.9) |
| Table 8 "Re-train" (CNN trained on the down-sampled 20-day images) | 9.9 | 2.3 | below every band |
| **I5/R5 on 60-day history sampled every 12 days, R60** (Table 9 "Transfer") | **5.4** | 1.1 | clears every band ≥ $5 (+0.7 to +1.5); $5–100 beyond rounding |
| Table 9 "Baseline+Transfer" | 4.0 | 2.2 | clears $20–100 and $5–20 by 0.05–0.1, within rounding |
| Table 9 "Re-train" | 2.5 | 2.1 | below every band |

"Baseline+Transfer" is JKX's 50/50 combination; its reported decile returns are not the average of the two
components' (Table 8's top decile is 23% against 18% and 24%), so it is read as reported, not as a two-book blend.
The VW column is not a harness construction and not an estimate for any price band: it is an equal average of ten
value-weighted deciles, shown for the direction of the edge once small names lose their equal weight. Table 7
reports other image/hold pairings as H−L Sharpe only, so it cannot enter this table. The one-week models (Table 6:
EW top − mean 20.9–41.5%/yr) sit below h = 5's lowest hurdle, 44.3%/yr.

## Why the two transfer cases are not funded
- **Population.** JKX sets no price floor and weights every stock equally; hunt 1 excluded sub-$5 names. Where the
  edge sits by price or size is not reported. Two readings conflict: the transfer model's value-weighted edge is
  4.2%/yr against 13.7 equal-weighted at 20 days (1.1 against 5.4 at 60), and Avramov et al. place ML profits in
  microcaps and distressed names; JKX, in the other direction, say the CNN *"continues to excel amid value
  weighting, illustrating that image data is predictive across the size spectrum"* (§4.3, on the original models).
  Neither identifies the ≥ $5, eToro-eligible edge.
- **Timing.** Harness v1 enters at lag ≥ 1; JKX forms portfolios on the last image day. Figure 10 shows the
  original CNN models' equal-weight H−L Sharpe for 20-day holds across delays of 0–20 days; it does not cover the
  transfer models, and the size of a one-session loss on a long-only mean is not reported.
- **Tracking error** (hunt 1, 3b). On JKX's own books, TE ≥ σ(top) − σ(universe) ≥ σ(top) − mean σ(decile)
  (reverse triangle and Minkowski inequalities), with σ = (return − rf) / Sharpe and rf taken as 0 or 2%/yr (JKX's
  convention is not stated). That gives TE ≥ 3.2–4.2%/yr at 60 days and 1.7–2.7 at 20 days at the printed values;
  rounding of the printed returns and Sharpes lowers the 60-day bound to about 1.3–2.4. For 50% power on a t > 3
  gate over ~12.2 validation years the stress-net IR must reach about 3/√12.2 ≈ 0.86. The 60-day transfer case's
  best stress-net margin is 1.7%/yr ($5–20 over Bar B), so its IR on JKX's books is at most about 0.40–0.53 at the
  printed values and 0.49–0.66 allowing rounding: below 0.86. At 20 days the affordable TE is 2.1–3.1%/yr against
  a bound of 1.7–2.7, which excludes the $20–100 band at rf = 0 only. ⚠ These are statements about JKX's
  calendar-rebalanced books in their sample; the harness's HAC long-run variance on daily overlapping cohorts, in
  our window and population, is a different quantity that this bound does not reach.
- **Validation overlap.** JKX's 2001–2019 test covers most of our 2009-01 → 2021-06-28 validation window, so
  validation would re-test data the choice was informed by (hunt 1, 3b). Post-publication decay (McLean & Pontiff,
  §2.6, a predictor average) is a forward-performance scenario, not a haircut on the validation mean.
- **Unquantified differences**: survivorship (our corpus is near survivor-only before 2013, programme data table),
  JKX's random rather than purged split, and the harness's without-dividend and termination-policy robustness cells.

None of these is a measured haircut. Together they leave the ≥ $5, lag-1, eligible-universe magnitude unknown.
Short of new external evidence (the reopen event below), learning it takes a discovery trial, which adds to M
permanently (programme rule 2).

## Nonlinear versus linear
JKX Table 16 (long-short decile Sharpe) compares the CNN with a linear model on the same image-scaled inputs. The
CNN leads in equal weight at 20 days (2.35 vs 2.07, 2.16 vs 1.99, 1.29 vs 1.23 for 5/20/60-day images) and at
5 days for 5- and 20-day images; the linear model leads in value weight at 20 days for two of three image sizes and
at 60 days in five of six cells (in equal weight, two of three; the 5-day-image case goes to the CNN, 1.30 vs
1.19); a 1-D CNN on image-scaled series leads both at 20 days in equal weight. The paper calls the linear
approximation generally inferior to the CNN and most competitive at longer horizons. No paired test is reported,
and H−L Sharpe differences do not give a long-only increment. So the published increment of nonlinearity is mixed
and unsized; it does not enter the verdict, which rests on the CNN's own figures. A linear image-scale model would
be a separate trial and would face the same table.

## Small-cap variant (north-star rule, 2026-09-26)
The declared variant is the bottom size buckets that pass eToro tradability, equal weight. **It cannot be formed
from OHLCV alone**: point-in-time size needs shares outstanding, which the held price corpus does not carry (the
assembly #2834 ARM B needs). Price is not size, so the band hurdles do not map onto it; a small company can trade
above $20. The table fixes one thing: a book made entirely of sub-$5 names faces Bar A 19.8%/yr at h = 20 and
7.6%/yr at h = 60 (g = 0), above every 20- and 60-day JKX figure; a mixed book pays weighted arm and control costs.
The #3381 recorded spreads are forward-only and cannot re-cost 1990–2021; the harness's frozen bands are the
historical counterfactual charge, as for every family.

## What is not screened
This screens the published-estimate route only. It does not bound other nonlinear architectures, feature sets,
selection fractions, horizons, or interactions using FTD/regime inputs (hunt 2 kept that route open); none of them
has a retrieved estimate, which is why they are not trials either.

## Reopens when
An estimate for a long-only construction on admissible inputs (price-only or not), from a sample ending before
2009-01 with no validation-era selection and purged, causal features, puts the active edge strictly above the
larger of Bar A and Bar B at its horizon, under harness v1's mapping: price floor of $5 or more (or its sub-$5 part
costed at 1.450%), entry at lag ≥ 1 with lag + h − 1 ≤ 63, a control of the eligible scored names, separate arm and
control cost averages for a mixed-band book, and g stated. Power is then checked twice: a preliminary figure for
the stress-net edge (edge − Bar B) from a published or pre-window tracking error, and the harness power
calculator's own figure for the canonical base-cost cell once discovery rows exist; both ≥ 0.5 at t > 3. A reopen
permits a spec; promotion still needs every programme gate (rule 3).

## What this leaves
- Family 8 registers nothing; step-6 ingests keep the reopen events in the hunt-2 pre-check.
- Unassessed: family 6 (MIDAS, validation-only and low power by the programme's declaration) and the Wikipedia
  source; family 7 waits on ≥ 12 months of #3381 recording; the reopen events of hunts 1, 2 and this note stay live.
- The terminal "no demonstrated edge" verdict (rule 5) is not reached here.
