# #3387 hunt 1, re-scoped under route A: large short-horizon effects in illiquid names

Refs #3387 (reopened), #2437 (operator decision 2026-09-26 22:07Z, "route A"), #3385 (harness), #3386 (store),
#3381 (recorded spreads). Supersedes the family list of `2026-09-26-3387-hunt-1-precheck.md`. Its verdicts stand.

## Mandate (operator decision, #2437 2026-09-26 22:07Z; "OR n" = its rule n)
- OR1: a short-horizon family's minimum worthwhile edge is **net per-trade expectancy ≥ 1% after the per-side
  tariff and the RECORDED spread**. Power-first is computed at that edge. A family whose plausible effect can't
  reach it is dropped at design time, not run.
- OR2: look at (a) small-cap, high-dispersion, short-horizon price/volume effects, then (b) regime-conditioned
  versions of (a), then (c) eToro crowd extremes (forward only; declare the minimum sample now).
- OR4: the hunt-1 time-box stands (first discovery screen ≤ ~3 days after #3386, which closed 2026-09-26). A
  family that clears discovery gets its validation immediately.

**Reading of OR1 used here.** Per-trade expectancy is the arm's own trade-weighted mean net return per position.
It is not excess over a tracker; OR1 states that one only for portfolio-level constructions. The harness's
premise, that the arm beats its matched control, is required alongside it.

## Recorded spreads (the "RECORDED spread" in OR1)
Source: `PYTHONPATH=. uv run python -m scripts.hunt_recorded_spread_census`, with defaults as below.
- Captures: perishables snapshot 1, one capture per instrument, 2026-09-25 17:22–17:27Z (13:22–13:27 ET).
- Domain: validated universe, close ≥ $5 on 2026-09-24, 20 returns. The domain is cut before any spread is
  joined.

Output at this commit, round trip, %:

| group | n | mean | median | p75 | p90 |
| --- | ---: | ---: | ---: | ---: | ---: |
| illiquid third | 1,187 | 0.555 | 0.248 | 0.531 | 1.233 |
| middle third | 1,186 | 0.213 | 0.126 | 0.247 | 0.498 |
| liquid third | 1,187 | 0.093 | 0.065 | 0.114 | 0.189 |
| illiquid, $5–20 (band 0.571) | 631 | 0.570 | 0.194 | 0.458 | 1.199 |
| illiquid, $20–100 (band 0.509) | 508 | 0.536 | 0.304 | 0.630 | 1.279 |
| illiquid, ≥ $100 (band 0.322) | 48 | 0.539 | 0.333 | 0.736 | 1.233 |

Expectancy is charged the **mean** spread here: a simplification. The exact target is E[G·q(S)], and the
selected arm's own mean spread is unmeasured. In the illiquid third, the largest sample under-charge of the
recorded mean by a base band is 0.217 (the ≥ $100 band: 0.539 − 0.322); the $5–20 band matches its mean. The stress band (1.450)
exceeds every band's p90. With fill-price costs (half-spread s/2 per side, no commission), 1% net needs gross
1.01·(1+s/2)/(1−s/2) − 1. At the illiquid mean s = 0.555, that is **1.56%**. The equivalent net hurdle on a
position charged the ≥ $100 band is 1.01·q(0.322)/q(0.539) − 1 = **1.2194%**, with q(x) = (1−x/2)/(1+x/2). The gate
below uses **1.22%** on every trade. Under the $5–20 band that is 1.80% gross.

Limits: one calm midday capture, and the spread is unconditional. The state the trial selects (just after a
loss) plausibly has wider spreads. Forward paper settles that at real quotes (see "After discovery").

## Design-time screen of OR2's families

| family | best published magnitude located | verdict |
| --- | --- | --- |
| (a1) overnight vs intraday | Berkman, Koch, Tuttle & Zhang (2012), *JFQA* 47(4): high overnight returns in retail-attention stocks, reversed intraday. The abstract only; no conditional per-night magnitude. The unconditional drift is ~4–5 bps/day (`strategy-evidence.md` §0), ~30–40× short of 1.56% | **dropped as unquantified**: no estimate to size or power a trial. It is not shown impossible. Reopen: a published conditional per-night estimate at or above the gross bar |
| (a2-i) extreme losers reverse (long) | Avramov, Chordia & Goyal (2006), *JF* 61(5), Table III, NYSE–AMEX 1962–2002, weekly, skip-day. Loser leg in the high-turnover, high-illiquidity cell: **1.83%/week** raw (weights ∝ return × turnover × illiquidity), 1.19% (return × turnover). Equal-weighted loser − winner in the same cell: **0.56%/week**. The authors: profits are "overwhelmed by transactions costs" | **one trial**. It is the only (a) construction with any published figure near the bar: 1.83% against a gate of 1.56–1.80% gross. It is not a bound for this construction: it is raw rather than over a control, turnover-sorted and heavily weighted, pre-decimal, and unmapped to the ≥ $5 validated population. A miss is the likelier outcome; one budgeted look settles this construction, not the family |
| (a2-ii) extreme winners continue (long) | the same table: winner leg −0.85%/week in that cell | **not run in hunt 1**: the one published cell has the wrong sign. That rejects nothing beyond it. The short side of (a2), drops continuing, is a different hypothesis (#2481, below) and needs the unpriced `stock_cfd_short_x1` lane |
| (a3) volume/attention shocks | Gervais, Kaniel & Mingelgrin (2001): small-stock high-volume leg 0.45% per 20 days (hunt-1 pre-check row 2a) | **dropped as unquantified at the bar**. The broad leg is ~⅓ of the bar over 20 days, five times this trial's four holding returns; no extreme-shock estimate located. Reopen: a published estimate at or above the gross bar |
| (b) regime-conditioned (a) | Nagel (2012), *RFS* 25(7), links reversal returns to VIX. Mechanism only; no magnitude taken | **sequenced, not infeasible**: a regime gate is a second trial and needs a VIX ingest. It is declared only if (a2-i) flags. A conditional-only effect is not searched in hunt 1 (a stated restriction) |
| (c) crowd extremes | none (unique, forward-only data) | minimum sample below |

#2481's in-house figure (+156.8 bps gross after ≥ 12% falls, short, liquid names, 2020+, ~100 variants
searched) is used only as a competing explanation. It chose no family or direction here, and it is a different
construction and population. No outcome of this trial's construction has been seen in any window.

## The trial: `extreme_loser_reversal_illiquid_v1`

| field | value |
| --- | --- |
| `hunt_id`, `family` | `hunt-1`, `extreme_move_illiquid` |
| scored domain on t | names in the view with their last 26 bars ≤ t on the 26 most recent sessions ≤ t (no gap), every close and volume > 0, close_t ≥ $5. Of those, the **most illiquid third**: sort by Amihud illiquidity = mean \|close_d/close_{d−1} − 1\| / (close_d × volume_d) over the 20 sessions ending t, descending, ties by `series_id` ascending; the first ⌈N/3⌉ are scored, the rest unscored. The control is therefore the illiquid third |
| score | close_t / close_{t−5} − 1, price-only (ACG's weekly formation; relative selection, as ACG's return weights are relative to the mean) |
| `sign`, `selection` | −1 (the lowest scores, the biggest relative losers), f = 0.05, ties per the harness |
| `lag`, `h`, points | 1, 5, close → close: entry at close t+1, exit at close t+5, four holding returns after one skipped return. ACG skip a day "to avoid the negative serial correlation induced by the bid–ask bounce". The skip reduces formation-close bounce; it does not remove stale prices or serially dependent price errors. The mapping to their week is approximate |
| weighting, lane | `equal`; `real_stock_long_x1` under the lane prerequisites the harness binds (USD account and order, real settlement, `hunt-cost-v1`) |
| `survivor_bias_direction` | `favours_arm`: illiquid losers are the likeliest to terminate. Declared, not corrected (harness residual before 2013-06-21) |
| mechanism | liquidity provision: non-informational selling in illiquid names clears at a concession that reverts within days (ACG 2006; Nagel 2012). Prediction: the arm beats the control, and the arm's own per-trade net clears OR1 |
| competing explanations | (1) pre-decimal tick bounce: most of discovery precedes 2001 decimalisation, and validation (2009+) does not; (2) informed selling drifts on (#2481's continuation); (3) real post-loss spreads exceed the charge (forward paper measures it) |

**Constructions, not source rules:** the $5 floor, the illiquid third, f = 0.05, the 20-session Amihud window,
the 26-bar no-gap history, and h = 5 with lag 1. The floor exists because below $5 the harness charges the frozen
1.450 band, which alone puts the gross bar at 1.01·(1.00725/0.99275) − 1 = 2.48%. Size and turnover are not formable. Both need point-in-time shares
outstanding, which OHLCV lacks before 2009. Illiquidity stands in for size, and ACG's turnover sort is dropped.
OR2's "small-cap" and "high-dispersion" are therefore not enforced. Dispersion enters only through selecting the
extreme tail. Relative selection need not select actual losses, and harness tie inclusion can make the arm
larger than 5% (continuous returns rarely tie).
Equal weighting in the extreme 5% has no demonstrated equivalence to ACG's weights. Volume and prices share the
view's split basis (`hunt_view`: volume ÷ k, so price × volume is unchanged).

## Discovery flag → validation → capital
The harness issues no discovery verdict. The implementation PR adds a descriptive readout to every cell: each
book's **trade-weighted mean position net return**, Σ over entered positions of (V_i(x) − 1) / #positions.
V_i(x) is the position's value factor at exit, including terminal haircuts. `HUNT_HARNESS_MODEL_ID` moves with
the code, and `hunt_trials` holds 0 rows, so nothing stored is invalidated.

Validation is declared only if **all** of these hold at the discovery readout:
1. the Benjamini–Yekutieli flag;
2. in every base cell, that cell's own active mean > 0 (the arm beats the control);
3. in every base cell, the arm's per-trade net mean ≥ **1.22%** (above).

A refused cell, an unavailable readout or zero entered arm positions counts as the condition not met.

A point-estimate screen (3) sits beside a significance screen (1); neither implies the other. Otherwise hunt 1
closes, recording every condition's outcome with its own label:
- (1) failing: "undetermined";
- (2) failing: "arm does not beat control";
- (3) failing: "below the operator bar (point estimate)".

Nothing is re-screened.

On a flag, validation (2009-01 → 2021-06-28) runs once, at once (OR4). The harness power statement comes
first (the harness defines it before validation and holdout looks only; it needs the discovery active series).
OR1's power-first is met there: the target active mean is 1.22% minus the control's per-trade net mean from the
discovery readout, per trade, spread over h. No power is computed before discovery. Promotion to forward paper
needs harness `PASS` **and** condition 3 again at the validation readout. Before any capital (programme rule 1:
"the real confirmation is forward"), forward demo paper must show realised per-trade net ≥ 1% at actual
entry/exit quotes, with the arm beating its control. That is also the conditional-spread measurement. The paper
window, minimum completed trades, fixed endpoint (no early stop), fill and quote protocol, and forward control
are frozen in their own declaration before the first paper trade. (b) conditioning follows as its own declared
trial.

## (c) crowd extremes: minimum sample, declared now
A crowd-extreme family (programme family 7) is evaluable only when both hold:
- ≥ 12 months of recording (programme doc, data table: *"forward evaluation after ≥ 12 months (6 months ≈ 2
  independent blocks at 63 days, too few)"*);
- ≥ 250 distinct decision dates with a non-empty arm (a construction).

The earliest date is 2027-09-25. Harness v1's power statement needs a discovery active series, which a
forward-only family has no historical form of. Family 7's own spec must therefore fix how its record splits
into discovery and validation before any crowd outcome is computed.

## Residuals
- Costs and prices: today's bands on 1990–2008 prices (`tariff-2026-counterfactual`). The gate prices
  historical trades with bands plus an offset, not recorded execution costs; only forward paper meets OR1
  literally. A position entering below $5 is charged 1.450, and no recorded mean exists for it.
- Execution: no market impact, size or depth. The arm readout is modelled position return: stale-mark exits
  and terminal haircuts are included.
- Harness: uncosted 1/h slot rebalancing (unsigned; it affects the active tests, not the position readout).
- Survivorship: conditioning before 2013-06-21; missing failed firms are absent from both readouts.
- Selection: price-only formation, so an ex-dividend drop can rank a name when five-session dispersion is small.
- Significance: the economic gates are point estimates; significance comes only from the active-series tests.
- Prior exposure: #2481's 2020+ variants overlap validation and holdout in time. They are counted in the
  inherited trial floor (#2827/#2832 register), not re-counted here.

## Budget and delivery
`HUNT_BUDGETS["hunt-1"] = 1`: one discovery `evaluate` row, no variants. A refusal before registration (identity,
lane, budget) registers nothing. Any registered row counts, whatever its outcome. Next PR:
`app/services/hunt_signals/extreme_move_illiquid.py`, the arm readout, the budget entry and the discovery run.
It closes hunt 1 or declares its validation.

## Outcome (2026-09-27)
The one discovery look ran (#3446, `hunt_trial_id` 1; `scripts.run_hunt_1_discovery --readout` reproduces it).
Conditions: (1) BY flag **met** (m = 378); (2) active mean > 0 **met** in every base cell (+0.00152/session);
(3) arm per-trade net **not met**: 0.325% (with dividends) / 0.311% (without) against 1.22%, label "below the
operator bar (point estimate)". Hunt 1 is closed, "no demonstrated edge" (`HUNT_CLOSED`; register entry
`hunt-1-discovery`, r14). Validation is not declared, and (b) stays unopened because its trigger, an (a2-i) flag,
did not fire. The per-regime readout is arm − control per session and says nothing about per-regime per-trade net.
