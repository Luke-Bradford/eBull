# Edge construction — what makes a bet strong, and how a cell earns the right to be believed

Refs #2437. Companion to `strategy-catalogue-and-backtest-validity.md`, which
defines *whether a strategy works*. This defines **how much to bet, when not to
bet at all, and how we tell a real conditioning effect from a curve fit.**

⚠ Nothing here is a new strategy. Every item is a multiplier on the strategies
we already have, and the multipliers are currently all set to 1.

---

## 0. The gap this closes

`strategy_results_store` says one sizing rule has ever been used:
`equal_weight_concurrent_v1`. That is the whole of our position construction.
There is no conviction weighting, no volatility targeting, no correlation
awareness between concurrent positions, and no regime gate. A strategy either
fires or it does not, and every fire is the same size.

So of the six things that decide whether a bet is strong, we currently do one:

| lever | status |
| --- | --- |
| 1. does the edge exist | measured (criterion 7) |
| 2. **how much to bet** | **absent** — equal weight, always |
| 3. **when NOT to bet** (regime gate) | **absent** |
| 4. entry/exit precision | partial — S-1/S-3 price-condition exits; S-4's stop/target/max-hold cannot be built. ⚠ Precisely: `ExitLevels` IS a dataclass in `outcome_resolver.py`; what is missing is any code that **computes strategy-specific levels** to construct one |
| 5. **correlation between concurrent positions** | **absent** — equal weight is correlation-blind |
| 6. cost control | modelled (~50.9 bps round trip) but not optimised against |

Items 2, 3 and 5 are unclaimed, and none of them requires finding a new signal.
⚠ Whether they are the *largest* gains is unmeasured — ranking them against
survivorship, cost and data quality is itself an experiment, not an assumption
this spec is entitled to make.

---

## 1. Sizing — the biggest single lever we are not pulling

**Source rule.** Grinold's fundamental law of active management:
`IR ≈ IC × √breadth`. Two consequences we are currently ignoring:

- **Breadth is bets, not strategies.** Five uncorrelated positions beat one
  position five times over. ⚠ This is the honest reading of "I want more
  strategies": the gain comes from holding more *independent* bets, not from
  testing more *hypotheses* — testing raises the deflated-Sharpe bar for
  everything (10 trials → 0.0833, 1,000 → 0.1738).
- **Equal weight throws away IC.** If the conditioning work (§3) says a fire in
  cell A has twice the expectancy of one in cell B, equal weight deliberately
  discards that.

**Proposed, each a declared `sizing_rule` and therefore hashed into result
identity — so a sizing change can never read as a performance improvement:**

1. `inverse_vol_v1` — position ∝ `1 / trailing_vol`. The standard risk-parity
   floor. Equalises *risk* contribution rather than *capital*, which is what
   equal weight actually gets wrong: a 90%-vol microcap and a 15%-vol large cap
   are not the same bet at the same notional.
2. `conviction_weighted_v1` — scale by the conditioning cell's measured
   expectancy, from §3. ⚠ Requires §3 to have produced out-of-sample-stable
   cells; sizing on an in-sample gradient is overfitting with extra steps.
3. `vol_targeted_v1` — scale the whole sleeve to a target portfolio volatility.
   The thing that makes drawdown a policy choice rather than an outcome.

⚠ **Kelly is named and NOT adopted.** Full Kelly maximises log growth and is
famously unlevered-optimal, but it assumes the edge estimate is correct; at our
parameter uncertainty it is a drawdown machine. Half-Kelly or less, and only
once §3's cells are stable. Stated here so it is a decision on record rather
than an omission.

**Acceptance:** each rule measured as its own arm on the full population against
`equal_weight_concurrent_v1`, **holding signals, corpus, cost model, benchmark
rule, namespace and both arms identical** — a sizing comparison across any other
difference is not a sizing comparison. Report the full criterion-7 set:
expectancy, profit factor, CAGR, annualised volatility, Sharpe, Sortino,
portfolio drawdown, exposure, turnover, `return_vs_buy_and_hold_pct` and the
deflated Sharpe.

⚠ A sizing rule that raises return and drawdown proportionally has changed
nothing — the ratio is the result, not the return. ⚠ And hashing `sizing_rule`
into the identity prevents a silent swap; it does **not** prevent a new arm
being *presented* as an improvement against a differently-configured baseline.
That is a discipline on the comparison, not a property of the hash.

---

## 2. Regime gating — when not to bet

Currently a strategy fires whenever its condition is met. The measured evidence
says its edge is not present at all times.

Reproduced on our corpus (Nagel, *Evaporating Liquidity*, RFS 25(7) 2012), alpha
by trailing SPY realised-vol quartile, day-clustered, n=4,904. ⚠ The universe,
period, horizon, alpha definition and non-overlap handling are recorded in
`.claude/skills/data-sources/market-structure.md`, not here — **re-measure from
there before building on the figures**, since a number quoted second-hand is how
the t 50.3 → 17.7 → 5.1 collapse happened:

```text
Q1      Q2      Q3      Q4
56.6    57.0    79.8   184.7   bps    <- monotone, 3.3x across the range
```

Equal weight takes the same notional in all four quartiles. The objection is not
that Q1 is sized like Q4 — it is that **expected return and risk are not
conditioned on the regime at all.** The gate: compute the regime, then either
suppress the fire or scale it.

Regimes computable **today** from data we hold, none of them requiring new
ingest:

| regime | source | note |
| --- | --- | --- |
| market volatility | trailing realised vol of SPY (#2398) | the Nagel conditioner above |
| market trend | SPY vs its own 200d | the crudest and most robust |
| dispersion | cross-sectional stdev of daily returns | ⚠ over **point-in-time eligible** names only, else the panel's changing composition moves the statistic. High dispersion favours cross-sectional (S-2), low favours time-series (S-1) |
| correlation regime | mean pairwise correlation | same point-in-time rule. When everything moves together, diversification is fictional and sizing must fall |
| sector rotation | the 16 comparator series (#2398, 102,027 bars) | ⚠ coverage stops **2024-09-27** |

⚠⚠ **THE COMPARATOR COVERAGE ENDS BEFORE THE CORPUS DOES.** SPY and the sector
series run to **2024-09-27**; the evaluation window runs to **2026-07-08**. So
roughly the last 21 months of the corpus has **no market leg at all** — no beta,
no relative strength, no market-vol regime. Every regime series must therefore
be **fail-closed on absence**: a fire on a date with no market leg is recorded
as `regime_unavailable` and excluded from the conditioning cell, never
back-filled with the last known value. ⚠ A carried-forward regime is a stale
regime, and this repo has already shipped that bug once (#1817, a benchmark
whose stale closes rendered as a computed 0%).

⚠ **`price_intraday` is 0 rows and there is no options data**, so VIX-style
forward-looking vol and the whole vol-surface conditioner are unavailable. The
trailing realised proxy is what we can build; say so rather than implying we
have the real thing.

---

## 3. Conditioning — and the rule that stops it becoming a curve fit

The operator's framing, which is exactly right and is adopted as the acceptance
rule: *"No point saying this strategy works and the success is on 2 instruments
only, or technology instruments in the \$100-200 range where their beta is
negative."*

**The study.** Take every historical fire point of every strategy and join the
state at that moment.

⚠⚠ **THE FIRE POINTS ARE NOT STORED, AND THIS SPEC ORIGINALLY CLAIMED THEY WERE**
(caught at Codex checkpoint 1). `strategy_signals` holds **0 rows**;
`backtest_run` computes signals in-pass via `_signals_for` and discards them.
So §3 needs a **fire-point writer** before it can begin — either the corpus pass
persists each fire with its joined state, or the study re-runs the pass. That is
a prerequisite, not a detail, and it moves §3 later in the build order.

The state to join at each fire:

- price band, dollar volume, spread band
- trailing volatility, beta and R² vs SPY, relative strength
- sector, listing venue, market cap band
- 13F flow QoQ, insider net 90d, RegSHO short volume
- days to / from the nearest filing event
- the market regime of §2

⚠ Availability is **not uniform** across these and must be established per
field before the study is designed, not assumed: price band, dollar volume and
trailing vol come from the bars; beta / R² / relative strength depend on
comparator coverage (§2's date bound); 13F flow, insider net, RegSHO short
volume and filing distance are stored but their join to an arbitrary historical
date is unproven. **Each field ships with a measured coverage rate over the fire
population, and a field below coverage is dropped rather than imputed.**

Then ask, per cell: **what is the expectancy, and does it survive?**

### 3.1 A cell must earn belief — four tests, all required

⚠⚠ This section is the whole point. A conditioning study without it is a
machine for producing beautiful, false cells.

1. **Breadth.** ≥ 30 distinct instruments and ≥ 100 distinct trading days in the
   cell. A cell carried by 2 names is a fact about those 2 names.
2. **Neighbour monotonicity.** The effect must be *smooth or monotone across
   adjacent cells*, not a spike. Nagel's `56.6 / 57.0 / 79.8 / 184.7` is
   believable **because it is monotone** — a gradient implies a mechanism. A
   single bucket that pays while both its neighbours do not is noise wearing a
   hypothesis.
3. **Out-of-sample persistence.** The cell is fitted in-sample and must hold in
   the hold-out. ⚠ One hold-out look per cell set, declared in
   `trial_register.py` *before* the in-sample fit.
4. **A stated mechanism.** Why *should* this cell pay? Liquidity provision,
   risk transfer, forced selling, information diffusion. ⚠ Not decoration: a
   cell with no mechanism is the one that vanishes, and requiring the sentence
   is what stops the cell count exploding.

### 3.2 Instrument-level persistence — the "handful of instruments" question

Traders who specialise in a few names are either exploiting real
instrument-level persistence, or overfitting. **This is directly testable and we
should test it rather than assume either:**

> Split the sample in half by time. Rank instruments by the strategy's
> per-instrument expectancy in the first half. Does that rank predict expectancy
> in the second half?

- **Rank correlation ≈ 0** → the "good instruments" are noise, instrument
  selection is overfitting, and breadth is the only defence.
- **Rank correlation materially positive** → instrument selection is a real
  edge, and a universe filter belongs in the strategy definition.

⚠ Either answer is worth having and changes the design.

⚠⚠ **BUT IT CANNOT SETTLE THE QUESTION ON SURVIVOR-ONLY DATA** (Codex checkpoint
1). The instruments absent from our universe are the ones that died — which is
precisely the population where a strategy's second-half performance collapses.
So first-half rank predicting second-half rank is measured only over names that
survived both halves, and the bias runs in the flattering direction. Run it
early as **exploratory machinery**; it may not justify a universe filter until
#2437 Tier 1 lands.

---

## 4. Correlation-aware portfolio construction

Equal weight across concurrent positions assumes the positions are independent.
When a strategy fires on 40 names in one sector on one day, the common factor
exposure is far larger than 40 independent bets would imply — ⚠ not literally
one bet at 40x, since idiosyncratic residuals remain, but the difference has to
be **measured from the covariance** rather than assumed either way.

Needed: a concurrent-position correlation estimate, and either a cap on
aggregate exposure per sector / per factor, or a covariance-aware weighting.
⚠ The estimate must be **causal** — trailing window only, `1 PRECEDING`, the
same rule §2's beta uses.

---

## 5. Execution precision — the TA half

The operator's list — *"when the bottom is in, where the support is, when to get
out, Fibonacci, testing the lines"* — maps onto primitives that **already exist**
in `app/services/price_structure.py` (`price-structure-v1`, #2279) and are
**not consumed by any shipped strategy** (they have tests and callers in
services, but no runnable strategy reads them):

| concept | ours | ⚠ the trap, from `market-structure.md` |
| --- | --- | --- |
| support / resistance | `cluster_levels`, single-linkage, `CLUSTER_ATR_K = 0.5` | **no published formulation exists** — fixed by construction, frozen in a version hash. Do not invent a citation |
| "the bottom is in" | `detect_swings(bars, n)` | ⚠⚠ **LOOK-AHEAD.** A pivot at `i` needs `2n` neighbours, so it is only knowable `n` bars later. "The last swing low" is not knowable in real time and S-6 fell into exactly this |
| testing the line | `find_break_and_retest`, `classify_interaction` | by construction; same freezing rule |
| Fibonacci | `fib_levels`, `FIB_RATIOS` | ⚠ **0.5 is not a Fibonacci ratio** — it is Dow Theory's halfway retracement. Inherits the swing look-ahead, since a leg needs two confirmed pivots |
| where to get out | `ExitLevels` (`outcome_resolver.py`) | ⚠ the dataclass exists; **nothing computes strategy-specific levels to build one**, which is why S-4 cannot run |

**So the honest position on "what the TA traders have":** we have daily
approximations of the same primitives. ⚠ Not the same inputs — a discretionary
trader reads intraday tape and often the order book, and we have **no intraday
bars at all**, so anything anchored to session structure is out of reach.

What we can match, and where the automatable advantage is, is the part that is
arithmetic rather than judgement: position sizing, regime awareness, and the
discipline to skip the setup when conditions are wrong. That is §1, §2 and §3.

⚠ The counterweight, stated once and not repeated: McLean & Pontiff (JF 2016)
measure published anomalies decaying **58% post-publication**, ~32 points of it
attributed to publication-informed trading. That is a measured average across
their sample, not a universal law about every published pattern. The
implication we act on is directional: the edge is less likely to sit in the
pattern itself than in the conditioning and the sizing, which is where §1-§3
point.

---

## 6. Build order

Re-ordered after Codex checkpoint 1, which broke the original sequence: §3.2
cannot lead, because the fire points are not stored and the survivor-only
universe biases it in the flattering direction.

1. **§2 regime series** — SPY vol / trend / dispersion / correlation as stored
   daily series, fail-closed after 2024-09-27. Feeds everything downstream and
   depends on nothing.
2. **§1 `inverse_vol_v1`** — the sizing floor, measured as its own arm. Needs no
   conditioning work and no new data.
3. **The fire-point writer** — persist each fire with its joined state and the
   per-field coverage rate. ⚠ §3 cannot start without it.
4. **§3.2 instrument-persistence** — exploratory on survivor-only data;
   re-run and only then believed once #2437 Tier 1 lands.
5. **§3 conditioning study** with the §3.1 acceptance rules and pre-registration.
6. **§4 correlation-aware construction.**
7. **§1 `conviction_weighted_v1`** — last, because it depends on §3 holding out
   of sample.

⚠ All of the above is measured on the **survivor-only** universe until #2437's
Tier 1 lands, so every number is provisional and every arm will need re-running.
That does not block starting — the machinery is what is being built here — but
no cell may be declared stable on survivor-only data.
