# The strategy landscape — what actually survives, where OUR edge plausibly is, and how to look for one

Refs #2437. Companion to `2026-08-08-edge-construction.md` (how to bet) and
`strategy-catalogue-and-backtest-validity.md` (whether a strategy works). This
one asks the prior question: **which strategies are worth building at all, given
the data we hold and the capital we run.**

Operator challenge (2026-08-08): *"3 strategies, with 2 of them sounding like
garbage… what strategies are there, have we researched this… at your core, think
you need to make money in a market."* Fair, and the literature sweep had not been
done. It has now.

---

## 1. ⚠⚠ The finding that indicts our current construction

**Hou, Xue & Zhang, "Replicating Anomalies", *Review of Financial Studies* (2020)**
— the largest replication study in finance, 452 anomalies:

> With microcaps mitigated via **NYSE breakpoints and value-weighted returns**,
> **65%** of the 452 anomalies cannot clear the single-test hurdle of |t| > 1.96.
> At the multiple-test hurdle of 2.78, the failure rate rises to **82%**.
> …Microcaps have the highest equal-weighted returns but the largest
> cross-sectional dispersion. **"Anomalies in microcaps are more apparent than
> real."**

Now measure our own setup:

```text
sizing rule ever used in strategy_results_store   equal_weight_concurrent_v1
instruments in the universe trading below $5      2,943 of 12,185  (24%)
size screen                                       S-2 only: MIN_CLOSE = 1.0
value weighting                                   none
NYSE-breakpoint equivalent                        none
```

⚠ **Scope this claim precisely — the first draft overstated it.** HXZ is about
**cross-sectional anomaly sorts**, so it maps *directly* onto **S-2**:
cross-sectional ranking, equal-weighted eligible names, no NYSE breakpoints, and
a \$1 close floor that is far below the microcap boundary the paper is about.

For **S-1 and S-3** it maps only *indirectly*: they are time-series strategies
whose positions are then pooled by `equal_weight_concurrent_v1`. The failure
mode there is not literally anomaly-replication — it is **small-stock
microstructure and rebalancing bias**, which is the same underlying disease
(bid-ask bounce in illiquid names, amplified by equal weighting) reached by a
different route.

Either way the corrective is identical, and that is why the distinction does not
change the plan: **screen on size, and weight by something other than equal.**

⚠ And it is the **second independent line of evidence** for the same conclusion.
Our own research pass (`market-structure.md`) found intraday expectancy negative
**only below \$5** — `<$5` at −21.90%/yr against `>=$100` at +19.88%/yr — and
concluded the corpus-wide result was *"penny-stock print noise outvoting every
tradable name."* Two different routes, same answer.

**Consequence, and it is a design change, not a caveat:** every strategy result
we hold is provisional until re-measured with a size screen and value or
inverse-vol weighting. This is cheap, it needs no new data, and it should
precede building any new strategy.

---

## 2. What actually survives

Two things are true at once and both matter:

- **Most published anomalies do not replicate** (§1), and of those that do,
  *"economic magnitudes are much smaller than originally reported."*
- **A short list does survive, including after trading costs.** Size, value,
  momentum and short-term reversal survive transaction costs; fundamentals,
  earnings revisions and momentum retained significance 2003-2018 including
  post-publication.

And the decay is measured: **McLean & Pontiff (JF 2016)** — 26% lower
out-of-sample, **58% lower post-publication**, ~32 points attributable to
publication-informed trading.

⚠ **PEAD is the cautionary case.** It is the textbook "safe" anomaly and the
evidence says it *"has largely disappeared in many segments of the market"*,
persisting only where limits to arbitrage bind. So it is not a free win — it is a
conditioning problem, which is exactly §3's shape.

**Reading for us:** do not expect a plain published anomaly to pay. Expect the
survivors to pay *in the segments where arbitrage is constrained*, which is a
statement about conditioning and about which names, not about the pattern.

---

## 3. Where OUR edge plausibly is — and it is not chart patterns

This is the part I have been under-weighting, and it is the direct answer to
*"what strategies could we have"*.

**Look at what we actually hold:**

```text
filing_documents                   9,243,776
ownership_institutions_*           ~7,000,000   (13F, quarterly, 2024q3+)
filing_events                      2,764,591
sec_filing_manifest                2,613,520
financial_facts_raw_*              ~2,000,000   (XBRL)
finra_regsho_daily_observations      539,388    (daily short volume)
ownership_insiders_*                 ~300,000
def14a_beneficial_holdings           110,832
research_price_daily              25,927,473
```

versus what we do **not** hold: **no intraday bars** (`price_intraday` = 0 rows),
no order book, no options/implied vol, no borrow cost, no analyst estimates.

**Hypothesis, stated so it can be attacked — and it is currently unproven:**
our highest-expectancy strategies are event-driven and cross-sectional, keyed on
filings, ownership flow and positioning, rather than on price geometry.

The argument for it: at daily resolution with no tape and no book, classical TA
is the axis where we are *weakest* relative to the rest of the market — every
participant has daily OHLC and the ones who matter have far more — while a
7-million-row institutional-ownership panel joined to XBRL fundamentals is
unusual at retail scale.

⚠⚠ **But data possession is not edge, and this is a just-so story until
measured** (Codex, checkpoint 1). Nothing here shows a filings-derived signal
beats a dumb price or fundamental baseline *after* reporting lag, costs,
coverage and survivorship — and the 13F lag alone (45 days) is enough to kill
the naive version. **Treat §3 as the reason to run the experiment, not as its
result.** The falsification is the same in both directions: build one
filings-keyed signal and one price baseline, measure both under identical
conditions.

⚠ This does **not** mean abandon TA. Price structure is the right *conditioner
and execution layer* — where to enter, where the stop goes, whether the regime
supports the trade. It is a poor *primary signal* for us.

---

## 4. The families worth building, with mechanism

⚠ A strategy without a stated mechanism — **who is forced to trade, and why** —
is a pattern, and patterns are what §1 says do not replicate. Mechanism first,
every time.

| # | family | mechanism (who is forced, why) | our data | feasible now |
| --- | --- | --- | --- | --- |
| A | **cross-sectional momentum** (S-2) | slow information diffusion; institutional herding | bars | ✅ shipped |
| B | **time-series momentum** (S-1) | trend-following flows, risk management | bars | ✅ shipped |
| C | **short-term reversal** (S-3) | **liquidity provision** — paid for absorbing forced flow. Nagel: predictable with vol, reproduced here 56.6/57.0/79.8/**184.7** bps by vol quartile | bars | ✅ shipped, but see §1 |
| D | **volatility-compression breakout** (S-4) | vol clustering; option-hedging feedback | bars + ATR | ⚠ blocked — nothing computes `ExitLevels` |
| E | **support/resistance retest** (S-5) | order clustering at round/prior levels | `price_structure.py` | ✅ **unblocked** (#2279) |
| F | **Fibonacci retracement** (S-6) | ⚠ weakest mechanism of the set — largely self-fulfilling | `price_structure.py` | ✅ unblocked; ⚠ inherits swing look-ahead |
| G | **post-earnings-announcement drift** | under-reaction to earnings news; limited arbitrage in small/illiquid names | `filing_events` + XBRL | ⚠ **partial** — we hold the event and the reported figure but NOT analyst estimates, so there is no consensus to surprise against. Buildable as a *realised*-surprise proxy against own history; the standard construction needs §6 item 4 |
| H | **13F flow / crowding** | quarterly institutional accumulation is slow and visible; crowded names unwind together | 7M ownership rows | ✅ held; ⚠ the **45-day reporting lag** is the whole design problem — anything a filing reveals is 45+ days stale |
| I | **insider transactions** | genuine information asymmetry; the one legal edge that is *definitionally* informed | Form 4/3 tables | ✅ data held |
| J | **short-VOLUME pressure** | forced covering; borrow constraint | RegSHO daily | ⚠ **proxy only.** The table holds `short_volume`, `short_exempt_volume`, `total_volume` — daily FLOW. It is **not short interest**, not borrow cost, not days-to-cover. A real squeeze strategy needs §6 item 3 |
| K | **index / ETF rebalance** | forced, calendar-known, price-insensitive buying | membership history (#2290) | ⚠ needs index membership |
| L | **accruals / earnings quality** | investors over-weight accruals vs cash flow | XBRL facts | ✅ data held |
| M | **filing-text change** | 10-K/10-Q language shifts predict returns; under-read because it is laborious | ⚠ **NOT held** | ⚠⚠ **The biggest error in this doc's first draft.** `filing_documents` is a **manifest** — `document_name`, `document_type`, `document_url`, `size_bytes`. **No text bodies.** Those 9.2M rows are pointers, not a corpus. Needs a fetch-and-store pipeline before it is a strategy at all |

**Honest count: ~13 families, not 900.** Quantpedia catalogues **900-1,200+**
quantified strategies (many with open-source QuantConnect implementations), but
that number is mostly parameterisations and asset-class variants of a much
smaller set of mechanisms. ⚠ And the trial arithmetic is unforgiving — testing
1,000 raises the deflated-Sharpe bar to **0.1738**, testing 100,000 to **0.2344**.
Breadth of *hypotheses tested* is a liability. Breadth of *bets held* is the asset.

---

## 5. How to look for an entry, programmatically

The method, stated concretely because it was asked for directly.

**Not** "scan for repeating patterns and keep the ones that paid" — that is a
trial-generating machine whose output is indistinguishable from noise at our
sample sizes.

**Instead, five steps, in order:**

1. **Name the forced participant.** Who *has* to trade, against their own
   interest, and why? Index funds at a rebalance. A fund meeting redemptions. A
   market maker absorbing a block. An investor under-reacting to an 8-K. If no
   one is forced, there is no reason for a price to be wrong.
2. **Find the observable that proxies the force**, and check it is *causal* —
   knowable at the decision bar. ⚠ Our own trap #1: never sort on a variable
   that terminates at price P and measure an outcome starting at P.
3. **Measure the response function, not a threshold.** Bucket the observable and
   look at the *shape*. Monotone → mechanism. A single paying bucket → noise.
   This is why Nagel's `56.6/57.0/79.8/184.7` is believable.
4. **Check the fill is reachable.** ⚠ Our own trap #2: a measured +43.9 bps lived
   entirely inside `close(t) → open(t+1)` and was consumed before any fill could
   exist. Measure from the first price you could actually transact at.
5. **Check it survives costs at size**, using the ~50.9 bps round trip — and
   under §1's rules, value-weighted with a size screen.

Only then does the chart matter, and it matters for *execution*: where the stop
goes, whether the level held, whether the regime supports it.

---

## 6. What data I would bring in, ranked by expected value

1. **Survivorship-free universe** — #2437 Tier 1. Not new data so much as the
   missing quarter of it. Everything else is measured wrong without it.
2. **Options / implied volatility** — the single biggest missing conditioner.
   IV rank, skew and term structure are the highest-information regime variables
   available, and Nagel's result says our best-understood edge is *vol-conditioned*.
3. **Borrow cost / short availability** — decides whether family J is a real
   edge or just an expensive one. RegSHO gives volume, not cost.
4. **Analyst estimates + revisions** — the standard PEAD companion; without it
   family G is running on half its inputs.
5. **Index membership history** — unlocks family K, which is the cleanest forced
   flow in the market. (#2290 shipped the membership table; it is empty by design.)
6. **Intraday bars** — expensive, and it unlocks a family where we are least
   differentiated. ⚠ Ranked last deliberately, against instinct.

---

## 7. What I would do first, and the thesis I am putting up to be falsified

**Thesis:** *the largest available improvement is not a new strategy — it is
re-measuring the three we have under a size screen with value or inverse-vol
weighting, because §1 says our current construction inflates exactly the segment
we most heavily weight.*

**⚠⚠ REDESIGNED after the evidence review — the first version tested the wrong
variable and would have burned 2h15m for an uninterpretable answer.**

Two errors. **(1) Price is not size.** HXZ's method is *NYSE breakpoints and
value weighting* — market capitalisation. The draft used a `$5` price floor. A
$3 stock can be a $2bn company and a $300 stock can be small, so a price screen
does not test the claim it is aimed at. Market cap is computable from
`share_count_history` (135,683 point-in-time rows) × price. **(2) Two literatures
were collapsed into one arm.** HXZ says value-weight by cap; Moreira & Muir say
inverse-variance weight. Different claims, opposite predictions on s3, and one
arm cannot separate them.

**One arm per hypothesis, each with a directional prediction stated before the
run.** That is what makes a null result informative rather than ambiguous.

| arm | change | tests | ⚠ prediction |
| --- | --- | --- | --- |
| 0 | baseline as today | — | — |
| 1 | market-cap screen, NYSE-breakpoint equivalent | Hou/Xue/Zhang | large move ⇒ our results are microcap artefacts |
| 2 | value weighting (by market cap) | Hou/Xue/Zhang | same direction as arm 1; if 1 and 2 disagree, the effect is weighting, not universe |
| 3 | `inverse_vol_v1` | Moreira-Muir vs Cederburg et al. | **helps s1 and s2, NOT s3.** ⚠ Helping s3 most is a red flag, not a win |
| 4 | **buy/hold spread** — stricter to establish than to maintain | Novy-Marx & Velikov | **the only arm that addresses s1's and s3's actual failure** |

⚠⚠ **Arm 4 is the one that matters for s1 and s3, and the reading is what
revealed it.** Their binding constraint is turnover — 600%/month and 333%/month
against a ~50% bar — and *no size screen or weighting change reduces turnover*.
Arms 1-3 cannot save them even in principle. If budget allowed only one arm for
those two strategies, it is arm 4.

Conversely s2 already sits inside the turnover bar, so arm 4 is largely irrelevant to
it and arms 1-2 are the ones that bite.

**Pre-registered rejection rule**, so the answer cannot be argued afterwards:

- **SUPPORTED** if the arm its prediction names moves `return_vs_buy_and_hold_pct`
  by **more than 50%** of arm 0's absolute value.
- **REJECTED** if every arm lands within **±20%** of arm 0.
- Anything else is **INCONCLUSIVE** and reported as such — ⚠ not reinterpreted
  into whichever story reads better.

All arms hold corpus, cost model, benchmark rule, namespace and both
quarantine/ambiguity arms identical, and are declared to `trial_register.py`
before the first measurement.

⚠ Five arms × ~34 min ≈ **2h50m** unattended. The redesign adds one arm and
removes the risk of learning nothing.

---

## 8. The evidence review — six papers that change what we should build

Added after the operator asked for a real study rather than three searches. Each
entry is here because it **changes a decision**, not because it is famous.

### 8.1 Volatility scaling works — but only on some families, and the field is split

**Moreira & Muir, "Volatility-Managed Portfolios", *Journal of Finance* 72(4)
(2017), 1611-1644.** Scale portfolio weight by the **inverse of last month's
realised daily return variance**. Result: *"large alphas, increase Sharpe ratios,
and produce large utility gains"* across market, value, momentum, profitability,
ROE, investment, betting-against-beta and currency carry. Mechanism: **changes
in volatility are not offset by proportional changes in expected return.**

⚠⚠ **And the direct rebuttal, which matters more than the original for us.**
**Cederburg, O'Doherty, Wang & Yan, "On the performance of volatility-managed
portfolios", *JFE* 138(1) (2020), 95-117** — 103 equity strategies:
volatility-managed portfolios *"do not systematically outperform their
corresponding unmanaged portfolios in direct comparisons."* The spanning-
regression alphas are **not implementable in real time**, and real out-of-sample
versions *"generally earn lower certainty equivalent returns and Sharpe ratios"*.
Cause: structural instability in the spanning regressions.

**But the exception is precisely our case:** volatility management **does** add
value for **momentum (in particular), profitability and BAB**, and not for the
other six factors tested.

> **Decision:** build `inverse_vol_v1` and expect it to help **S-1 and S-2**
> (momentum) and **not S-3**. ⚠ That is a *pre-registered directional
> prediction*, which makes the four-arm test of §7 sharper — if inverse-vol
> helps S-3 most, the literature says be suspicious of the result rather than
> pleased with it.

### 8.2 The honest significance bar is t > 3.0, and almost nothing clears it

**Harvey, Liu & Zhu, "…and the Cross-Section of Expected Returns", *RFS* (2016).**
Catalogues **316 factors across 313 articles** and, allowing for multiple
testing, correlation among tests and publication bias, concludes a new factor
needs **t > 3.0**. At that hurdle **only nine of 313 survive.**

⚠ The debate is **not settled** — Chen, *"Most claimed statistical findings in
cross-sectional return predictability are likely true"* argues the opposite from
the same literature. Both are worth reading; the asymmetry of consequences is
what decides our posture, and the cost of believing a false edge with real money
is higher than the cost of missing a true one.

> **Decision:** our deflated-Sharpe machinery is the right shape, but the bar
> should be calibrated to a **t > 3.0 equivalent**, not 1.96. Combined with the
> trial table (1,000 trials → 0.1738), this is the arithmetic that says *build
> fewer, better-motivated strategies*.

### 8.3 Real trading costs are far lower than academia claims — except for reversal

**Frazzini, Israel & Moskowitz, "Trading Costs of Asset Pricing Anomalies"** —
nearly **\$1 trillion of live trading data**, 19 developed markets, 1998-2011.
Actual costs are *"less than a tenth as large as previous studies suggest"*, so
capacity is *"more than an order of magnitude larger"*. Value and momentum are
**more scalable** than size. ⚠⚠ **"Short-term reversals are the most constrained
by trading costs."**

> **Decision, and it is the second independent indictment of S-3.** §1 said our
> microcap equal-weight construction inflates it; this says its *family* is the
> one costs kill first. S-3 runs `turnover_annualised ≈ 40/yr` with a −99.6%
> drawdown. ⚠ Note the direction of the caveat: their costs are low because they
> are an institution with excellent execution — **ours are worse, not better**,
> so the constraint binds harder on us than on them.

### 8.4 Machine learning genuinely helps, and it points at conditioning

**Gu, Kelly & Xiu, "Empirical Asset Pricing via Machine Learning", *RFS* 33(5)
(2020), 2223-2273.** Trees and neural networks are the best performers, *"in
some cases doubling the performance of leading regression-based strategies"*. A
neural-network portfolio reaches an out-of-sample Sharpe of **0.77 against 0.51
for buy-and-hold**; a long-short decile spread on NN predictions reaches **1.35
value-weighted / 1.45 equal-weighted**.

Two details matter more than the headline:

1. *"Their predictive gains come from allowing **nonlinear predictor
   interactions** missed by other methods."* ⚠ That is precisely the
   conditioning study — an interaction between signal and regime is exactly what
   a cell in §3 is.
2. *"All methods agree on the same set of dominant predictive signals… variations
   on **momentum, liquidity, and volatility**."* We hold all three.

> **Decision:** do **not** reach for ML yet — it multiplies the trial count and
> we have not fixed the measurement. But the finding validates §3's direction:
> the edge is in the *interactions*, not in a better single signal. Build the
> conditioning study first; it is the interpretable version of the same thing.

### 8.5 The open-source landscape — what is worth borrowing

- **Qlib** (Microsoft) — an AI-oriented quantitative investment platform with a
  full ML pipeline and a DataServer benchmarked ~10× faster than pandas for
  time series. ⚠ **The closest thing to what we are building**, and explicitly
  aimed at cross-sectional stock selection. Worth an evaluation pass before we
  hand-roll more infrastructure.
- **vectorbt** — already measured and **rejected** with reasons recorded in
  `equity_curve.py`: its Sharpe/Sortino/vol/return metrics raise
  `ValueError: Index frequency is None` on a real trading calendar, and forcing
  `freq="1D"` imposes an annualisation factor of exactly 365 against our ~196
  observations/year, inflating Sharpe by **1.37×**. Do not revisit without new
  evidence.
- **zipline-reloaded** — community fork; the original is effectively dead since
  Quantopian closed.
- **backtrader** — event-driven, simple, no cross-sectional strength.
- **TradeMaster**, **FinRL** — reinforcement-learning platforms. ⚠ RL multiplies
  the trial-count problem rather than solving it.
- **Quantpedia** — 900-1,200+ quantified strategies, a subset implemented in
  QuantConnect with out-of-sample replications. ⚠ Useful as a **hypothesis
  source with mechanisms attached**, not as a strategy shopping list; every one
  taken from it is a declared trial.

### 8.6 What the six papers change, in one place

| finding | what it changes |
| --- | --- |
| HXZ: 65-82% of anomalies fail under value weighting | the four-arm test of §7 is the top priority |
| Moreira-Muir vs Cederburg et al. | `inverse_vol_v1` predicted to help S-1/S-2, **not** S-3 — a pre-registered direction |
| Harvey-Liu-Zhu: t > 3.0, 9 of 313 survive | calibrate the promotion bar to t > 3.0, build fewer strategies |
| Frazzini-Israel-Moskowitz | S-3's family is the most cost-constrained; momentum is the most scalable |
| Gu-Kelly-Xiu | conditioning (interactions) beats better single signals; ML later, not now |
| Qlib | evaluate before hand-rolling more infrastructure |

⚠ **The convergent finding across four of the six: our two momentum strategies
sit in the families that survive costs, scale, and respond to volatility
management — and our reversal strategy sits in the one that does not.** That is a
stronger prior than anything our own backtest currently supports, and it is
testable with the measurement already specced.
