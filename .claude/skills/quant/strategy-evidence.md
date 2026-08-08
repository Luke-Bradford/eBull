# quant/strategy-evidence

## When to use

**MANDATORY before proposing, speccing or defending any trading strategy.** Read
it the moment a ticket says "new strategy", "signal", "edge", "alpha", "factor",
"backtest looks good", or proposes tuning an existing strategy.

It exists because the replication literature has already tested most of what we
would think of, and it says most of it does not work. Speccing without that
prior is how a session spends a week on a family the evidence killed in 2016.

> **The rule this file enforces:** a strategy proposal cites either (a) the
> evidence that its FAMILY survives, or (b) the mechanism and the measurement
> that will test it. **"The backtest looks good" is neither.**

⚠ Companion to `data-sources/market-structure.md`, which owns *indicator
formulations*. This file owns *whether a strategy family is worth building*.

---

## 1. ⚠⚠ The four filters, and our own three strategies scored against them

Each filter is a published result. The scores are **our own stored numbers**,
recomputed — never quoted from here without re-running.

| filter | source | s1 time-series mom | s2 cross-sectional mom | s3 mean-reversion |
| --- | --- | --- | --- | --- |
| **turnover < 50%/month** | Novy-Marx & Velikov | ✗ **600%/mo (12× over)** | ✓ **35%/mo INSIDE** | ✗ 333%/mo (6.7× over) |
| **family survives costs** | Frazzini/Israel/Moskowitz | ✓ momentum most scalable | ✓ momentum most scalable | ✗ **reversal is the MOST constrained** |
| **responds to vol scaling** | Cederburg et al. | ✓ momentum is the exception that works | ✓ same | ✗ not in the working subset |
| **construction not microcap-inflated** | Hou/Xue/Zhang | ⚠ indirect (equal-weight pooling) | ⚠ **direct hit** (cross-sectional sort, `MIN_CLOSE=1.0`) | ⚠ indirect |

**Our measured result: s2 is the only one of the three that beats buy-and-hold.**

⚠⚠ **The literature predicted that, ex ante, on four independent axes.** That is
the strongest evidence in this file — not that any single paper is right, but
that four unrelated results converge on the same ranking our own backtest
produced. Treat the framework as load-bearing.

Reproduce the turnover column:

```sql
SELECT strategy_id, turnover_annualised, turnover_annualised / 12 * 100 AS pct_per_month
FROM strategy_results_store
WHERE benchmark_rule = 'equal_weight_buy_and_hold_v1' AND ambiguity_arm = 'best_case'
ORDER BY turnover_annualised;
```

---

## 2. The results that should change a design decision

### 2.1 Turnover is the first-order filter — Novy-Marx & Velikov, *RFS* 29(1) (2016)

> *"Most anomalies with less than 50% turnover per month generate significant net
> spreads when designed to mitigate transaction costs; few with higher turnover
> do."*

Execution costs 20-57 bps for mid-turnover anomalies. Size, value and
profitability have the greatest capacity for new capital.

⚠⚠ **The mitigation is a design pattern we do not have: a BUY/HOLD SPREAD —
"more stringent requirements for establishing positions than for maintaining
them."** Named by the paper as *the most effective* cost-mitigation technique. It
is a small change to any strategy's exit rule and it directly attacks the filter
we fail worst on.

> **Check turnover BEFORE anything else.** It is one column, it is already
> stored, and it disqualifies faster than any backtest.

### 2.2 Most anomalies do not replicate — Hou, Xue & Zhang, *RFS* (2020)

452 anomalies. With microcaps mitigated via **NYSE breakpoints and value
weighting**, **65%** fail |t| > 1.96; **82%** fail at 2.78. *"Anomalies in
microcaps are more apparent than real."* Replicated ones have *"much smaller"*
magnitudes than published.

⚠ Ours: equal weight, no value weighting, **24% of the universe below $5**.
Directly applicable to cross-sectional sorts (s2); reaches s1/s3 as small-stock
microstructure bias via equal-weight pooling.

### 2.3 The significance bar is t > 3.0 — Harvey, Liu & Zhu, *RFS* (2016)

316 factors across 313 articles; after multiple-testing correction the hurdle is
**t > 3.0**, and **only 9 of 313 survive**.

⚠ The debate is live — Chen (*"Most claimed statistical findings … are likely
true"*) argues the other way. We adopt the strict side on asymmetry of
consequences, not on certainty.

### 2.4 Volatility scaling — Moreira & Muir (2017) vs Cederburg et al. (2020)

Moreira & Muir, *JF* 72(4): scale by inverse of last month's realised variance →
large alphas across eight factors, because *"changes in volatility are not offset
by proportional changes in expected returns."*

⚠⚠ Cederburg, O'Doherty, Wang & Yan, *JFE* 138(1), **103 strategies**: the
out-of-sample versions *"generally earn lower certainty equivalent returns and
Sharpe ratios"* than unmanaged — the spanning alphas are *"not implementable in
real time"*. **Exception: it does work for momentum, profitability and BAB.**

> **Pre-registered prediction for our four-arm test:** `inverse_vol_v1` helps s1
> and s2, not s3. ⚠ If it helps s3 most, that is a reason for suspicion, not
> celebration.

### 2.5 Real costs are lower than academia thinks — except for reversal

Frazzini, Israel & Moskowitz, ~**\$1 trillion** of live trading data, 19 markets,
1998-2011: actual costs *"less than a tenth as large as previous studies
suggest"*; capacity an order of magnitude larger; value and momentum more
scalable than size; ⚠ **"short-term reversals are the most constrained by
trading costs."**

⚠ Their costs are low because they execute like an institution. **Ours are
worse**, so every constraint here binds harder, not softer.

### 2.6 Published edges decay — McLean & Pontiff, *JF* (2016)

**26% lower out-of-sample, 58% lower post-publication**, ~32 points attributable
to publication-informed trading. ⚠ A measured average, not a universal law.

### 2.7 The gains are in interactions — Gu, Kelly & Xiu, *RFS* 33(5) (2020)

Trees and neural nets best; NN portfolio OOS Sharpe **0.77 vs 0.51**
buy-and-hold; long-short decile **1.35 VW / 1.45 EW**. Gains come from
*"nonlinear predictor interactions"*, and the dominant signals across all
methods are **momentum, liquidity and volatility**.

> Read as: **conditioning beats a better single signal**, and the three signals
> that matter are ones we already hold. ⚠ Not a licence to reach for ML — that
> multiplies the trial count before the measurement is fixed.

---

## 2.8 ⚠⚠ The one structural fact that organises the whole catalogue

**The sign of return autocorrelation flips with horizon.** This is the closest
thing to a law in the field, and every strategy family is a bet on one segment
of it:

| horizon | sign | mechanism | source | ours |
| --- | --- | --- | --- | --- |
| 1 day – 1 month | **negative** (reversal) | liquidity provision, bid-ask bounce | Jegadeesh (1990) | s3 |
| 3 – 12 months | **positive** (momentum) | underreaction, slow diffusion | Jegadeesh & Titman (1993) | s1, s2 |
| 3 – 5 years | **negative** (reversal) | overreaction — this is what *value* is | De Bondt & Thaler (1985) | ⚠ **nothing** |

Two consequences worth acting on:

1. ⚠ **s1 and s3 sit on opposite sides of the curve.** One bets continuation,
   the other reversal. Running both without conditioning means they take
   opposing positions in the same name, and the sleeve pays costs on both.
2. ⚠⚠ **We own nothing at the 3-5 year end** — which is the LOWEST-turnover
   corner and therefore the one most likely to survive §2.1's filter, and the
   one where our XBRL fundamentals are the right data rather than a bystander.
   **The biggest gap in the catalogue is the horizon we have not tried.**

## 2.9 Most measures are redundant — Green, Hand & Zhang (2017)

94 firm characteristics tested **simultaneously** in Fama-MacBeth regressions.
Only **12** are reliably independent determinants over 1980-2014 — and
⚠⚠ **since 2003, just TWO.**

> **This is the answer to "combine every metric we have".** The combination adds
> little because the characteristics are collinear: they are mostly the same
> few signals wearing different names. And **1/N — equal weighting the signals —
> is a hard benchmark that optimised weights routinely fail to beat.**

⚠ Read as a *budget*, not a prohibition: aim for a small number of genuinely
independent signals, equal-weighted, and prove independence rather than assuming
it. A 40-feature model is 38 features of noise and a multiple-testing problem.

## 2.10 Factor timing is harder than it looks — and our vol result is the exception

**Asness, Chandra, Ilmanen & Israel, "Contrarian Factor Timing is Deceptively
Difficult", *JPM* (2017).** Timing factor exposures on valuation spreads gives
*"somewhat disappointing"* results, and — the part that matters —
**"as the baseline portfolio became more diversified with more factors, it became
progressively harder for value timing to improve performance."** Trading
simulations failed to produce economically meaningful gains in gross return or
Sharpe.

⚠⚠ **This tempers the regime-gating ambition and must not be read as refuting
it.** Two different things:

- **Volatility conditioning** — a risk/liquidity mechanism at short horizons.
  Nagel's result reproduces monotonically on our own data (56.6 / 57.0 / 79.8 /
  **184.7** bps). Evidence FOR.
- **Contrarian valuation timing of factors** — a slow bet that a cheap factor
  mean-reverts. Evidence AGAINST.

> **Rule: gate on volatility and liquidity, where the evidence is. Do not
> generalise to "regime gating works" and start timing factors on spreads.**

## 2.11 You can see part of the order flow without the tape — Amihud (2002)

**Amihud, "Illiquidity and stock returns", *JFM* 5 (2002), 31-56.** The measure
is `mean(|daily return| / daily dollar volume)` — ⚠ **daily data only, no tick
data, no order book.** It proxies price impact per unit of order flow (the
empirical cousin of Kyle's λ) and is *"a relatively strong indicator of informed
trading when trading is urgent."*

Three uses, all available to us on 25.9M bars with nothing new ingested:

1. **A conditioner** — illiquid names are where limits to arbitrage bind, which
   is where §2.6's surviving anomalies survive.
2. **A per-name cost model**, replacing the flat ~50.9 bps round trip with
   something that varies the way real costs do. ⚠ Our current flat charge
   under-penalises exactly the microcaps §2.2 says are inflating us.
3. **A priced factor in its own right** — the illiquidity premium.

⚠ It is a *proxy*, not the tape. It cannot see a specific participant, and
nothing at daily resolution can. What it measures is how much price moved per
dollar traded, which is the footprint, not the actor.

---

## 3. Family viability, with the evidence attached

| family | evidence | our data | verdict |
| --- | --- | --- | --- |
| **insider purchases** | ⚠⚠ **the best-evidenced family we can actually build.** Lakonishok & Lee (2001): heavy insider buying beats the market ~6%/12mo. Jeng, Metrick & Zeckhauser (2003): ~11.2%/yr abnormal on purchases, **sales show no effect**. Cohen, Malloy & Pomorski: **routine trades have ZERO predictive power; opportunistic trades pay 82 bps/month** (~10%/yr) — ~4× the undifferentiated signal | Form 4 tables held | **build** — ⚠ purchases only, and the routine/opportunistic split is the whole edge |
| cross-sectional momentum | survives costs, scalable, responds to vol scaling | shipped (s2) | keep, re-measure per §2.2 |
| time-series momentum | family survives, but ⚠ our turnover is 12× the bar | shipped (s1) | ⚠ fix turnover or drop |
| short-term reversal | ⚠ most cost-constrained family; ours is 6.7× the turnover bar | shipped (s3) | ⚠ hardest case in the catalogue |
| PEAD | *"largely disappeared in many segments"*, persists only where limits to arbitrage bind | event + XBRL, ⚠ **no analyst estimates** | conditional at best |
| 13F flow / crowding | ⚠ mixed. Cloning ≈ market returns; the 45-day lag is the design problem, and institutions *deliberately* delay to deter copycats | 7M rows | ⚠ crowding/unwind angle only, not cloning |
| filing text | — | ⚠ **NOT held** — `filing_documents` is a manifest of URLs, no bodies | needs a fetch pipeline first |
| short volume | — | RegSHO daily flow; ⚠ not short interest, not borrow | proxy only |

---

## 3.1 Insider purchases — the family measured against our own data

**Cohen, Malloy & Pomorski, "Decoding Inside Information", *JF* 67(3) (2012),
1009-1043.** Routine trades — an insider with a **history of trading the same
calendar month across years** — are **over half the universe** and carry
*"essentially zero"* abnormal return. Strip them out and the remainder yields
**82 bps/month value-weighted**. *"Opportunistic trades predict future news and
events at a firm level, while routine trades do not."*

⚠ Two conditioning details that invert intuition and are checkable on our
`filer_role` column: the most informed opportunistic traders are **local,
NON-EXECUTIVE insiders from geographically concentrated, poorly governed
firms** — not the CEO.

**Measured on `insider_transactions` (1,052,947 rows), 2026-08-08:**

```text
distinct insiders                                        55,001
purchases (txn_code = 'P')                               48,278
sales     (txn_code = 'S')                              243,068
insiders with >= 3 distinct PURCHASE years                  997 of 9,936 (10.0%)
```

⚠⚠ **Two things this measurement changes, and neither was obvious:**

1. **The usable population is 48,278, not 1M.** Jeng/Metrick/Zeckhauser find
   purchases carry the signal and *"insider sales showed no comparable effect"* —
   and sales outnumber purchases **5:1** here. Anyone sizing this family off the
   row count will be out by a factor of 20.
2. **Only 10% of purchasing insiders have the ≥3 years of history CMP's routine
   test needs.** ⚠ That is not necessarily fatal — CMP's routine label requires
   an *established pattern*, so an insider with no pattern is arguably
   opportunistic by construction rather than unclassifiable. **But that is an
   inference about their method, not a quote from it — verify against the paper
   before building on it**, because the alternative reading (exclude the
   unclassifiable 90%) leaves a very different population.

⚠ Data defect found while measuring this: **20 rows carry two-digit-year
`txn_date`s** (year 23 in `-24-` accessions) that `txn_date_invalid` does not
flag. Negligible as a share, material for this family specifically — a
2,000-year gap wrecks the per-insider history the classification depends on.
Filed as **#2441**.

---

## 4. The checklist before speccing any strategy

1. **Turnover first.** Above ~50%/month, the evidence says do not bother unless a
   buy/hold spread brings it down.
2. **Name the forced participant.** No mechanism → it is a pattern → §2.2 applies.
3. **Check the family** against §3 before designing the signal.
4. **Declare the trial** to `trial_register.py` *before* measuring (10 trials →
   0.0833 bar; 1,000 → 0.1738; 100,000 → 0.2344).
5. **Value-weight or size-screen**, or expect §2.2 to explain your result for you.
6. **Verify every number against our own data.** ⚠ Everything in this file is a
   prior. The repo's rule stands: *grep before cite, run it or delete the
   quantifier.*

---

## 5. Open-source landscape

- **Qlib** (Microsoft) — closest to this design; ML pipeline, cross-sectional
  stock selection, DataServer ~10× pandas on time series. **Evaluate before
  hand-rolling more infrastructure.**
- **vectorbt** — ⚠ **measured and REJECTED**, reasons in `equity_curve.py`:
  forcing `freq="1D"` imposes annualisation of exactly 365 against our ~196
  observations/year, inflating Sharpe **1.37×**. Do not revisit without new
  evidence.
- **zipline-reloaded** (original dead since Quantopian closed), **backtrader**
  (event-driven, no cross-sectional strength).
- **TradeMaster / FinRL** — RL platforms. ⚠ RL multiplies the trial-count
  problem rather than solving it.
- **Quantpedia** — 900-1,200+ strategies, a subset replicated in QuantConnect.
  Use as a **hypothesis source with mechanisms attached**, never a shopping
  list. Every one taken is a declared trial.
