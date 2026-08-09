# The trade lifecycle, and the strategies that fit our actual constraints

Refs #2437. Produced by a second adversarial Codex pass over
`.claude/skills/quant/strategy-evidence.md`, run with our hard constraints
supplied so nothing proposed needs data we do not receive.

⚠ Read `.claude/skills/quant/strategy-evidence.md` first. This document assumes
its corrections — particularly that **ATR stops are not edge**, that **an
autocorrelation sign is not a tradable signal**, and that **day-trading skill is
real but requires order-flow depth we do not have**.

---

## 0. The constraints this was designed against

```text
venue          eToro. LONG ONLY v1, no leverage, no shorting, every trade auditable
historical     25.9M daily bars, 7,709 series, 1962-2026. NO intraday (price_intraday = 0)
live           authenticated WS: bid, ask, last. NO sizes, NO depth, NO trade side
SEC depth      1,052,947 insider transactions - ~7M 13F observations - 2.7M filing
               events - ~2M XBRL facts - 539K RegSHO daily - 16 benchmark series
costs          ~50.9 bps modelled round trip; observed bid/ask available live
```

⚠⚠ **Rejected outright as impossible on this feed:** OFI, footprint charts,
cumulative delta, volume profile, VWAP scalping, market making, true intraday
reversal. All need L1 sizes or trade-side classification. **This is a data
constraint, not a judgement about whether they work** — §2.10b of the skill shows
order flow demonstrably does work.

---

## 1. Candidate strategies, ranked

### 1. Opportunistic insider-purchase drift

**Mechanism.** Insiders are not *forced* to buy — but once they do, **Section 16
forces disclosure within two business days**. The asymmetry is legal and
structural: someone with genuine private information takes a costly, personally
risky position, and the law makes them tell us almost immediately.

**Evidence.** Cohen/Malloy/Pomorski: routine trades **zero**, opportunistic
**82 bps/month**. Jeng/Metrick/Zeckhauser: ~11.2%/yr on purchases, **sales show
no effect**. Lakonishok & Lee: ~6% over 12 months.

**Turnover:** low. Hold 3-12 months. ⚠ **This is why it survives 50.9 bps when
momentum does not** — the edge is multi-month and the trade count is naturally
sparse.

⚠ **Our measured population:** 48,278 purchases (not the 1.05M row count — sales
outnumber purchases 5:1 and carry no signal), and only **10% of purchasing
insiders have the ≥3 purchase-years** the routine/opportunistic split needs.

**Falsifier:** after excluding routine traders, 6-month alpha net of eToro cost
is not positive against a same-day random-entry cohort, day-clustered.

### 2. Insider purchase + event confirmation

**Mechanism.** The insider purchase is the **costly signal**; a subsequent
filing, earnings or XBRL improvement is the **public diffusion leg**. Trade only
when both fire — not on price action.

**Evidence.** CMP find opportunistic trades *predict future firm news and
events*. ⚠ PEAD is weaker in liquid names and often eaten by costs
(Chordia et al.: it lives in illiquid stocks where costs bind).

**Turnover:** very low — the event gate reduces trades further. ⚠ Survives cost
only with a 60-180 trading-day hold and **no churning on every new filing**.

**Falsifier:** insider-only alpha is not improved by the event gate after
DSR/trial adjustment.

### 3. Selective XBRL earnings surprise

**Mechanism.** Underreaction to earnings information produces drift.

⚠⚠ **We have no analyst estimates**, so surprise must be **time-series/XBRL**
(versus own history), not SUE versus consensus. **Livnat & Mendenhall show
analyst-based surprises produce materially stronger drift than time-series
forecasts** — so we are building the weaker version by necessity.

**Turnover:** quarterly. ⚠ Cost survival is **marginal** — require expected
60-day gross edge > **2× round-trip cost** and exclude sub-$5 names.

**Falsifier:** XBRL-only surprise deciles fail to beat unconditional
same-event-date controls net of cost.

### 4. 13F sustained-conviction accumulation

**Mechanism.** Managers over $100m disclose long holdings within **45 days**.
⚠ **Do not clone the book** — the staleness kills it, and
Christoffersen/Danesh/Musto show the delay is **partly strategic**. Build only
*persistence*: repeated quarter-on-quarter additions in concentrated positions.

**Turnover:** quarterly or slower. Survives cost only if positions persist across
**two** filings and are held 6-12 months.

**Falsifier:** added positions do not outperform unchanged same-manager holdings
after the filing-date lag and costs.

### 5. Equity-risk-premium core with volatility sizing

**Mechanism.** No forced trader — this is the **fallback exposure** and the thing
everything else must beat. Corrected benchmark compounds **6.3-6.6%/yr**, and the
overnight drift that survived every trap is *"captured by holding, not by
trading."*

**Turnover:** monthly or quarterly rebalance only.

**Falsifier:** the vol-managed core has lower hold-out Sharpe and lower CAGR than
a fixed-weight core after costs.

---

## 2. The trade lifecycle, as a state machine

```text
CANDIDATE -> ELIGIBLE -> SIZE -> ENTRY -> INITIAL_STOP -> RATCHET -> EXIT
```

**`CANDIDATE`** — ⚠⚠ **the signal must be auditable to a source record**: a Form
4 accession + transaction id, a filing event id, a 13F accession, or an XBRL fact
id. **No discretionary chart marks.** This is what makes the trade path
reconstructable, which the project posture requires.

**`ELIGIBLE`** — reject if: spread exceeds the model or live limit; price < \$5
unless a pre-registered microcap sleeve; `ATR14` missing; no tradable eToro
instrument; the standing promotion gate fails.

**`SIZE`**

```text
risk per position   = 50 bps of equity
position notional   = min(max_weight, risk_budget / stop_distance_pct)
max weight          = 5% single name, 20% strategy sleeve
```

⚠ Inverse-vol scaling applies **only if pre-registered** — §2.4's evidence
supports it for momentum, not universally.

**`ENTRY`** — historical: the signal is known only **after the filing
timestamp**, fill at the next open. Live: observed ask/mid policy, recording the
bid/ask/last snapshot. ⚠⚠ **Never measure close-to-open alpha as fillable** — the
trap that produced a +43.9 bps effect living entirely in an unreachable window.

**`INITIAL_STOP`**

```text
stop0 = entry - max( 10% * entry , 2 * ATR14 , 2 * observed_spread_value )
```

The 10% floor comes from the momentum stop-loss evidence; the ATR term from
Wilder; the spread term stops us placing a stop inside the noise. ⚠ **This is
risk control, not edge.**

**`EXIT`** — first of: stop hit, thesis expiry, invalidating filing event, cost
or spread violation, portfolio risk cap.

| family | default hold |
| --- | --- |
| insider | 126 trading days |
| insider + event | 63-126 |
| XBRL surprise | 60 |
| 13F conviction | 2 quarters |

⚠ Apply a **buy/hold spread** (Novy-Marx & Velikov): the threshold to *establish*
a position is stricter than the threshold to *maintain* it. Named by them as the
most effective cost mitigation available.

---

## 3. The ratcheting stop

⚠⚠ **The evidence does not validate "resistance ratchets" specifically.** What it
validates is volatility-distance trailing, on momentum-shaped positions.

- **Kaminski & Lo** — under a random walk, stops **reduce** expected return; with
  momentum present they can add value.
- **Lo & Remorov** — tight stops **underperform** individual-stock buy-and-hold
  after costs; outperformance **requires serial correlation** to exist.
- **Han, Zhou & Zhu** — on momentum: **67% lower max drawdown, 94% better
  Sharpe.**

**Design — hybrid chandelier plus structural offset:**

```text
trigger    daily close clears a CONFIRMED resistance by >= 0.5 * ATR14
           (pivots are causal only n bars later -- see the swing look-ahead trap)

candidate  = max( prev_stop,
                  min( highest_close_since_entry - 3 * ATR14,
                       broken_resistance        - 1.5 * ATR14 ) )

           ratchets UP only, never down
```

⚠⚠ **Never place the stop just below the obvious level.** Osler's order-book
evidence shows stop-losses **cluster just beyond** support levels, and that
cluster is what produces the violent run. The `1.5 * ATR14` offset exists to sit
*outside* the cluster, not on it.

**Verdict:** trailing stops help momentum and crash-risk strategies. ⚠ Applied to
broad long-only compounding **they usually hurt risk-adjusted return** — which
matters here, because family 5 is exactly that.

---

## 4. Research-to-production process

1. **Pre-register** every hypothesis in `trial_register.py` **before** measuring.
2. **Fixed train / validation / hold-out.** ⚠ **No parameter changes after the
   hold-out opens.** All constants enter the rule-set version hash.
3. **Report rejected names, not only accepted ones** (the narrowing-gate rule).
4. **Measurement discipline:** fillable windows only, day-clustered inference,
   non-overlapping forward returns, price/liquidity stratification, a
   random-entry cohort, net of live-observed or banded costs.
5. **Paper trading: minimum 6 months AND 30 independent trades.** ⚠ For sparse
   families (insider, 13F), **12 months or 30 trades, whichever is later.**
6. **Promotion:** positive net alpha versus the correct buy-and-hold comparator,
   DSR hurdle passed, `t >= 3` where applicable, turnover < 50%/month, no
   reliance on unfillable windows, **no single event or month dominating**.
7. **Monitoring:** realised spread slippage, fill failure rate, live-vs-backtest
   hit rate, drawdown, alpha versus a matched random-entry cohort, signal-count
   drift.
8. **Kill criteria — stop new entries when:** live net alpha is below control by
   2 standard errors; realised cost exceeds model by 25 bps round trip; drawdown
   exceeds the pre-registered limit; or signal data quality breaks.

⚠ Item 8 is the one most systems omit. **A strategy needs a documented way to
die** or it runs until it has given back everything it made.

---

## 5. The three ways this loses money, ranked

| # | failure | the control |
| --- | --- | --- |
| 1 | **Costs eat the edge** | turnover cap, live spread gate, no strategy whose expected gross edge < **2× round-trip cost** |
| 2 | **Data-snooping produces fake confidence** | pre-registration, DSR, an untouched hold-out, rule hashes, random-entry cohorts |
| 3 | **The signal lives in names eToro cannot trade well** | price/liquidity bands, observed-spread filter, reject sub-\$5 and illiquid unless separately pre-registered *and* costed |

⚠ All three have already happened to us in miniature during this research: s1 was
destroyed by costs, a pooled `t +18.4` evaporated under clustering, and the
long-horizon reversal we found lives in exactly the names we cannot trade.

---

## 6. Build order

1. **ATR/`ExitLevels` builder** — unblocks S-4, gives every family a stop.
   ⚠ Infrastructure, not edge.
2. **Insider routine/opportunistic classifier** — the family with the best
   evidence and the right turnover profile.
3. **Persist the live tick feed** (`quote_ticks`, `subscription_coverage`) —
   costs nothing, and without coverage intervals a future absence of ticks is
   indistinguishable from "nobody was watching".
4. **Observed-spread cost model** — replaces a calibration with a measurement and
   removes the era confound from the Roll test.
5. Families 2-5 in evidence order.
