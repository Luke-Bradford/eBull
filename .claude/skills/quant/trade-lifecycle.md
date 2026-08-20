# quant/trade-lifecycle

## When to use

**Before writing any code that decides how much to buy, where to put a stop, or
when to get out.** Read it whenever a design mentions position sizing, stop
loss, take profit, trailing stop, exit rule, risk budget, or paper trading.

⚠ It exists because for most of this project's life the answer to all of those
was *"equal weight, no stop, exit on the signal"* — one sizing rule has ever been
used and nothing computes an exit level.

> **The rule: the signal decides WHETHER to trade. Volatility decides HOW MUCH
> and HOW FAR. They are separate systems and must not be conflated.**

⚠ Companions: `quant/strategy-evidence.md` (what to trade),
`quant/measurement-discipline.md` (how to validate it),
`quant/data-capability.md` (what we can compute).

---

## 1. ⚠⚠ Stops and sizing are RISK CONTROL, not edge

State this first because the opposite is the seductive error, and this file
previously made it.

**ATR gives SCALE, not EXPECTANCY.** A stop reshapes the return distribution and
**can convert positive drift into negative** by truncating winners' paths and
locking in noise. **A strategy with no edge is not rescued by a good stop.**

The evidence is conditional, not blanket:

| finding | source |
| --- | --- |
| under a random walk, stops **reduce** expected return; with momentum they can add value | Kaminski & Lo |
| tight stops **underperform** individual-stock buy-and-hold after costs; outperformance **requires serial correlation** | Lo & Remorov |
| on momentum: **67% lower max drawdown, 94% better Sharpe** | Han, Zhou & Zhu (2014) |
| thresholds of **1.0-1.5 standard deviations** give higher excess returns, positive after costs | ETF study, 2001-2021 |
| stops raise returns on **lottery-shaped** names (frequent small losses, sporadic big gains) | same literature |
| US stocks 1926-2016: trailing stops gave lower risk but **lower return AND lower Sharpe** | broad-holding study |

> **So: stops belong on MOMENTUM and lottery-shaped positions. They cost you on a
> broad compounding hold.** Apply by family, never globally.

---

## 2. The state machine

```text
CANDIDATE -> ELIGIBLE -> SIZE -> ENTRY -> INITIAL_STOP -> RATCHET -> EXIT
```

### `CANDIDATE`

⚠⚠ **The signal must be auditable to a SOURCE RECORD** — a Form 4 accession plus
transaction id, a filing event id, a 13F accession, an XBRL fact id. **Never a
discretionary chart mark.** This is what makes the trade path reconstructable,
which the project posture requires of every trade.

### `ELIGIBLE`

Reject if: spread exceeds the model or live limit · price < \$5 unless a
pre-registered microcap sleeve · `ATR14` missing · no tradable eToro instrument ·
the standing promotion gate fails.

### `SIZE`

```text
risk per position  = 50 bps of equity
position notional  = min( max_weight , risk_budget / stop_distance_pct )
max weight         = 5% single name , 20% strategy sleeve
```

⚠ Inverse-vol scaling only if **pre-registered** — the evidence supports it for
momentum, not universally (Moreira-Muir vs Cederburg et al.).

### `ENTRY`

- **Historical:** the signal is knowable only **after the filing timestamp**;
  fill at the next open.
- **Live:** observed ask/mid policy, recording the bid/ask/last snapshot.
- ⚠⚠ **Never measure close-to-open alpha as fillable.** A measured +43.9 bps
  effect lived entirely in that unreachable window and the tradable half had the
  **opposite sign**.
- **Ownership is exact, never inferred.** Persist strategy trade → strategy
  entry order → detailed broker order lookup → `positionExecutions[].positionId`.
  A strategy may patch or close only that actively owned `positionId`; never use
  symbol/instrument matching, units, timestamps, source labels, or FIFO. A manual
  same-instrument position has no ownership row and must remain unmodifiable by
  strategy code, while still counting toward portfolio-wide risk.
- **Broker submission identity is durable before I/O.** Commit one immutable
  UUID, send it as eToro v2 `X-Request-Id`, and reuse it for both an idempotent
  retry and `orders:lookup.referenceId`. After an uncertain response, never
  rotate the UUID or submit a newly keyed order. Pending/partial executions are
  owned by exact returned position id but keep the entry backlog unresolved;
  an overdue backlog blocks new entries.
- **Paper allocation is a conjunction, not a score.** Current quote/session,
  fresh signal scan, fresh halt feed/no unresolved halt, current exact-arm
  eligibility, documented current costs, reconciliation health, global/strategy
  switches, operator cap, account cash/exposure/concentration/drawdown and a
  positive stressed lower-bound net expectancy must all pass. One strong metric
  never compensates for a missing safety input.
- **Size against the whole demo account.** Manual positions and pending orders
  reduce cash, portfolio exposure and instrument concentration capacity, but
  they never enter strategy P&L or become close/ratchet targets. Ticket size is
  the minimum of operator ticket rule/cap, remaining strategy sleeve, broker
  available cash, portfolio capacity and instrument capacity.
- **Use lower-bound net expectancy:** `min(pinned bootstrap expectancy CI) -
  stressed documented current costs / exact ticket amount * 100`. Unknown cost
  units or unmodelled recurring-horizon fees are refusals, not zero.
- **Persist the negative arm.** Each durable fired entry gets one compact funded
  or rejected preflight. A rejection is monitoring evidence and must name its
  closed reason; do not retain repeated raw broker/feed snapshots to achieve it.

### `INITIAL_STOP`

```text
stop0 = entry - max( 10% * entry , 2 * ATR14 , 2 * observed_spread_value )
```

The 10% floor from the momentum stop-loss evidence; the ATR term adapts to the
instrument's own scale; **the spread term stops us placing a stop inside the
noise**. ⚠ Risk control, not edge.

### `RATCHET` — see §3

### `EXIT`

First of: stop hit · thesis expiry · invalidating filing event · cost/spread
violation · portfolio risk cap.

| family | default hold |
| --- | --- |
| insider purchase | 126 trading days |
| insider + event | 63-126 |
| XBRL surprise | 60 |
| 13F conviction | 2 quarters |

⚠ Apply a **buy/hold spread** — the threshold to *establish* a position is
stricter than the threshold to *maintain* it. Novy-Marx & Velikov name this as
**the most effective cost-mitigation technique available**, and it directly
attacks the turnover filter we fail worst.

---

## 3. The ratcheting stop

⚠⚠ **No evidence validates ratcheting on structural levels specifically.** The
evidenced form is volatility-distance trailing. Structure is used only to
**offset**.

```text
trigger    daily close clears a CONFIRMED resistance by >= 0.5 * ATR14
           ⚠ pivots are causal only n bars later -- "the last swing high" is
             not knowable in real time (the look-ahead that caught S-6)

candidate  = max( prev_stop,
                  min( highest_close_since_entry - 3.0 * ATR14,
                       broken_resistance        - 1.5 * ATR14 ) )

           ratchets UP only, never down
```

⚠⚠ **Never place the stop just below the obvious level.** Osler's order-book data
shows **stop-loss orders cluster just beyond support**, and that cluster is
*what produces the violent move when a level breaks*. **Sitting on the level puts
you inside the hunt.** The `1.5 * ATR14` offset exists to sit outside it.

⚠ Unmeasured on our data and required before shipping: the right multiple for
our universe, and whether the ratchet earns its turnover after costs. The
literature gives a range, not our number.

---

## 4. Research-to-production process

1. **Pre-register** every hypothesis in `trial_register.py` **before** measuring.
2. **Fixed train / validation / hold-out.** ⚠ **No parameter changes once the
   hold-out is open.** All constants enter the rule-set version hash.
3. **Report rejected names, not only accepted ones.**
4. **Measurement discipline** — see `quant/measurement-discipline.md`. Fillable
   windows, clustered inference, non-overlapping returns, stratification, a
   random-entry cohort, net of observed or banded costs.
5. **Paper trading: minimum 6 months AND 30 independent trades.** ⚠ For sparse
   families (insider, 13F): **12 months or 30 trades, whichever is later.**
6. **Promotion requires all of:** positive net alpha versus the correct
   buy-and-hold comparator · DSR hurdle passed · `t >= 3` where applicable ·
   turnover < 50%/month · no reliance on unfillable windows · **no single event
   or month dominating the result**.
7. **Monitoring:** realised spread slippage · fill-failure rate ·
   live-vs-backtest hit rate · drawdown · alpha versus a matched random-entry
   cohort · signal-count drift.

### 4.1 ⚠⚠ Kill criteria — the part most systems omit

**Stop new entries when ANY of:**

- live net alpha is below control by **2 standard errors**
- realised cost exceeds model by **25 bps round trip**
- drawdown exceeds the **pre-registered** limit
- signal data quality breaks

> **A strategy without a documented way to die runs until it has given back
> everything it made.** Write the kill criteria before the strategy goes live,
> not after it starts losing.

---

## 5. The three ways this loses money

| # | failure | control |
| --- | --- | --- |
| 1 | **costs eat the edge** | turnover cap · live spread gate · no strategy whose expected gross edge < **2× round-trip cost** |
| 2 | **data-snooping produces fake confidence** | pre-registration · DSR · untouched hold-out · rule hashes · random-entry cohorts |
| 3 | **the signal lives in names eToro cannot trade well** | price/liquidity bands · observed-spread filter · reject sub-\$5 and illiquid unless separately pre-registered *and* costed |

⚠ **All three have already happened here in miniature.** s1 was destroyed by
costs (72×/yr turnover, 36.6% annual spread drag, ended at −100%). A pooled
`t +18.4` evaporated under clustering. And the long-horizon reversal we found
lives precisely in the names we cannot trade.
