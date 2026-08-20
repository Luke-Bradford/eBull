# The application to build — a probabilistic, cost-aware setup engine

Refs #2437. Produced by a **clean-slate** Codex brief: no mention of our existing
strategies, no SEC-data framing (which I had primed into the previous round), and
our real constraints supplied.

---

## 0. The answer, and it rejects the premise

> **"I would not build a 'day-trading bot' first."**
>
> Build a **probabilistic, cost-aware setup engine** where **daily data supplies
> most of the statistical base rate, live intraday data supplies confirmation and
> execution timing**, and paper trading is treated as an **empirical filter
> rather than a demo**.

⚠⚠ **The reframe that matters:** the target is **daily-to-multi-day signals with
intraday execution timing** — not intraday scalping.

> *"Daily effects have enough horizon to amortize 50 bps. Intraday candles are
> useful for **when not to enter**, where to place risk."*

**Intraday is not the strategy. It is the execution layer for a daily signal.**
That resolves the tension we have been circling: we cannot win an intraday
alpha contest without depth, but we can use intraday data for the part it is
genuinely good at — timing an entry we already decided on for other reasons.

---

## 1. Architecture — three layers

### Research and evidence layer

- Daily-bar research engine over the full 1962-2026 universe.
- Corporate-action-aware universe handling; **delisting/survivorship checks**.
- **A strategy registry where every tested setup, parameter set, and REJECTION is
  logged** — we have `trial_register.py`; this is its purpose.
- Walk-forward and regime tests with **broker-like costs**: spread, slippage,
  failed fills, position limits, no shorting, no leverage.
- ⚠ **A tick recorder from today forward** — bid, ask, last, timestamp, symbol,
  **session state**.
- **Intraday candle builder** — 1m, 5m, 15m, 30m, opening range. ⚠ VWAP-like
  measures **only if volume is defensible**, which on our feed it is not.

### Daily operation layer — each morning

1. **Build a tradable universe** — exclude low-priced, wide-spread, stale, gappy,
   hard-to-fill names.
2. **Rank by daily context** — trend, volatility compression/expansion, overnight
   gap, prior range, distance from 20/50/200-day levels, realised volatility,
   **spread as a percentage of price**.
3. ⚠⚠ **Generate HYPOTHESES, not trades** — continuation, reversal, no-trade,
   late-day momentum, overnight-hold candidate.
4. **Compute expected value after costs.** *"If the required move is not
   comfortably above 50 bps plus slippage, discard."*

### During the session

Watch **only prequalified names**. Each setup runs as a state machine:

```text
eligible -> forming -> trigger-near -> confirmed -> orderable -> invalidated -> exited
```

⚠ **Entry requires BOTH a statistical edge AND execution sanity**: spread below
threshold, price not too far from the trigger, expected move large enough, stop
distance not absurd. Orders small, long-only, **limit or controlled marketable
limit. No chasing.**

### After the close

- ⚠⚠ **Reconcile broker fills against SIMULATED fills.** This is the feedback loop
  that tells us whether the backtest is lying.
- Attribute P&L by **setup, symbol, time of day, spread bucket, volatility
  bucket, and regime**.
- Store paper/live shadow results **separately** from historical backtests.

---

## 2. Can anything work on intraday candles alone?

> **Plain answer: "pure intraday candles alone probably do not give enough edge
> across ordinary US equities after 50 bps."**

| setup | evidence | survives 50 bps? | try? |
| --- | --- | --- | --- |
| **overnight/intraday decomposition** | **strongest of the set** | **more plausible — the holding period absorbs the cost** | ✅ **yes** |
| gap fade | overnight/intraday reversal evidence | maybe, large liquid gap-DOWNS only, long-only | ⚠ carefully |
| volatility breakout | some technical-rule support, ⚠ unstable after data-snooping correction | maybe on high-vol liquid names | ⚠ hybrid |
| late-day intraday momentum | Gao, Han, Li & Zhou — first half-hour predicts last half-hour | uncertain at single-stock retail cost | ⚠ very liquid only |
| opening range breakout | ORB papers, mostly index/futures/ETF | **doubtful** for broad single stocks at retail spreads | ⚠ as a *filter feature*, not a strategy |
| time-of-day effects | real intraday seasonality exists | **usually too small after 50 bps** | ❌ mostly folklore here |
| VWAP reversion | an institutional *execution benchmark*, not retail alpha | ❌ no — needs real consolidated volume | ❌ |

**New citations worth having:**

- **Gao, Han, Li & Zhou**, *JFE* — US ETF **market intraday momentum**: the first
  half-hour return predicts the last half-hour return.
- **Cooper, Cliff & Gulen** — large differences between **overnight and intraday**
  equity returns.
- ⚠⚠ **NY Fed staff report 917, "The Overnight Drift"** — economically large
  positive returns during European opening hours, **linked to inventory and order
  imbalance**. **This independently corroborates the one effect that survived all
  four traps in our own research pass**, and supplies the mechanism we lacked.

> **The convergence is worth noting: our own measurement, Cooper/Cliff/Gulen, and
> the NY Fed all point at the overnight window — and it is the one intraday-ish
> effect whose holding period is long enough to absorb our costs.**

---

## 3. The probabilistic multi-setup architecture

⚠ **The operator's framing is validated, not humoured.** *"The
discretionary-trader framing is valid. The implementation should not be 'if
signal then buy.'"*

| component | role |
| --- | --- |
| **setup library** | defines candidate situations |
| **context model** | estimates conditional odds — regime, symbol volatility, spread, gap size, prior trend, range compression, time of day |
| **state machine** | waits for price behaviour — level touch, rejection, breakout, retest, failure, continuation |
| **probability model** | estimates **path** outcomes: hit target first, hit stop first, close positive, close negative, gap risk if held |
| **portfolio allocator** | chooses among competing setups **by expected value, not win rate** |

⚠ Note the fourth row: **path** probability, not just direction. "Which do I hit
first, the stop or the target" is the question that actually determines P&L, and
it is not the same as "does it go up".

### ⚠⚠ And a correction to the confirmation intuition

> **"Confirmation entries are not automatically better. They usually trade higher
> conditional probability for WORSE PRICE."**

A breakout confirmation reduces false positives but **gives up early edge and
increases adverse selection**. So the entry timing is itself a parameter to be
measured across four variants, not assumed:

1. anticipatory entry at setup formation
2. entry on level break
3. entry on close beyond level
4. entry on retest hold

…plus **no trade unless spread and volatility make the target/stop geometry
favourable.**

---

## 4. Measuring success honestly

- **Every tested idea logged BEFORE evaluation.**
- **Costs use observed broker spreads, not idealised midpoint fills.**
- **Paper trading uses the same order rules intended for live** — not a demo, an
  empirical filter.
- Reconcile live fills against simulated ones and treat the gap as the error bar
  on every backtest.

---

## 5. Build order implied by this

1. **Tick recorder + session state** — starts the clock on an intraday corpus we
   do not have and cannot buy retroactively. ⚠ Must include
   `subscription_coverage`, or a gap is indistinguishable from a quiet market.
2. **Observed-spread capture** — turns our cost model from a calibration into a
   measurement.
3. **Daily context ranker** — the morning universe and hypothesis generator.
4. **Overnight/intraday decomposition study** — the one intraday-adjacent effect
   with three independent sources behind it and a cost-absorbing horizon.
5. **State machine + EV gate**, then setups one at a time, each pre-registered.
