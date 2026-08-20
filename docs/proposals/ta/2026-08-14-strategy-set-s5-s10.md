# S-5 … S-10 — six independently-firing strategies, and a first-class regime filter

Operator direction, 2026-08-14: *"a handful of different strategies that could all
fire, TA, signals, how market is moving, support lines, resistance, buy and sell
signals"*, and — the design point that shapes everything below — *"the markets change
regularly so one test from 20 years ago is unlikely to behave the same today and there
is nothing in place or even possible to give you any variance factor to get it in line."*

That critique is correct and is adopted as a constraint, not a caveat. §0 exists because
of it.

## §0 The validation posture changes: walk-forward on regime, never one pooled number

S-1…S-4 were each judged on **one pooled statistic over the whole span**. That is the
wrong instrument for a non-stationary series, and it is why a strategy can look
catastrophic in aggregate (S-1: −49% total) while the aggregate says nothing about
whether it works *now*.

**Rules for every strategy in this document:**

1. **Report per-year and per-regime, never pooled-only.** A single number over 2006-2026
   is context, not evidence. The headline is the recent-regime block.
2. **Regime is an INPUT, not a post-hoc slice.** Each strategy declares which regimes it
   is permitted to fire in (§1). A strategy that only works in one regime is not broken —
   it is a strategy with a stated domain, and firing it outside that domain is the defect.
3. **Walk-forward with a purge.** Fit window → embargo → test window, rolling. No
   parameter chosen on data the test window can see.
4. **The recent window is the deciding one.** Older spans establish that a rule is not a
   fluke of one period; they do not establish that it pays today.

⚠ This does not weaken the promotion gates. It changes what evidence is *produced*, not
what is *required* to fund.

## §1 The regime filter (shared, versioned, frozen)

One classifier, consumed by every strategy below, so "which market is this" is answered
identically everywhere and is hashed into each strategy identity.

**Source rule.** The trend leg uses a published formulation; the volatility leg uses
Bollinger's own definition, not an invented percentile:

- **Trend:** SPY close vs its 200-day SMA. The 200-day is the conventional long-horizon
  trend reference and is not ours to invent.
- **Volatility:** Bollinger **BandWidth** at its lowest / highest reading in **126
  trading days (six months)** — the published Squeeze and Bulge (*Bollinger on Bollinger
  Bands*, ch. 21). ⚠ NOT a 20th/80th-percentile cut. That exact invention was caught at
  review on #2279; it is named here so it is not re-derived.

| regime | condition |
| --- | --- |
| `bull_quiet` | SPY > 200-SMA and BandWidth not in Bulge |
| `bull_volatile` | SPY > 200-SMA and BandWidth in Bulge |
| `bear_quiet` | SPY ≤ 200-SMA and BandWidth not in Bulge |
| `bear_volatile` | SPY ≤ 200-SMA and BandWidth in Bulge |

⚠ **Measured on SPY, and SPY.RTH is the same fund** — no column separates them and only
`.RTH` carries a `real` arm. Pin the series explicitly; do not resolve by symbol.

`REGIME_RULE_VERSION` is frozen and hashed into every identity below. Changing a
boundary is a new version, never a redefinition.

## §2 Levels: support and resistance, fixed BY CONSTRUCTION

⚠⚠ **There is no published formulation for price-level clustering.** The repo rule for
that case is explicit: say so, fix the rule by construction, and freeze the constants in
a version hash. Do not invent a citation.

**Construction (`LEVEL_RULE_VERSION`, frozen):**

- Candidate pivots: a **swing high** is a bar whose high exceeds the highs of the 5 bars
  either side; a **swing low** mirrors it. (5 is chosen, not derived — frozen.)
- Cluster pivots whose prices lie within **0.5 × ATR(14)** of each other; a level is the
  volume-weighted mean of its cluster.
- A level is **live** if it has ≥ 3 touches and its most recent touch is within 120 bars.
- **Strength** = touch count × log(1 + cluster volume share). Used for ranking only,
  never as a threshold.

Every constant above is arbitrary-but-declared. They are frozen together; moving one is
a new `LEVEL_RULE_VERSION`.

## §3 The six strategies

Each declares: Setup · Signal · Fill · Exit · Permitted regimes · Params · Data.
Fill is always `open(t+1)`; levels are fixed at signal time and never move — both
inherited from §3.5 of the parent catalogue.

### S-5 Support bounce (long)
- **Setup:** price within 0.5 × ATR(14) of a live support level (§2); regime ∈ {`bull_quiet`, `bull_volatile`}.
- **Signal:** bar `t` closes **above** the level having traded below it intrabar — rejection, not a close-through.
- **Exit:** stop `level − 1.0 × ATR(14)`; target `entry + 2.0 × ATR(14)`; max hold 30 bars.
- **Params:** 4. **Data:** OHLC + volume.

### S-6 Resistance breakout with volume confirmation (long)
- **Setup:** live resistance level; regime ∈ {`bull_quiet`}.  ⚠ Excluded from `bull_volatile` deliberately — breakouts into a Bulge are the classic false-break regime.
- **Signal:** close(t) > level **and** volume(t) ≥ 1.2 × 20-bar average volume. ⚠ The 1.2 multiple is retail convention, **not** a published result — frozen by construction, recorded as such.
- **Exit:** stop `level − 1.0 × ATR(14)` (back inside the range = failed break); target `entry + 3.0 × ATR(14)`; max hold 40 bars.
- **Params:** 4. **Data:** OHLC + volume.

### S-7 Trend pullback (long)
- **Setup:** close > 200-SMA **and** 50-SMA > 200-SMA; regime ∈ {`bull_quiet`, `bull_volatile`}.
- **Signal:** RSI(14) crosses back **above** 40 having been below it within 5 bars. ⚠ Wilder's RSI, smoothed his way — not a simple moving average of gains. 40 (not 30) because in an uptrend RSI rarely reaches 30; frozen by construction.
- **Exit:** stop `entry − 2.0 × ATR(14)`; exit-signal on close < 50-SMA; max hold 60 bars.
- **Params:** 5. **Data:** OHLC.

### S-8 Mean reversion in range (long)
- **Setup:** regime ∈ {`bear_quiet`, `bull_quiet`}; ADX(14) < 20 (no trend — Wilder's ADX).
- **Signal:** close(t) < lower Bollinger band (20, 2) **and** close(t) > close(t−1).
- **Exit:** target = middle band (20-SMA); stop `entry − 1.5 × ATR(14)`; max hold 15 bars.
- **Params:** 5. **Data:** OHLC.

### S-9 Volatility contraction expansion (long)
- **Setup:** BandWidth in **Squeeze** (lowest in 126 bars — §1's published rule).
- **Signal:** close(t) > highest close of bars `t−20 … t−1`, **and** regime ∈ {`bull_quiet`, `bull_volatile`}.
- **Exit:** stop `entry − 2.0 × ATR(14)`; target `entry + 3.0 × ATR(14)`; max hold 40 bars.
- **Params:** 4. **Data:** OHLC.
- ⚠ **Deliberately close to S-4 and that is the point.** S-4 used a bottom-quartile ATR cut; this uses Bollinger's published Squeeze plus a regime gate. If S-9 works where S-4 failed, the regime gate is the reason — a controlled comparison, not a duplicate.

### S-10 Relative-strength leader (long, cross-sectional)
- **Setup:** regime ∈ {`bull_quiet`}; universe ranked by 63-bar return.
- **Signal:** enter the top decile that ALSO closes above its own 50-SMA; rebalance monthly.
- **Exit:** leaves the top three deciles, or closes below 50-SMA; no fixed stop (cross-sectional).
- **Params:** 4. **Data:** OHLC, cross-section.
- ⚠ **Turnover check FIRST** (Novy-Marx/Velikov: >50%/month rarely survives costs). Monthly rebalance on deciles is near that bound — if measured turnover exceeds it, S-10 is disqualified before any backtest, exactly as S-1 was at 56×/yr.

## §4 What this document does NOT do

- It does not lower a promotion gate. Nothing here becomes fundable without the same
  survivorship-free + carry-modelled evidence every other result needs (#2698).
- It does not claim these will work. Four of four prior strategies failed, and the
  replication literature expects most to. The purpose is a **portfolio of independent
  shots with stated domains**, measured on the regime that is actually running.
- It does not add a short leg. Shorting is permitted for research (settled 2026-08-09)
  but every rule above is long-only; a short arm is a separate document with its own
  hard-to-borrow cost model.

## §4.1 Declaration before evaluation

The first price-data evaluation of S-5…S-10 is charged to the shared trial register
before the hold-out is opened: one exact search per frozen strategy. The ambiguity,
quarantine, pinned recent-window and declared regime reports are jointly required
robustness views; none may be selected as a winner, so they collapse to one search by
the register's standing fan rule. Any parameter, cadence or permitted-regime variant
is a new trial and must be appended before it reads price outcomes.

These six initial results remain `harness_validation`. Registering the searches does
not relabel them as capital candidates and does not weaken any promotion refusal.

## §5 Order of work

1. Regime filter + level construction (§1, §2) — shared, and every strategy depends on them.
2. S-6, S-5, S-9 — level/volatility family, closest to existing machinery.
3. S-7, S-8 — indicator family.
4. S-10 last, gated on its turnover measurement.

Refs #2437. Refs #2698.
