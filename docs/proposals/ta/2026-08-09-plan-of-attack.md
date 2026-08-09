# Plan of attack — where #2437 stands and what to run next

Written 2026-08-09 as a session handoff. ⚠ Read this before re-deriving anything;
every number below is measured and every script named exists and runs.

---

## 0. The state in five lines

- **One strategy candidate survives every check**: short a ≥12% one-day drop, cover
  after 5 bars, 20% stop. `t 4.83` day-clustered, 1,364 event days, 2020+.
- ⚠⚠ **It is not validated.** It came out of ~100 searched arms and is unregistered.
- **Shorting is permitted** (operator, 2026-08-09). **Leverage is not**, until validated.
- **The long side fails on COST**, not signal — 44 bps drift vs a 50 bps round trip.
- ⚠⚠ **We have intraday history** (`FourHours` ~8 months). This was wrong four times.

## 1. The candidate, precisely

`scripts/verify_2437_short_stops.py`. Universe: prior close ≥ \$20, 20-day median
dollar volume ≥ \$10m, 2020+. Entry SHORT at the next bar's adjusted open, cover at
the adjusted close 5 bars later, stop at +20% (gap-through fills at the open).

```text
stop   gross  median  win%     t  ex-top1%   worst  net@30%/yr
none   216.3   133.9  54.4  5.59      82.5  -41254       108.9
 20%   156.8    74.1  52.3  4.83      77.6   -8749        49.4
 12%   130.4   -68.7  48.1  4.61      81.0   -8749        23.0
  5%    81.4  -500.0  32.2  3.94      46.6   -3429       -26.0
```

⚠⚠ **Worst trade is −87% even with the stop**, identical at 12% and 8%: no stop level
catches a gap, and 141 of 1,402 stops filled at an open above the level. Sizing must
survive −87% on one name.

## 2. Run these next, in this order

### 2a. Portfolio simulation — DO THIS FIRST

Everything measured so far is **per trade**. 8,049 trades over 1,364 days is ~6
concurrent firings per day, so sizing and concurrency decide the equity curve, not the
per-trade mean. ⚠ A +49 bps net per-trade edge can be an excellent portfolio or a
ruinous one purely on how many are held at once and how the −87% tail lands.

Reuse `app/services/equity_curve.py`. ⚠ Read #2426's lesson first: `build_equity_curve`
**rebalances on every event date**, which manufactured 23.2 pts/yr on the old
benchmark. For a short book, decide the weighting rule deliberately and state it —
`equal_weight_buy_and_hold_v1` exists precisely because reuse imports policy, not just
plumbing.

Report: equity curve, max drawdown, worst day, concurrency distribution, and the
curve's sensitivity to a per-name weight cap.

### 2b. Register the trial before searching further

⚠⚠ `t 4.83` **searched-over is not `t 4.83` pre-registered.** Register in
`trial_register.py` with the honest count (~100 arms this session: gap fade across 5
bands × 5 eras, 15 reversal arms, 25 breadth cells, 12 confluence buckets, 13
conditions, 6 short arms, 5 stops). At 100 trials the deflated-Sharpe bar is ≈0.174.

⚠ Do this **before** more searching, or the denominator keeps growing.

### 2c. eToro borrow availability — could kill it outright

A ≥12% one-day drop is the archetypal hard-to-borrow name, and eToro's own docs list
*"temporary unavailability of shares to borrow"* as a live restriction. If the names
this fires on cannot be shorted at the moment they fire, the statistics are moot.

⚠ Unknown whether the API exposes shortability per instrument — **check, do not
assume** (`.claude/skills/data-sources/etoro-api.md`, verification protocol).

### 2d. Out-of-sample

2020-2026 is one sample and it has been searched hard. ⚠ There is no clean hold-out
left in the post-2020 regime, and pre-2020 is a *different regime* (see §4), so this
likely means **forward paper trading**, not a historical split. Say so rather than
carving a fake hold-out.

## 3. Open threads worth picking up

- **Confluence on the SHORT side.** The 11-condition alignment test was measured for
  long entries only. The short side has 200-440 bps of gross rather than 44, so
  conditioning has somewhere to go. ⚠ Reuse `verify_2437_confluence.py`; keep the
  alignment-count gradient (1 d.o.f.), do not search subsets.
- **`P(hit stop before target)`** — the first-passage model. Codex's spec: triple-barrier
  labels, walk-forward with purge and embargo, scored on **Brier + calibration curves**,
  not average return. ⚠ *"A model that ranks well but is miscalibrated is dangerous for
  sizing"* — and sizing is exactly what it would feed.
- **Intraday residual reversal** (Brogaard/Han/Kim, sample to Dec 2022) on **30-minute
  midpoints** — the granularity we can already pull ~1 month back. ⚠ Their construction
  is long-short; the long leg alone is the weaker one.
- ~~**`breadth up day` was the largest single marginal.**~~ **INVALIDATED
  2026-08-09.** The original confluence code used the next-open entry date's market
  return in a prior-close signal. After the causal-date fix, breadth-up is **37.70
  bps at 10 days versus a 44.28 bps unconditional baseline**. Reproduce with
  `PYTHONPATH=. uv run python scripts/verify_2437_confluence.py`. Do not reuse the
  old +91.56 bps / t 7.93 figure.
- **A tick recorder.** `price_intraday` is empty with no writer. ⚠ Schema mismatch
  noted in `data-capability.md`. Lower priority now that REST history exists, but it is
  the only way past the 1000-bar ceiling.

## 4. Rules this session established — do not relearn them

1. ⚠⚠ **Never test one condition and report the null.** `E[r|A]` and `E[r|A,B,C,D,E]`
   can have opposite signs. Our proof: the −5% drop is −437 bps at breadth 2-5 and
   **+91 at breadth 101+**. Tell = day- and year-clustering disagreeing **in sign**.
2. ⚠⚠ **A level type needs a placebo arm.** Fake fib 29/44/71 beat real 38.2/50/61.8;
   fake support beat real pivot-low support. Both real ones sit below baseline.
3. ⚠⚠ **Era-split before excitement.** Gap fade: t 5.73 over 1962-2026, t 1.39 over
   2020+, negative pre-2001. Usable short-horizon sample ≈ 6 years.
4. ⚠⚠ **A hard constraint makes findings look like nulls.** "Buy the loser" was filed
   as a null three times; it was an inverted signal with no expression.
5. ⚠ **Probe a provider before asserting a capability gap.** An empty table measures
   what we chose to store, not what the source will serve.

## 5. Closed — do not reopen

- **SEC-keyed intraday signals.** Measured ingest lag: median **2 days**, p90 37, only
  4.6% same-day. Even at zero latency, Form 4s are machine-parsed off EDGAR
  dissemination in milliseconds. SEC data is a multi-month horizon or nothing.
- **Support proximity and Fibonacci as entry conditions.** Falsified by placebo (§4.2).
- **The gap-down fade.** Dead since 2020 (+11.8 bps, t 1.39).
- **Long-only one-day loser reversal.** Wrong sign, monotonically.

## 6. Merge state

- PR **#2442** merged (`61fb17da`).
- PR **#2444** merged (`dbe5107b`).
- The evidence correction, broker preflight and automation control-plane
  follow-up is on `feature/2437-automation-control-plane`; PR number pending.
