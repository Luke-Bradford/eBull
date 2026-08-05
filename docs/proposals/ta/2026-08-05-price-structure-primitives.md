# Price-structure primitives (phase 2b) — spec

Ticket: #2279. Parent programme: #2240
(`docs/superpowers/specs/2026-08-04-ta-strategy-platform-design.md`).
Data: `research_price_daily` (#2282), **not** `price_daily`.

Phase 2b sits between phase 2 (indicator history) and phase 3 (strategy
registry), because the registry's value is that rules compose over a vocabulary,
and a registry built against seven scalars fixes that vocabulary prematurely.

Revision 2 — rewritten after Codex checkpoint 1, which found ~40 gaps. The
substantive corrections it forced are marked **[C1]** where they changed a
decision rather than just adding detail.

---

## 0. Source rule

No regulator governs chart structure, so the equivalent of "cite the reg" is:
name the published formulation each primitive implements, and say explicitly
where none exists rather than inventing a citation.

| primitive | governing published formulation |
| --- | --- |
| swing high / low | **N-bar fractal.** Bill Williams' fractal (*Trading Chaos*, 1995) is the N=2 case; the general N-bar pivot is the same rule with a wider window. Reference implementation compared against: `scipy.signal.argrelextrema(order=N)`. |
| ZigZag (rejected, §2) | StockCharts / TradingView ZigZag — percentage-reversal between alternating extremes. This is what `pandas_ta.zigzag` implements and cites. |
| "pivot point" (**not** this) | floor-trader pivots, `P = (H+L+C)/3` with R1/S1… derived. `pandas_ta.pivots` is this, and it is a *different object* from a pivot high/low. The name collision is a trap. |
| Fibonacci retracement | ratios 0.236 / 0.382 / 0.5 / 0.618 / 0.786 applied to a **leg**. **[C1]** The charting convention *does* specify the anchor — the most recent completed major swing leg — and §4.4 adopts it rather than claiming no rule exists. What has no published rule is which *N* makes a swing "major"; that is §3.1's ladder. |
| anchored VWAP | `Σ(typical_price × volume) / Σ(volume)` cumulative from an anchor bar. **[C1]** `typical_price` is a **choice**, not a standard: we take HLC3 `(H+L+C)/3`, the convention TradingView and Sierra Chart use for daily bars. A tick-level VWAP would use trade prices, which daily OHLCV cannot supply. |
| volatility regime | **[C1]** Bollinger's **Squeeze** is `BandWidth` at its **lowest value in six months**, and the **Bulge** its highest (Bollinger, *Bollinger on Bollinger Bands*, ch. 21). It is **not** a 20th/80th percentile — an earlier draft of this spec invented that and it is corrected in §4.6. Six months = **126 trading days**. |
| **level clustering** | **none exists.** There is no standard formulation for grouping swing points into a support/resistance level. Stated rather than papered over, and it is why §4.2 fixes the rule by construction and freezes it in the version string. |

---

## 1. Measured facts

Measured 2026-08-05 against the dev DB at migration 252. **Full population**
unless a row says otherwise.

### 1.1 Corpus

```
series                     7,693      resolved to an instrument   5,269
bars                  25,818,944      earliest bar   1962-01-02
                                      latest bar     2026-07-08
```

- **Every resolved series is `us_equity` + `instrument_type_id = 5`** (5,269 of
  5,269) — exactly the #2289 validated universe, ex-ETF. No asset-class
  branching is needed in this phase.
- **Depth on resolved series**: 904 under 1,000 bars, 1,133 at 1,000–2,000, and
  the median falls in the 3,000–4,000 band ≈ **12 years**, against eToro's
  ~1,000-bar / ~4-year cap. This is the gain #2279 was waiting on.
- **OHLC is 100% non-null** across all 25,818,944 bars, so no `not_evaluable`
  arises from absent OHLC on this corpus.
- The archive is **static** — `last_bar` clusters on 2026-07-06/07/08. Nothing
  tops it up yet, so every figure here is reproducible against a fixed snapshot.

### 1.2 Volume — the issue's premise is falsified here

Item 5 of #2279 says anchored VWAP "needs volume, which is **equity-only** on
our feed (S3 #2243)", and the parent proposal §2.3 says the same. Both are true of
`price_daily`, whose priced universe includes crypto/fx/index. Neither transfers
to the research corpus, which is 100% US equity:

```
bars 25,818,944   volume NULL 0   volume < 0 0   volume = 0  1,332,930 (5.16%)
                  min 0   max 9,230,856,000
                  adj_close non-null 25,818,944 (100%)
```

So the equity-only carve-out is **vacuous** for this corpus. The real coverage
limit is the 5.16% of zero-volume bars.

**[C1] Zero is not vendor-shorthand for missing.** Codex asked whether the zeros
are a "vendor writes 0 when it has no volume" artefact concentrated in early
history. Measured by decade, they are not — the rate rises and falls rather than
switching on:

| decade | bars | volume = 0 | % |
| --- | ---: | ---: | ---: |
| 1960s | 21,861 | 14 | 0.06 |
| 1970s | 145,249 | 7,168 | 4.93 |
| 1980s | 851,238 | 61,565 | 7.23 |
| 1990s | 1,993,462 | 166,882 | 8.37 |
| 2000s | 3,951,177 | 269,243 | 6.81 |
| 2010s | 6,103,781 | 366,018 | 6.00 |
| 2020s | 10,373,468 | 356,329 | 3.44 |
| 2030s | 2,378,708 | 105,711 | 4.44 |

Either way the treatment is the same and §4.5 states it exactly: a zero-volume
bar contributes zero weight **and is excluded from the denominator count**, and
a window with no positive-volume bar is `not_evaluable`.

### 1.3 Quarantine

Coverage is complete and single-versioned, so a fail-closed read is satisfiable
today:

```
series with bars 7,693   rows in research_price_quarantine_coverage 7,693
rule_set_version  price-quarantine-v1+d0423dbd9cb5   (1 distinct)

bars_range_unusable   1,315  across    669 series
bars_return_unusable    242  across     51 series
transitions_quarantined 1,564          admitted back  367
```

`provisional` is `price_date >= as_of − 5 days`
(`price_quarantine.PROVISIONAL_WINDOW_DAYS = 5`). Structure detection already
excludes the last N bars by construction (§3), so for N ≥ 5 the provisional
window sits inside the unconfirmable tail and needs no separate rule. **This is
asserted as a test, not assumed** (§8).

**Numerical hazards, measured** (Codex flagged the BandWidth denominator):

```
close <= 0        2 bars      low <= 0     30 bars     close < 0.01   9,351 bars
```

### 1.4 Cost — the input to the persistence decision (§6)

Pure-Python N-bar fractal over the 7-name panel (83,201 bars), extrapolated:

| N | throughput | full corpus (25.82M bars) |
| ---: | ---: | ---: |
| 5 | 2.10 M bars/s | **12.3 s** |
| 21 | 1.68 M bars/s | **15.4 s** |
| 63 | 1.03 M bars/s | **25.1 s** |

**[C1] This benchmarks swings only.** Codex is right that levels, VWAP and the
regime rank are not covered by it, so §6 does not treat this as settling the
question for all six primitives — it is an acceptance item (§8.6).

---

## 2. Prior-art check — tested, not assumed

Per the repo's standard-reuse rule, each candidate was run against our actual
bars (7 names, 83,201 bars: AAPL GE GME HD JPM KO MSFT, 1962→2026).

| candidate | has the primitive? | verdict |
| --- | --- | --- |
| **TA-Lib** (161 functions) | **no.** No zigzag, pivot, fractal or swing function in `get_functions()` — the closest are `MIN`/`MAX`/`MINMAX(INDEX)` and `SAR`. | cannot supply it |
| **`pandas_ta.pivots`** | wrong object — floor-trader pivot points (`method='traditional'`, `anchor='D'`), an arithmetic formula over the prior period. | not swing structure |
| **`pandas_ta.zigzag`** | yes — the only library implementation found. | **rejected, below** |
| **`scipy.signal.argrelextrema(order=N)`** | generic local extrema; reproduces our fractal to within 0.5% on the panel (AAPL 632 vs 630, GE 920 vs 918, GME 393 vs 390 — the residue is boundary handling). | a new dependency for ~10 lines, whose tie/plateau behaviour we must pin down explicitly regardless (§3.2) |
| **vectorbt** | not a structure detector; already verified for phase 5. | out of scope here |

### Why `pandas_ta.zigzag` is rejected

**(a) It cannot be installed — this ground alone is decisive and universal.**
`pandas-ta 0.4.71b0` depends on `numba`, which supports `>=3.10,<3.14`. This repo
runs **Python 3.14.4**. The install fails at build time, not at import. (b) and
(c) below are evidence about the *formulation*, which is why they still matter
after (a): they decide which family we hand-roll.

**(b) Its output collapses on the names we tested.** **[C1] Scoped: this is 7 of
7 panel names, not a full-corpus claim, and is not generalised beyond them.**
Swing count by `deviation`, `legs=10` default, full series per name:

| sym | 3% | 5% | 6% | 7% | 8% | 10% | 15% | 20% | max DD | bars, abs 20-bar return > 10% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| AAPL | 1120 | 990 | 918 | 820 | 738 | 548 | **1** | **1** | 82.2% | 4,156 |
| GE | 1493 | 1045 | 855 | 707 | 595 | 403 | **1** | **1** | 90.5% | 2,305 |
| GME | 635 | 591 | 553 | 511 | 489 | 409 | 263 | 182 | 95.6% | 2,735 |
| HD | 1165 | 925 | 815 | 689 | 595 | 447 | 225 | 117 | 73.8% | 2,535 |
| JPM | 1130 | 912 | 768 | 654 | 556 | 394 | **2** | **2** | 81.7% | 2,358 |
| KO | 1439 | 913 | 699 | **1** | **1** | **1** | **1** | **1** | 69.1% | 1,380 |
| MSFT | 1007 | 781 | 677 | 563 | 481 | 339 | 163 | 77 | 74.6% | 2,172 |

Five of the seven cliff to ≤ 2 swings over 60+ years, on series whose own
drawdown history contains hundreds of moves far larger than the threshold — KO
collapses 699 → 1 across a single one-point step while holding a 69.1% max
drawdown and 1,380 bars whose 20-bar return exceeds ±10%. Sweeping `legs`
(3/5/10/20/50) does not recover it. **The cause is not diagnosed** — we are not
debugging a library that cannot be installed — but a discontinuous cliff to
exactly 1 is not the shape of parameter sensitivity, and the output is unusable
on these names either way.

**(c) Its default mode is look-ahead-unsafe.** `backtest=False` is the default
and, per the library's own docstring, returns swing points *on the pivot index* —
the bar the pivot occurred on, which is not knowable then. `backtest=True` shifts
them to the bar that would have detected them. Measured offset between the two
modes, all swings, `deviation=5`:

| sym | n | min | p50 | p95 | max |
| --- | ---: | ---: | ---: | ---: | ---: |
| AAPL | 990 | 0 | 5 | 5 | 14 |
| GE | 1045 | 0 | 5 | 5 | 33 |
| GME | 591 | 0 | 5 | 5 | 5 |
| HD | 925 | 0 | 5 | 5 | 25 |
| JPM | 912 | 0 | 5 | 5 | 36 |
| KO | 913 | 0 | 5 | 5 | 38 |
| MSFT | 781 | 0 | 5 | 5 | 15 |

A backtest reading the default output sees some swing highs **up to 38 bars
before they could be known**, and the lag is variable and unbounded. This is not
a criticism of the library — it defaults to the charting use and documents the
backtest mode — but it is the single most important property for our use, and it
decides §3.

**Verdict: hand-roll the N-bar fractal**, compared against
`scipy.signal.argrelextrema` for agreement and against `pandas_ta.zigzag` for
family choice, and add no dependency.

---

## 3. Decision — the fractal family, and why

The two families differ in what the free parameter *is*:

| | **N-bar fractal** | **% deviation (ZigZag)** |
| --- | --- | --- |
| parameter | a time window | a price threshold |
| confirmation lag | **exactly N bars, always** | variable, unbounded (measured max 38) |
| scale-free in price | no | yes |
| scale-free in time | yes | no |
| degenerates when | never — always returns the window's local extrema | the threshold exceeds the instrument's realised reversal scale (§2b) |

**Adopted: the N-bar fractal**, for one reason that dominates:

**The confirmation lag is a constant, so look-ahead safety is structural rather
than remembered.** A pivot at bar *i* is emitted with `confirmed_index = i + N`,
and phase 5's fill rule can gate on it mechanically. With ZigZag the lag is
data-dependent, so every consumer carries the correction, and one that forgets
produces a backtest that looks excellent and is fiction.

**[C1] The second reason an earlier draft gave was overstated and is withdrawn.**
It claimed a time window "cannot be tuned on the magnitude axis" and is therefore
not a fitted parameter. That is false: N changes signal frequency and timing and
can be fitted to outcome as readily as a percentage. The honest statement is
narrower — **N is not fitted *here*** (§3.1 sets it from the calendar, before any
outcome exists to fit to), and the guard is procedural: the ladder is frozen in
the rule-set version, so a later tuning pass is visible rather than silent.

### 3.1 The N ladder — three fixed horizons

The operator named three horizons ("during the day", "days/weeks",
"weeks/months"). One detector parameterised by lookback, run at a fixed ladder
taken from the **calendar**:

| name | N | window (2N+1) | approximate structure |
| --- | ---: | ---: | --- |
| `short` | 5 | 11 bars | ~1 trading week either side |
| `medium` | 21 | 43 bars | ~1 trading month either side |
| `long` | 63 | 127 bars | ~1 trading quarter either side |

**[C1] This is a modelling decision and is labelled as one.** Codex is right that
5/21/63 has no evidence of mapping to market structure, and that half-days,
halts, suspensions and sparse series make "a trading month" approximate. The
claim is only that the ladder is chosen *before* and *independently of* any
outcome, and that it is frozen in the version string (§6). No stronger claim is
made for it.

Intraday is **not** in this phase — #600 made intraday candles deliberately
ephemeral and phase 0c is the unlock. `short` is the shortest *daily* structure.

### 3.2 Tie handling

A pivot high at *i* requires `high[i]` **strictly greater** than all 2N
neighbours. A plateau (two equal highs inside one window) yields **no pivot**,
not two. Rationale: an equal high is the absence of a new extreme, and emitting
both creates two "levels" at one price that then cluster into a level with an
inflated touch count — the count is the strength signal, so double-counting
corrupts the only thing the level asserts.

---

## 4. The primitives

All pure functions, no DB and no IO — the same contract
`app/services/technical_analysis.py` already holds (invariant recorded in the
`ta-analyst` skill; not being relaxed).

**[C1] Types.** Inputs are `Decimal` (the corpus stores `NUMERIC`); outputs are
`float`, matching the existing module. Codex flags float as a precision risk for
band-edge equality. Accepted and bounded: every equality decision in §4.3 is
defined with an explicit inequality direction so no comparison depends on exact
float equality, and prices are never round-tripped through a stored float in this
phase (nothing is persisted, §6).

Every detector returns a tri-state per design-doc §5 decision 5:
`fired` / `not_fired` / `not_evaluable`. A detector on a series shorter than its
warm-up returns `not_evaluable`, never an empty result.

### 4.1 Swings

```
detect_swings(bars, n) -> SwingSeries
```

`Swing(index, bar_date, kind: 'high'|'low', price, n, confirmed_index,
confirmed_date)` with `confirmed_index = index + n`. Emitting the confirmation
bar alongside the pivot bar is the whole look-ahead defence and is not optional.

`SwingSeries(swings, n, bars_evaluated, not_evaluable_indices, rule_set_version,
universe)`. **[C1]** `not_evaluable_indices` exists because a candidate
suppressed by the §5 mask cannot be represented as an emitted `Swing` — without
it, "masked" and "no swing here" are indistinguishable, which is the
vacuous-truth class this phase is supposed to avoid.

Warm-up: `2n + 1` bars. Fewer → `not_evaluable`, and **`SwingSeries.state` is the
tri-state**, distinct from `swings == []`.

### 4.2 Level clustering

No published formulation exists (§0), so the rule is fixed by construction.

- **[C1] Highs and lows cluster separately.** A level's `kind` is `resistance`
  (from swing highs) or `support` (from swing lows), and a swing high never joins
  a cluster of swing lows. A level asserts which side price approached it from;
  merging the two makes that unstateable.
- Agglomerative **single-linkage on price**, tolerance ATR-relative: two swings
  join when `|p1 − p2| <= k × ATR14`, ATR evaluated at **the later swing's bar**
  (stated because Codex found the index unspecified).
- **`k = 0.5`.** **[C1] The rationale an earlier draft gave — "the scale at which
  two touches are indistinguishable to a same-day order" — is withdrawn.** That
  is a microstructure assertion that daily OHLC cannot evidence. The honest
  statement: `k = 0.5` is a **modelling constant**, the smallest round fraction of
  the instrument's own daily range, chosen before any outcome exists and frozen
  in the version string. It is not derived and it is not tuned.
- **Why ATR-relative rather than a percentage.** A fixed percentage means
  different things on a $3 stock and a $600 one, and different things on the same
  stock in 2008 and 2017. ATR is the instrument's own current scale, so one rule
  replaces a per-asset-class table.
- **[C1] ATR over masked bars.** `technical_analysis.atr` takes non-null OHLC and
  cannot accept a masked bar. Rule: if any bar in the ATR window (period + 1
  bars) is masked by §5, the tolerance is `not_evaluable` and the swing **does not
  cluster** — it is reported as an unclustered swing, not silently merged on a
  fallback tolerance. Fail-closed.
- `Level(kind, price_low, price_high, price_mean, touches, first_touch_date,
  last_touch_date)`. **[C1] `price_mean` is the unweighted arithmetic mean of the
  clustered swing prices** — not volume-weighted (5.16% of bars have zero volume)
  and not time-decayed (that would be a second free parameter).
- **Minimum touch count is not a detector parameter.** The level is emitted with
  its count and the consumer filters. Baking in a minimum hides the denominator.

### 4.3 Level interaction

```
classify_interaction(level, bar) -> 'break_up' | 'break_down' | 'touch' | 'none'
```

**[C1] Direction is part of the output** — a close above resistance and a close
below support are different events and an earlier draft collapsed them.

Band: the level's `[price_low, price_high]` widened by the same `k × ATR14`,
evaluated at **the bar being classified** (not at the level's last touch).
No second free parameter. If that bar's ATR window contains a masked bar, the
result is `not_evaluable`.

- **Close-through, not wick-through.** A break requires the *close* strictly
  beyond the widened band. A wick beyond it with a close inside is a `touch`.
  Same distinction S7 (#2247) made for the quarantine rules and for the same
  reason: a wick is the part of a bar most likely to be a bad print, which is why
  `range_usable = false` exists at all (XPER 2024-06-03,
  `o 8.497 h 8.737 l 0.010 c 8.298`).
- **[C1] Equality is stated.** `close == band_high` is **inside** the band →
  `touch`. A break needs strict `>` (or strict `<`). Ties resolve toward "not a
  break", which is the fail-closed direction.
- `touch` requires the bar's `[low, high]` to intersect the band at all;
  otherwise `none`.

```
find_break_and_retest(level, bars, max_retest_bars) -> list[BreakRetest]
```

**[C1] Fully specified, because Codex found six aspects undefined.** A state
machine over the interaction stream, per level:

1. **Break** at bar *b*: the first `break_up` (resistance) or `break_down`
   (support).
2. **Retest** at bar *r > b*: the first bar whose `[low, high]` re-intersects the
   band, with `r − b <= max_retest_bars`.
3. **Confirmation** at bar *c > r*: the first close that is again strictly beyond
   the band on the break side. Emits `BreakRetest(break_index=b, retest_index=r,
   confirm_index=c, direction)`.
4. **Invalidation.** A close strictly beyond the band on the *opposite* side at
   any point between *b* and *c* voids the pattern; the state machine resets and
   may break again later. A retest that never confirms within `max_retest_bars`
   of *r* also voids it.
5. **Repeated breaks** are independent occurrences — the machine resets after
   each emission or invalidation, so one level can yield several.
6. **Gap-over.** If the bar following the break gaps entirely past the band
   (`low > band_high` for an up-break), no retest is possible at that bar; the
   window simply continues. If no bar re-intersects within `max_retest_bars`,
   the result is `not_fired` — **not** `not_evaluable`, because the absence was
   observed rather than unmeasurable.

`max_retest_bars = 2N` for the ladder rung the level was built at — derived from
the ladder, not a new constant.

### 4.4 Fibonacci retracement

```
fib_levels(leg) -> dict[ratio, float]      ratios 0.236 0.382 0.5 0.618 0.786
```

**[C1] The leg, its direction and the arithmetic are all stated** — an earlier
draft gave two anchors with no ordering and no formula, which does not determine
the output.

**Leg selection.** Fractals do not alternate, so "the last high and the last low"
is ambiguous. The rule: take the **most recent confirmed swing** as the leg *end*,
and the **most recent confirmed swing of the opposite kind occurring before it**
as the leg *start*. Deterministic under any run of same-kind swings.

- End is a **high** → **up-leg**. `level(r) = high − r × (high − low)`, i.e. the
  retracement measured down from the high.
- End is a **low** → **down-leg**. `level(r) = low + r × (high − low)`.

`not_evaluable` until the later anchor's `confirmed_index`; the returned object
carries `usable_from_index = max(start.confirmed_index, end.confirmed_index)`.
Also `not_evaluable` if either anchor bar is masked (§5).

### 4.5 Anchored VWAP

```
anchored_vwap(bars, anchor_index) -> AnchoredVwap
```

Cumulative from `anchor_index` inclusive, `typical_price = (H + L + C) / 3`.

- **[C1] Two indices, not one.** The sum starts at the anchor's **pivot** bar —
  that is the economically correct anchor. But if the anchor came from a swing,
  the *choice* of anchor is not knowable until `confirmed_index`, so the result
  carries `usable_from_index = anchor_swing.confirmed_index`. A signal may read
  the value at bar `t` only when `t >= usable_from_index`. Both indices are on
  the output; a consumer that ignores the second one leaks look-ahead.
- **[C1] The zero-volume rule, stated without the earlier contradiction.** A bar
  with `volume = 0` contributes `0` to both numerator and denominator **and is
  excluded from `bars_with_volume`**. That is not "treating zero as a zero
  weight" in the vacuous sense — the count is what makes the difference
  observable. If `bars_with_volume == 0` over the anchored window the result is
  **`not_evaluable`**, never `0.0`.
- Volume is never NULL in this corpus (§1.2, measured), but the function still
  treats NULL as `not_evaluable` for that bar rather than as zero, because the
  corpus is not the only future caller.
- Reads H, L, C and volume, so §5 gates it on **both** verdicts.

### 4.6 Volatility regime

```
volatility_regime(closes, window=20, lookback=126) -> Regime
```

**[C1] Corrected to Bollinger's actual published rule.** An earlier draft used a
20th/80th-percentile cut, which is invented. Bollinger defines the **Squeeze** as
BandWidth at its lowest value in six months and the **Bulge** as its highest;
six months = **126 trading days**.

- `bandwidth(t) = (bb_upper − bb_lower) / bb_middle`, where `bb_middle` is
  `sma(closes, window)` — the existing helper.
  **[C1] `bollinger_bands` returns `(upper, lower)` only, verified** — there is
  no middle to reuse and no second implementation is created by calling `sma`,
  which is what `bollinger_bands` computes the mean with anyway.
- **[C1] Denominator guard.** `bb_middle <= 0` → `not_evaluable`. Measured
  exposure: 2 bars with `close <= 0` and 9,351 sub-penny closes.
- `compression` when `bandwidth(t) == min(bandwidth over the trailing 126)`,
  `expansion` when `== max(...)`, `normal` otherwise.
- **[C1] The percentile is still reported**, as a continuous `bandwidth_pct_rank`
  — the fraction of the trailing 126 BandWidth values **strictly less than**
  `bandwidth(t)` (weak percentile; tie method stated because it changes the
  answer), with the window **including** bar `t`. It is diagnostic only; the
  classification uses Bollinger's rule above.
- **[C1] Warm-up is `window + lookback − 1` closes** (20 + 126 − 1 = 145), not
  `window + lookback`: the first BandWidth needs `window` closes and each
  subsequent one needs one more close, so 126 BandWidth values including the
  current one need 145 closes. Fewer → `not_evaluable`.

---

## 5. Quarantine consumption — fail-closed, per-field

**[C1] "Structure reads high/low, so `range_usable` is the verdict" was wrong.**
Different primitives read different fields, so the mapping is explicit:

| primitive | fields read | governing verdict(s) |
| --- | --- | --- |
| swings §4.1 | high, low | `range_usable` |
| levels §4.2 | swing prices + ATR (H, L, prev C) | `range_usable` **and** `return_usable` |
| interaction §4.3 | close, low, high + ATR | both |
| Fibonacci §4.4 | swing prices | `range_usable` |
| anchored VWAP §4.5 | high, low, close, volume | both |
| volatility regime §4.6 | close | `return_usable` |

**Masking is per field, not per bar:** `range_usable = false` masks `high` and
`low`; `return_usable = false` masks `close`. `open` has no verdict and no
primitive here reads it. **[C1]** An earlier draft masked only high/low, which
left a `return_usable = false` close available to VWAP and the regime.

**Pivot rule under masking:**

- a candidate **at** a masked bar is `not_evaluable`;
- a masked bar **inside** a candidate's comparison window cannot refute the
  candidate, so the candidate is `not_evaluable` rather than confirmed.

Fail-closed in the direction that matters: we never assert a swing we cannot
verify, and a spurious wick to `0.010` cannot suppress a real swing low by
looking like a lower low.

**[C1] Blast radius, measured on the full corpus rather than bounded.** An
earlier draft gave `1,315 × (2N+1)` as a bound; Codex flagged that it
double-counts overlapping windows, and it does — it overstates by 4–10×:

| rung | N | pivots `not_evaluable` | % of bars |
| --- | ---: | ---: | ---: |
| short | 5 | 3,915 | 0.015% |
| medium | 21 | 7,814 | 0.030% |
| long | 63 | 16,110 | 0.062% |

Input: 1,315 range-unusable bars across 669 series, 242 return-unusable across
51. Reproduce with `uv run --with scipy python scripts/verify_2279_price_structure.py`.

**The reader.** `usable_bar_filter_sql` in `price_quarantine_store.py` gates on
`return_usable` only and is keyed on `instrument_id`, so the research corpus
needs its own reader. The shape is reused verbatim and **[C1] all three
fail-closed elements are required, not just the COALESCE**:

1. an `EXISTS` on `research_price_quarantine_coverage` for the series,
2. `AND cov.rule_set_version = <current>`,
3. `AND bar_date BETWEEN cov.first_bar AND cov.last_bar`,
4. `COALESCE(q.<verdict>, TRUE) IS TRUE` for the sparse-absence case.

(4) alone is *not* fail-closed — it makes an unevaluated series read as clean.
A series evaluated at a stale rule-set version reads as **unusable**, which is
the point.

---

## 6. What is NOT built here, and why

**Nothing is persisted in this phase.** #2279 proposes "persisted derived objects
for levels/swings, versioned by rule-set hash". This spec declines, on measured
cost (§1.4: the full 25.8M-bar corpus recomputes swings in 12–25 s) and on
precedent — sql/249 makes exactly this argument when it declines to carry
indicator columns for the research corpus, and phase 3's signal ledger is where
structure-derived signals acquire a reader and the mandatory survivor-only label.

**[C1] Now measured for all six, not just swings.** §1.4 benchmarked swings
only, which Codex correctly refused to accept as settling the question. Full
corpus, 25,818,944 bars:

| primitive | full-corpus cost | persist? |
| --- | ---: | --- |
| swings, all three rungs | 76.0 s | **no** — this is what #2279 proposed persisting |
| levels | 7.6 s | **no** |
| volatility regime | 5.1 s | no |
| anchored VWAP | 0.2 s | no |
| Fibonacci | 0.1 s | no |
| **the five above, combined** | **89.0 s** | |
| level interaction, **per level** | 252.3 s | n/a — see below |
| break-and-retest, **per level** | 253.1 s | n/a |

So the no-persistence decision **holds for the objects #2279 actually proposed
storing**: swings and levels together recompute over the whole corpus in 84
seconds. A table would be storage plus a drift surface for no reader.

⚠ **The last two rows are a real phase-5 finding and are flagged rather than
explained away.** They are ~3× the swing cost *for a single level*, and a
backtest evaluates many levels across many strategies, so this multiplies. It is
**not** an argument for persisting swings — persisting them would leave this cost
unchanged, because it is a per-strategy scan, not a derived object. The cause is
identified: `_atr_at` recomputes a 15-bar Wilder window at every bar, making the
scan O(bars × period) where a rolling ATR would make it O(bars). Left unoptimised
here deliberately — it is correct, it is fast enough for this phase, and
premature optimisation against an unbuilt backtester is a guess. Phase 5 should
budget for it or fix it; it should not discover it.

What #2279 was protecting is kept without the table: **`RULE_SET_VERSION` follows
the `price_quarantine.py` pattern** — a stable rule-set id plus a SHA-256 of the
module source, returned on every result object. A rule change (N ladder, `k`, tie
rule, Bollinger lookback) changes the string, so a stored *signal* in phase 3 can
be invalidated against it.

**[C1] Over-invalidation is the deliberate, inherited trade.** Codex notes the
source hash changes on a comment edit. It does, and `price_quarantine.py` says so
in terms: *"Any edit to this module changes the version, which makes every
previously stored verdict visibly stale rather than silently mixed."* Following
the established pattern beats inventing a constants-only hash that then
under-invalidates when a rule moves into a helper. §8.5 therefore asserts the
version is *derived from module source*, not that it changes only on rule
changes — the latter is untestable and false.

**Also deferred:** intraday structure (#600 / phase 0c), trend lines, persistence.
**In this PR:** all six primitives of §4.

---

## 7. Survivorship, and one guard that turns out to be unwired

The corpus is survivor-only (#2284: every free source returns 0/259 on the
committed Form 25 cohort). Per the corpus skill and parent §0.1, a metric
computed on it is **labelled in the signal ledger, not in a comment**.

This phase produces no operator-visible metric, so there is nothing to label yet.
It carries the fact forward as a **required** field on every result object
(`universe: 'survivor_only'`), and phase 3 writes it to the ledger.
**[C1]** Codex is right that a field is weak if a consumer can bypass it: the
field has no default, so a caller cannot construct a result without stating the
universe.

**[C1] The ticker-reuse guard is not merely deferred — it is not implemented at
all, and this spec must not assume it ran.** Measured:

```
research_price_series             7,693 series
  ... with delisting_date             0
sec_form25_common_equity_delistings  317 rows, 260 with resolved_symbol
cohort symbols also in the corpus     16
```

The schema (sql/249) and the register (#2282 2c) both exist; nothing joins them.
So 16 corpus series are 2023-delisted names whose bars continue past the
delisting — the `SI`/Silvergate shape sql/249 names as its target failure mode.
**Filed as #2297.** It is a corpus-level guard and belongs to the ingest, not to
a detector, so #2279 does not implement it — but every number this phase produces
is computed over series that may be welded, and that is stated rather than
assumed away.

---

## 8. Acceptance

1. All six primitives are pure, table-tested, and return the tri-state.
   `not_evaluable` is asserted **distinguishable from an empty result**.
2. **Full-corpus** agreement run against `scipy.signal.argrelextrema(order=N)` at
   all three N — **[C1] not the 7-name panel** (Codex flagged the sample). It is
   affordable: §1.4 puts the full pass at 12–25 s per N. Per-name differences are
   explained, not tolerated.
3. Full-corpus run reporting, **exactly**: swing yield per rung, and the count of
   `not_evaluable` pivots caused by the §5 mask. Reported, not bounded.
4. A series shorter than its warm-up returns `not_evaluable` — one test per
   primitive, with the warm-up boundary asserted off-by-one in both directions
   (`warmup − 1` → `not_evaluable`, `warmup` → evaluable).
5. `RULE_SET_VERSION` is asserted to be derived from the module source (see §6),
   and to differ from `price_quarantine.RULE_SET_VERSION`.
6. Cost benchmark for **all six** primitives, not swings alone, so §6's
   no-persistence decision rests on the full workload.
7. Look-ahead tests, one per primitive that has a confirmation concept:
   `confirmed_index == index + n`; anchored VWAP exposes `usable_from_index`
   distinct from `anchor_index`; Fibonacci is `not_evaluable` before the later
   anchor's confirmation.
8. Quarantine reader tests: a series with **no** coverage row reads unusable; a
   series at a **stale** `rule_set_version` reads unusable; a bar with no verdict
   row **inside** an evaluated range reads usable.
9. Zero-volume: a window whose bars are all zero-volume returns `not_evaluable`,
   asserted distinct from `0.0`.
10. Provisional bars (last 5) fall inside the unconfirmable tail for every rung —
    asserted, not assumed (§1.3).

## 9. Acceptance results — full corpus, 7,693 series / 25,818,944 bars

`uv run --with scipy python scripts/verify_2279_price_structure.py`, 800 s wall.

**Swing yield.** Highs and lows come out within 0.01% of each other at every
rung, which is the cheapest sanity check available on a symmetric rule:

| rung | N | swings | highs | lows |
| --- | ---: | ---: | ---: | ---: |
| short | 5 | 2,646,515 | 1,323,324 | 1,323,191 |
| medium | 21 | 671,650 | 336,719 | 334,931 |
| long | 63 | 219,755 | 110,011 | 109,744 |

**scipy agreement (§8.2) — clean, with the residue fully explained.**

| rung | series compared | matched | ours only | scipy only | …at a boundary |
| --- | ---: | ---: | ---: | ---: | ---: |
| short | 6,944 | 2,425,030 | **0** | 7,882 | **7,882** |
| medium | 6,683 | 615,161 | **0** | 10,717 | **10,717** |
| long | 5,974 | 201,196 | **0** | 10,925 | **10,925** |

`ours_only = 0` everywhere, and **every** scipy-only extremum is within N of a
series end. That is the predicted and only difference: `argrelextrema` defaults
to `mode='clip'`, so at the edges it compares a bar against clipped indices —
i.e. against itself — and reports extrema in a window that does not exist. Our
detector refuses to emit a pivot it cannot confirm on both sides. Not a
tolerance; a complete account.

The comparison is restricted to the 6,944 / 6,683 / 5,974 series with **no**
masked bars, because scipy cannot represent one. Masking is measured separately
in §5.

**Series tri-state (§8.1).**

| rung | fired | not_fired | not_evaluable |
| --- | ---: | ---: | ---: |
| short | 7,277 | 327 | 89 |
| medium | 6,798 | 508 | 387 |
| long | 6,177 | 309 | 1,207 |

`not_evaluable` rises with N as it must — 1,207 series at N=63 are shorter than
the 127-bar window or were blinded by a mask. A version of this table produced
before the Codex checkpoint-2 fix understated the last column, because a blinded
empty result was reporting as `not_fired`; the run was discarded and repeated
rather than reconciled.

**Fail-closed reader (§8.8).** A nonexistent series returns 0 bars; the reader's
own SQL run at a stale `rule_set_version` returns 0 rows; and 0 series with bars
lack a current coverage row.
