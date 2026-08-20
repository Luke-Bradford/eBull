# TA strategy platform — milestone design

Operator milestone (2026-08-04): a technical-analysis layer that surfaces live TA
findings, fires strategies with entry / stop-loss / take-profit levels, measures
win rate and firing frequency, backtests within an honest window, and eventually
lets the operator allocate a capital amount to selected strategies running
**paper trades against the demo account**. No real money in this milestone.

Reviewed adversarially by Codex before any code. Four of its verdicts changed the
design and are recorded below.

## 1. What already exists — do not rebuild

| capability | location |
| --- | --- |
| `compute_indicators(bars)` — pure, no IO: sma_20/50/200, ema_12/26, macd_line/signal/histogram, rsi_14, stoch_k/d, bb_upper/lower, atr_14 | `app/services/technical_analysis.py` |
| Those columns + return_1w..1y + volatility_30d, stored per bar | `price_daily` (sql/025) |
| `price_vs_sma200`, `sma_50_200_regime`, derived at read | `derive_trend_signals` (#1989) |
| **stop-loss + take-profit rates, RSI / MACD / Bollinger / trend entry gates** | `app/services/entry_timing.py` — `_compute_stop_loss`, `_compute_take_profit` |
| momentum family consuming TA | `app/services/scoring.py::_momentum_score` |
| TA block in the LLM thesis prompt | `app/services/thesis.py::_shape_ta_state` |
| intraday candles OneMinute…FourHours, TTL cache, **no DB persistence** (#600) | `app/services/intraday_candles.py` |
| authenticated `wss://ws.etoro.com/ws`, `Trading.Instrument.Rate` pushes → `quotes` upsert + `QuoteBus` → SSE | `app/services/etoro_websocket.py`, `app/services/quote_stream.py` |
| deterministic pre-trade gate, one `decision_audit` row per invocation, kill switch / coverage / budget / sector rules, never raises on rule failure | `app/services/execution_guard.py::evaluate_recommendation` |
| `/research` lens hub with `?view=` presets, segmented control (#1917) | `frontend/src/pages/ResearchHubPage.tsx` |

The gap is **not** indicators, and not SL/TP. It is: strategies as data, a signal
ledger, win-rate measurement, backtesting, tick persistence, and the surfaces.

Existing related ticket: **#1822** — *Backtest + gated headline-weight promotion
for new scoring signals (P5 of #1815)*, still open.

## 2. Measured data facts (full population, 2026-08-03/04)

Every number below was measured, not estimated. They are the constraints the
design must satisfy.

### Universe and coverage

```
tradable instruments                        12,684
  with any price history                     5,221
  with NO price history                      7,463   (59%)
price_daily                             3,098,914 rows, 2020-10-19 → 2026-08-03
```

> ⚠⚠ **Superseded by S6 (#2246), 2026-08-04. The binding number is 1,406, not 5,221.**
>
> `5,221` counts instruments with *any* `price_daily` row. Measured on **freshness**
> instead, only **1,406 (11.1%)** have a bar in the last 7 days. `_T3_BOOTSTRAP_SELECT`
> carries `NOT EXISTS (price_daily)`, so a Tier-3 instrument leaves candle-refresh scope
> on its **first** bar and nothing takes over: 3,523 of 3,838 priced T3 have no bar in
> 30 days, and **every crypto, FX and commodity series is ~2 months stale**. Tracked as
> **#2254**, which gates phase 0a — an adjusted-price layer over a series frozen in June
> is not a price layer.
>
> ✅ **#2254 FIXED.** The T3 branch now carries a freshness-based maintenance arm
> alongside the seed arm, so a priced T3 behind the most recent trading day is
> re-admitted every night instead of leaving scope permanently. The numbers above are
> the pre-fix measurement and are kept as the record of what motivated it; the
> post-fix figures are on the PR. **The eligibility predicate that 0a inherits from
> this and from S6 is written down as decision 9 in §3** — use that, not these counts.
>
> The 7,463 break into **four** populations under **one** gate, not the two the ticket
> assumed:
>
> | population | n | why it fails the gate |
> | --- | --- | --- |
> | non-US equity (eu 2,805 · uk 989 · asia 894 · mena 61) | **4,749** | **no branch it can pass** — `fundamentals_snapshot` is SEC-fed, and their `asset_class` is not in the crypto/fx/commodity/index escape hatch |
> | `asset_class = 'unknown'` (CME 192 + 2) | 194 | deliberate; operator curates the exchange row first (#503 PR 4) |
> | `us_equity` with no fundamentals row | 2,493 | incl. the whole "Regular Trading Hours - RTH" exchange (562), OTC 54, CBOE 47 |
> | gate-passers with no eToro supply | 27 | selected nightly; probed all 27 → **0 bars**. The irreducible floor |
>
> **eToro serves the international set** — 27/28 probed non-US instruments returned bars
> current to the prior close. The gap is our selection gate, not provider supply, and it
> is an **accident**: nothing decides it, and it falls out of sourcing coverage tier from
> SEC fundamentals. All 1,383 T1/T2 instruments are `us_equity`; no non-US instrument has
> ever reached T1/T2.
>
> **Consequence for §7's open question ("which instruments get minute bars").** Defining
> the focus set as "scoring-eligible" would inherit this silently and mean **US-filer
> only**, while presenting as "the market". Define it on **price-data eligibility**
> (tradable ∧ exchange served ∧ not in population 4) — orthogonal to fundamentals
> coverage, and the thing the TA layer actually consumes. Full verdict on #2246.

### History depth per instrument

```
bars/instrument   p10=117  p25=128  median=609  p90=1036  max=1042
instruments with >=500 bars (~2y)          3,262 of 5,226
```

`max=1042` is the eToro 1000-trading-day per-request ceiling (#603).

### Volume coverage

```
bars with volume                              68.9%
instruments with NEVER any volume             1,906   (36%)
fully covered                                 1,428
partial (median 74% of bars)                  1,892
>=500 bars AND full volume coverage           1,147   (22% of priced universe)
```

### Two findings that invalidate naive approaches

**Indicators are stored on the latest bar only.**

```
total bars 3,098,914 · bars with rsi_14: 28,973 · with sma_200: 24,258
median bars carrying RSI, per instrument: 1
```

By design — `_compute_and_store_features` UPDATEs the newest row. **There is no
historical indicator series to replay.** Any backtest must recompute indicators
across history from raw OHLCV. The compute functions are pure, so this is a batch
job, not new maths.

**Prices are NOT split-adjusted.**

```
max 20-day forward return in corpus     2,964,499%
bars with >500% 20d return                    480
bars with >5000% 20d return                    24
unconditional baseline mean 20d return       +110%
```

A reverse split makes `close` jump by the split factor and every return
calculation reads it as a gain. This was caught only because the *baseline* row of
a signal-measurement table was sanity-checked; the conditional rows looked
plausible (RSI<30 → 76.8% hit rate). **Left unaddressed this inflates every win
rate the platform would ever report.** Same root cause as #2231.

> ⚠⚠ **Both claims above are falsified by S7 (#2247), 2026-08-04. Full verdict on the
> issue; decision 10 in §3 is what 0a inherits.**
>
> 1. **The corpus IS predominantly split-adjusted.** Tested against an independent
>    internal signal rather than inferred: a ≥5× step in
>    `fundamentals_snapshot.shares_outstanding` is a split signature, and **320 of the
>    330 that have price coverage (97.0%) pass through `price_daily` with no level
>    break**. AMZN and GOOG run $107→$120 through their June 2022 20:1 events — the
>    adjusted scale. The residual 10 are real and are what the §3 decision-8
>    adjustment table exists for.
> 2. **The +110% baseline is 10 observations.** The top 10 windows of 3,132,324
>    contribute **104.55 of the 105.73 points**, and they trace to one bad ingest day
>    (2025-12-24) writing sentinel closes of `0.01` / `0.0001`. Median is 0.000000%.
> 3. **It does not inflate win rates.** Dropping those 10 windows moves the mean
>    +105.73% → +1.18% and leaves the unconditional hit rate at **49.884%, unchanged
>    to three decimals**. The defect destroys *mean*-shaped statistics and leaves
>    *count*-shaped ones alone.
>
> ⚠ The danger is real but differently shaped: any rule whose **trigger** correlates
> with the defect is unbounded. A "buy the one-day ≤ −50% crash" trigger reads
> 48.95% hit / +626,683.7% mean raw, and **46.27% hit / −14.66% median** after
> quarantine — 19.5% of its firings were corrupt bars.
>
> ⚠ **The RSI<30 → 76.8% figure is therefore still unexplained** and must not be
> treated as closed. Candidates: look-ahead in indicator recomputation, survivorship
> in the current-universe replay, the overlapping-window denominator.

### The two live-data paths are NOT equivalent

| path | payload | cost model |
| --- | --- | --- |
| WebSocket `QuoteUpdate` | `bid, ask, last (nullable), quoted_at` — **no volume** | push; extra instruments cost subscribe frames, not REST calls |
| REST `IntradayBar` | `timestamp, OHLC, volume` | 55 req/min shared with market-data, universe sync, broker path |

Consequences:

- **Full-universe live prices are affordable** — via WebSocket.
- **Full-universe intraday OHLCV with volume is not.** One sweep of 12,684
  instruments is ~3.8 hours at 55 req/min.
- **Bars synthesised from WS ticks carry no volume**, so volume-confirmed rules
  are structurally impossible on them and they are not comparable to the daily
  bars already stored.

> ⚠ **Amended by S3 (#2243), 2026-08-04 — two claims above are wrong.**
>
> 1. `IntradayBar.volume` is **equity-only**. It is populated for US and Tokyo
>    equities and is always `None` for crypto and FX, so volume-confirmed rules
>    are impossible on those classes from *either* path, not just the WS one.
> 2. The two paths are **the same price series**. eToro's own `OneMinute` candle
>    is built from the **bid** (bars rebuilt from `Bid` reproduce it on 77.1% of
>    closes / 60.3% of full OHLC; from mid or ask, 0%). So "fall back to REST
>    candles for real prices" does not buy print-based bars — it buys volume, on
>    equities. The choice between the paths is about **volume and cost, not price
>    truthfulness**.
>
> Also settled there: `QuoteUpdate.last` is **not** a trade print — it never
> leaves `[bid, ask]` in 26,741 observations — and is bid-side (exactly
> `BidDiscounted` on 100% of Tokyo-equity and 97.8% of FX observations, but only
> 58.7% of crypto). So **bars must be built from `Bid`, not `last`**, as an
> empirical compatibility rule: `Bid` is what reproduces eToro's own candle.
> And the WS rate push is a field-level sparse delta whose partial shapes
> production currently discards (#2252) — the collector must carry
> per-instrument state.
>
> ⚠ Measured on demo, one 10-minute window, crypto / FX / Tokyo equities. Live
> env, US equities, HK and stressed regimes (auction, halt, wide spread) are
> unmeasured; phase 0b should not treat them as settled.

## 3. Decisions taken

1. **Win = take-profit hit before stop-loss, with a max-hold expiry.** Operator
   decision. Outcome classes: `tp_hit` / `sl_hit` / `expired` (actual return
   recorded) / `ambiguous` (one bar spans both levels and the order is
   unresolvable — see Spike 4).
2. **Full universe for live prices; watchlist for volume-bearing intraday.**
   Forced by the payload asymmetry above, not by preference.
3. **Backtester and signal ledger are built together on shared execution
   semantics**, not one before the other. The operator needs backtest numbers to
   pick strategies; forward validation needs months to reach significance. Both
   consume one definition of fills, spread, session gaps and the bar-touch
   tie-break. (Revised after Codex verdict 4; the original plan was ledger-first.)
4. **Paper trading routes through the existing `execution_guard`**, not a parallel
   paper path. If paper and live use different gates, paper results prove nothing
   about the live path — which is the entire point of paper-trading first. The
   guard's kill switch and `decision_audit` trail come free.
5. **A strategy rule returns `fired` / `not_fired` / `not_evaluable`**, never a
   bare boolean. On a NULL-volume bar `volume > sma*1.5` evaluates falsey, making
   "could not evaluate" indistinguishable from "did not fire" and silently
   corrupting the win-rate denominator. This is the vacuous-truth class already in
   the prevention log.
6. **Elliott wave is out of scope as a signal.** Wave counts are analyst
   judgement and two practitioners disagree on the same chart, which collides with
   the non-negotiable *"execution must be deterministic and hard-rule
   constrained"*. Fibonacci retracement, anchored VWAP, MACD, MAs, ATR and
   Bollinger are all deterministically computable and are in scope.
7. **The WS collector path and the visibility-driven SSE path coexist.** They are
   different consumers with different SLAs — the UI path needs sub-second latency
   for what is on screen; the collector tolerates lag but must not miss bars.
   (Revised after Codex verdict 1; the original plan flipped #498 wholesale.)
8. **Split adjustment is an auditable adjustment table**, not a live dependency on
   the unbuilt #2231 detector. Columns: source, confidence, effective date,
   factor, version. #2231 feeds it when it lands. Quarantine rules for impossible
   bars must work independently of split detection. (Revised after Codex
   verdict 3.)
9. **Price-data eligibility is defined on the price path, never on scoring
   eligibility.** Settled by S6 (#2246); this is 0a's output, stated here so no
   later phase re-derives it. `coverage_tier` and `fundamentals_snapshot` are
   SEC-fed, so any universe keyed on "scoring-eligible" is **US-filer only**
   while presenting as "the market" — all 1,383 T1/T2 instruments are
   `us_equity` and no non-US instrument has ever reached T1/T2.

   ```
   eligible for a price series  :=  instruments.is_tradable
                                AND exchanges.asset_class IS NOT NULL
                                AND exchanges.asset_class <> 'unknown'
   ```

   Orthogonal to fundamentals coverage, and the thing the TA layer actually
   consumes. Measured 2026-08-04 on the full population:

   | | n | disposition |
   | --- | --- | --- |
   | tradable | 12,684 | |
   | − `asset_class = 'unknown'` (CME 192 + 2) | 194 | **excluded** — operator curates the exchange row first (#503 PR 4). Renders "no data", not absent. |
   | = price-eligible | **12,490** | |
   | of which eToro serves 0 bars (probed all 27) | 27 | eligible but supply-less; renders "no data", not absent |

   Two consequences 0a owns:

   - **The admission is a cost decision, not a predicate change alone.** 7,242 of
     the eligible set (4,749 non-US equity + 2,493 `us_equity` with no
     fundamentals row) are currently unpriced solely because the seeding gate in
     `_T3_CANDLE_SELECT` is fundamentals-shaped. Seeding them is ~2.2 h of the
     55 req/min budget once, then they join the nightly maintenance arm fixed in
     #2254 (~1.1 s each per night thereafter). Raise `_T3_CANDLE_BATCH_SIZE`
     deliberately when this lands — it is sized for today's population, and the
     cap logs a WARNING rather than silently truncating.
   - **"Eligible but supply-less" needs a stored marker, and it is 108, not 27.**
     Measured on the full in-scope population after the #2254 backfill (2026-08-04),
     not by probe: **81 priced instruments were fetched and did not advance** (78
     `us_equity` — delisted/acquired names, oldest last bar 2021-05-21 — plus 2
     crypto and 1 fx), and the **27** unpriced gate-passers returned no bars. S6's
     probe could only see the 27 because it sampled the unpriced set; the 78 priced
     ones were invisible to it, so **the marker must cover priced instruments too**.

     ⚠ **eToro returns HTTP 200 with nothing new for these — it does not error.** A
     marker keyed on HTTP status or an exception would never fire; it has to record
     *fetch attempted, series did not advance, N consecutive times*. Until it
     exists, these are indistinguishable from "not yet refreshed", they are
     re-probed nightly forever, and every coverage number quietly counts them as
     refreshable.

   The gap is an **accident**, not a scope boundary: eToro serves the
   international set (27/28 probed non-US instruments returned bars current to
   the prior close). Do not reopen this as "should we cover non-US".

10. **Quarantine identifies "this return is not a return" — it never identifies a
    cause.** Settled by S7 (#2247); full census on the issue. A split and a bad
    print produce the same defect, so the rules never have to tell them apart —
    which is exactly why they work independently of the unbuilt #2231 detector,
    and why #2226's falsified drop-magnitude discriminator is not being
    re-proposed. **Magnitude is a trigger, not a verdict.**

    Four consequences 0a inherits:

    - **Two verdicts per bar, not one: `return_usable` and `range_usable`.** A bar
      can have a perfect close and a spurious wick (XPER 2024-06-03 is
      `o 8.497 h 8.737 l 0.010 c 8.298`). Returns are untouched; every stop-loss in
      the phase-4 outcome resolver reads as touched. A rule set that only protects
      returns hands phantom fills to the backtester.
    - **The defect lives on a transition, not a bar.** Bars either side of a level
      break are valid prices in their own unit regime. Quarantine the transition
      and the windows spanning it; keep the bars.
    - **Rules are a pure function over bars, versioned, with per-rule tests.** Both
      bugs Codex caught in this spike were "the SQL is not the written rule" — a
      raw `high/low` test where the spec said wick, and range-only rules feeding the
      return quarantine (which over-rejected by 587 windows).
    - **Containment is not classification, so the bias must be published.** The
      level-break rule rejects real moves at every threshold — demonstrably-real
      outnumber split-like ~10:1 — and turnover corroboration reaches only ~30% of
      the population (volume is equity-only, S3). The rejection census is an
      operator-visible figure, not a one-off.

    Measured rejection, full population at `a0ddf952` (3,236,874 bars):

    ```
    return_usable = false        177 bars     range_usable = false     492 bars
    transitions quarantined      884          20-bar windows           7,059 (0.225%)
    instruments w/ unresolved level break      59  (11,410 bars stranded pre-break)
    mean 20-bar return    +105.73% -> +0.95%   hit rate  49.884% -> 49.901%
    ```

    > ⚠⚠ **FOUR of those figures are SUPERSEDED by the 0a implementation (#2261,
    > merged `9bcb9f33`). Do not use the block above as an acceptance criterion.**
    > The live figures are served by `GET /price-quarantine/census`; read that, not
    > this. Unchanged and still exact: 3,236,874 bars / 5,226 instruments,
    > B1 137 / B2 140 / B3 215 / B4 40, `return_usable = false` **177**,
    > T1 **214** / T2 **556**, T3 **148 triggered / 34 turnover-admitted**.
    >
    > 1. **`range_usable = false` 492 → 532.** S7 contradicts itself: §4 assigns
    >    B4's class "both false", while §5's summary line computes
    >    `range = B1 ∪ B2 ∪ B3` (arithmetically 137+140+215). A sentinel bar stored
    >    flat at `0.0001` has an internally consistent range at a meaningless
    >    level, so every touch test against it is nonsense — §4 is right and §5's
    >    formula is the stale half. The delta is exactly B4's 40 bars.
    > 2. **T3 114 → 111, transitions quarantined 884 → 881.** The 3 missing are
    >    DEFERRED, not admitted: they touch a bar in the trailing correction
    >    window, and T3's corroboration reads `volume`, which on a part-session bar
    >    is a part-session count. S7 had no provisional concept and decided them.
    >    111 + 3 = 114; 214 + 556 + 111 = 881, + 3 = 884.
    > 3. **T3 excludes T2-explained transitions.** S7's written rule says "not
    >    explained by T1", but its own census excluded T2 overlaps — including them
    >    yields 181 triggers, not 148. Excluding them reproduces 148 exactly, and it
    >    is independently right: a ratio spanning a series hole is not a same-scale
    >    comparison, and a `price_series_break` minted from a gap strands history
    >    behind a break that never happened.
    > 4. **Breaks 59 instruments / 11,410 bars → 71 / 13,503.** ⚠ **S7 never stated
    >    its break derivation** — §8 gives counts but not the rule producing them.
    >    0a defines it explicitly: a break is a T3-quarantined transition. That is a
    >    broader set, and deliberately the safe direction — over-stranding is
    >    marked, published and reversible by an adjustment row; under-stranding
    >    silently joins across a real break. **Not tuned to reproduce 59.**
    >
    > Also inherited, and it changes nothing today but will: `eu_equity` /
    > `uk_equity` / `asia_equity` / `mena_equity` are given the **equity**
    > parameters (T = 5×), not the strict 2× default. S7's threshold table covers
    > only the five classes that had bars to measure, and the priced universe is
    > `us_equity` 4,795 / `crypto` 289 / `fx` 63 / `commodity` 44 / `index` 35 —
    > **zero non-US equities**. Once #2262's seeding gives them bars, per-class
    > p99.99 recalibration is owed.

    Unadjustable history is **marked, never dropped** — `price_series_break` rows
    plus a per-instrument **segment** model (a single `usable_from` gate discards
    joinable segments when an instrument has several breaks and only some resolve).
    Silent exclusion biases the eligible universe; every backtest states its window,
    its eligible-universe size, and its history-truncated count.

## 4. Where it lives

`/research` already models "N lenses on ONE dataset → one surface with `?view=`
presets" (#1917, `frontend/information-architecture` skill).

| surface | home | rationale |
| --- | --- | --- |
| live signals / what is firing | `/research?view=signals` | same universe, new lens; action-queue preset pattern (as Theses) |
| market heat map | `/research?view=heatmap` | same universe rendered as a treemap; first consumer of the spine |
| per-instrument TA workings + signal history | `/instrument/:symbol/chart` (**exists**) | overlays belong on the chart, not a new page |
| strategy performance (win rate, frequency, sample size, CI) | **new route** | keyed on *strategy*, not instrument — a different noun, so not a hub preset |
| automated portfolio subset + capital allocation | **strategy route, with the same positions also present in `/portfolio`** | the strategy route is an ownership-filtered control lens, not a second broker portfolio |
| collector health, strategy enable/disable, kill switch | `/admin`, `/admin/ingest-health` | `ProcessesTable` / `JobsTable` exist; the collector becomes a monitored process |

### 4.1 Product semantics correction (#2464, 2026-08-10)

The automated-strategy workspace is a portfolio control surface, not a signal
scanner and not a research-results table. Its hierarchy is fixed:

1. **Automated pot** — observed P&L, capital working, completed automated trades
   and open automated positions. Backtests never substitute for an empty live
   record and never contribute to a portfolio headline.
2. **Approved strategies** — only strategy versions that pass every promotion,
   evidence, risk, cost and execution-policy gate expose an allocation switch.
   A disabled switch beside a refused strategy is misleading; refused versions
   belong in the research pipeline and cannot be selected.
3. **Research pipeline** — one stable summary of the primary evidence window,
   its confidence interval, drawdown and blockers. Missing evidence windows are
   completeness state, not pages for the operator to browse. Detailed audit
   rows may sit behind a disclosure, but must not replace the stable summary.
4. **Forward validation** — aggregate fired, unresolved, successful and
   unsuccessful observations. The main workspace does not enumerate every
   instrument. A per-instrument ledger is an audit surface reached deliberately,
   not general activity.

There is no `pending strategy` state. A rule evaluation is `fired`,
`not_fired`, or `not_evaluable`; only `fired` becomes a durable signal. A fired
entry whose exit has not resolved is an **open observation**, not a strategy
that is about to fire.

The current `strategy_signal_scan` evaluates completed daily bars in arrears.
It therefore makes no real-time or near-trigger claim. A future pre-trigger
surface requires an explicit, versioned distance-to-threshold definition per
approved strategy and must be validated at the same sampling cadence it will
trade. Forming intraday bars must not be presented as an approximation of the
current daily-close rules, because that changes the rule and its evidence.

The three result levels from the validity proposal remain separate everywhere:
per-signal outcome, per-strategy sleeve performance, and total automated-pot
performance. Only the latter belongs in the workspace hero.

### 4.2 Exact-owned position contract (#2467, 2026-08-10)

The workspace includes a compact open-position table only when an automated
trade owns an active broker position. Each row shows the instrument, strategy,
assigned and current value, unrealised P&L and return, actual broker stop loss,
take profit and lifecycle state. Valuation must be the same contract used by
`/portfolio`; the strategy route must not create a competing valuation formula
or persist periodic valuation snapshots.

Ownership is resolved only by the exact
`(strategy_trade_id, broker_position_id)` row in
`strategy_position_ownership`. Instrument, FIFO and “only position in this
ticker” inference are forbidden because a manual and automated position may
coexist in the same instrument. The main Portfolio therefore continues to show
both positions while the strategy workspace shows only the automated subset.

An operator close from this table is a full, demo-only, risk-reducing action.
It remains available while new entries or the automated pot are paused, routes
through the existing exact-owned position manager, and writes the bounded
material-operation reason `operator_close`. The row remains `closing` until the
exact broker close order reconciles; only reconciliation releases ownership,
closes the strategy trade and feeds realised strategy P&L. No position history
or quote history table is introduced by this surface.

## 5. Phases

Phase tickets are deliberately NOT all minted up front — several are shaped by
spike results, and a ticket written against an assumption that a spike then
falsifies is worse than no ticket.

| # | phase | gated on |
| --- | --- | --- |
| 0a | adjusted-price + eligibility layer (adjustment table, quarantine rules, per-strategy eligible universe). Eligibility predicate + the two items it owns: §3 decision 9; quarantine rules + adjustment/break schema: §3 decision 10 | ✅ **SHIPPED 2026-08-04** — #2261 (`9bcb9f33`) + #2262 (`933125f5`). Live census: `GET /price-quarantine/census`. See the ⚠⚠ block on §3 decision 10 for the four figures the implementation superseded. |
| 0b | collector path — universe-wide WS subscription coexisting with the SSE path | S1, S3 |
| 0c | tick persistence — tiered: short-retention raw layer, durable 1-min bars, time partitioning | S2 |
| 1 | heat map (`?view=heatmap`) — first consumer, proves the spine end to end | 0b, 0c, S4 |
| 2 | historical indicator recomputation from raw OHLCV | 0a ✅ — **unblocked, next** |
| 3 | strategy registry + signal ledger + **execution semantics** (shared with 5) | 2 |
| 4 | outcome resolver (`tp_hit`/`sl_hit`/`expired`/`ambiguous`) | 3, S5 |
| 5 | bounded backtester — window and eligible universe stated on every number | 2, 3, **#2260** (RSI<30 → 76.8% unexplained; a backtest-validity signal of exactly phase 5's shape — do not start without it) |
| 6 | signals lens + strategy performance surface | 4, 5 |
| 7 | paper portfolio + capital allocation, through `execution_guard` | 6 |
| 8 | strategy discovery (constrained search) | 6, 7 |

## 6. Spikes — do these first

All read-only or throwaway. Each can invalidate design above it.

| # | spike | invalidates if wrong |
| --- | --- | --- |
| S1 | eToro WS subscription ceiling — subscribe in increasing batches, find the cap and the throttle behaviour | 0b entirely; a low cap forces a rotating-subscription model |
| S2 | tick volume + storage sizing over one market day — rows, bytes, peak rate | 0c tiering, partitioning, retention |
| S3 | is `QuoteUpdate.last` a true print or derived? It is nullable | whether WS-built bars are real or synthetic |
| S4 | session / calendar semantics — what "% change" means with the market closed, across currencies and exchanges | the heat map and every daily comparison |
| S5 | bar-touch tie-break — can intraday resolve TP-vs-SL order, or must some outcomes be `ambiguous`? | the operator's chosen win definition |
| S6 | why 7,463 tradable instruments have no price history | universe coverage for every surface |
| S7 ✅ | impossible-bar quarantine rules that work without the split detector | 0a, and every backtest number — **answered, §3 decision 10** |

## 7. Open questions carried from Codex review

Not yet answered; each must be closed before the phase that depends on it.

- Signal uniqueness key — strategy / instrument / timeframe / bar timestamp / params?
- Strategy **versioning**: how are historical signals tied to the strategy version
  that produced them, so old outcomes are not reinterpreted under new logic?
- What minimum sample size gates displaying a win rate at all, and are confidence
  intervals shown?
- What is the baseline comparator — buy/hold, random entry, regime, sector,
  volatility bucket? A win rate without a comparator is not evidence.
- How are correlated signals counted so firing frequency does not masquerade as
  independent evidence?
- How is multiple-testing / data-snooping controlled once strategy discovery
  exists?
- Do strategies evaluate on completed bars only, or on live forming bars?
- What prices are executable — quote mid, bid/ask, last, candle close? How are
  spread, overnight fees, currency conversion and market closure modelled?
- Delisted / renamed / merged instruments, and survivorship bias if the *current*
  universe is replayed over past dates.
- What is the fallback if WebSocket ingestion is partial or banned?
- What product boundary prevents this reading as implied financial advice?

## 8. Risks

- **Market-data correctness, not TA maths, is the milestone risk.** Codex's
  summary: *"the true milestone risk is not TA math — it is market-data
  correctness, execution semantics, and statistical validity."*
- **Scope.** This is four infrastructure projects plus a research platform. The
  phase gating above exists to stop it being attempted as one.
- **Overfitting.** Deferring strategy discovery to phase 8 is deliberate; it is
  the most overfittable component and is worthless without 4-6's evidence.
- **Backtest credibility.** Median 2.4 years of history, starting after the
  COVID crash, covers roughly one and a bit regimes. Every backtest number must
  carry its window and eligible-universe size, and must not be presented as
  regime-general.
