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

## 4. Where it lives

`/research` already models "N lenses on ONE dataset → one surface with `?view=`
presets" (#1917, `frontend/information-architecture` skill).

| surface | home | rationale |
| --- | --- | --- |
| live signals / what is firing | `/research?view=signals` | same universe, new lens; action-queue preset pattern (as Theses) |
| market heat map | `/research?view=heatmap` | same universe rendered as a treemap; first consumer of the spine |
| per-instrument TA workings + signal history | `/instrument/:symbol/chart` (**exists**) | overlays belong on the chart, not a new page |
| strategy performance (win rate, frequency, sample size, CI) | **new route** | keyed on *strategy*, not instrument — a different noun, so not a hub preset |
| paper portfolio + capital allocation | **route distinct from `/portfolio`** | demo and live money must never share a surface |
| collector health, strategy enable/disable, kill switch | `/admin`, `/admin/ingest-health` | `ProcessesTable` / `JobsTable` exist; the collector becomes a monitored process |

## 5. Phases

Phase tickets are deliberately NOT all minted up front — several are shaped by
spike results, and a ticket written against an assumption that a spike then
falsifies is worse than no ticket.

| # | phase | gated on |
| --- | --- | --- |
| 0a | adjusted-price + eligibility layer (adjustment table, quarantine rules, per-strategy eligible universe) | S6, S7 |
| 0b | collector path — universe-wide WS subscription coexisting with the SSE path | S1, S3 |
| 0c | tick persistence — tiered: short-retention raw layer, durable 1-min bars, time partitioning | S2 |
| 1 | heat map (`?view=heatmap`) — first consumer, proves the spine end to end | 0b, 0c, S4 |
| 2 | historical indicator recomputation from raw OHLCV | 0a |
| 3 | strategy registry + signal ledger + **execution semantics** (shared with 5) | 2 |
| 4 | outcome resolver (`tp_hit`/`sl_hit`/`expired`/`ambiguous`) | 3, S5 |
| 5 | bounded backtester — window and eligible universe stated on every number | 2, 3 |
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
| S7 | impossible-bar quarantine rules that work without the split detector | 0a, and every backtest number |

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
