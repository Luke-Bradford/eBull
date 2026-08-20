# market-technician / trend-and-averages

## When to use

Reading trend state from moving averages — the 20/50/200 horizons, slope vs price-relative reads, golden/death cross, MA confluence, pullback-to-MA behaviour, ADX trend strength — or reviewing anything that consumes them.

Provenance tags per the [hub](SKILL.md): **PUBLISHED** / **MEASURED** / **CONVENTION** / **REFUTED**.

## What each horizon proxies

**CONVENTION** — the 20/50/200 triplet is folklore, not a result; no published derivation exists for the specific periods. What each usefully proxies:

| MA | proxies | typical reading |
| --- | --- | --- |
| 20-day | ~one month of positioning; the swing-trader's reference | short-term trend + the pullback magnet in fast trends |
| 50-day | ~one quarter; the intermediate trend | institutions' conventional "in an uptrend" line |
| 200-day | ~one year; the primary trend | the regime line — the one horizon with published tests (below) |

Two separate reads per MA (**CONVENTION**, the practitioner distinction — kept because collapsing them loses information):

- **Price-relative**: close above/below the MA. Binary, immediate, whipsaw-prone near the line.
- **Slope**: the MA rising or falling. Slower, but a rising 200-day with price briefly below it is a different fact from a falling one — direction of the average carries the trend claim; price-relative carries the timing claim.

Stacking (20 > 50 > 200, all rising) is the **CONVENTION** definition of an established uptrend; inverted stacking mirrors it. Confluence — price meeting two references at one zone (e.g. the 200-day sitting on an old level) — strengthens a read for the mechanistic reason in [price-structure](price-structure.md): more eyes and orders anchored at one price.

## The 200-day — the one MA with a published evidence trail

- **PUBLISHED (origin):** Joseph Granville, *A Strategy of Daily Stock Market Timing for Maximum Profit* (1960) — eight rules around the 200-day MA. William Gordon, *The Stock Market Indicators* (1968) measured it on the Dow back to the late 1800s. (Not "William Gorman" — that attribution found no support.)
- **PUBLISHED (test):** Meb Faber, "A Quantitative Approach to Tactical Asset Allocation", *Journal of Wealth Management* (Spring 2007; SSRN 962461) — monthly close vs **10-month SMA** ("corresponds closest to the 200-day"), S&P 500 1901–2012: compounded 10.18% vs 9.32% buy-and-hold, **max drawdown 83.66% → 42.24%**, invested ~70% of the time. His own framing: the gain is risk management, not return — *average* returns are nearly identical (11.22% vs 11.26%).
- **PUBLISHED (costed test):** Jeremy Siegel, *Stocks for the Long Run* (5th ed.), DJIA 1886–2012, ±1% band around the 200-day: **after all transaction costs, timing falls short of buy-and-hold on absolute return** (~8.11% net vs 9.39%) but wins risk-adjusted; "a large number of small losses" in trendless markets, offset by sidestepping 1929/1987/2008.

The honest summary: the 200-day is a **volatility/drawdown filter with roughly break-even raw return**, not an alpha source. Read it as regime context — which is exactly how `app/services/market_regime.py` uses it (SPY vs 200-SMA as the trend leg, a stated-as-conventional reference, not tuned).

## Golden cross / death cross

- **CONVENTION (origin):** no identifiable first publisher — searches including the Japanese literature (ゴールデンクロス) found no documented coinage. It is a modern name for a 50/200 relation, not a published system.
- **MEASURED (literature, mixed):** full-sample S&P 500 tests (QuantifiedStrategies, ~33 signals since 1960): ~79% of trades positive, average ~+16%/signal — and still **trails buy-and-hold on raw return**, winning only on drawdown (~33% vs ~56%) and risk-adjustment. The favourable framing depends entirely on risk-adjustment; a study of crossover signals vs passive found no statistically significant advantage.
- ⚠ Our derived signal `sma_50_200_regime` ("golden"/"death") is the CURRENT 50-vs-200 **relation**, not a crossover **event** — `ta-analyst` is explicit. A "golden" state can persist for years; do not narrate it as a fresh signal.

**PUBLISHED (the crossover family's academic arc)** — lives in `.claude/skills/quant/strategy-evidence.md` ("the four papers"): Brock/Lakonishok/LeBaron (*JF* 1992) tested 26 rules — ten MA pairs including (1,200), (2,200), (5,150), ±1% bands — and found them significant against four nulls; Sullivan/Timmermann/White (*JF* 1999) applied the Reality Check over the full 7,846-rule universe: BLL's best rule survives the snooping correction **in-sample** but is dead in the 10-year out-of-sample window and on S&P futures. Read the precise form: not "the test was wrong", but "the edge did not survive its own publication window".

## Pullback-to-MA

- **CONVENTION with one practitioner print source:** no academic formulation or peer-reviewed test of "price pulls back to the 20/50-EMA in a trend" exists. The citable practitioner formulation is Connors & Raschke, *Street Smarts* (1995) — the "Holy Grail" setup: ADX(14) > 30, first pullback to the 20-EMA, entry above the touch bar's high, stop below the swing low. Quote it as a practitioner recipe, never as evidence.
- **MEASURED (adjacent, negative):** AQR's buy-the-dip study (196 implementations, 60 years) — >60% of implementations worse risk-adjusted than passive; only 8% statistically significant. That tests drawdown-depth dips, not MA touches, but it is the nearest controlled relative and it cautions against assuming bought weakness pays.
- The readable content: in a trending market the pullback-to-MA is where trend-followers add — so the MA touch is an *attention point* (same self-fulfilling mechanism as levels), and the tradable question is whether the trend resumes, answered by the resumption bar + volume, not by the touch itself.

## EMA — what it is and where the formula comes from

**PUBLISHED:** exponential smoothing from operations research (Holt 1957; R.G. Brown 1959); the familiar multiplier **2/(N+1)** is the average-age equivalence commonly traced to Brown's *Smoothing, Forecasting and Prediction of Discrete Time Series* (1963) — the attribution is via Siligardos (*Technical Analysis of Stocks & Commodities*, March 2013), not a page-verified quote — chosen so an EMA's average data age matches an N-period SMA's. First charted stock application: P.N. Haurlan (JPL engineer), *Measuring Trend Values* (1968). EMA vs SMA is a lag/smoothness trade, not an accuracy claim; our stored `ema_12/26` exist to feed MACD's published periods.

## ADX — trend strength (Wilder 1978)

**PUBLISHED** (J. Welles Wilder Jr., *New Concepts in Technical Trading Systems*, 1978 — the same book as RSI and ATR):

- +DM = today's high − yesterday's high, counted only if it exceeds the down-move and is positive; −DM mirrors. Only the larger of the two counts on a bar.
- ±DI = 100 × Wilder-smoothed(±DM) / Wilder-smoothed(TR). DX = 100 × |+DI − −DI| / (+DI + −DI). ADX = Wilder-smoothed DX, period 14.
- ⚠ **Wilder's smoothing is the recursive `(prev × (n−1) + current) / n`**, causal — never an SMA approximation. Substituting a different smoother silently changes every value (#2260 is the RSI version of that lesson).
- ADX is **directionless** — it measures trend *strength* either way; direction comes from which DI is on top.

**CONVENTION (the threshold):** "ADX > 25 = trending / < 20 = no trend" is quoted everywhere as Wilder's, but the verifiable book-shaped rule attaches 25/20 to his **Commodity Selection Index** (a market-selection rule using ADXR), not to ADX as a per-chart gate. Plausibly his, not verifiably his sentence — tag any ADX cut as convention-with-a-published-ancestor and freeze it by construction if it gates anything (S-8 froze `ADX(14) < 20` exactly this way).

## Reading trend state — the checklist form

1. 200-day: price side + slope → primary regime (and check the benchmark's regime too — [market-dynamics](market-dynamics.md)).
2. Stacking: 20/50/200 order + slopes → established trend, transition, or disorder.
3. Distance from the 200-day in ATRs → extension (a read far from the mean has worse entry geometry regardless of trend).
4. ADX if computed → is there a trend to respect at all, either direction.
5. A crossover just fired? It is a *description of the last N bars*, already visible in 1–3 — never additive evidence on top of them.

## Cross-links

Hub: [SKILL.md](SKILL.md). Tradability of any MA rule: `.claude/skills/quant/strategy-evidence.md` (BLL/STW arc, turnover bar). Repo encoding: `.claude/skills/ta-analyst/SKILL.md` (`sma_20/50/200`, `derive_trend_signals`, the momentum-blend ramps). Regime consumer: `app/services/market_regime.py`.
