# market-technician / chart-read-protocol

## When to use

Producing a read of an actual chart (or series of charts) — the synthesis step. The sub-skills teach the layers; this file fixes the order and the output contract.

## The contract

A read is **falsifiable or it is not a read**. Every read ends with a call that names the price at which it is wrong. "Looks bullish" with no invalidation is decoration; the protocol exists to make that unwritable.

Fixed order — each layer conditions the next, which is why the order is not negotiable:

1. **Regime & higher-timeframe context** — benchmark regime (`market_regime`), the instrument's primary trend, where price sits in its 52-week range. ([market-dynamics](market-dynamics.md))
2. **Trend state vs MAs** — 20/50/200: price side, slopes, stacking, extension from the 200-day in ATRs. ([trend-and-averages](trend-and-averages.md))
3. **Structure** — confirmed levels above and below with touch counts and recency; role-reversal candidates; confluence; open gaps. ([price-structure](price-structure.md))
4. **Volatility state** — BandWidth vs its own 126-bar extremes (Squeeze/Bulge/neither), ATR% of price, band walk vs range. ([volatility-and-bands](volatility-and-bands.md))
5. **Volume character** — trend legs vs corrective legs, dry-up/expansion/climax, divergence. NULL-volume check first. ([volume.md](volume.md))
6. **Candle context at the decision point** — rejection/absorption/drive at the level; Nison criteria or no pattern name. ([candles.md](candles.md))
7. **The call** — ALL of: bias (long / short / stand aside) · entry zone · **invalidation price** (the level that proves the read wrong) · target(s) as reference geometry · horizon in bars · what would flip the bias early.

Rules on the call:

- Stand-aside is a full-status call and often the right one; it still names what would make the chart readable.
- The invalidation is structural (a level that, broken, kills the thesis), never a P&L number, and it is written before any position exists so it cannot drift ([market-dynamics](market-dynamics.md), "narrative persistence").
- Targets are attention-point geometry (~51% honesty problem — [price-structure](price-structure.md)), stated as references, not expectancy.
- Tag claims inline: a read that leans on a CONVENTION (round number, volume multiplier) says so.
- ⚠ This protocol produces a READ, not a trade. Tradability — costs, turnover, sample discipline — is `strategy-evidence`'s bar; capital is behind the promotion gates. The hub says this once and it binds here.

## Worked example — AAPL, 2026-08-13 (MEASURED)

Every number below is computed from our stored bars via the repo's own pure functions. Reproduce (dev DB):

```python
# uv run python - <<'EOF'  (from the repo root)
import numpy as np, psycopg
from app.config import settings
from app.services.indicator_series import atr_series, bollinger_series, rsi_series, sma_series
from app.services.market_regime import SQUEEZE_LOOKBACK_BARS, bandwidth, classify_regimes, is_squeeze
from app.services.price_levels import LevelScan
from app.services.price_masked_bars import load_masked_bars

with psycopg.connect(settings.database_url) as conn:
    aapl = load_masked_bars(conn, 1001).series   # instruments.symbol = 'AAPL'
    spy = load_masked_bars(conn, 3000).series    # instruments.symbol = 'SPY'
# then: sma_series/rsi_series/atr_series/bollinger_series(aapl, universe="survivor_only", ...),
# bandwidth + is_squeeze on the Bollinger components, LevelScan.build(...).at(atr=..., index=-1),
# classify_regimes on SPY's closes/bands.
```

Measured 2026-08-14 (1,050 bars, last bar 2026-08-13, close **304.76**):

| layer | measurement | reading |
| --- | --- | --- |
| 1 regime | SPY 777.50 > its 200-SMA 705.16, no Bulge → **`bull_quiet`**; AAPL at 70% of its 52w range (224.63–339.78); 63-bar return +1.5% vs SPY +5.2% → **relative laggard, −3.7 pts** | market supportive; this name is not leading it |
| 2 trend | close **below** SMA20 318.92 (5-bar slope −3.73) and **below** SMA50 308.51 (slope −0.46), **above** SMA200 280.20; extension +2.9 ATR above the 200-day | primary uptrend intact; intermediate corrective leg in progress, 20-day rolling over |
| 3 structure | live levels (≥3 touches, ≤120 bars): all BELOW price — R 280.67 (5 touches, last 2026-02-26), R 258.52 (5), S 264.65 (3), S 244.07 (4). ⚠ Level *kind* is the pivot type (swing highs → "resistance"), so an old resistance now below price is exactly the role-reversal candidate of [price-structure](price-structure.md). **No live level overhead** → upside references are the falling SMA20 and the 52w high 339.78. Key confluence: old resistance 280.67 ≈ SMA200 280.20 → role-reversal support zone ~**280–281** | one structural floor, two references above, nothing tested overhead |
| 4 volatility | BandWidth 0.1548 vs 126-bar range [0.0527, 0.2188] → neither Squeeze nor Bulge; ATR 8.60 = 2.82% of close; %b 0.21 | mid-volatility corrective drift, not a compression setup and not an expansion climax |
| 5 volume | last bar 30.7M vs 20-bar avg 37.8M → **0.81×**, contracting through the pullback | dry-up on the corrective leg — constructive under effort-vs-result (no urgent supply) |
| 6 candle | last bar closed mid-range, no Nison pattern criteria met at a decision level (price is between references) | nothing to read at this bar — which is itself information: no decision point yet |
| 7 call | below | |

**The call (2026-08-13 close):**

- **Bias:** constructive-neutral — a pullback inside a primary uptrend, in a supportive regime, on contracting volume; but a relative laggard below its falling 20-day, so no long trigger yet. **Stand aside, long-watch.**
- **Entry zone (if triggered):** either (a) a tested hold of the **280–281** confluence (SMA200 + role-reversal level) — rejection/absorption per [candles](candles.md), or (b) a reclaim of the 20-day SMA (~319, falling) on ≥1.4–1.5× volume (O'Neil's published threshold; the only citable one).
- **Invalidation:** a daily **close below ~280** — that breaks the 200-day AND the only live role-reversal support in one move; below it the read is wrong, structure is 264.65 then 258.52, and no long thesis survives.
- **Targets (reference geometry, not expectancy):** 318.92 (falling 20-day) → 339.78 (52w high). Overhead is untested — targets here are attention points only.
- **Horizon:** 20–40 bars — price sits ~1.6 ATR below the falling 20-day and ~2.8 ATR above the 280 confluence; ATR here supplies the distance scale (per [volatility-and-bands](volatility-and-bands.md)), not a forecast — the horizon is a review deadline, and an unresolved read at 40 bars expires rather than extends.
- **Flips the bias:** bearish — volume *expansion* on down bars (supply arriving; kills the dry-up read) before any level breaks; bullish — relative-strength turn vs SPY plus the volume-confirmed 20-day reclaim.

Note what the protocol did: the same facts support no "signal" — every popular one-liner ("above the 200!", "oversold soon!") dissolves into a stand-aside with two triggers, one invalidation, and a stated horizon. That is the product.

## Multi-chart sequences

Reading a series of charts (one instrument over time, or a watchlist): run the protocol per chart, then rank by (a) proximity to a decision point (step 3/6) and (b) regime fit (step 1). A chart mid-range between references is unrankable by construction — park it. Never average two charts' reads into one; each read carries its own invalidation or it is not carried.

## Cross-links

Hub: [SKILL.md](SKILL.md). Every layer's depth: the sub-skill linked at each step. Tradability of anything a read suggests: `.claude/skills/quant/strategy-evidence.md`.
