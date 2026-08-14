# market-technician / volatility-and-bands

## When to use

Reading volatility state on a chart — Bollinger Bands, BandWidth, Squeeze/Bulge, ATR-denominated distances, band walks vs reversion, Keltner contrast — or reviewing any code/spec that consumes them.

Provenance tags per the [hub](SKILL.md): **PUBLISHED** / **MEASURED** / **CONVENTION** / **REFUTED**.

## Bollinger Bands — the published construction

**PUBLISHED** (John Bollinger, *Bollinger on Bollinger Bands*, 2001; rules verbatim on bollingerbands.com/bollinger-band-rules, verified 2026-08-14):

- Middle band = 20-period SMA; bands at ±2 standard deviations. His defaults, unchanged "35 years".
- **POPULATION standard deviation** (divide by N, not N−1) — his own site states "the population calculation for standard deviation". Our `indicator_series.bollinger_series` matches; its docstring flags this citation as "still owed" — it is now verified, but the module is identity-hashed, so the resolution is recorded here rather than edited into the file.
- **Rule 11 — the multiplier moves with the period:** 2.0 at 20 periods, 2.1 at 50, 1.9 at 10. Changing the window without changing the multiplier is not "the same indicator, longer".
- **%b** = (price − lower) / (upper − lower). ≥1 means closed above the upper band; ≤0 below the lower; 0.5 is the middle band.
- **BandWidth** = (upper − lower) / middle. Normalised by the middle band so it is comparable across price levels — the raw spread is not.
- **Rule 14 — his own statistical disclaimer:** "Make no statistical assumptions based on the use of the standard deviation calculation… The distribution of security prices is non-normal." The bands are an adaptive envelope, not a confidence interval; "price should be inside the bands 95% of the time" is a misreading Bollinger himself rejects (secondary reproductions of his rationale put actual containment at 20/2.0 around 88–89% — a figure not independently verified here; the load-bearing point is Rule 14 itself).

## The Squeeze and the Bulge — six-month EXTREMES, not percentiles

**PUBLISHED** (*Bollinger on Bollinger Bands* ch. 21; Bollinger's own screener defines it verbatim: "Squeeze means a stock's BandWidth is at its narrowest (lowest) in 6 months", "Bulge… widest (highest) in 6 months"):

- **Squeeze** = BandWidth at its lowest reading in six months (126 trading days). Volatility is cyclical — contraction precedes expansion; the Squeeze marks the setup, not the direction.
- **Bulge** = BandWidth at its highest in six months — the mirror extreme, and the `volatile` flag in our regime classifier. **CONVENTION** on top of it: reading a Bulge as volatility exhaustion near trend ends is the common practitioner gloss, not part of the verified definition.
- ⚠ **REFUTED locally:** a 20th/80th-percentile BandWidth cut was invented for exactly this purpose on #2279 and caught at review. A distributional quantile and a six-month extreme disagree precisely in the quiet regimes the Squeeze exists to find. `app/services/market_regime.py` encodes the published rule (`SQUEEZE_LOOKBACK_BARS = 126`) and names the failed invention in its docstring.
- **MEASURED:** the published Squeeze is ~17× more selective than S-4's invented bottom-quartile ATR cut on the same 71 instruments — 26 signals from 21 names vs 1,667 (commit `82915ee5`, 2026-08-14). A genuine six-month extreme *should* be rare; a percentile cut admits ordinary bars by construction.
- ⚠ Distinct construct: the **TTM Squeeze** (John Carter, *Mastering the Trade*, 2005) is Bollinger Bands entirely inside Keltner Channels — a cross-indicator containment rule with no lookback window. Different rule, different author; do not conflate with Bollinger's ch. 21 rule.

## Band walks vs reversion — the misread that costs most

**PUBLISHED** (Bollinger's own rules 6–8, verbatim):

- Rule 6: "Tags of the bands are just that, tags not signals. A tag of the upper Bollinger Band is NOT in-and-of-itself a sell signal."
- Rule 7: "In trending markets price can, and does, walk up the upper Bollinger Band and down the lower Bollinger Band."
- Rule 8: "Closes outside the Bollinger Bands are initially continuation signals, not reversal signals."

So the same event — price at the upper band — reads opposite ways by regime: in a trend it is strength (a band walk); in a range it is extension. Deciding which regime you are in comes FIRST (see [market-dynamics](market-dynamics.md)); the band tag alone decides nothing. Our scoring encodes both readings deliberately: Bollinger position scores as trend *strength* in the momentum blend while RSI/stoch score exhaustion — `ta-analyst` says "do not 'fix' one to match the other".

**MEASURED (literature):** academic tests of the naive band rules (Lento, Gradojevic & Wright 2007; Lento & Gradojevic 2011; Lento 2009 — US/Canadian indices + FX, then eight Asia-Pacific markets) find the touch-and-fade rule loses to buy-and-hold after costs, and in the markets they tested the *contrarian* application (fading the naive signal) was the profitable side. That is the same content as Rules 6–8: the naive reading is the wrong reading.

## ATR — the unit of instrument-relative distance

**PUBLISHED** (J. Welles Wilder Jr., *New Concepts in Technical Trading Systems*, 1978 — same book as RSI, ADX, Parabolic SAR):

- True Range = max(high − low, |high − prev close|, |low − prev close|). The prev-close arms exist so gaps count as range.
- ATR = Wilder-smoothed TR (recursive `(prev × (n−1) + current) / n`, causal), default 14. NOT a simple average — #2260 is what a non-causal smoother substitution costs.

Use ATR as the *denominator* for distances: "within 0.5 ATR of the level", "stop 2 ATR below entry". A $400 stock and a $4 stock have no shared notion of "close"; percent gets it wrong in opposite directions at the two ends (this is why `price_levels.py` clusters in ATR units).

**Stop multiples — provenance is messier than folklore admits:**

- **PUBLISHED:** Wilder's own Volatility System used a constant of **2.8–3.1 × ATR(7)**. The Chandelier Exit (Charles Le Beau, popularized in Elder's *Come Into My Trading Room*, 2002) hangs **3 × ATR** off a 22-day extreme.
- **CONVENTION:** the popular "2 × ATR stop" has no identifiable published origin — it is the retail short-horizon variant of the 3× convention. Our own `entry_timing` stop (entry − 2.0 × ATR(14), floored) is this convention, encoded and frozen; treat it as by-construction, not cited.
- **MEASURED (ours):** no stop level catches a gap — the 2026-08-09 short-side backtest filled 141 of 1,402 stops at an open beyond the level; worst trade −87% *with* a 20% stop. An ATR stop bounds intent, not outcome.

## Keltner Channels — the contrast

**PUBLISHED** (Chester W. Keltner, *How to Make Money in Commodities*, 1960 — "Ten-Day Moving Average Trading Rule"): centre = 10-day SMA of typical price (H+L+C)/3; offset = 10-day SMA of the daily range. No ATR, no multiplier — the modern form (EMA ± k × ATR, typically 20-EMA ± 2 × ATR(10)) is a later revision attributed to Linda Bradford Raschke (1980s); the attribution is consistent across secondary sources but no primary Raschke citation was found — treat the modern parameters as **CONVENTION** with a published ancestor.

Why the contrast matters (mechanical property of the formulas, not a claim about markets): a rolling σ re-prices a large move immediately and quadratically, an averaged range linearly — so Bollinger bands expand faster after a shock than a Keltner envelope on the same bars. That difference is exactly what the TTM Squeeze exploits — σ compressing inside the ATR envelope.

## Reading volatility state — the checklist form

1. BandWidth now, vs its own 126-bar min/max → Squeeze / Bulge / neither. (Six-month extreme, never a percentile.)
2. ATR as % of close → the instrument's recent average true range; the distance SCALE for every level/stop you quote, not a forecast of movement.
3. Price vs bands: %b + whether recent closes hug one band (walk) or oscillate between them (range).
4. Squeeze resolving? Direction comes from the breakout leg and regime, never from the Squeeze itself.

## Cross-links

Hub: [SKILL.md](SKILL.md). Regime consumption of BandWidth: `app/services/market_regime.py` (published-rule precedent). Tradability of any band-based rule: `.claude/skills/quant/strategy-evidence.md` (the Lento-line results above are reads, not strategies). Repo encoding of bands/ATR on `price_daily`: `.claude/skills/ta-analyst/SKILL.md`.
