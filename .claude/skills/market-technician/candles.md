# market-technician / candles

## When to use

Reading individual bars and candlestick patterns — anatomy, the major Nison patterns, inside/outside bars — at a decision point on a chart.

Provenance tags per the [hub](SKILL.md): **PUBLISHED** / **MEASURED** / **CONVENTION** / **REFUTED**.

⚠ **The frame comes first:** in every located developed-market test with a proper null (below), candlestick patterns alone showed no value on daily bars. They are kept in this family as a **confirmation layer** — the intrabar narrative at a level or decision point — never as standalone signals. A read that leads with a candle name has the hierarchy inverted; the [chart-read-protocol](chart-read-protocol.md) puts candle context sixth of seven, deliberately.

## Anatomy — the bar as intrabar narrative

**CONVENTION** (auction-logic reading — coherent, untested as such): a candle compresses one session's auction: body = where the session settled relative to where it opened; wicks = excursions that FAILED to hold. That is the entire content, and it is real content at the right place:

- Long lower wick at support = sellers pushed, buyers absorbed, close reclaimed — rejection.
- Long body closing near its extreme = one-sided conviction, little fade.
- Small body, long both-side wicks (high-wave/doji family) = auction unresolved; indecision, meaningful mainly after a directional run.
- Body-to-range ratio is the quick quantitative handle (**CONVENTION** thresholds; e.g. Nison's hammer wants the lower shadow ≥ 2× the body).

The same anatomy is regime-sensitive: a doji inside a quiet range is nothing; the same doji after eight expanding trend bars is the first no-supply/no-demand print.

## The major patterns (Nison), defined precisely

**PUBLISHED:** Steve Nison, *Japanese Candlestick Charting Techniques* (1991; 2nd ed. 2001) — the book that brought candlesticks West. Definitional details matter because retail paraphrase drifts:

| pattern | Nison's criteria | note |
| --- | --- | --- |
| engulfing | second **real body** totally engulfs the prior real body; opposite colours; requires a definable prior trend | **bodies only** — "ideally the body should engulf the shadows as well, but this is not a requirement" |
| hammer | after a downtrend: small body at the TOP of the range, lower shadow ≥ 2× body, little/no upper shadow | body colour not critical |
| shooting star | after an uptrend: small body at the BOTTOM, upper shadow ≥ 2× body, little/no lower shadow | inverse of hammer |
| doji | open ≈ close ("many technicians are flexible here") | indecision, weight after a trend leg |
| morning / evening star | long trend-direction candle → small body gapping away (colour unimportant) → third candle closing deep into the first body | three-bar; the middle bar is the stall |
| harami | small real body contained inside the prior bar's unusually large real body | "a state of truce"; = inside bar with body emphasis |

Inside/outside bars are the Western-terminology cousins (range containment / range engulfment, wicks included) — **CONVENTION**, same auction logic: contraction then resolution.

## What testing shows

**MEASURED (literature)** — every located developed-market test with a proper null is negative (2026-08-14 sweep; scope is what was located, not the whole literature):

- Marshall, Young & Rose, "Candlestick technical trading strategies: Can they create value for investors?", *J. Banking & Finance* 30 (2006), 2303–2323: 35 DJIA stocks 1992–2002, 28 candle rules, bootstrapped OHLC null — **no value**; actual-series profitability weaker than the bootstrap ~75% of the time.
- Marshall, Young & Cahan, "Are candlestick technical trading strategies profitable in the Japanese equity market?", *RQFA* 31 (2008), 191–207: top-100 TSE stocks 1975–2004 — **no value in candlesticks' home market**, in any sub-period or regime.
- Fock, Klein & Zwergel, "Performance of Candlestick Analysis on Intraday Futures Data", *J. Derivatives* 13(1) (2005), 28–40: 5-min DAX/Bund futures, 19 patterns — no predictive power.
- Horton, "Stars, crows, and doji: The use of candlesticks in stock selection", *QREF* (2009): 349 stocks — "not recommended".
- The located positives cluster in Taiwanese studies (Goo, Chen & Chang 2007; Lu, Shiu & Liu 2012) using holding-period optimization the negative papers control against.

So: **as standalone signals, refuted where tested carefully.** What the tests do NOT cover is the way this family uses them — as the last-layer read *conditional on structure* (a hammer AT a tested support inside an uptrend regime). That conditional use is **CONVENTION**: coherent with the Osler/auction mechanism, and untested — which is a different status from validated, and must be said that way. LMW 2000 (`strategy-evidence` §2.12) is the template if anyone wants to test it: algorithmic definition → conditional-vs-unconditional distribution → random-entry null.

## Reading candles at the decision point — the checklist form

1. Only read candles AFTER regime, trend, structure, volatility and volume (protocol order) — a candle modifies a thesis, it never creates one.
2. At the level: rejection wick, absorption body, or drive-through? Does the intrabar story agree with the structural story?
3. Pattern names only when Nison's actual criteria are met (bodies vs wicks; the trend prerequisite) — a green bar near support is not "a hammer".
4. Confirmation bar: Nison treats confirmation as pattern-specific (explicit for some patterns, e.g. the hanging man; optional for others). Waiting for next-session follow-through before acting is this family's discipline — **CONVENTION**, not his universal rule.
5. Tag honestly: any candle-based statement in a read is CONVENTION unless it cites the table above (PUBLISHED for the definition, never for efficacy).

## Cross-links

Hub: [SKILL.md](SKILL.md). Test recipe for any pattern claim: `strategy-evidence` §2.12. Structure context candles confirm: [price-structure](price-structure.md). Volume agreement at the bar: [volume.md](volume.md).
