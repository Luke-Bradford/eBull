---
name: market-technician
description: Professional chart-reading discipline — layered chart-read protocol (regime, trend, structure, volatility, volume, candles → falsifiable call), evidence-tagged TA domain knowledge (PUBLISHED/MEASURED/CONVENTION/REFUTED), and the refuted-on-our-data list. Read before producing any chart read, citing any TA formulation, or reviewing TA-flavoured specs. Routes to sub-skills per layer; defers to quant/strategy-evidence on tradability and to ta-analyst on our stored encoding.
---

# market-technician

## When to use

Producing or reviewing a read of a chart; citing any technical-analysis formulation, threshold or pattern; writing or reviewing a spec that consumes TA concepts. This family owns **"what does this chart say"**. It does NOT own "is this tradable" (→ `.claude/skills/quant/strategy-evidence.md`, whose turnover/cost/significance bars and promotion gates bind unchanged) and it does NOT own "what does our stored indicator mean here" (→ `.claude/skills/ta-analyst/SKILL.md`). Capital decisions stay behind strategy-evidence's bars and the promotion gates — nothing in this family funds anything.

## The provenance tag system — untagged claims are defects

Every factual claim in this family carries exactly one tag:

| tag | means | example |
| --- | --- | --- |
| **PUBLISHED** | a citable formulation — author, work, chapter/page — as the author stated it, not folklore-drifted paraphrase | Bollinger's Squeeze = BandWidth lowest in six months (*Bollinger on Bollinger Bands* ch. 21) |
| **MEASURED** | verified on our data or in cited empirical literature, with the command/citation that reproduces it | the published Squeeze fires ~17× less than S-4's percentile cut (commit `82915ee5`) |
| **CONVENTION** | practitioner folklore with no published basis — kept where it is load-bearing market psychology, labelled so nobody cites it as evidence | round numbers; "1.5–2× volume confirms"; the 20/50/200 triplet itself |
| **REFUTED** | killed by the literature or our own backtests — kept prominently, because a technician who does not know what fails is a hazard | fib levels (placebo-beaten here + null in the best published test) |

Rules: verify citations at write time, never from memory (#2279 is the precedent — an invented "20th/80th percentile" rule shipped into a spec where Bollinger's published six-month rule existed). Where no published formulation exists, say so and fix the rule by construction in a version hash (`price_levels.py` is the model). A drifted paraphrase of a real source is worse than a CONVENTION tag — it launders folklore with a citation.

## Step-0 vet of the existing TA surface (2026-08-14)

Recorded per the session brief; fixes applied where one-line:

1. **The academic canon already lives in `quant/strategy-evidence.md`** — §2.12 (LMW 2000 pattern-testing recipe), "the four papers" (Osler 2003 → BLL 1992 → STW 1999 → LMW 2000), the maths-grounding table, Park & Irwin 2007 context, and the Wyckoff computability audit. This family POINTS there and does not duplicate; one precision upgrade from this session's verification: STW's result is "BLL's best rule survives the snooping correction in-sample but is dead in the 10-year out-of-sample window and on S&P futures" — sharper than "no profitable rule", same conclusion.
2. **`ta-analyst/SKILL.md` is our encoding, not domain knowledge** — correct scope, current, and already carries the current-state vs strategy-platform boundary. Gap fixed by cross-link: it predates `market_regime.py` / `price_levels.py` (both 2026-08-14) and this family; pointers added there in this PR.
3. **The code precedent pair is load-bearing**: `app/services/market_regime.py` (both legs PUBLISHED: 200-SMA trend + Bollinger ch. 21 Squeeze/Bulge) vs `app/services/price_levels.py` (no published formulation exists for level clustering — says so, frozen by construction). The two files are deliberately different in provenance and this family agrees with both.
4. **One resolved citation debt**: `indicator_series.bollinger_series` flags population-σ as "a citation still owed". Verified 2026-08-14 from Bollinger's own site ("we use the population calculation for standard deviation") — recorded in [volatility-and-bands](volatility-and-bands.md); the module is identity-hashed, so the docstring is left untouched.
5. **S5-S10 proposal spot-checks hold**: S-6's 1.2× volume multiple was recorded as convention and verification confirms no published source for ANY specific multiplier except O'Neil's 40–50%-above-average (~1.4–1.5×); S-7's "RSI rarely reaches 30 in an uptrend" and S-8's ADX<20 are convention, correctly frozen; the ADX-25 "Wilder threshold" is *plausibly* his but verifiably attaches to his CSI market-selection rule, not per-chart ADX ([trend-and-averages](trend-and-averages.md)).
6. **`data-sources/market-structure.md` was found late in the vet and is the closest neighbour** — it owns the concept → published source → OUR implementation map (`indicator_series.py` / `price_structure.py`), the measurement traps (shared-print, unfillable-window, micro-cap means, back-adjusted stratification) and the trial budget. Division of labour: market-structure answers "what does our code compute and how do I measure it without lying to myself"; this family answers "how does a technician read a chart, and what is each concept's honest evidence status". Cross-links added; no contradictions found between it and this family (same #2260/#2279 verdicts, same 0.5-not-fib warning, same by-construction rule).

## The layered chart-read protocol

Fixed order, each layer conditioning the next — full contract + worked example in [chart-read-protocol](chart-read-protocol.md):

1. Regime & higher-timeframe context → 2. Trend vs MAs → 3. Structure (levels, touch counts) → 4. Volatility state → 5. Volume character → 6. Candle context at the decision point → 7. **The call**: bias · entry zone · invalidation price · targets · horizon · what flips it.

**A read with no invalidation level is not a read.** Stand-aside is a first-class call.

## Sub-skills

| file | owns |
| --- | --- |
| [trend-and-averages](trend-and-averages.md) | 20/50/200 horizons, slope vs price-relative, golden/death cross, pullback-to-MA, EMA provenance, ADX |
| [volatility-and-bands](volatility-and-bands.md) | Bollinger construction (population σ), %b/BandWidth, six-month Squeeze/Bulge, band walks, ATR as the distance unit, Keltner, TTM contrast |
| [volume](volume.md) | effort vs result, confirmation folklore, climax/dry-up, OBV, VWAP (execution benchmark) + anchored VWAP, NULL-volume caveat |
| [price-structure](price-structure.md) | level mechanism (Osler), role reversal, pivots + confirmation lag, trendlines' honest status, gaps taxonomy, false breaks/springs, measured moves |
| [fibonacci](fibonacci.md) | the mechanics as practiced + the evidence status (null + placebo-refuted here); exists to prevent fib-mysticism |
| [candles](candles.md) | anatomy as intrabar narrative, Nison's actual criteria, the negative empirical record, candles as confirmation-only |
| [market-dynamics](market-dynamics.md) | regimes, impulse/correction, breakout→retest, reversion after extension, relative strength, gap dynamics, short-side asymmetries, failure modes |
| [chart-read-protocol](chart-read-protocol.md) | the synthesis: the seven-step read + a MEASURED worked example (AAPL 2026-08-13) |

## Refuted on our data — read before proposing anything TA-shaped

Pointers, not re-derivations; each verdict lives with its evidence:

- **RSI<30 "76.8% win" did not reproduce.** Causal Wilder recompute on the full population: 51.846% vs 50.585% baseline on `price_daily` (n=311,332 triggers); BELOW baseline on the 25.7M-bar research corpus. The original figure was never reproduced; attribution by elimination points at look-ahead. (#2260; also `data-sources/market-structure.md`'s RSI row)
- **S-1…S-4: 0 of 20 hold-out years beat buy-and-hold** (4 strategies × 5 years, 2022–2026); on the primary holdout all four trail buy-and-hold by 44–147 pts with S-1 at 91.75× annual turnover — the turnover bar (`strategy-evidence` §2.1) predicted the ranking ex ante. (`docs/proposals/ta/2026-08-12-s1-s4-primary-holdout-result.md`)
- **Fake fib/support levels BEAT real ones.** Placebo fib 29/44/71 > real 38.2/50/61.8; fake support > real pivot-low support; both real variants below the no-level baseline. Univariate feature testing was itself the broken instrument (day/year clusters disagreeing in sign). (2026-08-09, `docs/proposals/ta/2026-08-09-plan-of-attack.md`; [fibonacci](fibonacci.md))
- **S-4's invented bottom-quartile ATR cut fired ~17× more than Bollinger's published Squeeze on the same leg** — 1,667 signals vs 26 on the same 71 instruments (measured 2026-08-14, commit `82915ee5`). An invented percentile admits ordinary bars; a published extreme is rare by construction.
- **Gap-down fade: dead since 2020** (t 1.39 in 2020+, vs t 5.73 pooled — the pooled number was the illusion). **≥12%-drop continuation short: positive per-trade, lost 13.76–47.72% on all 8 finite-capital sizing arms** (#2481). Both closed; do not resurrect. ([market-dynamics](market-dynamics.md))

## Cross-links

Tradability, costs, significance, family viability: `.claude/skills/quant/strategy-evidence.md` (MANDATORY before proposing any strategy — this family never overrides it). Concept → our implementation + measurement traps: `.claude/skills/data-sources/market-structure.md` (MANDATORY before speccing any indicator/level/pattern). Our stored indicators and their encoded ramps: `.claude/skills/ta-analyst/SKILL.md`. Platform code this family must agree with: `app/services/market_regime.py`, `app/services/price_levels.py`, `app/services/price_structure.py`. Strategy set consuming these concepts: `docs/proposals/ta/2026-08-14-strategy-set-s5-s10.md`.
