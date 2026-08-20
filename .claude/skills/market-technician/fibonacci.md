# market-technician / fibonacci

## When to use

Anyone proposes, cites, or asks about Fibonacci retracements/extensions. This file exists to prevent fib-mysticism, not to teach it — read the evidence section before using a fib level anywhere.

Provenance tags per the [hub](SKILL.md): **PUBLISHED** / **MEASURED** / **CONVENTION** / **REFUTED**.

## The mechanics, as practiced

**CONVENTION** (platform convention; no canonical published source rule fixes the level set):

- Anchor a swing high and swing low (already discretionary — two readers, two anchors; and per [price-structure](price-structure.md), a swing is only confirmed N bars late).
- Retracements at 23.6 / 38.2 / 50 / 61.8 (some platforms add 78.6 = √0.618) percent of the leg, drawn as horizontal lines.
- Extensions (127.2 / 161.8 / 261.8) project beyond the origin for targets.
- The ratios come from adjacent-term limits of the Fibonacci sequence (0.618 = 1/φ, 0.382 = 1 − 0.618, 0.236 = 0.382 × 0.618).

⚠ **50% is not a Fibonacci ratio.** It comes from Dow Theory's halfway-retracement observation (and Gann's independent 50% rule), grafted onto the fib tool. StockCharts states it plainly: "The 50% retracement is not based on a Fibonacci number." A tool whose most-used level is not from its own namesake sequence is telling you what it actually is: a set of round-fraction attention points.

## The evidence status — honest and short

- **MEASURED (literature, best test = null):** Tsinaslanidis, Guijarro & Voukelatos, "Automatic identification and evaluation of Fibonacci retracements", *Expert Systems with Applications* 187 (2022), 115893 — algorithmic fib zones across three equity markets: bounce behaviour on Fibonacci and non-Fibonacci zones was **statistically indistinguishable**. Bounce probability rose with zone *width* — mechanically, wider zones catch more bounces, which the authors offer as an explanation for the tool's popularity without profitability.
- **MIXED (weak positives):** Bhattacharya & Kumar (*Annals of Economics and Finance*, 2006) and scattered small-market studies report favorable rates, in venues below the replication literature's tier. No top-journal support was located (2026-08-14 sweep).
- ⚠⚠ **REFUTED (ours, 2026-08-09, #2437):** in our own backtest, **placebo fib levels at 29/44/71% BEAT the real 38.2/50/61.8%**, fake support beat real pivot-low support, and both *real* variants sat below the no-level baseline. A placebo arm that gets the identical measurement is conclusive about the levels carrying no specific information — whatever bounce statistics fib levels show, arbitrary nearby fractions show as much or more. Support + Fibonacci as entry conditions are on the closed-do-not-reopen list (`docs/proposals/ta/2026-08-09-plan-of-attack.md`, merged via PR #2444).

## What is left after the evidence

The honest ceiling: fib levels work — at most — as **self-fulfilling attention points**. Enough participants draw the same lines from the same obvious swings that orders cluster near them, the same mechanism (Osler; round numbers) that makes any widely-watched reference behave level-like. Two things follow:

1. Nothing is special about the *ratios*. Our placebo result is precisely what the attention-point account predicts: the crowd's coordination, where it exists at all, is approximate — and a level's power comes from being watched, not from φ.
2. A fib level with confluence (an old structural level, a round number, an MA at the same zone) reduces to the OTHER references — read those directly ([price-structure](price-structure.md)); the fib line adds narrative, not information.

## Rules for this repo

- Never cite a fib level as evidence for anything, in a spec, thesis, or read. If a chart-read mentions one, it must carry the CONVENTION tag and stand only where confluence with real structure exists.
- Never build a strategy leg on fib levels — placebo-refuted here, null in the best published test, closed on #2437. A proposal that reaches for them must first explain what would distinguish it from the 2026-08-09 placebo arm.
- Extensions-as-targets follow measured-move logic and inherit its honesty problem (~51% by the friendliest practitioner count — [price-structure](price-structure.md)): reference geometry, not expectancy.

## Cross-links

Hub: [SKILL.md](SKILL.md) (refuted list). Mechanism for why *any* watched line can behave level-like: [price-structure](price-structure.md) (Osler 2003). Our implementation (`price_structure.fib_levels`, `FIB_RATIOS = (0.236, 0.382, 0.5, 0.618, 0.786)`, inheriting the swing-confirmation lag): `.claude/skills/data-sources/market-structure.md` — which carries the same 0.5-is-not-fib warning. The transferable lesson from the 2026-08-09 result is the placebo-arm technique itself: any level-shaped claim needs a fake-twin control that receives the identical measurement.
