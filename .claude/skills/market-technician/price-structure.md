# market-technician / price-structure

## When to use

Reading support/resistance, swing pivots, trendlines/channels, gaps, false breaks and measured moves — or reviewing anything that consumes a level.

Provenance tags per the [hub](SKILL.md): **PUBLISHED** / **MEASURED** / **CONVENTION** / **REFUTED**.

## Why levels exist at all — the mechanism, before any drawing

**PUBLISHED (the strongest pro-TA evidence in the literature):** Osler, "Currency Orders and Exchange Rate Dynamics", *JF* 58(5) (2003) — actual dealer order-book data: take-profit orders cluster **at** round numbers (trends reverse at levels), stop-loss orders cluster **just beyond** them (breaks accelerate once a level goes). Mechanistic, not statistical — the order book actually contains the clustering. ⚠ Caveat that travels with it: the order data is **FX**; for equities, stop clustering is asserted convention with no located peer-reviewed demonstration (stops sit broker-side, off-book). `strategy-evidence` "four papers" carries the full arc.

Two consequences worth internalizing:

- A level is an **attention point**, not a physical floor. It works to the extent orders anchor there — which is also why round numbers matter (**CONVENTION**, load-bearing psychology) and why our own placebo test could beat "real" levels with fake ones (see [fibonacci](fibonacci.md) and the hub's refuted list).
- The interesting events are AT the level: rejection, absorption, or break-and-go. The level itself predicts nothing; the reaction is the information.

## Support/resistance formation and role reversal

**PUBLISHED:** Edwards & Magee, *Technical Analysis of Stock Trends* (1st ed. 1948; ch. 13 "Support and Resistance" in the modern editions): "these critical price levels constantly switch their roles from Support to Resistance and from Resistance to Support… A former Top, once it has been surpassed, becomes a bottom zone in a subsequent downtrend." The classic rationale is trapped inventory: buyers of the old top who suffered through a decline sell "at break-even" on the retest — a positioning story consistent with Osler's order data.

Reading rules (E&M-derived, **CONVENTION** at the edges):

- More touches + more volume transacted at a zone = more significant (**CONVENTION**, E&M-derived). The counter-consideration is an inference from Osler's mechanism, equally **CONVENTION**: each successful defense plausibly *consumes* the resting orders that made it work, so a level defended four times is both more widely watched AND possibly more depleted on the fifth approach. Hold both; neither is measured.
- Zones, not lines. Draw the level as a band scaled to the instrument's volatility — our `price_levels.py` clusters pivots within **0.5 × the caller-supplied ATR** (scan convention: ATR(14) at the decision bar — the module warns a later ATR is a leak), exactly this idea, frozen by construction.
- Recency decays relevance (`MAX_TOUCH_AGE_BARS = 120` in our encoding — a by-construction constant, not a finding).

## Swing pivots

A swing high is a bar whose high stands above its neighbours for N bars either side (fractal definition). ⚠ Tie handling differs between our two implementations and is load-bearing: `price_structure.detect_swings` uses strict comparison (a plateau yields NO pivot); `price_levels.swing_pivots` calls the FIRST of two equal highs the pivot (`argmax == half_window` plus `>=`). ⚠⚠ **Either way a pivot is only knowable N bars after it happens** — `price_levels.PivotSet` confirms at `index + half_window`, and reading a pivot at its own bar is look-ahead ("today is a swing high" cannot be known today). Any chart narrative that says "price bounced at the swing low" is written with hindsight unless the bounce postdates the pivot's confirmation.

**CONVENTION:** N itself (ours: 5) is a choice. Larger N → fewer, more significant pivots. Higher-highs/higher-lows sequences built from confirmed pivots are the structural definition of trend — more robust than any MA read, and slower.

## Trendlines and channels

**PUBLISHED (as practitioner canon):** Murphy, *Technical Analysis of the Financial Markets* (1999), ch. 4: two points draw a *tentative* trendline, the third touch validates it; the fan principle (successive redrawn lines, third break = reversal). E&M treat trendline breaks as significant with volume.

⚠ Honest status: trendlines resist algorithmic definition — which extremes to connect is discretionary, and two competent readers draw different lines. Lo/Mamaysky/Wang solved this for patterns with kernel smoothing; nobody has a canonical published trendline algorithm. That is why our platform ships **horizontal levels** and not trendlines — two frozen constructions: `price_structure.cluster_levels` (research corpus, `price-structure-v1`) and `price_levels.LevelScan` (S5-S10, `price-levels-v1`) — horizontal clustering has a freezable construction; sloped lines multiply free parameters. Treat trendline reads as narrative aids, levels as the testable structure.

## Gaps taxonomy

**PUBLISHED:** Edwards & Magee ch. 12 (codifying earlier 1930s work — Schabacker precedes them):

| gap | where it appears | classic read |
| --- | --- | --- |
| common | inside congestion | noise; fills routinely |
| breakaway | leaving a base/pattern | begins a move; should NOT fill soon; volume matters |
| runaway ("measuring") | mid-move | continuation; midpoint → project the first leg again |
| exhaustion | end of a move | last gasp; filled quickly, then reversal |

⚠ The taxonomy is diagnostic **after the fact** — breakaway vs exhaustion is decided by what follows (does it fill?), so at the bar itself the classification is a hypothesis. "Gaps always fill" is **CONVENTION** and false as stated — common gaps usually fill, breakaway gaps by definition should not. **MEASURED (ours):** the gap-down fade tested here was dead in the modern era — t 5.73 pooled over 1962–2026 but t 1.39 in 2020+, negative pre-2001 (2026-08-09, #2437) — a lesson in era-splitting, and in what "measured" means: pooled statistics on a non-stationary series flatter dead effects.

## False breaks and stop runs

**CONVENTION (Wyckoff course tradition — no citable original text located):** the **Spring** — price dips below a well-defined support near the end of accumulation and quickly reverses back above; the **Upthrust** mirrors it above resistance. The mechanism is Osler's: stops cluster beyond levels, a probe fills them, and absence of follow-through strands the breakout traders. The read: a failed break is *information about the other side* — a spring back above support after a stop run is a stronger long context than a clean defense, because the supply below has been consumed.

Operational reading (**CONVENTION**): the E&M 3%-close filter and the "wait for the retest" habit both exist to separate break from probe. Every "confirmation" filter trades false positives for lateness; pick one, freeze it, and never narrate it as evidence.

## Measured moves

**PUBLISHED:** E&M's "measuring implications" — e.g. head-and-shoulders target = head-to-neckline distance projected from the break; runaway-gap midpoint projection; flag "measured move" = repeat the pole. **MEASURED (practitioner, on the rule's honesty):** Bulkowski's *Encyclopedia of Chart Patterns* — a practitioner compilation, not peer-reviewed, "perfect trade" accounting, no costs — puts H&S targets hit at ~**51%**. By the friendliest available count, the measuring rule is a coin flip; use targets as *reference geometry* (where the crowd projects to — an attention point), never as an expectancy claim.

## Reading structure — the checklist form

1. Confirmed pivots only (respect the N-bar confirmation lag).
2. Levels above and below current price, each with touch count + last-touch recency; ATR-scaled zones, not lines.
3. Role-reversal candidates: old tops below price (support), old bottoms above (resistance), confluence with MAs ([trend-and-averages](trend-and-averages.md)).
4. Open gaps in range: which taxonomy hypothesis, and what would confirm/kill it.
5. Recent break attempts: clean, confirmed, or sprung? Who is trapped where?

## Cross-links

Hub: [SKILL.md](SKILL.md). Our frozen constructions: `app/services/price_levels.py` and `app/services/price_structure.py` (both by-construction — no published formulation exists for level clustering; the modules say so; do not invent a citation). Concept→implementation map + the break-and-retest primitives: `.claude/skills/data-sources/market-structure.md`. Pattern-testing recipe (algorithmic definition → conditional distribution → random-entry null): `strategy-evidence` §2.12. Wyckoff computability table: `strategy-evidence` §2.12 "The three the operator named".
