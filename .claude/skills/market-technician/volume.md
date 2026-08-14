# market-technician / volume

## When to use

Reading volume against price — confirmation vs contradiction, effort vs result, climax/exhaustion, dry-up, OBV, VWAP and anchored VWAP — or reviewing anything that consumes a volume signal.

Provenance tags per the [hub](SKILL.md): **PUBLISHED** / **MEASURED** / **CONVENTION** / **REFUTED**.

⚠ **Data caveat first:** a large share of our LIVE bars carry NULL volume — measured 2026-08-14: `select count(*), count(*) filter (where volume is null) from price_daily` → 6,766,612 / 1,899,147 (**28.07%** of `price_daily`, the eToro-fed live table; the #2437 S-6 session measured 26.13% on its validated-universe subset). The RESEARCH corpus is different: `research_price_daily` volume was measured present on all 25,818,944 rows (`data-sources/market-structure.md`) — name the table before quoting either figure. Any volume read must state what it does on a NULL bar — in `price_levels.py`, a wholly-absent volume array falls back to an unweighted cluster mean, while a per-bar NaN contributes zero through `nansum`/`nancumsum`; a volume-confirmation gate must refuse, not pass.

## Effort vs result — the organizing idea

**PUBLISHED (as modern codification):** Wyckoff's "law of effort versus result" — volume is effort, price movement is result; convergence confirms the move, divergence warns of a change. The three-laws codification is the Pruden/Bogomazov school's statement of the 1930s Wyckoff course; no verbatim 1931 text was located, so cite it as the Wyckoff *tradition*, not a page.

The quantity underneath is real and computable: high volume + small |return| (absorption) maps to **low Amihud illiquidity**; low volume + big move to high Amihud — and Amihud (2002) *is* published, priced, and measured on our corpus. `strategy-evidence` §2.11–2.12 carries the derivation of this inversion. Test the observable, not the story about "composite operators".

Practical reads (all **CONVENTION** as specific rules, coherent under effort-vs-result):

- Advance on rising volume, pullback on falling volume → participation confirms the trend.
- New price high on clearly lower volume than the prior high → effort divergence; a warning, not a signal.
- Huge volume, narrow range, no progress → absorption: someone is on the other side of the crowd. Direction resolves on the NEXT bars, not this one.

## Confirmation multipliers — the folklore, labelled

- **PUBLISHED (the only book-sourced threshold):** William O'Neil, *How to Make Money in Stocks* — breakout volume "at least 40% to 50% above normal" (part of the S — Supply and Demand — criterion in CANSLIM), i.e. ~1.4–1.5×.
- **PUBLISHED (qualitative only):** Edwards & Magee require "a conspicuous burst of activity" on a valid breakout — their only *number* is a **3% price** penetration, not a volume multiple.
- **CONVENTION:** the popular 1.2× / 1.5× / 2× 20-bar-average cuts have no published source; retail convention clusters at 1.5–2×. Our S-6 froze **1.2×** and recorded it as convention at the time — correctly tagged, and note it sits *below* even the folklore cluster; if a future version moves it, O'Neil's 1.4–1.5× is the only citable anchor.
- ⚠ A "confirmed" breakout is still a breakout — [price-structure](price-structure.md) covers false breaks; volume shifts the odds, it does not close the false-break case.

## Climax, exhaustion, dry-up

- **PUBLISHED (Wyckoff tradition):** the Selling Climax — "widening spread and selling pressure… climaxes and heavy or panicky selling by the public is being absorbed by larger professional interests at or near a bottom"; extreme volume, close well off the low. Mirror: Buying Climax. A climax is identified by what follows (the Automatic Rally / Secondary Test), not on the bar itself.
- **PUBLISHED (O'Neil):** volume dry-up on pullbacks and in the late stage of a base is constructive — "when a stock pulls back in price, you want to see volume dry up, indicating no significant selling pressure" — then expansion on the breakout. Practitioner rule, IBD-published.
- **MEASURED (adjacent academic):** Gervais, Kaniel & Mingelgrin, "The High-Volume Return Premium", *JF* 56(3) (2001) — unusually high-volume days are followed by appreciation over the next month (low volume by the reverse); follow-up work (Kaniel, Li & Starks) reports the effect internationally. Note the direction: a volume *shock* is bullish on average in their samples — which cuts against reading every volume spike as distribution.
- **PUBLISHED (theory):** Blume, Easley & O'Hara, "Market Statistics and Technical Analysis: The Role of Volume", *JF* 49 (1994) — volume carries information about signal *precision* that price alone cannot reveal; conditioning on both is rational. This legitimizes volume-reading as a class; it validates no specific rule. Karpoff's survey (*JFQA* 1987) adds the stylized facts: volume correlates with |return|, and in equities with the signed return (heavier on up-moves).

## OBV

**PUBLISHED:** Joseph Granville, *New Key to Stock Market Profits* (1963). Up close → add the bar's volume to the running total; down close → subtract; unchanged → nothing. The claim is that OBV divergence leads price ("volume precedes price").

**MEASURED (literature, thin):** the one located academic test (Tsang & Chong, *Economics Bulletin* 2009) finds OBV-based rules beat buy-and-hold in Greater China samples but "in general cannot beat the buy-and-hold strategy in the US and European markets". Lower-tier venue, mixed geography — treat OBV as a divergence lens, not an evidence-backed signal.

## VWAP — what it actually is

**PUBLISHED:** VWAP = Σ(price × volume) / Σ(volume) over **a session**. Its origin is not chartist at all: it is the institutional **execution benchmark** — Berkowitz, Logue & Noser, "The Total Cost of Transactions on the NYSE", *JF* 43 (1988) formalized measuring execution cost against the day's volume-weighted price (practitioner use from ~1984, Abel Noser). **CONVENTION** (mechanism rationale, untested here): desks benchmarked against VWAP must trade near it, and that flow is the usual explanation for the line's intraday gravity.

Consequences for reading:

- VWAP is a **session-anchored intraday** quantity. It resets at the open. "VWAP on a daily chart" has no canonical published definition — vendor "rolling VWAP" over N daily bars is a derived convention, not the benchmark quantity. Do not cite daily-chart VWAP as if it were the institutional line.
- Intraday uses (**CONVENTION**, coherent with the mechanism): above VWAP = buyers in control of the session; institutional buyers working orders tend to defend it; reversion to VWAP is the natural magnet in balanced sessions.
- ⚠ Our `price_intraday` is a build gap, not a data gap (`etoro-api` skill: `get_intraday_candles` exists, 1000 bars/request). Until intraday bars are stored, no *session* VWAP is computable here — flag any spec that pretends otherwise.

**Anchored VWAP** — **PUBLISHED (practitioner):** Brian Shannon, *Maximum Trading Gains with Anchored VWAP* (2023; a CMT Association presentation of the same material exists). Same formula, anchor chosen at an event — earnings, IPO, a swing extreme — answering "what is the average holder since X in at?". Above the anchor-VWAP, the average post-event buyer is in profit; below, trapped. A positioning lens, not a signal; the anchor choice is discretionary and two readers with different anchors get different lines. We DO ship this on daily bars: `price_structure.anchored_vwap` (HLC3 typical price — a stated daily-bar convention, not a standard; and `usable_from_index` guards the swing-anchor look-ahead — an anchor chosen after seeing the outcome is the trap, per `data-sources/market-structure.md`).

## Reading volume character — the checklist form

1. NULL check: does this instrument's series carry volume at all? (26.13% of bars do not.)
2. Trend legs vs corrective legs: which side carries the volume?
3. At the decision point: expansion (participation), contraction (dry-up), or extreme (possible climax — wait for the response bars)?
4. Divergence: is the latest push to a new extreme made on less effort than the prior push?
5. If intraday context exists (it does not yet, here): position vs session VWAP / relevant anchored VWAP.

## Cross-links

Hub: [SKILL.md](SKILL.md). Amihud/effort-result inversion + Wyckoff Nine Tests computability: `.claude/skills/quant/strategy-evidence.md` §2.11–2.12. Concept→implementation map (incl. the anchored-VWAP anchor trap, and "no volume-flow indicators shipped" status): `.claude/skills/data-sources/market-structure.md`. Level strength uses volume share: `app/services/price_levels.py`. Intraday capability facts: `.claude/skills/data-sources/etoro-api.md`.
