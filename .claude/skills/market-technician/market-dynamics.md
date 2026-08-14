# market-technician / market-dynamics

## When to use

Reasoning about how price actually moves — regimes, impulse/correction rhythm, breakout→retest, reversion after extension, relative strength, gap behaviour, short-side mechanics, and the standard ways reads fail.

Provenance tags per the [hub](SKILL.md): **PUBLISHED** / **MEASURED** / **CONVENTION** / **REFUTED**.

## Regimes — trend, range, transition

**CONVENTION** (the trend/range/transition framing is practitioner canon, not a measured taxonomy): markets are read as alternating between trending and ranging states, and the tools in this family are regime-conditional — Bollinger's own rules read band tags opposite ways by regime ([volatility-and-bands](volatility-and-bands.md)), S-6 excludes breakouts from the volatile regime by design, and S-8 only fires where ADX says no trend exists. So regime is the FIRST read, not a footnote — and in our platform it is an **input**: `app/services/market_regime.py` classifies every bar as `bull/bear × quiet/volatile` from two frozen legs with stated provenance (SPY vs 200-SMA — the conventional long-horizon reference with a published test trail; Bollinger's published Bulge). ⚠ Note the encoded classifier has NO "range" state — range-reading goes through ADX (S-8's `ADX(14) < 20`, a frozen convention) or structure, not the regime enum. Strategies declare permitted regimes, and firing outside the declared domain is the defect (S5-S10 §0).

**MEASURED (why this is structural here):** S-1…S-4 were each judged on one pooled statistic over the whole span — the wrong instrument for a non-stationary series. Concrete instance: the gap fade was t 5.73 pooled 1962–2026 and t 1.39 in 2020+ (negative pre-2001). Pooled numbers flatter dead effects; report per-regime and per-era, and treat the recent window as the deciding one.

⚠ An unclassifiable bar (warm-up, or a benchmark hole) is `None`/`not_evaluable`, never "neutral" — a strategy gated on regime refuses to fire on it. Fail-closed applies to reads too: "regime unknown" is a legitimate line in a chart read.

## Impulse and correction

**CONVENTION** (Dow-descended structure, codified by Wyckoff/Elliott traditions in incompatible ways — the common testable core is below):

- Trends move in alternating impulse legs (directional, expanding range/volume) and corrective legs (contra-directional, contracting range/volume). The volume asymmetry is the effort-vs-result read ([volume.md](volume.md)).
- A correction that keeps making progress on rising volume is not a correction — relabel it before it relabels you.
- Structure metric: confirmed higher-highs/higher-lows ([price-structure](price-structure.md)) — the slowest and most robust trend definition; MAs proxy it faster and dirtier.

## Breakout → retest

**CONVENTION** (mechanism-coherent: Osler's stop clustering beyond levels, plus trapped-inventory role reversal): a valid break converts the level; the pullback to it from the far side ("retest") is where the role reversal is confirmed or refuted. Waiting for the retest trades missed fast moves for cheaper invalidation — a stop just back inside the level is small, and S-6 encodes exactly that (`stop = level − 1 × ATR`, "back inside the range = failed break"). ⚠ Not every break retests; there is no published retest frequency worth quoting, and any number you see for it is folklore.

## Mean reversion after extension

- **PUBLISHED (where reversion is real):** short-horizon reversal is a documented effect family — and the most cost-constrained one (Frazzini/Israel/Moskowitz line, via `strategy-evidence` §2.5: the family's paper profits die to real trading costs fastest). De Bondt/Thaler long-horizon reversal lives in cheap/small names (§2.8a) — where we cannot trade.
- **CONVENTION (the chart version):** "price stretched N× ATR from its 20-MA snaps back" — no canonical published formulation; distance-from-mean as bad entry geometry is the defensible read (chasing an extended move buys someone else's exit), an expectancy claim is not.
- **MEASURED (ours, long side):** unconditional 10-day drift after big drops was ~44 bps against a ~50 bps round trip — the long-side short-horizon game is played inside the spread (2026-08-09). RSI(2)-style reversion setups pass through `strategy-evidence`'s cost bar before anything else.

## Relative strength vs the index

**PUBLISHED (family):** cross-sectional momentum — ranking by trailing return — is the best-documented anomaly family and the one the cost literature calls most scalable in liquid names (`strategy-evidence` §§2.1–2.2, and our own measurement found it strongest in expensive names). The chart-read version: compare the instrument's 63-bar return to the benchmark's; an instrument making relative highs while the index chops is under accumulation in the only sense that is measurable. Wyckoff's ninth buying test ("stock stronger than the market") is this quantity. ⚠ On our data, total-return-relative claims need the comparator rules in `strategy-evidence` §"Recent-regime and storage contract"; price-only relative strength is computable everywhere.

## Gap dynamics

Gap taxonomy and its after-the-fact nature: [price-structure](price-structure.md). Dynamics worth carrying here:

- A gap is the visible print of overnight order imbalance — the auction reopening away from equilibrium. Gaps through levels void the level's geometry (the stop cluster beyond it fills at the open, not at the level).
- **MEASURED (ours):** stops do not bound gap risk — 141 of 1,402 short-side stops filled at an open beyond the stop level; worst single trade −87% *with* a 20% stop (2026-08-09). Position size, not stop placement, is the only control that survives a gap.
- **REFUTED (ours):** the gap-down fade as a strategy — dead since 2020 on our own era-split. Closed on #2437; do not resurrect.

## Short-side mechanics — the asymmetries

The short side is not the long side mirrored; four asymmetries, all load-bearing:

1. **Unbounded loss.** A long risks its stake; a short's loss has no ceiling. A mean return is not a sufficient basis for a short strategy — the tail and the delisting attribution decide (risk posture, `.claude/CLAUDE.md`).
2. **Carry.** On eToro a short is a **CFD** — a contract with the broker, no ownership. Easy-to-borrow costs spread only; **hard-to-borrow (>10%/yr) accrues a daily fee at 21:00 GMT (22:00 during daylight saving, per eToro's fee page), tripled at weekends** — and a name that just fell hard is the archetypal hard-to-borrow candidate, so a drop-triggered short cannot reuse the long cost model.
3. **Availability.** Shorting is gated by share availability, volatility halts, and region — the signal firing does not mean the trade exists.
4. **Crowd geometry inverts.** Short squeezes are the stop-run mechanism with the signs flipped and leverage of forced buying behind it; support/resistance reads carry over, but the violent tail is on the adverse side.

**MEASURED (ours, the arc in one line):** the ≥12% one-day-drop continuation short survived its per-trade checks (t 4.83, net +49 bps at a 30%/yr borrow) and then **lost 13.76–47.72% on all 8 sizing arms once capital was finite** (#2481, 2026-08-12) — per-trade expectancy and an equity curve are different objects, and ~6 concurrent firings/day with −87% tails is how a positive mean becomes a negative account. ⛔ Closed; do not resurrect. (Full record: `docs/proposals/ta/2026-08-09-plan-of-attack.md` + #2481.)

## Common failure modes — name them to catch them

| failure | what it looks like | the counter |
| --- | --- | --- |
| chasing | entering N ATRs from the mean because the move "is confirmed" | extension check (protocol step 2/4); the confirmation you waited for is the entry geometry you gave up |
| fighting regime | fading a band walk; buying breakouts in a Bulge | regime first; S-6 excludes `bull_volatile` for exactly this |
| averaging into an invalidated setup | the invalidation level broke, so the read is dead — adding is a new (worse) thesis wearing the old one's name | the read dies AT its invalidation; re-underwrite from scratch or walk |
| narrative persistence | keeping the bias and moving the invalidation | invalidation is written down at read time (protocol step 7) precisely so it cannot move |
| pooled-statistic comfort | "it worked over 20 years" | era-split + regime-split before excitement (S-1…S-4 lesson) |

## Cross-links

Hub: [SKILL.md](SKILL.md). Regime encoding: `app/services/market_regime.py`. Cost/turnover bars and family evidence: `.claude/skills/quant/strategy-evidence.md`. Short-side closed results: `docs/proposals/ta/2026-08-09-plan-of-attack.md`, #2481.
