# Market-technician-derived candidates — combinations with ex-ante evidence, ordered

Companion to `.claude/skills/market-technician/quant-methods.md` (the equations) and the
S-5…S-10 set. These are **pre-spec candidates**, not strategies: each states its mechanism,
its ex-ante evidence, its first-order disqualifier check, and its falsification plan
BEFORE any backtest — the order the prevention history demands (#2260, fake-fib placebo,
S-1…S-4). Every one goes through `quant/strategy-evidence`'s checklist + the declaration
gate (#2599/#2600) + the trial register before a single scan runs; nothing here lowers a
promotion gate.

⚠ Ordering is by STRENGTH OF EX-ANTE EVIDENCE, strongest first. The bottom entries exist
to be falsified cheaply, and say so.

⚠ **Division of labour with the prereg:** this document fixes each candidate's MECHANISM,
evidence, disqualifier check and falsification SHAPE. Numeric acceptance thresholds,
tie/minimum-history/overlap rules, inference machinery (clustered errors, block bootstrap,
purged walk-forward) and the declared trial count are set in each candidate's
preregistration under the #2599/#2600 gate — they are not implied defaults here, and a
candidate picked up without writing that prereg first has skipped the fence this document
exists behind.

## MT-1 Capped volatility-managed long-only relative strength (testable variant, not established alpha)

- **Identity correction:** `MT-1` is unique to this market-technician queue. The earlier
  `C-1` label collided with the separate portfolio-alpha ledger's `C-1` insider-purchase
  candidate. A trial, declaration, result or UI row must use
  `mt1-capped-volatility-managed-relative-strength`; it must never reuse S-10's identity,
  purpose or evidence. S-10 remains a permanent `harness_validation` control.
- **Mechanism/evidence, with the transfer limit stated:** Moreira & Muir (*JF* 2017)
  scale exposure by `c / σ̂²_{t−1}` (previous month's realised variance).
  Cederburg et al. (*JFE* 2020, 103 strategies) find that direct volatility scaling
  improves all nine momentum portfolios they test, significantly for five. Those tests
  use zero-cost **long-short factor/anomaly portfolios**. S-10 is a **long-only** ranked
  leader strategy, so the papers supply a mechanism and prior, not evidence that this
  implementation is in their surviving set.
- **Form:** two published constructions exist and are NOT interchangeable — Moreira-Muir
  scale by inverse *variance* (`c/σ̂²_{t−1}`, prior month's realized daily variance);
  Harvey et al. (*JPM* 2018) target *vol* (`target_σ/σ̂`, and find it pays only on
  equities/credit). MT-1 may declare only the Moreira-Muir inverse-variance form. Its
  preregistration must use Cederburg et al.'s causal real-time normalisation: a 120-month
  initial training window, expanding thereafter, with `c_t` chosen from completed training
  months so the unscaled reference and raw inverse-variance reference have equal realised
  variance. No full-sample normalisation is permitted. The published construction can
  require substantial leverage: Cederburg et al. report a
  99th-percentile required position above 400% for each factor and as high as 864% for
  momentum. eBull forbids leverage, so MT-1 must cap exposure in `[0, 1]` and leave the
  remainder in zero-return cash. That cap and its fixed normalisation make MT-1 a new
  by-construction variant; they may not be described as the published rule.
- **Clock and causality:** compute the previous complete calendar month's realised daily
  variance of the **unscaled reference portfolio** as `(22/J) × Σ daily_return²`, using
  only returns known before the current month-end decision. Apply one portfolio-level
  multiplier to every selected holding. Do not substitute per-name inverse volatility:
  that changes the cross-sectional signal as well as its aggregate exposure. Fewer than
  120 usable historical month/lagged-variance pairs, a missing session in the complete
  calendar history, or non-positive prior-month variance refuses the decision rather than
  defaulting to 100% exposure. All-cash months remain in the expanding history but cannot
  serve as a positive lagged-variance divisor.
- **Turnover check:** the overlay may change target exposure only on S-10's existing
  month-end decision dates. This creates no extra decision dates, but it can create extra
  traded notional; therefore turnover does **not** pass by construction and must be
  measured before any outcome statistic is opened.
- **Falsification:** run a fresh controlled pair under the new identity: capped-scaled MT-1
  versus an unscaled reference with the exact same signal, universe, fills, exits, costs,
  ambiguity/quarantine arms and month-end portfolio clock. No stored S-10 result is a
  comparator. The negative-control family remains **S-8**, but "the same overlay" means an
  aggregate S-8 reference book whose exposure is updated on the identical month-end clock;
  applying a per-trade or signal-date multiplier is a different experiment and refuses the
  control. The preregistration must freeze the normalisation, minimum prior-month history,
  zero-variance handling, paired inference and difference-in-differences gate before either
  outcome is opened. Both evaluated pairs enter the trial register; an unexecuted design
  does not.
- **Capital boundary:** MT-1 may be declared `capital_candidate` only after its separate
  strategy version, survivorship-free universe, carry/FX stamps, forward-shadow floor and
  complete cost-aware preregistration exist. Until then this entry is a pre-spec and gives
  no manifest, promotion, forecast, allocator or broker authority.

## C-2 Spread-ranked admission gate (universe filter attacking the measured cost failure)

- **Mechanism/evidence:** our long-side results die on cost (44 bps drift vs ~50 bps round
  trip). Spread is estimable from daily bars: Corwin-Schultz (*JF* 2012) and Abdi-Ranaldo
  (*RFS* 2017, the daily-data horse-race winner). **MEASURED 2026-08-15 on our own panel
  (n=1,629 quoted instruments, 60-bar window, vs `quotes.spread_pct`):** both estimators'
  *level* is ~10× the true quoted spread (median ≈1.22% vs 0.126%) — unusable as a cost
  input — but the *ranking* is monotone (top quoted-spread quartile mean 1.79% vs bottom
  ~1.06–1.10%), and **CHL ranks better: Spearman 0.429 vs CS 0.353**, matching the
  published horse race. Formulas + recipe: `quant-methods.md` §"Spread and liquidity".
- **Form:** exclude names in the top decile of trailing-60-bar **Abdi-Ranaldo CHL**
  estimate from strategy universes. ⚠ Causality: a CHL contribution for day t consumes
  day t+1's mid-range — the trailing window at an admission date contains only PAIRS
  fully complete by that date (effectively data to t−1). Decile-vs-other-cut, tie and
  minimum-history rules go in the prereg; the decile is a starting convention, declared
  as such, with its own trial identity.
- **Falsification:** controlled pair on S-7 first (most signals), and the gate is
  UNIVERSE-scoped only if it also survives on a second, different-family strategy —
  one strategy cannot license a general gate. Placebo arm: a random matched-count
  exclusion (same number of names removed, chosen uniformly) — the gate must beat its
  own placebo on net expectancy, not just beat "no gate". Acceptance numbers in the
  prereg.
- ⚠ Where a live quote exists, the REAL spread wins outright; CHL covers the backtest
  corpus, where no quote history exists. The 2026-08-15 calibration is a RANKING check
  against point-in-time quote snapshots (not time-matched effective spreads) — it
  validates ordering, nothing more.

## C-3 Volume-shock drift (published family, monthly horizon)

- **Mechanism/evidence:** Gervais, Kaniel & Mingelgrin (*JF* 2001) — stocks with
  unusually high volume relative to their own recent history appreciate over the
  following month (visibility hypothesis); international extension: Kaniel, Li & Starks.
  ⚠ Their construction is a relative-volume PORTFOLIO-FORMATION design; the per-name
  trigger below is a VARIANT of the idea, not their tested rule — it gets its own trial
  identity and cannot borrow their result as validation, only as mechanism.
- **Form (shape; details in prereg):** volume(t) in the top decile of the name's own
  trailing 50-bar distribution (bar t excluded from its own reference window); hold 20
  bars; fill open(t+1). ⚠ The regime gate is OUR addition, not the paper's — run gated
  AND ungated arms so the gate's contribution is attributable. ⚠ Research corpus only
  (volume complete there; `price_daily` is 28.07% NULL — `volume.md` carries both
  queries); tie/split-adjustment/overlapping-shock rules in the prereg.
- **Turnover check:** ~monthly holding SUGGESTS it sits inside the Novy-Marx/Velikov
  bar, but overlapping shocks can multiply entries — measure turnover FIRST on the
  triggered population, before any outcome is computed.
- **Falsification:** era-split mandatory (the gap-fade lesson: pooled t 5.73 hid a dead
  2020+ effect). Primary placebo = matched RANDOM dates per name (same count, uniform);
  the published natural contrast (their LOW-volume leg should show the opposite sign)
  runs as a second arm. ⚠ A short lag is NOT a valid null — the claimed effect lasts
  ~a month, so a 5-bar-lagged twin sits inside the effect window.

## C-4 ER-gated squeeze expansion (S-9 refinement, controlled)

- **Mechanism/evidence:** S-9 fires on Squeeze + 20-bar breakout. Kaufman's Efficiency
  Ratio `ER = |P_t − P_{t−n}| / Σ|ΔP|` (*Smarter Trading*, 1995) is a bounded [0,1]
  path-efficiency measure. Ex-ante logic: an approach that reached the breakout
  EFFICIENTLY is a different auction than one that got there by chop.
- **Form (shape):** S-9's exact legs + an ER condition computed over bars `t−1−n … t−1` —
  ⚠ the signal bar itself is EXCLUDED, because a breakout bar is a large directional move
  and including it makes the gate mechanically self-confirming. ⚠ Threshold honesty: the
  panel distribution (ER(10) quartiles 0.14/0.30/0.49 across 1,629 instruments,
  2026-08-15, `quant-methods.md`) was measured on LAST-BAR cross-section, which makes any
  cut chosen from it data-informed, not "by construction" — the prereg declares the cut
  ex ante against the PRE-SIGNAL population and registers it as one trial (no threshold
  sweep).
- **Falsification:** the S-4/S-9 template a third time — identical bracket, one added
  condition, attribution clean — PLUS a placebo gate (the same admission rate applied by
  random draw) so "gate helps" is measured against selectivity itself, not against
  nothing. One failed threshold kills THIS rule, not ER as a family; the skill records
  exactly what died.

## C-5 Indicator divergence, by construction (folklore on trial — expected to die)

- **Status:** divergence (price higher-high + oscillator lower-high) is practitioner
  folklore; no published formulation was located (2026-08-15 sweep) — the same provenance
  class as fib levels, which our placebo arm killed. This candidate exists to test the
  folklore properly ONCE and record the verdict either way.
- **Form (shape):** **RSI-14 only** (OBV is not shipped — `market-structure.md` records
  no volume-flow indicators; an OBV variant is a separate candidate gated on building
  one). PRICE pivots via the confirmed-fractal rule (`price_levels.swing_pivots`,
  half-window 5 — note its first-of-equal-highs tie rule differs from
  `price_structure.detect_swings`' no-pivot-on-plateau; declare which); bearish
  divergence = two consecutive confirmed price swing-highs rising while RSI-14's values
  AT those two pivot bars fall (indicator pivots are NOT separately required — the
  comparison is at price-pivot bars, stated to remove the ambiguity). Signal exists only
  at the second pivot's confirmation bar (+5), fill open of the next bar. Max pivot
  separation, minimum deltas, direction/exit/overlap rules: prereg.
- **Falsification:** placebo arm NON-NEGOTIABLE, and the primary placebo is
  RANDOMIZED — fake divergences minted at randomly-chosen confirmed-pivot pairs at the
  same rate (a lagged-indicator twin retains autocorrelation and is kept only as a
  secondary arm). If fake ≥ real — the fib outcome — THIS divergence rule joins the
  refuted list; the verdict does not extend to untested folklore beyond it.

## Not candidates (and why, so they are not re-derived)

- **CS spread as a cost-model input** — falsified on arrival by the level mismatch above.
- **Kelly-fraction sizing from backtest μ, σ** — the estimation-error overbetting problem
  (fractional-Kelly literature) plus our μ estimates carrying declared-trial deflation;
  sizing stays at vol-targeting (C-1) until a strategy has forward paper history.
- **Anything fib/level-proximity-entry shaped** — closed, do-not-reopen
  (`2026-08-09-plan-of-attack.md` §5).
- **Session-VWAP signals** — no stored intraday; build gap (#2477 lineage), not a
  research gap.

Refs #2437.
