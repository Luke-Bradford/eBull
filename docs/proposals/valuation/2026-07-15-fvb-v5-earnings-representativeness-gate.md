# fvb_v5 — earnings-representativeness gate on the P/E leg (#2043)

Cross-leg base-coherence guard for the DCTH class. `METHOD_VERSION` →
`fvb_v5`. NOT a scoring model_version.

## 1. Problem (re-censused on LIVE fvb_v4, dev 2026-07-15)

346 multi-leg ok bands; cross-leg base max/min ratio p50 1.41, p95 4.55;
**77 >2x, 15 >5x, 10 >10x** (v3 was 93/21/11 — the #2032 screen compressed
but the class persists). Structural facts the v3 census did not surface:

- The **pe leg is the extreme leg in 67/77** of the >2x cases, in BOTH
  directions: near-zero EPS collapses the leg (DCTH pe base $0.31 vs ps
  $11.27 = 36.8x; MRAM 12.5x; INTT 10.4x), transient-gain EPS explodes it
  (TRS pe $696 vs ps $21.5 = 32.3x; WDC 4.0x).
- With exactly **2 contributing legs, median-of-bases = midpoint** — the
  deviant leg contaminates `base_value` directly (DCTH stored base 5.79 =
  midpoint of 0.31 and 11.27), not just the envelope. 29/77 >2x bands are
  2-leg.
- The uncovered mass (2–5x, 62 bands) is dominated by healthy-margin names
  (ADBE 2.6x, AMGN 2.3x, MU 3.6x, PTC 2.9x): ordinary inter-multiple
  dispersion — a firm can be cheap on sales and expensive on earnings at
  once. That is information, not defect; it stays (flag path, §6).

## 2. Source rule

- **Damodaran, Investment Valuation 2ed, ch. 35 "Choosing the Right
  Relative Valuation Model"** (pages.stern.nyu.edu/~adamodar/pdfiles/valn2ed/ch35d.pdf):
  combining disagreeing multiples by simple average "gives equal weight to
  the values from each multiple, even though some multiples may yield more
  precise answers than others"; the prescription is precision weighting or
  the *best multiple*, chosen by firm characteristics — "the level of its
  earnings". Table 35.1: High Growth / Negative Earnings → **PS** ("assume
  future margins will be positive"); PE is for firms whose current earnings
  correlate with value, cyclicals "often with normalized earnings".
- **Damodaran ch. 10 "Revenue Multiples"**
  (…/papers/revmult.pdf): earnings ratios "can become negative … and not
  meaningful" at the earnings extremes; revenue is less volatile and harder
  to distort; PS is an increasing function of net margin.
- **Damodaran ch. 22 "Valuing Firms with Negative Earnings"**
  (…/valn2ed/ch22.pdf): abnormal earnings are defined **relative to the
  firm's own history** — "the current earnings of the firm are much lower
  than what the firm has earned historically … they are likely to be
  meaningless because current earnings are depressed"; prescription is to
  normalize (replace current earnings with a history-representative level)
  or switch measure. One-time gains are the mirror case (back out
  temporary items).

Canon locates the invalidity in the **denominator** (earnings not
representative of earning power), never in cross-leg distance. No
documented rule arbitrates "drop the leg farthest from the median" — and
the full-population census independently kills distance arbitration (§3:
the >2x mass is healthy dispersion). So: **gate the pe leg on
earnings-representativeness; disagreement itself only flags.**

## 3. Full-population verification (dev, 569 fvb_v4 ok bands, 505 pe legs)

Candidate predicates tested full-pop before speccing; falsified in order:

1. **Absolute thin-margin floor** (issue's known partial candidate):
   margin<2% covers 7/77 >2x; margin<5% covers 13/77 while flagging 49/505
   pe legs. False positives are structural-thin intermediaries with fully
   representative EPS: USFD (distributor, 1.7%, band ratio 1.19), SNEX
   (broker, 0.35%). Thin margin ≠ meaningless EPS. REJECTED.
2. **margin > 100% (NI>revenue = non-operating)**: 17 flagged legs are
   dominated by banks/REITs whose `financial_periods.revenue` is
   under-captured in our data (UDR stored quarterly revenue $2.5M vs real
   ~$420M; CBL $4.3M; TRS has a −$140M revenue quarter). A method predicate
   must not key on a known-broken column. REJECTED (data-quality signal
   only).
3. **Margin-vs-own-history tests**: right shape (ch. 22 verbatim) but
   inherit the same revenue weakness (REITs VNO/IVT/AHR/UDR false-fire
   spiked). REJECTED in favour of the same test on the **NI level** — the
   actual earnings denominator, no revenue involved.

**Adopted predicate** (pure, per target, evaluated only when a pe leg is
selected): over the last **4 fiscal years** of `FY` net income (deduped
latest-filed per period_end; **≥3 required**, else no gate — fail-open,
young listings like LOAR/STRW/ONC stay ungated):

- **G1 never-sustainedly-profitable**: `median_low(FY NI) ≤ 0` (≥ half the
  window unprofitable — the ch. 10 / Table 35.1 life-cycle class).
- **G2 depressed**: `NI_ttm < median_low(FY NI) / 3` (ch. 22 "much lower
  than … historically"; `median_low` reference so one-time-gain years in
  history cannot fake depression — VVV divestiture-gain case).
- **G3 spiked**: `NI_ttm > 3 × median_high(FY NI)` **AND** margin
  conjunction `margin_ttm > 3 × median(FY margins)` (≥3 FY margins,
  median > 0). The margin conjunction protects compounding growers (PTC
  2.5x, APP 2.0x, CRDO 3.0x NI growth all kept — margin stable while NI
  grows) and the NI leg protects against the REIT revenue artifact (NI
  honest while margin fake-spikes).

**NI, not EPS, deliberately** (Codex ckpt-1 HIGH triaged): ch. 22's
abnormality test is stated on firm earnings, and (a) XBRL FY EPS is
as-reported, NOT retroactively split-adjusted — a split inside the window
false-fires any EPS-history test; (b) dilution-depressed EPS is *genuine*
current per-share earning power, not non-representativeness — the gate
must stay quiet there, and NI-based does; (c) the acquisition scale-up
case (NI up 3x via issued shares) is suppressed by the G3 margin
conjunction (acquired revenue arrives with the NI, margin stays flat).
Buyback drift is ≤ ~1.5x over a 4-FY window, well under K = 3.

**G3's margin conjunction is structurally artifact-safe** despite §3.2:
it can only SUPPRESS a drop, never cause one. The known artifact direction
(under-captured revenue → overstated margin) can at worst confirm a case
the NI leg has already flagged as ≥3x spiked — and a ≥3x NI spike is
itself the ch. 22 abnormality. All 11 G3-gated legs are enumerated in the
acceptance gate (§6) for eyeball confirmation.

K = 3 and window = 4 are frozen calibrated constants (same discipline as
`_R_UP`/`_R_DN`). Full-pop results at these values:

- **42/505 pe legs gated** (G1 27, G2 4, G3 11): >5x coverage 7/15
  including all four worst (DCTH 36.8 G1, TRS 32.3 G3, LQDA 25.1 G1, EIG
  21.4 G2; + MRAM 12.5, INTT 10.4, AMSC 9.3). >2x coverage 14/77 — the
  remaining 63 are the healthy-dispersion mass (by design, flagged not
  dropped).
- **26 multi-leg base shifts** (DCTH 5.79→11.27, TRS 358.95→21.55, EIG
  36.68→70.09, GME 28.40→15.27, MU 2367.59→1534.81; 17 shifts >25%).
  Directions both ways: dropping a collapsed leg RAISES base (that low
  base was false conservatism from meaningless data), dropping a spiked
  leg lowers it.
- **16 pe-only bands → statused absent** (ILMN, TEVA, TWLO, MRVL, CARG,
  AMRX, PBI, TDC, RAMP, RSI, HL, GFF, ANIP, ATRO, TBRG, MPAA — GAAP
  impairment/cliff cases). Honest absence > band anchored solely to a
  non-representative denominator. ok count 569 → 553.
- Residual >5x after gate: PLTR 17.3 / COLL 12.8 (**ev-extreme**, healthy
  EBITDA denominators — different mechanism, out of scope, revisit
  trigger §8), STRW 17.0 / ONC 16.1 / LOAR 6.0 (<3 FY history, fail-open),
  POWL 8.8 / PDFS 7.7 / XTNT 6.3 (genuine dispersion / knife-edge).

## 4. Mechanics

All in `app/services/fair_value_band.py` + one migration.

1. **Pure predicate** `earnings_nonrepresentative(fy_net_income,
   fy_margins, ni_ttm, revenue_ttm) -> str | None` returning
   `'G1_never_profitable' | 'G2_depressed' | 'G3_spiked' | None`.
   Constants `_EARNINGS_HIST_MIN_FY = 3`, `_EARNINGS_REP_K = 3.0`
   (window 4 applied in SQL).
2. **IO**: new `_FY_HISTORY_SQL` in `compute_band_for_instrument` — last 4
   `FY` rows with `net_income NOT NULL`, `superseded_at IS NULL`,
   `normalization_status='normalized'`; also selects `revenue` (G3 margin
   series: margin computed only where `revenue > 0`; <3 usable margins →
   G3 cannot fire, fail-open). Dedup/tie-break = the settled repo shape,
   verbatim from `_TARGET_SQL`'s prior-TTM CTE and sql/220
   `financial_periods_ttm`: `DISTINCT ON (period_end_date) … ORDER BY
   period_end_date, filed_date DESC NULLS LAST` (latest-filed row wins per
   period_end — the restatement rule; period_end ordering per #1823), then
   latest 4 by `period_end_date DESC`. NI_ttm / revenue_ttm already on
   `TargetInputs` (`net_income_ttm` via `_TARGET_SQL`).
3. **compute_band** gains `pe_earnings_gate: str | None = None` (defaulted
   — pre-v5 constructors untouched). When set and `m == "pe"`: basis entry
   keeps peer/own stats + provenance (auditable, the `dropped_nonpositive`
   precedent), gains `"earnings_nonrep": "<G*>"`, and the leg is skipped
   before `synth_multiple` — it never contributes a triple, never sets
   `screen_fallback`/sides/own_points. `n_selected` unchanged (shipped
   precedent for non-contributing legs).
4. **Absent reason**: if no triples remain AND the gated pe leg would have
   synthesized — tested as `synth_multiple(peer, own) is not None` at gate
   time, NOT a p50-presence proxy (synth requires a full p25/p50/p75
   side) — reason = `'earnings_nonrepresentative'`; else `'thin_cohort'`
   as today.
5. **Quality (flag half)**: new `QualityInputs.cross_leg_base_ratio`
   (defaulted so pre-v5 constructors are untouched): `score -= 1 if
   cross_leg_base_ratio > 3.0`. This is the **max/min ratio of
   contributing bases** — deliberately NOT the existing
   `cross_multiple_spread = (max-min)/base`, whose scale is damped by the
   median (DCTH at 36.8x ratio has spread only 1.89; a spread-keyed knock
   misses the canonical case — Codex ckpt-1 HIGH). Existing >0.5 spread
   knock unchanged. Base-neutral.
6. **Provenance**: basis_json top-level `"cross_leg_base_ratio"` (same
   max/min metric as §4.5, ≥2 contributing legs; absent otherwise) —
   future censuses read stored rows, no recompute.
7. **sql/229**: widen `fvb_obs_reason_chk` + `fvb_cur_reason_chk` to admit
   `'earnings_nonrepresentative'` (DROP+ADD, new-value-only widening,
   sql/226 pattern; applied migrations immutable).
8. `METHOD_VERSION = "fvb_v5"`.

Not touched: cohort materialization, screen walk, ev/ps/pb legs, thesis
consumer (shape-gated on bear/base/bull), scoring, FE (reason strings are
pass-through; no FE literals — grepped).

## 5. What a fix is NOT (falsified alternatives)

- **Cross-leg distance arbitration** (issue's original sketch): no canon
  rule exists; census shows the >2x mass is healthy dispersion; with 2
  legs there is no arbiter that doesn't reintroduce price-anchoring
  (#2025 kept separate).
- **Deterministic normalization** (ch. 22's first prescription —
  `median FY margin × revenue_ttm / shares` as normalized EPS): canonical
  but manufactures earnings for structurally-broken firms (a deterministic
  system cannot separate "temporarily depressed" from "deteriorated");
  dropping is the conservative, fail-closed variant. Documented tradeoff.
- **Flag-only**: leaves DCTH base 5.79 (midpoint with a meaningless leg)
  feeding thesis anchors + #2013 diffs. Quality knock does not fix a
  contaminated base.
- **ev-leg gate**: zero denominator-thin ev cases in the >5x tail;
  no evidence, not invented.

## 6. Acceptance gate (same-day controlled, #2021/#2032 discipline)

Populations named per prevention-log gate-population-match. Fresh v4
worktree run vs v5 run, same day, both full-universe:

1. Bands with **no gated pe leg AND cross_leg_base_ratio ≤ 3.0** (the
   max/min metric, §4.5): byte-identical bear/base/bull/quality.
2. Bands with cross_leg_base_ratio > 3.0 (no gated leg): values
   identical, quality may drop exactly one tier.
3. Gated multi-leg bands: **26 base shifts matching §3 census exactly**
   (DCTH 11.27, TRS 21.55, EIG 70.09, GME 15.27). All 11 G3 drops
   enumerated at the acceptance run and eyeballed against the census
   multi-leg set (TRS, MU, GME, ESTC, IVT, LIVN — one-time gains /
   cyclical peaks; no compounding grower may appear).
4. **16 pe-only absents** with reason `earnings_nonrepresentative`; ok
   569 → 553.
5. Panel: AAPL/MSFT/JPM/HD unchanged; DCTH base 11.27 quality knocked;
   GME base 15.27 (⚠ smoke-panel name, base halves — expected G3 census
   shift).

Expected #2013 re-thesis wave: ~26 base moves (17 >25%) + 16 absents —
anticipated, not a bug.

## 7. Tests

- Pure table-tests: G1/G2/G3/None boundaries incl. median_low knife-edge
  (MRAM shape), gain-inflated-history keep (VVV shape), structural-thin
  keep (SNEX/USFD shape), grower keep (PTC shape), <3 FY fail-open.
- compute_band wiring: gated leg non-contributing + annotated; absent
  reason selection; spread>2 knock; cross_leg_base_ratio.
- One db-tier test: `_FY_HISTORY_SQL` returns latest-filed-per-period_end,
  last-4, NI-non-null.

## 8. Revisit triggers

- ev-extreme residuals (PLTR/COLL): if a future census shows ev-leg
  denominator pathology (not growth pricing), extend the gate with an
  EBITDA-history analogue.
- <3-FY fail-open class (STRW/ONC/LOAR): re-census once those names age
  into 3 FYs.
- If normalized-EPS (ch. 22) is ever wanted, it replaces G2's drop, not
  the predicate.
