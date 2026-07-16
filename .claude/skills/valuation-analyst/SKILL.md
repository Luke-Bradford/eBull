---
name: valuation-analyst
description: eBull valuation ranges + entry bands — the thesis-produced bear/base/bull per-share targets and buy zone on the `theses` table, the fundamentals `instrument_valuation` VIEW, and how both feed the scoring value family + portfolio entry logic.
---

# valuation-analyst

## When to use

Any change to how bear/base/bull per-share targets or the buy zone are
produced, stored, read, or scored. Concretely: the valuation fields on the
`theses` table (`app/services/thesis.py`), the scoring value family
(`app/services/scoring.py::_value_score`), the fundamentals-derived
`instrument_valuation` VIEW (`sql/201`, `sql/080`, `sql/032`, `sql/024`),
the portfolio entry-price logic (`app/services/portfolio.py::_target_entry`,
`_evaluate_add`), take-profit (`app/services/entry_timing.py::_compute_take_profit`),
or the `/theses` / `/instruments` valuation fields (`app/api/theses.py`).

There is **no standalone valuation service** — these ranges are a *thesis
surface* consumed downstream. Owning skills: `thesis-writer` (produces),
`ranking-engine` (scores), `portfolio-manager` (acts). Note:
`app/services/valuation.py` is unrelated — it is portfolio AUM
(`compute_portfolio_valuation`), NOT bear/base/bull.

Also distinct: **`fair_value_band`** (#2009, `app/services/fair_value_band.py`)
is a *deterministic comps evidence band* (bear/base/bull per-share from P/E,
P/S, P/B + EV/EBITDA percentiles, **fvb_v5** #2043) that feeds the thesis writer
as passive evidence + a **base-to-base** divergence audit — NOT the thesis
bear/base/bull. Synthesis rules: peer + own percentiles, blend base,
**outer-envelope wings clamped by a fixed per-leg cap** (`_R_UP`/`_R_DN`, v2
#2022), own history uses **interior quantiles p25/p75** (not p20/p80 — a
near-max of ~6 quarters is unreproducible). **EV/EBITDA is peer-only** (no
own-history EBITDA exists; snapshot lacks it) with strict D&A + cash-present +
debt/interest coherence gates and a fail-closed leg drop when the net-debt
back-out turns any converted value ≤ 0 — the cap is that leg's sole wing bound.
**Winsorization is refuted** as a width fix (can't move a genuine peer quartile;
question the model, not the case); the root width cause was cohort
comparability — fixed by the **fvb_v4 companion-variable peer screen** (#2032):
ps/ev_ebitda cohorts matched on net margin + revenue-growth-YoY, pb on ROE,
frozen absolute width tiers, width-major walk, unscreened fallback +
`cohort_screened:false` + quality knock; **pe is EXCLUDED** (wrong companion —
canonical is forward EARNINGS growth, no source; revenue-YoY full-pop refuted
as churn-only) and a **margin-only degrade stage is refuted** (acceptance-gate
variant: ~40-45% median leg-p50 churn, no consistent tail win). The residual
DCTH-class cross-leg base disagreement was fixed by the **fvb_v5
earnings-representativeness gate** (#2043): the pe leg is non-contributing when
the last-4 FY **net-income** history says earnings are non-representative
(G1 `median_low(FY NI)≤0` never-profitable / G2 depressed `<med_low/3` / G3
spiked `>3×med_high` + margin conjunction), Damodaran ch.22/ch.10/ch.35
source-ruled. Judged on NI, NOT margin (revenue under-captured for
banks/REITs) and NOT EPS (XBRL FY EPS not split-adjusted). Fail-open <3 FYs.
Cross-leg-distance arbitration is REFUTED (no canon rule; the >2x mass is
healthy dispersion — ADBE/AMGN-class); disagreement only flags:
`basis_json.cross_leg_base_ratio` + quality knock at >3x (the `(max-min)/base`
spread misses the canonical case — DCTH 36.8x ratio = 1.89 spread). New absent
reason `earnings_nonrepresentative` (sql/229) when the gate removes the only
synthesizable leg. Specs:
`docs/proposals/valuation/2026-07-13-fair-value-band-v2-robustness.md`,
`docs/proposals/valuation/2026-07-15-fair-value-band-ev-ebitda.md`,
`docs/proposals/valuation/2026-07-15-fvb-v4-companion-screen.md`,
`docs/proposals/valuation/2026-07-15-fvb-v5-earnings-representativeness-gate.md`.

## What it is

**Produced by the thesis writer.** The Claude thesis prompt
(`app/services/thesis.py`, prompt spec ~L570-584) emits five per-share
figures, stored on the `theses` table (`sql/001_init.sql:95-99`, all
`NUMERIC(18,6)`): `buy_zone_low`, `buy_zone_high`, `base_value`,
`bull_value`, `bear_value`. Prompt rules: base/bull/bear are per-share
price targets in the instrument currency, null if insufficient data;
`buy_zone_low/high` are populated only when `stance == "buy"`, null
otherwise. Rows are append-only (`UNIQUE(instrument_id, thesis_version)`).

**Fundamentals fallback surface.** `instrument_valuation` is a VIEW
(`CREATE VIEW`, `sql/201_instrument_valuation_dual_class_suppress.sql:45`,
latest revision) joining latest quote + TTM fundamentals. It NULLs
shares-distorted columns for curated dual-class instruments (#1664 /
#1623). Scoring reads it (`scoring.py:1285`) only when no thesis
`base_value` exists.

**Consumed by the scoring value family** — weight `0.25`
(`scoring.py:70`, held constant across all model versions).
`_value_score` (`scoring.py:526`):
- Primary path (thesis `base_value` present):
  `upside_to_base = (base_value − price)/price`, clipped so 50% upside ⇒ 1.0;
  `downside_to_bear = (price − bear_value)/price`;
  `score = 0.75·upside + 0.25·(1 − downside_penalty)` (`scoring.py:563-573`).
- Fallback path (no `base_value`): blends P/E (35%), FCF yield (35%),
  price-target upside (30%) from `instrument_valuation`, renormalised over
  available components (`scoring.py:588-599`).

**Consumed by the portfolio manager + entry timing.**
- `_target_entry` (`portfolio.py:836`) = buy-zone midpoint when
  `buy_zone_low/high` both present and `high > low`, else current price.
- `_evaluate_exit` treats `current_price >= base_value` as
  "valuation target reached" — an EXIT signal (`portfolio.py:627-631`;
  documented at :609). `_evaluate_add` has no `current_price` input.
- `_compute_take_profit` (`entry_timing.py:239`) sets TP = `base_value`,
  guarded to null when `base_value` is missing or at/below entry.

**Exposed** via `app/api/theses.py` (`ThesisDetail`, L72-76) on routes
`GET /theses/{instrument_id}` and the `/instruments` thesis router (L32-37).

## Invariants

- **`docs/settled-decisions.md` → "Thesis semantics".** Each generation
  inserts a new `theses` row; never overwrite. Stance ∈ {buy, hold, watch,
  avoid}; buy zone is populated only on a `buy` stance. Freshness derives
  from `coverage.review_frequency`, not `last_reviewed_at`.
- **`docs/settled-decisions.md` → "Scoring and ranking".** The value family
  is heuristic, explicit, auditable — no ML, no cohort-relative
  normalization, penalties additive only. The bear/base bands drive the
  score deterministically; the same input always yields the same value
  score (auditable trade path).
- **Long only v1, no leverage.** These ranges drive BUY/ADD/HOLD/trim only;
  a `bear_value` breach is downside *context*, never a short signal.
- **Protective exits never gated.** `entry_timing` may defer entries on TA
  but must never let a valuation target block a protective EXIT (settled
  decision, `entry_timing.py` module header).
- Family weight `0.25` is fixed across model versions; changing the value
  *computation* bumps `model_version` (append-only score history).

## Failure conditions

Missing critical source data, stale timestamps, or contradictory evidence
must surface as explicit signals — never be papered over with a neutral
default:

- **Missing `bear_value` (base present):** `_value_score` applies an
  explicit 0.5 downside penalty AND records `"bear_value missing"` in the
  returned notes (`scoring.py:557,570-571`) — the gap is logged, not hidden.
- **Missing `current_price`:** returns 0.5 *with* a `"current_price missing
  or zero"` note (`scoring.py:550-551,561,584`); the absence is stamped on
  the row, not silently absorbed.
- **⚠ #1857 (OPEN, operator-gated):** the fundamentals fallback returns a
  bare `0.5` when no P/E, FCF, or price-target component is available
  (`scoring.py:601-603`). Because `instrument_valuation` gates on `quotes`
  (not `price_daily`), this fires for ~97% of the universe — a neutral
  default masking absent data, exactly the anti-pattern above. Fix =
  `COALESCE` quotes → `price_daily`; it bumps `model_version`, so it is
  operator-gated, not a silent patch.
- **Stale thesis:** additive `_PENALTY_STALE_THESIS = 0.15`
  (`scoring.py:175`); staleness is measured against `coverage.review_frequency`.
- **Contradictory evidence:** the `thesis-critic` attacks every memo
  (`critic_json`) and `break_conditions_json` records what falsifies the
  thesis — reconcile against those, do not average the conflict away.
