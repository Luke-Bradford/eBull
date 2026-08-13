# Realized-risk scoring penalty (scoring model v1.2) — #1633

Status: spec (pre-impl). Follows #591 (risk evidence layer), #1632 (thesis ingests
risk metrics). Part of #585.

## Goal

The scorer (`app/services/scoring.py`, v1.1) is **realized-risk-blind**: its only
volatility signal is a TA-based (Bollinger/ATR) term inside `_momentum_score`. It
never reads realized vol / drawdown / beta from `instrument_risk_metrics` (#591).
Make the engine risk-aware by adding a **realized-risk penalty** that dampens the
score of instruments with persistently high realized volatility or deep drawdowns.

Long-only, no-leverage, long-horizon: between two otherwise-comparable instruments
the engine should prefer the one that has historically been less violent.

## Operator-gated decisions (already signed off 2026-06-18)

- **Penalty, not a 7th family.** Fits the existing additive `_PENALTY_*` block; no
  re-weighting of the six families (which sum to 1.0). Settled-decision compliant
  (penalties additive in v1; no multiplicative penalties).
- **Vendor deferred.** No dividend / total-return vendor reintroduced (#1635 stays
  parked). The penalty uses only TR-insensitive metrics — see Source rule.

## Source rule

This is a **scoring-model design choice**, not an ownership/filings data-treatment
rule, so there is no governing SEC reg. The governing internal rules:

1. **Settled-decisions → "Penalty style":** penalties are *additive* in v1; *no
   multiplicative* penalties. → tiered additive deductions, never a multiplier.
2. **risk_v1 metric definitions** (`app/services/risk_metrics.py`, #591): vol =
   sample-std × √252 (annualized **fraction**); max_drawdown = min running-peak
   drawdown (negative **fraction**); beta = OLS vs SPY with r² = corr². Thresholds
   therefore live in **fraction** units.
3. **Price-return basis + TR caveat** (banked #1635 research): risk_v1 metrics are
   computed on price `close` (NOT dividend-adjusted total return) — the #591 design
   doc labels this honestly and lists TR as a follow-up. A dividend/TR series would
   move return-numerator ratios (CAGR / Calmar / trailing) **materially** but moves
   realized **vol** and **drawdown shape** only **marginally** (dividends are small,
   smooth, ~quarterly additions). → a penalty built on vol + drawdown is **accepted
   on the price-return basis**: it is the part of the risk picture least disturbed by
   the missing TR, so it does not require the deferred dividend vendor. This is NOT a
   claim of TR-equivalence. Calmar / any return-ratio reward IS materially TR-sensitive
   and is **excluded**, deferred to a v-next gated on a TR series (#1635).

## Full-population verification (dev DB, risk_v1, 2026-06-18)

Scanned the **full** `instrument_risk_metrics_current` population: **5179** instruments
have rows. Denominators below are stated per table; each metric is filtered to its own
`*_status='ok'` rows (a non-`ok` status means the metric is not trustworthy and is
excluded from both calibration and the live penalty — see Honest absence).

**Beta is a dead lever for this universe → dropped from the penalty.**
`beta_r2 ≥ 0.30` holds for only **171** of the **5076** `beta_status='ok'` instruments
(**3.4%**) at the 3y window. For ~96% of
instruments beta-vs-SPY is statistical noise; PR-B's own caveat says low-r² beta is
noise. Gating a penalty on r² ≥ 0.30 would fire for almost nobody; not gating would
penalize noise. The value of beta is portfolio/sector-relative (#1636, #1674), not
SPY-absolute. **No beta term in the penalty.**

**The universe baseline is extreme.** 3y-window percentiles over the full population
(vol: 5125 `vol_status='ok'` rows; drawdown: 5179 `drawdown_status='ok'` rows)
(p10/p25/p50/p75/p90/p95):

| metric          | p10    | p25    | p50    | p75    | p90    | p95    |
|-----------------|--------|--------|--------|--------|--------|--------|
| vol_annualized  | 0.246  | 0.343  | 0.557  | 0.929  | 1.446  | 2.081  |
| max_drawdown    | −0.884 | −0.708 | −0.496 | −0.317 | −0.205 | −0.143 |

Median annual vol 56%, median max-drawdown −50%. SPY-like thresholds (vol > 0.20)
would flag ~90% of the universe and discriminate nothing. Thresholds are calibrated
to **this population's tail**.

**Bite-rate on the scored-eligible universe** (3890 eligible; mirrors
`compute_rankings` eligibility; 3888 have a 3y row, 3875 vol_status='ok'):

| trigger                       | count | % of vol_ok |
|-------------------------------|-------|-------------|
| vol_annualized > 0.90 (≈p75)  | 859   | 22.2%       |
| vol_annualized > 1.45 (≈p90)  | 289   | 7.5%        |
| max_drawdown < −0.70 (≈p25)   | 905   | 23.4%       |
| max_drawdown < −0.85 (≈p10)   | 428   | 11.0%       |

Meaningful and tiered — not inert like beta.

## Design

### Window & version

- **Window: `3y`.** Decision-relevant recent realized regime, comparable across the
  bulk of the universe (3888/3890 eligible have a 3y row). The `full`-window
  distribution is near-identical (vol median 0.567, dd median −0.515) so the choice
  is not load-bearing; `3y` chosen for current-regime relevance over ancient history
  in a forward-looking ranking. Per-metric `*_status='ok'` gates already exclude
  thin/insufficient histories.
- **metric_version: `risk_v1`** — pulled from `RISK_METRICS_VERSION` via a **lazy
  import inside the loader** (mirrors #1632 thesis ingestion; avoids the
  risk_metrics → scheduler → refresh_cascade module-scope import chain; SSOT
  preserved).

### Calibrated constants (module-level, explicit; calibrated to the 2026-06-18 dev
population — reviewable at every version bump, like the existing penalty constants)

```
_RISK_PENALTY_WINDOW          = "3y"
_VOL_HIGH_THRESHOLD           = 0.90    # ≈ universe p75
_VOL_EXTREME_THRESHOLD        = 1.45    # ≈ universe p90
_DD_HIGH_THRESHOLD            = -0.70   # ≈ universe p25 (worst quartile)
_DD_EXTREME_THRESHOLD         = -0.85   # ≈ universe p10 (worst decile)
_PENALTY_RISK_HIGH_TIER       = 0.04
_PENALTY_RISK_EXTREME_TIER    = 0.08
```

### Penalty function (pure; table-testable, no DB)

`_realized_risk_penalties(vol_annualized, vol_status, max_drawdown, drawdown_status)
-> tuple[list[PenaltyRecord], list[str]]`

Returns **(penalty records, notes)**. The notes carry the no-row / non-`ok` / None
explanations (which are NOT penalties) so `compute_score` can append them to
`explanation_parts` exactly like the family notes (see Honest absence + Data load).

Two independent, **tiered additive** penalties (a tiered deduction is a single
additive figure sized by tier — NOT a multiplier; settled-decision compliant).
**Comparators are strict** (`>` for vol, `<` for drawdown); a value exactly on a
threshold falls into the lower tier (or none). Boundary cases are measure-zero on
continuous fractions but pinned by tests for determinism:

- `high_realized_volatility` (only if `vol_status == "ok"` and value not None):
  - `vol > _VOL_EXTREME_THRESHOLD` (> 1.45) → deduct `_PENALTY_RISK_EXTREME_TIER` (0.08)
  - else `vol > _VOL_HIGH_THRESHOLD` (> 0.90) → deduct `_PENALTY_RISK_HIGH_TIER` (0.04)
  - else (`vol <= 0.90`) → no record
- `deep_drawdown` (only if `drawdown_status == "ok"` and value not None):
  - `max_dd < _DD_EXTREME_THRESHOLD` (< −0.85) → 0.08
  - else `max_dd < _DD_HIGH_THRESHOLD` (< −0.70) → 0.04
  - else (`max_dd >= −0.70`) → no record

Each `PenaltyRecord.reason` states the value and the tier crossed (auditable). Max
combined realized-risk deduction = 0.16 (≈ existing `stale_thesis` 0.15). vol and
drawdown correlate but are distinct facets (path volatility vs worst peak-to-trough);
stacking correlated additive penalties matches the existing model (red_flag +
dilution both stack). Final `total_score` clip [0,1] is the only floor.

### Honest absence (prevention-log: `None`/non-`ok` status must not fold into a
signal bucket)

Absence is **not** a risk signal — it must never add a penalty:

- No risk row (table lookup returns None) → **no risk penalty**, note
  `"realized-risk: no risk_v1 3y metrics"`.
- `vol_status != "ok"` → skip the vol penalty, note `"realized-risk: vol status=<x>"`.
- `drawdown_status != "ok"` → skip the drawdown penalty, note.
- value None despite `ok` status → skip + note (defensive).

Notes flow into `explanation` exactly like the existing family notes.

### Version gating

- Add `v1.2-{balanced,conservative,speculative}` to `_WEIGHT_MODES` with the **same
  family weights as v1.1** (the penalty is additive; it does not touch family
  weights). The existing `test_scoring.py` mode-sum test auto-covers them.
- **v1.2 must inherit v1.1's TA-enhanced momentum.** TA is currently gated
  `model_version.startswith("v1.1")` at `scoring.py:1010`. A v1.2 model would NOT match
  that prefix and would silently revert to return-only momentum — a regression (Codex
  ckpt-1 HIGH). Widen the gate to enable TA for v1.1 **and** v1.2:
  `model_version.startswith(("v1.1", "v1.2"))`. v1.2 = v1.1 momentum/families + the new
  additive risk penalty; nothing else changes.
- `_realized_risk_penalties` is applied **only when**
  `model_version.startswith("v1.2")` — same gating pattern as TA-on-v1.1+.
  v1 and v1.1 score history is byte-for-byte unchanged (append-only;
  rank_delta compares only within a model_version — settled).

### Data load

In `_load_instrument_data`, add a single indexed lookup (PK
`(instrument_id, metric_version, window_key)`), wrapped in a savepoint like the
`instrument_valuation` query so a partial test DB degrades gracefully:

```sql
SELECT vol_annualized, vol_status, max_drawdown, drawdown_status
FROM instrument_risk_metrics_current
WHERE instrument_id = %(id)s AND metric_version = %(mv)s AND window_key = '3y'
```

`psycopg.errors.UndefinedTable` → row treated as None → no penalty + note. One extra
single-row indexed query per instrument, consistent with the ~7 per-instrument
queries `_load_instrument_data` already runs (no batching change).

### Default model version

`_DEFAULT_MODEL_VERSION` is `v1.1-balanced` today, and `docs/settled-decisions.md:208`
records that as the settled default. Flipping the default to `v1.2-balanced` makes the
live ranking risk-aware (the point of #1633) but is a **settled-decision change**: it
requires (a) operator sign-off, and (b) updating `settled-decisions.md:208` in the same
PR (Codex ckpt-1 MED). This is an operator gate — see the sign-off question. Two clean
options:

- **Flip in this PR** (default becomes v1.2-balanced): the PR leads with the dev
  `v1.1 vs v1.2` ranking delta so the behavior change is approved at merge review;
  `settled-decisions.md:208` updated in the same commit.
- **Ship v1.2 available, hold the default at v1.1**: the engine has v1.2 but does not
  use it until a tiny follow-up flips it after the operator reviews the delta.

Either way the dev ranking delta is produced and presented before the flip lands.

## Out of scope / deferred

- Calmar / any return-ratio **reward** term — TR-sensitive; deferred to a v-next
  gated on a dividend/TR series (#1635).
- Mode-scaled risk appetite (conservative penalizes risk more than speculative) —
  the existing penalties are mode-independent; kept so for v1.2. Possible v-next.
- Sector-relative beta penalty — depends on #1674.

## Tests

Pure-logic table tests on `_realized_risk_penalties` (no DB). Comparators are strict
(`>` vol, `<` dd) — boundary cases pinned:
- vol ≤ 0.90 → none; `0.90 < vol ≤ 1.45` → 0.04; vol > 1.45 → 0.08.
  Boundaries: vol == 0.90 → none; vol == 1.45 → 0.04.
- max_dd ≥ −0.70 → none; `−0.85 ≤ max_dd < −0.70` → 0.04; max_dd < −0.85 → 0.08.
  Boundaries: max_dd == −0.70 → none; max_dd == −0.85 → 0.04.
- `vol_status`/`drawdown_status` not `"ok"` → no penalty + a note, regardless of value.
- value None with ok status → no penalty + a note.
- no row at all → no penalty + a note.
- both fire → two records summing 0.16.
- v1.2 modes present in `_WEIGHT_MODES` and sum to 1.0 (existing test covers).
- a v1.1 model_version produces zero realized-risk penalties (gating); v1.2 with TA
  data still produces TA-enhanced momentum (gate-widening regression guard).

DB-backed: dev-verify (below) rather than a thick integration test, per test-tiering.

## DoD / dev-verify

1. Backfill not required (pure read-path; reads the already-populated
   `instrument_risk_metrics_current`). No migration, no job change, no jobs-proc
   restart.
2. Run `compute_rankings(conn, "v1.2-balanced")` on dev; capture the ranking and the
   `penalties_json` for a panel (AAPL, GME, MSFT, JPM, HD + a known-volatile name).
   Confirm high-vol/deep-drawdown names carry the expected risk penalties and calm
   names carry none.
3. Compute the `v1.1-balanced` vs `v1.2-balanced` rank delta across the eligible
   universe; report how many ranks moved and the largest movers (PR description).
4. Cross-check one penalty against the raw metric: the panel name's
   `vol_annualized` / `max_drawdown` in `instrument_risk_metrics_current` matches the
   tier its `PenaltyRecord.reason` cites.
5. Hit `GET /scores` (or the scores endpoint) for the panel under v1.2 and confirm
   the penalty renders in the response `penalties_json`.
