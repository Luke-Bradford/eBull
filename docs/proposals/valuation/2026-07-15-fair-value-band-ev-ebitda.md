# Fair-value band — EV/EBITDA multiple (#2021)

**Status:** proposal, 2026-07-15.
**Extends:** [`2026-07-12-deterministic-fair-value-band.md`](./2026-07-12-deterministic-fair-value-band.md) §4 (deferred EV/EBITDA, §11 item 5) and [`2026-07-13-fair-value-band-v2-robustness.md`](./2026-07-13-fair-value-band-v2-robustness.md) (cap machinery — its §6.2 "unknown multiple ⇒ no-op" guard is exactly what this fills in).
**Method version:** `fvb_v2` → **`fvb_v3`** (a 4th multiple joins the profitable profile → `base_value` changes for names that gain the leg; the issue text's "fvb_v1→v2" predates #2022 taking v2). Not a scoring `model_version` — the band is not a scoring input (same posture as #2022).

## 1. Source rule

| Decision | Governing rule |
|---|---|
| EBITDA definition | **Formula shape from repo invariant `sql/201:128/154`**: `EBITDA = operating_income_ttm + D&A` (OpInc-based). Conscious divergence: SEC Release 33-8176 defines EBITDA from *net income* (+interest+taxes+D&A); we are not publishing a non-GAAP disclosure, and an OpInc-based shape is what every other repo EV/EBITDA surface uses. **Strictness diverges from sql/201's letter** (next row) — where D&A is present, band EBITDA equals the view's `ebitda_ttm` exactly; where D&A is NULL the view understates (OpInc+0) while the band abstains. |
| Strict D&A (no `COALESCE(d&a,0)`) | `sql/220` is strict by construction (`COUNT(depreciation_amort)=4` else NULL, sql/220:106-107). Codex ckpt-1 on the v1 spec folded "EV/EBITDA strict-D&A" (v1 spec §11 resolved log). sql/201's `COALESCE(d&a,0)` is display-grade (one name's own ratio): for a *median over members* it would mix two definitions (true EBITDA vs OpInc-only) in one cohort statistic — silent median poisoning vs auditable statused absence. |
| EV composition | `sql/201:92-97`: `EV = market_cap + COALESCE(long_term_debt,0) + COALESCE(short_term_debt,0) − COALESCE(cash,0)`. Stock items from `sql/220:151-154` (`MAX FILTER (rn=1)`). Known limitation (as sql/201): no preferred equity / minority interest terms — not stored. |
| Missing debt = 0 | Not by assertion — **full-pop falsified via interest expense** (§2): 90/103 debt-both-null eligible names have NO positive `interest_expense_ttm` → consistent with zero debt (supporting evidence, not proof; COALESCE-0 matches sql/201's treatment). The 13 with positive interest are a data gap with a known bias direction (EV understated → implied value overstated) → **coherence gate**: debt-both-null AND `interest_expense_ttm > 0` ⇒ leg ineligible (same fail-closed family as `currency_mismatch`). NULL interest carries no evidence either way → treated as the dominant debt-free class. |
| Missing cash ≠ 0 | 27/330 eligible have `cash` NULL with median total_assets $1.3B — a going concern with zero cash is implausible ⇒ data gap, not a real zero; leaving it 0 overstates EV (missing subtrahend). **Gate: `cash IS NOT NULL`** for the leg (member + target). |
| Member market cap | `close × shares_outstanding` at the single batch as-of date — the already-shipped pass-1 pattern (v1 spec §4.3); valid because dual-class members are excluded (below), so the #1921 "cap policy in Python" rule is not triggered (that rule governs the multi-class total-company assembly). |
| Dual-class | EV is cap-based ⇒ target: the existing §4.2 `basis != not_multiclass → intersect {pe}` gate already drops it. Cohort: `keep_dual = (multiple == "pe")` already routes every non-pe multiple through the curated-oracle anti-join — `ev_ebitda` inherits it with no code change. Mirrors `sql/201:254` (ev_ebitda suppressed for dual-class). |
| Own-history | **Absent by construction** — `fundamentals_snapshot` has no historical EBITDA/debt/cash trio (`sql/201` legacy CTE: `NULL::numeric AS ebitda_ttm`); a 4th strict-TTM copy is banned (#2008). EV/EBITDA is **peer-only**; the v2 fixed cap is its only wing bound — precisely the tail class the cap was built for (v2 spec §6.2). |
| Multiple-per-profile | Profitable non-financial gains the leg: `{pe, ps, ev_ebitda}` — **add, not replace** (v1 spec §11 open item 2 "is P/S right for profitable?" stays open; it belongs to #2032's comparability work). Median-of-3 bases is *more* robust than mean-of-2 (an outlier P/S base loses base influence). Financials (SIC 60–67) unchanged `{pb, pe}` — EV meaningless for deposit-funded balance sheets (v1 spec §2). |

## 2. Full-population verification (dev DB, 2026-07-15)

Whole tradable universe, not a sample:

- `is_complete_ttm` 3,928 · profitable 2,032 · profitable non-financial 1,375.
- **EV-eligible strict: 330** (op+D&A both present, EBITDA>0, shares>0). Lost: 815 to NULL op/D&A (**671 = D&A-only-null**, 144 op-null), 8 to EBITDA≤0. The memory-cited "1,647" was the loose `COALESCE(d&a,0)` measure. The §1 debt/cash coherence gates remove up to a further ~40 (27 cash-null + 13 debt-null-with-interest, overlap unmeasured) → **~290 target legs**.
- **D&A gap — full-population classes** (671 D&A-only-null names): **555 have D&A rows somewhere** in `financial_periods` (519 in ≥1 discrete quarter, 36 FY-only) — the YTD-attribution class (verified on AAPL: D&A lands only in Q1+FY rows; 10-Q cash-flow facts are YTD; Q1 YTD = discrete quarter, Q2–Q4 never attributed); **116 have no D&A row anywhere** — the concept-map class (`sec_fundamentals.py:206-209` maps only 2 concepts; `Depreciation`, `DepreciationAmortizationAndAccretionNet`, `AmortizationOfIntangibleAssets` unmapped). → **Follow-up ticket** (parser YTD de-cumulation + concept widening); band coverage grows organically when it lands. AAPL/MSFT/GME are D&A-null today → no EV leg, band unchanged for them.
- **Member pool 532** (EBITDA>0 regardless of net-income sign — the multiple's own §4.1 gate; profitability gates only the *target's* profile); **negative-EV members excluded: 2/532** (net cash > cap ⇒ the multiple is meaningless — Damodaran, relative-valuation: only positive multiples are aggregable; a negative-EV firm cannot be priced by EV comps). Cohort reach: 407 have a ≥8 cohort (139 SIC-4 · 51 SIC-3 · 217 SIC-2 · 123 none). SIC-2-heavy ⇒ `quality_status` already down-scores coarse-ladder legs.
- **Net-debt back-out sign risk is real:** implied per-share ≤ 0 on raw cohort percentiles: bear 26/407 (6.4%), base 16 (3.9%), bull 6 (1.5%) → leg-level fail-closed drop guard required (§3.4).
- Field availability among the 330: both debt fields NULL 103 — **interest-expense falsification: 90/103 have no positive `interest_expense_ttm`** (consistent with zero debt; COALESCE-0 safe), 13 have positive interest (median interest/EBITDA 6.8% — real leverage, missing debt facts) → gated out (§1). Cash NULL 27 (median assets $1.3B vs pop median $3.3B — data gap, not zero) → gated out (§1).
- **Envelope-ratio calibration** (SIC-ladder cohort percentiles; approximation: no nearest-8 refinement, self-inclusion, staleness ignored — dev prices stale): `p75/p50` p50 1.74 · p90 2.44 · **p95 2.77** · p99 4.14 · max 4.14; `p50/p25` p50 1.46 · **p95 1.87** · p99 2.30 · max 2.30.
- **Fixture HD** (SIC 5211): op 20.738B + D&A 3.569B = EBITDA 24.307B; net debt 1.902B; close 317.48 → **EV/EBITDA 13.10×** — cross-source sane (public screens put HD ≈13–14×). JPM = financial → correctly never gets the leg.

## 3. Design

All in `app/services/fair_value_band.py` unless noted. Percentile synthesis, blend+envelope, freshness, currency, quality machinery: **unchanged** — `ev_ebitda` is a new leg flowing through the existing pipeline.

### 3.1 Eligibility (§4.1 extension)

`_computable(t, "ev_ebitda")` ⇔ all of:

1. `operating_income_ttm is not None and depreciation_amort_ttm is not None and (op + da) > 0` (strict EBITDA);
2. `shares_outstanding > 0`;
3. `cash is not None` (§1: NULL cash = data gap, EV overstated);
4. NOT (`long_term_debt is None and short_term_debt is None and interest_expense_ttm > 0`) (§1 coherence gate: interest betrays unrecorded debt). Debt-null with no positive interest ⇒ debt = 0 (falsified-safe, COALESCE-0 as sql/201).

New pure helpers (policy stays table-testable): `ebitda_ttm(op, da) -> float | None` (strict: None if either None), `net_debt(long_term_debt, short_term_debt, cash) -> float | None` (None iff cash None; debt COALESCE-0). `TargetInputs` gains `operating_income_ttm / depreciation_amort_ttm / long_term_debt / short_term_debt / cash / interest_expense_ttm`, all defaulted `None` (existing constructors unchanged).

### 3.2 Profile selection (§4.2)

Profitable non-financial: `["pe", "ps", "ev_ebitda"]`. Financial / rev-only / multiclass-intersect: unchanged.

### 3.3 Conversion (§4.5)

`to_per_share` gains the EV arm — an affine transform, not a scalar product:

```text
implied_per_share = (mult × ebitda_ttm − net_debt) / shares_outstanding
```

Monotonic increasing in `mult` (ebitda>0, shares>0) ⇒ low≤base≤high preserved through conversion. Negative net debt (net cash, e.g. GME −7.4B) raises implied value — correct, not a special case.

### 3.4 Leg-drop guard (fail-closed, new)

After cap + conversion, if **any** of the converted (bear, base, bull) ≤ 0 (mult×EBITDA < net debt — the equity is an option, not a price target): **drop the whole leg**. Never enters `combine_across` (a ≤0 bear would poison the combined `min`). Expected ~6% of EV legs (§2). Row-level `reason` unaffected (other legs still combine; a name whose ONLY leg dropped follows the existing `thin_cohort` absence path).

**Restructure required (Codex ckpt-1):** today `compute_band` writes `basis["multiples"][m]` only on the success path (fair_value_band.py:419). Build the basis entry (peer/own stats, cohort meta) BEFORE the append decision; the drop then adds `"dropped_nonpositive": true` and skips `per_share_triples.append` + the entry's `base_value`. Peer stats stay auditable on the stored row.

**`n_selected` / quality semantics after a drop — conscious decision:** a dropped leg follows the established synth-None precedent — `n_selected` stays `len(selected)` (profile-selected, the column's shipped meaning for non-contributing legs) and `QualityInputs.n_selected` likewise; contribution is visible in `basis_json` (entry without `base_value`, flag set). Changing to a contributing-count would silently alter existing pe/ps/pb rows' quality — out of scope.

### 3.5 Cap constants (v2 §6.2 extension)

```text
_R_UP["ev_ebitda"] = 2.8   # ≈ full-pop p95 of p75/p50 (2.77)
_R_DN["ev_ebitda"] = 1.9   # ≈ full-pop p95 of p50/p25 (1.87)
```

Provisional — calibrated from the §2 approximation, NOT from stored fvb legs (none exist pre-backfill). The #2022 §6.5 acceptance gate re-runs post-backfill with `ev_ebitda` included; retune to the fvb_v3 p95 if any leg fails.

### 3.6 Pass-1 / pass-2 (IO)

- `_MATERIALIZE_SQL`: 4th UNION arm — `('ev_ebitda', (close×shares + COALESCE(ltd,0)+COALESCE(std,0)−cash) / (op + da))` with `WHERE (op + da) > 0 AND shares > 0 AND cash IS NOT NULL AND NOT (ltd IS NULL AND std IS NULL AND interest_expense_ttm > 0)` — the member gate mirrors §3.1 exactly (one definition for cohort + target; NULL op/da propagates → dropped = strict). The outer `mult_value > 0` also excludes negative-EV members (2/532 full-pop, §2 — meaningless multiple per Damodaran). `base` CTE + `_TARGET_SQL` gain the six columns.
- `peer_pct_for`: no change (`keep_dual` already false for non-pe). Update the `_MEMBER_SQL` header comment (fair_value_band.py:621) — it currently reads "P/S & P/B" for the anti-join set.
- `_own_series`: add `"ev_ebitda": []` to the buckets dict (no snapshot series exists) → `own_range([])` → all-None → peer-only synth. No KeyError.
- **Migration `sql/226`**: extend `fvcm_multiple_chk` CHECK to `('pe','ps','pb','ev_ebitda')` (sql/221:38). DROP CONSTRAINT + ADD; the migration comment carries the new vocabulary (sql/221 itself is NOT edited — applied migrations are immutable, migration-content-drift rule).

### 3.7 Method version + audit

`METHOD_VERSION = "fvb_v3"`. Consumers (`thesis.py`, `freshness.py`, `fundamentals_admin.py`) import the constant → transparent. `basis_json.multiples.ev_ebitda` carries the standard leg shape + `ebitda_ttm` + `net_debt` (conversion reconstructable from the row) + `dropped_nonpositive` when tripped. freshness.py's "no rows for current method_version" detection reschedules the 24h refresh automatically; operator can force.

## 4. Tests (pure tier, `tests/test_fair_value_band_policy.py`)

- `_computable` ev gates: D&A None → False; op None → False; op+da ≤ 0 → False; shares required; cash None → False; debt-both-None + interest>0 → False; debt-both-None + interest None/0 → True; all-present → True.
- `select_multiples`: profitable+ev-computable → 3 legs; profitable ev-not-computable → `[pe, ps]` (existing tests unchanged by None defaults); financial excludes ev; multiclass intersect drops ev.
- `ebitda_ttm` strict None propagation; `net_debt` COALESCE-0 + net-cash negative.
- EV conversion arithmetic: mult 10 × EBITDA 100 − net_debt 200, shares 10 → 80; order preservation; negative net debt raises value.
- Leg-drop: bear ≤ 0 → leg absent from combine, flag set, peer stats retained, sibling legs still combine; ONLY-leg dropped → `thin_cohort`.
- `cap_envelope("ev_ebitda", …)` clamps (no longer the unknown-multiple no-op).
- Peer-only synth: own all-None → degrade to peer triple (existing path, ev-flavored case).
- Golden HD-style fixture: frozen inputs → frozen leg values (drift guard).

One db-tier integration case extends the existing two-pass test: a seeded ev_ebitda member materializes + a dual-class seeded member stays out of the ev median.

## 5. Rollout / DoD

Single PR (schema CHECK + service + tests + this doc). Standard gates + `pre-pr-fresh-agent-review` (filings-ETL-adjacent). Post-merge operator runbook (same as #2022):

1. Restart VS Code jobs task (service code changed).
2. Force full-universe recompute — `fair_value_band_refresh` (fvb_v3 rows write alongside v2; observations append-only).
3. Smoke panel AAPL/GME/MSFT/JPM/HD: AAPL/MSFT/GME unchanged (D&A-null, no ev leg — expected); HD gains ev leg ≈13×; JPM financial-no-leg.
4. **Acceptance gate re-run** (v2 spec §6.5 incl. ev), two separate checks: (a) **leg-level invariance** — pe/ps/pb per-leg `basis_json` p25/p50/p75 must be row-identical v2→v3 for identical inputs (this change must not touch those legs' own stats; only the combined band may move, via base-median + envelope composition where an ev leg joins); (b) **ev leg health** — tail ≤ R_UP+ε; cap-hit rate ~2–8%; if fail → retune R to fvb_v3 p95, re-backfill.
5. Cross-source confirm HD EV/EBITDA vs a public screen.

**Follow-up ticket (filed with PR):** D&A discrete-quarter coverage — YTD de-cumulation + concept-map widening (§2); unlocks ~671 names into the EV leg with zero band-code changes.
