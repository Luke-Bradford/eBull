# Fair-value band v4 — companion-variable peer screen (#2032)

**Status:** proposal, 2026-07-15. Operator GO + domain-subagent verdict GO confirmed 2026-07-15 (both on issue #2032). Three binding verdict modifications folded (§4.2, §5, §7).
**Extends:** [`2026-07-13-fair-value-band-v2-robustness.md`](./2026-07-13-fair-value-band-v2-robustness.md) §4 part 4 / §5 Phase 2 (the root-cause fix), [`2026-07-15-fair-value-band-ev-ebitda.md`](./2026-07-15-fair-value-band-ev-ebitda.md) (ev leg joins the screen).
**Method version:** `fvb_v3` → **`fvb_v4`** (base changes for re-screened cohorts). **Not a scoring `model_version`** — the band is not a scoring input.

---

## 1. Problem

The pass-2 peer cohort (`peer_pct_for`) is matched on SIC + size only. A multiple is a function of its **companion variables** (§2); a cohort unmatched on them mixes regimes — AAPL (27% net margin, mature) draws ANET (38% margin, high-growth, P/S 24×) and FTNT (16×) as "peers", inflating `peer.p75` and the bull wing. The v2 cap bounds the tail ratio but cannot fix comparability (v2 spec §6.2 honesty note). This is the root-cause fix deferred as Phase 2.

## 2. Source rule

- **Damodaran** ([ps.pdf](https://pages.stern.nyu.edu/~adamodar/pdfiles/eqnotes/ps.pdf)): `P/S = net margin · payout · (1+g)/(r−g)` — P/S is a function of **net margin AND growth**. P/B ↔ **ROE**; P/E ↔ **(forward) earnings growth**. EV/Sales-class multiples share the margin+growth companions; EV/EBITDA screened on the same pair.
- **McKinsey/Koller**, *The right role for multiples*: match peers on ROIC & growth, not industry membership alone. **Alford 1992 / Bhojraj & Lee 2002**: fundamental-matched comparables beat pure-industry cohorts.
- **Screen constants are frozen absolutes**, not cohort-relative percentiles — required by the settled no-cohort-relative-normalization invariant (`docs/settled-decisions.md`) and the R_UP/R_DN freeze discipline (v2 spec §6.2). The domain verdict explicitly disqualified quantile/bucket widths and nearest-k-on-companion (silently hands back the least-bad-8 — the ANET/FTNT failure mode).

## 3. Full-population verification (dev, 2026-07-15 — issue #2032 comments)

Standalone sim replicating `peer_pct_for` exactly (SIC 4→3→2 walk, 7d fresh gate, MIN_PEERS=8, real `_rank_peers`, same `percentiles()`), **validated against the stored AAPL fvb_v3 basis to 6dp**. Two variants: `fresh7` (live semantics on stale-censored dev) and `nogate` (proxy for first fresh-price population). Harness preserved in memory `project_2032_sim_harness.md`.

- Companion availability (of members): margin 92.8–93.5%, growth 74.7–85.1%, ROE 93.0%. Real but not fatal.
- Screen effect, `ratio_up p95` (peer p75/p50, the wing-tail metric), nogate: **ps 4.95→3.15, pb 2.54→1.79 (fresh7 3.56→1.38), ev 3.86→2.84**. Held-of-screenable: ps 50.5% fresh7 / 63.2% nogate; pb 93.5/90.2; ev ~83/85.
- **pe: nil** (fresh7 2.27→2.29; nogate 2.78→2.63) while re-anchoring 505/908 live legs both directions (AAPL improves, MSFT widens) → manufactured churn by the judge-by-calibration principle (v2 spec §2) → **excluded** (§4.2).
- AAPL (fresh7 = exact live cohorts): ps peer p75 24.46→11.86; band bear 202→152, base 321→299 (−6.9%), bull 753→511, bull/base 2.35→1.71.
- **DCTH-class NOT fixed** (no screened cohort at any width → falls back + flag). Its defect is cross-leg base disagreement (36.8×) → **#2043, separate ticket**, not bundled.

## 4. Design

### 4.1 Screen semantics

Companion screen on the **fresh** member set, per multiple:

| leg | companions | width tiers (tight → wide, frozen absolutes) |
| --- | --- | --- |
| `ps` | net margin + revenue growth YoY | (±0.05, ±0.10) → (±0.10, ±0.20) → (±0.20, ±0.40) |
| `ev_ebitda` | net margin + revenue growth YoY | same as ps |
| `pb` | ROE | ±0.05 → ±0.10 → ±0.20 |
| `pe` | — excluded (§4.2) | — |

Predicate: peer passes iff every companion in the tier is non-NULL on **both** sides and `|peer − target| ≤ width`. NULL companion on either side fails the peer (no imputation — absence is a data gap, not a zero).

**Walk order — width-major (the validated sim's semantics):** for each width tier tight→wide, walk the full SIC ladder 4→3→2; at each level, screen the fresh members; if screened survivors ≥ `MIN_PEERS`, size-refine (nearest-8 `_rank_peers`) and re-check ≥ `MIN_PEERS` post-refine (the F1 invariant); first (width, level) that clears wins. All tiers exhausted → fallback (§4.3).

> **Walk-order decision (full-population verified, Codex ckpt-1 MED-1):** the issue GO comment labels the walk "D1 level-outer" but its prose (level-outer, widths inner) and the validated sim harness (width-major: each width walks the full ladder) implement two different orders. A dedicated full-pop comparison of BOTH orders over all 569 stored fvb_v3 `ok` bands (fresh7, dev 2026-07-15, `fvb_order_compare.py`) found: **hold/fallback identical by construction** (both scan the same (width, level) cells; fallback ⇔ none clears); chosen cell differs on ps 62/152, ev 29/113, pb 4/85 held legs; **width-major compresses the wing tail better where they diverge** (`ratio_up p95`: ps 2.21 vs 2.42, ev 2.57 vs 2.78, pb identical 1.42) at slightly higher leg-p50 churn (ps |Δp50| med 28.1% vs 25.2%); **band-level base shifts statistically indistinguishable** (|Δbase| p50 0.000 both, >10% movers 96 vs 91 of 569). **v4 freezes width-major** — it wins the calibration metric (tail compression), matches every number in §3 and the §7 gate predictions, and is the Damodaran/Alford direction (fundamental comparability dominates industry membership).

Target lacking a required companion for the leg ⇒ screen not attempted; unscreened walk + `cohort_screened: false`, screen reason `target_companion_missing`.

### 4.2 pe exclusion — wrong-companion / data-gated (verdict binding mod 1)

P/E's canonical companion is **(forward) earnings growth** (Damodaran). We have no usable forward or normalized earnings-growth source; revenue-YoY as proxy was full-pop tested and **refuted** (nil tail compression, 505/908 legs re-anchored — churn without calibration benefit). This is NOT "P/E screening unnecessary": it is the wrong companion + a data gap. **Revisit trigger:** a forward or normalized earnings-growth source landing in the fundamentals layer reopens pe screening as its own ticket. P/E meanwhile keeps the strongest existing discipline (most two-sided legs; tightest R-caps).

### 4.3 Fallback + quality knock

Screen exhausted (or target companion missing) ⇒ the **current unscreened walk, unchanged** (same ladder, same fresh gate, same size refine, same `fallback_meta`), plus:

- `cohort_screened: false` + screen reason in the leg's basis entry (§4.4);
- quality knock (Codex ckpt-1 MED-4, made precise): `QualityInputs` gains `screen_fallback: bool = False`; `band_quality_status` subtracts 1 point when it is true. `compute_band` sets it true iff any **contributing** leg (appended to `per_share_triples` — synth-None legs and `dropped_nonpositive` ev legs are non-contributing and never knock) is screenable (`m ∈ {ps, pb, ev_ebitda}`), peer-backed (`peer.p50` present), and `cohort_screened` false. Own-only legs are not knocked — there is no peer cohort to screen; they are already handicapped via `n_comparator_sides`.

A high fallback rate is the finding, not mis-design (verdict): no comparable set exists ⇒ truthful flag beats a vacuous screen.

### 4.4 Screen provenance in stored basis (verdict binding mod 2)

Per screenable leg, `basis_json.multiples[m]` gains (same reconstructability discipline as `precap_*`/`capped_*`):

- held: `"cohort_screened": true, "screen": {"sic_level": L, "width_tier": i, "survivors_n": n}` (`width_tier` = 0-based index into the frozen tier table; `survivors_n` = post-screen fresh count pre-refine);
- fallback: `"cohort_screened": false, "screen": {"reason": "no_screened_cohort" | "target_companion_missing"}`.

`pe` legs carry neither key (screen not applicable). Width constants live in code under `METHOD_VERSION` pinning — (tier index, method_version) fully reconstructs the widths.

**Per-leg cohort level (Codex ckpt-1 re-pass #1):** every leg's basis entry additionally records `"sic_level"` from its own cohort meta (held legs: the screened walk's level, duplicating `screen.sic_level`; fallback/unscreened/pe legs: the unscreened walk's level, 0 when peer-absent) — the band-level `sic_level = max(...)` quality input is not per-leg reconstructable, and a fallback leg's contributing cohort level would otherwise be lost.

**Provenance survives non-contribution (Codex ckpt-1 MED-3):** `compute_band` today skips the basis entry entirely when `synth_multiple` returns None (both comparator sides absent). v4 builds the basis entry for **every selected leg** — peer/own percentile stats (possibly all-None) + cohort meta + screen provenance — before the synth-None continue, so a screened-leg fallback whose peer side then failed MIN_PEERS everywhere still leaves its screen audit trail on the stored row. Legs without a synth contribute nothing (no `base_value` key), same as the dropped-ev precedent.

### 4.5 Companion variable definitions (sql/220 strictness mirror — the validated sim's exact derivation)

**Source rule (Codex ckpt-1 MED-5 — explicit deferral):** every treatment below defers to the settled repo rule set, not first principles: quarter selection (`period_type IN (Q1..Q4)`, `superseded_at IS NULL`, `normalization_status='normalized'`, latest-`filed_date` wins per `period_end_date`), strict 4-quarter flow sums, the ≤330d window-span adjacency bound, and latest-quarter stock items are all `sql/220_ttm_strict_flow_sums.sql` + its spec `docs/specs/fundamentals/2026-07-12-2008-ttm-reconciliation.md` (#2008; interim-period treatment grounded in Reg S-X Art. 10, 17 CFR 210.10-01(c) — see also #2036's 10-01(c)(3) YTD de-cumulation). The one NEW rule this spec adds is the **cross-window adjacency bound** `ttm_start − prior_end ∈ [1, 120]` days: consecutive quarter-ends sit ~90d apart (the sql/220 comment's own 91–92d arithmetic); ≥1 excludes overlapping/duplicate windows, ≤120 tolerates a 53-week fiscal calendar while excluding a skipped quarter (~180d+). Full-pop validated: growth coverage 74.7–85.1% of members (§3) under this bound.

- **net margin** = `net_income_ttm / revenue_ttm`, requires `revenue_ttm > 0`, `net_income_ttm` non-NULL.
- **revenue growth YoY** = `(revenue_ttm − rev_prior_ttm) / abs(rev_prior_ttm)`; prior TTM = quarters rn 5–8 of the same deduped/ranked series (`DISTINCT ON (instrument_id, period_end_date)`, latest-filed wins, `period_type IN (Q1..Q4)`, not superseded, normalized), strict: COUNT(*)=4 AND span ≤ 330d AND COUNT(revenue)=4; **window adjacency**: `ttm_start − prior_end ∈ [1, 120]` days; `rev_prior ≠ 0`.
- **ROE** = `net_income_ttm / shareholders_equity` (latest-quarter stock item, rn=1 — same row the ttm view reads), requires equity > 0.

## 5. Schema + write path

- **sql/228** (additive; sql/221/226 immutable per migration-content-drift rule): `ALTER TABLE fair_value_cohort_members ADD COLUMN IF NOT EXISTS net_margin numeric(18,6), rev_growth_yoy numeric(18,6), roe numeric(18,6)` — nullable; pre-v4 as_of rows stay NULL (never re-read: each materialize DELETE+INSERTs its as_of).
- **`_MATERIALIZE_SQL`**: prior-TTM CTE (dedup+rank mirror of sql/220, rn 5–8, strict window, `prior_end`) LEFT-joined into `base`; companions computed per §4.5 and written on every member row of the name. Each companion NULLed when `abs(value) ≥ _MAX_SANE_MULTIPLE` (1e6). Rationale (Codex ckpt-1 LOW, corrected): `numeric(18,6)` overflows at 1e12, so overflow alone needs only `< 1e12`; 1e6 is a **degenerate-ratio sanity bound** reusing the existing constant for symmetry with the member multiples — a |margin| or |growth| ≥ 1e6 is a tiny-denominator artifact; NULLing it guarantees degenerate-ratio PAIRS cannot accidentally screen-match each other (two garbage values within ±width would otherwise pass). Population-loss check rides the acceptance gate: count companions NULLed by the bound post-backfill (expected ≈ 0).
- **`_MEMBER_SQL`**: project the 3 companion columns (dict-row key + projection in the SAME diff, real-query db test — prevention log #2021).
- **`_TARGET_SQL`**: LEFT JOIN LATERAL prior-TTM (same strictness) + project `ttm_start`, `rev_prior`, `prior_end`, `net_income_ttm` (already projected); target companions computed in a pure `companion_vars()` mirroring §4.5 exactly.

## 6. Method version + backfill

- `METHOD_VERSION = "fvb_v4"`. Consumers (`thesis.py`, `freshness.py`, `fundamentals_admin.py`) import the constant — transparent. Deploy→backfill window: `fair_value_band_current` has no fvb_v4 rows, thesis band block reads `available:false` (same accepted class as v2→v3); `freshness.py` detects the empty method_version and reschedules; we run the refresh immediately anyway.
- Backfill = operator restarts the jobs task (retired-writer rule — restart BEFORE re-backfill), then `fair_value_band_refresh` full-universe.
- Expect a **one-time #2013 re-thesis alert wave** (base moves >10% on ~500–800 legs; damped at band level by median-of-bases + own-blend) — anticipated, fine.
- **#2031 hysteresis sequenced AFTER v4** (screen adds nightly membership churn at band edges; hysteresis should damp the re-based cohorts). **#2043 leg-coherence separate** (own source-rule research).

## 7. Acceptance gate (verdict binding mod 3)

Same-day controlled v3-vs-v4 (**never cross-day** — #2021/#2022 lesson: input drift masquerades as method drift). v3 fresh from a main-code worktree, v4 from the branch, same dev DB, same day; rows coexist under distinct `method_version`.

1. **Tail compression ≈ sim:** per-leg `ratio_up p95` compression in line with the ok-band-leg predictions (fresh7, `fvb_order_compare.py`, width-major): **ps 2.74→2.21, pb 3.33→1.42, ev 3.36→2.57**. Materially below ⇒ **NO-GO flip**.
2. **Fallback rates ≈ sim:** compare like-with-like. Ok-band-leg predictions (the direct v3-vs-v4 comparable, fresh7): held-of-screenable **ps 152/182 = 83.5%, pb 85/90 = 94.4%, ev 113/127 = 89.0%** (target-companion-missing: ps 32/214, pb 12/102, ev 14/141 of peer-backed legs). The verdict's member-population figure (ps held 50.5% fresh7 — its "materially below ~37% falsifies" trigger) applies to the all-members population, which the stored-band comparison does not sample; both recorded here so the gate reads the right prediction for the right population.
3. **Base damping (full-pop prediction, Codex ckpt-1 MED-2):** predicted band-level |Δbase| over all 569 ok bands: **p50 = 0.000, p90 ≈ 0.20, p95 ≈ 0.32, >10% movers 96 (16.9%), >25% movers 43** — the median band is unmoved (median-of-bases + own-blend damping confirmed full-pop; the ~7% AAPL figure was one case, superseded by this distribution). **NO-GO flip** if observed p50 materially exceeds ~0 (e.g. > 5%) or >10%-movers far exceed ~17%.
4. **Margin-only degrade-stage variant** (harness variant, not shipped code): insert a margin-only intermediate stage for ps/ev; adopt in a follow-up ONLY if it compresses tails without pe-style churn; otherwise record the negative on #2032.
5. Standard panel: AAPL/GME/MSFT/JPM/HD via the rollup endpoint; AAPL cross-checked against §3 sim figures (its cohorts were 6dp-validated).
6. #2021's deferred R_UP/R_DN retune + cap-hit gate still ride the first fresh-price population — unchanged by this spec.

## 8. Tests

- Pure tier (`test_fair_value_band_policy.py`): `companion_vars` derivations + adjacency boundaries (0/1/120/121d, `rev_prior=0`, negative prior abs-denominator); screen predicate per leg (NULL either side fails; boundary `|Δ| = width` passes — sim used `≤`); frozen tier table shape (pe absent); quality knock fires only for contributing peer-backed screenable legs.
- DB tier (`test_fair_value_band_io.py`): real `_MATERIALIZE_SQL` writes companions; real `_MEMBER_SQL`/`_TARGET_SQL` projections drive the screen end-to-end (held path with provenance in `basis_json`, fallback path with flag); prior-TTM adjacency exercised through seeded quarters.

## 9. Limitations

- DCTH-class cross-leg disagreement is **not** addressed (→ #2043).
- Growth companion is **revenue** YoY — correct for ps/ev (Damodaran's P/S companion), unavailable-in-the-right-form for pe (§4.2).
- Dev acceptance runs on a stale-price-censored population (fresh7); full effect lands at the first fresh-price population alongside the deferred #2021 cap gate.
