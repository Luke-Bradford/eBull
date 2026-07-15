# fair_value_band cohort-membership hysteresis (#2031) — premise unsupported on every stored night; ship chosen-member provenance instead

**Status:** research verdict + minimal implementation, 2026-07-15.
**Resolves:** #2031 (v2 spec §4 part 3, Phase 1b — [`2026-07-13-fair-value-band-v2-robustness.md`](./2026-07-13-fair-value-band-v2-robustness.md)).
**Method version:** unchanged (`fvb_v4`) — band outputs (bear/base/bull/quality/reason) are byte-identical; only `basis_json` gains audit keys.

---

## 1. The premise under test

v2 §4 part 3: "the size-refined 8-nearest cohort is re-picked every recompute; membership churn jitters `peer.p50` → the audited base — add hysteresis (stored prior membership, retain unless material change)."

Scope constraint fixed at handoff (2026-07-15): a genuine cohort change (delist, screen re-tier, fresh fundamentals, freshness exit) MUST flow through; **only size-refine jitter at the nearest-8 boundary may damp**. So the premise stands or falls on one measurable: how often does the nearest-8 boundary churn WITHOUT a genuine-class cause?

## 2. Source rule / structural analysis (code, not memory)

Membership selection is deterministic and (almost) price-independent:

- Ranking key = `|ln(total_assets) − ln(target_total_assets)|`, tie-break `instrument_id` (`peer_comparison.py:262-264`). `total_assets` moves only on a new filing.
- Pool gates: SIC prefix (filing-driven), `close_date` freshness (`fair_value_band.py` `peer_pct_for`), `mult_value > 0` — price-independent for pe/ps/pb (`close > 0` always; sign is eps/revenue/equity), price-dependent only for the negative-EV ev edge (2/532 full-pop, #2021), dual-class routing (filing-driven), v4 companion screen (quarterly companions, sql/228).
- No RNG, no wall-clock, no dict-order dependence anywhere in the walk.

Therefore a chosen-8 change REQUIRES one of (Codex ckpt-1 MED: list is exhaustive over the walk's inputs): a member OR TARGET `total_assets` update/restatement re-ranking the pool (new filing → genuine; the target-side case re-ranks against an unchanged pool and is covered by the same displacement-margin measurement below), freshness flip (genuine exit — and a retained-but-stale member would put stale-price multiples into `peer.p50`, violating the §4.6 freshness invariant, so hysteresis could never legally retain it), universe/delist change (genuine), screen re-tier (genuine per handoff), EV-sign flip or `_MAX_SANE_MULTIPLE` crossing (price-dependent pool exits — genuine: the multiple stops being meaningful). Nightly `peer.p50` variance between filings is member VALUE drift (prices move, membership constant) — which hysteresis explicitly does not damp (membership pinned, values refresh; damping ≠ freezing).

## 3. Full-population verification (dev, 2026-07-15)

**Population (named, prevention-log "gate populations must match"):** the 868 peer-backed legs of the 569 `fvb_v4 reason='ok'` `fair_value_band_current` rows — the legs whose `peer.p50` feeds the audited base — replayed with the EXACT v4 walk (screened width-major + unscreened fallback, real `_rank_peers`, real `percentiles`) over every consecutive stored `fair_value_cohort_members` night-pair: 07-10→07-13, 07-13→07-14, 07-14→07-15 (~2,595 leg-nights). Harness: `#2032` validated sim pattern ([[project-2032-sim-harness]]); script `fvb_churn_measure.py` (session scratchpad; regenerate from the memory pattern).

Measurement notes (Codex ckpt-1 MEDs, stated as design choices with their effect on the class under test):

- **Companions = ONE current map applied to all dates** (derivation identical to the v4 rule — [`2026-07-15-fvb-v4-companion-screen.md`](./2026-07-15-fvb-v4-companion-screen.md) §4.5 / `companion_vars()`; sql/228 member cols are NULL pre-07-15 so per-night stored values do not exist). Holding the map constant makes the screen predicate IDENTICAL across each pair by construction → the measurement injects zero spurious screen churn and under-counts only screen-TIER churn (quarterly companion drift), which is genuine-class per the handoff and outside the dampable class. Size-refine jitter — the class under test — is measured EXACTLY: it depends on per-night `total_assets`/freshness/pool rows, all taken from each night's real stored member rows.
- **Dev's frozen closes** are conservative for the stale-exit class (staleness exits fire as `as_of` advances while closes don't; prod daily bars keep members fresh). They SUPPRESS the price-dependent negative-EV-flip and `_MAX_SANE_MULTIPLE`-crossing churn (a confound, not a blanket bias) — but both are pool EXITS classified genuine (§2), so their suppression cannot hide dampable size-refine jitter.

| Night pair | Result |
| --- | --- |
| 07-13→07-14 (quiet adjacent night) | **727/727 legs identical — zero churn of any kind** |
| 07-10→07-13 (3-day gap) | 7/726 swapped (pe), all materially-closer newcomer arrivals |
| 07-14→07-15 (#2036 D&A backfill, +405 members — max-churn night on record) | pe/ps/pb: 40 swapped legs, exit cause **stale ×40 (100%)** — hysteresis cannot retain (violates freshness), counterfactual `d_hyst ≡ d_fresh` exactly. ev: 30/42 legs re-tiered + 34 newcomers = the genuine fresh-fundamentals wave the backfill was FOR |

**The dampable class is empirically absent.** Of 128 pairwise rerank displacements (outgoing still eligible), the log-distance improvement of incoming over outgoing: min 0.048, **only 2 < 0.10**, 4 < 0.25, p50 1.62 (incoming ~5× closer in asset-ratio space), incoming-farther 0/128. Under the issue's own admit rule ("a materially-closer-by-assets name appears") every displacement above any sane threshold admits — hysteresis fires on ≤2 of ~2,595 leg-nights (0.08%), both inside the genuine backfill wave.

**Counterfactual hysteresis (retain-survivors + fill-vacancies, values from current night):** on every pe/ps/pb swapped leg, `d_hyst == d_fresh` (exits are undampable). The ONLY damping it produced was on ev during the #2036 backfill (Δp50 median 20.8% → 0%) — i.e. its only measured effect is **blocking a genuine data improvement**, exactly what the handoff constraint forbids.

## 4. Verdict

**Do not build stateful hysteresis.** No sql/229 state table, no `fvb_v5`, no backfill, no acceptance wave.

Precise claim (Codex ckpt-1 HIGH: scoped, not overclaimed): the premise ("re-pick jitters the base") is **unsupported in every stored night-pair** (the complete temporal population that exists, including the highest-churn night on record) AND **structurally confined** — §2 shows every membership-changing input moves only on filing/universe/freshness/sign events, all genuine-class under the handoff constraint. The decision does not require "jitter can never exist"; it requires "no evidence it exists + the machinery is a standing risk (its only measured effect was blocking a genuine data improvement) + §5 makes any future occurrence cheaply detectable on real fresh-price nights before any state machinery is built". Same falsified-premise family as #1645 / #1662 / #2022-winsorization (question the MODEL, not the case).

## 5. Deliverable instead — chosen-member provenance (`peer_ids`)

The measurement above required a full replay harness because the chosen-8 IDs are not stored — `basis_json` carries peer percentiles and screen provenance but not membership. That is both an auditability gap ("persist enough structured evidence for auditability") and the reason the hysteresis question could not be answered from stored rows.

Change (pure, additive, base-neutral):

1. `peer_pct_for` returns the chosen member `instrument_id`s in its meta dict (`peer_ids`, sorted ascending for determinism — rank order is NOT stored; it is reconstructable by joining `fair_value_cohort_members` at the row's `as_of_date`, Codex ckpt-1 LOW), on BOTH the screened and unscreened paths.
2. `compute_band` copies `meta["peer_ids"]` into the per-leg basis entry (same pattern as `cohort_screened`/`screen`).
3. Tests (pure tier): `compute_band` carries `peer_ids` into `basis["multiples"][m]`; absent when the leg is peer-absent.

Size: ≤ 8 ints per peer-backed leg (~908 legs universe-wide) — negligible. `METHOD_VERSION` unchanged (`own_capped_total` precedent: basis-key additions without value changes do not bump). No jobs-restart urgency (no #2008 clobber class — old code simply writes rows without the key until restart); no backfill (fills forward nightly).

## 6. Revisit trigger

Reopen hysteresis iff a stored-provenance churn scan over a **fresh-price population** (post market-data restoration; population = peer-backed legs of `ok` bands, consecutive nightly observations) shows small-margin rerank displacement (log-distance improvement < 0.25, outgoing still eligible) on **> 1% of leg-nights**.

Scan mechanics (Codex ckpt-1 HIGH: `peer_ids` alone is not sufficient): diff `peer_ids` across consecutive `fair_value_band_observations` rows per (instrument, multiple), then join each night's ids to `fair_value_cohort_members` ON `(as_of_date, instrument_id, multiple)` to recover `total_assets` (log-distance + margins), `close_date` (freshness/eligibility) and companion cols (screen eligibility) — every gate input is a stored member column, so the scan is SQL + a leg-classifier over stored rows, no walk replay. Retention assumption this rests on: `materialize_cohort_members` deletes only its OWN `as_of_date` (`fair_value_band.py::materialize_cohort_members`); no code path prunes historical nights (4 nights currently retained). If a member-table retention policy is ever added, it must keep ≥ the scan window.
