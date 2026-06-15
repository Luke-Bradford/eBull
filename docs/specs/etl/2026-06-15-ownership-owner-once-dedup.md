# Ownership rollup — one beneficial owner, counted once (#1640)

**Status:** proposed (unshipped)
**Issue:** #1640 (part of epic #788; depends on #1638, merged `1ddbea43`)
**Area:** `app/services/ownership_rollup.py` (pure service; **no schema change**)

## Problem

`_compute_concentration` (rollup.py:973) and `_compute_residual` (rollup.py:939)
both derive from the **same** `sum_known = Σ slice.total_shares over pie_wedge
slices`. A holder who appears in two pie-wedge slices is summed twice.

GME, live dev (post-#1638, both CIKs now `0001767470`):

| slice | source | shares | pct |
|---|---|---|---|
| insiders → Cohen Ryan | form4 | 38,347,842 | 8.55% |
| blockholders → Cohen Ryan | 13d | 36,847,842 | 8.21% |
| **concentration** | | | **48.37% known / 51.63% public** |

Cohen is one person holding ~38M shares, reported through two filing channels —
double-counted. Truthful: **40.16% known / 59.84% public**.

### The falsified premise behind today's behavior

The current split (rollup.py:1374-1390, `_dedup_within_source` for 13D/G kept
**parallel** to the Form 4 priority chain) is the #837/#788-P0b "beneficial ≠
direct, show both" design. Its justification (proposal `ownership-full-
decomposition.md:149-154`) was that Cohen's 13D reports **~75M beneficial**
(RC Ventures + family) vs **~38M direct** Form 4 — "different facts, both worth
surfacing."

**The live data falsifies this.** Cohen's 13D beneficial = 36.85M ≈ his Form 4
38.35M. They are the *same stake* through two lenses, not a 38-vs-75M divergence.
So "show both as additive wedges" *is* the double-count. This spec supersedes
that posture and records the supersession.

## The single rule (canonical)

Authority: the SEC proxy statement **"Security Ownership of Certain Beneficial
Owners and Management"** table — the gold-standard solved form of this exact
problem. It lists every 5%+ beneficial owner *and* every director/officer in one
table; a director who is also a 5% holder appears on **one line** at their
**total beneficial ownership**, footnotes reconcile the rest. No mainstream
provider (Nasdaq, WSJ, Yahoo proxy data) double-counts such a holder.

> **One beneficial owner (Rule 13d-3), keyed by identity (CIK), counted exactly
> once, at their total beneficial ownership, classified by WHO they are
> (deterministic role precedence) — never by which form they filed or how the
> name string reads.**

This is the same rule that fixes the older "insider shows up in institutions
because the name looked institutional" class of bug: both were
classification-by-source/name-string instead of by stable-identity-then-role.

### Operationalized

1. **Identity** = `_identity_key(filer_cik, filer_name)` — CIK when present,
   `LOWER(TRIM(name))` fallback (existing helper, rollup.py:343). Person-level:
   does **not** include `ownership_nature` (we are collapsing the same person
   across channels, not splitting their natures).

2. **Two filing concepts — do not conflate them:**
   - **Beneficial-restatement sources** `{form4, form3, def14a, 13d, 13g}` — each
     is an estimate of the *same* total beneficial stake (Rule 13d-3).
     - **Within-source aggregation depends on whether the natures ADD or
       OVERLAP** (Codex ckpt-2 F3). Only `form4` / `form3` are *additive*:
       `direct` + `indirect` are separate Section-16 holdings that SUM to the
       total (#905 JPM rule). Every other source's multiple nature rows
       *overlap* — DEF 14A `beneficial` vs `voting` are the same shares through
       two lenses (the `ownership_*_current` PK admits both), a 13D/G is one
       beneficial figure, a 13F is one economic position — so the owner's
       per-source figure is the **MAX** of those rows, never their sum.
       Amendments are already superseded by the existing within-source dedup.
     - `beneficial_total` = **MAX** over these sources of (Σ within source). MAX
       (not SUM) because they overlap — DEF 14A beneficial ≈ Form-4 Section-16
       total ≈ 13D beneficial. Keeps the fullest faithful figure (honors #837's
       intent — never discard the larger beneficial number). **This also corrects
       a latent double-count in today's insiders slice**, where a director with
       both a Form 4 and a matched DEF 14A row has the two summed.
   - **Managed-economic source** `13f` — shares an institutional manager has
     investment discretion over (often *clients'* assets). A **different concept**
     from personal beneficial ownership; it must NOT be MAX'd into an insider's
     `beneficial_total`. `managed_total` = Σ the owner's `13f` `Holder` rows.

3. **Role + figure (deterministic precedence):**
   1. owner has any `{form4, form3, def14a}` → category **insiders**,
      `figure = beneficial_total`. Any `13f` for this CIK → `dropped_source` only
      (managed assets, never added to an insider's stake).
   2. else owner has `13f` → category **institutions**/**etfs**
      (by the owner's largest `13f` row's `filer_type`: `etfs` if `== "ETF"`,
      else `institutions`), `figure = MAX(managed_total, beneficial_total)`
      (a 13F manager that also filed a passive 13G/D — same book, count once at
      the larger).
   3. else owner has only `{13d, 13g}` → category **blockholders**,
      `figure = beneficial_total`.

4. **`figure` is produced by exactly one source** (the arg-max). Keep that
   source's `Holder` row(s) (preserves Form 4 direct+indirect display); **every
   other source for that owner → `dropped_sources`** on the largest kept row
   (merged with any pre-existing dropped sources) — never a second wedge.

GME result: Cohen once in **insiders @ 38.35M** (max = his form4; 13d → his
`dropped_sources`); **blockholders wedge empties** (he was its only member — GME's
only 5%+ individual is an insider, which is *correct*). `sum_known`
180.19M → known **40.16%**, public **59.84%**. Pie wedges now mutually exclusive
and sum to 100%.

## Implementation

One new pure function + a 3-line rewire in `compute_rollup`. Residual,
concentration, coverage, and banner are **untouched** — they read slice totals,
which become correct automatically. `build_rollup_csv` gets a **small addition**
(below) so the audit trail survives.

### CSV audit (Codex F5)

Today `build_rollup_csv` emits one row per surviving holder and the sum invariant
`treasury + residual + Σ pie-wedge holders == shares_outstanding` is **broken**
for any dual-channel owner (Cohen counted twice → sum > outstanding). After
reconciliation the invariant **holds** (Cohen once). But the losing filing (his
13D) is now a `dropped_source`, so it disappears from the CSV. To keep the audit
trail, emit each holder's `dropped_sources` as trailing `__dropped:<source>__`
memo rows — excluded from the sum, mirroring the existing `__memo:<category>__`
funds rows. A spreadsheet consumer can still see "Cohen also filed 13D @36.85M"
and still get a clean `SUM(shares)` reconciliation.

### New: `_reconcile_owner_once(holders) -> dict[SliceCategory, list[Holder]]`

Input = combined `survivors + blockholders` (all pie-wedge `Holder`s, already
deduped per-(cik,nature) by the existing two passes). Output = per-category
holder lists ready for `_build_slice`.

```text
BENEFICIAL = {"form4", "form3", "def14a", "13d", "13g"}
group holders by _identity_key(h.filer_cik, h.filer_name)   # person-level, no nature
for each owner group:
    by_source[s]    = [h in group if h.winning_source == s]
    src_total[s]    = Σ shares in by_source[s]               # direct+indirect add; amendments already superseded
    present         = set(by_source)
    bene_sources    = present & BENEFICIAL
    bene_max_source = argmax over bene_sources of src_total  # tie-break: _PRIORITY_RANK asc; None if no bene source

    # role precedence -> category + the source that produces `figure`
    if present & {"form4", "form3", "def14a"}:
        category   = "insiders"
        figure_src = bene_max_source                         # 13f (if any) is managed assets -> dropped only
    elif "13f" in present:
        biggest_13f = max(by_source["13f"], key=shares)
        category    = "etfs" if (biggest_13f.filer_type or "").upper() == "ETF" else "institutions"
        # 13F manager that also filed a passive 13G/D -> same book, count once at the larger
        figure_src  = "13f" if (bene_max_source is None or src_total["13f"] >= src_total[bene_max_source]) else bene_max_source
    else:                                                    # only 13d/13g
        category   = "blockholders"
        figure_src = bene_max_source

    keep    = by_source[figure_src]                          # 1+ rows (form4 direct+indirect preserved)
    losers  = [h in group if h not in keep]
    primary = max(keep, key=shares)                          # largest kept row carries the drilldown
    primary.dropped_sources = merge(primary.dropped_sources, [DroppedSource(h) for h in losers])
    emit `keep` rows under `category`
```

- **Single-source owners** (the overwhelming majority) → one source present →
  `keep` = all their rows, no losers, category == today's bucket. **Output
  identical to current behavior.** Only **multi-channel** owners change — and that
  now includes a director with both a Form 4 and a matched DEF 14A row (one owner,
  two sources → reconciled, fixing the latent insiders double-count), not only the
  insider+blockholder case.
- The reconciled holder's `category` is role-driven and may differ from its
  `winning_source`'s usual channel (e.g. an institution whose 13G > 13F lands in
  `institutions` with `winning_source == "13g"`). Intended: the headline
  figure/link is truthful to the largest filing; the slice reflects who the owner
  is. The drilldown lists all channels.

### Rewire `compute_rollup` (rollup.py:1386-1397)

```python
block_candidates = [c for c in matched if c.source in ("13d", "13g")]
other_candidates = [c for c in matched if c.source not in ("13d", "13g")]
survivors    = _dedup_by_priority(other_candidates)
blockholders = _dedup_within_source(block_candidates)
by_category  = _reconcile_owner_once(survivors + blockholders)   # NEW: count each owner once
slices = _bucket_into_slices_from_categories(
    by_category, unmatched_def14a, outstanding, funds_holders=funds_holders,
)
```

`_bucket_into_slices` is refactored so its source-routing `for h in survivors`
loop is replaced by consuming the pre-bucketed `by_category` map; the
`unmatched_def14a` and `funds_holders` handling is unchanged.

### Out of scope (deliberate)

- **`unmatched_def14a`** (NULL cik) — excluded from reconciliation. No reliable
  identity; name-key merge risks false collapses. Unchanged.
- **funds (`nport`)** — memo overlay, already excluded from `sum_known`.
  Unchanged.
- **Vanguard institutions multi-CIK double-count** (Group + Capital Mgmt +
  Portfolio Mgmt, *different* CIKs) — that is **#1639** (quarter-mix across a
  corporate restructure), not an identity collapse. This rule keys on CIK so it
  correctly does **not** merge distinct CIKs.

## Tests

Flip the two contract tests that pin the superseded "show both" design, add new
coverage:

- `test_form4_and_13d_both_render_for_same_cik` → **rename/rewrite**
  `test_form4_and_13d_same_cik_counted_once`: Cohen renders **once** in insiders
  @ max(38.35M, 36.85M)=38.35M; **no** blockholders slice; his 13D is a
  `dropped_source` on the insiders row.
- `test_13g_and_13f_both_render_for_same_cik` → **rewrite**
  `test_13g_and_13f_same_cik_counted_once`: Vanguard renders **once** in
  institutions/etfs @ max; 13G (or 13F, whichever smaller) → `dropped_source`;
  **no** independent blockholders row.
- `test_837_repro_*` / `test_837_regression_other_instrument_*` — a pure
  blockholder (13D/G, **no** Form 4 / 13F for that CIK) still surfaces in the
  blockholders slice. Keep (these cases are unaffected by reconciliation).
- `test_def14a_*_routes_to_insiders` (867, 927) — **rewrite** the
  `sources_present == {"form4", "def14a"}` assertions: a matched DEF 14A for the
  same CIK/name as a Form 4 is now reconciled to **one** owner @ MAX, not two
  summed rows. The insiders slice total must equal the single beneficial figure,
  not Form4 + DEF14A. (This is the latent double-count Codex F2 surfaced.)
- **New** `test_concentration_counts_dual_channel_owner_once`: insiders 38.35M +
  blockholders 36.85M (same CIK) + institutions 140.84M / 448.69M outstanding →
  `pct_outstanding_known == 40.16%`, residual public == 59.84% (the GME repro).
- **New** `test_dual_channel_max_keeps_larger_figure`: same CIK form4=30M,
  13d=40M → owner counted once @ 40M in insiders (role precedence) with form4 as
  `dropped_source` (MAX honored).
- **New** `test_form4_def14a_13d_three_way_counted_once`: same CIK form4=38M (Σ
  direct+indirect), def14a=39M, 13d=37M → one insiders holder @ 39M (def14a max),
  form4 + 13d as dropped_sources; insiders total == 39M.
- **New** `test_form4_direct_plus_indirect_preserved_under_reconciliation`: one
  CIK with form4 direct + indirect (two rows) and no other channel → both rows
  survive, slice total = sum (no #905 within-source regression).
- **New** `test_13f_managed_assets_not_added_to_insider`: same CIK with form4=5M
  and a 13f=900M (the F1 pathological case) → insiders @ 5M (beneficial only); 13F
  is a `dropped_source`, **not** added — managed assets never inflate an insider.
- **New** `test_13f_etf_and_non_etf_same_cik_bucket_by_largest`: same CIK with an
  ETF 13F row and a larger non-ETF 13F row → lands in `institutions` (largest 13f
  row drives the bucket), deterministic.
- **New** `test_csv_emits_dropped_source_memo_rows`: dual-channel owner →
  `__dropped:13d__` memo row present; `SUM(shares)` over non-memo rows +
  treasury + residual == shares_outstanding (invariant restored).

Pure-logic tier where possible (extract `_reconcile_owner_once` and table-test
it directly, no DB); the GME repro can be a `_reconcile_owner_once` unit test on
hand-built `Holder`s.

## Verification (DoD clauses 8-12)

- Smoke panel GME (primary) + AAPL, MSFT, JPM, HD on dev `/instruments/{sym}/
  ownership-rollup`: known + public sum to ~100%, no owner in two wedges.
- GME operator figure: concentration 48.37% → **40.16%**, public 51.63% →
  **59.84%**; Cohen once.
- Cross-source: Cohen ~8.5% of GME float (one stake) vs public reporting (~8.4%).
- No backfill / no `sec_rebuild` needed — pure read-path math; effective on next
  endpoint hit after deploy.

## Settled-decision impact

Supersedes the #837/#788-P0b "show both as additive wedges" posture **as
implemented in the rollup math**. Honors #837's *intent* (the larger beneficial
figure is kept, never discarded — it becomes the headline via MAX; the losing
filing is preserved in the drilldown). Completes #840's two-axis intent (loser
preserved as provenance, not a double-counted wedge). Update
`ownership-full-decomposition.md` and the `compute_rollup` comment block to cite
this spec.
