# Collapse an owner's additive lots to one display line (#1942)

## Problem

When one Section-16 person holds both a non-zero `direct` and a non-zero
`indirect` lot, `_source_rows_and_total`'s additive path (correctly) returns BOTH
rows so the slice total SUMs them (#905). Those rows flow through
`_reconcile_owner_once` into `_build_slice` and render as **two lines for one
name** in the L1 holder list, with no nature differentiator (the reconciled
`Holder.ownership_nature` is dropped to `None` at the slice level). Reads as a
duplicate even though the lots are legitimately additive and the total is right.

`filer_count` also over-counts these: each lot is a separate `Holder` row, so a
person with direct+indirect counts as 2 filers.

## Source rule

- **data-engineer I14** — "one beneficial owner counted ONCE … classified by
  most-specific role." Honoured today in the *total* but not the *display*.
- **17 CFR 229.403** (Reg S-K Item 403), column (3) "Amount and nature of
  beneficial ownership": the amount is the person's **total** shares beneficially
  owned, determined under Rule 13d-3 (17 CFR 240.13d-3); the instructions warn
  against confusion / double-counting where the same securities have multiple
  beneficial owners. One owner → one aggregated amount.
- **SEC Form 4 General Instruction 4(b)** — direct and indirect beneficial
  ownership are reported on **separate lines** (and different indirect holdings on
  separate lines). This is why `_source_rows_and_total` keeps them as distinct
  additive rows (#905): the split is a reporting artefact of Form 4's line model,
  not two owners. Aggregating them for display at Item-403 total ownership is the
  correct denormalisation, not a data change.

Decision (settled on #1942, not an open operator choice): **Option 1 — collapse
to one line per owner** at the summed shares, preserving the per-lot split in a
drilldown. Option 2 (two lines + nature pills) keeps the visual repetition I14
exists to remove.

## Full-population verification (dev DB, 2026-07-04)

`ownership_insiders_current`, `shares > 0`, grouped `(instrument, holder)` with
`count >= 2`: **9,530** owner-groups render ≥2 non-zero lines today. Examples:
`AA / Oplinger William F` (direct 333,877 form4 + indirect 490 form3),
`AAOI / BLACK RICHARD B` (direct 165,529 + indirect 164,363, both form4). Live
`get_ownership_rollup('AA')` baseline: insiders slice = 41 holders / filer_count
41, with Oplinger / Beerman / Bevan each appearing twice.

### Safety proven on the full population (not a sample)

The collapse runs on the **post-`_reconcile_owner_once`** holder list, and the
inputs that reach it as multi-row same-identity groups are provably safe:

1. **Collapse is gated to the `insiders` slice.** Only the insiders additive path
   produces multi-row same-identity groups routed through `_reconcile_owner_once`:
   institutions/etfs (`figure_src='13f'`) and blockholders (`13d`/`13g`) get a
   single `[rep]` row (those sources are not in `_ADDITIVE_SOURCES`), families are
   injected as one holder. The `funds` and `def14a_unmatched` slices BYPASS
   reconcile and are intentionally one row per fund_series / per proxy nature while
   sharing a filer CIK — e.g. **221,540** dev `(instrument, fund_filer_cik)` groups
   hold ≥2 distinct fund series — so a CIK-first collapse there would wrongly merge
   distinct holders and undercount `filer_count`. `_build_slice` therefore calls
   `_collapse_owner_lots` only when `category == "insiders"` (Codex ckpt-2 HIGH).
2. **Multi-row insider groups are pure additive (`direct`/`indirect`).** Source
   full-pop: multi non-zero-lot groups contain `direct` (9,324) + `indirect`
   (9,180) + `beneficial` (647) rows, but `_source_rows_and_total`'s additive
   branch folds every `beneficial` (overlap) row into the primary lot's
   `dropped_sources`; when overlap instead wins it returns a **single** rep. So a
   `beneficial` row never surfaces as a second display line. Verified on the
   reconciled output: `GBFH / Griege` → 2 additive display rows, the `beneficial`
   folded to `dropped_sources` (not a 3rd line); `PLTK / Alpha Frontier` → 1 rep.
3. **No new merge risk vs `_reconcile_owner_once`.** `_collapse_owner_lots` groups
   by the SAME `_identity_key` reconcile already grouped by, so two same-key rows
   in `_build_slice` necessarily came from ONE reconcile group — the collapse can
   never merge two distinct owners that reconcile kept apart. NULL-CIK same-name
   collision is a pre-existing reconcile property, not introduced here — and is
   empirically empty: **0** of the 9,530 multi-lot groups have a non-`CIK:`
   identity key.

## Design

Collapse **at the display chokepoint** (`_build_slice`), NOT in
`_source_rows_and_total` / `_reconcile_owner_once` — those must keep the per-lot
rows so the additive SUM + per-source provenance (and #1941 Form 3/4 pooling)
stay correct.

1. New `HolderLot` frozen dataclass: `ownership_nature`, `shares`, `source`,
   `accession_number`, `edgar_url`, `as_of_date`. Display-only; NOT additive.
2. `Holder` gains `lots: tuple[HolderLot, ...] = ()`. Empty for single-row owners.
3. `_collapse_owner_lots(holders)` — group by `_identity_key(cik, name)` (the SAME
   key `_reconcile_owner_once` grouped by, so every same-key row here is one
   owner's lot, never two distinct owners). Single-row groups pass through
   untouched. Multi-row groups → one `Holder` at `shares = Σ lots`, representative
   = the max-shares row (keeps its source/accession/url/as_of/dropped_sources —
   the max row is the one `_reconcile_owner_once` stamped cross-source provenance
   on), `lots` = the constituent rows sorted by shares desc.
4. Call it in `_build_slice` after the `> 0` filter. **Compute `sources` /
   `dominant_source` from the PRE-collapse rows** (each lot keeps its own
   `winning_source`), so the slice source-mix / `dominant_source` is unchanged by
   the collapse — attributing a Form 3 indirect lot's shares to the Form 4
   representative source would otherwise skew the mix (Codex ckpt-1 HIGH). Then
   collapse for the display holder list, `total`, `filer_count`, enriched rows.
   `total` is unchanged (sum of the same shares). `filer_count` now counts the
   collapsed owner once (correct per I14).
5. The enriched-`Holder` rebuild must carry `lots=h.lots` (it explicitly lists
   kwargs and would otherwise drop the field).
6. `_slice_coherence` gathers each `lot.as_of_date` too (a person's direct-on-
   Form4 + indirect-on-Form3 can span quarters; the envelope must not hide it).

### What NOT to do (from the #1942 decision + prevention log)

- Do NOT fold lots into `family_members`: `filer_count = sum(len(h.family_members)
  or 1 …)` (ownership_rollup.py:2446) would count one person's two lots as 2
  filers. A single person's direct+indirect = ONE filer.
- Do NOT fold lots into `dropped_sources`: that field means "channel NOT counted
  in this figure"; the additive lots ARE counted, so the FE would mislabel them
  as superseded.

## API + FE

- `_HolderModel` gains `lots: list[_HolderLotModel] = []`, serialized in
  `_ownership_rollup_response` mirroring `family_members`.
- `frontend/src/api/ownership.ts`: `OwnershipHolder.lots?`, new `OwnershipLot`.
- `OwnershipPage.tsx`: `FilerRow.lots?`; `rollupToFilerRows` maps them; the name
  cell renders a `<details>` drilldown (same pattern as `family_members`) showing
  each lot's nature + shares + source when present, so the top row's
  representative source/accession is not read as sourcing the whole aggregate
  amount (Codex ckpt-1 MED). A holder can carry `family_members` XOR `lots` (an
  institutional family rep never has additive insider lots); render whichever is
  present.

## Scope / risk

Read-path display change only. No migration, no data write, no `sec_rebuild`, no
daemon restart (API is uvicorn `--reload`). Figure-neutral on slice
`total_shares` / `pct` / residual / concentration; `filer_count` decreases for
the ~9,530 affected groups (correct direction — de-duplicates over-counted
filers). L1 holder-list row count drops for ~2,833 instruments with multi-lot
insiders; FE-QA the ownership card top-to-bottom.

## Tests

Pure-logic (no DB): `_collapse_owner_lots` — (a) two additive lots collapse to one
Holder at summed shares with a 2-entry `lots`; (b) single-row owner passes through
with empty `lots`; (c) two distinct NULL-CIK owners sharing neither CIK nor name
do NOT merge; (d) representative keeps the max-shares row's provenance +
`dropped_sources`. `_build_slice` — filer_count counts a collapsed owner once;
`total` AND `dominant_source` unchanged vs pre-collapse. `_slice_coherence`
includes lot as_of dates. API: a serialization test proving `lots` survives
`_ownership_rollup_response`. FE: a `rollupToFilerRows` test proving lots map
through with source/as-of and the drilldown renders.
