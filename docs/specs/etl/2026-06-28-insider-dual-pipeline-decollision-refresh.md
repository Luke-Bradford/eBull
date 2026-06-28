# Insider dual-pipeline de-collision — relocate #1804 fix B to the `_current` refresh layer (#1805)

Follow-up to #1804 (merged). This does **not** introduce a new ownership
data-treatment rule. It relocates #1804's already-shipped, already-verified
same-accession de-collision (read-path fix B) from the read path to the
`_current` refresh layer, so the stored snapshot no longer carries the redundant
row. **Provably behaviour-equivalent** to current `main` (proof below).

## Problem (stored redundancy only)

The same Form 4/3 accession is written to `ownership_insiders_observations` by BOTH:
- the XML manifest parser — bare-accession `source_document_id`, nature from Table II
  (`direct` / `indirect`); and
- the bulk SEC insider dataset (`sec_insider_dataset_ingest`) — `:NDT:`/`:NDH:`
  marker, nature relabelled from the reporting person's relationship
  (`officer/director→direct`, `10%-owner→beneficial`).

`refresh_insiders_current` projects observations into `_current` with
`DISTINCT ON (holder_identity_key, ownership_nature)`. The two pipelines emit
**different** natures for the same stake, so both survive the DISTINCT ON and land
as two `_current` rows. #1804's read-path fix B already de-collides this at read
(the operator-visible figure is correct), but `_current` storage still carries both.

Dev DB 2026-06-28: **2726 groups / 1508 instruments** (`scripts/dq_audit.py`
dual-pipeline storage sentinel).

## Source rule (unchanged from #1804)

prevention-log 1835 / #905 / Rule 13d-3. For the **same accession**, the dataset's
relationship-derived `beneficial` relabel restates the *same Table-II shares the XML
parse already captured in full* — an overlapping restatement, not a second holding.
#1804 fix B settled the treatment: drop the dataset (`:NDT:`/`:NDH:`) row when an
XML-manifest row shares `(holder, accession)`; the XML parse is the authoritative
full-Table-II view of **that filing**. This spec does not re-derive that rule — it
moves where it is enforced. The **cross-accession** additive-vs-overlap regime
(`MAX(additive_sum, overlap_max)`, fix A in `_source_rows_and_total`) is a separate,
untouched code path that this change does not enter.

## Fix — refresh layer, on the post-DISTINCT-ON winner set

Both refresh functions wrap their DISTINCT-ON projection in a `winners` CTE (the rows
that WILL become `_current`), then drop a `:NDT:`/`:NDH:` winner whenever a
plain-accession (`!~ ':(NDT|NDH):'`) winner exists for the same
`(holder_cik, source_accession)` — `holder_cik IS NOT DISTINCT FROM`, mirroring fix B
exactly. Dropped rows never reach `_current`; existing `_current` collision rows are
deleted by the MERGE's `WHEN NOT MATCHED BY SOURCE → DELETE` on the next refresh.

Identical filter in both refresh functions → extracted to one module SQL constant
(`_INSIDER_DUAL_PIPELINE_DECOLLISION`) applied as `SELECT w.* FROM winners w {filter}`.

### Why the winner set, NOT raw observations (load-bearing)

Fix B keys on `_current` — the **post-DISTINCT-ON survivors**. A pre-DISTINCT-ON
filter on the raw observations is **stricter than fix B**: it also drops a `:NDT:`
row whose same-accession plain sibling exists in observations but **lost its own
`ownership_nature` DISTINCT-ON slot to a newer filing** (so the plain row is in
observations but NOT `_current`). Fix B keeps that `:NDT:` row (no plain sibling in
`_current`) and the rollup counts it. Dropping it changes operator-visible insider
figures in BOTH directions — measured on dev: 506 such rows across hundreds of
instruments, e.g. JPM 9,142,457 → 9,057,482, PFE 2,383,995 → 1,672,336, TM
25,906,827 → 26,029,681 (holders 28 → 18). Filtering the `winners` CTE keyed on the
future-`_current` rows reproduces fix B's drop set exactly (Codex ckpt-2 P2).

### Why refresh, not ingest (conscious tradeoff)

- **Ordering-independent.** The MERGE re-derives from the full current observation
  set each run, so the result is the same whether the dataset or the XML manifest
  landed first. An ingest-time "skip if XML exists" check leaks the redundant row
  whenever the dataset lands first (historical bulk) and the XML manifest lands
  later.
- **No immutable-observation deletion.** `*_observations` stays a complete
  append-only audit trail (both pipelines' rows preserved). The operative storage the
  rollup reads is `_current`; that is what "one authoritative row set" means here.
  Obs-level redundancy is benign (never read directly by the rollup) and is out of
  scope.

### Read path retained

#1804's read-path fix B is **kept** as tested defense-in-depth — after this change it
provably matches nothing new (the rows are gone from `_current`), but both layers are
pinned by a test so neither can silently drift. Fix A (`_source_rows_and_total`) is
untouched.

## Full-population verification (dev DB 2026-06-28)

**1. Behaviour-equivalence (the load-bearing proof).** The set of `_current`
`:NDT:`/`:NDH:` rows the winner-set filter removes is **identical** to the set #1804
fix B drops at read: **3082 == 3082, symmetric difference 0** (the `_current` rows
that have a plain `_current` sibling on the same `(holder, accession)`). Because every
removed row is already read-path-dropped on `main` today, it is counted in **no**
rollup total — so removing it from storage changes no operator-visible figure. This
holds the share-total invariant directly (not via row-existence): the rendered owner
subtotal cannot move when the removed rows were never summed.

**2. Fix A's cross-accession `beneficial` MAX cases are not in the removal set.** Those
`beneficial` rows have no same-accession plain XML sibling (that is why fix B keeps
them and fix A MAXes them), so the `(holder, accession)`-scoped predicate does not
match them — they survive into `_current` unchanged.

**3. Superseded-sibling rows preserved (the Codex ckpt-2 regression).** For a fresh
sample of edge instruments (a `:NDT:` whose plain sibling is superseded out of
`_current`) the full rollup is **identical** before/after a winner-set refresh
(SMCI / LVS / EA). Five instruments mistakenly cleaned by the earlier
observations-keyed prototype (JPM / MRK / PFE / C / TM) were restored by the
winner-set refresh to their **exact** original insider figures.

**4. Key.** The filter mirrors fix B's `holder_cik IS NOT DISTINCT FROM` + accession
on the winner set. `(holder_cik, accession)` and `(holder_identity_key, accession)`
partition the `_current` collision set identically (3082 rows each), so the choice is
immaterial to the removed set.

## Backfill

`scripts/backfill_1805_insider_decollision.py` — re-runs
`refresh_insiders_current_batch` over the instruments whose `_current` carries a
collision (a `:NDT:`/`:NDH:` row with a plain `_current` sibling on the same
`(holder_cik, accession)` — exactly the set the winner-set filter removes).
Idempotent; no re-ingest, observations unchanged. The repair sweep would NOT pick
these up (no new observations → watermark already current), so an explicit forced
refresh is required. Target: sentinel → 0.

## Tests

- `test_ownership_refresh_writer_merge.py`:
  - colliding `:NDT:` + plain pair on one accession → after refresh `_current` holds
    only the plain row; a dataset-only `:NDT:` (distinct holder, no plain sibling) is
    retained; the batch path behaves identically.
  - **regression guard (Codex ckpt-2):** a `:NDT:` `beneficial` whose same-accession
    plain `direct` sibling is superseded out of `_current` by a newer `direct` filing
    is **kept** — pins the winner-set semantics so a future edit cannot silently
    revert to the figure-changing observations-keyed filter.
