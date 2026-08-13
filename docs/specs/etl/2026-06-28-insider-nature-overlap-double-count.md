# Insider `ownership_nature` overlap double-count (#788 / #1764 residual)

## Problem

A Section-16 owner's `beneficial` figure is summed on top of the same owner's
`direct` + `indirect` Form 4/3 holdings in the operator-visible ownership pie,
double-counting one beneficial position.

Concretely (RNTX, instrument 1050230, accession `0001019231-24-000026`):

| nature | source | source_document_id | shares |
|--------|--------|--------------------|--------|
| direct | form4 | `0001019231-24-000026` (plain) | 1,746,549 |
| beneficial | form4 | `0001019231-24-000026:NDT:7168180` | 1,746,549 |

Both rows are UTIMCO (CIK 0001019231), the same accession, the same share
count. The rollup renders the owner **twice** at 1,746,549 (= 3,493,098), an
exact 2× overcount of one position.

### Root cause — two ingest pipelines, two `ownership_nature` vocabularies

`ownership_insiders_current` is MERGE-keyed on `ownership_nature`
(`app/services/ownership_observations.py:308` etc.), so two rows for one owner
under different natures coexist. Two pipelines populate it for the **same**
Form 4 accession under **different** nature taxonomies:

- **XML manifest parser** (`insider_transactions.py:1567`): nature from Form 4
  Table II **ownership form** — `direct` (D) / `indirect` (I). `source_document_id`
  = the plain accession.
- **Bulk SEC insider dataset** (`sec_insider_dataset_ingest.py:_map_relationship`,
  line 184): nature from the reporting person's **relationship** —
  officer/director → `direct`, ten-percent-owner → `beneficial`.
  `source_document_id` = `<accession>:NDT:<rownum>`.

The rollup's per-owner subtotal (`_source_rows_and_total`,
`ownership_rollup.py:1216`) treats **all** natures under an additive source
(`form4`/`form3` ∈ `_ADDITIVE_SOURCES`) as additive and **sums** them. The
dataset's `beneficial` row is therefore added to the XML `direct`/`indirect`
rows instead of being recognised as a restatement of the same stake.

## Source rule

Settled, already in the repo — `docs/review-prevention-log.md` line 1835 (#905 /
Rule 13d-3):

> Aggregation has TWO regimes that must not be conflated: **additive** natures
> (Form 4 / Form 3 `direct` + `indirect` are distinct Section-16 holdings → SUM,
> the #905 rule) vs **overlapping** restatements (DEF 14A beneficial/voting, a
> 13D/G beneficial figure, a 13F economic position, and the same owner's figures
> across channels → all the SAME shares → MAX, never SUM)… check it BOTH within a
> source's nature rows and across sources for the same identity.

SEC basis: Form 4 Table II reports "Amount of Securities Beneficially Owned
Following Reported Transaction(s)" per **ownership form** (Direct/Indirect) —
the additive Section-16 holdings. The reporting person's **relationship**
(Rule 16a-1 officer/director/10%-owner) is an orthogonal dimension that does
**not** create an additional holding. A `beneficial` row is the owner's total
beneficial figure (an overlapping restatement), never a third additive lot.

The current code honours the regime at the **source** level but not the
**nature** level — exactly the "check it BOTH within a source's nature rows"
clause it misses.

### Why `beneficial` is never a separate additive lot

The bulk dataset row carries its own `DIRECT_INDIRECT_OWNERSHIP` flag, but the
ingester derives `ownership_nature` from the reporting person's **relationship**
flags (`_map_relationship`), not from D/I, and writes the per-transaction
`SHRS_OWND_FOLWNG_TRANS`. So a 10 %-owner's directly-held lot is **relabelled**
`beneficial` purely because of the filer's relationship — it is the same
post-transaction holding the XML path records as `direct`/`indirect`, viewed
through the relationship lens. It is therefore an overlapping restatement of the
owner's beneficial total (Rule 13d-3), never a third additive lot. `MAX`, not
`SUM`, is the rule-defined treatment.

## Full-population verification (dev DB, 2026-06-28)

`ownership_insiders_current`, same `(instrument, accession, shares)`, one CIK,
≥2 natures:

- **1,581** dup groups (all magnitudes). Nature-pair split:
  `(direct, indirect)` 1,071 · `(beneficial, direct)` 363 · `(beneficial, indirect)` 147.
- **1,336 / 1,581** are **dual-pipeline** (both a plain-accession doc AND an
  `:NDT:` doc present) — the double-ingest collision. 245 are plain-only
  (XML's own genuine direct+indirect two-axis rows); 0 NDT-only.
- Operator-visible (rendered through `get_ownership_rollup`, insider slice, a
  residual same-`(accession, shares)` holder pair) across the 253 audit-candidate
  instruments: **65 groups / 46 instruments**, **every one** same-CIK
  cross-nature (Type B). **Zero** were distinct-CIK control groups — `#1764`'s
  same-accession pass already collapses 100 % of those on the read path, so the
  `scripts/dq_audit.py` "317 LIVE" distinct-CIK figure is a **false alarm** (it
  scans the raw write-through table and models only `#1652`'s 1 M floor, not
  `#1764`'s no-floor same-accession collapse).

### Direction of the fix is monotone (full population, all magnitudes)

Same-`(instrument, cik, accession)` form4/3 groups carrying **both** an additive
(`direct`/`indirect`) and an overlapping (`beneficial`) nature: **1,206** groups.
`MAX(additive_sum, overlap_max)` vs the current `additive_sum + overlap_max`:

| relation | groups | new owner subtotal |
|----------|--------|--------------------|
| `over == add_sum` | 871 | `add_sum` (drops the doubled restatement) |
| `over < add_sum` | 141 | `add_sum` (additive rows are the fuller figure) |
| `over > add_sum` | 194 | `over` (Rule 13d-3 total; XML under-captured) |

In **all 1,206** cases the new subtotal `MAX(add_sum, over)` is **strictly less
than** the current `add_sum + over`, so the change can only **remove or hold**
counted shares, never add — it cannot inflate any wedge. The 194 `over > add_sum`
cases take the larger `beneficial` figure, which is the Rule 13d-3 total
beneficial ownership (and still < the current sum). This grounds the
`direct(100)+beneficial(250)->250` rule on the full population, not an example.

## Fix (read-path, no stored-data change)

Match the existing read-path reconciliation pattern (#1640 / #1652 / #1764 —
collapse at read time, never mutate storage).

1. **Propagate `ownership_nature` onto `Holder`** (currently dropped at
   `_dedup_by_priority`). New defaulted field `ownership_nature:
   OwnershipNature | None = None`; set only on the form4/form3 candidate→Holder
   paths. Frozen-dataclass `replace()` calls preserve it.

2. **Make `_source_rows_and_total` nature-aware for additive sources.** Split an
   owner's rows for an additive source into:
   - additive natures `{direct, indirect}` → SUM (preserves #905),
   - overlapping natures `{beneficial, voting, economic}` → MAX.
   Owner subtotal = `MAX(additive_sum, overlap_max)`. Display rows = the
   additive rows when they dominate; else the single overlap representative
   (dataset-only owners with no XML rows). The subsumed overlap row is **folded
   into the kept rep's `dropped_sources`** (not silently dropped) so the `:NDT:`
   dataset provenance stays auditable — the existing read-path passes
   (#1652/#1764) preserve folded members the same way. Non-additive sources
   (`def14a`/`13d`/`13g`/`13f`) are unchanged (already MAX via the existing
   `else` branch).

3. **`scripts/dq_audit.py`** — stop crying wolf: re-point the insider check from
   the already-`#1764`-collapsed distinct-CIK signature to the genuine same-CIK
   cross-nature overlap signature this fix addresses, so the feeder tracks the
   real residual.

### Why read-path, not ingest

The dataset/XML redundancy (same accession written by both pipelines) is a
latent storage smell, but the nature-vocabulary mismatch would remain a trap
even after de-duping ingest, and an ingest fix needs a backfill + carries
parser risk. The read-path regime fix is correct regardless of how many
pipelines wrote, reversible, and immediately operator-visible. Ingest
de-duplication is filed as a follow-up.

## Tests

Pure-logic table tests on `_source_rows_and_total` / `_reconcile_owner_once`:

- form4 `direct`(100) + `indirect`(50) → 150 (additive, #905 preserved).
- form4 `direct`(100) + `beneficial`(100) → 100 (overlap MAX, the bug).
- form4 `beneficial`(200) only → 200 (dataset-only owner).
- form4 `direct`(100) + `beneficial`(250) → 250 (overlap exceeds; XML under-captured),
  with the additive `direct` row preserved in `dropped_sources`.
- form4 `direct`(100) + `indirect`(50) + `beneficial`(120) → MAX(150, 120) = 150.

Read-path test through the **lossy candidate→Holder boundary** (Codex ckpt-1):
build `_Candidate` rows mirroring the RNTX collision (one `direct` + one
`beneficial`, same cik/accession/shares) and assert
`_dedup_by_priority` → `_reconcile_owner_once` yields ONE insiders contribution
at the single share value — not a hand-built `Holder` set, so the
`ownership_nature` propagation through `_dedup_by_priority` is actually exercised.
- form4 `direct`(100) + `beneficial`(250) → 250 (overlap exceeds; XML under-captured).
- form4 `direct`(100) + `indirect`(50) + `beneficial`(120) → MAX(150, 120) = 150.

## Verification (DoD 8–11) — executed 2026-06-28

- **Full-population render** (`get_ownership_rollup` over all 253 audit-candidate
  instruments, residual same-`(accession, shares)` insider holder pairs):
  **65 → 0** dual-pipeline collisions. The only 2 remaining same-`(accession,
  shares)` pairs (FWRD) are genuine **same-pipeline** XML `direct`+`indirect`
  lots (#905-correct SUM, not a collision; 48 such groups exist population-wide).
- **Both fixes exercised:** B (pipeline de-collision) removes 2,054 dual-pipeline
  collisions at the candidate read; A (nature regime) does independent work on
  646 owners with a B-surviving dataset `beneficial` row + a `direct`/`indirect`
  row.
- **Live API** (`/instruments/{sym}/ownership-rollup`, uvicorn `--reload`):
  affected MANE, PVLA + default-panel AAPL, JPM all render insider slices with
  0 residual pairs. MANE `CHILDS JOHN W` now renders once at 294,117 (was 2×).
- **#905 regression:** `test_form4_direct_plus_indirect_preserved_single_source`
  + `test_nature_direct_plus_indirect_sums` green — direct+indirect still SUM.
- Gates: ruff + pyright clean; fast tier 4688 passed; smoke 135 passed; db-tier
  ownership suites (owner-reconcile, drillthrough, observations, rollup, 13f-nt,
  sweep) 95 passed.
- No backfill (read-path only, raw storage unchanged); no daemon restart (no
  jobs/parser change).

## Follow-ups (not in scope)

- Ingest redundancy: the bulk dataset and XML manifest both write the same
  accession (2,723 raw-storage collision groups). The read path now de-collides,
  but ingest could skip the dataset write when the manifest covers an accession.
- 48 same-pipeline XML `direct`+`indirect` rows at an exactly-equal share value
  (e.g. FWRD) — #905 says SUM, but exact equality may hint at a latent XML
  Table-II parse artifact worth a separate look.
