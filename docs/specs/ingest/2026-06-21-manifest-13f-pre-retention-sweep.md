# Extend the manifest pre-retention sweep to 13F-HR (#1703 PR1)

**Status:** spec
**Issue:** #1703 (prerequisite — the sweep #1703's premise missed). Refs #1686 (the sweep this extends).
**Scope:** one entry added to `gated_cutoffs()` + docstring/comment correction + tests. No parser change, no schema change, no new job.

## Problem

`sec_manifest_worker` dispatches ~52 `sec_13f_hr` rows/tick (it dominates the
Phase-B global-oldest top-up). Full-population measurement on dev
(2026-06-21):

| metric | value |
|---|---|
| 13F pending backlog | 190,017 |
| 13F parsed vs tombstoned, last 2h | **42 vs 1,354** |
| out-of-retention pending (`filed_at < 2024-06-30`) | **121,641** |
| in-retention infotables actually fetched/tick | **~2** |

So ~96% of dispatched 13F are **out-of-retention**: the worker fetches
`index.json` + `primary_doc.xml` (both via the #1700 concurrent prefetch),
parses the primary, then the post-parse `thirteen_f_within_retention(period_of_report)`
gate ([sec_13f_hr.py:332](../../../app/services/manifest_parsers/sec_13f_hr.py#L332))
tombstones the row — **after** burning two SEC fetches. 121,641 rows × 2
fetches at the 10-req/s client throttle is the per-tick wall, not the
infotable (#1703's stated premise — falsified).

#1686 built the bulk **pre-retention sweep** to tombstone exactly this class
of row via one SQL pass (zero HTTP) for `form4`/`form5`/`13d`/`13g`, but
**excluded 13F** ([manifest_pre_retention_sweep.py:29-32](../../../app/services/manifest_pre_retention_sweep.py#L29-L32))
on the reasoning *"a `filed_at` bulk sweep would wrongly tombstone in-scope
filings whose period is recent"* — i.e. it treated `filed_at` and
`period_of_report` as independent. They are not.

## Source rule

- **SEC Rule 13f-1(a) (17 CFR 240.13f-1(a))**: a 13F is filed *within 45 days
  after the end of the calendar quarter it reports*. Therefore
  **`filed_at >= period_of_report`** for every 13F-HR/A (you cannot file a
  quarter's holdings before that quarter ends).
- **Our retention invariant** (`thirteen_f_within_retention`,
  `THIRTEEN_F_HR_RETENTION_QUARTERS = 8`,
  [institutional_holdings.py:97-158](../../../app/services/institutional_holdings.py#L97-L158)):
  in-retention ⟺ `period_of_report >= thirteen_f_retention_cutoff()`
  (today = 2024-06-30).

Compose the two:
> `filed_at < thirteen_f_retention_cutoff()` ⟹ `period_of_report <= filed_at < cutoff` ⟹ `period_of_report < cutoff` ⟹ **out-of-retention**.

So sweeping `pending` 13F with `filed_at < thirteen_f_retention_cutoff()` is a
**provably-safe subset** of what the parser's period gate would tombstone —
**zero false tombstones**. `thirteen_f_retention_cutoff()` is the maximum safe
`filed_at` cutoff (any larger value could catch an in-retention boundary
filing); using the period cutoff directly also keeps the sweep coupled to the
parser's retention constant (single source of truth — change
`THIRTEEN_F_HR_RETENTION_QUARTERS` and both move together).

### Distinction from the 13F-NT period-vs-filed rule (prevention-log, I15)

The "model by `period_end`, never `filed_at`" rule (#1639) governs comparing
**two filings'** period ordering across amendments — a late-filed `13F-NT/A`
for an old quarter has a later `filed_at` than a live HR, so a filed-time
comparison would wrongly suppress the live HR. This sweep is **not** that: it
uses `filed_at` only as a conservative **lower bound on a single filing's own
period** (`filed >= period`), never to order two filings, never to suppress a
live snapshot. The parser's `period_of_report` gate remains the sole
retention authority; the sweep only pre-empts the fetch for rows that gate
would **provably** tombstone. The residual boundary band (filed in
`[cutoff, cutoff+45d)` for a pre-cutoff quarter — ~15k rows) is left to the
parser, unchanged.

## Full-population verification (dev DB, 2026-06-21)

The `filed >= period` premise, checked on the full parsed population:

```sql
SELECT count(*) FROM institutional_holdings_ingest_log l
  JOIN sec_filing_manifest m USING (accession_number)
 WHERE l.period_of_report IS NOT NULL
   AND (m.filed_at AT TIME ZONE 'UTC')::date < l.period_of_report;
-- violations = 0   (over 62,374 filings; min(filed - period) = 0 days, p50 = 42 days = the Rule 13f-1 45-day window)
```

Zero filings violate `filed >= period`. The sweep cannot tombstone an
in-retention 13F.

## Change

`app/services/manifest_pre_retention_sweep.py`:

1. `gated_cutoffs()` gains `"sec_13f_hr": thirteen_f_retention_cutoff` (lazy
   import from `app.services.institutional_holdings`, matching the existing
   lazy-import style that dodges the `insider_transactions ↔ manifest_parsers`
   cycle).
2. Module docstring: move `sec_13f_hr` out of the EXCLUDED block into the
   gated list, with the `filed >= period` rationale + the SEC Rule 13f-1
   citation. `sec_10q`/`sec_10k`/`sec_form3`/`sec_def14a` STAY excluded —
   they have **no parser retention gate at all**, so there is no out-of-
   retention tombstone to pre-empt and a `filed_at` sweep there would destroy
   data the parser keeps (the genuine silent-data-loss case).
3. `_sweep_one_source` docstring line 162: generalise the "matches the parser
   gate" claim — for 13F the `filed_at` cutoff is a provably-safe *subset* of
   the parser's `period_of_report` gate (`filed >= period`), not a literal
   match.

No change to `_sweep_one_source`'s SQL (it is source-agnostic; 13F is
filer-scoped, `subject_type='institutional_filer'`, but the sweep filters on
`source` + `filed_at` only — `pending` rows, `FOR UPDATE SKIP LOCKED`,
idempotent `tombstoned→tombstoned` race-absorbed exactly as for the other
sources).

## Why this is safe / reversible

- Tombstone is the soft-delete (invariant I6); `POST /jobs/sec_rebuild/run`
  re-pends if the cutoff later widens (invariant I9).
- **No operator-visible figure changes.** Out-of-retention 13F are never in
  `ownership_institutions_current` (the parser would tombstone them); the
  sweep only flips manifest state, never deletes an observation. Ownership
  rollups are unaffected.
- Pure pipeline-efficiency: removes wasted fetches; the genuine in-retention
  drain is untouched.

## Acceptance / dev-verify

1. Branch deployed to the dev jobs daemon.
2. `POST /jobs/sec_manifest_pre_retention_sweep/run` → expect ~121,641
   `sec_13f_hr` tombstoned, 0 errors; pending 13F drops ~190k → ~68k.
3. Excluded sources (`10q`/`10k`/`form3`/`def14a`) untouched by the sweep.
4. Ownership rollup unchanged for the panel (AAPL/GME/MSFT/JPM/HD).
5. Re-measure manifest-worker tick `processed_by_source` + durations to
   characterise the now-all-in-retention 13F regime — input to #1703 PR2
   (3-phase prefetch + per-source budgeting + max_rows raise).

## Out of scope (→ #1703 PR2)

3-phase concurrent infotable prefetch, per-source row budgeting, and the
`max_rows` raise. Those bound the *in-retention* infotable cost per tick and
are calibrated on the post-sweep measurement from step 5.
