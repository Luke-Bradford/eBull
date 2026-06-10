# Insider observations `filed_at` semantics (#899)

Status: plan for review.

## Problem (verified 2026-06-10)

`ownership_insiders_observations.filed_at` carries MIXED semantics today — worse than
issue #899 describes:

- Write-through Form 4 (`app/services/insider_transactions.py:1445`):
  `filed_at = txn_date @ midnight UTC` (trade date).
- Write-through Form 3 (`app/services/insider_form3_ingest.py:418`):
  `filed_at = as_of @ midnight UTC`.
- Legacy batch sync (`app/services/ownership_observations_sync.py:275/331`): same two
  wrong derivations (reads typed tables, which carry NO filing timestamp — only
  `txn_date` / `as_of_date` + `accession_number`).
- Bulk insider dataset (`app/services/sec_insider_dataset_ingest.py:544`):
  `filed_at = FILING_DATE` from the SEC dataset — the TRUE filing date.

So rows disagree on what the column means depending on which path wrote them.
Institutions / blockholders / treasury observations all use true filing time —
insiders are the outlier.

## Decision: fix in place (no new column), coordinated backfill

Issue #899 offers rename-or-add-accepted_at. Neither is needed:

- The trade/as-of date is NOT lost — it is already the natural key's `period_end`
  (`period_end=txn.txn_date` / `as_of`), and `txn_date` lives on the typed rows.
- `filed_at` is NOT in the conflict identity
  `(instrument_id, holder_identity_key, ownership_nature, source, source_document_id, period_end)`
  (sql/113, recorder `ownership_observations.py:201`) — rewriting it is a pure UPDATE,
  no identity/current-row drift, which was the #894-era blocker for a write-through-only
  change. Coordinated backfill is exactly what removes that blocker.
- Bulk-dataset rows already carry the target semantics.

## Changes

1. **Writers** — thread the true filing timestamp:
   - Form 4/5 manifest parser (`manifest_parsers/insider_345.py`) already holds
     `row.filed_at` (manifest, NOT NULL in practice) — pass it through
     `insider_transactions.py` ingest into `record_insider_observation(filed_at=...)`.
     Fallback when unavailable: keep `txn_date @ midnight` (defensive, logged).
   - Form 3 (`insider_form3_ingest.py`): same — use the manifest/submissions-index
     `filed_at` threaded into the ingest; fallback `as_of @ midnight`.
   - Legacy sync (`ownership_observations_sync.py`): JOIN
     `sec_filing_manifest.filed_at` (canonical) by accession with COALESCE fallback to
     the current derivation. (Sync is the retro/repair path; it must not resurrect
     trade-date semantics on a manual run.)
2. **Backfill** — migration `sql/<next>_backfill_insider_observations_filed_at.sql`:
   `UPDATE ownership_insiders_observations o SET filed_at = m.filed_at FROM
   sec_filing_manifest m WHERE m.accession_number = o.source_accession AND
   o.filed_at IS DISTINCT FROM m.filed_at` (scoped `source IN ('form4','form3')`).
   Dev scale: 707,184 rows (679k form4 / 28k form3) — bounded one-time UPDATE.
   Rows whose accession is absent from the manifest keep the old value (COALESCE-free
   inner join = untouched) — logged count in PR via pre/post probe.
3. **Downstream readers** — no query-shape changes required:
   - `refresh_insiders_current` MERGE + `ownership_history.py:114` use `filed_at DESC`
     as tie-break — semantics IMPROVE (true filing order; late-filed amendments now
     order correctly). `_current` rows refresh via the post-backfill repair sweep /
     normal drift detection (`ownership_refresh_state` watermark vs `ingested_at` —
     NOTE: plain UPDATE does not advance `ingested_at`; see Open question 1).
   - Frontend: verify which surfaces render insider `filed_at`; relabel only if a label
     says "trade date".

## Codex ckpt-1 resolutions (2026-06-10 — fix-in-place CONFIRMED sound)

1. **_current propagation = recompute, NOT mirror-update** (Codex HIGH): backfilled
   `filed_at` is the MERGE tie-break (`ownership_observations.py:289`) — it can flip
   the WINNING observation, not just its timestamp; a mirror UPDATE would leave stale
   `source_document_id`/`shares` on `_current`. Resolution: post-backfill operator
   step runs `refresh_insiders_current_batch` over every instrument with a rewritten
   row (drift sweep alone won't fire — plain UPDATE doesn't advance `ingested_at`).
2. **Writer chain must carry the param** (Codex HIGH): `filed_at` threads through
   `upsert_filing(...)` → `_record_form4_observations_for_filing(...)` and
   `upsert_form_3_filing(...)` → `_record_form3_observations_for_filing(...)`
   (`insider_345.py:286/486/719`, `insider_transactions.py:1104`,
   `insider_form3_ingest.py:81`) — manifest adapters hold `row.filed_at` but the
   observation writers never receive it today.
3. **Non-manifest callers** (Codex HIGH): legacy schedulers + Form 4/Form 3 rewash
   (`rewash_filings.py:328/393`, `insider_transactions.py:1803`,
   `insider_form3_ingest.py:677`) call `upsert_filing`/`upsert_form_3_filing`
   directly — rewash must look up manifest/filing-event filed time by accession,
   not fall back to txn/as-of.
4. **Form 5 routes through Form 4 plumbing** with `source='form4'`
   (`insider_345.py:543`, `insider_transactions.py:428`) — fixed by the same thread;
   backfill keys on `source_accession` so Form 5 accessions resolve via their own
   manifest rows. Intentional + tested.
5. **Backfill join filters manifest source/form explicitly**; keyed on
   `o.source_accession` (correct across the bulk path's `accn:NDT:*`/`accn:NDH:*`
   `source_document_id` shapes — tests cover both).
6. **Probe invariant** (Codex LOW fix): post-backfill count of
   `o.filed_at IS DISTINCT FROM m.filed_at` over the joined cohort == 0, plus an
   honest unmatched-accession count (rows left untouched). NOT a date-inequality
   heuristic — filing date legitimately follows the trade date.

No `accepted_at` column: `sec_filing_manifest.accepted_at` already exists if acceptance
precision is ever needed separately from filing date.

## Done criteria (from #899 + this plan)

- All writer paths (manifest Form 4/5 + Form 3, legacy sync, rewash) stamp the SEC
  filing timestamp; fallbacks logged.
- Backfill executed on dev; probe per resolution 6 recorded in PR.
- `_current` recomputed via `refresh_insiders_current_batch` over affected instruments;
  spot-verified.
- Tests: writer-level (filed_at == manifest value; fallback branch), backfill
  migration-behaviour test (seed wrong filed_at → run → assert manifest value; both
  source_document_id shapes), tie-break regression (late-filed amendment wins
  `_current` only post-fix).

## Gates

ruff / pyright / fast / smoke + `pytest -m db` + DoD ETL clauses 8-12 (panel smoke,
cross-source one fixture vs EDGAR acceptance date, backfill executed + probe, rollup
endpoint verify, SHAs in PR).
