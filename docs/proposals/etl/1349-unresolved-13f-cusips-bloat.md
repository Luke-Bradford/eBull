# #1349 — `unresolved_13f_cusips` bulk-partition bloat (1.3 GB / 6.7 M rows)

**Status**: draft 1.0 · 2026-05-30 · closes #1349.

**Scope**: stop the bulk partition of `unresolved_13f_cusips` from growing
without bound, and provide a one-shot cleanup for the existing bloat. Code +
synthetic-seeded unit tests land now; the data-side DoD (8–12: backfill +
operator-visible verify) is **deferred** — the dev DB is currently empty
(post-recovery, never bootstrapped), so there is no live bloat to verify
against. See §7.

---

## 1. Problem (code-grounded)

`unresolved_13f_cusips` has two partitions since `sql/164`:
- **legacy** — `source IS NULL`, unique on `(cusip)` (`sql/164:108`).
- **bulk** — `source IN ('bulk_13f_dataset','bulk_nport_dataset')`, unique on
  `(cusip, COALESCE(filer_cik,''), COALESCE(period_end,'0001-01-01'),
  COALESCE(source,''))` (`sql/164:92`). One row per (CUSIP × filer × period ×
  dataset) — deliberately **not** one-per-CUSIP.

**Nothing deletes bulk rows.** Every cleanup path only `UPDATE`s
`resolution_status`, and the only `DELETE` is legacy-scoped:
- `cusip_resolver.py:638,657` — `DELETE … WHERE cusip=%s AND source IS NULL`
  (legacy only).
- `sweep_resolvable_unresolved_cusips` (the extid sweep) — scoped to
  `source IS NULL` (`cusip_resolver.py:890`); marks `resolved_via_extid`, no
  delete.
- `sweep_unresolved_cusips_via_openfigi` (the bulk sweep) — `_tombstone_bulk_
  rows_for_cusip` (`:1217`) only `UPDATE`s `resolution_status='resolved_via_
  openfigi'`, never deletes; and `_select_unresolved_bulk_cusips` (`:1089`)
  **skips** CUSIPs already in `external_identifiers (provider IN ('sec',
  'openfigi'))`.

So the bulk partition only grows. Two accumulating classes:
1. **`resolution_status IS NULL` but CUSIP already mapped** in
   `external_identifiers` — the OpenFIGI sweep skips them (NOT EXISTS), the
   extid sweep is legacy-only → touched by neither → never cleaned. **The
   bulk of the 6.7 M rows** (e.g. SEC-resolved `02079K305`).
2. **`resolution_status = 'resolved_via_openfigi'`** — marked by the bulk
   sweep but never deleted.

Every quarterly bulk re-ingest adds rows; resolved ones are at best marked,
never removed → unbounded growth.

## 2. Safety invariant (corrected after Codex ckpt-1)

The first draft claimed "mapped ⇒ disposable, the next bulk ingest re-derives
the holding." **Codex ckpt-1 refuted this (2 HIGH):**

- **H1** — nothing re-derives a holding *from the unresolved row*; the bulk
  row is the only durable pointer that a specific
  `(cusip, filer_cik, period_end, source)` observation was skipped. The
  sweep only promotes extid + tombstones (`cusip_resolver.py:1217-1230`); it
  does not materialise old rows.
- **H2 — period dimension** — unresolved rows are recorded *before* the
  retention gate (`sec_13f_dataset_ingest.py:597-605`), while holdings are
  written *after* it (`:617-623`). So a bulk row can exist for a period now
  **outside retention** (8-quarter floor for 13F). A CUSIP mapped today does
  NOT cause that old period to be re-ingested — the retention gate skips it.
  Deleting such a row permanently loses the only evidence of the missed
  filer-period observation; the re-ingest will never re-create it.

**Corrected invariant — delete only PROVEN-REDUNDANT rows.** A bulk row is
safe to delete iff its observation is *already materialised* in the typed
table, i.e. there exists a holding row for the resolved `instrument_id` +
`filer_cik` + `period_end`:
- 13F (`source='bulk_13f_dataset'`) → `institutional_holdings`.
- N-PORT (`source='bulk_nport_dataset'`) → `ownership_funds_observations`.

If the holding row exists, the unresolved marker is genuinely redundant →
delete. If it does NOT exist (mapped but never materialised — the old-period
stranded case), **keep** the row: it is a real coverage gap, not bloat, and
silently dropping it is the data-loss H1/H2 warns against. Reclaim is
therefore correctness-bounded — we drain the materialised subset and preserve
genuine gaps for a separate rescue effort (out of scope here).

This supersedes the blanket "mapped ⇒ delete" predicate everywhere below.

### 2a. Codex ckpt-1 round 2 — grain mismatch (the materialised-EXISTS is still unsafe as keyed)

Codex re-review of the §2 rewrite found the materialised-EXISTS check is
correct in principle but **mis-keyed**, re-opening data loss:
- 13F bulk writes **`ownership_institutions_observations`** (not
  `institutional_holdings`) — `sec_13f_dataset_ingest.py:350`. Its natural
  key is `(instrument_id, filer_cik, ownership_nature, period_end,
  source_document_id, exposure_kind)` (`sql/114:69`).
- N-PORT writes `ownership_funds_observations`, keyed `(instrument_id,
  fund_series_id, period_end, source_document_id)` (`sql/123:89`).
- The `unresolved_13f_cusips` bulk row is **coarser** — `(cusip, filer_cik,
  period_end, source)`, with **no** accession / exposure_kind / fund_series.
  So an EXISTS at `(instrument_id, filer_cik, period_end)` can match a
  *different* exposure / series / accession than the one the marker
  represents → false-positive delete (e.g. N-PORT Series A stored, Series B
  skipped → A's presence wrongly deletes B's marker).

**Conclusion: the bulk-row grain is fundamentally too coarse to prove
redundancy against the fine-grained observation tables.** A correct fix
cannot be a standalone cleanup predicate keyed on the unresolved row alone.
The viable safe architecture (chosen 2026-05-30 — see §3):
1. **Inline delete at materialisation** — the bulk ingest deletes the EXACT
   matching unresolved row at the moment it writes the observation (it then
   holds cusip+filer+period+accession+exposure, the precise grain). Stops
   future accumulation correctly; no grain mismatch.
2. **Retention purge** — delete bulk rows whose `period_end` is outside the
   ingest retention floor (8 quarters for 13F). Those periods will never be
   re-ingested, so the observation is unrecoverable whether or not the marker
   is kept → the marker is pure dead weight (audit-only). Safe: no
   *recoverable* observation is lost. Drains the bulk of the 6.7 M (old
   periods).
3. **Keep** in-retention-but-unmapped rows — the genuine pending work-queue.

This avoids every grain-mismatch path Codex found. It is, however, a
reframe of the table's contract (transient queue + retention purge, not a
forever audit log) — chosen by the operator 2026-05-30 (§3).

## 3. Chosen design (operator call 2026-05-30): retention-purge now, inline-delete follow-up

Only a **period-based** predicate is provably safe — it has no dependency on
the coarse bulk-row grain (§2a). Split:

- **PR1 (this spec) — retention purge.** Delete bulk rows whose `period_end`
  is outside the per-source ingest retention floor. **Safety (both sources):
  a `period_end < cutoff` row is for a period that no pipeline will ever
  materialise** — the 13F bulk ingest rejects it at the retention gate
  (`sec_13f_dataset_ingest.py:621`), and S12/S23/N-PORT bulk ingest apply the
  same retention floor — so the observation is permanently unrecoverable
  whether or not the marker is kept → pure dead weight; deleting it loses no
  *recoverable* data.
  - Per-source ordering note (Codex 3): 13F records the unresolved marker
    *before* its retention gate (`:603` guard ⇒ marker only when `period_end
    IS NOT NULL`), so 13F bulk rows always carry a period. N-PORT gates
    retention *first*, then records the marker (`sec_nport_dataset_ingest.py:
    557-564`, `:619-623`) — so N-PORT `< cutoff` rows are aged-out markers,
    not pre-gate ones; the purge proof is "outside current retention," not
    "pre-gate," but the safety conclusion is identical.

  This drains the bulk of the 6.7 M (years of pre-window accumulation) and
  converts the table from **unbounded** growth to **bounded by the retention
  window**.
- **PR2 (follow-up #TBD) — inline delete at materialisation.** Shrinks the
  in-window set further by deleting the exact matching unresolved row when
  the bulk ingest writes the observation (precise grain — it holds
  cusip+filer+period+accession). Hot-path surgery in both ingesters; deferred
  so PR1's safe, bounded fix lands first.

### 3.1 Steady-state (PR1)

A new `purge_unresolved_bulk_rows_outside_retention(conn, *, source, cutoff,
limit)` in `cusip_resolver.py`:
```sql
DELETE FROM unresolved_13f_cusips
 WHERE ctid IN (
   SELECT ctid FROM unresolved_13f_cusips
    WHERE source = %(source)s AND period_end < %(cutoff)s
    LIMIT %(limit)s)
```
- `ctid`-bounded per-pass (Codex M — physical-row cap, not per-CUSIP).
- Called once per source from the cadenced `cusip_resolver_post_bulk_sweep`
  job, after the OpenFIGI sweep, with `cutoff = thirteen_f_retention_cutoff()`
  for `bulk_13f_dataset` and the N-PORT cutoff for `bulk_nport_dataset`.
- Loops until a pass deletes 0 rows OR a total cap is hit (drain across one
  invocation without one giant txn).

### 3.2 One-shot cleanup (PR1)

`scripts/cleanup_unresolved_13f_cusips_bloat.py` (matches `backfill_*.py`):
1. Report before: total rows, distinct cusips, `pg_total_relation_size`,
   plus a per-source `< cutoff` vs `>= cutoff` breakdown (reclaimable vs
   kept).
2. Run the §3.1 purge per source, batched by `ctid` until drained.
3. `VACUUM (FULL, ANALYZE) unresolved_13f_cusips` — **autocommit, outside any
   txn, after the deletes commit.** VACUUM FULL takes `ACCESS EXCLUSIVE`: it
   blocks **all readers and writers** (resolver sweeps, admin/coverage reads,
   bulk ingests), so run in a maintenance window. (Codex M.)
4. Report after: same metrics + delta.

## 4. N-PORT retry invariant (Codex M) — unaffected by the period purge

Unresolved N-PORT accessions are excluded from `n_port_ingest_log` so S23 can
refetch later (`sec_nport_dataset_ingest.py:624-628`, `:372-404`). The S23
retry trigger is the **missing ingest-log row**, NOT the
`unresolved_13f_cusips` row. The PR1 purge only removes rows for periods
outside retention — S23 itself won't refetch those (same retention floor) —
so the purge cannot change any in-window S23 refetch decision. A test pins
that purging an out-of-retention N-PORT bulk row leaves `n_port_ingest_log`
untouched.

## 5. Tests (synthetic-seeded; no live data required)

`tests/test_cusip_resolver_bulk_purge.py`: seed `unresolved_13f_cusips` rows
covering —
- bulk 13F row, `period_end < cutoff` → **deleted**;
- bulk 13F row, `period_end >= cutoff` (in-window pending) → **kept**;
- bulk N-PORT row, `period_end < nport_cutoff` → **deleted**; `n_port_ingest_
  log` row (if seeded) untouched (§4);
- legacy row (`source IS NULL`), any period → **kept** (period purge is
  bulk-only; legacy partition owned by the legacy DELETE path);
- `resolution_status='unresolvable'` in-window → **kept** (only period drives
  the purge, not status).

Assert post-purge membership + that the `ctid` per-pass `limit` bounds the
delete by physical rows (seed N>limit out-of-window rows, assert exactly
`limit` deleted in one pass, all drained after the loop).

## 6. Migration / schema

None. No DDL — partial unique indexes (`sql/164`) and `source` CHECK
unchanged. Pure DML purge + a new cadenced sweep step.

## 7. DoD 8–12 — DEFERRED (operator runbook)

The dev DB is empty (post-WAL-recovery, never bootstrapped — 0 instruments /
0 cusips), so there is no live bloat to smoke / backfill / verify. Per the
operator decision (2026-05-30) the **code + unit tests ship now**; clauses
8–12 are deferred to a runbook step executed once the DB is populated:
- run `scripts/cleanup_unresolved_13f_cusips_bloat.py` on the populated DB;
- record before/after row count + `pg_total_relation_size`;
- confirm the steady-state sweep keeps the count flat across a subsequent
  bulk ingest;
- spot-check that a previously-stranded mapped CUSIP (e.g. `02079K305`) has
  its holdings present in `institutional_holdings` after the next ingest.

## 8. Sibling bloat cluster (out of scope; sequence later)

#1219 (`VACUUM FULL financial_facts_raw`, ~25 GB), #1221 (ffr partitions past
2030), #1348 (retire S19/S20/S23), #1302 (13F bulk LEI column dropped).
