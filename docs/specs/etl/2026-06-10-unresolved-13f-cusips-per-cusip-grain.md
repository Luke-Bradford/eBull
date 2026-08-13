# #1349 — `unresolved_13f_cusips` bulk partition: per-(cusip, source) grain

**Status**: draft 1.0 · 2026-06-10 · closes #1349 (final remaining half — dead-bloat
half closed by #1398/#1399/#1403).

**Measured 2026-06-10 (dev)**: 5,300,848 rows / 54,707 distinct CUSIPs / 1.1 GB —
all live, all in-retention (#1398 purge dry-run = 0 reclaimable). The grain
`(cusip, filer_cik, period_end, source)` fans each CUSIP ~97×. This spec collapses
the bulk partition to one row per `(cusip, source)` (~55–60k rows, ~10–20 MB).

## 1. Consumer audit — nothing reads the fine grain

Every consumer of the bulk partition, with what it actually needs:

| Consumer | Location | Needs |
| --- | --- | --- |
| OpenFIGI sweep select | `cusip_resolver.py::_select_unresolved_bulk_cusips` | DISTINCT cusip |
| OpenFIGI tombstone | `cusip_resolver.py::_tombstone_bulk_rows_for_cusip` | per cusip |
| Retention purge (#1398) | `cusip_resolver.py::purge_unresolved_bulk_rows_outside_retention` | per-source latest period_end |
| Inline delete (#1399) | `delete_resolved_bulk_markers` + `reconcile_survived_markers` + `in_window_bulk_markers_exist` + per-archive marker collection in both ingesters | exists ONLY because the grain is fine — see §2 |
| Drillthrough audit note | `ownership_drillthrough.py::instrument_ownership_audit_notes` | count of unresolved rows per mapped instrument (semantics shift, §6) |
| Operator inspect | `cusip_resolver.py::iter_pending_unresolved` | observation_count ordering |
| Legacy writer | `institutional_holdings.py::_record_unresolved_cusip` | `ON CONFLICT (cusip) WHERE source IS NULL` — legacy partial index must be recreated in the same migration tx (it is, §3.5) |
| Coverage floor | `bootstrap_preconditions.py::compute_cusip_coverage` | does NOT read this table (instrument-based) |

No code path enumerates `(filer_cik, period_end)` pairs off a marker to drive
recovery. Recovery of a skipped observation is — and always was — bulk dataset
re-ingest over the in-retention window; the quarterly SEC archives are immutable
and re-downloadable.

## 2. Contract reframe (supersedes proposal §2/§2a H1/H2 framing)

Old contract: bulk row = durable per-observation pointer that a specific
`(cusip, filer_cik, period_end, source)` observation was skipped. That contract
is what made cleanup unsafe (proposal
`docs/proposals/etl/1349-unresolved-13f-cusips-bloat.md` §2a: the marker grain
is coarser than the obs-table keys, so a materialised-EXISTS check can
false-positive-delete).

New contract: bulk row = per-`(cusip, source)` **resolver work-queue entry** with
aggregate evidence (`observation_count`, `first_period_end`, `last_period_end`).
Once the grain stops claiming a specific observation, there is no per-observation
claim to falsify — "CUSIP is mapped in `external_identifiers` ⇒ entry is no longer
pending" becomes trivially safe.

This dissolves, rather than answers, the §2a grain-mismatch objection. The
whole #1399 inline-delete machinery (preflight EXISTS, per-archive marker collection,
survived-obs readback, reconcile, staged DELETE) exists solely to safely drain
fine-grain markers; with per-cusip grain it is removed (§5).

Precedent: operator 2026-05-30 already reframed the table as transient queue +
retention purge, not a forever audit log (proposal §2a/§3); roadmap 2026-06-10
queues "per-cusip grain (~100× reduction)" explicitly.

What is lost: per-(filer, period) enumeration of skipped observations. Retained:
aggregate evidence per cusip (count, period range, timestamps) + tombstone
statuses until the retention purge ages the row out. Per-observation enumeration
remains re-derivable from the immutable SEC quarterly datasets.

## 3. Schema — `sql/189` (new-table swap)

New file (never edit applied migrations — content-drift guard sql/175/#1333).
Single transaction:

1. `CREATE TABLE unresolved_13f_cusips_new` — same columns as today MINUS
   `filer_cik`, `period_end`, PLUS `first_period_end DATE`, `last_period_end DATE`.
   Same `resolution_status` CHECK (6 values, sql/168 shape), same `source` CHECK
   (sql/164 shape).
2. Copy legacy partition 1:1 (`source IS NULL` rows; period cols NULL).
3. Collapse bulk partition:

   ```sql
   INSERT INTO unresolved_13f_cusips_new (
       cusip, source, name_of_issuer, last_accession_number,
       observation_count, resolution_status,
       first_observed_at, last_observed_at,
       first_period_end, last_period_end)
   SELECT cusip, source, MAX(name_of_issuer), MAX(last_accession_number),
          SUM(observation_count),   -- not COUNT(*): preserves any >1 counters
          CASE WHEN BOOL_OR(resolution_status IS NULL) THEN NULL  -- pending dominates
               WHEN BOOL_OR(resolution_status IN ('resolved_via_extid',
                                                  'resolved_via_openfigi'))
                    THEN MAX(resolution_status)
                         FILTER (WHERE resolution_status IN
                                 ('resolved_via_extid','resolved_via_openfigi'))
               ELSE MAX(resolution_status) END,  -- resolved beats rejection;
                                                 -- ties break lexically (deterministic)
          MIN(first_observed_at), MAX(last_observed_at),
          MIN(period_end), MAX(period_end)
     FROM unresolved_13f_cusips
    WHERE source IS NOT NULL
    GROUP BY cusip, source;
   ```

   Explicit status precedence (Codex ckpt-1 M): pending (NULL) > resolved_*
   (a mapping exists) > rejection tombstones. In practice bulk statuses are
   uniform per cusip (`_tombstone_bulk_rows_for_cusip` updates all rows for a
   cusip in one statement); the precedence is belt-and-braces.

   The collapse aggregates whatever rows exist at migration time, with NO
   retention filter (Codex ckpt-2 M, conscious): the retention cutoffs are
   Python functions (`thirteen_f_retention_cutoff` /
   `n_port_retention_cutoff`) and duplicating them in migration SQL violates
   single-source-of-truth, while the #1398 steady-state purge already keeps
   the source rows in-window on any live DB (dev measured 2026-06-06 +
   2026-06-10: 0 out-of-retention rows). Consequence: a migrated row's
   `first_period_end` / `observation_count` describe the rows present at
   migration, which may reach back at most one purge cadence beyond the
   floor — lifetime-ish evidence, not a strict retention-window figure. The
   columns are heuristics (§4) either way.
4. `DROP TABLE unresolved_13f_cusips; ALTER TABLE … RENAME` — the swap takes a
   brief ACCESS EXCLUSIVE on the old table (Codex ckpt-1 M: lock is real, just
   short) but avoids the long heap-rewrite hold of `VACUUM FULL` and the
   5.3M-row DELETE WAL churn. Applied at boot before workers start (migration
   runner), so nothing contends in practice.
5. Recreate indexes under their existing names:
   - `unresolved_13f_cusips_bulk_idx` — UNIQUE `(cusip, source)`
     `WHERE source IS NOT NULL` (was 4-col COALESCE expression).
   - `unresolved_13f_cusips_legacy_idx` — UNIQUE `(cusip)` `WHERE source IS NULL`
     (unchanged).
   - `idx_unresolved_13f_cusips_pending`, `idx_unresolved_13f_cusips_accession`
     (unchanged shapes).

No FK references the table (verified — consumer audit). Status aggregation rule:
any pending row keeps the cusip pending; mixed tombstones collapse to MAX
(deterministic; bulk rows only ever carry NULL/'resolved_via_openfigi'/'unresolvable'
in practice — 'ambiguous'/'conflict'/'manual_review' are legacy-partition statuses).

## 4. Writers

`flush_unresolved_cusips_bulk` (`cusip_resolver.py:340`) — COPY staging unchanged
(buffer shape `(cusip, filer_cik, period_end)` + malformed-row filter stays; both
ingester call sites pass one new arg). The drain becomes a GROUP BY upsert:

```sql
INSERT INTO unresolved_13f_cusips (
    cusip, source, observation_count, first_period_end, last_period_end)
SELECT cusip, source, COUNT(*), MIN(period_end), MAX(period_end)
  FROM _stg_unresolved_cusips_bulk
 WHERE period_end >= %(cutoff)s
 GROUP BY cusip, source
ON CONFLICT (cusip, source) WHERE source IS NOT NULL
DO UPDATE SET
    observation_count = unresolved_13f_cusips.observation_count
                        + EXCLUDED.observation_count,
    first_period_end  = LEAST(unresolved_13f_cusips.first_period_end,
                              EXCLUDED.first_period_end),
    last_period_end   = GREATEST(unresolved_13f_cusips.last_period_end,
                                 EXCLUDED.last_period_end),
    last_observed_at  = NOW()
```

**Retention gate at the writer (Codex ckpt-1 H):** the 13F ingester buffers
unresolved markers BEFORE its retention gate
(`sec_13f_dataset_ingest.py:667`/`:702` — N-PORT gates first, 13F does not), so
an old-archive 13F replay would re-insert out-of-retention rows that only the
next purge pass drains. New `cutoff: date` parameter (13F passes
`thirteen_f_retention_cutoff()`, N-PORT `n_port_retention_cutoff()` — no-op
there by construction); staged rows with `period_end < cutoff` are excluded, so
the table is in-window **by construction** and the purge (§5) becomes a pure
aging backstop rather than a re-accumulation drain.

**Return semantics change (Codex ckpt-1 H):** today the function returns
newly-inserted rows and a duplicate re-flush returns 0 (exact-grain
`DO NOTHING`). Post-change it returns distinct `(cusip, source)` groups touched
(inserted OR updated) — a re-flush of the same archive returns the group count,
not 0. Docstring + the count-asserting tests updated; neither ingester consumes
the return value on its hot path (`_flush_unresolved_buffer` discards it).

ON CONFLICT predicate attached per the #1102 settled decision (partial-index
inference requires it). `record_unresolved_cusip_from_bulk` gets the same
single-row shape (count +1, same cutoff gate).

**Idempotency tradeoff (conscious):** re-flushing the same archive inflates
`observation_count` (the old exact-grain DO NOTHING was fully idempotent). The
counter's only consumers are the pending-index ordering and operator inspect —
it is a volume heuristic, not an audited figure. Documented in the docstring.
`first/last_period_end`, `resolution_status`, row identity stay idempotent
(LEAST/GREATEST/monotone). Exact idempotency would require per-observation
grain — the thing being removed.

## 5. Resolver / job changes

- **NEW** `sweep_bulk_cusips_resolved_via_extid(conn)` — bulk analogue of the
  legacy extid sweep: `UPDATE … SET resolution_status='resolved_via_extid',
  last_observed_at=NOW() WHERE source IS NOT NULL AND resolution_status IS NULL
  AND EXISTS (external_identifiers sec/openfigi cusip match)`. Wired into
  `cusip_resolver_post_bulk_sweep` (scheduler.py:5466) before the OpenFIGI
  sweep. Replaces #1399 inline delete as the mapped-cusip hygiene path: cusips
  resolved by ANY route (SEC curated backfill, fuzzy resolver, OpenFIGI) get
  tombstoned within one sweep cadence. Cheap by construction (≤ ~60k-row scan).
- `purge_unresolved_bulk_rows_outside_retention` — predicate becomes
  `last_period_end < cutoff` (a cusip last seen before the retention floor will
  never be re-recorded — the §4 writer gate guarantees it — nor materialised;
  an in-window sighting refreshes `last_period_end`, keeping the row).
  ctid-batching + the 1000-pass driver
  loop in the job collapse to one plain DELETE per source (table is ~55k rows).
  Purge removes tombstoned and pending rows alike once aged out — the table is
  bounded by the retention window in CUSIP terms.
- `_tombstone_bulk_rows_for_cusip`, `_select_unresolved_bulk_cusips`,
  OpenFIGI sweep: SQL unchanged (now touch ≤2 rows / scan ~55k).
- `iter_pending_unresolved` — SELECT gains `source`, `first_period_end`,
  `last_period_end` (Codex ckpt-1 M: bulk rows have NULL
  name_of_issuer/accession; the new columns keep the operator inspect output
  useful for the mixed legacy+bulk pending set).
- **REMOVED** (#1399 machinery + its callers):
  `delete_resolved_bulk_markers`, `reconcile_survived_markers`,
  `in_window_bulk_markers_exist`, `_RESOLVED_STG_COLS`; in both
  `sec_13f_dataset_ingest.py` + `sec_nport_dataset_ingest.py`: the
  `materialised_markers` collection in the archive walk, the survived-obs-keys
  readback, `_delete_resolved_markers`, and the `resolved_markers_deleted`
  result field. Field removal is an API-surface change on both result
  dataclasses — grep app/ + tests/ for every access (logging, admin
  serialisation, test asserts) before deleting (Codex ckpt-1 M). Hot-path
  simplification rides along.
- **DELETED in this PR**: `scripts/cleanup_unresolved_13f_cusips_bloat.py`
  (#1398 one-shot; already executed on dev; reads the dropped `period_end`
  column so leaving it in-tree is a footgun — Codex ckpt-1 L; superseded by
  the migration swap + steady-state purge).

## 6. Drillthrough audit note (semantics shift)

`instrument_ownership_audit_notes` counts rows (any status) whose cusip maps to
the instrument. Post-change the count is per-cusip (0–2 per instrument, by
source) instead of per-observation, and rows persist as `resolved_via_extid`
tombstones (not deleted) until the retention purge ages them out. The signal —
"this instrument's CUSIP sat unresolved during past ingests; observations may
be unmaterialised until re-ingest" — is exactly what the tombstones encode, so
the query keeps counting all statuses; the COPY changes from "N unresolved-CUSIP
row(s)" to "N CUSIP queue entr(y/ies) awaiting re-ingest (#740 backfill gap)"
so it no longer claims the rows are pending (Codex ckpt-1 M: count-vs-copy
mismatch). No FE change (string passthrough).

## 7. Sequencing

Code + migration land in one PR. Migration applies at boot of the new code.
Old jobs-proc code against the new schema would fail its ON CONFLICT inference
(old 4-col target) — operator runbook: restart the dev jobs process onto the
merge SHA after migrating (standard post-merge step, same as #1527/#1532).

## 8. Tests

DB tier (one per new SQL mechanism — test-tiering decision 2026-06-07):
1. Flush upsert: two flushes, same cusip, different (filer, period) → ONE row,
   `observation_count=2`, period range spans both; conflict branch actually
   exercised (prevention-log: ON CONFLICT needs a real conflict, not SQL-text
   asserts). Writer retention gate: staged row with `period_end < cutoff`
   excluded; mixed-period cusip keeps only in-window sightings in the range.
2. extid sweep: mapped bulk cusip → `resolved_via_extid`; unmapped stays
   pending; legacy partition untouched.
3. Purge: `last_period_end < cutoff` deleted, `>= cutoff` kept, legacy kept,
   tombstoned-but-in-window kept.
4. Migration collapse: follow the existing migration-replay test pattern if the
   harness supports seeding at N−1 cheaply; otherwise the dev backfill (§9) is
   the verification (recorded in PR).

Updated: `test_unresolved_cusip_bulk_capture` (per-row → per-cusip assertions),
`test_cusip_resolver_bulk_purge`, `test_cusip_resolver_openfigi`,
`test_sec_nport_dataset_ingest` counts. Removed:
`test_cusip_resolver_resolved_delete` (+ #1399 assertions elsewhere).
Delta-based count assertions per prevention-log.

## 9. DoD 8–12 runbook (dev)

1. Merge → restart dev jobs proc onto merge SHA (migration 189 applies at boot;
   record before/after `COUNT(*)`, `COUNT(DISTINCT cusip)`,
   `pg_total_relation_size`). Expected: 5,300,848 → ~55–60k rows; 1.1 GB →
   ~10–20 MB.
2. Manual-trigger `cusip_resolver_post_bulk_sweep` → confirm extid sweep
   tombstones mapped cusips + purge runs clean (log line).
3. Smoke panel AAPL/GME/MSFT/JPM/HD `/instruments/{sym}/ownership-rollup` → 200,
   figures unchanged vs pre-migration (markers feed no rollup — assert equality).
4. Cross-source: one resolved CUSIP mapping (e.g. `02079K305` → Alphabet)
   verified against OpenFIGI/EDGAR.
5. Record all of the above + commit SHA in the PR description.

## 10. Settled decisions / prevention log applied

- **#1102 partial-index ON CONFLICT predicate** — every upsert attaches
  `WHERE source IS NOT NULL` / `WHERE source IS NULL`; preserved.
- **#719 topology** — sweep stays in the jobs process; preserved.
- **OpenFIGI constraints** — sweep shape, lane, rate budget untouched.
- **Auditability principle** — aggregate evidence + tombstones retained;
  per-observation enumeration re-derivable from immutable SEC archives.
- Prevention log: migration content-drift (new file sql/189); allow-list-delete
  superset rule (n/a — no name-keyed delete; purge keys on period, sweep keys
  on EXISTS extid); ON CONFLICT real-conflict test; delta-based assertions;
  DELETE-then-INSERT atomicity (single migration tx; swap, not clear-repopulate).
