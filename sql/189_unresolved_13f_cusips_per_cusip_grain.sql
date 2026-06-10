-- 189_unresolved_13f_cusips_per_cusip_grain.sql
--
-- #1349 — collapse the bulk partition of ``unresolved_13f_cusips`` from
-- one row per ``(cusip, filer_cik, period_end, source)`` to one row per
-- ``(cusip, source)``.
--
-- Why: the fine grain fanned 54,707 distinct CUSIPs into 5,300,848 rows
-- / 1.1 GB (measured dev, 2026-06-10) and NO consumer reads the
-- per-(filer, period) detail — the OpenFIGI sweep needs DISTINCT cusip,
-- the retention purge needs the per-source latest period, and recovery
-- of a skipped observation is bulk dataset re-ingest (the quarterly SEC
-- archives are immutable), never marker enumeration. The #1399 inline
-- delete existed solely to drain fine-grain markers safely; the service
-- PR that ships with this migration removes it. Spec:
-- docs/specs/etl/2026-06-10-unresolved-13f-cusips-per-cusip-grain.md.
--
-- Shape: new-table swap (CREATE + INSERT aggregate + DROP + RENAME)
-- instead of in-place DELETE so the 1.1 GB heap is reclaimed without
-- VACUUM FULL and without 5.3M rows of DELETE WAL. The swap takes a
-- brief ACCESS EXCLUSIVE on the old table; the migration runner applies
-- it at boot before workers start, so nothing contends.
--
-- Column changes vs sql/164: DROP ``filer_cik``, ``period_end``; ADD
-- ``first_period_end`` / ``last_period_end`` (per-source sighting range,
-- LEAST/GREATEST-maintained by the writers). ``observation_count``
-- becomes the SUM over collapsed rows — a best-effort volume heuristic
-- (consumers: pending-index ordering + operator inspect only).
--
-- The collapse aggregates whatever rows exist at migration time, with NO
-- retention filter (Codex ckpt-2, conscious): the retention cutoffs are
-- Python functions and duplicating them here violates single-source-of-
-- truth, while the #1398 steady-state purge already keeps the source
-- rows in-window on any live DB (dev measured 0 out-of-retention rows).
--
-- Status precedence for mixed-status groups (belt-and-braces — bulk
-- statuses are uniform per cusip in practice because
-- ``_tombstone_bulk_rows_for_cusip`` updates all rows for a cusip in one
-- statement): pending (NULL) dominates, else resolved_* (a mapping
-- exists) beats rejection tombstones, lexical MAX breaks ties
-- deterministically.

-- Safety guard for a partially-applied manual replay: a leftover _new
-- relation from an aborted run would fail the CREATE below.
DROP TABLE IF EXISTS unresolved_13f_cusips_new;

CREATE TABLE unresolved_13f_cusips_new (
    cusip                  TEXT NOT NULL,
    name_of_issuer         TEXT,
    last_accession_number  TEXT,
    observation_count      INTEGER NOT NULL DEFAULT 1,
    resolution_status      TEXT
        CONSTRAINT unresolved_13f_cusips_resolution_status_check
        CHECK (resolution_status IS NULL OR resolution_status IN (
            'unresolvable',
            'ambiguous',
            'conflict',
            'manual_review',
            'resolved_via_extid',
            'resolved_via_openfigi'
        )),
    first_observed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_observed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source                 TEXT
        CONSTRAINT unresolved_13f_cusips_source_check
        CHECK (source IS NULL OR source IN (
            'bulk_13f_dataset',
            'bulk_nport_dataset'
        )),
    first_period_end       DATE,
    last_period_end        DATE
);

-- Legacy partition (source IS NULL): copy 1:1. Period range stays NULL —
-- the legacy per-filing writer has no period dimension.
INSERT INTO unresolved_13f_cusips_new (
    cusip, name_of_issuer, last_accession_number,
    observation_count, resolution_status,
    first_observed_at, last_observed_at, source
)
SELECT cusip, name_of_issuer, last_accession_number,
       observation_count, resolution_status,
       first_observed_at, last_observed_at, NULL
  FROM unresolved_13f_cusips
 WHERE source IS NULL;

-- Bulk partition: collapse to per-(cusip, source) aggregates.
INSERT INTO unresolved_13f_cusips_new (
    cusip, source, name_of_issuer, last_accession_number,
    observation_count, resolution_status,
    first_observed_at, last_observed_at,
    first_period_end, last_period_end
)
SELECT cusip,
       source,
       MAX(name_of_issuer),
       MAX(last_accession_number),
       -- SUM, not COUNT(*): preserves any pre-existing >1 counters
       -- (Codex ckpt-1).
       SUM(observation_count),
       CASE WHEN BOOL_OR(resolution_status IS NULL) THEN NULL
            WHEN BOOL_OR(resolution_status IN ('resolved_via_extid',
                                               'resolved_via_openfigi'))
                 THEN MAX(resolution_status)
                      FILTER (WHERE resolution_status IN
                              ('resolved_via_extid', 'resolved_via_openfigi'))
            ELSE MAX(resolution_status)
       END,
       MIN(first_observed_at),
       MAX(last_observed_at),
       MIN(period_end),
       MAX(period_end)
  FROM unresolved_13f_cusips
 WHERE source IS NOT NULL
 GROUP BY cusip, source;

-- Swap. Index names are schema-global, so the canonical names can only
-- be created after the old table (and its indexes) is gone.
DROP TABLE unresolved_13f_cusips;
ALTER TABLE unresolved_13f_cusips_new RENAME TO unresolved_13f_cusips;

-- Bulk-path dedup target: was the 4-column COALESCE expression index
-- (sql/164); now plain (cusip, source). The partial WHERE keeps the
-- two-partition split, and every ON CONFLICT against it must attach the
-- predicate (#1102 settled decision on partial-index inference).
CREATE UNIQUE INDEX unresolved_13f_cusips_bulk_idx
    ON unresolved_13f_cusips (cusip, source)
    WHERE source IS NOT NULL;

-- Legacy-path dedup target (unchanged shape, sql/164).
CREATE UNIQUE INDEX unresolved_13f_cusips_legacy_idx
    ON unresolved_13f_cusips (cusip)
    WHERE source IS NULL;

-- Resolver hot path (unchanged shape, sql/099).
CREATE INDEX idx_unresolved_13f_cusips_pending
    ON unresolved_13f_cusips (observation_count DESC, last_observed_at DESC)
    WHERE resolution_status IS NULL;

-- Operator per-accession tooling (unchanged shape, sql/099; bulk rows
-- carry NULL here — the column is legacy-partition data).
CREATE INDEX idx_unresolved_13f_cusips_accession
    ON unresolved_13f_cusips (last_accession_number);
