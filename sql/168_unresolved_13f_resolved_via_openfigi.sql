-- 168_unresolved_13f_resolved_via_openfigi.sql
--
-- #1233 PR-1b (spec §5) — extend the resolution_status CHECK on
-- ``unresolved_13f_cusips`` with a new ``resolved_via_openfigi`` value
-- so the OpenFIGI sweep can tombstone bulk-source rows once it has
-- promoted the CUSIP into ``external_identifiers``.
--
-- ## Why a separate tombstone value
--
-- ``resolved_via_extid`` (sql/112) marks the LEGACY-path race-loss
-- case: the CUSIP already had a mapping in ``external_identifiers``
-- at sweep time. The OpenFIGI sweep is different — IT WROTE the
-- mapping. Operator audit on a bulk-source row stamped
-- ``resolved_via_openfigi`` knows the resolution path was external
-- (OpenFIGI API) rather than internal (curated extids), which matters
-- when reviewing data lineage or rolling back a faulty OpenFIGI batch.
--
-- ## Idempotency
--
-- DROP + re-ADD under one transaction. Re-application sees the same
-- CHECK definition and the constraint state is re-established. Safe
-- against existing rows: any row whose value is already in the
-- post-shift set passes the constraint.

BEGIN;

ALTER TABLE unresolved_13f_cusips
    DROP CONSTRAINT IF EXISTS unresolved_13f_cusips_resolution_status_check;

ALTER TABLE unresolved_13f_cusips
    ADD CONSTRAINT unresolved_13f_cusips_resolution_status_check
    CHECK (resolution_status IS NULL OR resolution_status IN (
        'unresolvable',
        'ambiguous',
        'conflict',
        'manual_review',
        'resolved_via_extid',
        'resolved_via_openfigi'
    ));

COMMIT;
