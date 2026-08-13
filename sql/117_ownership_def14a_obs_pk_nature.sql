-- 117_ownership_def14a_obs_pk_nature.sql
--
-- Issue #840 P1.D follow-up — review caught observation-table PK gap
-- on PR #854. The _current PK was correctly updated to include
-- ``ownership_nature`` so dual-nature rows (beneficial + voting on
-- same proxy) coexist; the observations PK was NOT mirrored, so the
-- ON CONFLICT DO UPDATE in record_def14a_observation silently
-- overwrites the first nature with the second whenever both share
-- the same accession (which a real single-proxy filing does).
--
-- Fix: drop the prior PK + recreate including ownership_nature.
-- The table is empty in production today (no backfill yet) so this
-- is a clean DDL change with no data migration step.

BEGIN;

-- Drop the parent's PK; partition tables inherit the parent's PK
-- definition, so the change cascades to every partition.
ALTER TABLE ownership_def14a_observations
    DROP CONSTRAINT IF EXISTS ownership_def14a_observations_pkey;

ALTER TABLE ownership_def14a_observations
    ADD PRIMARY KEY (instrument_id, holder_name_key, ownership_nature, period_end, source_document_id);

COMMIT;
