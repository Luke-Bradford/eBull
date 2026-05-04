-- 112_unresolved_13f_resolved_via_extid.sql
--
-- Issue #836 — extend the resolution_status CHECK on
-- ``unresolved_13f_cusips`` with a new ``resolved_via_extid``
-- tombstone reason. Used by the new
-- ``cusip_resolver.sweep_resolvable_unresolved_cusips`` sweep that
-- promotes rows whose CUSIP already matches an
-- ``external_identifiers`` mapping (race-loss between the 13F-HR
-- ingest and the CUSIP backfill — see Phase 0 of
-- docs/superpowers/specs/2026-05-04-ownership-full-decomposition-design.md).
--
-- The new state means: the CUSIP→instrument_id mapping was already
-- present in ``external_identifiers`` when the sweep ran. No new
-- mapping is written; the row stays for audit so an operator can
-- trace which sweep promoted which CUSIP. Distinct from the existing
-- ``conflict`` (mapping points at a DIFFERENT instrument_id) and
-- ``unresolvable`` (no fuzzy candidate met threshold) reasons.

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
        'resolved_via_extid'
    ));

COMMIT;
