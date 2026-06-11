-- 192_unresolved_13f_openfigi_negative_statuses.sql
--
-- #740 (spec docs/specs/etl/2026-06-11-openfigi-sweep-negative-status.md §3)
-- — extend the resolution_status CHECK on ``unresolved_13f_cusips``
-- with the two NEGATIVE OpenFIGI-sweep outcomes:
--
--   * ``openfigi_unknown``        — the resolver call succeeded but
--     OpenFIGI returned no US-primary common-stock mapping for the
--     CUSIP (warning row / no eligible data entry).
--   * ``openfigi_no_instrument``  — OpenFIGI returned a ticker but the
--     normalised ticker has no unique ``is_tradable``
--     ``instruments.symbol`` match (security not in the eToro
--     universe, or ambiguous).
--
-- ## Why negative tombstones at all
--
-- Both outcomes previously left ``resolution_status IS NULL``, so the
-- sweep's ``ORDER BY cusip LIMIT n`` selection re-scanned the same
-- alphabet-head candidates on every run and the 54k-CUSIP backlog
-- never drained (measured 2026-06-10: net 2 promotions per 1000-cusip
-- pass). NULL now strictly means "not yet decided"; transient
-- transport/429 failures keep rows NULL so they retry next pass.
--
-- Statuses are TERMINAL in v1 (operator decision 2026-06-11 — no
-- auto-retry). Escape hatch: manual ``SET resolution_status = NULL``
-- per spec §7. Rows whose CUSIP is later mapped by another route are
-- flipped to ``resolved_via_extid`` by the widened extid sweep.
--
-- ## Idempotency
--
-- DROP + re-ADD under one transaction (same pattern as sql/112 and
-- sql/168). Safe against existing rows: the post-shift set is a
-- strict superset of the current value population.

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
        'resolved_via_openfigi',
        'openfigi_unknown',
        'openfigi_no_instrument'
    ));

COMMIT;
