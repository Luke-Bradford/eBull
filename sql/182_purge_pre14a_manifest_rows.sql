-- 182_purge_pre14a_manifest_rows.sql
--
-- #1320: purge PRE 14A (preliminary proxy) rows mis-seeded into the
-- sec_def14a manifest namespace, and repair the freshness index they
-- polluted.
--
-- WHY
-- ---
--   `app/services/sec_manifest._FORM_TO_SOURCE` previously mapped
--   ``PRE 14A`` -> ``sec_def14a``. PRE 14A is a pre-finalisation draft,
--   classified metadata-only — its ownership figures were never counted;
--   the definitive DEF 14A that follows is what we ingest. The mapping
--   routed 6k+ drafts into the sec_def14a manifest, which the parser
--   (manifest_parsers/def14a.py) then tombstoned PRE-FETCH. No raw doc, no
--   ingest-log row, no observation is ever written for a PRE 14A accession.
--
--   But `data_freshness.seed_freshness_for_manifest_row` advances
--   `data_freshness_index.last_known_filed_at` from EVERY seeded manifest row
--   by max(filed_at) — including PRE rows. So a PRE draft filed after the
--   latest real DEF 14A left the subject's freshness pointer aimed at a
--   preliminary draft. `watermarks.py` reports the operator-facing sec_def14a
--   watermark as `MAX(last_known_filed_at)` across subjects, so a single PRE
--   pointer inflates it (Codex #1320 review).
--
--   The `_FORM_TO_SOURCE` entry was removed in the same change, so discovery
--   no longer re-seeds PRE 14A. This migration cleans up the rows + freshness
--   pointers already on disk.
--
-- WHAT IT DOES
-- -----------
--   1. DELETE every sec_def14a manifest row with a PRE 14A form. Nothing
--      downstream references them (parser tombstones before any write).
--   2+3 only touch the freshness rows that are PROVABLY PRE-polluted: those
--      whose `last_known_filing_id` no longer resolves to any manifest row
--      after step 1 (it pointed at a now-deleted PRE accession). A freshness
--      row whose pointer is NULL or still resolves is left untouched — it was
--      not advanced by the draft we deleted, so it may be a legitimately
--      watched issuer with no PRE involvement.
--   2. RECOMPUTE last_known_*/expected_next_at for dangling rows whose subject
--      STILL has a sec_def14a manifest row — from the latest remaining row,
--      mirroring `seed_scheduler_from_manifest` (DISTINCT ON max filed_at).
--      Cadence 365d = `data_freshness.cadence_for('sec_def14a')`.
--   3. CLEAR last_known_* (-> NULL, state='unknown') for dangling rows whose
--      subject has NO remaining sec_def14a manifest row: the pointer aimed
--      solely at a draft, so there is no known real filing. The row is kept so
--      the subject stays watched; the poll path re-derives state. NULL
--      last_known_filed_at drops out of the `MAX()` operator watermark.
--
-- Idempotent: a no-op once clean (mapping removal stops re-seeding; both
-- freshness predicates self-restrict to dangling pointers).
-- TRIM guards the unlikely whitespace variant SEC sometimes emits.

BEGIN;

-- 1. Purge the PRE 14A manifest rows.
DELETE FROM sec_filing_manifest
WHERE source = 'sec_def14a'
  AND TRIM(form) = 'PRE 14A';

-- 2. Recompute dangling-pointer freshness rows that still have a manifest row.
UPDATE data_freshness_index dfi
SET last_known_filing_id = latest.accession_number,
    last_known_filed_at  = latest.filed_at,
    expected_next_at     = latest.filed_at + INTERVAL '365 days',
    updated_at           = NOW()
FROM (
    SELECT DISTINCT ON (subject_type, subject_id)
        subject_type, subject_id, accession_number, filed_at
    FROM sec_filing_manifest
    WHERE source = 'sec_def14a'
    ORDER BY subject_type, subject_id, filed_at DESC NULLS LAST
) latest
WHERE dfi.source = 'sec_def14a'
  AND dfi.subject_type = latest.subject_type
  AND dfi.subject_id = latest.subject_id
  AND dfi.last_known_filing_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM sec_filing_manifest m
      WHERE m.accession_number = dfi.last_known_filing_id
  );

-- 3. Clear dangling-pointer freshness rows with no remaining manifest row.
UPDATE data_freshness_index dfi
SET last_known_filing_id = NULL,
    last_known_filed_at  = NULL,
    expected_next_at     = NULL,
    state                = 'unknown',
    state_reason         = 'pre14a_pointer_cleared_1320',
    updated_at           = NOW()
WHERE dfi.source = 'sec_def14a'
  AND dfi.last_known_filing_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM sec_filing_manifest m
      WHERE m.accession_number = dfi.last_known_filing_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM sec_filing_manifest m2
      WHERE m2.source = 'sec_def14a'
        AND m2.subject_type = dfi.subject_type
        AND m2.subject_id = dfi.subject_id
  );

COMMIT;
