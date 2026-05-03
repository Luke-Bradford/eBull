-- 111_fix_filer_seed_drifts.sql
--
-- Issue #788 — operator triage of the 4 drifts surfaced by PR #821's
-- filer_seed_verification sweep.
--
-- Each row was added in PR #001 / migration 091 with a label that
-- preceded the verification gate. Now that the gate is in place we
-- can resolve the 4 mismatches against SEC's authoritative
-- submissions.json:
--
--   * 0000080255 — SEC name "PRICE T ROWE ASSOCIATES INC /MD/" (14
--     recent 13F-HR). Canonical T. Rowe Price 13F filer; just
--     update expected_name to match SEC's literal form.
--   * 0000093751 — SEC name "STATE STREET CORP" (9 recent 13F-HR).
--     Canonical State Street 13F filer; update expected_name to
--     drop the "oration" suffix that doesn't normalise-match
--     "Corp".
--   * 0000315066 — SEC name "FMR LLC" (4 recent 13F-HR). Canonical
--     Fidelity 13F filer; update expected_name to drop the
--     "(Fidelity)" disambiguation suffix that the operator UI used
--     for display but isn't part of SEC's canonical name.
--   * 0001364742 — SEC name "BlackRock Finance, Inc." (only 3
--     recent 13F-HR). NOT the canonical BlackRock 13F filer. The
--     real one is CIK 0001086364 ("BLACKROCK ADVISORS LLC", 48
--     recent 13F-HR). Drop the wrong row, insert the correct one.
--
-- Idempotent — re-running this migration produces the same end state.

-- 1. T. Rowe Price — update expected_name to SEC's canonical form.
UPDATE institutional_filer_seeds
SET expected_name = 'PRICE T ROWE ASSOCIATES INC /MD/'
WHERE cik = '0000080255';

-- 2. State Street — update expected_name to SEC's canonical form.
UPDATE institutional_filer_seeds
SET expected_name = 'STATE STREET CORP'
WHERE cik = '0000093751';

-- 3. FMR LLC — update expected_name to SEC's canonical form
--    (drop the "(Fidelity)" display suffix).
UPDATE institutional_filer_seeds
SET expected_name = 'FMR LLC'
WHERE cik = '0000315066';

-- 4. BlackRock — replace the wrong CIK with the canonical
--    13F-filing entity. The previous row pointed at BlackRock
--    Finance Inc which is a financing subsidiary that files only
--    occasional 13F-HRs. The actual top-level 13F filer is
--    BlackRock Advisors LLC (0001086364) with 48 recent 13F-HRs.
--
-- Downstream cleanup: any institutional_holdings / institutional_filers
-- rows that landed against the wrong CIK before the verification
-- gate caught it would still feed ownership-rollup reads. Clear
-- them so the next 13F-HR ingest pass against the corrected CIK
-- is the only source of truth. Migration 106 (Soros/Geode relabel)
-- established the same precedent.
DELETE FROM institutional_holdings
 WHERE filer_id IN (
     SELECT filer_id FROM institutional_filers WHERE cik = '0001364742'
 );

DELETE FROM institutional_filers WHERE cik = '0001364742';

DELETE FROM institutional_filer_seeds WHERE cik = '0001364742';

INSERT INTO institutional_filer_seeds (cik, label, expected_name, active, notes)
VALUES (
    '0001086364',
    'BlackRock Advisors LLC',
    'BLACKROCK ADVISORS LLC',
    TRUE,
    'Replaces 0001364742 (BlackRock Finance, Inc., 3 recent 13F-HRs) — '
    'verified canonical 13F filer with 48 recent 13F-HRs as of 2026-05-03 '
    'via filer_seed_verification gate (PR #821).'
)
ON CONFLICT (cik) DO UPDATE SET
    label = EXCLUDED.label,
    expected_name = EXCLUDED.expected_name,
    active = TRUE,
    notes = COALESCE(institutional_filer_seeds.notes, EXCLUDED.notes);
