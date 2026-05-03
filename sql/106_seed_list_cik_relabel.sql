-- 106_seed_list_cik_relabel.sql
--
-- Seed-list CIK relabel + drop bogus row + add intended top
-- managers (operator complaint 2026-05-03: pie chart sparse, only
-- 124 of 12,379 instruments have any 13F holding).
--
-- The investigation found that ``_INSTITUTIONAL_SEEDS`` in
-- ``scripts/seed_holder_coverage.py`` had FOUR more wrong CIKs
-- beyond the Soros/Geode disambig (migration 104). The labels
-- claimed "Northern Trust" / "T. Rowe Price" / "Capital World" /
-- "Wellington Management" but the CIKs actually mapped to:
--
--   * 0000200217 → DODGE & COX (claimed Northern Trust)
--   * 0000354204 → DIMENSIONAL FUND ADVISORS LP (claimed T. Rowe Price)
--   * 0000895421 → MORGAN STANLEY (claimed Capital World Investors)
--   * 0000866787 → AUTOZONE INC (claimed Wellington Management) —
--     AutoZone is an issuer, not a 13F filer; this row was
--     hallucinated.
--
-- The actually-seeded rows (Dodge & Cox, Dimensional, Morgan
-- Stanley) are real 13F filers with substantial holdings (1,375 /
-- 696 / 827 holdings respectively in dev DB) — they stay seeded;
-- we only relabel the operator-facing ``label`` column to match
-- reality.
--
-- AUTOZONE is dropped from the seed list — it's not a 13F filer
-- and the row produced zero holdings.
--
-- The intended top managers are added with correct CIKs:
--
--   * Northern Trust Corp = 0000073124
--   * T. Rowe Price Associates = 0000080255
--   * Capital World Investors = 0001422849
--   * Wellington Management Group = 0000902219
--
-- These are tagged INV (active institutional managers); the
-- ingester picks them up on the next 13F-HR sync pass and the pie
-- chart's institutions slice grows accordingly.
--
-- Idempotent — UPSERT / DELETE WHERE patterns mean re-running is
-- safe.

-- Step 1: relabel the actually-seeded rows to match the SEC entity
-- name. The institutional_filers ingester already writes the real
-- name to ``institutional_filers.name`` from primary_doc.xml; the
-- seed table's ``label`` is operator-facing only.
UPDATE institutional_filer_seeds
SET label = 'Dodge & Cox',
    notes = 'Was mis-labelled "Northern Trust Corp." through migration 091; corrected by migration 106. CIK 0000200217 actually maps to Dodge & Cox per SEC submissions.json.'
WHERE cik = '0000200217';

UPDATE institutional_filer_seeds
SET label = 'Dimensional Fund Advisors LP',
    notes = 'Was mis-labelled "T. Rowe Price Associates" through migration 091; corrected by migration 106. CIK 0000354204 actually maps to Dimensional Fund Advisors LP per SEC submissions.json.'
WHERE cik = '0000354204';

UPDATE institutional_filer_seeds
SET label = 'Morgan Stanley',
    notes = 'Was mis-labelled "Capital World Investors" through migration 091; corrected by migration 106. CIK 0000895421 actually maps to Morgan Stanley per SEC submissions.json.'
WHERE cik = '0000895421';

-- Step 2: drop the bogus AutoZone row + any holdings it pulled
-- (none expected since AutoZone isn't a 13F filer).
DELETE FROM institutional_holdings
WHERE filer_id IN (SELECT filer_id FROM institutional_filers WHERE cik = '0000866787');
DELETE FROM institutional_filers WHERE cik = '0000866787';
DELETE FROM institutional_filer_seeds WHERE cik = '0000866787';

-- Step 3: add the intended top managers with correct CIKs. The
-- ingester picks them up on the next 13F-HR pass; row counts on
-- ``institutional_holdings`` grow over the following days.
INSERT INTO institutional_filer_seeds (cik, label, active, notes) VALUES
    ('0000073124', 'Northern Trust Corp.', TRUE,
     'Top-25 institutional manager by AUM. Added via migration 106 alongside the seed-list relabel.'),
    ('0000080255', 'T. Rowe Price Associates Inc.', TRUE,
     'Top-25 institutional manager by AUM. Added via migration 106 (the prior 0000354204 CIK was Dimensional, not T. Rowe).'),
    ('0001422849', 'Capital World Investors', TRUE,
     'American Funds parent. Added via migration 106 (the prior 0000895421 CIK was Morgan Stanley, not Capital World).'),
    ('0000902219', 'Wellington Management Group LLP', TRUE,
     'Top-25 institutional manager by AUM. Added via migration 106 (the prior 0000866787 CIK was AutoZone — not a 13F filer at all).')
ON CONFLICT (cik) DO UPDATE
SET label = EXCLUDED.label,
    notes = EXCLUDED.notes,
    active = TRUE;

-- Step 4: pre-create the institutional_filers rows so the ingester
-- on its next pass writes holdings against them rather than first
-- having to discover the filer. Mirrors the Soros/Geode pattern in
-- migration 104.
INSERT INTO institutional_filers (cik, name, filer_type) VALUES
    ('0000073124', 'NORTHERN TRUST CORP', 'INV'),
    ('0000080255', 'PRICE T ROWE ASSOCIATES INC /MD/', 'INV'),
    ('0001422849', 'Capital World Investors', 'INV'),
    ('0000902219', 'WELLINGTON MANAGEMENT GROUP LLP', 'INV')
ON CONFLICT (cik) DO UPDATE
SET name = EXCLUDED.name,
    filer_type = EXCLUDED.filer_type;
