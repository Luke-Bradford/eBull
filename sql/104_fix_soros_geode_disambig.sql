-- 104_fix_soros_geode_disambig.sql
--
-- Soros / Geode CIK disambiguation (#790 P2, Batch 2 of #788).
--
-- Live state pre-fix:
--   * institutional_filer_seeds row CIK 0001029160 labelled
--     "Geode Capital Management LLC" — WRONG. SEC submissions.json
--     for CIK 0001029160 confirms the entity name is
--     "SOROS FUND MANAGEMENT LLC". Verified via:
--     https://data.sec.gov/submissions/CIK0001029160.json
--   * etf_filer_cik_seeds row CIK 0001029160 tags it as ETF —
--     WRONG. Soros is a hedge fund, not an ETF issuer; every Soros
--     position is currently routed to the ETFs slice on the
--     ownership card.
--   * Real Geode Capital Management LLC is CIK 0001214717
--     (verified via SEC EDGAR full-text search for 13F-HR filings;
--     submissions.json confirms entity name). Geode operates
--     Fidelity's passive-index funds and IS legitimately an ETF
--     filer for the chart's filer_type split.
--
-- Note on the issue text: #790 cited Geode's CIK as 0001572162.
-- That's wrong — 0001572162 is Sinclair Television of Illinois, LLC.
-- The 0001214717 figure is what SEC EDGAR full-text search returns
-- for 13F-HR filings whose primary_doc.xml names "GEODE CAPITAL
-- MANAGEMENT, LLC".
--
-- Apply order:
--   1. Relabel the Soros row in institutional_filer_seeds.
--   2. Drop the bogus Soros ETF override.
--   3. Add Geode (real CIK) to institutional_filer_seeds.
--   4. Tag Geode (real CIK) in etf_filer_cik_seeds.
--
-- Idempotent: each step uses ON CONFLICT DO UPDATE / DO NOTHING /
-- DELETE WHERE so a re-run is safe. The institutional_filers /
-- institutional_holdings tables (which inherit from the seeds via
-- the ingester) are NOT touched here — the next ingester run picks
-- up the corrected name from the live SEC fetch (filer_name on
-- institutional_filers updates via the existing ON CONFLICT path
-- in the ingester's seed_filer code).

-- Step 1: relabel CIK 0001029160 as Soros (canonical SEC name).
UPDATE institutional_filer_seeds
SET label = 'Soros Fund Management LLC',
    notes = 'Was mis-labelled "Geode Capital Management" through migration 091; corrected by migration 104 (#790 P2). Routing Soros positions to the institutions slice (NOT etfs).'
WHERE cik = '0001029160';

-- Step 2: drop the bogus ETF override on Soros's CIK so the
-- filer_type classifier no longer routes Soros positions to ETFs.
DELETE FROM etf_filer_cik_seeds
WHERE cik = '0001029160';

-- Step 3: add real Geode Capital Management LLC (CIK 0001214717)
-- to the institutional seed list. ``institutional_filers.filer_type``
-- is updated by the next ingester run (filer_type lookup walks the
-- etf_filer_cik_seeds override below).
INSERT INTO institutional_filer_seeds (cik, label, active, notes)
VALUES (
    '0001214717',
    'Geode Capital Management LLC',
    TRUE,
    'Operates Fidelity''s passive-index funds. Migrated in by #790 P2 alongside the Soros/Geode disambiguation.'
)
ON CONFLICT (cik) DO UPDATE
SET label = EXCLUDED.label,
    notes = EXCLUDED.notes;

-- Step 4: tag the real Geode CIK as an ETF issuer so the filer_type
-- classifier routes its 13F-HR holdings into the etfs slice.
INSERT INTO etf_filer_cik_seeds (cik, label)
VALUES ('0001214717', 'Geode Capital Management (Fidelity passive-index funds)')
ON CONFLICT (cik) DO UPDATE
SET label = EXCLUDED.label;

-- Defensive: existing institutional_filers.filer_type rows for the
-- two CIKs need to flip on the next ingester run. The filer_type
-- column is set during seed_filer / ingester upsert from
-- ``etf_filer_cik_seeds`` lookup, so this UPDATE preempts the
-- mis-tagging that would otherwise persist until the next 13F-HR
-- ingest pass touches each filer.
UPDATE institutional_filers
SET filer_type = 'INV',  -- INV = investment manager (hedge fund / advisor); not ETF
    name = 'SOROS FUND MANAGEMENT LLC'
WHERE cik = '0001029160';

-- ``institutional_filers`` row for the real Geode CIK may not yet
-- exist; the next ingester run creates it. ON CONFLICT DO UPDATE
-- defensively handles the case where it has been ingested under a
-- name variant.
INSERT INTO institutional_filers (cik, name, filer_type)
VALUES ('0001214717', 'GEODE CAPITAL MANAGEMENT, LLC', 'ETF')
ON CONFLICT (cik) DO UPDATE
SET name = EXCLUDED.name,
    filer_type = 'ETF';
