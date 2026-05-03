-- 110_filer_seed_expected_name.sql
--
-- Issue #807 — operator-curated filer-seed list grows from 14 to
-- (target) 150 names. Hand-curated CIK lists drift fast: migrations
-- 104 + 106 caught a 6-of-10 mis-label rate on the prior pass, with
-- one entirely-hallucinated row (AutoZone). At a 150-row scale, a
-- similar mis-label rate would silently mis-attribute thousands of
-- 13F holdings to the wrong issuer.
--
-- ``expected_name`` records what the operator THOUGHT the SEC name
-- of the entity at this CIK was when they added the seed. The
-- verification sweep (app.services.filer_seed_verification) fetches
-- ``data.sec.gov/submissions/CIK{cik}.json`` and compares against
-- the SEC-side ``name`` field. Mismatches surface as a finding —
-- operator either fixes the CIK or updates the expected_name to
-- match SEC's authoritative form.
--
-- Backfill: copy ``label`` into ``expected_name`` for the existing
-- 14 rows. The verification sweep will flag any that don't match
-- SEC's canonical name (some labels were pre-disambiguation, e.g.
-- "FMR LLC (Fidelity)" — SEC's name is just "FMR LLC").

ALTER TABLE institutional_filer_seeds
    ADD COLUMN IF NOT EXISTS expected_name TEXT;

UPDATE institutional_filer_seeds
SET expected_name = label
WHERE expected_name IS NULL;

COMMENT ON COLUMN institutional_filer_seeds.expected_name IS
    'Operator-recorded SEC entity name as of seed creation. The '
    'verification sweep compares this against the live submissions.'
    'json ``name`` and flags drift. Distinct from ``label`` which '
    'is operator-display text and may include disambiguation '
    'suffixes ("(Fidelity)") that the SEC name does not carry.';
