-- #2214 — no 13F MANAGER is an ETF. Retire the curated ETF overrides on manager CIKs
-- and re-type the rows they produced.
--
-- Source rule: Rule 13f-1 — Form 13F is filed by the institutional investment MANAGER
-- and reports every account over which it exercises discretion, with no fund
-- breakdown. A manager's book mixes ETF and non-ETF mandates, so a filer_type value can
-- never mean "shares held by ETFs". Fund-level ETF holdings belong to N-PORT (settled
-- #1577, "ETF identity = series/class"), and they already render as the `funds`
-- memo overlay. See `.claude/skills/data-sources/sec-edgar.md` §2.2.1.
--
-- Measured on the source itself: in Geode's own N-CEN records, most of the series it
-- advises or subadvises carry IS_ETF blank, not 'Y' (2026-09-24: 25 Y / 106 not). The
-- seed script's other two overrides (Vanguard 0000102909, BlackRock 0001086364) are the
-- same shape (Vanguard: 137 Y / 138 not). Reproduce both splits with
-- `uv run python -m scripts.audit_ncen_etf_advisers` (sections 5 and 6). Those two
-- overrides were never applied on dev, where etf_filer_cik_seeds held Geode alone.
--
-- Re-typed value is 'INV': with the seed gone, `compose_filer_type` falls to its
-- N-CEN tier, which writes nothing for a 13F-manager CIK (the N-CEN registrant is the
-- trust, never the manager — sec-edgar §2.2.1), so it lands on the 'INV' default. The
-- UPDATEs are scoped to rows still tagged 'ETF', so a re-run is a no-op.
--
-- This is a split move, not a re-count. On `/instruments/{symbol}/institutional-holdings`
-- every re-typed share leaves `etfs_shares` and enters `institutions_shares`, so the sum is
-- unchanged per instrument. The ownership rollup already routed Geode to `institutions`
-- through its `geode` family collapse, and does not move
-- (`scripts/ab_2214_retire_etf_seeds`).

DELETE FROM etf_filer_cik_seeds
 WHERE cik IN ('0001214717', '0000102909', '0001086364');

UPDATE institutional_filers
   SET filer_type = 'INV'
 WHERE filer_type = 'ETF'
   AND cik IN ('0001214717', '0000102909', '0001086364');

UPDATE ownership_institutions_observations
   SET filer_type = 'INV'
 WHERE filer_type = 'ETF'
   AND filer_cik IN ('0001214717', '0000102909', '0001086364');

-- refreshed_at advances whenever a business column changes (current-table contract).
UPDATE ownership_institutions_current
   SET filer_type = 'INV', refreshed_at = NOW()
 WHERE filer_type = 'ETF'
   AND filer_cik IN ('0001214717', '0000102909', '0001086364');
