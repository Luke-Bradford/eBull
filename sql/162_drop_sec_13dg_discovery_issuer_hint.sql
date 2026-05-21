-- 162_drop_sec_13dg_discovery_issuer_hint.sql
--
-- Drop SC 13D/G discovery issuer-hint table (sql/159 from PR11 design).
-- PR11 (#1233) v8 empirical pivot 2026-05-21 — efts.sec.gov post-
-- 2024-12-18 does NOT index SC 13D/G by SUBJECT CIK, only by FILER
-- CIK. Operator smoke against AAPL/GME/MSFT/JPM/HD returned 0 hits
-- via the universe-issuer-CIK discovery path even though Vanguard
-- (filer CIK 102909) files SCHEDULE 13G/A against those issuers.
-- The PR11 discovery layer + hint cross-validation are therefore
-- abandoned; the legacy daily-index path remains the discovery
-- mechanism. The hint table has zero rows in dev DB (no successful
-- discovery runs landed before the empirical finding).

DROP INDEX IF EXISTS idx_sec_13dg_discovery_issuer_hint_accession;
DROP INDEX IF EXISTS idx_sec_13dg_discovery_issuer_hint_instrument_id;
DROP TABLE IF EXISTS sec_13dg_discovery_issuer_hint;
