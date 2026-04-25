-- Migration 065 — purge bogus crypto SEC CIK mappings (#475).
--
-- Before #475's mapper scope fix, daily_cik_refresh matched every
-- tradable instrument by symbol against SEC's company_tickers.json.
-- SEC's ticker list covers US-registered companies only; when a
-- crypto coin shares a ticker with a US listing (BTC, BCH, ATOM,
-- COMP, ...) the mapper blindly stamped the unrelated CIK onto
-- the crypto row. 47 instruments on dev had this bad mapping,
-- which in turn caused the SEC profile + business-summary panels
-- on the crypto page to render data for a completely different
-- company (e.g. Grayscale Bitcoin Mini Trust on the BTC coin
-- page).
--
-- Purge every external_identifiers row that pairs a crypto
-- instrument (exchange = '8') with a SEC CIK. The mapper's new
-- exchange filter prevents re-introduction on the next daily run.
-- instrument_sec_profile rows for these instruments stay in place
-- but become orphaned (no external_identifiers link); a separate
-- cleanup will surface them as "no mapping" on the API layer, and
-- the next daily_cik_refresh cycle won't re-link them because the
-- source tuple is no longer in scope.
--
-- This migration is idempotent: re-running on a clean DB is a
-- zero-row delete.

DELETE FROM external_identifiers e
USING instruments i
WHERE e.instrument_id = i.instrument_id
  AND i.exchange = '8'
  AND e.provider = 'sec'
  AND e.identifier_type = 'cik';

-- Also drop the orphaned profile rows so the SEC-profile endpoint
-- cleanly 404s for these crypto instruments instead of rendering
-- stale data from a prior (now-purged) CIK binding. Narrowly scoped
-- to rows whose external_identifiers link was just removed — any
-- crypto profile row that still has a SEC CIK link (none exist
-- today, but defensive) is left alone so this migration cannot
-- silently over-delete.
DELETE FROM instrument_sec_profile p
USING instruments i
WHERE p.instrument_id = i.instrument_id
  AND i.exchange = '8'
  AND NOT EXISTS (
      SELECT 1 FROM external_identifiers e
      WHERE e.instrument_id = p.instrument_id
        AND e.provider = 'sec'
        AND e.identifier_type = 'cik'
  );
