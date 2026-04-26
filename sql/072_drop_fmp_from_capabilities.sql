-- Migration 072 — drop FMP from exchanges.capabilities (#532).
--
-- FMP is a paid third-party data provider; eBull stance is
-- free regulated-source-only. Migration 071 seeded us_equity
-- capabilities with FMP listed under ``fundamentals`` and
-- ``analyst``. This migration retracts that to keep the seed
-- consistent with the runtime capability resolver after the
-- FMP provider tag was removed from CAPABILITY_PROVIDERS.
--
-- Surgical: removes only the ``fmp`` element from each array.
-- Operator overrides that have added other providers (e.g.
-- ``["sec_xbrl", "fmp", "custom_provider"]``) keep their
-- non-fmp additions. Idempotent: rows that no longer contain
-- ``fmp`` are untouched.

BEGIN;

-- fundamentals: drop fmp element if present
UPDATE exchanges
   SET capabilities = jsonb_set(
           capabilities,
           '{fundamentals}',
           COALESCE(
               (SELECT jsonb_agg(elem)
                  FROM jsonb_array_elements(capabilities -> 'fundamentals') elem
                 WHERE elem <> '"fmp"'::jsonb),
               '[]'::jsonb
           )
       ),
       updated_at = NOW()
 WHERE capabilities ? 'fundamentals'
   AND (capabilities -> 'fundamentals') ? 'fmp';

-- analyst: drop fmp element if present
UPDATE exchanges
   SET capabilities = jsonb_set(
           capabilities,
           '{analyst}',
           COALESCE(
               (SELECT jsonb_agg(elem)
                  FROM jsonb_array_elements(capabilities -> 'analyst') elem
                 WHERE elem <> '"fmp"'::jsonb),
               '[]'::jsonb
           )
       ),
       updated_at = NOW()
 WHERE capabilities ? 'analyst'
   AND (capabilities -> 'analyst') ? 'fmp';

COMMIT;
