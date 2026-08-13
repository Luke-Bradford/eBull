-- Migration 069 — reclassify exchanges from real eToro descriptions (#514).
--
-- Migration 067 manually seeded exchange_ids 1-7, 19, 20 as
-- ``asset_class = 'us_equity'`` based on the SEC mapper's hardcoded
-- list (#496). After PR #513 populated ``exchanges.description`` from
-- the live eToro API, the seed is visibly wrong:
--
--   1 → "FX"                (NOT us_equity → fx)
--   2 → "Commodity"         (NOT us_equity → commodity)
--   3 → "CFD"               (cross-asset wrapper → unknown for review)
--   6 → "FRA"               (Frankfurt → eu_equity / DE)
--   7 → "LSE"               (London → uk_equity / GB)
--
-- Without this fix the SEC mapper (``daily_cik_refresh``) attaches
-- US CIKs to commodities (Aluminum, .FUT contracts), Frankfurt
-- listings (.DE), and London listings (.L) — the same cross-source
-- leak #503 was meant to prevent. PR #496 + migration 066 cleared
-- the orphan SEC rows once, but the source pump kept running.
--
-- This migration also classifies the rows migration 068 left as
-- ``unknown`` because their dominant suffix didn't pass the >80%
-- gate (mostly small-universe exchanges where the heuristic
-- correctly stayed conservative). Real descriptions from the
-- eToro API let us classify them deterministically:
--
--   13 → "TYO"                       → asia_equity / JP
--   24 → "Tadawul"                   → mena_equity / SA
--   32 → "Vienna"                    → eu_equity / AT
--   34 → "Dublin EN"                 → eu_equity / IE
--   35 → "Prague SE"                 → eu_equity / CZ
--   36 → "Warsaw"                    → eu_equity / PL
--   37 → "Budapest"                  → eu_equity / HU
--   40 → "CME"                       → commodity / NULL
--   45 → "Shenzen Stock Exchange"    → asia_equity / CN
--   46 → "Shanghai Stock Exchange"   → asia_equity / CN
--   47 → "National Stock Exchange of India" → asia_equity / IN
--   49 → "Singapore Exchange"        → asia_equity / SG
--   50 → "Nasdaq Iceland"            → eu_equity / IS
--   51 → "Nasdaq Tallinn"            → eu_equity / EE
--   52 → "Nasdaq Vilnius"            → eu_equity / LT
--   53 → "Nasdaq Riga"               → eu_equity / LV
--   54 → "Korea Exchange"            → asia_equity / KR
--   55 → "Taiwan Stock Exchange"     → asia_equity / TW
--
-- Rows 18 (Toronto) and 48 (TSX Venture) stay ``unknown`` —
-- Canada's data landscape gets its own asset_class via the
-- workstream-2 Canada ticket (#523). Vocabulary extension waits
-- for that ticket so we don't pre-emptively bake a name the
-- operator hasn't approved.
--
-- Idempotency: each UPDATE filters on the CURRENT value, so re-
-- running this migration on a hand-edited DB (e.g. the operator
-- already corrected one row manually) doesn't clobber the
-- correction. The check ``WHERE asset_class = '<old>'`` means a
-- row that's already moved to its target value is a no-op.

BEGIN;

-- ---------------------------------------------------------------
-- Section 1 — fix the wrongly-seeded us_equity rows.
-- ---------------------------------------------------------------

-- 1 → FX
UPDATE exchanges
   SET asset_class = 'fx',
       country     = NULL,
       updated_at  = NOW()
 WHERE exchange_id = '1'
   AND asset_class = 'us_equity'
  ;

-- 2 → commodity
UPDATE exchanges
   SET asset_class = 'commodity',
       country     = NULL,
       updated_at  = NOW()
 WHERE exchange_id = '2'
   AND asset_class = 'us_equity'
  ;

-- 3 → unknown (CFD is cross-asset; defer to operator review)
UPDATE exchanges
   SET asset_class = 'unknown',
       country     = NULL,
       updated_at  = NOW()
 WHERE exchange_id = '3'
   AND asset_class = 'us_equity'
  ;

-- 6 → eu_equity / DE
UPDATE exchanges
   SET asset_class = 'eu_equity',
       country     = 'DE',
       updated_at  = NOW()
 WHERE exchange_id = '6'
   AND asset_class = 'us_equity'
  ;

-- 7 → uk_equity / GB
UPDATE exchanges
   SET asset_class = 'uk_equity',
       country     = 'GB',
       updated_at  = NOW()
 WHERE exchange_id = '7'
   AND asset_class = 'us_equity'
  ;

-- ---------------------------------------------------------------
-- Section 2 — classify previously-unknown rows from descriptions.
-- ---------------------------------------------------------------

UPDATE exchanges SET asset_class = 'asia_equity', country = 'JP', updated_at = NOW()
 WHERE exchange_id = '13' AND asset_class = 'unknown';

UPDATE exchanges SET asset_class = 'mena_equity', country = 'SA', updated_at = NOW()
 WHERE exchange_id = '24' AND asset_class = 'unknown';

UPDATE exchanges SET asset_class = 'eu_equity', country = 'AT', updated_at = NOW()
 WHERE exchange_id = '32' AND asset_class = 'unknown';

UPDATE exchanges SET asset_class = 'eu_equity', country = 'IE', updated_at = NOW()
 WHERE exchange_id = '34' AND asset_class = 'unknown';

UPDATE exchanges SET asset_class = 'eu_equity', country = 'CZ', updated_at = NOW()
 WHERE exchange_id = '35' AND asset_class = 'unknown';

UPDATE exchanges SET asset_class = 'eu_equity', country = 'PL', updated_at = NOW()
 WHERE exchange_id = '36' AND asset_class = 'unknown';

UPDATE exchanges SET asset_class = 'eu_equity', country = 'HU', updated_at = NOW()
 WHERE exchange_id = '37' AND asset_class = 'unknown';

UPDATE exchanges SET asset_class = 'commodity', country = NULL, updated_at = NOW()
 WHERE exchange_id = '40' AND asset_class = 'unknown';

UPDATE exchanges SET asset_class = 'asia_equity', country = 'CN', updated_at = NOW()
 WHERE exchange_id = '45' AND asset_class = 'unknown';

UPDATE exchanges SET asset_class = 'asia_equity', country = 'CN', updated_at = NOW()
 WHERE exchange_id = '46' AND asset_class = 'unknown';

UPDATE exchanges SET asset_class = 'asia_equity', country = 'IN', updated_at = NOW()
 WHERE exchange_id = '47' AND asset_class = 'unknown';

UPDATE exchanges SET asset_class = 'asia_equity', country = 'SG', updated_at = NOW()
 WHERE exchange_id = '49' AND asset_class = 'unknown';

UPDATE exchanges SET asset_class = 'eu_equity', country = 'IS', updated_at = NOW()
 WHERE exchange_id = '50' AND asset_class = 'unknown';

UPDATE exchanges SET asset_class = 'eu_equity', country = 'EE', updated_at = NOW()
 WHERE exchange_id = '51' AND asset_class = 'unknown';

UPDATE exchanges SET asset_class = 'eu_equity', country = 'LT', updated_at = NOW()
 WHERE exchange_id = '52' AND asset_class = 'unknown';

UPDATE exchanges SET asset_class = 'eu_equity', country = 'LV', updated_at = NOW()
 WHERE exchange_id = '53' AND asset_class = 'unknown';

UPDATE exchanges SET asset_class = 'asia_equity', country = 'KR', updated_at = NOW()
 WHERE exchange_id = '54' AND asset_class = 'unknown';

UPDATE exchanges SET asset_class = 'asia_equity', country = 'TW', updated_at = NOW()
 WHERE exchange_id = '55' AND asset_class = 'unknown';

COMMIT;
