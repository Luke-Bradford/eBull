-- Migration 068 — auto-classify exchanges from symbol suffixes +
-- add instrument_type column for cross-validation (#503 PR 4).
--
-- After #503 PR 3 created the ``exchanges`` table with a manual
-- seed of 8 ids, every other id eToro uses landed as
-- ``asset_class = 'unknown'``. The operator asked for an audit;
-- inspection of the dev DB shows 35+ distinct exchange ids each
-- with a recognisable symbol-suffix convention:
--
--   .L      → London Stock Exchange
--   .DE     → Frankfurt / XETRA
--   .PA     → Euronext Paris
--   .HK     → Hong Kong Stock Exchange
--   .T      → Tokyo Stock Exchange
--   .ASX    → Australian Securities Exchange
--   …       and many more
--
-- This migration:
--
--   1. Auto-classifies every ``exchanges`` row whose
--      ``asset_class = 'unknown'`` based on the symbol suffix
--      observed on its instruments. The classification is the
--      single most-frequent suffix in instruments rows tagged
--      with that exchange_id.
--   2. Adds ``instrument_type`` TEXT column to ``instruments`` so
--      the universe upsert (PR 4 backend changes) can capture
--      eToro's ``instrumentTypeID``. Cross-validates against the
--      exchange-level classification: a stock-typed instrument on
--      a crypto-classified exchange is a data integrity flag.
--
-- The classification table here is deliberately conservative —
-- only suffixes with enough confidence get a real class. An id
-- whose symbols don't share a recognisable suffix (mixed bag,
-- e.g. crypto-like raw tickers) stays ``unknown`` so the operator
-- can review.

BEGIN;

-- ---------------------------------------------------------------
-- 0. Extend the asset_class vocabulary.
-- ---------------------------------------------------------------
-- Migration 067 fixed the vocabulary at 9 values. The suffix-based
-- classifier below recognises ``.DH`` and ``.AE`` (Dubai / Abu Dhabi)
-- so we extend the constraint to allow ``mena_equity`` before any
-- update tries to write that value. Drop + re-add is the standard
-- Postgres pattern for editing a CHECK list.

ALTER TABLE exchanges
    DROP CONSTRAINT IF EXISTS exchanges_asset_class_check;

ALTER TABLE exchanges
    ADD CONSTRAINT exchanges_asset_class_check CHECK (asset_class IN (
        'us_equity', 'crypto', 'eu_equity', 'uk_equity', 'asia_equity',
        'mena_equity', 'commodity', 'fx', 'index', 'unknown'
    ));

-- ---------------------------------------------------------------
-- 1. Auto-classify exchanges from observed suffixes.
-- ---------------------------------------------------------------

-- Helper temp table: per-exchange most-common suffix + sample size.
-- Suffix = everything after the last ``.`` in symbol; if no ``.``,
-- we fall back to detecting common patterns (FX pair / crypto
-- token / commodity name) via length + character heuristics.
WITH suffix_counts AS (
    SELECT
        i.exchange AS exchange_id,
        CASE
            WHEN POSITION('.' IN i.symbol) > 0
                THEN UPPER(SPLIT_PART(REVERSE(i.symbol), '.', 1))
            ELSE NULL
        END AS suffix,
        COUNT(*) AS n
    FROM instruments i
    WHERE i.exchange IS NOT NULL
    GROUP BY 1, 2
),
-- Pick the dominant suffix per exchange.
ranked AS (
    SELECT exchange_id, suffix, n,
           ROW_NUMBER() OVER (PARTITION BY exchange_id ORDER BY n DESC) AS rn,
           SUM(n) OVER (PARTITION BY exchange_id) AS total_n
    FROM suffix_counts
),
-- Dominance gate: pick the top suffix per exchange ONLY if it covers
-- more than 80% of that exchange's instruments and ties (a second
-- suffix with the same n) are absent. A 51% plurality on a mixed bag
-- (e.g. half ``.L`` half ``.MI``) must NOT classify — the comment at
-- the top of this migration promises operator review for those, and
-- a wrong country/asset_class downstream would gate the SEC mapper
-- on a false positive.
dominant AS (
    SELECT
        r.exchange_id,
        REVERSE(r.suffix) AS suffix,
        r.n,
        r.total_n
    FROM ranked r
    WHERE r.rn = 1
      AND r.n::numeric / NULLIF(r.total_n, 0) > 0.80
      -- Exclude tied winners: if a second row in `ranked` shares the
      -- same n, ROW_NUMBER picks one arbitrarily — refuse to act.
      AND NOT EXISTS (
          SELECT 1 FROM ranked r2
          WHERE r2.exchange_id = r.exchange_id
            AND r2.rn = 2
            AND r2.n = r.n
      )
)
UPDATE exchanges e
   SET asset_class = m.asset_class,
       country     = m.country,
       updated_at  = NOW()
  FROM (
      SELECT d.exchange_id,
             CASE
                 -- US: no suffix on most rows AND total n is large
                 -- (NASDAQ/NYSE main listings).
                 WHEN d.suffix IS NULL AND d.total_n > 30 THEN 'us_equity'
                 -- UK / European / Asian / Middle-Eastern / AU stocks
                 -- by suffix.
                 WHEN d.suffix = 'L'    THEN 'uk_equity'
                 WHEN d.suffix = 'DE'   THEN 'eu_equity'
                 WHEN d.suffix = 'PA'   THEN 'eu_equity'
                 WHEN d.suffix = 'ST'   THEN 'eu_equity'
                 WHEN d.suffix = 'OL'   THEN 'eu_equity'
                 WHEN d.suffix = 'IM'   THEN 'eu_equity'
                 WHEN d.suffix = 'MI'   THEN 'eu_equity'
                 WHEN d.suffix = 'HE'   THEN 'eu_equity'
                 WHEN d.suffix = 'NV'   THEN 'eu_equity'
                 WHEN d.suffix = 'AS'   THEN 'eu_equity'
                 WHEN d.suffix = 'CO'   THEN 'eu_equity'
                 WHEN d.suffix = 'BR'   THEN 'eu_equity'
                 WHEN d.suffix = 'MC'   THEN 'eu_equity'
                 WHEN d.suffix = 'ZU'   THEN 'eu_equity'
                 WHEN d.suffix = 'LS'   THEN 'eu_equity'
                 WHEN d.suffix = 'LSB'  THEN 'eu_equity'
                 WHEN d.suffix = 'HK'   THEN 'asia_equity'
                 WHEN d.suffix = 'T'    THEN 'asia_equity'
                 WHEN d.suffix = 'ASX'  THEN 'asia_equity'
                 WHEN d.suffix = 'DH'   THEN 'mena_equity'
                 WHEN d.suffix = 'AE'   THEN 'mena_equity'
                 WHEN d.suffix = 'RTH'  THEN 'us_equity'  -- US extended-hours
                 WHEN d.suffix = 'FUT'  THEN 'commodity'
                 ELSE NULL
             END AS asset_class,
             CASE
                 WHEN d.suffix = 'L'    THEN 'GB'
                 WHEN d.suffix = 'DE'   THEN 'DE'
                 WHEN d.suffix = 'PA'   THEN 'FR'
                 WHEN d.suffix = 'ST'   THEN 'SE'
                 WHEN d.suffix = 'OL'   THEN 'NO'
                 WHEN d.suffix IN ('IM', 'MI') THEN 'IT'
                 WHEN d.suffix = 'HE'   THEN 'FI'
                 WHEN d.suffix IN ('NV', 'AS') THEN 'NL'
                 WHEN d.suffix = 'CO'   THEN 'DK'
                 WHEN d.suffix = 'BR'   THEN 'BE'
                 WHEN d.suffix = 'MC'   THEN 'ES'
                 WHEN d.suffix = 'ZU'   THEN 'CH'
                 WHEN d.suffix IN ('LS', 'LSB') THEN 'PT'
                 WHEN d.suffix = 'HK'   THEN 'HK'
                 WHEN d.suffix = 'T'    THEN 'JP'
                 WHEN d.suffix = 'ASX'  THEN 'AU'
                 WHEN d.suffix = 'DH'   THEN 'AE'
                 WHEN d.suffix = 'AE'   THEN 'AE'
                 WHEN d.suffix IS NULL AND d.total_n > 30 THEN 'US'
                 WHEN d.suffix = 'RTH'  THEN 'US'
                 ELSE NULL
             END AS country
      FROM dominant d
  ) AS m
 WHERE e.exchange_id = m.exchange_id
   AND e.asset_class = 'unknown'
   AND m.asset_class IS NOT NULL;

-- Manual classification for ids that don't fit the suffix
-- heuristic but are well-known by sample-symbol shape. Each UPDATE
-- is gated on ``asset_class = 'unknown'`` so a manually-curated DB
-- (e.g. one where the operator already classified id 1) is never
-- overwritten — only fresh seeds from #503 PR 3 land here.
--
-- Note ids 2, 19, 20 are ALREADY seeded as ``us_equity`` by #503 PR 3
-- (the eight US-equity ids the SEC mapper has used since #496) and
-- therefore the WHERE clauses below will never match them on a
-- normally-migrated DB. They are listed here as a belt-and-braces
-- backstop for a hand-edited DB where the operator demoted one of
-- those rows to ``unknown`` before re-running migrations.
--
--   1  → FX pairs (AUDCAD, AUDJPY)
--   3  → indices / themed baskets (AI.Leaders, AUS200)
--   8  → crypto (already seeded by #503 PR 3)
--   40 → futures (.FUT — caught by suffix rule above as commodity)
UPDATE exchanges SET asset_class = 'fx',        country = NULL, updated_at = NOW()
    WHERE exchange_id = '1'  AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'index',     country = NULL, updated_at = NOW()
    WHERE exchange_id = '3'  AND asset_class = 'unknown';
-- Backstop UPDATEs (no-op on a normally-seeded DB; see note above):
UPDATE exchanges SET asset_class = 'commodity', country = NULL, updated_at = NOW()
    WHERE exchange_id = '2'  AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'us_equity', country = 'US', updated_at = NOW()
    WHERE exchange_id = '19' AND asset_class = 'unknown';
UPDATE exchanges SET asset_class = 'us_equity', country = 'US', updated_at = NOW()
    WHERE exchange_id = '20' AND asset_class = 'unknown';

-- ---------------------------------------------------------------
-- 2. Add instrument_type column for the per-instrument cross-check.
-- ---------------------------------------------------------------
--
-- eToro's instruments endpoint returns ``instrumentTypeID`` (int)
-- + ``instrumentTypeName`` (string) per row. We capture the
-- string form so the column is human-readable in operator queries
-- and so a mismatched int → name mapping in eToro's catalog
-- doesn't leave us with a stale int. The universe upsert (PR 4
-- backend changes) populates this column on every refresh.

ALTER TABLE instruments
    ADD COLUMN IF NOT EXISTS instrument_type TEXT;

COMMENT ON COLUMN instruments.instrument_type IS
    'eToro instrumentTypeName (Stock / Crypto / ETF / Index / Currency / '
    'Commodity / etc.) — captured from /api/v1/market-data/instruments. '
    'Cross-validates against exchanges.asset_class; a stock-typed instrument '
    'on a crypto-classified exchange is a data integrity flag.';

CREATE INDEX IF NOT EXISTS idx_instruments_instrument_type
    ON instruments (instrument_type);

COMMIT;
