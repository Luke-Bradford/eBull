-- 405_research_corpus_split_stamps.sql
--
-- #2834 ARM B, §7 item 2 slice A — store the per-bar corporate-action stamps
-- the Intrader archive already ships and nothing has ever read.
--
--
-- 1. WHY THIS EXISTS
-- ---------------------------------------------------------------------------
-- `b65abd9c` measured the signal basis for ARM B and found `icyDenev/Intrader`
-- ships a per-bar SPLIT RATIO in CSV field 6 and a cash dividend in field 7.
-- `_INTRADER_COLUMNS` has named both since #2398 and the loader stored neither,
-- so the only split-aware column in the corpus for this vendor is `adj_close`,
-- which carries the split AND the dividend adjustment fused together and
-- therefore cannot produce the PRICE return §4 requires.
--
-- Measured on the full mirror (22,879 CSVs, 50,134,060 rows; reproduce with
-- `scripts/measure_2834_split_stamp_load.py --census`):
--
--     split factor = 1            50,124,706
--     split factor <> 1                9,354   across 4,219 of 22,879 series
--     non-positive / unparseable           0
--     duplicate (symbol, date)             0
--     stamp on a bar with no close         0
--     dividend <> 0                  460,693   (one of them NEGATIVE, see below)
--
--
-- 2. WHY RAW STAMPS AND NOT A CORRECTED `close`
-- ---------------------------------------------------------------------------
-- `d15e680e` settled the correction POLICY (`apply_all`) and the correction is
-- a DERIVATION: `close / scale(d)`, where `scale(d)` is the product of the
-- factors of every event strictly after `d`. Materialising that as a column
-- would store a second copy of every price whose correctness depends on a
-- policy decision recorded in a different file, and a policy change would then
-- leave 50.1M silently stale rows. The raw close is an observation; the
-- corrected close is an opinion about it. Only the observation is stored.
--
-- This also keeps `research_price_series.adjustment_basis = 'unadjusted'` true
-- of the OHLC columns after the load, which it would not be if the load
-- rewrote them.
--
--
-- 3. WHY THE SERIES-LEVEL MARKER — THE `COALESCE(split_factor, 1)` TRAP
-- ---------------------------------------------------------------------------
-- `split_factor` must be nullable: the other loaded vendor
-- (`paperswithbacktest/Stocks-Daily-Price`, 25.8M rows) is a Parquet archive
-- with no stamp columns at all, so every one of its bars is NULL here.
--
-- A reader deriving a scale will reach for `COALESCE(split_factor, 1)`, and
-- that expression reads "this vendor ships no stamps" EXACTLY like "no split
-- happened on this bar" — silently converting a vendor with unknown corporate
-- actions into a vendor with none. That is the same shape as the defect this
-- ticket exists to fix (#2834: `sql/251` verified `split_adjusted` on one
-- vendor, s2 cited it as a source rule, and the backtest universe is the other
-- vendor). `corporate_action_stamps` is the column that makes the two states
-- distinguishable, and a derivation that does not read it is wrong by
-- construction.
--
-- ⚠ It is written at the END of the bar pass (`_write_census`), NOT alongside
-- `adjustment_basis` in the symbol pass, so that "marker says vendor_supplied"
-- implies "bars were written". A load that dies between the two passes leaves
-- the marker at 'absent', which understates coverage and cannot mislead a
-- divider. The opposite ordering fails in the dangerous direction.
--
--
-- 4. INVARIANTS, AND THE ONE THAT IS DELIBERATELY NOT ASSERTED
-- ---------------------------------------------------------------------------
-- `split_factor > 0` is a CHECK because the derivation DIVIDES by a product of
-- these values: a zero factor is a division by zero and a negative one flips
-- the sign of every price before the event. The full mirror contains neither,
-- so this CHECK cannot fire from today's loader — it is there for the next
-- writer, and `parse_intrader_rows` independently reads a non-positive factor
-- as ABSENT rather than letting it reach the table.
--
-- ⚠ `> 0` ALONE IS NOT ENOUGH, and this is a Postgres fact rather than a
-- theoretical one. `NUMERIC` has a NaN and (since PG14) an infinity, and
-- Postgres orders NaN as GREATER THAN every non-NaN value, so measured on the
-- test cluster:
--
--     select 'NaN'::numeric > 0, 'Infinity'::numeric > 0   ->   t | t
--
-- Both would pass a bare `> 0` and then propagate through a cumulative product
-- into NaN prices — exactly the next-writer scenario this guard exists for.
-- `<> 'NaN'::numeric` excludes NaN (Postgres defines NaN = NaN as true, so the
-- inequality is the working test) and `< 'Infinity'::numeric` excludes +Inf.
-- `-Infinity` is already excluded by `> 0`. Caught by Codex checkpoint 2.
--
-- `dividend` gets NO sign CHECK, and that is measured rather than lax. AGII
-- 2016-05-27 carries `-4.80545454545455` — one row in 460,693. A CHECK would
-- abort a 50.1M-row load over a single vendor artefact in a column nothing
-- reads yet; the honest treatment is to store what the vendor published and
-- report it by name (`--census`). ⚠ Whoever first CONSUMES field 7 owns
-- deciding what a negative distribution means; storing it does not.
--
-- One stamp per bar needs no new constraint: `research_price_daily`'s primary
-- key is already `(series_id, bar_date)`.

-- ⚠ Same lock discipline as sql/400, and for the same reason: this table is
-- 9.3 GB / 76.0M rows and a long reader on it is the NORMAL state, not an
-- unlucky collision. `lock_timeout` turns an unbounded stall-everything wait
-- into a clean, retryable LockNotAvailable — and since the FastAPI lifespan
-- runs migrations, a locked-out migration fails the boot loudly instead of
-- hanging it.
SET LOCAL lock_timeout = '5s';

ALTER TABLE research_price_daily
    ADD COLUMN IF NOT EXISTS split_factor NUMERIC,
    ADD COLUMN IF NOT EXISTS dividend     NUMERIC;

-- NOT VALID, and it costs nothing to be — sql/400's idiom, same shape.
-- A validated CHECK scans all 76.0M rows while still holding the ACCESS
-- EXCLUSIVE lock the column ALTER just took, to discover nothing: every
-- pre-existing row receives NULL for a column added in this same statement, so
-- the constraint is satisfied by construction. NOT VALID still enforces on
-- every INSERT and UPDATE from here on, which is the whole point — the
-- re-load's 50.1M writes go through it. No follow-up VALIDATE is needed.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'research_price_daily_split_factor_positive'
    ) THEN
        ALTER TABLE research_price_daily
            ADD CONSTRAINT research_price_daily_split_factor_positive
            CHECK (
                split_factor IS NULL
                OR (    split_factor > 0
                    AND split_factor <> 'NaN'::numeric
                    AND split_factor < 'Infinity'::numeric)
            ) NOT VALID;
    END IF;
END
$$;

COMMENT ON COLUMN research_price_daily.split_factor IS
    'The vendor''s per-bar split ratio, as published, stamped ON the effective '
    'bar (AAPL 2020-08-31 = 4). 1 means no event; NULL means the vendor ships '
    'no stamps — read research_price_series.corporate_action_stamps to tell '
    'those apart, because COALESCE(split_factor, 1) cannot. The split-adjusted '
    'close is close / (product of the factors of every event strictly after '
    'the bar); it is NOT stored (see this migration''s header).';

COMMENT ON COLUMN research_price_daily.dividend IS
    'The vendor''s per-bar cash distribution, as published. 0 means none; NULL '
    'means the vendor ships no stamps. Nothing consumes this yet: #2834 §4 '
    'wants a PRICE return, so the split factor is the signal input and this '
    'column is the other half of what adj_close fuses together — it is stored '
    'so the two components are separable rather than inferred. Sign is NOT '
    'constrained: AGII 2016-05-27 publishes a negative value.';

-- ---------------------------------------------------------------------------
-- The marker
-- ---------------------------------------------------------------------------
--
-- DEFAULT 'absent' is the safe direction for the 30,591 series that already
-- exist: until a vendor's bars are re-loaded, no stamp has been stored for it
-- and 'absent' is the literal truth.

ALTER TABLE research_price_series
    ADD COLUMN IF NOT EXISTS corporate_action_stamps TEXT NOT NULL DEFAULT 'absent';

-- Validated, not NOT VALID: 30,591 rows, and every one of them was just given
-- the DEFAULT by the statement above, so the scan is instant and there is
-- nothing to repair. The NOT VALID treatment above is about 76.0M rows, not
-- about the constraint being weaker.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'research_price_series_corporate_action_stamps'
    ) THEN
        ALTER TABLE research_price_series
            ADD CONSTRAINT research_price_series_corporate_action_stamps
            CHECK (corporate_action_stamps IN ('vendor_supplied', 'absent'));
    END IF;
END
$$;

COMMENT ON COLUMN research_price_series.corporate_action_stamps IS
    'Whether this series'' bars carry the vendor''s own split/dividend stamps. '
    'vendor_supplied: research_price_daily.split_factor and .dividend are '
    'populated for every bar. absent: they are NULL throughout and the series'' '
    'corporate actions are UNKNOWN, not nil. A derivation that divides by a '
    'split scale MUST read this first — COALESCE(split_factor, 1) reads the '
    'two states identically. Written at the end of the bar pass, so the marker '
    'implies the bars exist and never the reverse.';
