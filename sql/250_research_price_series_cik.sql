-- 250_research_price_series_cik.sql
--
-- #2282 stage 2b/2c prerequisite — give a research series somewhere to carry
-- a CIK.
--
--
-- WHY THIS IS NEEDED BEFORE THE FORM 25 REGISTER (2c)
-- ---------------------------------------------------------------------------
-- The Form 25 register is keyed on CIK: 17 CFR 240.12d2-2 filings identify the
-- issuer by CIK and carry no ticker at all (sec-edgar.md §2.6 trap 4 — SEC
-- drops `tickers` to `[]` on delisting and `companyconcept/…/dei/
-- TradingSymbol.json` 404s). So the register's natural join key to the corpus
-- is CIK.
--
-- `research_price_series` (sql/249) had nowhere to put one. Every CIK-bearing
-- table in this schema hangs off `instrument_id` (`instrument_sec_profile`
-- sql/051, `instrument_cik_history` sql/102) or off a filer
-- (`institutional_filers` sql/090, `blockholder_filers` sql/095). But the
-- entire 2c population is UNRESOLVED series — delisted companies have no
-- `instruments` row by construction, since eToro's book is survivors only.
-- Without this column the register can only join on the symbol string, which
-- is precisely the ticker-reuse hazard sql/249 documents at length: `SI`
-- (Silvergate, failed 2023) is a live ticker for a different company, and 10
-- of Yahoo's 48 hits on the 2023 cohort begin AFTER the delisting they are
-- supposed to record.
--
-- CIK is stable across delisting, ticker change and reuse. Symbol is not.
--
--
-- WHY TEXT AND NOT BIGINT
-- ---------------------------------------------------------------------------
-- Every CIK in this schema is TEXT, 10-digit zero-padded (verified on dev:
-- 5,339/5,339 `instrument_sec_profile` rows and 5,109/5,109
-- `instrument_cik_history` rows match `^[0-9]{10}$`). Storing it as BIGINT
-- here would make every join to those tables a cast, and casts on identity
-- columns are how identity bugs hide. The CHECK below pins the padding so a
-- caller cannot insert `1067983` alongside `0001067983` and create two
-- spellings of Berkshire.
--
--
-- WHY THERE IS NO UNIQUE (vendor, cik)
-- ---------------------------------------------------------------------------
-- Deliberately asymmetric with the `uq_research_price_series_vendor_instrument`
-- partial unique index sql/249 added. Two vendor symbols mapping to one
-- INSTRUMENT within a vendor is a resolver bug (the ticker-reuse pair). Two
-- vendor symbols mapping to one CIK is normal and correct: multi-class issuers
-- file under a single CIK (GOOG/GOOGL under 0001652044, BRK.A/BRK.B under
-- 0001067983). A unique index here would reject real data.

ALTER TABLE research_price_series
    ADD COLUMN IF NOT EXISTS cik        TEXT,
    ADD COLUMN IF NOT EXISTS cik_source TEXT;

-- Closed vocabulary, same shape as `resolution_method` / `delisting_source`.
--   sec_form25             — read off the Form 25 / 25-NSE filing itself (2c)
--   instrument_sec_profile — carried across from an already-resolved instrument
--   manual                 — operator-asserted, and therefore auditable as such
ALTER TABLE research_price_series
    DROP CONSTRAINT IF EXISTS research_price_series_cik_source_vocab;
ALTER TABLE research_price_series
    ADD CONSTRAINT research_price_series_cik_source_vocab
        CHECK (cik_source IS NULL
               OR cik_source IN ('sec_form25', 'instrument_sec_profile', 'manual'));

-- A CIK without its provenance is an unauditable join — the same invariant
-- sql/249 applies to `instrument_id`/`resolution_method` and to
-- `delisting_date`/`delisting_source`.
ALTER TABLE research_price_series
    DROP CONSTRAINT IF EXISTS research_price_series_cik_evidenced;
ALTER TABLE research_price_series
    ADD CONSTRAINT research_price_series_cik_evidenced
        CHECK ((cik IS NULL) = (cik_source IS NULL));

-- 10-digit zero-padded, matching every other CIK column in the schema.
ALTER TABLE research_price_series
    DROP CONSTRAINT IF EXISTS research_price_series_cik_padded;
ALTER TABLE research_price_series
    ADD CONSTRAINT research_price_series_cik_padded
        CHECK (cik IS NULL OR cik ~ '^[0-9]{10}$');

-- The register's read path: "which series belong to the issuer this Form 25
-- names". Partial — most series will never carry a CIK (non-US names have no
-- SEC identity at all).
CREATE INDEX IF NOT EXISTS idx_research_price_series_cik
    ON research_price_series (cik)
    WHERE cik IS NOT NULL;

COMMENT ON COLUMN research_price_series.cik IS
    'SEC CIK, 10-digit zero-padded TEXT to match instrument_sec_profile / '
    'instrument_cik_history. The join key for the Form 25 delisting register: '
    'CIK survives delisting, ticker change and ticker reuse, and a delisted '
    'name has no instruments row to borrow one from. Not unique per vendor — '
    'multi-class issuers file one CIK for several symbols.';

COMMENT ON COLUMN research_price_series.cik_source IS
    'How the CIK was established. A CIK without provenance is an unauditable '
    'join; CHECK-paired with cik, same invariant as instrument_id/'
    'resolution_method.';
