-- 051_instrument_sec_profile.sql
--
-- SEC entity metadata extracted from submissions.json
-- (#427 — audit 2026-04-24 identified 8+ rich fields per filer that we
-- already pull daily but discard after parsing the filings array).
--
-- No new HTTP path — ``_run_cik_upsert`` already has the submissions
-- dict in memory (see app/services/fundamentals.py). This table is
-- the normalised landing spot; raw payloads remain gated by the
-- retention sweep (#325 flipped).
--
-- Surfaces on the instrument page as:
--   - Business description (replaces yfinance long_business_summary)
--   - SIC sector + industry (replaces yfinance sector/industry for US)
--   - Website link, stock exchanges, filer size tier
--   - Former-names timeline (badge on legacy symbols)
--   - "Has insider activity" flag (gates Form 4 widget, #429)

CREATE TABLE IF NOT EXISTS instrument_sec_profile (
    instrument_id         BIGINT PRIMARY KEY REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    cik                   TEXT NOT NULL,              -- 10-digit zero-padded
    sic                   TEXT,                       -- 4-digit SIC code
    sic_description       TEXT,                       -- "Industrial Inorganic Chemicals"
    owner_org             TEXT,                       -- "08 Industrial Applications and Services"
    description           TEXT,                       -- entity-level description blurb (rarely populated)
    website               TEXT,
    investor_website      TEXT,
    ein                   TEXT,                       -- Employer ID Number
    lei                   TEXT,                       -- Legal Entity Identifier (ISO 17442)
    state_of_incorporation       TEXT,
    state_of_incorporation_desc  TEXT,
    fiscal_year_end       TEXT,                       -- "0930" = Sept 30 YE
    category              TEXT,                       -- filer-size tier, e.g. "Large accelerated filer"
    exchanges             TEXT[],                     -- ["NYSE"] / ["NASDAQ"]
    former_names          JSONB,                      -- [{name, from, to}, …]
    -- Boolean flags hoisted from submissions.json. Integer in source,
    -- stored as BOOLEAN for consumer ergonomics. NULL when the source
    -- omitted the key (pre-2020 filers for the insider flags).
    has_insider_issuer    BOOLEAN,
    has_insider_owner     BOOLEAN,
    fetched_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE instrument_sec_profile IS
    'Normalised SEC entity metadata per tradable instrument. One row '
    'per instrument_id. Populated by the fundamentals_sync job from '
    'submissions.json (already pulled); zero new HTTP. Operator-facing '
    'description + sector for US tickers.';

-- Index not needed on SIC — all reads go via instrument_id PK — but
-- a CIK-based lookup is useful for reverse-mapping during dev, and
-- the row count stays bounded (<5k live instruments on the roadmap).
CREATE INDEX IF NOT EXISTS idx_instrument_sec_profile_cik
    ON instrument_sec_profile(cik);
