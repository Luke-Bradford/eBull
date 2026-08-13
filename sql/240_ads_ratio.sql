-- 240_ads_ratio.sql
--
-- #2117 (#1939 step-2): curated ADS-ratio reference table — ordinary shares
-- per 1 American Depositary Share. Corrects the FPI/ADR market-cap residual
-- that sql/237 (#1939 step-1) could only SUPPRESS: the SEC DEI share count is
-- the issuer's ORDINARY count, the tradable price is PER-ADS, so any
-- ordinary-shares × ADS-price product overstates by the ADS ratio.
--
-- Source rule: the ADS ratio is fixed in the Form F-6 registration statement /
-- deposit agreement (Securities Act Rule 466 / Form F-6); the 20-F cover
-- restates it. NO XBRL tag exists. Standard-filing reuse check (mandated):
-- edgartools 5.30.2 has no F-6 parser (registration coverage stops at
-- S-1/F-1/S-3/424B/497K) and F-6 is not linked to the scored ADR instruments
-- in filing_events — so this is a CURATED reference table, not a parser
-- (docs/specs/etl/2026-07-24-ads-ratio-adr-caps.md).
--
-- Keyed by instrument_id (not CIK): a ratio applies to the specific ADS
-- listing, not all of an issuer's listings — CIK-keying would mis-apply to a
-- same-CIK ordinary listing. Membership ALSO serves as detection: it catches
-- the domestic-form ADR filers (AKTX) the Rule 3b-4 fpi fingerprint + name
-- marker miss.
--
-- CURRENT-ratio contract: this table holds ONLY the ratio in force NOW.
-- Consumers compute only CURRENT figures (current price × current shares).
-- Ratios change over time (AKTX changed to 2000:1 in 2023; ZLAB was 1:1
-- pre-2022) — effective_date / source_date support periodic manual
-- re-verification. Do NOT use for historical-period recompute; a valid_to /
-- history table is a future concern if historical ADR recompute is ever needed.

BEGIN;

CREATE TABLE IF NOT EXISTS ads_ratio (
    instrument_id    BIGINT PRIMARY KEY REFERENCES instruments (instrument_id) ON DELETE CASCADE,
    ratio            NUMERIC NOT NULL CHECK (ratio > 0),  -- ordinary shares per 1 ADS
    effective_date   DATE,        -- when this ratio took effect
    source_form      TEXT,        -- e.g. 'F-6 POS', '20-F'
    source_accession TEXT,        -- EDGAR accession of the evidence
    source_date      DATE,        -- filing date of the evidence
    note             TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE ads_ratio IS
    '#2117 curated CURRENT ADS ratio (ordinary shares per 1 ADS). CURRENT-only; '
    'do not use for historical recompute. Membership also detects domestic-form ADR filers.';

-- Seed the full-pop scored ADR population (dev scan: the only scored ADR
-- instruments that render a market cap). Symbol-resolved so it is portable
-- across DBs and a no-op where the instrument is absent. Idempotent.
-- ``symbol`` is NOT unique (sql/043 documents collisions) — resolve to the
-- PRIMARY listing only so a collision cannot divide a non-ADS sibling's price.
INSERT INTO ads_ratio (instrument_id, ratio, effective_date, source_form, source_accession, source_date, note)
SELECT instrument_id, v.ratio, v.effective_date, v.source_form, v.source_accession, v.source_date, v.note
FROM instruments i
JOIN (
    VALUES
        ('AKTX', 2000::numeric, DATE '2023-08-17', 'F-6 POS', '0000919574-23-004884', DATE '2023-08-17',
         'Akari Therapeutics: 1 ADS = 2000 ordinary (F-6 POS ratio change, Deutsche Bank depositary)'),
        ('ONC',    13::numeric, NULL,               'F-6',     NULL,                    DATE '2025-05-01',
         'BeiGene/ONC: 1 ADS = 13 ordinary (F-6 "thirteen (13) ordinary shares")'),
        ('ZLAB',   10::numeric, NULL,               '20-F',    NULL,                    NULL,
         'Zai Lab: 1 ADS = 10 ordinary (was 1:1 pre-2022; cap-math 1,110M/10=111M ADS)'),
        ('TEVA',    1::numeric, NULL,               'F-6',     NULL,                    DATE '2023-02-08',
         'Teva: 1 ADS = 1 ordinary (F-6 "one (1) ordinary share")'),
        ('CRTO',    1::numeric, NULL,               '20-F',    NULL,                    DATE '2015-03-27',
         'Criteo: 1 ADS = 1 ordinary (20-F "Each ADS represents one ordinary share")')
) AS v (symbol, ratio, effective_date, source_form, source_accession, source_date, note)
  ON v.symbol = i.symbol AND i.is_primary_listing
ON CONFLICT (instrument_id) DO UPDATE SET
    ratio            = EXCLUDED.ratio,
    effective_date   = EXCLUDED.effective_date,
    source_form      = EXCLUDED.source_form,
    source_accession = EXCLUDED.source_accession,
    source_date      = EXCLUDED.source_date,
    note             = EXCLUDED.note,
    updated_at       = now();

COMMIT;
