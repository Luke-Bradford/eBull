-- 216_prospectus_offerings.sql
--
-- Issue #1816 (child of #1015 item 2) — 424B prospectus offering parser.
--
-- Promotes the tier-1 (equity-likely) 424B subtypes (424B1/B3/B4/B5/B7) from
-- metadata-only to PARSE+RAW via a new manifest source ``sec_424b``. This
-- migration:
--
--   1. Creates ``prospectus_offerings`` — one row per parsed 424B prospectus,
--      keyed on accession_number (mirrors nt_filing_notices shape). Tombstones
--      live in the manifest (``sec_filing_manifest.ingest_status``), not here.
--   2. Widens three CHECK constraints for the new source/kind:
--      - filing_raw_documents.document_kind  += 'prospectus_body'
--      - sec_filing_manifest.source          += 'sec_424b'
--      - data_freshness_index.source         += 'sec_424b'
--
-- Source rule: Securities Act Rule 424(b)(1)-(8) (17 CFR 230.424(b)) — the
-- subtype is a filing-trigger bucket, not an instrument taxonomy. Extracted
-- fields come from the Reg S-K Item 501(b)(3) cover disclosure (17 CFR
-- 229.501(b)(3): Price to Public / Underwriting Discounts and Commissions /
-- Proceeds to Issuer|Selling Shareholders), which is best-effort (a table is
-- NOT mandated) — every money field is nullable; NULL means "not resolvable
-- from the cover", never a guessed value. 424B2/424B8 stay metadata-only
-- (deferred on yield — B2 volume here is bank/ETN structured-note takedowns).

BEGIN;

CREATE TABLE IF NOT EXISTS prospectus_offerings (
    accession_number            TEXT        PRIMARY KEY,
    instrument_id               BIGINT      NOT NULL
                                  REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    -- Rule 424(b) paragraph the filing was made under. From the manifest form
    -- (authoritative). Tier-1 scope only; widen when a B2/B8 parser lands.
    subtype                     TEXT        NOT NULL
                                  CHECK (subtype IN ('424B1', '424B3', '424B4', '424B5', '424B7')),
    -- Derived from the cover proceeds rows: an issuer-proceeds row present ⇒
    -- TRUE; only a selling-shareholders row ⇒ FALSE; unresolved ⇒ NULL.
    -- NEVER inferred from the subtype (a B7 can carry issuer proceeds).
    is_issuer_offering          BOOLEAN,
    -- Item 501(b)(3) "Price to Public" per-unit cell. NULL when the cover
    -- prices as a range, as percent-of-principal (structured notes), or the
    -- presentation is unresolvable.
    price_per_unit              NUMERIC,
    -- The per-unit row/column label ("Per Share" / "Per Note" / "Per ADS" …).
    unit_label                  TEXT,
    -- Item 501(b)(3) "Price to Public" total (gross offering size). Never
    -- computed as price × share-count (cover counts often exclude
    -- over-allotment; a computed total would fabricate a figure).
    aggregate_offering_amount   NUMERIC,
    -- "Underwriting Discounts and Commissions" total.
    underwriting_discount       NUMERIC,
    -- "Proceeds to <issuer>" total. NULL when absent — NOT hard-zeroed.
    net_proceeds_to_issuer      NUMERIC,
    -- "Proceeds to Selling Shareholders" total, when disclosed.
    proceeds_to_selling_holders NUMERIC,
    currency                    TEXT        NOT NULL DEFAULT 'USD'
                                  CHECK (currency IN ('USD', 'EUR', 'GBP', 'CAD')),
    -- Coarse cover-title label ("Common Stock" / "Notes" / "ADSs" …).
    -- Advisory display only; drives no semantic flag.
    security_type               TEXT,
    parser_version              INT         NOT NULL DEFAULT 1,
    parsed_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_prospectus_offerings_instrument
    ON prospectus_offerings (instrument_id, parsed_at DESC);

COMMENT ON TABLE prospectus_offerings IS
    'Parsed 424B prospectus cover offerings (Reg S-K Item 501(b)(3)). One row '
    'per accession; tombstones live in sec_filing_manifest. Source #1816.';

-- ---------------------------------------------------------------------------
-- Widen the three source/kind CHECK constraints for the new ``sec_424b``
-- source. Full enum lists carried forward from sql/211 (the latest widening).
-- ---------------------------------------------------------------------------

ALTER TABLE filing_raw_documents
    DROP CONSTRAINT IF EXISTS filing_raw_documents_document_kind_check;
ALTER TABLE filing_raw_documents
    ADD CONSTRAINT filing_raw_documents_document_kind_check
    CHECK (document_kind IN (
        'primary_doc',
        'infotable_13f',
        'primary_doc_13dg',
        'form4_xml',
        'form3_xml',
        'form5_xml',
        'def14a_body',
        'nport_xml',
        'finra_short_interest_csv',
        'finra_regsho_daily_txt',
        'nt_body',
        'pre14a_body',
        'prospectus_body'
    ));

ALTER TABLE sec_filing_manifest
    DROP CONSTRAINT IF EXISTS sec_filing_manifest_source_check;
ALTER TABLE sec_filing_manifest
    ADD CONSTRAINT sec_filing_manifest_source_check
    CHECK (source IN (
        'sec_form3',
        'sec_form4',
        'sec_form5',
        'sec_13d',
        'sec_13g',
        'sec_13f_hr',
        'sec_def14a',
        'sec_n_port',
        'sec_n_csr',
        'sec_10k',
        'sec_10q',
        'sec_8k',
        'sec_xbrl_facts',
        'finra_short_interest',
        'finra_regsho_daily',
        'sec_nt',
        'sec_pre14a',
        'sec_424b'
    ));

ALTER TABLE data_freshness_index
    DROP CONSTRAINT IF EXISTS data_freshness_index_source_check;
ALTER TABLE data_freshness_index
    ADD CONSTRAINT data_freshness_index_source_check
    CHECK (source IN (
        'sec_form3',
        'sec_form4',
        'sec_form5',
        'sec_13d',
        'sec_13g',
        'sec_13f_hr',
        'sec_def14a',
        'sec_n_port',
        'sec_n_csr',
        'sec_10k',
        'sec_10q',
        'sec_8k',
        'sec_xbrl_facts',
        'finra_short_interest',
        'finra_regsho_daily',
        'sec_nt',
        'sec_pre14a',
        'sec_424b'
    ));

COMMIT;
