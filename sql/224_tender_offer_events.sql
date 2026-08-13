-- 224_tender_offer_events.sql
--
-- Issue #1982 (child of #1015 item 4) — tender / going-private parser.
--
-- Promotes the Schedule TO / 14D-9 / 13E-3 family (SC TO-T, SC TO-I,
-- SC 14D9, SC 13E3 + /A) from metadata-only to PARSE+RAW via a new manifest
-- source ``sec_tender``. This migration:
--
--   1. Creates ``tender_offer_events`` — one row per (accession, instrument).
--      ``sec_filing_manifest.accession_number`` is the PRIMARY KEY (sql/118),
--      so a dual-attributed accession (subject + offeror both in universe —
--      113 TO-T(/A) accessions land on >1 instrument via the master-index
--      path) gets ONE manifest row but one typed row per party here; ``role``
--      is derived from the EDGAR SGML header SUBJECT COMPANY / FILED BY CIK
--      blocks, never from which instrument happened to own the manifest row.
--   2. Widens three CHECK constraints for the new source/kind:
--      - filing_raw_documents.document_kind  += 'tender_body'
--      - sec_filing_manifest.source          += 'sec_tender'
--      - data_freshness_index.source         += 'sec_tender'
--
-- Source rule: Schedule TO (17 CFR 240.14d-100, Rules 14d-1(g)/13e-4),
-- Schedule 14D-9 (17 CFR 240.14d-101, Rule 14d-9), Schedule 13E-3 (17 CFR
-- 240.13e-100, Rule 13e-3). Content items per Reg M-A (17 CFR 229.1000-1016):
-- Item 1004(a)(1)(ii)/(v) mandate consideration + expiration CONTENT (prose
-- presentation — extraction is anchored-formula, nullable); Item 1012(a)
-- enumerates the only permitted 14D-9 board positions (closed 4-state enum).
-- Every extracted field is nullable-when-unresolved, never guessed.

BEGIN;

CREATE TABLE IF NOT EXISTS tender_offer_events (
    accession_number     TEXT   NOT NULL,
    -- FK-free on purpose (mirrors nt_filing_notices / prospectus_offerings'
    -- accession-keyed shape while allowing the offeror row of a dual-party
    -- accession to outlive the subject's delisting cleanup).
    instrument_id        BIGINT NOT NULL,
    -- Which SGML header block this instrument's CIK matched. A CIK in BOTH
    -- blocks (self-filed TO-I / 14D-9 / most 13E-3) collapses to 'subject'.
    role                 TEXT   NOT NULL
                           CHECK (role IN ('subject', 'offeror')),
    -- Manifest form, raw (incl. /A): SC TO-T, SC TO-I, SC 14D9, SC 13E3 + /A.
    form                 TEXT   NOT NULL,
    subject_company_name TEXT   NOT NULL,
    subject_cik          TEXT   NOT NULL,
    -- Header FILED BY conformed names (JSON array; multiple blocks allowed).
    -- NULL when the filing is self-filed with no separate FILED BY identity.
    offeror_names        JSONB,
    -- Schedule TO cover transaction-type checkboxes, label-anchored. NULL =
    -- box not resolvable (or the form has no such cover: 14D-9 / 13E-3's
    -- a-d context boxes) — never guessed from the form type.
    is_third_party_tender BOOLEAN,
    is_issuer_tender      BOOLEAN,
    is_going_private      BOOLEAN,
    amends_13d            BOOLEAN,
    -- "final amendment reporting the results" cover checkbox.
    is_final_amendment    BOOLEAN,
    -- Cover "(Amendment No. N)".
    amendment_no          INT,
    -- Reg M-A Item 1004(a)(1)(ii) consideration via the anchored body formula
    -- ("for $124.00 per Share, net ... in cash"). Conflicting distinct
    -- amounts => NULL (ambiguous). Never multiplied into a transaction value
    -- (share counts drift; a computed total would fabricate a figure).
    offer_price_per_unit  NUMERIC,
    -- The matched per-unit word ("Share" / "ADS" / "Note" / "Unit").
    unit_label            TEXT,
    -- Currency glyph AT the matched price. NULL whenever the price is NULL —
    -- never defaulted without a matched price.
    currency              TEXT
                            CHECK (currency IN ('USD', 'EUR', 'GBP')),
    -- Reg M-A Item 1004(a)(1)(v) scheduled expiration, best-effort
    -- "expire(s) ... on <date>" body formula.
    expiration_date       DATE,
    -- Reg M-A Item 1012(a) — the rule itself enumerates the only permitted
    -- positions, which is what makes this a deterministic pattern extraction
    -- (4-state enum + NULL), not free-text classification.
    board_recommendation  TEXT
                            CHECK (board_recommendation IN ('accept', 'reject', 'neutral', 'unable')),
    -- Cover "(Title of Class of Securities)" / "(CUSIP Number ...)".
    security_class_title  TEXT,
    cusip                 TEXT,
    parser_version        INT         NOT NULL DEFAULT 1,
    parsed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (accession_number, instrument_id)
);

CREATE INDEX IF NOT EXISTS idx_tender_offer_events_instrument
    ON tender_offer_events (instrument_id, parsed_at DESC);

COMMENT ON TABLE tender_offer_events IS
    'Parsed tender / going-private schedule events (Reg M-A cover + Item '
    '1004/1012 extractions). One row per (accession, instrument) — role from '
    'the EDGAR SGML header party blocks. Source #1982.';

-- ---------------------------------------------------------------------------
-- Widen the three source/kind CHECK constraints for the new ``sec_tender``
-- source. Full enum lists carried forward from sql/216 (the latest widening).
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
        'prospectus_body',
        'tender_body'
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
        'sec_424b',
        'sec_tender'
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
        'sec_424b',
        'sec_tender'
    ));

COMMIT;
