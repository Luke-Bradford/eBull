-- 211_pre14a_proposal_signals.sql
--
-- Issue #1892 (#1015 item 3) — PRE 14A / PRER14A meeting-agenda proposal-
-- signal parser. Upgrades PRE 14A / PRER14A from METADATA_ONLY to PARSE+RAW
-- via a new manifest source ``sec_pre14a``. This migration:
--
--   1. Creates ``pre14a_proposal_signals`` — one row per parsed accession,
--      keyed on accession_number (mirrors nt_filing_notices shape).
--      Tombstones live in the manifest, not in this table.
--   2. Widens three CHECK constraints for the new source/kind:
--      - filing_raw_documents.document_kind  += 'pre14a_body'
--      - sec_filing_manifest.source          += 'sec_pre14a'
--      - data_freshness_index.source         += 'sec_pre14a'
--
-- Source rule: Regulation 14A, Schedule 14A (17 CFR 240.14a-101) + Rule
-- 14a-4(a)(3) (17 CFR 240.14a-4), which requires the proxy to "identify
-- clearly and impartially each separate matter intended to be acted upon" —
-- the numbered "purposes"/"items of business" list in the Notice of Meeting.
-- Category anchors: Item 11 (share-authorization increases), Item 19
-- (charter amendments incl. reverse stock splits), Item 24 / Rule 14a-21(a)
-- (say-on-pay). Does NOT touch ``sec_def14a`` — #1320's ownership-pipeline
-- concern (PRE 14A drafts never counted for ownership) is fully preserved;
-- this is a wholly separate source/table for a different (proposal-signal)
-- purpose.

BEGIN;

CREATE TABLE IF NOT EXISTS pre14a_proposal_signals (
    accession_number                    TEXT        PRIMARY KEY,
    instrument_id                       BIGINT      NOT NULL
                                          REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    proposal_count                      SMALLINT    NOT NULL,
    reverse_stock_split_proposal        BOOLEAN     NOT NULL DEFAULT FALSE,
    authorized_share_increase_proposal  BOOLEAN     NOT NULL DEFAULT FALSE,
    say_on_pay_advisory_vote            BOOLEAN     NOT NULL DEFAULT FALSE,
    -- Raw numbered agenda-item strings (bounded length per item at the
    -- extractor), for LLM/thesis consumption. One array element per
    -- proposal, in agenda order.
    agenda_items                        JSONB       NOT NULL DEFAULT '[]'::jsonb,
    parser_version                      INT         NOT NULL DEFAULT 1,
    parsed_at                           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at                          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pre14a_proposal_signals_instrument
    ON pre14a_proposal_signals (instrument_id, parsed_at DESC);

COMMENT ON TABLE pre14a_proposal_signals IS
    'Parsed SEC PRE 14A / PRER14A meeting-agenda proposals (Rule 14a-4(a)(3) '
    'numbered purposes list). One row per accession; tombstones live in '
    'sec_filing_manifest. Source #1892 / #1015 item 3.';

-- ---------------------------------------------------------------------------
-- Widen the three source/kind CHECK constraints for the new ``sec_pre14a``
-- source.
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
        'pre14a_body'
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
        'sec_pre14a'
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
        'sec_pre14a'
    ));

COMMIT;
