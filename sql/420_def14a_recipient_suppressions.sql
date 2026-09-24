-- 420_def14a_recipient_suppressions.sql
--
-- #2351 slice 2 — a warrant / preferred sibling does not own the DEF 14A Item 403
-- rows its issuer CIK fans out to it.
--
-- Spec: docs/proposals/ownership/2026-09-24-2351-def14a-class-recipients.md (slice 2).
--
-- Readers, not writers: the three DEF 14A writers keep fanning every accession out
-- to every sibling. `def14a_recipient_suppressions` records the (instrument,
-- accession) pairs a reader must not attribute, and the three `*_attributed` views
-- below are the single shared predicate every attribution reader uses. Nothing is
-- deleted from a fact table, so reversal is deleting ledger rows and refreshing
-- `_current`.
--
-- Source rule: Item 403 reports beneficial ownership per class, determined under
-- Rule 13d-3; Rule 13d-3(d)(1)(i) counts warrants into the UNDERLYING common class's
-- figure. The instrument's own class comes from the 10-K/10-Q/20-F cover 12(b) table
-- (`dei:Security12bTitle` + `dei:TradingSymbol` in one context; Reg S-K Item
-- 601(b)(104), Reg S-T Rule 406), cached in `sec_cover_12b_*` below.
--
-- The only writer of all three tables is the `def14a_recipient_suppressions` job
-- (app/services/def14a_recipients.py).

CREATE TABLE IF NOT EXISTS def14a_recipient_suppressions (
    instrument_id         BIGINT      NOT NULL REFERENCES instruments(instrument_id),
    accession_number      TEXT        NOT NULL,
    issuer_cik            TEXT        NOT NULL,
    reason                TEXT        NOT NULL CHECK (reason = 'non_common_sibling'),
    rule_version          INTEGER     NOT NULL CHECK (rule_version > 0),
    cover_accession       TEXT        NOT NULL,
    cover_title           TEXT        NOT NULL,
    cover_symbol          TEXT        NOT NULL,
    witness_instrument_id BIGINT      NOT NULL REFERENCES instruments(instrument_id),
    witness_title         TEXT        NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_id, accession_number)
);

-- Cover filings are immutable once published, so a terminal fetch outcome is cached
-- forever. Transient outcomes (transport error, 403/429/5xx, unparseable 200) are
-- never written here.
CREATE TABLE IF NOT EXISTS sec_cover_12b_fetches (
    cover_accession TEXT        PRIMARY KEY,
    outcome         TEXT        NOT NULL CHECK (outcome IN ('pairs', 'no_pairs', 'not_found')),
    -- dei:EntityCentralIndexKey values; >1 on a co-registrant cover (e.g. a holdco and
    -- its operating subsidiary filing one 10-K).
    entity_ciks     TEXT[]      NOT NULL DEFAULT '{}',
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sec_cover_12b_pairs (
    cover_accession TEXT NOT NULL REFERENCES sec_cover_12b_fetches(cover_accession),
    security_title  TEXT NOT NULL,
    trading_symbol  TEXT NOT NULL,
    PRIMARY KEY (cover_accession, trading_symbol, security_title)
);

CREATE OR REPLACE VIEW def14a_beneficial_holdings_attributed AS
SELECT h.*
  FROM def14a_beneficial_holdings h
 WHERE NOT EXISTS (
       SELECT 1 FROM def14a_recipient_suppressions s
        WHERE s.instrument_id = h.instrument_id
          AND s.accession_number = h.accession_number);

CREATE OR REPLACE VIEW ownership_def14a_observations_attributed AS
SELECT o.*
  FROM ownership_def14a_observations o
 WHERE NOT EXISTS (
       SELECT 1 FROM def14a_recipient_suppressions s
        WHERE s.instrument_id = o.instrument_id
          AND s.accession_number = o.source_accession);

CREATE OR REPLACE VIEW ownership_esop_observations_attributed AS
SELECT o.*
  FROM ownership_esop_observations o
 WHERE NOT EXISTS (
       SELECT 1 FROM def14a_recipient_suppressions s
        WHERE s.instrument_id = o.instrument_id
          AND s.accession_number = o.source_accession);

-- Raw cover instance, stored before parse (prevention log: raw payload before parse).
-- Swept kind (born-compacted: hash + source_url). Full list carried forward from
-- sql/224, the latest widening.
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
        'tender_body',
        'xbrl_cover_instance'
    ));
