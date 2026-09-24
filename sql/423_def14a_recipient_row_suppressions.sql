-- 423_def14a_recipient_row_suppressions.sql
--
-- #2351 slice 3b — a V-shape Item 403 table carries a *Title of class* cell per row.
-- The parser keeps each holder's FIRST row and every sibling receives it, so LEN
-- (Class A) shows Stuart Miller's Class B line and LEN.B shows BlackRock's Class A line.
-- `def14a_recipient_row_suppressions` records the single (instrument, accession, holder)
-- rows a reader must not attribute: the row's class cell names another sibling's class.
-- The three `*_attributed` views gain a second NOT EXISTS; their column lists are
-- unchanged, so readers are untouched.
--
-- Spec: docs/proposals/ownership/2026-09-24-2351-def14a-class-recipients.md (slice 3b).
-- The only writer is the `def14a_recipient_suppressions` job
-- (app/services/def14a_recipients.py).

CREATE TABLE IF NOT EXISTS def14a_recipient_row_suppressions (
    instrument_id         BIGINT      NOT NULL REFERENCES instruments(instrument_id),
    accession_number      TEXT        NOT NULL,
    holder_name           TEXT        NOT NULL,
    issuer_cik            TEXT        NOT NULL,
    reason                TEXT        NOT NULL CHECK (reason = 'other_common_class_row'),
    rule_version          INTEGER     NOT NULL CHECK (rule_version > 0),
    cover_accession       TEXT        NOT NULL,
    cover_title           TEXT        NOT NULL,
    cover_symbol          TEXT        NOT NULL,
    witness_instrument_id BIGINT      NOT NULL REFERENCES instruments(instrument_id),
    witness_title         TEXT        NOT NULL,
    -- The *Title of class* cell the row sits on (evidence).
    class_cell            TEXT        NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_id, accession_number, holder_name)
);

CREATE OR REPLACE VIEW def14a_beneficial_holdings_attributed AS
SELECT h.*
  FROM def14a_beneficial_holdings h
 WHERE NOT EXISTS (
       SELECT 1 FROM def14a_recipient_suppressions s
        WHERE s.instrument_id = h.instrument_id
          AND s.accession_number = h.accession_number)
   AND NOT EXISTS (
       SELECT 1 FROM def14a_recipient_row_suppressions r
        WHERE r.instrument_id = h.instrument_id
          AND r.accession_number = h.accession_number
          AND r.holder_name = h.holder_name);

CREATE OR REPLACE VIEW ownership_def14a_observations_attributed AS
SELECT o.*
  FROM ownership_def14a_observations o
 WHERE NOT EXISTS (
       SELECT 1 FROM def14a_recipient_suppressions s
        WHERE s.instrument_id = o.instrument_id
          AND s.accession_number = o.source_accession)
   AND NOT EXISTS (
       SELECT 1 FROM def14a_recipient_row_suppressions r
        WHERE r.instrument_id = o.instrument_id
          AND r.accession_number = o.source_accession
          AND r.holder_name = o.holder_name);

CREATE OR REPLACE VIEW ownership_esop_observations_attributed AS
SELECT o.*
  FROM ownership_esop_observations o
 WHERE NOT EXISTS (
       SELECT 1 FROM def14a_recipient_suppressions s
        WHERE s.instrument_id = o.instrument_id
          AND s.accession_number = o.source_accession)
   AND NOT EXISTS (
       SELECT 1 FROM def14a_recipient_row_suppressions r
        WHERE r.instrument_id = o.instrument_id
          AND r.accession_number = o.source_accession
          AND r.holder_name = o.plan_name);
