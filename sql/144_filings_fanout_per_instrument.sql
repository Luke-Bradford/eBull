-- Migration 144: filings fan-out per instrument (#1117 PR-B)
--
-- Companion to sql/143 (#1102 PR-A). Once external_identifiers allows N
-- (provider='sec', identifier_type='cik', identifier_value=X) rows
-- pointing at distinct instrument_ids (sql/143), every code path that
-- resolves CIK -> instrument must fan out to ALL siblings. The bulk
-- ingester multimap shape change ships in the same PR's first commit.
--
-- This migration relaxes:
--
-- 1. ``filing_events`` UNIQUE (provider, provider_filing_id) ->
--    UNIQUE (provider, provider_filing_id, instrument_id). filing_events
--    is the per-instrument bridge: every read filters by
--    ``WHERE fe.instrument_id = X`` already; the relaxation lets two
--    siblings each carry their own (accession, instrument) row instead
--    of one collapsing on top of the other.
--
-- 2. ``def14a_beneficial_holdings`` UNIQUE INDEX
--    ``uq_def14a_holdings_accession_holder`` (accession_number,
--    holder_name) -> ``uq_def14a_holdings_instrument_accession_holder``
--    (instrument_id, accession_number, holder_name). Per-share-class
--    proxies legitimately list the same holder name on the same
--    accession for both siblings; pre-relaxation the second sibling's
--    INSERT ON CONFLICT overwrote instrument_id instead of creating a
--    distinct row.
--
-- Tables NOT relaxed (entity-level, child FKs anchor on accession alone):
-- - ``eight_k_filings`` PK accession (eight_k_items / eight_k_exhibits FK)
-- - ``insider_filings`` PK accession (insider_filers /
--   insider_transaction_footnotes / insider_transactions /
--   insider_initial_holdings FK)
-- - ``def14a_ingest_log`` PK accession (entity-level tombstone)
-- - ``sec_filing_manifest`` PK accession (entity-level)
-- - ``filing_raw_documents`` PK (accession, document_kind)
--
-- Read-side fan-out for those tables routes through ``filing_events``
-- as the per-instrument bridge -- application code change, not schema.
-- See docs/superpowers/specs/2026-05-10-1117-filings-fanout-complete.md.
--
-- Idempotency: each constraint/index swap is gated on a shape check
-- (pg_constraint.contype + conkey for table constraints; pg_index +
-- pg_class.relname for indexes) so partial-applied dev DBs re-run
-- cleanly. Name-only checks would falsely skip when the name points
-- at a wrong-shape constraint.

-- 1. filing_events: relax UNIQUE to (provider, provider_filing_id, instrument_id)
DO $$
DECLARE
    has_correct_shape BOOLEAN;
BEGIN
    SELECT EXISTS(
        SELECT 1 FROM pg_constraint c
        WHERE c.conname = 'uq_filing_events_provider_unique'
          AND c.contype = 'u'
          AND c.conrelid = 'filing_events'::regclass
          AND (
              SELECT array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum))
              FROM pg_attribute a
              WHERE a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
          ) = ARRAY['provider', 'provider_filing_id', 'instrument_id']::name[]
    ) INTO has_correct_shape;

    IF NOT has_correct_shape THEN
        ALTER TABLE filing_events
            DROP CONSTRAINT IF EXISTS uq_filing_events_provider_unique;
        ALTER TABLE filing_events
            ADD CONSTRAINT uq_filing_events_provider_unique
                UNIQUE (provider, provider_filing_id, instrument_id);
    END IF;
END$$;

-- 2. def14a_beneficial_holdings: relax UNIQUE INDEX to per-(instrument, accession, holder)
DO $$
DECLARE
    has_correct_shape BOOLEAN;
BEGIN
    SELECT EXISTS(
        SELECT 1
          FROM pg_index i
          JOIN pg_class c ON c.oid = i.indexrelid
         WHERE c.relname = 'uq_def14a_holdings_instrument_accession_holder'
           AND i.indrelid = 'def14a_beneficial_holdings'::regclass
           AND i.indisunique
           AND (
               SELECT array_agg(a.attname ORDER BY array_position(i.indkey::int[], a.attnum::int))
                 FROM pg_attribute a
                WHERE a.attrelid = i.indrelid
                  AND a.attnum = ANY(i.indkey::int[])
           ) = ARRAY['instrument_id', 'accession_number', 'holder_name']::name[]
    ) INTO has_correct_shape;

    IF NOT has_correct_shape THEN
        DROP INDEX IF EXISTS uq_def14a_holdings_accession_holder;
        DROP INDEX IF EXISTS uq_def14a_holdings_instrument_accession_holder;
        CREATE UNIQUE INDEX uq_def14a_holdings_instrument_accession_holder
            ON def14a_beneficial_holdings (instrument_id, accession_number, holder_name);
    END IF;
END$$;
