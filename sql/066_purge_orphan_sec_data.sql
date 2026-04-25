-- Migration 066 — purge orphan SEC-derived data left behind by #496.
--
-- PR #496 (sql/065_purge_bogus_crypto_sec_ciks.sql) removed bogus
-- (provider='sec', identifier_type='cik') rows from
-- ``external_identifiers`` for 47 crypto instruments where the SEC
-- ticker map had blindly stamped an unrelated US-listed company's
-- CIK (BTC ↔ Grayscale Bitcoin Mini Trust, etc.). It also wiped
-- ``instrument_sec_profile`` for those rows.
--
-- Every other SEC-derived table that keys on ``instrument_id``
-- without consulting ``external_identifiers`` was missed. Audit
-- (2026-04-25) shows the orphan rows that remain:
--
--   filing_events                            32,500
--   financial_facts_raw                     126,475
--   insider_filings                             407
--   eight_k_filings                              43
--   instrument_business_summary_sections         19
--   instrument_business_summary                   2
--   dividend_events                               0
--   instrument_sec_profile                        0  (cleaned in #496)
--   sec_entity_change_log                         0
--   financial_periods_raw (source='sec_edgar')    0
--   financial_periods (source='sec_edgar')        0
--
-- Frontend reads these tables directly without checking
-- ``external_identifiers``, so the BTC instrument page still
-- renders SEC content despite the underlying CIK link being gone.
-- Migration 066 deletes those rows so the visible-state matches
-- the operator's settled "every region uses its appropriate
-- data source" rule.
--
-- Predicate per table group:
--
--   SEC-only base tables (no ``source`` column):
--       DELETE FROM <t> AS x
--       WHERE NOT EXISTS (
--           SELECT 1 FROM external_identifiers ei
--           WHERE ei.instrument_id = x.instrument_id
--             AND ei.provider = 'sec'
--             AND ei.identifier_type = 'cik'
--       );
--
--   Multi-source tables (carry ``source TEXT NOT NULL``):
--       same predicate AND source IN ('sec', 'sec_edgar',
--       'sec_xbrl', 'sec_companyfacts'). The known live value on
--       dev is 'sec_edgar' (verified 2026-04-25); the fuller list
--       is defensive in case ingest writes a different label
--       under a future code path.
--
-- Cascade chains handle children automatically:
--   filing_events     → filing_documents (sql/062:36)
--   insider_filings   → insider_filers, insider_transactions,
--                       insider_transaction_footnotes
--                       (sql/057:147,201,419)
--   eight_k_filings   → eight_k_items, eight_k_exhibits
--                       (sql/061:98,145)
--
-- Views ``dividend_history`` + ``instrument_dividend_summary``
-- (sql/050) derive from ``financial_periods``, NOT from
-- ``dividend_events``. They recompute via the multi-source delete
-- against ``financial_periods``. ``dividend_events`` is the base
-- table for the upcoming-dividends calendar
-- (``app/services/dividends.py:177``); it gets purged in its own
-- right.
--
-- ``fundamentals_snapshot`` (sql/001:29) lacks both a current-CIK
-- gate AND a ``source`` column. Cleaning it requires either a
-- schema change (add ``source``) or a recompute pass. Out of scope
-- for this migration; tracked as follow-up.
--
-- ``sec_facts_concept_catalog`` (sql/063) is concept-keyed
-- (``UNIQUE (taxonomy, concept)``), not instrument-keyed. Out of
-- scope.
--
-- Idempotent: re-running on a clean DB is a zero-row delete.

BEGIN;

-- ---------------------------------------------------------------
-- SEC-only base tables (no ``source`` column).
-- Predicate: instrument lacks a current SEC CIK in
-- ``external_identifiers``. Cascades handle children.
-- ---------------------------------------------------------------

DELETE FROM filing_events fe
WHERE NOT EXISTS (
    SELECT 1 FROM external_identifiers ei
    WHERE ei.instrument_id = fe.instrument_id
      AND ei.provider = 'sec'
      AND ei.identifier_type = 'cik'
);

DELETE FROM insider_filings i
USING instruments inst
WHERE inst.instrument_id = i.instrument_id
  AND NOT EXISTS (
      SELECT 1 FROM external_identifiers ei
      WHERE ei.instrument_id = i.instrument_id
        AND ei.provider = 'sec'
        AND ei.identifier_type = 'cik'
  );

DELETE FROM eight_k_filings ek
WHERE NOT EXISTS (
    SELECT 1 FROM external_identifiers ei
    WHERE ei.instrument_id = ek.instrument_id
      AND ei.provider = 'sec'
      AND ei.identifier_type = 'cik'
);

DELETE FROM instrument_business_summary_sections ibss
WHERE NOT EXISTS (
    SELECT 1 FROM external_identifiers ei
    WHERE ei.instrument_id = ibss.instrument_id
      AND ei.provider = 'sec'
      AND ei.identifier_type = 'cik'
);

DELETE FROM instrument_business_summary ibs
WHERE NOT EXISTS (
    SELECT 1 FROM external_identifiers ei
    WHERE ei.instrument_id = ibs.instrument_id
      AND ei.provider = 'sec'
      AND ei.identifier_type = 'cik'
);

DELETE FROM dividend_events de
WHERE NOT EXISTS (
    SELECT 1 FROM external_identifiers ei
    WHERE ei.instrument_id = de.instrument_id
      AND ei.provider = 'sec'
      AND ei.identifier_type = 'cik'
);

DELETE FROM financial_facts_raw ffr
WHERE NOT EXISTS (
    SELECT 1 FROM external_identifiers ei
    WHERE ei.instrument_id = ffr.instrument_id
      AND ei.provider = 'sec'
      AND ei.identifier_type = 'cik'
);

DELETE FROM instrument_sec_profile isp
WHERE NOT EXISTS (
    SELECT 1 FROM external_identifiers ei
    WHERE ei.instrument_id = isp.instrument_id
      AND ei.provider = 'sec'
      AND ei.identifier_type = 'cik'
);

DELETE FROM sec_entity_change_log secl
WHERE NOT EXISTS (
    SELECT 1 FROM external_identifiers ei
    WHERE ei.instrument_id = secl.instrument_id
      AND ei.provider = 'sec'
      AND ei.identifier_type = 'cik'
);

-- ---------------------------------------------------------------
-- Multi-source tables. Predicate adds ``source IN (sec*)`` so
-- legitimate FMP / future non-SEC rows on instruments that may
-- carry a different identifier are not touched.
-- ---------------------------------------------------------------

DELETE FROM financial_periods_raw fpr
WHERE fpr.source IN ('sec', 'sec_edgar', 'sec_xbrl', 'sec_companyfacts')
  AND NOT EXISTS (
      SELECT 1 FROM external_identifiers ei
      WHERE ei.instrument_id = fpr.instrument_id
        AND ei.provider = 'sec'
        AND ei.identifier_type = 'cik'
  );

DELETE FROM financial_periods fp
WHERE fp.source IN ('sec', 'sec_edgar', 'sec_xbrl', 'sec_companyfacts')
  AND NOT EXISTS (
      SELECT 1 FROM external_identifiers ei
      WHERE ei.instrument_id = fp.instrument_id
        AND ei.provider = 'sec'
        AND ei.identifier_type = 'cik'
  );

COMMIT;
