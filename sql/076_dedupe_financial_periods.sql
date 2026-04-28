-- 076_dedupe_financial_periods.sql
--
-- #558: financial_periods (and financial_periods_raw) shows the same
-- fiscal label twice — sometimes more — for one instrument, polluting
-- the instrument page Financials tab with duplicate columns and
-- out-of-order quarters.
--
-- Two distinct mechanisms produce the dupes:
--
-- 1. **DEI-context pollution (same accession, two period_ends).**
--    `_derive_periods_from_facts` did
--    ``period_end = max(f.period_end for f in period_facts)``. DEI facts
--    such as ``dei:EntityCommonStockSharesOutstanding`` carry an
--    "as-of" instant context endDate equal to the filing date, ~6 weeks
--    after the real fiscal period end. When a normalisation run
--    included one, max() lifted period_end to the filing date. Each
--    distinct period_end becomes a separate row because period_end_date
--    is part of the canonical PK.
--
-- 2. **Cross-accession restatement leftover (different accessions, two
--    period_ends).**
--    A 10-K, 10-K/A, or 10-Q amendment can re-tag the same fiscal label
--    with a different period_end_date than the original filing. Both
--    rows persist because the canonical PK includes period_end_date.
--    Restatements are legitimate — but only the most recently filed
--    row should drive the operator UI, not both.
--
-- Strategy: TWO DELETE passes.
--
-- Pass 1 — same source_ref, smaller-period-end-wins. Catches DEI
-- pollution: same filing produced two rows whose only difference is
-- the as-of context. The real fiscal end is always the smaller
-- period_end; the polluted row is the larger.
--
-- Pass 2 — across source_refs, latest filed_date wins (with period_end
-- as a tie-break). Catches restatement leftover: two rows with the
-- same fiscal label but different accessions. The most recent filing
-- supersedes the older one.
--
-- Both passes are scoped to a single ``source`` so a future
-- multi-source row (sec_edgar vs companies_house) is never collapsed
-- across providers — those are independently authoritative. The
-- companion code fix in app/services/fundamentals.py also restricts
-- period_end derivation to facts whose concept maps to a canonical
-- column, so future runs cannot reintroduce DEI pollution.
--
-- Idempotent: running the file twice on already-deduped data is a
-- no-op; both DELETEs return rowcount 0.

BEGIN;

-- ── Pass 1a. financial_periods (canonical) — same source_ref ────

DELETE FROM financial_periods AS keep
WHERE EXISTS (
    SELECT 1
    FROM financial_periods AS other
    WHERE other.instrument_id = keep.instrument_id
      AND other.source        = keep.source
      AND other.source_ref    = keep.source_ref
      AND other.fiscal_year   = keep.fiscal_year
      AND other.fiscal_quarter IS NOT DISTINCT FROM keep.fiscal_quarter
      AND other.period_type   = keep.period_type
      AND other.period_end_date < keep.period_end_date
);

-- ── Pass 1b. financial_periods_raw — same source_ref ────────────

DELETE FROM financial_periods_raw AS keep
WHERE EXISTS (
    SELECT 1
    FROM financial_periods_raw AS other
    WHERE other.instrument_id = keep.instrument_id
      AND other.source        = keep.source
      AND other.source_ref    = keep.source_ref
      AND other.fiscal_year   = keep.fiscal_year
      AND other.fiscal_quarter IS NOT DISTINCT FROM keep.fiscal_quarter
      AND other.period_type   = keep.period_type
      AND other.period_end_date < keep.period_end_date
);

-- ── Pass 2a. financial_periods — across source_refs ─────────────
--
-- Keep the row with the most recent ``filed_date`` per
-- (instrument_id, source, fiscal_year, fiscal_quarter, period_type).
-- Tie-break on ``period_end_date DESC`` so the most-recently-reported
-- period end wins when two filings share a date. NULL filed_date
-- sorts last (NULLS LAST) so a row with a known filing date always
-- beats an unfiled-stub row.

DELETE FROM financial_periods AS keep
WHERE EXISTS (
    SELECT 1
    FROM financial_periods AS other
    WHERE other.instrument_id = keep.instrument_id
      AND other.source        = keep.source
      AND other.fiscal_year   = keep.fiscal_year
      AND other.fiscal_quarter IS NOT DISTINCT FROM keep.fiscal_quarter
      AND other.period_type   = keep.period_type
      AND (
              -- A different row has a strictly later filed_date.
              (other.filed_date IS NOT NULL AND keep.filed_date IS NULL)
           OR (other.filed_date > keep.filed_date)
              -- Same filed_date (or both NULL) — fall back to period_end.
           OR (other.filed_date IS NOT DISTINCT FROM keep.filed_date
               AND other.period_end_date > keep.period_end_date)
          )
);

-- ── Pass 2b. financial_periods_raw — across source_refs ─────────

DELETE FROM financial_periods_raw AS keep
WHERE EXISTS (
    SELECT 1
    FROM financial_periods_raw AS other
    WHERE other.instrument_id = keep.instrument_id
      AND other.source        = keep.source
      AND other.fiscal_year   = keep.fiscal_year
      AND other.fiscal_quarter IS NOT DISTINCT FROM keep.fiscal_quarter
      AND other.period_type   = keep.period_type
      AND (
              (other.filed_date IS NOT NULL AND keep.filed_date IS NULL)
           OR (other.filed_date > keep.filed_date)
           OR (other.filed_date IS NOT DISTINCT FROM keep.filed_date
               AND other.period_end_date > keep.period_end_date)
          )
);

COMMIT;
