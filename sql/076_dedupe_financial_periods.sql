-- 076_dedupe_financial_periods.sql
--
-- #558: financial_periods (and financial_periods_raw) shows the same
-- fiscal label twice — sometimes more — for one instrument, polluting
-- the instrument page Financials tab with duplicate columns and
-- out-of-order quarters.
--
-- Smoking-gun example from BBBY (CIK 0001130713 = Beyond Inc, formerly
-- Overstock — ticker reuse after the legacy Bed Bath & Beyond
-- bankruptcy):
--
--   fy=2023 Q4: real row period_end=2023-12-31, source_ref=
--   '0001130713-24-000013', filed=2024-02-23, revenue=1.232B.
--
--   Polluted sibling: period_end=2024-10-29, source_ref=
--   '0001130713-24-000013,000113071' (compound — extraction merged
--   facts from a 10-Q across the same fiscal label), filed=2024-10-31,
--   revenue=1.232B (same value).
--
-- Mechanisms producing dupes:
--
-- 1. **DEI-context pollution within one filing.**
--    `_derive_periods_from_facts` did
--    ``period_end = max(f.period_end for f in period_facts)``. DEI facts
--    such as ``dei:EntityCommonStockSharesOutstanding`` carry an
--    "as-of" instant context endDate equal to the filing date, ~6 weeks
--    after the real fiscal period end. When a normalisation run
--    included one, max() lifted period_end to the filing date. Each
--    distinct period_end becomes a separate row because period_end_date
--    is part of the canonical PK.
--
-- 2. **Cross-accession pollution leftover.**
--    Re-extraction over multiple filings can produce a compound
--    source_ref (comma-joined accessions) and inherit a polluted
--    period_end from one of the included filings. Same fiscal label,
--    different period_end, different source_ref.
--
-- Reliable signal in both cases: pollution always shifts period_end
-- LATER than the real fiscal end. The smallest period_end_date per
-- (instrument, source, fiscal_year, fiscal_quarter, period_type) is
-- always the row to keep.
--
-- Genuine restatements file the SAME period_end_date as the original
-- (fiscal calendar doesn't move with an amendment), so a "smaller
-- wins" rule preserves them on tied period_end. Tie-break for true
-- restatements: keep the most recently filed row.
--
-- Strategy: TWO DELETE passes per table.
--
-- Pass 1 — same source_ref, smaller period_end wins. Catches the
-- pure DEI case where one filing produced two rows.
--
-- Pass 2 — across source_refs, smaller period_end wins; on tied
-- period_end keep the latest filed_date. Catches the compound /
-- mixed-extraction cases and preserves real restatements.
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
              -- Real period end is always smaller than the polluted one.
              (other.period_end_date < keep.period_end_date)
              -- Same period_end_date — true restatement; keep latest filed.
           OR (other.period_end_date = keep.period_end_date
               AND ((other.filed_date IS NOT NULL AND keep.filed_date IS NULL)
                    OR (other.filed_date > keep.filed_date)))
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
              (other.period_end_date < keep.period_end_date)
           OR (other.period_end_date = keep.period_end_date
               AND ((other.filed_date IS NOT NULL AND keep.filed_date IS NULL)
                    OR (other.filed_date > keep.filed_date)))
          )
);

COMMIT;
