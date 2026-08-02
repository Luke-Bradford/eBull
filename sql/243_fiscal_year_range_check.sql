-- 243_fiscal_year_range_check.sql
--
-- #2192 — range CHECK on the derived fiscal_year columns, which had none
-- while their sibling fiscal_quarter has had `BETWEEN 1 AND 4` since
-- sql/032. The #1955 class (a contract field wired into one model but not
-- its sibling), expressed in DDL.
--
-- Source rule (verified against the source, not inferred): SEC companyfacts
-- `fy` is the DEI DocumentFiscalYearFocus of the FILING the fact appeared in
-- (#682), and SEC republishes whatever the filer tagged. Fetching
-- data.sec.gov/api/xbrl/companyfacts for the affected issuers shows SEC
-- ITSELF publishes the out-of-range values — PRTH 43465/43555/43830,
-- TNET 43646/43738/43921/44012, ACIC 43101, WTBA 2107. The 4xxxx values are
-- Excel serial dates (43830 = 2019-12-31) mis-tagged by the filer; 2107 is a
-- digit transposition of 2017. No parser of ours coerced a date: the values
-- are faithfully-stored filer errors.
--
-- WHY THE LANDING TABLE IS DELIBERATELY NOT CONSTRAINED.
-- `financial_facts_raw` is source-faithful evidence and MUST keep accepting
-- what SEC publishes. It holds 3,188 rows with `fiscal_year = 0` across 23
-- instruments — and that zero is SEC's own: companyfacts emits
-- {"fy": 0, "fp": ""} for facts from filings with no fiscal-period focus
-- (1,825 such entries for OLN alone, e.g. 8-K accn 0001193125-16-664678).
-- A range CHECK there would reject legitimate SEC data and convert a filer
-- error into an ingest outage. The constraint belongs on the DERIVED layers
-- that metrics actually read, where fiscal_year is a computed label.
--
-- Bound is FIXED, not `EXTRACT(YEAR FROM CURRENT_DATE) + 2`. Postgres does
-- accept a non-immutable CHECK (tested — it is not rejected), but a row that
-- was valid at insert would silently become invalid later, breaking
-- pg_dump/restore and making VALIDATE CONSTRAINT drift with the clock.
-- Observed sane range on the dev corpus is 2005-2027, so 1995..2100 leaves
-- headroom at both ends without readmitting Excel serials (>= 34700 for any
-- date after 1995) or transpositions.
--
-- Ordering matters: repair BEFORE the constraint, or ADD CONSTRAINT fails on
-- the existing rows. The repair is paired with the normalizer guard in
-- app/services/fundamentals/__init__.py in the same PR — without it, the next
-- fundamentals sync would re-emit an out-of-range label and the new CHECK
-- would turn today's silent skew into a hard ingest failure.

BEGIN;

-- ── 1. Repair the derived rows ─────────────────────────────────────────
--
-- Per-instrument convention, NOT a blanket `year(period_end_date)`: an
-- off-December fiscal-year filer labels a period_end with a different
-- calendar year, so the offset is derived from that instrument's OWN sane
-- rows (modal fiscal_year - year(period_end_date)) and applied to the bad
-- ones. On the current corpus every affected issuer is calendar-fiscal —
-- 240 sane rows across PRTH/TNET/ACIC/WTBA, zero counter-examples — so the
-- offset resolves to 0 and this reproduces the hand-derived values. The
-- offset form is what makes the migration correct on a corpus where the
-- damaged issuer is NOT calendar-fiscal.

-- Sane rows from BOTH derived tables (review NITPICK): an instrument whose
-- only in-range rows live in financial_periods_raw must still be able to
-- supply its own offset. `fiscal_year` and `period_end_date` are both NOT
-- NULL, and MODE() over a non-empty group always returns a value, so
-- `fy_offset` is non-null for every instrument PRESENT here — absence, not
-- NULL, is how "no offset derivable" is represented.
CREATE TEMP TABLE _fy_offset ON COMMIT DROP AS
SELECT instrument_id,
       MODE() WITHIN GROUP (ORDER BY offset_years) AS fy_offset
FROM (
    SELECT instrument_id,
           fiscal_year - EXTRACT(YEAR FROM period_end_date)::int AS offset_years
    FROM financial_periods
    WHERE fiscal_year BETWEEN 1995 AND 2100
    UNION ALL
    SELECT instrument_id,
           fiscal_year - EXTRACT(YEAR FROM period_end_date)::int
    FROM financial_periods_raw
    WHERE fiscal_year BETWEEN 1995 AND 2100
) sane
GROUP BY instrument_id;

-- Assert the fallback path was never taken, BEFORE repairing (review
-- PREVENTION). A post-hoc range check alone is not enough: a wrong offset
-- that happens to land in range would pass it silently. An instrument with
-- damaged rows and NO sane row to derive from must stop the migration, not
-- be quietly assumed calendar-fiscal.
DO $$
DECLARE orphaned INTEGER;
BEGIN
    SELECT count(DISTINCT d.instrument_id) INTO orphaned
    FROM (
        SELECT instrument_id FROM financial_periods
         WHERE fiscal_year < 1995 OR fiscal_year > 2100
        UNION
        SELECT instrument_id FROM financial_periods_raw
         WHERE fiscal_year < 1995 OR fiscal_year > 2100
    ) d
    LEFT JOIN _fy_offset o ON o.instrument_id = d.instrument_id
    WHERE o.instrument_id IS NULL;
    IF orphaned > 0 THEN
        RAISE EXCEPTION
            '#2192: % instrument(s) have out-of-range fiscal_year rows but NO '
            'in-range row to derive a fiscal-year offset from. Assuming a zero '
            'offset would silently mis-label a non-calendar-fiscal issuer; '
            'repair those instruments by hand before re-running.',
            orphaned;
    END IF;
END $$;

-- No COALESCE: the join is inner and `fy_offset` is non-null by construction,
-- so a zero offset can only ever be a MEASURED zero, never a defaulted one.
UPDATE financial_periods fp
SET fiscal_year = EXTRACT(YEAR FROM fp.period_end_date)::int + o.fy_offset
FROM _fy_offset o
WHERE o.instrument_id = fp.instrument_id
  AND (fp.fiscal_year < 1995 OR fp.fiscal_year > 2100);

UPDATE financial_periods_raw fpr
SET fiscal_year = EXTRACT(YEAR FROM fpr.period_end_date)::int + o.fy_offset
FROM _fy_offset o
WHERE o.instrument_id = fpr.instrument_id
  AND (fpr.fiscal_year < 1995 OR fpr.fiscal_year > 2100);

-- Backstop: a repair that produced an out-of-range value (offset itself
-- absurd) must not reach ADD CONSTRAINT as an opaque failure.
DO $$
DECLARE leftover INTEGER;
BEGIN
    SELECT (SELECT count(*) FROM financial_periods
             WHERE fiscal_year < 1995 OR fiscal_year > 2100)
         + (SELECT count(*) FROM financial_periods_raw
             WHERE fiscal_year < 1995 OR fiscal_year > 2100)
      INTO leftover;
    IF leftover > 0 THEN
        RAISE EXCEPTION
            '#2192: % fiscal_year rows still out of range after repair.',
            leftover;
    END IF;
END $$;

-- ── 2. Constrain the derived layers ────────────────────────────────────

ALTER TABLE financial_periods
    ADD CONSTRAINT financial_periods_fiscal_year_check
    CHECK (fiscal_year BETWEEN 1995 AND 2100);

ALTER TABLE financial_periods_raw
    ADD CONSTRAINT financial_periods_raw_fiscal_year_check
    CHECK (fiscal_year BETWEEN 1995 AND 2100);

-- Same class, different source (DEF 14A Item 402(c) SCT `row_year`). Zero
-- violations today — this is prophylactic, and the point of the ticket is
-- the class, not the three rows that surfaced it.
ALTER TABLE def14a_exec_compensation
    ADD CONSTRAINT def14a_exec_compensation_fiscal_year_check
    CHECK (fiscal_year BETWEEN 1995 AND 2100);

COMMIT;
