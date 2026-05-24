-- 175: financial_facts_raw partition extension to 2040
--
-- Extends the quarterly partition tail from 2030 (sql/156) to 2040 (10y
-- headroom). Original partitions cover 2010-2030 (84 quarterly + pre2010
-- + default). This migration adds 40 partitions for 2031-2040.
--
-- Why: DE IMP2 from #1233 ETL sweep committee. The DEFAULT partition
-- catches 2031+ rows today (sql/156:77-83) — functionally correct but
-- defeats partition pruning + retention sweep targets. New quarterly
-- partitions restore both.
--
-- DEFAULT-stragglers cleanup (POST-MERGE FOLLOW-UP fix on this same
-- migration file): the DEFAULT partition turned out to contain XBRL
-- parser garbage (filings dated 2023-2024 claiming period_end years
-- 2031+ — impossible). #1218 added a parser-side guard rejecting
-- period_end < 1900 OR ≥ 2100, but the rows that landed before #1218
-- still sit in DEFAULT and block new quarterly partition CREATE with
-- "updated partition constraint for default partition would be
-- violated by some row". 18 such rows in dev as of 2026-05-24 from
-- 48 total junk rows (others sit in legitimately-named quarters
-- from year-overflow bugs).
--
-- Cleanup predicate (strictly impossible-by-physics):
--   period_end > filed_date + INTERVAL '5 years'
-- A claimed fiscal-period end-date more than 5 years past the
-- filing-date cannot be a real filing — no public company files
-- accounts for periods 5+ years in the future. Tagging filed_date
-- IS NOT NULL guards rows where the filing-date is genuinely
-- unknown (legacy ingest gaps).
--
-- Idempotent: cleanup uses DELETE so re-run is a no-op once empty;
-- every CREATE uses IF NOT EXISTS so re-running the migration is safe.
--
-- Run order: after sql/156_financial_facts_raw_partition.sql.

-- Phase 1a: defensive cleanup of XBRL parser garbage. Must run BEFORE
-- partition CREATE so the DEFAULT partition has no in-range rows
-- blocking the new partition CHECK predicates.
DELETE FROM financial_facts_raw
WHERE filed_date IS NOT NULL
  AND period_end > filed_date + INTERVAL '5 years';

-- Phase 1b: complementary assertion — refuse to proceed if any row in
-- the 2031+ range has NULL filed_date. The Phase 1a predicate cannot
-- catch those (no filed_date to compare against), but they would
-- trigger the same CheckViolation on Phase 2. Bot iter 1 WARNING fold
-- (PR #1314, 2026-05-24): bot caught the gap; dev DB had zero such
-- rows so dev verification passed silently, but any DB with legacy
-- NPORT bulk-ingest rows lacking filed_date could hit it.
--
-- The operator action on assertion-fail: audit the NULL-filed_date
-- rows. If legitimate (rare — usually means upstream bulk ingest
-- dropped the date), backfill filed_date from accession_number's
-- header data + re-run. If junk, DELETE them explicitly.
DO $$
DECLARE
    null_count INT;
BEGIN
    SELECT COUNT(*) INTO null_count
    FROM financial_facts_raw
    WHERE filed_date IS NULL
      AND period_end >= DATE '2031-01-01';
    IF null_count > 0 THEN
        RAISE EXCEPTION
            'sql/175 refuses to proceed: % row(s) in financial_facts_raw have '
            'filed_date IS NULL AND period_end >= 2031-01-01. These would '
            'trigger CheckViolation on the new quarterly-partition CREATE. '
            'Audit + clean these rows first (backfill filed_date from accession '
            'header data OR DELETE if junk), then re-run.',
            null_count;
    END IF;
END $$;

-- Phase 2: extend quarterly partitions through 2040 (10y headroom).
DO $$
DECLARE
    y           INT;
    q           INT;
    start_date  TEXT;
    end_date    TEXT;
    part_name   TEXT;
BEGIN
    FOR y IN 2031..2040 LOOP
        FOR q IN 1..4 LOOP
            start_date := format('%s-%s-01', y, lpad(((q - 1) * 3 + 1)::text, 2, '0'));
            IF q = 4 THEN
                end_date := format('%s-01-01', y + 1);
            ELSE
                end_date := format('%s-%s-01', y, lpad((q * 3 + 1)::text, 2, '0'));
            END IF;
            part_name := format('financial_facts_raw_%sq%s', y, q);
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF financial_facts_raw '
                'FOR VALUES FROM (%L) TO (%L)',
                part_name, start_date, end_date
            );
        END LOOP;
    END LOOP;
END $$;
