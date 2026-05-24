-- 177: ownership_*_observations partition extension to 2040
--
-- Extends the quarterly partition tail from 2030-Q4 (sql/113-116 +
-- sql/123 + sql/127) to 2040 for all 7 ownership observation tables.
-- Each table currently has 84 quarterly partitions (2010-2030) + 1
-- DEFAULT. This migration adds 40 new quarterly partitions per table
-- = 280 partitions total.
--
-- Why: DE BL-2 from final committee (2026-05-24). Same risk class as
-- financial_facts_raw pre-#1218 + sql/175: DEFAULT partition silently
-- catches 2031+ rows but defeats partition pruning + retention sweep
-- targets. Cheaper to extend now (one migration) than after Run #8
-- backfills more legacy data into the DEFAULT bucket.
--
-- Tables in scope:
-- * ownership_insiders_observations (sql/113)
-- * ownership_institutions_observations (sql/114)
-- * ownership_blockholders_observations (sql/115)
-- * ownership_treasury_observations (sql/116)
-- * ownership_def14a_observations (sql/116)
-- * ownership_funds_observations (sql/123)
-- * ownership_esop_observations (sql/127)
--
-- Defensive cleanup: like sql/175, junk rows in DEFAULT with
-- impossible period_end (claimed > filed_at + 5y) could block new
-- partition CREATE. Apply same cleanup predicate to each DEFAULT.
-- Observation tables use ``filed_at`` (TIMESTAMPTZ) not ``filed_date``
-- — adjust the predicate accordingly.
--
-- NULL-filed_at defensive assertion: refuse to proceed if any
-- DEFAULT-partition row has NULL filed_at + period_end >= 2031-01-01
-- (same pattern as sql/175 Phase 1b).
--
-- Idempotent: every CREATE uses IF NOT EXISTS; cleanup DELETEs are
-- empty on re-run.
--
-- Run order: after sql/113, sql/114, sql/115, sql/116, sql/123, sql/127.

-- Phase 1a: defensive cleanup of parser-bug junk across all 7
-- DEFAULT partitions. Predicate is strictly-impossible-by-physics:
-- claimed period_end more than 5 years past filed_at = parser bug
-- (no public filing claims fiscal period 5+ years in future).
DO $$
DECLARE
    tbl TEXT;
BEGIN
    FOR tbl IN
        SELECT unnest(ARRAY[
            'ownership_insiders_observations',
            'ownership_institutions_observations',
            'ownership_blockholders_observations',
            'ownership_treasury_observations',
            'ownership_def14a_observations',
            'ownership_funds_observations',
            'ownership_esop_observations'
        ])
    LOOP
        EXECUTE format(
            'DELETE FROM %I WHERE filed_at IS NOT NULL '
            'AND period_end > (filed_at::DATE + INTERVAL ''5 years'')',
            tbl
        );
    END LOOP;
END$$;

-- Phase 1b: refuse if any NULL-filed_at row in 2031+ range remains
-- (cleanup predicate can't catch those — same pattern as sql/175).
DO $$
DECLARE
    tbl TEXT;
    null_count INT;
BEGIN
    FOR tbl IN
        SELECT unnest(ARRAY[
            'ownership_insiders_observations',
            'ownership_institutions_observations',
            'ownership_blockholders_observations',
            'ownership_treasury_observations',
            'ownership_def14a_observations',
            'ownership_funds_observations',
            'ownership_esop_observations'
        ])
    LOOP
        EXECUTE format(
            'SELECT COUNT(*) FROM %I WHERE filed_at IS NULL AND period_end >= DATE ''2031-01-01''',
            tbl
        ) INTO null_count;
        IF null_count > 0 THEN
            RAISE EXCEPTION
                'sql/177 refuses to proceed: % row(s) in % have filed_at IS NULL '
                'AND period_end >= 2031-01-01. These would trigger CheckViolation '
                'on the new quarterly-partition CREATE. Audit + clean these rows '
                'first (backfill filed_at from accession header OR DELETE if junk), '
                'then re-run.',
                null_count, tbl;
        END IF;
    END LOOP;
END$$;

-- Phase 2: create new quarterly partitions 2031..2040 for each table.
DO $$
DECLARE
    tbl       TEXT;
    yr        INT;
    qtr       INT;
    qstart    DATE;
    qend      DATE;
    pname     TEXT;
BEGIN
    FOR tbl IN
        SELECT unnest(ARRAY[
            'ownership_insiders_observations',
            'ownership_institutions_observations',
            'ownership_blockholders_observations',
            'ownership_treasury_observations',
            'ownership_def14a_observations',
            'ownership_funds_observations',
            'ownership_esop_observations'
        ])
    LOOP
        FOR yr IN 2031..2040 LOOP
            FOR qtr IN 1..4 LOOP
                qstart := MAKE_DATE(yr, (qtr - 1) * 3 + 1, 1);
                qend := qstart + INTERVAL '3 months';
                pname := format('%s_%sq%s', tbl, yr, qtr);
                EXECUTE format(
                    'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I '
                    'FOR VALUES FROM (%L) TO (%L)',
                    pname, tbl, qstart, qend
                );
            END LOOP;
        END LOOP;
    END LOOP;
END$$;
