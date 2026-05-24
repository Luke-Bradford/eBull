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
-- Idempotent: every CREATE uses IF NOT EXISTS so re-running the migration
-- is safe.
--
-- Run order: after sql/156_financial_facts_raw_partition.sql.

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
