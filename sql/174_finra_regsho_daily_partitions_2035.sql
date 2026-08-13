-- 174: finra_regsho_daily_observations partition extension to 2035-Q1
--
-- Extends the quarterly partition tail from 2030-Q1 (sql/154) to 2035-Q1
-- (5y headroom). Original partitions cover 2024-Q1 through 2030-Q1
-- exclusive (25 partitions). This migration adds 20 partitions covering
-- 2030-Q2 through 2035-Q1 exclusive.
--
-- Why: DE IMP2 from #1233 ETL sweep committee. Without extension,
-- INSERTs into finra_regsho_daily_observations fail starting 2030-04-01
-- when trade dates exceed the partition tail.
--
-- Idempotent: every CREATE uses IF NOT EXISTS so re-running the migration
-- is safe.
--
-- Run order: after sql/154_finra_regsho_daily.sql.

DO $$
DECLARE
    q_start DATE := '2030-04-01';
    q_end   DATE;
    q_name  TEXT;
BEGIN
    WHILE q_start < '2035-04-01' LOOP
        q_end := q_start + INTERVAL '3 months';
        q_name := format(
            'finra_regsho_daily_observations_p_%sq%s',
            EXTRACT(YEAR FROM q_start)::INT,
            EXTRACT(QUARTER FROM q_start)::INT
        );
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF finra_regsho_daily_observations '
            'FOR VALUES FROM (%L) TO (%L)',
            q_name, q_start, q_end
        );
        q_start := q_end;
    END LOOP;
END$$;
