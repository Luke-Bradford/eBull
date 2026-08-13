-- 176: finra_short_interest_observations partition extension to 2035-Q1
--
-- Extends the quarterly partition tail from 2027-Q1 (sql/152) to
-- 2035-Q1 (~8y headroom). Original partitions cover 2021-Q3 through
-- 2027-Q1 exclusive (23 partitions). This migration adds 32
-- partitions covering 2027-Q2 through 2035-Q1 exclusive.
--
-- Why: DE BL-1 from final committee (2026-05-24). The sql/174 + sql/175
-- partition-extension sweep missed this sibling. The table has NO
-- DEFAULT partition (sql/152 omitted it intentionally per spec §4.2
-- — "exchange-listed cohort post-June 2021 only"). Without this
-- extension, INSERT into finra_short_interest_observations hard-fails
-- on 2027-04-01 (~11 months runway as of 2026-05-24). The bimonthly
-- ScheduledJob would error every fire after that date.
--
-- Idempotent: every CREATE uses IF NOT EXISTS so re-running the
-- migration is safe.
--
-- Run order: after sql/152_finra_short_interest.sql.

DO $$
DECLARE
    q_start DATE := '2027-04-01';
    q_end   DATE;
    q_name  TEXT;
BEGIN
    WHILE q_start < '2035-04-01' LOOP
        q_end := q_start + INTERVAL '3 months';
        q_name := format(
            'finra_short_interest_observations_p_%sq%s',
            EXTRACT(YEAR FROM q_start)::INT,
            EXTRACT(QUARTER FROM q_start)::INT
        );
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF finra_short_interest_observations '
            'FOR VALUES FROM (%L) TO (%L)',
            q_name, q_start, q_end
        );
        q_start := q_end;
    END LOOP;
END$$;
