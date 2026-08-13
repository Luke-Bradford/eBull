-- 280_strategy_signal_negative_detail_move.sql
--
-- #2448 one-time transition of still-retained routine verdicts out of the
-- durable fired ledger. This is deliberately the only row DELETE in the
-- design: recurring retention drops whole strategy_signal_observations leaf
-- partitions. The two guards run before and after the move so no aggregate or
-- outcome dependency can be lost.

DO $$
DECLARE
    month_start DATE;
    month_end DATE;
    partition_name TEXT;
    mismatch_count BIGINT;
    dependent_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO dependent_count
    FROM strategy_outcomes o
    JOIN strategy_signals s ON s.signal_id = o.signal_id
    WHERE s.verdict <> 'fired';

    IF dependent_count <> 0 THEN
        RAISE EXCEPTION
            'refusing #2448 move: % outcome row(s) refer to non-fired signal detail',
            dependent_count;
    END IF;

    WITH detail AS (
        SELECT strategy_id, strategy_version, signal_bar_date, signal_kind,
               verdict, COALESCE(not_evaluable_reason, '') AS reason_code,
               COUNT(*) AS row_count
        FROM strategy_signals
        GROUP BY 1, 2, 3, 4, 5, 6
    ), delta AS (
        (SELECT strategy_id, strategy_version, signal_bar_date, signal_kind,
                verdict, reason_code, row_count
         FROM strategy_signal_daily_counts
         EXCEPT ALL
         SELECT * FROM detail)
        UNION ALL
        (SELECT * FROM detail
         EXCEPT ALL
         SELECT strategy_id, strategy_version, signal_bar_date, signal_kind,
                verdict, reason_code, row_count
         FROM strategy_signal_daily_counts)
    )
    SELECT COUNT(*) INTO mismatch_count FROM delta;

    IF mismatch_count <> 0 THEN
        RAISE EXCEPTION
            'refusing #2448 move: daily aggregate differs from pre-move detail in % group(s)',
            mismatch_count;
    END IF;

    FOR month_start IN
        SELECT DISTINCT date_trunc('month', signal_bar_date)::date
        FROM strategy_signals
        WHERE verdict <> 'fired'
    LOOP
        month_end := (month_start + INTERVAL '1 month')::date;
        partition_name := format(
            'strategy_signal_observations_y%sm%s',
            to_char(month_start, 'YYYY'),
            to_char(month_start, 'MM')
        );
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF strategy_signal_observations '
            'FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            month_start,
            month_end
        );
    END LOOP;

    WITH moved AS (
        DELETE FROM strategy_signals
        WHERE verdict <> 'fired'
        RETURNING strategy_id, strategy_version, instrument_id,
                  signal_bar_date, signal_kind, verdict,
                  COALESCE(not_evaluable_reason, '') AS reason_code,
                  created_at
    )
    INSERT INTO strategy_signal_observations (
        strategy_id, strategy_version, instrument_id, signal_bar_date,
        signal_kind, verdict, reason_code, created_at
    )
    SELECT strategy_id, strategy_version, instrument_id, signal_bar_date,
           signal_kind, verdict, reason_code, created_at
    FROM moved;

    WITH detail AS (
        SELECT strategy_id, strategy_version, signal_bar_date, signal_kind,
               verdict, COALESCE(not_evaluable_reason, '') AS reason_code,
               COUNT(*) AS row_count
        FROM strategy_signals
        GROUP BY 1, 2, 3, 4, 5, 6
        UNION ALL
        SELECT strategy_id, strategy_version, signal_bar_date, signal_kind,
               verdict, reason_code, COUNT(*) AS row_count
        FROM strategy_signal_observations
        GROUP BY 1, 2, 3, 4, 5, 6
    ), combined AS (
        SELECT strategy_id, strategy_version, signal_bar_date, signal_kind,
               verdict, reason_code, SUM(row_count) AS row_count
        FROM detail
        GROUP BY 1, 2, 3, 4, 5, 6
    ), delta AS (
        (SELECT strategy_id, strategy_version, signal_bar_date, signal_kind,
                verdict, reason_code, row_count
         FROM strategy_signal_daily_counts
         EXCEPT ALL
         SELECT * FROM combined)
        UNION ALL
        (SELECT * FROM combined
         EXCEPT ALL
         SELECT strategy_id, strategy_version, signal_bar_date, signal_kind,
                verdict, reason_code, row_count
         FROM strategy_signal_daily_counts)
    )
    SELECT COUNT(*) INTO mismatch_count FROM delta;

    IF mismatch_count <> 0 THEN
        RAISE EXCEPTION
            'rolling back #2448 move: aggregate differs from split detail in % group(s)',
            mismatch_count;
    END IF;
END $$;
