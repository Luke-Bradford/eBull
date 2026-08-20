-- #2489 — one unorderable S-4 bracket must not abort the evidence or forward
-- outcome batch. The existing bounded ledger gets one new closed refusal code;
-- no rows, metrics or time-series stores are added.
--
-- Live constraint inspected before authoring (2026-08-10):
--   window_truncated, series_break, quarantined_bar, missing_bar_data
-- This preserves that complete union and appends only
-- unorderable_exit_levels.

BEGIN;

ALTER TABLE strategy_outcomes
    DROP CONSTRAINT strategy_outcomes_reason_check;

ALTER TABLE strategy_outcomes
    ADD CONSTRAINT strategy_outcomes_reason_check CHECK (
        reason IS NULL OR reason IN (
            'window_truncated',
            'series_break',
            'quarantined_bar',
            'missing_bar_data',
            'unorderable_exit_levels'
        )
    );

COMMIT;
