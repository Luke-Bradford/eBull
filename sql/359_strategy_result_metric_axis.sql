-- #2697 -- bind every current strategy result to the complete, causal metric
-- date tuple and the pre-mask opportunity population measured on that tuple.
--
-- Existing rows are deliberately not backfilled. Their performance interval
-- was selected from realised positions and cannot acquire current provenance
-- in a migration; the all-NULL state is therefore permanent legacy evidence.

BEGIN;

SET LOCAL lock_timeout = '5s';

ALTER TABLE strategy_results_store
    ADD COLUMN IF NOT EXISTS metric_axis_rule_version TEXT,
    ADD COLUMN IF NOT EXISTS metric_axis_dates DATE[],
    ADD COLUMN IF NOT EXISTS metric_axis_start DATE,
    ADD COLUMN IF NOT EXISTS metric_axis_end DATE,
    ADD COLUMN IF NOT EXISTS metric_axis_digest TEXT,
    ADD COLUMN IF NOT EXISTS opportunity_set_digest TEXT,
    ADD COLUMN IF NOT EXISTS evidence_window_id TEXT;

CREATE OR REPLACE FUNCTION strategy_metric_axis_is_valid(
    axis_dates DATE[], declared_start DATE, declared_end DATE
) RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
STRICT
AS $$
    SELECT cardinality(axis_dates) >= 2
       AND axis_dates[1] = declared_start
       AND axis_dates[cardinality(axis_dates)] = declared_end
       AND NOT EXISTS (
           SELECT 1
             FROM unnest(axis_dates) WITH ORDINALITY AS item(axis_date, ordinal)
            WHERE axis_date IS NULL
               OR axis_date < declared_start
               OR axis_date > declared_end
               OR (ordinal > 1 AND axis_date <= axis_dates[ordinal - 1])
       )
$$;

-- This is the SQL mirror of strategy_recent_evidence.RECENT_EVIDENCE_WINDOWS.
-- IDs are append-only: changing dates requires a new ID and a new migration.
CREATE OR REPLACE FUNCTION strategy_evidence_window_is_registered(
    window_id TEXT, declared_start DATE, declared_end DATE
) RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
STRICT
AS $$
    SELECT CASE window_id
        WHEN 'primary-2022-plus' THEN declared_start = DATE '2022-01-01' AND declared_end = DATE '2024-09-27'
        WHEN 'rolling-36m'       THEN declared_start = DATE '2021-09-28' AND declared_end = DATE '2024-09-27'
        WHEN 'rolling-24m'       THEN declared_start = DATE '2022-09-28' AND declared_end = DATE '2024-09-27'
        WHEN 'year-2022'         THEN declared_start = DATE '2022-01-01' AND declared_end = DATE '2022-12-31'
        WHEN 'year-2023'         THEN declared_start = DATE '2023-01-01' AND declared_end = DATE '2023-12-31'
        WHEN 'year-2024'         THEN declared_start = DATE '2024-01-01' AND declared_end = DATE '2024-09-27'
        ELSE FALSE
    END
$$;

ALTER TABLE strategy_results_store
    ADD CONSTRAINT strategy_results_metric_axis_all_or_none CHECK (
        num_nulls(
            metric_axis_rule_version, metric_axis_dates, metric_axis_start,
            metric_axis_end, metric_axis_digest, opportunity_set_digest
        ) IN (0, 6)
    ),
    ADD CONSTRAINT strategy_results_metric_axis_legacy_window_null CHECK (
        metric_axis_rule_version IS NOT NULL OR evidence_window_id IS NULL
    ),
    ADD CONSTRAINT strategy_results_metric_axis_current_shape CHECK (
        metric_axis_rule_version IS NULL OR (
            metric_axis_rule_version = 'full-namespace-panel-v1'
            AND metric_axis_digest ~ '^[0-9a-f]{64}$'
            AND opportunity_set_digest ~ '^[0-9a-f]{64}$'
            AND strategy_metric_axis_is_valid(metric_axis_dates, metric_axis_start, metric_axis_end)
            AND metric_axis_start >= window_start
            AND metric_axis_end <= window_end
        )
    ),
    ADD CONSTRAINT strategy_results_metric_axis_namespace CHECK (
        metric_axis_rule_version IS NULL OR CASE namespace
            WHEN 'in_sample' THEN evidence_window_id IS NULL
                AND metric_axis_end < DATE '2021-06-29'
            WHEN 'hold_out' THEN evidence_window_id IS NOT NULL
                AND strategy_evidence_window_is_registered(evidence_window_id, window_start, window_end)
            ELSE FALSE
        END
    );

COMMENT ON COLUMN strategy_results_store.metric_axis_dates IS
    'Complete ordered panel-date tuple used for every annualised result metric; NULL only on legacy rows.';
COMMENT ON COLUMN strategy_results_store.opportunity_set_digest IS
    'SHA-256 of the frozen strategy_result_universe opportunity population; NULL only on legacy rows.';
COMMENT ON COLUMN strategy_results_store.evidence_window_id IS
    'Append-only registered recent hold-out window ID; NULL for in-sample and legacy rows.';

CREATE OR REPLACE VIEW strategy_results AS
    SELECT *
    FROM strategy_results_store
    WHERE namespace = 'in_sample';

ALTER VIEW strategy_results SET (check_option = 'cascaded');

COMMIT;
