-- 316_drop_redundant_forecast_outcome_index.sql
--
-- #2553 review: the UNIQUE constraint on
-- (forecast_id,resolver_version,input_rule_set_version) already owns an index
-- with exactly the lookup shape created explicitly by sql/315. Keep one copy:
-- every terminal forecast should cost one row, not duplicate index storage.

DROP INDEX IF EXISTS idx_strategy_forecast_outcomes_forecast;
