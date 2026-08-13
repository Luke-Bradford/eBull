-- Migration 009: add score_id and model_version to trade_recommendations
--
-- score_id: FK back to the scores row that drove this recommendation.
--           Nullable — recommendations can exist before scoring is wired
--           (e.g. manual overrides, early-dev fixtures).
-- model_version: the scoring model version string used (e.g. "v1-balanced").
--                Denormalised here so queries don't need a join to scores.
--
-- cash_balance_known: whether cash was available at recommendation time.
--           Null = unknown. Stored so the execution guard can distinguish
--           "cash was checked and sufficient" from "cash was not checked".

ALTER TABLE trade_recommendations
    ADD COLUMN IF NOT EXISTS score_id          BIGINT REFERENCES scores(score_id),
    ADD COLUMN IF NOT EXISTS model_version     TEXT,
    ADD COLUMN IF NOT EXISTS cash_balance_known BOOLEAN;
