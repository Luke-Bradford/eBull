-- Migration 007: add auditability and ranking columns to scores table

ALTER TABLE scores
    ADD COLUMN IF NOT EXISTS penalties_json JSONB,
    ADD COLUMN IF NOT EXISTS explanation   TEXT,
    ADD COLUMN IF NOT EXISTS rank          INTEGER,
    ADD COLUMN IF NOT EXISTS rank_delta    INTEGER;
