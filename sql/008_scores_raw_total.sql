-- Migration 008: add raw_total column to scores for pre-penalty audit trail

ALTER TABLE scores
    ADD COLUMN IF NOT EXISTS raw_total NUMERIC(10, 4);
