-- 040_layer_enabled.sql
-- Per-layer enable/disable flag (spec §3.2 rule 1). Default: enabled.
-- Absent row counts as enabled so adding a new layer to the registry
-- never surprises an operator with a disabled-by-default row.

CREATE TABLE IF NOT EXISTS layer_enabled (
    layer_name  TEXT PRIMARY KEY,
    is_enabled  BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
