-- 078_layer_enabled_audit.sql
--
-- #346: audit trail + reason string for layer_enabled toggles.
--
-- Before this migration, ``POST /sync/layers/{name}/enabled`` accepted
-- only ``{enabled}`` and stored a single bit on ``layer_enabled``. A
-- direct API caller (or anyone with a service token) could disable
-- ``fx_rates`` / ``portfolio_sync`` without a reason or operator
-- attribution — inconsistent with the kill-switch / live-trading
-- toggles which already require ``reason`` + ``activated_by`` and
-- write audit rows.
--
-- This migration:
--   1. Adds ``reason`` and ``changed_by`` columns to ``layer_enabled``
--      so the latest toggle's context is queryable from the same row
--      the orchestrator already reads.
--   2. Adds ``layer_enabled_audit`` so toggle history is preserved
--      across overwrites; the latest-row columns are denormalised but
--      the full sequence lives here.
--
-- Idempotent: ``ADD COLUMN IF NOT EXISTS`` + ``CREATE TABLE IF NOT
-- EXISTS`` make re-running the file a no-op.

ALTER TABLE layer_enabled
    ADD COLUMN IF NOT EXISTS reason     TEXT,
    ADD COLUMN IF NOT EXISTS changed_by TEXT;

CREATE TABLE IF NOT EXISTS layer_enabled_audit (
    audit_id    BIGSERIAL PRIMARY KEY,
    layer_name  TEXT NOT NULL,
    is_enabled  BOOLEAN NOT NULL,
    reason      TEXT,
    changed_by  TEXT,
    changed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_layer_enabled_audit_layer_changed_at
    ON layer_enabled_audit(layer_name, changed_at DESC);

COMMENT ON TABLE layer_enabled_audit IS
    '#346: append-only history of every layer_enabled toggle. The '
    'latest reason/changed_by are denormalised onto layer_enabled '
    'itself for hot-path reads; this table is the audit trail.';
