-- Migration 038: raw_persistence_state (#268 follow-up, Plan A PR 3).
--
-- Tracks per-source compaction + sweep timestamps so the daily
-- raw_data_retention_sweep scheduler job can skip expensive hash
-- scans when the source was compacted recently. Without this the
-- job rehashes 225 GB on every daily fire in dry-run mode.
--
-- last_compacted_at is only updated when compaction actually ran
-- successfully (not on advisory-lock skip). COMPACTION_STALENESS
-- in app/services/raw_persistence.py determines the throttle
-- window (default 7 days).

CREATE TABLE IF NOT EXISTS raw_persistence_state (
    source TEXT PRIMARY KEY,
    last_compacted_at TIMESTAMPTZ,
    last_compaction_files_scanned INTEGER,
    last_compaction_bytes_reclaimed BIGINT,
    last_sweep_at TIMESTAMPTZ
);
