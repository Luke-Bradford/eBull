-- Migration 021: add provenance column to positions
--
-- Distinguishes positions created by eBull's execution layer from those
-- discovered via broker portfolio sync (externally opened via the eToro
-- UI or copy trading). Required so the execution guard and other
-- consumers can tell eBull-managed positions from externally-held ones.
--
-- Semantics of `source`:
--   'ebull'       — currently-open position is one eBull actively
--                   opened or reopened via its own execution layer.
--   'broker_sync' — currently-open position was opened externally and
--                   discovered via the eToro portfolio sync.
--
-- "Currently-open" is the key qualifier. On reopen (current_units <= 0
-- then a fresh BUY lands), callers reset `source` to reflect the new
-- opener — see CASE WHEN logic in order_client._update_position_buy
-- and portfolio_sync.sync_portfolio.  Preserving source across a
-- close/reopen cycle would mislead the guard about who is currently
-- managing the position.
--
-- Existing rows are backfilled to 'broker_sync' because the only
-- INSERT path that ran before this migration was
-- portfolio_sync (PR #179).  The 'ebull' code path only came online
-- with PR #181 (execute approved orders, issue #174), and the
-- positions table has not yet had an eBull-originated row.
--
-- Migration strategy:
--   1. ADD COLUMN with NOT NULL DEFAULT 'broker_sync' — non-volatile
--      default avoids a table rewrite on Postgres 11+; existing rows
--      inherit the default atomically with the metadata update.
--   2. DROP DEFAULT afterwards so every future INSERT must specify
--      source explicitly.  Prevents silent "default to broker_sync"
--      bugs on any future INSERT path that forgets the column.
--
-- Issue: #180

BEGIN;

ALTER TABLE positions
    ADD COLUMN source TEXT NOT NULL DEFAULT 'broker_sync'
    CHECK (source IN ('ebull', 'broker_sync'));

ALTER TABLE positions
    ALTER COLUMN source DROP DEFAULT;

COMMIT;
