-- 132_bootstrap_stages_lane_extension.sql
--
-- Extend the bootstrap_stages.lane CHECK with the finer-grained
-- lane names introduced by the #1020 orchestration redesign:
--
--   init               - A1 universe_sync only.
--   etoro              - A2 candle_refresh (separate eToro budget).
--   sec                - LEGACY catch-all for prior schema; preserved
--                        so existing rows from the 17-stage run history
--                        stay valid.
--   sec_rate           - SEC stages that share the per-IP rate clock
--                        (B1, B2, B3, B4, C1.b, D1, D2, D3).
--   sec_bulk_download  - A3 only — separate connection but shares
--                        SEC clock for HEAD requests.
--   db                 - DB-bound Phase C/E stages (separate
--                        psycopg connections, parallel-able).

BEGIN;

ALTER TABLE bootstrap_stages
    DROP CONSTRAINT IF EXISTS bootstrap_stages_lane_check;

ALTER TABLE bootstrap_stages
    ADD CONSTRAINT bootstrap_stages_lane_check
    CHECK (lane IN ('init', 'etoro', 'sec', 'sec_rate', 'sec_bulk_download', 'db'));

COMMIT;
