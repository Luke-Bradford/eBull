-- 487_trade_events_environment_no_default.sql
--
-- #3796 (#3616 slice 3c follow-up). sql/485 gave trade_events.environment a DEFAULT of 'demo' only for writers that
-- bypass `ingest_trade_events`: the jobs process still on pre-485 code until its reload, and test fixtures. #3795 is
-- deployed (the jobs child was replaced on 2026-10-13 at 20:34Z) and the fixtures now pass the environment, so the
-- default is removed: a writer that omits the environment fails instead of being labelled demo under any scope.

BEGIN;

ALTER TABLE trade_events ALTER COLUMN environment DROP DEFAULT;

COMMIT;
