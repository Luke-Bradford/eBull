-- 235_drop_cascade_retry_queue.sql
--
-- #2087 (#2065 follow-up) — drop the retired cascade retry queue.
--
-- PR #2085 removed every reader/writer of ``cascade_retry_queue`` but kept
-- the table so the still-running pre-merge daemon could not error against a
-- dropped relation (retired-writer ordering, #2008 lesson). Gate cleared
-- 2026-07-18: first post-restart fundamentals_sync run succeeded in 4m56s
-- (cascade-free), zero trigger='cascade' thesis_runs since the restart, and
-- the queue held 0 rows at drop time.

DROP TABLE IF EXISTS cascade_retry_queue;
