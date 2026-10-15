-- 498_momentum_book_catch_up_entries.sql
--
-- #3822 — a formation's book name that never entered gets a fresh entry signal on each later session of the month
-- (`app.services.momentum_book_job.catch_up_entries`). An entry row therefore carries its own target session, and a
-- (formation, name) pair may hold one entry per session instead of one per formation.
--
-- Existing rows take their formation's target session, which is the session every entry was emitted for until now.
-- The insert-only trigger is disabled for that backfill alone.

BEGIN;

ALTER TABLE momentum_book_entries ADD COLUMN IF NOT EXISTS target_session DATE;

ALTER TABLE momentum_book_entries DISABLE TRIGGER momentum_book_entries_insert_only;
UPDATE momentum_book_entries me
   SET target_session = f.target_session
  FROM momentum_book_formations f
 WHERE f.formation_id = me.formation_id AND me.target_session IS NULL;
ALTER TABLE momentum_book_entries ENABLE TRIGGER momentum_book_entries_insert_only;

ALTER TABLE momentum_book_entries ALTER COLUMN target_session SET NOT NULL;

ALTER TABLE momentum_book_entries DROP CONSTRAINT IF EXISTS momentum_book_entries_formation_id_instrument_id_key;
ALTER TABLE momentum_book_entries DROP CONSTRAINT IF EXISTS momentum_book_entries_formation_instrument_session_key;
ALTER TABLE momentum_book_entries ADD CONSTRAINT momentum_book_entries_formation_instrument_session_key
    UNIQUE (formation_id, instrument_id, target_session);

COMMIT;
