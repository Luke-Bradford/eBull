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

-- sql/495's inline UNIQUE (formation_id, instrument_id) takes Postgres's default name (checked in `pg_constraint` on
-- the dev DB). The assertion below fails the migration if a uniqueness on exactly those two columns survives anyway.
ALTER TABLE momentum_book_entries DROP CONSTRAINT IF EXISTS momentum_book_entries_formation_id_instrument_id_key;
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint c
        WHERE c.conrelid = 'momentum_book_entries'::regclass AND c.contype = 'u'
          AND (SELECT array_agg(a.attname::text ORDER BY a.attname)
               FROM pg_attribute a WHERE a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey))
              = ARRAY['formation_id', 'instrument_id']
    ) THEN
        RAISE EXCEPTION 'momentum_book_entries still has a unique (formation_id, instrument_id)';
    END IF;
END $$;
ALTER TABLE momentum_book_entries DROP CONSTRAINT IF EXISTS momentum_book_entries_formation_instrument_session_key;
ALTER TABLE momentum_book_entries ADD CONSTRAINT momentum_book_entries_formation_instrument_session_key
    UNIQUE (formation_id, instrument_id, target_session);

COMMIT;
