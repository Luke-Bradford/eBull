-- 495_momentum_book.sql
--
-- #3822 — the momentum book's monthly formations, the entry signals each formation emits, and the exits it orders.
-- Writer: `app.services.momentum_book_job`. The rule is `app.services.momentum_book` (pure).
--
-- A formation is one row per (strategy version, month t): the book formed from month t's data, decided on the first
-- session after month t that saw no formation for it. Formations, entries and exits are insert only.

BEGIN;

-- The momentum book's entry tickets cite their formation.
ALTER TABLE strategy_entry_tickets DROP CONSTRAINT IF EXISTS strategy_entry_tickets_evidence_kind_check;
ALTER TABLE strategy_entry_tickets ADD CONSTRAINT strategy_entry_tickets_evidence_kind_check CHECK (evidence_kind IN (
    'strategy_promotion', 'ai_trial_declaration', 'ranking_pot_declaration', 'core_mandate_event',
    'momentum_book_formation'));

CREATE TABLE IF NOT EXISTS momentum_book_formations (
    formation_id      BIGSERIAL PRIMARY KEY,
    strategy_version  TEXT NOT NULL CHECK (btrim(strategy_version) <> ''),
    -- Month t, as its first day.
    formation_month   DATE NOT NULL CHECK (EXTRACT(DAY FROM formation_month) = 1),
    -- The NYSE session the book is traded on (after month t).
    target_session    DATE NOT NULL CHECK (target_session > formation_month),
    formed_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    candidates        INTEGER NOT NULL CHECK (candidates >= 0),
    -- [{"instrument_id", "rank", "score"}], best first.
    book              JSONB NOT NULL CHECK (jsonb_typeof(book) = 'array'),
    -- Instrument ids the book held when it was formed.
    held              JSONB NOT NULL CHECK (jsonb_typeof(held) = 'array'),
    UNIQUE (strategy_version, formation_month)
);

CREATE TABLE IF NOT EXISTS momentum_book_entries (
    signal_id      BIGINT PRIMARY KEY REFERENCES strategy_signals (signal_id),
    formation_id   BIGINT NOT NULL REFERENCES momentum_book_formations (formation_id),
    instrument_id  BIGINT NOT NULL REFERENCES instruments (instrument_id),
    rank           INTEGER NOT NULL CHECK (rank >= 1),
    UNIQUE (formation_id, instrument_id)
);

CREATE TABLE IF NOT EXISTS momentum_book_exits (
    formation_id       BIGINT NOT NULL REFERENCES momentum_book_formations (formation_id),
    strategy_trade_id  BIGINT NOT NULL REFERENCES strategy_trades (strategy_trade_id),
    instrument_id      BIGINT NOT NULL REFERENCES instruments (instrument_id),
    recorded_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (formation_id, strategy_trade_id)
);

CREATE OR REPLACE FUNCTION momentum_book_insert_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is insert only', TG_TABLE_NAME;
END $$;

DO $$
DECLARE
    t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['momentum_book_formations', 'momentum_book_entries', 'momentum_book_exits'] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS %I_insert_only ON %I', t, t);
        EXECUTE format(
            'CREATE TRIGGER %I_insert_only BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION momentum_book_insert_only()',
            t, t);
    END LOOP;
END $$;

COMMIT;
