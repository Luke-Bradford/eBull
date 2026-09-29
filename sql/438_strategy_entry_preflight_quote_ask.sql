-- 438_strategy_entry_preflight_quote_ask.sql
--
-- #3471 §8 "Protective levels" / §9 "Readout": the readout records the fill-versus-ask gap per
-- leg. The executor prices every entry from `quotes.ask` (stop and target rates are derived from
-- it, `_protective_rates`), and the preflight already stores WHEN that quote was taken
-- (`quote_at`) but not the price itself — so the gap had no stored input.
--
-- `quote_ask` is the ask the preflight evaluated, written wherever `quote_at` is: the paper path's
-- allocated and rejected rows and the trial executor's allocated row. Same type as its source
-- (`quotes.ask NUMERIC(18,6)`), so nothing is rounded on the way in.
--
-- ⚠ NO BACKFILL. `quotes` holds one current row per instrument and is overwritten on refresh, so
-- the ask a past preflight saw is not recoverable; existing rows stay NULL and the readout counts
-- a filled leg with no stored ask as `ask_missing`, never estimates it.

ALTER TABLE strategy_entry_preflights
    ADD COLUMN IF NOT EXISTS quote_ask NUMERIC(18,6);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'strategy_entry_preflights_quote_ask_positive'
    ) THEN
        ALTER TABLE strategy_entry_preflights
            ADD CONSTRAINT strategy_entry_preflights_quote_ask_positive
            CHECK (quote_ask IS NULL OR quote_ask > 0);
    END IF;
END $$;

COMMENT ON COLUMN strategy_entry_preflights.quote_ask IS
    'The quotes.ask the preflight priced from (observed at quote_at). Stop/target rates derive '
    'from it; the #3471 readout compares it with the entry fill. NULL before sql/438.';
