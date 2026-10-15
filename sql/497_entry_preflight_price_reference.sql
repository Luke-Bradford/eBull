-- 497_entry_preflight_price_reference.sql
--
-- #3833 — which price an allocated entry's protective levels were derived from. eToro's rates endpoint serves
-- delayed quotes for some US equities while their OneMinute candles stay real-time, so the momentum book prices those
-- names from the latest candle close. `quote_at` / `quote_ask` then hold the bar's open time and its close.
--   'quote'        — the stored `quotes` row.
--   'candle_close' — the latest OneMinute candle, used because the stored quote was older than the policy's
--                    `max_quote_age_seconds`.
-- NULL: a writer that does not record it (every row before this migration, and the paper, trial and pot paths,
-- all of which price from the stored quote). Writers: `app.services.momentum_book_executor._commit_authority`
-- (allocated rows) and `app.services.strategy_paper_executor._persist_rejection` (momentum rejected rows).
-- This narrows sql/438's `quote_ask` contract: on a 'candle_close' row it holds the candle close, not `quotes.ask`.

BEGIN;

ALTER TABLE strategy_entry_preflights
    ADD COLUMN IF NOT EXISTS price_reference TEXT
        CHECK (price_reference IN ('quote', 'candle_close'));

COMMIT;
