-- 434_ai_trial_trade_link_amount.sql
--
-- #3471 slice 2b-ii — spec §8 "Sizing" and §15 O8: the trial accepts a capacity-reduced amount
-- down to the broker open minimum and records REQUESTED versus ACTUAL. The actual amount is the
-- funding decision's (`strategy_funding_decisions.amount`); the requested one is the decision's
-- `size_tier` ticket (`ai_trial_intent.TRIAL_TICKET_USD`), recorded here on the trade link the
-- trial executor writes inside the same transaction as the allocation.
-- Writer: app/services/ai_trial_executor.py.
--
-- The dev DB held 0 rows in `ai_trial_trade_links` when this was written
-- (`SELECT count(*) FROM ai_trial_trade_links`), so the column is NOT NULL with no backfill.

ALTER TABLE ai_trial_trade_links
    ADD COLUMN IF NOT EXISTS requested_amount NUMERIC(18,6) NOT NULL CHECK (requested_amount > 0);

COMMENT ON COLUMN ai_trial_trade_links.requested_amount IS
    '#3471 §8/O8: the size_tier ticket the leg asked for, USD. The funded amount '
    '(strategy_funding_decisions.amount) may be lower, never higher (trigger).';

-- 432's verify, plus: the trade was ALLOCATED and its funded amount never exceeds the request.
-- "The inherited capacity arithmetic may reduce it; nothing may raise it" (§8).
CREATE OR REPLACE FUNCTION ai_trial_trade_links_verify()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    trade_signal   BIGINT;
    trade_verdict  TEXT;
    trade_amount   NUMERIC;
    leg_signal     BIGINT;
BEGIN
    SELECT fd.signal_id, fd.verdict, fd.amount INTO trade_signal, trade_verdict, trade_amount
    FROM strategy_trades t JOIN strategy_funding_decisions fd ON fd.funding_decision_id = t.funding_decision_id
    WHERE t.strategy_trade_id = NEW.strategy_trade_id;
    SELECT signal_id INTO leg_signal FROM ai_trial_leg_links WHERE pair_id = NEW.pair_id AND leg = NEW.leg;
    IF trade_signal IS DISTINCT FROM leg_signal THEN
        RAISE EXCEPTION 'trade % was funded from signal %, not the % leg''s %',
            NEW.strategy_trade_id, trade_signal, NEW.leg, leg_signal;
    END IF;
    IF trade_verdict IS DISTINCT FROM 'allocated' OR trade_amount IS NULL
       OR NOT coalesce(trade_amount <= NEW.requested_amount, false) THEN
        RAISE EXCEPTION 'trade % funded % (%), above or without the requested %',
            NEW.strategy_trade_id, trade_amount, trade_verdict, NEW.requested_amount;
    END IF;
    RETURN NEW;
END $$;
