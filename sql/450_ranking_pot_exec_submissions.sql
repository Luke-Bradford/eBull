-- 450_ranking_pot_exec_submissions.sql
--
-- #2842 slice 5b — ranking-pot-v1's executed entries: the capital the slot ledger divides and the
-- durable pre-I/O record of each submission (spec docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md
-- §7.2, "The loader, the slot ledger and the executor").
--
--   ranking_pot_activations        — one row per declaration: `POT_CAPITAL` (§7.3), fixed for the
--                                    declaration. Writer: 5c's activation script. No row → every entry
--                                    refuses `pot_capital_missing`.
--   ranking_pot_exec_submissions   — one row per lifecycle submitted, written by
--                                    `app/services/ranking_pot_executor.py` in the authority transaction,
--                                    BEFORE any broker I/O (r3-109): the slot, its wealth, the requested
--                                    ticket, the amount, the ask and ATR14 the levels came from and the
--                                    SL/TP sent.
--
-- ⚠ APPEND-ONLY, BY TRIGGER (sql/264: a trigger binds the superuser this app connects as).

CREATE TABLE IF NOT EXISTS ranking_pot_activations (
    declaration_id  BIGINT PRIMARY KEY REFERENCES ranking_pot_declarations (declaration_id) ON DELETE RESTRICT,
    -- §7.3: a multiple of $500, capped at $12,000 (the 30% active-risk term on the $40,000 pool).
    pot_capital     NUMERIC(14, 2) NOT NULL
        CHECK (pot_capital > 0 AND pot_capital <= 12000 AND pot_capital % 500 = 0),
    detail          JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(detail) = 'object'),
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ranking_pot_exec_submissions (
    lifecycle_id      BIGINT PRIMARY KEY REFERENCES ranking_pot_exec_lifecycles (lifecycle_id) ON DELETE RESTRICT,
    strategy_trade_id BIGINT NOT NULL UNIQUE REFERENCES strategy_trades (strategy_trade_id) ON DELETE RESTRICT,
    slot              SMALLINT NOT NULL CHECK (slot >= 1),
    slot_wealth       NUMERIC(14, 2) NOT NULL CHECK (slot_wealth > 0),
    requested_amount  NUMERIC(14, 2) NOT NULL,
    amount            NUMERIC(14, 2) NOT NULL,
    ask               NUMERIC(20, 6) NOT NULL,
    quote_at          TIMESTAMPTZ NOT NULL,
    atr14             NUMERIC(20, 6) NOT NULL CHECK (atr14 > 0),
    stop_loss_rate    NUMERIC(20, 6) NOT NULL,
    take_profit_rate  NUMERIC(20, 6) NOT NULL,
    recorded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- The ticket is the slot's wealth; capacity may reduce it, never raise it (§7.2).
    CONSTRAINT ranking_pot_exec_submissions_amount
        CHECK (0 < amount AND amount <= requested_amount AND requested_amount <= slot_wealth),
    CONSTRAINT ranking_pot_exec_submissions_levels
        CHECK (0 < stop_loss_rate AND stop_loss_rate < ask AND ask < take_profit_rate)
);

-- The submission belongs to its lifecycle and is its authority's own record: the trade funds the lifecycle's
-- signal, in its slot, for exactly this amount, and was created by THIS transaction (so the submission row
-- commits with the funding decision, trade and order, before any broker I/O, or not at all).
CREATE OR REPLACE FUNCTION ranking_pot_exec_submissions_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    lc RECORD;
    t  RECORD;
BEGIN
    SELECT signal_id, slot INTO lc FROM ranking_pot_exec_lifecycles WHERE lifecycle_id = NEW.lifecycle_id;
    SELECT fd.signal_id, fd.amount, st.xmin AS trade_xmin INTO t
    FROM strategy_trades st
    JOIN strategy_funding_decisions fd ON fd.funding_decision_id = st.funding_decision_id
    WHERE st.strategy_trade_id = NEW.strategy_trade_id AND fd.verdict = 'allocated';
    IF t.signal_id IS DISTINCT FROM lc.signal_id OR NEW.slot IS DISTINCT FROM lc.slot
       OR t.amount IS DISTINCT FROM NEW.amount THEN
        RAISE EXCEPTION 'trade % does not fund lifecycle % in slot % for %', NEW.strategy_trade_id,
            NEW.lifecycle_id, NEW.slot, NEW.amount;
    END IF;
    IF t.trade_xmin <> pg_current_xact_id()::xid THEN
        RAISE EXCEPTION 'the submission row for trade % is written only by the transaction that created it',
            NEW.strategy_trade_id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ranking_pot_exec_submissions_guard ON ranking_pot_exec_submissions;
CREATE TRIGGER trg_ranking_pot_exec_submissions_guard
BEFORE INSERT ON ranking_pot_exec_submissions
FOR EACH ROW EXECUTE FUNCTION ranking_pot_exec_submissions_guard();

DO $$
DECLARE
    t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['ranking_pot_activations', 'ranking_pot_exec_submissions'] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_append_only ON %I', t, t);
        EXECUTE format('CREATE TRIGGER trg_%s_append_only BEFORE UPDATE OR DELETE ON %I '
                       'FOR EACH ROW EXECUTE FUNCTION prevent_ranking_pot_mutation()', t, t);
    END LOOP;
END $$;
