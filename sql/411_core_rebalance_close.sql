-- #2603 sell leg — a `sell_core` is executed as a WHOLE close of the one owned core
-- position (trigger `core_rebalance`), and the allocator rebuys to `lower` on a later
-- attended POST.  Spec: docs/proposals/ta/2026-09-23-core-sell-leg-close-rebuy.md §5.
--
-- ⚠ Deviation from the spec, stated: the close quote lives in its OWN append-only table
-- (`strategy_core_rebalance_close_quotes`, one row per intent) instead of four nullable
-- columns on `strategy_core_rebalance_intents`.  The spec deferred the intent INSERT to
-- after the quote so the intent could carry it; that cannot hold, because
-- `admit_core_rebalance_intent` reads the PERSISTED intent, and the intent's
-- `reason_code` CHECK (sql/348) admits only allocator codes, so an executor refusal
-- cannot be written onto it.  The invariant the columns existed for is kept by the
-- table: a quote row exists only for an intent handed to the manager, and the operation
-- link trigger below requires it.
--
-- One transaction (the runner applies each file in one), so nothing is half-applied.

LOCK TABLE strategy_core_rebalance_intents, strategy_trades, strategy_position_ownership,
    strategy_position_operations IN ACCESS EXCLUSIVE MODE;

-- Built from the LIVE constraint, read from the dev cluster 2026-09-23 with
-- `pg_get_constraintdef`, not from sql/292.  A mismatch refuses (fail-closed).
DO $$
DECLARE
    live TEXT;
BEGIN
    SELECT pg_get_constraintdef(oid) INTO live
    FROM pg_constraint
    WHERE conrelid = 'strategy_position_operations'::regclass
      AND conname = 'strategy_position_operations_trigger_code_check';
    IF live IS DISTINCT FROM
        'CHECK ((trigger_code = ANY (ARRAY[''entry_exit_gap''::text, ''causal_resistance_break''::text, '
        '''timeout''::text, ''strategy_exit''::text, ''emergency_risk''::text, ''operator_close''::text])))'
    THEN
        RAISE EXCEPTION 'strategy_position_operations_trigger_code_check drifted: %', live;
    END IF;
END $$;

ALTER TABLE strategy_position_operations
    DROP CONSTRAINT strategy_position_operations_trigger_code_check,
    ADD CONSTRAINT strategy_position_operations_trigger_code_check CHECK (trigger_code IN (
        'entry_exit_gap', 'causal_resistance_break', 'timeout', 'strategy_exit',
        'emergency_risk', 'operator_close', 'core_rebalance'
    ));

CREATE TABLE IF NOT EXISTS strategy_core_rebalance_close_quotes (
    core_rebalance_intent_id BIGINT PRIMARY KEY
        REFERENCES strategy_core_rebalance_intents(core_rebalance_intent_id) ON DELETE RESTRICT,
    broker_position_id       BIGINT NOT NULL CHECK (broker_position_id > 0),
    units                    NUMERIC(24,8) NOT NULL CHECK (units > 0),
    ticket_amount            NUMERIC(18,6) NOT NULL CHECK (ticket_amount > 0),
    cost_upper_bound         NUMERIC(18,6) NOT NULL CHECK (cost_upper_bound >= 0),
    currency                 TEXT NOT NULL CHECK (btrim(currency) <> '' AND length(currency) <= 16),
    quoted_at                TIMESTAMPTZ NOT NULL,
    recorded_at              TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE strategy_core_rebalance_close_quotes IS
    '#2603: the close-arm what-if quote for one sell_core intent, written immediately before '
    'the manager is handed the whole close.  Present iff the intent reached the manager.';

ALTER TABLE strategy_position_operations
    ADD COLUMN IF NOT EXISTS core_rebalance_intent_id BIGINT
        REFERENCES strategy_core_rebalance_intents(core_rebalance_intent_id) ON DELETE RESTRICT,
    ADD CONSTRAINT strategy_position_operations_core_rebalance_link
        CHECK ((trigger_code = 'core_rebalance') = (core_rebalance_intent_id IS NOT NULL)),
    ADD CONSTRAINT strategy_position_operations_core_rebalance_is_close
        CHECK (trigger_code <> 'core_rebalance' OR operation_type = 'close');

CREATE UNIQUE INDEX IF NOT EXISTS strategy_position_operations_core_rebalance_intent_uidx
    ON strategy_position_operations (core_rebalance_intent_id)
    WHERE core_rebalance_intent_id IS NOT NULL;

-- A link names a QUOTED sell_core intent, on the ownership's instrument and position,
-- owned by a core trade.  A refused sell has no quote row, so it can never be linked.
CREATE OR REPLACE FUNCTION strategy_position_operations_core_rebalance_link_check()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.core_rebalance_intent_id IS NULL THEN
        RETURN NEW;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM strategy_core_rebalance_intents intent
        JOIN strategy_core_rebalance_close_quotes quote
          ON quote.core_rebalance_intent_id = intent.core_rebalance_intent_id
        JOIN strategy_position_ownership own ON own.ownership_id = NEW.ownership_id
        JOIN strategy_trades t ON t.strategy_trade_id = own.strategy_trade_id
        WHERE intent.core_rebalance_intent_id = NEW.core_rebalance_intent_id
          AND intent.action = 'sell_core'
          AND quote.broker_position_id = own.broker_position_id
          AND intent.core_instrument_id = t.instrument_id
          AND t.core_rebalance_intent_id IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'core_rebalance operation % does not link a quoted sell_core intent on its core position',
            NEW.core_rebalance_intent_id;
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER strategy_position_operations_core_rebalance_link_check
    BEFORE INSERT ON strategy_position_operations
    FOR EACH ROW EXECUTE FUNCTION strategy_position_operations_core_rebalance_link_check();

-- The link and the identity it binds are write-once.  No writer deletes operations
-- (grep "DELETE FROM strategy_position_operations" over app/ and sql/: 0 hits).
CREATE OR REPLACE FUNCTION strategy_position_operations_identity_frozen()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.core_rebalance_intent_id IS NOT NULL THEN
            RAISE EXCEPTION 'a core_rebalance operation is permanent';
        END IF;
        RETURN OLD;
    END IF;
    IF NEW.core_rebalance_intent_id IS DISTINCT FROM OLD.core_rebalance_intent_id
       OR NEW.ownership_id IS DISTINCT FROM OLD.ownership_id
       OR NEW.trigger_code IS DISTINCT FROM OLD.trigger_code
       OR NEW.operation_type IS DISTINCT FROM OLD.operation_type
       OR NEW.order_id IS DISTINCT FROM OLD.order_id
       OR NEW.request_id IS DISTINCT FROM OLD.request_id
    THEN
        RAISE EXCEPTION 'strategy_position_operations identity columns are write-once';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER strategy_position_operations_identity_frozen
    BEFORE UPDATE OR DELETE ON strategy_position_operations
    FOR EACH ROW EXECUTE FUNCTION strategy_position_operations_identity_frozen();

-- A CORE ownership's parents cannot move.  Scoped to core trades: the signal arm's
-- tests rewrite `broker_position_id` on a fixture ownership, and nothing here depends on
-- the signal arm.  The three production UPDATE sites write status/release fields only.
CREATE OR REPLACE FUNCTION strategy_position_ownership_core_parents_frozen()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.strategy_trade_id IS DISTINCT FROM OLD.strategy_trade_id
        OR NEW.broker_position_id IS DISTINCT FROM OLD.broker_position_id)
       AND EXISTS (
           SELECT 1 FROM strategy_trades t
           WHERE t.strategy_trade_id IN (OLD.strategy_trade_id, NEW.strategy_trade_id)
             AND t.core_rebalance_intent_id IS NOT NULL
       )
    THEN
        RAISE EXCEPTION 'a core ownership''s trade and position are write-once';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER strategy_position_ownership_core_parents_frozen
    BEFORE UPDATE ON strategy_position_ownership
    FOR EACH ROW EXECUTE FUNCTION strategy_position_ownership_core_parents_frozen();

-- `_load_owned` derives `is_core` from `core_rebalance_intent_id` and reads the
-- instrument off the trade, so neither may move once written.
CREATE OR REPLACE FUNCTION strategy_trades_core_identity_frozen()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.core_rebalance_intent_id IS DISTINCT FROM OLD.core_rebalance_intent_id
       OR (OLD.core_rebalance_intent_id IS NOT NULL AND NEW.instrument_id IS DISTINCT FROM OLD.instrument_id)
    THEN
        RAISE EXCEPTION 'a core trade''s intent and instrument are write-once';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER strategy_trades_core_identity_frozen
    BEFORE UPDATE ON strategy_trades
    FOR EACH ROW EXECUTE FUNCTION strategy_trades_core_identity_frozen();

-- Intents and close quotes are append-only.  No writer updates or deletes an intent
-- today (grep "UPDATE strategy_core_rebalance_intents" / "DELETE FROM
-- strategy_core_rebalance_intents" over app/, sql/, tests/: 0 hits); this makes that
-- practice a constraint.
--
-- ⚠ Intents refuse UPDATE only.  DELETE of an intent that authorised anything (a trade,
-- a close quote, a close operation) is already refused by those FKs (RESTRICT / NO
-- ACTION); an unreferenced hold or refusal row stays deletable.  A row trigger on DELETE
-- would break the test harness's DELETE-based cleanup, and intents cannot join its
-- TRUNCATE-first set because they are an FK parent (tests/fixtures/ebull_test_db.py
-- `_TRUNCATE_BEFORE_DELETE`).  The two leaf tables guarded on DELETE here
-- (`strategy_position_operations`, `strategy_core_rebalance_close_quotes`) are in it.
CREATE OR REPLACE FUNCTION strategy_core_rebalance_append_only()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END $$;

CREATE TRIGGER strategy_core_rebalance_intents_append_only
    BEFORE UPDATE ON strategy_core_rebalance_intents
    FOR EACH ROW EXECUTE FUNCTION strategy_core_rebalance_append_only();

CREATE TRIGGER strategy_core_rebalance_close_quotes_append_only
    BEFORE UPDATE OR DELETE ON strategy_core_rebalance_close_quotes
    FOR EACH ROW EXECUTE FUNCTION strategy_core_rebalance_append_only();
