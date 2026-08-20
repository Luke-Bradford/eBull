-- 349_core_trade_arc.sql
--
-- #2603 item 3, execution half, step 2.  Step 1 (sql/348) recorded the core
-- rebalance VERDICT and deliberately authorised nothing.  This migration makes a
-- core holding STORABLE and therefore visible to the position manager.
--
-- Spec: docs/proposals/ta/2026-08-14-core-trade-arc.md
--
-- ⚠⚠ THE SHAPE IS AN EXCLUSIVE ARC ON `strategy_trades`, NOT A SIBLING TABLE.
-- Settled in the spec, and the deciding evidence is the consumer inventory's
-- class C: `strategy_order_reconciliation`, `strategy_wealth`, the status
-- UPDATEs and the three `strategy_control_plane` helpers all key on
-- `strategy_trade_id` / `broker_position_id` and work on a core trade with no
-- change at all.  An arc costs them nothing.  A sibling table would have to be
-- dual-written into every one of them, and each omission would be a silently
-- unreconciled position.
--
-- ⚠⚠ THE MIGRATION IS THE MOMENT THE INVARIANT CHANGES, NOT THE WRITER.  From
-- the instant this applies, a core trade row is legal -- and this repo's tests
-- and fixtures insert rows directly.  So every query that would DROP such a row
-- is fixed in the same commit; "correct today because no core trade exists yet"
-- stops being true here.  Nothing writes a core trade yet (step 3 owns the
-- executor), but nothing may silently lose one either.
--
-- Blast radius: `strategy_trades` has 0 rows on dev, as do
-- `strategy_funding_decisions`, `strategy_position_ownership`,
-- `strategy_deployments`, `strategy_core_mandate_events` and
-- `strategy_core_rebalance_intents`.  Reproduce:
--   SELECT count(*) FROM strategy_trades;   -- 0
-- So there is no backfill and no rewrite; every constraint below is added
-- against an empty table and can be stated at full strength.

---------------------------------------------------------------------------
-- 1.  The arc.
---------------------------------------------------------------------------

ALTER TABLE strategy_trades
    ADD COLUMN IF NOT EXISTS core_rebalance_intent_id BIGINT
        REFERENCES strategy_core_rebalance_intents(core_rebalance_intent_id)
        ON DELETE RESTRICT;

-- ⚠ DROP NOT NULL AUDIT (docs/review-prevention-log.md): every CHECK, index
-- predicate and generated column mentioning the column silently changes meaning
-- from "enforced" to "enforced except on NULL".  Run BEFORE relying on this:
--
--   SELECT conrelid::regclass, conname, pg_get_constraintdef(oid) FROM pg_constraint
--    WHERE pg_get_constraintdef(oid) LIKE '%funding_decision_id%';
--   SELECT tablename, indexname FROM pg_indexes WHERE indexdef LIKE '%funding_decision_id%';
--   SELECT table_name, column_name FROM information_schema.columns
--    WHERE generation_expression LIKE '%funding_decision%';
--
-- Result on dev 2026-08-14: three constraints
-- (`strategy_funding_decisions_pkey`, `strategy_trades_funding_decision_id_key`
-- UNIQUE, `strategy_trades_funding_decision_id_fkey`), two indexes (both the
-- above), ZERO CHECKs, zero partial-index predicates, zero generated columns.
-- The UNIQUE therefore weakens from "every trade has a funding decision, at most
-- one each" to "at most one each" -- which is intended, and the exactly-one CHECK
-- below restores the other half for both arms at once.
--
-- ⚠ The catalogue is only the DATABASE half.  The APPLICATION half is the same
-- audit and is larger: casts like `int(row["max_quote_age_seconds"])` and
-- `Decimal(str(row["entry_stop"]))` assume a funding-backed trade.  Those are
-- handled in the same commit, in the load predicates rather than here.
ALTER TABLE strategy_trades
    ALTER COLUMN funding_decision_id DROP NOT NULL;

-- Exactly one authorisation, never both and never neither.  `num_nonnulls` is
-- NULL-safe by construction, which is the point: a bare `a IS NULL <> b IS NULL`
-- pair invites the `col = x` passes-on-NULL trap (prevention log, #2679).
ALTER TABLE strategy_trades
    DROP CONSTRAINT IF EXISTS strategy_trades_exactly_one_authorisation;
ALTER TABLE strategy_trades
    ADD CONSTRAINT strategy_trades_exactly_one_authorisation CHECK (
        num_nonnulls(funding_decision_id, core_rebalance_intent_id) = 1
    );

-- One trade per authorisation, on the core arm as on the signal arm.  Without
-- this the two arms would enforce different cardinalities off one CHECK, and a
-- repeated rebalance evaluation could open a second position against a verdict
-- already acted on.  ⚠ A UNIQUE index does not constrain NULLs, so this bounds
-- the core arm only -- exactly as `strategy_trades_funding_decision_id_key` now
-- bounds only the signal arm.
CREATE UNIQUE INDEX IF NOT EXISTS strategy_trades_core_rebalance_intent_id_key
    ON strategy_trades (core_rebalance_intent_id)
    WHERE core_rebalance_intent_id IS NOT NULL;

COMMENT ON COLUMN strategy_trades.core_rebalance_intent_id IS
    'Core/cash arm authorisation (#2603 item 3). Exactly one of this and '
    'funding_decision_id is non-null. A trade on this arm has no signal, no '
    'deployment and no execution policy, so any query projecting those must '
    'tolerate NULL rather than INNER JOIN them away.';

COMMENT ON COLUMN strategy_trades.funding_decision_id IS
    'Signal arm authorisation. NULLABLE since sql/349: a core trade is '
    'authorised by core_rebalance_intent_id instead. NULL here does NOT mean '
    'unauthorised -- see strategy_trades_exactly_one_authorisation.';

---------------------------------------------------------------------------
-- 2.  The core arm's declared paper mode.
---------------------------------------------------------------------------
--
-- The signal arm's demo property is `strategy_deployments.mode = 'paper'`, which
-- the load predicate witnesses.  A mandate has no deployment, so without this the
-- core arm would ship with no equivalent link to witness.
--
-- ⚠ REQUIRED ON INSERT, NOT DEFAULTED.  A DEFAULT 'paper' would let a writer that
-- forgets the column inherit safety it never asked for; the next policy that
-- admits a second mode would then silently relabel those rows.
--
-- ⚠⚠ STATE PLAINLY WHAT THIS IS NOT.  An event-level constant records the
-- AUTHORITY'S DECLARED mode.  It does not record which account, environment or
-- broker credentials a trade actually used, and nothing here can.  The real
-- backstop remains the demo-only credential configuration and
-- `app/security/unattended_guard.py`.  Calling this "the equivalent of the
-- deployment gate" would overstate it; it is the same SHAPE of gate over a
-- weaker claim.
--
-- ⚠ ADDED NULLABLE, BACKFILLED, THEN CONSTRAINED -- three statements where one
-- would do TODAY.  Postgres cannot add a NOT NULL column with no default to a
-- non-empty table, and `strategy_core_mandate_events` is empty on dev (0 rows,
-- verified) only because `configure_core_mandate` currently has NO caller --
-- no endpoint, no job, no script.  That is a fact about today's wiring, not a
-- property of the schema, and it is the first thing the next slice changes.
-- The three-step form costs nothing on an empty table and keeps the migration
-- applicable to a machine that acquired a row before pulling it.
ALTER TABLE strategy_core_mandate_events
    ADD COLUMN IF NOT EXISTS mode TEXT;

UPDATE strategy_core_mandate_events SET mode = 'paper' WHERE mode IS NULL;

ALTER TABLE strategy_core_mandate_events
    ALTER COLUMN mode SET NOT NULL;

ALTER TABLE strategy_core_mandate_events
    DROP CONSTRAINT IF EXISTS strategy_core_mandate_events_mode_check;
ALTER TABLE strategy_core_mandate_events
    ADD CONSTRAINT strategy_core_mandate_events_mode_check CHECK (mode = 'paper');

COMMENT ON COLUMN strategy_core_mandate_events.mode IS
    'Declared execution mode of this mandate revision. CHECK-pinned to paper for '
    'the current policy. Records the authority''s declaration only -- NOT the '
    'account or credentials a resulting trade actually used.';
