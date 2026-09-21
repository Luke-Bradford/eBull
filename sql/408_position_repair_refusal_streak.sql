-- #3284 item 4a — a repair that refuses on every visit must leave a trace.
--
-- Items 1-3 made the core sleeve's stop and target derived, submitted at open and
-- re-verified every five minutes.  Item 4 is the ticket's remaining clause: *"if the
-- stop cannot be set after N attempts, that is an alert-class event, not a silent
-- carry-on"*.  Before anything can count attempts, a refused attempt has to be
-- DURABLE, and today it is not.
--
-- WHAT IS ACTUALLY INVISIBLE TODAY.  `manage_owned_position` returns
-- `PositionManagerResult(..., "rejected", reason)` for both fixed-exit refusals —
-- `broker_fixed_exit_edit_not_allowed` and `fixed_exit_quote_unsafe` — and BOTH return
-- before `_persist_edit_intent` is reached, so no operation row exists.  The caller
-- (`strategy_paper_runtime.run_strategy_paper_cycle`) discards the result entirely and
-- increments `managed` on the ATTEMPT, not the outcome.  Measured on dev before this
-- migration::
--
--     SELECT operation_type, status, count(*)
--     FROM strategy_position_operations GROUP BY 1,2;
--     -- one group: fixed_exit_repair | applied | 2
--
-- There has never been a `rejected` row on this path and there cannot be one.  So a
-- position that refuses repair on every single visit is indistinguishable from one that
-- needed no repair: the cycle reports `success`, `managed` counts up, and the position
-- stays NAKED.  That is the self-consistent-success class the prevention log names, on
-- the safety net the operator asked for.
--
-- ⛔ WHY NOT `strategy_position_operations`, WHICH ALREADY HAS A `rejected` STATUS.
-- Because `sql/406`'s `idx_strategy_position_operation_material_identity` is UNIQUE on
-- (ownership_id, operation_type, desired_stop_rate, desired_take_profit_rate,
-- completed_bar_at) NULLS NOT DISTINCT WHERE status <> 'applied'.  A core
-- `fixed_exit_repair` passes `bar=None` and its desired levels are a pure function of
-- the position's unchanged entry, so EVERY refusal for one ownership collapses onto the
-- same index key: the first would insert and the second would raise `UniqueViolation`.
--
-- That index is not in the way by accident — it is the guard that stops us re-entering
-- an edit the broker already refused, every five minutes, forever.  Counting a streak
-- and forbidding re-entry are in direct tension in that table, so the counter gets its
-- own row, keyed on the ownership alone and on nothing that varies with the levels.
--
-- GRAIN: one row per ownership, so it is bounded by the sleeve and not by time.  A
-- streak is a state, not an event log; the events that DID reach the broker are already
-- in `strategy_position_operations` and nothing here replaces them.
--
-- ⚠ `last_checked_at` means "the last visit on which the fixed-exit arm EVALUATED this
-- ownership", not "the last paper cycle".  A visit that closes the position, ages it
-- out, or finds it missing returns before the arm and says nothing about repair, so it
-- deliberately does not stamp.  Reading this column as a cycle heartbeat would make a
-- closing position look like a stalled check.
--
-- ⚠ ON DELETE RESTRICT, matching every other reference to this table.  Ownership rows
-- are RELEASED (`status='released'`), never deleted; a cascade here would quietly
-- license a delete path that does not exist.
--
-- The thresholds are NOT here.  Item 4b reads this state and decides what alerts; the
-- split exists so the recording lands and starts accruing evidence before any alarm is
-- wired to it.

CREATE TABLE IF NOT EXISTS strategy_position_repair_streaks (
    ownership_id          BIGINT PRIMARY KEY
        REFERENCES strategy_position_ownership(ownership_id) ON DELETE RESTRICT,
    consecutive_refusals  INTEGER NOT NULL DEFAULT 0
        CHECK (consecutive_refusals >= 0),
    first_refused_at      TIMESTAMPTZ,
    last_refusal_reason   TEXT,
    last_checked_at       TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- A zero streak carries no refusal detail and a live streak must carry both.
    --
    -- ⚠ `last_refusal_reason IS NOT NULL AND <> ''`, not the bare `<> ''` that the
    -- `strategy_position_ownership_release_shape` precedent uses.  A comparison against
    -- NULL yields NULL, and a CHECK passes on NULL — so the bare form admits exactly
    -- the row the constraint is written to forbid (a live streak with no reason).
    CONSTRAINT strategy_position_repair_streak_shape CHECK (
        (consecutive_refusals = 0 AND first_refused_at IS NULL AND last_refusal_reason IS NULL)
        OR (
            consecutive_refusals > 0
            AND first_refused_at IS NOT NULL
            AND last_refusal_reason IS NOT NULL
            AND last_refusal_reason <> ''
        )
    )
);

COMMENT ON TABLE strategy_position_repair_streaks IS
    '#3284 item 4a. One row per ownership: consecutive fixed-exit repair refusals, '
    'incremented when the repair arm declines or a submitted edit is observed NOT in '
    'effect. ⚠ Only BROKER-CONFIRMED protection resets it — the levels observed in '
    'place, or an edit confirmed against the broker''s own rates. Acceptance of an edit '
    '(202) does NOT reset it: acknowledgement is not landing, and a submitted edit whose '
    'levels never arrive is never terminalised, so clearing on acceptance left a '
    'permanently naked position reading zero. Exists because both refusals return before any '
    'strategy_position_operations row is written, and because sql/406''s UNIQUE '
    'material-identity index collapses every refusal for one ownership onto a single '
    'key — so a row-per-attempt ledger cannot count a streak. Item 4b reads this to '
    'decide what alerts; the thresholds are not stored here.';

COMMENT ON COLUMN strategy_position_repair_streaks.consecutive_refusals IS
    'Consecutive VISITS on which the fixed-exit arm refused, not wall-clock and not a '
    'lifetime total. _OWNED_BATCH_SQL rotates the owned batch by five-minute slot, so '
    'visit rate is a function of sleeve size; a time-based counter would silently mean '
    'different things at different sizes. Reset to 0 makes a later failure a new episode.';

COMMENT ON COLUMN strategy_position_repair_streaks.last_checked_at IS
    'Last visit on which the fixed-exit arm EVALUATED this ownership — not the last '
    'paper cycle. A visit that closes, ages out or fails to find the position returns '
    'before the arm and does not stamp.';
