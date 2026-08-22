-- 364_core_rebalance_intent_reader_correction.sql
--
-- #2603 step 3b-3.  A COMMENT-only migration correcting a stale safety claim.
--
-- `sql/348`'s header and table comment both say of
-- `strategy_core_rebalance_intents`: "no table has a foreign key to it and no
-- module reads it, so no code path can turn a row into an action".  BOTH HALVES
-- ARE NOW FALSE:
--
--   * `sql/349` added `strategy_trades.core_rebalance_intent_id`, an FK to this
--     table;
--   * `app/services/strategy_core_submission_gate.py` SELECTs from it to decide
--     whether a stored verdict may become an order.
--
-- The reading side arrived exactly as sql/348 promised it would.  The promise
-- was not re-read when it did, and it survived because it SOUNDED structural --
-- "mechanical rather than a promise" is what a reader trusts and stops checking.
--
-- ⚠ Why a new file rather than an edit to sql/348: the migration runner pins
-- each applied file's content_sha256, so editing an applied migration -- even
-- its comments -- fails the boot check with "content changed since applied".
-- A COMMENT ON is idempotent and re-issuing it is the cheapest correct way to
-- put the true text where `\d+` shows it.
--
-- What holds instead, deliberately weaker: a row here is submission-gate INPUT,
-- not authority.  The gate has no acting caller anywhere in `app/` or
-- `scripts/`, so no path runs from a row to an order.  That is a fact about
-- today's call graph, NOT a mechanical impossibility -- and saying so is the
-- point.
--
-- The producer is `app/workers/scheduler.py::core_rebalance_observation`.
--
-- Spec: docs/proposals/ta/2026-08-22-core-rebalance-observation-job.md

COMMENT ON TABLE strategy_core_rebalance_intents IS
    'Append-only record of every core/cash rebalance EVALUATION, including holds '
    'and refusals -- a verdict is evidence, and a sleeve correctly holding for a '
    'month is the normal case. Written by '
    'app/workers/scheduler.py::core_rebalance_observation. '
    'A row is submission-gate INPUT, not authority: strategy_trades has an FK to '
    'this table (sql/349) and strategy_core_submission_gate reads it, but that '
    'gate has no acting caller, so no code path runs from a row here to an order. '
    'That is a fact about the current call graph, not a mechanical impossibility '
    '-- sql/348''s original claim that nothing reads this table was true when '
    'written and is not true now.';
