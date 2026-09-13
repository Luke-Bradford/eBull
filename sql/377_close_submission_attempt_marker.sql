-- #2979: a close crash must say WHICH side of the broker call it died on.
--
-- `_submit_close` commits the close intent, then calls the broker, then records
-- the broker's order ref.  A crash anywhere in that span left the operation at
-- `intent_persisted`, so "the broker never received it" (harmless, re-requestable)
-- and "the broker executed it" (a permanent ownership wedge) produced the
-- IDENTICAL row -- #2949 round 2 demonstrated both across a real process boundary
-- as scenarios 7a and 7b.
--
-- `submitting` is a durable marker committed immediately BEFORE the broker verb is
-- entered.  It splits that span at the only boundary we can observe from our own
-- side:
--
--   `intent_persisted` -> the marker never committed, so the broker call was never
--                         entered.  PROVABLY not submitted; the request can be
--                         abandoned and re-requested without risking a second close.
--   `submitting`       -> the verb was entered.  What the broker did is unknown
--                         and stays unknown until close identity can be resolved
--                         (#2961's blocked `orders:lookup?referenceId=` question).
--
-- This does NOT resolve `submitting`; #2979 stays open for that half.  It stops the
-- unambiguous case being charged the ambiguous case's cost.

ALTER TABLE strategy_position_operations
    DROP CONSTRAINT IF EXISTS strategy_position_operations_status_check;
ALTER TABLE strategy_position_operations
    DROP CONSTRAINT IF EXISTS strategy_position_operations_status_vocabulary;
ALTER TABLE strategy_position_operations
    ADD CONSTRAINT strategy_position_operations_status_vocabulary CHECK (
        status IN ('intent_persisted', 'submitting', 'submitted',
                   'applied', 'rejected', 'reconcile_required')
    );

-- The resolution shape: `submitting` is an IN-FLIGHT status, so it carries no
-- resolved_at.  Dropped by its generated name (`..._check4`) and re-added under an
-- explicit one so the next change to it does not have to count checks again.
ALTER TABLE strategy_position_operations
    DROP CONSTRAINT IF EXISTS strategy_position_operations_check4;
ALTER TABLE strategy_position_operations
    DROP CONSTRAINT IF EXISTS strategy_position_operations_resolution_shape;
ALTER TABLE strategy_position_operations
    ADD CONSTRAINT strategy_position_operations_resolution_shape CHECK (
        (status IN ('intent_persisted', 'submitting', 'submitted') AND resolved_at IS NULL)
        OR (status IN ('applied', 'rejected', 'reconcile_required') AND resolved_at IS NOT NULL)
    );

-- ⚠ LOAD-BEARING, and the reason the index is rebuilt rather than left alone: this
-- is the "one unresolved operation per ownership" guard, which is what stops a
-- second close being requested while one is outstanding.  A `submitting` row
-- omitted from the predicate would fall OUT of that guard -- i.e. the one status
-- that means "the broker may already be holding a close" would be the one status
-- that permits another.
DROP INDEX IF EXISTS idx_strategy_position_one_unresolved_operation;
CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_position_one_unresolved_operation
    ON strategy_position_operations (ownership_id)
    WHERE status IN ('intent_persisted', 'submitting', 'submitted');

COMMENT ON INDEX idx_strategy_position_one_unresolved_operation IS
    'One unresolved operation per ownership. Covers submitting (#2979): that status '
    'means the broker may already hold a close, so it must block a second request.';
