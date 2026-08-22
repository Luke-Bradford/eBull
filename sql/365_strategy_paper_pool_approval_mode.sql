-- 365_strategy_paper_pool_approval_mode.sql
--
-- #2843.  The autonomy flag: `approval_mode` on the mandate.
--
-- Operator decision 2026-08-22 (docs/settled-decisions.md, "Live-capital
-- approval is a mandate FLAG, not a person-gate"), reversing the prior
-- person-gate.  Under `autonomous` the engine approves promotions on evidence
-- alone.  The flag flips WHO approves; it flips nothing about WHAT qualifies.
--
-- ⚠ WHY THIS TABLE AND NOT A NEW ONE.  The mandate is already stored here:
-- sql/311 hung the whole portfolio-risk mandate off `strategy_paper_pool_events`
-- as columns, and #2844 established that minting a parallel surface for
-- something already stored is the defect (it added no column at all, because
-- `capital_limit`/`capital_mode` already were the operator's "assigned capital,
-- capped or expanding").  This is an append-only operator-change table, so the
-- flag inherits its versioning and its audit trail from the table's own shape:
-- the authority in force at any promotion is the latest event at or before it.
--
-- ⚠ "Latest by event id" is safe rather than assumed: `configure_paper_pool`
-- holds `PAPER_ALLOCATOR_ADVISORY_LOCK` for its whole transaction, so two
-- revisions cannot commit out of id order.
--
-- Measured on dev, the only database:
--   select count(*) from strategy_paper_pool_events;  -> 0
--   select count(*) from strategy_promotions;         -> 0
-- Nothing to backfill and no promotion this can retroactively reclassify.
--
-- ⚠ The dev row count is NOT the safety argument, because it is a fact about
-- one database.  The DEFAULT is.  sql/311 deliberately preserves legacy
-- `enabled` + `unconfigured` events; every such row takes `approval_mode =
-- 'manual'` and satisfies the CHECK below unchanged.  Only a NEW row can be
-- `autonomous`, and a new row goes through `configure_paper_pool`.

ALTER TABLE strategy_paper_pool_events
    ADD COLUMN IF NOT EXISTS approval_mode TEXT NOT NULL DEFAULT 'manual';

-- DROP-then-ADD, matching sql/311's own pattern.  A bare `ADD CONSTRAINT` is
-- not rerunnable: a replayed or partially applied migration fails on the
-- existing constraint name rather than converging.
ALTER TABLE strategy_paper_pool_events
    DROP CONSTRAINT IF EXISTS strategy_paper_pool_approval_mode;
ALTER TABLE strategy_paper_pool_events
    ADD CONSTRAINT strategy_paper_pool_approval_mode CHECK (
        approval_mode IN ('manual', 'autonomous')
        -- An unconfigured mandate authorises nothing, so it cannot authorise a
        -- policy approver either.
        --
        -- ⚠ DECLARED DEPENDENCY.  `risk_profile <> 'unconfigured'` is the exact
        -- definition of `PortfolioMandate.configured`, and it is only as strong
        -- as sql/311's `strategy_paper_pool_mandate_shape` -- that constraint is
        -- what forces a non-`unconfigured` profile to carry complete, in-range
        -- v1 limits.  This clause is meaningful only while that one stands.
        AND (approval_mode = 'manual' OR risk_profile <> 'unconfigured')
    );

COMMENT ON COLUMN strategy_paper_pool_events.approval_mode IS
    'Who may approve a stage promotion under this authority: manual = an authenticated '
    'operator only; autonomous = additionally the policy approver in '
    'app/services/strategy_autonomous_promotion.py, which stamps promoted_by = '
    'policy@<AUTONOMY_POLICY_VERSION>. Evidence bars are identical under both (#2843).';
