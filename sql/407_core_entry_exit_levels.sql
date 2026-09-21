-- #3284 item 1 — the core entry's stop and target, committed BEFORE the broker call.
--
-- Items 2-5 (`18012b77`, `fa81bf7c`) made the core sleeve's stop and target derived,
-- verified and repaired on the five-minute cycle.  That closes the steady state and
-- leaves one window open: a NEW core entry is still born naked and protected only by
-- the NEXT cycle.  This table is what closes it.
--
-- WHY A TABLE AND NOT JUST A LOCAL VARIABLE AT SUBMIT TIME.  Two reasons, and the
-- FIRST draft of this comment gave a third that is FALSE -- recorded here because a
-- later reader would otherwise inherit it.  It claimed the resume path re-reads the
-- authority and replays the same `request_id`, so recomputing the rates would send a
-- different body under an accepted idempotency key.  `resume_core_submission`
-- RECONCILES and never resubmits (its own docstring: "without ever retrying its
-- mutation"), so that path does not exist.  Codex checkpoint 2 caught it.
--
-- The two real reasons:
--
--   1. ORDERING.  Deriving the rates BEFORE the `orders` INSERT is what makes an
--      underivable anchor refuse while there is still nothing durable to refuse
--      against.  A derivation that happens at the broker call instead can only fail
--      after an authority exists, and a failed authority blocks every later core entry.
--   2. PROVENANCE.  This row is the only record of what the OPEN BODY claimed.  The
--      five-minute repair re-anchors on `broker_positions.open_price` (the true fill)
--      and will write a DIFFERENT pair within minutes of any entry; without the
--      submitted pair stored, that first repair is unexplainable after the fact.  That
--      is not hypothetical -- reconstructing the 2026-09-21 level incident depended
--      entirely on `prior_stop_rate` having been recorded on each operation row.
--
-- ⚠ It also holds the line if the resume path is ever ALLOWED to resubmit: the rates
-- would then have to be reproducible from committed state, and they already are.  That
-- is a consequence, not the justification.
--
-- `orders` carries no SL/TP columns (`raw_payload_json` is written NULL on this path),
-- so this is a store of its own rather than two columns on an existing table.
--
-- SHAPE PRECEDENT: `strategy_entry_preflights` (the signal arm) already persists
-- `stop_loss_rate` / `take_profit_rate` inside the authority transaction, before the
-- broker call, and `strategy_position_manager` reads them back as `entry_stop`.  This
-- is the core arm's analogue at the core arm's grain -- one row per ORDER, because the
-- core authority is an order and `strategy_entry_preflights` is keyed on a signal the
-- core arm does not have.
--
-- ⚠ ONE ROW PER ORDER, and `order_id` is the primary key rather than a unique index on
-- a surrogate.  A second row would be a second opinion about the same submitted body,
-- and there is no rule for choosing between them.
--
-- ⚠ APPEND-ONLY BY CONSTRUCTION, and that is deliberate rather than enforced by a
-- trigger: the row records what was SENT.  The rates the position should carry LATER
-- are re-derived every cycle from the broker's own `open_price` (`core_exit_levels`),
-- which is the true fill and not this anchor.  Updating this row to match a repair
-- would destroy the only record of what the submitted body actually claimed.
--
-- ⚠ `anchor_rate` is stored ALONGSIDE the two rates rather than instead of them.  It
-- is not redundant: it is the evidence for the derivation, and it is the quantity that
-- diverges from the fill.  A market order fills at a price that is not the anchor, so
-- a submitted stop is approximately -50% of the fill, not exactly; the cycle's repair
-- re-anchors to `broker_positions.open_price` and corrects it.  Without the anchor
-- stored, that first correction is unexplainable after the fact.
--
-- The anchor is the ASK returned by `preflight_core_submission`, taken inside the same
-- `core_submission_lock` hold as the INSERT below and already gated on
-- `CORE_MAX_QUOTE_AGE_SECONDS` -- which `CORE_EXIT_MAX_QUOTE_AGE_SECONDS` re-exports,
-- so the anchor's freshness bound and the repair's are one policy, not two that agree.
--
-- ⚠ NO BACKFILL, and the population is why: at the time of writing this repo holds
-- exactly ONE core order (`order_id` 2) and it is `resolved`, so
-- `load_core_resume_authority`'s `state NOT IN ('resolved','rejected')` filter cannot
-- return it.  Reproduce with `SELECT state.order_id, state.state FROM
-- strategy_order_reconciliation_state state JOIN strategy_trade_orders link USING
-- (order_id) JOIN strategy_trades t USING (strategy_trade_id) WHERE
-- t.core_rebalance_intent_id IS NOT NULL`.
--
-- ⚠⚠ "Zero rows today" is NOT the safety argument, and treating it as one is the
-- documented failure mode (prevention log, 2026-09-21, "a guard justified by no such
-- row can exist today").  A pre-`sql/407` authority LOADS NORMALLY with NULL rates:
-- the only thing that can still happen to it is `resume_core_submission`, which
-- reconciles and never resubmits, so it needs no rates.  What it cannot do is reach
-- `_submit_core_authority_locked`, which raises on a NULL pair.  The refusal is at the
-- submit site, not the load site -- a guard at the load site would have made exactly
-- those authorities unreconcilable, which is the first draft this comment replaces.

CREATE TABLE IF NOT EXISTS strategy_core_entry_exit_levels (
    order_id             BIGINT PRIMARY KEY
                         REFERENCES orders(order_id) ON DELETE CASCADE,
    -- The quote the rates were derived from, and when the broker observed it.
    anchor_rate          NUMERIC NOT NULL CHECK (anchor_rate > 0),
    anchor_quoted_at     TIMESTAMPTZ NOT NULL,
    -- The two absolute rates submitted in the open body.  NOT NULL is the whole
    -- point of the table: a core authority that cannot express its exit levels must
    -- not be submittable at all.
    stop_loss_rate       NUMERIC NOT NULL CHECK (stop_loss_rate > 0),
    take_profit_rate     NUMERIC NOT NULL CHECK (take_profit_rate > 0),
    -- Cheap, and it catches an inverted pair that both CHECKs above would admit.
    CONSTRAINT strategy_core_entry_exit_levels_ordered
        CHECK (take_profit_rate > stop_loss_rate),
    -- The stop must sit BELOW the anchor and the target above it.  Guaranteed by
    -- `core_exit_levels` arithmetic, restated here because a body with a stop above
    -- the market is one eToro may accept and then trigger immediately.
    CONSTRAINT strategy_core_entry_exit_levels_straddle_anchor
        CHECK (stop_loss_rate < anchor_rate AND take_profit_rate > anchor_rate),
    -- Which frozen constants produced them, so a stored pair can be read back against
    -- the policy that derived it (`CORE_EXIT_POLICY_VERSION`).
    policy_version       TEXT NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE strategy_core_entry_exit_levels IS
    '#3284 item 1: the stop/target submitted with one core entry, committed with the '
    'durable authority so a resume replays the same body under the same request_id.';
