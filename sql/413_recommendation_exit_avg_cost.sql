-- #3007 part 2, slice 1 — record the pool's average cost with the EXIT lot at claim time.
--
-- Spec: docs/proposals/execution/2026-09-23-late-exit-fill-booking.md.
--
-- A late EXIT fill's realized P&L is `(close rate - avg_cost) * units`, which is the
-- synchronous path's formula (`order_client._update_position_exit`). But the booking
-- runs up to one poll interval after the close. By then `positions.avg_cost` may
-- belong to a different pool: a later BUY/ADD recomputes it, and `portfolio_sync`
-- imports a new one when it reopens a row externally. Only the submission moment has
-- the cost the lot was disposed against. So it is stored then, like the lot itself
-- (`sql/409`).
--
-- NULL means NOT RECORDED. That covers every pre-413 row, every non-lot row, and a
-- lot whose `positions.avg_cost` was NULL, non-positive or NaN at claim time. The
-- booking slice must refuse a NULL. ⚠ An unusable cost never refuses the EXIT
-- itself: EXIT is never blocked (`.claude/skills/execution-guard/SKILL.md`).
--
-- A separate CHECK, not an extension of `orders_recommendation_exit_lot_check`. That
-- way a post-409 / pre-413 lot row stays valid with a NULL cost, and a worker still on
-- the pre-413 code writes a legal row during rollout.
--
-- Measured on dev before this migration:
-- `SELECT count(*) FROM orders WHERE recommendation_id IS NOT NULL` = 0.

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS recommendation_exit_avg_cost NUMERIC(18, 6);

ALTER TABLE orders
    DROP CONSTRAINT IF EXISTS orders_recommendation_exit_avg_cost_check;
ALTER TABLE orders
    ADD CONSTRAINT orders_recommendation_exit_avg_cost_check CHECK (
        recommendation_exit_avg_cost IS NULL
        -- Explicit IS NOT NULL on every column. An UNKNOWN arm passes a CHECK. And
        -- `'NaN'::numeric > 0` is TRUE in Postgres, so NaN is refused by name
        -- (the sql/409 lesson).
        OR (
            action = 'EXIT'
            AND recommendation_exit_position_id IS NOT NULL
            AND recommendation_exit_avg_cost > 0
            AND recommendation_exit_avg_cost <> 'NaN'::numeric
        )
    );

COMMENT ON COLUMN orders.recommendation_exit_avg_cost IS
    'positions.avg_cost of the instrument when the claim INSERT recorded the EXIT lot '
    '(#3007). The cost the lot is disposed against for realized P&L. NULL = not recorded '
    '(pre-413, non-lot, or unusable avg_cost at claim time).';
