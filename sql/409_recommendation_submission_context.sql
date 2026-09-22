-- #3007 part 1 / #2942 slice C prerequisite — persist a recommendation order's
-- submission context at claim time.
--
-- Booking a late fill (`order_client._record_unbooked_fill`) is blocked on data that
-- exists only while `execute_order` is running:
--
--   * the EXIT lot. `_load_exit_lot` resolves ONE broker position at submission, and
--     `broker.close_position(exit_pos_id, ...)` closes exactly that position. Nothing
--     stores which one. `orders.requested_units` holds its units but not its id, and
--     rounds them to numeric(18,6). Re-selecting at poll time is the
--     instrument/time/FIFO guess #2942 forbids.
--   * the `OrderParams` sent to `place_order`. They are built from
--     `trade_recommendations.stop_loss_rate` / `take_profit_rate`, which the timing
--     scheduler rewrites (`scheduler.py` timing arm). So a poll-time re-read is not
--     guaranteed to reproduce what was sent, and `_persist_broker_position` records the
--     sent values.
--
-- ⚠ `quote_data` is deliberately NOT stored. On the live path `execute_order` never
-- loads it before the broker call (only the demo branch does, and a synthetic fill is
-- never `pending`). The synchronous live path's only reader, cost recording, loads it
-- lazily AFTER the call. So there is no submission-time quote to lose.
--
-- NULL on all three means NOT RECORDED: every pre-409 row, and every row written by
-- the demo / no-lot `_persist_order` path, which never reaches the broker. Nothing
-- is backfilled, because the lot of a pre-409 EXIT cannot be recovered without the
-- guess this migration exists to avoid. Measured on dev before this migration:
-- `SELECT count(*) FROM orders WHERE recommendation_id IS NOT NULL` = 0.

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS recommendation_exit_position_id BIGINT,
    ADD COLUMN IF NOT EXISTS recommendation_exit_units NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS recommendation_submission_context JSONB;

ALTER TABLE orders
    DROP CONSTRAINT IF EXISTS orders_recommendation_exit_lot_check;

-- The lot is one fact: both columns or neither. Only on an EXIT. Both strictly
-- positive, because `_load_exit_lot` excludes synthetic (negative) ids and a zero-unit
-- lot is not closeable. Numeric(20,8) is `broker_positions.units`' own scale, so the
-- stored lot is the lot that was selected, not a rounding of it.
ALTER TABLE orders
    ADD CONSTRAINT orders_recommendation_exit_lot_check CHECK (
        (recommendation_exit_position_id IS NULL AND recommendation_exit_units IS NULL)
        -- ⚠ Every column is tested with an explicit IS NOT NULL: a NULL inside a CHECK
        -- makes the arm UNKNOWN, and Postgres ACCEPTS an unknown CHECK, so `> 0` alone
        -- would admit a half-written lot (Codex checkpoint 2).  And `'NaN'::numeric > 0`
        -- is TRUE in Postgres (NaN sorts above every number), so NaN is refused by name.
        OR (
            action = 'EXIT'
            AND recommendation_exit_position_id IS NOT NULL
            AND recommendation_exit_units IS NOT NULL
            AND recommendation_exit_position_id > 0
            AND recommendation_exit_units > 0
            AND recommendation_exit_units <> 'NaN'::numeric
            -- One record, written by one INSERT: a lot without its context would be
            -- half a record, so the reader would have to choose which half to trust.
            AND recommendation_submission_context IS NOT NULL
        )
    );

COMMENT ON COLUMN orders.recommendation_exit_position_id IS
    'The exact broker position a recommendation EXIT asked eToro to close, recorded in '
    'the claim INSERT before the broker call (#3007/#2942). NULL = not recorded.';
COMMENT ON COLUMN orders.recommendation_exit_units IS
    'Units of recommendation_exit_position_id when it was selected, at broker_positions '
    'scale. Descriptive: the close is sent WHOLE (UnitsToDeduct=null).';
COMMENT ON COLUMN orders.recommendation_submission_context IS
    'The OrderParams sent with this recommendation order, as {"order_params": null | '
    '{stop_loss_rate, take_profit_rate, is_tsl_enabled, leverage}} with rates as decimal '
    'strings. NULL = not recorded (pre-409 or never sent to a broker).';
