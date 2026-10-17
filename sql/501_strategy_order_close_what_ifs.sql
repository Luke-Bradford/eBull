-- 501_strategy_order_close_what_ifs.sql
--
-- #3611 slice 2 — the broker's close-arm what-if cost, quoted immediately before a strategy close is submitted.
-- The open-arm quote does not bound the close (`BrokerWhatIfOrder` docstring, #2712: close dearer on 4 of 5 held
-- positions, by up to 18.5x), so the cost of an exit is measured on the close arm itself, bound to the exact position
-- being closed (`action=close`, `transaction=sell`, `positionIds=[id]`).
--
-- One row per exit order, written once, before the broker write, best-effort: a failed quote records `failed` and
-- the close is submitted either way. `ticket_amount` is the USD amount the quote was asked for: the position's units
-- at the arrival bid fetched just before (the broker's portfolio carries no current mark), so `cost / ticket_amount`
-- is the quoted cost rate.
--   'quoted'      — every returned component decoded to USD and one field (`amount` or `value`, #2598); `cost` is
--                   their sum, `quoted_at` the response's lastUpdated.
--   'undecodable' — the broker answered, but the response did not decode by that rule; `raw_payload` keeps it.
--   'failed'      — the request raised.
--   'unpriced'    — no usable arrival bid to size the ticket, so nothing was requested; `ticket_amount` is NULL.
-- Writer: `app.services.execution_tca.record_close_what_if` (the momentum book's exits, via `manage_owned_position`).
-- Reader: `app.services.execution_tca.load_exit_tca` (`scripts/tca_readout.py`).

BEGIN;

CREATE TABLE IF NOT EXISTS strategy_order_close_what_ifs (
    order_id      BIGINT PRIMARY KEY REFERENCES orders(order_id) ON DELETE RESTRICT,
    requested_at  TIMESTAMPTZ NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL,
    ticket_amount NUMERIC CHECK (ticket_amount > 0),
    outcome       TEXT NOT NULL CHECK (outcome IN ('quoted', 'undecodable', 'failed', 'unpriced')),
    cost          NUMERIC,
    currency      TEXT,
    quoted_at     TIMESTAMPTZ,
    raw_payload   JSONB,
    CHECK (received_at >= requested_at),
    CHECK (
        CASE outcome
            WHEN 'unpriced'
                THEN ticket_amount IS NULL AND cost IS NULL AND currency IS NULL AND quoted_at IS NULL
                     AND raw_payload IS NULL
            WHEN 'quoted'
                THEN ticket_amount IS NOT NULL AND cost IS NOT NULL AND cost >= 0 AND currency IS NOT NULL
                     AND quoted_at IS NOT NULL AND raw_payload IS NOT NULL
            WHEN 'undecodable'
                THEN ticket_amount IS NOT NULL AND cost IS NULL AND currency IS NULL AND raw_payload IS NOT NULL
            ELSE ticket_amount IS NOT NULL AND cost IS NULL AND currency IS NULL AND quoted_at IS NULL
                 AND raw_payload IS NULL
        END
    )
);

COMMENT ON TABLE strategy_order_close_what_ifs IS
    'The eToro close-arm what-if cost quoted immediately before a strategy close was submitted (#3611). '
    'Best-effort evidence for transaction-cost analysis; never a gate.';

COMMIT;
