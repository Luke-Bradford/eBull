-- 500_strategy_order_arrival_quotes.sql
--
-- #3611 — the broker's bid and ask fetched immediately before a strategy order is submitted (its "arrival" quote).
-- The preflight's `quote_ask` is the price the order was sized and protected from. It can be minutes old at
-- submission (`scripts/tca_readout.py --since 2026-10-15`, column `decision_age_s`), so fill vs `quote_ask` mixes
-- price drift with execution cost.
-- A quote fetched seconds before submission separates the two: delay = arrival mid vs decision price, execution =
-- fill vs arrival mid, effective spread = 2·|fill − mid| / mid (SEC Rule 605's definition, mid at order receipt).
--
-- One row per order, written once, before the broker write, best-effort: a failed fetch records `failed` and the
-- order is still submitted. `quoted_at` is the rate's own timestamp from eToro, never the fetch time; eToro serves
-- some US equities a quote 1–2 hours late (#3833), so the readout judges the quote's age, not the row's.
--   'quoted'     — eToro returned a rate for the instrument; `quoted_at`, `bid`, `ask` hold it.
--   'not_quoted' — the rates call succeeded and omitted the instrument.
--   'failed'     — the rates call raised.
-- Writer: `app.services.execution_tca.record_arrival_quote` (called by `momentum_book_executor`).
-- Reader: `app.services.execution_tca.load_entry_tca` (`scripts/tca_readout.py`).

BEGIN;

CREATE TABLE IF NOT EXISTS strategy_order_arrival_quotes (
    order_id     BIGINT PRIMARY KEY REFERENCES orders(order_id) ON DELETE RESTRICT,
    requested_at TIMESTAMPTZ NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL,
    outcome      TEXT NOT NULL CHECK (outcome IN ('quoted', 'not_quoted', 'failed')),
    quoted_at    TIMESTAMPTZ,
    bid          NUMERIC,
    ask          NUMERIC,
    CHECK (received_at >= requested_at),
    CHECK (
        CASE WHEN outcome = 'quoted'
             THEN quoted_at IS NOT NULL AND bid IS NOT NULL AND ask IS NOT NULL AND bid > 0 AND ask > 0
             ELSE quoted_at IS NULL AND bid IS NULL AND ask IS NULL
        END
    )
);

COMMENT ON TABLE strategy_order_arrival_quotes IS
    'The eToro bid/ask fetched immediately before a strategy order was submitted (#3611). quoted_at is the rate''s '
    'own timestamp. Best-effort evidence for transaction-cost analysis; never a gate.';

COMMIT;
