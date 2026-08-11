-- #2429 -- make the performance accounting basis explicit and immutable.
--
-- Historical result rows used split-adjusted raw close for both executable
-- levels and portfolio returns. They remain truthful v1 identities: Python's
-- ResultIdentity deliberately preserves their original hash when this legacy
-- literal is present. New runs use split-and-dividend-adjusted wealth and emit
-- a v2 identity. No historical metric is rewritten.

BEGIN;

ALTER TABLE strategy_results_store
    ADD COLUMN IF NOT EXISTS return_basis TEXT;

UPDATE strategy_results_store
   SET return_basis = 'raw-close-price-return-v1'
 WHERE return_basis IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'strategy_results_store'::regclass
          AND conname = 'strategy_results_return_basis_known'
    ) THEN
        ALTER TABLE strategy_results_store
            ADD CONSTRAINT strategy_results_return_basis_known
            CHECK (return_basis IN ('raw-close-price-return-v1', 'split-dividend-adjusted-wealth-v1'));
    END IF;
END
$$;

ALTER TABLE strategy_results_store
    ALTER COLUMN return_basis SET NOT NULL;

COMMENT ON COLUMN strategy_results_store.return_basis IS
    'Portfolio wealth/return accounting basis. Raw OHLC remains authoritative for signals, fills, spreads and TP/SL.';

-- SELECT * is expanded when a view is created, so expose the new identity
-- member and restore the check option that CREATE OR REPLACE drops.
CREATE OR REPLACE VIEW strategy_results AS
    SELECT *
    FROM strategy_results_store
    WHERE namespace = 'in_sample';

ALTER VIEW strategy_results SET (check_option = 'cascaded');

COMMIT;
