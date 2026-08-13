-- #2443 -- distinguish harness controls from candidates that may seek capital.
-- Purpose is stamped on each immutable result so a later manifest edit cannot
-- retroactively turn control evidence into investable evidence.

BEGIN;

ALTER TABLE strategy_results_store
    ADD COLUMN purpose TEXT;

UPDATE strategy_results_store
   SET purpose = 'harness_validation'
 WHERE strategy_id IN (
    's1-time-series-momentum',
    's2-cross-sectional-momentum',
    's3-mean-reversion-in-trend',
    's4-volatility-compression-breakout'
 );

DO $$
DECLARE
    unclassified TEXT;
BEGIN
    SELECT string_agg(DISTINCT strategy_id, ', ' ORDER BY strategy_id)
      INTO unclassified
      FROM strategy_results_store
     WHERE purpose IS NULL;
    IF unclassified IS NOT NULL THEN
        RAISE EXCEPTION
            'strategy result purpose migration has no classification for: %. Add an explicit classification before applying.',
            unclassified
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
END $$;

ALTER TABLE strategy_results_store
    ALTER COLUMN purpose SET NOT NULL;

ALTER TABLE strategy_results_store
    ADD CONSTRAINT strategy_results_purpose_known
    CHECK (purpose IN ('harness_validation', 'capital_candidate'));

CREATE OR REPLACE FUNCTION enforce_strategy_result_purpose_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.purpose IS DISTINCT FROM OLD.purpose THEN
        RAISE EXCEPTION 'strategy result purpose is immutable once written'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_strategy_result_purpose_immutable
BEFORE UPDATE OF purpose ON strategy_results_store
FOR EACH ROW EXECUTE FUNCTION enforce_strategy_result_purpose_immutable();

COMMENT ON COLUMN strategy_results_store.purpose IS
    'Immutable evaluation purpose. harness_validation results are permanent controls and cannot authorise capital.';

-- SELECT * is expanded when the view is created; expose the new column and
-- restore the hold-out check option after replacing the view.
CREATE OR REPLACE VIEW strategy_results AS
    SELECT *
    FROM strategy_results_store
    WHERE namespace = 'in_sample';

ALTER VIEW strategy_results SET (check_option = 'cascaded');

COMMIT;
