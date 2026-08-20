-- #2485 / #2484 -- the first quote observation in a five-minute bucket is
-- evidence. A later scheduler/manual run must not replace it with a quote that
-- knows more of the bucket. Deletes remain available only for the declared
-- retention path.

CREATE OR REPLACE FUNCTION reject_strategy_quote_observation_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'strategy quote observations are immutable; insert the next five-minute bucket';
END;
$$;

DROP TRIGGER IF EXISTS trg_strategy_quote_observation_immutable
    ON strategy_quote_observations;

CREATE TRIGGER trg_strategy_quote_observation_immutable
BEFORE UPDATE ON strategy_quote_observations
FOR EACH ROW EXECUTE FUNCTION reject_strategy_quote_observation_update();

COMMENT ON TABLE strategy_quote_observations IS
    'Five-minute prospective best-bid/ask samples for the bounded active '
    'strategy research panel. First observation in a bucket is immutable; '
    'missing/invalid coverage is explicit; rows expire after 24 months.';
