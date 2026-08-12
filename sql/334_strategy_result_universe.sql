-- #2621: freeze the universe inputs of the result-production gate so the
-- promotion transition can replay ``evaluated ⊆ validated`` itself, instead of
-- trusting a refusal that was returned in ``WrittenRow.refusals`` and died with
-- the writer's return value.
--
-- One row per result, written in the same transaction as the result's arm
-- pair. Both arrays are stored sorted ascending and unique (the loader
-- verifies; a record that fails that is corruption, not a refusal).
--
-- ⚠ The record freezes the universe AS THE RUN LOADED IT (#2621 scope item 3:
-- frozen at result time, never today's `load_validated_universe`). The
-- transition replays the evidence-time check; enforcement against the CURRENT
-- universe is the execution guard's order-time job (§4.0 allocation
-- invariant 2), not promotion's. `universe_rule_version` names the definition
-- the ids were produced under, so the definition can evolve without
-- re-interpreting old records.
--
-- ⚠ The cardinality bounds are a size backstop, not a population claim: the
-- §4.0 validated universe measures ~6.7k instruments today and moves with
-- every sync_universe run. A future definition that legitimately exceeds the
-- bound arrives with a new rule version and its own migration.

CREATE TABLE IF NOT EXISTS strategy_result_universe (
    result_id                BIGINT PRIMARY KEY
        REFERENCES strategy_results_store(result_id) ON DELETE RESTRICT,
    universe_rule_version    TEXT NOT NULL CHECK (universe_rule_version <> ''),
    evaluated_instrument_ids BIGINT[] NOT NULL,
    validated_universe_ids   BIGINT[] NOT NULL,
    payload_sha256           TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- One bound for both arrays so the two cannot drift independently.
    CONSTRAINT strategy_result_universe_size_backstop CHECK (
        GREATEST(cardinality(evaluated_instrument_ids), cardinality(validated_universe_ids)) <= 20000
    )
);

COMMENT ON TABLE strategy_result_universe IS
    'One immutable frozen-universe record per strategy result (#2621): the '
    'evaluated instrument ids and the §4.0 validated universe as loaded by the '
    'run that wrote the result. promote_strategy replays the universe check '
    'from this record; a pinned result without one refuses '
    'evaluated_universe_unrecorded.';

CREATE OR REPLACE FUNCTION refuse_strategy_result_universe_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'strategy result universe records are immutable; create a new result identity';
END $$;

DROP TRIGGER IF EXISTS trg_strategy_result_universe_immutable
    ON strategy_result_universe;
CREATE TRIGGER trg_strategy_result_universe_immutable
BEFORE UPDATE OR DELETE ON strategy_result_universe
FOR EACH ROW EXECUTE FUNCTION refuse_strategy_result_universe_mutation();
