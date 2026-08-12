-- #2505: one compact immutable viability/edge-attribution record per result.
-- This is aggregate evidence only: no per-bar features, non-firing scans,
-- polling snapshots or raw broker payloads belong here.

CREATE TABLE IF NOT EXISTS strategy_promotion_evidence (
    result_id        BIGINT PRIMARY KEY
        REFERENCES strategy_results_store(result_id) ON DELETE RESTRICT,
    evidence_version TEXT NOT NULL CHECK (evidence_version <> ''),
    payload_sha256   TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    evidence_payload JSONB NOT NULL
        CHECK (jsonb_typeof(evidence_payload) = 'object')
        CHECK (octet_length(evidence_payload::text) <= 65536),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE strategy_promotion_evidence IS
    'One immutable <=64 KiB aggregate #2505 evidence record per strategy result: '
    'lower-bound viability, tails/concentration, calibration, EV buckets, exact '
    'same-path challengers and executable-cost completeness. No feature heap.';

CREATE OR REPLACE FUNCTION refuse_strategy_promotion_evidence_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'strategy promotion evidence is immutable; create a new result identity';
END $$;

DROP TRIGGER IF EXISTS trg_strategy_promotion_evidence_immutable
    ON strategy_promotion_evidence;
CREATE TRIGGER trg_strategy_promotion_evidence_immutable
BEFORE UPDATE OR DELETE ON strategy_promotion_evidence
FOR EACH ROW EXECUTE FUNCTION refuse_strategy_promotion_evidence_mutation();
