-- #2625: freeze the §3.4 ambiguity-arm comparison so the promotion transition
-- can re-derive `ambiguity_material` itself, instead of trusting a verdict that
-- was computed in `_write_rows` memory and died with the writer's return value.
--
-- The same defect and the same repair as #2621's `strategy_result_universe`:
-- one immutable, hashed row per result, written in the pair's own transaction,
-- replayed by `promote_strategy` through a pure refusal function.
--
-- ⚠ THE RECORD STORES THE COMPARISON'S INPUTS, NOT ITS VERDICT. A stored
-- boolean cannot be audited — there is no way to check it against anything.
-- Storing the basis, the two arm Sharpes and the cohort threshold lets the
-- verdict be re-derived, and lets an auditor disagree with it.
--
-- ⚠ `comparison_basis` IS NOT INFERABLE FROM THE SHARPES, which is why it is a
-- column rather than a derivation. `_ambiguity_material_for` returns False from
-- the mere PRESENCE of a matching measurement whose `ambiguity_arm` is NULL --
-- before it reads a Sharpe, and whatever their values. Two equal Sharpes are
-- not equivalent evidence: they cannot distinguish one shared measurement
-- copied to both identities from two independently-evaluated equal arms.
--
-- ⚠ `cohort_gap_threshold` IS NULL ON EVERYTHING THE CURRENT RUNNER WRITES.
-- §3.4 judges the arm gap against the random cohort's 95th-percentile gap, and
-- no cohort is attached yet, so the honest verdict is "not compared" and the
-- gate stays closed. The column exists rather than being deferred because
-- without it the `ambiguity_material` verdict is unreachable and therefore
-- untestable, and because a result later measured WITH a cohort would otherwise
-- need a second migration to be recorded faithfully.

CREATE TABLE IF NOT EXISTS strategy_result_ambiguity (
    result_id              BIGINT PRIMARY KEY
        REFERENCES strategy_results_store(result_id) ON DELETE RESTRICT,
    ambiguity_rule_version TEXT NOT NULL CHECK (ambiguity_rule_version <> ''),
    comparison_basis       TEXT NOT NULL
        CHECK (comparison_basis IN ('shared_measurement', 'arm_sharpes')),
    best_case_sharpe       DOUBLE PRECISION,
    worst_case_sharpe      DOUBLE PRECISION,
    cohort_gap_threshold   DOUBLE PRECISION,
    payload_sha256         TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- ⚠ NaN AND BOTH INFINITIES ARE REFUSED, and the range form is deliberate:
    -- Postgres defines `'NaN'::float8 = 'NaN'::float8` as TRUE (it orders NaN
    -- above every other value, unlike IEEE-754), so the obvious self-equality
    -- test would ADMIT NaN. Verified against this database before writing:
    -- NaN > -Infinity is true but NaN < Infinity is false, so the conjunction
    -- rejects NaN, +Infinity and -Infinity, and admits every finite value.
    -- A NaN Sharpe would make the gap comparison silently false-y downstream.
    CONSTRAINT strategy_result_ambiguity_sharpes_finite CHECK (
        (best_case_sharpe IS NULL
            OR (best_case_sharpe > '-Infinity'::float8 AND best_case_sharpe < 'Infinity'::float8))
        AND (worst_case_sharpe IS NULL
            OR (worst_case_sharpe > '-Infinity'::float8 AND worst_case_sharpe < 'Infinity'::float8))
        AND (cohort_gap_threshold IS NULL
            OR (cohort_gap_threshold > '-Infinity'::float8 AND cohort_gap_threshold < 'Infinity'::float8))
    ),

    -- §3.4's threshold is a GAP between two Sharpes, so a negative one is not a
    -- lenient threshold, it is an incoherent record: every gap exceeds it and
    -- the arm comparison would report `material` unconditionally.
    CONSTRAINT strategy_result_ambiguity_threshold_non_negative CHECK (
        cohort_gap_threshold IS NULL OR cohort_gap_threshold >= 0
    ),

    -- A shared measurement is decided by its basis alone, so carrying arm
    -- Sharpes or a threshold beside it would be recording numbers the verdict
    -- provably did not consult. Keeps the canonical form single-valued, which
    -- is what makes the payload hash meaningful.
    CONSTRAINT strategy_result_ambiguity_shared_carries_no_measurements CHECK (
        comparison_basis <> 'shared_measurement'
        OR (best_case_sharpe IS NULL AND worst_case_sharpe IS NULL AND cohort_gap_threshold IS NULL)
    )
);

COMMENT ON TABLE strategy_result_ambiguity IS
    'One immutable frozen §3.4 ambiguity-comparison record per strategy result '
    '(#2625): the comparison basis, the two arm Sharpes and the random-cohort '
    'gap threshold as the run measured them. promote_strategy re-derives the '
    'ambiguity_material verdict from this record; a pinned result without one '
    'refuses ambiguity_verdict_unrecorded.';

COMMENT ON COLUMN strategy_result_ambiguity.cohort_gap_threshold IS
    'Section 3.4 random-cohort 95th-percentile gap. NULL on every row the current '
    'runner writes -- no cohort is attached yet, so the verdict is '
    '"not compared" and the gate stays closed.';

CREATE OR REPLACE FUNCTION refuse_strategy_result_ambiguity_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'strategy result ambiguity records are immutable; create a new result identity';
END $$;

DROP TRIGGER IF EXISTS trg_strategy_result_ambiguity_immutable
    ON strategy_result_ambiguity;
CREATE TRIGGER trg_strategy_result_ambiguity_immutable
BEFORE UPDATE OR DELETE ON strategy_result_ambiguity
FOR EACH ROW EXECUTE FUNCTION refuse_strategy_result_ambiguity_mutation();
