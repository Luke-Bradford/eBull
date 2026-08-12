-- 333_strategy_preregistration_declarations.sql
--
-- #2599 — the research-side counterpart to the runtime funding gate.
-- Spec: docs/proposals/ta/2026-08-12-preregistration-declaration-gate.md.
-- Writer: app/services/result_ledger.py. Coherence rules and the refusal
-- vocabulary: app/services/prereg_contract.py.
--
--
-- ⚠⚠ WHY A TABLE AND NOT A FUNCTION ARGUMENT.
-- ---------------------------------------------------------------------------
-- The first draft of #2599 passed the declaration into the access helper as a
-- value object. Codex checkpoint 1 killed it in one sentence: "a caller can
-- construct a favourable declaration after seeing/reading outcomes". A
-- declaration that is not written down BEFORE the look is not a declaration —
-- it is a description of what you found. So the row is the artefact, it is
-- frozen with a hash and a timestamp, and every read path loads it by trial
-- identity rather than accepting one.
--
--
-- ⚠ NO RETROACTIVE INVALIDATION.
-- ---------------------------------------------------------------------------
-- A trial with no row here behaves exactly as it did before this migration.
-- The 224 existing `strategy_holdout_accesses` rows and every current
-- evaluator keep working. The gate is opt-in by freezing — and once a trial
-- HAS frozen a declaration, it cannot escape back through the old door,
-- because the check sits in `record_holdout_access`, which is the single
-- chokepoint all three hold-out doors already funnel through.

BEGIN;

CREATE TABLE IF NOT EXISTS strategy_preregistration_declarations (
    declaration_id                      BIGSERIAL PRIMARY KEY,
    strategy_id                         TEXT NOT NULL,
    strategy_version                    TEXT NOT NULL,
    --: The frozen candidate contract this declaration belongs to, e.g.
    --: `schedule13d-public-catalyst-v1`. Free text on purpose: contracts are
    --: files under docs/proposals/ta/contracts/, not rows.
    contract_version                    TEXT NOT NULL,
    prereg_purpose                      TEXT NOT NULL CHECK (
        prereg_purpose IN ('capital_candidate', 'falsification_only')
    ),
    --: Which structural-refusal policy the expectation below was computed
    --: under. Same shape as `trial_register_version` on sql/266: a frozen
    --: artefact computed under a superseded policy is REFUSED, never
    --: re-interpreted.
    structural_refusal_policy_version   TEXT NOT NULL,
    --: The stamps the run WILL carry. Compared against the stored row's actual
    --: stamps at hold-out write time — declaring eligible and then storing
    --: survivor-only is the substitution this table exists to prevent.
    declared_universe_basis             TEXT NOT NULL,
    declared_carry_unmodelled           BOOLEAN NOT NULL,
    --: ⚠ A LIST, NEVER A BOOLEAN. Side- and product-dependence means a bare
    --: "is it promotable" loses the reasons, and the reasons are what an
    --: operator acts on. Stored despite being recomputable so that a later
    --: change to the refusal policy shows up as a disagreement rather than
    --: being silently absorbed.
    expected_structural_refusals        TEXT[] NOT NULL,
    --: #2437's forward-shadow floor: N independent decision-dates AND M
    --: calendar weeks, frozen from the candidate's own power calculation.
    --: ⚠ NO DEFAULT AND NO VALUE CHOSEN HERE. Picking one centrally would be
    --: the #2600 padded-floor defect — a number no artefact evidences.
    min_forward_decision_dates          INTEGER NOT NULL CHECK (min_forward_decision_dates > 0),
    min_forward_calendar_weeks          INTEGER NOT NULL CHECK (min_forward_calendar_weeks > 0),
    --: Names the power calculation the two floors came from. Non-empty, so a
    --: floor always carries its derivation next to it.
    forward_shadow_derivation           TEXT NOT NULL CHECK (
        char_length(forward_shadow_derivation) BETWEEN 1 AND 1000
    ),
    declared_by                         TEXT NOT NULL CHECK (
        char_length(declared_by) BETWEEN 1 AND 200
    ),
    --: sha256 over canonical JSON (sort_keys, compact separators) of the
    --: declared fields. Same freezing pattern as
    --: scripts/verify_2582_schedule13d_preregistration.py uses on a contract
    --: file: the bytes are what is frozen, not the intent.
    declaration_sha256                  TEXT NOT NULL CHECK (
        declaration_sha256 ~ '^[0-9a-f]{64}$'
    ),
    frozen_at                           TIMESTAMPTZ NOT NULL DEFAULT now(),
    --: ⚠ ONE DECLARATION PER TRIAL. Two contradictory declarations for the
    --: same (strategy_id, strategy_version) would let a caller pick whichever
    --: one the outcome favours, which is the fabrication this table prevents
    --: in a second costume.
    CONSTRAINT strategy_preregistration_declaration_unique
        UNIQUE (strategy_id, strategy_version),
    CONSTRAINT strategy_preregistration_declaration_identity CHECK (
        char_length(strategy_id) BETWEEN 1 AND 200
        AND char_length(strategy_version) BETWEEN 1 AND 200
        AND char_length(contract_version) BETWEEN 1 AND 200
        AND char_length(structural_refusal_policy_version) BETWEEN 1 AND 200
        AND char_length(declared_universe_basis) BETWEEN 1 AND 200
    )
);

COMMENT ON TABLE strategy_preregistration_declarations IS
    'One frozen, immutable preregistration declaration per strategy trial: its '
    'purpose (capital_candidate vs falsification_only), the structural '
    'refusals expected from the stamps the run will carry, and the '
    'forward-shadow floor frozen from the candidate power calculation.';

-- Immutability. Pattern: sql/307_strategy_quote_observation_immutability.sql.
-- A declaration that can be edited after the look is not frozen, and DELETE is
-- barred for the same reason UPDATE is — "unfreeze, look, re-freeze" is the
-- same fabrication with an extra step.
CREATE OR REPLACE FUNCTION reject_strategy_preregistration_declaration_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION
        'preregistration declarations are immutable; a trial that needs different terms is a new strategy_version';
END;
$$;

DROP TRIGGER IF EXISTS trg_strategy_preregistration_declaration_immutable
    ON strategy_preregistration_declarations;

CREATE TRIGGER trg_strategy_preregistration_declaration_immutable
BEFORE UPDATE OR DELETE ON strategy_preregistration_declarations
FOR EACH ROW EXECUTE FUNCTION reject_strategy_preregistration_declaration_change();

-- The forward-shadow floor reaches the live gate by reference, so the policy
-- cannot restate it with a different number.
--
-- ⚠ NULLABLE, DELIBERATELY. A policy registered before this migration carries
-- no declaration, and `assess_live_gate` reads that NULL as
-- `forward_shadow_floor_missing` — fail-closed. Making it NOT NULL would need
-- a backfill whose safety rested on one dev database being empty today, which
-- is not an argument about anyone else's database.
ALTER TABLE strategy_live_gate_policies
    ADD COLUMN IF NOT EXISTS declaration_id BIGINT
        REFERENCES strategy_preregistration_declarations(declaration_id) ON DELETE RESTRICT;

COMMENT ON COLUMN strategy_live_gate_policies.declaration_id IS
    'The frozen preregistration declaration whose forward-shadow floor binds '
    'this policy. NULL means no floor was frozen, which the live gate refuses.';

COMMIT;
