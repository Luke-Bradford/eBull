-- 337_preregistration_supersession.sql
--
-- #2634 — a re-declaration path for a declaration stranded by a policy bump.
-- Spec: docs/proposals/ta/2026-08-13-preregistration-supersession.md.
-- Rules: app/services/prereg_contract.py. Writer: app/services/result_ledger.py.
--
--
-- ⚠⚠ THE WEDGE THIS OPENS.
-- ---------------------------------------------------------------------------
-- sql/333 made a declaration immutable (UPDATE and DELETE both barred) and
-- unique on (strategy_id, strategy_version). prereg_contract voids it the
-- moment STRUCTURAL_REFUSAL_POLICY_VERSION moves. Composed, ONE policy bump
-- strands every declaration frozen before it, permanently — and main moved
-- v1 -> v2-carry-fx-split inside a single day (#2363).
--
-- The repair is a SUPERSEDING ROW, never an edit: same trial, pointing back at
-- its predecessor, carrying identical terms and the current policy version.
-- No existing row is touched, so sql/333's immutability trigger is left exactly
-- as it was.
--
--
-- ⚠⚠ WHY THE `<` CHECK IS THE LOAD-BEARING CONSTRAINT (Codex checkpoint 1).
-- ---------------------------------------------------------------------------
-- Uniqueness on `supersedes_declaration_id` forbids two rows superseding one
-- predecessor (a tree), and the partial unique forbids a second root. Neither
-- forbids a CYCLE: rows inserted with explicit ids, or a multi-row INSERT
-- forming a closed loop, satisfy both while leaving the trial with ZERO
-- current declarations — a different permanent wedge from the one being fixed.
-- `declaration_id` is BIGSERIAL and a predecessor is always inserted first, so
-- every real edge points at a SMALLER id and a cycle needs at least one edge
-- that does not. One single-row CHECK proves a global graph property, with no
-- recursion and no cycle-detection code.
--
--
-- ⚠ NOTHING IS STRANDED YET. Measured on dev 2026-08-13, before this migration:
--   select count(*) from strategy_preregistration_declarations;              -- 0
--   select count(*) from strategy_holdout_accesses;                          -- 304
--   select count(*) from strategy_results_store where namespace='hold_out';  -- 0
--   select count(*) from strategy_live_gate_policies
--          where declaration_id is not null;                                 -- 0
-- Which is why this lands BEFORE scripts/freeze_2582_schedule13d_declaration.py
-- and scripts/freeze_2616_precutoff_declarations.py are run.

BEGIN;

-- ---------------------------------------------------------------------------
-- The chain
-- ---------------------------------------------------------------------------

ALTER TABLE strategy_preregistration_declarations
    ADD COLUMN IF NOT EXISTS supersedes_declaration_id BIGINT,
    --: Closed vocabulary of ONE. Other reasons to re-declare are exactly the
    --: adaptivity supersession exists to forbid; adding one is a migration and
    --: a visible act, not a free-text field nobody reads.
    ADD COLUMN IF NOT EXISTS supersession_reason TEXT,
    --: The operator's explicit no-exposure claim. ⚠ A CLAIM, NOT A PROOF —
    --: `strategy_holdout_accesses` records committed paved-path looks and
    --: cannot see a direct SELECT, an export, a log or another database. The
    --: zero-count is the cheap disqualifier; this is what carries the rest, and
    --: the immutability trigger is what makes it unrewritable afterwards.
    ADD COLUMN IF NOT EXISTS supersession_attestation TEXT;

COMMENT ON COLUMN strategy_preregistration_declarations.supersedes_declaration_id IS
    'The declaration this row replaces, NULL on the trial''s first (root) '
    'declaration. Forward-only: superseding never touches the predecessor row.';
COMMENT ON COLUMN strategy_preregistration_declarations.supersession_attestation IS
    'The declarer''s explicit statement that no outcome of this trial has been '
    'seen by any route. Necessary because the access ledger proves committed '
    'paved-path looks only, and can never prove non-access.';

-- The three columns move together: all NULL on a root, all present on a
-- superseding row. A half-filled supersession is a writer bug, not a state.
ALTER TABLE strategy_preregistration_declarations
    DROP CONSTRAINT IF EXISTS strategy_preregistration_declaration_supersession_complete;
ALTER TABLE strategy_preregistration_declarations
    ADD CONSTRAINT strategy_preregistration_declaration_supersession_complete CHECK (
        (supersedes_declaration_id IS NULL
            AND supersession_reason IS NULL
            AND supersession_attestation IS NULL)
        OR (supersedes_declaration_id IS NOT NULL
            AND supersession_reason IS NOT NULL
            AND supersession_attestation IS NOT NULL)
    );

ALTER TABLE strategy_preregistration_declarations
    DROP CONSTRAINT IF EXISTS strategy_preregistration_declaration_supersession_reason;
ALTER TABLE strategy_preregistration_declarations
    ADD CONSTRAINT strategy_preregistration_declaration_supersession_reason CHECK (
        supersession_reason IS NULL
        OR supersession_reason = 'structural_refusal_policy_superseded'
    );

-- ⚠ btrim, not char_length alone: an attestation of three spaces is non-empty
-- and says nothing. Same rule as the Python validation, which is the one that
-- names the field; this is the backstop for a writer that bypasses it.
ALTER TABLE strategy_preregistration_declarations
    DROP CONSTRAINT IF EXISTS strategy_preregistration_declaration_attestation_length;
ALTER TABLE strategy_preregistration_declarations
    ADD CONSTRAINT strategy_preregistration_declaration_attestation_length CHECK (
        supersession_attestation IS NULL
        OR char_length(btrim(supersession_attestation)) BETWEEN 1 AND 2000
    );

-- ⚠⚠ NO CYCLES. See the header — this is what turns "one root, no branching"
-- into "exactly one current declaration".
ALTER TABLE strategy_preregistration_declarations
    DROP CONSTRAINT IF EXISTS strategy_preregistration_declaration_supersedes_earlier;
ALTER TABLE strategy_preregistration_declarations
    ADD CONSTRAINT strategy_preregistration_declaration_supersedes_earlier CHECK (
        supersedes_declaration_id IS NULL
        OR supersedes_declaration_id < declaration_id
    );

-- NO BRANCHING: a predecessor may be superseded at most once, so the rows for
-- one trial form a list rather than a tree.
ALTER TABLE strategy_preregistration_declarations
    DROP CONSTRAINT IF EXISTS strategy_preregistration_declaration_supersedes_once;
ALTER TABLE strategy_preregistration_declarations
    ADD CONSTRAINT strategy_preregistration_declaration_supersedes_once
        UNIQUE (supersedes_declaration_id);

-- Redundant against the PRIMARY KEY and required anyway: a three-column FK
-- needs a unique constraint on exactly those three referenced columns, and a
-- PK on declaration_id alone does not satisfy one.
ALTER TABLE strategy_preregistration_declarations
    DROP CONSTRAINT IF EXISTS strategy_preregistration_declaration_trial_identity;
ALTER TABLE strategy_preregistration_declarations
    ADD CONSTRAINT strategy_preregistration_declaration_trial_identity
        UNIQUE (declaration_id, strategy_id, strategy_version);

-- SAME TRIAL ONLY. Without the trial columns in the reference, a declaration
-- could supersede another strategy's — which would silently move a trial's
-- terms under the identity preregistration exists to hold fixed.
ALTER TABLE strategy_preregistration_declarations
    DROP CONSTRAINT IF EXISTS strategy_preregistration_declaration_supersedes_same_trial;
ALTER TABLE strategy_preregistration_declarations
    ADD CONSTRAINT strategy_preregistration_declaration_supersedes_same_trial
        FOREIGN KEY (supersedes_declaration_id, strategy_id, strategy_version)
        REFERENCES strategy_preregistration_declarations (declaration_id, strategy_id, strategy_version)
        ON DELETE RESTRICT;

-- ⚠ THE OLD KEY IS DROPPED, NOT KEPT. `UNIQUE (strategy_id, strategy_version)`
-- is precisely what forbids the second row, i.e. the defect. It is replaced by
-- the same rule narrowed to ROOTS: one trial, one first declaration.
ALTER TABLE strategy_preregistration_declarations
    DROP CONSTRAINT IF EXISTS strategy_preregistration_declaration_unique;

CREATE UNIQUE INDEX IF NOT EXISTS strategy_preregistration_declaration_one_root
    ON strategy_preregistration_declarations (strategy_id, strategy_version)
    WHERE supersedes_declaration_id IS NULL;

-- Dropping the old UNIQUE dropped the index every read path uses to find a
-- trial's declarations. Put a plain one back.
CREATE INDEX IF NOT EXISTS strategy_preregistration_declaration_trial
    ON strategy_preregistration_declarations (strategy_id, strategy_version);

-- ---------------------------------------------------------------------------
-- Attribution (#2634 scope item 2)
-- ---------------------------------------------------------------------------

-- ⚠ NULLABLE. The 304 existing access rows predate any declaration and stay
-- NULL, which is the pre-#2599 behaviour unchanged; a trial with no declaration
-- still records NULL after this. What it buys is that an access recorded under
-- a chain names the REVISION that authorised it, instead of the reader
-- inferring it from an invariant a later ticket might relax.
ALTER TABLE strategy_holdout_accesses
    ADD COLUMN IF NOT EXISTS declaration_id BIGINT
        REFERENCES strategy_preregistration_declarations(declaration_id) ON DELETE RESTRICT;

COMMENT ON COLUMN strategy_holdout_accesses.declaration_id IS
    'The declaration revision that authorised this look, NULL for a trial that '
    'froze none. Written by result_ledger.record_holdout_access from the row it '
    'actually checked, and compared by verify_outcome_access_provenance.';

COMMIT;
