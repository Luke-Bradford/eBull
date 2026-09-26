-- 428_hunt_declarations.sql
--
-- #3385 slice 2b-i — the hunt declaration a validation (later: holdout) look is pinned to.
-- Spec: docs/proposals/ta/2026-09-26-3385-hunt-harness.md, "The audited door: validation and
-- holdout", and implementation obligations 91, 108, 110, 122-123.
-- Writer: app/services/hunt_door.py (the only sanctioned writer).
--
-- A hunt's validation batch is ONE #2599 declaration (`strategy_preregistration_declarations`,
-- strategy_id = hunt-<n>-validation, v1). That row cannot carry the batch's pins: it has fixed
-- columns. This table carries the hashed declaration document (the pinned specs and every
-- freeze-time number) against the declaration row it was frozen with, in the same transaction.
-- The #2599 row's `contract_version` is `hunt-declaration-v1:<doc_sha256>`, so the declaration's
-- own digest covers the document's hash as well.
--
-- ⚠ APPEND-ONLY, BY TRIGGER, for the reason `sql/333` gives for the declaration itself:
-- "unfreeze, look, re-freeze" is the fabrication the door exists to prevent.
--
-- ⚠ WHAT THE DATABASE CANNOT CHECK: `doc_sha256` is sha256 over the harness's canonical JSON
-- of `doc`. `hunt_door.load_hunt_declaration` recomputes it on every read and refuses a
-- mismatch.

CREATE TABLE IF NOT EXISTS hunt_declarations (
    declaration_id  BIGINT PRIMARY KEY
                    REFERENCES strategy_preregistration_declarations (declaration_id) ON DELETE RESTRICT,
    hunt_id         TEXT NOT NULL CHECK (hunt_id ~ '^hunt-[1-9][0-9]*$'),
    split           TEXT NOT NULL CHECK (split IN ('validation', 'holdout')),
    -- The repo path the declaration PR committed the document at; the register entry's
    -- evidence names the same path and hash.
    doc_path        TEXT NOT NULL CHECK (btrim(doc_path) <> ''),
    doc             JSONB NOT NULL,
    doc_sha256      TEXT NOT NULL CHECK (doc_sha256 ~ '^[0-9a-f]{64}$'),
    -- The trial register version the freeze read M_inh from. Not in `doc`: the register
    -- entry claiming the document names its sha256, so the version it creates cannot be
    -- inside it.
    register_version TEXT NOT NULL CHECK (btrim(register_version) <> ''),
    frozen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT hunt_declarations_columns_match_doc
        CHECK (doc ->> 'hunt_id' = hunt_id AND doc ->> 'split' = split),
    -- A hunt has at most one declaration per split (validation is one batch; holdout is v1 only).
    CONSTRAINT hunt_declarations_one_per_hunt_split UNIQUE (hunt_id, split)
);

COMMENT ON TABLE hunt_declarations IS
    '#3385 hunt declarations, append-only: the pinned specs and freeze-time numbers of one '
    'hunt-<n>-<split> #2599 declaration. Writer: app/services/hunt_door.py.';

-- The declaration row this document rides on must be the hunt's own.
CREATE OR REPLACE FUNCTION hunt_declarations_bind_declaration()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    declared_id      TEXT;
    declared_version TEXT;
    declared_contract TEXT;
BEGIN
    SELECT strategy_id, strategy_version, contract_version
      INTO declared_id, declared_version, declared_contract
    FROM strategy_preregistration_declarations WHERE declaration_id = NEW.declaration_id;
    IF declared_id IS DISTINCT FROM NEW.hunt_id || '-' || NEW.split OR declared_version IS DISTINCT FROM 'v1' THEN
        RAISE EXCEPTION 'declaration % is %/%, not %-%/v1',
            NEW.declaration_id, declared_id, declared_version, NEW.hunt_id, NEW.split;
    END IF;
    IF declared_contract IS DISTINCT FROM 'hunt-declaration-v1:' || NEW.doc_sha256 THEN
        RAISE EXCEPTION 'declaration % contract % does not name document %',
            NEW.declaration_id, declared_contract, NEW.doc_sha256;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_hunt_declarations_bind_declaration ON hunt_declarations;
CREATE TRIGGER trg_hunt_declarations_bind_declaration
BEFORE INSERT ON hunt_declarations
FOR EACH ROW EXECUTE FUNCTION hunt_declarations_bind_declaration();

-- `prevent_hunt_trial_mutation` is sql/427's; it names the table it fires on.
DROP TRIGGER IF EXISTS trg_hunt_declarations_append_only ON hunt_declarations;
CREATE TRIGGER trg_hunt_declarations_append_only
BEFORE UPDATE OR DELETE ON hunt_declarations
FOR EACH ROW EXECUTE FUNCTION prevent_hunt_trial_mutation();
