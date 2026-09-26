-- 427_hunt_trials.sql
--
-- #3385 slice 2a — the hunt trial log and its outcomes.
-- Spec: docs/proposals/ta/2026-09-26-3385-hunt-harness.md, "Registration: the log comes
-- before the number", and implementation obligation 83 (columns tied to the stored spec,
-- an outcome's access tied to its own hunt and split).
-- Writer: app/services/hunt_harness.py (the only sanctioned writer).
--
-- ⚠ BOTH TABLES ARE APPEND-ONLY, BY TRIGGER. The trial count M is read from `hunt_trials`,
-- not from outcomes: a crashed or abandoned trial is still a search. A row that could be
-- deleted or rewritten would let M go down, which is exactly the number the deflation bar
-- must never under-count. Triggers bind the superuser this app connects as (sql/264's
-- measured reason for preferring a trigger over RLS).
--
-- ⚠ WHAT THE DATABASE CANNOT CHECK. `candidate_sha256` and `spec_sha256` are sha256 over the
-- harness's canonical JSON of a SUBSET of `spec`; Postgres has no canonical-JSON function to
-- recompute them. The CHECKs below tie the duplicated scalar columns to `spec` (hunt, family,
-- split, harness model), and `hunt_harness.verify_trial_row` recomputes both hashes from the
-- stored `spec`. A test round-trips a spec through this jsonb column and asserts the hashes
-- survive (jsonb keeps integers exact and floats are stored as tagged strings).


-- ---------------------------------------------------------------------------
-- 1. The log: every search, registered BEFORE its outcome exists
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hunt_trials (
    hunt_trial_id     BIGSERIAL PRIMARY KEY,
    hunt_id           TEXT NOT NULL CHECK (hunt_id ~ '^hunt-[1-9][0-9]*$'),
    family            TEXT NOT NULL CHECK (family ~ '^[a-z][a-z0-9_]*$'),
    split             TEXT NOT NULL CHECK (split IN ('discovery', 'validation', 'holdout')),
    candidate_sha256  TEXT NOT NULL CHECK (candidate_sha256 ~ '^[0-9a-f]{64}$'),
    spec_sha256       TEXT NOT NULL CHECK (spec_sha256 ~ '^[0-9a-f]{64}$'),
    -- NULL only for a `recorded_after` look whose candidate cannot be reconstructed; its
    -- `candidate_sha256` is then the sha256 of `note` and it burns the whole (hunt, split).
    spec              JSONB,
    harness_model_id  TEXT NOT NULL CHECK (btrim(harness_model_id) <> ''),
    registered_by     TEXT NOT NULL CHECK (btrim(registered_by) <> ''),
    purpose           TEXT NOT NULL CHECK (purpose IN ('evaluate', 'recorded_after')),
    floor             BOOLEAN NOT NULL DEFAULT false,
    note              TEXT,
    registered_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT hunt_trials_evaluate_has_spec
        CHECK (purpose <> 'evaluate' OR spec IS NOT NULL),
    CONSTRAINT hunt_trials_floor_is_a_recording
        CHECK (purpose = 'recorded_after' OR NOT floor),
    -- The note is a recording's idempotency key and names the look and its variant ordinal.
    CONSTRAINT hunt_trials_recording_has_note
        CHECK (purpose <> 'recorded_after' OR (note IS NOT NULL AND btrim(note) <> '')),
    -- Obligation 83: a duplicated column may not disagree with the spec it duplicates.
    CONSTRAINT hunt_trials_columns_match_spec
        CHECK (
            spec IS NULL OR (
                spec ->> 'hunt_id' = hunt_id
                AND spec ->> 'family' = family
                AND spec ->> 'split' = split
                AND spec ->> 'harness_model_id' = harness_model_id
            )
        )
);

COMMENT ON TABLE hunt_trials IS
    '#3385 hunt trial log, append-only. One row per search: purpose=evaluate is a harness '
    'registration committed before any price read; purpose=recorded_after is a look taken '
    'outside the harness. M counts every row. Writer: app/services/hunt_harness.py.';

-- One `evaluate` row per (candidate, split), programme-wide: relabelling a candidate into
-- another hunt or family cannot make it a new search.
CREATE UNIQUE INDEX IF NOT EXISTS hunt_trials_one_evaluate_per_candidate_split
    ON hunt_trials (candidate_sha256, split)
    WHERE purpose = 'evaluate';

-- Recording a look twice records it once.
CREATE UNIQUE INDEX IF NOT EXISTS hunt_trials_one_recording_per_note
    ON hunt_trials (note)
    WHERE purpose = 'recorded_after';

CREATE INDEX IF NOT EXISTS hunt_trials_hunt_split
    ON hunt_trials (hunt_id, split);


-- ---------------------------------------------------------------------------
-- 2. Outcomes: at most one per registered evaluate row
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hunt_trial_outcomes (
    hunt_trial_id       BIGINT PRIMARY KEY REFERENCES hunt_trials (hunt_trial_id),
    -- `refused` = the base canonical cell refused (a statistical refusal is an outcome);
    -- `abandoned` is written only by a reviewed script and counts as refused.
    status              TEXT NOT NULL CHECK (status IN ('computed', 'refused', 'abandoned')),
    statistics          JSONB NOT NULL,
    active_series       JSONB,
    access_id           BIGINT REFERENCES strategy_holdout_accesses (access_id),
    declaration_sha256  TEXT CHECK (declaration_sha256 ~ '^[0-9a-f]{64}$'),
    outcome_sha256      TEXT NOT NULL CHECK (outcome_sha256 ~ '^[0-9a-f]{64}$'),
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE hunt_trial_outcomes IS
    '#3385 hunt trial outcomes, append-only. access_id/declaration_sha256 are NULL exactly '
    'for discovery; otherwise the access row must be for hunt-<n>-<split>. Writer: '
    'app/services/hunt_harness.py.';

-- Obligation 83: an outcome belongs to an evaluate registration, and a validation or
-- holdout outcome's access row is the audited door's row for THAT hunt and split.
CREATE OR REPLACE FUNCTION hunt_trial_outcomes_bind_trial()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    trial_hunt   TEXT;
    trial_split  TEXT;
    trial_purpose TEXT;
    access_trial TEXT;
BEGIN
    SELECT hunt_id, split, purpose INTO trial_hunt, trial_split, trial_purpose
    FROM hunt_trials WHERE hunt_trial_id = NEW.hunt_trial_id;
    IF trial_purpose IS DISTINCT FROM 'evaluate' THEN
        RAISE EXCEPTION 'hunt trial % is not an evaluate registration; a recorded look has no outcome',
            NEW.hunt_trial_id;
    END IF;
    IF trial_split = 'discovery' THEN
        IF NEW.access_id IS NOT NULL OR NEW.declaration_sha256 IS NOT NULL THEN
            RAISE EXCEPTION 'discovery outcome for hunt trial % carries a door access', NEW.hunt_trial_id;
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.access_id IS NULL OR NEW.declaration_sha256 IS NULL THEN
        RAISE EXCEPTION '% outcome for hunt trial % has no door access', trial_split, NEW.hunt_trial_id;
    END IF;
    SELECT strategy_id INTO access_trial
    FROM strategy_holdout_accesses WHERE access_id = NEW.access_id;
    IF access_trial IS DISTINCT FROM trial_hunt || '-' || trial_split THEN
        RAISE EXCEPTION 'access % is for %, not %-%', NEW.access_id, access_trial, trial_hunt, trial_split;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_hunt_trial_outcomes_bind_trial ON hunt_trial_outcomes;
CREATE TRIGGER trg_hunt_trial_outcomes_bind_trial
BEFORE INSERT ON hunt_trial_outcomes
FOR EACH ROW EXECUTE FUNCTION hunt_trial_outcomes_bind_trial();


-- ---------------------------------------------------------------------------
-- 3. Append-only
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION prevent_hunt_trial_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only (#3385): a search, once logged, stays counted', TG_TABLE_NAME;
END $$;

DROP TRIGGER IF EXISTS trg_hunt_trials_append_only ON hunt_trials;
CREATE TRIGGER trg_hunt_trials_append_only
BEFORE UPDATE OR DELETE ON hunt_trials
FOR EACH ROW EXECUTE FUNCTION prevent_hunt_trial_mutation();

DROP TRIGGER IF EXISTS trg_hunt_trial_outcomes_append_only ON hunt_trial_outcomes;
CREATE TRIGGER trg_hunt_trial_outcomes_append_only
BEFORE UPDATE OR DELETE ON hunt_trial_outcomes
FOR EACH ROW EXECUTE FUNCTION prevent_hunt_trial_mutation();
