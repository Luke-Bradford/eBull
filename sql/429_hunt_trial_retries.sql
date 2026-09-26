-- 429_hunt_trial_retries.sql
--
-- #3385 — the retry record abandonment needs (implementation obligation 131).
-- Spec: docs/proposals/ta/2026-09-26-3385-hunt-harness.md, "The audited door" (holdout):
-- a pinned spec that cannot be computed "is retried, or marked `abandoned` by the reviewed
-- script, which accepts only a trial whose infrastructure error has recurred on three
-- recorded retries". This is where those failures are recorded.
-- Writer: app/services/hunt_harness.py (`evaluate`, on an infrastructure error after the
-- registration is committed). Reader: `hunt_harness.abandon_trial`.
--
-- ⚠ APPEND-ONLY, BY TRIGGER (the sql/427 pattern): abandonment eligibility is read from
-- this log, so a row that could be deleted or rewritten would let a candidate be abandoned
-- on failures that never happened, or kept alive past ones that did.

CREATE TABLE IF NOT EXISTS hunt_trial_retries (
    retry_id       BIGSERIAL PRIMARY KEY,
    hunt_trial_id  BIGINT NOT NULL REFERENCES hunt_trials (hunt_trial_id),
    -- The exception's qualified class name; "recurred" compares it across attempts.
    error_class    TEXT NOT NULL CHECK (btrim(error_class) <> ''),
    error_text     TEXT NOT NULL,
    recorded_by    TEXT NOT NULL CHECK (btrim(recorded_by) <> ''),
    recorded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE hunt_trial_retries IS
    '#3385 failed evaluate attempts of a registered hunt trial, append-only. One row per '
    'infrastructure error after the registration committed. Abandonment reads it. Writer: '
    'app/services/hunt_harness.py.';

CREATE INDEX IF NOT EXISTS hunt_trial_retries_trial
    ON hunt_trial_retries (hunt_trial_id, retry_id);

-- A failure belongs to an `evaluate` registration that has no outcome yet.
CREATE OR REPLACE FUNCTION hunt_trial_retries_bind_trial()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM hunt_trials WHERE hunt_trial_id = NEW.hunt_trial_id AND purpose = 'evaluate'
    ) THEN
        RAISE EXCEPTION 'hunt trial % is not an evaluate registration', NEW.hunt_trial_id;
    END IF;
    IF EXISTS (SELECT 1 FROM hunt_trial_outcomes WHERE hunt_trial_id = NEW.hunt_trial_id) THEN
        RAISE EXCEPTION 'hunt trial % already has an outcome; nothing is left to retry', NEW.hunt_trial_id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_hunt_trial_retries_bind_trial ON hunt_trial_retries;
CREATE TRIGGER trg_hunt_trial_retries_bind_trial
BEFORE INSERT ON hunt_trial_retries
FOR EACH ROW EXECUTE FUNCTION hunt_trial_retries_bind_trial();

DROP TRIGGER IF EXISTS trg_hunt_trial_retries_append_only ON hunt_trial_retries;
CREATE TRIGGER trg_hunt_trial_retries_append_only
BEFORE UPDATE OR DELETE ON hunt_trial_retries
FOR EACH ROW EXECUTE FUNCTION prevent_hunt_trial_mutation();
