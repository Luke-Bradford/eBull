-- 446_ranking_pot_rebalance_attempts.sql
--
-- #2842 slice 4b-i — ranking-pot-v1's rebalance attempts (spec
-- docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md §4 steps 0–3).
-- Writer: `app/services/ranking_pot_rebalance.py` (refused / skipped rows; the `decided` row and
-- its decision rows are written by slice 4b-ii's decide step, in one transaction).
--
-- ⚠ APPEND-ONLY, BY TRIGGER (sql/264: a trigger binds the superuser this app connects as).
--
-- One row per FIRE that reached a verdict (r3-88: every attempt, refused ones included, keeps an
-- immutable identity, so the "first decided snapshot" behind the §9.2 seed is a fact, not a
-- re-derivation):
--
--   refused  — a §4 step-2 input check failed; retried at the next fire. `refusal` is the code.
--   decided  — the month's rebalance. Carries the step-3 snapshot (canonical JSON of INPUTS only,
--              never ranks or control-derived fields, r3-9) and its sha256.
--   skipped  — the month closed undecided. `refusal` is the month's last refusal, or
--              `not_attempted` when no fire reached a verdict (r3-71: written by the first fire
--              that sees the month can no longer be decided, not only at session six).
--
-- `month` is the rebalance month the row is ABOUT: the target session's month for refused /
-- decided; for a skip, the month it closes (≤ the firing target session's month).
-- One `decided` or `skipped` row per (declaration, month): the partial unique index below.
--
-- ⚠ WHAT THE DATABASE CHECKS, AND WHAT IT CANNOT.
--   * It CAN refuse any attempt once the trial is `winding_down` / `completed` or has no state
--     (§5.1 rule 3: after the wind-down event no rebalance runs), under the declaration row lock
--     the state trigger (sql/445) takes, so a concurrent wind-down serialises against it.
--   * It CANNOT check that `snapshot_sha256` is the canonical sha256 of `snapshot` (JSONB does not
--     preserve the canonical byte form): the writer reads the row back and asserts it, as the
--     freeze does for the declaration document.
--   * It CANNOT check that a `decided` month's target session is among the first five NYSE
--     sessions of the month (the NYSE calendar lives in `market_calendar.py`); the writer's pure
--     `due` decides it and a test pins it.

CREATE TABLE IF NOT EXISTS ranking_pot_rebalance_attempts (
    attempt_id       BIGSERIAL PRIMARY KEY,
    declaration_id   BIGINT NOT NULL REFERENCES ranking_pot_declarations (declaration_id) ON DELETE RESTRICT,
    fired_at         TIMESTAMPTZ NOT NULL,
    -- The next NYSE session after the last completed one at `fired_at`.
    target_session   DATE NOT NULL,
    month            DATE NOT NULL CHECK (month = date_trunc('month', month)::date),
    outcome          TEXT NOT NULL CHECK (outcome IN ('refused', 'decided', 'skipped')),
    refusal          TEXT CHECK (refusal IN (
                         'scores_run_incomplete', 'price_daily_stale', 'ranking_drift',
                         'universe_collapse', 'breakpoint_unavailable', 'max_cut_unavailable',
                         'spy_unavailable', 'scoring_failed', 'look_pending', 'not_attempted')),
    -- The consumed scores run (`scores` has no run id; `(model_version, scored_at)` is the key).
    scored_at        TIMESTAMPTZ,
    policy_hash      TEXT NOT NULL CHECK (policy_hash ~ '^[0-9a-f]{64}$'),
    -- Measured counts behind the verdict (coverage numerators/denominators, |R|, contraction).
    -- Not hashed: derived figures, kept beside the inputs, never inside the snapshot.
    detail           JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(detail) = 'object'),
    snapshot         JSONB,
    snapshot_sha256  TEXT CHECK (snapshot_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ranking_pot_rebalance_attempts_refusal
        CHECK ((outcome = 'decided') = (refusal IS NULL)),
    CONSTRAINT ranking_pot_rebalance_attempts_snapshot
        CHECK (((outcome = 'decided') = (snapshot IS NOT NULL))
               AND ((snapshot IS NULL) = (snapshot_sha256 IS NULL))),
    CONSTRAINT ranking_pot_rebalance_attempts_decided_run
        CHECK (outcome <> 'decided' OR scored_at IS NOT NULL),
    CONSTRAINT ranking_pot_rebalance_attempts_month
        CHECK (CASE WHEN outcome = 'skipped'
                    THEN month <= date_trunc('month', target_session)::date
                    ELSE month = date_trunc('month', target_session)::date END),
    CONSTRAINT ranking_pot_rebalance_attempts_not_attempted
        CHECK ((refusal = 'not_attempted') IS NOT TRUE OR outcome = 'skipped')
);

COMMENT ON TABLE ranking_pot_rebalance_attempts IS
    '#2842 ranking-pot rebalance attempts, append-only: refused / decided (with the canonical '
    'input snapshot) / skipped, one decided-or-skipped per (declaration, month).';

CREATE UNIQUE INDEX IF NOT EXISTS ranking_pot_rebalance_attempts_one_per_month
    ON ranking_pot_rebalance_attempts (declaration_id, month)
    WHERE outcome IN ('decided', 'skipped');

CREATE INDEX IF NOT EXISTS ranking_pot_rebalance_attempts_declaration
    ON ranking_pot_rebalance_attempts (declaration_id, attempt_id);

-- §5.1 rule 3: no rebalance once the trial winds down. The declaration row lock is the one the
-- state trigger takes (sql/445), so this check and a concurrent `→ winding_down` serialise.
CREATE OR REPLACE FUNCTION ranking_pot_rebalance_attempts_state_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    cur TEXT;
BEGIN
    PERFORM 1 FROM ranking_pot_declarations WHERE declaration_id = NEW.declaration_id FOR NO KEY UPDATE;
    SELECT to_state INTO cur
    FROM ranking_pot_state_events WHERE declaration_id = NEW.declaration_id
    ORDER BY event_id DESC LIMIT 1;
    IF cur IS NULL OR cur IN ('winding_down', 'completed') THEN
        RAISE EXCEPTION 'ranking pot % is %: no rebalance runs', NEW.declaration_id, coalesce(cur, '<none>');
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ranking_pot_rebalance_attempts_state_guard ON ranking_pot_rebalance_attempts;
CREATE TRIGGER trg_ranking_pot_rebalance_attempts_state_guard
BEFORE INSERT ON ranking_pot_rebalance_attempts
FOR EACH ROW EXECUTE FUNCTION ranking_pot_rebalance_attempts_state_guard();

DROP TRIGGER IF EXISTS trg_ranking_pot_rebalance_attempts_append_only ON ranking_pot_rebalance_attempts;
CREATE TRIGGER trg_ranking_pot_rebalance_attempts_append_only
BEFORE UPDATE OR DELETE ON ranking_pot_rebalance_attempts
FOR EACH ROW EXECUTE FUNCTION prevent_ranking_pot_mutation();
