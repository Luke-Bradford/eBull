-- 447_ranking_pot_steps.sql
--
-- #2842 slice 6a — ranking-pot-v1's online step ledger (spec
-- docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md §9.1, "The step job" paragraph).
-- Writer: `app/services/ranking_pot_step.py`, one transaction per stepped session.
--
-- Two tables:
--
--   ranking_pot_step_refusals    — APPEND-ONLY, by trigger. A due session the step job could not step: its
--                                  bar gate failed, or the process policy hash differs from the declared one.
--   ranking_pot_steps            — APPEND-ONLY, by trigger. One row per (declaration, session): the step
--                                  fence (r3-42 exactly once). Carries the consumed inputs (r3-43), the
--                                  shadow's full step, per-control per-session aggregates and the digest
--                                  of every book's checkpoint after the step.
--   ranking_pot_book_checkpoints — one MUTABLE row per (declaration, book): book 0 is the shadow, books
--                                  1..K the §9.2 controls. A cache of the simulator state, reproducible
--                                  from the step rows' inputs, the decided snapshots and the seed; the
--                                  writer checks it against the previous step's `checkpoint_sha256`
--                                  before every step.
--
-- ⚠ WHAT THE DATABASE CHECKS, AND WHAT IT CANNOT.
--   * It CAN refuse a second step row for one session (the primary key) and a checkpoint that does not
--     move forward (the trigger below: `last_session` strictly increases, `declaration_id`/`book` fixed).
--   * It CAN fence publication against stepping (Codex ckpt-1, slice 6a): a step for S must apply the
--     `decided` row targeting S if one exists, and a `decided` row for a session already stepped is refused.
--     Both run under the transaction's snapshot, so two transactions committing at the same instant are not
--     fenced by them; the time fence is: a decision for S commits by 12:00 UTC on S, and S is stepped from
--     09:00 UTC the next calendar day (spec §4 (*), §9.1).
--   * It CANNOT check that `session` is the NYSE session after the previous step's (the NYSE calendar
--     lives in `market_calendar.py`): `ranking_pot_sim.step` refuses any other, and a test pins it.
--   * It CANNOT check that `inputs_sha256` / `checkpoint_sha256` are the canonical sha256 of what they
--     cover (JSONB does not keep the canonical byte form): the writer reads the inputs back and asserts.

CREATE TABLE IF NOT EXISTS ranking_pot_steps (
    declaration_id     BIGINT NOT NULL REFERENCES ranking_pot_declarations (declaration_id) ON DELETE RESTRICT,
    session            DATE NOT NULL,
    stepped_at         TIMESTAMPTZ NOT NULL,
    policy_hash        TEXT NOT NULL CHECK (policy_hash ~ '^[0-9a-f]{64}$'),
    -- The `decided` rebalance applied before this session's step (§4 (**)), if one targeted it.
    applied_attempt_id BIGINT REFERENCES ranking_pot_rebalance_attempts (attempt_id) ON DELETE RESTRICT,
    -- The wind-down event whose stamps were applied at this session (§5.1 rule 3).
    wind_down_event_id BIGINT REFERENCES ranking_pot_state_events (event_id) ON DELETE RESTRICT,
    -- Stepped without the bar gate: 10 later NYSE sessions had completed (spec §9.1, §9.3 unevaluable).
    forced             BOOLEAN NOT NULL,
    -- Canonical JSON: each consumed name's bar for `session`, every reference close the split check
    -- read, and SPY's bar (r3-43).
    inputs             JSONB NOT NULL CHECK (jsonb_typeof(inputs) = 'object'),
    inputs_sha256      TEXT NOT NULL CHECK (inputs_sha256 ~ '^[0-9a-f]{64}$'),
    -- Book 0's full step: position-session records, closed lifecycles, refusals, rescales, NAV, and its
    -- §5.2 decision rows when a rebalance was applied.
    shadow             JSONB NOT NULL CHECK (jsonb_typeof(shadow) = 'object'),
    -- Books 1..K as columns (one array element per control, in book order): this session's record
    -- count, Σ net return, NAV, closes, missing-bar exits, refusals, rescales; at a rebalance also its
    -- entries, exits, unfilled slots and missing-donor count.
    controls           JSONB NOT NULL CHECK (jsonb_typeof(controls) = 'object'),
    checkpoint_sha256  TEXT NOT NULL CHECK (checkpoint_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (declaration_id, session)
);

COMMENT ON TABLE ranking_pot_steps IS
    '#2842 ranking-pot online step ledger, append-only: one row per (declaration, NYSE session) with the '
    'consumed inputs, the shadow''s full step and per-control aggregates (spec §9.1).';

CREATE UNIQUE INDEX IF NOT EXISTS ranking_pot_steps_one_application
    ON ranking_pot_steps (applied_attempt_id) WHERE applied_attempt_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ranking_pot_steps_one_wind_down
    ON ranking_pot_steps (declaration_id) WHERE wind_down_event_id IS NOT NULL;

DROP TRIGGER IF EXISTS trg_ranking_pot_steps_append_only ON ranking_pot_steps;
CREATE TRIGGER trg_ranking_pot_steps_append_only
BEFORE UPDATE OR DELETE ON ranking_pot_steps
FOR EACH ROW EXECUTE FUNCTION prevent_ranking_pot_mutation();


CREATE TABLE IF NOT EXISTS ranking_pot_book_checkpoints (
    declaration_id BIGINT NOT NULL REFERENCES ranking_pot_declarations (declaration_id) ON DELETE RESTRICT,
    -- 0 = the shadow; 1..K = control k.
    book           INTEGER NOT NULL CHECK (book >= 0),
    last_session   DATE NOT NULL,
    state          JSONB NOT NULL CHECK (jsonb_typeof(state) = 'object'),
    -- Names held or pending entry: the union over books is the next step's bar population.
    instrument_ids BIGINT[] NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (declaration_id, book)
);

COMMENT ON TABLE ranking_pot_book_checkpoints IS
    '#2842 ranking-pot simulator state per book (0 = shadow, 1..K = controls): a cache reproducible from '
    'ranking_pot_steps, checked against its checkpoint_sha256 before every step.';

CREATE OR REPLACE FUNCTION ranking_pot_book_checkpoints_forward_only()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'ranking_pot_book_checkpoints rows are never deleted (#2842)';
    END IF;
    IF NEW.declaration_id <> OLD.declaration_id OR NEW.book <> OLD.book THEN
        RAISE EXCEPTION 'a ranking-pot checkpoint keeps its declaration and book (#2842)';
    END IF;
    IF NEW.last_session <= OLD.last_session THEN
        RAISE EXCEPTION 'a ranking-pot checkpoint only moves forward (% -> %)', OLD.last_session, NEW.last_session;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ranking_pot_book_checkpoints_forward_only ON ranking_pot_book_checkpoints;
CREATE TRIGGER trg_ranking_pot_book_checkpoints_forward_only
BEFORE UPDATE OR DELETE ON ranking_pot_book_checkpoints
FOR EACH ROW EXECUTE FUNCTION ranking_pot_book_checkpoints_forward_only();


-- Publication fence, step side: a step applies the decided row targeting its session, if any.
CREATE OR REPLACE FUNCTION ranking_pot_steps_publication_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    decided BIGINT;
BEGIN
    SELECT attempt_id INTO decided FROM ranking_pot_rebalance_attempts
    WHERE declaration_id = NEW.declaration_id AND outcome = 'decided' AND target_session = NEW.session;
    IF decided IS DISTINCT FROM NEW.applied_attempt_id THEN
        RAISE EXCEPTION 'ranking pot % step %: must apply decided attempt % (got %)',
            NEW.declaration_id, NEW.session, coalesce(decided::text, '<none>'),
            coalesce(NEW.applied_attempt_id::text, '<none>');
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ranking_pot_steps_publication_fence ON ranking_pot_steps;
CREATE TRIGGER trg_ranking_pot_steps_publication_fence
BEFORE INSERT ON ranking_pot_steps
FOR EACH ROW EXECUTE FUNCTION ranking_pot_steps_publication_fence();

-- Publication fence, rebalance side: no decided row for a session the books have already stepped.
CREATE OR REPLACE FUNCTION ranking_pot_attempts_step_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.outcome = 'decided' AND EXISTS (
        SELECT 1 FROM ranking_pot_steps
        WHERE declaration_id = NEW.declaration_id AND session >= NEW.target_session
    ) THEN
        RAISE EXCEPTION 'ranking pot %: session % is already stepped, so no decision may target it',
            NEW.declaration_id, NEW.target_session;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ranking_pot_attempts_step_fence ON ranking_pot_rebalance_attempts;
CREATE TRIGGER trg_ranking_pot_attempts_step_fence
BEFORE INSERT ON ranking_pot_rebalance_attempts
FOR EACH ROW EXECUTE FUNCTION ranking_pot_attempts_step_fence();


CREATE TABLE IF NOT EXISTS ranking_pot_step_refusals (
    refusal_id     BIGSERIAL PRIMARY KEY,
    declaration_id BIGINT NOT NULL REFERENCES ranking_pot_declarations (declaration_id) ON DELETE RESTRICT,
    session        DATE NOT NULL,
    fired_at       TIMESTAMPTZ NOT NULL,
    reason         TEXT NOT NULL CHECK (reason IN ('bar_gate', 'policy_drift')),
    -- Counts behind the verdict (valid bars / population, SPY valid).
    detail         JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(detail) = 'object'),
    recorded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE ranking_pot_step_refusals IS
    '#2842 ranking-pot step refusals, append-only: a due session not stepped (bar gate or policy drift).';

CREATE INDEX IF NOT EXISTS ranking_pot_step_refusals_declaration
    ON ranking_pot_step_refusals (declaration_id, session);

DROP TRIGGER IF EXISTS trg_ranking_pot_step_refusals_append_only ON ranking_pot_step_refusals;
CREATE TRIGGER trg_ranking_pot_step_refusals_append_only
BEFORE UPDATE OR DELETE ON ranking_pot_step_refusals
FOR EACH ROW EXECUTE FUNCTION prevent_ranking_pot_mutation();
