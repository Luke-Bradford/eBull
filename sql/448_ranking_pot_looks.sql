-- 448_ranking_pot_looks.sql
--
-- #2842 slice 6b — ranking-pot-v1's §9.3 looks (spec
-- docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md §9.3, "The looks" paragraph).
-- Writer: `app/services/ranking_pot_look.py`, called by the step job.
--
--   ranking_pot_looks — APPEND-ONLY, by trigger. One authoritative `result` per (declaration, look);
--                       an `invalidation` cites a result and its evidence, a `recomputation` cites its
--                       invalidation. v1's engine writes only `result`; the other kinds have no writer.
--
-- It also extends sql/445's state-transition trigger with two fences:
--   * ACTIVATION (r3-34): `→ executing` is refused once the New York date has reached the 12-month
--     anniversary of the first decided target session (A_12, stricter than the endpoint session
--     E_12 ≥ A_12), activation and resumption alike (§7.1), at `clock_timestamp()`. With no decided
--     rebalance there is no fence. `12` mirrors `ranking_pot_policy.LOOK_MONTHS[0]`; a test pins it.
--   * COMPLETION (r3-63): once a decided rebalance exists, `→ completed` is refused unless exactly
--     K + 1 book checkpoints exist, none holds a position or a pending entry, and a step applied the
--     declaration's first `winding_down` event. With no decided rebalance there are no books.
--     The executed book's flatness (r3-64) stays the `completed` writer's check (slice 5).
--
-- ⚠ WHAT THE DATABASE CHECKS, AND WHAT IT CANNOT.
--   * It CAN refuse a second `result` for one look (the partial unique index) and any mutation.
--   * It CANNOT check that `endpoint_session` is the first NYSE session on or after the anniversary
--     (the NYSE calendar lives in `market_calendar.py`): `ranking_pot_look.endpoint` decides it and a
--     test pins it.

CREATE TABLE IF NOT EXISTS ranking_pot_looks (
    look_id          BIGSERIAL PRIMARY KEY,
    declaration_id   BIGINT NOT NULL REFERENCES ranking_pot_declarations (declaration_id) ON DELETE RESTRICT,
    look_months      INTEGER NOT NULL CHECK (look_months IN (12, 24)),
    endpoint_session DATE NOT NULL,
    kind             TEXT NOT NULL CHECK (kind IN ('result', 'invalidation', 'recomputation')),
    verdict          TEXT CHECK (verdict IN
                         ('pass', 'shadow_pass_execution_unproven', 'not_passed', 'unevaluable')),
    harm             BOOLEAN,
    -- The unevaluable reasons that held (empty unless `verdict = 'unevaluable'`).
    reasons          TEXT[] NOT NULL DEFAULT '{}',
    -- An invalidation cites its result; a recomputation cites its invalidation.
    cites_look_id    BIGINT REFERENCES ranking_pot_looks (look_id) ON DELETE RESTRICT,
    note             TEXT,
    -- T_obs, p-values, every condition's operands and outcome, the counts.
    detail           JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(detail) = 'object'),
    policy_hash      TEXT NOT NULL CHECK (policy_hash ~ '^[0-9a-f]{64}$'),
    computed_at      TIMESTAMPTZ NOT NULL,
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ranking_pot_looks_kind_shape CHECK (
        CASE kind
            WHEN 'result' THEN verdict IS NOT NULL AND harm IS NOT NULL AND cites_look_id IS NULL
            WHEN 'invalidation' THEN verdict IS NULL AND harm IS NULL AND cites_look_id IS NOT NULL
                                     AND btrim(coalesce(note, '')) <> ''
            ELSE verdict IS NOT NULL AND harm IS NOT NULL AND cites_look_id IS NOT NULL
        END),
    CONSTRAINT ranking_pot_looks_reasons
        CHECK ((verdict = 'unevaluable') IS NOT DISTINCT FROM (cardinality(reasons) > 0)
               OR verdict IS NULL)
);

COMMENT ON TABLE ranking_pot_looks IS
    '#2842 ranking-pot §9.3 looks, append-only: one authoritative result per (declaration, look); '
    'invalidations and recomputations are separate rows citing it.';

CREATE UNIQUE INDEX IF NOT EXISTS ranking_pot_looks_one_result
    ON ranking_pot_looks (declaration_id, look_months) WHERE kind = 'result';

DROP TRIGGER IF EXISTS trg_ranking_pot_looks_append_only ON ranking_pot_looks;
CREATE TRIGGER trg_ranking_pot_looks_append_only
BEFORE UPDATE OR DELETE ON ranking_pot_looks
FOR EACH ROW EXECUTE FUNCTION prevent_ranking_pot_mutation();


-- sql/445's transition trigger, unchanged except the two fences marked "slice 6b".
CREATE OR REPLACE FUNCTION ranking_pot_state_events_transition()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    cur TEXT;
    nxt TEXT := NEW.to_state;
    t0  DATE;
    a12 DATE;
BEGIN
    PERFORM 1 FROM ranking_pot_declarations WHERE declaration_id = NEW.declaration_id FOR NO KEY UPDATE;
    SELECT to_state INTO cur
    FROM ranking_pot_state_events WHERE declaration_id = NEW.declaration_id
    ORDER BY event_id DESC LIMIT 1;

    IF NEW.from_state IS DISTINCT FROM cur THEN
        RAISE EXCEPTION 'stale transition: ranking pot % is in state %, not %',
            NEW.declaration_id, coalesce(cur, '<none>'), coalesce(NEW.from_state, '<none>');
    END IF;
    IF cur = 'completed' THEN
        RAISE EXCEPTION 'completed is terminal';
    END IF;

    IF cur IS NULL THEN
        IF nxt <> 'shadow_only' OR NEW.actor <> 'supervisor' THEN
            RAISE EXCEPTION 'the genesis event is <none> -> shadow_only by the supervisor, not -> % by %',
                nxt, NEW.actor;
        END IF;
    ELSIF nxt = 'winding_down' THEN
        IF cur = 'winding_down' THEN
            RAISE EXCEPTION 'already winding_down';
        END IF;
        IF (NEW.wind_down_reason IN ('harm', 'completed_window')) <> (NEW.actor = 'engine') THEN
            RAISE EXCEPTION 'wind-down reason % is not written by %', NEW.wind_down_reason, NEW.actor;
        END IF;
    ELSIF nxt = 'completed' THEN
        IF cur <> 'winding_down' OR NEW.actor <> 'engine' THEN
            RAISE EXCEPTION 'completed follows winding_down, by the engine (not % from %)', NEW.actor, cur;
        END IF;
        -- slice 6b, completion fence (r3-63). With a decided snapshot the K + 1 books exist (or will, at the
        -- first step): all of them must be flat, and a step must have applied this declaration's FIRST
        -- winding_down event. With none there are no books and no fence.
        IF EXISTS (
            SELECT 1 FROM ranking_pot_rebalance_attempts
            WHERE declaration_id = NEW.declaration_id AND outcome = 'decided'
        ) THEN
            IF (SELECT count(*) FROM ranking_pot_book_checkpoints WHERE declaration_id = NEW.declaration_id)
               <> (SELECT (doc -> 'terms' ->> 'k_controls')::int + 1 FROM ranking_pot_declarations
                   WHERE declaration_id = NEW.declaration_id)
            THEN
                RAISE EXCEPTION 'books_incomplete: ranking pot % lacks K + 1 book checkpoints', NEW.declaration_id;
            END IF;
            IF EXISTS (
                SELECT 1 FROM ranking_pot_book_checkpoints
                WHERE declaration_id = NEW.declaration_id
                  AND (jsonb_array_length(state -> 'positions') > 0 OR jsonb_array_length(state -> 'pending') > 0)
            ) THEN
                RAISE EXCEPTION 'books_not_flat: a ranking-pot book still holds a position or a pending entry';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM ranking_pot_steps s
                WHERE s.declaration_id = NEW.declaration_id
                  AND s.wind_down_event_id = (
                      SELECT min(e.event_id) FROM ranking_pot_state_events e
                      WHERE e.declaration_id = NEW.declaration_id AND e.to_state = 'winding_down')
            ) THEN
                RAISE EXCEPTION 'wind_down_not_stepped: the books have not applied the wind-down';
            END IF;
        END IF;
    ELSIF nxt = 'executing' THEN
        IF cur NOT IN ('shadow_only', 'halted_loss', 'halted_operator') OR NEW.actor <> 'supervisor' THEN
            RAISE EXCEPTION 'executing is entered from shadow_only or a halt by the supervisor (not % from %)',
                NEW.actor, cur;
        END IF;
        -- slice 6b, activation fence (r3-34): never on or after A_12.
        SELECT min(target_session) INTO t0 FROM ranking_pot_rebalance_attempts
        WHERE declaration_id = NEW.declaration_id AND outcome = 'decided';
        -- A_12 as `ranking_pot_look.anniversary` defines it: 29 February advances to 1 March (PostgreSQL's
        -- interval arithmetic would clamp it to 28 February, a day early; Codex ckpt-2).
        a12 := CASE WHEN extract(month FROM t0) = 2 AND extract(day FROM t0) = 29
                    THEN make_date(extract(year FROM t0)::int + 1, 3, 1)
                    ELSE (t0 + interval '12 months')::date END;
        IF t0 IS NOT NULL AND (clock_timestamp() AT TIME ZONE 'America/New_York')::date >= a12 THEN
            RAISE EXCEPTION 'look_window: executing is not entered on or after the 12-month anniversary %', a12;
        END IF;
        -- §7.3 `v1_active`, locked against both writers that can make a trial active:
        --   * a NEW declaration (an AI-trial freeze inserts it with its genesis `active` event):
        --     SHARE conflicts with the inserter's ROW EXCLUSIVE, so this waits for an in-flight
        --     freeze to commit and blocks new ones until this transaction ends (Codex ckpt-2: a row
        --     lock alone cannot see an uncommitted phantom);
        --   * a RESUMPTION of an existing one: its state trigger takes the declaration row lock.
        LOCK TABLE ai_trial_declarations IN SHARE MODE;
        PERFORM 1 FROM ai_trial_declarations FOR NO KEY UPDATE;
        IF EXISTS (
            SELECT 1 FROM ai_trial_declarations d
            WHERE (SELECT e.to_state FROM ai_trial_state_events e
                   WHERE e.declaration_id = d.declaration_id
                   ORDER BY e.event_id DESC LIMIT 1) = 'active'
        ) THEN
            RAISE EXCEPTION 'v1_active: an AI-trial declaration is active';
        END IF;
    ELSIF nxt = 'halted_loss' THEN
        IF cur <> 'executing' OR NEW.actor <> 'engine' THEN
            RAISE EXCEPTION 'halted_loss is entered from executing by the engine (not % from %)', NEW.actor, cur;
        END IF;
    ELSIF nxt = 'halted_operator' THEN
        IF cur NOT IN ('executing', 'halted_loss') OR NEW.actor NOT IN ('supervisor', 'operator') THEN
            RAISE EXCEPTION 'halted_operator is entered from executing or halted_loss by a person (not % from %)',
                NEW.actor, cur;
        END IF;
    ELSE
        RAISE EXCEPTION 'illegal transition % -> %', cur, nxt;
    END IF;
    RETURN NEW;
END $$;
