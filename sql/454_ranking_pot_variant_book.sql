-- 454_ranking_pot_variant_book.sql
--
-- #2842 slice 6c-ii-a — ranking-pot-v1's no-SL/TP variant book (spec
-- docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md §9.4, "The no-SL/TP variant" paragraph; r3-147).
-- Writer: `app/services/ranking_pot_step.py` (book K + 1, in the step job's one transaction).
--
--   ranking_pot_steps.variant — the variant's step for the session, in the shadow's document format.
--     NOT NULL: no step row exists on any database (no declaration has been frozen), so nothing needs
--     back-filling; the ADD fails loudly if one does, rather than leaving a step row without its variant.
--
-- It also moves sql/448's completion fence (r3-63) from K + 1 book checkpoints to exactly books 0..K + 1. The function is
-- sql/448's, unchanged except that fence; the guard below refuses to replace any other version of it.

ALTER TABLE ranking_pot_steps
    ADD COLUMN IF NOT EXISTS variant JSONB NOT NULL CHECK (jsonb_typeof(variant) = 'object');

COMMENT ON COLUMN ranking_pot_steps.variant IS
    '#2842 the no-SL/TP variant (book K + 1): the shadow with the protective check and the '
    'protective_levels_invalid refusal removed; the shadow''s document format (spec §9.4).';

COMMENT ON TABLE ranking_pot_book_checkpoints IS
    '#2842 ranking-pot simulator state per book (0 = shadow, 1..K = controls, K + 1 = the no-SL/TP variant): '
    'a cache reproducible from ranking_pot_steps, checked against its checkpoint_sha256 before every step.';

DO $$
BEGIN
    IF pg_get_functiondef('ranking_pot_state_events_transition()'::regprocedure)
       NOT LIKE '%lacks K + 1 book checkpoints%'
       AND pg_get_functiondef('ranking_pot_state_events_transition()'::regprocedure)
       NOT LIKE '%lacks K + 2 book checkpoints%' THEN
        RAISE EXCEPTION 'ranking_pot_state_events_transition drifted from sql/448: not replacing it';
    END IF;
END $$;

-- sql/448's transition trigger, unchanged except the completion fence's book set.
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
        -- slice 6b, completion fence (r3-63); slice 6c-ii-a: K + 2 books (the shadow, K controls, the
        -- no-SL/TP variant). With a decided snapshot the books exist (or will, at the first step): all of them
        -- must be flat, and a step must have applied this declaration's FIRST winding_down event. With none
        -- there are no books and no fence.
        IF EXISTS (
            SELECT 1 FROM ranking_pot_rebalance_attempts
            WHERE declaration_id = NEW.declaration_id AND outcome = 'decided'
        ) THEN
            -- Count AND the highest book: with the (declaration, book) key and `book >= 0`, together they mean
            -- exactly books 0..K + 1 (Codex ckpt-1: a count alone admits a set without the variant).
            IF (SELECT (count(*), max(book)) FROM ranking_pot_book_checkpoints
                WHERE declaration_id = NEW.declaration_id)
               IS DISTINCT FROM (SELECT ((doc -> 'terms' ->> 'k_controls')::bigint + 2,
                                         (doc -> 'terms' ->> 'k_controls')::int + 1)
                                 FROM ranking_pot_declarations WHERE declaration_id = NEW.declaration_id)
            THEN
                RAISE EXCEPTION 'books_incomplete: ranking pot % lacks K + 2 book checkpoints', NEW.declaration_id;
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
