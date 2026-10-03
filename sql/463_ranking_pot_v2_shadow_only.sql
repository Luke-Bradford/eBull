-- 463_ranking_pot_v2_shadow_only.sql
--
-- #3592 slice 3a — ranking-pot-v2's declaration terms, its completion fence and its no-executed-book enforcement
-- (spec docs/proposals/execution/2026-10-03-3592-ranking-pot-v2.md §4 "Step job" and "No executed book, enforced",
-- §8; Appendix A R2-46–52). v2 is a shadow-only seat: books 0 (shadow), 1..K (controls), K + 1 (no-SL/TP variant),
-- K + 2 (the v1-reference book), and no executed book at all.
--
--   1. Declaration terms (a BEFORE INSERT trigger beside sql/445's bind trigger, which is not restated):
--      `ranking-pot-v2` must carry `terms.execution = "none"` and `terms.book_count` = `terms.k_controls` + 3, both
--      as canonical JSON (a string, and an unsigned integer literal). Any other strategy id may carry
--      `execution = "none"` (more restrictive, never less) and, if it carries `book_count`, exactly
--      `k_controls` + 2 (v1's layout); a future layout extends this trigger in its own migration.
--   2. The completion fence (sql/454's revision of sql/448's transition function, unchanged except the fence):
--      the book count comes from `terms.book_count` (legacy declarations without it keep K + 2), the checkpoints
--      must be exactly books 0..count − 1, and every book's `positions` and `pending` must be JSON arrays, and empty.
--   3. `→ executing` is refused under `execution = "none"` — in sql/452's activation trigger, which fires before
--      the transition trigger (trigger names order BEFORE triggers), so the refusal names the real reason rather
--      than `not_activated`.
--   4. Every executed-book relation refuses an INSERT or UPDATE whose declaration carries `execution = "none"`:
--      sql/449's rebalances, lifecycles, decisions and exit stamps; sql/450's activations and submissions; sql/453's
--      level observations; and sql/458's entry tickets citing a ranking-pot declaration. Absence of an activation
--      row is not relied on. `tests/test_ranking_pot_v2_schema_db.py` pins the relation inventory.
--   5. The v2 rebalance refusals join `ranking_pot_rebalance_attempts.refusal`'s vocabulary.
--
-- SQL is outside both policy hashes: v1 cannot drift by hash, and its completion, activation and wind-down paths
-- are pinned by the existing ranking-pot DB tests plus the v1 regressions in the new test file.


-- ---------------------------------------------------------------------------
-- 1. Declaration terms
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ranking_pot_declarations_shadow_terms()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    terms JSONB := NEW.doc -> 'terms';
    k     JSONB := NEW.doc -> 'terms' -> 'k_controls';
    bc    JSONB := NEW.doc -> 'terms' -> 'book_count';
    ex    JSONB := NEW.doc -> 'terms' -> 'execution';
BEGIN
    IF ex IS NOT NULL AND ex IS DISTINCT FROM '"none"'::jsonb THEN
        RAISE EXCEPTION 'declaration % terms.execution is %, and "none" is the only value', NEW.declaration_id, ex;
    END IF;
    IF bc IS NOT NULL AND (jsonb_typeof(k) IS DISTINCT FROM 'number' OR k::text !~ '^[0-9]{1,9}$'
                           OR jsonb_typeof(bc) <> 'number' OR bc::text !~ '^[0-9]{1,9}$') THEN
        RAISE EXCEPTION 'declaration % terms.book_count % and k_controls % must be unsigned integers',
            NEW.declaration_id, bc, k;
    END IF;
    IF NEW.strategy_id = 'ranking-pot-v2' THEN
        IF ex IS NULL THEN
            RAISE EXCEPTION 'ranking-pot-v2 declaration % must carry terms.execution = "none"', NEW.declaration_id;
        END IF;
        IF bc IS NULL OR bc::text::bigint <> k::text::bigint + 3 THEN
            RAISE EXCEPTION 'ranking-pot-v2 declaration % terms.book_count % is not k_controls % + 3',
                NEW.declaration_id, bc, k;
        END IF;
    ELSIF bc IS NOT NULL AND bc::text::bigint <> k::text::bigint + 2 THEN
        RAISE EXCEPTION 'declaration % (%) terms.book_count % is not k_controls % + 2',
            NEW.declaration_id, NEW.strategy_id, bc, k;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ranking_pot_declarations_shadow_terms ON ranking_pot_declarations;
CREATE TRIGGER trg_ranking_pot_declarations_shadow_terms
BEFORE INSERT ON ranking_pot_declarations
FOR EACH ROW EXECUTE FUNCTION ranking_pot_declarations_shadow_terms();


-- ---------------------------------------------------------------------------
-- 2. The completion fence
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF pg_get_functiondef('ranking_pot_state_events_transition()'::regprocedure)
       NOT LIKE '%lacks K + 2 book checkpoints%'
       AND pg_get_functiondef('ranking_pot_state_events_transition()'::regprocedure)
       NOT LIKE '%lacks its % book checkpoints (books 0..%'
    THEN
        RAISE EXCEPTION 'ranking_pot_state_events_transition drifted from sql/454: not replacing it';
    END IF;
END $$;

-- sql/454's transition trigger, unchanged except the completion fence.
CREATE OR REPLACE FUNCTION ranking_pot_state_events_transition()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    cur TEXT;
    nxt TEXT := NEW.to_state;
    t0  DATE;
    a12 DATE;
    nb  BIGINT;
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
        -- slice 6b, completion fence (r3-63); #3592 sql/463: the declaration's own book count (`terms.book_count`,
        -- validated at insert; K + 2 for a declaration without it). With a decided snapshot the books exist (or
        -- will, at the first step): all of them must be flat, and a step must have applied this declaration's
        -- FIRST winding_down event. With none there are no books and no fence.
        IF EXISTS (
            SELECT 1 FROM ranking_pot_rebalance_attempts
            WHERE declaration_id = NEW.declaration_id AND outcome = 'decided'
        ) THEN
            SELECT coalesce((doc -> 'terms' ->> 'book_count')::bigint, (doc -> 'terms' ->> 'k_controls')::bigint + 2)
              INTO nb
            FROM ranking_pot_declarations WHERE declaration_id = NEW.declaration_id;
            -- Count AND the highest book: with the (declaration, book) key and `book >= 0`, together they mean
            -- exactly books 0..nb − 1 (Codex ckpt-1: a count alone admits a set without the last book).
            IF (SELECT (count(*), max(book)) FROM ranking_pot_book_checkpoints
                WHERE declaration_id = NEW.declaration_id)
               IS DISTINCT FROM (nb, (nb - 1)::int)
            THEN
                RAISE EXCEPTION 'books_incomplete: ranking pot % lacks its % book checkpoints (books 0..%)',
                    NEW.declaration_id, nb, nb - 1;
            END IF;
            IF EXISTS (
                SELECT 1 FROM ranking_pot_book_checkpoints
                WHERE declaration_id = NEW.declaration_id
                  AND (jsonb_typeof(state -> 'positions') IS DISTINCT FROM 'array'
                       OR jsonb_typeof(state -> 'pending') IS DISTINCT FROM 'array')
            ) THEN
                RAISE EXCEPTION 'books_malformed: a ranking-pot book''s positions or pending is not an array';
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


-- ---------------------------------------------------------------------------
-- 3. `→ executing` under `execution = "none"` (sql/452's function, the refusal added first)
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF pg_get_functiondef('ranking_pot_executing_activated()'::regprocedure)
       NOT LIKE '%not_activated: ranking pot % has no ranking_pot_activations row%'
    THEN
        RAISE EXCEPTION 'ranking_pot_executing_activated drifted from sql/452: not replacing it';
    END IF;
END $$;

-- TRUE / FALSE for a visible declaration, NULL for one this call cannot see. VOLATILE, so each call reads under a
-- fresh snapshot, not the calling statement's (Codex ckpt-2: a STABLE lookup reads the statement snapshot, which
-- cannot see a declaration inserted by a sibling sub-statement of the same data-modifying CTE;
-- tests/test_ranking_pot_v2_schema_db.py pins that this one refuses that case).
CREATE OR REPLACE FUNCTION ranking_pot_execution_none(p_declaration_id BIGINT)
RETURNS BOOLEAN LANGUAGE sql VOLATILE AS $$
    SELECT coalesce(doc -> 'terms' -> 'execution' = '"none"'::jsonb, FALSE)
    FROM ranking_pot_declarations WHERE declaration_id = p_declaration_id
$$;

CREATE OR REPLACE FUNCTION ranking_pot_executing_activated()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.to_state = 'executing' AND ranking_pot_execution_none(NEW.declaration_id) IS NOT FALSE THEN
        RAISE EXCEPTION 'execution_none: ranking pot % is declared without an executed book', NEW.declaration_id;
    END IF;
    IF NEW.to_state = 'executing'
       AND NOT EXISTS (SELECT 1 FROM ranking_pot_activations a WHERE a.declaration_id = NEW.declaration_id) THEN
        RAISE EXCEPTION 'not_activated: ranking pot % has no ranking_pot_activations row', NEW.declaration_id;
    END IF;
    RETURN NEW;
END $$;


-- ---------------------------------------------------------------------------
-- 4. Executed-book relations refuse a declaration without an executed book
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ranking_pot_executed_book_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    decl BIGINT;
BEGIN
    -- Every writer inserts the parent row (declaration, rebalance, lifecycle, submission) in an EARLIER statement,
    -- so a parent this trigger cannot resolve is never a legitimate write: it is refused, fail-closed, rather than
    -- let through as "not declared none" (ahead of the foreign key, which would refuse a missing one anyway).
    IF TG_TABLE_NAME = 'strategy_entry_tickets' THEN  -- nested: NEW.evidence_kind exists on this table only
        IF NEW.evidence_kind IS DISTINCT FROM 'ranking_pot_declaration' THEN
            RETURN NEW;
        END IF;
    END IF;
    CASE TG_TABLE_NAME
        WHEN 'ranking_pot_activations', 'ranking_pot_exec_rebalances', 'ranking_pot_exec_lifecycles',
             'ranking_pot_exec_exit_stamps' THEN
            decl := NEW.declaration_id;
        WHEN 'ranking_pot_exec_decisions' THEN
            SELECT r.declaration_id INTO decl FROM ranking_pot_exec_rebalances r WHERE r.attempt_id = NEW.attempt_id;
        WHEN 'ranking_pot_exec_submissions' THEN
            SELECT l.declaration_id INTO decl FROM ranking_pot_exec_lifecycles l WHERE l.lifecycle_id = NEW.lifecycle_id;
        WHEN 'ranking_pot_exec_level_observations' THEN
            SELECT l.declaration_id INTO decl
            FROM ranking_pot_exec_submissions s JOIN ranking_pot_exec_lifecycles l USING (lifecycle_id)
            WHERE s.strategy_trade_id = NEW.strategy_trade_id;
        WHEN 'strategy_entry_tickets' THEN
            decl := NEW.evidence_id;
        ELSE
            RAISE EXCEPTION 'ranking_pot_executed_book_guard is not wired for %', TG_TABLE_NAME;
    END CASE;
    IF ranking_pot_execution_none(decl) IS NULL THEN
        RAISE EXCEPTION 'execution_unresolved: % row''s ranking-pot declaration is not visible (%)', TG_TABLE_NAME, decl;
    END IF;
    IF ranking_pot_execution_none(decl) THEN
        RAISE EXCEPTION 'execution_none: ranking pot % is declared without an executed book (% refused)',
            decl, TG_TABLE_NAME;
    END IF;
    RETURN NEW;
END $$;

DO $$
DECLARE
    t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['ranking_pot_activations', 'ranking_pot_exec_rebalances', 'ranking_pot_exec_lifecycles',
                             'ranking_pot_exec_decisions', 'ranking_pot_exec_exit_stamps',
                             'ranking_pot_exec_submissions', 'ranking_pot_exec_level_observations',
                             'strategy_entry_tickets'] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_execution_none ON %I', t, t);
        EXECUTE format('CREATE TRIGGER trg_%s_execution_none BEFORE INSERT OR UPDATE ON %I '
                       'FOR EACH ROW EXECUTE FUNCTION ranking_pot_executed_book_guard()', t, t);
    END LOOP;
END $$;


-- ---------------------------------------------------------------------------
-- 5. v2's rebalance refusals (spec §4: checked before scoring, and `insider_read_failed` after it)
-- ---------------------------------------------------------------------------
ALTER TABLE ranking_pot_rebalance_attempts DROP CONSTRAINT IF EXISTS ranking_pot_rebalance_attempts_refusal_check;
ALTER TABLE ranking_pot_rebalance_attempts
    ADD CONSTRAINT ranking_pot_rebalance_attempts_refusal_check CHECK (refusal IN (
        'scores_run_incomplete', 'price_daily_stale', 'ranking_drift',
        'universe_collapse', 'breakpoint_unavailable', 'max_cut_unavailable',
        'spy_unavailable', 'scoring_failed', 'look_pending', 'not_attempted',
        'dtc_unavailable', 'dtc_incomplete', 'insider_history_floor_moved', 'insider_read_failed'));
