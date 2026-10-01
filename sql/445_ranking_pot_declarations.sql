-- 445_ranking_pot_declarations.sql
--
-- #2842 slice 4a — ranking-pot-v1's declaration and trial-state tables.
-- Spec: docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md §7.1 (identity, states), §8
-- (declaration, family spending). Writer: `app/services/ranking_pot_freeze.py` (the freeze,
-- a supervisor step) and, from slice 5, the activation script and the engine's halts. The
-- rebalance, decision and ledger tables land with the jobs that write them (slices 4b, 6).
--
-- ⚠ APPEND-ONLY, BY TRIGGER (sql/264: a trigger binds the superuser this app connects as).
--
-- §7.1 transition matrix (r3-65). Actor in brackets; any other pair is refused.
--
--   <none>          → shadow_only                      [supervisor]  the freeze
--   shadow_only     → executing                        [supervisor]  activation (slice 5 script)
--   executing       → halted_loss                      [engine]      §7.4 loss check
--   executing       → halted_operator                  [supervisor | operator]
--   halted_loss     → halted_operator                  [supervisor | operator]
--   halted_loss     → executing                        [supervisor]
--   halted_operator → executing                        [supervisor]
--   any non-terminal → winding_down                    harm | completed_window [engine];
--                                                      operator [supervisor | operator]
--   winding_down    → completed                        [engine]      once the executed book is flat
--
-- Precedence (r3-65): winding_down dominates every halt (it is reachable from each, and nothing
-- leaves it but `completed`); an operator halt dominates a loss halt (`halted_loss →
-- halted_operator` is legal, the reverse is not: a loss found while operator-halted is caught by
-- the entry path's own loss check at resumption, §7.4). `completed` is terminal.
--
-- ⚠ WHAT THE DATABASE CHECKS, AND WHAT IT CANNOT.
--   * It CAN refuse `→ executing` while any AI-trial declaration is `active` (§7.3 `v1_active`),
--     under a lock on that declaration, so a concurrent v1 resumption serialises against it
--     (r3-127). The entry path re-checks it at every rebalance (§7.1, slice 5).
--   * LOCK ORDER (deadlock-free by construction): pot declaration row → `ai_trial_declarations`.
--     No AI-trial writer touches a ranking_pot table, so nothing takes them in the reverse order;
--     a future writer that needs both MUST take them in this order.
--   * It CANNOT check "before the 12-month look" (the look table is slice 6, which extends this
--     trigger) nor that the executed book is flat at `completed` (r3-63/64: slice 5's
--     wind-down writer asserts it, including uncertain submissions and pending orders).
--   * It CANNOT establish WHO the actor is (r3-129): `actor` is the writer's own claim. What
--     bounds `supervisor` is that its only writers are scripts run from the main checkout,
--     stated in §12 as a residual, not enforced here.


-- ---------------------------------------------------------------------------
-- 0. Shared: append-only guard
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION prevent_ranking_pot_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only (#2842): a ranking-pot record, once written, stays as written', TG_TABLE_NAME;
END $$;


-- ---------------------------------------------------------------------------
-- 1. Declarations (§8): the frozen document, bound to its #2599 row
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ranking_pot_declarations (
    declaration_id   BIGINT PRIMARY KEY
                     REFERENCES strategy_preregistration_declarations (declaration_id) ON DELETE RESTRICT,
    strategy_id      TEXT NOT NULL CHECK (strategy_id ~ '^ranking-pot-v[1-9][0-9]*$'),
    strategy_version TEXT NOT NULL CHECK (strategy_version ~ '^v[1-9][0-9]*$'),
    -- §8 family spending: the m-th declaration of the family spends 0.05 * 2^-m. Assigned by the
    -- bind trigger (dense, from 1), never by the writer, so a re-declaration cannot re-use an m.
    family           TEXT NOT NULL CHECK (family = 'ranking-pot'),
    family_seq       INTEGER NOT NULL CHECK (family_seq >= 1),
    doc_path         TEXT NOT NULL CHECK (btrim(doc_path) <> ''),
    doc              JSONB NOT NULL,
    doc_sha256       TEXT NOT NULL CHECK (doc_sha256 ~ '^[0-9a-f]{64}$'),
    frozen_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ranking_pot_declarations_columns_match_doc
        CHECK ((doc ->> 'strategy_id') IS NOT DISTINCT FROM strategy_id
               AND (doc ->> 'strategy_version') IS NOT DISTINCT FROM strategy_version
               AND (doc ->> 'family') IS NOT DISTINCT FROM family
               AND (doc ->> 'family_seq') IS NOT DISTINCT FROM family_seq::text),
    CONSTRAINT ranking_pot_declarations_one_per_version UNIQUE (strategy_id, strategy_version),
    CONSTRAINT ranking_pot_declarations_family_seq_unique UNIQUE (family, family_seq)
);

COMMENT ON TABLE ranking_pot_declarations IS
    '#2842 ranking-pot declarations, append-only: the frozen document of one #2599 declaration '
    '(contract_version ranking-pot-declaration-v1:<doc_sha256>), with its family spending index.';

CREATE OR REPLACE FUNCTION ranking_pot_declarations_bind_declaration()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    declared_id       TEXT;
    declared_version  TEXT;
    declared_contract TEXT;
    next_seq          INTEGER;
BEGIN
    SELECT strategy_id, strategy_version, contract_version
      INTO declared_id, declared_version, declared_contract
    FROM strategy_preregistration_declarations WHERE declaration_id = NEW.declaration_id;
    IF declared_id IS DISTINCT FROM NEW.strategy_id OR declared_version IS DISTINCT FROM NEW.strategy_version THEN
        RAISE EXCEPTION 'declaration % is %/%, not %/%',
            NEW.declaration_id, declared_id, declared_version, NEW.strategy_id, NEW.strategy_version;
    END IF;
    IF declared_contract IS DISTINCT FROM 'ranking-pot-declaration-v1:' || NEW.doc_sha256 THEN
        RAISE EXCEPTION 'declaration % contract % does not name document %',
            NEW.declaration_id, declared_contract, NEW.doc_sha256;
    END IF;

    -- Serialise every declaration of the family: the family seq and the non-terminal check
    -- below must see each other's commits.
    PERFORM pg_advisory_xact_lock(hashtext('ranking_pot_declarations:' || NEW.family));

    -- §8: every declaration charges, abandoned or re-declared ones included; the seq is dense.
    SELECT coalesce(max(family_seq), 0) + 1 INTO next_seq
    FROM ranking_pot_declarations WHERE family = NEW.family;
    IF NEW.family_seq <> next_seq THEN
        RAISE EXCEPTION 'family % seq % is not the next (%)', NEW.family, NEW.family_seq, next_seq;
    END IF;

    -- §7.1: one non-terminal declaration per strategy id. A declaration with no event yet is
    -- non-terminal (the freeze writes its genesis event in the same transaction). The earlier
    -- declarations' rows are locked first: their state trigger takes the same row lock, so the
    -- check below reads their committed latest state, never one a concurrent transition is writing.
    PERFORM 1 FROM ranking_pot_declarations WHERE strategy_id = NEW.strategy_id FOR NO KEY UPDATE;
    IF EXISTS (
        SELECT 1 FROM ranking_pot_declarations d
        WHERE d.strategy_id = NEW.strategy_id
          AND coalesce((SELECT e.to_state FROM ranking_pot_state_events e
                        WHERE e.declaration_id = d.declaration_id
                        ORDER BY e.event_id DESC LIMIT 1), '<none>') <> 'completed'
    ) THEN
        RAISE EXCEPTION '% already has a non-terminal declaration', NEW.strategy_id;
    END IF;
    RETURN NEW;
END $$;


-- ---------------------------------------------------------------------------
-- 2. State events (§7.1): the trial's state is its latest event's `to_state`
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ranking_pot_state_events (
    event_id         BIGSERIAL PRIMARY KEY,
    declaration_id   BIGINT NOT NULL REFERENCES ranking_pot_declarations (declaration_id) ON DELETE RESTRICT,
    -- NULL only on the genesis event. No event at all means NOT shadow_only: readers fail closed.
    from_state       TEXT CHECK (from_state IN
                         ('shadow_only', 'executing', 'halted_loss', 'halted_operator', 'winding_down')),
    to_state         TEXT NOT NULL CHECK (to_state IN
                         ('shadow_only', 'executing', 'halted_loss', 'halted_operator', 'winding_down',
                          'completed')),
    -- §7.1: `winding_down` carries its reason; no other state does.
    wind_down_reason TEXT CHECK (wind_down_reason IN ('harm', 'completed_window', 'operator')),
    reason           TEXT NOT NULL CHECK (btrim(reason) <> ''),
    actor            TEXT NOT NULL CHECK (actor IN ('engine', 'supervisor', 'operator')),
    at               TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ranking_pot_state_events_wind_down_reason
        CHECK ((to_state = 'winding_down') = (wind_down_reason IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS ranking_pot_state_events_declaration
    ON ranking_pot_state_events (declaration_id, event_id);

-- Declared after the events table: the bind trigger above reads it.
DROP TRIGGER IF EXISTS trg_ranking_pot_declarations_bind ON ranking_pot_declarations;
CREATE TRIGGER trg_ranking_pot_declarations_bind
BEFORE INSERT ON ranking_pot_declarations
FOR EACH ROW EXECUTE FUNCTION ranking_pot_declarations_bind_declaration();

DROP TRIGGER IF EXISTS trg_ranking_pot_declarations_append_only ON ranking_pot_declarations;
CREATE TRIGGER trg_ranking_pot_declarations_append_only
BEFORE UPDATE OR DELETE ON ranking_pot_declarations
FOR EACH ROW EXECUTE FUNCTION prevent_ranking_pot_mutation();

-- The matrix in the header. `from_state` must equal the CURRENT state (optimistic concurrency):
-- the declaration row lock serialises writers, and under READ COMMITTED each statement below
-- takes a fresh snapshot, so a writer that waited sees the winner's event and is refused.
CREATE OR REPLACE FUNCTION ranking_pot_state_events_transition()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    cur TEXT;
    nxt TEXT := NEW.to_state;
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
    ELSIF nxt = 'executing' THEN
        IF cur NOT IN ('shadow_only', 'halted_loss', 'halted_operator') OR NEW.actor <> 'supervisor' THEN
            RAISE EXCEPTION 'executing is entered from shadow_only or a halt by the supervisor (not % from %)',
                NEW.actor, cur;
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

DROP TRIGGER IF EXISTS trg_ranking_pot_state_events_transition ON ranking_pot_state_events;
CREATE TRIGGER trg_ranking_pot_state_events_transition
BEFORE INSERT ON ranking_pot_state_events
FOR EACH ROW EXECUTE FUNCTION ranking_pot_state_events_transition();

DROP TRIGGER IF EXISTS trg_ranking_pot_state_events_append_only ON ranking_pot_state_events;
CREATE TRIGGER trg_ranking_pot_state_events_append_only
BEFORE UPDATE OR DELETE ON ranking_pot_state_events
FOR EACH ROW EXECUTE FUNCTION prevent_ranking_pot_mutation();
