-- 449_ranking_pot_executed_book.sql
--
-- #2842 slice 5a — ranking-pot-v1's EXECUTED book: its §4 step-4 decision at a rebalance (spec
-- docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md §4, "The executed book's decision").
-- Writer: `app/services/ranking_pot_exec.py`, called by the rebalance job inside the transaction
-- that inserts the `decided` attempt. No broker I/O here; the loader / executor (5b) and the exits
-- (5c) read these rows.
--
-- ⚠ APPEND-ONLY, BY TRIGGER (sql/264: a trigger binds the superuser this app connects as).
--
--   ranking_pot_exec_rebalances  — one header per decided attempt taken in `executing` or a halt:
--                                  the state, `entries_allowed`, the `v1_active` reading and the
--                                  lifecycle classification the decision read.
--   ranking_pot_exec_lifecycles  — one per executed `enter` row: slot, the slot it takes over
--                                  (`replaces_lifecycle_id`), its `strategy_signals` row, the §6 ticket.
--   ranking_pot_exec_decisions   — the §5.2 rows; every non-`not_selected` row names its lifecycle.
--   ranking_pot_exec_exit_stamps — one immutable exit stamp per lifecycle (§7.4), from the attempt's
--                                  `exit` row or (5c) the `winding_down` event.
--
-- ⚠ SAME TRANSACTION. Every row here is written by the transaction that inserted its decided
-- attempt: the triggers compare the attempt row's `xmin` with `pg_current_xact_id()` (the TOP-level
-- id, so a savepoint inside the writer still passes; an attempt inserted inside a savepoint does not,
-- and fails closed). A decision and its records therefore commit together or not at all, and no
-- later transaction can add executed rows to an old attempt. That transaction holds `sql/446`'s SHARE
-- lock on `ranking_pot_state_events` from before its snapshot, so the state the header records is
-- the state the decision read, and no state event can commit until it ends.
--
-- ⚠ WHAT THE DATABASE CANNOT CHECK: that the rows follow `ranking_pot.decide` (the writer reads the
-- rows back and asserts them), that held lifecycles have distinct names and slots across attempts
-- (classification depends on trade status; the reader raises), and a wind-down stamp's session (the
-- writer computes W with `ranking_pot_step.wind_down_session`).

CREATE TABLE IF NOT EXISTS ranking_pot_exec_rebalances (
    attempt_id       BIGINT PRIMARY KEY REFERENCES ranking_pot_rebalance_attempts (attempt_id) ON DELETE RESTRICT,
    declaration_id   BIGINT NOT NULL REFERENCES ranking_pot_declarations (declaration_id) ON DELETE RESTRICT,
    state            TEXT NOT NULL CHECK (state IN ('executing', 'halted_loss', 'halted_operator')),
    entries_allowed  BOOLEAN NOT NULL,
    -- The snapshot's reading of "an AI-trial declaration is active" (§7.1). Recorded, not authoritative:
    -- the executor re-checks it under locks at submission.
    v1_active        BOOLEAN NOT NULL,
    -- Every non-terminal lifecycle the decision read, as [lifecycle_id, instrument_id, slot, status,
    -- trade_status], ascending by lifecycle_id.
    held             JSONB NOT NULL CHECK (jsonb_typeof(held) = 'array'),
    -- Every lifecycle classified `closed` at this decision: the next decision's `recently_exited`
    -- is the set closed then and not here.
    closed_lifecycle_ids BIGINT[] NOT NULL,
    detail           JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(detail) = 'object'),
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ranking_pot_exec_rebalances_entries
        CHECK (NOT entries_allowed OR (state = 'executing' AND NOT v1_active))
);

CREATE INDEX IF NOT EXISTS ranking_pot_exec_rebalances_declaration
    ON ranking_pot_exec_rebalances (declaration_id, attempt_id);

CREATE TABLE IF NOT EXISTS ranking_pot_exec_lifecycles (
    lifecycle_id          BIGSERIAL PRIMARY KEY,
    attempt_id            BIGINT NOT NULL REFERENCES ranking_pot_exec_rebalances (attempt_id) ON DELETE RESTRICT,
    declaration_id        BIGINT NOT NULL REFERENCES ranking_pot_declarations (declaration_id) ON DELETE RESTRICT,
    instrument_id         BIGINT NOT NULL REFERENCES instruments (instrument_id) ON DELETE RESTRICT,
    slot                  SMALLINT NOT NULL CHECK (slot >= 1),
    replaces_lifecycle_id BIGINT REFERENCES ranking_pot_exec_lifecycles (lifecycle_id) ON DELETE RESTRICT,
    signal_id             BIGINT NOT NULL UNIQUE REFERENCES strategy_signals (signal_id) ON DELETE RESTRICT,
    ticket                JSONB NOT NULL CHECK (jsonb_typeof(ticket) = 'object'),
    ticket_sha256         TEXT NOT NULL CHECK (ticket_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ranking_pot_exec_lifecycles_name UNIQUE (attempt_id, instrument_id),
    CONSTRAINT ranking_pot_exec_lifecycles_slot UNIQUE (attempt_id, slot)
);

CREATE INDEX IF NOT EXISTS ranking_pot_exec_lifecycles_declaration
    ON ranking_pot_exec_lifecycles (declaration_id, lifecycle_id);

CREATE TABLE IF NOT EXISTS ranking_pot_exec_decisions (
    attempt_id     BIGINT NOT NULL REFERENCES ranking_pot_exec_rebalances (attempt_id) ON DELETE RESTRICT,
    instrument_id  BIGINT NOT NULL REFERENCES instruments (instrument_id) ON DELETE RESTRICT,
    action         TEXT NOT NULL CHECK (action IN ('exit', 'exit_pending', 'hold', 'enter', 'not_selected')),
    reason         TEXT,
    r_rank         INTEGER CHECK (r_rank >= 1),
    f_rank         INTEGER CHECK (f_rank >= 1),
    lifecycle_id   BIGINT REFERENCES ranking_pot_exec_lifecycles (lifecycle_id) ON DELETE RESTRICT,

    PRIMARY KEY (attempt_id, instrument_id),
    CONSTRAINT ranking_pot_exec_decisions_lifecycle
        CHECK ((action = 'not_selected') = (lifecycle_id IS NULL)),
    CONSTRAINT ranking_pot_exec_decisions_reason
        CHECK ((action IN ('exit', 'not_selected')) = (reason IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS ranking_pot_exec_exit_stamps (
    lifecycle_id     BIGINT PRIMARY KEY REFERENCES ranking_pot_exec_lifecycles (lifecycle_id) ON DELETE RESTRICT,
    declaration_id   BIGINT NOT NULL REFERENCES ranking_pot_declarations (declaration_id) ON DELETE RESTRICT,
    attempt_id       BIGINT REFERENCES ranking_pot_exec_rebalances (attempt_id) ON DELETE RESTRICT,
    state_event_id   BIGINT REFERENCES ranking_pot_state_events (event_id) ON DELETE RESTRICT,
    exit_session     DATE NOT NULL,
    reason           TEXT NOT NULL CHECK (btrim(reason) <> ''),
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ranking_pot_exec_exit_stamps_one_source CHECK (num_nonnulls(attempt_id, state_event_id) = 1)
);

-- The shared fence: the attempt is `decided`, of this declaration, inserted by THIS transaction, and
-- this transaction holds sql/446's SHARE lock. Returns the attempt's target session.
CREATE OR REPLACE FUNCTION ranking_pot_exec_same_transaction(p_attempt_id BIGINT, p_declaration_id BIGINT)
RETURNS DATE LANGUAGE plpgsql AS $$
DECLARE
    a RECORD;
BEGIN
    SELECT declaration_id, outcome, target_session, xmin INTO a
    FROM ranking_pot_rebalance_attempts WHERE attempt_id = p_attempt_id;
    IF a IS NULL OR a.outcome <> 'decided' OR a.declaration_id <> p_declaration_id THEN
        RAISE EXCEPTION 'attempt % is not a decided attempt of ranking pot %', p_attempt_id, p_declaration_id;
    END IF;
    IF a.xmin <> pg_current_xact_id()::xid THEN
        RAISE EXCEPTION 'executed-book rows for attempt % are written only by the transaction that decided it',
            p_attempt_id;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_locks
        WHERE locktype = 'relation' AND relation = 'ranking_pot_state_events'::regclass
          AND pid = pg_backend_pid() AND granted
          AND mode IN ('ShareLock', 'ShareRowExclusiveLock', 'ExclusiveLock', 'AccessExclusiveLock')
    ) THEN
        RAISE EXCEPTION 'executed-book rows need SHARE on ranking_pot_state_events (sql/446 header)';
    END IF;
    RETURN a.target_session;
END $$;

CREATE OR REPLACE FUNCTION ranking_pot_exec_rebalances_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    cur TEXT;
BEGIN
    PERFORM ranking_pot_exec_same_transaction(NEW.attempt_id, NEW.declaration_id);
    SELECT to_state INTO cur FROM ranking_pot_state_events
    WHERE declaration_id = NEW.declaration_id ORDER BY event_id DESC LIMIT 1;
    IF cur IS DISTINCT FROM NEW.state THEN
        RAISE EXCEPTION 'ranking pot % is %, not %: no executed-book decision', NEW.declaration_id,
            coalesce(cur, '<none>'), NEW.state;
    END IF;
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION ranking_pot_exec_lifecycles_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    target   DATE;
    h        RECORD;
    sig      RECORD;
    decl     RECORD;
    replaced RECORD;
BEGIN
    target := ranking_pot_exec_same_transaction(NEW.attempt_id, NEW.declaration_id);
    SELECT state, entries_allowed INTO h FROM ranking_pot_exec_rebalances WHERE attempt_id = NEW.attempt_id;
    IF NOT h.entries_allowed THEN
        RAISE EXCEPTION 'attempt % allows no executed entry (state %)', NEW.attempt_id, h.state;
    END IF;
    SELECT strategy_id, strategy_version, (doc -> 'terms' ->> 'n')::int AS n INTO decl
    FROM ranking_pot_declarations WHERE declaration_id = NEW.declaration_id;
    IF NEW.slot > decl.n THEN
        RAISE EXCEPTION 'slot % exceeds N = %', NEW.slot, decl.n;
    END IF;
    SELECT strategy_id, strategy_version, instrument_id, signal_kind, verdict, fill_bar_date INTO sig
    FROM strategy_signals WHERE signal_id = NEW.signal_id;
    IF sig.strategy_id IS DISTINCT FROM decl.strategy_id OR sig.strategy_version IS DISTINCT FROM decl.strategy_version
       OR sig.instrument_id IS DISTINCT FROM NEW.instrument_id OR sig.signal_kind IS DISTINCT FROM 'entry'
       OR sig.verdict IS DISTINCT FROM 'fired' OR sig.fill_bar_date IS DISTINCT FROM target THEN
        RAISE EXCEPTION 'signal % is not this lifecycle''s fired entry for %', NEW.signal_id, target;
    END IF;
    IF NEW.replaces_lifecycle_id IS NOT NULL THEN
        SELECT l.declaration_id, l.slot, s.attempt_id AS stamped_by INTO replaced
        FROM ranking_pot_exec_lifecycles l
        LEFT JOIN ranking_pot_exec_exit_stamps s ON s.lifecycle_id = l.lifecycle_id
        WHERE l.lifecycle_id = NEW.replaces_lifecycle_id;
        IF replaced.declaration_id IS DISTINCT FROM NEW.declaration_id OR replaced.slot IS DISTINCT FROM NEW.slot
           OR replaced.stamped_by IS DISTINCT FROM NEW.attempt_id THEN
            RAISE EXCEPTION 'lifecycle % takes over slot % only from a lifecycle this attempt stamps for exit',
                NEW.lifecycle_id, NEW.slot;
        END IF;
    END IF;
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION ranking_pot_exec_decisions_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    decl_id BIGINT;
    lc      RECORD;
BEGIN
    SELECT declaration_id INTO decl_id FROM ranking_pot_exec_rebalances WHERE attempt_id = NEW.attempt_id;
    PERFORM ranking_pot_exec_same_transaction(NEW.attempt_id, decl_id);
    IF NEW.lifecycle_id IS NOT NULL THEN
        SELECT declaration_id, instrument_id, attempt_id INTO lc
        FROM ranking_pot_exec_lifecycles WHERE lifecycle_id = NEW.lifecycle_id;
        IF lc.declaration_id IS DISTINCT FROM decl_id OR lc.instrument_id IS DISTINCT FROM NEW.instrument_id
           OR (NEW.action = 'enter') <> (lc.attempt_id = NEW.attempt_id) THEN
            RAISE EXCEPTION 'decision % for % names lifecycle % of another name, declaration or attempt',
                NEW.action, NEW.instrument_id, NEW.lifecycle_id;
        END IF;
    END IF;
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION ranking_pot_exec_exit_stamps_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    target DATE;
    lc     RECORD;
    d      RECORD;
BEGIN
    SELECT declaration_id, instrument_id INTO lc FROM ranking_pot_exec_lifecycles WHERE lifecycle_id = NEW.lifecycle_id;
    IF lc.declaration_id IS DISTINCT FROM NEW.declaration_id THEN
        RAISE EXCEPTION 'lifecycle % is not of ranking pot %', NEW.lifecycle_id, NEW.declaration_id;
    END IF;
    IF NEW.attempt_id IS NOT NULL THEN
        target := ranking_pot_exec_same_transaction(NEW.attempt_id, NEW.declaration_id);
        SELECT action, reason, lifecycle_id INTO d FROM ranking_pot_exec_decisions
        WHERE attempt_id = NEW.attempt_id AND instrument_id = lc.instrument_id;
        IF d.action IS DISTINCT FROM 'exit' OR d.lifecycle_id IS DISTINCT FROM NEW.lifecycle_id
           OR d.reason IS DISTINCT FROM NEW.reason OR NEW.exit_session IS DISTINCT FROM target THEN
            RAISE EXCEPTION 'a rebalance stamp needs that attempt''s exit row for lifecycle % on its target session %',
                NEW.lifecycle_id, target;
        END IF;
    ELSIF NOT EXISTS (
        SELECT 1 FROM ranking_pot_state_events
        WHERE event_id = NEW.state_event_id AND declaration_id = NEW.declaration_id AND to_state = 'winding_down'
    ) THEN
        RAISE EXCEPTION 'state event % is not a winding_down event of ranking pot %', NEW.state_event_id,
            NEW.declaration_id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ranking_pot_exec_rebalances_guard ON ranking_pot_exec_rebalances;
CREATE TRIGGER trg_ranking_pot_exec_rebalances_guard
BEFORE INSERT ON ranking_pot_exec_rebalances
FOR EACH ROW EXECUTE FUNCTION ranking_pot_exec_rebalances_guard();

DROP TRIGGER IF EXISTS trg_ranking_pot_exec_lifecycles_guard ON ranking_pot_exec_lifecycles;
CREATE TRIGGER trg_ranking_pot_exec_lifecycles_guard
BEFORE INSERT ON ranking_pot_exec_lifecycles
FOR EACH ROW EXECUTE FUNCTION ranking_pot_exec_lifecycles_guard();

DROP TRIGGER IF EXISTS trg_ranking_pot_exec_decisions_guard ON ranking_pot_exec_decisions;
CREATE TRIGGER trg_ranking_pot_exec_decisions_guard
BEFORE INSERT ON ranking_pot_exec_decisions
FOR EACH ROW EXECUTE FUNCTION ranking_pot_exec_decisions_guard();

DROP TRIGGER IF EXISTS trg_ranking_pot_exec_exit_stamps_guard ON ranking_pot_exec_exit_stamps;
CREATE TRIGGER trg_ranking_pot_exec_exit_stamps_guard
BEFORE INSERT ON ranking_pot_exec_exit_stamps
FOR EACH ROW EXECUTE FUNCTION ranking_pot_exec_exit_stamps_guard();

DO $$
DECLARE
    t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['ranking_pot_exec_rebalances', 'ranking_pot_exec_lifecycles',
                             'ranking_pot_exec_decisions', 'ranking_pot_exec_exit_stamps'] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_append_only ON %I', t, t);
        EXECUTE format('CREATE TRIGGER trg_%s_append_only BEFORE UPDATE OR DELETE ON %I '
                       'FOR EACH ROW EXECUTE FUNCTION prevent_ranking_pot_mutation()', t, t);
    END LOOP;
END $$;
