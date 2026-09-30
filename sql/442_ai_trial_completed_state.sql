-- 442_ai_trial_completed_state.sql
--
-- #3515 slice 3b-i (spec docs/proposals/execution/2026-09-30-3515-ai-discretionary-fund-v1.md §0, §7).
--
-- 1. A `completed` terminal state. sql/432 makes only `halted_harm` / `halted_loss` terminal, so a
--    trial that simply finishes (v1 spec §9: 60 sessions, then the readout) has no state that ends
--    it: `halted_operator` is resumable, and fund-v1's start gate (`ai_trial_wind_down`) refuses
--    `v1_not_wound_down` until every v1 declaration is terminal. `completed` is written by the
--    SUPERVISOR only, from `active` or from a resumable halt, and admits no further transition.
--    Every trial reader keys on `= 'active'`, so a completed trial is treated as a halted one: no
--    new entries, open positions still run to their exits.
-- 2. `ai_trial_declarations.strategy_id` admits `ai-discretionary-fund-v<N>` (spec §7). As for
--    v1, the column holds the ARM's id only; the control leg's id (`<arm>-control`) is derived,
--    never declared (sql/432 §1). The bind trigger still requires the #2599 row to carry the
--    same id and version.
--
-- Nothing existing changes: every stored state and strategy id satisfies the new CHECKs, and the
-- transition function keeps every sql/432 rule; `completed` is the only addition.

ALTER TABLE ai_trial_state_events DROP CONSTRAINT IF EXISTS ai_trial_state_events_to_state_check;
ALTER TABLE ai_trial_state_events ADD CONSTRAINT ai_trial_state_events_to_state_check
    CHECK (to_state IN ('active', 'halted_harm', 'halted_loss', 'halted_mandate', 'halted_operator', 'completed'));
-- `from_state` is unchanged: `completed` is terminal, so it is never the state a transition leaves.

ALTER TABLE ai_trial_declarations DROP CONSTRAINT IF EXISTS ai_trial_declarations_strategy_id_check;
ALTER TABLE ai_trial_declarations ADD CONSTRAINT ai_trial_declarations_strategy_id_check
    CHECK (strategy_id ~ '^ai-discretionary-(fund-)?v[1-9][0-9]*$');

CREATE OR REPLACE FUNCTION ai_trial_state_events_transition()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    current_state TEXT;
BEGIN
    PERFORM 1 FROM ai_trial_declarations WHERE declaration_id = NEW.declaration_id FOR NO KEY UPDATE;
    SELECT to_state INTO current_state
    FROM ai_trial_state_events WHERE declaration_id = NEW.declaration_id
    ORDER BY event_id DESC LIMIT 1;

    IF NEW.from_state IS DISTINCT FROM current_state THEN
        RAISE EXCEPTION 'stale transition: trial % is in state %, not %',
            NEW.declaration_id, coalesce(current_state, '<none>'), coalesce(NEW.from_state, '<none>');
    END IF;
    IF NEW.to_state = 'completed' AND NEW.actor <> 'supervisor' THEN
        RAISE EXCEPTION 'completing a trial is a supervisor action, not a % one', NEW.actor;
    END IF;
    IF current_state IS NULL THEN
        IF NEW.to_state <> 'active' THEN
            RAISE EXCEPTION 'the genesis event must be <none> -> active, not -> %', NEW.to_state;
        END IF;
    ELSIF current_state = 'active' THEN
        IF NEW.to_state = 'active' THEN
            RAISE EXCEPTION 'illegal transition active -> %', NEW.to_state;
        END IF;
    ELSIF current_state IN ('halted_mandate', 'halted_operator') THEN
        IF NEW.to_state NOT IN ('active', 'completed') THEN
            RAISE EXCEPTION 'illegal transition % -> %', current_state, NEW.to_state;
        END IF;
        IF NEW.actor <> 'supervisor' THEN
            RAISE EXCEPTION 'resuming from % is a supervisor action, not a % one', current_state, NEW.actor;
        END IF;
    ELSE
        RAISE EXCEPTION '% is terminal', current_state;
    END IF;
    RETURN NEW;
END $$;
