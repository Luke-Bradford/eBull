-- 452_ranking_pot_executing_activated.sql
--
-- #2842 slice 5c-ii-b — ranking-pot-v1's activation (spec docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md
-- §7.3, "The activation script and precheck").
--
-- `→ executing` is refused while the declaration has no `ranking_pot_activations` row. The activation script
-- (`app/services/ranking_pot_activation.py`) writes the row, the pot's deployment and policies and the event in one
-- transaction; this makes "no executing pot without its POT_CAPITAL" binding on every writer. Resumption from a
-- halt always finds the row (a halt is reachable only from `executing`). A second BEFORE INSERT trigger, so
-- sql/448's transition function is not restated (as sql/451). The row is append-only, so reading it unlocked is
-- exact.

CREATE OR REPLACE FUNCTION ranking_pot_executing_activated()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.to_state = 'executing'
       AND NOT EXISTS (SELECT 1 FROM ranking_pot_activations a WHERE a.declaration_id = NEW.declaration_id) THEN
        RAISE EXCEPTION 'not_activated: ranking pot % has no ranking_pot_activations row', NEW.declaration_id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ranking_pot_executing_activated ON ranking_pot_state_events;
CREATE TRIGGER trg_ranking_pot_executing_activated
BEFORE INSERT ON ranking_pot_state_events
FOR EACH ROW EXECUTE FUNCTION ranking_pot_executing_activated();
