-- 464_ranking_pot_v2_reference_book.sql
--
-- #3592 slice 4a — ranking-pot-v2's v1-reference book (spec docs/proposals/execution/2026-10-03-3592-ranking-pot-v2.md
-- §4 "Step job": book K + 2, the v1.5 order on v2's own snapshots, with protective exits; its records live beside the
-- shadow's, never in the K-wide control arrays). Writer: `app/services/ranking_pot_v2_step.py`.
--
--   ranking_pot_steps.reference — the reference book's step for the session, in the shadow's document format.
--     Present exactly when the row's declaration carries v2's layout (`terms.book_count` = `terms.k_controls` + 3,
--     which sql/463 requires of `ranking-pot-v2` and refuses for every other id); NULL for every v1 row. A BEFORE
--     INSERT trigger holds that, so a v2 step can never be stored without its reference, nor a v1 step with one.

ALTER TABLE ranking_pot_steps
    ADD COLUMN IF NOT EXISTS reference JSONB CHECK (reference IS NULL OR jsonb_typeof(reference) = 'object');

COMMENT ON COLUMN ranking_pot_steps.reference IS
    '#3592 ranking-pot-v2''s v1-reference book (K + 2): the v1.5 order on v2''s snapshots with protective exits; '
    'the shadow''s document format. NULL exactly for a declaration without v2''s K + 3 layout.';

CREATE OR REPLACE FUNCTION ranking_pot_steps_reference_layout()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    v2_layout BOOLEAN;
BEGIN
    SELECT coalesce((doc -> 'terms' ->> 'book_count')::bigint = (doc -> 'terms' ->> 'k_controls')::bigint + 3, FALSE)
      INTO v2_layout
    FROM ranking_pot_declarations WHERE declaration_id = NEW.declaration_id;
    IF v2_layout IS NULL THEN
        RAISE EXCEPTION 'ranking_pot_steps: declaration % is not visible', NEW.declaration_id;
    END IF;
    IF v2_layout AND NEW.reference IS NULL THEN
        RAISE EXCEPTION 'reference_missing: declaration % keeps a v1-reference book, and its step % has none',
            NEW.declaration_id, NEW.session;
    END IF;
    IF NOT v2_layout AND NEW.reference IS NOT NULL THEN
        RAISE EXCEPTION 'reference_unexpected: declaration % keeps no v1-reference book (step %)',
            NEW.declaration_id, NEW.session;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ranking_pot_steps_reference_layout ON ranking_pot_steps;
CREATE TRIGGER trg_ranking_pot_steps_reference_layout
BEFORE INSERT ON ranking_pot_steps
FOR EACH ROW EXECUTE FUNCTION ranking_pot_steps_reference_layout();
