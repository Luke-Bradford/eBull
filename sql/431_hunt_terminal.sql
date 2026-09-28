-- 431_hunt_terminal.sql
--
-- #3454 slice B — the durable terminal refusal of a gated hunt.
-- Spec: docs/proposals/ta/2026-09-28-3454-hunt-3-spec.md, "Lineage closure (terminal invariant)":
-- "A substantive refusal, whether at the freeze or at the look, is persisted as it happens.
-- Under hunt_programme_lock, the door writes a terminal row (hunt_id, reason, at)". Every later
-- freeze or `evaluate` for the hunt, or for any spec in its lineage, refuses `hunt_terminal`.
-- Writer: app/services/hunt_harness.py (`write_terminal`, called by the door and `evaluate`).
--
-- ⚠ APPEND-ONLY, BY TRIGGER (the sql/427 pattern): the row IS the closure until the reviewed
-- `HUNT_CLOSED` edit records it. A row that could be deleted would reopen a closed hunt by
-- hand, which is exactly the "retry after a bad look" the terminal rule exists to prevent.

CREATE TABLE IF NOT EXISTS hunt_terminal (
    hunt_terminal_id BIGSERIAL PRIMARY KEY,
    hunt_id          TEXT NOT NULL CHECK (hunt_id ~ '^hunt-[1-9][0-9]*$'),
    -- The lineage family the refusal also closes (`hunt_harness.LAST_LOOK_LINEAGES`); NULL
    -- when the refused hunt's specs belong to none.
    lineage          TEXT CHECK (lineage IS NULL OR lineage ~ '^[a-z][a-z0-9_]*$'),
    reason           TEXT NOT NULL CHECK (btrim(reason) <> ''),
    at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE hunt_terminal IS
    '#3454 terminal refusals of gated hunts, append-only: one row per substantive refusal at a '
    'validation freeze, look or readout. Writer: app/services/hunt_harness.py (write_terminal).';

CREATE INDEX IF NOT EXISTS hunt_terminal_hunt ON hunt_terminal (hunt_id);
CREATE INDEX IF NOT EXISTS hunt_terminal_lineage ON hunt_terminal (lineage) WHERE lineage IS NOT NULL;

-- `prevent_hunt_trial_mutation` is sql/427's; it names the table it fires on.
DROP TRIGGER IF EXISTS trg_hunt_terminal_append_only ON hunt_terminal;
CREATE TRIGGER trg_hunt_terminal_append_only
BEFORE UPDATE OR DELETE ON hunt_terminal
FOR EACH ROW EXECUTE FUNCTION prevent_hunt_trial_mutation();
