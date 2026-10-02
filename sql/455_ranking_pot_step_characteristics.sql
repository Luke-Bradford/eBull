-- 455_ranking_pot_step_characteristics.sql
--
-- #2842 slice 6c-ii-c-1 — ranking-pot-v1's exposures and beta (spec
-- docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md §9.4, "Exposures and beta" paragraph; r3-144).
-- Writer: `app/services/ranking_pot_step.py` (in the step job's one transaction).
--
--   ranking_pot_steps.characteristics — at an applied session, the characteristics table its decided snapshot fixes
--     ([id, sector, ln cap, beta, beta null reason, ATR%] per name, `ranking_pot_exposure.encode_table`); NULL exactly
--     when no attempt is applied. Every later step reads the latest one until the next applied target.
--     No step row exists on any database (no declaration has been frozen), so the CHECK holds for every row.

ALTER TABLE ranking_pot_steps
    ADD COLUMN IF NOT EXISTS characteristics JSONB
        CHECK (characteristics IS NULL OR jsonb_typeof(characteristics) = 'array');

ALTER TABLE ranking_pot_steps DROP CONSTRAINT IF EXISTS ranking_pot_steps_characteristics_applied;
ALTER TABLE ranking_pot_steps
    ADD CONSTRAINT ranking_pot_steps_characteristics_applied
        CHECK ((characteristics IS NULL) = (applied_attempt_id IS NULL));

COMMENT ON COLUMN ranking_pot_steps.characteristics IS
    '#2842 the exposures'' characteristics table fixed by the applied snapshot (sector, ln cap, beta, ATR%; '
    'spec §9.4); NULL exactly when no attempt is applied.';
