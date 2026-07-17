-- 232_thesis_writer_native_break_predicates.sql
--
-- #2010 — thesis writer prompt v5: writer-native break predicates.
-- Spec: docs/proposals/thesis/2026-07-16-thesis-prompt-v5-valuation-scaffold.md.
--
-- theses.break_predicates_json — the writer's OWN structured twins of its
--   prose break_conditions ([{condition_index, metric, op, threshold}, ...]).
--   Soft-validated at INSERT (invalid entries dropped, never a retry-fail);
--   only validated survivors are stored. Prose stays canonical.
-- thesis_break_predicates.origin — provenance of the scan's predicate row:
--   'extractor' (the PR-A precision channel) or 'writer' (v5 recall channel).
--   The writer channel is purely ADDITIVE: it fills only indexes the
--   extractor returned None for, and can never override the extractor.

BEGIN;

ALTER TABLE theses
    ADD COLUMN IF NOT EXISTS break_predicates_json JSONB;

ALTER TABLE thesis_break_predicates
    ADD COLUMN IF NOT EXISTS origin TEXT NOT NULL DEFAULT 'extractor'
        CHECK (origin IN ('extractor', 'writer'));

COMMIT;
