-- 267_strategy_results_quarantine_arm.sql
--
-- Phase 5e-5a — criterion 9's sensitivity arm, given a place on the result
-- identity so its result cannot be mistaken for the shipped one.
-- Spec: docs/proposals/ta/2026-08-07-bounded-backtester.md §8 (stage 5e-5a),
-- acceptance C9 and C11. Parent:
-- docs/proposals/ta/strategy-catalogue-and-backtest-validity.md criterion 9.
-- Producers: app/services/research_price_structure_store.py (the arm),
-- app/services/quarantine_sensitivity.py (the census + delta).
--
--
-- ⚠⚠ WITHOUT THIS COLUMN THE TWO ARMS COLLIDE, SILENTLY.
-- ---------------------------------------------------------------------------
-- `result_version` is a hash over the result identity, and until now that
-- identity carried the quarantine RULE SET (`input_rule_set_version`) but not
-- which HANDLING was applied under it. Criterion 9's arm re-runs the same
-- strategy, over the same corpus, at the same rule-set version, with the
-- flagged fields admitted instead of masked — every hashed field identical.
-- The two results would land on `strategy_results_unique (strategy_id,
-- strategy_version, result_version)` as one row, and the second write would
-- either fail as a duplicate or replace a number it is not comparable with.
--
-- ⚠ NOT hashed into `strategy_version`. Criterion 11's identity covers the
-- strategy's own rule; this is a property of how a RESULT was measured, which
-- is the same distinction that puts `input_rule_set_version` on this table
-- rather than in the strategy hash. A masked and an admitted run are the same
-- strategy measured two ways, not two strategies.
--
--
-- ⚠ NOT NULL WITH NO DEFAULT, and the table is empty (0 rows, measured
-- 2026-08-07) so it costs nothing today. sql/262's `universe_basis` header
-- gives the reason in full: "a default would let a writer forget, and the
-- forgotten value would be the favourable one". Here the forgotten value would
-- be 'masked' — the arm whose numbers are the ones anybody quotes.
--
--
-- ⚠⚠ THE TARGET IS `strategy_results_store`, AND THE VIEW MUST BE RE-CREATED.
-- ---------------------------------------------------------------------------
-- sql/264's `strategy_results` is a VIEW whose `SELECT *` was expanded at
-- creation time, so a column added to the store does not appear in it. The
-- CREATE OR REPLACE at the foot is load-bearing; the store-vs-view column
-- parity test in tests/test_strategy_holdout_namespace.py catches a future
-- migration that forgets it.

ALTER TABLE strategy_results_store
    ADD COLUMN IF NOT EXISTS quarantine_arm TEXT NOT NULL;

DO $$
BEGIN
    -- Criterion 9's declared pair, and only that pair. ⚠ There is no third
    -- "drop the whole bar" arm: C9 asks what the exclusion COST, and the only
    -- handling whose delta answers that is the one that stops excluding.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_quarantine_arm_known') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_quarantine_arm_known
            CHECK (quarantine_arm IN ('masked', 'admitted'));
    END IF;
END $$;

COMMENT ON COLUMN strategy_results_store.quarantine_arm IS
    'Criterion 9''s handling of quarantined bars. `masked` is the shipped read '
    '— high/low dropped on a range verdict, close dropped on a return verdict. '
    '`admitted` is the sensitivity arm: the same bars with those fields left at '
    'their STORED values, run so the exclusion is visible rather than assumed '
    'harmless. ⚠ Hashed into result_version — without it both arms hash '
    'identically and the second overwrites the first. ⚠ An `admitted` row is a '
    'measurement of what masking cost and is NEVER the number to quote.';

-- ---------------------------------------------------------------------------
-- ⚠ Re-created because `SELECT *` was expanded at sql/264's creation time and
-- would otherwise not carry the column added above. See the header.
CREATE OR REPLACE VIEW strategy_results AS
    SELECT *
    FROM strategy_results_store
    WHERE namespace = 'in_sample';

ALTER VIEW strategy_results SET (check_option = 'cascaded');
