-- 269_strategy_result_folds.sql
--
-- Phase 5e-5c — the per-fold walk-forward rows stage 5e-4 deliberately left
-- unwritten.
-- Spec: docs/proposals/ta/2026-08-07-bounded-backtester.md §5.3 (purge and
-- embargo), §8 (stage 5e-5c), acceptance C5. Parent:
-- docs/proposals/ta/strategy-catalogue-and-backtest-validity.md criterion 5 and
-- §2.2. Producer: app/services/walk_forward.py. Writer:
-- app/services/result_ledger.py.
--
--
-- ⚠⚠ WHY THE COLUMNS LAND NOW AND NOT AT 5e-4
-- ---------------------------------------------------------------------------
-- §5.3, verbatim: "Nothing is STORED by 5e-4 and that is deliberate. […]
-- Adding nullable walk-forward columns nobody populates is precisely the defect
-- sql/266's own header records. The columns land with the writer, in 5e-5."
-- This is that migration, and it ships WITH its writer and its full-population
-- run rather than ahead of them.
--
--
-- ⚠⚠ A CHILD TABLE, NOT COLUMNS ON THE RESULT ROW
-- ---------------------------------------------------------------------------
-- The grain is (result, fold) and FOLD_COUNT is 4, so four columns per field
-- on `strategy_results_store` would encode the fold count in the SCHEMA — and
-- `walk_forward.FOLD_COUNT`'s own comment is that a fold count which can be
-- passed in is a fold count that can be swept. Encoding it in column names
-- would make changing it a migration instead of a model-id bump, which is
-- exactly backwards: the count is frozen behind WALK_FORWARD_MODEL_ID, and a
-- future construction with a different count must be storable beside the old
-- one rather than in place of it.
--
--
-- ⚠⚠ NO PER-FOLD METRIC COLUMN, AND THAT IS A DECISION
-- ---------------------------------------------------------------------------
-- §5.3: "These strategies fit no parameters, so the split is a validity gate
-- rather than a training loop." What a fold row records is therefore the GATE
-- HAVING RUN — its geometry, its measured embargo and its census — not a
-- performance number. A per-fold Sharpe would be a set of four numbers whose
-- only natural use is picking the best one, which is a search over folds and is
-- what criterion 6's trial count exists to bound. Nothing in the spec says what
-- a per-fold return would be compared against, so there is no rule that could
-- be cited for storing one.
--
--
-- ⚠⚠ THE FOLDS BELONG TO AN IN-SAMPLE RESULT, ENFORCED BY TRIGGER
-- ---------------------------------------------------------------------------
-- Every fold is cut INSIDE the in-sample side — `walk_forward`'s header: "the
-- hold-out is not an input to any function in this module and never becomes
-- one". A fold row hanging off a `hold_out` result would therefore claim a
-- cross-validation of the withheld side that nobody ran, and criterion 5 is the
-- criterion that class of claim damages.
--
-- A CHECK cannot see the parent's namespace and the FK must name a table (the
-- `strategy_results` view is not a valid FK target), so the invariant is a
-- trigger — the same construction, and for the same reason, as sql/264's
-- access-record trigger: "a convention in Python is not that; a trigger is."
--
--
-- ⚠ WHAT IS NOT ADDED: A PROMOTION REFUSAL
-- ---------------------------------------------------------------------------
-- `strategy_result.check_promotable`'s vocabulary is sourced clause by clause
-- from §6 and §3.4, and neither declares a walk-forward bullet — C5's stored
-- requirements are the namespace, the access records and the frozen boundary,
-- all of which already have refusals. A `walk_forward_not_run` code invented
-- here would be a gate semantic with no source rule behind it, which is the
-- made-up-constant defect one layer up from a threshold. The rows are evidence;
-- what gates on them is `scripts/verify_2240_walk_forward.py`'s exit code,
-- which is where C5's purge/embargo assertions already live.
--
-- Measured 2026-08-08 before this migration: `strategy_results_store` holds 0
-- rows, so the FK, the trigger and every CHECK cost nothing today.

CREATE TABLE IF NOT EXISTS strategy_result_folds (
    -- ⚠ CASCADE. A fold row describes ONE result's split and is meaningless
    -- without it; an orphan would be a stored cross-validation of nothing.
    result_id             BIGINT NOT NULL
        REFERENCES strategy_results_store (result_id) ON DELETE CASCADE,

    -- Position in the split, 0-based, matching `walk_forward.Fold.index`.
    fold_index            INTEGER NOT NULL,

    -- ⚠ Stored per ROW rather than per result: it is what the numbers beside it
    -- MEAN, and a split written under a superseded construction must stay
    -- readable as that construction rather than inherit today's label. Same
    -- argument as `bootstrap_model_id` on sql/265.
    walk_forward_model_id TEXT NOT NULL,
    fold_count            INTEGER NOT NULL,

    -- Geometry on the in-sample panel axis, both ends INCLUSIVE — the
    -- convention `walk_forward.Fold` and `position_builder.Window` share.
    first_index           INTEGER NOT NULL,
    last_index            INTEGER NOT NULL,

    -- ⚠ The same block in DATES. An index is a position on an axis that is a
    -- property of the corpus at split time, so an index alone is unreadable
    -- once the corpus moves; §5.3's own fold table is written in dates. Both are
    -- stored and the writer refuses a pair whose orders disagree.
    first_date            DATE NOT NULL,
    last_date             DATE NOT NULL,

    -- §5.2's realised bar share, re-derivable from the stored rows.
    -- ⚠ `bar_weighted_folds`' non-empty-fold clamp is silent in the library and
    -- loud in the caller; a stored bar count is how it stays loud after the run
    -- that produced it has gone.
    bar_count             BIGINT NOT NULL,

    -- §5.3's MEASURED embargo, in PANEL bars — the maximum label-window span
    -- among this fold's post-purge, pre-embargo training observations.
    -- ⚠ 0 is legal and means "nothing to measure on this fold's training side",
    -- not "no embargo applied". `walk_forward.role`'s header says why refusing
    -- it would force a caller to invent a number instead.
    embargo_bars          INTEGER NOT NULL,

    -- The four verdicts, counted. ⚠ `purged` and `embargoed` are SEPARATE
    -- columns and not one `dropped` total: they are different leaks of
    -- different sizes, and §5.3's finding — that the embargo removes far less
    -- than the purge — is unreportable from a collapsed count.
    -- ⚠ BIGINT: S-1 contributes 2,456,097 in-sample observations per fold today
    -- and the corpus grows with every harvest.
    test_count            BIGINT NOT NULL,
    train_count           BIGINT NOT NULL,
    purged_count          BIGINT NOT NULL,
    embargoed_count       BIGINT NOT NULL,

    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (result_id, fold_index),

    CONSTRAINT strategy_result_folds_index_inside_split
        CHECK (fold_index >= 0 AND fold_index < fold_count),

    -- One fold is not a cross-validation — `bar_weighted_folds` refuses the
    -- same thing at its own boundary.
    CONSTRAINT strategy_result_folds_count_sane
        CHECK (fold_count >= 2),

    CONSTRAINT strategy_result_folds_block_ordered
        CHECK (first_index >= 0 AND last_index >= first_index AND last_date >= first_date),

    CONSTRAINT strategy_result_folds_counts_non_negative
        CHECK (
            bar_count >= 0
            AND embargo_bars >= 0
            AND test_count >= 0
            AND train_count >= 0
            AND purged_count >= 0
            AND embargoed_count >= 0
        ),

    -- ⚠ The #2286 shape: NOT NULL admits a PRESENT-but-empty value, and a blank
    -- model id is a split whose construction is undeclared while looking
    -- declared. sql/262, sql/264 and sql/256 all make this check.
    CONSTRAINT strategy_result_folds_model_id_non_empty
        CHECK (walk_forward_model_id <> '')

    -- ⚠ NO CHECK requires `test_count > 0`. A fold covering a thin era can
    -- legitimately contain no observation that STARTS inside it — the clamp
    -- guarantees a non-empty DATE block, never a non-empty observation set —
    -- and refusing that would refuse a true measurement of a lumpy axis.
);

-- The phase-6 read, and the writer's own read-back: one result's whole split in
-- fold order.
CREATE INDEX IF NOT EXISTS idx_strategy_result_folds_result
    ON strategy_result_folds (result_id, fold_index);

COMMENT ON TABLE strategy_result_folds IS
    'Criterion 5''s purged walk-forward, per fold, for one result row: the '
    'in-sample block, its MEASURED panel-axis embargo and the four-way census '
    '(test / train / purged / embargoed). ⚠ Written only for an in_sample '
    'result — every fold is cut inside the in-sample side, so a fold row on a '
    'hold_out result would claim a cross-validation of the withheld side that '
    'nobody ran; a trigger refuses it. ⚠ No per-fold METRIC, deliberately: §5.3 '
    'makes the split a validity gate rather than a training loop, and four '
    'per-fold Sharpes exist mainly to be picked between.';

COMMENT ON COLUMN strategy_result_folds.embargo_bars IS
    'The embargo for this fold in PANEL bars, measured off its own post-purge '
    'training side (walk_forward.training_embargo_bars). ⚠ NOT a declared '
    'max_hold_bars: that constant counts an INSTRUMENT''s bars and a fold '
    'window counts panel dates, which under-covers in the direction that leaks '
    '(§5.3, §8.4 — measured, 3 of 2,456,097 S-1 positions, by up to 374 '
    'dates). ⚠ 0 means nothing was measurable on this fold''s training side.';

COMMENT ON COLUMN strategy_result_folds.purged_count IS
    'Observations dropped because their label window OVERLAPS the fold (AFML '
    'ch. 7''s purge). ⚠ Kept apart from embargoed_count: the two are different '
    'leaks of very different sizes, and collapsing them makes §5.3''s finding '
    'unreportable.';


-- ---------------------------------------------------------------------------
-- The trigger: folds belong to an in-sample result
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION strategy_result_folds_require_in_sample()
RETURNS TRIGGER AS $$
DECLARE
    parent_namespace TEXT;
BEGIN
    SELECT namespace INTO parent_namespace
    FROM strategy_results_store
    WHERE result_id = NEW.result_id;

    IF parent_namespace IS DISTINCT FROM 'in_sample' THEN
        RAISE EXCEPTION
            'result % is namespace % — walk-forward folds are cut inside the IN-SAMPLE side (spec §5.3), so a fold '
            'row here would claim a cross-validation of the withheld data that never ran',
            NEW.result_id, coalesce(parent_namespace, 'missing')
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_strategy_result_folds_in_sample ON strategy_result_folds;

-- ⚠ INSERT OR UPDATE, for sql/264's reason one table down: an UPDATE moving
-- `result_id` onto a hold-out result is the same unrecorded claim reached in
-- two statements, and a BEFORE INSERT trigger is blind to it.
CREATE TRIGGER trg_strategy_result_folds_in_sample
    BEFORE INSERT OR UPDATE ON strategy_result_folds
    FOR EACH ROW
    EXECUTE FUNCTION strategy_result_folds_require_in_sample();
