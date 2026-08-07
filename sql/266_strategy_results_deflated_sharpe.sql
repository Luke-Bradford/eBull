-- 266_strategy_results_deflated_sharpe.sql
--
-- Phase 5e-3 — criterion 6's Deflated Sharpe: the declared inputs that make the
-- stored number mean something, and the constraint that stops half of them.
-- Spec: docs/proposals/ta/2026-08-07-bounded-backtester.md §8 (stage 5e-3),
-- acceptance C6. Parent:
-- docs/proposals/ta/strategy-catalogue-and-backtest-validity.md criterion 6.
-- Producer: app/services/deflated_sharpe.py. Declaration:
-- app/services/trial_register.py. Source rule: Bailey & López de Prado (2014),
-- SSRN 2460551, equations (1), (2), (8), (9).
--
--
-- ⚠⚠ sql/262 SHIPPED TWO OF CRITERION 6'S COLUMNS AND NONE OF ITS INPUTS.
-- ---------------------------------------------------------------------------
-- `trial_count` and `deflated_sharpe` have existed (nullable, fail-closed)
-- since sql/262. A DSR is a function of six further declared quantities —
-- the trials' Sharpe variance, their average correlation, the implied
-- INDEPENDENT trial count, and the selected strategy's own per-trade Sharpe,
-- skew and kurtosis — and NONE of them is re-derivable from the row. Stored
-- without them, a `deflated_sharpe` is a number with no way back to what it
-- deflated against, which criterion 11 makes part of what it MEANS.
--
--
-- ⚠⚠ `dsr_trade_sharpe` IS NOT `sharpe`, AND THE TWO MUST NEVER BE CONFLATED.
-- ---------------------------------------------------------------------------
-- `sharpe` (sql/263) is criterion 7's: ANNUALISED, computed on the equity
-- curve's per-period returns. `dsr_trade_sharpe` is PER TRADE and not
-- annualised, because equation (2) requires its Sharpe, skew, kurtosis and
-- sample length to describe one and the same series — and the sample length it
-- is handed is `effective_sample_size`, which criterion 3 measures in TRADES.
-- Feeding the annualised per-period Sharpe into (2) beside a trade-count T
-- would inflate the numerator by sqrt(periods per year) and nothing downstream
-- could see it. Two columns, two names, one axis each.
--
--
-- ⚠⚠ THE TARGET IS `strategy_results_store`, AND THE VIEW MUST BE RE-CREATED.
-- ---------------------------------------------------------------------------
-- Same two traps sql/265's header records. `strategy_results` is a VIEW over
-- the store (sql/264, criterion 5); and its `SELECT *` was expanded at creation
-- time, so a column added to the store does NOT appear in the view. Every
-- in-sample read would silently lose these ten with no error anywhere. The
-- CREATE OR REPLACE at the foot is load-bearing; the store-vs-view column
-- parity test in tests/test_strategy_holdout_namespace.py is what catches a
-- future migration that forgets it.

ALTER TABLE strategy_results_store
    -- The selected strategy's half of equation (2). ⚠ Per TRADE — see above.
    ADD COLUMN IF NOT EXISTS dsr_trade_sharpe              NUMERIC,
    ADD COLUMN IF NOT EXISTS dsr_skewness                  NUMERIC,
    -- ⚠ RAW kurtosis (Normal = 3), not excess. Equation (2)'s `(y4 - 1) / 4`
    -- expects the +3 convention; storing excess would shrink the denominator
    -- and inflate every DSR by a silent constant.
    ADD COLUMN IF NOT EXISTS dsr_kurtosis                  NUMERIC,
    -- Equation (1) under H0 — the multiple-testing threshold being deflated
    -- against, in the same per-trade units as `dsr_trade_sharpe`.
    ADD COLUMN IF NOT EXISTS dsr_expected_max_sharpe       NUMERIC,
    -- Equation (9): N_hat = rho + (1 - rho) M. ⚠ NUMERIC and not INTEGER —
    -- (9) interpolates between 1 and M, and rounding it would move the
    -- rejection threshold by an amount nobody chose.
    ADD COLUMN IF NOT EXISTS dsr_independent_trials        NUMERIC,
    -- Equation (8).
    ADD COLUMN IF NOT EXISTS dsr_average_trial_correlation NUMERIC,
    -- V[{SR_n}] across the trials measured for this evaluation.
    ADD COLUMN IF NOT EXISTS dsr_trial_sharpe_variance     NUMERIC,
    -- How many of `trial_count` carried a measured Sharpe. ⚠ Stored because
    -- V[{SR_n}] is estimated from THESE and applied to ALL of them, and the
    -- gap is the size of that extrapolation.
    ADD COLUMN IF NOT EXISTS dsr_measured_trials           INTEGER,
    ADD COLUMN IF NOT EXISTS dsr_model_id                  TEXT,
    -- ⚠ The register version, not just the count. A DSR deflated against
    -- eleven declared trials is a different statement from one deflated
    -- against thirty, and the count alone does not say WHICH eleven.
    ADD COLUMN IF NOT EXISTS trial_register_version        TEXT;

DO $$
BEGIN
    -- ⚠⚠ ALL-OR-NOTHING OVER THE DSR AND ITS INPUTS — ELEVEN COLUMNS, so 0
    -- nulls or 11. A `deflated_sharpe` stored without the quantities it was
    -- deflated against is a number with no way back to what it corrected for,
    -- which criterion 11 makes part of what it MEANS. sql/262's "NULL until 5e
    -- computes it" is the all-absent case and existing rows satisfy it.
    --
    -- ⚠ `trial_count` IS DELIBERATELY NOT IN THIS SET. It is governed by the
    -- DEPENDENCY below instead, because the two states are not symmetric: a DSR
    -- without a declared count is forbidden by criterion 6, but a declared
    -- count with no DSR yet is a REAL state — the register exists and the
    -- evaluation has not run. `strategy_result.check_promotable` has a separate
    -- live refusal for each (`deflated_sharpe_not_computed` and
    -- `trial_count_undeclared`), and folding `trial_count` in here would make a
    -- row the gate is designed to describe unwritable.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_dsr_all_or_nothing') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_dsr_all_or_nothing
            CHECK (num_nulls(
                deflated_sharpe, dsr_trade_sharpe, dsr_skewness, dsr_kurtosis,
                dsr_expected_max_sharpe, dsr_independent_trials, dsr_average_trial_correlation,
                dsr_trial_sharpe_variance, dsr_measured_trials, dsr_model_id, trial_register_version
            ) IN (0, 11));
    END IF;

    -- ⚠ CRITERION 6'S ONE-WAY DEPENDENCY, stated as its own constraint so the
    -- asymmetry above is visible rather than implied: *"an undeclared trial
    -- count fails; it does not default to the number of shipped strategies"*.
    -- A DSR requires a count. A count does not require a DSR.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_dsr_needs_trial_count') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_dsr_needs_trial_count
            CHECK (deflated_sharpe IS NULL OR trial_count IS NOT NULL);
    END IF;

    -- The DSR is a PROBABILITY (equation 2 is a Normal CDF). A value outside
    -- [0, 1] is not a strong or weak result, it is an arithmetic failure.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_dsr_is_probability') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_dsr_is_probability
            CHECK (deflated_sharpe IS NULL OR (deflated_sharpe >= 0 AND deflated_sharpe <= 1));
    END IF;

    -- ⚠ M >= 2 WHENEVER A DSR IS PRESENT, which is stricter than sql/262's
    -- `trial_count >= 1`. Appendix A.3 requires M > 1 for an average
    -- correlation to exist at all, and equation (9) has no meaning below it.
    -- sql/262's floor stays as it is: a trial count may be declared on a row
    -- that has no DSR yet.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_dsr_trials_above_one') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_dsr_trials_above_one
            CHECK (dsr_independent_trials IS NULL
                   OR (trial_count >= 2
                       AND dsr_independent_trials > 1
                       AND dsr_independent_trials <= trial_count));
    END IF;

    -- ⚠ A.3's own bound: for a positive-definite MxM correlation matrix the
    -- average correlation lies in (-1/(M-1), 1]. Tighter than (-1, 1], and the
    -- tighter one is what catches a matrix that was never a correlation matrix.
    -- ⚠ `trial_count > 1` GUARDS THE DIVISION, and it is not redundant with
    -- `strategy_results_dsr_trials_above_one`. Postgres does not guarantee the
    -- order CHECK constraints are evaluated in, so at `trial_count = 1` this
    -- one can run first and raise DivisionByZero (SQLSTATE 22012, measured)
    -- instead of the intended check violation. The other constraint still
    -- REJECTS that row; this clause only decides which error the writer sees.
    -- ⚠ A test asserting "any exception" cannot tell the two apart, which is
    -- why the reject-case test pins the SQLSTATE.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_dsr_correlation_bounded') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_dsr_correlation_bounded
            CHECK (dsr_average_trial_correlation IS NULL
                   OR (trial_count > 1
                       AND dsr_average_trial_correlation > -1.0 / (trial_count - 1)
                       AND dsr_average_trial_correlation <= 1));
    END IF;

    -- V[{SR_n}] is a variance and equation (1) takes its square root. Zero
    -- means no distribution of trial Sharpes, which is a refusal upstream and
    -- must never reach a stored row.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_dsr_variance_positive') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_dsr_variance_positive
            CHECK (dsr_trial_sharpe_variance IS NULL OR dsr_trial_sharpe_variance > 0);
    END IF;

    -- ⚠ At least two measured trials (a sample variance needs two), and never
    -- more than were declared — a measured trial missing from the register is a
    -- trial missing from M, which is the under-count criterion 6 calls
    -- decorative.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_dsr_measured_within_declared') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_dsr_measured_within_declared
            CHECK (dsr_measured_trials IS NULL
                   OR (dsr_measured_trials >= 2 AND dsr_measured_trials <= trial_count));
    END IF;

    -- ⚠ THE BOUND IS 1, NOT 0, AND THE DIFFERENCE IS THE WHOLE POINT OF THE
    -- CHECK. For any real distribution `y4 >= y3^2 + 1 >= 1`, with equality at
    -- a two-point symmetric distribution — reachable here as a trade population
    -- where every win is one size and every loss another. A `> 0` bound would
    -- admit the whole of (0, 1) while the comment beside it claimed those
    -- values were impossible, and excess kurtosis for a near-Normal series
    -- lands in exactly that range.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_dsr_kurtosis_raw') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_dsr_kurtosis_raw
            CHECK (dsr_kurtosis IS NULL OR dsr_kurtosis >= 1);
    END IF;

    -- ⚠ A BLANK id is PRESENT and meaningless — the #2286 shape. It would
    -- satisfy the all-or-nothing constraint above while naming no construction
    -- and no trial population at all.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'strategy_results_dsr_ids_non_empty') THEN
        ALTER TABLE strategy_results_store ADD CONSTRAINT strategy_results_dsr_ids_non_empty
            CHECK ((dsr_model_id IS NULL OR dsr_model_id <> '')
                   AND (trial_register_version IS NULL OR trial_register_version <> ''));
    END IF;
END $$;

COMMENT ON COLUMN strategy_results_store.deflated_sharpe IS
    'Criterion 6''s Deflated Sharpe Ratio — Bailey & López de Prado (2014) eq. '
    '(2), SSRN 2460551. The probability the true Sharpe exceeds the '
    'multiple-testing threshold dsr_expected_max_sharpe. ⚠ Computed on the '
    'TRADE axis with T = effective_sample_size (criterion 3), never the nominal '
    'trade count.';

COMMENT ON COLUMN strategy_results_store.dsr_trade_sharpe IS
    'Per-trade Sharpe: mean(net return) / stdev(net return) over realised '
    'trades. ⚠ NOT the `sharpe` column, which is annualised and computed on the '
    'equity curve. Equation (2) requires its Sharpe, skew, kurtosis and sample '
    'length to describe ONE series, and the sample length it consumes is in '
    'units of trades.';

COMMENT ON COLUMN strategy_results_store.dsr_kurtosis IS
    'RAW fourth standardised moment (Normal = 3), NOT excess kurtosis. '
    'Equation (2)''s (y4 - 1)/4 term expects the +3 convention; storing excess '
    'would shrink the denominator and inflate every DSR by a silent constant.';

COMMENT ON COLUMN strategy_results_store.dsr_independent_trials IS
    'Appendix A.3 eq. (9): N_hat = rho + (1 - rho) M, the trials that were '
    'INDEPENDENT. ⚠ Always <= trial_count — "using M instead of N will '
    'overstate E[max{SR_n}]". NUMERIC because (9) interpolates.';

COMMENT ON COLUMN strategy_results_store.trial_register_version IS
    'The app/services/trial_register.py declaration this DSR was deflated '
    'against. ⚠ A DSR against eleven declared trials is a different statement '
    'from one against thirty, and trial_count alone does not say WHICH. The '
    'register is a documented FLOOR: under-counting M raises the DSR, so a '
    'stored value is an upper bound on the honest one.';

-- ---------------------------------------------------------------------------
-- ⚠ Re-created because `SELECT *` was expanded at sql/264's creation time and
-- would otherwise not carry the ten columns added above. See the header.
CREATE OR REPLACE VIEW strategy_results AS
    SELECT *
    FROM strategy_results_store
    WHERE namespace = 'in_sample';

ALTER VIEW strategy_results SET (check_option = 'cascaded');
