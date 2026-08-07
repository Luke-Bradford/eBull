-- 260_strategy_signals_thin_cross_section.sql
--
-- #2240 S-2 — the ninth `not_evaluable` reason code.
-- Registry: app/services/strategy_registry.py (NotEvaluableReason,
-- evaluate_cross_sectional). Table: sql/255_strategy_signals.sql.
-- Spec: docs/proposals/ta/2026-08-06-cross-sectional-contract-and-s2.md.
--
--
-- WHY A NEW CODE RATHER THAN REUSING ONE
-- ---------------------------------------------------------------------------
-- Every code in sql/255 describes a property of the BAR — a missing field, a
-- quarantined bar, a series break, the end of the series. `thin_cross_section`
-- is the first that is a property of the PANEL: S-2 ranks an instrument against
-- its peers on a rebalance date and holds the top decile, and a cross-section
-- of six names has no decile for anyone to be in the top of. The bar is fine;
-- the comparison is not defined.
--
-- The two alternatives were considered and are worse:
--
--   * round the decile up (`k = max(1, N // 10)`) — silently redefines "top
--     decile" as "best of six" at exactly the dates where the population is
--     thinnest, i.e. the early corpus;
--   * report `not_fired` (`k = N // 10 = 0`, so nobody is in the top decile) —
--     this is parent criterion 8's exact prohibition. It makes a data
--     availability fact indistinguishable from a rule verdict, and criterion 9
--     then cannot count what was rejected.
--
-- ⚠ OURS, NOT THE PARENT'S. Criterion 8 lists seven codes; `no_fill_bar` was
-- our eighth and this is our ninth. Both are flagged as additions in
-- strategy_registry.py rather than passed off as the parent's, and
-- `OUR_ADDITIONAL_REASON_CODES` keeps the two sets separable in Python.
--
-- ⚠ THE PYTHON LITERAL IS THE SOURCE, THIS IS THE MIRROR. The vocabulary lives
-- in `NotEvaluableReason` and every Python set is derived from it via
-- `get_args`. tests/test_strategy_registry.py reads the LATEST migration that
-- redefines this list — sql/260 from here on, not sql/255 — so the two cannot
-- drift.
--
-- Renamed while widening: the 255 constraint was inline and therefore
-- auto-named `strategy_signals_not_evaluable_reason_check`. The replacement is
-- named explicitly, so the next widening drops a name that was chosen rather
-- than one that was generated.

ALTER TABLE strategy_signals
    DROP CONSTRAINT IF EXISTS strategy_signals_not_evaluable_reason_check;

ALTER TABLE strategy_signals
    DROP CONSTRAINT IF EXISTS strategy_signals_reason_codes;

ALTER TABLE strategy_signals
    ADD CONSTRAINT strategy_signals_reason_codes
    CHECK (not_evaluable_reason IS NULL OR not_evaluable_reason IN (
        'missing_volume', 'missing_spread', 'insufficient_warmup',
        'quarantined_bar', 'series_break', 'not_listed',
        'ambiguous_intrabar', 'no_fill_bar', 'thin_cross_section'
    ));

COMMENT ON COLUMN strategy_signals.not_evaluable_reason IS
    'Closed vocabulary: parent criterion 8''s seven codes, plus no_fill_bar '
    '(the series has no t+1) and thin_cross_section (the ranked panel was '
    'smaller than the ranking rule is defined on). The Python Literal in '
    'strategy_registry.NotEvaluableReason is the source; this CHECK mirrors it.';
