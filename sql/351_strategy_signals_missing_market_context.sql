-- 351_strategy_signals_missing_market_context.sql
--
-- #2437 — S-6 lands the first REGIME-GATED strategy, and with it the eleventh
-- `not_evaluable` reason code.
--
-- WHAT IT NAMES
--   `missing_market_context` — the strategy gates on a benchmark's market regime
--   (S-5…S-10 §1: SPY vs its 200-SMA, plus Bollinger's six-month Bulge) and the
--   benchmark has no classifiable session on this bar's date.
--
-- ⚠ IT IS THE FIRST CODE THAT IS A PROPERTY OF A DIFFERENT INSTRUMENT than the
-- one being judged. Every one of the ten before it describes the row's own bar:
-- its volume is missing, its window is warming, it is quarantined, its series
-- broke, it has no t+1. This one fires when the row's bar is PERFECT and the
-- strategy still cannot decide, because SPY has no bar that day.
--
-- ⚠ IT IS NOT `insufficient_warmup`, AND THE SPLIT WAS MEASURED FIRST. Over all
-- 3,650,325 loadable bars in the validated universe:
--
--     regime known                        3,112,351   85.26%
--     before the benchmark's first
--       classifiable bar (real warm-up)     528,569   14.48%
--     benchmark has no session at all         9,405    0.26%   over 353 dates
--
-- The third class is not empty, which is the whole argument for giving it a
-- code. Its worst single date is 2026-02-06: 1,735 instruments traded and SPY
-- has no bar, so an entire session of the universe would otherwise be reported
-- as "the series had not started yet". That is criterion 8's exact prohibition —
-- *"these have different bias implications and collapsing them loses the ability
-- to tell a data gap from a real absence"*. Warm-up ends; a hole does not.
--
-- ⚠ THE INCIDENTAL FINDING, RECORDED WHERE IT WILL BE READ: those 353 dates are
-- holes in OUR STORED SPY SERIES, not market holidays. Most are thin
-- weekend/holiday rows contributed by `.24-7` instruments, but a minority are
-- ordinary sessions (2023-06-30 — a Friday — has 237 instruments trading and no
-- SPY bar). SPY is in `scheduler.BENCHMARK_SYMBOLS` so it IS refreshed; the gaps
-- predate that scope or were dropped by a failed fetch. Every regime-gated
-- strategy is blind on those days, honestly and countably.
--
-- ⚠ OURS, NOT THE PARENT'S. Criterion 8 lists seven codes; `no_fill_bar` was our
-- eighth, `thin_cross_section` the ninth, `unusable_fill_price` the tenth, and
-- this is the eleventh. All four are flagged as additions in
-- strategy_registry.py rather than passed off as the parent's, and
-- `OUR_ADDITIONAL_REASON_CODES` keeps the two sets separable in Python.
--
-- ⚠ THE PYTHON LITERAL IS THE SOURCE, THESE ARE THE MIRRORS — unchanged from
-- sql/270. tests/test_strategy_registry.py reads the LATEST migration that
-- redefines each list, which is this file from here on. THREE tables carry the
-- vocabulary (sql/270 widened one, sql/276 created two more) and all three are
-- widened here: the prevention log's "a closed vocabulary declared in three
-- places is validated in none of them" is precisely this shape, and a member
-- added to two of three writes rows the third refuses at insert time.

ALTER TABLE strategy_signals
    DROP CONSTRAINT IF EXISTS strategy_signals_reason_codes;

ALTER TABLE strategy_signals
    ADD CONSTRAINT strategy_signals_reason_codes
    CHECK (not_evaluable_reason IS NULL OR not_evaluable_reason IN (
        'missing_volume', 'missing_spread', 'insufficient_warmup',
        'quarantined_bar', 'series_break', 'not_listed',
        'ambiguous_intrabar', 'no_fill_bar', 'thin_cross_section',
        'unusable_fill_price', 'missing_market_context'
    ));

ALTER TABLE strategy_signal_daily_counts
    DROP CONSTRAINT IF EXISTS strategy_signal_daily_counts_reason_code_check;

ALTER TABLE strategy_signal_daily_counts
    ADD CONSTRAINT strategy_signal_daily_counts_reason_code_check
    CHECK (reason_code IN (
        '', 'missing_volume', 'missing_spread', 'insufficient_warmup',
        'quarantined_bar', 'series_break', 'not_listed',
        'ambiguous_intrabar', 'no_fill_bar', 'thin_cross_section',
        'unusable_fill_price', 'missing_market_context'
    ));

-- ⚠ `strategy_signal_observations` is RANGE-PARTITIONED. The CHECK is declared
-- on the parent, so replacing it there covers every existing and future
-- partition — a per-partition loop would leave whichever partition is created
-- next still refusing the new code.
ALTER TABLE strategy_signal_observations
    DROP CONSTRAINT IF EXISTS strategy_signal_observations_reason_code_check;

ALTER TABLE strategy_signal_observations
    ADD CONSTRAINT strategy_signal_observations_reason_code_check
    CHECK (reason_code IN (
        '', 'missing_volume', 'missing_spread', 'insufficient_warmup',
        'quarantined_bar', 'series_break', 'not_listed',
        'ambiguous_intrabar', 'no_fill_bar', 'thin_cross_section',
        'unusable_fill_price', 'missing_market_context'
    ));

COMMENT ON COLUMN strategy_signals.not_evaluable_reason IS
    'Closed vocabulary: parent criterion 8''s seven codes, plus no_fill_bar '
    '(the series has no t+1), thin_cross_section (the ranked panel was smaller '
    'than the ranking rule is defined on), unusable_fill_price (bar t+1 exists '
    'and its open is NULL or <= 0, so no fill can be priced) and '
    'missing_market_context (the strategy is gated on a benchmark''s market '
    'regime and the benchmark has no classifiable session on this date). The '
    'Python Literal in strategy_registry.NotEvaluableReason is the source; this '
    'CHECK mirrors it.';
