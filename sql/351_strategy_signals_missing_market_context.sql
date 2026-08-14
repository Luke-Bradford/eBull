-- 351_strategy_signals_missing_market_context.sql
--
-- #2437 — the eleventh `not_evaluable` reason code. Registry:
-- app/services/strategy_registry.py (NotEvaluableReason). Producer:
-- app/services/market_regime_provider.py (MarketRegimeProvider.for_dates).
-- Tables: sql/255 (widened by sql/260, sql/270) and sql/276.
--
--
-- WHAT WAS BEING RECORDED WRONGLY
-- ---------------------------------------------------------------------------
-- S-5, S-6 and S-9 gate on a market regime classified from the benchmark (SPY).
-- The regime was checked INSIDE each strategy body rather than declared as a
-- `StrategyInput`, so `strategy_registry.evaluate` never got the chance to
-- refuse the bar. A date on which the instrument traded and the benchmark did
-- not therefore produced `regime.permits(...) == False`, and was stored as
-- `not_fired`.
--
-- That is parent criterion 8's stated prohibition — a data-availability fact
-- wearing a rule verdict's clothes — and it silently shrinks the `not_fired`
-- denominator of every regime-gated strategy.
--
-- Full validated universe, dev DB 2026-08-14 (6,774 instruments):
--
--     WITH bench AS (
--       SELECT p.price_date FROM price_daily p
--       JOIN instruments i ON i.instrument_id = p.instrument_id
--       WHERE i.symbol = 'SPY' AND p.close IS NOT NULL
--     )
--     SELECT count(*) AS bars, count(DISTINCT p.price_date) AS dates
--     FROM price_daily p
--     WHERE p.instrument_id = ANY(<validated universe>) AND p.close IS NOT NULL
--       AND p.price_date >= (SELECT min(price_date) FROM bench)
--       AND NOT EXISTS (SELECT 1 FROM bench b WHERE b.price_date = p.price_date);
--     -- 9,688 | 360
--
-- Worst single date is 2026-02-06: 1,735 instruments traded and SPY has no bar.
-- These are holes in our stored benchmark series rather than market holidays —
-- most are thin weekend/holiday rows contributed by `.24-7` instruments, but a
-- minority are ordinary sessions (2023-06-30, a Friday, has 237 instruments
-- trading and no SPY bar).
--
--
-- WHY NOT AN EXISTING CODE
-- ---------------------------------------------------------------------------
-- This is the first code that is a property of a DIFFERENT INSTRUMENT than the
-- one being judged, so none of the ten fits:
--
--   * `insufficient_warmup` is the benchmark's OTHER failure and stays separate.
--     A benchmark bar that EXISTS but is not yet classifiable (the 200-SMA or
--     the 126-bar BandWidth window still filling) is warm-up, and `evaluate`
--     already derives that structurally from a bare `None`. Only an ABSENT
--     benchmark observation earns the new code. `MarketRegimeProvider.for_dates`
--     is the only place that can tell the two apart — by the time a strategy
--     holds the `RegimeSeries` both are `None`.
--   * `thin_cross_section` is the nearest relative and is still wrong: it
--     describes a ranked panel that EXISTS and is too small, not a series that
--     is absent.
--   * `series_break` / `not_listed` are statements about the instrument's own
--     series, which here is intact — it is the market context that is missing.
--
-- ⚠ A regime that IS classifiable and simply is not one the strategy permits
-- stays `not_fired`. That bar WAS judged, and the parent spec's §0 rule 2 makes
-- firing outside a declared domain the defect, not evidence the rule is broken.
--
-- ⚠ OURS, NOT THE PARENT'S. Criterion 8 lists seven codes; `no_fill_bar` was
-- the eighth, `thin_cross_section` the ninth, `unusable_fill_price` the tenth,
-- and this is the eleventh. All four are flagged as additions in
-- strategy_registry.py rather than passed off as the parent's, and
-- `OUR_ADDITIONAL_REASON_CODES` keeps the two sets separable in Python.
--
-- ⚠ THE PYTHON LITERAL IS THE SOURCE, THIS IS THE MIRROR — unchanged from
-- sql/260 and sql/270. tests/test_strategy_registry.py reads the LATEST
-- migration that redefines each list, which is this file from here on.
--
--
-- WHAT THIS DOES TO STORED ROWS
-- ---------------------------------------------------------------------------
-- Declaring the regime as an input moves `REGIME_RULE_VERSION` (it hashes
-- market_regime.py's source) and therefore all three strategy identities. What
-- is currently stored under the OLD versions, dev DB 2026-08-14:
--
--     select strategy_id, verdict, count(*) from strategy_signals
--     where strategy_id in ('s5-support-bounce','s6-resistance-breakout','s9-squeeze-expansion')
--     group by 1,2;
--     -- s5-support-bounce | fired | 316
--     -- s6-resistance-breakout | fired | 36
--     -- s9-squeeze-expansion | fired | 3
--
-- 355 rows, all `fired`, all from the single scan of 2026-08-12, and NONE of
-- them is referenced by a `strategy_outcomes` row (measured: 0). They are left
-- in place under their old `strategy_version`, which is this repo's standing
-- trade — visibly stale beats silently mixed — and the next scan re-emits under
-- the new versions.
--
-- ⚠ An earlier note on #2437 said all three had "0 stored strategy_signals
-- rows, so nothing is lost". That was wrong on the count and right on the
-- consequence; the figures above are the measured replacement.

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

COMMENT ON COLUMN strategy_signals.not_evaluable_reason IS
    'Closed vocabulary: parent criterion 8''s seven codes, plus no_fill_bar '
    '(the series has no t+1), thin_cross_section (the ranked panel was smaller '
    'than the ranking rule is defined on), unusable_fill_price (bar t+1 '
    'exists and its open is NULL or <= 0, so no fill can be priced) and '
    'missing_market_context (the benchmark the strategy''s regime gate reads '
    'contributed no bar on this date). The Python Literal in '
    'strategy_registry.NotEvaluableReason is the source; this CHECK mirrors it.';

-- The same vocabulary, in the two sql/276 relations. ⚠ Both spell "no reason"
-- as the empty string so it can sit in a primary key, so the widened list keeps
-- '' as its first member; dropping it would reject every non-not_evaluable row.
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
