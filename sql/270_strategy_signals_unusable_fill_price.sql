-- 270_strategy_signals_unusable_fill_price.sql
--
-- #2354 — the tenth `not_evaluable` reason code, splitting `no_fill_bar`.
-- Registry: app/services/strategy_registry.py (NotEvaluableReason). Writer:
-- app/services/signal_ledger.py (resolve_fills). Table:
-- sql/255_strategy_signals.sql, widened by sql/260.
--
--
-- WHY A NEW CODE, AND WHY NOW
-- ---------------------------------------------------------------------------
-- `resolve_fills` collapsed two different facts into `no_fill_bar`, and its own
-- docstring recorded the collapse as conditional:
--
--     "Reusing `no_fill_bar` for it is a deliberate widening of a code whose
--      stated meaning is 'the series ended' … It is accepted here because the
--      alternative — a ninth reason code — needs the parent vocabulary reopened
--      and sql/255's CHECK widened for a case that has NEVER OCCURRED.
--      **If the measured count ever leaves zero, split it.**"
--
-- The measured count has left zero. Full population, dev DB 2026-08-08:
--
--     select count(*) filter (where open is null), count(*) filter (where open = 0),
--            count(*) filter (where open < 0), count(distinct series_id) filter (where open <= 0),
--            count(*)
--       from research_price_daily;
--     -- 0 | 16 | 0 | 9 | 25,920,971
--
--     select count(*) filter (where open = 0), count(*) filter (where open < 0),
--            count(distinct instrument_id)
--       from price_daily where open <= 0;
--     -- 154 | 0 | 14
--
-- So 170 bars across the two corpora carry an open that is present, non-null,
-- and not a price. The two facts the old branch merged:
--
--   * `no_fill_bar`          — bar t+1 does not exist. The series ended. This is
--                              a property of the SERIES and is not a data gap.
--   * `unusable_fill_price`  — bar t+1 exists, and its OPEN is not a usable
--                              price (NULL, or <= 0). This IS a data gap.
--
-- Parent criterion 8 is explicit that these must stay apart: *"These have
-- different bias implications and collapsing them loses the ability to tell a
-- data gap from a real absence."* Under the old code a corpus that silently
-- grew zero-open bars would have reported them as series endings, and
-- criterion 9's "measure what you reject" would have counted them as such.
--
--
-- WHY NOT REUSE `quarantined_bar`
-- ---------------------------------------------------------------------------
-- Every one of the 170 bars above is `rules = ['B1']` with both axes false —
-- measured, not assumed, on the full population of both corpora — so
-- `quarantined_bar` would be TRUE today. It is still the wrong code, because
-- `resolve_fills` cannot see a quarantine verdict: it receives a `BarSeries`
-- and nothing else, and the quarantine is the CALLER's gate (every strategy
-- module says so, and takes its `close_reason` from the caller for exactly this
-- reason). A writer that stamped `quarantined_bar` would be asserting a cause
-- it has no input for, and would keep asserting it against a raw loader that
-- never ran the quarantine at all. `unusable_fill_price` states only what the
-- writer can see: the bar is there and its open is not a price.
--
-- ⚠ OURS, NOT THE PARENT'S. Criterion 8 lists seven codes; `no_fill_bar` was
-- our eighth, `thin_cross_section` the ninth, and this is the tenth. All three
-- are flagged as additions in strategy_registry.py rather than passed off as
-- the parent's, and `OUR_ADDITIONAL_REASON_CODES` keeps the two sets separable
-- in Python.
--
-- ⚠ THE PYTHON LITERAL IS THE SOURCE, THIS IS THE MIRROR — unchanged from
-- sql/260. tests/test_strategy_registry.py reads the LATEST migration that
-- redefines this list, which is this file from here on.
--
-- ⚠ NOT a positivity CHECK on `fill_price`. sql/256's header argues against
-- exactly that and the argument still holds: such a bound "holds only while
-- every bar's open is positive — which is `price_quarantine`'s business, not
-- this table's". The fix is a refusal in the writer plus masking in the loader,
-- both of which produce a countable reason code; a constraint would produce an
-- exception at insert time with no record of how many bars it rejected.

ALTER TABLE strategy_signals
    DROP CONSTRAINT IF EXISTS strategy_signals_reason_codes;

ALTER TABLE strategy_signals
    ADD CONSTRAINT strategy_signals_reason_codes
    CHECK (not_evaluable_reason IS NULL OR not_evaluable_reason IN (
        'missing_volume', 'missing_spread', 'insufficient_warmup',
        'quarantined_bar', 'series_break', 'not_listed',
        'ambiguous_intrabar', 'no_fill_bar', 'thin_cross_section',
        'unusable_fill_price'
    ));

COMMENT ON COLUMN strategy_signals.not_evaluable_reason IS
    'Closed vocabulary: parent criterion 8''s seven codes, plus no_fill_bar '
    '(the series has no t+1), thin_cross_section (the ranked panel was smaller '
    'than the ranking rule is defined on) and unusable_fill_price (bar t+1 '
    'exists and its open is NULL or <= 0, so no fill can be priced). The Python '
    'Literal in strategy_registry.NotEvaluableReason is the source; this CHECK '
    'mirrors it.';
