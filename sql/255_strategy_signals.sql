-- 255_strategy_signals.sql
--
-- Phase 3b — the signal ledger.
-- Spec: docs/proposals/ta/2026-08-05-strategy-registry-and-signal-ledger.md
-- Registry contract: app/services/strategy_registry.py (phase 3a).
--
--
-- ⚠⚠ WHAT THE CONSTRAINTS BELOW DO AND DO NOT PROVE
-- ---------------------------------------------------------------------------
-- Parent §3.5 requires same-bar fills to be "structurally impossible rather
-- than merely discouraged". An earlier draft of the spec claimed the CHECK at
-- the bottom of this file WAS that mechanism. It is not, and saying so here
-- matters more than the constraint itself:
--
--   A writer can record signal_bar_date = t-1, fill on t, and use bar t's
--   data. Every constraint in this file passes.
--
-- The actual mechanism is the SHAPE of the registry API — a StrategySignal
-- carries a bar INDEX and no fill field at all, so a same-bar fill cannot be
-- expressed. These constraints are a BACKSTOP against a buggy writer, and are
-- described as one so nobody mistakes them for the guarantee.
--
--
-- GRAIN: ONE ROW PER (STRATEGY VERSION, INSTRUMENT, SIGNAL BAR, KIND)
-- ---------------------------------------------------------------------------
-- `strategy_version` is IN the key, not beside it. Without it, re-running a
-- changed strategy either collides with or silently overwrites the old signal,
-- and the ledger stops being a record of what was actually decided.
--
-- `signal_kind` is in the key because parent §3.5 applies the fill rule to
-- "entries and exits alike" — a strategy exiting one position and entering
-- another on the same bar for the same instrument is legitimate.
--
-- ⚠ `universe` is deliberately NOT in the key. Parent criterion 11: "universe
-- is part of the identity hash … 'S-1 on US stocks' and 'S-1 on eu_equity' are
-- two strategies and always were." It is INSIDE strategy_version, so a key
-- carrying it too would permit one strategy identity to span two universes,
-- which criterion 11 says is not one strategy. It is stored as a column for
-- querying and labelling (#2288), never for identity.
--
-- ⚠ NO TIMEFRAME COLUMN, and intraday is NOT a "just add one" migration.
-- v1 is daily and `signal_bar_date` is a DATE, so every intraday bar on one
-- date would collide on this key. Intraday needs bar-INSTANT identity — a
-- different key, not an extra column. Stated so the next person prices it
-- correctly rather than discovering it.

CREATE TABLE IF NOT EXISTS strategy_signals (
    signal_id        BIGSERIAL PRIMARY KEY,

    -- Identity of the producer. `strategy_version` hashes code + params +
    -- universe + cost model (criterion 11), so it changes whenever any of
    -- those do and old signals become visibly stale rather than silently
    -- reinterpreted under new logic.
    strategy_id      TEXT NOT NULL,
    strategy_version TEXT NOT NULL,

    instrument_id    BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,

    -- Bar t: the close that triggered. NOT the fill date, NOT wall clock.
    signal_bar_date  DATE NOT NULL,
    signal_kind      TEXT NOT NULL
        CHECK (signal_kind IN ('entry', 'exit')),

    verdict          TEXT NOT NULL
        CHECK (verdict IN ('fired', 'not_fired', 'not_evaluable')),

    -- ⚠ CLOSED vocabulary — parent criterion 8's seven codes plus `no_fill_bar`.
    -- Free text was the first draft and is rejected: criterion 9 requires
    -- counting what was excluded ("measure what you reject"), and free text
    -- cannot be counted. `no_fill_bar` is OUR addition, flagged as such in
    -- strategy_registry.py rather than passed off as the parent's.
    not_evaluable_reason TEXT
        CHECK (not_evaluable_reason IS NULL OR not_evaluable_reason IN (
            'missing_volume', 'missing_spread', 'insufficient_warmup',
            'quarantined_bar', 'series_break', 'not_listed',
            'ambiguous_intrabar', 'no_fill_bar'
        )),

    -- Bar t+1. Populated ONLY when the signal fired — a not_fired or
    -- not_evaluable bar has no fill.
    fill_bar_date    DATE,
    fill_price       NUMERIC,

    -- #2288's labelling contract. NOT NULL and no default: a metric computed
    -- on a survivor-only universe must be marked as such, and a column with a
    -- default is a column a writer can forget.
    universe         TEXT NOT NULL
        CHECK (universe IN ('survivor_only', 'survivorship_free')),

    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT strategy_signals_unique
        UNIQUE (strategy_id, strategy_version, instrument_id, signal_bar_date, signal_kind),

    -- A reason is required exactly when the verdict is not_evaluable, and
    -- meaningless otherwise. Without this, a `fired` row could carry a reason
    -- code and corrupt the criterion-9 counts.
    CONSTRAINT strategy_signals_reason_matches_verdict
        CHECK (
            (verdict = 'not_evaluable' AND not_evaluable_reason IS NOT NULL)
            OR (verdict <> 'not_evaluable' AND not_evaluable_reason IS NULL)
        ),

    -- A fill exists exactly when the signal fired.
    CONSTRAINT strategy_signals_fill_matches_verdict
        CHECK (
            (verdict = 'fired' AND fill_bar_date IS NOT NULL AND fill_price IS NOT NULL)
            OR (verdict <> 'fired' AND fill_bar_date IS NULL AND fill_price IS NULL)
        ),

    -- ⚠ THE BACKSTOP, not the mechanism. See the header. It proves the two
    -- stored dates are ordered and nothing about which bar was read.
    CONSTRAINT strategy_signals_fill_after_signal
        CHECK (fill_bar_date IS NULL OR fill_bar_date > signal_bar_date)
);

-- The read pattern phase 4/5 have: every signal a strategy version produced,
-- in bar order, to walk outcomes forward.
CREATE INDEX IF NOT EXISTS idx_strategy_signals_version_bar
    ON strategy_signals (strategy_id, strategy_version, signal_bar_date);

-- Per-instrument replay, and the join phase 4's resolver needs.
CREATE INDEX IF NOT EXISTS idx_strategy_signals_instrument_bar
    ON strategy_signals (instrument_id, signal_bar_date);

-- Criterion 9 — "measure what you reject". Counting exclusions by reason is a
-- reporting requirement, not an ad-hoc query, so it gets an index.
CREATE INDEX IF NOT EXISTS idx_strategy_signals_reason
    ON strategy_signals (strategy_id, strategy_version, not_evaluable_reason)
    WHERE not_evaluable_reason IS NOT NULL;

COMMENT ON TABLE strategy_signals IS
    'Signal ledger: one row per (strategy version, instrument, signal bar, '
    'kind). strategy_version hashes code + params + universe + cost model '
    '(criterion 11), so a changed strategy never overwrites or reinterprets '
    'an old signal. ⚠ The fill-order CHECK is a BACKSTOP — same-bar fills are '
    'made impossible by the registry API carrying no fill field, not by this '
    'table.';

COMMENT ON COLUMN strategy_signals.fill_bar_date IS
    'Bar t+1: the NEXT BAR IN THAT INSTRUMENT''S SERIES, never '
    'signal_bar_date + 1 day. Calendar gaps are normal — S4 measured 1,204 '
    'tradable instruments whose latest bar is over a month old — and date '
    'arithmetic would invent a fill on a day the instrument did not trade.';

COMMENT ON COLUMN strategy_signals.universe IS
    'survivor_only | survivorship_free (#2288). Every v1 signal is '
    'survivor_only: #2284 measured the research corpus serving 0 of 259 known '
    'delisted names. Any win rate computed from these rows inherits that.';
