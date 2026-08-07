-- 262_strategy_results.sql
--
-- Phase 5c — the backtest result model, and #2288 clauses 2-4.
-- Spec: docs/proposals/ta/2026-08-07-bounded-backtester.md §6 (the clauses),
-- §5.2 (the frozen split), §5.4 (sizing as a result input), §3.4 (the
-- ambiguity arms). Gate + literals: app/services/strategy_result.py.
-- Signal ledger: sql/255. Outcome ledger: sql/256.
--
--
-- ⚠⚠ WHY THIS TABLE SHIPS BEFORE ANYTHING WRITES TO IT
-- ---------------------------------------------------------------------------
-- It has no writer today; stage 5d is the writer, and it adds criterion 7's
-- twelve metrics as columns then. That ordering is the point rather than an
-- accident of sequencing.
--
-- #2288 clause 2 is "fail closed on absence", and the only moment a NOT NULL
-- basis is free is BEFORE the rows exist. Ship the metrics table first and the
-- basis arrives as an ALTER against populated rows, which needs either a
-- default (the exact thing clause 2 forbids — "a column with a default is a
-- column a writer can forget") or an invented history for every row already
-- written. So the provenance lands first and 5d writes its numbers INTO a shape
-- that already refuses an unlabelled result.
--
-- Measured 2026-08-07: `strategy_signals` and `strategy_outcomes` both hold 0
-- rows (spec M10), so nothing upstream has to be backfilled either.
--
--
-- ⚠⚠ NO METRIC COLUMN IS DEFINED HERE, DELIBERATELY
-- ---------------------------------------------------------------------------
-- Not a JSONB blob either, which was the tempting shortcut. Criterion 7 names
-- TWELVE metrics and says "a result missing any of the twelve is incomplete" —
-- a JSONB column cannot express that, so a partial metric set would store
-- silently and read as a complete result. 5d declares them as NOT NULL columns
-- where the criterion requires them.
--
--
-- GRAIN: ONE ROW PER (STRATEGY VERSION, RESULT VERSION)
-- ---------------------------------------------------------------------------
-- `result_version` is `strategy_result.ResultIdentity.version` — a hash over
-- the execution assumptions that decide a result but NOT a signal: the scope,
-- the namespace, the ambiguity arm, the sizing rule, the cost model, the corpus
-- version, the window, and the three rule-set versions of the pipeline that
-- produced the trades.
--
-- ⚠ WHY THOSE ARE NOT IN `strategy_version` INSTEAD, which is criterion 11's
-- obvious reading. C11 warns that a sizing change "that did not move the
-- version would let a different strategy inherit a track record". True — but
-- `strategy_signals` KEYS on `strategy_version`, and the signals are
-- byte-identical under either sizing rule or either ambiguity arm. Folding them
-- in would duplicate the whole signal ledger once per sizing rule and once per
-- arm, for rows that do not differ. They are properties of the EVALUATION, so
-- they get their own hash and C11's requirement is asserted against that one.
--
-- ⚠ EVERY HASHED MEMBER IS ALSO STORED AS ITS OWN COLUMN. A hash tells you two
-- rows differ and never which field moved, and a result nobody can diff is a
-- result nobody can debug. `sql/255` makes the same argument for storing
-- `universe` beside a version that already contains it.
--
--
-- ⚠ WHAT IS DELIBERATELY ABSENT
-- ---------------------------------------------------------------------------
-- The evaluated INSTRUMENT IDS — thousands per row (5,266 in the corpus ∩
--   validated-universe slice). The count is stored; the set is the gate's
--   argument, compared against `load_validated_universe` at check time. A
--   membership snapshot frozen into this table would go stale against a
--   universe that moves with every `sync_universe`.
-- The HOLD-OUT ACCESS LOG — criterion 5's timestamped access records are stage
--   5e's, and they are a property of the STRATEGY's history rather than of any
--   one result row. The gate reads two counts; nothing produces them yet, so
--   they are zero and it refuses, which is §6's stated initial state.
-- `ambiguity_material` — a property of the ARM PAIR (§3.4 computes the equity
--   curve twice and compares the two Sharpes), so it belongs to neither arm's
--   row. Also the gate's argument.
-- A `promotable` COLUMN — the refusal is COMPUTED by
--   `strategy_result.check_promotable`, never stored. A stored boolean is a
--   cached verdict that survives the fact that produced it: measure carry, and
--   a stored `true` would still be sitting on a row that never charged it.

CREATE TABLE IF NOT EXISTS strategy_results (
    result_id        BIGSERIAL PRIMARY KEY,

    -- The producer. Both inside `result_version`'s payload too; stored so the
    -- common read ("every result for this strategy") needs no hash.
    strategy_id      TEXT NOT NULL,
    strategy_version TEXT NOT NULL,
    result_version   TEXT NOT NULL,

    -- §5.4's levels. ⚠ `signal` is ABSENT: "Drawdown and Sharpe are computed at
    -- the latter two ONLY — a per-trade max drawdown does not compose."
    -- Per-signal results are the ledger (sql/255), not this table.
    result_scope     TEXT NOT NULL
        CHECK (result_scope IN ('sleeve', 'portfolio')),

    -- Criterion 5's two namespaces. ⚠ `purged` is NOT here and must never be:
    -- it is the verdict `strategy_result.namespace_for_signal` returns for a
    -- signal decided in-sample but filled on or after the boundary, and such a
    -- signal contributes to NO result. A third namespace would give it one.
    namespace        TEXT NOT NULL
        CHECK (namespace IN ('in_sample', 'hold_out')),

    -- §3.4's declared sensitivity pair. ⚠ NOT "assume the stop for
    -- conservatism", which §3.5 rule 4 and spike S5 reject — "it is not
    -- conservative, it is a different bias". Both arms are computed, both are
    -- reported, and a material gap between them blocks promotion.
    ambiguity_arm    TEXT NOT NULL
        CHECK (ambiguity_arm IN ('worst_case', 'best_case')),

    -- The evaluation window, closed on both ends. Frozen literals live in
    -- `strategy_result` (EVALUATION_WINDOW_START / _END); stored per row
    -- because a re-freeze is a corpus-version event and old rows keep the
    -- window they were actually computed over.
    window_start     DATE NOT NULL,
    window_end       DATE NOT NULL,

    -- ⚠⚠ #2288 CLAUSE 2, AND THE REASON THIS MIGRATION EXISTS. NOT NULL with NO
    -- DEFAULT, exactly as sql/255 does it for `strategy_signals.universe`: "An
    -- unlabelled result is treated as survivor_only, never as validated. A
    -- missing label must not read as 'fine'." A default would let a writer
    -- forget, and the forgotten value would be the favourable one.
    --
    -- ⚠ `survivorship_free` is in the vocabulary and is NOT a value any current
    -- corpus can produce (§6): US survivorship is only PARTIALLY correctable at
    -- 86.2% issuer resolution, with CEF/FPI-shaped residue and eToro-listing
    -- bias, and non-US is not correctable at all.
    universe_basis   TEXT NOT NULL
        CHECK (universe_basis IN ('survivor_only', 'survivorship_free')),

    -- ⚠ §6: "the binary label is not sufficient and the result model must not
    -- pretend otherwise". The basis says WHICH KIND of bias; this says WHICH
    -- CORPUS, so two results computed before and after a re-freeze are visibly
    -- different rather than silently comparable. Vendor + frozen last bar —
    -- NOT a code hash, because no module hash moves when the archive gains a
    -- year of bars.
    corpus_version   TEXT NOT NULL,

    -- 5b's model. Already inside `strategy_version` AND inside
    -- `result_version`; stored so a reader holding a result learns what costs
    -- were charged without reversing a hash.
    cost_model_id    TEXT NOT NULL,

    -- ⚠ AS AT COMPUTE TIME, never re-read from `cost_model` at gate time. When
    -- carry is finally measured (#2277), every row computed before that
    -- measurement must STAY unpromotable — a gate consulting today's module
    -- constant would silently promote a result that never charged it.
    carry_unmodelled BOOLEAN NOT NULL,

    -- §5.4: "Phase 5 computes statistics for a declared sizing rule, which is
    -- an input to the result identity." Naming it is what stops a later sizing
    -- change reading as a performance improvement.
    sizing_rule      TEXT NOT NULL,

    -- The three rule-set versions of the pipeline that produced the trades.
    -- ⚠ `input_rule_set_version` is sql/256's third key member and is here for
    -- its reason: re-run the quarantine under a changed rule set and the same
    -- signal resolves differently with the resolver byte-identical.
    position_rule_set_version TEXT NOT NULL,
    outcome_rule_set_version  TEXT NOT NULL,
    input_rule_set_version    TEXT NOT NULL,

    -- Criterion 9's census input. The SET is the gate's argument (see the
    -- header); this is the count, so a result whose population shrank is
    -- visible without re-running anything.
    evaluated_instrument_count INTEGER NOT NULL
        CHECK (evaluated_instrument_count >= 0),

    -- ⚠ CRITERION 6, AND NULL IS THE FAIL-CLOSED DEFAULT ON BOTH. "An
    -- undeclared trial count FAILS; it does not default to the number of
    -- shipped strategies" — and the count must include abandoned branches,
    -- manual eyeballing and discarded parameter values, so it is something a
    -- human declares rather than something this schema can derive.
    --
    -- Nullable rather than NOT NULL because 5d computes statistics before 5e
    -- computes the Deflated Sharpe, and a row that exists without one is a real
    -- state. The gate refuses on it; the schema records it.
    trial_count      INTEGER
        CHECK (trial_count IS NULL OR trial_count >= 1),
    deflated_sharpe  NUMERIC,

    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT strategy_results_unique
        UNIQUE (strategy_id, strategy_version, result_version),

    CONSTRAINT strategy_results_window_ordered
        CHECK (window_end >= window_start),

    -- ⚠ A BLANK version is PRESENT and meaningless, and NOT NULL does not catch
    -- it — the #2286 shape, where an empty `EBULL_SERVICE_TOKEN=` won an alias
    -- race against a real credential because a blank var is present. Every
    -- field below is identity, so an empty one silently merges two results into
    -- one bucket. sql/256 makes the same check for the same reason.
    CONSTRAINT strategy_results_identity_non_empty
        CHECK (
            strategy_id <> ''
            AND strategy_version <> ''
            AND result_version <> ''
            AND corpus_version <> ''
            AND cost_model_id <> ''
            AND sizing_rule <> ''
            AND position_rule_set_version <> ''
            AND outcome_rule_set_version <> ''
            AND input_rule_set_version <> ''
        )

    -- ⚠ NO CHECK ties `namespace` to `window_start` / `window_end`, and the
    -- omission is a decision. The window is the EVALUATION window and the
    -- namespace selects within it, so a hold-out row legitimately carries the
    -- full window. A CHECK asserting otherwise would encode the split's
    -- boundary as a SQL literal in a second place — and `strategy_result`
    -- already owns it, verified against the corpus by
    -- scripts/verify_2240_result_model.py --frozen. Two copies of a frozen date
    -- is how they diverge.
);

-- The phase-6 read: every result for one strategy version, both namespaces and
-- both arms together, because §3.4 reports the sensitivity pair side by side.
CREATE INDEX IF NOT EXISTS idx_strategy_results_strategy
    ON strategy_results (strategy_id, strategy_version, namespace);

-- The promotion sweep: "what, if anything, is promotable right now". Partial on
-- the one basis that can ever pass, because today the answer is nothing and the
-- index should cost nothing to say so.
CREATE INDEX IF NOT EXISTS idx_strategy_results_promotable_basis
    ON strategy_results (strategy_id, strategy_version)
    WHERE universe_basis = 'survivorship_free' AND NOT carry_unmodelled;

COMMENT ON TABLE strategy_results IS
    'Backtest result provenance: one row per (strategy version, result '
    'version). result_version hashes the EXECUTION assumptions that decide a '
    'result but not a signal — scope, namespace, ambiguity arm, sizing rule, '
    'cost model, corpus version, window and the three pipeline rule-set '
    'versions. ⚠ Those are NOT in strategy_version because the signals are '
    'byte-identical across them and sql/255 keys on it. ⚠ Criterion 7''s twelve '
    'metrics are stage 5d''s and are absent here on purpose: the basis is NOT '
    'NULL before any row exists, so 5d cannot write a number without one. '
    '⚠ There is no promotable column — strategy_result.check_promotable '
    'computes the refusal, because a stored verdict outlives the fact that '
    'produced it.';

COMMENT ON COLUMN strategy_results.universe_basis IS
    'survivor_only | survivorship_free (#2288 clause 2). NOT NULL, no default: '
    'an unlabelled result is treated as survivor_only, never as validated. '
    '⚠ Every result today is survivor_only — measured 2026-08-07, the corpus '
    'holds 7,693 series of which 2,424 have no instruments row, and the '
    'delisted half is the purchase that lands at #2284''s validation gate.';

COMMENT ON COLUMN strategy_results.corpus_version IS
    'Vendor(s) + the frozen last bar, e.g. '
    'paperswithbacktest/Stocks-Daily-Price@2026-07-08. ⚠ NOT a code hash: a '
    'corpus is DATA, and no module hash moves when the archive gains a year of '
    'bars. Bumping it is §5.2''s "deliberate re-freeze", which invalidates '
    'prior hold-out results and must be visible as such.';

COMMENT ON COLUMN strategy_results.carry_unmodelled IS
    'True when carry and/or FX were not charged (cost_model.CARRY_BPS / FX_BPS '
    'are None, not zero). ⚠ Stamped AS AT COMPUTE TIME and never re-derived: '
    'when carry is measured (#2277), rows computed before it must stay '
    'unpromotable. The promotion gate refuses on it (§5.1).';

COMMENT ON COLUMN strategy_results.trial_count IS
    'Criterion 6''s declared trial count, INCLUDING abandoned branches, manual '
    'eyeballing and discarded parameter values. ⚠ NULL is refused by the '
    'promotion gate and does NOT default to the number of shipped strategies.';
