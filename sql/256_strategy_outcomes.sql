-- 256_strategy_outcomes.sql
--
-- Phase 4b — the outcome ledger.
-- Spec: docs/proposals/ta/2026-08-06-outcome-ledger.md
-- Resolver: app/services/outcome_resolver.py (phase 4a), whose §3.7 fields
-- these columns are. Signal ledger: sql/255_strategy_signals.sql (phase 3b).
--
--
-- GRAIN: ONE ROW PER (SIGNAL, RESOLVER VERSION, INPUT VERSION)
-- ---------------------------------------------------------------------------
-- `rule_set_version` is the resolver's own source hash, and it is in the key
-- for the reason `strategy_version` is in the ledger's: a changed execution
-- assumption must produce a SECOND outcome beside the first, never overwrite
-- it. The resolver's version is NOT inside `strategy_version` — that hash
-- covers the strategy's code, params, universe and cost model, not ours.
--
-- ⚠ `input_rule_set_version` is the THIRD key member and the one that is easy
-- to miss. The resolver is not the only thing that decides an outcome: its
-- INPUTS do too. `research_price_structure_store.load_masked_series` masks
-- high/low/close per `price_quarantine`'s rule set, and 4a's masking contract
-- is that an absent field REFUSES. Re-run the quarantine under a changed rule
-- set and the same signal can resolve differently with `outcome_resolver.py`
-- byte-identical. Without this column that is not merely unrecorded, it is
-- UNSTORABLE — the corrected outcome collides on the two-part key and no
-- `ON CONFLICT` exists, so the operator would have to touch the resolver's
-- source to move its hash before the fix could be written down.
--
-- It is TEXT with no CHECK, exactly like `strategy_version`: a version string
-- is open by nature and a vocabulary CHECK would need widening by every
-- producer.
--
-- ⚠ `resolution_method` is stored and is deliberately NOT in the key. v1 has
-- one value; when an intraday method exists, a second resolution of the same
-- signal at the same versions COLLIDES (loud) rather than storing a second row
-- that every aggregate then double-counts (silent). Spec §2.2 carries the
-- trigger to revisit and what must change with it.
--
-- ⚠ EVERY AGGREGATE OVER THIS TABLE MUST PIN ONE (rule_set_version,
-- input_rule_set_version) PAIR. Two resolver versions coexist by design, so an
-- unpinned `count(*) FILTER (WHERE outcome = 'tp_hit')` counts one trade twice.
-- Same property `strategy_version` has in the ledger.
--
--
-- ⚠⚠ WHAT THE FOREIGN KEY DOES NOT PROVE
-- ---------------------------------------------------------------------------
-- It proves the parent signal EXISTS. It does not prove the parent was a FIRED
-- ENTRY — and 4a resolves nothing else: a `signal_kind = 'exit'` row is not an
-- input (spec 4a §1) and a `not_fired` / `not_evaluable` row has no fill to
-- resolve. A CHECK cannot read the parent row, so the constraint set below is
-- silent on it.
--
-- The mechanism is in the WRITER'S STATEMENT, not here:
-- `outcome_ledger.store_outcomes` inserts via `SELECT … FROM strategy_signals
-- WHERE signal_kind = 'entry' AND verdict = 'fired' AND (exit_bar_date IS NULL
-- OR exit_bar_date >= fill_bar_date)`, so a non-qualifying parent inserts ZERO
-- rows and the writer raises on the shortfall. Same shape as sql/255's header:
-- the constraints are a backstop, the statement is the guarantee.
--
-- ⚠ ON DELETE CASCADE is correct here and does not contradict the
-- prevention-log rule against cascading into audit tables. An outcome is
-- DERIVED FROM its signal, not an independent record of an action — a signal
-- deleted with its outcomes left behind is an orphan no query can interpret.
--
--
-- WHAT IS DELIBERATELY ABSENT
-- ---------------------------------------------------------------------------
-- `exit_index` — 4a §3.7: an index is not durable across a corpus rebuild, the
--   date is. Storing both would let them disagree after a re-adjustment, and
--   the index is the one that would be wrong.
-- `entry_price`, take-profit, stop-loss, `max_hold_bars` — the entry is
--   `strategy_signals.fill_price` and the levels are strategy parameters,
--   already inside `strategy_version` (criterion 11). Re-declaring either
--   creates a second source of truth that can disagree with the first.
-- `universe`, `instrument_id`, `signal_bar_date` — on the signal row, one join
--   away. ⚠ Consequence: EVERY read of this table is a join. A phase-6 surface
--   wanting a wide row builds a VIEW; it does not add columns here.

CREATE TABLE IF NOT EXISTS strategy_outcomes (
    outcome_id       BIGSERIAL PRIMARY KEY,

    signal_id        BIGINT NOT NULL
        REFERENCES strategy_signals(signal_id) ON DELETE CASCADE,

    -- The resolver's own source hash (outcome_resolver.RULE_SET_VERSION).
    rule_set_version TEXT NOT NULL,

    -- The version stamp of the pipeline that produced the BARS — for a masked
    -- read, price_quarantine.RULE_SET_VERSION. See the header.
    input_rule_set_version TEXT NOT NULL,

    -- ⚠ CLOSED vocabulary: the parent design's four (§3 decision 1) plus
    -- `unresolved`, which is ours and is argued for in 4a's spec §3.4 rather
    -- than assumed. Restated here because SQL cannot import a Literal, and
    -- PINNED to the Python constants by tests/test_strategy_outcomes_ledger.py
    -- — a closed vocabulary declared in two places and validated in neither is
    -- the #2218 defect.
    outcome          TEXT NOT NULL
        CHECK (outcome IN ('tp_hit', 'sl_hit', 'expired', 'ambiguous', 'unresolved')),

    -- ⚠ One member in v1 and not decoration. S5 (#2245): a historical bar can
    -- never be resolved intraday (the candle endpoint has no date parameter,
    -- no offset, no cursor) while a forward-going signal can be, inside a
    -- ~2-session window. Without the stamp a later intraday-backed resolution
    -- mixes silently into the same statistics.
    resolution_method TEXT NOT NULL
        CHECK (resolution_method IN ('daily_bar')),

    -- ⚠ CLOSED vocabulary. `series_break` / `quarantined_bar` are criterion 8's
    -- own codes; `window_truncated` / `missing_bar_data` are OURS and flagged
    -- as additions in 4a's spec §3.4 rather than smuggled in.
    reason           TEXT
        CHECK (reason IS NULL OR reason IN (
            'window_truncated', 'series_break', 'quarantined_bar', 'missing_bar_data'
        )),

    -- ⚠ The DATE, never the index. See the header.
    exit_bar_date    DATE,
    exit_price       NUMERIC,
    -- `exit_index - fill_index`. ⚠ 0 IS LEGAL — a TP or SL touched on the fill
    -- bar. It is a bar count and NOT exposure time; criterion 7's exposure
    -- metric is phase 5's and must be defined there, not read off this column.
    bars_held        INTEGER,
    -- ⚠ GROSS. Criterion 2 requires costs per trade and this number has none.
    -- The name is the guard against something downstream averaging it as
    -- performance.
    gross_return_pct NUMERIC,

    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT strategy_outcomes_unique
        UNIQUE (signal_id, rule_set_version, input_rule_set_version),

    -- A reason is required exactly when the outcome is `unresolved`, and is
    -- meaningless otherwise. Without this an `expired` row could carry
    -- `window_truncated` and corrupt the criterion-9 refusal counts — which is
    -- the exact confusion 4a's §3.4 exists to prevent.
    CONSTRAINT strategy_outcomes_reason_matches_outcome
        CHECK ((outcome = 'unresolved') = (reason IS NOT NULL)),

    -- ⚠ Written as a pair of nullity EQUALITIES, not `a IS NOT NULL AND b IS
    -- NOT NULL`. `A IS NOT NULL AND B IS NOT NULL` is three states in SQL and
    -- two in the Python expression that mirrors it, which is how a half-filled
    -- row slipped past 3c's mirror (prevention log, #2240 3c). The Python side
    -- COUNTS the fields for the same reason.
    --
    -- An unresolved outcome has no exit location; every other outcome has one,
    -- INCLUDING `ambiguous` — the bar is known, only the touch order is not.
    CONSTRAINT strategy_outcomes_location_matches_outcome
        CHECK (
            (exit_bar_date IS NULL) = (outcome = 'unresolved')
            AND (bars_held IS NULL) = (outcome = 'unresolved')
        ),

    -- A price and a return exist exactly for the three booked outcomes.
    -- ⚠ `ambiguous` carries a location but NO price: 4a §3.7 — "a return column
    -- that is populated for them is a column something will eventually
    -- average", and §3.5.4 excludes ambiguous outcomes from the win rate.
    CONSTRAINT strategy_outcomes_booked_matches_outcome
        CHECK (
            (exit_price IS NULL) = (outcome NOT IN ('tp_hit', 'sl_hit', 'expired'))
            AND (gross_return_pct IS NULL) = (outcome NOT IN ('tp_hit', 'sl_hit', 'expired'))
        ),

    -- ⚠ The exit cannot precede the fill. This bounds it at 0 and no further:
    -- `bars_held` cannot be re-derived here, because trading days are not
    -- calendar days and `exit_index` is deliberately not stored.
    CONSTRAINT strategy_outcomes_bars_held_non_negative
        CHECK (bars_held IS NULL OR bars_held >= 0),

    -- ⚠ A blank version is PRESENT and meaningless, and NOT NULL does not catch
    -- it — the #2286 shape, where an empty `EBULL_SERVICE_TOKEN=` won an alias
    -- race against a real credential because a blank var is present. Both key
    -- members are load-bearing identity, so an empty one silently merges two
    -- rule sets into one bucket. sql/255 does not do this for
    -- `strategy_version`; that is a gap there, not a precedent here.
    CONSTRAINT strategy_outcomes_versions_non_empty
        CHECK (rule_set_version <> '' AND input_rule_set_version <> '')

    -- ⚠ NO lower bound on `exit_price` or `gross_return_pct`, deliberately.
    -- Both have an obvious-looking floor (> 0, > -1) that holds only while
    -- every bar's open is positive — which is `price_quarantine`'s business,
    -- not this table's. A CHECK here would start rejecting rows on a code path
    -- nobody watches, and Postgres validates a new CHECK against EXISTING rows
    -- only, so the absence of a violating row today proves nothing (#2218).
    -- `scripts/verify_2240_outcome_ledger.py` reports the full-population
    -- minimum of each instead; adding the bound is a decision for whoever
    -- reads that number.
);

-- The phase-5/6 read: every outcome for one strategy version at one resolver
-- version. ⚠ The join to `strategy_signals` is unavoidable by design (see the
-- header), so this index serves the anti-join in
-- `outcome_ledger.select_pending_fills`, which probes by signal + both
-- versions.
CREATE INDEX IF NOT EXISTS idx_strategy_outcomes_signal_versions
    ON strategy_outcomes (signal_id, rule_set_version, input_rule_set_version);

-- Criterion 9 — "measure what you reject". Counting `unresolved` by reason is a
-- reporting requirement, not an ad-hoc query, so it gets an index. Same shape
-- as sql/255's `idx_strategy_signals_reason`.
CREATE INDEX IF NOT EXISTS idx_strategy_outcomes_reason
    ON strategy_outcomes (rule_set_version, input_rule_set_version, reason)
    WHERE reason IS NOT NULL;

COMMENT ON TABLE strategy_outcomes IS
    'Outcome ledger: one row per (signal, resolver version, input version). '
    'The resolver''s rule_set_version and the bars'' input_rule_set_version are '
    'BOTH in the key, because both decide the outcome — a quarantine rule-set '
    'change can flip a class with the resolver byte-identical. ⚠ Every '
    'aggregate must pin one (rule_set_version, input_rule_set_version) pair: '
    'two versions coexist by design, so an unpinned count counts one trade '
    'twice. ⚠ The FK does not prove the parent was a fired ENTRY — the writer''s '
    'INSERT … SELECT does.';

COMMENT ON COLUMN strategy_outcomes.input_rule_set_version IS
    'Version stamp of the pipeline that produced the BARS the resolver read — '
    'price_quarantine.RULE_SET_VERSION for a masked read. Not the resolver''s '
    'own. Required, no default: a caller reading bars from an unversioned path '
    'must say so explicitly rather than pass nothing.';

COMMENT ON COLUMN strategy_outcomes.gross_return_pct IS
    'GROSS: (exit_price - entry) / entry, with NO costs. Criterion 2 requires '
    'per-trade costs and this number has none. NULL for ambiguous and '
    'unresolved, which are excluded from the win rate with their counts shown '
    '(criterion 9). ⚠ Not re-derived by any CHECK — the resolver computes it in '
    'Decimal at 28 digits and a NUMERIC re-derivation differs in scale; it is '
    'cross-checked on the full population by scripts/verify_2240_outcome_ledger.py.';
