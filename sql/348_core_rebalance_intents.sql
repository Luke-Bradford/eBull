-- 348_core_rebalance_intents.sql
--
-- #2603 item 3, execution half, step 1.  One durable record per core/cash
-- rebalance EVALUATION -- the mandate-driven analogue of
-- `strategy_entry_preflights`, which is signal-driven and cannot carry this.
--
-- ⚠⚠ CORRECTED 2026-08-22 (#2603 step 3b-3).  This header used to read "no
-- table has a foreign key to it and no module reads it, so no code path can
-- turn a row into an action".  BOTH HALVES ARE NOW FALSE: sql/349 added
-- `strategy_trades.core_rebalance_intent_id`, and
-- `app/services/strategy_core_submission_gate.py` SELECTs from this table to
-- decide whether a stored verdict may become an order.  The reading side did
-- arrive as promised; the promise was not re-read when it did.
--
-- What holds instead, and it is weaker on purpose: a row here is submission-gate
-- INPUT, not authority.  The gate has no acting caller anywhere in `app/` or
-- `scripts/`, so no path runs from a row here to an order.  That is a fact about
-- today's call graph rather than a mechanical impossibility, which is exactly
-- why it is written this way -- the previous wording survived the change that
-- falsified it because it sounded structural.
--
-- Why not a synthetic `strategy_signals` row instead: #2603 scope clause 5 says
-- the core path "reads mandate + current positions only; never reads
-- `strategy_signals`".  That clause separates two populations, and writing a
-- rebalance into the signal table merges them from the other side -- every
-- consumer of `strategy_signals` would then need a predicate none of them
-- carries.  (Six of its columns are also NOT NULL with no mandate-side meaning,
-- but that is the symptom, not the argument.)
--
-- APPEND-ONLY.  One row per evaluation, INCLUDING holds and refusals: a verdict
-- is evidence, and a core sleeve correctly holding for a month is the normal
-- case, so suppressing holds would make the common state indistinguishable from
-- "never evaluated".  Same posture as `strategy_core_eligibility_proofs`
-- (sql/346).
--
-- Spec: docs/proposals/ta/2026-08-14-core-rebalance-intent.md

CREATE TABLE IF NOT EXISTS strategy_core_rebalance_intents (
    core_rebalance_intent_id BIGSERIAL PRIMARY KEY,
    -- DEFAULT and never a parameter: a caller supplying its own evaluation time
    -- can backdate or extend a verdict at will (sql/346's rule, same reason).
    --
    -- ⚠ `clock_timestamp()`, NOT `now()`.  `now()` is TRANSACTION start time, so a
    -- writer called inside a transaction that opened before the sleeve was valued
    -- would stamp an evaluation EARLIER than the observation it evaluated -- and
    -- the `state_as_of <= evaluated_at` CHECK below would then reject a perfectly
    -- fresh snapshot for no reason but the caller's transaction boundary.  The
    -- wall clock at insert is what "when this was evaluated" means.
    evaluated_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    -- The mandate revision that GOVERNED this evaluation, by reference and never
    -- copied: `strategy_core_mandate_events` is already append-only and
    -- versioned, so the join is lossless, and copying core_target_pct and friends
    -- would create a second place for them to disagree.
    --
    -- NULLABLE, and NULL for exactly one verdict: `core_mandate_absent`, where
    -- there is no revision to point at.  Every other refusal loaded a
    -- `CoreMandate` and therefore has an event id.  Enforced below.
    core_mandate_event_id    BIGINT
                             REFERENCES strategy_core_mandate_events(core_mandate_event_id)
                             ON DELETE RESTRICT,
    -- The version THIS EVALUATION ran under -- always ours, always known.  The
    -- mandate's own policy_version lives on the event row and is deliberately not
    -- copied: on `core_mandate_policy_unsupported` the two differ, and one column
    -- named `policy_version` could not say which of them it held.
    allocator_policy_version TEXT NOT NULL CHECK (btrim(allocator_policy_version) <> ''
                                                  AND length(allocator_policy_version) <= 64),
    recorded_by              TEXT NOT NULL CHECK (btrim(recorded_by) <> ''
                                                  AND length(recorded_by) <= 128),

    ---------------------------------------------------------------------------
    -- The observed sleeve, stored AS OBSERVED.
    ---------------------------------------------------------------------------
    -- No FK to `instruments`: on a `sleeve_instrument_mismatch` refusal the id is
    -- whatever the caller supplied and need not resolve.  This is an observed
    -- input, not a resolved reference.
    core_instrument_id       BIGINT NOT NULL,
    -- ⚠ NULLABLE for the SAME reason as the two valuations below, and the reason
    -- is easy to miss on a text column.  `_state_refusal` compares
    -- `state.currency.strip().upper()` to the mandate's base currency, so a BLANK
    -- or absurdly long observed currency is refused as `sleeve_currency_mismatch`
    -- -- and a NOT NULL non-blank CHECK here would then make that refusal the one
    -- row that cannot be written, which is the exact trap the valuation columns
    -- were shaped to avoid.
    currency                 TEXT CHECK (currency IS NULL
                                         OR (btrim(currency) <> ''
                                             AND length(currency) <= 16)),
    -- ⚠⚠ NULLABLE, and the reason is the whole point.  `_state_refusal` refuses
    -- `sleeve_valuation_invalid` on a component that is non-finite, negative or
    -- >= 10^12, and the currency/instrument mismatches are checked BEFORE it --
    -- so a refusal row can legitimately carry Decimal("NaN") or 10^30.  Storing
    -- those into NUMERIC(18,6) raises `numeric field overflow`, which would make
    -- the evidence for an unrepresentable valuation the one row that cannot be
    -- written: the control unable to express a state the system reaches.
    --
    -- So NULL here means "the observed value was not representable in this
    -- column's shape; reason_code says what was wrong with it".  The enforceable
    -- half is the CHECK below -- a NULL implies `refused`, because every
    -- non-refused path has already passed `_state_refusal` and is storable.
    --
    -- ⚠ A storable value with more than 6 decimal places is ROUNDED to fit, so a
    -- stored input need not reproduce the stored output to the last place.  That
    -- is immaterial at the 7th decimal of a currency valuation and is not a
    -- refusal.
    core_market_value        NUMERIC(18,6),
    cash_balance             NUMERIC(18,6),
    state_as_of              TIMESTAMPTZ NOT NULL,

    ---------------------------------------------------------------------------
    -- The verdict: every field of `CoreRebalanceDecision`, one column each.
    ---------------------------------------------------------------------------
    action                   TEXT NOT NULL
                             CHECK (action IN ('hold', 'buy_core', 'sell_core', 'refused')),
    -- Closed enum of the allocator's eleven codes.  `broker_minimum_invalid` is
    -- unreachable until the executor supplies a broker minimum; admitted here
    -- because that writer is the next slice, not because it can occur now.
    reason_code              TEXT CHECK (reason_code IN (
                                 'core_mandate_absent',
                                 'core_mandate_policy_unsupported',
                                 'core_mandate_invalid',
                                 'core_mandate_disabled',
                                 'core_instrument_unset',
                                 'sleeve_currency_mismatch',
                                 'sleeve_instrument_mismatch',
                                 'sleeve_valuation_invalid',
                                 'broker_minimum_invalid',
                                 'core_sleeve_empty',
                                 'below_min_rebalance_amount'
                             )),
    amount                   NUMERIC(18,6) NOT NULL CHECK (amount >= 0),
    -- Computed percentages get the repo's computed-percentage shape
    -- (NUMERIC(12,8), as `strategy_entry_preflights.account_drawdown_pct`).  The
    -- band edges are mandate-derived and get the mandate's own NUMERIC(8,4), so
    -- they store exactly rather than being re-rounded.
    core_pct                 NUMERIC(12,8),
    target_pct               NUMERIC(8,4),
    lower_pct                NUMERIC(8,4),
    upper_pct                NUMERIC(8,4),
    effective_floor          NUMERIC(18,6),
    floor_source             TEXT CHECK (floor_source IN ('mandate', 'broker')),
    reserve_breached         BOOLEAN,
    -- Signed: negative IS the breach.
    reserve_margin_pct       NUMERIC(12,8),

    ---------------------------------------------------------------------------
    -- Shape, per action.  A CASE rather than an OR-of-implications: `action` is
    -- NOT NULL and its enum is closed, so every row takes exactly one branch and
    -- there is no arm a NULL can slip through.  ⚠ Every comparison inside a
    -- branch is either IS [NOT] NULL or on a NOT NULL column -- a bare `col = x`
    -- on a nullable column passes on NULL and would admit the omission the
    -- constraint exists to catch (docs/review-prevention-log.md, #2679 and
    -- sql/341).
    ---------------------------------------------------------------------------
    CONSTRAINT core_rebalance_intent_shape_matches_action CHECK (
        CASE action
            WHEN 'refused' THEN
                -- A refusal computed no weights, so a non-NULL derived field is a
                -- writer bug rather than a value judgement.
                reason_code IS NOT NULL
                AND amount = 0
                AND core_pct IS NULL AND target_pct IS NULL AND lower_pct IS NULL
                AND upper_pct IS NULL AND effective_floor IS NULL
                AND floor_source IS NULL AND reserve_breached IS NULL
                AND reserve_margin_pct IS NULL
            WHEN 'hold' THEN
                -- ⚠ A hold MAY carry a reason code and it is not an error:
                -- strategy_core_allocator.py:303 returns a hold with
                -- `below_min_rebalance_amount` when the gap to the band edge is
                -- under the floor.  Constrained to that one code.
                amount = 0
                AND (reason_code IS NULL OR reason_code = 'below_min_rebalance_amount')
                AND core_pct IS NOT NULL AND target_pct IS NOT NULL
                AND lower_pct IS NOT NULL AND upper_pct IS NOT NULL
                AND effective_floor IS NOT NULL AND floor_source IS NOT NULL
                AND reserve_breached IS NOT NULL AND reserve_margin_pct IS NOT NULL
                AND lower_pct <= target_pct AND target_pct <= upper_pct
                AND effective_floor > 0
            ELSE  -- buy_core / sell_core
                amount > 0
                AND reason_code IS NULL
                AND core_pct IS NOT NULL AND target_pct IS NOT NULL
                AND lower_pct IS NOT NULL AND upper_pct IS NOT NULL
                AND effective_floor IS NOT NULL AND floor_source IS NOT NULL
                AND reserve_breached IS NOT NULL AND reserve_margin_pct IS NOT NULL
                AND lower_pct <= target_pct AND target_pct <= upper_pct
                AND effective_floor > 0
        END
    ),
    -- The band ordering above is true by construction in the allocator
    -- (lower = target - band, upper = target + band, band > 0; floor is
    -- min_rebalance_amount > 0 or a strictly greater broker minimum).  It is
    -- constrained anyway because #2623 shipped a value into the wrong block by
    -- appending columns at a different ordinal in the INSERT list, the
    -- placeholder block and the reader tuple -- psycopg binds by NAME, so it
    -- feels order-free, and an unrelated all-or-nothing CHECK is what caught it.

    -- Exactly one verdict is reachable with no mandate row to cite.  Both sides
    -- are non-NULL booleans, so `=` is safe; `IS NOT DISTINCT FROM` on the right
    -- is what keeps a NULL reason_code from leaking through as unknown.
    CONSTRAINT core_rebalance_intent_event_absent_only_when_mandate_absent CHECK (
        (core_mandate_event_id IS NULL)
        = (reason_code IS NOT DISTINCT FROM 'core_mandate_absent')
    ),
    -- An unstorable OBSERVATION -- of either kind -- implies a refusal.  Every
    -- non-refused path has already passed `_state_refusal`, which requires the
    -- currency to match the mandate's and both valuations to be finite and inside
    -- the amount bound, so all three are representable there by construction.
    CONSTRAINT core_rebalance_intent_null_observation_implies_refused CHECK (
        (core_market_value IS NOT NULL AND cash_balance IS NOT NULL AND currency IS NOT NULL)
        OR action = 'refused'
    ),
    -- A valuation from the future is a caller bug; the allocator holds no clock
    -- and cannot catch it.
    CONSTRAINT core_rebalance_intent_state_not_after_evaluation CHECK (
        state_as_of <= evaluated_at
    )
);

-- The read the next slice needs: the latest intent, and the latest ACTIONABLE
-- intent, for the freshness bound the executor must apply.
CREATE INDEX IF NOT EXISTS strategy_core_rebalance_intents_evaluated_at_idx
    ON strategy_core_rebalance_intents (evaluated_at DESC);

COMMENT ON TABLE strategy_core_rebalance_intents IS
    'One durable record per core/cash rebalance evaluation (#2603 item 3). '
    'Append-only; holds and refusals are stored exactly as trades are, because a '
    'verdict is evidence. AUTHORISES NOTHING: nothing references it and nothing '
    'reads it, and it carries no eligibility proof -- the executor re-proves at '
    'submission. Provides no in-flight suppression and no expiry; both are owed '
    'by the slice that adds the trade linkage.';

COMMENT ON COLUMN strategy_core_rebalance_intents.core_market_value IS
    'Observed core value, or NULL when the observed value was not representable '
    'in NUMERIC(18,6) -- reason_code then says what was wrong with it. NULL '
    'implies action = refused.';

COMMENT ON COLUMN strategy_core_rebalance_intents.cash_balance IS
    'Observed settled cash, or NULL when not representable -- see core_market_value.';

COMMENT ON COLUMN strategy_core_rebalance_intents.allocator_policy_version IS
    'The policy version THIS evaluation ran under (always ours). The mandate''s '
    'own policy_version is on the referenced event row and differs on a '
    'core_mandate_policy_unsupported verdict.';
