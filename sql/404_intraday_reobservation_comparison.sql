-- 404_intraday_reobservation_comparison.sql
--
-- #2840 — record what the provider NOW says about an intraday bar we ALREADY HOLD.
--
-- The harvester has always fetched this evidence and always thrown it away.
-- `strategy_intraday_harvest._fetch_count` asks for a COUNT with `_OVERLAP_BARS = 3`, so a
-- steady-state fire requests bars reaching behind the durable watermark;
-- `_completed_rth_bars` keeps them (its only filters are "still forming" and "outside
-- RTH"); and line 301 then drops everything at or below the watermark unread. The only
-- equality check in the whole path compares two bars WITHIN one response. Nothing has ever
-- compared a delivered bar against the row already stored.
--
-- These two tables are that comparison, made durable. Spec:
-- `docs/proposals/ta/2026-09-20-2840-re-observation-comparison-spec.md` (revision 2).
--
-- ⚠ WHAT THIS IS FOR. `bar_capture_certificate.PROVIDER_REWRITE_TIMING_VERIFIED` is False
-- and downgrades every certifying verdict while it stays False. Flipping it needs a
-- BRACKET: an observation that AGREED and a later one that DIVERGED, both lying inside
-- (corporate-action effective instant, following session open). A bracket that straddles
-- the open is inconclusive however narrow — agreement at 09:25 and divergence at 09:35
-- permits a rewrite at 09:26. Hence two real observation rows, each carrying its own time
-- bounds, and not a "last seen" pointer that the next observation overwrites.
--
-- ⚠⚠ NO FOREIGN KEY TO `strategy_intraday_bars`, DELIBERATELY. That table is partitioned
-- and retention DROPS whole partitions (`strategy_observation_storage.drop_expired_
-- partitions`). A reference would make a drop either block on the evidence or cascade it
-- away; the evidence must OUTLIVE the horizon that produced it. Bar identity and BOTH
-- compared value sets are therefore COPIED, not referenced. For the same reason no advisory
-- lock is taken around the seed read: ordinary relation locking plus MVCC already make it
-- safe, and `pg_advisory_xact_lock(2448, tier)` would only add a deadlock edge against
-- retention's own tier order (`1m -> 30m -> 5m`).
--
-- ⚠⚠ A RECORDED DIVERGENCE IS NOT A RE-BASING. A price correction, a late exchange
-- adjustment and a split re-base all produce the same row. These tables record THAT the
-- provider's answer moved. Attribution is the consumer's job and none is implied here.
--
-- ⚠ THE CLAIM THIS EVIDENCE LICENSES is "no price change was detected BETWEEN THESE TWO
-- OBSERVATIONS" — never "the provider did not rewrite in that interval". A -> B -> A
-- between two polls is invisible to any polling scheme.
--
-- Refs #2840, #2437.
-- Schema it sits beside: sql/276 (bars), sql/278 (watermarks, and the dropped bar PK),
-- sql/402 (captured_at = clock_timestamp()).

-- ⚠ Both tables take a FOREIGN KEY on `instruments`, which needs SHARE ROW EXCLUSIVE on a
-- hot relation. A PENDING lock queues ahead of new readers, so an unbounded wait would stop
-- subsequent reads of `instruments` from starting — and the FastAPI lifespan runs
-- migrations, so that hangs app boot rather than failing it. Bounded, retryable, five
-- seconds. Precedent: `docs/review-prevention-log.md`, "A migration that is WAITING for a
-- lock is not passive" (#2363); sql/335; sql/402.
SET LOCAL lock_timeout = '5s';

-- ---------------------------------------------------------------------------
-- One row per provider call per SELECTED member. The denominator of calls, and
-- the durable record of the calls that produced no comparison at all.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS strategy_intraday_reobservations (
    reobservation_id      BIGSERIAL PRIMARY KEY,

    -- Member identity AS SELECTED. `ordinal` and `symbol` and not merely
    -- `instrument_id`, because a member that fails universe resolution has no
    -- instrument id and would otherwise be unidentifiable — and because a unique
    -- key containing a NULL column never conflicts in PostgreSQL, so an
    -- instrument-keyed constraint would silently stop deduplicating exactly the
    -- rows that need it most.
    universe_version      TEXT        NOT NULL,
    ordinal               INTEGER     NOT NULL,
    timeframe             TEXT        NOT NULL CHECK (timeframe IN ('30m', '5m', '1m')),
    symbol                TEXT        NOT NULL,
    instrument_id         BIGINT      REFERENCES instruments(instrument_id) ON DELETE CASCADE,

    -- ⚠⚠ BOUNDS OF THIS CALL, NOT THE JOB CLOCK. `app/workers/scheduler.py` computes
    -- `observed_at = datetime.now(tz=UTC)` once, BEFORE `run_intraday_harvest` fetches
    -- anything, so it precedes every provider call. Stamping an observation with it
    -- reproduces exactly the defect sql/402 moved `captured_at` to `clock_timestamp()`
    -- to fix: a time computed before a fetch is not a bound on the observation.
    -- ⚠ These bound the LOGICAL call INCLUDING `ResilientClient`'s internal retries —
    -- wider than one HTTP attempt, which is the conservative direction for a bracket.
    -- ⚠ They are CLIENT-SIDE bounds. A cached or replica-stale response bounds when this
    -- client received an assertion, not when the provider's history actually changed.
    requested_at          TIMESTAMPTZ NOT NULL,
    received_at           TIMESTAMPTZ NOT NULL,

    outcome               TEXT        NOT NULL CHECK (outcome IN (
                                          'unresolved_member',
                                          'not_attempted',
                                          'fetch_failed',
                                          'invalid_response',
                                          'comparison_skipped',
                                          'no_overlap',
                                          'no_baseline',
                                          'compared')),

    -- The overlap set is the delivered completed-RTH bars with `bar_time <= watermark`
    -- (empty when the watermark is NULL). Every bar in it lands in exactly one of
    -- compared / missing_baseline / invalid_baseline.
    overlap_bars          INTEGER     NOT NULL DEFAULT 0 CHECK (overlap_bars >= 0),
    compared_bars         INTEGER     NOT NULL DEFAULT 0 CHECK (compared_bars >= 0),
    agreed_bars           INTEGER     NOT NULL DEFAULT 0 CHECK (agreed_bars >= 0),
    diverged_bars         INTEGER     NOT NULL DEFAULT 0 CHECK (diverged_bars >= 0),
    missing_baseline_bars INTEGER     NOT NULL DEFAULT 0 CHECK (missing_baseline_bars >= 0),
    invalid_baseline_bars INTEGER     NOT NULL DEFAULT 0 CHECK (invalid_baseline_bars >= 0),

    -- Exception CLASS NAME ONLY. Provider messages can carry request URLs, identifiers and
    -- upstream response fragments; `strategy_intraday_harvest.HarvestFailure` already
    -- applies this rule to `job_runs` and it applies here for the same reason.
    failure_class         TEXT,

    CONSTRAINT strategy_intraday_reobservations_interval
        CHECK (received_at >= requested_at),
    CONSTRAINT strategy_intraday_reobservations_compared_split
        CHECK (agreed_bars + diverged_bars = compared_bars),
    CONSTRAINT strategy_intraday_reobservations_overlap_split
        CHECK (compared_bars + missing_baseline_bars + invalid_baseline_bars = overlap_bars),
    -- An unresolved member is exactly the case with no instrument, and it makes no call.
    CONSTRAINT strategy_intraday_reobservations_unresolved_shape
        CHECK ((outcome = 'unresolved_member') = (instrument_id IS NULL)),
    -- No provider call was made, so the two bounds collapse to the instant of the refusal.
    CONSTRAINT strategy_intraday_reobservations_uncalled_instant
        CHECK (outcome NOT IN ('unresolved_member', 'not_attempted') OR received_at = requested_at),
    -- A failure class is present exactly when something raised.
    CONSTRAINT strategy_intraday_reobservations_failure_shape
        CHECK ((failure_class IS NOT NULL)
               = (outcome IN ('not_attempted', 'fetch_failed', 'invalid_response',
                              'comparison_skipped'))),
    -- No response was usable, so no bar can have been in an overlap set.
    CONSTRAINT strategy_intraday_reobservations_barren_outcomes
        CHECK (outcome NOT IN ('unresolved_member', 'not_attempted', 'fetch_failed',
                               'invalid_response', 'comparison_skipped')
               OR overlap_bars = 0),
    CONSTRAINT strategy_intraday_reobservations_compared_outcome
        CHECK ((outcome = 'compared') = (compared_bars > 0)),
    CONSTRAINT strategy_intraday_reobservations_no_overlap_outcome
        CHECK (outcome <> 'no_overlap' OR overlap_bars = 0),
    CONSTRAINT strategy_intraday_reobservations_no_baseline_outcome
        CHECK (outcome <> 'no_baseline' OR (overlap_bars > 0 AND compared_bars = 0)),

    -- Replay idempotency. There is no response identity in this provider, and this design
    -- does not invent one: the unit of the denominator is the OBSERVATION, whose identity
    -- is its own instant. A genuinely repeated provider call is a genuinely new observation
    -- and correctly gets a new row; a retried WRITE of the same in-memory call carries the
    -- same `requested_at` and is refused. That is the replay hazard that actually exists.
    CONSTRAINT strategy_intraday_reobservations_replay
        UNIQUE (universe_version, ordinal, requested_at)
);

COMMENT ON TABLE strategy_intraday_reobservations IS
    'One row per provider call per selected intraday-harvest member, including calls that '
    'produced no comparison. requested_at/received_at bound THIS call (client-side, '
    'including internal retries) and are never the job clock — see sql/402. ABSENCE of a '
    'row means "no completed observation record", NOT "the member was not selected": a '
    'crash between fetch and insert leaves nothing behind. #2840.';

CREATE INDEX IF NOT EXISTS idx_strategy_intraday_reobservations_member_time
    ON strategy_intraday_reobservations (timeframe, instrument_id, requested_at DESC);

-- ---------------------------------------------------------------------------
-- One row per COMPARED BAR per call. This is the denominator carrying bar
-- identity, and it is why the bracket's left edge survives: a durable row, not
-- a "latest agreement" pointer that the next observation overwrites.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS strategy_intraday_reobserved_bars (
    reobservation_id          BIGINT      NOT NULL
                                  REFERENCES strategy_intraday_reobservations(reobservation_id)
                                  ON DELETE CASCADE,
    bar_time                  TIMESTAMPTZ NOT NULL,

    -- Copied identity: queryable without a join, and independent of the bar's partition.
    timeframe                 TEXT        NOT NULL CHECK (timeframe IN ('30m', '5m', '1m')),
    instrument_id             BIGINT      NOT NULL
                                  REFERENCES instruments(instrument_id) ON DELETE CASCADE,

    -- What the provider delivered THIS call, at storage precision (see the comparison note
    -- below). Same widths and same positivity CHECKs as `strategy_intraday_bars`, so any
    -- value that could be stored as a bar can be recorded here.
    observed_open             DOUBLE PRECISION NOT NULL CHECK (observed_open > 0),
    observed_high             DOUBLE PRECISION NOT NULL CHECK (observed_high > 0),
    observed_low              DOUBLE PRECISION NOT NULL CHECK (observed_low > 0),
    observed_close            DOUBLE PRECISION NOT NULL CHECK (observed_close > 0),
    observed_volume           DOUBLE PRECISION CHECK (observed_volume IS NULL OR observed_volume >= 0),

    -- What it was compared AGAINST. Always stored, never inferred from `price_changed`:
    -- a reader must be able to see both sides without reconstructing one of them.
    baseline_open             DOUBLE PRECISION NOT NULL CHECK (baseline_open > 0),
    baseline_high             DOUBLE PRECISION NOT NULL CHECK (baseline_high > 0),
    baseline_low              DOUBLE PRECISION NOT NULL CHECK (baseline_low > 0),
    baseline_close            DOUBLE PRECISION NOT NULL CHECK (baseline_close > 0),
    baseline_volume           DOUBLE PRECISION CHECK (baseline_volume IS NULL OR baseline_volume >= 0),

    baseline_source           TEXT        NOT NULL
                                  CHECK (baseline_source IN ('stored_bar', 'prior_reobservation')),

    -- ⚠⚠ THE BRACKET'S LEFT EDGE. Non-null exactly when the baseline was itself an
    -- observation, whose `requested_at` is then a genuine LOWER bound. ON DELETE RESTRICT
    -- and not SET NULL: a call that is somebody's left edge must not become deletable into
    -- silence. No retention exists for these tables today; when one is written it has to
    -- confront this constraint, which is the point.
    baseline_reobservation_id BIGINT      REFERENCES strategy_intraday_reobservations(reobservation_id)
                                  ON DELETE RESTRICT,

    -- ⚠ `strategy_intraday_bars.captured_at` of the seeding row, copied so contemporaneity
    -- stays judgeable after that partition is dropped. It is an UPPER bound on when the
    -- baseline was observed and NOT a lower one — so a divergence against a stored_bar
    -- baseline is evidence of a rewrite but is NOT A BRACKET, and read-time eligibility
    -- must exclude it rather than treat this column as an observation instant.
    baseline_captured_at      TIMESTAMPTZ,

    -- ⚠⚠ COMPARISON IS AT STORAGE PRECISION, AND PRICE-ONLY.
    --
    -- The provider constructs `Decimal(str(raw))` (etoro.py:706-709) while sql/277 stores
    -- DOUBLE PRECISION, so a naive `Decimal == float` is exact and therefore FALSE for
    -- almost every bar — it would manufacture a divergence on every comparison. The
    -- delivered Decimal is converted with `float(d)`, which is the same value PostgreSQL
    -- produces for `numeric::double precision` (verified on 3,006 values including
    -- 1.0000000000000002 and 20-digit inputs: zero mismatches). Two Decimals differing
    -- below float resolution therefore compare EQUAL. That is a stated limit of the
    -- detector, not a defect.
    --
    -- Volume is NOT a divergence trigger. `etoro._int_or_none` evaluates
    -- `int(float(str(value)))` and maps a result of zero to NULL, so a genuinely
    -- zero-volume bar and a missing one are the same value by the time we see it. It is
    -- recorded and flagged; it never decides.
    --
    -- Both flags are CHECK-derived from the stored values, so neither can be forged by a
    -- writer that computes them differently.
    price_changed             BOOLEAN     NOT NULL,
    volume_changed            BOOLEAN     NOT NULL,

    PRIMARY KEY (reobservation_id, bar_time),

    CONSTRAINT strategy_intraday_reobserved_bars_prior_shape
        CHECK ((baseline_source = 'prior_reobservation') = (baseline_reobservation_id IS NOT NULL)),
    CONSTRAINT strategy_intraday_reobserved_bars_stored_shape
        CHECK ((baseline_source = 'stored_bar') = (baseline_captured_at IS NOT NULL)),
    CONSTRAINT strategy_intraday_reobserved_bars_price_changed_derived
        CHECK (price_changed = (observed_open    IS DISTINCT FROM baseline_open
                             OR observed_high    IS DISTINCT FROM baseline_high
                             OR observed_low     IS DISTINCT FROM baseline_low
                             OR observed_close   IS DISTINCT FROM baseline_close)),
    CONSTRAINT strategy_intraday_reobserved_bars_volume_changed_derived
        CHECK (volume_changed = (observed_volume IS DISTINCT FROM baseline_volume)),
    CONSTRAINT strategy_intraday_reobserved_bars_observed_ohlc_shape
        CHECK (observed_high >= GREATEST(observed_open, observed_close, observed_low)
           AND observed_low  <= LEAST(observed_open, observed_close, observed_high)),
    CONSTRAINT strategy_intraday_reobserved_bars_baseline_ohlc_shape
        CHECK (baseline_high >= GREATEST(baseline_open, baseline_close, baseline_low)
           AND baseline_low  <= LEAST(baseline_open, baseline_close, baseline_high))
);

COMMENT ON TABLE strategy_intraday_reobserved_bars IS
    'One row per compared bar per provider call. The bracket is (baseline_reobservation_id '
    'parent requested_at, this row''s parent received_at) — two real rows, using the '
    'conservative side of each bound. A divergence is NOT a re-basing, and the claim this '
    'evidence licenses is "no price change detected between these two observations", never '
    '"the provider did not rewrite in that interval". #2840.';

-- The baseline lookup ("what did the last observation of this bar say") and every
-- per-bar readout order by the PARENT's requested_at, so this index carries bar identity
-- and the join column.
CREATE INDEX IF NOT EXISTS idx_strategy_intraday_reobserved_bars_identity
    ON strategy_intraday_reobserved_bars (timeframe, instrument_id, bar_time);

-- Divergences are the product and are rare by construction; a partial index keeps the
-- bracket query off the full agreement log.
CREATE INDEX IF NOT EXISTS idx_strategy_intraday_reobserved_bars_changed
    ON strategy_intraday_reobserved_bars (timeframe, instrument_id, bar_time)
    WHERE price_changed;
