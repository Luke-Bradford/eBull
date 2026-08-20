-- 230_thesis_break_predicates.sql
--
-- #2012 — machine-checkable thesis break predicates (PR-A).
-- Spec: docs/proposals/thesis/2026-07-16-thesis-break-predicates.md.
--
-- Two tables:
--   thesis_break_predicates — one row per machine-checkable condition extracted
--     from a thesis's break_conditions_json (index-aligned; prose conditions get
--     no row). baseline_state implements the arm/baseline model: a predicate
--     that is TRUE at its contemporaneous first evaluation is the writer's own
--     premise ('already_true') and can never fire; only 'armed' predicates fire,
--     on a genuine false→true transition.
--   thesis_break_events — at most ONE event per predicate per thesis version
--     (UNIQUE (thesis_id, predicate_index)); theses are append-only, so a break
--     begets a new thesis with fresh predicates. inputs_json carries per-input
--     {value, as_of, source} evidence — a ratio metric has independently
--     bounded inputs, and a single scalar as-of cannot audit a conjunctive
--     freshness invariant.
--
-- threshold is NULL exactly for the two REGIME metrics (column-vs-column
-- comparisons: price_vs_sma200, sma_50_vs_sma_200); every threshold metric
-- carries a number.

BEGIN;

CREATE TABLE IF NOT EXISTS thesis_break_predicates (
    thesis_id        BIGINT      NOT NULL REFERENCES theses (thesis_id) ON DELETE CASCADE,
    predicate_index  SMALLINT    NOT NULL,
    instrument_id    BIGINT      NOT NULL REFERENCES instruments (instrument_id),
    metric           TEXT        NOT NULL CHECK (metric IN
        ('short_interest_pct_shares_out', 'short_interest_days_to_cover',
         'short_interest_change_pct', 'altman_z', 'rsi_14',
         'price_vs_sma200', 'sma_50_vs_sma_200')),
    op               TEXT        NOT NULL CHECK (op IN ('<', '>')),
    threshold        NUMERIC(18, 6),
    unit             TEXT        NOT NULL,
    source_text      TEXT        NOT NULL,
    baseline_state   TEXT        NOT NULL DEFAULT 'pending' CHECK (baseline_state IN
        ('pending', 'armed', 'already_true', 'already_true_after_gap')),
    baselined_at     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (thesis_id, predicate_index),
    CHECK ((threshold IS NULL) = (metric IN ('price_vs_sma200', 'sma_50_vs_sma_200')))
);

CREATE INDEX IF NOT EXISTS idx_thesis_break_predicates_instrument
    ON thesis_break_predicates (instrument_id);

CREATE TABLE IF NOT EXISTS thesis_break_events (
    break_event_id   BIGSERIAL   PRIMARY KEY,
    thesis_id        BIGINT      NOT NULL,
    predicate_index  SMALLINT    NOT NULL,
    instrument_id    BIGINT      NOT NULL REFERENCES instruments (instrument_id),
    metric           TEXT        NOT NULL,
    op               TEXT        NOT NULL,
    threshold        NUMERIC(18, 6),
    observed_value   NUMERIC(18, 6) NOT NULL,
    observed_as_of   DATE,
    inputs_json      JSONB       NOT NULL,
    fired_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (thesis_id, predicate_index),
    FOREIGN KEY (thesis_id, predicate_index)
        REFERENCES thesis_break_predicates (thesis_id, predicate_index)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_thesis_break_events_instrument_fired
    ON thesis_break_events (instrument_id, fired_at DESC);

COMMIT;
