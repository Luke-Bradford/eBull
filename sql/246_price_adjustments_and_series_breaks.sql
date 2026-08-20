-- #2261 (phase 0a of #2240) — the auditable price-adjustment table and the
-- series-break record, per the S7 verdict (#2247) §7 and §8.
--
-- WHY THIS IS NOT A SPLIT DETECTOR. Design-doc decision 8: split adjustment is
-- an auditable TABLE, not a live dependency on the unbuilt #2231 detector.
-- Nothing here requires #2231 to exist. The quarantine rules (sql/247) read raw
-- price_daily only and never read this table; the coupling is one-way and
-- downstream — a quarantined transition that turns out to have an active
-- adjustment row on its date is RECLASSIFIED from `quarantined` to `adjusted`.
-- That is a resolution step, not an input to the classifier.
--
-- WHAT IT IS FOR. S7 measured the corpus as PREDOMINANTLY split-adjusted
-- already (320 of 330 shares-outstanding split signatures pass through
-- price_daily with no level break). The residual is real: 59 instruments carry
-- an unresolved level break with 11,410 bars stranded in a pre-break unit
-- regime. This table is how a factor gets recorded so those bars become
-- joinable again, from whatever source establishes it.

-- ---------------------------------------------------------------
-- price_adjustments — S7 §7 verbatim.
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS price_adjustments (
    adjustment_id    BIGSERIAL PRIMARY KEY,
    instrument_id    BIGINT      NOT NULL REFERENCES instruments(instrument_id),
    effective_date   DATE        NOT NULL,      -- first bar at the NEW scale

    -- FACTOR DIRECTION — pinned here and covered by a table test in BOTH
    -- directions (tests/test_price_adjustments.py). Multiply every bar
    -- STRICTLY BEFORE effective_date by F to express it in the new scale.
    --   1:10 reverse split  => F = 10    (old $1 bar reads $10 on the new scale)
    --   20:1 forward split  => F = 0.05  (old $2000 bar reads $100)
    -- Getting this backwards moves every historical bar the wrong way by
    -- factor SQUARED, and the series still looks internally consistent — the
    -- same inversion Codex caught on the #2231 spec. It is not detectable by
    -- eyeballing a chart, only by a two-direction test.
    factor           NUMERIC(24,12) NOT NULL CHECK (factor > 0),

    kind             TEXT        NOT NULL,      -- split | reverse_split | vendor_rescale
                                                -- | spin_off | adr_ratio | redenomination | unknown
    source           TEXT        NOT NULL,      -- sec_xbrl (#2231) | etoro | operator | derived_turnover
    source_priority  SMALLINT    NOT NULL,      -- arbitration when sources disagree; lower wins
    confidence       TEXT        NOT NULL,      -- confirmed | corroborated | inferred
    detector_version TEXT        NOT NULL,      -- rule-set id + code hash, NOT an int: an int
                                                -- cannot tell you whether two rows were produced
                                                -- by the same code.
    source_event_date DATE,                     -- the corporate action's own date, vs when we saw it
    source_document_id BIGINT,                  -- filing/accession id where the source has one
    observed_at      TIMESTAMPTZ NOT NULL,      -- when the evidence existed (replay pin, not created_at)
    created_by       TEXT        NOT NULL,      -- job name or operator identity
    evidence_json    JSONB,                     -- r_in, turnover ratio, corroborating refs

    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Append-only: corrections SUPERSEDE, they never UPDATE. A backtest run
    -- pins max(adjustment_id) + a run timestamp and replays with
    --   WHERE adjustment_id <= :pinned
    --     AND (superseded_at IS NULL OR superseded_at > :run_ts)
    -- which reconstructs exactly the adjustment state that run saw.
    -- superseded_by alone cannot do that.
    superseded_by    BIGINT      REFERENCES price_adjustments(adjustment_id),
    superseded_at    TIMESTAMPTZ,

    -- Cycle-freedom is enforced by only ever pointing at a STRICTLY LARGER
    -- adjustment_id (the writer's job); this CHECK catches the degenerate
    -- self-reference, which is the one a constraint can see.
    CHECK (superseded_by IS NULL OR superseded_by <> adjustment_id),
    CHECK ((superseded_by IS NULL) = (superseded_at IS NULL)),

    UNIQUE (instrument_id, effective_date, source, detector_version)
);

-- NOT OPTIONAL. Exactly one ACTIVE row per (instrument, effective_date).
-- The read path is `close × ∏ factor over active rows with effective_date > d`,
-- so two active rows from different sources on the same date MULTIPLY together
-- and double-apply. The UNIQUE above cannot prevent that: it is keyed on
-- (source, detector_version) precisely so two sources CAN both record their
-- view — arbitration then supersedes the loser, and this index is what forces
-- the arbitration to have happened.
CREATE UNIQUE INDEX IF NOT EXISTS price_adjustments_active
    ON price_adjustments (instrument_id, effective_date) WHERE superseded_by IS NULL;

-- The read path scans an instrument's active rows with effective_date > bar
-- date. Leading instrument_id, then the date it filters on.
CREATE INDEX IF NOT EXISTS idx_price_adjustments_read
    ON price_adjustments (instrument_id, effective_date);

-- ---------------------------------------------------------------
-- price_series_break — S7 §8.
-- ---------------------------------------------------------------
--
-- Unadjustable history is MARKED, never dropped. Silently dropping the 11,410
-- stranded bars biases the eligible universe, and the bias is invisible in
-- every downstream number it touches.
--
-- History is modelled as SEGMENTS BETWEEN BREAKS, not a single `usable_from`
-- gate. An instrument with three breaks where the middle one resolves has TWO
-- joinable segments and one stranded one; a single gate date discards the
-- joinable pair. The segment derivation lives in
-- app/services/price_quarantine.py::series_segments — this table is only the
-- break record it reads.
CREATE TABLE IF NOT EXISTS price_series_break (
    break_id       BIGSERIAL PRIMARY KEY,
    instrument_id  BIGINT NOT NULL REFERENCES instruments(instrument_id),
    break_date     DATE   NOT NULL,             -- the bar at the NEW scale
    observed_ratio NUMERIC(24,12) NOT NULL,     -- close[t] / close[t-1] AS STORED (not adjusted)
    direction      TEXT   NOT NULL CHECK (direction IN ('up', 'down')),
    rule_version   TEXT   NOT NULL,
    evidence_json  JSONB,
    -- NULL = unresolved = the two sides cannot be joined. Set when an
    -- adjustment row explains the ratio.
    resolved_by    BIGINT REFERENCES price_adjustments(adjustment_id),
    UNIQUE (instrument_id, break_date)
);

CREATE INDEX IF NOT EXISTS idx_price_series_break_unresolved
    ON price_series_break (instrument_id, break_date) WHERE resolved_by IS NULL;
