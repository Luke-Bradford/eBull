-- #2261 (phase 0a of #2240) — derived bar/transition quarantine verdicts,
-- per the S7 verdict (#2247) §4, §5 and §9.2.
--
-- DERIVED, NEVER IN-PLACE. These are recomputable side tables stamped with the
-- rule-set version that produced them. price_daily is raw vendor data and stays
-- that way: a verdict written onto it could not be replayed at an older rule
-- set, and re-running the rules would be indistinguishable from an ingest.
--
-- TWO VERDICTS PER BAR, NOT ONE (S7 §4). XPER 2024-06-03 is
-- `o 8.497 h 8.737 l 0.010 c 8.298` — the close is perfect and the bar claims
-- the stock traded at one cent. Its RETURNS are fine; every stop-loss in the
-- phase-4 outcome resolver reads as touched. So `return_usable` and
-- `range_usable` are separate columns and separate rule sets:
--   B1 (non-positive/NULL OHLC), B4 (reverting spike)  -> both false
--   B2 (containment), B3 (phantom wick)                -> range only
-- Folding B2/B3 into the return quarantine over-rejected by 587 windows in the
-- first draft of the spike. One verdict class = one column.

-- ---------------------------------------------------------------
-- Coverage — what has been evaluated, at which rule-set version.
-- ---------------------------------------------------------------
--
-- WHY THIS TABLE EXISTS AT ALL. The verdict tables below are SPARSE: they hold
-- only bars/transitions that have something to say (492 and 884 rows on a
-- 3.2M-bar corpus). Absence of a row therefore has TWO possible meanings —
-- "evaluated and clean" or "never evaluated" — and a read path that cannot tell
-- them apart admits precisely the population it has not verified. That is the
-- `NOT (col > 0 AND …) IS NULL` failure mode in table form.
--
-- So the read helper is FAIL-CLOSED against this table: a bar is usable only if
-- its instrument has a coverage row at the current rule-set version AND the bar
-- falls inside [first_bar, last_bar]. Everything else reads UNKNOWN, which is
-- not the same as usable.
CREATE TABLE IF NOT EXISTS price_quarantine_coverage (
    instrument_id     BIGINT NOT NULL PRIMARY KEY REFERENCES instruments(instrument_id),
    rule_set_version  TEXT   NOT NULL,
    first_bar         DATE   NOT NULL,
    last_bar          DATE   NOT NULL,
    bars_evaluated    INTEGER NOT NULL CHECK (bars_evaluated > 0),
    transitions_evaluated INTEGER NOT NULL CHECK (transitions_evaluated >= 0),
    -- Denominators for the operator-visible rejection census. Stored rather
    -- than counted from the sparse tables at read time so the census is a
    -- fraction of what was actually evaluated, not of what happens to be in
    -- price_daily at the moment somebody looks.
    asset_class       TEXT,                  -- as seen at evaluation time; NULL is a real state
    evaluated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (last_bar >= first_bar)
);

CREATE INDEX IF NOT EXISTS idx_price_quarantine_coverage_version
    ON price_quarantine_coverage (rule_set_version);

-- ---------------------------------------------------------------
-- Bar verdicts (B1–B4).
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS price_bar_quarantine (
    instrument_id    BIGINT  NOT NULL REFERENCES instruments(instrument_id),
    price_date       DATE    NOT NULL,
    return_usable    BOOLEAN NOT NULL,
    range_usable     BOOLEAN NOT NULL,
    -- Taxonomy class 8: today's bar is a PARTIAL. AAPL 2026-08-04 carried
    -- volume 87,572 against 53,121,635 the prior day. A verdict that reads
    -- volume on a mid-session bar is wrong, and T3's turnover corroboration
    -- reads volume — so a genuine move today would show turnover ≈0.002, fail
    -- the >2× test and be quarantined as split-like. Provisional bars are
    -- marked and are NEVER verdict-bearing corroboration.
    provisional      BOOLEAN NOT NULL,
    rules            TEXT[]  NOT NULL,   -- which of B1..B4 fired; empty for provisional-only rows
    rule_set_version TEXT    NOT NULL,
    PRIMARY KEY (instrument_id, price_date),
    -- Sparse-table invariant: a row exists only if it says something. Without
    -- this, a writer bug that emits an all-clean row for every bar turns the
    -- table into a 3.2M-row copy of price_daily and the census denominators
    -- silently change meaning.
    CHECK (NOT return_usable OR NOT range_usable OR provisional)
);

CREATE INDEX IF NOT EXISTS idx_price_bar_quarantine_version
    ON price_bar_quarantine (rule_set_version);

-- ---------------------------------------------------------------
-- Transition verdicts (T1–T3).
-- ---------------------------------------------------------------
--
-- THE DEFECT LIVES ON A TRANSITION, NOT A BAR (design-doc decision 10). Bars
-- either side of a level break are valid prices in their own unit regime — it
-- is the ratio between them that is not a return. So the transition is
-- quarantined and both bars are kept.
--
-- Keyed on the LATER bar: a transition is (prior_date -> price_date).
CREATE TABLE IF NOT EXISTS price_transition_quarantine (
    instrument_id    BIGINT  NOT NULL REFERENCES instruments(instrument_id),
    price_date       DATE    NOT NULL,          -- the later bar
    prior_date       DATE    NOT NULL,          -- the earlier bar
    observed_ratio   NUMERIC(24,12),            -- close[t]/close[t-1] as stored; NULL when unmeasurable
    -- A transition touching a provisional bar gets NO T3 verdict — it is
    -- deferred, not quarantined, and recomputed once the bar is final.
    provisional      BOOLEAN NOT NULL,
    rules            TEXT[]  NOT NULL,          -- which of T1..T3 fired
    -- T3's admit-back signal, stored because it is the census's own evidence:
    -- turnover (close × volume) is split-invariant, so a turnover spike says a
    -- level break was a REAL move. It reaches only ~30% of the population
    -- (volume is equity-only, S3), so it is an admit-back signal and a
    -- confidence input — NEVER the gate.
    turnover_ratio   NUMERIC(24,12),
    corroboration    TEXT NOT NULL CHECK (corroboration IN (
                         'spike', 'flat', 'collapse', 'unclassifiable', 'not_applicable')),
    rule_set_version TEXT    NOT NULL,
    PRIMARY KEY (instrument_id, price_date),
    CHECK (prior_date < price_date)
);

CREATE INDEX IF NOT EXISTS idx_price_transition_quarantine_version
    ON price_transition_quarantine (rule_set_version);
