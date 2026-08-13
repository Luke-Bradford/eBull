-- 251_research_corpus_adj_close_and_quarantine.sql
--
-- #2282 stage 2b — two things the archive's actual shape forced, both
-- discovered by reading the file rather than the dataset card.
--
--
-- 1. WHY `adj_close` IS A SEPARATE COLUMN AND NOT A CHOICE OF `close`
-- ---------------------------------------------------------------------------
-- The HF archive carries BOTH `close` and `adj_close`, with Yahoo's standard
-- semantics — verified empirically, not assumed:
--
--   AAPL 2020-08-27  close 125.01   adj_close 121.2564
--   AAPL 2020-08-31  close 129.04   adj_close 125.1654
--
-- AAPL's unadjusted close on 2020-08-27 was ~$500; the 4:1 split settled
-- 2020-08-31. 125.01 = 500.04/4, so **OHLC are SPLIT-ADJUSTED**. And
-- `adj_close` differs from `close` only for dividend payers (AAPL, CSCO) while
-- being bit-identical for non-payers (AMZN, AAL, BRK-A), so **`adj_close` is
-- split AND dividend adjusted**.
--
-- That is a schema constraint, not a preference. `open`/`high`/`low` carry only
-- the split adjustment, so writing `adj_close` into the `close` column would
-- produce bars whose close sits OUTSIDE [low, high] for every dividend payer —
-- silently, and in a way that breaks every candle-shaped indicator and every
-- containment check (including rule B2 of the #2261 quarantine, which would
-- then fire on the whole dividend-paying universe).
--
-- So `close` stays the split-adjusted price that is consistent with OHLC, and
-- the total-return series gets its own nullable column rather than being
-- discarded. Discarding it would be irreversible without re-loading 25.8M rows.
--
-- ⚠ `research_price_series.adjustment_basis` describes the OHLC columns. A
-- series loaded from this archive is 'split_adjusted'. The #2282 handoff said
-- to record 'unknown' and to "verify it before claiming it" — this is that
-- verification, and it is reproducible from the two AAPL bars above.

ALTER TABLE research_price_daily
    ADD COLUMN IF NOT EXISTS adj_close NUMERIC;

COMMENT ON COLUMN research_price_daily.adj_close IS
    'Split AND dividend adjusted close, where the vendor supplies one. The '
    'OHLC columns carry only the split adjustment (see '
    'research_price_series.adjustment_basis), so adj_close is NOT '
    'interchangeable with close: for a dividend payer it sits outside '
    '[low, high]. Total-return work reads this column; candle-shaped '
    'indicators read close.';

-- ---------------------------------------------------------------------------
-- 2. QUARANTINE VERDICTS FOR THE RESEARCH CORPUS
-- ---------------------------------------------------------------------------
--
-- Same model as sql/247 (#2261) and the SAME RULES — `evaluate_series` in
-- app/services/price_quarantine.py is reused verbatim, not re-implemented.
-- Re-expressing B1-B4/T1-T3 in SQL would be the closed-vocabulary-in-three-
-- places failure the 2026-08-03 session hit: one rule set, one implementation.
--
-- Separate TABLES are unavoidable, though. sql/247's are keyed on
-- `instrument_id` with an FK to `instruments`, and the research corpus is keyed
-- on `series_id` precisely because most of its series have no instruments row
-- (that population IS the eToro-listing-bias measurement — see sql/249). A
-- research verdict therefore has nowhere to live in the 247 tables.
--
-- ⚠ WHY QUARANTINE AND NOT A PRICE FILTER. A hard price floor at ingest would
-- be a survivorship filter wearing a data-quality hat: FRCB's last bar is
-- $0.0004 and `price_daily` already holds 154 rows at `close <= 0`. Failed
-- companies trade at fractions of a cent — that is the signal, not noise. The
-- ingest rejects nothing on price; it records a verdict and counts it.
--
-- Sparse, like 247: a row exists only when it says something.

CREATE TABLE IF NOT EXISTS research_price_quarantine_coverage (
    series_id             BIGINT NOT NULL PRIMARY KEY
        REFERENCES research_price_series(series_id) ON DELETE CASCADE,
    rule_set_version      TEXT    NOT NULL,
    first_bar             DATE    NOT NULL,
    last_bar              DATE    NOT NULL,
    bars_evaluated        INTEGER NOT NULL CHECK (bars_evaluated > 0),
    transitions_evaluated INTEGER NOT NULL CHECK (transitions_evaluated >= 0),
    evaluated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (last_bar >= first_bar)
);

CREATE INDEX IF NOT EXISTS idx_research_quarantine_coverage_version
    ON research_price_quarantine_coverage (rule_set_version);

COMMENT ON TABLE research_price_quarantine_coverage IS
    'Which research series have been evaluated, at which rule-set version. '
    'The verdict tables are SPARSE, so absence of a verdict row means either '
    '"clean" or "never evaluated" — reads must be fail-closed against this '
    'table, exactly as for price_quarantine_coverage (sql/247).';

CREATE TABLE IF NOT EXISTS research_bar_quarantine (
    series_id        BIGINT  NOT NULL
        REFERENCES research_price_series(series_id) ON DELETE CASCADE,
    bar_date         DATE    NOT NULL,
    return_usable    BOOLEAN NOT NULL,
    range_usable     BOOLEAN NOT NULL,
    provisional      BOOLEAN NOT NULL,
    rules            TEXT[]  NOT NULL,
    rule_set_version TEXT    NOT NULL,
    PRIMARY KEY (series_id, bar_date),
    -- Sparse-table invariant, same as price_bar_quarantine: a writer bug that
    -- emits an all-clean row per bar would turn this into a 25.8M-row copy of
    -- research_price_daily and silently change every census denominator.
    CHECK (NOT return_usable OR NOT range_usable OR provisional)
);

CREATE INDEX IF NOT EXISTS idx_research_bar_quarantine_version
    ON research_bar_quarantine (rule_set_version);

CREATE TABLE IF NOT EXISTS research_transition_quarantine (
    series_id        BIGINT  NOT NULL
        REFERENCES research_price_series(series_id) ON DELETE CASCADE,
    bar_date         DATE    NOT NULL,   -- the LATER bar
    prior_date       DATE    NOT NULL,   -- the earlier bar
    observed_ratio   NUMERIC(24,12),
    provisional      BOOLEAN NOT NULL,
    rules            TEXT[]  NOT NULL,
    turnover_ratio   NUMERIC(24,12),
    corroboration    TEXT NOT NULL CHECK (corroboration IN (
                         'spike', 'flat', 'collapse', 'unclassifiable', 'not_applicable')),
    rule_set_version TEXT    NOT NULL,
    PRIMARY KEY (series_id, bar_date),
    CHECK (prior_date < bar_date)
);

CREATE INDEX IF NOT EXISTS idx_research_transition_quarantine_version
    ON research_transition_quarantine (rule_set_version);

COMMENT ON TABLE research_transition_quarantine IS
    'Transition verdicts (T1-T3) for research series, keyed on the LATER bar. '
    'Stores ADMITTED-BACK transitions too (corroboration = spike): a narrowing '
    'gate is measured by what it rejects against what it saw, so the admitted '
    'side is the denominator and cannot be dropped.';

-- The operator-visible rejection census. Fractions of what was actually
-- EVALUATED (the coverage table's denominators), never of whatever happens to
-- be in research_price_daily when somebody looks.
-- ⚠ The verdict counts are LEFT JOINed per series, not computed by correlated
-- subqueries over the whole verdict table. A subquery filtered only on
-- rule_set_version counts every vendor's verdicts and then repeats that global
-- total on each vendor's row — invisible while one vendor is loaded, wrong the
-- day a second one is. Same class as the sql/249 drift-view COALESCE: a census
-- that is right by accident is a census that will be read after the accident
-- stops holding.
CREATE OR REPLACE VIEW research_quarantine_census AS
SELECT
    c.rule_set_version,
    s.vendor,
    count(*)                                        AS series_evaluated,
    sum(c.bars_evaluated)                           AS bars_evaluated,
    sum(c.transitions_evaluated)                    AS transitions_evaluated,
    -- ::bigint because sum() over a bigint yields numeric, and a later
    -- CREATE OR REPLACE cannot change a view column's type.
    coalesce(sum(b.return_unusable), 0)::bigint     AS bars_return_unusable,
    coalesce(sum(b.range_unusable), 0)::bigint      AS bars_range_unusable,
    coalesce(sum(t.quarantined), 0)::bigint         AS transitions_quarantined,
    coalesce(sum(t.admitted_back), 0)::bigint       AS transitions_admitted_back
FROM research_price_quarantine_coverage c
JOIN research_price_series s ON s.series_id = c.series_id
LEFT JOIN (
    SELECT series_id,
           rule_set_version,
           count(*) FILTER (WHERE NOT return_usable) AS return_unusable,
           count(*) FILTER (WHERE NOT range_usable)  AS range_unusable
    FROM research_bar_quarantine
    GROUP BY series_id, rule_set_version
) b ON b.series_id = c.series_id AND b.rule_set_version = c.rule_set_version
LEFT JOIN (
    SELECT series_id,
           rule_set_version,
           count(*) FILTER (WHERE cardinality(rules) > 0) AS quarantined,
           count(*) FILTER (WHERE cardinality(rules) = 0
                              AND corroboration = 'spike') AS admitted_back
    FROM research_transition_quarantine
    GROUP BY series_id, rule_set_version
) t ON t.series_id = c.series_id AND t.rule_set_version = c.rule_set_version
GROUP BY c.rule_set_version, s.vendor;

COMMENT ON VIEW research_quarantine_census IS
    'Research-corpus quarantine outcome per rule-set version and vendor. '
    'Reports the ADMITTED side as well as the rejected side — a narrowing '
    'gate whose census only counts rejections cannot be checked for '
    'over-rejection.';
