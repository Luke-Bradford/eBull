-- 120_data_freshness_index.sql
--
-- Issue #865 / spec §"data_freshness_index"
-- (``docs/superpowers/specs/2026-05-04-etl-coverage-model.md``).
--
-- Subject-polymorphic poll scheduler. One row per (subject, source)
-- triple tracking when to next ASK SEC about new filings for that
-- subject. Distinct from ``sec_filing_manifest`` which tracks
-- per-accession lifecycle: this answers "should I poll?", that one
-- answers "have I seen this accession?".
--
-- Why subject-polymorphic? 13F-HR is filer-centric (BlackRock files;
-- AAPL doesn't), so an (instrument_id, source) shape would be wrong
-- for that source. The scheduler row carries:
--   - subject_type: 'issuer' | 'institutional_filer' |
--                   'blockholder_filer' | 'fund_series' | 'finra_universe'
--   - subject_id:   string identifier (instrument_id for issuers,
--                   filer CIK for institutions, series id for funds,
--                   'FINRA_SI' singleton for short interest)
--   - cik:          denormalised for fast filtering (NULL only for
--                   the FINRA universe singleton)
--   - instrument_id: convenience FK back to ``instruments``, non-null
--                    only for issuer-scoped subjects
--
-- The scheduler does NOT track per-accession state — that lives in
-- ``sec_filing_manifest``. ``last_known_filing_id`` is the steady-
-- state pointer to "newest accession we've observed for this
-- (subject, source)" — used to short-circuit submissions.json polls
-- (Codex review v2: ``check_freshness`` takes this as an argument).
--
-- States (per spec):
--   unknown                  — never polled
--   current                  — last poll = no new + within cadence
--   expected_filing_overdue  — past expected_next_at without new
--   never_filed              — inferred from history; rechecked annually
--   error                    — last poll failed (rate limit / 404 / parse)

BEGIN;

CREATE TABLE data_freshness_index (
    subject_type            TEXT NOT NULL CHECK (subject_type IN (
        'issuer',
        'institutional_filer',
        'blockholder_filer',
        'fund_series',
        'finra_universe'
    )),
    subject_id              TEXT NOT NULL,
    cik                     TEXT,
    instrument_id           BIGINT REFERENCES instruments(instrument_id) ON DELETE CASCADE,
        -- ON DELETE CASCADE not SET NULL: the issuer-row CHECK below
        -- requires non-null instrument_id, and SET NULL would violate
        -- it. CASCADE is also semantically correct — if the instrument
        -- is deleted the scheduler row is meaningless.
    source                  TEXT NOT NULL CHECK (source IN (
        'sec_form3', 'sec_form4', 'sec_form5',
        'sec_13d', 'sec_13g',
        'sec_13f_hr',
        'sec_def14a',
        'sec_n_port', 'sec_n_csr',
        'sec_10k', 'sec_10q', 'sec_8k',
        'sec_xbrl_facts',
        'finra_short_interest'
    )),
    last_known_filing_id    TEXT,
        -- Newest accession observed for this (subject, source) in
        -- steady-state. NULL until first poll lands a result.
    last_known_filed_at     TIMESTAMPTZ,
    last_polled_at          TIMESTAMPTZ,
        -- Codex review v2 finding 5: nullable. ``never`` outcome rows
        -- (seeded from tombstones / first install) carry NULL here —
        -- no fake timestamp masquerading as a real poll.
    last_polled_outcome     TEXT NOT NULL DEFAULT 'never' CHECK (last_polled_outcome IN (
        'current',
        'new_data',
        'error',
        'never'
    )),
    new_filings_since       INTEGER NOT NULL DEFAULT 0,
    expected_next_at        TIMESTAMPTZ,
        -- Predicted next filing time. Per-source cadence calculator
        -- in ``app/services/data_freshness.py`` derives this from
        -- ``last_known_filed_at`` + the source's typical cadence.
        -- Worker filters ``WHERE expected_next_at <= NOW()`` to find
        -- subjects due for poll.
    next_recheck_at         TIMESTAMPTZ,
        -- Explicit recheck cadence for ``never_filed`` / ``error``
        -- states. AAPL has never filed DEF 14C; rather than poll
        -- weekly to confirm absence, we set next_recheck_at = +1 year
        -- and skip until then.
    state                   TEXT NOT NULL DEFAULT 'unknown' CHECK (state IN (
        'unknown',
        'current',
        'expected_filing_overdue',
        'never_filed',
        'error'
    )),
    state_reason            TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (subject_type, subject_id, source),
    -- Issuer subjects must carry an instrument_id; non-issuer
    -- subjects must NOT (the issuer dimension is per-holding inside
    -- the body of 13F / N-PORT etc.).
    CONSTRAINT chk_freshness_issuer_has_instrument CHECK (
        (subject_type = 'issuer' AND instrument_id IS NOT NULL)
        OR (subject_type <> 'issuer' AND instrument_id IS NULL)
    )
);

-- Worker queue: subjects past their expected-next-at, in the active
-- polling states. Codex review v3 finding 4: ``unknown`` MUST be
-- included so reset-by-rebuild rows drain immediately.
CREATE INDEX idx_freshness_due_for_poll
    ON data_freshness_index (expected_next_at, source)
    WHERE state IN ('unknown', 'current', 'expected_filing_overdue');

-- Recheck queue: never_filed / error states with their own cadence.
CREATE INDEX idx_freshness_recheck
    ON data_freshness_index (next_recheck_at, source)
    WHERE state IN ('never_filed', 'error');

-- Issuer convenience index for "every (source, state) for this
-- instrument" lookups during rebuild + operator audits.
CREATE INDEX idx_freshness_by_instrument
    ON data_freshness_index (instrument_id, source)
    WHERE instrument_id IS NOT NULL;

-- CIK lookup for daily-index reconciliation: when daily index emits a
-- (cik, accession), we may want the matching scheduler row for outcome
-- updating without a polymorphic join.
CREATE INDEX idx_freshness_by_cik
    ON data_freshness_index (cik, source)
    WHERE cik IS NOT NULL;

-- Touch updated_at on every UPDATE.
CREATE OR REPLACE FUNCTION data_freshness_index_touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_data_freshness_index_touch
    BEFORE UPDATE ON data_freshness_index
    FOR EACH ROW
    EXECUTE FUNCTION data_freshness_index_touch_updated_at();

COMMIT;
