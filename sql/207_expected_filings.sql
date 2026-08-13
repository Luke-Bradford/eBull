-- #1788 (#677 Part B) — expected-filings watchlist for event-driven
-- fundamentals catch-up. One row per instrument = its single *next*
-- expected periodic filing (10-Q or 10-K); the expected_filings_poller
-- force-refreshes fundamentals the moment that filing appears, ahead of
-- the once-a-day daily_financial_facts backstop.
--
-- Source rule (SEC): periodic reports are mandated by Exchange Act
-- Rule 13a-1 (annual 10-K) + Rule 13a-13 (quarterly 10-Q); a domestic
-- issuer files three 10-Qs (fiscal Q1/Q2/Q3) and one 10-K per year — no
-- Q4 10-Q. Statutory deadlines run from FISCAL PERIOD-END (Form 10-K
-- Gen. Instr. A.(2): 60/75/90d; Form 10-Q Gen. Instr. A.(1): 40/45d).
-- The seed derives form + window from financial_periods (period-end
-- anchored), never from prior-filing-date arithmetic. WHEN-to-poll is an
-- operational schedule, not a data-treatment decision — correctness is
-- enforced by exact-form + non-amendment + baseline-accession matching
-- in the poller, not by the window.
--
-- See docs/specs/etl/2026-06-28-expected-filings-poller.md.
--
-- New table name → no prior shape exists, so the prevention-log
-- "pair CREATE with ALTER ADD COLUMN IF NOT EXISTS" self-review resolves
-- clean (there is no earlier expected_filings to reconcile against).
CREATE TABLE IF NOT EXISTS expected_filings (
    id                    BIGSERIAL PRIMARY KEY,
    instrument_id         INT  NOT NULL UNIQUE
                              REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    -- Expected form for the *next* filing; flips '10-Q'<->'10-K' across
    -- the fiscal year as the latest reported period advances.
    expected_filing_type  TEXT NOT NULL CHECK (expected_filing_type IN ('10-Q', '10-K')),
    -- Cycle key: the latest financial_periods.period_end_date the seed
    -- used to derive this expectation. The conditional re-seed rolls the
    -- row forward (and resets fulfilment) only when this advances.
    anchor_period_end     DATE NOT NULL,
    expected_window_start DATE NOT NULL,
    expected_window_end   DATE NOT NULL,
    poll_interval_minutes INT  NOT NULL DEFAULT 30 CHECK (poll_interval_minutes > 0),
    -- Last known non-amendment accession of the expected form; passed to
    -- check_freshness as last_known_filing_id so only a strictly-newer
    -- accession counts as the expected filing (no false-fulfil on the
    -- existing last filing).
    baseline_accession    TEXT,
    last_polled_at        TIMESTAMPTZ,
    fulfilled_at          TIMESTAMPTZ,
    fulfilled_accession   TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (expected_window_end >= expected_window_start)
);

-- Hot path: the poller selects unfulfilled rows whose window is open and
-- whose poll interval has elapsed, most-stale first.
CREATE INDEX IF NOT EXISTS idx_expected_filings_due
    ON expected_filings (expected_window_end, last_polled_at)
    WHERE fulfilled_at IS NULL;

COMMENT ON TABLE expected_filings IS
    '#1788 — per-instrument next-expected periodic filing (10-Q/10-K). '
    'Seeded from financial_periods (period-end anchored) for the operator '
    'high-value set (watchlist + open positions); the expected_filings_poller '
    'force-refreshes fundamentals on the matching filing ahead of the daily path.';
