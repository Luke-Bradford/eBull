-- 098_def14a_drift_alerts.sql
--
-- Issue #769 PR 3 of N — schema for DEF 14A vs Form 4 drift
-- alerts. The drift detector compares each named insider's DEF 14A
-- snapshot (most-recent-per-holder beneficial-ownership row) against
-- the equivalent Form 4 cumulative running total + Form 3 baseline
-- (#768 PR 4) and writes a row here when the absolute drift exceeds
-- the warning threshold.
--
-- Why this matters operationally:
--
--   * DEF 14A is the SEC-authoritative annual reconciliation point.
--     If our Form 4 + Form 3 cumulative for a named officer disagrees
--     with the proxy by >5%, either we missed a Form 4 transaction,
--     mis-classified a baseline row, or the insider has off-platform
--     activity (gifts, family-trust transfers) that's invisible to
--     Form 4. All three deserve operator attention.
--   * 13D/G blockholders (#766) are validated the same way — Item 12
--     lists 5%+ holders independently of the holders' own filings,
--     so a blockholder visible on the proxy but missing from
--     ``blockholder_filings`` is a coverage gap.
--   * The ops monitor (#13) reads this table to render a per-issuer
--     "reconciliation health" indicator.
--
-- Schema decisions:
--
--   * ``severity`` is a constrained text enum, mirroring the pattern
--     used by ``ingest_log`` rows elsewhere in the codebase. Values:
--       - ``info``     — no Form 4 filer matched (coverage gap).
--       - ``warning``  — drift >= 5% (the issue's flag threshold).
--       - ``critical`` — drift >= 25% (likely missed transaction or
--         systematic mis-mapping).
--   * ``matched_filer_cik`` is nullable: when the DEF 14A holder
--     name cannot be matched to any Form 4 filer, the alert is
--     still emitted (severity=info) so the operator sees the gap.
--     Per #769 the holder→CIK auto-resolution is explicitly out of
--     scope for v1; a curated mapping seed table is a follow-up.
--   * ``def14a_shares`` and ``form4_cumulative`` are both
--     NUMERIC(24, 4) to match the source columns
--     (``def14a_beneficial_holdings.shares`` and
--     ``insider_transactions.shares``) so cross-source diffs stay
--     arithmetic-clean.
--   * ``drift_pct`` is NUMERIC(10, 4) to fit a four-decimal
--     percentage (e.g. ``0.0537`` for 5.37% drift, or ``5.37`` —
--     the detector stores the percent-of-DEF14A as a fraction so
--     ``0.05`` = 5%; the operator UI multiplies by 100 for display).
--   * Identity / dedupe = ``(instrument_id, holder_name,
--     accession_number)`` — re-running the detector on the same
--     accession should UPSERT the alert row in place rather than
--     stack duplicates. UPDATE refreshes ``detected_at`` so the
--     ops monitor sees the latest evaluation timestamp.
--
-- _PLANNER_TABLES in tests/fixtures/ebull_test_db.py is updated in
-- the same PR per the prevention-log entry.

CREATE TABLE IF NOT EXISTS def14a_drift_alerts (
    alert_id            BIGSERIAL PRIMARY KEY,
    instrument_id       BIGINT NOT NULL REFERENCES instruments(instrument_id),
    holder_name         TEXT NOT NULL,
    matched_filer_cik   TEXT,
    def14a_shares       NUMERIC(24, 4),
    form4_cumulative    NUMERIC(24, 4),
    drift_pct           NUMERIC(10, 4),
    severity            TEXT NOT NULL
        CHECK (severity IN ('info', 'warning', 'critical')),
    accession_number    TEXT NOT NULL,
    as_of_date          DATE,
    detected_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Idempotent re-detection: re-running the detector on the same
-- accession promotes the existing alert row in place rather than
-- stacking duplicates each time.
CREATE UNIQUE INDEX IF NOT EXISTS uq_def14a_drift_alerts_holder_accession
    ON def14a_drift_alerts (instrument_id, holder_name, accession_number);

-- Hot path: per-instrument alert reader for the ops monitor view —
-- "which issuers have open drift alerts, ordered most recent first".
CREATE INDEX IF NOT EXISTS idx_def14a_drift_alerts_instrument_detected
    ON def14a_drift_alerts (instrument_id, detected_at DESC);

-- Hot path: per-severity scan for the ops monitor's
-- "any open critical alerts?" indicator.
CREATE INDEX IF NOT EXISTS idx_def14a_drift_alerts_severity_detected
    ON def14a_drift_alerts (severity, detected_at DESC);
