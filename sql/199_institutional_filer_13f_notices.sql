-- 199_institutional_filer_13f_notices.sql
--
-- #1639 — 13F-NT (Notice) supersession capture. Spec:
--   docs/specs/etl/2026-06-15-institutional-13f-nt-supersession.md
--
-- A 13F-NT ("Notice") is the SEC's native anti-double-count primitive: the
-- filer declares it holds NOTHING reportable this quarter — its holdings are
-- reported by OTHER managers (post-reorg sub-entity CIKs, each filing its own
-- 13F-HR). The parent's prior 13F-HR is thereby superseded. Our pipeline
-- ingests 13F-HR only (13F-NT is intentionally absent from _FORM_TO_SOURCE),
-- so we never learn the parent's prior HR is dead and the rollup double-counts
-- the stale parent quarter alongside the post-reorg sub-entity quarters
-- (Vanguard AAPL: 2.86B sh / 19.5% ≈ 2× the real ~9.8%).
--
-- This table is the standalone capture buffer for NT filings, keyed on
-- accession_number (idempotent re-capture). The rollup read excludes a filer's
-- 13F-HR when this table holds an NT for that filer with a LATER period_end
-- (see _read_notice_suppressions / the NOT EXISTS clause in
-- app/services/ownership_rollup.py).
--
-- KEYED ON filer_cik INDEPENDENT OF institutional_filers: some held filer_ciks
-- are absent from that directory (it lags by up to a quarter — it walks only
-- CLOSED quarters' form.idx, and a Q2-open NT is not yet there). We only ever
-- EXCLUDE a holding on positive NT evidence, so a directory gap is safe — it
-- can never cause an over-suppression, only an under-suppression (errs toward
-- the existing non-suppressing behaviour).
--
-- SUPERSESSION AXIS = period_end, NEVER filed_at. A 13F-NT/A amending an OLD
-- quarter can be filed AFTER a filer has resumed holdings reporting — its
-- filed_at is later than the live HR's, but its period is older. A filed_at
-- comparison would wrongly suppress the live HR. period_end ordering is immune
-- to amendment file-time scramble (Codex ckpt-1 HIGH #1). filed_at is retained
-- for audit only and is NOT part of the supersession predicate.

CREATE TABLE IF NOT EXISTS institutional_filer_13f_notices (
    filer_cik         TEXT NOT NULL CHECK (filer_cik ~ '^[0-9]{10}$'),
    accession_number  TEXT NOT NULL,
    period_end        DATE NOT NULL,        -- <periodOfReport> from the NT primary_doc.xml
    form              TEXT NOT NULL CHECK (form IN ('13F-NT', '13F-NT/A')),
    filed_at          TIMESTAMPTZ NOT NULL, -- audit only; NOT used in the supersession predicate
    discovered_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (accession_number)
);

-- Serves the rollup's correlated NOT EXISTS (n.filer_cik = c.filer_cik AND
-- n.period_end > c.period_end) and the suppression-listing companion query.
CREATE INDEX IF NOT EXISTS idx_13f_notices_filer_period
    ON institutional_filer_13f_notices (filer_cik, period_end DESC);
