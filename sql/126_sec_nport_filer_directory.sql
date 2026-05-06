-- 126_sec_nport_filer_directory.sql
--
-- Issue #963 — dedicated N-PORT registered-investment-company (RIC)
-- trust-CIK directory. Sibling of ``institutional_filers`` (#912)
-- but for the disjoint N-PORT filing universe.
--
-- ## Why a separate table
--
-- ``institutional_filers`` holds 13F-HR filer CIKs — the MANAGER
-- entity that crosses the $100M discretionary-AUM threshold (e.g.
-- ``VANGUARD GROUP INC`` cik 0000102909). N-PORT is filed by the
-- RIC TRUST entity that wraps the fund series (e.g.
-- ``VANGUARD INDEX FUNDS`` cik 0000036405 wraps VFIAX, VTSAX, etc.).
-- These are DIFFERENT CIKs in SEC EDGAR — walking the manager's
-- submissions endpoint returns no NPORT-P filings.
--
-- #919 surfaced this empirically: ``sec_n_port_ingest`` job
-- (``app/workers/scheduler.py``) walked ``institutional_filers WHERE
-- filer_type IN ('INV','INS','ETF')`` (11,206 rows on dev) and found
-- zero NPORT-P filings, leaving ``ownership_funds_observations``
-- empty until #919 hardcoded a panel-targeted RIC CIK list in
-- ``.claude/nport-panel-backfill.py``. This table replaces the
-- workaround with a proper standing directory.
--
-- ## Schema decisions
--
-- * ``cik`` is PK (no synthetic id) — sibling tables in this repo
--   use BIGSERIAL but the directory is a flat reference dimension
--   keyed solely on the SEC identifier. CIK PK keeps joins direct.
-- * ``fund_trust_name`` is the canonical trust name from the most
--   recent NPORT-P filing's ``form.idx`` row. Refreshed on every
--   sync so a trust rename propagates.
-- * ``last_seen_period_end`` is the period_of_report of the latest
--   NPORT-P seen for this CIK across all walked quarters. Used by
--   the ingester to bound the per-CIK submissions walk and skip
--   trust-CIKs whose last filing is older than the freshness window.
-- * ``last_seen_filed_at`` is the date_filed of the same. Both come
--   from the SEC quarterly ``form.idx`` (date-only) lifted to
--   midnight UTC for TIMESTAMPTZ comparisons.
-- * No ``filer_type`` column — every N-PORT filer is by definition
--   a RIC trust. Operator runbook can layer additional sub-class
--   tagging (open-end vs closed-end) later if needed; not in scope.
--
-- ``_PLANNER_TABLES`` in tests/fixtures/ebull_test_db.py is updated
-- in the same PR per the prevention-log entry "Test-teardown list
-- missing new FK-child tables".

BEGIN;

CREATE TABLE IF NOT EXISTS sec_nport_filer_directory (
    cik                   TEXT PRIMARY KEY,
    fund_trust_name       TEXT NOT NULL,
    last_seen_period_end  DATE,
    last_seen_filed_at    TIMESTAMPTZ,
    fetched_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Order-by-recency index for the ingester's selector
-- (``ORDER BY last_seen_filed_at DESC NULLS LAST``).
CREATE INDEX IF NOT EXISTS idx_sec_nport_filer_directory_filed
    ON sec_nport_filer_directory (last_seen_filed_at DESC NULLS LAST);

COMMENT ON TABLE sec_nport_filer_directory IS
    'RIC trust-CIK directory for N-PORT ingest (#963). Populated by '
    'sec_nport_filer_directory_sync from SEC quarterly form.idx. '
    'Disjoint from institutional_filers — N-PORT files under trust '
    'CIKs (Vanguard Index Funds, iShares Trust, etc.), 13F-HR files '
    'under manager CIKs (Vanguard Group, BlackRock).';

COMMIT;
