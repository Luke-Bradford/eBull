-- Migration 036: coverage.filings_status + audit/backfill tracking columns.
--
-- Adds the gate column that downstream thesis / scoring / cascade work
-- (#273, #276) consults via "filings_status = 'analysable'". Classifier
-- values are populated by app.services.coverage_audit.audit_all_instruments
-- (Chunk D); backfill-tracking columns are consumed by Chunk E.
--
-- NULL filings_status is a pre-audit placeholder. Post-first-audit,
-- every tradable instrument's coverage row MUST have one of the six
-- CHECK values. null_anomalies counter in AuditSummary surfaces any
-- regression.

ALTER TABLE coverage
    ADD COLUMN IF NOT EXISTS filings_status TEXT
    CHECK (
        filings_status IS NULL
        OR filings_status IN (
            'analysable',
            'insufficient',
            'fpi',
            'no_primary_sec_cik',
            'structurally_young',
            'unknown'
        )
    );

ALTER TABLE coverage ADD COLUMN IF NOT EXISTS filings_audit_at TIMESTAMPTZ;

ALTER TABLE coverage ADD COLUMN IF NOT EXISTS filings_backfill_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE coverage ADD COLUMN IF NOT EXISTS filings_backfill_last_at TIMESTAMPTZ;
ALTER TABLE coverage ADD COLUMN IF NOT EXISTS filings_backfill_reason TEXT;

CREATE INDEX IF NOT EXISTS idx_coverage_filings_status ON coverage(filings_status);
