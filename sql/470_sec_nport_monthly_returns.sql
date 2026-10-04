-- 470_sec_nport_monthly_returns.sql
--
-- #3619 slice 2b — Form N-PORT Item B.5.a monthly total returns, from SEC's quarterly N-PORT data sets
-- (table MONTHLY_TOTAL_RETURN joined to SUBMISSION). Each fund reports, per class, its total return for the three
-- months ending at REPORT_DATE, computed under Form N-1A Item 26(b)(1): NAV-based, distributions reinvested.
-- Read by app/services/etf_total_return_reader.py to extend ETF total return past the Intrader archive (2024-08).
--
-- Key: the data-set readme's (ACCESSION_NUMBER, MONTHLY_TOTAL_RETURN_ID) plus the month position (1..3 =
-- MONTHLY_TOTAL_RETURN1..3, "First/Second/Third Month"). class_id is nullable as in the source ("if any").
-- return_pct is as filed, in percent. A blank cell stores no row. Rows are filings: append-only, an amendment
-- arrives as a new accession. Spec: docs/proposals/etl/2026-10-04-3619-total-return-splice.md §"Slice 2b".

BEGIN;

CREATE TABLE IF NOT EXISTS sec_nport_monthly_returns (
    accession_number         TEXT NOT NULL CHECK (accession_number ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    monthly_total_return_id  BIGINT NOT NULL,
    month_position           SMALLINT NOT NULL CHECK (month_position BETWEEN 1 AND 3),
    class_id                 TEXT CHECK (class_id <> ''),
    month                    DATE NOT NULL CHECK (EXTRACT(DAY FROM month) = 1),
    return_pct               NUMERIC NOT NULL CHECK (return_pct <> 'NaN' AND abs(return_pct) < 'Infinity'),
    report_date              DATE NOT NULL,
    filing_date              DATE NOT NULL,
    sub_type                 TEXT NOT NULL CHECK (sub_type IN ('NPORT-P', 'NPORT-P/A')),
    dataset_quarter          TEXT NOT NULL CHECK (dataset_quarter ~ '^[0-9]{4}q[1-4]$'),
    PRIMARY KEY (accession_number, monthly_total_return_id, month_position)
);

CREATE INDEX IF NOT EXISTS idx_sec_nport_monthly_returns_class_month
    ON sec_nport_monthly_returns (class_id, month)
    WHERE class_id IS NOT NULL;

CREATE OR REPLACE FUNCTION sec_nport_monthly_returns_append_only()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END $$;

DROP TRIGGER IF EXISTS sec_nport_monthly_returns_append_only ON sec_nport_monthly_returns;
CREATE TRIGGER sec_nport_monthly_returns_append_only
    BEFORE UPDATE ON sec_nport_monthly_returns
    FOR EACH ROW EXECUTE FUNCTION sec_nport_monthly_returns_append_only();

COMMENT ON TABLE sec_nport_monthly_returns IS
    'Form N-PORT Item B.5.a monthly total returns per class, percent as filed, one row per data-set row and month '
    'position (#3619 slice 2b). Append-only; the latest filing per (class, month) is resolved at read time.';

COMMIT;
