-- #1687 — guard against future-dated insider transaction dates.
--
-- Source rule: Securities Exchange Act §16(a) + Rule 16a-3(a)
-- (17 CFR 240.16a-3(a)) + Form 4 General Instructions — a Form 4 is due
-- before the end of the 2nd business day following the day the reportable
-- transaction is executed, so the execution date precedes/equals the filing
-- date. EXCEPTION: an early filing (EDGAR ownership XML
-- transactionTimeliness='E'; see sql/057:262-265) may legitimately report a
-- transaction dated after the filing. exercise_date / expiration_date are
-- Table II derivative milestones that are legitimately future and are NOT
-- touched.
--
-- Finding (#1687): the impossible txn_date is a FILER SOURCE TYPO carried
-- verbatim in the raw <transactionDate><value> (75/75 retained payloads
-- among the 80 non-E violators) — re-ingest cannot fix it, so we flag the
-- row (txn_date stays NOT NULL, raw value retained for audit) and the
-- operator-visible readers exclude flagged rows. deemed_execution_date is
-- nullable, so a violation is quarantined to NULL (never invent).

ALTER TABLE insider_transactions
    ADD COLUMN IF NOT EXISTS txn_date_invalid BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN insider_transactions.txn_date_invalid IS
    '#1687 — TRUE when txn_date postdates the SEC filing date (filed_at), '
    'which Rule 16a-3(a) makes impossible for a non-early filing (a filer '
    'source typo). The raw (impossible) txn_date is retained for audit; '
    'operator-visible readers exclude flagged rows. An early filing '
    '(transaction_timeliness=''E'') is exempt — its txn_date may legitimately '
    'postdate the filing.';

-- One-off cleanup: flag existing non-early future-dated rows. Anchored on
-- the manifest filed_at (#1233 canonical); manifest-only is sufficient —
-- 0 of the violators are unjoined to sec_filing_manifest (#1687 full-pop).
UPDATE insider_transactions it
SET txn_date_invalid = TRUE
FROM sec_filing_manifest m
WHERE m.accession_number = it.accession_number
  AND it.txn_date > (m.filed_at AT TIME ZONE 'UTC')::date
  AND it.transaction_timeliness IS DISTINCT FROM 'E'
  AND it.txn_date_invalid = FALSE;

-- One-off cleanup: quarantine impossible deemed_execution_date to NULL.
UPDATE insider_transactions it
SET deemed_execution_date = NULL
FROM sec_filing_manifest m
WHERE m.accession_number = it.accession_number
  AND it.deemed_execution_date > (m.filed_at AT TIME ZONE 'UTC')::date
  AND it.transaction_timeliness IS DISTINCT FROM 'E';
