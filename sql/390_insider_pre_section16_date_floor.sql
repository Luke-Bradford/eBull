-- #2441 — a Section 16 date that predates Section 16.
--
-- Source rule: Securities Exchange Act §16, 15 U.S.C. §78p — "June 6, 1934,
-- ch. 404, title I, §16, 48 Stat. 896". §16(a)(1) attaches the reporting duty
-- to an insider of a class of equity security registered under §12; a Form
-- 3/4/5 reports events in that capacity. Neither the class, the capacity nor
-- the duty existed before the statute, so a reported §16 date before
-- 1934-06-06 is impossible on its face. The enactment date is deliberately the
-- WEAKEST anchor available: it cannot reject a date a tighter one would allow.
--
-- Finding (#2441, full population 2026-09-17): 20 rows carry a two-digit year
-- parsed literally — 0023-06-23 in a filing accepted 2024-06-07, and so on.
-- The typo is the FILER's, carried verbatim: for all 20 of 20 rows the row's
-- own date string appears inside the retained primary payload, and EDGAR
-- accepted and still serves it (0001434728-24-000220 carries
-- <transactionDate><value>0023-06-23</value></transactionDate>). Re-ingest
-- cannot fix it — flag the row, keep the raw value (txn_date is NOT NULL), and
-- let the operator-visible readers exclude it, exactly as #1687 did above.
--
-- ⚠ TWO deliberate differences from sql/205, which implements the upper bound:
--   1. NO manifest join. The floor needs no filing anchor, so it also reaches
--      rows whose filed_at cannot be resolved.
--   2. NO transaction_timeliness='E' exemption. Whatever 'E' means — and
--      sql/057 contradicts itself while EDGAR Ownership XML Tech Spec §4.3.8.2
--      supports neither reading (#2790) — it cannot make a pre-1934 date
--      possible.
--
-- ⛔ NOT touched: exercise_date / expiration_date. 12 and 24 rows carry the
-- same two-digit shape, but those are Table II security TERMS (Form 4 General
-- Instruction 4(c)), not §16 event dates, and no source rule authorises
-- nulling them. Measured and recorded on #2441 instead of mutated.

COMMENT ON COLUMN insider_transactions.txn_date_invalid IS
    '#1687 + #2441 — TRUE when txn_date is impossible under Section 16: it '
    'either postdates the SEC filing date (filed_at), which Rule 16a-3(a) '
    'forbids for a non-early filing, or it predates the enactment of Section '
    '16 itself (1934-06-06, 48 Stat. 896). Both are filer source typos. The '
    'raw txn_date is retained for audit; operator-visible readers exclude '
    'flagged rows. The UPPER bound exempts an early filing '
    '(transaction_timeliness=''E'') and rows with no resolvable filed_at; the '
    'LOWER bound exempts nothing, on any form.';

-- One-off cleanup: flag existing pre-enactment rows. Preserves rows already
-- flagged by sql/205 (the AND ... = FALSE guard), and touches nothing at or
-- after the boundary date.
UPDATE insider_transactions
SET txn_date_invalid = TRUE
WHERE txn_date < DATE '1934-06-06'
  AND txn_date_invalid = FALSE;

-- One-off cleanup: quarantine an impossible deemed_execution_date to NULL
-- (nullable ⇒ quarantine, never invent — #1687). Stated INDEPENDENTLY of the
-- statement above: a row whose txn_date is valid can still carry a bad deemed
-- date, and a row flagged above can still carry a good one. 0 rows on dev at
-- write time (min = 2015-04-15); the statement exists so the invariant holds
-- on any database, not because it has work to do here.
UPDATE insider_transactions
SET deemed_execution_date = NULL
WHERE deemed_execution_date < DATE '1934-06-06';

-- One-off cleanup: tombstone the observation rows that #1687's contract would
-- never have written. sync_insiders filters its INPUT on NOT txn_date_invalid
-- and upserts the survivors — it never retracts — so flagging a transaction
-- leaves any observation already derived from it live forever.
--
-- ⚠ known_to = NOW(), NEVER a hard delete: invariant I6
-- (app/services/ownership_observations.py:455, "never hard-delete
-- observations"). That is also what makes this reversible.
--
-- Scoped to the insider §16 sources explicitly rather than to the table name:
-- the same table carries other provenance, and the statutory rule is a claim
-- about Section 16 filings, not about every row that happens to live here.
-- All 38 live pre-enactment rows on dev are source='form4' (5 written by the
-- XML path, 32 DERA ':NDT:', 1 DERA ':NDH:'). They are tombstoned together:
-- the rule is a property of the DATE, so leaving the 33 bulk-written ones live
-- would be an asymmetry with no source rule behind it.
--
-- 0 of the 38 reach ownership_insiders_current today, so no operator-visible
-- figure moves. The point is forward-looking: refresh_insiders_current picks a
-- winner per (instrument, holder, nature) by date, and a live impossible row
-- is eligible to become that winner as soon as the rows outranking it are
-- superseded or retention-dropped. Per I6 the caller must then run
-- refresh_insiders_current for the affected instruments; that is executed as
-- the #2441 backfill step and recorded on the PR.
UPDATE ownership_insiders_observations
SET known_to = NOW()
WHERE period_end < DATE '1934-06-06'
  AND known_to IS NULL
  AND source IN ('form3', 'form4');
