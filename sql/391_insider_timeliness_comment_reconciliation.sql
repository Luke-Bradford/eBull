-- 391_insider_timeliness_comment_reconciliation.sql
--
-- #2790 — reconcile sql/057's two contradictory descriptions of
-- ``insider_transactions.transaction_timeliness``, against the SEC's own
-- specification.
--
-- ``sql/057:264`` (inline comment):  "``E`` = filed early (before the event)"
-- ``sql/057:338`` (column COMMENT):  "``E`` = early (filed before the deadline)"
--
-- The first is wrong and is the one #1687's date-validity exemption cited. EDGAR
-- Ownership XML Technical Specification v5.1 §3.6.8 gives the whole vocabulary —
-- ``E`` Early / ``L`` Late / empty On-time — and §4.3.8.2 says what "early"
-- attaches to on a Form 4 submission:
--
--   "The <transactionFormType> is mandatory and must be '4' or '5.' … By
--    definition, a '4' transaction is on time. Provide no value for this case.
--    By definition, a '5' transaction is early. You do not have to provide a
--    value of 'E,' but you can if you wish. A value other than 'E' will cause a
--    SUSPENSE error."
--
-- So "early" is measured against Rule 16a-3(f)'s Form 5 deadline (45 days after
-- fiscal year end), NOT against the transaction. The letter is an OPTIONAL
-- marker of a form-type-5 line, which is why ``transaction_timeliness`` alone
-- is the wrong key for that exemption — see
-- ``insider_transactions.is_early_form5_line``.
--
-- ⚠ sql/057 itself is NOT edited: it is applied, and ``migrations.py:188``
-- raises ``Migration content drift`` when an applied file's sha256 moves.
-- Forward-only correction of the COMMENT is the available form of the fix.

COMMENT ON COLUMN insider_transactions.transaction_timeliness IS
    'EDGAR Ownership XML Tech Spec v5.1 §3.6.8 TimelinessList: "E" = Early, '
    '"L" = Late, NULL/empty = On-time. ⚠ "Early" is measured against the '
    'reported line''s OWN deadline, not against the transaction: per §4.3.8.2 a '
    '"4" transaction is on-time by definition and a "5" transaction is early by '
    'definition, so on a Form 4 submission "E" marks a Form-5-eligible line '
    '(Rule 16a-3(f), 45 days after fiscal year end) volunteered ahead of that '
    'deadline. It does NOT mean "filed before the event" — sql/057''s inline '
    'comment said so and was wrong (#2790). The letter is OPTIONAL on such a '
    'line, so it under-counts them; transactionFormType is the deciding field. '
    'Late filings remain a reporting-discipline signal.';
