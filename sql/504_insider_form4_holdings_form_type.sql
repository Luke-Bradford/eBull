-- 504_insider_form4_holdings_form_type.sql
--
-- #3227 — a Form 5's Table I holding lines join a Form 4's in ``insider_form4_holdings``.
-- Spec: docs/specs/ownership/2026-10-18-3227-form5-holdings.md, Change 2.
--
-- ``holding_form_type`` is the line's ``transactionCoding/transactionFormType``. On a Form 5 a "3"
-- marks a holding that should have been reported on a Form 3 (Form 5 Instruction 8), whose as-of
-- date the form leaves open, so the observation writers and the rollup's component scope refuse it.
-- NULL when absent, which is the Form 4 case. ``insider_filings.document_type`` records the form.

ALTER TABLE insider_form4_holdings ADD COLUMN IF NOT EXISTS holding_form_type TEXT;

COMMENT ON TABLE insider_form4_holdings IS
    'Form 4/4A and Form 5/5A Table I and II holding lines (no transaction, column 5 only), one row '
    'per line. Not Form 3 baselines: those are insider_initial_holdings. #3227.';
