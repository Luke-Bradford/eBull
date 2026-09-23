-- #2182 part B — per-row balance-sheet provenance on financial_periods_raw.
--
-- Spec: docs/proposals/etl/2026-09-23-2182-p2-balance-sheet-provenance.md.
--
-- Reg S-X 3-01(a) balance sheets cover two fiscal years, 3-02(a) income statements
-- three. With financial_facts_raw retention-swept to the latest 3 10-Ks, the oldest
-- retained 10-K's earliest income-statement year derives a FY row with no presented
-- balance sheet. FALSE marks those rows so the canonical merge keeps the durable
-- balance-sheet cells instead of overwriting them with NULL.
--
-- DEFAULT TRUE = the pre-existing overwrite behaviour for every row already stored;
-- the next re-normalize (DELETE-then-INSERT per instrument) writes the real flag.

ALTER TABLE financial_periods_raw
    ADD COLUMN IF NOT EXISTS balance_sheet_presented BOOLEAN NOT NULL DEFAULT TRUE;
