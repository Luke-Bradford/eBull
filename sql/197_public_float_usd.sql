-- 197_public_float_usd.sql
--
-- Issue #735 — project DEI EntityPublicFloat into financial_periods as
-- public_float_usd. Reserved by sql/088 (#731), which shipped the four
-- us-gaap ownership columns and split this one out:
--
--   "public_float_usd (DEI EntityPublicFloat) is split out to #735 — its
--    cover-page period_end (issuer Q2-end) does not match the FY anchor,
--    so the existing _derive_periods_from_facts canonical-end filter
--    silently drops it."
--
-- EntityPublicFloat is a 10-K cover-page DEI fact. Its period_end is the
-- issuer's most-recent-Q2-end (the SEC-prescribed public-float "as of"
-- date), NOT fiscal year-end (AAPL FY2025: float period_end 2025-03-28 vs
-- FY anchor 2025-09-27). The matching service change adds an FY-only
-- overlay pass in _derive_periods_from_facts that pulls EntityPublicFloat
-- from the (fiscal_year, 'FY') group's facts AFTER the canonical-end value
-- application, so it never lifts the FY anchor (#558) and never enters the
-- Q4-derivation copy loops.
--
-- Purely additive nullable column on both the raw + canonical tables;
-- existing rows stay NULL until the next normalisation run, which re-reads
-- facts_raw and rewrites the canonical row via the ON CONFLICT update path.
-- USD dollars; AAPL ~3.25e12 → 13 digits, well within NUMERIC(24,2).

ALTER TABLE financial_periods_raw
    ADD COLUMN IF NOT EXISTS public_float_usd NUMERIC(24,2);

ALTER TABLE financial_periods
    ADD COLUMN IF NOT EXISTS public_float_usd NUMERIC(24,2);
