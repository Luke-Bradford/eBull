-- 088_xbrl_ownership_columns.sql
--
-- Issue #731 — project four us-gaap balance-sheet concepts into the
-- canonical financial_periods table to unblock the ownership reporting
-- card (#729): treasury_shares, shares_authorized, shares_issued,
-- retained_earnings.
--
-- All four are top-30 most-frequent XBRL concepts in the SEC
-- companyfacts corpus and currently land in financial_facts_raw but
-- are dropped during normalisation because TRACKED_CONCEPTS lacks the
-- alias. After this migration + the matching service-layer changes,
-- normalize_financial_periods will project them on the next re-derive.
--
-- public_float_usd (DEI EntityPublicFloat) is split out to #735 — its
-- cover-page period_end (issuer Q2-end) does not match the FY anchor,
-- so the existing _derive_periods_from_facts canonical-end filter
-- silently drops it. Designed there once an annual-cover-page path is
-- in place.
--
-- Backfill plan: the migration is purely additive (new nullable
-- columns); no data movement here. Existing rows pre-date the
-- columns and stay NULL until the next normalisation run, which
-- re-reads facts_raw and rewrites the canonical row via the
-- ON CONFLICT update path in _canonical_merge_instrument.

ALTER TABLE financial_periods_raw
    ADD COLUMN IF NOT EXISTS treasury_shares    NUMERIC(20,0),
    ADD COLUMN IF NOT EXISTS shares_authorized  NUMERIC(20,0),
    ADD COLUMN IF NOT EXISTS shares_issued      NUMERIC(20,0),
    ADD COLUMN IF NOT EXISTS retained_earnings  NUMERIC(20,4);

ALTER TABLE financial_periods
    ADD COLUMN IF NOT EXISTS treasury_shares    NUMERIC(20,0),
    ADD COLUMN IF NOT EXISTS shares_authorized  NUMERIC(20,0),
    ADD COLUMN IF NOT EXISTS shares_issued      NUMERIC(20,0),
    ADD COLUMN IF NOT EXISTS retained_earnings  NUMERIC(20,4);
