-- 089_xbrl_tier1_tier2_columns.sql
--
-- Issue #732 — extend the XBRL allowlist with the Tier 1 + Tier 2
-- concepts identified by the 2026-05-01 coverage audit. These all
-- live in financial_facts_raw today (per #451 Phase A the extractor
-- already captures every concept) but are dropped at normalisation
-- because TRACKED_CONCEPTS lacks the alias.
--
-- Tier 1 (operationally load-bearing for working-capital + liquidity):
--   AssetsCurrent                                                 -> assets_current        (BS, NUMERIC)
--   LiabilitiesCurrent                                            -> liabilities_current   (BS, NUMERIC)
--   CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents -> cash_restricted       (BS, NUMERIC)
--   ComprehensiveIncomeNetOfTax                                   -> comprehensive_income  (P&L flow)
--   AmortizationOfIntangibleAssets                                -> intangible_amortization (P&L flow)
--
-- Tier 2 (useful, secondary):
--   DeferredIncomeTaxExpenseBenefit                                  -> deferred_income_tax       (P&L flow)
--   OtherNonoperatingIncomeExpense                                   -> other_nonoperating_income (P&L flow)
--   AdditionalPaidInCapital                                          -> additional_paid_in_capital (BS)
--   AccumulatedOtherComprehensiveIncomeLossNetOfTax                  -> accumulated_oci           (BS)
--   AntidilutiveSecuritiesExcludedFromComputationOfEarningsPerShareAmount
--                                                                    -> antidilutive_securities  (weighted-average count)
--
-- cash_restricted is added as a SEPARATE column from existing `cash`
-- because the FASB ASU 2016-18 concept includes restricted cash by
-- definition while the legacy CashAndCashEquivalentsAtCarryingValue
-- excludes it. Mixing them in one column would corrupt the time
-- series. Migration of `cash` callers to the broader concept is a
-- follow-up not in scope here.
--
-- antidilutive_securities is a weighted-average share count, not a
-- balance-sheet stock; classified as point-in-time (BS) for the Q4
-- derivation copy + TTM "latest quarter" aggregation, mirroring how
-- shares_basic / shares_diluted are handled (see comments on
-- _FLOW_COLUMNS in app/services/fundamentals.py).
--
-- Backfill plan: purely additive (new nullable columns); no data
-- movement. Existing rows stay NULL until the next
-- normalize_financial_periods run, which re-derives via the
-- ON CONFLICT DO UPDATE branch in _canonical_merge_instrument
-- (verified against the same path tested in PR #737 / #731).

ALTER TABLE financial_periods_raw
    ADD COLUMN IF NOT EXISTS assets_current             NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS liabilities_current        NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS cash_restricted            NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS comprehensive_income       NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS intangible_amortization    NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS deferred_income_tax        NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS other_nonoperating_income  NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS additional_paid_in_capital NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS accumulated_oci            NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS antidilutive_securities    NUMERIC(20,0);

ALTER TABLE financial_periods
    ADD COLUMN IF NOT EXISTS assets_current             NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS liabilities_current        NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS cash_restricted            NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS comprehensive_income       NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS intangible_amortization    NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS deferred_income_tax        NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS other_nonoperating_income  NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS additional_paid_in_capital NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS accumulated_oci            NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS antidilutive_securities    NUMERIC(20,0);
