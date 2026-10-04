-- 472_reference_jkp_cutoff_units.sql
--
-- #3609 step 1 slice 1 — JKP `nyse_cutoffs.csv` enters the #2912 immutable reference store. Its NYSE
-- market-equity percentiles are in million USD (JKP Documentation.pdf, "Market Equity": "quoted in
-- million USD") and its `n` column is a stock count, so the unit CHECK gains `usd_millions` and `count`.

BEGIN;

ALTER TABLE reference_data_observations DROP CONSTRAINT reference_data_observations_unit_check;
ALTER TABLE reference_data_observations ADD CONSTRAINT reference_data_observations_unit_check
    CHECK (unit IN ('decimal_return', 'percent_per_annum', 'binary_indicator', 'probability', 'usd_millions', 'count'));

COMMIT;
