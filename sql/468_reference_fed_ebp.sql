-- 468_reference_fed_ebp.sql
--
-- #3622 slice 2 — archive every release of the Federal Reserve's Excess Bond Premium (Gilchrist & Zakrajšek
-- 2012; FEDS Notes `ebp_csv.csv`) in the #2912 immutable reference store. The whole history is re-estimated
-- at each release, so a release not archived is lost; each distinct response becomes its own snapshot.
-- `gz_spread` and `ebp` are percentage points (percent_per_annum); `est_prob` is a model probability in [0, 1],
-- which needs the new `probability` unit.

BEGIN;

ALTER TABLE reference_data_snapshots DROP CONSTRAINT reference_data_snapshots_source_check;
ALTER TABLE reference_data_snapshots ADD CONSTRAINT reference_data_snapshots_source_check
    CHECK (source IN ('kenneth_french', 'aqr', 'fred', 'global_q', 'jkp', 'federal_reserve'));

ALTER TABLE reference_data_observations DROP CONSTRAINT reference_data_observations_unit_check;
ALTER TABLE reference_data_observations ADD CONSTRAINT reference_data_observations_unit_check
    CHECK (unit IN ('decimal_return', 'percent_per_annum', 'binary_indicator', 'probability'));

COMMIT;
