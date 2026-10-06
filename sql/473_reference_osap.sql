-- 473_reference_osap.sql
--
-- #3623 — admit Chen & Zimmermann's Open Source Asset Pricing (openassetpricing.com) to the #2912 immutable
-- reference-data store: the monthly long-short returns of every predictor, each following its original paper
-- (`PredictorLSretWide.csv`). They add published counterparts no loaded library has, such as short interest
-- for #3621's avoidance filters (JKP has MAX as `rmax1_21d`, but no short-interest factor). Only the source
-- CHECK changes.

BEGIN;

ALTER TABLE reference_data_snapshots DROP CONSTRAINT reference_data_snapshots_source_check;
ALTER TABLE reference_data_snapshots ADD CONSTRAINT reference_data_snapshots_source_check
    CHECK (source IN ('kenneth_french', 'aqr', 'fred', 'global_q', 'jkp', 'federal_reserve', 'osap'));

COMMIT;
