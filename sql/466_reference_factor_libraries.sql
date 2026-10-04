-- 466_reference_factor_libraries.sql
--
-- #3623 — admit two published factor libraries to the #2912 immutable reference-data store, for
-- construction validation (our long-short vs the published series) and adoption-track priors:
--   * global_q — Hou, Mo, Xue & Zhang q5 factors (global-q.org), monthly;
--   * jkp      — Jensen, Kelly & Pedersen factor returns (jkpfactors.com), USA, monthly, capped value weight.
-- The Kenneth French and AQR additions reuse their existing source values. Only the source CHECK changes.

BEGIN;

ALTER TABLE reference_data_snapshots DROP CONSTRAINT reference_data_snapshots_source_check;
ALTER TABLE reference_data_snapshots ADD CONSTRAINT reference_data_snapshots_source_check
    CHECK (source IN ('kenneth_french', 'aqr', 'fred', 'global_q', 'jkp'));

COMMIT;
