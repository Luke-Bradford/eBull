-- 491_tax_evidence_v3.sql
--
-- #3616 slice 3d-3b — position income does not withhold a held share.
-- Spec: docs/specs/metrics/2026-10-14-3616-slice3d3b-position-income.md (Design 2, Storage). Writer:
-- `app.services.tax_broker_evidence` (one row per subject per run, policy `broker_evidence_v3`).
--
-- The positions with a non-zero `totalFees` in any retained sync payload: the predicate of class reason A8
-- (`distribution_character_assumed`, `consideration_classes_v2`). NULL on rows of runs before v3; the v3
-- derivation always writes it, empty when none qualifies.

BEGIN;

ALTER TABLE tax_evidence_subjects ADD COLUMN IF NOT EXISTS total_fees_nonzero_position_ids BIGINT[];

COMMIT;
