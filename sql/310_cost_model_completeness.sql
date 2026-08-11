-- Migration 310: distinguish measured transaction costs from legacy zeros.
--
-- A numeric zero cannot say whether a cost was measured as zero or merely
-- omitted.  Existing rows were created while FX was defaulted to zero for USD
-- instruments even though the operator account is GBP-funded.  Keep the
-- numeric columns for compatibility, but make both cost components explicitly
-- unknown until their provenance has been established.

ALTER TABLE cost_model
    ADD COLUMN IF NOT EXISTS carry_cost_known BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS fx_cost_known BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN cost_model.carry_cost_known IS
    'TRUE only when overnight/carry applicability and amount are established for the proposed product posture';
COMMENT ON COLUMN cost_model.fx_cost_known IS
    'TRUE only when the funding currency/path and FX conversion cost are established; numeric zero alone is not evidence';

