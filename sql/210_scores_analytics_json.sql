-- Migration 210: scores.analytics_json — IAR evidence signals (#1823, P2 of #1815)
--
-- One nullable JSONB column carrying the per-instrument-per-run Instrument
-- Analytical Record evidence block: Piotroski F, Altman Z", insider/13F/short
-- positioning signals, and the hybrid peer grade. All EVIDENCE-ONLY — these
-- signals enter the headline composite at weight 0, so total_score is UNCHANGED
-- and model_version is NOT bumped (the same additive-nullable-evidence blessing
-- used for the risk_v1 layer and the #1820 completeness columns; see
-- settled-decisions "Risk-metrics evidence layer").
--
-- Additive + nullable: pre-migration rows keep NULL ("not computed then").
-- Append-only — never mutated, consistent with the scores table contract.
ALTER TABLE scores
    ADD COLUMN IF NOT EXISTS analytics_json JSONB;
