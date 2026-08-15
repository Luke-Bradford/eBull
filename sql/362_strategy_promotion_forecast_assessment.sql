-- 362_strategy_promotion_forecast_assessment.sql
--
-- #2770: a paper approval must pin the exact immutable declaration and
-- prospective assessment it consumed, plus the forward-shadow facts checked
-- against that declaration.  The source rows remain directly resolvable and
-- neither can be deleted behind the audit event.  The redundant identity
-- columns make cross-trial substitution impossible at the FK layer rather
-- than trusting every writer to join the three sources correctly.

ALTER TABLE strategy_promotions
    ADD CONSTRAINT strategy_promotions_forward_evidence_identity
    UNIQUE (promotion_id,strategy_id,strategy_version,to_stage);

ALTER TABLE strategy_forecast_assessments
    ADD CONSTRAINT strategy_forecast_assessments_forward_evidence_identity
    UNIQUE (assessment_id,strategy_id,strategy_version);

-- These tables were documented and consumed as append-only evidence, but the
-- original migrations did not enforce that claim. A mutable promotion time
-- could move the forward boundary after approval; a mutable assessment could
-- flip the exact verdict the approval pins.
CREATE OR REPLACE FUNCTION reject_strategy_promotion_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'strategy promotions are append-only';
END;
$$;

CREATE TRIGGER trg_strategy_promotions_immutable
BEFORE UPDATE OR DELETE ON strategy_promotions
FOR EACH ROW EXECUTE FUNCTION reject_strategy_promotion_change();

CREATE OR REPLACE FUNCTION reject_strategy_forecast_assessment_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'strategy forecast assessments are immutable';
END;
$$;

CREATE TRIGGER trg_strategy_forecast_assessments_immutable
BEFORE UPDATE OR DELETE ON strategy_forecast_assessments
FOR EACH ROW EXECUTE FUNCTION reject_strategy_forecast_assessment_change();

CREATE TABLE IF NOT EXISTS strategy_promotion_forward_evidence (
    promotion_id BIGINT PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    strategy_version TEXT NOT NULL,
    promotion_stage TEXT NOT NULL DEFAULT 'paper_enabled'
        CHECK (promotion_stage = 'paper_enabled'),
    declaration_id BIGINT NOT NULL,
    assessment_id BIGINT NOT NULL,
    forward_resolved_signals INTEGER NOT NULL CHECK (forward_resolved_signals >= 0),
    forward_decision_dates INTEGER NOT NULL CHECK (forward_decision_dates >= 0),
    forward_elapsed_days INTEGER NOT NULL CHECK (forward_elapsed_days >= 0),
    assessed_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT strategy_promotion_forward_evidence_promotion_fk
        FOREIGN KEY (promotion_id,strategy_id,strategy_version,promotion_stage)
        REFERENCES strategy_promotions(promotion_id,strategy_id,strategy_version,to_stage)
        ON DELETE RESTRICT,
    CONSTRAINT strategy_promotion_forward_evidence_declaration_fk
        FOREIGN KEY (declaration_id,strategy_id,strategy_version)
        REFERENCES strategy_preregistration_declarations(declaration_id,strategy_id,strategy_version)
        ON DELETE RESTRICT,
    CONSTRAINT strategy_promotion_forward_evidence_assessment_fk
        FOREIGN KEY (assessment_id,strategy_id,strategy_version)
        REFERENCES strategy_forecast_assessments(assessment_id,strategy_id,strategy_version)
        ON DELETE RESTRICT,
    CONSTRAINT strategy_promotion_forward_evidence_identity_nonempty
        CHECK (strategy_id <> '' AND strategy_version <> '')
);

CREATE INDEX IF NOT EXISTS idx_strategy_promotion_forward_evidence_declaration
    ON strategy_promotion_forward_evidence(declaration_id);
CREATE INDEX IF NOT EXISTS idx_strategy_promotion_forward_evidence_assessment
    ON strategy_promotion_forward_evidence(assessment_id);

COMMENT ON TABLE strategy_promotion_forward_evidence IS
    'Exact preregistration, prospective assessment and frozen forward-shadow facts consumed by a #2770 paper approval.';

CREATE OR REPLACE FUNCTION reject_strategy_promotion_forward_evidence_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'paper-promotion forward evidence is immutable';
END;
$$;

CREATE TRIGGER trg_strategy_promotion_forward_evidence_immutable
BEFORE UPDATE OR DELETE ON strategy_promotion_forward_evidence
FOR EACH ROW EXECUTE FUNCTION reject_strategy_promotion_forward_evidence_change();
