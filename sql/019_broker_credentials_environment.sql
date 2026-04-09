-- 019_broker_credentials_environment.sql
--
-- Add environment dimension to broker credential identity (issue #139).
--
-- eToro keys are scoped to both an environment (Demo / Real) and a
-- permission level. The identity tuple becomes
-- (operator_id, provider, label, environment) so demo and real
-- credentials coexist without ambiguity.
--
-- Existing rows are backfilled as 'demo' because the legacy env-var
-- path was demo-only.

ALTER TABLE broker_credentials
    ADD COLUMN IF NOT EXISTS environment TEXT NOT NULL DEFAULT 'demo'
    CHECK (environment IN ('demo', 'real'));

-- Replace the old partial unique index (operator_id, provider, label)
-- with one that includes environment.
DROP INDEX IF EXISTS broker_credentials_unique_active;

CREATE UNIQUE INDEX broker_credentials_unique_active
    ON broker_credentials(operator_id, provider, label, environment)
    WHERE revoked_at IS NULL;
