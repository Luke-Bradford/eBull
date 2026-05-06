-- 128_broker_credentials_health_state.sql
--
-- Issue #975 (parent #974) — Credential health as scheduling pre-condition.
--
-- Spec: docs/superpowers/specs/2026-05-06-credential-health-precondition-design.md
--
-- ## Why
--
-- Today, when an operator's eToro keys are wrong, the orchestrator
-- keeps firing every credential-using batch on schedule, the
-- WebSocket subscriber reconnects every 5s with stale in-memory keys,
-- and the admin Problems panel cascades N copies of the same root
-- 401. Saving corrected keys does not feed back into the scheduler
-- or WS — the operator has to manually click Sync now.
--
-- This migration introduces row-level credential health that the
-- orchestrator pre-flight gate, the WS subscriber, and the admin UI
-- all observe. Aggregate operator health is computed in code from
-- the row-level columns; see app/services/credential_health.py.
--
-- ## Columns added to broker_credentials
--
--   * health_state             — UNTESTED / VALID / REJECTED.
--                                Default UNTESTED so existing rows
--                                naturally start at "we have not
--                                proven they work yet".
--   * health_state_updated_at  — last time the row's health changed.
--                                Lets the admin UI render "last
--                                checked 5 minutes ago".
--   * last_health_check_at     — last time ANY auth-using path checked
--                                this row (success or failure).
--                                Distinct from health_state_updated_at
--                                so a no-op same-state check still
--                                touches a timestamp without trigger-
--                                ing a NOTIFY.
--   * last_health_error        — operator-visible string from the most
--                                recent failure. NULL on VALID rows.
--
-- ## operator_credential_health_transitions
--
-- Records the most recent REJECTED -> VALID transition timestamp per
-- operator. The orchestrator's AUTH_EXPIRED failure-row suppression
-- query filters job_runs failures with failed_at < last_recovered_at,
-- so once the operator saves valid keys the cascade of stale 401
-- failure rows stops counting toward the operator-visible streak.
--
-- A row is created on the FIRST REJECTED -> VALID transition for an
-- operator. Missing-row case = "no recovery has ever happened" —
-- callers MUST treat that as "no filter applied", same as
-- last_recovered_at IS NULL.
--
-- ## Backfill
--
-- All existing broker_credentials rows are stamped UNTESTED via the
-- column default. The next call to validate-stored or any auth-using
-- path will write through their real state. No data migration needed
-- for the transitions table — it stays empty until the first recovery.

ALTER TABLE broker_credentials
    ADD COLUMN health_state TEXT NOT NULL DEFAULT 'untested'
        CHECK (health_state IN ('untested', 'valid', 'rejected')),
    ADD COLUMN health_state_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN last_health_check_at TIMESTAMPTZ,
    ADD COLUMN last_health_error TEXT;

-- Index for the operator-level aggregate computation. The aggregate
-- query joins required-labels CTE against this table; the index
-- covers the WHERE operator_id=? AND provider=? AND revoked_at IS NULL
-- + the label/health_state SELECT columns. Partial-WHERE filters out
-- revoked rows since they don't participate in health computation.
CREATE INDEX idx_broker_credentials_operator_health
    ON broker_credentials (operator_id, label, health_state)
    WHERE revoked_at IS NULL;

CREATE TABLE operator_credential_health_transitions (
    operator_id        UUID NOT NULL,
    last_recovered_at  TIMESTAMPTZ,
    PRIMARY KEY (operator_id)
);

COMMENT ON COLUMN broker_credentials.health_state IS
    'untested | valid | rejected. Set by app/services/credential_health.record_row_health_transition.';
COMMENT ON COLUMN broker_credentials.health_state_updated_at IS
    'Last time health_state actually changed. Used by admin UI for "last checked Nm ago".';
COMMENT ON COLUMN broker_credentials.last_health_check_at IS
    'Last time any auth-using path checked this row, regardless of state change.';
COMMENT ON COLUMN broker_credentials.last_health_error IS
    'Operator-visible error from the most recent failure. NULL on VALID rows.';
COMMENT ON TABLE operator_credential_health_transitions IS
    'Tracks last REJECTED -> VALID transition per operator. Used by AUTH_EXPIRED suppression query at orchestrator/ProblemsPanel boundary.';
