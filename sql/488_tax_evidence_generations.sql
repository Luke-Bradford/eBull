-- 488_tax_evidence_generations.sql
--
-- #3616 slice 3c, part 2b-2 — the published generations of tax evidence from the broker record.
-- Spec: docs/specs/metrics/2026-10-13-3616-slice3c-broker-tax-evidence.md (Design 1 `tax_evidence_runs`,
-- `tax_evidence_subjects`, `tax_evidence_lots`; Design 6). Writer: `app.services.tax_broker_evidence.run_tax_broker_evidence`.
--
-- The effective generation is the latest `published` run whose (environment, account_id) equals trade_ledger_scope.
-- Earlier generations stay for audit: subjects and lots are never updated or deleted, and a run row changes only
-- while `in_progress`.

BEGIN;

CREATE TABLE IF NOT EXISTS tax_evidence_runs (
    run_id                     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    environment                TEXT NOT NULL CHECK (environment IN ('demo', 'real')),
    -- From trade_ledger_scope; NULL when there is none.
    account_id                 BIGINT CHECK (account_id > 0),
    started_at                 TIMESTAMPTZ NOT NULL,
    finished_at                TIMESTAMPTZ,
    state                      TEXT NOT NULL CHECK (state IN ('in_progress', 'published', 'refused', 'abandoned')),
    reason                     TEXT,
    -- Watermarks of what the snapshot read.
    trade_events_max_event_id  BIGINT,
    lookups_max_id             BIGINT,
    observations_max_id        BIGINT,
    -- The latest trade_history_fetches row (any outcome) and the latest `ok` one.
    history_fetch_latest_id    BIGINT,
    history_fetch_ok_id        BIGINT,
    fx_run_id                  BIGINT,
    fx_policy_id               TEXT,
    evidence_policy_id         TEXT NOT NULL CHECK (evidence_policy_id <> ''),
    counts                     JSONB,
    lookups_attempted          INTEGER CHECK (lookups_attempted >= 0),
    lookups_deferred           INTEGER CHECK (lookups_deferred >= 0),
    lookups_stopped_by         TEXT,
    CHECK ((state = 'in_progress') = (finished_at IS NULL)),
    CHECK (state <> 'published' OR (account_id IS NOT NULL AND counts IS NOT NULL AND reason IS NULL)),
    CHECK (state NOT IN ('refused', 'abandoned') OR reason IS NOT NULL)
);

-- The session lock covers a whole run; this is the schema's own statement of one run at a time.
CREATE UNIQUE INDEX IF NOT EXISTS uq_tax_evidence_runs_in_progress
    ON tax_evidence_runs ((TRUE)) WHERE state = 'in_progress';
CREATE INDEX IF NOT EXISTS idx_tax_evidence_runs_published
    ON tax_evidence_runs (environment, account_id, run_id) WHERE state = 'published';

CREATE TABLE IF NOT EXISTS tax_evidence_subjects (
    run_id                              BIGINT NOT NULL REFERENCES tax_evidence_runs (run_id),
    subject                             TEXT NOT NULL CHECK (subject ~ '^(order|position):[0-9]+$'),
    order_id                            BIGINT CHECK (order_id > 0),
    etoro_instrument_id                 BIGINT,
    instrument_id                       BIGINT,
    product                             TEXT CHECK (product IN (
                                            'real', 'cfd', 'real_futures', 'margin_trade', 'swap', 'crypto_margin')),
    outcome                             TEXT NOT NULL CHECK (outcome IN (
                                            'admitted', 'excluded_non_share_product', 'unresolved')),
    -- Every failing check, in Design 4 order; empty unless unresolved.
    reasons                             TEXT[] NOT NULL,
    trade_event_ids                     BIGINT[] NOT NULL,
    lookup_id                           BIGINT,
    -- position id -> {units, amount, cid} from broker_positions, or null when the position has no row.
    held_positions                      JSONB NOT NULL,
    first_event_at                      TIMESTAMPTZ,
    last_event_at                       TIMESTAMPTZ,
    execution_evidence                  TEXT CHECK (execution_evidence IN ('lookup', 'sync_only')),
    total_fees_unobserved_position_ids  BIGINT[] NOT NULL,
    PRIMARY KEY (run_id, subject),
    CHECK ((outcome = 'unresolved') = (cardinality(reasons) > 0)),
    CHECK ((outcome = 'admitted') = (execution_evidence IS NOT NULL)),
    CHECK ((subject LIKE 'order:%') = (order_id IS NOT NULL)),
    CHECK (subject LIKE 'order:%' OR outcome = 'unresolved')
);

CREATE TABLE IF NOT EXISTS tax_evidence_lots (
    run_id                BIGINT NOT NULL,
    lot_key               TEXT NOT NULL,
    subject               TEXT NOT NULL,
    order_id              BIGINT NOT NULL CHECK (order_id > 0),
    instrument_id         BIGINT NOT NULL,
    etoro_instrument_id   BIGINT NOT NULL,
    direction             TEXT NOT NULL CHECK (direction IN ('acquisition', 'disposal')),
    position_ids          BIGINT[] NOT NULL CHECK (cardinality(position_ids) > 0),
    trade_event_ids       BIGINT[] NOT NULL CHECK (cardinality(trade_event_ids) > 0),
    executed_at           TIMESTAMPTZ NOT NULL,
    uk_date               DATE NOT NULL,
    tax_year              TEXT NOT NULL CHECK (tax_year ~ '^[0-9]{4}/[0-9]{2}$'),
    units                 NUMERIC(20, 8) NOT NULL CHECK (units > 0),
    usd_amount            NUMERIC(20, 4) NOT NULL,
    consideration_status  TEXT NOT NULL CHECK (consideration_status = 'broker_stated_unreconciled'),
    execution_evidence    TEXT NOT NULL CHECK (execution_evidence IN ('lookup', 'sync_only')),
    -- 3b's consumer contract (3b spec, Design 3).
    fx_rate_date          DATE NOT NULL,
    fx_status             TEXT NOT NULL CHECK (fx_status IN ('published', 'carried_forward', 'pending')),
    fx_observation_date   DATE,
    fx_usd_per_gbp        NUMERIC(12, 6),
    fx_gbp_per_usd        NUMERIC(20, 10),
    fx_policy_id          TEXT NOT NULL,
    fx_run_id             BIGINT,
    -- The exact product usd_amount x fx_gbp_per_usd; NULL while the rate is pending.
    amount_gbp            NUMERIC(40, 14),
    fingerprint           TEXT NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (run_id, lot_key),
    FOREIGN KEY (run_id, subject) REFERENCES tax_evidence_subjects (run_id, subject),
    CHECK ((fx_status = 'pending') = (amount_gbp IS NULL)),
    CHECK ((fx_status = 'pending') = (fx_gbp_per_usd IS NULL)),
    CHECK (fx_rate_date = uk_date)
);

-- Generations are evidence: never rewritten (the function is sql/486's, generic on TG_TABLE_NAME).
DROP TRIGGER IF EXISTS tax_evidence_subjects_append_only ON tax_evidence_subjects;
CREATE TRIGGER tax_evidence_subjects_append_only
    BEFORE UPDATE OR DELETE ON tax_evidence_subjects
    FOR EACH ROW EXECUTE FUNCTION tax_order_lookups_append_only();

DROP TRIGGER IF EXISTS tax_evidence_lots_append_only ON tax_evidence_lots;
CREATE TRIGGER tax_evidence_lots_append_only
    BEFORE UPDATE OR DELETE ON tax_evidence_lots
    FOR EACH ROW EXECUTE FUNCTION tax_order_lookups_append_only();

-- A run row changes only while in progress, and is never deleted.
CREATE OR REPLACE FUNCTION tax_evidence_runs_terminal() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' OR OLD.state <> 'in_progress' THEN
        RAISE EXCEPTION 'tax_evidence_runs: run % is % and cannot change', OLD.run_id, OLD.state;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS tax_evidence_runs_terminal ON tax_evidence_runs;
CREATE TRIGGER tax_evidence_runs_terminal
    BEFORE UPDATE OR DELETE ON tax_evidence_runs
    FOR EACH ROW EXECUTE FUNCTION tax_evidence_runs_terminal();

COMMIT;
