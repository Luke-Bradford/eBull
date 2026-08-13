-- 108_data_reconciliation.sql
--
-- Reconciliation framework — operator-driven spot-check of stored
-- values vs live SEC EDGAR (operator audit 2026-05-03).
--
-- The framework runs N random instruments through a set of
-- registered checks, comparing what we have in SQL against what SEC
-- says right now. Drift findings land in
-- ``data_reconciliation_findings`` so the operator can triage in
-- one place rather than spot-checking by hand. Surfaces on the
-- ingest-health page (follow-up PR).
--
-- Schema decisions:
--
--   * ``data_reconciliation_runs`` is the per-run audit. One row
--     per ``run_spot_check`` invocation. Mirrors the
--     ``data_ingestion_runs`` shape so operator tooling can JOIN
--     them with familiar semantics.
--   * ``data_reconciliation_findings`` is per-(run, instrument,
--     check) drift finding. ``severity`` is a CHECK-constrained
--     enum so a typo can't smuggle a quietly-mis-classified row.
--   * ``check_name`` is free-text — a new check ships under its own
--     name without a schema migration. The check registry lives in
--     ``app.services.reconciliation``; if an operator sees a
--     finding under an unfamiliar check_name they can grep the
--     code.
--   * Both tables include ``fetched_at`` provenance so the operator
--     can answer "is this finding still current?" without
--     re-running the check.
--
-- Out of scope: scheduler entry, operator UI surface. Both are
-- separate PRs once the framework + first checks are shipped.

CREATE TABLE IF NOT EXISTS data_reconciliation_runs (
    run_id              BIGSERIAL PRIMARY KEY,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    status              TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'success', 'failed')),
    instruments_checked INTEGER NOT NULL DEFAULT 0,
    findings_emitted    INTEGER NOT NULL DEFAULT 0,
    -- ``sample_seed`` records the random seed used to pick the
    -- instrument cohort so re-running the same seed reproduces the
    -- same selection. Useful for "is this finding still there?"
    -- triage workflows.
    sample_seed         BIGINT,
    error               TEXT,
    triggered_by        TEXT NOT NULL DEFAULT 'system'
        CHECK (triggered_by IN ('system', 'operator', 'scheduler'))
);

CREATE INDEX IF NOT EXISTS idx_data_reconciliation_runs_started
    ON data_reconciliation_runs (started_at DESC);


CREATE TABLE IF NOT EXISTS data_reconciliation_findings (
    finding_id      BIGSERIAL PRIMARY KEY,
    run_id          BIGINT NOT NULL
        REFERENCES data_reconciliation_runs(run_id) ON DELETE CASCADE,
    instrument_id   BIGINT NOT NULL
        REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    check_name      TEXT NOT NULL,
    severity        TEXT NOT NULL
        CHECK (severity IN ('info', 'warning', 'critical')),
    summary         TEXT NOT NULL,
    expected        TEXT,
    observed        TEXT,
    -- Optional URL to the SEC source so the operator can verify on
    -- the regulator's site without re-running anything.
    source_url      TEXT,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_data_reconciliation_findings_run
    ON data_reconciliation_findings (run_id, severity);

CREATE INDEX IF NOT EXISTS idx_data_reconciliation_findings_instrument
    ON data_reconciliation_findings (instrument_id, fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_data_reconciliation_findings_check
    ON data_reconciliation_findings (check_name, fetched_at DESC);


COMMENT ON TABLE data_reconciliation_runs IS
    'Per-run audit for the reconciliation spot-check framework. '
    'One row per run_spot_check invocation. Mirrors '
    'data_ingestion_runs for familiar operator-tool joins. '
    'sample_seed reproduces the instrument selection if needed.';

COMMENT ON TABLE data_reconciliation_findings IS
    'Per-(run, instrument, check) drift finding. severity is a '
    'CHECK-constrained enum (info / warning / critical). check_name '
    'is free-text; the registry lives in '
    'app.services.reconciliation. Operator triage point — surfaces '
    'on the ingest-health page in a follow-up PR.';
