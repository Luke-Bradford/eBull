-- 129_bootstrap_state.sql
--
-- Issue #993 (umbrella #992) — First-install bootstrap orchestrator.
--
-- Spec: docs/superpowers/specs/2026-05-07-first-install-bootstrap.md
--
-- ## Why
--
-- Fresh eBull installs leave the database empty after operator setup.
-- Scheduled jobs fire but no-op against an empty universe. There is
-- no automation walking the operator from "credentials saved" to
-- "system fully populated", and no admin surface that explains what
-- is missing. This migration introduces the persistence layer for an
-- explicit "Run bootstrap" admin button: a singleton state row that
-- the scheduler reads as a prerequisite gate, plus per-run history
-- and per-stage detail.
--
-- ## Tables
--
--   * bootstrap_runs   — one row per "Run bootstrap" click. Status
--                        flips from running to terminal complete /
--                        partial_error when both lane threads have
--                        joined.
--   * bootstrap_stages — one row per stage in a run. 17 stages per
--                        run today (1 init + 1 eToro + 15 SEC; S12
--                        dividend calendar dropped — see spec).
--   * bootstrap_state  — singleton row (id=1) carrying the canonical
--                        scheduler-gate status. Read by
--                        _bootstrap_complete in app/workers/scheduler.py.
--
-- ## Concurrency
--
-- A partial unique index on bootstrap_runs(status='running') gives
-- defense-in-depth against two concurrent /run handlers both
-- inserting a new run. The /run API also takes
-- SELECT ... FOR UPDATE on the bootstrap_state singleton row to
-- serialise at the application layer; the index exists to make a
-- second insert fail loudly if that lock is ever bypassed.
--
-- ## Status semantics
--
--   bootstrap_state.status:
--     pending       — never run on this install.
--     running       — a bootstrap_runs row is in flight.
--     complete      — most recent run finalised with zero stage errors.
--     partial_error — most recent run finalised with one or more
--                     stage errors. Scheduler gate stays closed.
--
--   bootstrap_runs.status:
--     running       — orchestrator dispatched, lanes still working.
--     complete      — all stages success or harmlessly skipped.
--     partial_error — one or more stages ended in error.
--
--   bootstrap_stages.status:
--     pending  — created with the run; not yet dispatched.
--     running  — lane runner is currently executing this stage.
--     success  — invoker returned without raising; rows_processed
--                may be set.
--     error    — invoker raised, JobLock contention, or boot recovery
--                swept; last_error explains.
--     skipped  — reserved for retry-failed paths that intentionally
--                skip a stage (e.g. successor stages skipped because
--                a predecessor failed terminally).

CREATE TABLE IF NOT EXISTS bootstrap_runs (
    id                       BIGSERIAL PRIMARY KEY,
    triggered_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    triggered_by_operator_id UUID REFERENCES operators(operator_id),
    status                   TEXT NOT NULL DEFAULT 'running'
                             CHECK (status IN ('running', 'complete', 'partial_error')),
    completed_at             TIMESTAMPTZ,
    notes                    TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS bootstrap_runs_one_running_idx
    ON bootstrap_runs (status)
    WHERE status = 'running';

CREATE TABLE IF NOT EXISTS bootstrap_stages (
    id               BIGSERIAL PRIMARY KEY,
    bootstrap_run_id BIGINT NOT NULL REFERENCES bootstrap_runs(id) ON DELETE CASCADE,
    stage_key        TEXT NOT NULL,
    stage_order      SMALLINT NOT NULL,
    lane             TEXT NOT NULL CHECK (lane IN ('init', 'etoro', 'sec')),
    job_name         TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'running', 'success', 'error', 'skipped')),
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    rows_processed   INTEGER,
    expected_units   INTEGER,
    units_done       INTEGER,
    last_error       TEXT,
    attempt_count    INTEGER NOT NULL DEFAULT 0,
    UNIQUE (bootstrap_run_id, stage_key)
);

CREATE INDEX IF NOT EXISTS bootstrap_stages_run_status_idx
    ON bootstrap_stages (bootstrap_run_id, status);

CREATE TABLE IF NOT EXISTS bootstrap_state (
    id                INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    status            TEXT NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending', 'running', 'complete', 'partial_error')),
    -- last_run_id is informational, not a FK. The truncate-cascade
    -- semantics in the test fixture would otherwise wipe the
    -- singleton row whenever bootstrap_runs is truncated. A dangling
    -- last_run_id pointing at a deleted run is harmless — the API
    -- just stops finding the run.
    last_run_id       BIGINT,
    last_completed_at TIMESTAMPTZ
);

INSERT INTO bootstrap_state (id) VALUES (1) ON CONFLICT DO NOTHING;
