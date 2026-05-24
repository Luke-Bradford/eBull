# app/runbooks/

Operator runbooks for the Stream A bootstrap re-verification cycle (#1233). One executable Python module per runbook; shared safety primitives in `safety.py`.

**Path is `app/runbooks/` NOT `app/cli/runbooks/`** — `app/cli.py` already exists as the operator break-glass credential CLI (single-file module). A sibling `app/cli/` package would shadow it. Stream A PR-D / #1311 chose this flat layout after RV1 BLOCKING pre-empted the collision.

## Quick reference

| Runbook | Class | Invocation | Exit codes |
|---|---|---|---|
| `stream_a_run_8_verify.py` | DESTRUCTIVE | `EBULL_ENV=dev python -m app.runbooks.stream_a_run_8_verify --apply` | 0 / 1 / 2 / 3 |
| `stream_a_t13_sidecar_repair.py` | NON-DESTRUCTIVE | `EBULL_ENV=dev python -m app.runbooks.stream_a_t13_sidecar_repair --apply <CIK>` | 0 / 1 / 2 |
| `stream_a_stream_c_gate.py` | READ-ONLY | `EBULL_ENV=dev python -m app.runbooks.stream_a_stream_c_gate` | 0 / 1 |

Each runbook defaults to **dry-run** mode — `--apply` is REQUIRED to mutate.

## Pre-flight

Read `docs/operator/runbooks/run-8-readiness.md` **before** running anything destructive. It covers:

- §6.3 pre-wipe procedure for `pg_resetwal`-damaged dev DBs (multixact wraparound).
- `EBULL_ENV=dev` + `EBULL_DEV_DB_NAMES` env discipline.
- Jobs service stop semantics (`systemctl stop ebull-jobs`, NOT SIGINT).
- `pg_stat_activity` enumeration (no foreign sessions on `ebull_dev`).
- `/system/postgres-health` green-check.
- Exit-code interpretation matrix.

## Safety primitives (`safety.py`)

All runbooks use these defensively. The three-tier gate is consumed BEFORE any destructive or HTTP action:

| Primitive | When | Raises |
|---|---|---|
| `assert_dev_env()` | First call in `main()` | `RunbookRefused` if `EBULL_ENV != 'dev'` |
| `assert_dev_db_name_in_url()` | Called from `assert_dev_env()` | `RunbookRefused` if `DATABASE_URL` DB name not in `EBULL_DEV_DB_NAMES` (default `{"ebull_dev"}`) — fail BEFORE psycopg connect |
| `assert_dev_db(conn)` | Post-connect | `RunbookRefused` if `current_database()` not in allowlist (belt-and-braces with the URL check) |
| `assert_no_multixact_wraparound(conn)` | Post-connect, pre-destructive | `RunbookRefused` if `pg_database.datminmxid` or top-5 `pg_class.relminmxid` ages exceed 80% of `autovacuum_multixact_freeze_max_age` |
| `assert_jobs_process_stopped(database_url)` | Pre-destructive | `RunbookRefused` if `JOBS_PROCESS_LOCK_KEY` is held on the app DB (jobs service still running) |
| `wait_for_jobs_process_started(database_url, timeout_sec)` | Post-bootstrap-dispatch | `RunbookRefused` on timeout — operator failed to start jobs service after dispatch |

All failures emit exit code 2 (`SystemExit(2)` via `RunbookRefused`).

## Logging

Each runbook appends a JSONL envelope to `var/runbooks/<runbook>-<run_id>-<ts>.jsonl`. Envelope shape is pinned by:

- `stream_a_stream_c_gate.py` — Pydantic model in `stream_a_stream_c_gate_schema.py` (Run-#8-readiness fixes). 8 top-level keys; `extra='forbid'`; contract-tested in `tests/runbooks/test_stream_c_gate_envelope_contract.py`.
- Other runbooks — ad-hoc JSON (operator-readable; not yet contract-pinned).

## Discoverability

If you arrived here via `git ls`, the spec that justifies each runbook lives at `docs/proposals/etl/stream-a-run-8-fixes.md` §17 (Stream A spec v2.4) and `docs/proposals/etl/run-8-readiness-fixes.md` (Run-#8-readiness fixes spec v1.3).

If you arrived via search, the operator-facing reference is `docs/operator/runbooks/run-8-readiness.md`.
