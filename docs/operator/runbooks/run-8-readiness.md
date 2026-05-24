# Run #8 operator readiness gate

> **Purpose.** Pre-`--apply` checklist for `stream_a_run_8_verify` + `stream_a_stream_c_gate`. Reading this carefully WILL prevent mid-run surprises.
>
> **When to use.** Before running `python -m app.runbooks.stream_a_run_8_verify --apply` on dev DB for Run #8 (final operator drive for #1233 close).
>
> **Estimated wall-clock.** Bootstrap = 100-180 min predicted (Stage C analysis); add ~5 min for Stream-C gate JSON envelope.
>
> **Source memos.** Stages A-G of the 2026-05-24 ETL sweep (see §11 for cross-references). Code paths verified against working tree at `91aa214` (Stream A PR-D merge).

---

## 0. Headline blockers from committee review (READ FIRST)

Eight-lens committee on Stages A-F surfaced the following items that pre-condition the operator drive. Status legend: **fix-before-apply** (must address before `--apply`), **accept-and-monitor** (know about, watch during run), **post-Run-8** (track but does not block this drive).

| ID | Lens | Severity | Summary | Status |
|---|---|---|---|---|
| OP-B1 | Operator | BLOCKING | §6.3 pre-wipe procedure (multixact wraparound) not in any operator-reachable doc | **fix-before-apply** — covered by §2 of this runbook |
| OP-B2 | Operator | BLOCKING | `EBULL_DEV_DB_NAMES` mismatch fails post-conn-open with no upfront env dump | **fix-before-apply** — covered by §1.1 + §1.2 of this runbook |
| OP-I1 | Operator | IMPORTANT | No disk/WAL pre-flight inside the runbook itself | **fix-before-apply** — operator manual check in §1.2 |
| OP-I2 | Operator | IMPORTANT | No `pg_stat_activity` pre-flight session enumeration | **fix-before-apply** — operator manual check in §1.2 |
| OP-I3 | Operator | IMPORTANT | Poll heartbeat only logs `stage_count`; cannot tell "stuck" from "slow" | **accept-and-monitor** — §7 supplies the manual SQL query |
| ARCH-B1 | Architect | BLOCKING | `sync_all` dispatches 5 cats; repair sweep has 7 — `funds` + `esop` only recompute via 03:30 UTC drift sweep | **accept-and-monitor** — does NOT block Run #8; surfaces post-drive as C6 quiescent warnings for those categories |
| API-B1 | API Contract | BLOCKING | FINRA bimonthly maps only 404, RegSHO maps 403+404 | **post-Run-8** — daily FINRA crons fire DURING Run #8, may emit spurious alarms; ignore non-fatal log noise |
| API-B2 | API Contract | BLOCKING | Conditional-GET `Last-Modified` not persisted past provider | **post-Run-8** — burns SEC budget steadily but does not block this run |
| API-B3 | API Contract | BLOCKING | Sidecar single-primary-page assumption (deep-history CIKs) | **accept-and-monitor** — C7 gate may false-green for mega-funds (Vanguard, BlackRock); cross-check sidecar count is non-zero in §4.3 |
| API-B4 | API Contract | BLOCKING | Stream-C gate JSON envelope is client-pinned, no server validation | **accept-and-monitor** — current envelope is `schema_version=1`; do NOT hand-edit before posting |
| TE-B1 | Test Engineer | BLOCKING | No lint guard on "services must NOT enter own `with conn.transaction():`" | **post-Run-8** — tech-debt; does not affect this run |
| TE-B2 | Test Engineer | BLOCKING | Stream-C gate JSON envelope has no schema contract test | **post-Run-8** — tech-debt |
| TE-B3 | Test Engineer | BLOCKING | Per-source parser fixtures not pinned to specific accessions | **post-Run-8** — tech-debt |
| REV-B1 | Reviewer | BLOCKING | "28 SCHEDULED_JOBS" / "14 SEC ManifestSource" stale counts in Stages B/D | **doc-only** — already noted; does not affect runtime |
| DE-IMP1 | Data Engineer | IMPORTANT | `sec_n_cen` ad-hoc bypass = invisible to audit framework | **post-Run-8** — N-CEN does not surface in C4 gate (registered_parser_sources skips it) |
| DE-IMP2 | Data Engineer | IMPORTANT | Two partitioned tables (`finra_regsho_daily_observations`, `financial_facts_raw`) die 2030-Q1/Q4 | **post-Run-8** — long-horizon; not Run-8 risk |
| DE-IMP3 | Data Engineer | IMPORTANT | CUSIP resolver = 19/16M effectively no-op | **accept-and-monitor** — expected during Run #8; tracked via #740 |
| PM-B2 | PM | BLOCKING | MUST/SHOULD/FAIL-LOUD rubric never formalised | **fix-before-apply** — encoded verbatim in §3.3 of this runbook |
| PM-B3 | PM | BLOCKING | Stream-C gate persistence channel ambiguous (column + #1233 comment) | **fix-before-apply** — clarified in §4 |
| PM-I3 | PM | IMPORTANT | No time-box for operator attention | **fix-before-apply** — encoded in §3.2 (240-min hard stop) |
| PM-I4 | PM | IMPORTANT | Resume protocol on partial completion thin | **fix-before-apply** — covered in §6 |
| CODEX-B1 | Codex CTO | BLOCKING | Stream-C gate envelope `schema_version=1` is write-only pin | **post-Run-8** — does not affect this drive but file follow-up ticket |

**Net for this drive.** Five "fix-before-apply" items are addressed by reading + executing §§1-3 of this runbook in order. Five "accept-and-monitor" items mean keep §7 (observability) open in a second terminal and keep §9 (gotchas) within reach during the run.

---

## 1. Pre-flight checklist

Run this checklist top-to-bottom. Each row is a SQL or shell command + a pass criterion. Do NOT proceed to §3 until every row passes.

### 1.1 Environment

| # | Check | Command | Pass criterion |
|---|---|---|---|
| E1 | `EBULL_ENV` explicitly set | `echo "${EBULL_ENV:-UNSET}"` | Prints `dev` (not `UNSET`, not `prod`, not empty). Per `app/runbooks/safety.py:52` — `assert_dev_env` fails CLOSED if unset. |
| E2 | `EBULL_DEV_DB_NAMES` matches actual DB | `echo "${EBULL_DEV_DB_NAMES:-ebull_dev}"` then `psql "$DATABASE_URL" -At -c 'SELECT current_database()'` | Two outputs match. If your DB is `ebull_dev_local`, set `export EBULL_DEV_DB_NAMES=ebull_dev_local`. Per `app/runbooks/safety.py:71` — comma-separated, whitespace-tolerant. **B2 fold:** without this match, runbook refuses at `assert_dev_db` AFTER opening a real psycopg connection (exit 2 with `current_database()='...' not in dev allowlist`). |
| E3 | `DATABASE_URL` points at the dev DB | `python -c 'from urllib.parse import urlparse; from app.config import settings; print(urlparse(settings.database_url).path)'` | Prints `/ebull_dev` (or your local equivalent matching E2). NOT `/postgres`, NOT `/ebull` (prod). |
| E4 | `pyproject.toml` edgartools pin intact | `python -c 'import importlib.metadata as m; print(m.version("edgartools"))'` | Prints `5.30.2` exactly. Mismatch → STOP and `uv sync --reinstall edgartools` (per API-I6 finding; #932 Pydantic-validation cliff). |
| E5 | Working tree at expected SHA | `git -C /Users/lukebradford/Dev/eBull rev-parse HEAD` | Prints `91aa214` (Stream A PR-D merge) OR newer. If older, `git pull --ff-only` first. |

### 1.2 Postgres state

| # | Check | Command | Pass criterion |
|---|---|---|---|
| P1 | PG cluster up + version ≥ 17 | `psql "$DATABASE_URL" -At -c 'SHOW server_version_num'` | Prints integer ≥ `170000`. PR12 `app/system/postgres_version_guard.py` enforces this at lifespan; verify pre-`--apply` so you don't lose 5 min to a guard error. |
| P2 | DB size under 50 GB | `psql "$DATABASE_URL" -At -c "SELECT pg_database_size(current_database())"` then divide by `1073741824` | Result < 50. Post-#1208 retention sweep state was 41 GB; close to band. If > 50, run `VACUUM FULL ANALYZE` on top-3 by `pg_total_relation_size` before `--apply`. |
| P3 | WAL dir under 10 GB | `psql "$DATABASE_URL" -At -c "SELECT pg_size_bytes(pg_size_pretty(SUM(size))) FROM pg_ls_waldir()"` then divide by `1073741824` | Result < 10. Post-#1208 was 3.9 GB. **OP-I1 fold:** runbook does NOT pre-flight this; must check manually. WAL at 9 GB + bootstrap WAL traffic = OOM-crash mid-run. |
| P4 | No other sessions on target DB | `psql "$DATABASE_URL" -c "SELECT pid, application_name, client_addr, state, query_start, left(query, 80) AS q FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()"` | ZERO rows OR only `application_name='ebull-jobs'` (which §1.3 J1 will require you to stop anyway). **OP-I2 fold:** runbook does NOT pre-flight session enumeration; reactive only on DROP DATABASE 55006. Run this query NOW and kill any DataGrip / psql / pgAdmin sessions before `--apply` to avoid the 7-second 55006 retry tax. |
| P5 | `/system/postgres-health` reports green | `curl -s -u operator:$PASSWORD http://127.0.0.1:8000/system/postgres-health \| python -m json.tool` | All `*_breached_warn` are `false` (NOT `null` — `null` means probe error). **Codex 2 HIGH DEFERRED:** if the endpoint returns `401 Unauthorized` and you're certain credentials are correct, that means PG itself is down (auth-dep tries to read user table first; returns 401 before the 503 path runs). In that case fall back to P1-P4 raw psql + skip this row. |
| P6 | `bootstrap_state.last_jobs_boot_error` is NULL | `psql "$DATABASE_URL" -At -c "SELECT last_jobs_boot_error FROM bootstrap_state"` | Empty output (NULL). Non-NULL means a prior jobs boot guard fired — read it, fix it (likely operator-existence), THEN proceed. |
| P7 | No bootstrap currently in flight | `curl -s -u operator:$PASSWORD http://127.0.0.1:8000/system/bootstrap-status \| python -c 'import sys, json; d=json.load(sys.stdin); print(d["state_status"], d["summary"]["running"])'` | Prints `pending 0` OR `complete 0` OR `partial_error 0`. If prints `running N` with N > 0, a foreign run is in flight — STOP and resolve before `--apply` (exit-3 drift trap waits otherwise). |

### 1.3 Service state

| # | Check | Command | Pass criterion |
|---|---|---|---|
| J1 | Jobs process STOPPED (systemctl, not SIGINT) | `systemctl status ebull-jobs` (or whatever your unit name is) | Active state is `inactive (dead)` or unit not found. **OP-I6 fold:** `Ctrl-C` (SIGINT) only triggers graceful shutdown; the `JOBS_PROCESS_LOCK_KEY` advisory lock releases on connection close but `app/jobs/__main__.py` shutdown path may take seconds. Use `systemctl stop ebull-jobs` to guarantee the lock has been released before the runbook probes for it. If unit was started outside systemd, `pkill -TERM -f 'python -m app.jobs'` then `pkill -KILL -f 'python -m app.jobs'` after 30 s. |
| J2 | Advisory lock not held | `psql "$DATABASE_URL" -At -c "SELECT pid, mode FROM pg_locks WHERE locktype = 'advisory' AND classid = 0 AND objid = 9173"` | Empty output. (9173 = `JOBS_PROCESS_LOCK_KEY` per `app/jobs/locks.py`; verify constant if it has changed.) Non-empty = jobs process or a runaway probe is still holding it; kill the PID. |
| J3 | API process running | `curl -sf http://127.0.0.1:8000/health` | HTTP 200 with `{"status":"ok"}`. Runbook needs the API to dispatch `POST /system/bootstrap/run` (see §3.1 step C). If down, `systemctl start ebull-api` (or VS Code task) and wait until /health responds. |

### 1.4 Migration state

| # | Check | Command | Pass criterion |
|---|---|---|---|
| M1 | All migrations applied | `psql "$DATABASE_URL" -At -c "SELECT count(*) FROM schema_migrations"` then `ls /Users/lukebradford/Dev/eBull/sql/*.sql \| wc -l` | Two numbers match (or DB count is one greater if you count the bootstrap migration). |
| M2 | sql/171, 172, 173 present in DB | `psql "$DATABASE_URL" -At -c "SELECT filename FROM schema_migrations WHERE filename LIKE '17%' ORDER BY filename"` | Prints `171_bootstrap_state_last_jobs_boot_error.sql`, `172_sec_cik_submissions_files_index.sql`, `173_bootstrap_runs_stream_c_gate.sql` (Stream A PR-A/B columns). |
| M3 | `bootstrap_runs.stream_c_gate_status` column exists | `psql "$DATABASE_URL" -At -c "SELECT column_name FROM information_schema.columns WHERE table_name='bootstrap_runs' AND column_name='stream_c_gate_status'"` | Prints `stream_c_gate_status`. PR-B `sql/173` gate column — needed by §4. |

---

## 2. The §6.3 pre-wipe procedure (multixact wraparound)

**Why this matters.** Per `project_1233_pr12_ownership_merge_writer.md` §"Operator follow-up (BLOCKING for closing #1233 umbrella)" #1, the dev DB carries `pg_resetwal -f` damage from the PR12 development cycle. Tables `job_runtime_heartbeat` + `broker_credentials` bleed `MultiXactId N has not been created yet -- apparent wraparound` errors. `stream_a_run_8_verify --apply` calls `DROP DATABASE ebull_dev` at `app/runbooks/stream_a_run_8_verify.py:130`. **DROP DATABASE on a multixact-damaged catalog can itself error or hang silently**, and the runbook gives no actionable recovery breadcrumb beyond `pg_stat_activity` output (per OP-B1).

The §6.3 procedure (canonical text at `docs/specs/etl/retention-rubric.md:343-372`) is a whole-DB controlled wipe. The runbook's `_drop_and_create_db` is the mechanised version of §6.3 step 1-2 BUT it presupposes a healthy catalog. The first time you run `--apply` after PR12 ships, you must drop-and-recreate from a sibling DB **manually first** so the runbook's own DROP is a no-op on a fresh-enough catalog.

### 2.1 Is this step idempotent?

YES — both the manual procedure below AND the runbook's own DROP/CREATE inside `--apply` are idempotent against a healthy catalog. The manual procedure exists solely to clear multixact damage that the runbook's DROP would inherit.

### 2.2 What if the operator skips it?

If the manual pre-wipe is skipped on a multixact-damaged catalog, `--apply` may:

- Hang at `DROP DATABASE ebull_dev` past the 2 s + 5 s retry budget at `stream_a_run_8_verify.py:127`.
- Raise `psycopg.errors.ObjectInUse` (55006) twice → `RunbookRefused` with `pg_stat_activity` dump → exit 1.
- Worst case (silent corruption): `DROP` succeeds, `CREATE` succeeds, migrations run, but new `bootstrap_runs` writes hit the same multixact wraparound on a tuple-header level. Symptom = `--apply` exit 1 mid-run with a multixact error message.

If §1.2 P1-P4 all pass AND you've never run `pg_resetwal` on this DB, the procedure below is a no-op safety net. Run it anyway the first time.

### 2.3 Procedure

Run these in order, from a shell pointed at the dev cluster. Sample target: `ebull_dev`. Substitute your value from §1.1 E3.

```bash
# Step 1 — verify no jobs / API holds an open conn to ebull_dev.
# Stop everything; double-check via §1.3 + §1.2 P4 BEFORE this step.
# This is the same enumeration the runbook does reactively at
# stream_a_run_8_verify.py:124, but you're doing it pre-emptively.
psql "postgresql://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PGPORT}/postgres" -c \
  "SELECT pid, application_name, client_addr, state
   FROM pg_stat_activity WHERE datname = 'ebull_dev'"
# Expect zero rows. If non-zero, kill them with pg_terminate_backend(pid).

# Step 2 — terminate any stragglers (safety net even if step 1 looked clean).
psql "postgresql://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PGPORT}/postgres" -c \
  "SELECT pg_terminate_backend(pid)
   FROM pg_stat_activity
   WHERE datname = 'ebull_dev' AND pid <> pg_backend_pid()"

# Step 3 — DROP + CREATE from the sibling 'postgres' admin DB.
# DO NOT run this while connected to ebull_dev itself.
psql "postgresql://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PGPORT}/postgres" -c \
  "DROP DATABASE IF EXISTS ebull_dev"
psql "postgresql://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PGPORT}/postgres" -c \
  "CREATE DATABASE ebull_dev"

# Step 4 — verify multixact state on the fresh DB.
# pg_multixact/offsets should be small (< 1 MB) on a fresh DB.
du -sh "$(psql "$DATABASE_URL" -At -c 'SHOW data_directory')/pg_multixact" 2>/dev/null
# Expect: < 1 MB. If > 100 MB, pg_resetwal damage persists at cluster level;
# escalate to a full PG cluster reinitdb (out of scope here — file ticket).
```

**Verification.** After step 3, `psql "$DATABASE_URL" -At -c 'SELECT count(*) FROM pg_tables WHERE schemaname=current_schema()'` should print `0` (the fresh DB has only the system catalog).

**Time budget.** 30-60 s if steps 1-2 are clean; up to 5 min if you need to chase down DataGrip sessions.

---

## 3. Run #8 execution

### 3.1 Command

Dry-run first (mandatory):

```bash
# Always start with --dry-run (no flag = dry-run).
EBULL_ENV=dev python -m app.runbooks.stream_a_run_8_verify
```

Inspect the planned actions JSON. If it lists 12 `would_execute` lines starting with `assert_dev_env` and ending with `poll /system/bootstrap-status`, proceed:

```bash
# Destructive Run #8.
EBULL_ENV=dev python -m app.runbooks.stream_a_run_8_verify --apply
```

**Optional flags** (see `app/runbooks/stream_a_run_8_verify.py:381-407`):

| Flag | Default | When to override |
|---|---|---|
| `--api-base` | `http://127.0.0.1:8000` | If API runs on non-default host/port |
| `--timeout-min` | `90` | **Override to `180`** for realistic-band run; per §3.2 the realistic band exceeds 90 min |
| `--poll-sec` | `30` | Lower to `10` only if you want more frequent log entries (no orchestrator impact) |
| `--wait-for-jobs-sec` | `600` | Raise to `1800` if you might step away after dispatch — per O2 the default is 10 min and exceeding it forces a manual restart |

**Recommended invocation for this drive:**

```bash
EBULL_ENV=dev python -m app.runbooks.stream_a_run_8_verify --apply --timeout-min 180 --wait-for-jobs-sec 1800
```

### 3.2 Expected per-stage wall-clock band

Per Stage C analysis (`project_etl_sweep_stage_c_2026_05_24.md`). Critical path = `sec_rate` lane serialiser (~120-180 min); db-lane stages run concurrent. Watch these 6 cells closely:

| Stage | Lane | Run #7 wall | Predicted Run #8 | If exceeds 3× | Diagnosis |
|---|---|---|---|---|---|
| S7 sec_bulk_download | sec_bulk_download | 64s | 60-180s | > 9 min | Slow SEC CDN; runbook falls back to `BootstrapPhaseSkipped` cascade. Watch for cascade-skipped S8/S10/S11. |
| S8 sec_submissions_ingest | db_filings | 22 min | 22-28 min | > 84 min | PR-B sidecar write may be exhausting db_filings pool — check `pg_stat_activity` for blocked queries on `sec_cik_submissions_files_index`. |
| S14 sec_submissions_files_walk | sec_rate | 48 min | 45-55 min | > 150 min | Per-CIK secondary-page HTTP walk; lock-contended with S25. **Tier 2 #1277 target**. Worst-case at 200+ min suggests SEC EDGAR throttling — read recent 429s. |
| S16 sec_first_install_drain | sec_rate | 65 min | 60-70 min | > 200 min | Single-tx freshness_index inserts. **Highest-known leak risk per Stage C #4 advisory-lock conn leak**; run query in §7.2 if exceeds 90 min. |
| S22 sec_13f_recent_sweep | sec_rate | cancelled | 30-120 min | > 180 min | Cohort cap (#1010 11.2k→8.7k CIKs) should bound this. Worst case = per-CIK retries on flaky EDGAR. |
| S25 fundamentals_sync | db_fundamentals_raw | 101 min | **≤ 15 min target** | > 30 min | **PR-C2 derivation-only entrypoint** should slash this. If S25 stays HTTP, PR-C2 did not wire — bail and triage. |

**Total budget.** Recommend hard-stop at **240 min** wall-clock. **Soft alert at 180 min** — open §7 in a second terminal and re-check `bootstrap_status` per-stage `state_status`. Per Codex Strategic Finding #4: at minute 130 the operator cannot tell from `stage_count` alone whether S14/S16 is stuck or progressing; use the per-stage SQL in §7.

### 3.3 MUST-pass / SHOULD-pass / FAIL-LOUD rubric (PM-B2 fold)

After the runbook returns, evaluate against this rubric before §4 gate:

**MUST-pass for `accepted=true` close of #1233:**

- All 27 stages reach terminal status (`success` OR `skipped` per cascade-skip semantics — see `_STAGE_PROVIDES_ON_SKIP` at `app/services/bootstrap_orchestrator.py:409-430`).
- Runbook exit code `0`.
- Stream-C gate (§4) exit code `0` with `accepted=true`.
- S25 fundamentals_sync wall-clock ≤ 30 min (PR-C2 derivation-only target).
- Zero advisory-lock leak — verify via §7.2 at exit.

**SHOULD-pass (warn but don't block):**

- Total bootstrap wall-clock 60-180 min.
- CUSIP `external_identifiers` row count > 19 (the Run #7 baseline; Stage E #740 tracks the deeper fix).

**FAIL-LOUD (block #1233 close until triaged):**

- Any stage stuck-pending past 90 min (#1224 exemplar) → file replay against `compute_retryable_view`.
- Any bulk ingester (S8/S9/S10/S11/S12) writing `rows_processed=NULL` (#1225 partial) → check `job_runs.rows_processed WHERE job_name LIKE 'JOB_SEC_%_INGEST%' ORDER BY started_at DESC LIMIT 10`.
- CardinalityViolation on S25 (Run #7 fix-list #5).
- PG `max_locks_per_transaction` breach (#1187 boot guard; floor=1024).
- Exit code `3` (concurrent bootstrap detected — see §3.4).

### 3.4 Exit code interpretation (verified from code)

Verified against `app/runbooks/stream_a_run_8_verify.py:34-42`.

| Exit | Meaning | Operator action |
|---|---|---|
| `0` | Bootstrap reached terminal status (`complete` OR `partial_error`). | Proceed to §4 Stream-C gate. `partial_error` is acceptable here — gate will surface per-stage failures via C1-C7. |
| `1` | Gate-side failure (DROP retried twice + still 55006, DB connect error, migration error, /auth/setup non-200, /system/bootstrap/run non-202). | Read stderr first — runbook prints `pg_stat_activity` on DROP failure (see `:143-148`). Most common cause = §1.2 P4 not enforced. Resolve, re-run from §1 P4. |
| `2` | Invalid input / refused precondition (env guards, missing `--apply`, drift detected mid-poll, JobAlreadyRunning, timeout exceeded). | Read stderr — message includes recovery hint. Common: `EBULL_ENV` not `dev`, `current_database()` not in allowlist, jobs process detected. If timeout (90 min default), use `--timeout-min 180` per §3.1 and re-run; the captured `run_id` from the JSONL log can be re-targeted via direct `/system/bootstrap-status` polling. |
| `3` | **CRITICAL** — concurrent bootstrap detected. Foreign `run_id` observed in poll; data-corruption risk; foreign run NOT cancelled. | **STOP. Do NOT start a third run.** Per `:285-293`: read `bootstrap_runs WHERE status='running'` (`psql "$DATABASE_URL" -c "SELECT id, started_at, status FROM bootstrap_runs WHERE status='running'"`) to see both run IDs. Investigate via `pg_stat_activity` who dispatched the other run. The captured run_id from your invocation IS in the JSONL log (`var/runbooks/stream_a_run_8_verify-*-*.jsonl`). Triage required before any further `--apply`. |

JSONL log path always printed on stderr at exit (`stream_a_run_8_verify.py:547`):

```
# JSONL log: var/runbooks/stream_a_run_8_verify-<run_id>-<ts>.jsonl
```

---

## 4. Post-run Stream-C gate

### 4.1 Command

```bash
EBULL_ENV=dev python -m app.runbooks.stream_a_stream_c_gate \
  --bootstrap-run-id <RUN_ID_FROM_§3> \
  --json-out var/runbooks/stream_c_gate_$(date -u +%Y%m%d_%H%M%S).json
```

`--strict` defaults to `True` (`stream_a_stream_c_gate.py:364-372`) — keep it. `--no-strict` downgrades any failed check to exit 0 (warnings only) and is reserved for triage-only re-runs.

### 4.2 JSON envelope shape

Verified against `app/runbooks/stream_a_stream_c_gate.py:325-334`. `JSON_SCHEMA_VERSION = 1` (pinned at `:53`).

```json
{
  "schema_version": 1,
  "runbook": "stream_a_stream_c_gate",
  "bootstrap_run_id": <int>,
  "started_at": "<iso>",
  "ended_at": "<iso>",
  "checks": [
    {"id": "c1", "status": "passed|failed", "count": <int>, "detail": "<str>"},
    {"id": "c2", ...}, {"id": "c3", ...}, {"id": "c4", ...}, {"id": "c5", ...},
    {"id": "c6_insiders", ...}, {"id": "c6_institutions", ...}, {"id": "c6_blockholders", ...},
    {"id": "c6_treasury", ...}, {"id": "c6_def14a", ...}, {"id": "c6_funds", ...}, {"id": "c6_esop", ...},
    {"id": "c7", ...}
  ],
  "accepted": true|false,
  "first_failed": <str|null>,
  "exit_code": 0|1
}
```

**Persistence (PM-B3 fold — BOTH channels):**

- **Column persistence:** `bootstrap_runs.stream_c_gate_status` UPDATEd to one of `pending` (at start), `passed` (accepted), or `failed_<first_failed_check_id>` (e.g. `failed_c4`, `failed_c6_funds`). Constraint at `sql/173:54-66`: `LIKE 'failed\_%' ESCAPE '\'` literal underscore + length guard. If the runbook crashes mid-gate, the status is forcibly set to `failed_runbook_crashed` (per `:411`).
- **#1233 comment:** operator pastes the full JSON envelope as a comment on issue #1233 for audit trail. **Do NOT hand-edit between paste and post** — there is no server-side validation (per API-B4). Use:

  ```bash
  gh issue comment 1233 --body "$(cat var/runbooks/stream_c_gate_*.json | jq -R -s 'split(\"\n\") | map(select(length > 0)) | last | fromjson | tostring | \"```json\n\" + . + \"\n```\"')"
  ```

  Or simpler: copy the file contents into the comment body via the GitHub UI inside triple-backtick `json` fences.

### 4.3 Per-check meaning

Verified from `app/runbooks/stream_a_stream_c_gate.py:78-267`.

| Check | What it asserts | Common failure mode |
|---|---|---|
| C1 | `job_runs WHERE job_name='sec_atom_fast_lane' AND status='success' AND started_at > Run-#8 completed_at` has ≥ 1 row | Atom-fast-lane cron has NOT fired since Run #8 completed. Wait for next 5-min tick (it runs every 5 min) or check scheduler health. |
| C2 | Same shape, `job_name='sec_daily_index_reconcile'` | Daily-index reconcile cron has NOT fired since Run #8. Runs daily 04:00 UTC — if Run #8 completed AFTER 04:00 UTC, wait until next morning. |
| C3 | Same shape, `job_name='sec_per_cik_poll'` | Per-CIK poll cron has NOT fired since Run #8. Runs hourly. Wait or check scheduler. |
| C4 | Every `registered_parser_sources()` source has ≥ 1 manifest row drained (`ingest_status IN ('parsed','tombstoned')`) post-Run-#8 | A parser source is registered but its manifest worker has not drained anything. Check `sec_filing_manifest WHERE source='<missing>' AND ingest_status='pending'`. **DE-IMP1 caveat:** `sec_n_cen` is NOT in `registered_parser_sources()` so does NOT participate in C4 — invisible by design (post-Run-8 cleanup ticket). |
| C5 | `data_freshness_index WHERE updated_at > Run-#8 completed_at AND state='current'` has ≥ 1 row | Manifest worker is not writing freshness rows. Likely C4 also failing. |
| C6_<cat> | For each of 7 categories: ≥ 1 new observation OR (no obs AND no upstream manifest rows in last 24h → quiescent warning, not fail) | **ARCH-B1 caveat:** `funds` + `esop` only receive observations via repair-sweep 03:30 UTC. If Run #8 completed AFTER 03:30 UTC, these may legitimately warn-quiescent. |
| C7 | Sidecar populated for in-universe + tradable CIKs (minus KNOWN_FILING_AGENT_CIKS overlap) | PR-B sidecar write failed for some CIKs. Run `stream_a_t13_sidecar_repair --bootstrap-run-id <id>` (see §6.2) before re-running gate. **API-B3 caveat:** mega-funds with > 1 primary submissions JSON page may pass C7 falsely. Verify post-gate via `SELECT count(*) FROM sec_cik_submissions_files_index WHERE cik='0001067983' /* Berkshire */ AND bootstrap_run_id=<id>` — expect ≥ 1. |

### 4.4 What to do if gate fails

Per check ID (OP-I4 fold):

| first_failed | Recovery |
|---|---|
| `c1` / `c2` / `c3` | Wait for the relevant scheduled job to fire (5 min / 24 h / 1 h cadences respectively). Re-run gate. If a cron is stuck, restart `ebull-jobs` and read the most recent `job_runs WHERE status='error' ORDER BY started_at DESC LIMIT 5`. |
| `c4` | Some registered parser source has no drained manifest rows. Identify missing source from `detail` field, then check whether the worker registered the parser at boot: `grep -r "register.*<source>" app/services/manifest_parsers/`. If registered, check worker logs for parse errors. |
| `c5` | Manifest worker did not write freshness rows. Likely correlates with C4 — fix C4 first then retry. |
| `c6_<category>` | For `funds`/`esop`: wait until next 03:30 UTC repair sweep, then re-run gate. For others: real failure — check the corresponding `<category>_observations` writer for ingest errors. |
| `c7` | Sidecar coverage gap. Run `stream_a_t13_sidecar_repair --apply --archive-path <submissions.zip> --bootstrap-run-id <id>` (see §6.2). Re-run gate. |
| `failed_runbook_crashed` | The gate runbook itself crashed mid-execution. Status column reflects this distinctly from check-failure. Read stderr Python traceback, fix the underlying error, re-run gate. |

---

## 5. Concurrent-execution guards

The runbook stack assumes **at most one operator at a time**. Three layers of defence:

1. **`assert_jobs_process_stopped`** (`app/runbooks/safety.py:79-101`) — point-in-time probe. Refuses if `JOBS_PROCESS_LOCK_KEY` advisory lock is held on the application DB.
2. **`acquire_jobs_process_fence`** (`app/jobs/locks.py`) — runbook holds the fence for the duration of DROP+CREATE+migrate window. Per `stream_a_run_8_verify.py:23-32`: the fence **dies with DROP DATABASE** (PG advisory locks are PER-DATABASE in PG 9.0+; empirically verified in `tests/test_jobs_process_probe_fence.py::test_per_database_isolation_regression_gate`). The runbook re-acquires the fence on the FRESH DB after migrations. The TOCTOU window during drop-and-create is unavoidable at the lock layer alone — **operator MUST keep the jobs service stopped throughout**.
3. **Drift detection mid-poll** (`stream_a_run_8_verify.py:276-293`) — if `observed_run_id != captured_run_id` during poll, runbook exits 3 (CRITICAL) without cancelling the foreign run. Manual triage required (see §3.4 exit 3).

**If two operators run `--apply` simultaneously:**

- Operator B's `assert_jobs_process_stopped` likely PASSES (the lock is per-DB; Operator A holds it on the application DB).
- Operator B tries to acquire the fence → `JobAlreadyRunning` → exit 2 with `"REFUSE: could not acquire JOBS_PROCESS_LOCK_KEY fence"` message.
- If Operator B somehow races past the fence (very narrow window), Operator A's mid-poll drift detection at `:276` triggers exit 3.

**Practical guidance:** before `--apply`, coordinate via Slack/wiki. The lock layer protects against accidents, not deliberate concurrent drives.

---

## 6. Resume protocol (partial Run #8 failure)

### 6.1 If `--apply` dies after migration but before terminal status

Per Stage C row 8: PR-B sidecar may be populated for some CIKs; PR-A boot guard holds. The runbook is **not** mid-flight re-runnable — re-invoking `--apply` will DROP+CREATE the DB again, wiping the partial work.

**Recovery decision tree:**

1. Read the JSONL log from the failed invocation: `cat var/runbooks/stream_a_run_8_verify-*-*.jsonl | tail -1 | jq .`.
2. Check `captured_run_id` and last logged `step`.
3. If the run reached `step: "bootstrap_run_captured"` (`:247`), the bootstrap is queued under that `run_id` and may already be progressing or stuck.
4. Check current state: `curl -s -u operator:$PW http://127.0.0.1:8000/system/bootstrap-status | jq '.state_status, .summary'`.
5. If `state_status='running'`: do NOT re-`--apply`. Either wait it out (manual `_poll_with_retry` equivalent: poll `/system/bootstrap-status` every 30 s) OR cancel via `POST /system/bootstrap/cancel` then re-`--apply` from clean.
6. If `state_status` ∈ `{complete, partial_error}`: proceed to §4 gate against the captured `run_id`.
7. If `state_status='pending'` and `summary.running=0`: the dispatcher exited with stuck-pending (#1224 exemplar). Manually invoke `POST /system/bootstrap/retry-failed` via the admin UI OR re-`--apply` from clean.

### 6.2 Sidecar-only repair (no full re-run needed)

If §4 C7 fails for a small subset of CIKs but C1-C6 pass, you do NOT need to re-bootstrap. Use the sidecar repair runbook:

```bash
# Dry-run first.
EBULL_ENV=dev python -m app.runbooks.stream_a_t13_sidecar_repair \
  --archive-path /var/lib/ebull/sec_archives/submissions.zip

# Apply for a specific CIK (10-digit padded).
EBULL_ENV=dev python -m app.runbooks.stream_a_t13_sidecar_repair \
  --apply --archive-path /var/lib/ebull/sec_archives/submissions.zip \
  --cik 0000320193 --bootstrap-run-id <RUN_ID>

# Apply for ALL CIKs in archive.
EBULL_ENV=dev python -m app.runbooks.stream_a_t13_sidecar_repair \
  --apply --archive-path /var/lib/ebull/sec_archives/submissions.zip \
  --bootstrap-run-id <RUN_ID>
```

Per `app/runbooks/stream_a_t13_sidecar_repair.py:166-182`: repair is per-CIK idempotent (DELETE+INSERT under per-CIK transaction). Failure prints `RECOVERY:` hint to re-run with same arguments OR re-download `submissions.zip` via S8 path.

Exit codes (verified):

| Exit | Meaning |
|---|---|
| `0` | Repair complete, telemetry envelope printed |
| `1` | Repair raised an exception mid-run (re-runnable per docstring) |
| `2` | Invalid input / refused precondition (env guards, missing `--archive-path`, archive does not exist) |

After repair, re-run §4 gate against the same `--bootstrap-run-id`.

---

## 7. Observability during the run

Keep two terminals open: one running `--apply` (or tailing its stdout), one for live SQL probes below.

### 7.1 Per-stage live progress

```bash
# OP-I3 fold: the runbook itself only logs stage_count per tick.
# This query surfaces what's actually running RIGHT NOW.
watch -n 15 'psql "$DATABASE_URL" -c "
  SELECT
    stage_order,
    stage_key,
    lane,
    status,
    attempt_count,
    EXTRACT(EPOCH FROM (now() - started_at))::int AS elapsed_sec,
    left(coalesce(last_error, '\'\''),  80) AS err
  FROM bootstrap_run_stages
  WHERE run_id = (SELECT max(id) FROM bootstrap_runs)
  ORDER BY stage_order"'
```

### 7.2 Advisory-lock leak probe

Stage C residual #4: Run #7 leaked 7+ conns >35 min. If wall-clock exceeds 200 min, check:

```bash
psql "$DATABASE_URL" -c "
  SELECT pid, application_name, state,
         EXTRACT(EPOCH FROM (now() - state_change))::int AS state_age_sec,
         left(query, 80) AS q
  FROM pg_stat_activity
  WHERE datname = current_database()
    AND state = 'idle in transaction'
    AND (now() - state_change) > INTERVAL '5 minutes'
  ORDER BY state_change ASC"
```

Non-empty rows older than 5 min during bootstrap = advisory-lock leak. Kill with `SELECT pg_terminate_backend(<pid>)` (this terminates the leaked conn but does NOT cancel the bootstrap if it's a stage worker).

### 7.3 PG health snapshot (live)

```bash
# Replace $PW with your generated operator password (printed by --apply banner).
# If 401: see §1.2 P5 caveat (Codex 2 HIGH DEFERRED).
curl -s -u operator:$PW http://127.0.0.1:8000/system/postgres-health | jq '
  {
    db_size_pretty,
    db_size_breached: .db_size_breached_warn,
    wal_dir_pretty,
    wal_breached: .wal_breached_warn,
    facts_raw_default_rows: .financial_facts_raw_default_rows,
    metric_errors
  }'
```

Watch for `db_size_breached_warn=true` (> 50 GB) or `wal_breached_warn=true` (> 4 GB per `.githooks/pre-push` budget).

### 7.4 Bootstrap status (live)

```bash
curl -s -u operator:$PW http://127.0.0.1:8000/system/bootstrap-status | jq '
  {
    state_status,
    current_run_id,
    summary,
    retry_available,
    retry_blocked_reason,
    running_stages: [.stages[] | select(.status == "running") | {stage_key, attempt_count}],
    failed_stages: [.stages[] | select(.status == "error") | {stage_key, last_error}]
  }'
```

---

## 8. Logging

Both runbooks write JSONL envelopes to `var/runbooks/`:

- `var/runbooks/stream_a_run_8_verify-<run_id>-<ts>.jsonl` (`stream_a_run_8_verify.py:74`)
- `var/runbooks/stream_a_stream_c_gate-<run_id>-<ts>.jsonl` (`stream_a_stream_c_gate.py:54`)
- `var/runbooks/stream_a_t13_sidecar_repair-<ts>.jsonl` (`stream_a_t13_sidecar_repair.py:74`)

Each file is append-only. **OP-I8 caveat:** no rotation policy in place — log dir grows monotonically. Add `var/runbooks/` to your local `.gitignore` if not already.

Path always printed on stderr at exit. Quick survey: `ls -lh var/runbooks/ | tail -20`.

---

## 9. Known operator gotchas (from committee)

These IMPORTANT / OBSERVATION findings won't fail Run #8 outright but will surprise an unprepared operator. Skim before `--apply`.

| # | Finding | Effect during run | Mitigation |
|---|---|---|---|
| OP-O2 | `wait_for_jobs_process_started` defaults to 600 s (10 min) | If you walk away after dispatch, jobs-process-start may exceed budget → exit 2 with run_id captured | Use `--wait-for-jobs-sec 1800` per §3.1 |
| OP-I3 | Poll heartbeat only logs `stage_count` | Cannot tell "stuck" from "slow" at minute 130 | Open §7.1 in second terminal |
| OP-I7 | Exit 3 (drift) does NOT print foreign run details | You see `CRITICAL: ... observed current_run_id=<int>` but no context | Manually run `SELECT id, started_at, status FROM bootstrap_runs WHERE status='running'` post-exit |
| ARCH-B1 | `funds` + `esop` only refresh via 03:30 UTC sweep | C6_funds + C6_esop may legitimately warn-quiescent if Run #8 finished after 03:30 UTC | Accept warn; re-run gate after next 03:30 UTC if needed |
| API-I1 | `JOB_SEC_DAILY_INDEX_RECONCILE` exempt from `_bootstrap_complete` gate (#1181 carve-out) | Steady-state cron can steal SEC budget mid-bootstrap, ~1 reconcile run per ~hour | Accept; budget impact ~1-3 min per occurrence |
| API-I5 | SEC 429 / Retry-After not honoured | If SEC briefly throttles, providers raise `HTTPStatusError` → stage marks fetch failure | Re-run the failed stage via admin UI retry-failed once SEC unthrottles |
| API-I4 | 13D/G pre-2024-12-18 filings tombstone-on-parse | Ownership rollup wedge stays empty for any CIK whose only 13D/G filings predate the XML mandate | Acceptable; documented in skill |
| TE-I1 | Smoke test does NOT exercise PR-A boot guard runtime | Future PR-A regression would not be caught by `test_app_boots.py` | Tech-debt; does not affect this drive |
| TE-I4 | No CUSIP resolution-rate floor test | S13 cusip_resolver effectively no-op (19/16M); regression invisible | Post-Run-8 ticket #740 / Stage D ticket-gap-3 |
| DE-IMP3 | CUSIP resolver ~10⁻⁶ resolution rate | Expected during Run #8; will see 0-50 new `external_identifiers` rows from S13 | Accept; Codex's #1 next-work item |
| O5 | `var/runbooks/` retention unmanaged | Logs accrete forever | Manual rotation post-drive |

---

## 10. After Run #8 success — close-out

Per PM-I5 #1233 close-eligibility checklist. All four MUST be true:

1. **Wall-clock recorded.** Post a #1233 comment summarising total wall-clock + per-critical-path-stage wall-clocks (S14, S16, S22, S25). Use the JSONL log entries at `step:"terminal"` for accurate timing.

2. **Stream-C gate JSON envelope posted to #1233 + persisted.** Use the `gh issue comment` invocation from §4.2. Verify column persistence:

   ```bash
   psql "$DATABASE_URL" -At -c \
     "SELECT id, stream_c_gate_status FROM bootstrap_runs WHERE id = <RUN_ID>"
   ```

   Expect `<RUN_ID>|passed`.

3. **Any FAIL-LOUD output triaged.** Per §3.3: stuck-pending, `rows_processed=NULL`, CardinalityViolation, max_locks breach, exit 3 — each must either be FIXED in a follow-up PR or DEFERRED to a tracked ticket. Document in #1233 comment.

4. **Stage H runbook merged.** This document (`docs/operator/runbooks/run-8-readiness.md`) merged via the post-sweep skill-polish PR — verify the merge SHA is in `git log --oneline main | head -5` before claiming close.

If all 4 yes → close #1233. Update memory:

```bash
# Append to project memory.
cat >> /Users/lukebradford/.claude/projects/-Users-lukebradford-Dev-eBull/memory/project_1233_close.md <<'EOF'
# #1233 CLOSED — <date>
- Wall-clock: <total min>
- S14: <min>, S16: <min>, S22: <min>, S25: <min>
- Stream-C gate: passed (run_id=<id>)
- FAIL-LOUD residuals: <none | list>
EOF
```

Next-best-work per Codex Strategic Findings: **CUSIP resolution / #740** (highest-ROI post-Stream-A residual; will be the first "unexpected operator surprise" on chart drill-down).

---

## 11. Cross-references

### Stage memos (this sweep)

- `/Users/lukebradford/.claude/projects/-Users-lukebradford-Dev-eBull/memory/project_etl_sweep_stage_a_2026_05_24.md` — origin trace
- `/Users/lukebradford/.claude/projects/-Users-lukebradford-Dev-eBull/memory/project_etl_sweep_stage_b_2026_05_24.md` — 5-layer pipeline matrix
- `/Users/lukebradford/.claude/projects/-Users-lukebradford-Dev-eBull/memory/project_etl_sweep_stage_c_2026_05_24.md` — bootstrap S1-S27 + wall-clock bands
- `/Users/lukebradford/.claude/projects/-Users-lukebradford-Dev-eBull/memory/project_etl_sweep_stage_d_2026_05_24.md` — steady-state per-source
- `/Users/lukebradford/.claude/projects/-Users-lukebradford-Dev-eBull/memory/project_etl_sweep_stage_e_2026_05_24.md` — tech-debt triage
- `/Users/lukebradford/.claude/projects/-Users-lukebradford-Dev-eBull/memory/project_etl_sweep_stage_f_2026_05_24.md` — skill-polish audit

### Stage G committee memos

- `project_etl_sweep_stage_g_committee_architect_2026_05_24.md`
- `project_etl_sweep_stage_g_committee_reviewer_2026_05_24.md`
- `project_etl_sweep_stage_g_committee_data_engineer_2026_05_24.md`
- `project_etl_sweep_stage_g_committee_api_contract_2026_05_24.md`
- `project_etl_sweep_stage_g_committee_operator_2026_05_24.md` — primary input to this runbook
- `project_etl_sweep_stage_g_committee_test_engineer_2026_05_24.md`
- `project_etl_sweep_stage_g_committee_pm_2026_05_24.md` — primary input for §3.3 rubric + §10 close-out
- `project_etl_sweep_stage_g_committee_codex_2026_05_24.md` — strategic synthesis

### Code paths

- `app/runbooks/safety.py:36-144` — `RunbookRefused` + 3-tier safety primitives + `wait_for_jobs_process_started`
- `app/runbooks/stream_a_run_8_verify.py:34-42,113-156,251-323,371-548` — exit codes, drop/create, poll loop, main
- `app/runbooks/stream_a_t13_sidecar_repair.py:80-205` — repair main + dry-run preview
- `app/runbooks/stream_a_stream_c_gate.py:53-435` — gate main + C1-C7 + persistence
- `app/services/bootstrap_orchestrator.py:237-270` — `_LANE_MAX_CONCURRENCY` (12 entries; 11 operationally active per REV-I1)
- `app/services/bootstrap_orchestrator.py:1035-1193` — `_BOOTSTRAP_STAGE_SPECS` (27 stages)
- `app/services/bootstrap_orchestrator.py:286-338` — `Capability` Literal (15 values per REV-I2)
- `app/api/system.py:687-770` — `/system/bootstrap-status` (lean readout)
- `app/api/system.py:773-851` — `/system/postgres-health` (7 metrics + breach flags)
- `sql/171_bootstrap_state_last_jobs_boot_error.sql` — PR-A boot-error column
- `sql/172_sec_cik_submissions_files_index.sql` — PR-B sidecar table (PK `(cik, page_name)` + sentinel-row CHECK)
- `sql/173_bootstrap_runs_stream_c_gate.sql` — PR-B gate-status column (CHECK constraint on `pending|passed|failed_<id>`)

### Spec / settled-decisions

- `docs/specs/etl/retention-rubric.md:343-372` — §6.3 pre-wipe canonical text
- `docs/proposals/etl/stream-a-run-8-fixes.md` v2.4 §17 — Stream A spec (live)
- `project_1233_pr12_ownership_merge_writer.md` — operator follow-up (multixact wraparound context)
- `project_1208` — postgres-health 7 metrics + Codex 2 HIGH DEFERRED (401-not-503 when PG down)
- `project_stream_a_pr_d_session_end_2026_05_24.md` — PR-D merge `91aa214` summary
