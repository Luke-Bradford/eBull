# `/system/postgres-health` + pre-push hook bloat warn

> Status: **2026-05-19 (v3 — post-Codex-1a + 1b).**
>
> Codex 1a (v1 → v2) summary: 2 BLOCKING + 8 WARNING + 3 LOW — all resolved in §8.1. Biggest corrections: (a) psycopg `try/except` per query inside a normal tx does NOT isolate — first SQL error aborts the tx and every subsequent query fails. Service opens its own autocommit conn. (b) Hook's double-quoted `echo` with backticks triggers command substitution at hook-run time; rewritten with single quotes.
>
> Codex 1b (v2 → v3) summary: 0 BLOCKING + 5 WARNING + 3 LOW — all resolved in §8.2. Drift cleanup from the v2 rewrites (T2 signature, query-count claim, nullable semantics in §6, WAL breach-field disambiguation, WAL test coverage).
>
> Issue: **#1208 Subs 4 + 5.** Branch: `feature/1208-phase4-postgres-health`.
>
> Phase 4 (final phase) of `docs/superpowers/plans/2026-05-18-backend-stability.md`. Phase 5 (prevention-log + skill updates) folds into this PR per plan §3.
>
> Closure framing: **OBSERVABILITY PRIMITIVE.** Phases 1-3 added tuning + hygiene + partition + retention but no operator-visible health signal. Phase 4 closes that gap.

## 1. Problem

Phases 1-3 of #1208 made dev DB substantially more resilient — tuned Postgres knobs (Phase 1), orphan sweep + keepalive (Phase 2), partitioned `financial_facts_raw` + daily retention sweep (Phase 3). But operator has **no live visibility** into whether those guardrails are holding:

- Is `pg_database_size('ebull')` still under control? (Today: 41 GB; target post-Phase-3 < 25 GB; epic target < 5 GB needs further compaction.)
- Did orphan sweep keep `ebull_test_*` leaks at zero? (Phase 2 swept 45 → 0; no live readout.)
- Is `max_wal_size=4 GB` being approached during ingest bursts? (Phase 1 tuned it; no dashboard.)
- Is autovacuum lag bounded per partition? (Phase 3's whole motivation.)
- Did parser junk start landing in `financial_facts_raw_default` at scale? (Spec §4.1.1 set 5000-row alarm; no live check.)

Closure framing dictates one endpoint that surfaces all five signals + a pre-push hook that warns the operator if the dev DB bloats past 10 GB so unmerged growth is visible before more code piles on.

## 2. Spike receipts (2026-05-19 dev cluster, post-Phase-3 merge + retention sweep)

```text
$ docker exec ebull-postgres psql -U postgres -d ebull -tAc \
    "SELECT pg_size_pretty(pg_database_size('ebull'));"
41 GB

$ docker exec ebull-postgres psql -U postgres -d ebull -tAc \
    "SELECT pg_size_pretty(sum(size)) FROM pg_ls_waldir();"
4096 MB

$ docker exec ebull-postgres psql -U postgres -d ebull -tAc \
    "SELECT checkpoint_time FROM pg_control_checkpoint();"
2026-05-19 01:46:09+00

$ docker exec ebull-postgres psql -U postgres -d ebull -c \
    "SELECT relname, last_autovacuum, n_dead_tup FROM pg_stat_user_tables \
     ORDER BY n_dead_tup DESC LIMIT 5;"
financial_facts_raw_2024q3 | 2026-05-19 01:25:59 |  2397
financial_facts_raw_2025q2 | 2026-05-19 01:26:22 |   394
...
```

Every signal is queryable via SQL + standard pg catalog functions (`pg_database_size`, `pg_ls_waldir`, `pg_control_checkpoint`, `pg_stat_user_tables`). No extra extensions needed.

## 3. Scope

| Task | Deliverable | Closure framing |
|---|---|---|
| T1 | `GET /system/postgres-health` endpoint returning `PostgresHealthResponse` | OBSERVABILITY PRIMITIVE |
| T2 | `app/services/postgres_health.py::collect_postgres_health(*, database_url=None)` — opens its OWN autocommit conn for per-query failure isolation (Codex 1a BLOCKING #1) | SERVICE PRIMITIVE |
| T3 | `.githooks/pre-push` addition: warn (not block) if `pg_database_size('ebull') > 10 GB` | OPS PRIMITIVE |
| T4 | `tests/test_postgres_health_endpoint.py` — endpoint shape + auth gate + service collection | TEST PRIMITIVE |
| T5 | `docs/review-prevention-log.md` + `.claude/skills/data-engineer/SKILL.md` — fold Phase 5 lessons (cross-link Phase 3 partition checklist to the new health endpoint as the operator-visible enforcement point) | DOCS PRIMITIVE |

## 4. Design

### 4.1 Endpoint shape

`GET /system/postgres-health` — mounted on the existing `app/api/system.py` router (auto-gated by `require_session_or_service_token`). Returns:

```python
class AutovacuumTableLag(BaseModel):
    relname: str
    last_autovacuum: datetime | None
    last_analyze: datetime | None
    n_dead_tup: int
    n_live_tup: int
    # v2 (Codex 1a LOW #2): explicit semantics. Float ratio of dead
    # to (dead + live) so the value sits in [0, 1] when both are
    # non-zero. Null when both are zero (no signal).
    dead_fraction: float | None


class PostgresHealthResponse(BaseModel):
    # v2 (Codex 1a WARNING #5): every metric is nullable. A single
    # query failure no longer falsely clears or trips a breach flag.
    # breach flags are nullable too — null means "metric not collected
    # this poll; cannot say."
    db_size_bytes: int | None
    db_size_pretty: str | None
    db_size_warn_threshold_bytes: int   # 10 GB — matches pre-push hook gate
    db_size_breached_warn: bool | None
    leaked_test_db_count: int | None
    leaked_test_db_names: list[str] | None
    # WAL metric (v2, Codex 1a WARNING #4 + #5): two distinct signals —
    # `wal_dir_bytes` is the size of pg_wal/ on disk (what
    # max_wal_size controls); `wal_since_checkpoint_bytes` is the
    # distance from the previous checkpoint's redo_lsn (the actual
    # "burst pressure" signal). Threshold + breach against
    # max_wal_size = 4 GB.
    wal_dir_bytes: int | None
    wal_dir_pretty: str | None
    wal_since_checkpoint_bytes: int | None
    wal_warn_threshold_bytes: int   # 4 GB — matches max_wal_size
    # v3 (Codex 1b): explicit disambiguation. Breach keys ONLY on
    # `wal_dir_bytes > wal_warn_threshold_bytes`. `wal_since_checkpoint_bytes`
    # is informational (burst-pressure signal); operator wires their
    # own alert if they want a second threshold there.
    wal_breached_warn: bool | None
    last_checkpoint_at: datetime | None
    autovacuum_top10: list[AutovacuumTableLag] | None   # sorted by n_dead_tup DESC
    financial_facts_raw_default_rows: int | None
    financial_facts_raw_default_warn_threshold: int   # 5000
    financial_facts_raw_default_breached_warn: bool | None
    # v2 (Codex 1a WARNING #5): per-metric error log for ops triage.
    # Empty list when every probe succeeded.
    metric_errors: list[str]
    collected_at: datetime
```

`*_breached_warn` flags are precomputed server-side so the FE doesn't need to know the thresholds. Each is `bool | None` — `None` when the underlying metric query failed so the operator can distinguish "value not exceeded" from "value unknown."

### 4.2 Service implementation

```python
# app/services/postgres_health.py
DB_SIZE_WARN_BYTES = 10 * 1024 * 1024 * 1024     # 10 GB
WAL_WARN_BYTES = 4 * 1024 * 1024 * 1024          # 4 GB (= max_wal_size)
DEFAULT_PARTITION_WARN_ROWS = 5000


def collect_postgres_health(
    *,
    database_url: str | None = None,
) -> PostgresHealthResponse:
    """Read-only metrics collection.

    v2 (Codex 1a BLOCKING #1): opens its OWN autocommit connection.
    Per-query try/except inside a normal (autocommit=False) tx does
    NOT isolate failures — the first SQL error aborts the entire tx
    and every subsequent query fails with `current transaction is
    aborted`. Autocommit gives each query its own implicit tx so an
    error on one metric never leaks into the next.

    Seven metric queries, seven independent round-trips. Failed metrics
    contribute `None` to the response + a string entry to
    `metric_errors`. The endpoint always returns 200 unless the
    connection itself cannot be opened (then 503, per the existing
    fail-closed posture documented at `app/api/system.py:24`).
    """
    url = database_url or settings.database_url
    errors: list[str] = []
    with psycopg.connect(url, autocommit=True) as conn:
        db_size_bytes = _safe(conn, _q_db_size, errors)
        leaked = _safe(conn, _q_leaked_dbs, errors)
        wal_dir_bytes = _safe(conn, _q_wal_dir, errors)
        wal_since_ckpt = _safe(conn, _q_wal_since_checkpoint, errors)
        last_ckpt = _safe(conn, _q_last_checkpoint, errors)
        top10 = _safe(conn, _q_autovacuum_top10, errors)
        default_rows = _safe(conn, _q_default_partition_rows, errors)
    return _assemble(...)
```

Metric queries (seven independent statements; each runs in its own implicit autocommit tx):

```sql
-- db_size
SELECT pg_database_size(current_database());

-- leaked test DBs
SELECT datname FROM pg_database
 WHERE datname LIKE 'ebull_test_%'
   AND datname != 'ebull_test_template'
 ORDER BY datname;

-- WAL directory size on disk (PG13+ pg_ls_waldir requires pg_monitor
-- role or superuser). Measures retained WAL bytes against max_wal_size.
SELECT COALESCE(sum(size), 0)::bigint FROM pg_ls_waldir();

-- WAL written since the previous checkpoint (the actual "burst pressure"
-- signal — distance to checkpoint exhaustion). Codex 1a WARNING #5
-- semantic clarification.
SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), redo_lsn)::bigint
  FROM pg_control_checkpoint();

-- last checkpoint
SELECT checkpoint_time FROM pg_control_checkpoint();

-- autovacuum top-10 (Codex 1a WARNING #9: full sort across all user
-- tables; documented as O(n_user_tables) — fine on our dev DB with
-- ~100 user tables, document the cap if user-table count balloons).
SELECT relname, last_autovacuum, last_analyze, n_dead_tup, n_live_tup
  FROM pg_stat_user_tables
 ORDER BY n_dead_tup DESC NULLS LAST
 LIMIT 10;

-- DEFAULT-partition row count (post-Phase-3 alarm threshold = 5000
-- per spec §4.1.1). Codex 1a WARNING #6: full seq scan on the
-- partition; current size ~1055 rows + small drift — cheap. If the
-- partition ever grows past ~100k rows the alarm has already fired
-- long since; the cost stays bounded by the alarm semantics.
SELECT count(*) FROM financial_facts_raw_default;
```

### 4.3 Failure isolation

Each metric query runs in its own implicit autocommit tx (§4.2). A SQL-layer error (e.g. `pg_monitor` role required for `pg_ls_waldir` on a non-superuser DB) is caught + logged + appended to `metric_errors` + the corresponding response field is `None`. Breach flags are computed only against non-null metrics; a missing metric leaves its breach flag `None` rather than silently `False` (Codex 1a WARNING #5 — `0` defaults falsely clear warn flags).

A connection-level failure (psycopg cannot open the conn) raises 503 per the existing fail-closed posture documented at `app/api/system.py:24`.

### 4.4 Pre-push hook bloat warn

Append to `.githooks/pre-push` just before the final `green. Pushing.` line.

v2 (Codex 1a BLOCKING #2): the warning echo uses **single quotes** so backticks inside the message stay literal. With double quotes, `` `docker exec ebull-postgres psql ...` `` inside the echo triggers command substitution AT HOOK-RUN TIME — that runs the entire psql probe a second time, with no error handling, every push.

v2 (Codex 1a WARNING #3): the 10 GB threshold appears in TWO places — `DB_SIZE_WARN_BYTES` in `app/services/postgres_health.py` and `10737418240` in this hook. Pinned via a single bash variable + a checked-in cross-reference comment + a Python test (§5.3) that loads the hook text and asserts the literal matches `DB_SIZE_WARN_BYTES`.

```bash
# #1208 Phase 4 — dev DB bloat warning (non-blocking).
# Surfaces creeping growth at push time so the operator notices BEFORE
# more code piles on top of an unswept DB. Threshold is the same
# constant exposed by GET /system/postgres-health
# (`DB_SIZE_WARN_BYTES` in app/services/postgres_health.py); a test
# (tests/test_pre_push_hook_bloat_warn.py) asserts the two stay
# aligned.
DB_SIZE_WARN_BYTES=10737418240  # 10 GB
if command -v docker >/dev/null 2>&1; then
  db_size=$(docker exec ebull-postgres psql -U postgres -d ebull -tAc \
    "SELECT pg_database_size('ebull')" 2>/dev/null | tr -d ' ' || true)
  if [[ -n "${db_size}" && "${db_size}" -gt "${DB_SIZE_WARN_BYTES}" ]]; then
    pretty=$(docker exec ebull-postgres psql -U postgres -d ebull -tAc \
      "SELECT pg_size_pretty(pg_database_size('ebull'))" 2>/dev/null \
      | tr -d ' ' || true)
    echo '==> WARN: dev DB ebull size = '"${pretty}"' (> 10 GB).'
    echo '==> WARN: investigate via GET /system/postgres-health.'
    echo '==> WARN: non-blocking; push continues.'
  fi
fi
```

Non-blocking: if `docker` is absent (e.g. CI runner, fresh clone) the probe silently no-ops. If size is within threshold, no output (avoid hook noise). Single-quoted echoes with `"${var}"` concatenation keep the threshold message ASCII-only (Codex 1a LOW #3) and free of backtick command substitution.

The first `docker exec` failure-path now uses `... || true` so the `set -euo pipefail` shell config (line 33) cannot kill the whole hook if psql is briefly unavailable — same posture as the orphan-sweep best-effort cleanup.

### 4.5 Auth posture

Endpoint inherits router-level `Depends(require_session_or_service_token)` — same gate as `/system/status` + `/system/jobs`. Test asserts a no-auth request returns 401/403 (whichever the auth helper returns).

## 5. Tests

### 5.1 `tests/test_postgres_health_endpoint.py` + `tests/test_pre_push_hook_bloat_warn.py`

| Case | Assertion |
|---|---|
| `test_endpoint_requires_auth` | No token → 401/403 |
| `test_endpoint_returns_all_fields` | With session/service token → 200 + every `PostgresHealthResponse` field present, types correct (nullable fields tolerated) |
| `test_db_size_breach_flag_below_threshold` | Threshold injected at 10 GB; test DB <10 GB → `db_size_breached_warn = False` |
| `test_db_size_breach_flag_above_threshold` | Threshold injected at 1 byte → `db_size_breached_warn = True` (Codex 1a WARNING #6 — above-threshold coverage) |
| `test_default_partition_warn_flag_above_threshold` | Threshold injected at 0 rows; seed 1 row → `financial_facts_raw_default_breached_warn = True` |
| `test_wal_breach_flag_above_threshold` | Threshold injected at 1 byte → `wal_breached_warn = True`; verifies the flag keys on `wal_dir_bytes` not `wal_since_checkpoint_bytes` (Codex 1b WARNING #5 — disambiguation) |
| `test_wal_metrics_null_when_pg_ls_waldir_denied` | Patch the wal_dir query to raise `psycopg.errors.InsufficientPrivilege` → `wal_dir_bytes is None` + `wal_breached_warn is None` + `metric_errors` lists `wal_dir`; other metrics unaffected |
| `test_metric_isolation_under_psycopg_error` | Patch one query function to raise `psycopg.Error`; endpoint still 200; affected metric is `None`; affected breach flag is `None`; `metric_errors` lists the failed metric name (Codex 1a BLOCKING #1 regression) |
| `test_autocommit_conn_used` | Recording proxy asserts the service opens an autocommit conn (otherwise per-metric isolation is broken). Codex 1a BLOCKING #1 regression. |
| `test_leaked_test_db_names_filtered` | The endpoint output excludes `ebull_test_template`; if no leaks, `leaked_test_db_names == []` |
| `test_pre_push_hook_threshold_matches_db_size_warn` | Loads `.githooks/pre-push`, asserts the literal `DB_SIZE_WARN_BYTES=10737418240` matches `app.services.postgres_health.DB_SIZE_WARN_BYTES` (Codex 1a WARNING #3 — drift gate) |
| `test_pre_push_hook_syntax` | Runs `bash -n .githooks/pre-push`; non-zero exit fails the test. Codex 1a WARNING #11 — syntax-validate the hook before merge. |

The pre-push hook itself is bash, but two Python tests guard against drift + syntax errors (above). Manual smoke is documented in the operator runbook §7.

## 6. Risk + rollback

| Risk | Mitigation |
|---|---|
| `pg_ls_waldir()` requires `pg_monitor` role or superuser; non-superuser dev role gets `ERROR: permission denied` | Per-metric isolation (§4.3) absorbs the error; endpoint returns `wal_dir_bytes = None` + `wal_breached_warn = None` + `metric_errors` lists the failed metric. Operator's dev role is already superuser. |
| Slow autovacuum query on a large `pg_stat_user_tables` (1000+ user tables) | `ORDER BY n_dead_tup DESC LIMIT 10` requires a full scan + top-K sort (Codex 1b LOW #1 — earlier claim "no full sort" was wrong). Current user-table count ~100 on dev so cost is negligible; revisit if `\dt` ever returns thousands of tables. |
| Pre-push hook adds latency to every push | Two `docker exec psql` calls on the warning path (one to read the size, one to pretty-print); one call on the non-warning path. <200 ms each. Hidden behind `command -v docker` check so non-docker environments skip cleanly. |
| Endpoint exposes leaked-DB names (operator-internal info) | Already behind `require_session_or_service_token`. Same auth surface as `/system/status` which reveals the entire job + freshness map. |

Rollback: remove the endpoint file + the hook block. No schema changes. Idempotent.

## 7. Operator runbook

Post-merge:
1. Hit `GET /system/postgres-health` with the session cookie. Expect 200 with current dev numbers (db ~41 GB pre-VACUUM-FULL, WAL ~4 GB, default-partition rows ~1055 + small drift).
2. `db_size_breached_warn` will be `true` until VACUUM FULL or natural insert-reuse drops the file size below 10 GB. Acceptable; the flag is the operator signal for "schedule a maintenance window."
3. Run `git push` on an unrelated branch — confirm the pre-push hook emits the warning line (since db_size > 10 GB).

## 8. Codex iterations

### 8.1 Codex 1a — spec v1 review (2026-05-19)

| Severity | Finding | Resolution |
|---|---|---|
| BLOCKING | `try/except psycopg.Error` per query inside one normal tx doesn't isolate — first error aborts the tx | §4.2 service opens its OWN `psycopg.connect(url, autocommit=True)`. Six independent metric queries, each in its own implicit tx. |
| BLOCKING | `echo "... \`docker exec ...\` ..."` runs the backticks at hook-run time | §4.4 single-quoted echo with `"${var}"` concatenation; backticks never appear in the message; ASCII-only. |
| WARNING | Hook hardcodes `10737418240` separately from `DB_SIZE_WARN_BYTES` | §4.4 hook + §5 new test `test_pre_push_hook_threshold_matches_db_size_warn`. |
| WARNING | WAL metric has no threshold/breach flag | §4.1 + §4.2: two WAL fields — `wal_dir_bytes` against `max_wal_size=4 GB` + `wal_since_checkpoint_bytes` (the burst-pressure signal). Breach flag against 4 GB threshold. |
| WARNING | `pg_ls_waldir()` semantics unclear ("retained" not "burst") | §4.2 inline comment + §4.1 dual-field response. |
| WARNING | `count(*)` on default partition is the expensive path when junk grows | §4.2 inline comment — bounded by the alarm semantics (>5000 rows fires the alarm long before scan cost matters). |
| WARNING | Failed metrics defaulting to `0` falsely clear breach flags | §4.1: every metric is `int \| None` / `str \| None`; breach flags are `bool \| None`. §5 isolation test asserts the null pathway. |
| WARNING | "Five round-trips" vs six queries spec mismatch | §4.2 reads "six metric queries" everywhere now. |
| WARNING | LIMIT 10 still scans + sorts all `pg_stat_user_tables` | §4.2 inline comment + acceptance note (current user-table count ~100, document if it balloons). |
| WARNING | Tests only assert below-threshold flags | §5 adds `test_db_size_breach_flag_above_threshold` + `test_default_partition_warn_flag_above_threshold` via injectable threshold. |
| WARNING | No hook test despite command-substitution risk | §5 adds `test_pre_push_hook_threshold_matches_db_size_warn` (drift gate) + `test_pre_push_hook_syntax` (`bash -n`). |
| LOW | Inconsistent docker-exec call count description | §4.4 prose rewritten — no count claim, just the literal block. |
| LOW | `dead_ratio` description suggests percentage | §4.1 renamed to `dead_fraction` + comment: dead/(dead+live), bounded in [0, 1], null when both zero. |
| LOW | Non-ASCII emoji in hook output | §4.4 ASCII-only `==> WARN:` prefixes. |

### 8.2 Codex 1b — spec v2 review (2026-05-19)

| Severity | Finding | Resolution |
|---|---|---|
| WARNING | T2 scope row advertised old `(conn)` signature contradicting §4.2's autocommit-conn rewrite | §3 updated to `collect_postgres_health(*, database_url=None)`. |
| WARNING | §4.2 still said "six metric queries"; actual count is seven | Both occurrences updated to "seven". |
| WARNING | §6 risk table said failed `pg_ls_waldir()` returns `wal_size_bytes = 0` — regresses nullable semantics | §6 row updated to `wal_dir_bytes = None` + `wal_breached_warn = None`. |
| WARNING | `wal_breached_warn` didn't disambiguate which WAL field it keys on | §4.1 inline comment: breach keys ONLY on `wal_dir_bytes`. |
| WARNING | Test plan lacked WAL breach + WAL-null coverage | §5 adds `test_wal_breach_flag_above_threshold` + `test_wal_metrics_null_when_pg_ls_waldir_denied`. |
| LOW | §6 "no full sort" still contradicted §4.2's O(n) acceptance note | §6 row rewritten to acknowledge full scan + top-K sort. |
| LOW | "Latency: one docker exec" mismatched 2-call warning path | §6 row updated to "two calls on warning path, one on non-warning." |
| LOW | §5 heading named only one test file, but spec defines two | §5.1 heading updated to list both files. |

### 8.3 Codex 2 — pre-push diff review (2026-05-19)

| Severity | Finding | Resolution |
|---|---|---|
| HIGH | `app/api/auth.py:125` resolves `Depends(get_conn)` before bearer/session branching → `/system/postgres-health` can fail in auth DB checkout before reaching the handler's 503 path | `DEFERRED` (tech-debt follow-up). Pre-existing pattern across every `/system/*` endpoint; refactoring the auth helper to lazy-resolve the DB conn is a cross-cutting change touching every other auth-gated route. Phase 4 inherits the limitation; documented in §7 + a follow-up issue. Practical impact: if PG is fully down, the endpoint returns 401 (from get_conn raising) rather than 503 (which is what the operator would prefer). Either way the operator concludes "PG is broken." |
| MED | 503 test patches `collect_postgres_health` not the underlying `psycopg.connect` | `tests/test_api_postgres_health.py::test_endpoint_returns_503_on_real_psycopg_connect_failure` — patches `psycopg.connect` directly. |
| MED | Per-metric isolation test mocks the snapshot; would still pass if `autocommit=True` is removed | `tests/test_postgres_health_service.py` — runs the real service against the test DB, monkey-patches one `_q_*` to raise `InsufficientPrivilege`, asserts subsequent probes succeed. The test fails if autocommit is removed. |
| LOW | `-gt` on raw psql stdout fails on non-numeric blip under `set -euo pipefail` | Hook adds `[[ "${db_size}" =~ ^[0-9]+$ && ... ]]` integer-regex guard. |
