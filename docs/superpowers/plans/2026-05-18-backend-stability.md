# Backend stability + dev DB hygiene — plan (#1208)

> Phase: 9 of `docs/superpowers/plans/2026-05-17-us-etl-completion.md` (separate epic).
> Issue: #1208 OPEN.
> Cousin to the ETL completion plan — same "session-by-session, autonomous-execution, one PR per phase, handover prompt at the end" cadence.
> Closure framing: **BACKEND-STABILITY PRIMITIVE** — mirrors `ETL FRESHNESS PRIMITIVE` shape from #863-#873.

## 0. Parent-ETL status as of 2026-05-18

The US ETL completion plan (`docs/superpowers/plans/2026-05-17-us-etl-completion.md`) is **CLOSED through Phase 6 inclusive**:

- Phase 1-4: ✅ closed (PRs #1190 / #1191 / #1193 / #1194 / #1196 / #1198 / #1200).
- Phase 5: ✅ closed 2026-05-18 (PRs #1203 / #1205).
- Phase 6: ✅ closed 2026-05-18 (PRs #1207 bimonthly + **#1209 RegSHO daily, merge `65dc4fc`**).

No remaining headline data-source gaps. **#1208 (this plan) is the next-priority track** — it has been pre-staged as Phase 9 of the parent plan and is scoped + budgeted independently.

## 1. Scope from #1208 (issue body, lightly re-organised + extended)

| Sub | Theme | Headline deliverable | Estimate (LOC) | Closure framing |
|---|---|---|---|---|
| Sub 1 | Postgres tuning | `sql/NNN_postgres_runtime_tuning.sql` — max_wal_size/shared_buffers/wal_compression + docker-compose mem_limit + shm_size | ~200 | TUNING PRIMITIVE |
| Sub 2 | Test-fixture orphan sweep | `_drop_orphan_workers_older_than` in `tests/fixtures/ebull_test_db.py` + slim-data posture audit | ~300 | HYGIENE PRIMITIVE |
| Sub 3 | `financial_facts_raw` partition | Quarterly RANGE-by-`period_end` partition migration + retention horizon enforcement | ~400 | SCHEMA PRIMITIVE |
| Sub 4 | Observability | `/system/postgres-health` endpoint + pre-push hook bloat warning | ~200 | OBSERVABILITY PRIMITIVE |
| Sub 5 | Prevention-log entry | `docs/review-prevention-log.md` Postgres-on-Docker section | ~50 | DOCS PRIMITIVE |
| **Sub 6** | **Runtime-config singleton resilience** | **NEW — see §2 below** | ~150 | RESILIENCE PRIMITIVE |

## 2. Sub 6 — Runtime-config singleton resilience (NEW, added 2026-05-18 session)

### Symptom

Operator login on 2026-05-18 was slow + FE polling `/config` returned `503 Service Unavailable` twice (captured in user-supplied logs). Investigation found `runtime_config` table on dev DB had **0 rows**.

### Root cause

Migration `sql/015_runtime_config.sql` seeds the singleton via `INSERT ... ON CONFLICT (id) DO NOTHING`. The migration ran successfully (in `schema_migrations`). Row was deleted later — most likely by a test cleanup that violated the [[test-db-isolation]] contract (tests pointing at `ebull` instead of `ebull_test`).

### Immediate operator action (already taken)

Re-seeded the singleton inline against dev DB:

```sql
INSERT INTO runtime_config (id, enable_auto_trading, enable_live_trading, updated_by, reason, display_currency)
VALUES (TRUE, FALSE, FALSE, 'recovery', 'singleton vanished — re-seeded 2026-05-18 after #1209 merge', 'GBP')
ON CONFLICT (id) DO NOTHING;
```

`/config` returns 200 again as of 2026-05-18T17:10 UTC.

### Permanent fix (this sub-ticket)

1. **Boot-time guard** at `app/main.py` lifespan startup: after migrations run, assert `SELECT count(*) FROM runtime_config = 1`. If zero, log a WARNING + re-seed with the same safe defaults (`enable_auto_trading=FALSE, enable_live_trading=FALSE, updated_by='boot_recovery', reason='singleton vanished — re-seeded by boot guard', display_currency='GBP'`). Same shape as `kill_switch` boot recovery (sql/010 + `ops_monitor.get_kill_switch_status` fail-closed pattern). Fail-closed posture preserved (defaults are off).
2. **Test-DB isolation invariant test** — `tests/test_dev_db_no_test_writes.py` (NEW): records `pg_database_size('ebull')` at session start, asserts no growth at session end. Catches a test that accidentally points at dev DB. Failing this test = a tech-debt issue + a prevention-log entry per `feedback_test_db_isolation`.
3. **Prevention-log entry** — `docs/review-prevention-log.md`: "Runtime singleton vanished after test contamination" — Symptom / Root cause / Detection / Prevention.

### Tests

| File | Asserts |
|---|---|
| `tests/test_runtime_config_boot_guard.py` | Drop the singleton row in `ebull_test_conn`; call the boot-guard helper; assert row exists + values are the safe defaults; assert WARNING log fired. |
| `tests/test_dev_db_no_test_writes.py` | Records dev-DB size at fixture setup + teardown; assert delta < 1 MB. Skipped on CI (CI uses a separate DB). |

## 3. Task DAG

```
P1 (= Sub 1 + Sub 6) — Postgres tuning + runtime_config boot guard
   → quick win; ~350 LOC; one PR.
P2 (= Sub 2)         — Test-fixture orphan sweep + slim-data audit
   → medium; ~300 LOC; one PR.
P3 (= Sub 3)         — financial_facts_raw partition + retention
   → biggest; ~400 LOC; one PR (schema migration + retention sweep).
P4 (= Sub 4)         — /system/postgres-health + pre-push hook warning
   → medium; ~200 LOC; one PR.
P5 (= Sub 5)         — Prevention-log entry
   → small; ~50 LOC; folded into the LAST landing PR (P4) to keep prevention writing close to the lessons.
```

Dependency order: P1 unlocks everything (tuning + boot resilience first). P2 + P3 are independent — could land in parallel sessions; default sequential P2 → P3 to keep blast radius small. P4 + P5 are last (depend on the prior changes being live so the health endpoint has interesting numbers to report).

## 4. Per-phase brief (each = one session)

Each phase follows the **same shape as the ETL plan**: spike → spec → Codex 1a → plan → Codex 1b → implementation → tests → local gates → Codex 2 → push → bot review → merge. **Autonomous-execution contract: no operator signoff between Codex iterations; drive each PR to merge in one session.**

### Phase 1 — Postgres tuning + runtime_config boot guard (Subs 1 + 6) — **SHIPPED 2026-05-18 (PR #1210 merge SHA `471a3b3`)**

Architectural sibling: G14 `bootstrap_orchestrator` source-registry (boot-time resilience pattern, PR #1191) + kill_switch fail-closed (sql/010).

- `sql/155_postgres_runtime_tuning.sql` — `ALTER SYSTEM SET` knobs per #1208 Sub 1. Applied via the new `-- runner: autocommit` migration-runner directive at `app/db/migrations.py` (multi-statement ClientCursor under autocommit still implicitly transacts — split + per-statement execute via `_split_autocommit_statements`). Test-template builder mirrored in `tests/fixtures/ebull_test_db.py`.
- `docker-compose.yml` — `mem_limit: 4g` + `shm_size: 1g`.
- `app/services/runtime_config.py::ensure_runtime_config_singleton` — hard-enforces autocommit-conn contract; race-safe via `INSERT ON CONFLICT DO NOTHING RETURNING id`; 3 `runtime_config_audit` rows on re-seed in one `conn.transaction()`; fails loud on non-canonical row.
- `app/main.py` lifespan + `app/jobs/__main__.py::_ensure_runtime_config_singleton_with_cleanup` — boot wiring with fence+pool cleanup-on-raise pattern. API-first migration contract (jobs has not called `run_migrations` since #719).
- `tests/test_runtime_config_boot_guard.py` (5 cases incl. atomic rollback + non-autocommit rejection) + `tests/test_migration_runner_autocommit.py` (directive parser + splitter) + `tests/conftest.py::_dev_db_size_tripwire` (session-autouse — moved from inert standalone module per Codex 2).
- `docs/review-prevention-log.md` — extended singleton-row entry + added Postgres-on-Docker + ALTER-SYSTEM-autocommit-directive sections.
- `.claude/skills/engineering/test-quality.md` — new "Dev-DB isolation invariant" section.

Codex iterations recorded inline in `docs/superpowers/specs/2026-05-18-phase1-tuning-boot-guard.md` §3.4 + §3.5.

Operator runbook (post-merge):
1. `docker compose up -d` to pick up `mem_limit` + `shm_size`.
2. Restart `python -m app.main` → migration 155 applies; `pg_reload_conf()` activates everything except `shared_buffers`.
3. `docker restart ebull-postgres` to pick up `shared_buffers=2GB`.
4. Confirm `SHOW shared_buffers; SHOW max_wal_size; SHOW wal_compression;` reports the tuned values.

### Phase 2 — Test-fixture orphan sweep + slim-data posture (Sub 2)

- `tests/fixtures/ebull_test_db.py::_drop_orphan_workers_older_than(min_age='1h')` (NEW).
- Audit: build fresh template DB; measure size; identify non-zero tables; flag any migrations that populate user data (defect — migrations should be schema only).
- Codify slim-test-data rule in `.claude/skills/engineering/test-quality.md` (NEW section or skill update — owns the rule).

### Phase 3 — `financial_facts_raw` partition + retention (Sub 3)

- Schema migration: `PARTITION BY RANGE (period_end)` quarterly buckets. Backfill via online detach/attach OR a fresh-table swap. Decision deferred to spec.
- Retention sweep: enforce per-table horizons documented at `.claude/skills/data-engineer/SKILL.md` §13 (10-K = last 3 annual, 10-Q = last 8 quarterly).
- Closure framing: SCHEMA PRIMITIVE (no operator-visible UI change; the partition is the deliverable).

### Phase 4 — `/system/postgres-health` + pre-push hook bloat warn (Sub 4)

- New endpoint `GET /system/postgres-health` returning: `pg_database_size('ebull')`, leaked-DB count, current WAL size, last checkpoint, autovacuum lag per top-10 tables.
- Pre-push hook addition at `.githooks/pre-push`: warn (don't block) if `pg_database_size('ebull') > 10 GB`.
- FE: small admin-page tile if scope budget allows; otherwise endpoint-only with operator runbook.

### Phase 5 — Prevention-log + skill updates (Sub 5)

Folded into the P4 PR. Lessons accumulated across P1-P4 land in `docs/review-prevention-log.md` as a single coherent section, not scattered. Skill updates flow inline as gaps are spotted (per the "own skill updates" rule — see §6 below).

## 5. Skill ownership posture (per user instruction 2026-05-18)

Skill files at `.claude/skills/**` are **owned by the agent** for this maintenance track:

1. When a gap is observed mid-task (e.g. an empirical finding contradicts the skill, a new pattern emerges, or a recurring trap surfaces), update the skill **inline**, in the same session, in the same PR. **Do not** ask for approval to read or edit skill files — they are part of the engineering substrate.
2. When a skill is found to be **stale** (claims something the codebase no longer does), correct it as a routine maintenance edit — same shape as a doc-comment update.
3. When a **new** prevention-log lesson surfaces mid-task, extract it into the relevant skill + the prevention-log in the SAME PR — never defer the "I'll write that up later" trap.

This document is itself a skill output — `.claude/skills/data-sources/finra.md` was updated mid-#916 to absorb the 403 lesson without ceremony.

## 6. Handover prompt for the next session (Phase 1)

Paste the block below verbatim into the next session — self-contained, no prior conversation context required.

---

```
Pick up Phase 1 of docs/superpowers/plans/2026-05-18-backend-stability.md
(Backend stability + dev DB hygiene, autonomous-execution contract per
ETL plan §1 — no operator signoff between Codex iterations, drive PR to
merge in one session).

PHASE 1 SCOPE — Postgres tuning + runtime_config boot guard (#1208 Subs 1 + 6):

Sub 1 — Postgres tuning (TUNING PRIMITIVE):
- New `sql/NNN_postgres_runtime_tuning.sql`:
    ALTER SYSTEM SET max_wal_size = '4GB';
    ALTER SYSTEM SET min_wal_size = '512MB';
    ALTER SYSTEM SET wal_compression = 'on';
    ALTER SYSTEM SET checkpoint_completion_target = '0.9';
    ALTER SYSTEM SET shared_buffers = '2GB';            -- restart req
    ALTER SYSTEM SET maintenance_work_mem = '512MB';
    ALTER SYSTEM SET effective_cache_size = '4GB';
    ALTER SYSTEM SET work_mem = '32MB';
    SELECT pg_reload_conf();
- `docker-compose.yml`: `mem_limit: 4g` + `shm_size: 1g`.

Sub 6 — runtime_config boot guard (RESILIENCE PRIMITIVE):
- `app/services/runtime_config.py::ensure_runtime_config_singleton(conn)`:
  After migrations run, check `SELECT count(*) FROM runtime_config = 1`.
  If 0, INSERT safe defaults (enable_auto_trading=FALSE,
  enable_live_trading=FALSE, updated_by='boot_recovery', reason=
  'singleton vanished — re-seeded by boot guard', display_currency='GBP')
  + log WARNING. Same fail-closed posture as kill_switch (sql/010).
- Wire `ensure_runtime_config_singleton(conn)` into `app/main.py`
  lifespan AFTER `run_migrations()` and BEFORE pool open.
- New invariant test `tests/test_dev_db_no_test_writes.py`: records
  pg_database_size('ebull') at session-scoped fixture setup + teardown;
  assert delta < 1 MB. SKIPPED on CI (different DB).

FIRST ACTIONS:

1. Read CLAUDE.md working order. Confirm #1208 still OPEN; #1209 merged.
2. Read docs/settled-decisions.md for kill_switch fail-closed pattern.
3. Read docs/review-prevention-log.md for any test-DB-isolation entries.
4. Read sql/010_kill_switch.sql + sql/015_runtime_config.sql to clone
   the singleton + fail-closed shape verbatim.
5. Read app/main.py lifespan startup ordering.

DESIGN STEPS (follow CLAUDE.md working order verbatim):

1. Branch: feature/1208-phase1-postgres-tuning-runtime-config-boot-guard.
2. Spike: verify dev-DB current Postgres knob values + assert
   pg_database_size shrinks after the proposed tuning takes effect on a
   sample workload (POST /jobs/finra_regsho_daily_refresh/run with
   backfill_window_days=14 → ~84 fetches; observe WAL gen + autovacuum
   behaviour pre/post tuning).
3. Spec at docs/superpowers/specs/2026-05-18-phase1-tuning-boot-guard.md
   mirroring the ETL spec shape.
4. Codex 1a on spec + Codex 1b on plan + Codex 2 pre-push. Non-negotiable
   per CLAUDE.md.
5. Implementation order:
   - T1: sql/NNN migration.
   - T2: docker-compose mem_limit + shm_size.
   - T3: ensure_runtime_config_singleton helper.
   - T4: app/main.py lifespan wiring.
   - T5: tests/test_runtime_config_boot_guard.py.
   - T6: tests/test_dev_db_no_test_writes.py.
   - T7: prevention-log entry (Postgres on Docker + singleton vanished).
   - T8: skill updates — `.claude/skills/engineering/test-quality.md`
        if it exists, otherwise `.claude/skills/data-engineer/SKILL.md`
        §"Dev-DB isolation invariant".

ETL DoD CLAUSES that apply (#8-#12):

- #8 Smoke: app boot succeeds with re-seeded singleton on fresh dev DB
  (delete the row, restart `python -m app.main`, observe WARNING +
  /config returns 200).
- #9 Cross-source: N/A (not a data-source change).
- #10 Backfill: N/A.
- #11 Operator-visible: GET /config returns 200 after the boot guard
  re-seeds; tail the app log for the WARNING line.
- #12 PR records verification + SHA.

NON-NEGOTIABLES (carried from ETL plan):

- Autonomous-execution contract per ETL plan §1 — no operator signoff
  between Codex iterations; merge to master in one session.
- Service-no-commit invariant + psycopg3 savepoint discipline still
  apply (no DB-touching service should enter its own transaction).
- Skill ownership posture per Phase 1 plan §6 — update skills inline
  on any observed gap; do NOT defer.
- Per feedback_post_push_cycle.md: poll gh pr view + gh pr checks
  IMMEDIATELY after push.
- Per feedback_pre_push_xdist_postgres_locks.md: if pre-push pytest
  wedges on Postgres locks, `--no-verify` justified when impacted-files
  clean + Codex green + targeted pytest + smoke pass.
- Per feedback_pr_auto_close_required.md: PR body MUST contain
  `Refs #1208` on its own line (sub-ticket, not closing parent).

REFERENCES:

- Parent maintenance plan: docs/superpowers/plans/2026-05-18-backend-stability.md.
- Issue: #1208 (Postgres tuning + dev-DB hygiene umbrella).
- Sibling resilience pattern: sql/010_kill_switch.sql + sql/015_runtime_config.sql.
- ETL plan template: docs/superpowers/plans/2026-05-17-us-etl-completion.md.
- Live evidence: 2026-05-18 user-reported login slowness + /config 503 →
  re-seeded singleton inline; Postgres PANIC log evidence captured in
  #1208 issue body.

If Phase 1 lands clean, the next session picks up Phase 2 (test-fixture
orphan sweep + slim-data audit). The same handover prompt template at
docs/superpowers/plans/2026-05-18-backend-stability.md §6 is re-used
with the Phase 2 brief substituted.
```

---

## 7. Out of scope for the whole #1208 epic (yet)

- Production HA / replication tuning. eBull demo-first; production posture is a separate epic.
- Postgres-on-K8s / managed-Postgres migration.
- WAL archiving / point-in-time-recovery setup.
- Frontend admin observability tiles beyond the simplest /system/postgres-health embed (deferred to a UI-revisit epic).

## 8. Acceptance for the whole epic

When Phases 1-5 land:

1. Postgres survives `POST /jobs/finra_regsho_daily_refresh/run` with `backfill_window_days=90` (worst-case current ingest burst: 6 prefixes × ~63 trading days = 378 fetches + ~7 M observation rows) WITHOUT WAL PANIC or container restart.
2. Fresh pytest run leaves **zero** `ebull_test_*` DBs after teardown (orphan sweep validates).
3. `pg_database_size('ebull')` < 5 GB after the `financial_facts_raw` retention sweep.
4. `/system/postgres-health` returns the documented metrics + pre-push hook warns on bloat.
5. Prevention-log section + skill updates merged in the same PRs they came from.
6. `/config` returns 200 even after the singleton row is manually deleted (boot guard re-seeds).
