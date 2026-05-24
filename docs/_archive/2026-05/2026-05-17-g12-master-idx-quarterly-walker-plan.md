# G12 — `master.idx` quarterly cross-quarter walker — implementation plan

> **Status:** DRAFT v1 2026-05-17.
> **Spec:** `docs/superpowers/specs/2026-05-17-g12-master-idx-quarterly-walker.md` (CLEAN v3).
> **Parent plan:** `docs/superpowers/plans/2026-05-17-us-etl-completion.md` Phase 3 PR 6.

## 0. DAG

```
T1  schema-check (no migration)                ─► informational

T2  sec_full_index.py provider                 ─► T6  test_sec_full_index_provider
                                                  ├─► T7  test_sec_master_idx_quarterly_sweep
T3  sec_master_idx_quarterly_sweep.py job      ──┘     (also depends on T2)

T4  scheduler.py wiring (JOB_ + ScheduledJob + invoker)  ─┐
T5  runtime.py invoker registration                       ─┼─► T8 test_sec_master_idx_scheduler_wiring
                                                          ─┘
                                                          ─► T9 test_universal_gate_carve_out (depends on T4)
                                                          ─► T10 test_layer_123_wiring (depends on T4)

T2 + T3 + T4 + T5 → T11 matrix + sec-edgar skill doc updates (file:line citations)

T6-T11 → T12 local gates (ruff / pyright / pytest) → T13 Codex 2 pre-push
```

T1 is informational — confirms "no migration needed". T2-T5 are the
new code surface; T4 is a strict predecessor of T9 + T10 because both
edit-tests reference the new `JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP`
constant. T11's file:line matrix updates need T2/T3/T4 in-place so the
cited file paths resolve. T12 + T13 are the pre-push gates.

## 1. Tasks

### T1 — Confirm no migration required

- Read `sql/120_data_freshness_index.sql` + `sql/118_sec_filing_manifest.sql`
  to confirm:
  - `sec_filing_manifest` already has the PK + columns G12 needs.
  - No new global / static / seed row required in
    `data_freshness_index` (per spec §3.2). The walker DOES write to
    `data_freshness_index` indirectly — `record_manifest_entry` calls
    `seed_freshness_for_manifest_row` on every UPSERT (`app/services/sec_manifest.py:285-300`) —
    but per-(subject, source) rows are populated naturally by the
    walker's matched-in-universe path; no seed migration is needed.
    T7 transaction tests assert that BOTH `sec_filing_manifest` and
    `data_freshness_index` writes are rolled back as one unit on a
    quarter failure.
- Output: 1-line note in the PR body confirming "schema unchanged".
- No file edits.

### T2 — `app/providers/implementations/sec_full_index.py` (NEW)

- Implement per spec §5.1. Code body:
  - `_quarter_start_date(year, quarter) -> date` with `ValueError` for
    quarter not in 1..4.
  - `_build_url(year, quarter) -> str` with same `ValueError`.
  - `read_master_idx(http_get, year, quarter, *, user_agent=..., allow_404=False) -> Iterator[FilingIndexRow]`:
    - status==404 + `allow_404=True` → log "not yet published" + empty.
    - status==404 + `allow_404=False` → raise `RuntimeError(...)` with
      a message that mentions `status=404` + the explanation.
    - status != 200 (and != 404 handled above) → raise `RuntimeError`.
    - status == 200 → yield from `parse_daily_index(body, default_filed_at=_quarter_start_date(year, quarter))`.
- **Reuses** `parse_daily_index` from sibling daily-index provider.
- Module docstring per spec §5.1 wording.

### T3 — `app/jobs/sec_master_idx_quarterly_sweep.py` (NEW)

- Implement per spec §5.2. Body:
  - `QuarterStats` frozen dataclass.
  - `MasterIdxSweepStats` frozen dataclass with `total_upserted` +
    `failed_quarters` properties.
  - `_current_calendar_quarter(now) -> (year, q)`.
  - `_previous_calendar_quarter(year, q) -> (year, q)`.
  - `_quarters_to_walk(now) -> [(y, q), (y, q)]`.
  - `build_preloaded_subject_resolver(conn) -> SubjectResolver` per
    spec §3.5.
  - `run_master_idx_quarterly_sweep(conn, *, http_get, now=None, subject_resolver=None, quarters=None) -> MasterIdxSweepStats`
    with per-quarter try/except + `conn.commit()`/`conn.rollback()`
    boundary + cumulative stats list.
- Module docstring captures: cross-quarter discovery use case +
  per-quarter txn isolation + 404 asymmetry + preloaded resolver +
  ">1-quarter outage = Python REPL runbook" line.

### T4 — `app/workers/scheduler.py` (EDIT)

- Add `JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP = "sec_master_idx_quarterly_sweep"`
  in the JOB_ constant block (insert after `JOB_SEC_PER_CIK_POLL`).
- Add `ScheduledJob(...)` entry per spec §5.3 to `SCHEDULED_JOBS`
  list, inserted after the `JOB_SEC_PER_CIK_POLL` entry. Verify the
  surrounding context is the Layer-3 block (line ~1062-1080 in current
  source).
- Add `def sec_master_idx_quarterly_sweep() -> None:` invoker body
  after `def sec_per_cik_poll()`. Imports `run_master_idx_quarterly_sweep`
  lazily inside the function to keep module-load cycle clean (sibling
  pattern: `sec_daily_index_reconcile` + `sec_per_cik_poll`).

### T5 — `app/jobs/runtime.py` (EDIT)

- Add `JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP` + `sec_master_idx_quarterly_sweep`
  to the `from app.workers.scheduler import (...)` block (alphabetical
  by JOB_ constant convention).
- Register `_INVOKERS[_scheduler.JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP] = _adapt_zero_arg(_scheduler.sec_master_idx_quarterly_sweep)`
  in the `_INVOKERS` dict (insert after `JOB_SEC_PER_CIK_POLL`
  registration).

### T6 — `tests/test_sec_full_index_provider.py` (NEW)

- 7 unit tests per spec §6.1. Driver: pure unit; injects a fake
  `http_get`. No DB fixture.
- Canned bodies share with the sibling `test_sec_daily_index_provider.py`
  fixture if available; otherwise inline minimal bodies.

### T7 — `tests/test_sec_master_idx_quarterly_sweep.py` (NEW)

- 16 tests per spec §6.2 (including the added blockholder-cohort
  test #16 from Codex 1b r1 HIGH-2). Uses the `ebull_test_db` fixture
  (per `tests/fixtures/ebull_test_db.py`) for the DB-touching cases.
- **Seed order matters** (Codex 1b r1 HIGH-1 + r2 HIGH): `instrument_sec_profile`
  is FK-bound to `instruments` via `instrument_id`
  (`sql/051_instrument_sec_profile.sql:19-20`). The fixture truncates
  both. Test-level seeding MUST insert into `instruments` BEFORE
  `instrument_sec_profile`. The `instruments` table PK is
  `instrument_id` (NOT `id`) and `company_name TEXT NOT NULL` —
  verified against `sql/001_init.sql:1-4`. Helper:

  ```python
  def _seed_issuer(conn, *, instrument_id: int, cik: str, symbol: str, company_name: str | None = None) -> None:
      """Seed an issuer row in instruments + instrument_sec_profile.

      ``company_name`` defaults to ``symbol`` for tests that don't care
      about the marketing string. The FK chain is
      instruments(instrument_id) PK ←  instrument_sec_profile(instrument_id).
      """
      with conn.cursor() as cur:
          cur.execute(
              "INSERT INTO instruments (instrument_id, symbol, company_name) "
              "VALUES (%s, %s, %s) "
              "ON CONFLICT (instrument_id) DO NOTHING",
              (instrument_id, symbol, company_name or symbol),
          )
          cur.execute(
              "INSERT INTO instrument_sec_profile (instrument_id, cik) "
              "VALUES (%s, %s) ON CONFLICT (instrument_id) DO NOTHING",
              (instrument_id, cik),
          )
  ```

  Verify FK column names + null constraints against the live schema
  before writing. Tests using the helper:
  - Test 4 (happy-path): `_seed_issuer(conn, instrument_id=7, cik="0000320193", symbol="AAPL")`.
  - Test 9 + 10 (txn isolation): seed 1 issuer so the matched-in-universe
    branch fires before the synthetic error.
  - Test 14 (priority): seed CIK both as issuer AND as institutional_filer.
- Pre-seeds an `institutional_filers` row for a distinct fake CIK to
  exercise the institutional-only path. **Also pre-seeds a
  `blockholder_filers` row** for a third fake CIK to exercise the
  blockholder-only path (Codex 1b r1 HIGH-2).
- **Added test T7 #16** — `test_preloaded_resolver_blockholder_priority_below_institutional`:
  seed a CIK as BOTH `institutional_filers` AND `blockholder_filers`;
  assert `build_preloaded_subject_resolver(conn)(_, cik).subject_type == "institutional_filer"`.
  Pins the issuer > institutional > blockholder chain that
  `default_subject_resolver` enforces and `setdefault` mirrors.
- Injects fake `http_get` that returns canned 200/404/503 + canned
  bodies per quarter URL.
- For tests 9 + 10 (txn isolation): builds a fake subject_resolver
  closure that raises `psycopg.errors.OperationalError` on the third
  CIK to simulate a mid-loop DB error without actually corrupting the
  tx state.
- **Test 9 + 10 cross-table rollback assertion** (Codex 1b r1 MED-1):
  after the synthetic mid-loop failure, assert BOTH
  `sec_filing_manifest` AND `data_freshness_index` have zero rows for
  the failed-quarter accessions (rollback discarded the
  `record_manifest_entry` + `seed_freshness_for_manifest_row` writes
  as one unit). Test 10's commit assertion mirrors the inverse —
  successful quarter has BOTH the manifest row AND the freshness row
  visible to a fresh connection.
- **Fixture-cleanup**: the `ebull_test_db` fixture truncates the
  `_PLANNER_TABLES` set between tests (per `tests/fixtures/ebull_test_db.py`),
  which already includes `sec_filing_manifest` + `data_freshness_index`
  + `instruments` + `instrument_sec_profile` + `institutional_filers`
  + `blockholder_filers`. No new entry needed. (Not "ALL tables" —
  the planner-table list specifically.)

### T8 — `tests/test_sec_master_idx_scheduler_wiring.py` (NEW)

- 5 tests per spec §6.3:
  - Job-name constant existence + value.
  - `SCHEDULED_JOBS` contains exactly one entry with that name.
  - Cadence + source + prereq + flag fields all correct.
  - `_INVOKERS` registers the zero-arg wrapper. **Assertion shape**:
    `_INVOKERS[JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP].__wrapped__ is sec_master_idx_quarterly_sweep`
    (Codex 1b r1 LOW-1). DO NOT compare against a fresh
    `_adapt_zero_arg(...)` instance — that returns a new function
    every call, so identity would always fail. The `__wrapped__`
    attribute is set by `_adapt_zero_arg` at registration time
    (`app/jobs/runtime.py:187-203`).
  - `source_for(JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP) == "sec_rate"`.

### T9 — `tests/test_universal_gate_carve_out.py` (EDIT)

- Locate the exempt-allow-list test (per `feedback_universal_gate_supersession.md`).
- Add positive assertion that `JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP` is
  NOT in the exempt set.

### T10 — `tests/test_layer_123_wiring.py` (EDIT)

- Add `test_layer4_master_idx_quarterly_sweep_registered` per spec §6.5.
- Cross-references the existing Layer-1/2/3 tests for the same shape.

### T11 — Matrix + skill doc updates

- `.claude/skills/data-engineer/etl-endpoint-coverage.md`:
  - §4 row "Full-index `master.idx` quarterly" → ✅ WIRED 2026-05-17 (G12)
    with file:line link to the new provider + new job.
  - §7 G12 row → `OPEN (low)` → `✅ CLOSED 2026-05-17 (G12 PR)`.
- `.claude/skills/data-sources/sec-edgar.md`:
  - §1 "Indexes + Atom feeds" full-index row gets a "Consumed by:
    `sec_master_idx_quarterly_sweep`" annotation parallel to the
    daily-index annotation.

### T12 — Local gates

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest -k "master_idx or full_index or layer_123 or universal_gate"
uv run pytest tests/smoke/test_app_boots.py
```

All five must pass. The smoke test is the load-bearing gate — the
new `JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP` will be discovered by
`get_job_name_to_source()` at FastAPI lifespan; any conflict or
KeyError surfaces here.

### T13 — Codex 2 pre-push

```bash
codex exec review
```

Address findings per the standard CLEAN loop. Pre-flight contract:
no follow-up tickets opened during this run; all fixes go in-scope.

## 2. Cross-cutting invariants

- **PR scope.** No FE work. No new migration. No new MANUAL_TRIGGER
  registry entries. Just provider + job + scheduler + runtime + tests
  + matrix doc.
- **No new dependencies.** Reuses existing `parse_daily_index` +
  `record_manifest_entry` + `SubjectResolver` types.
- **Time-monotonicity prevention-log:** no monotonic-timestamp assertion
  in any new test. The idempotency test (T7 #12) reads post-UPSERT
  `ingest_status` from the DB via SQL.
- **No ScheduleWakeup / no sleep loops** for monitoring — terminal-
  condition `gh pr view` / `gh pr checks` polling per autonomy contract.
- **Codex iteration count.** Target: 1a CLEAN by r2-r3 (already there);
  1b CLEAN by r1-r2; pre-push (Codex 2) CLEAN by r1-r2; PR review bot
  CLEAN with ≤2 follow-up pushes.

## 3. Risks

| Risk | Mitigation |
|---|---|
| `parse_daily_index` import cycle when `sec_full_index.py` re-exports it implicitly. | T2 imports `parse_daily_index` directly from the sibling provider; no re-export. Verified manually + by ruff. |
| `sec_filing_manifest` PK clash with rows already present from Layer 1/2/3. | Idempotent ON CONFLICT (sibling-tested behaviour); T7 #12 pins. |
| `psycopg.errors.OperationalError` simulation in T7 #9 is hard to inject cleanly. | Use a fake `subject_resolver` closure that just raises a real `psycopg.errors.OperationalError(...)` for the target CIK — no real connection state corruption needed; the walker's `except Exception:` handler treats both real + synthetic the same way. The handler's `conn.rollback()` is a real call against a real connection (test uses `ebull_test_db`) so the rollback path is exercised. |
| The eager universe preload in T3 issues 3 SELECTs synchronously at fire time on a cold dev DB; if any table is missing/locked the test setup raises before walker starts. | T3 raises inside `build_preloaded_subject_resolver` — caller (T7 tests) injects an explicit resolver to skip the preload for tests that don't care. The default-path test (T7 #14) seeds the rows first. |
| Param-validation drift if a future PR adds operator-tunable params. | T8 #5 pins `params_metadata == ()`. Any future PR adding params must update both the ScheduledJob entry AND the wiring test. |

## 4. Branch + commit shape

- Branch: `feat/g12-master-idx-quarterly-walker`.
- One commit: `feat(G12): master.idx quarterly cross-quarter walker`.
- PR title: `feat(G12): master.idx quarterly cross-quarter walker`.
- PR body sections per `.claude/skills/engineering/pr-authoring.md`:
  - What: provider + job + scheduler + runtime + matrix updates.
  - Why: closes §7 G12 gap; tombstoned-CIK + late-amendment safety net.
  - Test plan: T6-T10 + local gates + smoke; explicit "no schema /
    no migration" caveat.
  - Tradeoffs / decisions: cadence + persistence + watermark per
    spec §3 (one-line summary each); >1-quarter outage runbook
    explicitly called out.
  - Cohort smoke panel: AAPL/MSFT (issuer side, 8-K/10-K/10-Q/DEF14A/Form4);
    Berkshire/BlackRock (institutional filer side, 13F-HR) IF seeded;
    explicit "AAPL ≠ 13F-HR cohort" note.
  - Spec + plan paths.

## 5. Acceptance — plan layer

The plan is COMPLETE when:

1. All T1-T13 ticked off in the session log.
2. Codex 1a + 1b + 2 all CLEAN.
3. Local gates all green.
4. PR opened.
5. Subsequent merge cycle handled per the autonomy contract.

The PR is mergeable when spec §9 acceptance holds.
