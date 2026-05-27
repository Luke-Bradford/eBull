# #1273 PR1 — cohort-shape audit + helper interfaces

**Status**: draft 1.1 · 2026-05-27 · PR1 deliverable per [`phase-0-instrumentation.md`](./phase-0-instrumentation.md) §2.2.

**Changelog**:
- v1.0 — initial draft
- v1.1 — Codex 1 fold: 2 BLOCKING (test plan monkeypatch + rollback test shape), 4 IMPORTANT/NIT (S17 cite, S18 fingerprint, S22 deadline, S25 audit cite), 1 process (reset → PR2 acceptance item, not optional follow-up)
- v1.2 — Codex 2 pre-push P2 fold: BOTH helpers also bump `bootstrap_stages.last_progress_at = now()`. Without this the long-pole stage's `processed_count` would advance while `bootstrap_adapter.MAX(last_progress_at)` lagged `started_at`, tripping `processes.stale_detection.mid_flight_stuck` falsely. Mirrors the sibling `job_telemetry.record_processed` at `app/services/job_telemetry.py:194-205`. Test #11 (NEW) asserts `last_progress_at IS NOT NULL` after a helper write.

**Parent**: [`phase-0-instrumentation.md`](./phase-0-instrumentation.md) · [`bootstrap-sub-1h-plan.md`](./bootstrap-sub-1h-plan.md) (master plan v5.2)

**Scope**: PR1 lands the three progress-write helpers + this audit memo. **No instrumentation call sites are touched in PR1** — those land in PR2 along with the `target_cohort_fingerprint` column migration and frontend wiring.

**Why a standalone memo**: PR2 is the surgical bit (7 stages × 2 helper calls + cohort-fingerprint compute). PR2 author needs a frozen reference of "where the cohort gets built + what knobs pin it" without having to re-grep 7 stages. This memo is that reference.

---

## 1. PR1 deliverables

| Deliverable | Location | Surface change |
|---|---|---|
| `set_stage_target(*, run_id, stage_key, target_count) -> int` helper | `app/services/bootstrap_state.py` | NEW public function; fresh-connection write of `bootstrap_stages.target_count`; returns rowcount |
| `set_stage_processed(*, run_id, stage_key, processed_count) -> int` helper | `app/services/bootstrap_state.py` | NEW public function; fresh-connection write of `bootstrap_stages.processed_count` (ABSOLUTE value, not delta); returns rowcount |
| `_current_running_stage_key(job_name) -> str \| None` helper | `app/services/bootstrap_state.py` | NEW module-private function; resolves the stage_key of the single running stage with the given job_name on the in-flight run |
| Cohort-shape audit (this doc) | `docs/proposals/etl/1273-pr1-cohort-shapes.md` | NEW |
| Tests | `tests/services/test_bootstrap_state_progress.py` | NEW — direct helper exercise against `ebull_test` |

**Not in PR1**: cohort-fingerprint column migration, frontend wiring, instrumentation call sites, skill updates. All deferred to PR2.

---

## 2. Helper-interface contract

### 2.1 `set_stage_target`

```python
def set_stage_target(*, run_id: int, stage_key: str, target_count: int) -> int:
    """Write bootstrap_stages.target_count for an in-flight stage.

    Opens its own psycopg connection, commits, and closes. Survives
    caller rollback — progress writes must persist even if the
    ingester's transaction blows up mid-stage (per spec §2.2 #1
    Codex iter-1 IMPORTANT-1 fold).

    Predicate: status='running'. A late call after the stage has
    transitioned to success/error/cancelled is a benign no-op.

    Returns the row-update count (1 on a successful write, 0 on
    no-op). Callers can ignore the return value; the test suite
    asserts on it.
    """
```

SQL (single statement):

```sql
UPDATE bootstrap_stages
   SET target_count     = %(target_count)s,
       last_progress_at = now()
 WHERE bootstrap_run_id = %(run_id)s
   AND stage_key        = %(stage_key)s
   AND status           = 'running'
```

`last_progress_at` is the heartbeat column (sql/140) read by `bootstrap_adapter.MAX(last_progress_at)` + `stale_detection.mid_flight_stuck`. Codex 2 P2 fold.

### 2.2 `set_stage_processed`

```python
def set_stage_processed(*, run_id: int, stage_key: str, processed_count: int) -> int:
    """Write bootstrap_stages.processed_count for an in-flight stage.

    Fresh-connection write, mirrors set_stage_target. processed_count
    is an ABSOLUTE value (not a delta) — the caller passes the running
    total. Spec §2.2 #1 Codex iter-1 NIT-1 fold: this is intentionally
    NOT a `bump_stage_processed` increment helper, because the caller
    already tracks its own counter and an absolute write is one fewer
    round-trip + immune to lost-update races.
    """
```

SQL:

```sql
UPDATE bootstrap_stages
   SET processed_count  = %(processed_count)s,
       last_progress_at = now()
 WHERE bootstrap_run_id = %(run_id)s
   AND stage_key        = %(stage_key)s
   AND status           = 'running'
```

Mirrors `job_telemetry.record_processed` at `app/services/job_telemetry.py:194-205` — every in-flight progress signal bumps the heartbeat.

### 2.3 `_current_running_stage_key`

```python
def _current_running_stage_key(job_name: str) -> str | None:
    """Resolve the stage_key of the single running stage for job_name.

    Source of truth: bootstrap_runs.status='running' (mirrors
    sec_bulk_orchestrator_jobs._current_running_bootstrap_run_id at
    :90). Joined to bootstrap_stages with status='running' AND
    job_name=%s. Returns the stage_key or None.

    Handles S25's stage_key/job_name divergence: stage_key=
    'fundamentals_sync', job_name='fundamentals_sync_bootstrap'
    (per _BOOTSTRAP_STAGE_SPECS in bootstrap_orchestrator.py).
    """
```

SQL:

```sql
SELECT s.stage_key
  FROM bootstrap_runs r
  JOIN bootstrap_stages s ON s.bootstrap_run_id = r.id
 WHERE r.status     = 'running'
   AND s.job_name   = %(job_name)s
   AND s.status     = 'running'
 ORDER BY r.id DESC
 LIMIT 1
```

`ORDER BY r.id DESC LIMIT 1` defends against the (impossible-by-partial-unique-index but cheap-to-guard) case of multiple `bootstrap_runs.status='running'`.

### 2.4 Why fresh connection (no `conn=` param)

The orchestrator's per-CIK ingest tx may rollback on a single bad row. If progress writes shared the ingest connection, they'd rollback alongside — operator-visible bar would not advance even though 9/10 CIKs succeeded.

Spec §2.2 #1 mandates: "Opens own psycopg connection, commits, closes. Survives caller rollback." Three helpers all follow this pattern.

**Connection source**: helpers `from app.config import settings` then `psycopg.connect(settings.database_url)`. Mirrors the sibling `_current_running_bootstrap_run_id` at `sec_bulk_orchestrator_jobs.py:90-110`. PR1 must NOT accept a `database_url` override kwarg — tests monkeypatch `settings.database_url` instead (see §5).

Round-trip cost: one connection-open per write. At cadence `max(1, len(cohort)//100)` OR 30s (whichever first), an 8.7k-CIK S22 emits ~87 + ~6-min-deadline progress writes = ~100 connection-opens per stage. Trivial overhead vs the multi-minute stage runtime.

### 2.5 No new schema migration

Verified via grep: `bootstrap_stages.target_count` + `bootstrap_stages.processed_count` ship in `sql/140_per_run_progress_telemetry.sql` (existing on main). No PR1 migration needed.

`target_cohort_fingerprint TEXT` is a PR2 deliverable (spec §2.2 #5 DB layer).

---

## 3. Per-stage cohort-shape audit

Conventions: every file:line cited verified at write time. "Cohort source" = the call site that materialises the iteration set. "Count shape" classifies what `target_count` represents. "Fingerprint inputs" = the deterministic knobs PR2's fingerprint compute must concatenate (semicolon-separated `key=value` per spec §2.2 #5).

| Stage | stage_key | job_name | Cohort source | Count shape | Fingerprint inputs |
|---|---|---|---|---|---|
| S14 | `sec_submissions_files_walk` | `sec_submissions_files_walk` | [`sec_submissions_files_walk.py:132-158`](../../app/services/sec_submissions_files_walk.py#L132-L158) — `_list_cik_secondary_pages()` LEFT JOIN `sec_cik_submissions_files_index` (sidecar) to `external_identifiers` filtered `is_tradable=TRUE` | **list** `len(targets)` at `:185` | `is_tradable_only=TRUE`, sidecar-state buckets (sentinel `__no_overflow_pages__` vs real-pages vs empty) |
| S15 | `filings_history_seed` | `filings_history_seed` | [`scheduler.py:4657-4681`](../../app/workers/scheduler.py#L4657-L4681) — CIK-mapped tradable instruments WHERE `is_primary=TRUE` AND `is_tradable=TRUE`, optionally scoped to `instrument_id` | **list** `len(cik_rows)`, batched per instrument | `days_back` (default 730), `filing_types` (default `SEC_INGEST_KEEP_FORMS`), `instrument_id` (None = full universe) |
| S16 | `sec_first_install_drain` | `sec_first_install_drain` | [`sec_first_install_drain.py:221-265`](../../app/jobs/sec_first_install_drain.py#L221-L265) — `_iter_in_universe_subjects()` streams from `instrument_sec_profile` → `institutional_filers` → `blockholder_filers` 3-table union | **streaming** running counter at `:291-295` — no upfront materialisation | `max_subjects` (None = unbounded), `follow_pagination` (bool), fast-path gate `filing_events` pre-seeded by S8/S9 (bool) |
| S17 | `sec_def14a_bootstrap` | `sec_def14a_bootstrap` | bootstrap entry [`def14a_ingest.py:1160-1219 bootstrap_def14a`](../../app/services/def14a_ingest.py#L1160-L1219); discovery [`def14a_ingest.py:301-348`](../../app/services/def14a_ingest.py#L301-L348) — full-universe `discover_pending_def14a()` branch: `filing_events` LEFT JOIN `def14a_ingest_log` WHERE `filing_type = ANY(_DEF14A_FORM_TYPES)` AND `primary_document_url IS NOT NULL` AND `log.accession_number IS NULL` (untombstoned), CTE `ranked` filters `rank_within_form <= DEF14A_LATEST_PER_FILER_CAP` for DEF 14A | **bounded-paged with deadline** — `chunk_limit=500` per page; outer wall-clock `max_runtime_seconds=3600` | `chunk_limit=500`, `max_runtime_seconds=3600`, `_DEF14A_FORM_TYPES` (frozen), `DEF14A_LATEST_PER_FILER_CAP=2` ([`:107`](../../app/services/def14a_ingest.py#L107)), `primary_document_url IS NOT NULL` filter, per-CIK/form rank cap predicate (`r.filing_type <> 'DEF 14A' OR r.cik IS NULL OR r.rank_within_form <= cap`) |
| S18 | `sec_business_summary_bootstrap` | `sec_business_summary_bootstrap` | bootstrap entry [`business_summary.py:1446-1535 bootstrap_business_summaries`](../../app/services/business_summary.py#L1446-L1535); discovery [`business_summary.py:1606-1647`](../../app/services/business_summary.py#L1606-L1647) — `ingest_business_summaries()` CTE `latest_per_instrument` DISTINCT ON (`instrument_id`) FROM `filing_events` WHERE `filing_type IN ('10-K', '10-K/A')` AND `primary_document_url IS NOT NULL`, LEFT JOIN `instrument_business_summary bs` ON `instrument_id` with disjunctive pending predicate | **bounded-paged with deadline** — `chunk_limit=500` per page; outer wall-clock `max_runtime_seconds=3600` | `chunk_limit=500`, `max_runtime_seconds=3600`, `filing_type IN ('10-K', '10-K/A')` (frozen), `primary_document_url IS NOT NULL` filter, DISTINCT ON (`instrument_id`) ORDER BY `filing_date DESC, filing_event_id DESC` (latest filing wins), pending predicate disjunction: `bs IS NULL` OR `bs.source_accession <> lpi.provider_filing_id` (newer 10-K supersedes) OR `bs.next_retry_at <= NOW()` (#533 quarantine elapsed) OR `tables_json IS NULL` backfill (#560, AND-guarded by quarantine clock) |
| S22 | `sec_13f_recent_sweep` | `sec_13f_quarterly_sweep` | [`institutional_holdings.py:481-524`](../../app/services/institutional_holdings.py#L481-L524) — `list_directory_filer_ciks(min_last_13f_hr_at=...)` from `institutional_filers` WHERE `last_13f_hr_at IS NOT NULL AND last_13f_hr_at >= cutoff` | **list** `len(ciks)`, batched per-filer at `:1069-1178` with deadline loop | `min_period_of_report` (runtime: `date.today() - 380d` via `_PARAM_DYNAMIC_BOOTSTRAP_13F_CUTOFF`), `min_last_13f_hr_at` (runtime: `datetime.now(tz=UTC).date() - 380d` UTC-midnight via `_PARAM_DYNAMIC_BOOTSTRAP_13F_HR_CUTOFF`), `source_label='sec_edgar_13f_directory_bootstrap'`, `deadline_seconds` resolved from [`settings.sec_13f_sweep_deadline_seconds`](../../app/config.py#L37) (default `21600.0` = 6h; fingerprint MUST pin the resolved float value so config drift is visible) |
| S25 | `fundamentals_sync` | `fundamentals_sync_bootstrap` | [`fundamentals/__init__.py:1639-1651`](../../app/services/fundamentals/__init__.py#L1639-L1651) — `normalize_financial_periods()` streams `SELECT DISTINCT instrument_id FROM financial_facts_raw` (all instruments with facts) OR explicit `instrument_ids` param; pre-stage audit at [`coverage.py:1018 audit_all_instruments`](../../app/services/coverage.py#L1018) covers all tradable instruments | **streaming** per-instrument cursor loop `:1656-1708` with running counter | `instrument_ids=None` (full universe with facts) vs explicit list, pre-stage `audit_all_instruments()` scope = all tradable instruments at [`coverage.py:1018`](../../app/services/coverage.py#L1018), dataset filter "has `financial_facts_raw` rows" |

### 3.1 Notes per stage

- **S14**. Cohort = CIKs whose submissions secondary-pages have not yet been walked (sidecar absence) plus CIKs with stale sidecar. Fingerprint focuses on the `is_tradable` filter + sidecar-state buckets so the operator can audit "we walked the right slice".
- **S15**. `days_back` is the dominant fingerprint knob: at bootstrap it's hard-coded 730d; a future tuning PR may need a wider window. Pinning `filing_types` in fingerprint catches `SEC_INGEST_KEEP_FORMS` drift.
- **S16**. Streaming-only. **No `set_stage_target` call** — the cohort size is not known up-front (3-table union is consumed lazily). PR2 implementation: only `set_stage_processed` with the running CIK-processed counter; fingerprint pins `max_subjects` + `follow_pagination` + fast-path gate state.
- **S17**. Bounded-paged: each page materialises 500 candidates; the outer loop drains until `deadline_seconds`. The actual pending universe is gated by the per-CIK/form rank cap (`DEF14A_LATEST_PER_FILER_CAP=2`) so the total pending count is finite-and-pre-knowable via a one-off `COUNT(*)` over the same CTE. PR2 design decision pending in §4: should `target_count` reflect the INITIAL pending-discovery count (set once before the deadline loop) or the per-page count (overwritten each page)?
- **S18**. Same shape as S17 — `latest_per_instrument` DISTINCT ON + LEFT JOIN `instrument_business_summary` disjunctive pending predicate. Fingerprint MUST pin all four pending-predicate branches (`bs IS NULL` / `source_accession <>` / `next_retry_at <= NOW()` / `tables_json IS NULL` backfill) so operator can audit which mix of new vs retry vs backfill filings the page is draining.
- **S22**. List shape, runtime-resolved cutoffs. Fingerprint MUST include both `min_period_of_report` (resolved per UTC midnight) AND `min_last_13f_hr_at` AND the resolved `settings.sec_13f_sweep_deadline_seconds` float so reviewers can audit cohort overlap with prior runs + deadline-config drift.
- **S25**. Streaming over `SELECT DISTINCT instrument_id FROM financial_facts_raw`. PR2 may choose to pre-materialise the count via a separate `SELECT COUNT(DISTINCT instrument_id)` round-trip so the bar shows progress against a known target — that's a PR2 trade-off (extra round-trip vs operator-visible target).

---

## 4. PR2 open design questions (carried over)

These are NOT PR1 decisions — they are flagged here so PR2 author resolves them with a fresh design pass + Codex 1 review.

1. **S17/S18 `target_count` semantics**. Initial-pending-count (pinned once) vs per-page (overwritten). Trade-off: initial-pending is more accurate when discovery is single-pass; per-page is more accurate when retry/dedupe shifts pending mid-run. Recommend initial-pending (one upfront `SELECT COUNT(*) ... FROM filing_events WHERE <pending predicate>` before the deadline loop) — operator-visible "X of Y, deadline-stopped at Y/2" is more legible than "X of last-page-N".
2. **S25 pre-materialised count**. Add `SELECT COUNT(DISTINCT instrument_id) FROM financial_facts_raw` to set `target_count`? Round-trip cost minimal (~ms); operator-visible benefit large. Recommend yes.
3. **Fingerprint hash vs format**. Spec §2.2 #5 specifies `<key>=<value>;<key>=<value>;…` semicolon-separated. Sufficient legibility for `title=` tooltip; no SHA-256 needed. PR2 must avoid SHA — operator must be able to eyeball it.
4. **Out-of-order target/processed writes**. If the cohort-materialisation block takes >30s, the first `set_stage_processed(0)` may land BEFORE the first `set_stage_target(N)`. UI must tolerate `processed_count > 0 AND target_count IS NULL` for a few seconds. (PR2 frontend code change.)

---

## 5. Test plan for PR1

`tests/services/test_bootstrap_state_progress.py` — direct helper exercise against `ebull_test` (NOT dev DB).

**Test-DB discipline** (Codex 1 BLOCKING fold): helpers open via `settings.database_url`. Tests MUST monkeypatch `settings.database_url` → `test_database_url()` from `tests/fixtures/ebull_test_db` at module/autouse scope, mirroring sibling files like `tests/test_jobs_queue_recovery.py:45` and `tests/test_bootstrap_orchestrator.py:78`. Without the patch, helper writes would land in the dev DB — violates `feedback_test_db_isolation` + tripping the test-DB-write guard.

```python
# Module-level autouse fixture (sketch — final implementation in PR)
@pytest.fixture(autouse=True)
def _pin_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("app.config.settings.database_url", test_database_url())
```

**Seed factory**: a `bootstrap_test_run` factory that COMMITS a `bootstrap_runs(triggered_by_operator_id=NULL, status='running')` + N `bootstrap_stages(bootstrap_run_id, stage_key, stage_order, lane, job_name, status='running')` rows via a fresh autocommit connection. Seed rows MUST be committed before the helper-under-test invocation — the helper opens its own connection and cannot see uncommitted caller state. No `_BOOTSTRAP_STAGE_SPECS` dependency; tests drive row contents directly.

**Tests**:

1. **set_stage_target happy path**: seed factory commits run + stage(status='running'); call `set_stage_target(run_id=R, stage_key=K, target_count=42)`; assert returned rowcount=1 AND `SELECT target_count FROM bootstrap_stages WHERE bootstrap_run_id=R AND stage_key=K` = 42.
2. **set_stage_target no-op on terminal stage**: seed stage status='success' (committed); call helper; assert rowcount=0 AND `target_count IS NULL`.
3. **set_stage_processed happy path**: seeded committed running stage + non-zero processed_count. Assert write.
4. **set_stage_processed absolute-write contract**: call helper twice with `processed_count=10` then `=5`. Assert second write lands (no monotonicity guard) — helper is "ABSOLUTE write"; caller responsibility to enforce monotonicity. Documents the contract.
5. **set_stage_processed no-op on terminal stage**: same as #2 for processed_count.
6. **_current_running_stage_key resolves S25 divergence**: seed committed run with stage stage_key='fundamentals_sync', job_name='fundamentals_sync_bootstrap', status='running'; call `_current_running_stage_key('fundamentals_sync_bootstrap')` → returns `'fundamentals_sync'`.
7. **_current_running_stage_key returns None for unknown job_name**: seed run + stage as in #6; call with `'nonexistent_job'` → None.
8. **_current_running_stage_key returns None when no running run**: seed bootstrap_runs status='complete'; call helper → None.
9. **_current_running_stage_key returns None when stage not yet running**: seed run='running' + stage status='pending'; call helper → None.
10. **Survives caller rollback** (Codex 1 BLOCKING-2 rewrite): (a) seed factory COMMITS run + stage(status='running'); (b) open a caller psycopg connection in non-autocommit mode, `BEGIN`, INSERT a side row into `bootstrap_archive_results` with the same `bootstrap_run_id` (the table FKs `bootstrap_run_id REFERENCES bootstrap_runs(id)` per sql/130 — the FK is satisfied because step (a) committed the parent run); (c) WHILE the caller tx is still open, call `set_stage_target(run_id=R, stage_key=K, target_count=99)`; (d) ROLLBACK the caller tx; (e) assert via a third fresh connection that `target_count = 99` persisted AND the side `bootstrap_archive_results` row did NOT persist. Proves the helper's fresh-connection commit is independent of the caller transaction.

Runtime budget: <5s for all 10 tests.

---

## 6. Cross-impact

Verified at draft time:

- **`bootstrap_stages.target_count` / `processed_count`**: present per `sql/140_per_run_progress_telemetry.sql` ALTER TABLE; no migration in PR1.
- **`mark_stage_success`**: writes `rows_processed` only (not `processed_count`). PR1's `set_stage_processed` is a separate column write; no collision.
- **`reset_failed_stages_for_retry` (line 906-922)**: resets `rows_processed = NULL` on lane-restart. **Does NOT reset `processed_count` or `target_count`**. PR2 MUST extend the reset to clear both columns (Codex 1 fold: this is a **PR2 acceptance item, not an optional follow-up** — without it, the operator-visible bar would show stale values from the last-failed pass on a fresh retry, contradicting the operator-visible-progress goal that motivates the whole stage). PR1 makes no change here because PR1 ships no instrumentation writes; this audit memo entry is the binding handoff to PR2.
- **`bootstrap_orchestrator._BOOTSTRAP_STAGE_SPECS`**: PR1 does not modify. PR2's instrumentation call sites read stage_key/job_name from this catalogue.
- **`_current_running_bootstrap_run_id` at `sec_bulk_orchestrator_jobs.py:90`**: PR1's `_current_running_stage_key` mirrors its connection + source-of-truth pattern (`bootstrap_runs.status='running'`). No refactor needed.
- **Frontend types / endpoint projections**: untouched in PR1. PR2 extends `BootstrapTimelineStageResponse` + `frontend/src/api/types.ts` + `ProcessDetailPage.tsx` tooltip rendering.
- **Skills**: untouched in PR1. PR2 updates `.claude/skills/metrics-analyst/SKILL.md` per spec §2.2 #7.

---

## 7. Settled-decisions check

Reviewed `docs/settled-decisions.md`. None of the live decisions apply to PR1 (helpers don't touch identifiers, fundamentals provider, scoring, portfolio manager, execution guard, or bootstrap-gate carve-outs). Recorded for the audit trail.

## 8. Review-prevention-log check

Relevant entries:

- **"Single-row UPDATE silent no-op on missing row"** (PR #70). PR1's helpers DO no-op silently when `status != 'running'` — but that is by-design (late writes are benign no-ops, not bugs). Tests #2 and #5 assert the no-op explicitly with `rowcount=0`, documenting the contract.
- **"psycopg3 transaction inside open tx is SAVEPOINT not COMMIT"** ([[feedback_psycopg3_savepoint_commit]]). PR1's helpers open their own connection — `psycopg.connect(url) as conn` + `conn.commit()` after the UPDATE — so the SAVEPOINT trap does not apply.
- **"Grep both CREATE TABLE and ALTER TABLE constraints"** ([[feedback_grep_alter_constraints]]). Grep'd both: `target_count` / `processed_count` exist only in `sql/140` ALTER TABLE; no CHECK constraint on either column. No constraint surface to violate.
- **"UPDATE-by-PK helpers must assert rowcount"** (sql-correctness skill). PR1 helpers return rowcount; callers are tested code paths in PR2 (not user input). PR1 helpers themselves do not assert rowcount because a 0-rowcount return is the documented late-write no-op.

No prevention-log entry blocks PR1.
