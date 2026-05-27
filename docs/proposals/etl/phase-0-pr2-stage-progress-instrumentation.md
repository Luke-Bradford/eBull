# #1273 PR2 — stage-progress instrumentation + cohort-fingerprint wiring

**Status**: Proposal · 2026-05-27 · draft 1.2 (Codex 1 diff-only re-pass fold)
**Parent**: [`phase-0-instrumentation.md`](./phase-0-instrumentation.md) v1.5 §2.2 · [`bootstrap-sub-1h-plan.md`](./bootstrap-sub-1h-plan.md) v5.2
**PR1 audit memo (frozen handoff)**: [`1273-pr1-cohort-shapes.md`](./1273-pr1-cohort-shapes.md) v1.2 (merged in PR #1361 `792291e`)
**Branch**: `feature/1273-stage-progress-instrumentation`

**Changelog**:
- v1.0 — initial draft
- v1.1 — Codex 1 fold: 2 BLOCKING (S18 fingerprint under-specifies cohort; S17/S18 upfront COUNT is not ms-cost) + 5 IMPORTANT (S25 redundant pre-count; S16 fingerprint not computable at entry; S23 missing long-pole; FE fixtures require field; S25 file path wrong) + 1 NIT (S16 loop line cite). 7-stage cohort widened to 8 (added S23 sec_n_port_ingest). S17/S18 dropped from initial-pending-COUNT design to streaming-style (target_count=None, fingerprint-only) — upfront `COUNT(*)` over discovery CTE not defensible as ms-cost.
- v1.2 — Codex 1 diff-only re-pass fold: 3 GAPs + 1 NEW-IMPORTANT. B1 GAP — S17 fingerprint added `rank_scope=def14a_with_cik` + verbatim rank predicate; S18 fingerprint added `(tables_null AND quarantine_elapsed)` AND-guard per PR1 audit §3 S18 note. B2 GAP — §5.1 + §5.2 + §9.2 caveats now correctly list S17/S18 as streaming-style (not list-shaped); §5.2 heading widened from "S16 only" to "S16, S17, S18". I3 GAP — stale "7 stages" / "7 long-pole" references in §2 migration comment + §5.3 job-name note + §9.2 source-data caveat all flipped to 8. NEW-IMPORTANT — §9.2 metrics formula now carves out streaming stages (`processed_count` cumulative only, no denominator) from list-shaped (`processed/target`).

PR2 is the surgical bit of #1273. PR1 shipped the helpers + audit memo. PR2 wires the 8 long-pole stages to those helpers, adds the `target_cohort_fingerprint` column, extends `set_stage_target` to carry the fingerprint, extends `reset_failed_stages_for_retry` to clear progress + fingerprint columns, and adds the frontend tooltip.

---

## 1. Deliverables

| # | Deliverable | Location | Surface |
|---|---|---|---|
| 1 | sql/178 migration | `sql/178_bootstrap_stages_target_cohort_fingerprint.sql` | ALTER TABLE bootstrap_stages ADD COLUMN IF NOT EXISTS `target_cohort_fingerprint TEXT` |
| 2 | Extend `set_stage_target` | `app/services/bootstrap_state.py:779-803` | New kwarg `cohort_fingerprint: str \| None = None`; `target_count` becomes `int \| None`; COALESCE writes preserve existing values on None |
| 3 | Extend `reset_failed_stages_for_retry` | `app/services/bootstrap_state.py:1032-1049` | Reset 4 additional columns: `target_count`, `processed_count`, `last_progress_at`, `target_cohort_fingerprint` (PR1 audit §6 — MANDATORY PR2 acceptance) |
| 4 | Per-stage instrumentation calls | 8 files (table §5) | 5 list-shaped stages call `set_stage_target` + cadenced `set_stage_processed` (S14, S15, S22, S23, S25); 3 streaming-style stages call `set_stage_target(target_count=None, cohort_fingerprint=…)` + cadenced `set_stage_processed` (S16, S17, S18) |
| 5 | Backend response model + endpoint projection | `app/api/processes.py:276-301`, `:692-700` | Add `target_cohort_fingerprint: str \| None` to `BootstrapTimelineStageResponse`; project column in SELECT |
| 6 | Frontend type + tooltip | `frontend/src/api/types.ts:1417-1435`, `frontend/src/pages/ProcessDetailPage.tsx:1186-1221` | Add `target_cohort_fingerprint: string \| null` to TS type; render as `title=` on the progress-bar wrapper (additive tooltip, no new chrome) |
| 7 | Metrics-analyst skill update | `.claude/skills/metrics-analyst/SKILL.md` | Split bootstrap_stages master-index row into `rows_processed`, `target_count + processed_count`, `target_cohort_fingerprint`; new §8 sub-section "Bootstrap stage progress (live)" |
| 8 | Tests | `tests/services/test_bootstrap_state_progress.py` (extend); `tests/smoke/test_long_pole_progress.py` (NEW) | Fingerprint write + reset clears all 5 progress columns; per-stage 10-row synthetic cohort with monotonic counters + survives mid-stage ROLLBACK |

---

## 2. SQL — migration `sql/178`

```sql
-- 178_bootstrap_stages_target_cohort_fingerprint.sql
--
-- Issue #1273 PR2 — long-pole stage cohort-fingerprint plumbing.
--
-- Spec: docs/proposals/etl/phase-0-pr2-stage-progress-instrumentation.md §2.
--
-- ## Why
--
-- PR1 (#1361, 792291e) shipped `set_stage_target` / `set_stage_processed` /
-- `_current_running_stage_key` helpers. PR2 wires the 8 long-pole stages
-- (S14/15/16/17/18/22/25) to those helpers and surfaces a cohort-definition
-- fingerprint to the operator timeline as an additive tooltip so reviewers
-- can audit "we walked the right slice" without re-greping seven source
-- files.
--
-- Format: `key=value;key=value;...` (semicolon-separated). Operator
-- eyeballs the tooltip — no SHA hash, no JSON, no escaping. Per-stage
-- fingerprint composition documented in spec §4.
--
-- ## Lock impact
--
-- PG 14+ ADD COLUMN with no DEFAULT or with a constant DEFAULT is a
-- metadata-only change, no table rewrite. The column is nullable so a
-- pre-PR2 row reads as NULL (frontend renders no tooltip — same as a
-- stage that never set a fingerprint).

BEGIN;

ALTER TABLE bootstrap_stages
    ADD COLUMN IF NOT EXISTS target_cohort_fingerprint TEXT;

COMMIT;
```

No CHECK constraint on length / shape — fingerprint is operator-readable text. Per [[feedback_grep_alter_constraints]]: no CREATE TABLE definition to update (column lives only in this ALTER per sql/140 + sql/178).

---

## 3. Helper extensions

### 3.1 `set_stage_target` — extend for fingerprint + nullable target

```python
def set_stage_target(
    *,
    run_id: int,
    stage_key: str,
    target_count: int | None,
    cohort_fingerprint: str | None = None,
) -> int:
    """Write target_count + cohort_fingerprint for an in-flight stage.

    Both inputs are independently optional. `target_count=None` is the
    S16 streaming-stage path: caller pins fingerprint without claiming
    a cohort size. `cohort_fingerprint=None` preserves any existing
    value (helpers are first-write-wins; PR2's only callers pass both
    on the same call, so the COALESCE branch is defensive).

    SQL uses COALESCE on both columns so a None param preserves the
    existing DB value rather than NULL-ing it. `last_progress_at`
    always bumps (mirrors set_stage_processed; Codex 2 P2 fold).

    Opens its own psycopg connection, commits, closes — survives caller
    rollback (spec §2.2 #1 Codex iter-1 IMPORTANT-1).

    Returns rowcount: 1 on a write, 0 on a late no-op against a
    terminal stage (status != 'running').
    """
    with psycopg.connect(settings.database_url) as conn:
        cur = conn.execute(
            """
            UPDATE bootstrap_stages
               SET target_count              = COALESCE(%(target_count)s, target_count),
                   target_cohort_fingerprint = COALESCE(%(cohort_fingerprint)s, target_cohort_fingerprint),
                   last_progress_at          = now()
             WHERE bootstrap_run_id = %(run_id)s
               AND stage_key        = %(stage_key)s
               AND status           = 'running'
            """,
            {
                "run_id": run_id,
                "stage_key": stage_key,
                "target_count": target_count,
                "cohort_fingerprint": cohort_fingerprint,
            },
        )
        conn.commit()
        return cur.rowcount or 0
```

**Backward compat**: PR1 callers (none in main; PR1 shipped helpers only) keep working — keyword-only, `cohort_fingerprint` defaults to None, `target_count` was already a required int but the type widens to `int | None` (any int still satisfies `int | None`).

**`set_stage_processed`**: unchanged.

### 3.2 `reset_failed_stages_for_retry` — extend reset surface

At `app/services/bootstrap_state.py:1032-1049`, the per-lane UPDATE currently resets `status`, `started_at`, `completed_at`, `last_error`, `rows_processed`. PR2 adds 4 columns:

```python
cursor = conn.execute(
    """
    UPDATE bootstrap_stages
       SET status                    = 'pending',
           started_at                = NULL,
           completed_at              = NULL,
           last_error                = NULL,
           rows_processed            = NULL,
           -- #1273 PR2 — clear in-flight progress columns alongside
           -- the rest of the stage row so the operator timeline does
           -- not show stale target / processed / fingerprint from
           -- the prior failed pass on a fresh retry.
           target_count              = NULL,
           processed_count           = 0,
           last_progress_at          = NULL,
           target_cohort_fingerprint = NULL
     WHERE bootstrap_run_id = %(run_id)s
       AND lane             = %(lane)s
       AND stage_order      >= %(min_order)s
    """,
    {"run_id": run_id, "lane": lane, "min_order": min_order},
)
```

**Why all 4**: per audit §6 — without it, a fresh retry shows the bar at last-failed processed/target (misleading) AND the tooltip reflects last-failed cohort fingerprint even when the retry re-discovers a different cohort (e.g. S22 cutoff drifted overnight).

---

## 4. Cohort-fingerprint compute per stage

Format: `key=value;key=value;…` semicolon-separated. No URL-encoding (operator pastes into terminal raw; no `%` or `&` ever appears in the values below). Numbers rendered as decimal; booleans as `true`/`false`; dates as ISO `YYYY-MM-DD`; missing/unbounded values as the literal string `unbounded` or `all`.

| Stage | Shape | Fingerprint (literal `;`-separated, evaluated at cohort-materialization time) |
|---|---|---|
| S14 | list | `is_tradable_only=true;sidecar_sentinel=<n>;sidecar_real_pages=<n>;sidecar_empty=<n>` |
| S15 | list | `days_back=730;filing_types=<count>;instrument_id=<id_or_all>` |
| S16 | streaming | `max_subjects=<n_or_unbounded>;follow_pagination=<bool>;fast_path_seeded=<bool>` |
| S17 | streaming | `chunk_limit=500;max_runtime_seconds=3600;form_types=<count>;cap_per_filer=2;rank_scope=def14a_with_cik;rank_predicate=type<>DEF14A_OR_cik_null_OR_rank<=cap;url_filter=true;tombstone_filter=true;pending_predicate_v1=true` |
| S18 | streaming | `chunk_limit=500;max_runtime_seconds=3600;form_types=10-K,10-K/A;distinct_on=instrument_id;ordering=filing_date_desc,filing_event_id_desc;url_filter=true;pending_predicate_v2=bs_null_OR_acn_diff_OR_retry_due_OR_(tables_null_AND_quarantine_elapsed)` |
| S22 | list | `min_period_of_report=<YYYY-MM-DD>;min_last_13f_hr_at=<YYYY-MM-DD>;deadline_seconds=<float>` |
| S23 | list | `min_last_seen_filed_at=<YYYY-MM-DD_or_none>;deadline_seconds=<float>;directory=sec_nport_filer_directory` |
| S25 | list | `instrument_scope=universe_with_facts;source_table=financial_facts_raw` |

**Reading guide**:

- `<n>` = integer value resolved at fingerprint-compute time.
- `<bool>` = `true` / `false`.
- `rank_scope=def14a_with_cik` (S17) declares the rank cap applies ONLY to DEF 14A rows with non-null CIK; PRE 14A + non-CIK rows bypass the cap. `rank_predicate=type<>DEF14A_OR_cik_null_OR_rank<=cap` is the verbatim disjunction (per PR1 audit memo §3 S17 row). `pending_predicate_v1=true` declares "filing_type IN form_types AND primary_document_url IS NOT NULL AND log.accession_number IS NULL"; bump `v1` → `v2` on any predicate change.
- `pending_predicate_v2=bs_null_OR_acn_diff_OR_retry_due_OR_(tables_null_AND_quarantine_elapsed)` (S18) declares all 4 branches verbatim, including the AND-guard on the `tables_json IS NULL` backfill branch (#560 quarantine clock — see PR1 audit memo §3 S18 row). Bump version on any branch add/remove or guard change.
- S25 does NOT include `total=<n>` in the fingerprint — `len(instrument_ids)` populates `target_count` directly (free after the existing DISTINCT materialization at `app/services/fundamentals/__init__.py:1651`); a separate pre-count over the 10M-row `financial_facts_raw` partition is not defensible as ms-cost (Codex 1 fold).
- **No `pending_at_entry` count** on S17/S18: upfront `COUNT(*)` over the discovery CTE doubles the discovery-pass cost without a LIMIT; rejected per Codex 1 BLOCKING 2. Streaming-style `target_count=None` + cumulative `processed_count` is the operator-visible affordance.

---

## 5. Instrumentation pattern

### 5.1 List-shaped (S14, S15, S22, S23, S25)

```python
from app.services.bootstrap_state import (
    set_stage_target,
    set_stage_processed,
    _current_running_stage_key,
)
from app.services.sec_bulk_orchestrator_jobs import _current_running_bootstrap_run_id

# Resolve orchestration context on entry. If unset, all writes no-op.
_run_id = _current_running_bootstrap_run_id()
_stage_key = _current_running_stage_key(__JOB_NAME__)
_progress_enabled = _run_id is not None and _stage_key is not None

# ... materialize cohort ...
cohort = build_cohort(...)

# Pin target + fingerprint atomically (single helper call, one round-trip).
if _progress_enabled:
    _fingerprint = f"key1={v1};key2={v2};..."   # per §4 table
    set_stage_target(
        run_id=_run_id,
        stage_key=_stage_key,
        target_count=len(cohort),
        cohort_fingerprint=_fingerprint,
    )

# Iterate. Hybrid count+time cadence.
_emit_every_n = max(1, len(cohort) // 100)
_last_emit = monotonic()
for i, item in enumerate(cohort, start=1):
    process(item)
    if _progress_enabled and (i % _emit_every_n == 0 or monotonic() - _last_emit > 30):
        set_stage_processed(run_id=_run_id, stage_key=_stage_key, processed_count=i)
        _last_emit = monotonic()

# Final write on exit.
if _progress_enabled:
    set_stage_processed(run_id=_run_id, stage_key=_stage_key, processed_count=len(cohort))
```

### 5.2 Streaming-style (S16, S17, S18)

S16 has no upfront cohort (streams `_iter_in_universe_subjects`). S17 + S18 have page-bounded discovery (`chunk_limit=500` per page) but PR2 deliberately does NOT pin `target_count` — upfront `COUNT(*)` over the discovery CTE is not defensible as ms-cost (Codex 1 BLOCKING 2). All three follow the same pattern: pin fingerprint on entry, advance `processed_count` cumulatively, never pin `target_count`.

```python
_run_id = _current_running_bootstrap_run_id()
_stage_key = _current_running_stage_key(__JOB_NAME__)
_progress_enabled = _run_id is not None and _stage_key is not None

# No upfront cohort. Fingerprint only on entry; target_count stays NULL.
if _progress_enabled:
    set_stage_target(
        run_id=_run_id,
        stage_key=_stage_key,
        target_count=None,
        cohort_fingerprint=f"max_subjects={max_subjects or 'unbounded'};follow_pagination={follow_pagination};fast_path_seeded={fast_path_seeded}",
    )

_running = 0
_last_emit = monotonic()
for subject in _iter_in_universe_subjects(...):
    process(subject)
    _running += 1
    if _progress_enabled and monotonic() - _last_emit > 30:
        set_stage_processed(run_id=_run_id, stage_key=_stage_key, processed_count=_running)
        _last_emit = monotonic()

if _progress_enabled:
    set_stage_processed(run_id=_run_id, stage_key=_stage_key, processed_count=_running)
```

### 5.3 Per-stage call sites

| Stage | File | Cohort site | Entry guard + target/fingerprint write site | Cadenced emit site | Final emit site |
|---|---|---|---|---|---|
| S14 | `app/services/sec_submissions_files_walk.py` | `_list_cik_secondary_pages()` at `:106` then `:185` `len(targets)` | top of `sec_submissions_files_walk_job` at `:364` (write after cohort materialization) | inside the per-CIK walk loop inside `walk_files_pages` at `:168` | after walk loop returns |
| S15 | `app/workers/scheduler.py` | `cik_rows` at `:4657` / `:4671`, `instrument_ids = ...` at `:4688` | top of `filings_history_seed` at `:4620` (write after `instrument_ids` resolves) | inside the per-instrument seed loop | after loop |
| S16 | `app/jobs/sec_first_install_drain.py` | streaming `_iter_in_universe_subjects` at `:221` | inside `run_first_install_drain` AFTER `seed_manifest_from_filing_events` returns (capture `fast_path_seeded = rows_seeded_from_filing_events > 0`) — write before subject loop starts at `:326` | inside the streaming consumer loop starting `:326`; increment site is `ciks_processed += 1` at `:355` | after streaming exits |
| S17 | `app/services/def14a_ingest.py` | `bootstrap_def14a` at `:1160`; discovery at `:301-348` (page-bounded `chunk_limit=500`, no upfront COUNT) | top of `bootstrap_def14a` at `:1160` (`target_count=None`, fingerprint only) | inside the per-page-drain loop (page boundary: emit `processed_count = cumulative_done`; 30s safety ticker) | after deadline loop exits |
| S18 | `app/services/business_summary.py` | `bootstrap_business_summaries` at `:1446`; discovery at `:1606` (page-bounded `chunk_limit=500`, no upfront COUNT) | top of `bootstrap_business_summaries` at `:1446` (`target_count=None`, fingerprint only) | inside the per-page-drain loop (page boundary: emit `processed_count = cumulative_done`; 30s safety ticker) | after deadline loop exits |
| S22 | `app/workers/scheduler.py` (job entry) + `app/services/institutional_holdings.py` (cohort + loop) | `list_directory_filer_ciks(min_last_13f_hr_at=…)` at `institutional_holdings.py:481-524`, `len(ciks)` | top of `sec_13f_quarterly_sweep` job entry (write after `list_directory_filer_ciks` returns) | inside the per-filer batched sweep at `institutional_holdings.py:1069-1178` | after sweep loop exits |
| S23 | `app/workers/scheduler.py` (job entry, `sec_n_port_ingest` at `:5305`) + `app/services/n_port_ingest.py` (cohort + loop) | `list_nport_filer_ciks(min_last_seen_filed_at=…)` at `scheduler.py:5349`, `len(ciks)` at `:5362` | top of `sec_n_port_ingest` at `:5305` (write after `list_nport_filer_ciks` returns) | inside `ingest_all_fund_filers` per-filer loop | after sweep loop exits |
| S25 | `app/services/fundamentals/bootstrap.py` (job entry, `fundamentals_sync_bootstrap` at `:96`) + `app/services/fundamentals/__init__.py` (cohort + loop, `normalize_financial_periods` at `:1639`) | `instrument_ids = [row[0] for row in cur.fetchall()]` at `__init__.py:1651` (existing materialization — `len(instrument_ids)` is free) | inside `normalize_financial_periods` immediately after `:1651` (write `target_count=len(instrument_ids)`, fingerprint) | inside the per-instrument cursor loop at `:1656-1708` | after cursor exits |

**Job-name resolution**: `_current_running_stage_key` is called with the orchestrator-registered `job_name`, NOT the stage_key. S25 divergence (stage_key=`fundamentals_sync`, job_name=`fundamentals_sync_bootstrap`) is handled by the PR1 helper. For each of the 8 stages the implementation reads its `job_name` from a module-local constant aligned with `_BOOTSTRAP_STAGE_SPECS` (no JOB_INTERNAL_KEYS injection).

**Manual-fire path**: when an operator triggers a bulk job outside the orchestrator (no in-flight bootstrap run), `_current_running_bootstrap_run_id()` returns None, `_progress_enabled=False`, and every helper call is skipped — zero overhead, zero side-effect. Applies to all 8 instrumented stages.

---

## 6. Resolved design questions (carried from PR1 audit §4)

1. **S17/S18 `target_count` semantics** *(REVISED v1.1 per Codex 1 BLOCKING 2)*: **NO upfront COUNT; streaming-style (`target_count=None`) with cumulative `processed_count`**. Audit §4 recommended initial-pending COUNT for operator legibility; Codex showed the COUNT cost is not defensible — the discovery CTE has no LIMIT in COUNT mode and would full-scan `filing_events` before the real loop. S17/S18 follow the S16 streaming pattern: fingerprint pins all cohort dimensions (§4 table — `chunk_limit`, `cap_per_filer`, `pending_predicate_vN`, etc.), `processed_count` advances as cumulative-done across pages. Operator sees "X processed" (no denominator) — same affordance as S16. Trade-off: no "X of Y, deadline-cut at Y/2" — accepted because the fingerprint + cumulative counter still tell the operator what was walked.
2. **S25 pre-materialized count** *(REVISED v1.1 per Codex 1 IMPORTANT 1)*: **YES, via `len(instrument_ids)` after the existing DISTINCT materialization at `app/services/fundamentals/__init__.py:1651` — NO separate COUNT query**. A separate `SELECT COUNT(DISTINCT instrument_id) FROM financial_facts_raw` over the 10M-row partitioned floor table is not defensible as ms-cost (indexes only prefix `instrument_id` as part of wider keys per `sql/156:103`). The existing `cur.fetchall()` at `:1651` already produces the row count for free.
3. **Fingerprint format**: `<key>=<value>;<key>=<value>;…` semicolon-separated. No SHA hash, no JSON. Operator pastes into terminal raw.
4. **Out-of-order target / processed writes**: **frontend already tolerates this**. `ProcessDetailPage.tsx:1188-1217` reads `if (!hasTarget && processed === 0) return null` and renders the no-target branch `"X processed (no target set)"` when target IS NULL + processed > 0. No FE change needed for this case — only the tooltip is additive (§7).

---

## 7. Backend response model + endpoint projection

### 7.1 `BootstrapTimelineStageResponse` (`app/api/processes.py:276-308`)

Add one field after `target_count`:

```python
target_count: int | None
# #1273 PR2 — operator-readable cohort-definition fingerprint set by
# `set_stage_target` at stage entry. Semicolon-separated key=value
# tokens (see spec §4); NULL on legacy rows + stages that never set a
# fingerprint. Surfaced as a `title=` tooltip on the progress-bar
# wrapper; no new visual chrome.
target_cohort_fingerprint: str | None = None
archives: list[BootstrapTimelineArchiveResponse]
```

### 7.2 SELECT projection (`app/api/processes.py:694`)

```sql
SELECT stage_key, stage_order, lane, job_name, status,
       started_at, completed_at, last_error,
       rows_processed, processed_count, target_count,
       target_cohort_fingerprint
  FROM bootstrap_stages
 WHERE bootstrap_run_id = %s
 ORDER BY stage_order ASC, stage_key ASC
```

And in the `BootstrapTimelineStageResponse(...)` construction at `app/api/processes.py:786`, add:

```python
target_cohort_fingerprint=row["target_cohort_fingerprint"],
```

---

## 8. Frontend type + tooltip

### 8.1 `frontend/src/api/types.ts:1417-1435`

Add after `target_count`:

```ts
export interface BootstrapTimelineStageResponse {
  // … existing …
  processed_count: number;
  target_count: number | null;
  /**
   * #1273 PR2 — operator-readable cohort-definition fingerprint. Set
   * by `set_stage_target` at stage entry; null on legacy rows and on
   * stages that never instrument. Rendered as a `title=` tooltip on
   * the progress-bar wrapper.
   */
  target_cohort_fingerprint: string | null;
  archives: BootstrapTimelineArchiveResponse[];
  warning: string | null;
}
```

### 8.2 `frontend/src/pages/ProcessDetailPage.tsx:1186-1221` — tooltip wiring

Additive tooltip on the existing progress-bar wrapper. Wrap the `<div className="mt-1.5">` with `title={…}` reading the new field; no styling change.

```tsx
return (
  <div
    className="mt-1.5"
    title={stage.target_cohort_fingerprint ?? undefined}
  >
    {hasTarget ? (
      // existing block
    ) : (
      // existing block
    )}
  </div>
);
```

`title=undefined` (when the field is null) suppresses the native browser tooltip — same behaviour as before PR2. No new dependency, no a11y change (native `title=` is the same affordance Timeline already uses at `:1225` + `:1233` for warnings + errors).

### 8.3 Test fixtures (Codex 1 IMPORTANT 4 fold)

The new field is **required** on the TS type (`string | null`) to mirror the backend Pydantic contract. Existing test fixtures in `frontend/src/pages/ProcessDetailPage.test.tsx` (~2 stage objects at `:389-410` + `:412-435`) MUST add `target_cohort_fingerprint: null,` per stage entry to compile. PR2 grep-and-add: every literal `processed_count:` line in the file is adjacent to a stage object that needs the new field.

---

## 9. Metrics-analyst skill update

`.claude/skills/metrics-analyst/SKILL.md` — two edits:

### 9.1 Master-index split (line 28)

Current row:

```
| Bootstrap state + stage status | Pipeline | `bootstrap_state`, `bootstrap_stages`, `bootstrap_archive_results` | `/system/bootstrap/status` |
```

Replace with 3 rows so the operator can find each metric independently:

```
| Bootstrap stage final row count | Pipeline | `bootstrap_stages.rows_processed` | `/processes/bootstrap/overview` |
| Bootstrap stage live progress | Pipeline | `bootstrap_stages.target_count + processed_count` | `/processes/bootstrap/timeline` |
| Bootstrap stage cohort fingerprint | Pipeline | `bootstrap_stages.target_cohort_fingerprint` | `/processes/bootstrap/timeline` (tooltip) |
| Bootstrap state + run status | Pipeline | `bootstrap_state`, `bootstrap_runs`, `bootstrap_archive_results` | `/system/bootstrap/status`, `/processes/bootstrap/*` |
```

### 9.2 New §8 sub-section "Bootstrap stage progress (live)"

Full per-metric template fill:

```markdown
### Bootstrap stage progress (live)

- **Definition**: per-stage operator-visible progress bar + cohort tooltip; renders only while `bootstrap_stages.status='running'`.
- **Formula**: list-shaped stages (S14, S15, S22, S23, S25) → `processed_count / target_count` (% complete); streaming-style stages (S16, S17, S18) → `processed_count` cumulative only (no denominator; `target_count IS NULL` by design). Tooltip = `target_cohort_fingerprint` text on every instrumented stage.
- **Source data**: `set_stage_target` + `set_stage_processed` helpers in `app/services/bootstrap_state.py`, called from 8 long-pole stages (#1273 PR2 spec §5).
- **Storage**: `bootstrap_stages.{target_count, processed_count, target_cohort_fingerprint, last_progress_at}` (sql/140 + sql/178).
- **Service**: orchestrator-resolved via `_current_running_bootstrap_run_id` + `_current_running_stage_key`; manual-fire paths skip (zero overhead).
- **Endpoint**: `GET /processes/bootstrap/timeline` projects all 4 fields.
- **Chart**: `frontend/src/pages/ProcessDetailPage.tsx` progress-bar block at `:1186-1221`; tooltip via `title=`.
- **Cadence**: list-shaped — every `max(1, len(cohort)//100)` iterations OR every 30s (whichever first); always once on success exit. Streaming — every 30s wall-clock (no count-based ticker because no upfront cohort size) plus once on exit; S17/S18 also emit at every page boundary (chunk_limit=500).
- **Caveats**: `target_count IS NULL` for all 3 streaming stages (S16, S17, S18) — frontend renders "X processed (no target set)" instead of a bar. List-shaped stages always pin `target_count` pre-loop. `processed_count` reset to 0 on `reset_failed_stages_for_retry` so a fresh retry does not display stale counters.
- **Validation**: `tests/services/test_bootstrap_state_progress.py` + `tests/smoke/test_long_pole_progress.py` (8 stage tests); live SQL `SELECT stage_key, target_count, processed_count, target_cohort_fingerprint FROM bootstrap_stages WHERE bootstrap_run_id=<id> AND status='running'`.
```

---

## 10. Tests

### 10.1 Extend `tests/services/test_bootstrap_state_progress.py`

Add to the existing PR1 suite (which monkeypatches `settings.database_url` per PR1 audit §5):

- **Test 11 — fingerprint write**: seed committed running stage; call `set_stage_target(run_id, stage_key, target_count=42, cohort_fingerprint='k=v;a=b')`; assert column write + `last_progress_at` bumped.
- **Test 12 — fingerprint preserves on None**: seed committed running stage with existing `target_cohort_fingerprint='OLD'`; call `set_stage_target(run_id, stage_key, target_count=99, cohort_fingerprint=None)`; assert target_count updated but fingerprint still `'OLD'` (COALESCE branch).
- **Test 13 — target_count=None preserves on None**: seed committed running stage with `target_count=100`; call `set_stage_target(run_id, stage_key, target_count=None, cohort_fingerprint='NEW')`; assert fingerprint updated but target_count still 100.
- **Test 14 — both-None bumps heartbeat only**: edge case; seed running stage; call with both fields None; assert `last_progress_at` bumped, no other column touched.
- **Test 15 — reset_failed_stages_for_retry clears all 5 progress columns**: seed `bootstrap_state.status='partial_error'` + `last_run_id=R` + a stage status='error' with all 5 progress columns populated (`rows_processed=10`, `target_count=20`, `processed_count=15`, `last_progress_at=now()`, `target_cohort_fingerprint='x=y'`); call `reset_failed_stages_for_retry(conn)`; assert all 5 columns reset (rows_processed=NULL, target_count=NULL, processed_count=0, last_progress_at=NULL, target_cohort_fingerprint=NULL) + status='pending'.

### 10.2 NEW `tests/smoke/test_long_pole_progress.py`

Per spec §2.2 acceptance. One test per stage (8 tests), each:

1. Stub the cohort source to return 10 synthetic items.
2. Drive the stage entry function with a seeded `bootstrap_runs.status='running'` + matching `bootstrap_stages` row.
3. Assert post-run: `target_count == 10` for the 5 list-shaped stages (S14, S15, S22, S23, S25); `target_count IS NULL` for the 3 streaming stages (S16, S17, S18); `processed_count == 10` for every stage; `target_cohort_fingerprint` matches the §4 shape for that stage (regex on key= prefixes).
4. Mid-stage ROLLBACK: monkeypatch the per-item processor on item 5 to raise; assert that progress writes up to item 4 PERSIST (fresh-connection contract from PR1) AND status stays consistent (stage either errored or partial — depends on stage's own error handling, asserted per-stage).

`pytestmark = pytest.mark.xdist_group(name="long_pole_progress")` to serialize within a worker group (DB writes against test DB).

### 10.3 Frontend

- `pnpm --dir frontend typecheck` — new field is required `string | null`. Existing fixtures in `ProcessDetailPage.test.tsx:389-435` MUST be patched to include `target_cohort_fingerprint: null` per §8.3.
- `pnpm --dir frontend test:unit` — add one assertion in `ProcessDetailPage.test.tsx` for a stage with a non-null fingerprint: render, query the progress-bar wrapper, assert `title=` attribute matches the fingerprint string. If the test file doesn't already test the progress-bar branch, add a fixture stage + minimal assertion.

### 10.4 Real-numbers dev-DB smoke (per CLAUDE.md ETL clause #11)

After PR2 lands locally, before push:

1. `POST /system/bootstrap/run` against dev DB.
2. Wait until S14/S15 enter `status='running'` (~1-2 min after dispatch).
3. `SELECT stage_key, target_count, processed_count, target_cohort_fingerprint FROM bootstrap_stages WHERE bootstrap_run_id=<id> AND status='running'`. Capture output.
4. Open `/admin/process/bootstrap` in browser. Screenshot the Timeline showing the progress bar + tooltip on hover.
5. Wait until S25 (or any long-pole stage) enters running. Repeat 3+4.
6. Paste SQL output + screenshots into PR description.

If S22 / S25 don't run within session (Run #8 took 617 min): smoke S14/S15/S17/S18 (~10-60 min stages) only and document S22/S25 as "verified by unit test §10.2; live smoke at operator's discretion".

---

## 11. Cross-impact

Verified at draft time:

- **`BootstrapTimelineStageResponse` consumers** (grep): `app/api/processes.py:276` (definition), `:331` (used in `BootstrapTimelineResponse`), `:748` (`stage_payload` list type), `:786` (construction); `frontend/src/api/types.ts:1417` (definition), `:1453` (used in `BootstrapTimelineResponse`); `frontend/src/pages/ProcessDetailPage.tsx:31` (import), `:1068`, `:1070`, `:1083`, `:1138` (consumers). Adding an optional field with `= None` default + `string | null` TS type does NOT break any consumer.
- **`bootstrap_stages` schema history** (grep `ALTER TABLE bootstrap_stages`): sql/129 (CREATE) + 131/132/142/147/165 (ALTERs). sql/140 added target_count/processed_count/last_progress_at. sql/178 (this PR) adds target_cohort_fingerprint. No CHECK constraint surface to widen.
- **`reset_failed_stages_for_retry` callers**: `app/api/bootstrap.py:499` (retry-failed endpoint) + `app/api/processes.py:1088/1287` (admin processes wrappers). Both consume `(run_id, reset_count)` tuple unchanged — extending the SET clause is invisible to callers.
- **`_resolve_stage_rows`** at `bootstrap_orchestrator.py:1255 / :1558`: reads `rows_processed` only; untouched by PR2.
- **`mark_stage_success`**: writes `rows_processed` only; does NOT touch `processed_count` or `target_count`. PR2's `set_stage_processed` final-emit write at end-of-stage is the operator-visible counter; `rows_processed` continues as the post-hoc DB-row count for cap-eval. No double-write conflict.
- **Manual-fire path**: `_current_running_bootstrap_run_id()` returns None outside orchestration → all 8 stages skip progress writes → no orphan rows or counter drift.
- **`bootstrap_adapter.MAX(last_progress_at)`** (referenced by stale-detection `mid_flight_stuck`): both helpers bump `last_progress_at` per PR1 Codex 2 P2 fold. PR2 preserves the bump in the COALESCE-extended `set_stage_target`. No stale-detection regression.
- **S23 vs Phase 5 retirement** (Codex 1 IMPORTANT 3 fold): master plan §7 Phase 5 (`#1348`) retires S19/S20/S23 from `_BOOTSTRAP_STAGE_SPECS`. Until Phase 5 ships, S23 is a live long-pole stage doing real work (per `app/workers/scheduler.py:5305-5365`) and SHOULD be instrumented for operator-visible parity with S22 (same list+deadline shape). PR2 instruments S23; Phase 5 will later remove both the spec entry AND the instrumentation in a single coherent retirement PR.
- **FE test fixtures** (Codex 1 IMPORTANT 4 fold): `frontend/src/pages/ProcessDetailPage.test.tsx:389-435` has 2 stage-object fixtures missing `target_cohort_fingerprint`. PR2 adds `target_cohort_fingerprint: null` to each. Grep confirms only this file has stage-object fixtures (2 `processed_count:` occurrences total in the test suite).

---

## 12. Acceptance

PR2 ships when ALL of:

1. ✅ sql/178 applied (CI migration runner asserts on `psql -c "\d bootstrap_stages"` showing the column).
2. ✅ `set_stage_target` extended; PR1 callsites (none in main) keep compiling.
3. ✅ `reset_failed_stages_for_retry` resets all 5 progress columns; Test 15 green.
4. ✅ 8 stages instrumented per §5 + §4 fingerprint shapes (S14, S15, S16, S17, S18, S22, S23, S25); Tests 11-14 + `test_long_pole_progress.py` (8 stage tests) green.
5. ✅ Backend response model + SELECT projection extended; existing endpoint tests green.
6. ✅ Frontend type + tooltip wired; `pnpm typecheck` + `test:unit` green.
7. ✅ Metrics-analyst skill updated per §9.
8. ✅ Pre-push gates: `uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest`.
9. ✅ Frontend gates: `pnpm --dir frontend typecheck && pnpm --dir frontend test:unit`.
10. ✅ Real-numbers dev-DB smoke per §10.4; SQL + screenshots in PR description.
11. ✅ Codex 1 (this spec) + Codex 2 (pre-push diff) both APPROVE.
12. ✅ PR description records cross-impact + design-question resolutions + commit SHA per CLAUDE.md ETL clauses 8-12 (PR2 is schema-migration-touching: clause 8 = the 5-instrument panel doesn't apply because the only operator-visible figure PR2 changes is a UI affordance, not a metric value; clauses 9-12 are satisfied by §10.4 dev-DB smoke).

---

## 13. Settled-decisions check

Reviewed `docs/settled-decisions.md`. None of the live decisions apply to PR2 — fingerprint plumbing does not touch identifiers, fundamentals provider, scoring, portfolio manager, execution guard, bootstrap-gate carve-outs, share-class CIK semantics, or canonical-instrument redirects. Recorded for audit trail.

## 14. Review-prevention-log check

Relevant entries:

- **`feedback_psycopg3_savepoint_commit`** ([prevention log: psycopg3 transaction inside open tx is SAVEPOINT not COMMIT]): PR2 helpers open their own connection via `psycopg.connect(url)` + `conn.commit()` — no SAVEPOINT trap. PR1 audit §7 already cleared this for `set_stage_target` / `set_stage_processed`; PR2's COALESCE extension keeps the same shape.
- **`feedback_grep_alter_constraints`** ([prevention log: Grep both CREATE TABLE and ALTER TABLE constraints]): Grep'd both. `bootstrap_stages` CREATE at sql/129 + ALTERs at 131/132/140/142/147/165; no CHECK / FK on target_count or processed_count, no constraint surface on the new column. sql/178 adds no CHECK either.
- **`feedback_writethrough_needs_backfill`** ([prevention log: Write-through retrofit needs explicit backfill]): not applicable — PR2 fingerprint is forward-only; legacy rows (status not 'running' at deploy time) keep target_cohort_fingerprint=NULL forever, frontend renders no tooltip — graceful degradation.
- **"Single-row UPDATE silent no-op on missing row"** (PR #70): PR2 helpers' UPDATEs match on `status='running'`; rowcount=0 is the documented late-write no-op. PR1's helper test pattern covers it (Tests #2 + #5). PR2 inherits.
- **"f-string SQL composition for column / table identifiers"** (PR #110): COALESCE column names + SET targets are static literal SQL; no f-string interpolation. Helper SQL is one fixed UPDATE statement.
- **`feedback_test_db_isolation`** ([no test wipes dev DB]): tests monkeypatch `settings.database_url` per PR1 §5 pattern; PR2 extension tests follow the same fixture.

No prevention-log entry blocks PR2.
