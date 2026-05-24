# PR1 implementation plan — Job registry refactor (source-lock + ParamMetadata + params_snapshot)

> Plan-stage doc for PR1 of the #1064 admin-control-hub follow-up sequence. Built on top of PR0 (`docs/wiki/job-registry-audit.md`). Codex plan-stage review (mandatory checkpoint 1) — 3 rounds, final CLEAN.

## Carve-out — PR1 → PR1a + PR1b + PR1c (operator-approved 2026-05-09)

PR1 is large (15 sub-tasks, multi-hour). Split into three sequential PRs to reduce blast radius per merge:

### PR1a — Foundation (no behaviour change)
- sql/141 migration (`job_runs.params_snapshot JSONB`).
- New `app/jobs/sources.py` — `Lane` type + `JobInvoker` alias + `JOB_NAME_TO_SOURCE` registry built from BOTH `SCHEDULED_JOBS` AND `_BOOTSTRAP_STAGE_SPECS`.
- New `app/services/processes/param_metadata.py` — `ParamMetadata` Pydantic model + `validate_job_params` (single validator with `allow_internal_keys` mode) + `materialise_scheduled_params` helper + `JOB_INTERNAL_KEYS` allow-list.
- `ScheduledJob` extension: add `source: Lane`, `params_metadata: tuple[ParamMetadata, ...]`, `display_name: str | None` (PR4 reads `display_name` later — populate now to avoid touching every entry twice).
- Populate every entry in `SCHEDULED_JOBS` per audit §2.
- `StageSpec` extension: add `params: Mapping[str, Any]` field.
- Populate every stage in `_BOOTSTRAP_STAGE_SPECS` per audit §3 + §4.
- `JobLock` source-keyed (registry-aware lookup; conflict assert at module-load; KeyError on unknown job_name).
- `frontend/src/api/types.ts` mirror of `ParamMetadata`.
- Tests: registry shape, no-legacy-`sec`-leak regression, source lookup coverage, source lock semantics, ParamMetadata validation, types.ts round-trip.
- **NO** API change, **NO** bootstrap gate change, **NO** wrapper deletion, **NO** `_INVOKERS` contract change.

### PR1b — Operator gate + envelope
- New `app/services/processes/bootstrap_gate.py` — `check_bootstrap_state_gate()` helper (audit-row only on actual override).
- `_INVOKERS` contract widen to `dict[str, Callable[[Mapping[str, Any]], None]]`. Every existing zero-arg body migrates by accepting + ignoring `params`.
- `app/api/jobs.py` — accept `{params, control}` envelope; legacy flat-dict normalisation; 400 on validate_job_params failure or unknown control key. Contract preserved as 202 queue-first.
- Wire `check_bootstrap_state_gate` at scheduled-fire path (`record_job_skip` BEFORE `_tracked_job` per prevention-log L791) AND queue-consumer path (`mark_request_rejected` on skip — NOT `mark_request_completed`).
- `materialise_scheduled_params` invocation at scheduled-fire path; `params_snapshot` populated from validated effective params on all three paths (manual / scheduled / bootstrap).
- 409 reason key `bootstrap_not_complete` added to `frontend/src/components/admin/processStatus.ts::REASON_TOOLTIP`.
- Tests: gate paths (scheduled / manual / override), envelope normalisation, snapshot 3-paths, `mark_request_rejected` PREVENTION-grade.

### PR1c — Bespoke wrapper deletion + 11-file ref rewire
- Delete `bootstrap_filings_history_seed`, `sec_first_install_drain_job`, `bootstrap_sec_13f_recent_sweep_job` from `app/services/bootstrap_orchestrator.py`.
- Promote `filings_history_seed` + `sec_first_install_drain` + extend `sec_13f_quarterly_sweep` to honour the bootstrap params (`min_period_of_report`, audit-only `source_label`).
- Bootstrap stages 14, 15, 21 dispatch to the SCHEDULED_JOBS entries with `StageSpec.params`.
- Rewire `app/jobs/runtime.py`, `app/services/sec_submissions_ingest.py`, `tests/test_bootstrap_orchestrator.py`, `tests/test_filings_form_allowlist.py`, `tests/test_jobs_runtime.py`, `docs/specs/bootstrap/first-install.md`, `2026-05-08-admin-control-hub-rewrite.md`, `2026-05-08-bulk-datasets-first-bootstrap.md`, `2026-05-08-filing-allow-list-and-raw-retention.md`, `docs/wiki/runbooks/runbook-first-install-bootstrap.md`, `docs/wiki/job-registry-audit.md`.
- Tests: extraction equivalence (deleted-wrapper namespace gone; new SCHEDULED_JOBS entries produce same outputs with bootstrap-supplied params dict).
- `_LANE_MAX_CONCURRENCY` retired (or all values set to 1) — bootstrap dispatcher's parallel-DB-stage assumption from #1020 dropped. Tech-debt note for first-install wall-clock regression.

Each PR sized for ≤1 hour Codex-pre-push + review-bot turnaround.

---

## Goal

Lift duplication out of bootstrap-only wrappers into shared workflow helpers; introduce `ParamMetadata` per job; move `JobLock` key from `job_name` → `source`; add `job_runs.params_snapshot JSONB`; unify the `bootstrap_state.status='complete'` gate across scheduled-fire and manual-trigger paths. Foundation for PR2 (FE Advanced disclosure) and the rest of the umbrella.

## Operator-locked decisions (do NOT revisit)

- Source-level `JobLock`. Sources: `init` / `etoro` / `sec_rate` / `sec_bulk_download` / `db`. Cross-source = parallel; within-source = serialised.
- Per-job `ParamMetadata` data model on BE; mirrored in `frontend/src/api/types.ts`.
- Bootstrap stage = `(stage_key, stage_order, lane, job_name, params dict)`. No bespoke wrappers.
- `bootstrap_state.status='complete'` gates BOTH scheduled fires and manual triggers. `partial_error` blocks scheduled. Manual remediation allowed with `?override_bootstrap_gate=true` + `decision_audit` row.
- Same `job_name` + different params still serialises under one source lock. Per-param-set lock identity deferred to v2.
- `prefetch_urls`, `follow_pagination`, `use_bulk_zip`, `paginate`, `source_label`, `match_threshold` deliberately NOT operator-exposed (per audit §6).
- No raw Pydantic JSON Schema exposed to FE — `ParamMetadata` is the contract.

## Implementation sequence

### Step 1 — Schema migration

New `sql/<NNN>_job_runs_params_snapshot.sql`:

```sql
ALTER TABLE job_runs ADD COLUMN IF NOT EXISTS params_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb;
COMMENT ON COLUMN job_runs.params_snapshot IS 'Params dict the run was invoked with. Manual triggers write the operator payload; scheduled fires write registry defaults.';
```

ETL clauses 8-11 N/A (`job_runs` is operator-process audit, not ownership/fundamentals/observations). Clause 12 records this in PR description. Smoke covered by `tests/smoke/test_app_boots.py`.

### Step 2 — `ParamMetadata` model + types.ts mirror

New `app/services/processes/param_metadata.py`:

```python
from pydantic import BaseModel
from typing import Any, Literal

ParamFieldType = Literal[
    "string", "int", "float", "date", "quarter",
    "ticker", "cik", "bool", "enum", "multi_enum",
]

class ParamMetadata(BaseModel):
    name: str
    label: str
    help_text: str
    field_type: ParamFieldType
    default: Any | None = None
    advanced_group: bool = True
    enum_values: tuple[str, ...] | None = None
    min_value: int | float | None = None
    max_value: int | float | None = None
```

Mirror in `frontend/src/api/types.ts`:

```ts
export type ParamFieldType =
  | "string" | "int" | "float" | "date" | "quarter"
  | "ticker" | "cik" | "bool" | "enum" | "multi_enum";

export interface ParamMetadata {
  name: string;
  label: string;
  help_text: string;
  field_type: ParamFieldType;
  default: unknown | null;
  advanced_group: boolean;
  enum_values: readonly string[] | null;
  min_value: number | null;
  max_value: number | null;
}
```

Round-trip test covers one canonical job.

### Step 3 — `ScheduledJob` extensions

`app/workers/scheduler.py:193`:

```python
@dataclass(frozen=True)
class ScheduledJob:
    name: str
    description: str
    cadence: Cadence
    source: Lane  # NEW (required) — picks the JobLock bucket
    catch_up_on_boot: bool = True
    prerequisite: PrerequisiteFn | None = None
    params_metadata: tuple[ParamMetadata, ...] = ()  # NEW
    display_name: str | None = None  # PR4 reads from this; populate now to keep PR4 trivial
```

Every entry in `SCHEDULED_JOBS` gets:
- `source=Lane(...)` per audit §2.
- `params_metadata=(ParamMetadata(...), ...)` per audit §2 (empty tuple where audit says no operator-exposable params).
- `display_name="..."` per audit §2 (populated now even though only PR4 renders ⓘ).

Lane type alias `Lane = Literal["init", "etoro", "sec_rate", "sec_bulk_download", "db"]`.

### Step 4 — `JobLock` keyed on source

`app/jobs/locks.py:76` plus new `app/jobs/sources.py`:

- Build a canonical `JOB_NAME_TO_SOURCE: dict[str, Lane]` from BOTH `SCHEDULED_JOBS` AND `_BOOTSTRAP_STAGE_SPECS` (using `_effective_lane(stage_key, default_lane)` for stage entries). Bootstrap-only invokers (`nightly_universe_sync`, `sec_bulk_download`, `sec_*_ingest_from_dataset`, `sec_submissions_files_walk`, etc.) MUST appear here even though they're not in SCHEDULED_JOBS — without them, source lookup falls back silently and the source-lock semantics break.
- Module-load time: assert no conflict between scheduled `source` and bootstrap `_effective_lane` for any job that appears in both. Any conflict is a fail-fast import error (Codex BLOCKING — silent fallback violates the source-lock decision).
- Lock key = `f"job_source:{source}"` instead of `f"job:{job_name}"`. Postgres advisory key derived via deterministic hash of source string.
- Existing call sites (`with JobLock(database_url, job_name): ...`) keep their signature; the source resolution is internal — `JobLock` looks up `JOB_NAME_TO_SOURCE[job_name]`.
- Unknown `job_name` (e.g. test fixtures): raise `KeyError` from JobLock construction. NO production fallback. Tests must register their fixture jobs in the registry or use a test-only escape hatch (`JobLock.test_only_per_name(job_name)`).
- Legacy `lane='sec'` from the pre-#1020 catch-all does NOT leak into source keys — every entry must map to one of the five new sources. Regression test pins this.

### Step 5 — `/jobs/<name>/run` endpoint + single-validator design

Codex BLOCKING fix: ONE validator with a mode flag, not two parallel paths.

New `app/services/processes/param_validation.py`:

```python
def validate_job_params(
    job_name: str,
    params: dict[str, Any],
    *,
    allow_internal_keys: bool,  # bootstrap dispatcher = True; manual API = False
) -> dict[str, Any]:
    """Validates against the job's ParamMetadata + canonical INTERNAL_KEYS allow-list.

    Returns the coerced dict. Raises ParamValidationError on mismatch.
    Coercion: int from string, date from ISO string, ticker → instrument_id, cik → 10-digit zero-padded.
    Unknown keys raise unless allow_internal_keys=True AND key is in JOB_INTERNAL_KEYS[job_name].
    """
```

`JOB_INTERNAL_KEYS: dict[str, frozenset[str]]` is a per-job allow-list (e.g. `sec_13f_quarterly_sweep`: `{"source_label"}`). Lives next to the registry. Bootstrap dispatcher passes `allow_internal_keys=True`; API path passes `False`. Both paths run the SAME coercion + bounds checking.

`app/api/jobs.py` `POST /jobs/<name>/run`:

- Body envelope is canonical (Codex round-3 WARNING — body shape ambiguity resolved):

  ```jsonc
  // POST /jobs/<name>/run — canonical body
  {
    "params": { /* validated job params */ },
    "control": { /* operator-control flags; allowed: override_bootstrap_gate */ }
  }
  ```

- For backwards-ergonomic operator scripts that POST a flat dict (e.g. `{"start_date": "..."}`), the API normalises legacy flat bodies → `{"params": <body>, "control": {}}` BEFORE validation. A normalisation test pins the legacy → envelope mapping. Per-job test asserts both shapes produce identical `pending_job_requests.payload`.
- Call `validate_job_params(job_name, body.params, allow_internal_keys=False)` synchronously. ParamValidationError → 400 (this IS input validation, not gate).
- Validate `body.control` keys against fixed allow-list `{"override_bootstrap_gate"}`. Unknown control keys → 400.
- Contract preserved as 202 queue-first (current behaviour; do NOT introduce synchronous 409). Bootstrap gate runs at queue-consumer dispatch time, NOT at API time.
- 404 on unknown job_name (existing).

**Payload envelope (Codex round-2 BLOCKING).** Mixing operator params + control flags in the same dict risks `_override_bootstrap_gate` leaking into `params_snapshot` or invoker params. Use a strict envelope:

```jsonc
// pending_job_requests.payload
{
  "params": { /* validated job params; flows to invoker AND job_runs.params_snapshot */ },
  "control": { /* operator-control flags; consumed by dispatcher only, never reaches invoker */
    "override_bootstrap_gate": true
  }
}
```

Flow:
- API: `validate_job_params(...)` runs against `body.params` only. `body.control` keys validated against a fixed allow-list (`{"override_bootstrap_gate"}`). Unknown control keys → 400.
- Dispatcher: reads `payload.control.override_bootstrap_gate` to drive the gate; passes `payload.params` to the invoker.
- `job_runs.params_snapshot` is populated from `payload.params` ONLY — control flags never appear in operator audit history. (Audit row in `decision_audit` records the override separately.)
- `?override_bootstrap_gate=true` query param at the API layer is the ergonomic shortcut; API code translates it into `body.control.override_bootstrap_gate=True` before publishing.

### Step 5.5 — `StageSpec.params` field (Codex round-2 WARNING)

Operator-locked decision says bootstrap stage is `(stage_key, stage_order, lane, job_name, params dict)`. Add `params` as a `StageSpec` field:

```python
@dataclass(frozen=True)
class StageSpec:
    stage_key: str
    stage_order: int
    lane: Lane
    job_name: str
    params: Mapping[str, Any] = field(default_factory=dict)
```

Bootstrap stage tuple in `_BOOTSTRAP_STAGE_SPECS` populated per audit §3 / §4. Example:

```python
_spec("filings_history_seed", 14, "sec_rate", "filings_history_seed",
      params={"days_back": 730, "filing_types": _SEC_INGEST_KEEP_FORMS_TUPLE}),
_spec("sec_first_install_drain", 15, "sec_rate", "sec_first_install_drain",
      params={"max_subjects": None}),
_spec("sec_13f_recent_sweep", 21, "sec_rate", "sec_13f_quarterly_sweep",
      params={"min_period_of_report": "<materialised at dispatch: today - 380d>", "source_label": "sec_edgar_13f_directory_bootstrap"}),
# Codex round-3 BLOCKING: param name must match invoker contract — invoker honours
# `min_period_of_report` (date), not `min_period_of_report_days_back` (int). Bootstrap
# dispatcher computes the absolute date at dispatch time so the stage spec stays a
# data-only declaration (no `date.today()` call at module-load time).
```

Dispatcher: passes `stage.params` through `validate_job_params(allow_internal_keys=True)` then to invoker. No DB schema change — `bootstrap_stages` table doesn't store params (immutable across runs; lives in code).

### Step 6 — Extract workflow helpers + delete bespoke wrappers

Codex BLOCKING reframe: the registered invoker MUST stay a thin wrapper that owns `_tracked_job + tracker.row_count + helper call`. What gets DELETED is the bespoke wrapper FILE / module-level function for the bootstrap-only variant; the SCHEDULED_JOBS body is rewritten to be params-aware.

**Invoker contract change (Codex round-2 BLOCKING).** Current `_INVOKERS: dict[str, Callable[[], None]]` (zero-arg). PR1 widens to `_INVOKERS: dict[str, Callable[[Mapping[str, Any]], None]]` so bodies receive a validated `params` dict. Adapter at the call site (runtime + bootstrap dispatcher) supplies `{}` when no params present. Call-site rewire is module-load-time wiring; existing zero-arg bodies migrate by ignoring the param: `def heartbeat(params: Mapping[str, Any]) -> None:` with `del params` in the body. Type alias `JobInvoker = Callable[[Mapping[str, Any]], None]` lives in `app/jobs/sources.py` next to the source registry.

**Scheduled-default snapshot materialisation (Codex round-3 BLOCKING).** When a scheduled cron fires with no operator payload, the dispatch path MUST materialise registry defaults from `ParamMetadata.default` BEFORE invoker dispatch + `params_snapshot` write. Helper `materialise_scheduled_params(job_name) -> dict[str, Any]` lives in `app/services/processes/param_metadata.py`:

```python
def materialise_scheduled_params(job_name: str) -> dict[str, Any]:
    """Build the params dict a scheduled fire of this job would invoke with.
    Reads each ParamMetadata.default; omits None defaults so invoker logic
    can distinguish 'operator left it blank' from 'operator set it to null'."""
    ...
```

Scheduled fire flow: `materialise_scheduled_params(job_name) → validate (allow_internal_keys=False) → invoker(params) + job_runs.params_snapshot=params`. Both manual and scheduled paths therefore go through validate before snapshot, ensuring snapshot reflects the EFFECTIVE params, not the raw dict.

Pattern for each of the three:

```python
def filings_history_seed(params: dict[str, Any]) -> None:
    """Registered as _INVOKERS['filings_history_seed']. Bootstrap stage 14 dispatches
    with params={'days_back': 730, 'filing_types': sorted(SEC_INGEST_KEEP_FORMS)};
    operator manual trigger uses operator-supplied params or registry defaults."""
    days_back = params.get('days_back', _FILINGS_HISTORY_DAYS)  # default 730
    filing_types = params.get('filing_types', sorted(SEC_INGEST_KEEP_FORMS))
    instrument_id = params.get('instrument_id')  # operator-only narrow scope

    with _tracked_job(JOB_FILINGS_HISTORY_SEED) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            instrument_ids = _resolve_target_instruments(conn, instrument_id)
        from_date = date.today() - timedelta(days=days_back)
        to_date = date.today()
        with (
            SecFilingsProvider(user_agent=settings.sec_user_agent) as sec,
            psycopg.connect(settings.database_url) as conn,
        ):
            summary = refresh_filings(provider=sec, ..., filing_types=filing_types)
        tracker.row_count = summary.filings_upserted
```

The above is now ONE function (in `app/workers/scheduler.py` or a new `app/jobs/scheduled_bodies.py`), not THREE: scheduled-cron + bootstrap-only-wrapper + a hidden third one. Same for `sec_first_install_drain` + `sec_13f_quarterly_sweep`.

Per audit §4, three wrappers collapse:

#### 4.1 `bootstrap_filings_history_seed`

- The wrapper's hardcoded params (`days_back=730`, `filing_types=sorted(SEC_INGEST_KEEP_FORMS)`, `instrument_ids=<every CIK-mapped tradable>`) are operator-tunable.
- New scheduled job `filings_history_seed` (cadence: manual-only — no auto-fire; bootstrap dispatches it).
- Body: zero-arg → `run_filings_history_seed(conn, provider, **params_with_defaults)`.
- The "every CIK-mapped tradable" SELECT moves into the scheduled-job body (was inside the wrapper).
- Bespoke wrapper file deleted; `JOB_BOOTSTRAP_FILINGS_HISTORY_SEED` constant deleted; bootstrap stage 14 dispatches `filings_history_seed` with `params={"days_back": 730, "filing_types": [...]}`.

#### 4.2 `sec_first_install_drain_job`

- Wrapper hardcodes `follow_pagination=True`, `use_bulk_zip=False`, `max_subjects=None`.
- `follow_pagination` + `use_bulk_zip` stay frozen at the registered-callable layer (NOT operator-exposed per audit). `max_subjects` becomes the only operator-exposable param.
- New scheduled job `sec_first_install_drain` (cadence: manual-only).
- Body: zero-arg → `run_first_install_drain(conn, http_get=_make_sec_http_get(sec), follow_pagination=True, use_bulk_zip=False, max_subjects=params.get("max_subjects"))`.
- HTTP-get adapter stays internal.
- Bespoke wrapper file deleted; bootstrap stage 15 dispatches `sec_first_install_drain` with `params={"max_subjects": null}`.

#### 4.3 `bootstrap_sec_13f_recent_sweep_job`

- Wrapper hardcodes `min_period_of_report=date.today()-380d`, `source_label="sec_edgar_13f_directory_bootstrap"`.
- The existing scheduled `sec_13f_quarterly_sweep` already calls the same helper (`ingest_all_active_filers`) with `source_label="sec_edgar_13f_directory"`, `min_period_of_report=None`. PR1 extends `sec_13f_quarterly_sweep` to honour `params["min_period_of_report"]` + an internal `params["source_label"]` (NOT operator-exposed, but bootstrap dispatcher can override).
- Bespoke wrapper file deleted; bootstrap stage 21 dispatches `sec_13f_quarterly_sweep` with `params={"min_period_of_report": "<today-380d>", "source_label": "sec_edgar_13f_directory_bootstrap"}`.
- The `source_label` in `params` lives in the bootstrap stage's hardcoded params dict, NOT in the operator-facing ParamMetadata. Schema enforced: `validate_params` on `/jobs/<name>/run` rejects unknown keys; bootstrap dispatcher uses a separate code path that allows the audit-only keys.

### Step 7 — `bootstrap_state.status='complete'` universal gate

Codex WARNING reframe: the gate is ORTHOGONAL to the per-job prerequisite (NOT folded into it). Order: bootstrap gate first, then prereq. If both fail, gate wins (operator must see "bootstrap not complete" not "no coverage rows" — the gate is the actionable signal).

API path stays 202 queue-first; gate fires at the queue consumer (dispatcher / listener) when the row dequeues. Manual outcome: `mark_request_rejected` with reason `bootstrap_not_complete`. Scheduled-fire / catch-up outcome: `job_runs.status='skipped'` with reason `bootstrap_not_complete`.

New `app/services/processes/bootstrap_gate.py`:

```python
def check_bootstrap_state_gate(
    conn: psycopg.Connection[Any],
    *,
    job_name: str,
    invocation_path: Literal["scheduled", "manual_queue"],
    override_present: bool,
    operator_id: UUID | None = None,
) -> tuple[bool, str]:
    """Returns (allowed, reason).

    Logic (Codex round-2 WARNING — audit row only when override actually fires):
      1. status='complete' → return (True, '') with NO audit row.
      2. status != 'complete' AND invocation_path='manual_queue' AND override_present=True
         → write decision_audit row 'bootstrap_gate_override' + return (True, '').
      3. otherwise → return (False, 'bootstrap_not_complete').

    Override audit fires ONLY when the gate would have blocked but the operator
    chose to bypass — never on a happy-path 'complete' run.
    """
```

- Wired into:
  - `app/workers/scheduler.py::_run_scheduled_job` — scheduled fire path. On (False, ...) → `record_job_skip(JOB_NAME, reason)` BEFORE entering `_tracked_job` (per prevention-log L791).
  - The queue dispatcher / listener (`app/jobs/listener.py` or `app/jobs/runtime.py`) — manual queue path. On (False, ...) → `mark_request_rejected(request_id, error_msg=reason)` (Step 8 PREVENTION-grade rule from skill addendum §6.5.7). NEVER `mark_request_completed` for skipped runs.
- Gate runs BEFORE the per-job prerequisite. If gate passes but prereq fails → existing skipped-with-reason path.
- 409 reason key `bootstrap_not_complete` added to `frontend/src/components/admin/processStatus.ts::REASON_TOOLTIP`. Note: the FE sees the rejection via the `pending_job_requests.status='rejected'` row, not via a synchronous 409 — the `REASON_TOOLTIP` text is consumed by the queue-row renderer.

### Step 8 — Tests

New test files / sections (Codex WARNING coverage gaps applied):

| Test | Coverage |
|---|---|
| `tests/test_job_registry.py` | Every `ScheduledJob` has non-NULL `source`; `params_metadata` validates against runtime invocation; `display_name` non-empty; conflict-test where scheduled `source` and bootstrap `_effective_lane` disagree fails fast at module-load |
| `tests/test_job_registry.py::test_no_legacy_sec_lane_leak` | Regression — legacy `lane='sec'` (pre-#1020 catch-all) does NOT appear in any source key |
| `tests/test_job_name_to_source_lookup.py` | Every name in `_INVOKERS` used by bootstrap stages resolves to a source; bootstrap-only names (`nightly_universe_sync`, `sec_bulk_download`, `sec_*_ingest_from_dataset`, `sec_submissions_files_walk`) all present |
| `tests/test_joblock_per_source.py` | Two same-source jobs serialise; two cross-source jobs run in parallel; unknown job_name raises KeyError (no silent fallback) |
| `tests/test_bootstrap_state_gate.py` | Scheduled fire path: `partial_error` skipped with `bootstrap_not_complete`; manual queue path: `mark_request_rejected` with `bootstrap_not_complete` (NOT `mark_request_completed` — PREVENTION-grade); manual override flag → `decision_audit` row + run proceeds; gate vs prereq priority — both failing means gate reason wins |
| `tests/test_params_snapshot.py` | Three populate paths: (a) manual operator payload → snapshot reflects payload; (b) scheduled cron with no payload → snapshot is registry default; (c) bootstrap dispatcher internal payload (`source_label`, etc) → snapshot reflects bootstrap-supplied dict |
| `tests/test_param_metadata_validation.py` | Each field_type validates + coerces; enum_values membership enforced; bounds enforced; unknown keys rejected with `allow_internal_keys=False`; allow_internal_keys=True permits keys in `JOB_INTERNAL_KEYS[job_name]` only; manual API path always uses `allow_internal_keys=False` (prevents operator from setting `source_label`) |
| `tests/test_workflow_helper_extraction.py` | The three former bespoke wrapper functions are gone (assert AttributeError on import); the registered invokers `filings_history_seed` / `sec_first_install_drain` / `sec_13f_quarterly_sweep` accept the bootstrap stage's hardcoded params dict and produce equivalent outputs (same `tracker.row_count`, same downstream row counts) |
| `frontend/src/api/types.test.ts` | Round-trip ParamMetadata Pydantic ↔ TS types — at least one canonical job's metadata round-trips through JSON |

### Step 9 — Local gates + Codex pre-push diff review (checkpoint 2)

- `unset VIRTUAL_ENV; uv run ruff check .`
- `uv run ruff format --check .`
- `uv run pyright`
- `uv run pytest` (full suite — registry shape + bootstrap gate + params_snapshot tests new)
- `pnpm --dir frontend typecheck`
- `pnpm --dir frontend test:unit`
- Codex `codex exec review` on branch — fix any real findings before push.

## Out of scope for PR1 (deferred)

- FE Advanced disclosure renderer (PR2).
- ⓘ tooltip rendering (PR4 — PR1 just populates `display_name` strings).
- Stage-level cancellation polish (PR7).
- Per-param-set lock identity (v2).
- `cik_discovery.py` deletion (PR5).
- Pre-flight resource estimate ("this will hit SEC ~12,000 times").

## Smoke verification

After implementation:

1. `uv run pytest tests/smoke/test_app_boots.py` — confirms FastAPI lifespan still boots.
2. `POST /jobs/daily_cik_refresh/run` with `{"start_date": "2026-01-01"}` body — confirms params payload accepted, `job_runs.params_snapshot` reflects it.
3. With `bootstrap_state.status='partial_error'`: scheduled fire rejected; manual without override rejected; manual with `?override_bootstrap_gate=true` runs + writes audit row.
4. Trigger `cusip_universe_backfill` (sec_rate) and `daily_cik_refresh` (sec_rate) at the same time → second one waits.
5. Trigger `cusip_universe_backfill` (sec_rate) and `daily_candle_refresh` (etoro) at the same time → both run concurrently.
6. Bootstrap dispatcher fires stage 14, 15, 21 — all complete using the new params-dict path with no `bootstrap_*` wrapper files in the codebase.

## Risks + open questions

- [DECIDED] `Lane` type lives in new `app/jobs/sources.py` module — single source of truth, imported by both scheduler.py and bootstrap_orchestrator.py.
- [DECIDED — Codex WARNING] `_LANE_MAX_CONCURRENCY` vs source-keyed `JobLock`. The locked operator decision is unambiguous: same-source = serialised. Setting `_LANE_MAX_CONCURRENCY[db]=5` AND source-locking db at the cross-process layer creates a misleading dispatcher shape — dispatcher submits 5 db stages, 4 block on the source lock. PR1 retires `_LANE_MAX_CONCURRENCY` (or sets every lane to 1). The bootstrap dispatcher's parallel-DB-ingest claim from #1020 is dropped. **Tech-debt note in PR description: first-install bootstrap wall-clock regresses from "5 db stages parallel" → "1 db stage at a time" — measure on dev DB and file follow-up if operator-visible.**
- [DECIDED — Codex WARNING] Bespoke wrapper deletion repo-wide ref check (already grep'd 2026-05-09):
  - `app/jobs/runtime.py` — registers invokers; updated to register the new params-aware bodies.
  - `app/services/bootstrap_orchestrator.py` — defines them; bespoke functions deleted; bootstrap stage params dicts populated with the hardcoded values.
  - `app/services/sec_submissions_ingest.py` — likely just imports a constant; check + update.
  - `tests/test_bootstrap_orchestrator.py`, `tests/test_filings_form_allowlist.py`, `tests/test_jobs_runtime.py` — test rewires to the new invoker names.
  - `docs/specs/bootstrap/first-install.md`, `2026-05-08-admin-control-hub-rewrite.md`, `2026-05-08-bulk-datasets-first-bootstrap.md`, `2026-05-08-filing-allow-list-and-raw-retention.md`, `docs/wiki/runbooks/runbook-first-install-bootstrap.md` — narrative references; update to the new names where load-bearing, leave historical refs intact.
  - `docs/wiki/job-registry-audit.md` — produced by PR0, references the deletion targets; PR1 updates to reflect the merged state.
- [DECIDED] `display_name` field added to `ScheduledJob` in PR1 even though PR4 owns the FE rendering — avoids PR4 needing to edit every entry. PR1 populates strings from audit §2; PR4 wires them to the FE ⓘ component.
- [Q-LATER] `_LANE_MAX_CONCURRENCY` retirement: kept as historical comment block for one cycle, then removed in a follow-up. PR1 sets all values to 1 (no behaviour change) but keeps the map structure to minimise diff churn during the JobLock cutover.
