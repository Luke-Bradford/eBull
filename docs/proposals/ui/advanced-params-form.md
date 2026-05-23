# PR2 implementation plan — FE Advanced disclosure renderer

> Plan-stage doc for PR2 of the #1064 admin-control-hub follow-up sequence. Builds on PR1a/b/c.
> Mandatory Codex checkpoint 1 (spec review) before user sign-off.

## Goal

Render one form field per `ParamMetadata` entry on the operator manual-trigger UI so operators can run jobs with custom params without raw API POSTs. Today only `sec_13f_quarterly_sweep` declares `params_metadata`; PR2 wires the foundation so any future `ScheduledJob.params_metadata=(...)` declaration auto-renders.

## Out of scope

- Per-stage cancellation polish (PR7).
- ⓘ tooltip rendering (PR4 — separate field, PR1a populated).
- `cik_discovery.py` deletion (PR5).
- Pre-flight resource estimate ("this will hit SEC ~12,000 times").
- Promoting `filings_history_seed` / `sec_first_install_drain` / wider `sec_13f_quarterly_sweep` params to operator-exposable (`JOB_INTERNAL_KEYS`-only today; metadata declarations are a future PR).

## Operator-locked decisions

- **Trigger surface for params-aware run = `/jobs/<name>/run`**, NOT `/system/processes/<id>/trigger`. Reasons: (a) the params + control envelope is already validated server-side in PR1b-2; (b) iterate / full_wash semantics don't compose cleanly with operator-supplied params (full_wash resets watermark; operator-supplied `min_period_of_report` is orthogonal); (c) zero new BE API surface — only adds `params_metadata` to the read envelope.
- **UI placement = new "Advanced" drill-in tab.** Visible iff `mechanism === "scheduled_job"` AND `params_metadata.length > 0`. Bootstrap + ingest_sweep mechanisms never show the tab (they own no operator-exposable params). Form lives on its own tab so operators can read field help_text without modal pressure; iterate / full_wash buttons stay on the action bar. Today every declared `params_metadata` entry has `advanced_group=True`; PR2 renders the full set inside the Advanced tab regardless of the flag. Surfacing `advanced_group=False` as a primary always-visible field is a future PR — when the first such entry lands we revisit the IA.
- **Submit success = 202 toast with `request_id`.** Operator pivots to `/admin/jobs?request_id=N` (or the existing requests panel) to follow outcome. No inline polling on the form.
- **Empty / blank fields are omitted from the POST body**, NOT sent as empty strings. The manual `/jobs/{name}/run` path does NOT materialise registry defaults — `materialise_scheduled_params` is the scheduled-fire helper only — so an absent key falls through to the invoker's own `params.get(key, fallback)` default. Empty string would fail coercion (`int("")` raises). Bool fields always submit.
- **Field rendering matrix** (one row per `ParamFieldType`). Coercion column matches `_coerce_value` in `app/services/processes/param_metadata.py` so the FE produces a JSON value the validator round-trips:

  | field_type   | Input element                                      | FE-emitted JSON value | BE coercion target |
  |--------------|----------------------------------------------------|-----------------------|--------------------|
  | `string`     | `<input type="text">`                              | string                | `str(raw)`         |
  | `int`        | `<input type="number" step="1" min/max>`           | number                | `int(raw)`         |
  | `float`      | `<input type="number" step="any" min/max>`         | number                | `float(raw)`       |
  | `date`       | `<input type="date">` (HTML5 `YYYY-MM-DD`)         | ISO date string       | `date.fromisoformat(str(raw))` |
  | `quarter`    | `<input type="text" pattern="\d{4}Q[1-4]">`        | string `YYYYQN`       | upper-case parse   |
  | `ticker`     | `<input type="number">` (instrument_id integer)    | number                | `int(raw)` — same as `int` |
  | `cik`        | `<input type="text" pattern="\d{1,10}">`           | digit string          | `str.zfill(10)`    |
  | `bool`       | `<input type="checkbox">`                          | boolean               | bool / truthy-string parse |
  | `enum`       | `<select>` from `enum_values`                      | string member         | enum_values membership |
  | `multi_enum` | one `<input type="checkbox">` per enum value       | `string[]`            | enum_values membership per item |

  Two non-obvious entries: `ticker` IS `int(raw)` in `_coerce_value` (today PR1a wired the field type to instrument_id, not symbol resolution); the renderer asks for an integer with help_text guidance — symbol-to-id resolution is a future PR. `cik` returns a left-zero-padded digit string; render plain text and let BE pad.

  Non-finite numbers (`NaN`, `Infinity`) are blocked at submit (FE rejects with an inline error) — `JSON.stringify(NaN)` emits `null`, which then 400s downstream with a confusing message.

## Implementation sequence

### Step 1 — BE: surface `params_metadata` on `ProcessRow` + `ProcessRowResponse`

`app/services/processes/__init__.py::ProcessRow` gains:

```python
from app.services.processes.param_metadata import ParamMetadata  # avoids forward-reference NameError at module-load

@dataclass(frozen=True, slots=True)
class ProcessRow:
    ...
    params_metadata: tuple[ParamMetadata, ...] = ()
```

(Default empty so bootstrap + ingest_sweep adapters inherit no-op behaviour without code change. Import is concrete — `ParamMetadata` is a Pydantic model not a forward-ref string — so `from __future__ import annotations` does not save us if the import is missing.)

`app/services/processes/scheduled_adapter.py::_build_row` populates from `job.params_metadata`. Existing `ScheduledJob` registry entry is the single source of truth — no duplication.

`app/api/processes.py::ProcessRowResponse` gains:

```python
from pydantic import Field  # already imported in this module
from app.services.processes.param_metadata import ParamMetadata  # NEW import

class ProcessRowResponse(BaseModel):
    ...
    params_metadata: list[ParamMetadata] = Field(default_factory=list)
```

Pydantic v2 — use `Field(default_factory=list)` so the default is per-instance, not a shared mutable. Re-uses the existing Pydantic `ParamMetadata` model from `app/services/processes/param_metadata.py`. `_convert_row` translator copies tuple → list. Tests pin imports + Field-default round-trip.

### Step 1.5 — BE: JSON-safe coercion before queue publish

PR1b-2 leaks native `datetime.date` from `validate_job_params` into `Jsonb(payload)` at `app/api/jobs.py:246`. The first operator who submits a `date` field today crashes the publish path (`TypeError: Object of type date is not JSON serializable`). The bug is latent because no test exercises a date-typed `params_metadata` end-to-end through the API.

`_jsonable_params` already exists in `app/services/ops_monitor.py:40` and is used by `app/jobs/runtime.py:585` for `params_snapshot` writes. Move it to a shared module so it can be imported from `app/api/jobs.py` without dragging the `ops_monitor` blast radius:

- New `app/services/processes/json_safe.py` with `to_jsonsafe_params(params: Mapping[str, Any]) -> dict[str, Any]`. Same body as `_jsonable_params` (date / datetime → ISO string; list/tuple → list).
- `app/services/ops_monitor.py::_jsonable_params` becomes a thin re-export for back-compat (one cycle, then deletion in a follow-up PR).
- `app/jobs/runtime.py:583` and ops_monitor's two call sites switch to the new path.
- `app/api/jobs.py::run_job` calls `to_jsonsafe_params(validated_params)` before constructing `payload`.

Listener re-validates after dequeue (`app/jobs/listener.py:157`), so writing ISO strings here round-trips back to native `date` for the invoker.

### Step 2 — FE types mirror

`frontend/src/api/types.ts::ProcessRowResponse` gains:

```ts
params_metadata: ParamMetadata[];
```

The `ParamMetadata` interface already exists (PR1a). Round-trip is implicit via the existing `types.test.ts` covering one canonical job; extend that test to assert the field flows through `ProcessRowResponse`.

### Step 3 — FE: `apiFetch` 202-body fix + `runJob` envelope client

**`frontend/src/api/client.ts::apiFetch` — read body on 202.** Today the wrapper returns `undefined` for every 202 ("both have empty bodies in this codebase" comment is now wrong: PR1b-2 made `/jobs/{name}/run` return `JobRunQueuedResponse`). Fix:

```ts
if (res.status === 204) return undefined as T;
if (res.status === 202) {
  // PR1b-2 (#1064): /jobs/<name>/run returns {request_id} on 202.
  // Pre-PR1b-2 callers expected undefined; we preserve that for empty
  // bodies so existing call sites (auth flows etc.) stay typed correctly.
  const text = await res.text();
  if (!text) return undefined as T;
  try {
    return JSON.parse(text) as T;
  } catch {
    return undefined as T;
  }
}
```

Existing 204 contract unchanged. Existing 202 call sites that previously got `undefined` (e.g. `cancelProcess`'s pre-PR1 path) still get `undefined` because BE returns no body for them.

**`frontend/src/api/jobs.ts::runJob` widens to:**

```ts
export function runJob(
  jobName: string,
  body?: { params?: Record<string, unknown>; control?: { override_bootstrap_gate?: boolean } },
): Promise<{ request_id: number } | undefined> {
  return apiFetch<{ request_id: number } | undefined>(
    `/jobs/${encodeURIComponent(jobName)}/run`,
    {
      method: "POST",
      // Send a body only when the caller passed one. apiFetch sets
      // Content-Type: application/json automatically when init.body
      // is present (client.ts:60). Zero-arg calls keep the pre-PR2
      // shape: no body, BE _safe_read_json_body returns {}.
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    },
  );
}
```

Returns 202 body shape `{request_id: N}` when present, else `undefined` (defensive — apiFetch falls back if BE drops body). Existing zero-arg call sites unchanged.

### Step 4 — FE: `<AdvancedParamsForm metadata={...} onSubmit={...} busy={...} />`

New `frontend/src/components/admin/AdvancedParamsForm.tsx`. Pure presentational + local form state. Props:

```ts
interface AdvancedParamsFormProps {
  metadata: readonly ParamMetadata[];
  busy: boolean;
  onSubmit: (params: Record<string, unknown>) => Promise<void>;
}
```

Renderer dispatches on `field_type` per the matrix above. Local state per field; submit aggregates non-empty values into the params object. Validation is BE-only (FE submits, surfaces 400 detail string on failure).

### Step 5 — FE: Advanced tab in `ProcessDetailPage`

Tab key `"advanced"` added to `TabKey` union. `TabBar` renders the tab iff `row.mechanism === "scheduled_job"` AND `row.params_metadata.length > 0`. Tab body is the form + a status line for the last submit (`"queued as request #N"` / `"trigger rejected: <detail>"`).

Submit handler. Defensive on the `undefined` 202-body fallback — if BE drops the body the FE shows "queued" without an id rather than crashing on a destructure:

```ts
async function handleAdvancedSubmit(params: Record<string, unknown>) {
  setAdvancedError(null);
  setAdvancedRequestId(null);
  setBusy(true);
  try {
    const result = await runJob(id, { params });
    setAdvancedRequestId(result?.request_id ?? null);
    refetchAll();
  } catch (err) {
    setAdvancedError(err);
    if (!(err instanceof ApiError)) console.error("runJob failed", err);
  } finally {
    setBusy(false);
  }
}
```

`/jobs/<job_name>/run` is the right surface even when the operator clicked through `/admin/processes/<process_id>` because for `scheduled_job` mechanism `process_id === job_name` (see `_resolve_mechanism` + `target_job_name` in `app/api/processes.py::trigger_process`).

### Step 6 — Tests

| Test | Coverage |
|---|---|
| `tests/test_scheduled_adapter.py::test_params_metadata_surfaces` | `_build_row(job=sec_13f_quarterly_sweep)` returns `params_metadata == job.params_metadata` |
| `tests/test_processes_api.py::test_get_process_returns_params_metadata` | `GET /system/processes/sec_13f_quarterly_sweep` body includes the date-field metadata |
| `tests/test_processes_api.py::test_get_process_returns_empty_metadata_for_bootstrap` | bootstrap + ingest_sweep rows return `params_metadata=[]` |
| `tests/test_jobs_api_run_endpoint.py::test_date_param_round_trips_through_publish` | POST `/jobs/sec_13f_quarterly_sweep/run` with `{"params": {"min_period_of_report": "2024-01-01"}}` → 202 → `pending_job_requests.payload.params.min_period_of_report == "2024-01-01"` (string) — pin the JSON-safe coercion |
| `tests/test_processes_json_safe.py::test_to_jsonsafe_params` | `date`, `datetime`, `list`/`tuple` all coerce; existing `_jsonable_params` re-export still works (back-compat shim) |
| `frontend/src/api/client.test.ts::test_apiFetch_reads_202_body_when_present` | mocked 202 with JSON body returns parsed object; mocked 202 with empty body returns undefined |
| `frontend/src/components/admin/AdvancedParamsForm.test.tsx` | one render+submit case per `ParamFieldType` (10 fields); enum/multi_enum exhaustion of `enum_values`; bool default-false submits `false` not omitted; empty optional string omitted from submit; NaN/Infinity blocked at submit; `enum`/`multi_enum` with missing `enum_values` falls back to text input + emits a console warning (defensive — BE rejects but FE shouldn't crash); `cik` accepts unpadded digit string and lets BE pad |
| `frontend/src/pages/ProcessDetailPage.test.tsx::test_advanced_tab_visible_iff_metadata_present` | tab hidden for bootstrap row + scheduled row with empty metadata; visible for scheduled row with metadata |
| `frontend/src/pages/ProcessDetailPage.test.tsx::test_advanced_submit_calls_runJob` | mocked `runJob` invoked with `{params: {...}}` envelope; success surfaces `request_id` |
| `frontend/src/api/types.test.ts` | extend round-trip to assert `params_metadata` flows through `ProcessRowResponse` |

### Step 7 — Local gates + Codex pre-push (checkpoint 2)

- `unset VIRTUAL_ENV; uv run ruff check .`
- `uv run ruff format --check .`
- `uv run pyright`
- `uv run pytest` (full suite — adapter + API new tests)
- `pnpm --dir frontend typecheck`
- `pnpm --dir frontend test:unit`
- `codex.cmd exec review` on branch — fix real findings before push.

## Smoke verification

After implementation:

1. `uv run pytest tests/smoke/test_app_boots.py` — confirms FastAPI lifespan boots.
2. Open `/admin/processes/sec_13f_quarterly_sweep` → "Advanced" tab visible → date input rendered with help_text → submit `{min_period_of_report: "2024-01-01"}` → 202 → `pending_job_requests` row has `payload.params.min_period_of_report = "2024-01-01"` → listener dispatches → `job_runs.params_snapshot = {"min_period_of_report": "2024-01-01"}`.
3. Open `/admin/processes/bootstrap` → "Advanced" tab NOT visible.
4. Open `/admin/processes/daily_cik_refresh` (scheduled, empty metadata) → "Advanced" tab NOT visible.

## Risks + open questions

- [DECIDED] `/jobs/<name>/run` over `/system/processes/<id>/trigger` for params-aware path. Avoids extending the trigger envelope; keeps iterate / full_wash fence semantics decoupled.
- [DECIDED] `params_metadata` lives on `ProcessRow` (not on a separate endpoint). One round-trip per drill-in is fine; no caching layer needed.
- [DECIDED] No FE-side validation beyond HTML5 `pattern` / `min` / `max` hints. BE's `validate_job_params` is authoritative; FE just renders the 400 detail string. Avoids a dual-validator drift surface.
- [DECIDED] Empty fields omitted from submit. Treat absent → invoker's own `params.get(key, fallback)` default (the manual `/jobs/{name}/run` path does not run `materialise_scheduled_params`); explicit empty string would break coercion.
- [Q-LATER] Resource-estimate tooltip ("this will hit SEC ~12,000 times") deferred to a follow-up PR. Operator UX, not blocking.
- [Q-LATER] Operator-tunable `filings_history_seed` / `sec_first_install_drain` / `sec_13f_quarterly_sweep` (full set) — promoting from `JOB_INTERNAL_KEYS` to `params_metadata` is a future scoping decision per audit §6.

## Tech-debt to consider during PR2

- #1091 `cik_discovery` cleanup — orthogonal, not touched.
- #1092 bootstrap cancel mode hardcoded — orthogonal.
- #1093 Timeline cancelled rendering — orthogonal.
- #1094 GOOG/GOOGL share-class redirect — orthogonal.
