# Bootstrap precondition + final-data row-count gates

**Date:** 2026-05-13
**Issue:** [#1140](https://github.com/Luke-Bradford/eBull/issues/1140)
(Task C of [#1136](https://github.com/Luke-Bradford/eBull/issues/1136)
audit)
**Status:** Draft — pending Codex review

## 1. Problem

Task A (#1138) introduced the capability layer
(`_STAGE_REQUIRES_CAPS` / `_STAGE_PROVIDES` /
`_satisfied_capabilities`) but advertises a capability the moment the
provider stage's `bootstrap_stages.status` reaches `success`. The
provider's `rows_processed` is not consulted. Two consequences flagged
by the #1136 audit §2:

- **Fundamentals gate is status-only.** `fundamentals_sync` (S24)
  depends on `fundamentals_raw_seeded`, which `sec_companyfacts_ingest`
  (S9) advertises on `success`. S9 can finish `success` with zero
  `company_facts` rows ingested (empty CIK cohort, malformed archive
  decoded as zero JSON objects, mapping-table drift). S24 then runs
  and derives an empty fundamentals slice without any operator-visible
  signal that the upstream produced no data.
- **Ownership backfill gate is status-only.** Same shape for
  `ownership_observations_backfill` (S23) — each per-family cap
  (`insider_inputs_seeded`, `form3_inputs_seeded`,
  `institutional_inputs_seeded`, `nport_inputs_seeded`) is satisfied
  the moment any one provider hits `success`, even if that provider
  ingested zero rows.

The C1.b gate in `bootstrap_preconditions.py::assert_c1b_preconditions`
already checks `sec_submissions_ingest` wrote ≥ 1 row in the current
run (queries `bootstrap_archive_results.rows_written`). The same gate
is missing for the final-derivation stages.

Operator-side fallout: a bootstrap run can land in `complete` with
the entire fundamentals / ownership slice silently empty. Coverage
floors in `bootstrap_preconditions.py` are advisory (default
`min_ratio=0.0`); they log but never block. Audit §2 acceptance:

> Fundamentals + ownership final stages prove non-empty current-run
> inputs OR surface a warning/partial state.

> `complete with warnings` is operator-visible in process/timeline UI
> (admin control hub).

> Low coverage no longer masks an empty post-install state.

Secondary defect surfaced during pre-spec audit: the orchestrator's
`mark_stage_success` writes `bootstrap_stages.rows_processed`, but
`_run_one_stage` never passes a value — every stage row in
`bootstrap_stages` carries `rows_processed = NULL` regardless of
what the invoker actually did. The `processes/bootstrap_adapter.py`
aggregate at line 144 already `COALESCE(SUM(rows_processed), 0)`s
the column, so the operator panel reads zero motion even on a
healthy run. Populating the column is a structural prerequisite
for the cap-eval widening below.

## 2. Goal

Make the capability dispatcher prove that strict-gate caps (the four
per-family ownership caps + `fundamentals_raw_seeded`) carry a
non-zero `rows_processed` from at least one surviving provider
before advertising the cap. When a provider reaches `success` with
`rows_processed = 0`, treat that provider as "alive but
non-contributing" — the cap stays alive only if another provider can
still satisfy it; if not, downstream consumers block or cascade-skip
exactly the way Task A handles a dead-cap branch.

Populate `bootstrap_stages.rows_processed` for every stage so the
gate has real numbers to read.

Surface "complete with warnings" in the admin process/timeline UI
when at least one stage finished `success` but failed to satisfy a
strict-gate cap (third visual state between green and red).

## 3. Non-goals

- **New DB enum status** (`complete_with_warnings` on
  `bootstrap_state.status` or `bootstrap_runs.status`). The third
  state is a derived view on the API side; no schema migration. A
  future change can promote it to a first-class enum if the derived
  view proves load-bearing, but that's wider blast radius than this
  ticket needs.
- **Widening `JobInvoker` to return `int | None`.** The contract
  stays `Callable[[Mapping[str, Any]], None]`. The orchestrator
  populates `rows_processed` from existing side-channels (job_runs
  + bootstrap_archive_results), not from invoker return values.
- **Cap-level coverage-ratio floors** (e.g. "advertise
  `cik_mapping_ready` only if ≥ 80% of universe instruments are
  mapped"). The audit explicitly endorses `min_rows` as the knob —
  do not hardcode percentages. A future ticket can layer
  ratio-based gates on top once min_rows is in place.
- **Task A non-goals carry through**: capability provisioning on
  partial-success within a single stage; per-row-archive-level
  warnings; DB lane concurrency (Task E / #1141).
- **Reworking `bootstrap_preconditions.py`'s coverage floors** to be
  hard-blocking. They stay advisory; the row-count gate is the new
  hard signal.

## 4. Design

### 4.1 Per-cap minimum-rows knob

```python
# Strict-gate caps: the dispatcher requires the provider's rows_processed
# to meet this floor before advertising the cap. Caps absent from this
# map fall back to status-only gating (current Task A behaviour).
_CAPABILITY_MIN_ROWS: Final[dict[Capability, int]] = {
    # Audit §2 acceptance — fundamentals derivation must prove its
    # raw input was actually ingested.
    "fundamentals_raw_seeded": 1,
    # Audit §2 acceptance — ownership backfill must prove every
    # per-family input was actually ingested.
    "insider_inputs_seeded": 1,
    "institutional_inputs_seeded": 1,
    "nport_inputs_seeded": 1,
}
```

**Multi-cap provider exclusion** (Codex pre-push round 2 BLOCKING):
`sec_insider_ingest_from_dataset` is the only multi-cap provider in
`_STAGE_PROVIDES` — it advertises BOTH `insider_inputs_seeded` and
`form3_inputs_seeded` from a single aggregate `rows_processed`
(the bulk ingester maps rows to form3 vs form4 internally but
records the sum). A bulk wash that landed 10 Form 4 + 0 Form 3
rows would falsely advertise `form3_inputs_seeded` under the naive
strict-gate rule.

To preserve the strict gate for form3 AND avoid the false positive,
a parallel map `_STRICT_CAP_PROVIDER_EXCLUSIONS: dict[Capability,
frozenset[str]]` lists per-cap providers that CANNOT contribute to
the floor:

```python
_STRICT_CAP_PROVIDER_EXCLUSIONS: Final[dict[Capability, frozenset[str]]] = {
    "form3_inputs_seeded": frozenset({"sec_insider_ingest_from_dataset"}),
}
```

Semantics: the excluded provider is **neutral** for the strict cap.
Its `success` status doesn't satisfy the floor (rows can't be
trusted), but it also doesn't drive the cap's death-classification
(it's not a failed provider, just one whose row signal is ambiguous).
The cap stays alive iff another (non-excluded) provider satisfies
the floor — for form3 that's the legacy `sec_form3_ingest` single-cap
provider. If legacy succeeds with `rows_processed >= 1` the cap is
satisfied; if it succeeds with rows=0 the cap is dead (classified
error → consumer blocks). If only the bulk provider ran (legacy
skipped or absent), the cap is dead too — no surviving non-excluded
provider met the floor.

When per-family bulk row counts land (follow-up ticket — either
split the bulk-insider stage into two per-family stages OR write
per-cap rows via the existing `bootstrap_archive_results` per-archive
shape) the exclusion entry can be dropped and the bulk provider can
satisfy `form3_inputs_seeded` directly.

Default behaviour for unlisted caps is unchanged from Task A — the
cap is satisfied iff at least one provider hit `success`. Only the
five caps above pick up the new floor in v1. The blast radius is
deliberately small: changing the gate for a cap can mask or unmask
existing fallback paths, so each addition needs its own audit.

The threshold `1` is the cheapest non-trivial floor: any positive
write counts. A reviewer should not interpret `1` as "the system
needs exactly one row to be useful" — it means "if zero rows landed
we know something is wrong". Higher floors (e.g. `100` company_facts
rows) are a follow-up scope decision and explicitly out for v1.

### 4.2 Cap eval widening

Today `_satisfied_capabilities(statuses)` returns the set of caps
whose providers are in `success` (or `skipped` with a
`_STAGE_PROVIDES_ON_SKIP` entry). The new signature consults
`rows_processed`:

```python
def _satisfied_capabilities(
    statuses: Mapping[str, str],
    rows_processed: Mapping[str, int | None],
    *,
    provides: Mapping[str, tuple[Capability, ...]] = _STAGE_PROVIDES,
    provides_on_skip: Mapping[str, tuple[Capability, ...]] = _STAGE_PROVIDES_ON_SKIP,
    min_rows: Mapping[Capability, int] = _CAPABILITY_MIN_ROWS,
) -> set[Capability]:
    """Cap set derived from current stage statuses + per-stage rows.

    For a cap C with min_rows[C] = N:
      A provider P satisfies C iff statuses[P] == 'success' AND
      rows_processed.get(P) is not None AND rows_processed[P] >= N.

    For a cap C NOT in min_rows:
      A provider P satisfies C iff statuses[P] == 'success'.
      (Legacy Task A behaviour preserved.)

    `skipped` providers continue to satisfy C iff C is in
    `provides_on_skip[P]` — the skipped path is never row-counted
    (the slow-connection fallback explicitly doesn't write rows
    through the bulk stage).
    """
```

A provider that reached `success` but failed the rows floor for a
strict-gate cap does NOT contribute that cap. The provider stage's
status stays `success` on the DB; only the cap-eval layer treats
the provider as "non-contributing" for the strict cap.

`_capability_is_dead` then needs to know: for a strict-gate cap, a
provider in `success` with `rows_processed < min_rows` is **dead
for this cap** (cannot now or in future provide it — the stage is
already in a terminal state). The helper changes:

```python
def _capability_is_dead(
    cap: Capability,
    statuses: Mapping[str, str],
    rows_processed: Mapping[str, int | None],
    *,
    providers_map: Mapping[Capability, tuple[str, ...]] = _CAPABILITY_PROVIDERS,
    provides_on_skip: Mapping[str, tuple[Capability, ...]] = _STAGE_PROVIDES_ON_SKIP,
    min_rows: Mapping[Capability, int] = _CAPABILITY_MIN_ROWS,
) -> bool:
    ...
    floor = min_rows.get(cap)
    for provider_key in providers:
        status = statuses.get(provider_key)
        if status in ("pending", "running"):
            return False
        if status == "success":
            if floor is None:
                return False                     # legacy behaviour
            rows = rows_processed.get(provider_key)
            if rows is not None and rows >= floor:
                return False                     # provider satisfies the floor
            # rows < floor (or NULL) → this provider is dead for the
            # strict cap; keep checking the others.
            continue
        if status == "skipped":
            on_skip = provides_on_skip.get(provider_key, ())
            if cap in on_skip:
                return False
    return True
```

`_classify_dead_cap` change: a provider that's `success` but
under-floor for a strict cap classifies as `"error"` (not
`"skip_only"`). The intent matches operator expectation — the
provider ran and produced zero rows, which is a failure mode, not a
deliberate bypass. The dispatcher therefore **blocks** the
downstream consumer with a structured reason, not cascade-skips:

```text
blocked: missing capability fundamentals_raw_seeded; no surviving
provider met rows floor 1 (providers: sec_companyfacts_ingest=success
[rows_processed=0])
```

`_format_block_reason` widens to include `rows_processed=N` /
`rows_processed=NULL` annotations for `success` providers that
failed the floor; other states render as today.

### 4.3 Populating `bootstrap_stages.rows_processed`

The orchestrator's `_run_one_stage` already records a `__job__` row
in `bootstrap_archive_results` after a successful invoker exit, then
calls `mark_stage_success(conn, run_id=..., stage_key=...)`. Today
neither write supplies `rows_processed`.

Three sources of row counts exist in current code, each authoritative
for a different stage shape:

1. **`bootstrap_archive_results` non-`__job__` rows** — Phase C
   bulk wrappers in `app/services/sec_bulk_orchestrator_jobs.py`
   (e.g. `sec_companyfacts_ingest_job`) write one row per archive
   they processed. They do NOT use `_tracked_job` (Codex R1
   BLOCKING §1 — the original spec wording wrongly claimed they
   did), so this is their only signal.
2. **`bootstrap_archive_results` `__job__` row with operator-set
   `rows_written`** — service invokers like
   `app/services/sec_submissions_files_walk.py:179` overload the
   `__job__` provenance row to carry their real result count
   (`rows_written=result.filings_upserted`). The orchestrator's
   own auto-recorded `__job__` row defaults to `rows_written=0`
   (`bootstrap_orchestrator.py:820`); the current upsert in
   `record_archive_result` is last-write-wins, so the orchestrator's
   subsequent default-zero write clobbers the invoker's value —
   §4.3.1 introduces a `DO NOTHING` variant to fix this.
3. **`job_runs.row_count`** — `_tracked_job(job_name)` in
   `app/workers/scheduler.py` records this from the invoker's
   `tracker.row_count = N` side-effect. Every cap-providing
   scheduler invoker either already sets it (verified for
   `sec_form3_ingest`, `sec_n_port_ingest`,
   `sec_insider_transactions_backfill` per Codex R1 INFO §1) or
   has the data available.

Strategy:

- BEFORE acquiring `JobLock` in `_run_one_stage`, snapshot
  `SELECT COALESCE(MAX(id), 0) FROM job_runs WHERE job_name = %s`
  into a local `job_runs_id_before` variable.
- AFTER the invoker returns and BEFORE `JobLock` releases (still
  inside the `with JobLock(...)` block), snapshot the same query
  into `job_runs_id_after`. This second snapshot anchors the
  upper bound to "rows created while we held the lock" — without
  it, another scheduled fire of the same `job_name` could acquire
  the lock immediately after our release and insert a higher id
  before the resolver reads, polluting the `id DESC LIMIT 1`
  pick (Codex R2 BLOCKING §1).
- After `JobLock` releases (BEFORE the orchestrator's auto
  `__job__` write), call `_resolve_stage_rows` with the captured
  `(job_runs_id_before, job_runs_id_after)` window. The
  resolver's `job_runs` fallback restricts to
  `id > job_runs_id_before AND id <= job_runs_id_after`.
- The resolved integer (or `None` if no source has data) is
  passed to `mark_stage_success(rows_processed=N)` exactly as
  today.

```python
def _resolve_stage_rows(
    conn: psycopg.Connection[Any],
    *,
    bootstrap_run_id: int,
    stage_key: str,
    job_name: str,
    job_runs_id_before: int,
    job_runs_id_after: int,
) -> int | None:
    """Look up rows_processed from the three side-channels.

    Returns None when no source has a value (callers preserve
    legacy behaviour: cap-eval layer treats None as "below floor"
    for strict caps, satisfied via status alone for non-strict).

    Resolution order (first match wins):
      1. Per-archive bootstrap_archive_results (non-__job__).
         If COUNT > 0 → return SUM(rows_written), preserving 0.
         This is the Phase C bulk-wrapper shape. SUM=0 with real
         per-archive rows means "the C-stage ran every archive and
         every archive was empty" — that's a real signal, not
         absence of signal (Codex R1 BLOCKING §1).
      2. __job__ row with operator-set rows_written.
         If the __job__ row exists AND rows_written > 0 → return
         that integer. Covers the service-invoker shape (e.g.
         sec_submissions_files_walk overloads __job__ with its
         real result count; Codex R1 BLOCKING §2). The default
         orchestrator-written __job__ row carries rows_written=0
         which intentionally falls through to source 3 — we can't
         distinguish "service invoker explicitly set 0" from
         "orchestrator default 0" without a side-channel, so we
         prefer the job_runs fallback when the value is 0. In
         practice the only service invokers that write __job__
         set rows_written > 0 on success; if a future one needs to
         report 0 explicitly, add a service-invoker-side
         `_tracked_job` wrapper instead (which already does the
         right thing via source 3).
      3. job_runs.row_count from _tracked_job.
         SELECT row_count FROM job_runs WHERE job_name = %s AND
         id > job_runs_id_before AND id <= job_runs_id_after AND
         status = 'success' ORDER BY id DESC LIMIT 1. The
         double-bound `id > before AND id <= after` window pins the
         match to rows created while the dispatcher held the
         JobLock (the bounds are captured before lock acquisition
         and just before lock release — see strategy bullet 1+2
         above). Without the upper bound a same-job_name scheduled
         fire that landed after our release could pollute the
         pick (Codex R2 BLOCKING §1).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(rows_written), 0)
              FROM bootstrap_archive_results
             WHERE bootstrap_run_id = %s
               AND stage_key = %s
               AND archive_name <> '__job__'
            """,
            (bootstrap_run_id, stage_key),
        )
        row = cur.fetchone()
        archive_count = int(row[0]) if row else 0
        archive_sum = int(row[1]) if row and row[1] is not None else 0
        if archive_count > 0:
            return archive_sum

        cur.execute(
            """
            SELECT rows_written
              FROM bootstrap_archive_results
             WHERE bootstrap_run_id = %s
               AND stage_key = %s
               AND archive_name = '__job__'
            """,
            (bootstrap_run_id, stage_key),
        )
        row = cur.fetchone()
        if row is not None and row[0] is not None and int(row[0]) > 0:
            return int(row[0])

        cur.execute(
            """
            SELECT row_count
              FROM job_runs
             WHERE job_name = %s
               AND id > %s
               AND id <= %s
               AND status = 'success'
             ORDER BY id DESC
             LIMIT 1
            """,
            (job_name, job_runs_id_before, job_runs_id_after),
        )
        row = cur.fetchone()
        if row is not None and row[0] is not None:
            return int(row[0])
    return None
```

Total cost per stage: 1 capture (before lock) + 3 reads (after
invoker) + 1 already-existing UPDATE. No new indexes needed — every
predicate hits an existing PK / FK / job_name index.

#### 4.3.1 Why the resolver picks archive rows before `__job__`

A C-stage that processed K archives writes K rows in
`bootstrap_archive_results` AND the orchestrator auto-writes the
`__job__` provenance row at `_run_one_stage:813-828`. Both shapes
co-exist on the same `(bootstrap_run_id, stage_key)` key. The
resolver checks the per-archive rows first because their sum is the
real ingest count; the `__job__` row's `rows_written=0` is a
provenance marker, not data.

A C-stage where every archive landed empty (`rows_written=0` on
every per-archive row) has `archive_count > 0` AND `archive_sum =
0`. The resolver returns `0` — that's the correct semantic: "the
stage ran every archive and produced zero rows", distinguishable
from "the stage never wrote archive rows" (where `archive_count =
0` and we fall through to source 2 / 3). This is the Codex R1
BLOCKING §1 fix: pre-revision the spec returned `None` for a real
zero, hiding the warning case.

Service-invoker shape (source 2): `sec_submissions_files_walk` is
the current example. It calls `record_archive_result(..., archive_name="__job__",
rows_written=result.filings_upserted)` itself; the orchestrator's
own auto-write at `_run_one_stage:813-828` then runs and calls
`record_archive_result(..., archive_name="__job__", rows_written=0)`
with `ON CONFLICT (run_id, stage_key, archive_name) DO UPDATE SET
rows_written = EXCLUDED.rows_written`. The orchestrator's write
fires AFTER the invoker's, so the final `rows_written` is 0 again
— erasing the invoker's value.

**Fix (part of this PR):** add a new helper
`record_archive_result_if_absent(conn, *, bootstrap_run_id,
stage_key, archive_name, rows_written, rows_skipped=None)` in
`bootstrap_preconditions.py`. Same body as `record_archive_result`
but with `ON CONFLICT (bootstrap_run_id, stage_key, archive_name) DO
NOTHING`. The orchestrator's auto `__job__` write at
`_run_one_stage:813-828` switches to this helper. The existing
`record_archive_result` upsert helper stays unchanged — the
shared C-stage / retry path still upserts as before, which is the
correct semantic for per-archive retries (Codex R2 BLOCKING §2).

Existing tests that read `__job__.rows_written = 0` after a
vanilla B-stage continue to pass — the orchestrator still seeds
the row when the invoker hasn't already.

Audit step for the reviewer: grep for `record_archive_result(.*archive_name="__job__"`
to enumerate every invoker that writes a non-default `__job__` row.
Today the only one is `sec_submissions_files_walk`; if a future
invoker writes `__job__` with `rows_written=0` on purpose, the
resolver will fall through to `job_runs` — which is fine if the
invoker also uses `_tracked_job`, and surfaceable in test as a
NULL on the stage row otherwise.

#### 4.3.2 Per-stage cap-providing inventory + row source

For each cap-providing stage (the keys in `_STAGE_PROVIDES`), the
resolved row count comes from:

| Stage | Source | Already populated? |
|---|---|---|
| `universe_sync` | `job_runs.row_count` via `_universe_sync_job` tracker | yes (scheduler.py:1427) |
| `cusip_universe_backfill` | `job_runs.row_count` | yes |
| `cik_refresh` | `job_runs.row_count` via `_daily_cik_refresh` | yes (scheduler.py:1714) |
| `sec_bulk_download` | `bootstrap_archive_results` (per-archive landed) | yes |
| `sec_submissions_ingest` | `bootstrap_archive_results` | yes |
| `sec_companyfacts_ingest` | `bootstrap_archive_results` | yes |
| `sec_insider_ingest_from_dataset` | `bootstrap_archive_results` | yes |
| `sec_13f_ingest_from_dataset` | `bootstrap_archive_results` | yes |
| `sec_nport_ingest_from_dataset` | `bootstrap_archive_results` | yes |
| `sec_submissions_files_walk` | `__job__.rows_written` (source 2) | yes — `sec_submissions_files_walk.py:228` writes `result.filings_upserted` |
| `filings_history_seed` | `job_runs.row_count` | yes |
| `sec_first_install_drain` | `job_runs.row_count` | yes |
| `sec_insider_transactions_backfill` | `job_runs.row_count` | yes — `scheduler.py:4709` |
| `sec_form3_ingest` | `job_runs.row_count` | yes — `scheduler.py:3935` (Codex R1 INFO §1) |
| `sec_13f_recent_sweep` | `job_runs.row_count` | yes |
| `sec_n_port_ingest` | `job_runs.row_count` | yes — `scheduler.py:4566` |

Every cap-providing stage has at least one populated source. The
strict-gate caps (`fundamentals_raw_seeded` +
`insider_inputs_seeded` / `form3_inputs_seeded` /
`institutional_inputs_seeded` / `nport_inputs_seeded`) all map onto
stages whose row source is already populated, so the cap-eval
widening in §4.1-§4.2 has real numbers to gate on from PR merge
forward. The submissions walker (source 2) is non-strict today
(its cap `submissions_secondary_pages_walked` doesn't appear in
`_CAPABILITY_MIN_ROWS`); its `rows_processed` plumbing is for
operator-panel visibility only.

### 4.4 Dispatcher integration

`_phase_batched_dispatch` already plumbs `statuses` through
`_satisfied_capabilities` / `_capability_is_dead` /
`_classify_requirement_unsatisfiable`. Add a parallel
`rows_processed: dict[str, int | None]` dict alongside, populated
each iteration by reading from `bootstrap_stages` along with the
status refresh.

The dispatcher currently re-reads stage statuses via the in-process
`statuses` dict (mutated as stages complete). The new code also
updates `rows_processed[stage_key]` from the outcome of
`_run_one_stage` — `_StageOutcome` widens to carry the resolved
`rows_processed: int | None` so the dispatcher doesn't need a
round-trip to the DB.

```python
@dataclass(frozen=True)
class _StageOutcome:
    stage_key: str
    success: bool
    error: str | None
    skipped: bool = False
    cancelled: bool = False
    rows_processed: int | None = None  # NEW
```

For preexisting terminal stages (from a `retry-failed` pass), the
existing snapshot load at `run_bootstrap_orchestrator` already
reads each stage's row from `bootstrap_stages`; widen the projection
to include `rows_processed` and seed it into the dispatcher's dict.

### 4.5 "Complete with warnings" UI surface

API-side derivation, no DB enum.

Add a `warning: str | None` field to
`BootstrapTimelineStageResponse` (one per stage) and a
`has_warnings: bool` field to `BootstrapTimelineRunResponse`. Both
are derived from the timeline query:

- `warning` is set on a stage row iff `status == 'success'` AND the
  stage is a cap provider AND `rows_processed` is `0` or `NULL` AND
  at least one cap the stage provides is in `_CAPABILITY_MIN_ROWS`
  (i.e. strict-gate). Value is a short operator-readable string:
  `"stage succeeded but wrote 0 rows; downstream caps cannot be satisfied"`.
- `has_warnings` is `True` iff any stage in the run has `warning !=
  None`.

The frontend (`ProcessDetailPage.tsx` timeline tab + the
`processes` table row for `bootstrap`) reads `warning` and renders
an amber chip next to the stage's success tick. The processes table
checks `has_warnings` and renders an amber dot next to the
`complete` status text when set. Existing `STATUS_VISUAL` mapping
in `frontend/src/components/admin/processStatus.ts` gains a new
key `success_warning` (style: amber, hover tooltip = the stage's
`warning` text).

Pre-existing behaviour for `complete` / `partial_error` /
`cancelled` is unchanged. A run that finishes `complete` AND
`has_warnings == true` renders as `complete` with the amber dot —
operator sees both "the run finished" and "but something inside
needs attention". A run that finishes `partial_error` already
renders red; the amber dot doesn't add information there and is
suppressed (the red signal is louder).

### 4.6 `processes/bootstrap_adapter.py` aggregate fixup

`bootstrap_adapter.py:144` currently does
`COALESCE(SUM(rows_processed), 0) AS rows_processed`. With this
spec the column gets real numbers, so the aggregate starts reading
non-zero on the operator panel — desired side effect (the panel
has been showing zero motion for every bootstrap run since the
24-stage rewrite).

No change to the adapter SQL or shape. Verify the operator panel
renders the aggregate correctly post-fix via the dev-DB smoke step
in §6.

## 5. Tests

### 5.1 Unit (`tests/test_bootstrap_orchestrator.py`)

Add four tests covering the cap-eval rule changes. All use the
existing fixture mechanism with synthetic stage_keys and synthetic
caps registered via `provides_map` / `provides_on_skip_map`
overrides on `_phase_batched_dispatch`. A new override parameter
`min_rows_map` plumbs the per-cap floors into the dispatcher for
fixture-only caps; production code uses the module-level map.

1. **`test_strict_cap_blocks_consumer_on_zero_rows`** — Provider
   stage reaches `success` with `rows_processed=0`. Consumer with
   a strict-gate cap requirement transitions to `blocked` with a
   reason naming `rows_processed=0` and the cap. Run finalises as
   `partial_error`.

2. **`test_strict_cap_satisfied_by_one_of_two_providers`** — Cap
   has two providers; one reaches `success` with `rows_processed=0`
   (under floor), the other reaches `success` with
   `rows_processed=5` (above floor). Consumer runs. Run finalises
   as `complete`. Smokes the per-family ownership cap shape (bulk
   ingester landed zero, legacy ingester landed rows).

3. **`test_strict_cap_dead_on_zero_rows_classifies_error_not_skip`**
   — Single-provider strict cap; provider reaches `success` with
   `rows_processed=0`. Assert the dispatcher transitions the
   consumer to `blocked` (not `skipped`). Distinct from Task A's
   cascade-skip path, which only fires for `skipped` providers.

4. **`test_non_strict_cap_unchanged_by_zero_rows`** — A cap NOT in
   `_CAPABILITY_MIN_ROWS` is satisfied by a `success` provider
   with `rows_processed=0` (legacy Task A behaviour preserved).
   Asserts the new rule doesn't accidentally widen to caps it
   shouldn't touch — guards against the blast-radius concern in
   §4.1.

Catalogue invariant test addition:

5. **`test_min_rows_caps_have_at_least_one_provider`** — every key
   in `_CAPABILITY_MIN_ROWS` is also a key in
   `_CAPABILITY_PROVIDERS` (i.e. has at least one provider stage).
   Catches a stale entry that names a removed cap.

### 5.2 Real-DB integration (`tests/test_bootstrap_atomic_enqueue.py`
or new file `tests/test_bootstrap_rows_processed_gates.py`)

Uses `ebull_test_conn` per `feedback_test_db_isolation`.

1. **`test_resolver_archive_sum_wins_when_count_positive`** —
   seed `bootstrap_runs` + `bootstrap_stages` rows for
   `sec_companyfacts_ingest`. Insert two
   `bootstrap_archive_results` rows: (`__job__`, `rows_written=0`)
   and (`companyfacts.zip`, `rows_written=42`). Call
   `_resolve_stage_rows(...)` and assert the return is `42`.

2. **`test_resolver_archive_sum_zero_preserved`** (Codex R1
   BLOCKING §1 regression) — same setup but the per-archive row
   carries `rows_written=0`. `__job__` row also `rows_written=0`.
   Call `_resolve_stage_rows` and assert the return is `0` (NOT
   `None` — `archive_count > 0` short-circuits before sources 2
   and 3, preserving the real-zero signal that the C-stage ran
   every archive and produced no rows).

3. **`test_resolver_uses_job_row_when_set_above_zero`** (Codex
   R1 BLOCKING §2 regression) — service-invoker shape: insert
   only the `__job__` row with `rows_written=7` (mirrors
   `sec_submissions_files_walk` overloading the provenance row).
   No per-archive rows. Call `_resolve_stage_rows` and assert the
   return is `7`.

4. **`test_resolver_job_runs_window_excludes_outside_ids`**
   (Codex R2 BLOCKING §1 regression) — no archive rows at all.
   Insert THREE `job_runs` rows for the same `job_name`,
   `status='success'`: one with `id < before` (`row_count=1`,
   should be excluded), one with `id` in `(before, after]`
   (`row_count=2`, the target), and one with `id > after`
   (`row_count=3`, simulates a parallel scheduled fire after
   JobLock release, should be excluded). Call
   `_resolve_stage_rows(..., job_runs_id_before=before,
   job_runs_id_after=after)` and assert the return is `2`.

5. **`test_resolver_falls_back_to_job_runs_when_no_archive_rows`**
   — no archive rows; one `job_runs` row inside the window with
   `row_count=1500`. Call `_resolve_stage_rows` and assert the
   return is `1500`. Negative variant: no archive rows AND no
   `job_runs` row → return is `None`.

6. **`test_orchestrator_job_row_preserves_invoker_value`** —
   mirror the real write order. First call the EXISTING upsert
   helper `record_archive_result(... archive_name="__job__",
   rows_written=99)` to simulate the service invoker (e.g.
   `sec_submissions_files_walk`). Then call the NEW
   `record_archive_result_if_absent(... rows_written=0)` to
   simulate the orchestrator's default auto-write. Re-read the
   row and assert `rows_written` is still `99` — the orchestrator's
   `DO NOTHING` write didn't overwrite the invoker's value (Codex
   R2 BLOCKING §2 + R3 WARNING). Control: call
   `record_archive_result(... rows_written=0)` (the existing
   upsert helper) against the same row and assert the value flips
   to `0` (upsert semantics unchanged by this PR).

7. **`test_fundamentals_blocked_when_companyfacts_wrote_zero`** —
   bootstrap end-to-end via fake invokers. Configure
   `sec_companyfacts_ingest` fake to land `success` with zero
   rows (its archive result row carries `rows_written=0`, no
   fallback `job_runs.row_count`). Assert `fundamentals_sync`
   transitions to `blocked` with a reason naming
   `fundamentals_raw_seeded` + `rows_processed=0`. Assert the
   timeline endpoint returns `warning` populated on the
   `sec_companyfacts_ingest` row and `has_warnings=True` on the
   run payload. Bootstrap finalises as `partial_error`.

8. **`test_ownership_backfill_blocked_when_all_families_wrote_zero`**
   — every per-family ownership provider (bulk + legacy) lands
   `success` with `rows_processed=0`. Assert
   `ownership_observations_backfill` transitions to `blocked` with
   a reason naming the first-encountered dead family cap. Run
   finalises as `partial_error`.

9. **`test_ownership_backfill_runs_when_one_family_legacy_recovers`**
   — bulk ownership ingester for the institutional family lands
   `success` with `rows_processed=0`; legacy `sec_13f_recent_sweep`
   lands `success` with `rows_processed=120`. Assert
   `ownership_observations_backfill` runs to `success`. (Combined
   with Task A's per-family cap shape — this is the regression
   gate that the row-count widening doesn't break the bulk-OR-
   legacy recovery story.)

10. **`test_timeline_has_warnings_derived_from_stage_rows`** —
    seed a finished `complete` run where one stage has
    `rows_processed=0` and is a strict-cap provider (e.g.
    `sec_companyfacts_ingest`). Call
    `GET /processes/bootstrap/timeline` and assert
    `response['run']['has_warnings'] == True` and
    `response['stages'][i]['warning']` is the documented
    operator-readable string. A control stage with
    `rows_processed=42` has `warning == None`.

### 5.3 Frontend (`frontend/src/pages/ProcessDetailPage.test.tsx`)

1. **bootstrap-timeline renders warning chip on success+zero-rows
   stage** — mock the timeline fetch to return a stage with
   `status='success'`, `warning='stage succeeded but wrote 0 rows…'`.
   Assert the amber chip + tooltip text render alongside the
   success tick.
2. **processes table renders amber dot on complete run with
   has_warnings** — mock `fetchProcess('bootstrap')` to return
   `complete` with the derived `has_warnings=true` shape. Assert
   the amber dot is in the DOM and the status word is still
   `complete` (not `partial_error`).

## 6. Migration / rollout

- **No schema change.** `bootstrap_stages.rows_processed` is an
  existing column.
- **No data migration.** Pre-fix runs with `rows_processed=NULL`
  remain unchanged; the cap-eval layer treats NULL as "below
  floor" for strict caps. On the next bootstrap run after merge,
  the new dispatcher populates real values.
- **Backwards-compatible.** Existing scheduler jobs that set
  `tracker.row_count` continue to do so unchanged. Existing
  `bootstrap_archive_results` writes are unchanged.
- **Backfill of `rows_processed` on already-completed runs is
  out of scope.** The operator sees `0` on the bootstrap_adapter
  aggregate for old runs (same as today's display); new runs
  start populating real values immediately.

Manual verification on dev DB:

1. Start dev stack.
2. `curl -X POST :8000/system/bootstrap/run` — wait for run to
   reach a terminal state (the dev stack already has a recent
   complete run; this re-runs).
3. `psql -c "SELECT stage_key, rows_processed FROM bootstrap_stages
   WHERE bootstrap_run_id = (SELECT MAX(id) FROM bootstrap_runs)
   ORDER BY stage_order"` — every stage that ran shows a non-NULL
   `rows_processed` (some will be 0 by nature; cap-providing
   strict-gate stages should be > 0 for the panel of 3-5 known
   instruments AAPL/GME/MSFT/JPM/HD per CLAUDE.md).
4. Hit `GET /processes/bootstrap/timeline` — assert the JSON
   payload carries `rows_processed` on each stage AND
   `has_warnings=False` (healthy run).
5. Open the admin control hub and the bootstrap timeline drill-in;
   confirm the row-count aggregate is non-zero (was always 0 on
   the panel pre-fix; this is the regression gate).
6. To exercise the warning path on dev: manually
   `UPDATE bootstrap_stages SET rows_processed = 0 WHERE
   stage_key = 'sec_companyfacts_ingest' AND bootstrap_run_id =
   <id>`, then `GET /processes/bootstrap/timeline` and confirm
   `warning` is set on that stage and `has_warnings=true` on the
   run. Then revert the UPDATE.

## 7. Acceptance criteria (from #1140 / #1136 audit §2)

- [x] `fundamentals_sync` cannot proceed when `sec_companyfacts_ingest`
  wrote zero rows. Test 7 in §5.2.
- [x] `ownership_observations_backfill` cannot proceed when all four
  per-family caps lack a row-producing provider. Test 8 in §5.2.
- [x] Per-family bulk-OR-legacy recovery still works when at least one
  provider produced rows. Test 9 in §5.2.
- [x] Operator-visible "complete with warnings" surfaces on the admin
  process timeline + processes table when a strict-cap provider
  finished `success` with `rows_processed=0` AND downstream cap
  consumers recovered via another provider. Test 10 in §5.2 + §5.3.
- [x] `rows_processed` is populated for every stage after this PR
  lands — operator panel aggregate stops reading zero.
- [x] `min_rows` knob is configurable per cap (no hardcoded
  percentages or absolute counts inside the eval rule). §4.1.

## 8. Pre-flight review focus

- **NULL vs 0 semantics** at the cap layer. `rows_processed = NULL`
  must classify as "below floor" for strict-gate caps; otherwise
  fundamentals_sync would silently pass when
  `sec_companyfacts_ingest` doesn't write its row count for any
  reason. §4.2 encodes this; reviewers should sanity-check it
  matches the test expectations.
- **`archive_count > 0` short-circuit** in `_resolve_stage_rows`
  (§4.3.1). A C-stage where every archive landed `rows_written=0`
  returns `archive_sum=0` (NOT `None`) — the resolver preserves the
  real-zero signal that the C-stage ran every archive and produced
  no rows. Test 2 in §5.2 is the regression gate. Pre-revision
  drafts fell through to `job_runs` here, which would hide the
  zero on the strict-cap warning surface.
- **`job_runs` window scope** in `_resolve_stage_rows` (§4.3).
  Two snapshots: `job_runs_id_before` captured before
  `JobLock` acquisition, `job_runs_id_after` captured inside the
  `with JobLock(...)` block AFTER the invoker returns. The
  resolver uses `id > before AND id <= after`. Without the upper
  bound a same-`job_name` scheduled fire that landed after lock
  release could pollute the pick. Test 4 in §5.2 is the regression
  gate.
- **Orchestrator `__job__` write uses `record_archive_result_if_absent`**
  (§4.3.1). The shared upsert helper `record_archive_result` is
  unchanged — Phase C ingesters and retries still upsert as
  before. Only the orchestrator's default-zero provenance write
  uses the new `DO NOTHING` helper so a service invoker like
  `sec_submissions_files_walk` that already wrote `__job__` with a
  real count isn't overwritten by the default `rows_written=0`.
  Test 6 in §5.2 is the regression gate.
- **Per-family ownership rollout** is the highest-blast-radius
  change. The four caps are strict; if any one cap has no
  surviving provider with non-zero rows, the backfill blocks. A
  reviewer should confirm the per-family caps map correctly to
  the per-family row counts (e.g. `insider_inputs_seeded` rows
  come from the insider ingester, not from the 13F ingester).
- **Block reason wording**: the structured reason includes
  `rows_processed=N` — that string lands in
  `bootstrap_stages.last_error` (capped at 1000 chars) and
  surfaces directly to the operator timeline. Phrasing should be
  unambiguous in the partial-recovery case (one provider passed
  the floor, others didn't, cap is alive — no block message
  should fire in that case at all, but if it did the wording
  needs to make clear which provider was the dead one).
- **Frontend `success_warning` STATUS_VISUAL entry**: confirm
  the amber palette matches existing dark-mode + the
  class-hygiene gate at `frontend/scripts/check-dark-classes.mjs`.
  Add a STATUS_VISUAL test if the existing
  `processStatus.test.ts` covers the warning case.

## 9. References

- [#1136](https://github.com/Luke-Bradford/eBull/issues/1136) §2 + §4 — original audit
- [#1140](https://github.com/Luke-Bradford/eBull/issues/1140) — sub-ticket
- [#1138](https://github.com/Luke-Bradford/eBull/issues/1138) — Task A capability layer (prerequisite)
- `docs/superpowers/specs/2026-05-13-bootstrap-capability-layer.md` — Task A spec
- `app/services/bootstrap_orchestrator.py:254-340` — current `_STAGE_PROVIDES` / `_STAGE_REQUIRES_CAPS` / `_satisfied_capabilities`
- `app/services/bootstrap_orchestrator.py:704-833` — current `_run_one_stage`
- `app/services/bootstrap_preconditions.py:294-513` — existing precondition gates (unchanged by this spec)
- `app/services/processes/bootstrap_adapter.py:144` — pre-existing rows_processed aggregate (consumer of the populated column)
- `app/api/processes.py:636-765` — timeline endpoint (warning + has_warnings derivation lives here)
- `frontend/src/components/admin/processStatus.ts` — `STATUS_VISUAL` map (new `success_warning` key)
