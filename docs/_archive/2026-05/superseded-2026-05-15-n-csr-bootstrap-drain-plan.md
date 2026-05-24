# N-CSR/S fund-scoped bootstrap drain — implementation plan

> Status: **DRAFT 2026-05-15** — pending Codex pre-spec 1b + operator signoff.
>
> Spec: `docs/superpowers/specs/2026-05-15-n-csr-bootstrap-drain.md` (signed off 2026-05-15; Codex 1a CLEAN round 7).
> Issue: **#1174**. Branch: `feature/1174-n-csr-bootstrap-drain`.
> Output preference (CLAUDE.md): schema → service logic → tests → integration glue. No schema in this PR.

## 1. Task decomposition

| # | Task | Scope (files) | Depends on | Deliverable |
|---|---|---|---|---|
| T1 | Trust-CIK iterator + horizon helper | `app/jobs/sec_first_install_drain.py` (new private helpers) | — | `_iter_trust_ciks(conn)` + `_within_horizon(filed_at, cutoff)` predicates with unit-callable shape |
| T2 | Per-trust enqueue + secondary-page walk | `app/jobs/sec_first_install_drain.py` (new private helper `_enqueue_n_csr_for_trust`) | T1 | Walks primary `submissions.json` + secondary `files[]` pages with `source='sec_n_csr'` filter + horizon row filter + `subject_type='institutional_filer'` enqueue |
| T3 | `bootstrap_n_csr_drain` public function + stats dataclass | `app/jobs/sec_first_install_drain.py` (new `NCsrDrainStats` + `bootstrap_n_csr_drain`) | T1 + T2 | Public function with cancel-cooperative polling + manual-trigger row-count guard at entry |
| T4 | Bootstrap orchestrator wiring | `app/services/bootstrap_orchestrator.py` (capability literal, 2 stage specs S25 + S26, 1 `_STAGE_PROVIDES` entry for S25, 2 `_STAGE_REQUIRES_CAPS` entries, 2 job-name constants) | T3 | Stages declared in catalogue; capability gating wired |
| T5 | Scheduler invokers | `app/workers/scheduler.py` (new `mf_directory_sync(params)` + `sec_n_csr_bootstrap_drain(params)` wrappers) | T4 | Wrappers open provider + conn, dispatch to underlying functions; tracker row_count recorded |
| T6 | Invoker registration | `app/jobs/runtime.py` (add 2 entries to `_INVOKERS` dict) | T5 | Dispatcher can route both job-name constants to their wrappers |
| T7 | Tests | `tests/test_sec_first_install_drain.py` (extend), `tests/test_bootstrap_orchestrator.py` (extend for catalogue invariants + cap test) | T1-T6 | All 13 cases enumerated in spec §6 pass under `uv run pytest -n0` |
| T8 | Docs — etl-endpoint-coverage row 47 restate | `.claude/skills/data-engineer/etl-endpoint-coverage.md` (row 47 `sec_n_csr` line) | T3 + T4 | Row reflects "real parser + dedicated bootstrap drain landed (#1174)" |

**Total tasks: 8.** Dispatch order:

```
T1 (helpers) → T2 (enqueue + pagination) → T3 (public function)
                                              └── T4 (orchestrator wiring) → T5 (scheduler wrappers) → T6 (invoker registration) → T7 (tests, FULL: covers T1-T6 wiring) → T8 (docs)
```

Codex 1b WARNING: T7 depends on T1+T2+T3+T4+T5+T6 because the orchestrator-stage tests reach into the catalogue + the runtime invoker tests reach into `_INVOKERS` + the wrapper-effect tests need the scheduler functions registered. Tests cannot land incrementally per-task without stubbing pieces that don't yet exist; serial execution is the natural shape.

## 2. Per-task contracts

### T1 — Trust-CIK iterator + horizon helper

**File:** `app/jobs/sec_first_install_drain.py`.

**Add private helpers:**

```python
def _iter_trust_ciks(conn: psycopg.Connection[Any]) -> Iterable[str]:
    """Yield distinct trust_cik values from cik_refresh_mf_directory in
    deterministic order. Crash-resume + tests rely on the order; the
    manifest UPSERT idempotency carries actual safety."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT trust_cik
            FROM cik_refresh_mf_directory
            WHERE trust_cik IS NOT NULL
            ORDER BY trust_cik
            """
        )
        for (cik,) in cur.fetchall():
            yield str(cik)


def _within_horizon(filed_at: datetime, cutoff: datetime) -> bool:
    """True iff filed_at >= cutoff. Both must be tz-aware UTC."""
    return filed_at >= cutoff
```

**Test (spec §6 case 1 dependency)** — verified inline via test case 1 (no dedicated test).

### T2 — Per-trust enqueue + secondary-page walk

**File:** `app/jobs/sec_first_install_drain.py`.

**Add private helper:**

```python
@dataclass(frozen=True)
class _TrustDrainOutcome:
    rows_upserted: int
    accessions_outside_horizon: int
    secondary_pages_fetched: int
    skipped: bool  # True on 404 / fetch error (counted separately by caller)
    errored: bool


def _enqueue_n_csr_for_trust(
    conn: psycopg.Connection[Any],
    *,
    http_get: HttpGet,
    trust_cik: str,
    cutoff: datetime,
) -> _TrustDrainOutcome:
    """Fetch the trust's submissions.json (primary + secondary pages),
    filter to source='sec_n_csr' rows within horizon, enqueue manifest
    rows with subject_type='institutional_filer' + subject_id=trust_cik
    + instrument_id=None.

    Returns _TrustDrainOutcome counters. Internal exceptions are
    caught + recorded (errored=True); 404 → skipped=True; the caller
    aggregates into NCsrDrainStats.
    """
```

**Implementation steps:**

1. Call `check_freshness(http_get, cik=trust_cik, last_known_filing_id=None, sources={'sec_n_csr'})`. On `RuntimeError` (non-200 / non-404 status): return `errored=True`.
2. If returned `FreshnessDelta` is empty AND `delta.has_more_in_files=False`: return `skipped=True` (covers 404 path which returns empty delta + no pagination).
3. For each `row` in `delta.new_filings`:
   - If `row.source != 'sec_n_csr'`: defensive skip (already filtered by `check_freshness`).
   - If NOT `_within_horizon(row.filed_at, cutoff)`: bump `accessions_outside_horizon` + continue.
   - Call `record_manifest_entry(conn, row.accession_number, cik=row.cik, form=row.form, source='sec_n_csr', subject_type='institutional_filer', subject_id=trust_cik, instrument_id=None, filed_at=row.filed_at, accepted_at=row.accepted_at, primary_document_url=row.primary_document_url, is_amendment=row.is_amendment)`. On `ValueError`: log warning + continue (mirrors existing `seed_manifest_from_filing_events` shape).
   - Bump `rows_upserted` on success.
4. If `delta.has_more_in_files and delta.files_pages`:
   - For each page name in `delta.files_pages`:
     - `page_url = f"https://data.sec.gov/submissions/{name}"`; `http_get(page_url, _drain_headers())`.
     - On non-200: log warning + continue (existing pattern; no error bubbling).
     - Parse via `parse_submissions_page(body, cik=trust_cik_padded)` — returns `(rows, _has_more)`.
     - Apply EXPLICIT `source == 'sec_n_csr'` filter on every row (Codex 1a WARNING — the helper writes every mapped source; we filter at our call site).
     - Apply horizon filter row-by-row (Codex 1a WARNING — full secondary walk, row-filtered).
     - For matching + in-horizon rows: call `record_manifest_entry` with the same trust-scoped subject identity.
     - Bump `secondary_pages_fetched += 1`.

**Headers helper:** reuse the existing `_drain_headers()` at `sec_first_install_drain.py:503` (User-Agent + Accept-Encoding).

**Why not reuse `_drain_secondary_pages` directly:** that helper takes a `ResolvedSubject` + writes every mapped source unconditionally. The trust-scoped enqueue needs (a) `source='sec_n_csr'` filter, (b) horizon filter, (c) hardcoded `institutional_filer + trust_cik + None` subject identity. The cleanest shape is a small dedicated loop body inside `_enqueue_n_csr_for_trust`; ~25 lines, no DRY violation worth a refactor.

### T3 — `bootstrap_n_csr_drain` public function + stats dataclass

**File:** `app/jobs/sec_first_install_drain.py`.

**Public surface:**

```python
@dataclass(frozen=True)
class NCsrDrainStats:
    trusts_processed: int
    trusts_skipped: int           # 404 / empty submissions / non-200 with no rows
    secondary_pages_fetched: int
    manifest_rows_upserted: int
    accessions_outside_horizon: int
    errors: int


_N_CSR_DRAIN_CANCEL_POLL_EVERY_N = 50


def bootstrap_n_csr_drain(
    conn: psycopg.Connection[Any],
    *,
    http_get: HttpGet,
    horizon_days: int = 730,
) -> NCsrDrainStats:
    """Walk fund-trust CIKs from cik_refresh_mf_directory + enqueue
    last-`horizon_days` N-CSR + N-CSRS accessions per trust to
    sec_filing_manifest.

    Pre-condition: ``class_id_mapping_ready`` capability (S25
    ``mf_directory_sync`` populates ``cik_refresh_mf_directory``).
    A manual-trigger entry-check raises ``BootstrapPhaseSkipped`` if
    ``cik_refresh_mf_directory`` is empty (e.g. operator runs the
    stage before S25 has ever fired).

    Cancel-cooperative: polls ``bootstrap_cancel_requested()`` every
    ``_N_CSR_DRAIN_CANCEL_POLL_EVERY_N`` trusts; raises
    ``BootstrapStageCancelled`` on observed cancel.
    """
```

**Implementation steps:**

1. **Entry guard** (spec §3.4): `SELECT COUNT(*) FROM cik_refresh_mf_directory`. If 0 → raise `BootstrapPhaseSkipped("class_id_mapping_ready unsatisfied — cik_refresh_mf_directory empty")`. No HTTP calls.
2. Compute `cutoff = datetime.now(UTC) - timedelta(days=horizon_days)`.
3. Initialize counters: `trusts_processed=0`, `trusts_skipped=0`, `secondary_pages_fetched=0`, `manifest_rows_upserted=0`, `accessions_outside_horizon=0`, `errors=0`.
4. For each `trust_cik` in `_iter_trust_ciks(conn)`:
   - Every `_N_CSR_DRAIN_CANCEL_POLL_EVERY_N` iterations (i.e. `n % N == 0`), check `bootstrap_cancel_requested()`. If True → raise `BootstrapStageCancelled(...stage_key=active_bootstrap_stage_key() or "")`.
   - Call `_enqueue_n_csr_for_trust(conn, http_get=http_get, trust_cik=trust_cik, cutoff=cutoff)`.
   - Aggregate: `trusts_processed += 1`. If `outcome.skipped`: `trusts_skipped += 1`. If `outcome.errored`: `errors += 1`. Else accumulate the rest of the counters.
5. Log summary line `bootstrap_n_csr_drain: trusts=%d skipped=%d errors=%d secondary_pages=%d upserted=%d outside_horizon=%d`.
6. Return `NCsrDrainStats(...)`.

**Imports** (top of file): `from app.services.bootstrap_preconditions import BootstrapPhaseSkipped` — confirmed location (`app/services/bootstrap_preconditions.py:60`); same import path used by `app/services/sec_bulk_download.py:51`.

### T4 — Bootstrap orchestrator wiring

**File:** `app/services/bootstrap_orchestrator.py`.

**Edits:**

1. Add `class_id_mapping_ready` to the `Capability = Literal[...]` union (currently 11 caps; this is the 12th).
2. Add job-name constants near the existing `JOB_DAILY_CIK_REFRESH` (line 111):
   ```python
   JOB_MF_DIRECTORY_SYNC = "mf_directory_sync"
   JOB_SEC_N_CSR_BOOTSTRAP_DRAIN = "sec_n_csr_bootstrap_drain"
   ```
3. Add to `_STAGE_PROVIDES`:
   ```python
   "mf_directory_sync": ("class_id_mapping_ready",),
   ```
   No entry for `sec_n_csr_bootstrap_drain` (terminal stage).
4. Add to `_STAGE_REQUIRES_CAPS`:
   ```python
   "mf_directory_sync": CapRequirement(all_of=("universe_seeded",)),
   "sec_n_csr_bootstrap_drain": CapRequirement(all_of=("class_id_mapping_ready",)),
   ```
5. Append to `_BOOTSTRAP_STAGE_SPECS` after `_spec("fundamentals_sync", 24, "db", "fundamentals_sync")`:
   ```python
   _spec("mf_directory_sync", 25, "sec_rate", JOB_MF_DIRECTORY_SYNC),
   _spec(
       "sec_n_csr_bootstrap_drain",
       26,
       "sec_rate",
       JOB_SEC_N_CSR_BOOTSTRAP_DRAIN,
       params={"horizon_days": 730},
   ),
   ```
6. **Bump stage-count assertion + dependent references** (Codex 1b BLOCKING):
   - `app/services/bootstrap_orchestrator.py:1795` — change `assert len(...) == 24` → `== 26`. Update both the message strings (`expected 24`, `current 24-stage shape` comment block at `:1791-1794`) and the module docstring at `:3` ("the 24-stage end-to-end first-install backfill").
   - `tests/test_bootstrap_orchestrator.py:109-114, 122, 334-335` — every `== 24` / `24 = 1 init + ...` math comment needs updating to 26 (1 init + 1 etoro + the existing breakdown + 2 new sec_rate stages). The "All 24 invokers called" assertion at `:334-335` becomes `== 26` and the `_patch_invokers_with_fakes` fake set must include the two new job-name strings.
   - `docs/wiki/runbooks/runbook-first-install-bootstrap.md` + `docs/wiki/job-registry-audit.md` — restate "24 stages" → "26 stages" with a one-line note "+ S25 mf_directory_sync (#1174) + S26 sec_n_csr_bootstrap_drain (#1174)".
7. Add `JOB_MF_DIRECTORY_SYNC` + `JOB_SEC_N_CSR_BOOTSTRAP_DRAIN` to the module-level `__all__` if present (verify in implementation phase).

### T5 — Scheduler invokers

**File:** `app/workers/scheduler.py`.

**Add two new functions, modelled on the existing `sec_first_install_drain` wrapper at `:4275`:**

```python
def mf_directory_sync(params: Mapping[str, Any]) -> None:
    """``_INVOKERS['mf_directory_sync']`` — dedicated bootstrap-side
    MF directory refresh (#1174).

    Calls the existing ``refresh_mf_directory`` in a fresh provider +
    conn context. No fail-soft: if the fetch fails, the stage fails,
    the ``class_id_mapping_ready`` capability is not advertised, and
    the dependent S26 (``sec_n_csr_bootstrap_drain``) transitions to
    ``blocked``.

    The daily cron ``daily_cik_refresh`` keeps its bundled
    ``refresh_mf_directory`` call (with fail-soft preserved) as the
    drift-heal safety net — this dedicated stage is the bootstrap-side
    truthful capability provider.

    No operator-tweakable params.
    """
    with _tracked_job(JOB_MF_DIRECTORY_SYNC) as tracker:
        with (
            SecFilingsProvider(user_agent=settings.sec_user_agent) as provider,
            psycopg.connect(settings.database_url) as conn,
        ):
            result = refresh_mf_directory(conn, provider=provider)
        tracker.row_count = result["directory_rows"]
        logger.info(
            "mf_directory_sync: fetched=%s directory_rows=%s ext_id_rows=%s",
            result["fetched"],
            result["directory_rows"],
            result["external_identifier_rows"],
        )


def sec_n_csr_bootstrap_drain(params: Mapping[str, Any]) -> None:
    """``_INVOKERS['sec_n_csr_bootstrap_drain']`` — fund-scoped manifest
    bootstrap drain for N-CSR / N-CSRS (#1174 / T8 deferred from #1171).

    Walks ``cik_refresh_mf_directory`` for distinct trust CIKs +
    enqueues last-``horizon_days`` (default 730) N-CSR + N-CSRS
    accessions per trust to ``sec_filing_manifest``. Manifest worker
    drains via the #1171 fund-metadata parser.

    Honoured params:

    * ``horizon_days`` (int) — retention window in days. Default 730
      (matches ``filings_history_seed.days_back``).

    Internal invariants (NOT operator-exposed): ``http_get`` adapter
    closure via ``_make_sec_http_get`` mirrors the existing
    ``sec_first_install_drain`` shape.
    """
    from app.jobs.sec_first_install_drain import bootstrap_n_csr_drain

    horizon_days_param = params.get("horizon_days", 730)
    horizon_days = int(horizon_days_param)

    with _tracked_job(JOB_SEC_N_CSR_BOOTSTRAP_DRAIN) as tracker:
        with (
            SecFilingsProvider(user_agent=settings.sec_user_agent) as sec,
            psycopg.connect(settings.database_url) as conn,
        ):
            stats = bootstrap_n_csr_drain(
                conn,
                http_get=_make_sec_http_get(sec),  # type: ignore[arg-type]
                horizon_days=horizon_days,
            )
        tracker.row_count = stats.manifest_rows_upserted
        logger.info(
            "sec_n_csr_bootstrap_drain: trusts=%d skipped=%d manifest_rows=%d "
            "errors=%d secondary_pages=%d outside_horizon=%d horizon_days=%d",
            stats.trusts_processed,
            stats.trusts_skipped,
            stats.manifest_rows_upserted,
            stats.errors,
            stats.secondary_pages_fetched,
            stats.accessions_outside_horizon,
            horizon_days,
        )
```

**Constants:** add `JOB_MF_DIRECTORY_SYNC` + `JOB_SEC_N_CSR_BOOTSTRAP_DRAIN` near the existing `JOB_SEC_FIRST_INSTALL_DRAIN` at `:296` to keep the constant cluster local. Imports: reuse existing `refresh_mf_directory` at `:51`.

### T6 — Invoker registration

**File:** `app/jobs/runtime.py`.

**Add to the body of the file BEFORE `VALID_JOB_NAMES = frozenset(_INVOKERS.keys())` at `:342`** (Codex 1b round-2 WARNING — registrations must land before the frozenset is derived, otherwise the new job-name constants won't appear in `VALID_JOB_NAMES` and the listener's dispatch boundary at `app/jobs/listener.py:114` will reject them). Place alongside the existing `_INVOKERS[_scheduler.JOB_SEC_FIRST_INSTALL_DRAIN] = _scheduler.sec_first_install_drain` cluster at `:292`:

```python
from app.workers.scheduler import (  # noqa: E402
    mf_directory_sync as _mf_directory_sync,
    sec_n_csr_bootstrap_drain as _sec_n_csr_bootstrap_drain,
)
from app.services.bootstrap_orchestrator import (  # noqa: E402
    JOB_MF_DIRECTORY_SYNC,
    JOB_SEC_N_CSR_BOOTSTRAP_DRAIN,
)

_INVOKERS[JOB_MF_DIRECTORY_SYNC] = _mf_directory_sync
_INVOKERS[JOB_SEC_N_CSR_BOOTSTRAP_DRAIN] = _sec_n_csr_bootstrap_drain
```

Both wrappers take `params: Mapping[str, Any]` — no `_adapt_zero_arg` needed.

### T7 — Tests

**File:** `tests/test_sec_first_install_drain.py` (extend; do not overwrite).

Implements spec §6 cases 1-11 (cases 12-13 land in the new `tests/test_mf_directory_sync_wrapper.py` defined below).

**Fixtures needed:**

- `tests/fixtures/sec_submissions/trust_n_csr_primary.json` — golden primary submissions.json with 5 N-CSR/N-CSRS + 5 non-N-CSR filings within horizon (case 1).
- `tests/fixtures/sec_submissions/trust_n_csr_horizon_overflow.json` — same plus 1 N-CSR with `filingDate` 800d ago (case 3).
- `tests/fixtures/sec_submissions/trust_n_csr_with_files.json` — primary with `filings.files[]` carrying 2 secondary-page names (case 6).
- `tests/fixtures/sec_submissions/trust_n_csr_secondary_page_1.json` — 5 N-CSR rows in horizon (case 6).
- `tests/fixtures/sec_submissions/trust_n_csr_secondary_page_2.json` — 3 N-CSR + 2 N-PORT all >730d old (case 6).
- `tests/fixtures/sec_submissions/trust_n_csr_secondary_mixed.json` — N-CSR + 10-K + 13F-HR rows (case 7).

**Test-case-to-fixture mapping** (spec §6):

| Case | Fixture | Notes |
|---|---|---|
| 1 first-run writes | `trust_n_csr_primary.json` | Fake http_get keyed by CIK → fixture body. Seed `cik_refresh_mf_directory` with one row. |
| 2 idempotent | `trust_n_csr_primary.json` | Run twice. Assert no `ingest_status` flip back to `pending` for any row that was transitioned by a parallel test. |
| 3 horizon | `trust_n_csr_horizon_overflow.json` | Assert `accessions_outside_horizon == 1`. |
| 4 CHECK | `trust_n_csr_primary.json` | Direct SELECT — assert `subject_type='institutional_filer' AND instrument_id IS NULL`. |
| 5 cancel | Seed 75 trusts in `cik_refresh_mf_directory` (sufficient to exercise the 50-trust poll bound mid-run). Monkeypatch `bootstrap_cancel_requested` to start returning `True` AFTER N internal calls so the cancel observes on the second poll cycle. Fake `http_get` returns a minimal empty `submissions.json` body for each trust so the drain advances. | `pytest.raises(BootstrapStageCancelled)`. Assert http_get was invoked > 1 time (proving mid-run cancel cadence, not pre-fetch). Assert http_get invocations ≤ `_N_CSR_DRAIN_CANCEL_POLL_EVERY_N + 1` (proving the bounded poll cadence — drain didn't run all 75 before observing cancel). |
| 6 pagination + horizon | `trust_n_csr_with_files.json` + `trust_n_csr_secondary_page_1.json` + `trust_n_csr_secondary_page_2.json` | Assert `secondary_pages_fetched == 2`, `manifest_rows_upserted == 5`, no N-PORT rows. |
| 7 source filter | `trust_n_csr_secondary_mixed.json` | Mixed forms in secondary; assert only `sec_n_csr` rows enqueued. |
| 8 404 | n/a (fake http_get returns `(404, b"")`) | `trusts_skipped == 1`. |
| 9 fetch exception | n/a (fake http_get raises) | `errors == 1`. Drain continues to next trust (test with 2 trusts). |
| 10 empty cohort | n/a (no DB seed) | `pytest.raises(BootstrapPhaseSkipped)`. No HTTP. |
| 11 freshness side-effect | `trust_n_csr_primary.json` | SELECT from `data_freshness_index` for `(institutional_filer, trust_cik, sec_n_csr)` — exactly 1 row. |

**Test infra:** reuse the existing `ebull_test_conn` fixture + `monkeypatch` for `bootstrap_cancel_requested`. http_get is a closure `def fake_http_get(url, headers): return _FIXTURE_RESPONSES[url]` populated per test.

**Prevention-log applicability** (spec §10):
- #1290 catalogue-resolved indirection: cancel test (case 5) uses `_N_CSR_DRAIN_CANCEL_POLL_EVERY_N` constant directly (not catalogue-resolved) because that's an internal implementation constant, not a job_name string. Test stays robust if the constant moves; no indirection needed.
- #1296 CheckViolation transaction abort: case 4 reads after a successful UPSERT; no malformed-INSERT in the test path.

**File:** `tests/test_mf_directory_sync_wrapper.py` (new — Codex 1b BLOCKING — invoker effect-tests).

Spec §6 cases 12-13 require exercising the actual `mf_directory_sync` wrapper at `app/workers/scheduler.py`, not just the capability helpers. The wrapper-effect tests verify (a) the no-fail-soft contract on the bootstrap path, (b) the tracker.row_count is recorded from `result["directory_rows"]`, and (c) downstream capability gating reflects the stage outcome.

| Case | What to test |
|---|---|
| 12 `mf_directory_sync` success: writes rows + records tracker + advertises cap | Monkeypatch `SecFilingsProvider.fetch_document_text` to return a golden mf.json with 5 trusts. Call `mf_directory_sync({})`. After: assert `cik_refresh_mf_directory` has 5 rows; assert the tracker row for `JOB_MF_DIRECTORY_SYNC` recorded `row_count=5`; assert `_satisfied_capabilities({'mf_directory_sync': 'success'}, {'mf_directory_sync': 5})` includes `class_id_mapping_ready`. |
| 13 `mf_directory_sync` failure: NO fail-soft + cap not advertised + T8 blocks | Monkeypatch `SecFilingsProvider.fetch_document_text` to raise `RuntimeError("simulated SEC outage")`. Call `mf_directory_sync({})`. Assert `pytest.raises(RuntimeError)` propagates (no fail-soft swallow on the bootstrap-stage path). Assert `_classify_dead_cap('class_id_mapping_ready', {'mf_directory_sync': 'error'}, {})` returns the discriminant indicating downstream stages transition to `blocked` (per orchestrator semantics — provider error → blocked, not skipped). |

**File:** `tests/test_bootstrap_orchestrator.py` (extend with catalogue-invariant + cancel tests; stage-count assertions are updated in T4 — see step 6 — not added new).

Additional catalogue test additions:
- Every stage in `_BOOTSTRAP_STAGE_SPECS` appears in `_STAGE_REQUIRES_CAPS` (existing invariant; the catalogue-invariant test runs against the extended catalogue automatically once T4 lands).
- `_patch_invokers_with_fakes` fake set (used across the file's dispatcher tests) must include `'mf_directory_sync'` + `'sec_n_csr_bootstrap_drain'` per [[1290-catalogue-resolved-failing-jobs]]. The mapping `{spec.stage_key: spec.job_name for spec in get_bootstrap_stage_specs()}` lookup pattern automatically covers the new stages once T4 lands; verify in implementation phase that no test hardcodes a 24-element job-name list that would silently no-op on the new stages.

**File:** `tests/test_jobs_runtime.py` (**extend** — Codex 1b WARNING + round-2 NIT — file already exists at `tests/test_jobs_runtime.py` per `ls`; add new test functions for the two new invokers, do NOT create a new file).

| Case | What to test |
|---|---|
| R1 | `_INVOKERS[JOB_MF_DIRECTORY_SYNC]` is callable + matches `mf_directory_sync` from `app.workers.scheduler`. |
| R2 | `_INVOKERS[JOB_SEC_N_CSR_BOOTSTRAP_DRAIN]` is callable + matches `sec_n_csr_bootstrap_drain` from `app.workers.scheduler`. |
| R3 | Both job-name constants appear in `VALID_JOB_NAMES` (the `frozenset(_INVOKERS.keys())` derived at runtime registration). |
| R4 | Listener round-trip: `app.jobs.listener.dispatch_job_name` (or equivalent boundary helper) routes `JOB_MF_DIRECTORY_SYNC` / `JOB_SEC_N_CSR_BOOTSTRAP_DRAIN` strings to the registered invoker (mock the invoker body; assert the dispatcher reaches it). |

File exists (`tests/test_jobs_runtime.py`, 44 KB per `ls`). Pattern reference: any existing test that imports `from app.jobs.runtime import _INVOKERS, VALID_JOB_NAMES` and asserts a subset. Add the R1-R4 functions adjacent to the existing coverage; do NOT create a parallel file.

**Allow-list update** (Codex 1b round 4 BLOCKING): `tests/test_fetch_document_text_callers.py:31` `_ALLOWED_CALLER_FILES` frozenset must include `tests/test_mf_directory_sync_wrapper.py` if that new test file monkeypatches `SecFilingsProvider.fetch_document_text` to inject a fake mf.json body. The sentinel test scans test files for any reference to the symbol and fails on unlisted callers. Add the new test file to the frozenset. The wrapper itself at `app/workers/scheduler.py` does NOT call `fetch_document_text` directly (it delegates to `refresh_mf_directory`, which is already in the allowed app-code set), so the app-code allow-list section is unchanged.

### T8 — Docs — etl-endpoint-coverage row 47

**File:** `.claude/skills/data-engineer/etl-endpoint-coverage.md`.

**Edit row 47** (the `sec_n_csr` line). Current text:

> ✅ `sec_n_csr.py` (#1171, 2026-05-15, replaced #918 / PR #1170 synth no-op) | **WIRED** — real fund-metadata parser. [...] classId → instrument_id via `external_identifiers (provider='sec', identifier_type='class_id')` populated by bundled `company_tickers_mf.json` ingest (Stage 6 extension). [...]

**Restate**: replace "(Stage 6 extension)" with "(dedicated S25 `mf_directory_sync` bootstrap stage; #1174)". Append a sentence: "Bootstrap drain at S26 `sec_n_csr_bootstrap_drain` (#1174) walks distinct trust CIKs from `cik_refresh_mf_directory` and enqueues last-2-years N-CSR + N-CSRS accessions for the manifest worker to drain."

## 3. Pre-push gate checklist

Per `.claude/CLAUDE.md` + memory `[[checklist-pre-push]]`:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest -n0 tests/test_sec_first_install_drain.py \
                  tests/test_bootstrap_orchestrator.py \
                  tests/test_mf_directory_sync_wrapper.py \
                  tests/test_mf_directory.py \
                  tests/test_jobs_runtime.py \
                  tests/test_fetch_document_text_callers.py \
                  tests/smoke/test_app_boots.py
```

All four (lint, format, pyright, pytest scoped) must pass. Smoke test catches lifespan issues from the new invoker registrations.

If impacted-files-clean, optional full `uv run pytest` with xdist for last-mile parity (per pre-push hook).

ETL DoD clauses 8-12 require the PR description to record observed operator-visible figures. PR body MUST embed this table (filled at smoke time):

| Instrument | Trust CIK | Expected outcome | Observed result | manifest rows (post-drain) | fund_metadata_current populated? | ER (operator-visible) | NAV | Commit SHA |
|---|---|---|---|---|---|---|---|---|
| VFIAX | 0000036405 | drain → parse → ER ≈ 0.04% | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ |
| VOO | 0000036405 | already populated; idempotent | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ |
| IVV | 0001100663 | drain → parse → ER ≈ 0.03% | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ |
| AGG | 0001100663 | drain → parse → credit_quality populated | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ |
| FXAIX | 0000819118 | drain → parse → ER ≈ 0.015% (TSR coverage held) OR tombstone with explanatory log | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ | _(fill)_ |

Cross-source verification (DoD clause 9):

| Instrument | Field | Parser value | Independent source | Source value | Delta | Acceptable? |
|---|---|---|---|---|---|---|
| VFIAX | expense_ratio_pct | _(fill)_ | Vanguard factsheet `investor.vanguard.com/vfiax` | _(fill)_ | _(fill)_ | ±0% exact |
| VFIAX | net_assets_amt | _(fill)_ | Vanguard factsheet | _(fill)_ | _(fill)_ | ±1% (period vs publish snapshot) |

Backfill record (DoD clause 10):

- Invocation: dispatch S25 `mf_directory_sync` followed by S26 `sec_n_csr_bootstrap_drain` on dev DB at commit `<sha>`. Manifest worker drain timing recorded.
- Observation count after drain: `<N>` rows in `fund_metadata_observations` where `known_to IS NULL`.
- `fund_metadata_current` row count: `<N>`.

Operator-visible figure verification (DoD clause 11) — confirmed against the smoke panel above.

PR description (DoD clause 12) embeds all three tables/blocks above plus the commit SHA at each verification step.

## 4. Codex pre-push 2

After self-review + local gates pass:

```bash
codex exec --output-last-message /tmp/codex_1174_2.txt \
  "Review the branch feature/1174-n-csr-bootstrap-drain against /Users/lukebradford/Dev/eBull/docs/superpowers/specs/2026-05-15-n-csr-bootstrap-drain.md and /Users/lukebradford/Dev/eBull/docs/superpowers/specs/2026-05-15-n-csr-bootstrap-drain-plan.md. Focus on: subject-type wiring at every record_manifest_entry call site; horizon row-filter correctness on both primary + secondary pages; source filter correctness on secondary pages; cancel-cooperative pattern fidelity; new orchestrator capability + stage spec correctness (no co-declaration; truthful provider); test coverage gaps (13 cases); record_manifest_entry idempotency claims still hold; review-prevention-log applicability; ETL DoD clauses 8-12 completeness in PR body. Reply terse — BLOCKING/WARNING/NIT only." < /dev/null
```

Fix all BLOCKING + WARNING before pushing. NIT triaged: fix-now if small + coupled, else file tech-debt issue.

## 5. PR description outline

Per memory `[[feedback_pr_description_brevity]]`:

### What

- New bootstrap stage S25 `mf_directory_sync` — dedicated MF directory refresh (no fail-soft on bootstrap path; daily cron retains fail-soft).
- New bootstrap stage S26 `sec_n_csr_bootstrap_drain` — walks distinct trust CIKs from `cik_refresh_mf_directory` + enqueues last-2-years N-CSR + N-CSRS accessions for the #1171 manifest-worker parser.
- New capability `class_id_mapping_ready` (S25 provides; S26 requires).
- Closes #1174 (T8 deferred from #1171).

### Why

- #1171 landed the real fund-metadata parser; without this drain stage, the parser has zero work for the ~600 distinct trust CIKs catalogued in `cik_refresh_mf_directory`.
- Dedicated bootstrap stage (instead of bundled with Stage 6) keeps capability advertisement truthful — operator pushback during spec phase: source updates daily, staleness thresholds are a gappy patch; clean separation is structurally correct.

### Test plan

- [ ] Local gates: ruff check, format check, pyright, pytest scoped (5 test files).
- [ ] Smoke panel: VFIAX / VOO / IVV / AGG / FXAIX — dispatch S25 + S26 on dev DB, verify drain + parse.
- [ ] Cross-source verify VFIAX expense ratio vs Vanguard factsheet.
- [ ] Verify `fund_metadata_current` populates with operator-visible ER + NAV (record commit SHA).

### Settled-decisions

- PRESERVES #1171's source-priority chain.
- ADDS: new capability `class_id_mapping_ready`. No existing capability semantics change.

## 6. Risks

- **Wall-clock 5-15 min on full universe** — bootstrap is once-per-install; acceptable.
- **Layer 2 daily-index reconciler not yet firing in prod (Lane B blocker)** — orthogonal; T8 is the bootstrap path, not the steady-state path.
- **Trust CIK that's in directory but missing from EDGAR** — `check_freshness` 404 handling at `sec_submissions.py:246`; counted as `trusts_skipped`.
- **`instrument_not_in_universe` flood from non-universe trusts** — parser tombstones cleanly (#1171 behaviour); operator coverage signal.

## 7. Dispatch matrix

```
T1 (helpers) → T2 (enqueue) → T3 (public function) → T4 (orchestrator wiring) → T5 (scheduler wrappers) → T6 (invoker reg) → T7 (tests; FULL coverage of T1-T6 wiring) → T8 (docs)
```

Single-author serial execution: T1 → T2 → T3 → T4 → T5 → T6 → T7 → T8 → self-review → Codex 2 → push.

## 8. Out of scope (file follow-ups)

- Stage 6 `daily_cik_refresh` 304/hash-skip relocation (the cron-side bug). Spec §3.4 declares OUT OF SCOPE; tech-debt ticket if it surfaces operationally.
- Layer 2 daily-index reconciler verification (Lane B).
- `data_freshness_index` seeding cadence verification per source (Lane C).
- Universe expansion (#841 CUSIP + ETF). Today 436 classes resolve; that's coverage breadth, not parser correctness.

## 9. Open questions

None — `BootstrapPhaseSkipped` confirmed at `app/services/bootstrap_preconditions.py:60`. Import as:

```python
from app.services.bootstrap_preconditions import BootstrapPhaseSkipped
```

Same import the existing `sec_bulk_download.py` uses.

## 10. Sign-off

- Spec: `docs/superpowers/specs/2026-05-15-n-csr-bootstrap-drain.md` — operator-signed 2026-05-15.
- Codex 1a: round 7 CLEAN (cached at `/tmp/codex_1174_1a_round7.txt`).
- Codex 1b round 1: 2 BLOCKING (stage-count assert; cases 12-13 only test capability helpers) + 4 WARNING (T7 dep arrows; BootstrapPhaseSkipped import path; cancel case shape; missing runtime-registration test). All addressed:
  - BLOCKING (stage count) → T4 step 6 adds explicit assertion + comment + docstring + tests + wiki bumps.
  - BLOCKING (cases 12-13) → new `tests/test_mf_directory_sync_wrapper.py` exercises the actual invoker with fake provider effects (no fail-soft + tracker.row_count + cap propagation).
  - WARNING (T7 deps) → §1 dispatch matrix corrected to serial T1→T6→T7.
  - WARNING (BootstrapPhaseSkipped path) → T3 import line corrected to `app.services.bootstrap_preconditions`.
  - WARNING (cancel case) → §T7 case 5 rewritten to seed 75 trusts + mid-run cancel poll.
  - WARNING (runtime registration test) → new `tests/test_jobs_runtime.py` covering R1-R4.
- Codex 1b round 2: 3 WARNING (§7 arrows duplicate, T7 intro stale cases 12-13 location, T6 placement before VALID_JOB_NAMES + extend-don't-create test_jobs_runtime). Addressed.
- Codex 1b round 3: 1 WARNING (§T7 line 387 stale 'does NOT exist'). Addressed.
- Codex 1b round 4: 1 BLOCKING (allow-list edit needed for new test file). Addressed by documenting at §T7 line 389.
- Codex 1b round 5: false-positive "BLOCKING" (asking for repo edit at plan phase). Clarified in round 6.
- Codex 1b round 6: **CLEAN** (cached at `/tmp/codex_1174_1b_round6.txt`).
- Operator signoff on this plan: pending.
