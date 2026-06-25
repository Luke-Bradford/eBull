# Parallel filer ingest — share one SEC budget across N filer pipelines (#1274)

## Problem

`institutional_holdings.ingest_all_active_filers` and
`n_port_ingest.ingest_all_fund_filers` loop filer CIKs **serially**
(`for cik in ciks:` → full fetch→parse→write per filer before the next
starts). `sec_pipelined_fetcher`'s 4-way concurrency only parallelises
*within* one filer's archive list. Measured (stage 21, 2026-05-22): 3.9
filers/min, ~5-10% of the SEC 600 req/min budget. 9,148-filer cohort
drains in ~3-4 h.

## Source rule (governing invariant)

The SEC 10 req/s limit is a **shared per-IP budget**, enforced
process-globally and **thread-safely** by the in-process rate gate — NOT
per-client:

- `app/providers/rate_gate.py::InProcessFloorGate._reserve()` advances
  `_next_free_at` under a `threading.Lock`, then sleeps *outside* the lock
  (GCRA reservation). N threads sharing one gate get one globally-enforced
  9.09 req/s floor (`SEC_MIN_REQUEST_INTERVAL_S = 0.11`).
- The gate is a process-global singleton via
  `sec_rate_gate_holder.get_sec_rate_gate()` (never value-imported).
- `sec_edgar.py` adds the legacy companion `_PROCESS_RATE_LIMIT_CLOCK:
  list[float]` (#537) + `_PROCESS_RATE_LIMIT_LOCK: threading.Lock` (#726),
  shared by every `ResilientClient` so the throttle's read-modify-write is
  atomic across concurrent fetchers.
- Lineage: the `sec_rate` Lane was *dissolved* to this in-process gate —
  "the HTTP clock is the real rate gate" (#1526/#1542).

**Consequence (Codex ckpt-1 refinement):** the rate-budget boundary is the
**process-global gate + legacy clock**, NOT the provider-instance count —
every SEC HTTP call routes through `get_sec_rate_gate()` +
`_PROCESS_RATE_LIMIT_LOCK` regardless of how many providers exist. Sharing
the single `sec` passed in is therefore *sufficient* (and simplest); we do
not construct per-worker providers. `httpx.Client` is documented
thread-safe for concurrent requests (internal pool locking), so sharing one
HTTP client across worker threads is safe. The within-filer async fetcher
uses `asyncio.run()` (fresh event loop per call/thread) — cross-thread
coordination is the `threading.Lock` gate, not any `asyncio.Semaphore`.

**Non-thread-safe shared state — the zip fetcher (Codex ckpt-1 HIGH/LOW):**
the N-PORT bootstrap path wraps the provider in
`sec_submissions_zip.ZipBackedArchiveFetcher(zip_handle, fallback=sec)`
(scheduler.py:6648) over ONE `zipfile.ZipFile`. `ZipFile.open()` shares the
underlying file object's seek position → **not** safe for concurrent reads.
Fix: add a `threading.Lock` inside `ZipBackedArchiveFetcher` guarding the
local `read_zip_entry` call ONLY. The HTTP fallback delegation stays outside
the lock (the gate handles HTTP concurrency); the zip-entry read is a tiny
fraction of per-filer wall-clock (one local JSON read vs the HTTP archive
fetches that dominate), so the lock barely serialises the pipeline.

## Connection model (data-engineer SKILL §6.5.1 — caller-wraps-tx)

`psycopg.Connection` is **not** shareable across threads. Each worker
pipeline opens its **own** conn via `connect_job()` and owns its filer's
tx boundary (commit on success / rollback on its own exception — the
`with connect_job() as conn:` context manager does this automatically).
The orchestrator thread keeps the original `conn` solely for the
`data_ingestion_runs` audit row (start/finish + accumulated counts).

Per-filer writes touch `institutional_holdings` /
`institutional_holdings_ingest_log` (resp. `n_port_*`), keyed by
accession. With **distinct** CIKs (the driver de-dupes — see below) each
worker owns a disjoint accession set, so there is no cross-filer write
race. Shared-table upserts that DO span filers —
`unresolved_13f_cusips` `ON CONFLICT (cusip)` — are concurrency-safe by
Postgres design (one txn waits on the other's row lock, re-checks). The
13F per-accession `acquire_13f_accession_write_lock` is a
`pg_advisory_xact_lock` → **cluster-wide**, so it serialises correctly
across worker conns / processes even on a duplicate accession.
`ingest_filer_13f`'s `ingestion_run_id` param does not write
`data_ingestion_runs` (verified: only the orchestrator does).

**CIK de-dupe (Codex ckpt-1 HIGH):** `list_directory_filer_ciks` /
`list_nport_filer_ciks` are `SELECT cik ... ORDER BY` with no `DISTINCT`.
The driver de-dupes order-preservingly (`dict.fromkeys`) before dispatch
so two workers never process the same filer concurrently (wasted work +
N-PORT has no per-accession advisory lock).

**ContextVar caveat (Codex ckpt-1 MED):** `connect_job()` reads
`job_statement_timeout_ms` (a ContextVar set by `_tracked_job`, #1690).
ContextVars do **not** propagate into `ThreadPoolExecutor` threads. The
orchestrator captures `job_statement_timeout_ms.get()` once; the worker
`.set()`s it at thread entry before `connect_job()` and `.reset(token)`s
in a `finally` — pool threads are reused, and an explicit reset prevents a
stale timeout leaking to the next task on that thread.

## Design — shared bounded-concurrency driver

New leaf module `app/services/sec_filer_concurrency.py`:

```python
@dataclass(frozen=True)
class FilerWorkResult:
    cik: str
    summary: IngestSummary | None   # None iff crashed
    error: str | None               # crash message iff crashed

@dataclass(frozen=True)
class DrainOutcome:
    submitted: int
    completed: int
    deadline_hit: bool
    cancelled: bool

def drain_filers_concurrently(
    ciks: Sequence[str],
    *,
    worker: Callable[[psycopg.Connection[Any], str], IngestSummary],
    concurrency: int,
    deadline_ts: float | None,           # monotonic; None = no deadline
    should_cancel: Callable[[], bool] | None,
    on_result: Callable[[FilerWorkResult], None],  # runs in orchestrator thread
    statement_timeout_ms: int | None,
) -> DrainOutcome
```

- **Worker wrapper** (runs on a pool thread) — the `try/except` wraps the
  `with` from the **outside** (Codex ckpt-1 HIGH) so a worker exception
  reaches `connect_job().__exit__` → ROLLBACK before it is caught; catching
  *inside* the `with` would let the context manager COMMIT a failed tx:

  ```python
  def _run_one(cik):
      token = job_statement_timeout_ms.set(statement_timeout_ms)
      try:
          with connect_job() as wconn:        # __exit__ commits on clean exit,
              summary = worker(wconn, cik)     # rolls back if worker raised
              wconn.commit()
          return FilerWorkResult(cik, summary, None)
      except Exception as exc:                 # noqa: BLE001 — per-filer isolation
          return FilerWorkResult(cik, None, str(exc))
      finally:
          job_statement_timeout_ms.reset(token)
  ```

  One filer's failure never propagates into the pool / poisons the batch.
- **`concurrency` validation (Codex ckpt-1 MED):** clamp `max(1, concurrency)`
  (a 0/negative setting would no-op the pool).
- **Sliding-window dispatch** (orchestrator thread): check stop **before
  every submit including the initial prime** (Codex ckpt-1 HIGH — an
  already-past deadline / pre-set cancel submits zero). Prime up to
  `concurrency`; loop `wait(in_flight, timeout=HEARTBEAT_S,
  return_when=FIRST_COMPLETED)`; on each wakeup (completion **or** timeout):
  call `on_result` for any done future (single-threaded accumulation — no
  lock on counters), emit cadenced progress, and re-check cancel/deadline.
  *Only if* not stopped, top the window back up to `concurrency`. The
  `HEARTBEAT_S` timeout (≈5 s) means: (a) cancel/deadline halts new
  submissions within ~5 s even when all in-flight filers are slow (not
  "after one completes"), and (b) the 30 s wall-clock progress heartbeat
  still fires during a long in-flight batch. On stop: submit nothing more,
  **drain in-flight to completion** (partial commits are valid + resumable
  via ingest-log tombstones), then return.
- Cancel ranks above deadline (mirrors the existing serial precedence).

This preserves every existing semantic under concurrency:
deadline soft-budget, partial-commit + ingest-log tombstone resume,
operator-cancel checkpoint latency (now ~one *completed* filer instead of
one serial filer), per-filer failure isolation.

### Shared-write deadlock avoidance (dev-verify finding)

The first dev-verify run deadlocked 38/60 filers at concurrency=8. Root cause:
concurrent `INSERT ... ON CONFLICT` on a **shared** unique index. The
accession-keyed tables (`institutional_holdings`, ingest-log) and filer-keyed
observations are disjoint across filers, but `unresolved_13f_cusips (cusip)` is
hammered by every filer (many institutions report the same unresolved CUSIPs;
`observation_count + 1` on hot shared rows). Two filer transactions inserting
overlapping CUSIPs in **opposite order** form a lock cycle → Postgres aborts one
as the deadlock victim. The per-accession `acquire_13f_accession_write_lock` is
filer-disjoint and does not cover this.

Two-part fix:
- **Deterministic global lock order:** upsert the unresolved CUSIPs sorted by
  the stored cusip key, so every transaction acquires the shared row locks in
  the same order — no cycle is possible. (`refresh_institutions_current_batch`
  already normalises/sorts its instrument ids; observations are filer-keyed.)
- **Deadlock retry (safety net):** `make_filer_runner` catches
  `DeadlockDetected` / `SerializationFailure`, rolls back, and retries the whole
  filer on a fresh connection (the contender has committed by then), up to
  `DEFAULT_MAX_DEADLOCK_RETRIES`. Covers any residual cross-table contention;
  exhaustion returns a crashed result (still isolated).

### Benign semantic drift (documented)

`first_accession_error` / first `crash_error` become "first to **complete**"
rather than "first in **cik order**" — an error-summary string in
`data_ingestion_runs.error`; per-accession detail is unaffected (lives in
the ingest-log). Acceptable.

## Orchestrator changes

`ingest_all_active_filers` (13F) and `ingest_all_fund_filers` (N-PORT):
replace the `for cik in ciks:` body with a `drain_filers_concurrently`
call. Each builds:
- `worker = lambda wconn, cik: ingest_filer_13f(wconn, sec, filer_cik=cik,
  ingestion_run_id=run_id, min_period_of_report=...)` (resp.
  `ingest_fund_n_port`).
- `on_result` accumulating `rows_upserted/rows_skipped/accession_failures/
  first_error/crash_error` + cadenced progress emit (completed count).
- 13F passes `should_cancel=bootstrap_cancel_requested`; N-PORT passes
  `None` (no cancel signal today — unchanged).
- finalize (`finish_ingestion_run`) + raise `BootstrapStageCancelled`
  (13F only) identical to today.

Signature gains `concurrency: int | None = None` (defaults to
`settings.sec_filer_ingest_concurrency`). Back-compat: existing callers
omit it.

## Settings

`app/config.py`: `sec_filer_ingest_concurrency: int = 8` (acceptance:
factor is a setting, default 8). Shared by both sweeps. Cap rationale:
8 concurrent pipelines × per-request floor still ≤ the 9.09 req/s gate
(the gate, not the worker count, bounds request rate); 8 keeps DB conns +
heap bounded while saturating the budget.

## Scope

- **In:** 13F (`ingest_all_active_filers`) + N-PORT
  (`ingest_all_fund_filers`) — both named in #1274, same `ingest_all_*`
  shape, both benchmark-relevant; + `ZipBackedArchiveFetcher` thread-lock
  (required for the N-PORT bootstrap concurrent path).
- **Out (follow-up ticket):** `sec_def14a_bootstrap`,
  `sec_first_install_drain` — "likely also" per #1274 but separate-shaped
  scheduler funcs; folding 4 surfaces risks an unreviewable diff.

## Tests

- **Pure-logic** (no DB), `tests/test_sec_filer_concurrency.py` — the worker
  callable is faked (no `connect_job`); the driver's conn handling is
  exercised via dev-verify on the real path:
  - fan-out: N ciks, fake worker → all complete; `on_result` called once
    per cik; observed max-in-flight ≤ `concurrency`.
  - failure isolation: one worker raises → its result is `crashed`, the rest
    complete (assert **downstream** of the injected failure — prevention log).
  - cancel checkpoint: `should_cancel` flips true mid-drain → no new
    submissions after, in-flight drains, `cancelled=True`.
  - deadline: past `deadline_ts` at entry → zero submitted, `deadline_hit`.
  - de-dupe: duplicate CIKs in input → worker invoked once per distinct cik.
  - concurrency clamp: `concurrency=0` → treated as 1, still drains all.
- **One DB test** only if a new SQL mechanism — there is none (reuses
  existing per-filer writers). Rely on dev-verify for the real conn path.
- `ZipBackedArchiveFetcher` lock: a small test asserting concurrent
  `fetch_document_text` calls don't corrupt reads (or simply that the lock
  exists + delegates) — keep minimal; the contract is "accelerator never a
  coverage reducer", unchanged.

## Verification (ETL DoD 8-12)

- Parity: run a bounded slice (e.g. 50 filers) through the new path on dev
  in a rolled-back tx; assert row-identical holdings vs the serial path
  (byte/row parity, like #1436) → proves **no `sec_rebuild` needed**.
- Smoke AAPL/GME/MSFT/JPM/HD ownership-rollup institutions slice renders
  post-drain.
- Benchmark: timed bounded slice → filers/min; extrapolate 9,148-cohort
  ETA < 30 min (or full drain on dev). Record rate + commit SHA.
