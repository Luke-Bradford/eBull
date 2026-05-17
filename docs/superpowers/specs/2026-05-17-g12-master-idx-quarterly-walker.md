# G12 — `master.idx` quarterly cross-quarter walker

> **Status:** CLEAN v3 2026-05-17 (Codex 1a r1+r2+r3 revisions: per-quarter txn
> isolation; strict-vs-tolerant 404 contract; outage-window invariant + manual
> backfill kwarg/runbook (NO operator param surface — see §3.1); preloaded
> universe resolver; cohort-correct smoke panel; test-fixture realism fix).
> **Phase / PR:** US ETL completion plan §2 Phase 3, PR 6.
> **Gap closed:** §7 G12 — full-index `master.idx` quarterly not consumed.

## 1. Goal

Close the **cross-quarter discovery** gap. Today the freshness redesign
(Layer 1 Atom + Layer 2 daily-index + Layer 3 per-CIK poll, all wired by
#1155) catches all "today + yesterday" filings + walks per-CIK
submissions.json for known subjects. It does NOT cover the case where a
CIK is **tombstoned** (or deactivated / merged) yet still emits late
accessions or amendments past the per-CIK polling cadence — those rows
slip past every steady-state lane.

The fix is a periodic walk of the SEC quarterly full-index `master.idx`,
which lists EVERY accepted accession across the entire universe for a
given quarter (~250k-300k rows / ~50 MB per quarter). For every row whose
CIK resolves to a known subject in our universe AND whose form maps to a
ManifestSource, UPSERT a `sec_filing_manifest` row. The manifest worker
drains it.

The walker is **stateless idempotent**: `record_manifest_entry`'s
`ON CONFLICT` clause preserves any in-flight `ingest_status`, so a
re-walk of the same quarter never downgrades a row.

## 2. Non-goals

- **No new persistence table.** Walker writes only to
  `sec_filing_manifest`. No `sec_master_idx_walk_runs` / no per-quarter
  cursor row / no `data_freshness_index` row. The job is stateless —
  every fire re-walks the same window from scratch. Re-walks are
  idempotent + ~30s wall-clock + ~100 MB SEC fetch, which is below the
  "needs persistent watermark" threshold.
- **No backfill of pre-bootstrap history.** Stage 14
  `filings_history_seed` (730-day per-CIK history) + Stage 8
  `sec_submissions_ingest` (bulk submissions.zip) already cover
  first-install backfill. The walker exists only for the
  **steady-state cross-quarter sanity net**.
- **No raw-payload persistence.** `master.idx` is a SEC-published
  reference index, not a per-filing payload. It is fetched and discarded
  on every walk (sibling pattern: `sec_daily_index_reconcile` /
  `top_filer_discovery.fetch_form_index`). The accession rows it
  surfaces are written to `sec_filing_manifest`; the per-accession raw
  payload is fetched lazily by the per-source parser at drain time.
- **No `companyconcept` / `frames` consumption.** Those are G10 / G11,
  Phase 4 in the plan.
- **No FINRA / FRED / BLS extension.** Out of scope.
- **No new operator UI surface.** The walker shows up in the existing
  `/system/jobs` view via its `ScheduledJob` registration; no new admin
  page or operator-facing chart.

## 3. Design decisions

Three design calls flagged in the plan brief, resolved here.

### 3.1 Cadence — **weekly Sunday 05:15 UTC, walks (current quarter, immediately-previous quarter); >1-quarter outage recovery via runbook only**

- One `ScheduledJob` named `JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP`.
- `Cadence.weekly(weekday=6, hour=5, minute=15)` — Sunday after the SEC
  Saturday PAC rebuild settles. Matches the existing
  weekday=6 weekly-job convention in `SCHEDULED_JOBS` (every other
  weekly is Sunday) so operator mental model is preserved.
- Each fire walks **exactly two quarters**: the current calendar
  quarter (CQ) AND the immediately-previous calendar quarter (CQ-1).
  No operator param surface on the ScheduledJob — `params_metadata=()`.
- **>1-quarter outage runbook (Codex 1a r1 HIGH-3 ownership).** The
  scheduled walker's 2-quarter window is intentionally bounded so
  every fire is O(100 MB) regardless of outage duration. If the stack
  has been down for >1 quarter (genuine multi-quarter loss of coverage),
  the operator's recovery path is:

  ```
  # Admin Python REPL / one-shot scripts/ entry:
  from app.jobs.sec_master_idx_quarterly_sweep import (
      run_master_idx_quarterly_sweep,
  )
  from app.providers.implementations.sec_edgar import SecFilingsProvider
  from app.workers.scheduler import _make_sec_http_get
  import psycopg
  from app.config import settings

  with (
      SecFilingsProvider(user_agent=settings.sec_user_agent) as sec,
      psycopg.connect(settings.database_url) as conn,
  ):
      stats = run_master_idx_quarterly_sweep(
          conn,
          http_get=_make_sec_http_get(sec),
          quarters=[(2024, 1), (2024, 2), (2024, 3), (2024, 4)],
      )
      print(stats)
  ```

  The `quarters` kwarg is kept on `run_master_idx_quarterly_sweep`
  (NOT exposed via `params_metadata`) precisely so the
  recovery path exists without a separate admin "rebuild" job.
  Operator UX for this case is "follow the runbook"; productisation as
  an admin-button-driven `quarters` param surface is intentionally
  out of scope for G12 (requires extending `ParamFieldType` with a
  `multi_quarter` shape that doesn't exist today — a cross-cutting
  ParamMetadata change distinct from cross-quarter discovery).
- **Why a fixed 2-quarter default window?**
  - Tombstoned-CIK / late-amendment discovery cost is sharply
    front-loaded — the bulk of late accessions for any quarter land
    within ~90 days of quarter end. By the time a quarter is two
    quarters old, the cohort of "still arriving" rows is effectively
    zero against the cost of fetching another ~50 MB file. Stage 14
    `filings_history_seed` (730-day) already covers anything deeper
    via per-CIK enumeration.
  - "Walks both quarters every fire" beats "walks current quarter
    weekly, walks previous quarter monthly" because:
    - The implementation is one for-loop instead of two cron-style
      conditional schedules. No "is today the first Sunday of the
      month?" trap.
    - Re-walking CQ-1 weekly (instead of monthly) re-spends ~50 MB
      and ~30 s of wall-clock. Not load-bearing. The simplicity
      saves Codex / bot review surface, prevention-log entries on
      missed monthly fires, and an operator-visible "why didn't it
      walk this month?" thread.
- **Recovery invariant (Codex 1a r1 HIGH-3 ownership):** missing one
  weekly fire is recovered by the next Sunday walk so long as the gap
  did not span more than one quarter boundary. If the stack is down
  for >1 quarter, the operator MUST run the manual override above —
  the scheduled walker's 2-quarter window is intentionally bounded so
  every fire is O(100 MB) regardless of outage duration. Documented in
  the scheduler ScheduledJob.description and in the operator runbook
  in §6 acceptance.
- `prerequisite=_bootstrap_complete`. Justification: pre-bootstrap the
  universe is empty + Stage 14 has not yet seeded per-CIK history; a
  walker fire would be a no-op (subject_resolver returns None for every
  row). Sibling Layer-3 per-CIK poll (`sec_per_cik_poll`) takes the same
  position (`prerequisite=_bootstrap_complete`,
  `catch_up_on_boot=False`).
- `catch_up_on_boot=False`. Justification: missing one weekly fire
  doesn't lose data permanently within the 2-quarter window — the very
  next Sunday's walk re-discovers everything. The >1-quarter outage
  case is the operator-backfill path (above). Contrast with Layer 2
  (`catch_up_on_boot=True`, `exempt_from_universal_bootstrap_gate=True`)
  where missing a daily fire = losing yesterday's reconcile
  permanently.
- **NOT exempt from the universal bootstrap-state gate.** The
  `exempt_from_universal_bootstrap_gate=True` carve-out (see
  `app/workers/scheduler.py:218-238` ScheduledJob doc + spec
  2026-05-16-lane-b-discovery-firing.md §4.2) is reserved for jobs that
  meet ALL four eligibility criteria. The master.idx walker is empty-DB
  safe ✅ and bounded-cost ✅ — but it fails (1) `catch_up_on_boot=False`
  AND it fails (2) `prerequisite=_bootstrap_complete is not None`. With
  both `catch_up_on_boot=False` AND a per-job prereq, the carve-out's
  motivation (boot-time-only catch_up trap) does not apply. The
  universal gate is the right behaviour here — bootstrap-incomplete
  systems should not be issuing 50 MB SEC fetches against an empty
  universe.

### 3.2 Persistence target — **`sec_filing_manifest` only; no new table; no watermark**

- The walker is **stateless idempotent**. `record_manifest_entry` is
  the only write path. SEC publishes `master.idx` as a static
  per-quarter file; re-walks observe the same content (modulo SEC
  quarter-edge additions). UPSERT on conflict preserves any
  in-flight `ingest_status` so a re-walk never downgrades a row.
- **Per-quarter transaction isolation** (Codex 1a r1 HIGH-1): each
  quarter walks inside its own `try` block. On any exception (HTTP
  error from `read_master_idx`, psycopg error from `subject_resolver`
  or `record_manifest_entry`, parser error) the job:
  1. `logger.exception`s with the quarter context.
  2. Calls `conn.rollback()` to discard the aborted transaction state
     so the next quarter starts on a clean implicit `BEGIN`.
  3. Records `QuarterStats(failed=True, error_detail=str(exc))` in the
     return value and continues to the next quarter.
  On successful quarter completion the job calls `conn.commit()` so
  that quarter's UPSERTs are durably persisted BEFORE the next quarter
  begins. The invoker's terminal `conn.commit()` is therefore a no-op
  on the happy path (the per-quarter commit ran the work), and the
  scheduler `_tracked_job` row counts what actually committed.
- **No `sec_master_idx_walk_runs(year, quarter, walked_at, ...)` audit
  table.** Operator audit is via `job_runs.row_count` (≈ upserted
  count) + structured `logger.info` line per fire emitting per-quarter
  breakdown (`index_rows`, `matched_in_universe`, `upserted`,
  `skipped_unmapped_form`, `skipped_unknown_subject`, `failed`,
  `error_detail`). The `job_runs` row stamps `ran_at` so "when did we
  last walk?" is recoverable via SQL on the existing audit table.
- **No `data_freshness_index` row for the walker's subject set.** The
  walker's "subject" is the entire universe of in-universe CIKs, not a
  single CIK. `data_freshness_index` is per-(subject, source) — the
  shape doesn't fit.
- **Honest re-walk cost** (Codex 1a r1 MED-1): a re-walk of an
  unchanged quarter is NOT "an HTTP fetch only". It parses ~250-300k
  rows, runs ~250-300k resolver lookups (O(1) once the preloaded
  universe map is built — see §3.5), runs ~Nₘ
  `record_manifest_entry` UPSERTs where Nₘ is the matched-in-universe
  subset (~10-30k rows per quarter for the operating-issuer cohort).
  Each UPSERT touches `updated_at` even when `ingest_status` is
  preserved by `ON CONFLICT DO UPDATE`. Total DB write cost per fire:
  ~20-60k row touches against an indexed PK. Bounded; acceptable for
  a weekly fire; documented honestly so future-Codex / bot reviews
  don't re-litigate the cost argument.

### 3.3 Watermark shape — **none; deterministic re-compute of (year, quarter) from `datetime.now(UTC)`**

- The two quarters to walk are computed at fire time:
  - `cq = current_calendar_quarter(now_utc)`
  - `cq_minus_1 = previous_calendar_quarter(cq)`
- Pure-function helper `_quarters_to_walk(now)` returns
  `[(year, q), ...]` — injectable for tests so the walker can be
  exercised against a fixed clock.
- No watermark = no schema = no migration = no "what happens at
  Jan 1 when CQ flips and CQ-1 was last year's Q4" edge case (the
  helper handles it; the test pins it).

### 3.4 Reuse vs new code

- **Reuse `parse_daily_index`** at
  `app/providers/implementations/sec_daily_index.py:80` — the file
  format is IDENTICAL (`CIK|Company Name|Form Type|Date Filed|Filename`
  header + dashed separator + pipe-delimited rows). The daily-index
  parser yields `FilingIndexRow` and uses the same `is_amendment_form`
  + `map_form_to_source` helpers we need. **One textual difference**:
  `master.idx` carries the same pipe schema as `master.YYYYMMDD.idx`
  per `.claude/skills/data-sources/sec-edgar.md` §1.5; the only
  variability is rows-per-file (~250k for quarterly vs ~6k for daily).
- **Reuse `ResolvedSubject` + `SubjectResolver`** types from
  `app/jobs/sec_atom_fast_lane.py:35-46`.
- **NEW preloaded universe resolver** — `build_preloaded_subject_resolver(conn)`
  in `app/jobs/sec_master_idx_quarterly_sweep.py`. See §3.5.
- **Reuse `record_manifest_entry`** at
  `app/services/sec_manifest.py` — same UPSERT contract.
- **New surface:**
  - `app/providers/implementations/sec_full_index.py` — pure
    HTTP-getter-driven reader for one quarter's master.idx, yielding
    `FilingIndexRow`. Mirrors `read_daily_index` shape (per-quarter
    URL builder + parse-body helper). **Strict-by-default 404
    contract**: raises `RuntimeError` on 404 unless caller explicitly
    passes `allow_404=True` (current-quarter-not-yet-published case).
    See §3.6.
  - `app/jobs/sec_master_idx_quarterly_sweep.py` — job module with
    `run_master_idx_quarterly_sweep(conn, *, http_get, now=None,
    subject_resolver=None, quarters=None)` returning
    `MasterIdxSweepStats`. The default `subject_resolver=None` triggers
    `build_preloaded_subject_resolver(conn)` (O(1) lookups against an
    eagerly-loaded universe map). Tests can pass an explicit
    resolver to override.
  - `app/workers/scheduler.py` — `JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP`
    constant + `ScheduledJob` entry (no `params_metadata` — empty
    default `()`) + zero-arg `sec_master_idx_quarterly_sweep()`
    invoker. >1-quarter outage recovery is a Python REPL runbook (see
    §3.1), not an operator-facing param surface.
  - `app/jobs/runtime.py` — `_INVOKERS[JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP]`
    registration via `_adapt_zero_arg`. Same shape as
    `JOB_SEC_DAILY_INDEX_RECONCILE` / `JOB_SEC_PER_CIK_POLL`.

### 3.5 Preloaded subject resolver (Codex 1a r1 MED-2)

`default_subject_resolver` is per-row + per-table — 3 indexed PK
lookups per CIK across 250k-300k rows = ~750k-900k SQL roundtrips
per quarter on a cold cache. Replace with a one-shot preload:

```python
def build_preloaded_subject_resolver(
    conn: psycopg.Connection[Any],
) -> SubjectResolver:
    """Build a closure-resolver from a single eager universe load.

    Issues three SELECTs at fire time, materialises the union as
    ``dict[cik, ResolvedSubject]``, returns a ``(conn, cik) ->
    ResolvedSubject | None`` closure that runs in O(1).

    Resolution priority matches default_subject_resolver:
        issuer > institutional_filer > blockholder_filer.

    Memory profile: ~10k issuers + ~5k institutional filers +
    ~1-2k blockholder filers = ~17k entries × ~80 bytes per
    ResolvedSubject ≈ 1.5 MB. Bounded.
    """
    universe: dict[str, ResolvedSubject] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT cik, instrument_id FROM instrument_sec_profile")
        for cik, instrument_id in cur.fetchall():
            universe[cik] = ResolvedSubject(
                subject_type="issuer",
                subject_id=str(int(instrument_id)),
                instrument_id=int(instrument_id),
            )
        cur.execute("SELECT cik FROM institutional_filers")
        for (cik,) in cur.fetchall():
            universe.setdefault(
                cik,
                ResolvedSubject(
                    subject_type="institutional_filer",
                    subject_id=cik,
                    instrument_id=None,
                ),
            )
        cur.execute("SELECT cik FROM blockholder_filers")
        for (cik,) in cur.fetchall():
            universe.setdefault(
                cik,
                ResolvedSubject(
                    subject_type="blockholder_filer",
                    subject_id=cik,
                    instrument_id=None,
                ),
            )

    def _resolve(_conn: psycopg.Connection[Any], cik: str) -> ResolvedSubject | None:
        return universe.get(cik)

    return _resolve
```

- Default for `run_master_idx_quarterly_sweep`. Tests inject a
  pre-built dict resolver to bypass the preload SQL.
- `setdefault` guarantees the issuer-priority precedence
  (any CIK that's BOTH an issuer AND an institutional filer keeps the
  issuer subject_type — matches `default_subject_resolver` order).
- Closure ignores its `conn` arg — kept in the signature so the
  `SubjectResolver` contract is preserved + the per-row sibling
  default remains pluggable.

### 3.6 Strict-by-default 404 contract (Codex 1a r1 HIGH-2)

`read_master_idx(http_get, year, quarter, *, allow_404=False)`:

- `allow_404=False` (default): 404 raises
  `RuntimeError("master.idx fetch failed: status=404 year=Y quarter=Q")`.
- `allow_404=True`: 404 logs the canonical "not yet published" line
  and returns an empty iterator.

The job opt-in is keyed off "is this the current calendar quarter at
fire time?":

```python
cq_year, cq_q = _current_calendar_quarter(now)
for year, q in quarters_to_walk:
    is_current = (year, q) == (cq_year, cq_q)
    try:
        iterator = read_master_idx(http_get, year, q, allow_404=is_current)
        ...
```

A previous-quarter 404 (network typo, SEC outage, intermittent CDN
failure) thereby surfaces as `QuarterStats(failed=True,
error_detail="master.idx fetch failed: status=404 ...")` and is
visible in logs + the `job_runs` row. A current-quarter 404 (the only
defensible case — newborn quarter, PAC build pending) is silently
empty as before. The asymmetry is the point.

## 4. URL + format

```
URL:  https://www.sec.gov/Archives/edgar/full-index/{YYYY}/QTR{n}/master.idx
Body shape (after preamble + dashed separator):
  CIK|Company Name|Form Type|Date Filed|Filename
  ----------------------------------------------------------
  320193|Apple Inc.|8-K|2026-04-30|edgar/data/320193/0000320193-26-000042.txt
  ...
Size: ~50 MB per quarter; ~250-300k rows.
Cache-friendly: SEC publishes ETag/Last-Modified. PAC rebuild Saturdays.
```

- Encoding: latin-1 is the SEC archive convention for indexes
  (`top_filer_discovery.fetch_form_index` decodes as latin-1). We
  follow daily-index's `body.decode("utf-8", errors="replace")` since
  the parser only consumes ASCII-safe fields (CIK digits + form +
  ISO date + path); the company-name column is parsed but immediately
  discarded — `parse_daily_index` never returns it. **No new failure
  mode introduced.**
- 404 handling: the current quarter's master.idx may be 404 for the
  first few hours of a new quarter while SEC PAC builds it; the
  walker logs + skips that quarter, walks the previous one only.
  Sibling pattern: `read_daily_index` already 404-tolerates daily
  files for weekends / holidays / not-yet-published.
- Other non-200 statuses: raise `RuntimeError` per quarter, caught at
  the per-quarter level so one quarter's failure does not abort the
  other. Sibling pattern: `aggregate_top_filers`
  ([top_filer_discovery.py:197](../../../app/services/top_filer_discovery.py#L197))
  isolates per-quarter exceptions and logs + continues.

## 5. Code surface

### 5.1 `app/providers/implementations/sec_full_index.py` (NEW)

```python
"""Pure SEC full-index quarterly reader (G12).

Quarterly full-index files at
``https://www.sec.gov/Archives/edgar/full-index/{YYYY}/QTR{n}/master.idx``
list every filing accepted across the entire SEC universe for one
calendar quarter (~250-300k rows / ~50 MB).

Used by ``sec_master_idx_quarterly_sweep`` as a CROSS-QUARTER
SAFETY NET — catches accessions that Layer 1 (Atom) and Layer 2
(daily-index reconcile) missed AND that Layer 3 (per-CIK poll) cannot
discover because the CIK is tombstoned / deactivated / no longer
emitting submissions.json updates.

Format: pipe-delimited, identical schema to daily-index. Reuses
``parse_daily_index`` byte-for-byte.

404 contract: strict by default — only the JOB's current-quarter walk
should pass ``allow_404=True`` (newborn quarter / PAC-build pending).
Previous-quarter 404s indicate a network/SEC failure and MUST surface
so the sweep can record ``QuarterStats(failed=True)`` instead of
silently committing a zero-row walk.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date

from app.providers.implementations.sec_daily_index import HttpGet, parse_daily_index
from app.providers.implementations.sec_submissions import FilingIndexRow

logger = logging.getLogger(__name__)


def _quarter_start_date(year: int, quarter: int) -> date:
    """First day of the given quarter — used as the default
    ``filed_at`` anchor for any row whose date column is malformed."""
    if not 1 <= quarter <= 4:
        raise ValueError(f"quarter must be 1..4, got {quarter}")
    return date(year, (quarter - 1) * 3 + 1, 1)


def _build_url(year: int, quarter: int) -> str:
    if not 1 <= quarter <= 4:
        raise ValueError(f"quarter must be 1..4, got {quarter}")
    return (
        f"https://www.sec.gov/Archives/edgar/full-index/"
        f"{year}/QTR{quarter}/master.idx"
    )


def read_master_idx(
    http_get: HttpGet,
    year: int,
    quarter: int,
    *,
    user_agent: str = "eBull research/1.0 contact@example.com",
    allow_404: bool = False,
) -> Iterator[FilingIndexRow]:
    """Fetch + parse one quarter's master.idx.

    ``allow_404=False`` (default): 404 raises ``RuntimeError`` so the
    caller's per-quarter ``try/except`` records the failure. Only
    pass ``allow_404=True`` for the current calendar quarter (the only
    case where 404 = "not yet published"). Previous-quarter 404s
    indicate an SEC outage / typo / CDN failure and must NOT be
    silenced.

    Raises ``RuntimeError`` on any other non-200 status.
    """
    url = _build_url(year, quarter)
    headers = {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
    }
    status, body = http_get(url, headers)
    if status == 404:
        if allow_404:
            logger.info(
                "master.idx not yet published for %sQ%s (404; allowed)",
                year, quarter,
            )
            return
            yield  # pragma: no cover — keeps signature as Iterator
        raise RuntimeError(
            f"master.idx fetch failed: status=404 year={year} quarter={quarter} "
            f"(allow_404=False; previous-quarter 404 indicates SEC/network failure)"
        )
    if status != 200:
        raise RuntimeError(
            f"master.idx fetch failed: status={status} year={year} quarter={quarter}"
        )
    yield from parse_daily_index(
        body, default_filed_at=_quarter_start_date(year, quarter)
    )
```

### 5.2 `app/jobs/sec_master_idx_quarterly_sweep.py` (NEW)

```python
"""Quarterly full-index cross-quarter discovery (G12).

Plan: docs/superpowers/plans/2026-05-17-us-etl-completion.md §2 Phase 3.
Spec: docs/superpowers/specs/2026-05-17-g12-master-idx-quarterly-walker.md.

One scheduled fire walks the current calendar quarter AND the
immediately-previous calendar quarter by default. Callers can pass an
explicit ``quarters=[(year, q), ...]`` kwarg for >1-quarter outage
backfill (operator runbook in spec §3.1) or for tests.

Per-(year, quarter) failure is isolated: HTTP/parse/DB errors in one
quarter trigger ``conn.rollback()`` + ``QuarterStats(failed=True)``
and the loop continues to the next quarter. Successful quarters
``conn.commit()`` before the next iteration so already-walked work is
durably persisted before the next quarter's risk surface.

Subject resolution uses a preloaded universe map (one-shot SELECTs at
fire time, O(1) per-row lookup) instead of the per-row 3-table
default_subject_resolver — see spec §3.5.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import psycopg

from app.jobs.sec_atom_fast_lane import (
    ResolvedSubject,
    SubjectResolver,
)
from app.providers.implementations.sec_daily_index import HttpGet
from app.providers.implementations.sec_full_index import read_master_idx
from app.services.sec_manifest import record_manifest_entry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class QuarterStats:
    year: int
    quarter: int
    index_rows: int = 0
    matched_in_universe: int = 0
    upserted: int = 0
    skipped_unmapped_form: int = 0
    skipped_unknown_subject: int = 0
    failed: bool = False
    error_detail: str | None = None


@dataclass(frozen=True)
class MasterIdxSweepStats:
    quarters: list[QuarterStats] = field(default_factory=list)

    @property
    def total_upserted(self) -> int:
        return sum(q.upserted for q in self.quarters)

    @property
    def failed_quarters(self) -> int:
        return sum(1 for q in self.quarters if q.failed)


def _current_calendar_quarter(now: datetime) -> tuple[int, int]:
    """Return ``(year, quarter)`` for the UTC moment ``now``."""
    return now.year, (now.month - 1) // 3 + 1


def _previous_calendar_quarter(year: int, quarter: int) -> tuple[int, int]:
    """Return ``(year, quarter)`` for the calendar quarter immediately
    before ``(year, quarter)``."""
    if quarter == 1:
        return year - 1, 4
    return year, quarter - 1


def _quarters_to_walk(now: datetime) -> list[tuple[int, int]]:
    """Return ``[(current_year, current_q), (prev_year, prev_q)]``.

    Pure function — exercised against a fixed clock in tests so the
    Jan-1-rollover branch (CQ1 → CQ4-prev-year) is pinned.
    """
    cq = _current_calendar_quarter(now)
    return [cq, _previous_calendar_quarter(*cq)]


def build_preloaded_subject_resolver(
    conn: psycopg.Connection[Any],
) -> SubjectResolver:
    """Eager universe preload → O(1) closure resolver. See spec §3.5."""
    universe: dict[str, ResolvedSubject] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT cik, instrument_id FROM instrument_sec_profile")
        for cik, instrument_id in cur.fetchall():
            universe[cik] = ResolvedSubject(
                subject_type="issuer",
                subject_id=str(int(instrument_id)),
                instrument_id=int(instrument_id),
            )
        cur.execute("SELECT cik FROM institutional_filers")
        for (cik,) in cur.fetchall():
            universe.setdefault(
                cik,
                ResolvedSubject(
                    subject_type="institutional_filer",
                    subject_id=cik,
                    instrument_id=None,
                ),
            )
        cur.execute("SELECT cik FROM blockholder_filers")
        for (cik,) in cur.fetchall():
            universe.setdefault(
                cik,
                ResolvedSubject(
                    subject_type="blockholder_filer",
                    subject_id=cik,
                    instrument_id=None,
                ),
            )

    logger.info(
        "master_idx sweep: preloaded universe size=%d "
        "(issuer / institutional_filer / blockholder_filer)",
        len(universe),
    )

    def _resolve(_conn: psycopg.Connection[Any], cik: str) -> ResolvedSubject | None:
        return universe.get(cik)

    return _resolve


def run_master_idx_quarterly_sweep(
    conn: psycopg.Connection[Any],
    *,
    http_get: HttpGet,
    now: datetime | None = None,
    subject_resolver: SubjectResolver | None = None,
    quarters: Sequence[tuple[int, int]] | None = None,
) -> MasterIdxSweepStats:
    """One quarterly-sweep cycle.

    Per-quarter commit / rollback isolation — see spec §3.2.

    ``quarters=None`` (default) walks ``[CQ, CQ-1]`` from ``now``.
    Pass an explicit ``[(year, q), ...]`` sequence for the >1-quarter
    outage backfill runbook (spec §3.1) — each pair MUST have year >=
    1993 (EDGAR full-index history start) and quarter in 1..4;
    out-of-range pairs propagate as a ``RuntimeError`` from
    ``read_master_idx`` and surface in ``QuarterStats(failed=True)``.

    ``subject_resolver=None`` (default) preloads the universe map via
    ``build_preloaded_subject_resolver``. Tests inject a pre-built
    resolver.
    """
    if now is None:
        now = datetime.now(tz=UTC)
    walk = list(quarters) if quarters else _quarters_to_walk(now)
    cq_year, cq_q = _current_calendar_quarter(now)
    resolver: SubjectResolver = (
        subject_resolver
        if subject_resolver is not None
        else build_preloaded_subject_resolver(conn)
    )

    quarter_stats: list[QuarterStats] = []
    for year, q in walk:
        index_rows = 0
        matched = 0
        upserted = 0
        skipped_unmapped = 0
        skipped_unknown = 0
        is_current = (year, q) == (cq_year, cq_q)

        try:
            for row in read_master_idx(http_get, year, q, allow_404=is_current):
                index_rows += 1
                if row.source is None:
                    skipped_unmapped += 1
                    continue
                subject: ResolvedSubject | None = resolver(conn, row.cik)
                if subject is None:
                    skipped_unknown += 1
                    continue
                matched += 1
                try:
                    record_manifest_entry(
                        conn,
                        row.accession_number,
                        cik=row.cik,
                        form=row.form,
                        source=row.source,
                        subject_type=subject.subject_type,  # type: ignore[arg-type]
                        subject_id=subject.subject_id,
                        instrument_id=subject.instrument_id,
                        filed_at=row.filed_at,
                        accepted_at=row.accepted_at,
                        primary_document_url=row.primary_document_url,
                        is_amendment=row.is_amendment,
                    )
                    upserted += 1
                except ValueError as exc:
                    logger.warning(
                        "master_idx sweep %sQ%s: rejected accession=%s: %s",
                        year, q, row.accession_number, exc,
                    )
            conn.commit()
        except Exception as exc:  # noqa: BLE001 — per-quarter failure isolation
            logger.exception(
                "master_idx sweep %sQ%s: quarter failed; rolling back",
                year, q,
            )
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001 — ensure loop continues even if rollback chokes
                logger.exception(
                    "master_idx sweep %sQ%s: rollback raised; continuing",
                    year, q,
                )
            quarter_stats.append(
                QuarterStats(
                    year=year, quarter=q,
                    index_rows=index_rows,
                    matched_in_universe=matched,
                    upserted=0,  # rollback discarded any UPSERTs from this quarter
                    skipped_unmapped_form=skipped_unmapped,
                    skipped_unknown_subject=skipped_unknown,
                    failed=True,
                    error_detail=str(exc),
                )
            )
            continue

        logger.info(
            "master_idx sweep %sQ%s: index=%d matched=%d upserted=%d unmapped=%d unknown=%d",
            year, q, index_rows, matched, upserted, skipped_unmapped, skipped_unknown,
        )
        quarter_stats.append(
            QuarterStats(
                year=year, quarter=q,
                index_rows=index_rows,
                matched_in_universe=matched,
                upserted=upserted,
                skipped_unmapped_form=skipped_unmapped,
                skipped_unknown_subject=skipped_unknown,
            )
        )

    return MasterIdxSweepStats(quarters=quarter_stats)
```

### 5.3 `app/workers/scheduler.py` (EDIT)

- Add `JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP = "sec_master_idx_quarterly_sweep"`
  in the JOB_ constant block.
- Append a `ScheduledJob` entry to `SCHEDULED_JOBS` after the
  `sec_per_cik_poll` entry (sibling Layer-3 wiring section):

  ```python
  ScheduledJob(
      name=JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP,
      display_name="SEC master.idx quarterly sweep (G12)",
      source="sec_rate",
      description=(
          "G12 — cross-quarter discovery safety net. Weekly Sun "
          "05:15 UTC walks the current AND previous calendar "
          "quarter master.idx files (~250-300k rows / ~50 MB each), "
          "filters to (cik IN universe) + (form mapped to "
          "ManifestSource), UPSERTs any sec_filing_manifest rows the "
          "Atom + daily-index + per-CIK layers missed. Catches "
          "late-arriving amendments + tombstoned-CIK accessions. "
          ">1-quarter outage recovery is a Python REPL runbook — "
          "see spec §3.1."
      ),
      cadence=Cadence.weekly(weekday=6, hour=5, minute=15),
      catch_up_on_boot=False,
      prerequisite=_bootstrap_complete,
  ),
  ```

- Add invoker body after `sec_per_cik_poll()`:

  ```python
  def sec_master_idx_quarterly_sweep() -> None:
      """``_INVOKERS['sec_master_idx_quarterly_sweep']`` — G12 cross-quarter sweep.

      Walks ``[CQ, CQ-1]`` from the fire-time UTC moment. No operator
      params. >1-quarter outage backfill is a Python REPL runbook
      against ``run_master_idx_quarterly_sweep(conn, ..., quarters=...)``
      — see spec §3.1.
      """
      from app.jobs.sec_master_idx_quarterly_sweep import run_master_idx_quarterly_sweep

      with _tracked_job(JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP) as tracker:
          with (
              SecFilingsProvider(user_agent=settings.sec_user_agent) as sec,
              psycopg.connect(settings.database_url) as conn,
          ):
              stats = run_master_idx_quarterly_sweep(
                  conn,
                  http_get=_make_sec_http_get(sec),  # type: ignore[arg-type]
              )
              # No terminal conn.commit() — run_master_idx_quarterly_sweep
              # commits per quarter on success and rollbacks per quarter
              # on failure. Whatever state remains is already settled.
          tracker.row_count = stats.total_upserted
          logger.info(
              "sec_master_idx_quarterly_sweep: quarters=%d total_upserted=%d failed=%d",
              len(stats.quarters),
              stats.total_upserted,
              stats.failed_quarters,
          )
  ```

### 5.4 `app/jobs/runtime.py` (EDIT)

- Add `JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP` to the
  `app.workers.scheduler` import block.
- Add `sec_master_idx_quarterly_sweep` to the import block.
- Register `_INVOKERS[_scheduler.JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP] = _adapt_zero_arg(_scheduler.sec_master_idx_quarterly_sweep)`
  — zero-arg invoker; same pattern as `JOB_SEC_DAILY_INDEX_RECONCILE` /
  `JOB_SEC_PER_CIK_POLL`.

### 5.5 No schema migration

No new table. No alter on `sec_filing_manifest`. No `data_freshness_index`
row. Migration count unchanged. The walker is the canonical
"stateless cross-quarter sanity" pattern.

### 5.6 No `MANUAL_TRIGGER_JOB_SOURCES` entry needed

The new job_name appears in `SCHEDULED_JOBS` so `_build_job_name_to_source`
discovers it from pass 1. No manual-trigger registry entry required
(the registry is for jobs that exist in `_INVOKERS` but NOT in
`SCHEDULED_JOBS` — see `app/jobs/sources.py` MANUAL_TRIGGER block
comment).

### 5.7 No new `MANUAL_TRIGGER_JOB_METADATA` row

No operator-tunable params for this job (see §3.1). `params_metadata=()`
on the ScheduledJob. `MANUAL_TRIGGER_JOB_METADATA` is reserved for
jobs that DO have operator-tunable params without a ScheduledJob row —
not relevant here.

## 6. Test plan

All new tests pinned at module load + behavioural level. Five test
files added.

### 6.1 `tests/test_sec_full_index_provider.py` (NEW)

Unit tests for `app/providers/implementations/sec_full_index.py`:

1. `test_build_url_canonical_shape` — `_build_url(2025, 3) == "https://www.sec.gov/Archives/edgar/full-index/2025/QTR3/master.idx"`.
2. `test_build_url_rejects_out_of_range_quarter` — `_build_url(2025, 0)` + `_build_url(2025, 5)` raise `ValueError`.
3. `test_read_master_idx_happy_path` — feed a fake `http_get` returning a 200 + canned 3-row body; expect three `FilingIndexRow`s with correct accession/form/source mapping.
4. `test_read_master_idx_404_strict_default_raises` — fake returns 404 + no `allow_404` kwarg; expect `RuntimeError` matching `status=404`. **Pins the strict-by-default contract** (Codex 1a r1 HIGH-2).
5. `test_read_master_idx_404_allow_404_true_yields_empty` — fake returns 404 + `allow_404=True`; iterator yields no rows + logs the canonical "not yet published" line.
6. `test_read_master_idx_non_200_non_404_raises` — fake returns 503; expect `RuntimeError` matching `master.idx fetch failed: status=503`.
7. `test_default_filed_at_anchors_to_quarter_start` — fake returns body with a malformed date column; parser falls back to the quarter-start date, NOT today / NOT epoch.

Driver: existing daily-index parser is tested by
`tests/test_sec_daily_index_provider.py`; this file pins only the
**new** wrapper's contract.

### 6.2 `tests/test_sec_master_idx_quarterly_sweep.py` (NEW)

Integration tests for `app/jobs/sec_master_idx_quarterly_sweep.py`
against the per-worker `ebull_test_db` fixture
(`tests/fixtures/ebull_test_db.py`), NOT the dev DB. Sibling test
pattern: `tests/test_sec_daily_index_reconcile.py`. The fixture is
truncated between tests; seeding helpers in the plan §1 T7 block
re-insert `instruments` + `instrument_sec_profile` + filer rows on
each test.

1. `test_quarters_to_walk_mid_year` — `_quarters_to_walk(datetime(2026,5,17,UTC)) == [(2026, 2), (2026, 1)]`.
2. `test_quarters_to_walk_jan_rollover` — `_quarters_to_walk(datetime(2026,1,5,UTC)) == [(2026, 1), (2025, 4)]`.
3. `test_quarters_to_walk_quarter_boundaries` — covers each of Q1/Q2/Q3/Q4 first-day + last-day.
4. `test_happy_path_walks_two_quarters_and_upserts_in_universe_rows` — fake `http_get` returns canned bodies for both expected quarter URLs; injected resolver maps CIK `320193` → instrument 7 + CIK `1318605` → None; manifest gets exactly the CIK-7 row UPSERTed twice (once per quarter); out-of-universe row is `skipped_unknown_subject`.
5. `test_unmapped_form_skipped` — body row with form `S-1`; row counted in `skipped_unmapped_form`, never reaches subject_resolver.
6. `test_current_quarter_404_does_not_abort_previous_quarter` — fake returns 404 for CQ (allowed via `allow_404=True` driven by `is_current` branch) + 200 + 1 in-universe row for CQ-1; stats show CQ.index_rows=0 + CQ.failed=False + CQ-1.upserted=1 + total_upserted=1.
7. `test_previous_quarter_404_is_treated_as_failure` — fake returns 404 for CQ-1 (`allow_404=False` for non-current quarter) + 200 + 2 in-universe rows for CQ; stats show CQ.upserted=2 + CQ-1.failed=True + CQ-1.error_detail mentions `status=404`. **Pins the asymmetric 404 contract** (Codex 1a r1 HIGH-2).
8. `test_previous_quarter_failure_does_not_abort_current_quarter` — fake raises `RuntimeError("network down")` from `http_get` for CQ-1 + 200 + 2 in-universe rows for CQ; stats show CQ.upserted=2 + CQ-1.failed=True + CQ-1.error_detail contains `"network down"`.
9. `test_quarter_failure_rolls_back_partial_writes_in_that_quarter` — fake first quarter body has 3 in-universe rows; AFTER the 2nd UPSERT, the resolver's call for the 3rd raises a synthetic `psycopg.errors.OperationalError`. Assert: that quarter's `failed=True` + `upserted=0` (the 2 partial UPSERTs rolled back); the NEXT quarter still runs cleanly. **Pins the per-quarter txn isolation contract for BOTH `sec_filing_manifest` AND `data_freshness_index`** (Codex 1a r1 HIGH-1 + Codex 1b r1 MED-1) — assert zero rows in both tables for the failed-quarter accessions.
10. `test_successful_quarter_commits_before_next_quarter` — fake first quarter: 1 in-universe row + commit. Fake second quarter: raises mid-loop. After the run: first-quarter row durably committed in `sec_filing_manifest` AND the matching `data_freshness_index` row visible (per `record_manifest_entry`'s `seed_freshness_for_manifest_row` call); second-quarter `failed=True`. Manifest + freshness rows survive a fresh connection re-open.
11. `test_explicit_quarters_kwarg_overrides_default_window` — pass `quarters=[(2024, 1)]`; only that one URL is fetched; CQ + CQ-1 URLs NOT touched. Covers the operator-runbook backfill path.
12. `test_idempotency_re_walk_preserves_in_flight_status` — pre-seed a manifest row with `ingest_status='parsed'`; run sweep against fake body containing that accession; assert manifest row still `ingest_status='parsed'` + walker counts it as upserted (the UPSERT fires but ON CONFLICT preserves status). **Reads the post-UPSERT `ingest_status` directly from DB (not Python arithmetic on a captured timestamp) — complies with the time-monotonicity prevention-log entry.**
13. `test_stats_total_upserted_aggregates_across_quarters` — both quarters return 2 in-universe rows; `MasterIdxSweepStats.total_upserted == 4`; `failed_quarters == 0`.
14. `test_preloaded_resolver_priority_issuer_over_institutional_filer` — seed a CIK as BOTH `instrument_sec_profile` AND `institutional_filers`; assert `build_preloaded_subject_resolver(conn)(_, cik).subject_type == "issuer"`. Pins step 1 of the `setdefault` priority chain.
15. `test_preloaded_resolver_unknown_cik_returns_none` — sweep against a body whose CIKs are all out-of-universe; assert `subject_resolver` returns None for every row + `skipped_unknown_subject` is correct.
16. `test_preloaded_resolver_priority_institutional_over_blockholder` — seed a CIK as BOTH `institutional_filers` AND `blockholder_filers`; assert subject_type is `"institutional_filer"`. Pins step 2 of the priority chain (Codex 1b r1 HIGH-2).

### 6.3 `tests/test_sec_master_idx_scheduler_wiring.py` (NEW)

Static-AST + registry-membership tests guaranteeing every wiring
layer is present and consistent:

1. `test_job_name_constant_exported` — `JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP` in `app.workers.scheduler` + value is `"sec_master_idx_quarterly_sweep"`.
2. `test_scheduled_jobs_contains_master_idx_entry` — exactly one `ScheduledJob` with `name == JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP` in `SCHEDULED_JOBS`.
3. `test_master_idx_scheduled_job_cadence_and_gating` — the ScheduledJob's `source == "sec_rate"`, `cadence.kind == "weekly"`, `cadence.weekday == 6`, `cadence.hour == 5`, `cadence.minute == 15`, `catch_up_on_boot is False`, `prerequisite is _bootstrap_complete`, `exempt_from_universal_bootstrap_gate is False`.
4. `test_invoker_registered_in_runtime` — `_INVOKERS[JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP]` is callable AND `_INVOKERS[...].__wrapped__ is sec_master_idx_quarterly_sweep`. The identity is checked via the `__wrapped__` attribute set by `_adapt_zero_arg`; DO NOT compare against a fresh `_adapt_zero_arg(...)` call (each invocation builds a new closure with no identity to the registered one).
5. `test_source_for_job_name_resolves_to_sec_rate` — `source_for(JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP) == "sec_rate"`. Pins the `JobLock` acquisition path against the universal-gate supersession trap (`feedback_universal_gate_supersession.md`).

### 6.4 `tests/test_universal_gate_carve_out.py` (EDIT — invariant addition)

The existing allow-list asserts that every `ScheduledJob` with
`exempt_from_universal_bootstrap_gate=True` meets all four eligibility
criteria. Add a positive assertion that the new job
`JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP` is **NOT** in the exempt set
(belt-and-braces against accidental opt-in during follow-up edits).

### 6.5 `tests/test_layer_123_wiring.py` (EDIT — invariant addition)

The existing file pins Layer-1 (Atom), Layer-2 (daily-index), Layer-3
(per-CIK) registration in SCHEDULED_JOBS. Add a Layer-4 row asserting:

```python
def test_layer4_master_idx_quarterly_sweep_registered():
    jobs = {j.name: j for j in SCHEDULED_JOBS}
    job = jobs[JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP]
    assert job.source == "sec_rate"
    assert job.cadence.kind == "weekly"
    assert job.cadence.weekday == 6  # Sunday
    assert job.cadence.hour == 5
    assert job.cadence.minute == 15
    assert job.catch_up_on_boot is False
    assert job.prerequisite is not None
    assert job.exempt_from_universal_bootstrap_gate is False
```

The "Layer 4" label is informal — keeps the test file's mental
model coherent (existing tests are `test_layerN_..._registered`).

## 7. Risks + mitigations

| Risk | Mitigation |
|---|---|
| **Re-walking ~50 MB weekly is wasteful.** | Walker is stateless idempotent; bandwidth cost (~100 MB/week from SEC over 10 req/s budget) is two requests + ~30 s wall-clock against a budget that already supports the manifest worker drain. A re-walk of unchanged quarter incurs ~250-300k row parses + ~250-300k O(1) resolver lookups + ~Nₘ (~10-30k) `record_manifest_entry` UPSERT roundtrips — cheap, but the cost is real and called out honestly in §3.2 to head off future-review re-litigation. |
| **Layer-3 per-CIK poll already covers most of the cross-quarter cases.** | True — for in-universe CIKs that are still emitting submissions.json updates. The walker exists ONLY for tombstoned-CIK / deactivated-CIK / merged-CIK / late-amendment scenarios where Layer 3 cannot help. Operator value-add is the long-tail safety net, not the headline coverage. |
| **Empty/partial universe at first run.** | `prerequisite=_bootstrap_complete` blocks fires until bootstrap completes; sibling pattern `sec_per_cik_poll`. Layer-2 daily-index opted into the carve-out because daily granularity makes a missed fire permanent; weekly granularity does not. |
| **SEC quarter-edge 404 (current quarter not yet published).** | `read_master_idx(..., allow_404=True)` for current-quarter only; the walker job sets `allow_404=is_current` where `is_current = (year, q) == _current_calendar_quarter(now)`. Test 6.2.6 pins the current-quarter case; test 6.2.7 pins the previous-quarter strict-failure case. |
| **Per-quarter txn state cascade.** | Each quarter inside `try`/`except` with `conn.rollback()` on failure + `conn.commit()` on success. Quarter-N failure cannot taint quarter-N+1's tx. Tests 6.2.9 + 6.2.10 pin this. (Codex 1a r1 HIGH-1.) |
| **>1-quarter outage permanent gap.** | Explicitly owned in §3.1: scheduled walker covers `[CQ, CQ-1]` only; >1-quarter recovery is the Python REPL runbook via the `quarters` kwarg. ScheduledJob description references the runbook. (Codex 1a r1 HIGH-3.) |
| **Time-monotonicity prevention-log entry.** | The walker has no monotonic timestamp column to assert on (no `last_seen`, no `last_known_filing_id`). Idempotency test 6.2.12 reads the actual post-UPSERT `ingest_status` from the DB via SQL (not Python arithmetic on a captured timestamp). Compliant with prevention-log §"Time-monotonicity assertions". |
| **Subject_resolver hot path (~250k rows × 2 quarters = ~500k lookups).** | `build_preloaded_subject_resolver(conn)` runs three eager `SELECT cik...` queries once per fire, materialises a `dict[str, ResolvedSubject]` (~17k entries, ~1.5 MB), returns an O(1) closure resolver. Replaces the per-row 3-table lookup. (Codex 1a r1 MED-2.) |
| **Universal-gate supersession recurrence (`feedback_universal_gate_supersession.md`).** | The new job is deliberately NOT exempt — universal gate applies. Test 6.3.5 + 6.4 pin this. Avoiding the carve-out is the right default. |
| **Cohort-counts trap (`feedback_pr_review_efficiency.md` lesson from G8).** | The walker has no "rows-per-quarter" PK granularity decision — `sec_filing_manifest`'s PK is already `accession_number`, set in stone. No new schema PK to mis-count. |

## 8. Open questions

None. All three design calls (cadence, persistence target, watermark
shape) resolved at design time. Each is justified inline at §3.1 /
§3.2 / §3.3 with sibling-precedent + dominated-alternative analysis.

## 9. Acceptance

The PR is mergeable when:

1. New `app/providers/implementations/sec_full_index.py` exists, registered by §5.1.
2. New `app/jobs/sec_master_idx_quarterly_sweep.py` exists per §5.2.
3. `app/workers/scheduler.py` has the new JOB_ constant, ScheduledJob entry, and invoker body per §5.3.
4. `app/jobs/runtime.py` registers the invoker per §5.4.
5. All five test files (§6.1-6.5) pass.
6. Local `uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest` clean.
7. Codex 2 (pre-push) clean against the branch.
8. ETL clauses #8-#12: smoke-tested empirically — invoke the new
   `_INVOKERS['sec_master_idx_quarterly_sweep']` against the dev DB
   for the most-recent published quarter. Cohort-correct panel:
   - **Issuer-side (AAPL CIK `0000320193`, MSFT CIK `0000789019`):**
     assert ≥1 manifest row UPSERTed per CIK across the
     `sec_8k` / `sec_10k` / `sec_10q` / `sec_def14a` / `sec_form4`
     source mappings — these are the forms an operating issuer files.
     Cohort note: AAPL does NOT emit `13F-HR` (Codex 1a r1 MED-3) —
     13F-HR is filer-scoped, not issuer-scoped. The smoke panel does
     not assert AAPL→13F-HR.
   - **Institutional filer-side (Berkshire CIK `0001067983`,
     BlackRock CIK `0001364742`):** assert ≥1 manifest row UPSERTed
     per CIK in `sec_13f_hr` provided the filer is seeded in
     `institutional_filers` on the dev DB. If the filer is not
     seeded, the row falls to `skipped_unknown_subject` (expected) —
     PR description records which filers were seeded vs not.
   PR description records the panel + invocation + outcome.
9. Matrix updates:
   - `.claude/skills/data-engineer/etl-endpoint-coverage.md` §4
     "Full-index `master.idx` quarterly" row: ❌ GAP → ✅ WIRED.
   - `.claude/skills/data-engineer/etl-endpoint-coverage.md` §7 G12
     row: OPEN (low) → ✅ CLOSED 2026-05-17.
   - `.claude/skills/data-sources/sec-edgar.md` §1 "Indexes + Atom feeds"
     row gets a `Consumed by: sec_master_idx_quarterly_sweep` annotation
     (parallel to the daily-index annotation).
10. `[[us-source-coverage]]` memory updated with G12 closure + PR SHA.

## 10. Sibling-precedent index

| Need | Sibling file:line | Why apt |
|---|---|---|
| Pipe-delimited index parse | `app/providers/implementations/sec_daily_index.py:80` `parse_daily_index` | Same byte format, same row shape, same skipping rules — reuse byte-for-byte |
| URL builder + 404 tolerance | `app/providers/implementations/sec_daily_index.py:129` `read_daily_index` | Same SEC archive convention; G12 wrapper adds the `allow_404` knob so the strict-by-default contract holds |
| Subject resolver type | `app/jobs/sec_atom_fast_lane.py:35-46` `SubjectResolver` / `ResolvedSubject` | Same closure shape, same priority chain; we replace the per-row default with an eagerly-preloaded variant |
| Manifest UPSERT | `app/services/sec_manifest.py` `record_manifest_entry` | Same UPSERT-with-ingest_status-preserved contract; same kwargs |
| Per-fire stats dataclass | `app/jobs/sec_daily_index_reconcile.py:37` `ReconcileStats` | Same shape — we extend to per-quarter list with `failed` + `error_detail` |
| ScheduledJob entry | `app/workers/scheduler.py:1062-1080` `JOB_SEC_PER_CIK_POLL` | Same source=`sec_rate`, same `prerequisite=_bootstrap_complete`, same `catch_up_on_boot=False`; the closest sibling |
| Invoker body shape | `app/workers/scheduler.py:4547-4575` `sec_per_cik_poll()` | Same shape — `_tracked_job` + provider + connection + stats; copy-pattern (zero-arg) |
| Per-quarter failure isolation | `app/services/top_filer_discovery.py:194-200` `aggregate_top_filers` | Same per-quarter try/except + logger.exception + continue, extended with explicit `conn.rollback()` for DB-error-safe iteration |
| Stateless idempotent walk | `app/jobs/sec_daily_index_reconcile.py:46` `run_daily_index_reconcile` | Same idempotency property — `record_manifest_entry`'s ON CONFLICT preserves in-flight ingest_status |
