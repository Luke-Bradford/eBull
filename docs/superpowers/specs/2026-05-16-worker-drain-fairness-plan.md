# Manifest worker drain fairness — implementation plan

> Status: **DRAFT 2026-05-16** — pending Codex pre-spec 1b + operator signoff.
>
> Spec: `docs/superpowers/specs/2026-05-16-worker-drain-fairness.md` (Codex 1a CLEAN round 8).
> Issue: **#1179**. Branch: `fix/1179-worker-drain-perf`.
> Output preference (CLAUDE.md): schema → service logic → tests → integration glue. **No schema in this PR.**

## 1. Task decomposition

| # | Task | Scope (files) | Depends on | Deliverable |
|---|---|---|---|---|
| T1 | `compute_quotas` helper + `_TICK_COUNTER` | `app/jobs/sec_manifest_worker.py` (new private helpers near top of module) | — | `compute_quotas(sources, max_rows, tick_id) -> dict[ManifestSource, int]` + `_TICK_COUNTER = itertools.count(0)` module-global. Unit-callable shape. |
| T2 | `iter_pending`/`iter_retryable` stable tie-break | `app/services/sec_manifest.py` (4 SQL bodies) | — | Append `, accession_number ASC` to both queries' `ORDER BY`. No other shape change. |
| T3 | `iter_pending_topup` + `iter_retryable_topup` | `app/services/sec_manifest.py` (new free functions) | T2 | Two free functions matching the SQL shapes in spec §3.x. Both take `(conn, *, sources, exclude_accessions, limit)` with explicit `%s::text[]` casts. |
| T4 | `WorkerStats.processed_by_source` | `app/jobs/sec_manifest_worker.py` (`WorkerStats` dataclass) | — | New field `processed_by_source: dict[ManifestSource, int] = field(default_factory=dict)`. Populated in `run_manifest_worker` dispatch loop. |
| T5 | `run_manifest_worker` rewrite — Phase A/B | `app/jobs/sec_manifest_worker.py::run_manifest_worker` | T1 + T2 + T3 + T4 | New `tick_id: int \| None = None` kw-only parameter. `source=None` path: compute quotas, Phase A per-source slice (pending + retryable), Phase B top-up (pending then retryable). `source is not None` path: unchanged shape. Audit invariant (#938) + parser exception handling unchanged. |
| T6 | Tests | `tests/test_sec_manifest_worker.py` (extend) | T1-T5 | 10 cases from spec §6. All pass under `uv run pytest -n0 tests/test_sec_manifest_worker.py`. |
| T7 | Skill docs | `.claude/skills/data-engineer/SKILL.md` §11 + `.claude/skills/data-sources/sec-edgar.md` §11 | T5 | One paragraph each explaining fairness shape + cross-link to spec / #1179. |
| T8 | `sec_manifest_worker_tick` wrapper | `app/workers/scheduler.py::sec_manifest_worker_tick` | T5 | Pass `tick_id=None` (uses module-global fallback). Logger line includes per-source breakdown. |

**Total tasks: 8.** Dispatch order:

```
T1, T2, T4         (independent helpers + dataclass field + ordering tweak — parallelisable)
  ↓
T3                 (depends on T2 ordering for shape symmetry)
  ↓
T5                 (depends on T1 + T2 + T3 + T4)
  ↓
T6                 (covers T1-T5 contract surface; cannot pre-stub)
  ↓
T7, T8             (docs + scheduler wrapper — independent of each other)
```

Codex 1b: T6 covers the full contract surface so it cannot land incrementally per-task without stubbing helpers; serial execution is the natural shape (mirrors #1175 plan).

## 2. Per-task contracts

### T1 — `compute_quotas` helper + `_TICK_COUNTER`

**File:** `app/jobs/sec_manifest_worker.py`.

**Add module-level imports + globals near top:**

```python
import itertools
...

# Tick counter for Phase A `lead` rotation. Module-global because
# the production scheduled tick wrapper passes `tick_id=None` and
# the worker must advance by exactly +1 per call regardless of
# scheduler cadence. Tests inject `tick_id` explicitly so the
# counter is irrelevant under test.
_TICK_COUNTER = itertools.count(0)


def compute_quotas(
    sources: Sequence[ManifestSource],
    max_rows: int,
    tick_id: int,
) -> dict[ManifestSource, int]:
    """Per-source quota with tick-rotated lead.

    Returns a {source: slot_count} mapping such that
    sum(quotas.values()) == max_rows when sources is non-empty.
    Rotation: lead = tick_id % len(sources); the first
    `max_rows mod n` sources at rotated index get `base + 1` slots,
    the rest get `base` (`base = max_rows // n`). Every source
    receives a Phase A slot within `n - remainder + 1` consecutive
    ticks regardless of scheduler cadence (independent of
    `gcd(tick_step, n)`).
    """
    n = len(sources)
    if n == 0:
        return {}
    base, remainder = divmod(max_rows, n)
    lead = tick_id % n
    return {
        s: base + (1 if (i - lead) % n < remainder else 0)
        for i, s in enumerate(sources)
    }
```

**Direct unit tests for `compute_quotas`** (Codex 1b round-2
WARNING — full helper coverage with hardcoded expected maps):

```python
def test_compute_quotas_max_rows_greater_than_n():
    # n=3, max_rows=10, tick_id=0. base=3, remainder=1.
    # Rotated lead=0 → first 1 source gets +1.
    assert compute_quotas(
        sources=("sec_form4", "sec_n_csr", "sec_def14a"),
        max_rows=10, tick_id=0,
    ) == {"sec_form4": 4, "sec_n_csr": 3, "sec_def14a": 3}


def test_compute_quotas_max_rows_less_than_n():
    # n=4, max_rows=2, tick_id=0. base=0, remainder=2.
    # First 2 sources at rotated index get 1; rest get 0.
    assert compute_quotas(
        sources=("sec_form3", "sec_form4", "sec_form5", "sec_8k"),
        max_rows=2, tick_id=0,
    ) == {"sec_form3": 1, "sec_form4": 1, "sec_form5": 0, "sec_8k": 0}


def test_compute_quotas_rotated_tick_id():
    # Same n=4, max_rows=2, tick_id=1. lead=1.
    # Rotated indices: idx 0 → (0-1)%4=3, idx 1 → 0, idx 2 → 1, idx 3 → 2.
    # remainder=2 → rot in {0, 1} gets +1 → indices 1 + 2.
    assert compute_quotas(
        sources=("sec_form3", "sec_form4", "sec_form5", "sec_8k"),
        max_rows=2, tick_id=1,
    ) == {"sec_form3": 0, "sec_form4": 1, "sec_form5": 1, "sec_8k": 0}


def test_compute_quotas_empty_sources():
    assert compute_quotas(sources=(), max_rows=100, tick_id=0) == {}


def test_compute_quotas_sum_invariant():
    # Total Phase A budget always equals max_rows for any tick_id.
    sources_ms: tuple[ManifestSource, ...] = (
        "sec_form3", "sec_form4", "sec_form5",
        "sec_13d", "sec_13g", "sec_8k", "sec_def14a",
    )
    for tick_id in range(20):
        quotas = compute_quotas(sources_ms, max_rows=23, tick_id=tick_id)
        assert sum(quotas.values()) == 23


def test_compute_quotas_rotation_covers_every_source():
    # Under base=0 regime, ticks 0..(n-remainder) must visit every
    # source at least once.
    sources_ms: tuple[ManifestSource, ...] = (
        "sec_form3", "sec_form4", "sec_form5",
        "sec_13d", "sec_13g", "sec_13f_hr",
        "sec_def14a", "sec_n_port", "sec_n_csr",
        "sec_10k", "sec_10q", "sec_8k",
    )
    n, max_rows = len(sources_ms), 8                    # base=0, remainder=8
    touched: set[ManifestSource] = set()
    for tick_id in range(n - max_rows + 1):             # 5 ticks
        quotas = compute_quotas(sources_ms, max_rows, tick_id)
        touched.update(s for s, q in quotas.items() if q > 0)
    assert touched == set(sources_ms)
```

All tests use real `ManifestSource` literals (Codex 1b round-3
WARNING — `Sequence[ManifestSource]` is the helper type; pyright
gates flag string placeholders).

Cases 1, 6, 9 in §6 of the spec are integration-level checks; the
direct unit tests above pin the helper contract with hardcoded
expected maps so a future refactor cannot drift the math.

### T2 — `iter_pending` / `iter_retryable` stable tie-break

**File:** `app/services/sec_manifest.py`.

**Edit 4 SQL bodies** (existing `iter_pending` source=None / source=X
branches at `:506` / `:522`; `iter_retryable` source=None / source=X
branches at `:556` / `:574`). Append `, accession_number ASC` to
each `ORDER BY`. No other change.

Diff shape:

```
-                ORDER BY filed_at ASC
+                ORDER BY filed_at ASC, accession_number ASC
```

```
-                ORDER BY COALESCE(next_retry_at, last_attempted_at, filed_at) ASC
+                ORDER BY COALESCE(next_retry_at, last_attempted_at, filed_at) ASC,
+                         accession_number ASC
```

**Regression sweep**: grep for existing tests that assert ordering
of `iter_pending` / `iter_retryable` output. If any pin specific
accession orderings for same-`filed_at` rows, verify the new
`accession_number ASC` tie-break is consistent with the assertion
(or update the test fixture).

### T3 — `iter_pending_topup` + `iter_retryable_topup`

**File:** `app/services/sec_manifest.py`.

**New imports** at top of `app/services/sec_manifest.py` (verify
existing `from collections.abc import ...` block; ADD `Sequence`
if absent):

```python
from collections.abc import Iterator, Sequence   # ADD Sequence
```

**Add two free functions** (sibling to `iter_pending` /
`iter_retryable`):

```python
def iter_pending_topup(
    conn: psycopg.Connection[Any],
    *,
    sources: Sequence[ManifestSource],
    exclude_accessions: Sequence[str],
    limit: int,
) -> Iterator[ManifestRow]:
    """Global oldest-pending top-up, scoped to registered sources +
    excluding accessions already picked in Phase A.

    Used by `run_manifest_worker` Phase B to fill leftover budget
    after per-source quotas. Both array params are explicitly cast
    `%s::text[]` so psycopg3 type inference handles empty lists.
    Empty `sources` returns immediately without SQL.
    """
    if not sources:
        return
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT accession_number, cik, form, source,
                   subject_type, subject_id, instrument_id,
                   filed_at, accepted_at, primary_document_url,
                   is_amendment, amends_accession,
                   ingest_status, parser_version, raw_status,
                   last_attempted_at, next_retry_at, error
            FROM sec_filing_manifest
            WHERE ingest_status = 'pending'
              AND source = ANY(%s::text[])
              AND accession_number != ALL(%s::text[])
            ORDER BY filed_at ASC, accession_number ASC
            LIMIT %s
            """,
            (list(sources), list(exclude_accessions), limit),
        )
        for row in cur.fetchall():
            yield ManifestRow(**row)


def iter_retryable_topup(
    conn: psycopg.Connection[Any],
    *,
    sources: Sequence[ManifestSource],
    exclude_accessions: Sequence[str],
    limit: int,
) -> Iterator[ManifestRow]:
    """Global oldest-retryable top-up, mirroring `iter_pending_topup`.

    Predicate matches `iter_retryable`:
    `ingest_status='failed' AND (next_retry_at IS NULL OR
    next_retry_at <= NOW())`.
    """
    if not sources:
        return
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT accession_number, cik, form, source,
                   subject_type, subject_id, instrument_id,
                   filed_at, accepted_at, primary_document_url,
                   is_amendment, amends_accession,
                   ingest_status, parser_version, raw_status,
                   last_attempted_at, next_retry_at, error
            FROM sec_filing_manifest
            WHERE ingest_status = 'failed'
              AND (next_retry_at IS NULL OR next_retry_at <= NOW())
              AND source = ANY(%s::text[])
              AND accession_number != ALL(%s::text[])
            ORDER BY COALESCE(next_retry_at, last_attempted_at, filed_at) ASC,
                     accession_number ASC
            LIMIT %s
            """,
            (list(sources), list(exclude_accessions), limit),
        )
        for row in cur.fetchall():
            yield ManifestRow(**row)
```

### T4 — `WorkerStats.processed_by_source`

**File:** `app/jobs/sec_manifest_worker.py` (`WorkerStats` dataclass
at `:136`).

**Add field**:

```python
@dataclass(frozen=True)
class WorkerStats:
    rows_processed: int
    parsed: int
    tombstoned: int
    failed: int
    skipped_no_parser: int
    raw_payload_violations: int = 0
    skipped_no_parser_by_source: dict[ManifestSource, int] = field(default_factory=dict)
    # NEW (#1179): per-source dispatched count. Excludes
    # `skipped_no_parser` rows (already counted in
    # `skipped_no_parser_by_source`). Sum equals
    # `rows_processed - skipped_no_parser`.
    processed_by_source: dict[ManifestSource, int] = field(default_factory=dict)
```

Field is populated incrementally inside `run_manifest_worker`'s
dispatch loop: bump on every row that exits via `parsed` /
`tombstoned` / `failed` / `raw_payload_violations` transition; do
NOT bump on `skipped_no_parser`.

### T5 — `run_manifest_worker` rewrite

**File:** `app/jobs/sec_manifest_worker.py::run_manifest_worker`.

**New imports** at top of `app/jobs/sec_manifest_worker.py`:

```python
import itertools                                       # NEW (#1179)
from collections.abc import Callable, Sequence         # ADD Sequence
...
from app.services.sec_manifest import (
    IngestStatus,
    ManifestRow,
    ManifestSource,
    iter_pending,
    iter_pending_topup,                                # NEW (#1179)
    iter_retryable,
    iter_retryable_topup,                              # NEW (#1179)
    transition_status,
)
```

**Signature change** (add `tick_id`):

```python
def run_manifest_worker(
    conn: psycopg.Connection[Any],
    *,
    source: ManifestSource | None = None,
    max_rows: int = 100,
    now: datetime | None = None,
    tick_id: int | None = None,   # NEW (#1179)
) -> WorkerStats:
```

**Body shape**:

```python
# Codex 1b BLOCKING — normalise `now` BEFORE branching so
# `_dispatch_rows` always has a tz-aware UTC value for the
# parser-exception + raw-payload-violation backoff math
# (`now + _backoff_for(0)`).
if now is None:
    now = datetime.now(tz=UTC)

if source is not None:
    rows: list[ManifestRow] = list(
        iter_pending(conn, source=source, limit=max_rows)
    )
    if len(rows) < max_rows:
        rows.extend(iter_retryable(
            conn, source=source, limit=max_rows - len(rows)
        ))
    return _dispatch_rows(conn, rows, now=now)

# Fairness path — Phase A per-source slice + Phase B top-up.
sources = sorted(registered_parser_sources())
n = len(sources)
if n == 0:
    return WorkerStats(
        rows_processed=0, parsed=0, tombstoned=0, failed=0,
        skipped_no_parser=0,
    )

if tick_id is None:
    tick_id = next(_TICK_COUNTER)
quotas = compute_quotas(sources, max_rows, tick_id)

rows: list[ManifestRow] = []

# Phase A — per-source quota slice (pending first, then retryable).
for s in sources:
    q = quotas[s]
    if q == 0:
        continue
    per_source: list[ManifestRow] = list(
        iter_pending(conn, source=s, limit=q)
    )
    if len(per_source) < q:
        per_source.extend(iter_retryable(
            conn, source=s, limit=q - len(per_source),
        ))
    rows.extend(per_source)

# Phase B — top-up pending, then retryable, both scoped to
# registered sources, excluding Phase A picks.
seen: set[str] = {r.accession_number for r in rows}
remaining = max_rows - len(rows)
if remaining > 0:
    topup_pending = list(iter_pending_topup(
        conn,
        sources=sources,
        exclude_accessions=sorted(seen),
        limit=remaining,
    ))
    rows.extend(topup_pending)
    seen.update(r.accession_number for r in topup_pending)
    remaining = max_rows - len(rows)
if remaining > 0:
    topup_retryable = list(iter_retryable_topup(
        conn,
        sources=sources,
        exclude_accessions=sorted(seen),
        limit=remaining,
    ))
    rows.extend(topup_retryable)

return _dispatch_rows(conn, rows, now=now)
```

**Refactor — extract dispatch loop** into `_dispatch_rows(conn,
rows, *, now: datetime)` (note: `now` is non-optional in the
private helper; caller normalises it). The helper hosts the
existing for-loop body from `:193-282` + the `WorkerStats`
construction at `:294`.

**Codex 1b BLOCKING — `processed_by_source` single-bump invariant**:
the per-source counter must be incremented EXACTLY ONCE per
dispatched row, regardless of outcome (parsed / tombstoned / failed
/ raw-payload-violation). Concrete placement: bump
`processed_by_source[row.source] += 1` IMMEDIATELY AFTER the
`spec is None` skip-check passes, BEFORE invoking `spec.fn(...)`.
This is the single dispatch-entry point — any row that reaches it
will exit via parsed / tombstoned / failed / raw-payload-violation
exactly once. Result: `sum(processed_by_source.values()) ==
rows_processed - skipped_no_parser` invariant always holds.

```python
for row in rows:
    spec = _PARSERS.get(row.source)
    if spec is None:
        ...  # existing debug + skipped_no_parser_by_source bump
        skipped += 1
        skipped_by_source[row.source] += 1
        continue

    # NEW (#1179): single dispatched-row counter bump, BEFORE
    # parser invocation. Every code path below this line is a
    # dispatched-row outcome.
    processed_by_source[row.source] += 1

    try:
        outcome = spec.fn(conn, row)
    except Exception as exc:
        ...  # failed transition + failed += 1 — UNCHANGED
        continue

    # raw-payload-violation block UNCHANGED; failed += 1 +
    # raw_violations += 1 still both fire because they are
    # ORTHOGONAL counters (raw_violations is a sub-classifier of
    # failed). processed_by_source already bumped once above.
    ...
```

**Audit invariant (#938 raw_payload_violations) preserved**:
`requires_raw_payload=True` parsers still get the "parsed + absent"
violation check intact. The fairness changes only affect WHICH rows
the dispatch loop sees, not WHAT happens to each row.

**Codex 1b BLOCKING — contract change for `source=None` + zero
registered parsers**: under the new fairness path, the early-return
at `n == 0` means `WorkerStats.skipped_no_parser` is `0` for the
unscoped tick when NO sources are registered. This is intentional
— under the new fairness model, the unscoped tick ONLY queries rows
from `registered_parser_sources()`. Unregistered-source rows
(`sec_xbrl_facts`, `finra_short_interest`) are filtered out by
Phase A SQL (`source=s` per-source query) + Phase B SQL
(`source = ANY(%s::text[])`). Operator visibility for "X has
pending rows but no parser" remains via
`/coverage/manifest-parsers` (#935 §5). The dispatch-loop skip
mechanism still fires when a source IS registered at SQL time but
the registry has been mutated mid-tick (test-only). T6 updates
`tests/test_sec_manifest_worker.py::test_unregistered_source_emits_warning_with_breakdown`
to reflect the new contract (rows for unregistered sources are not
fetched; assertion flipped to confirm zero skip count).

### T6 — Tests

**File:** `tests/test_sec_manifest_worker.py`.

**Setup helpers**:

```python
@pytest.fixture
def seeded_manifest(ebull_test_conn):
    """Returns a helper that seeds N pending rows for a given
    (source, base_filed_at). Each row gets accession
    f'{source}-{filed_at:%Y%m%d}-{seq:05d}' (deterministic).
    """
    def _seed(source, n, *, base_filed_at, status="pending",
              next_retry_at=None):
        ...
    return _seed


def _register_fake(captures, source):
    """Register a fake parser that appends (accession, source) to
    `captures` and returns ParseOutcome(status='parsed',
    parser_version='fake-v1', raw_status='stored')."""
    ...
```

**Pin xdist group** at file top so dependency-override mutations
don't bleed across files (prevention-log #1159):

```python
pytestmark = pytest.mark.xdist_group("test_sec_manifest_worker")
```

Cases are the 10 enumerated in spec §6, each implemented as a
discrete `def test_<case_name>(...)`. Each test:

1. Calls `clear_registered_parsers()` in setup.
2. Registers fakes via `_register_fake(...)`.
3. Calls `run_manifest_worker(conn, ..., tick_id=<explicit>)`
   per case spec (case 9 loops 0..4; cases 1/2/4/6/8 pass
   `tick_id=0`).
4. Asserts on `WorkerStats.processed_by_source` + the captured
   dispatch list.

**Case 10** test calls `iter_pending_topup` and
`iter_retryable_topup` directly with `exclude_accessions=[]` to
confirm the explicit cast handles empty arrays.

### T7 — Skill docs

**File 1:** `.claude/skills/data-engineer/SKILL.md` §11 manifest
worker block.

**Append paragraph** (≤80 words):

```markdown
**Fairness contract (#1179)**: the unscoped `run_manifest_worker
(source=None)` tick allocates a per-source quota
(`compute_quotas(sources, max_rows, tick_id)`) then tops up
residual budget against the global oldest tail (pending first,
retryable second). Both top-up queries are scoped to registered
sources only; unregistered (`sec_xbrl_facts`,
`finra_short_interest`) cannot consume residual. `tick_id` advances
by +1 per tick (module-global counter; tests inject explicitly).
```

**File 2:** `.claude/skills/data-sources/sec-edgar.md` §11 manifest
worker dispatch block.

**Append paragraph** with the same shape, cross-linked to the spec
+ #1179 + plan doc.

### T8 — `sec_manifest_worker_tick` wrapper

**File:** `app/workers/scheduler.py::sec_manifest_worker_tick` at
`:3694`.

**Edits**:

1. Call `run_manifest_worker(conn, source=None, max_rows=100,
   tick_id=None)` — explicit `tick_id=None` to document the
   counter-fallback contract.
2. Extend the logger line to include
   `processed_by_source=stats.processed_by_source` for operator
   observability:

```python
logger.info(
    "sec_manifest_worker tick: processed=%d parsed=%d "
    "tombstoned=%d failed=%d skipped_no_parser=%d "
    "processed_by_source=%s",
    stats.rows_processed,
    stats.parsed,
    stats.tombstoned,
    stats.failed,
    stats.skipped_no_parser,
    dict(sorted(stats.processed_by_source.items())),
)
```

No other change to the wrapper.

## 3. Cross-cutting contracts

### 3.1 No schema change

No migration. No new column. No new index. Existing partial index
`idx_manifest_status_retry (ingest_status, next_retry_at) WHERE
ingest_status IN ('pending', 'failed')` covers all 4 read paths
(`iter_pending`, `iter_retryable`, top-up pending, top-up
retryable). If `EXPLAIN ANALYZE` at pre-push shows sort-scan cost
exceeds ~5ms per query, file a separate ticket for
`(ingest_status, source, filed_at, accession_number)` covering
index; not in scope for this PR.

### 3.2 Process topology (#719)

The fairness logic lives entirely in the jobs process.
`run_manifest_worker(...)` is invoked ONLY from
`sec_manifest_worker_tick` in `app/workers/scheduler.py`
(jobs-process side). The FastAPI side imports
`app/jobs/sec_manifest_worker.py` for registry visibility only:
`app/api/coverage.py:367` calls `registered_parser_sources()` and
`app/main.py` triggers parser-module imports that call
`register_parser`. Neither path advances the `_TICK_COUNTER` —
nothing in the API ever calls `run_manifest_worker`. The
module-global counter is therefore safe across both processes (one
counter per process; only the jobs process advances it). ✓

### 3.3 Service-accepts-conn-must-not-commit (review-prevention)

`run_manifest_worker(conn, ...)` accepts a caller-owned connection.
The function must NOT call `conn.commit()` directly — the existing
`sec_manifest_worker_tick` wrapper at `app/workers/scheduler.py:3712`
owns the commit boundary via `conn.commit()` after the function
returns. Verified: current `run_manifest_worker` body has no
`commit()` call; new fairness body preserves this.

### 3.4 Determinism + idempotency

- Phase A per-source slices: deterministic via
  `iter_pending(source=s, limit=q) ORDER BY filed_at ASC,
  accession_number ASC`.
- Phase B top-up: deterministic via the same ordering against the
  global tail, scoped to registered sources, excluding Phase A
  picks (`!= ALL(%s::text[])`).
- Tick rotation: deterministic when `tick_id` is injected; tests
  pin `tick_id=0` for reproducibility.
- Re-running a tick before commit (caller-error scenario): the
  fairness logic does not flip row state. State transitions happen
  inside the dispatch loop via `transition_status` — same as today.
  Re-running before commit would re-pick the same Phase A rows
  (PK-deduplicated by the dispatch loop's `transition_status`
  UPDATE).

### 3.5 Raw payload audit invariant (#938)

`requires_raw_payload=True` parsers still get the violation check
for `parsed + raw_status='absent'`. The fairness changes only
affect row selection, not row processing. Audit log line
unchanged.

### 3.6 #1131 transient-vs-deterministic upsert exception split

Untouched. Parser exception handling at
`run_manifest_worker:207-221` (parser raises → `transition_status`
with `failed` + 1h backoff) is preserved verbatim.

### 3.7 Empty-array cast hazard

All array params use explicit `%s::text[]`. Verified by §6 case 10
direct invocation. Empty `sources` short-circuits in
`iter_pending_topup` / `iter_retryable_topup` before SQL fires.

## 4. Pre-push verification

Per CLAUDE.md pre-push checklist:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest -n0 tests/test_sec_manifest_worker.py
```

All four gates. Additionally:

- `EXPLAIN ANALYZE` on both top-up queries against dev DB. Confirm
  ≤5ms per query; record `Buffers: shared hit=X` for the PR
  description.
- Smoke trigger: `POST /jobs/sec_manifest_worker/run` (no body) on
  dev DB. Compare pre/post `sec_filing_manifest GROUP BY source`
  pending counts. Expected: every source with non-zero pending
  rows shows a drop. Record in PR description per CLAUDE.md DoD
  clauses 8-12.
- Codex 2 review: `codex exec review` against branch diff. Fix
  before push.

## 5. Smoke panel + verification (DoD clauses 8-12)

The fix is operator-visible via the per-source pending counts on
the live `sec_filing_manifest` table.

| Source | Pre-fix pending (dev) | Pre-fix per-tick drain | Post-fix per-tick drain |
|---|---|---|---|
| `sec_form4` | ~2.5M | ~100 (dominates) | ~q + Phase B share |
| `sec_n_csr` | ~9k | 0 (starved) | ≥q (≥7) |
| `sec_n_port` | ~10k | 0 (starved) | ≥q |
| `sec_def14a` | ~100k | 0 (starved) | ≥q |
| `sec_8k` | ~50k | 0 (starved) | ≥q |

Cross-source verify: after triggering 2 ticks back-to-back, confirm
pending-row count drops for ALL sources with pending rows, not just
sec_form4.

Operator-visible figure: the `sec_manifest_worker` scheduler log
line (extended by T8 to include `processed_by_source=%s`) shows
non-zero counts for every source that had pending rows. Verified
post-trigger by reading the latest `job_runs` row for
`sec_manifest_worker` (stdout / structured logs depending on
deployment) or grepping the running jobs-process stderr.

PR description records:

1. `EXPLAIN ANALYZE` Buffers + cost for both top-up queries.
2. Pre/post pending counts per source.
3. Two-tick smoke result with the `processed_by_source` line.
4. Commit SHA for each step.

## 6. Settled-decisions

- **#719 process topology** — fairness logic stays in jobs process;
  module-global `_TICK_COUNTER` lives in
  `app/jobs/sec_manifest_worker.py`. ✓
- **#1171 fund-metadata source priority** — drain starvation
  unblocks the priority chain for `sec_n_csr` rows; chain itself is
  parser-side and untouched. ✓

## 7. Review-prevention-log applicability

- **#1158/1159 dependency-override xdist** — `pytestmark =
  pytest.mark.xdist_group("test_sec_manifest_worker")` pinned at
  file top per the prevention contract (already applied to
  existing file; T6 preserves).
- **"Service accepting conn must not commit"** — §3.3.
- **#1131 transient-vs-deterministic upsert** — §3.6.

No new prevention-log entry as part of this PR diff. **Extract
post-merge** per spec §10 + post-merge todo.

## 8. Acceptance criteria

1. `compute_quotas(sources, max_rows, tick_id)` returns a stable
   mapping with `sum(values) == max_rows` for non-empty sources.
   Unit-asserted in §6 case 1 setup.
2. `iter_pending` / `iter_retryable` queries carry
   `, accession_number ASC` tie-break (T2 diff). Regression sweep
   confirms no existing test breaks.
3. `iter_pending_topup` / `iter_retryable_topup` exist with the SQL
   shapes in §3.x of the spec. Both gracefully handle empty
   `exclude_accessions` via `%s::text[]` cast (§6 case 10).
4. `run_manifest_worker(conn, source=None, ...)` allocates a Phase
   A per-source slice + Phase B top-up. `WorkerStats.processed_by_source`
   reflects the per-source dispatched count (§6 cases 1, 2, 6, 8).
5. `run_manifest_worker(conn, source='sec_n_csr', ...)` is
   unchanged in shape (§6 case 3 regression).
6. Tick rotation under `n > max_rows` covers every source within
   `n - remainder + 1` ticks (§6 case 9).
7. Pre-push gates pass: ruff, format, pyright, pytest on impacted
   test file.
8. Dev DB smoke confirms every source with non-zero pending rows
   sees drain progress after a single tick fire.
9. PR description records EXPLAIN ANALYZE + smoke + commit SHA per
   §5.

## 9. Codex pre-spec 1b checklist (self-review hints)

- Task decomposition matches spec §4 implementation files; no
  hidden cross-file edits.
- `_TICK_COUNTER` module-global vs alternatives (DB sequence /
  job_runs.id): chose module-global for simplicity + restart
  behaviour (§3.x.1) is acceptable; flag if a DB-backed counter is
  preferable for stronger persistence across restarts.
- `_dispatch_rows` helper extracted to share logic between
  source=None and source=X branches — flag if this refactor risks
  any per-row behaviour drift vs the current loop.
- Test xdist-group pin already present per prevention contract.
- Empty-array call path covered by case 10 (direct invocation).
- `EXPLAIN ANALYZE` for top-up queries rolls into pre-push; spec
  §4 + plan §4 both require it.
- Settled-decisions + prevention-log entries cited (§6, §7).

## 10. Sign-off

- Codex 1b round 1: 3 BLOCKING + 2 WARNING + 1 NIT. All addressed:
  - BLOCKING (now normalization): plan T5 now normalises `now`
    before either branch.
  - BLOCKING (processed_by_source double-count): plan T5 specifies
    single-bump invariant after `spec is None` skip-check, before
    parser invocation.
  - BLOCKING (zero-parser contract change): plan T5 documents the
    contract change; T6 updates existing
    `test_unregistered_source_emits_warning_with_breakdown` to flip
    assertion. Operator visibility remains via
    `/coverage/manifest-parsers` (#935 §5).
  - WARNING (worker imports): T5 imports block lists
    `iter_pending_topup` / `iter_retryable_topup` /
    `Sequence` / `itertools`.
  - WARNING (Sequence import in T3): T3 imports row adds `Sequence`.
  - NIT (no-SQL wording): spec §6 case 5 reworded to use a
    `transition_status` monkeypatched call-counter sentinel; plan
    T6 inherits.
- Codex 1b round 2: 2 WARNING (no BLOCKING). Both addressed:
  - §3.2 process-topology wording corrected — API imports the
    module for registry visibility but never calls
    `run_manifest_worker`, so the `_TICK_COUNTER` is jobs-process
    only.
  - T1 gains 6 direct `compute_quotas` unit tests with hardcoded
    expected maps (max_rows > n, max_rows < n, rotated tick_id,
    empty, sum invariant, rotation coverage).
- Codex 1b round 3: 2 WARNING (no BLOCKING). Both addressed:
  - T1 direct `compute_quotas` tests rewritten to use real
    `ManifestSource` literals (pyright-friendly).
  - §5 operator-visible-figure reference updated — no
    `/jobs/sec_manifest_worker/status` endpoint exists; observability
    is via the scheduler log line + `job_runs` row.
- Codex 1b round 4: **CLEAN** (cached at `/tmp/codex_1179_1b_round4.txt`).
- Operator: pending.
- Implementation: blocks until plan signoff.
