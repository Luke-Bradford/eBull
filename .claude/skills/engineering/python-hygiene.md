# python-hygiene

Engineering standard for Python code quality in this stack (Python 3.14+, Pydantic v2, psycopg3).

## Type annotations

**Sequence vs list for read-only parameters**
```python
# Wrong — implies the function may mutate the input
def process(ids: list[int]) -> None: ...

# Correct — read-only, accepts any sequence
def process(ids: Sequence[int]) -> None: ...
```
Use `list[T]` only when the function appends, pops, or assigns into the parameter.

**Literal for bounded string values**
```python
# Wrong — accepts any string
action: str

# Correct — validates at type-check time
Action = Literal["BUY", "ADD", "HOLD", "EXIT"]
action: Action
```
Define once at module level, import everywhere. No magic strings.

**Optional/Union**
```python
# Wrong — verbose, pre-3.10 style
from typing import Optional, Union
x: Optional[str]
y: Union[str, int]

# Correct
x: str | None
y: str | int
```

**from __future__ import annotations**
Add at the top of every new file for forward reference support.

## JSONB columns

```python
# Wrong — psycopg3 cannot infer jsonb from a plain dict
conn.execute("INSERT INTO t (data) VALUES (%s)", [{"key": "val"}])

# Correct
from psycopg.types.json import Jsonb
conn.execute("INSERT INTO t (data) VALUES (%s)", [Jsonb({"key": "val"})])
```

## Imports

Within each `from X import a, b, c`: names alphabetically sorted. Blank line between:
1. stdlib
2. third-party
3. first-party

**All imports at the top of the file.** Never import inside a function or method body. This includes test files — `import pytest` belongs at module level with the other imports, not inside a test case. Ruff does not enforce this by default (`PLC0415` is opt-in), so it is on the author to catch it in pre-flight review.

Exceptions are rare and narrow:

- Avoiding a genuine circular import that cannot be resolved by rearranging modules.
- Lazy-loading an expensive optional dependency that is only used on a cold path.

Every inline import must have a comment explaining *which* exception applies.

Run `uv run ruff check --select I .` to verify. Any import sort violation fails CI.

## Error escalation from helpers

When a helper raises and is called from an orchestrator with `except Exception`, the raise aborts the entire run with zero output. For data-inconsistency cases (unexpected-but-recoverable), prefer log-and-return so partial results are produced. Reserve raise for genuine programmer errors — invariant violations that cannot occur in correct code.

Before pushing any helper that raises: trace who catches it and what the failure scope is. If a single bad instrument would kill the whole batch run, that's the wrong failure mode.

When an `except Exception` swallows the error to trigger a **fallback** path, log it with `exc_info=True` (or `logger.exception(...)`). The fallback may fully recover (e.g. a transient batch-level serialization/deadlock abort that succeeds on per-instrument retry), so without `exc_info` there is *zero* record of why the primary path failed — root-cause diagnosis in prod becomes impossible. Bare `logger.warning("X failed; falling back", ...)` on a swallowed-then-recovered exception is a silent drop.

## Production invariants

Never use `assert` for a condition that must hold in production.
Use an explicit runtime check and raise a concrete exception, for example:

```python
if some_required_value is None:
    raise RuntimeError("some_required_value must be present here")
```
`assert` is for developer assumptions and can be optimized away; production invariants must remain enforced.

## Sequential evaluation loops with shared resource limits

Any loop evaluating candidates against a shared constraint (position count, sector cap, cash) must maintain a mutable accumulator updated after each approval:

```python
pending_count: int = 0
pending_sector_pct: dict[str, float] = {}

for candidate in ranked:
    ok, reason = _evaluate(candidate, ..., pending_count, pending_sector_pct)
    if ok:
        pending_count += 1
        pending_sector_pct[sector] = pending_sector_pct.get(sector, 0.0) + alloc
```

Without this, each candidate is evaluated against the committed DB state only — approving 10 candidates in the same sector when the cap should have stopped at 5.

If evaluation is split into phases (held instruments first, then unowned), add a comment at the phase boundary:
```python
# Evaluation order: held instruments (EXIT/ADD/HOLD) BEFORE unowned (BUY).
# This ordering is load-bearing: <explain why correctness depends on it>.
```

Before pushing: grep for all resource-check calls in the file and verify each has either an accumulator parameter or an ordering-dependency comment.

## Free-text dedup

Any dedup logic that compares explanation/rationale strings must derive the expected string from the same helper as the production code — never a hardcoded literal:

```python
# Wrong — breaks silently when format changes
prior_rationale = "No action trigger met; score=0.600 rank=2"

# Correct — format change propagates automatically
prior_rationale = _hold_rationale({"total_score": 0.60, "rank": 2}, quote_is_fallback=False)
```

## Log counts after filtering

Any function with a "complete: total=N" log that follows a filter/dedup step must split counts:

```python
# Wrong — N includes suppressed HOLDs
logger.info("complete: total=%d", len(recommendations))

# Correct
logger.info("complete: generated=%d written=%d", len(recommendations), written)
```
Compute `generated` before the filter, `written` after.

## `or`-chaining on numeric fields from external data

Python `or` evaluates truthiness, not nullness. `0`, `0.0`, and `Decimal("0")` are all falsy.

```python
# Wrong — if the API returns {"Fees": 0}, `or` treats it as falsy
fees = raw.get("Fees") or raw.get("fees")

# Correct — explicit None check preserves zero
fees = raw.get("Fees") if raw.get("Fees") is not None else raw.get("fees")
```

For string fields (order ref, status label), `or`-chaining is fine because empty strings are invalid.
For numeric fields (price, units, fees), use explicit `is not None` checks.

## psycopg3 `executemany` rowcount IS cumulative (verified)

`cursor.rowcount` after a **non-returning** `executemany(...)` reflects the **sum** of affected rows across the whole batch in psycopg 3.3.3 — NOT the last statement only. Empirically verified (2026-06-04):

```python
cur.executemany("INSERT ... ON CONFLICT DO NOTHING", [3 distinct rows])  # cur.rowcount == 3
cur.executemany("INSERT ... ON CONFLICT DO NOTHING", [1 conflict, 1 new]) # cur.rowcount == 1
```

So using `cur.rowcount` as a batch insert/affected total after `executemany` is correct for this driver. Two caveats:
- It is **driver/version-specific** (DB-API leaves it implementation-defined; some drivers/older psycopg return -1 or the last count). When the total matters across a version bump, **pin it with a regression test** that inserts N distinct rows and asserts the count == N (e.g. `tests/test_sec_13f_dataset_ingest.py::...test_multiple_distinct_figis_counted_cumulatively`), rather than trusting the doc.
- It does NOT hold for `executemany(..., returning=True)` — that path iterates result sets differently.

Origin: PR #1468 (#1302) review WARNING claimed the FIGI counter "will be at most 1"; rebutted by empirical probe + the pinned regression test.

## Cleanup-path ordering: clear ownership flags AFTER the released-side operation

Release the resource FIRST, then clear the "I own it" flag. NOT in a `try/finally` that fires on raise either — `finally` would still clear the flag if release blew up, masking the very leak signal the flag is supposed to surface.

```python
# Wrong — clears the flag before the operation that depends on it. If
# release raises, the flag is already False — caller / diagnostics can't
# tell we still held the resource.
if self._held:
    self._held = False
    resource.release(key)
    return

# Wrong — finally clears even on raise. Same suppression as above.
if self._held:
    try:
        resource.release(key)
    finally:
        self._held = False
    return

# Correct — release first; clear only on success. If release raises, the
# exception propagates AND _held stays True so the leak is visible.
if self._held:
    resource.release(key)
    self._held = False
    return
```

Origin: PR #1543 (#1542) review WARNING on `app/jobs/locks.py::JobLock.__exit__` sec_rate release block.

Applies to any cleanup path with an ownership/held flag: `__exit__`, `close()`, manual teardown helpers. The principle is the same as never resetting `committed = True` before `conn.commit()` returns.
