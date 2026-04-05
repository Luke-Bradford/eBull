# python-hygiene

Engineering standard for Python code quality in this stack (Python 3.11+, Pydantic v2, psycopg3).

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

Run `uv run ruff check --select I .` to verify. Any import sort violation fails CI.

## Error escalation from helpers

When a helper raises and is called from an orchestrator with `except Exception`, the raise aborts the entire run with zero output. For data-inconsistency cases (unexpected-but-recoverable), prefer log-and-return so partial results are produced. Reserve raise for genuine programmer errors — invariant violations that cannot occur in correct code.

Before pushing any helper that raises: trace who catches it and what the failure scope is. If a single bad instrument would kill the whole batch run, that's the wrong failure mode.

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
