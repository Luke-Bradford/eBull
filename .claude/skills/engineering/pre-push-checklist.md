# pre-push-checklist

Run before every push. No exceptions. No bypassing CI to "let it catch things."

## Gate — all four must be green

```
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

Fix failures before pushing. If `uv` is not on PATH, run `where uv` to find it and add to shell config.

## Then read `git diff origin/HEAD` top to bottom

Adopt the reviewer's posture: read what is there, not what you intended.

---

## SQL checks

For every query in the diff:

- [ ] `fetchone()` — is there an `ORDER BY`? Without it the result is non-deterministic
- [ ] "Latest row" query — has both `ORDER BY <ts> DESC` and `LIMIT 1`?
- [ ] Row access — `row["name"]` not `row[0]`? `dict_row` applied to all cursors in the file?
- [ ] Sequence/version — `MAX()+1` inside a scalar subquery in VALUES, not a two-step SELECT then INSERT?
- [ ] `INSERT ... SELECT WHERE` — what happens when WHERE matches zero rows? Trace the first-row case.
- [ ] `conn.transaction()` — any network call or file I/O inside? Must not be.
- [ ] Nullable column comparisons — `IS DISTINCT FROM` not `!=` when NULLs should be included?
- [ ] Parameters — no f-strings, no `.format()` in SQL; `= ANY(%s)` not `IN %s`?

---

## Python checks

- [ ] Read-only sequence params typed `Sequence[T]`, not `list[T]`?
- [ ] Bounded string values typed as `Literal[...]`, defined once at module level?
- [ ] `Optional[X]` replaced with `X | None`?
- [ ] Dict passed to jsonb column wrapped with `Jsonb(...)`, not `json.dumps()`?
- [ ] Imports alphabetically sorted within groups; stdlib / third-party / first-party separated by blank lines?
- [ ] Sequential evaluation loop with a shared resource limit (position count, sector cap)? Accumulators updated after each approval?
- [ ] Any helper that raises — who catches it? Does a raise here abort an entire orchestration run?
- [ ] Any dedup on free-text strings — expected value derived from a helper, not a hardcoded literal?
- [ ] Any "total=N" log line after a filter step — split into `generated=N written=M`?

---

## Test checks

- [ ] Every test asserts a specific value, not just `is not None`?
- [ ] Boundary cases covered: first row, zero results, failure path?
- [ ] Any code calling `_utcnow()` — is it patched in the test?
- [ ] Mocks: `fetchone()` returns `None` not `MagicMock`; `spec=` set on attribute-accessed mocks?
- [ ] Free-text comparisons derived from helpers, not hardcoded?

---

## Same-class scan — after any fix

| Found | Grep for |
|---|---|
| `fetchone()` without ORDER BY | every `fetchone()` in the file |
| Positional `row[0]` | `\[[0-9]\]` on cursor results |
| `json.dumps` into jsonb | `json.dumps` in services/ |
| `Optional[` or `Union[` | `Optional\[` and `Union\[` |
| `list[` read-only param | function signatures with `list[` |
| `dict_row` added to one cursor | all cursor calls in the file |
| Resource-check call (e.g. `_sector_pct`) | all call sites — accumulator or ordering comment? |

---

## Review comment handling

After the review posts — read the **full body**, not just the verdict.

- BLOCKING: fix before any further push
- WARNING: fix on this PR, or open a `tech-debt` issue and put the number in the reply
- NITPICK: fix if trivial; otherwise open a `tech-debt` issue and put the number in the reply
- PREVENTION: extract each note to this file or the relevant skill before merging
- Nothing silently discarded — every comment gets a reply

**Merge gate:** APPROVE + all WARNINGs and NITPICKs resolved or issued + all PREVENTION notes extracted + CI green on the most recent commit.
