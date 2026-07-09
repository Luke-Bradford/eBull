# sql-correctness

Engineering standard for writing correct SQL in this stack (psycopg3 + PostgreSQL 17).

## Atomic versioning — no two-step sequences

Never compute a sequence value as a separate SELECT then INSERT. That's a TOCTOU race: two concurrent writers can read the same MAX and produce duplicate versions.

**Wrong:**

```python
version = conn.execute("SELECT COALESCE(MAX(version), 0) + 1 FROM t WHERE id = %s", [id]).fetchone()[0]
conn.execute("INSERT INTO t (id, version) VALUES (%s, %s)", [id, version])
```

**Correct — scalar subquery inside VALUES:**

```sql
INSERT INTO t (id, version, ...)
VALUES (
    %(id)s,
    (SELECT COALESCE(MAX(version), 0) + 1 FROM t WHERE id = %(id)s),
    ...
)
```

This is atomic. COALESCE handles NULL from MAX on an empty table — always trace the first-row case.

## INSERT ... SELECT zero-rows trap

`INSERT INTO t SELECT ... FROM t WHERE condition` inserts zero rows silently when WHERE matches nothing. No error is raised. Always trace what happens on the very first row for a given key.

## fetchone() requires ORDER BY

Any `fetchone()` on a query whose predicate can match more than one row needs an explicit `ORDER BY` — without it the row returned is non-deterministic. (Unique-key/singleton lookups, e.g. `WHERE id = TRUE` on `kill_switch`, are exempt.) Any query for "the latest" row needs both `ORDER BY <timestamp> DESC` and `LIMIT 1`.

After fixing a missing ORDER BY: grep the whole file for every `fetchone()` call — a partial fix is worse than none.

## No positional row access

`row[0]`, `row[1]` silently returns wrong data if a column is ever added before the indexed column. Use `row_factory=psycopg.rows.dict_row` and access by name: `row["column_name"]`.

Apply `dict_row` consistently to every cursor in a file. A partial migration (some cursors named, some positional) is a latent bug. After switching one cursor, grep the file for `row[0]` and `row[1]`.

## No I/O inside transactions

No HTTP calls, no external API calls, no file reads inside `with conn.transaction()`. A slow or failed network call holds a DB lock for its duration.

Pattern: do all I/O first, then open the transaction for the writes only.

## NULL in comparisons

`col != 'value'` excludes NULLs silently — they are neither equal nor not-equal. Decide whether NULLs should be included and use the right form:

- Include NULLs: `col IS DISTINCT FROM 'value'`
- Parameterised NULL equality: `col IS NOT DISTINCT FROM %s`
- Never: `col IS %s` — illegal in psycopg3

## Parameterisation

- Named params: `%(name)s` with a dict
- Positional params: `%s` with a list or tuple
- Never f-strings or `.format()` in SQL strings — SQL injection vector
- `IN` clauses: `= ANY(%s)` with a list, not `IN %s` with a tuple
- Literal `%` in LIKE patterns: `%%`
- **Nullable-filter param must be cast to its column type.** A `None` binds as an untyped NULL (OID 0); psycopg3's extended-protocol send gives Postgres no type, and `%(x)s IS NULL OR col = %(x)s` can leave the planner unable to infer it — `psycopg.errors.AmbiguousParameter: could not determine data type of parameter $N`. Cast every occurrence: `%(x)s::bigint IS NULL OR col = %(x)s::bigint`. The trap hides when the only exercised path always passes a concrete value (which *does* give Postgres the type) — the no-filter/`None` path never gets tested. (#1961: `get_activity` in `app/api/portfolio.py` — optional `instrument_id` filter added by #1926 500'd the whole Portfolio Activity tab.)

## Conditional JOINs in filter-aware list queries

A paginated list endpoint should only JOIN tables / views its active filters actually consume. A view backing a filter (e.g. `instrument_dividend_summary` for a `has_dividend` filter) scans every row in its source — adding the JOIN unconditionally to the items query penalises every caller, including the default no-filter case.

```python
# Wrong — dividend-summary view scanned on every list call.
items_sql = f"""
    SELECT ... FROM instruments i
    LEFT JOIN coverage c USING (instrument_id)
    LEFT JOIN instrument_dividend_summary ds USING (instrument_id)
    {where_sql}
"""

# Correct — JOIN composed only when the filter is active, matching the
# pattern already used for the COUNT query.
dividend_join = "LEFT JOIN instrument_dividend_summary ds USING (instrument_id)" if has_dividend is not None else ""
items_sql = f"""
    SELECT ... FROM instruments i
    LEFT JOIN coverage c USING (instrument_id)
    {dividend_join}
    {where_sql}
"""
```

Self-check: `grep -n "LEFT JOIN.*_summary\|LEFT JOIN.*_view" app/api/*.py` — every match inside an `items_sql =` f-string must be gated by a conditional variable, not hardcoded.

## Single-row UPDATE must verify rowcount

`UPDATE ... WHERE` silently affects zero rows when the predicate matches nothing. For any UPDATE that must affect exactly one row (singleton tables, primary-key lookups), check `result.rowcount`:

```python
result = conn.execute("UPDATE kill_switch SET ... WHERE id = TRUE", params)
if result.rowcount == 0:
    raise RuntimeError("expected row missing — cannot update")
conn.commit()
```

Without this, the caller believes the mutation succeeded while the row is unchanged.

## Same-class scan after any fix

| Found | Grep for |
| --- | --- |
| `fetchone()` missing ORDER BY | every `fetchone()` in the file |
| Positional `row[0]` | `\[[0-9]\]` on cursor results |
| `MAX(` in a two-step sequence | `MAX(` in service files |
| `json.dumps` into jsonb | `json.dumps` in services/ |
| `dict_row` added to one cursor | all cursor calls in the file |
| Missing `rowcount` after UPDATE | every `conn.execute("UPDATE` in the file |

## Chained-CTE filter consistency

When a query selects an anchor in one CTE (a winner accession, a target period, a max watermark) and joins it in a later stage, every stage must apply the SAME eligibility filters. A filter present in the final join but absent from the anchor CTE lets the anchor land on a row the join then excludes — the query silently returns zero rows despite eligible data.

Self-check: for each CTE that computes a `MAX(...)`/`ORDER BY ... LIMIT 1` anchor, diff its WHERE clause against the final SELECT's — any predicate present in one and not the other needs a reason in a comment. Origin: PR #1588 review WARNING (`target` CTE missing `NOT is_subtotal` carried by winner + main query).

## Never edit an applied migration — bump to a new NNN+1 file

The runner records each applied file's SHA-256 in `schema_migrations.content_sha256` (#1333) and **raises at boot** if an applied file's content changed. Editing `sql/NNN_*.sql` after any DB recorded it (dev included — drafts applied during PR development count) is therefore a boot-breaker, not a silent no-op. All follow-up changes go into a new `NNN+1` file. If you knowingly replayed an edited file manually (idempotent), reset its hash: `UPDATE schema_migrations SET content_sha256 = NULL WHERE filename = '<file>'` — never DELETE the row. Full RCA in `docs/review-prevention-log.md` ("Migration content drift").

## Constraints live in two places — grep both

The `CREATE TABLE` statement is **not** authoritative for CHECK / FK / UNIQUE constraints. Subsequent migrations land additional constraints via `ALTER TABLE ... ADD CONSTRAINT`. Before writing any code (seeder, fixture, parser, ingester) that emits or accepts values for a column, grep both:

```bash
rg -n "CREATE TABLE.*<table>" sql/
rg -n "ALTER TABLE.*<table>" sql/
rg -n "ADD CONSTRAINT.*<column>" sql/
```

Worked example (Codex 2 catch, 2026-05-27 PR phase-0-new-b-c-bundle):

- `sql/114_ownership_institutions_observations.sql` creates `ownership_institutions_current` with `filer_cik TEXT NOT NULL` — looks unconstrained.
- `sql/134_ownership_identifier_check_constraints.sql:57-59` adds `CHECK (filer_cik ~ '^[0-9]{10}$')` (`chk_institutions_cur_filer_cik`).
- A seeder that grepped only the CREATE TABLE saw "NOT NULL TEXT" and emitted `SYN00000000` → COPY aborted on first row.

The lesson lives in `feedback_grep_alter_constraints` (memory) and `docs/review-prevention-log.md`.

## Inclusive day-upper-bounds on timestamp columns: `< %(end)s::date + 1`

For "rows up to and including day X" against a TIMESTAMPTZ column, the
repo convention is the half-open form:

```sql
WHERE ts_col >= %(start)s
  AND ts_col <  %(end)s::date + 1
```

Do NOT "simplify" to `ts_col <= %(end)s::date` — comparing a timestamp
to a date coerces the date to midnight, silently dropping every
intraday row on the last day. (PR #1597 review suggested exactly that
rewrite; it would have excluded the whole final day of each report
period.) `app/services/reporting.py` uses the half-open form at every
period-bounded query — keep new queries consistent with it.

## Two date-resolved lookups can collapse to one row → a fake zero

When a metric is a difference between two "latest row relative to a
date" lookups — e.g. `close_end / close_start - 1` where
`close_start` = latest close `< period_start` and `close_end` = latest
close `<= period_end` — stale or sparse data can make BOTH queries
resolve to the **same row**, so the difference is a confident `0`,
indistinguishable from a real flat period.

```sql
-- close_start: latest STRICTLY BEFORE the window
SELECT price_date, close FROM price_daily
WHERE instrument_id = %(iid)s AND close IS NOT NULL AND price_date < %(start)s
ORDER BY price_date DESC LIMIT 1;
-- close_end: latest AT-OR-BEFORE the window end
SELECT price_date, close FROM price_daily
WHERE instrument_id = %(iid)s AND close IS NOT NULL AND price_date <= %(end)s
ORDER BY price_date DESC LIMIT 1;
-- if the latest available close predates the whole window, BOTH return
-- the same pre-window row → close_end/close_start - 1 == 0 (FAKE).
```

Fix: select each endpoint WITH its `price_date` and gate the
computation on coverage — only compute when the end row's date
`>= period_start` (a row actually fell inside the span); otherwise
return null so the UI shows "—"/"unavailable", never `0`. Same
"no-data ≠ zero" rule the risk layer encodes as `benchmark_missing`.
Verify on a window the data does NOT cover, not just a healthy one.
(#1817 `_benchmark_closes`.)
