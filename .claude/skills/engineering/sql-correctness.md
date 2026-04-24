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

Any `fetchone()` without an explicit `ORDER BY` returns a non-deterministic row. Any query for "the latest" row needs both `ORDER BY <timestamp> DESC` and `LIMIT 1`.

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
|---|---|
| `fetchone()` missing ORDER BY | every `fetchone()` in the file |
| Positional `row[0]` | `\[[0-9]\]` on cursor results |
| `MAX(` in a two-step sequence | `MAX(` in service files |
| `json.dumps` into jsonb | `json.dumps` in services/ |
| `dict_row` added to one cursor | all cursor calls in the file |
| Missing `rowcount` after UPDATE | every `conn.execute("UPDATE` in the file |
