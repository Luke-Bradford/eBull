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

## DISTINCT ON / ROW_NUMBER need a UNIQUE final tie-break

`DISTINCT ON (...)` (and `ROW_NUMBER() OVER (... ORDER BY ...)`, and `fetchone()` with `ORDER BY ... LIMIT 1`) pick the FIRST row per group by the `ORDER BY`. If the `ORDER BY` columns are not jointly UNIQUE, the winner among tied rows is arbitrary and **plan-dependent** — a single-key query (`WHERE id = %s`) looks stable because its plan is consistent, but a bulk `WHERE id = ANY(%(ids)s::bigint[])` query has a different plan → different physical row order → picks a DIFFERENT arbitrary winner. So a per-row reader and its bulk twin silently diverge on tied rows even with identical `ORDER BY` clauses.

Prevention: append the surrogate PK (`fact_id DESC`, `id DESC`) as the final `ORDER BY` key on any `DISTINCT ON`/`ROW_NUMBER`/latest-row query whose earlier keys aren't provably unique — for BOTH the per-row and the bulk form, so they agree AND are reproducible run-to-run. When bulking a reader for a batch job, verify equivalence on the FULL population (a same-process A/B), not a sample: the tie only bites the rows that have duplicates. Origin: #2127 P2 — `_read_latest_two_fy_facts` tied on `(concept, fiscal_year, period_end, filed_date, accession_number)` because `financial_facts_raw` co-tags an annual and a Q4 value both `fiscal_period='FY'`; 495 instruments carried ≥1 such tie and the golden silently picked the Q4 stub. Fixed by a domain tie-break (`(period_end - period_start) DESC` prefers the annual) plus `fact_id DESC` as the unique final key.

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

## View recreate — diff the outer SELECT column list old vs new

A `DROP VIEW` + `CREATE VIEW` migration replaces the reader contract wholesale; an accidentally dropped (or reordered-and-renamed) output column breaks every consumer silently at read time, not at migrate time. Before committing a view recreate, diff the two files' outer SELECT lists mechanically, e.g.:

```bash
for f in sql/OLD_view.sql sql/NEW_view.sql; do
  sed -n '/^SELECT$/,/^FROM (/p' "$f" | grep -oE "AS [a-z_]+|v\.[a-z_]+" | sed 's/AS //; s/v\.//' | tr '\n' ' '; echo "<- $f"
done
```

Identical output ⇒ contract preserved; any delta needs a stated reason in the migration header. Inner-CTE columns (UNION-shape padding like `NULL::numeric AS x`) are NOT part of the contract — a reviewer flagging one as "dropped" is refutable by exactly this diff (PR #2115 review WARNING, sql/236). Origin: PR #2115.

## Never edit an applied migration — bump to a new NNN+1 file

The runner records each applied file's SHA-256 in `schema_migrations.content_sha256` (#1333) and **raises at boot** if an applied file's content changed. Editing `sql/NNN_*.sql` after any DB recorded it (dev included — drafts applied during PR development count) is therefore a boot-breaker, not a silent no-op. All follow-up changes go into a new `NNN+1` file. If you knowingly replayed an edited file manually (idempotent), reset its hash: `UPDATE schema_migrations SET content_sha256 = NULL WHERE filename = '<file>'` — never DELETE the row. Full RCA in `docs/review-prevention-log.md` ("Migration content drift").

**The unshipped-draft case is different, and nulling the hash is WRONG for it (#2262, 2026-08-04).** While a migration is still in an open PR it exists on exactly one DB — your dev box — and review feedback routinely changes it. A new `NNN+1` file to fix a file that has never left your branch ships two migrations where the PR contains one concept. Nulling the hash is worse still: it declares "the DB matches the file" when it does not. A review that made me **remove an index** from `sql/248` left that index sitting in dev while the file no longer created it, so a null hash would have recorded agreement between a file and a schema that disagreed.

For a draft migration whose content genuinely DIVERGED (not an idempotent replay), undo its effects and re-apply from scratch:

```python
with psycopg.connect(settings.database_url, autocommit=True) as c:   # autocommit — see below
    c.execute("DROP TABLE IF EXISTS <what the migration created>")
    c.execute("DELETE FROM schema_migrations WHERE filename = '<file>'")
run_migrations()
# then ASSERT the recorded hash equals sha256(file bytes) — do not assume
```

This is the one place `DELETE FROM schema_migrations` is correct, and it stops being correct the moment the migration is merged. **Two traps came with it:**

- ⚠ **`autocommit=True` on DDL scripts.** `psycopg.connect()` defaults to a transaction. A `DROP INDEX`/`DROP TABLE` takes an ACCESS EXCLUSIVE lock and holds it until commit — so a script that raises, times out, or is killed by the harness leaves an `idle in transaction` backend blocking every later attempt. I stacked three of these and deadlocked myself. Diagnose with `SELECT pid, state, wait_event_type, left(query,60) FROM pg_stat_activity WHERE datname='ebull' AND state <> 'idle'`; clear with `pg_terminate_backend(pid)`. Killing the CLIENT does not close the backend.
- ⚠ **The dev API server runs migrations on every `--reload`**, so touching anything under `app/` re-applies whatever is on disk *at that moment* and can re-record a hash mid-repair. Do the reset and the re-apply in ONE script, then verify the stored hash rather than trusting the sequence.

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

## SQL/JSON path wildcards: `[*]` is array-only — against an object it is silently false

`jsonb_path_exists(col, '$.multiples[*].peer_ids')` returns FALSE for every
row when `multiples` is an OBJECT keyed by name (`{"pe": {...}, "ps": {...}}`)
— `[*]` matches array elements only; the member wildcard is `.*`
(`'$.multiples.*.peer_ids'`). No error is raised, so "0 rows have X" from a
jsonpath probe is indistinguishable from "wrong wildcard": before concluding
a jsonb feature is absent, `SELECT jsonb_pretty(col) … LIMIT 1` and check the
actual container shape. (2026-07-16 #2012 session: a `[*]` probe on
fair_value_band `basis_json.multiples` read shipped #2031 peer_ids provenance
as missing — a `.*` re-probe found 448/553.)

## Bounded NUMERIC columns need write-side saturation for unbounded ratios

A ratio computed in Python (`diff / base`) is unbounded, but the column it
lands in is not — `NUMERIC(10,4)` rejects anything ≥ 10^6 with
`numeric field overflow`. A tiny denominator against a large numerator
(parser-garbage share counts, 1-share proxy rows vs multi-million Form 4
cumulatives) WILL eventually exceed any precision you pick, and the
harness fixtures won't contain that row — only a full-population run
finds it (#966: `drift_pct` overflowed on the dev seed run, 12-fixture
harness green). Fix at the WRITE side: clamp to an explicit saturation
sentinel (`min(ratio, SENTINEL)`) chosen so the consumer's thresholds
still classify correctly, and document the sentinel where it's defined.
Do not widen the column instead — the next pathological row outgrows
that too, and a saturated sentinel is honest about "beyond measurable".

## A new WHERE predicate in a per-row chokepoint needs an index check BEFORE the backfill

Adding a lookup to a function that runs once per row of a corpus sweep is a
performance decision, not just a correctness one. Check that an index actually
leads with the column you filter on — "the table has four indexes" means nothing
if all four lead with something else.

Issue #2157 added an instrument-set lookup filtering `source_document_id` **alone** —
by construction there was no `instrument_id` to lead with, since finding the
instruments was the point. Every index on the table led with `instrument_id` or
`holder_name_key`, so the lookup sequentially scanned 111,867 rows per accession
and the full-corpus backfill dropped to **~28 accessions/min against a ~145/min
baseline**. With `CREATE INDEX … (source_document_id)`: **~300/min**.

- `EXPLAIN` the new query once against the dev DB before starting the sweep. It
  costs seconds and the sweep costs hours.
- Compare against the *previous* sweep's throughput, not against nothing. A
  5× slowdown is obvious next to a baseline and invisible on its own.
- On a **partitioned** table, `CREATE INDEX` on the parent cascades to existing
  and future partitions — no per-partition follow-up. It cannot be
  `CONCURRENTLY` if the migration runner wraps each file in a transaction, so
  create it while the sweep is paused rather than fighting the lock.
- Ship the index in the same PR as the predicate. A backfill that only runs fast
  on the author's machine because they added the index by hand is not reproducible.

## Never index a GROUP BY result positionally

A `GROUP BY` emits a row **only where one exists**. Code that reads
`rows[0]` / `rows[1]` after grouping on a boolean or a small enum is asserting
that every group is non-empty — which is true of the data you developed
against and false the first time a category is legitimately absent.

```python
# WRONG — IndexError the day a year has nothing unresolved
(ur_a, ur_b), (r_a, r_b) = (bias[0][1], bias[0][2]), (bias[1][1], bias[1][2])

# RIGHT — keyed on the grouping value, with an explicit zero default
by_resolved = {bool(row[0]): (row[1], row[2]) for row in rows}
ur_a, ur_b = by_resolved.get(False, (0, 0))
r_a, r_b = by_resolved.get(True, (0, 0))
```

⚠ The failure mode is inverted from the usual one: it crashes on the **clean**
case. Full data produces both groups and the code works; a narrower or
healthier slice produces one group and it raises. So it survives every test run
against the corpus you built it on.

Precedent: #2282 2c `build_2282_form25_register.py --census` unpacked a
two-row `GROUP BY resolved` positionally. Correct for 2023, `IndexError` for
any year whose Form 25 cohort resolved completely. Caught by the review bot,
not by any test.

Same shape, same fix: `GROUP BY status`, `GROUP BY provision_class`,
`GROUP BY is_tradable` — anywhere the result feeds a fixed-arity unpack.

## Widening a CHECK constraint — build the new list from the LIVE constraint

An enum-style `CHECK` gets rewritten by successive migrations, and **only the
database knows the union**. Writing the new one by copying the most recent
migration file you happen to find silently DROPS every member added after it.

```bash
# The only safe source. Do this BEFORE writing the migration.
grep -rn "<constraint_name>" sql/*.sql          # how many files touched it?
```
```python
# ...and read what is actually installed:
conn.execute(
    "select pg_get_constraintdef(oid) from pg_constraint where conname = %s",
    (constraint_name,),
).fetchone()
```

⚠ **It fails silently in the direction that hides it.** Postgres validates a new
CHECK against existing rows only — so a dropped member with no row exercising it
applies clean, and the constraint then rejects the *next* one. There is no error,
no warning, and the migration log says success.

Precedent (2026-08-05, #2218): `sql/254` was written from `sql/020`'s
`job_runs_status_check`, which predates `sql/137`, and dropped `'cancelled'`.
Four migrations had rewritten that one constraint (014 → 020 → 137 → 254). It
applied clean on the dev DB — `select status, count(*) from job_runs group by 1`
returns only `success` / `skipped` / `failure` / `running`, zero cancelled rows to
validate against — and would have rejected the next cancelled run. Caught by Codex
at checkpoint 2; no gate saw it, and no test would have.

**The durable fix is a derived contract test**, not care: parse the constraint out
of the migration text and assert the Python constants equal it
(`tests/test_job_terminal_status_contract.py`).

## A closed vocabulary lives in the READ paths too, and SQL strings hide them

pyright type-checks a `Literal`; it cannot see inside a query string. So adding a
member to a vocabulary makes the *writer* type-check clean while every
hand-spelled `IN (...)` filter silently excludes the new value — the row is
written and nothing reads it, which looks exactly like the feature not working.

Before adding a member to any status/kind/type vocabulary:

```bash
grep -rn "IN ('<existing_member>'" app/          # SQL filters
grep -rn '"<existing_member>"' app/ frontend/src # Literals, unions, maps
```

Then collapse them: one exported constant plus a pre-rendered SQL fragment, and a
test that greps `app/` for the hand-spelled shape.

⚠ **Two precision rules for that grep guard, both learned by running it:**
- Match the *shape*, not an exact string — sites differ only in whitespace.
- Require a member unique to this vocabulary. A pattern of `'success'…'skipped'`
  also matches `bootstrap_adapter`'s `('success', 'error', 'skipped', 'blocked',
  'cancelled')`, a different vocabulary on a different table. Requiring
  `'failure'` separates them.
- Strip comment lines first, or the guard flags the comment that warns about the
  anti-pattern.

⚠ Not every narrow list is drift. `dispatcher.reset_stale_in_flight` uses
`('success', 'failure', 'degraded')` **deliberately** — `skipped`/`cancelled` mean
the work was not done, and whether boot recovery should re-fire those is a separate
question. Say so at the site, or the next person "fixes" it to the shared constant.

Precedent (2026-08-05, #2218): five SQL literals across four modules plus two
`Literal`s. Three of the five were latest-terminal-run queries, so a `degraded` row
would have been written and then skipped in favour of an older, greener run —
shipping the ticket and changing nothing. Caught by Codex at checkpoint 2.
