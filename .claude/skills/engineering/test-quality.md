# test-quality

Engineering standard for writing tests that prove something. A test that doesn't assert observable behaviour is noise, not coverage.

## A test must assert on a specific value

```python
# Noise — proves nothing except no crash
def test_generate_thesis():
    result = generate_thesis(...)
    assert result is not None

# Coverage — proves the contract
def test_generate_thesis_returns_correct_version():
    result = generate_thesis(...)
    assert result.thesis_version == 1
    assert result.stance == "buy"
    assert result.confidence == pytest.approx(0.8)
```

## A content-hash / signature test must isolate the term it claims to test

When a function builds a composite key (a memo signature, cache key, ETag, dedup hash) from several inputs, a test named after **one** contributing term must vary **only that term** between two otherwise-identical inputs — and ideally mutation-prove it.

The trap: comparing two inputs that already differ in a field the hash *also* serialises. The assertion passes for the wrong reason and would still pass if the term under test were deleted.

```ts
// Garbage — claims to test the "stuck elapsed" term, but the two rows
// also differ in `stale_reasons`, which is already in JSON.stringify(row).
// Deleting the elapsed term entirely leaves this test green.
expect(sig({ ...row, stale_reasons: ["stuck"] }))
  .not.toBe(sig({ ...row, stale_reasons: [] }));

// Coverage — hold the data frozen, vary ONLY wall-clock (the novel term),
// and prove the quiescent case stays stable.
nowSpy.mockReturnValue(base + 60_000);  const at1m  = sig(stuckRow);
nowSpy.mockReturnValue(base + 600_000); const at10m = sig(stuckRow);
expect(at1m).not.toBe(at10m);           // time term advances a frozen row
expect(sig(okRow_at_t0)).toBe(sig(okRow_at_t1)); // …but not a healthy one
```

Mutation check before trusting it: delete the term from the production hash, run the test, confirm it **fails**, revert. If it still passes, the test proves nothing about that term. Origin: #1480 (`processRowSignature` stuck-clock suffix) — the first draft compared stuck-vs-non-stuck and was caught in self-review.

## Mandatory boundary cases

For every function, identify and test:
- **First row / empty table** — does an INSERT using `MAX()` work when there are no prior rows?
- **Zero results** — does a query returning a list handle the empty case without raising?
- **None / null fields** — does optional data come back as `None`, not raise `AttributeError`?
- **Failure path** — does a best-effort operation (API call, critic scoring) fail gracefully without blocking the happy path?

These aren't edge cases. They're the first things a reviewer checks.

## Semantic boundary checks

For any rule about affordability, capacity, or limits, include tests for the actual business boundary:
- zero
- exact cap
- just below cap
- just above cap

Do not stop at proving branch execution.
Prove the rule matches its intended meaning.

## Mock discipline

**Match what the real library returns.** psycopg `fetchone()` returns `None` on exhaustion — not a `MagicMock`. A mock that returns `MagicMock` instead of `None` will never trigger the None-check branch.

**Use `spec=` on MagicMock** so accessing an unexpected attribute raises `AttributeError` rather than silently returning another mock:
```python
mock_conn = MagicMock(spec=psycopg.Connection)
```

**Patch at the point of use**, not the point of definition:
```python
# Wrong — patches the original, not where it's imported
patch("datetime.datetime.now")

# Correct — patches the name as used in the module under test
patch("app.services.thesis._utcnow")
```

**SQL text-dispatch mocks** that match on substrings must document branch priority. INSERT branches must come before SELECT branches because a scalar subquery inside VALUES contains `SELECT ... FROM table` as a substring. Add a comment noting what structural SQL defects this matching approach cannot catch.

**Mock the name the code actually calls — verify with a canary.** A connection-helper migration (e.g. raw `psycopg.connect(settings.database_url)` → a wrapper like `connect_job()`) silently orphans any test that still patches the OLD name — `patch.object(scheduler, "psycopg")` becomes dead code, and the real wrapper (which may resolve settings via its OWN module import, unaffected by a `patch.object(module, "settings", stub)` scoped to a different module) opens a REAL connection instead. Symptom: assertion values that look like real production data rather than the test's small fixture, or a `call_count`/`call_args` read on the mock that comes back `0`. When a test patches a connection/IO factory, assert the mock was actually invoked (`mock.assert_called()` or a nonzero `call_count`) before asserting on downstream values — that's the canary that isolation is real, not a leak wearing a green checkmark. When you migrate a call site, grep every test file that mocks the OLD name and update it in the SAME PR (#1887).

**Frontend `vi.spyOn` needs per-test restore when assertions read call history.** Since vitest 4, `vi.spyOn` on an already-spied method returns the SAME spy, so `mock.calls` accumulates across tests in a file (vitest 2 effectively gave each test a fresh view). Any file where tests assert `spy.mock.calls[0]`, `not.toHaveBeenCalled()`, or `toHaveBeenCalledTimes(n)` on a module-level spy target must register

```ts
afterEach(() => {
  vi.restoreAllMocks();
});
```

at module scope. Symptom of the gap: the assertion sees the PREVIOUS test's call as `calls[0]` and fails (or worse, passes) depending on test order. Found in the #1417 vitest 2→4 upgrade: `FilingsPane.test.tsx` + `EightKListPage.test.tsx` were order-dependent and only green because vitest 2's spy semantics masked it.

## Time-dependent code

Any function calling `_utcnow()` — directly or transitively — must have it patched in tests. If unsure whether a function calls it transitively, read the call chain. An unpatched `_utcnow()` makes the test non-deterministic.

## Free-text comparisons

Any test comparing a rationale or explanation string must derive the expected value from the same helper used in production — never a hardcoded literal:

```python
# Wrong — breaks silently when production format changes
assert rec.rationale == "No action trigger met; score=0.600 rank=2"

# Correct — format change propagates automatically
assert rec.rationale == _hold_rationale(score_row, quote_is_fallback=False)
```

## Scope the deliberate DB-tier run to the diff — NEVER bare `pytest -m db` locally

Bare full-suite `uv run pytest -m db` on the dev Mac has wedged twice (2026-06-09:
froze at startup, zero workers; 2026-06-10: ran 2h02m to 98% then froze with all
xdist workers dead — a moving progress bar does not mean it will finish). Even when
it moves, ~4,300 db tests × 1-3s fixture overhead ≈ an hour+ of wall-clock for
milliseconds of test logic; suite-shape fix tracked in #1568.

Operator decision: never block a small PR on the full tier. The "run the DB tier
deliberately" rule is satisfied by running the test files for the touched modules +
immediate neighbours (e.g. a 2-file rewash change → 4 files / 105 tests / ~60s).
For broad surface (migrations touching many tables, conftest/fixture changes,
schema-wide refactors) run the tier in file-scoped batches, not bare `-m db`. A run
with 0% CPU and no `gw*` workers is wedged: `kill -9` it, then reap leaked test DBs
with `uv run python -m tests.fixtures.cleanup_test_dbs`.

## DB write + return value consistency

If a function both writes to the DB and returns a result object, there must be a test verifying the returned object matches what was written. Silent divergence between in-memory and persisted state is a real bug class.

## Integration-marker discipline

Any test that uses the `clean_client` fixture (or any fixture that touches a real DB) MUST be decorated with `@pytest.mark.integration` (registered in `pyproject.toml`). Since the 2026-06-07 test-tiering decision this marker is documentation, not the gate: push-gate exclusion is automatic via the `db` marker, auto-applied at collection (`tests/conftest.py::pytest_collection_modifyitems`) to any test pulling a real-DB fixture or whose module source references a real-DB entrypoint (`TestClient`, `ebull_test_conn`, `settings.database_url`, `run_migrations(`, …). Keep the explicit decoration anyway — it declares the tier boundary at the test site instead of leaving it to the collection-time source scan.

```python
# Wrong — real-DB test with no tier declaration at the test site
def test_post_ingest_enabled_unknown_key_404(clean_client: TestClient) -> None:
    ...

# Correct
@pytest.mark.integration
def test_post_ingest_enabled_unknown_key_404(clean_client: TestClient) -> None:
    ...
```

Self-check before pushing: `grep -rn "def test_.*(clean_client" tests/` and assert each match is decorated `@pytest.mark.integration` (or its module sets `pytestmark = pytest.mark.integration`).

## Dev-DB isolation invariant

The test suite MUST point at `ebull_test_*` databases, never the operator dev DB at `settings.database_url` (= `ebull`). A test that writes to dev DB has at minimum two failure modes:

1. **Singleton-row drops** — a test that `TRUNCATE`s or `DELETE`s from a singleton table (e.g. `runtime_config`, `kill_switch`) takes the live system into a fail-closed 503 state until an operator re-seeds. 2026-05-18 is on record.
2. **Test pollution** — fixtures that mutate state run against a moving target, hiding non-determinism behind cross-suite interference.

Defense in depth (six rails, in order of how often they fire):

1. **Primary:** `tests/fixtures/ebull_test_db.py::_assert_test_db` rejects any destructive op against a DB whose name does not match `ebull_test_*`. Every cursor obtained through `ebull_test_conn` goes through this guard.
2. **Tripwire:** `tests/conftest.py::_dev_db_size_tripwire` records `pg_database_size('ebull')` at session start + asserts <1 MB growth at session end. Catches the residual case where a test opens a raw `psycopg.connect(settings.database_url)` outside the fixture. Tripwire only — misses deletes and HOT updates; can false-positive on idle autovacuum.
3. **Orphan sweep** (#1208 Phase 2): `_drop_orphan_workers_older_than` runs on every controller-start inside `build_template_if_stale()`. (Sweep logic + `_NEVER_DROP` protect-set now live in `app/db/dev_test_db_reaper.py`; `tests/fixtures/ebull_test_db.py` wraps/re-exports them.) Catches the residue of `ebull_test_<epoch>_<hex>_<gw>` databases left behind when a worker SIGSEGV's, the operator force-quits pytest, or the OS reboots mid-run — failure modes that `pytest_sessionfinish` cannot reach because it only fires on graceful exit. Three-rail safety model (activity guard via `pg_stat_activity`, age backstop via parsed epoch, hard-coded `_NEVER_DROP` literal); plain `DROP DATABASE` without `WITH (FORCE)` to eliminate the TOCTOU race against a sibling pytest worker. CI is short-circuited (`os.getenv("CI") == "true"`).
4. **Invalid-DB force-reaper** (#1401): `_force_drop_invalid_test_dbs` runs alongside the orphan sweep at controller-start. It force-drops every `ebull_test_*` **and `ebull_mig*`** database marked `datconnlimit = -2` — the "interrupted-drop corpse" state PG sets when a `kill -9`'d worker (or a wedged `DROP ... WITH (FORCE)`) dies mid-drop. The age-gated, plain-DROP orphan sweep above **cannot** clear these: PG refuses all new connections to a `-2` DB so plain DROP is blocked by the wedged backend, and the corpse has no parseable epoch. Because a `-2` DB is connection-refused there is no concurrent-invocation race — force-drop is unconditionally safe (no age gate, no activity rail). This is the rail that actually clears the leak: the 13.1M-file dev-PG bloat (2026-05-30) was `-2` worker/mig corpses the old sweep matched only by `ebull_test%` and never force-dropped.
5. **Session-lifetime keepalive** (`_worker_db_keepalive`, autouse from `conftest.py`): each worker holds one autocommit connection to its private DB for the whole pytest session, so the worker DB appears in `pg_stat_activity` even between tests — the load-bearing input to the orphan-sweep activity rail.
6. **Worker-DB relation-count tripwire** (#1401): `_assert_worker_relations_under_ceiling` runs in `ebull_test_conn` teardown. The worker DB is **reused across every test on the worker** and per-test cleanup is `TRUNCATE` only — it wipes rows but **never drops relations**. Any test (or app code under test) that `CREATE`s a table/index/partition without dropping it leaks relations that accumulate for the whole session; one such runaway ballooned a worker DB past ~2.1M relations and bloated the data dir to 13.1M files. The tripwire fails the first test that pushes `count(*) FROM pg_class` past `_WORKER_DB_RELATION_CEILING` (50k; template baseline ≈9.6k), so a relation leak surfaces as a named failing test instead of a silent disk disaster.

When the dev-DB-size tripwire fires: grep the tests directory for `psycopg.connect(settings.database_url)` and route each use through `tests/fixtures/ebull_test_db.py::test_database_url`. Never silence the tripwire by raising the threshold — fix the offending test.

When the relation-count tripwire fires: the failing test is the (or first) culprit. It `CREATE`s relations in the worker DB without dropping them. Bound the relation creation and tear it down via a **registered finalizer** — never a bare `try/finally` (a `kill -9` skips it). Do NOT raise `_WORKER_DB_RELATION_CEILING` to silence it.

When the orphan sweep stops cleaning (leaked `ebull_test_*` / `ebull_mig*` DBs accumulate again): first run `uv run python -m tests.fixtures.cleanup_test_dbs` (operator escape hatch — force-drops both families incl. `-2` corpses; run only when no pytest is active). Then check `pg_stat_activity` for stuck keepalive connections from prior pytest runs; the sweep correctly skips DBs with any active backend. If the sweep itself is broken, `_drop_orphan_workers_older_than` raises `AssertionError` on a regex regression — read the warning and re-tighten the regex. **Never `docker restart ebull-postgres` while the data dir is bloated** — the entrypoint `chown` + unclean-shutdown `fsync` are file-count-bound and cost ~30 min on a millions-of-files dir; clear the leak first (cleanup tool) or kill the specific wedged backend in-container (`docker exec ebull-postgres kill <pid>`).

## Scope shared-DB assertions to the entity under test

In an integration test that runs against a per-worker DB shared across tests in the same worker, do NOT assert on a *global* shape of an accumulating table — `assert seen == []`, `assert len(rows) == 1`, `assert count == N`. Even with a per-test `TRUNCATE` rail, that assertion is implicitly coupled to the truncation behaviour: a future fixture change that stops truncating one table, or a sibling test that commits a row of the same kind, flips the result and the failure reads as a logic regression when it is test cross-talk.

Assert on the **specific entity the test created** instead. Seed a unique key (CIK, accession, symbol) and assert membership on that key — `assert any(cik in url for url in seen)` / `assert not any(...)` — not the whole collection's cardinality. First seen: #1337 P2 (PR #1377) — `assert seen == []` on a recording `http_get` over a per-test-TRUNCATEd `institutional_filers`; the assertion happened to pass but was fragile by construction. The fix is CIK-targeted membership.

## Slim test-data posture

Migrations are **schema-only**. Any `INSERT`/`UPDATE` in a `sql/NNN_*.sql` migration that puts more than ~5 rows of non-reference data into a public table is a defect — file a follow-up ticket, move the seed into a per-test fixture.

The audit shape (run on a fresh `ebull_test_template`):

```bash
docker exec ebull-postgres psql -U postgres -d ebull_test_template -tAc \
  "SELECT relname, reltuples::bigint FROM pg_class \
   WHERE relkind='r' AND relnamespace='public'::regnamespace \
     AND reltuples > 0 ORDER BY reltuples DESC LIMIT 20;"
```

A clean template returns ONLY `schema_migrations` (= count of applied migration files). Anything else with non-zero `reltuples` is either documented reference data (currency / country / fundamental-key catalogues) or a defect.

Operationally:

- Test fixtures seed 1–5 rows per-test through `ebull_test_conn`.
- Bulk-data tests (e.g. ranking-engine integration over 10k rows) are auto-`db`-marked, so they never run on the push gate; genuinely long-wall-clock ones also take `@pytest.mark.perf` (registered nightly-tier marker; deselect with `-m "not perf"`). There is no `slow` marker, and CI runs no pytest — no separate CI job to punt them to.
- Reference data that genuinely belongs in a migration must be flat (`INSERT VALUES (...)` only, no DML loops); a future migration cannot start growing the seed without showing up in the audit above.

Audited 2026-05-19 against #1208 Phase 2 — fresh template returned exactly `schema_migrations | 133`, no other non-zero tables. Codebase already honours this rule; the audit + this skill are the prevention against drift.

## Test naming

Method names describe the scenario and expected outcome:
- `test_first_thesis_gets_version_1` ✓
- `test_insert` ✗

No test longer than ~20 lines. If it's longer, the function under test probably does too much, or the test is testing too many things at once.

## DB-down / 503 tests must pin the connection state, not just a service mock

A test that claims to prove a "Postgres unreachable → 503" path must make the DB-down condition **deterministic and environment-independent** — not rely on whatever the shared `TestClient` happens to carry in CI.

- If the endpoint **self-connects** (`psycopg.connect`, e.g. `/system/postgres-health`), patch that symbol → the 503 is independent of the pool. Say so in a comment so a reader doesn't assume the pool matters.
- If the endpoint uses the pooled **`get_conn`** path (e.g. `/system/status`), force the failure via `app.dependency_overrides[get_conn]` (raise the 503, or yield from a dead pool), NOT by mutating `app.state.db_pool`. `app.state` is shared global state — mutating it leaks across tests (a `delattr`/restore in teardown silently broke an unrelated `/auth/me` test in PR #1394). `dependency_overrides` is the designed-for-tests seam; save/restore it tracking presence separately (prevention-log #234).
- Cover BOTH `/system/*` flavors when both exist (self-connect handler + `Depends(get_conn)` handler) — they fail through different code paths. Unit-test `get_conn`'s own error→503 mapping separately.

Origin: PR #1394 review (#1325/#1217). A bearer-path 503 test that only patched the service-layer `psycopg.connect` was flagged as potentially passing for the wrong reason; the first fix (mutating `app.state.db_pool`) then leaked across tests — `dependency_overrides` is the leak-free seam.

## Migration-behaviour tests: strip the BEGIN/COMMIT wrapper before inline execution

A migration-behaviour test (seed pre-state → run the migration SQL inline → assert post-state) must NOT `cur.execute()` the raw migration file when that file carries its own `BEGIN; … COMMIT;` wrapper. The embedded `COMMIT` commits out from under the test's transaction control; the trailing `conn.commit()` then flushes an empty implicit transaction, and whether that raises depends on the test connection's autocommit mode — the test passes by accident.

- Strip the standalone `BEGIN;`/`COMMIT;` lines and execute the body inside the fixture's transaction, letting the test commit once:
  ```python
  _BODY = "\n".join(l for l in _MIGRATION_SQL.splitlines() if l.strip() not in ("BEGIN;", "COMMIT;"))
  ```
- Or run the file via a dedicated `autocommit=True` connection. Either way the wrapper must not be executed inline through the regular fixture cursor.
- Migrations with NO embedded transaction (e.g. `sql/111`) are fine to execute directly — the issue is only the embedded `BEGIN/COMMIT`.

Origin: PR #1467 (#1320) review WARNING — `test_migration_182_pre14a_purge.py` executed `sql/182` (which wraps in `BEGIN;…COMMIT;`) inline then called `conn.commit()` again. EXTRACTED here rather than as a grep lint: the failure mode is narrow (migration tests only) and the fix is a one-liner at the call site.

## A negative test must fail for the RIGHT reason

Asserting `== []` / "does NOT fire" only proves something if the branch you
*claim* to cover is what suppressed it — not an incidental earlier guard. A
multi-guard function (e.g. `detectCoverageGaps`: cross-day guard THEN
closed-window guard) will short-circuit on the first guard, so a fixture meant
to exercise guard #2 can pass entirely on guard #1 and silently cover nothing.

Construct the fixture so ONLY the target guard can be responsible: hold every
earlier guard non-firing. For the closed-window case that meant a SAME-NY-date
pair (cross-day guard inert) with the prev bar in a pre-04:00 closed window —
not a cross-day pair that passes before the closed check is ever reached.

Origin: PR #1763 (#1754) review WARNING — the "closed window" gap test passed
because the two bars were different NY dates, never reaching the closed guard.

### Vacuous `all(pred for x in seq)` when the empty case is the expected outcome

`assert all(pred for p in seq)` is `True` for an empty `seq` — so when the
DOCUMENTED expected outcome is "no rows are produced", the `all(...)` proves
nothing (it passes whether the bad row was rejected or was simply never built).
When emptiness is the expected result, assert it directly:

```python
assert fy == []            # not: assert all(p.revenue is None for p in fy)
```

If a non-empty result is also valid (some rows survive, none carrying the bad
value), pair an emptiness/length assertion with a value assertion that does not
quantify over a possibly-empty set — e.g. `assert all(p.revenue != BAD for p in periods)`
guarded by a prior `assert len(periods) >= 1`.

Origin: PR #1837 (#1835) review WARNING — `test_fy_rejects_quarter_duration_mislabeled_fy`
asserted `all(p.revenue is None for p in fy)` where the expected outcome was an
empty `fy`, so the assertion was vacuously true and did not cover the rejection.
