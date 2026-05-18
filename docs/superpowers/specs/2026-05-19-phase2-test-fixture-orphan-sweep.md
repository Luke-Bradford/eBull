# Test-fixture orphan sweep + slim-data audit

> Status: **2026-05-19 (v1).**
>
> Issue: **#1208 Sub 2.** Branch: `feature/1208-phase2-test-fixture-orphan-sweep`.
>
> Phase 2 of `docs/superpowers/plans/2026-05-18-backend-stability.md`.

## 1. Problem

`pytest-xdist` (landed via #893) gives every worker its own private DB
named `ebull_test_<epoch>_<6hex>_<worker_id>`. The fixture relies on
`pytest_sessionfinish` to drop the DB at the end of the run. That hook
only fires on a graceful exit — a worker SIGSEGV, a user `Ctrl+C`, an
OS-level kill, or an OOM all leak the DB. The teardown path in
`drop_worker_database()` also catches `Exception` and demotes to
`warnings.warn(...)`, so an admin connection that fails for any reason
(role lost privilege, container restart mid-shutdown) silently leaves
the DB behind.

As of 2026-05-18 the dev cluster carries **45 leaked databases**.
Smoke spike (`docker exec ebull-postgres psql -tAc "SELECT datname
FROM pg_database WHERE datname LIKE 'ebull_test%';"`) returned the
list below — 11 distinct run-id epochs, two legacy `*sanity*` rows
from a pre-#893 helper, plus the live `ebull_test_template`.

```text
ebull_test_1778099633_acdcd2_sanity    -- 2026-05-06, legacy `*sanity*` shape
ebull_test_1778099667_0e2d64_sanity2   -- 2026-05-06, legacy
ebull_test_1779027822_7dfa2d_gw0..3    -- 2026-05-17
ebull_test_1779094078_38dd06_gw0..3    -- 2026-05-18
ebull_test_1779094949_5ddcec_gw0..3    -- 2026-05-18
ebull_test_1779107136_048a4c_gw0..3    -- 2026-05-18
ebull_test_1779108047_96523d_gw0..3    -- 2026-05-18
ebull_test_1779122029_058a86_gw0       -- 2026-05-18 (only gw0)
ebull_test_1779128473_116165_gw0..3    -- 2026-05-18
ebull_test_1779128758_a5e246_gw0..3    -- 2026-05-18
ebull_test_1779129079_5b46e4_gw0..3    -- 2026-05-18
ebull_test_1779132488_775fc8_gw0..3    -- 2026-05-18
ebull_test_1779132496_3c96b7_gw0,1,3   -- 2026-05-18 (gw2 missing)
ebull_test_1779132851_fc79d0_gw0..3    -- 2026-05-18
ebull_test_template                    -- LIVE, do not touch
```

Epoch dates verified with `python -c "import datetime; print(datetime.datetime.utcfromtimestamp(<n>).date())"` post-Codex-1a.

`pg_database_size(...)` for the leaked set blocks indefinitely (one
foreground probe hung for 90+ s and required `pg_terminate_backend`).
That hang is itself a symptom — pages on disk for the leaked set
swamp the per-DB walk and starve the autovacuum scheduler. The
cleanup is therefore not just hygiene; it unblocks Postgres health
checks that are about to land in Phase 4 (`/system/postgres-health`).

Closure framing: **HYGIENE PRIMITIVE.** Same shape as the
`drop_worker_database()` teardown that ships today — except this
fires on every controller-start, not only on graceful exit, and
sweeps the cross-invocation residue that the per-run teardown
fundamentally cannot reach.

## 2. Spike receipts (2026-05-18 dev cluster)

```text
$ docker exec ebull-postgres psql -U postgres -d postgres -tAc \
    "SELECT count(*) FROM pg_database
     WHERE datname LIKE 'ebull_test_%' AND datname != 'ebull_test_template';"
45

$ docker exec ebull-postgres psql -tAc \
    "SELECT pg_size_pretty(SUM(pg_database_size(datname))) FROM pg_database
     WHERE datname LIKE 'ebull_test_%' AND datname != 'ebull_test_template';"
# hangs > 90s; pg_terminate_backend required.
# du -sh /var/lib/postgresql/data/base inside container also stalls
# under autovacuum load. Cleanup itself is the unblocker.

$ docker exec ebull-postgres psql -tAc "SHOW shared_buffers; SHOW max_wal_size;"
shared_buffers | 2GB     # Phase 1
max_wal_size   | 4096    # Phase 1
```

Phase 1 (PR #1210 merge SHA `471a3b3`) Postgres tuning is in force on
this cluster. The leak count above accumulated under the pre-tuning
defaults; new runs after Phase 1 land on top of a now-stable WAL
budget but still leak DBs on the same crash paths.

## 3. Scope

| Task | Deliverable | Closure framing |
|---|---|---|
| T1 | `tests/fixtures/ebull_test_db.py::_drop_orphan_workers_older_than(min_age, *, now=...) -> list[str]` | HYGIENE PRIMITIVE |
| T2 | Wire `_drop_orphan_workers_older_than` into `build_template_if_stale()` under the existing `EBULL_TEMPLATE_LOCK` advisory lock | HYGIENE PRIMITIVE |
| T3 | `tests/test_orphan_sweep.py` — pre-create stale-named fake DB; assert dropped + `ebull` + `ebull_test_template` survive | TEST PRIMITIVE |
| T4 | One-shot slim-data audit captured in §6 of this spec + the PR description (no checked-in script — single-use measurement) | DOCS PRIMITIVE |
| T5 | `.claude/skills/engineering/test-quality.md` — append §"Slim test-data posture" + extend §"Dev-DB isolation invariant" with orphan-sweep reference | SKILL PRIMITIVE |
| T6 | `docs/review-prevention-log.md` — append §"Test-DB leaks accumulate on xdist-worker crash" | DOCS PRIMITIVE |

Subs 1 + 6 of #1208 shipped in Phase 1. Subs 3/4/5 are deferred to
Phases 3/4/5.

## 4. Design — `_drop_orphan_workers_older_than`

### 4.1 Signature

```python
from datetime import timedelta

def _drop_orphan_workers_older_than(
    min_age: timedelta = timedelta(hours=1),
    *,
    now: datetime | None = None,
) -> list[str]:
    """Drop ``ebull_test_<epoch>_<hex>_<suffix>`` databases older than ``min_age``.

    Returns the list of dropped DB names. ``now`` is injectable for the
    test path; production callers pass the default.
    """
```

`min_age` defaults to **1 hour**. Rationale below in §4.4.

### 4.2 Name parse — epoch from DB name

The worker DB name layout is fixed by `test_db_name()` in the same
module:

```python
name = f"ebull_test_{int(time.time())}_{token_hex(3)}_{worker_id}"
```

Parse regex pinned in the helper:

```python
_ORPHAN_NAME_PATTERN = re.compile(
    r"^ebull_test_(?P<epoch>\d{10})_[0-9a-f]{6}_(?:gw\d+|main|sanity\d*)$"
)
```

`{10}` digits because `int(time.time())` has been 10 digits since
2001 and stays so until 2286. A future widening would require a
fixture-name bump in this same file, so the regex is co-located with
the producer.

A DB name that does NOT match the regex is left alone. This is the
catch-all safety rail: `ebull`, `ebull_test_template`, `postgres`,
any human-managed DB the operator created by hand all fail the regex
and survive.

### 4.3 Three-rail safety model

The sweep applies THREE independent filters before any DROP. Each
rail closes a hole the prior one leaves open.

**Rail 1 — Session-lifetime keepalive (NEW, required for correctness).**
Today `ebull_test_conn` is function-scoped, so a worker's DB has NO
backend in `pg_stat_activity` between tests. A pure `pg_stat_activity`
filter would miss this gap and a sibling run >`min_age` old would lose
DBs mid-suite.

A new session-scoped autouse fixture
`tests/fixtures/ebull_test_db.py::_worker_db_keepalive` opens a single
admin-style `psycopg.connect(test_database_url(), autocommit=True)`
at session start and closes it at session end. That backend appears
in `pg_stat_activity` for the whole worker session, even between
tests, so the activity rail becomes load-bearing for live runs.

Test pseudo-code:

```python
# tests/fixtures/ebull_test_db.py
@pytest.fixture(scope="session", autouse=True)
def _worker_db_keepalive() -> Iterator[None]:
    if not test_db_available():
        yield
        return
    keepalive = psycopg.connect(test_database_url(), autocommit=True)
    try:
        yield
    finally:
        keepalive.close()
```

**Discovery wiring.** Fixtures defined in `tests/fixtures/ebull_test_db.py`
are NOT auto-discovered by pytest — only `conftest.py` is. The fixture
must be re-exported into `tests/conftest.py` exactly as
`ebull_test_conn` already is (current line 54-55):

```python
# tests/conftest.py
from tests.fixtures.ebull_test_db import (
    _worker_db_keepalive as _worker_db_keepalive,  # noqa: F401 — autouse import
    ebull_test_conn as ebull_test_conn,
)
```

Without this re-export the Rail 1 invariant is silently un-enforced,
which is exactly the BLOCKING failure mode Codex 1c flagged on the
prior revision.

**Rail 2 — Activity (pg_stat_activity).** With Rail 1 in place, a
sibling run's DB always has at least one backend (the keepalive).
The sweep filters candidates with `NOT EXISTS (SELECT FROM
pg_stat_activity a WHERE a.datname = d.datname)`.

```sql
SELECT datname
FROM pg_database d
WHERE d.datname LIKE 'ebull_test%'
  AND NOT EXISTS (
    SELECT 1 FROM pg_stat_activity a
    WHERE a.datname = d.datname
  )
```

**Rail 3 — Age backstop.** `min_age` defaults to **1 hour** as the
"backends-have-since-exited but pages remain" backstop. Catches the
crash residue.

**Plain DROP without FORCE.** The actual DROP uses
`DROP DATABASE IF EXISTS {name}` — NO `WITH (FORCE)`. Plain DROP
fails atomically (PG raises `ObjectInUse` / SQLSTATE 55006) if any
backend is still connected. That closes the TOCTOU window between
Rail 2's `pg_stat_activity` check and the DROP itself: a worker that
opens its keepalive in the gap will make the DROP raise
`ObjectInUse`, which the sweep catches + logs + skips. Eliminates
the "drop succeeds because we forced eviction" failure mode.

**Rail 0 — Hard-coded protect set.** Final literal guard. Helper
asserts `name not in _NEVER_DROP` immediately before the DROP. Hit
re-raises `AssertionError` past the outer `except Exception` (see
§4.6 for the explicit `except AssertionError: raise` ordering).

```python
_NEVER_DROP = frozenset({
    "ebull",
    "ebull_test_template",
    "postgres",
    "template0",
    "template1",
})
```

### 4.4 `min_age` + activity rationale (concurrency)

Two pytest invocations can run in parallel on the same dev cluster
(operator running `uv run pytest` in two terminals; pre-push hook
firing while CI clone runs locally). The sweep must NOT drop a sibling
invocation's live DBs.

Worker DB epoch is captured at `_run_id()` first-call (controller
process, before workers spawn). `_drop_orphan_workers_older_than`
applies BOTH filters:

1. **Activity (load-bearing):** any backend in `pg_stat_activity`
   pinned to the candidate DB ⇒ skip. A pytest worker holds an open
   connection through its test session, so a live sibling
   invocation's DBs cannot be dropped regardless of age.
2. **Age (backstop):** `now - parsed_epoch >= min_age` (default 1h) ⇒
   candidate eligible. Catches the residue: DBs whose backends have
   drained but whose disk pages remain because a crash skipped
   `pytest_sessionfinish`.

`pg_stat_activity` is the live evidence; `min_age` is the
"backends-have-since-exited but pages remain" backstop. Both must
agree before the DROP fires.

**CI exclusion:** the sweep is gated by `os.getenv("CI") == "true"`
on its own internal check — same gate the dev-DB tripwire uses
(`tests/conftest.py:122`). CI workflows tear the container down at
the end of the run so leaks are zero-cost there; spending sweep
overhead on every CI invocation is pure waste. `build_template_if_stale()`
itself still runs on CI; only the sweep call inside it is gated.

The `min_age` default is overridable via the `min_age` parameter for
the test path (`timedelta(seconds=0)` to drop the test fixture's
synthetic stale DB).

### 4.5 Drop strategy

`DROP DATABASE IF EXISTS {name}` — **NO `WITH (FORCE)`**. Plain DROP
raises `psycopg.errors.ObjectInUse` (SQLSTATE 55006) atomically if
any backend is still connected to the target; the sweep catches that
specifically + warns + moves on. This is what closes the TOCTOU race
between Rail 2's `pg_stat_activity` check and the DROP — a worker
that opens its keepalive in the gap forces the DROP to fail safely
instead of getting force-killed.

Rationale for diverging from the existing `_drop_database_force()`
helper: that helper is for the per-run teardown path where the
caller owns the DB and wants the eviction. The sweep targets DBs
whose ownership is exactly what we're trying to verify; eviction
without proof of ownership is the bug.

The helper uses its OWN admin connection (`_admin_database_url()`,
the maintenance `postgres` DB, `autocommit=True`). Does not share the
caller's connection — caller in `build_template_if_stale()` already
holds an admin conn at the call site, but reusing it would commingle
the sweep error-handling with the template-build error-handling; the
sweep wants to log + continue on a single-DB failure, the template
build wants to fail-loud. Separate connection keeps the contract
clean.

### 4.6 Error handling

Per-DB DROP wrapped in its own `try` that catches:

* `AssertionError` → **re-raised** (programmer-error rail, see §4.3
  Rail 0). MUST escape every outer handler.
* `psycopg.errors.ObjectInUse` → expected on a worker that opened
  its keepalive in the post-`pg_stat_activity` gap; logs DEBUG +
  continues.
* `Exception` → other psycopg/system failure on one DB; logs WARNING
  + continues so a sibling candidate still gets cleaned up.

The **whole helper body** is additionally wrapped in a top-level
`try/except Exception` (also explicitly re-raising `AssertionError`
first) so admin-connect failure, pg_stat_activity SELECT failure,
regex/parse failure, etc. all reduce to a `warnings.warn` + return
empty list. Orphan sweep is a hygiene step, not a correctness gate —
it MUST NOT turn a template-build path into a template-build
FAILURE. Critically: the outer handler MUST NOT swallow
`AssertionError` raised by the `_NEVER_DROP` rail. Shape:

```python
def _drop_orphan_workers_older_than(...) -> list[str]:
    if os.getenv("CI") == "true":
        return []
    try:
        return _do_sweep(...)
    except AssertionError:
        raise
    except Exception as exc:
        warnings.warn(f"Orphan sweep failed: {type(exc).__name__}: {exc}")
        return []
```

The `_NEVER_DROP` whitelist hits stay an `AssertionError` so a
programmer who introduces a regex regression sees the failure
loudly instead of a silent skip.

## 5. Design — `build_template_if_stale()` wiring

`build_template_if_stale()` (currently at
`tests/fixtures/ebull_test_db.py:388`) already acquires
`EBULL_TEMPLATE_LOCK` on the maintenance DB before doing any work.
The sweep runs **inside that lock**:

```python
def build_template_if_stale() -> None:
    ...
    with psycopg.connect(_admin_database_url(), autocommit=True) as admin:
        with admin.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (EBULL_TEMPLATE_LOCK,))
        try:
            # NEW: sweep orphans before any template work. Holds the
            # advisory lock so concurrent pytest controllers serialise
            # on the sweep + template build as a unit.
            _drop_orphan_workers_older_than()

            template_exists = _ensure_database(admin, TEMPLATE_DB_NAME)
            ...
        finally:
            with admin.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (EBULL_TEMPLATE_LOCK,))
```

Why under the lock: two controllers sweeping concurrently is safe
(the activity guard + regex + min_age prevent dropping each other's
DBs) but the serialisation is free given the lock is already there
and prevents one controller's sweep from racing another controller's
template-build mid-flight. Live sibling worker DBs (younger than
`min_age` or with active backends) keep running — the lock guards
sweep/template-build sequencing on the controller path, not test
execution generally. Holding the lock during the sweep adds at most
a few hundred milliseconds — DROP DATABASE WITH FORCE on an empty
leaked DB is sub-second; the worst case in the spike receipts is 45
drops, ~30s wall-time, one-shot.

The sweep does NOT fire from `ensure_worker_database()` — that path
is per-worker, called many times per pytest run, and a worker has no
business sweeping other workers' DBs.

## 6. Slim-data posture audit

### 6.1 Method

After T1+T2 land, rebuild `ebull_test_template` from scratch:

```bash
docker exec ebull-postgres psql -U postgres -d postgres -c \
    "DROP DATABASE IF EXISTS ebull_test_template WITH (FORCE);"
uv run pytest --collect-only tests/smoke/test_app_boots.py
# That triggers build_template_if_stale() in the controller.

docker exec ebull-postgres psql -U postgres -d ebull_test_template -tAc \
    "SELECT relname, pg_size_pretty(pg_total_relation_size(oid)) \
     FROM pg_class \
     WHERE relkind='r' AND relnamespace = 'public'::regnamespace \
       AND pg_total_relation_size(oid) > 0 \
     ORDER BY pg_total_relation_size(oid) DESC LIMIT 20;"
```

### 6.2 Expected output (filled post-implementation)

Each non-zero table is one of:

- **Reference data** (must stay): currency / country / fundamental-key
  catalogues seeded by a migration because tests rely on them.
- **Schema-only artefact** (acceptable): e.g. an `INSERT` on
  `schema_migrations` itself, or a `bootstrap_state` singleton row.
- **Bulk fixture leak** (defect): universe rows, financial_facts_raw
  spikes, anything that grows linearly with how many migrations a
  developer has run. Each defect → follow-up ticket to move the seed
  into a per-test fixture.

Findings table populated inline in this spec at PR-author time, and
mirrored in the PR description.

### 6.3 Rule codification

`.claude/skills/engineering/test-quality.md` gains a new section
`Slim test-data posture`:

- Migrations are **schema-only**. Any `INSERT`/`UPDATE` in a migration
  that puts more than ~5 rows of non-reference data into a public
  table is a defect — file a ticket, move the seed into a per-test
  fixture.
- Test fixtures seed 1-5 rows per-test through `ebull_test_conn`.
  Bulk-data tests opt out of the default suite with
  `@pytest.mark.slow` and run in a separate CI job.
- The orphan sweep (this PR) + the existing dev-DB tripwire +
  `_assert_test_db` form the three rails of the test-DB hygiene
  contract; cite the rails in this section.

## 7. Tests

### 7.1 `tests/test_orphan_sweep.py`

```python
from datetime import datetime, timedelta, timezone

import psycopg
import pytest
from psycopg import sql

from app.config import settings
from tests.fixtures.ebull_test_db import (
    TEMPLATE_DB_NAME,
    _admin_database_url,
    _create_empty_database,
    _drop_orphan_workers_older_than,
    _swap_database,
)


@pytest.mark.integration
def test_drops_stale_orphan_leaves_protected_dbs() -> None:
    # Stale-named fake (epoch=1 = 1970-01-01) older than any min_age.
    stale_name = "ebull_test_0000000001_aaaaaa_gw99"
    fresh_name = (
        f"ebull_test_{int(datetime.now(timezone.utc).timestamp())}_bbbbbb_gw98"
    )

    with psycopg.connect(_admin_database_url(), autocommit=True) as admin:
        _create_empty_database(admin, stale_name)
        _create_empty_database(admin, fresh_name)
    try:
        dropped = _drop_orphan_workers_older_than(timedelta(hours=1))
        assert stale_name in dropped
        assert fresh_name not in dropped
        # Sanity rails survive.
        assert "ebull" not in dropped
        assert TEMPLATE_DB_NAME not in dropped
        assert "postgres" not in dropped
    finally:
        # Belt + suspenders cleanup even if assertions blew up.
        with psycopg.connect(_admin_database_url(), autocommit=True) as admin:
            for name in (stale_name, fresh_name):
                with admin.cursor() as cur:
                    cur.execute(
                        sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                            sql.Identifier(name),
                        )
                    )
```

Plus a negative test pinning the protect-list:

```python
def test_drop_orphan_workers_refuses_protected_name() -> None:
    # Helper short-circuits on _NEVER_DROP regardless of regex.
    # (Internal contract test — exercised via parametrize over the protect set.)
    ...
```

Plus a test pinning Rail 2 (active-backend skip) — the load-bearing
invariant from Codex 1b:

```python
@pytest.mark.integration
def test_old_but_active_db_is_not_dropped() -> None:
    # Stale-named fake DB (epoch=1) BUT with a live keepalive — the
    # exact between-tests-gap case Codex 1b flagged.
    stale_active = "ebull_test_0000000001_cccccc_gw97"

    with psycopg.connect(_admin_database_url(), autocommit=True) as admin:
        _create_empty_database(admin, stale_active)

    # Hold a keepalive on the candidate so pg_stat_activity sees it.
    keepalive_url = _swap_database(settings.database_url, stale_active)
    keepalive = psycopg.connect(keepalive_url, autocommit=True)
    try:
        dropped = _drop_orphan_workers_older_than(timedelta(hours=1))
        assert stale_active not in dropped, (
            "active-backend DB must survive even when older than min_age "
            "(Codex 1b BLOCKING invariant)"
        )
    finally:
        keepalive.close()
        with psycopg.connect(_admin_database_url(), autocommit=True) as admin:
            with admin.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                        sql.Identifier(stale_active),
                    )
                )
```

### 7.2 Smoke

`uv run pytest tests/test_orphan_sweep.py` runs against the real test
cluster on dev box. Documented in PR description per ETL DoD §8.

## 8. Concurrency + race scenarios

| Scenario | Outcome |
|---|---|
| Two pytest controllers start within 1h of each other | Both sweeps see each other's DBs as < min_age ⇒ neither drops the other. Advisory-lock serialises the sweeps, so the second controller's sweep observes the first's freshly-built worker DBs. |
| Operator force-quits pytest mid-sweep | Sweep holds the advisory lock + an autocommit DROP. Worst case: the sweep dies mid-loop; the next controller-start picks up the remaining stale DBs. No corruption — DROP is atomic per DB. |
| Sweep racing autovacuum on a leaked DB | Autovacuum backend shows up in `pg_stat_activity` ⇒ Rail 2 skips the candidate. If the backend exits in the gap, plain DROP (no FORCE) succeeds atomically. Worst-case: candidate is skipped this round, swept on the next pytest invocation. No partial state. |
| Sibling invocation crashes while sweep is running | Sibling's DBs are < 1h old (skipped by min_age); only the prior-day leaks get dropped. |
| Helper called from an xdist worker by mistake | `build_template_if_stale()` already raises `RuntimeError` if `PYTEST_XDIST_WORKER` is set. Sweep is downstream of that check ⇒ workers never reach it. |

## 9. Definition of Done

ETL DoD clauses #8 + #11 + #12 apply (this is not a data-source change, so #9/#10 are N/A):

- **#8 Smoke** — `pytest tests/test_orphan_sweep.py` against real cluster: stale fake DB dropped, `ebull` + `ebull_test_template` survive.
- **#11 Operator-visible** — post-merge single `uv run pytest -q`, then `SELECT count(*) FROM pg_database WHERE datname LIKE 'ebull_test_%' AND datname != 'ebull_test_template'` returns ≤ worker count (ideally 0 after teardown).
- **#12 PR records** — spike receipts (45 leaked → 0), test cmd output, both SHAs.

## 10. Out of scope

- Migrating `pytest_sessionfinish` to run synchronously under a global lock — overkill; the sweep handles the residue.
- Aging the sweep based on `pg_stat_file('base/oid/PG_VERSION').modification` — superuser-only on managed PG; epoch-from-name is more portable.
- Dropping the template itself on a schema-hash mismatch — the existing `build_template_if_stale()` path already does that.
- Phase 4 `/system/postgres-health` endpoint — separate PR.

## 11. Codex 1a addressed findings (2026-05-19)

| Finding | Resolution |
|---|---|
| HIGH: age-only protection drops live pytest runs older than 1h ⇒ `DROP ... WITH (FORCE)` kills sibling workers, violating the stated invariant. | §4.3 + §4.4 — add **activity guard** (`pg_stat_activity` NOT EXISTS) as load-bearing rail; `min_age` demoted to backstop. Live workers cannot be dropped regardless of age because their backend holds an open connection. |
| MEDIUM: CI exclusion claim is unimplemented if scoped to caller-side; current `pytest_configure` builds the template on CI unconditionally. | §4.4 — CI gate moved INSIDE the sweep helper (`os.getenv("CI") == "true"` short-circuit returning empty list). `build_template_if_stale()` still runs on CI; only the sweep call is gated. |
| MEDIUM: aggregate helper specified to not raise but only per-DB DROP is wrapped — admin-connect/list/parse failures bubble up. | §4.6 — top-level `try/except Exception` wraps the whole helper body, reducing every internal failure to `warnings.warn` + return empty list. Hygiene step never breaks template build. |
| LOW: `_NEVER_DROP` is unreachable through the documented path (every entry already fails the regex). | §4.3 — kept as literal final guard, documented as belt-on-belt unreachable-through-current-path safety. Hits raise `AssertionError` (programmer error, not operational condition). |
| LOW: "quiescent state" wording overclaims — live sibling worker DBs younger than `min_age` still exist and execute concurrently. | §5 — wording softened to "lock guards sweep/template-build sequencing on the controller path, not test execution generally". |
| LOW: spike receipts table had wrong calendar dates (epoch 1778099633 ≠ 2026-04-30; recomputed to 2026-05-06 etc.). | §1 — dates recomputed; footnote added that `python -c "import datetime; print(datetime.datetime.utcfromtimestamp(<n>).date())"` verified each row. |
| LOW: test snippet missing imports + unused `monkeypatch` parameter. | §7.1 — imports added (`psycopg`, `pytest`, `sql`); `monkeypatch` removed; return type annotation added. |

## 12. Codex 1b addressed findings (2026-05-19)

| Finding | Resolution |
|---|---|
| BLOCKING: §4.3/§4.4 safety invariant is false — `ebull_test_conn` is function-scoped, so workers do not hold an open DB connection across the whole session; sibling >1h pytest run can be between-tests, look inactive, then get dropped. | §4.3 Rail 1 (NEW) — add session-scoped autouse `_worker_db_keepalive` fixture in `tests/fixtures/ebull_test_db.py`. Opens a single autocommit `psycopg.connect(test_database_url(), ...)` at session start, closes at session end. `pg_stat_activity` now shows entries for live worker DBs through the whole pytest session, even between tests. The activity rail becomes load-bearing. |
| BLOCKING: §4.5 TOCTOU — candidate passes `NOT EXISTS pg_stat_activity`, then a live worker opens a conn before DROP, `WITH (FORCE)` kills it. | §4.5 — dropped `WITH (FORCE)`. Use plain `DROP DATABASE IF EXISTS {name}`; PG raises `ObjectInUse` atomically if backends are still connected. Sweep catches `psycopg.errors.ObjectInUse` + logs + skips. Eviction-without-ownership-proof failure mode eliminated. |
| MEDIUM: §4.6 — outer `try/except Exception` swallows the `AssertionError` raised by `_NEVER_DROP`, contradicting the "raises AssertionError on hit" contract. | §4.6 — explicit `except AssertionError: raise` re-raise pinned in BOTH the per-DB loop and the outer wrapper. `AssertionError` is unconditionally re-raised past both handlers. Worked example block added to spec. |
| MEDIUM: §7.1 does not test the load-bearing activity guard — only stale-inactive drop and fresh-inactive survival. Old-but-active is exactly where the invariant lives. | §7.1 — added `test_old_but_active_db_is_not_dropped` exercising the between-tests-gap case (stale name + open keepalive ⇒ must survive). Test docstring cites the Codex 1b BLOCKING invariant. |
| LOW: §12 had duplicate heading "Codex 1b addressed findings". | This section — single heading, table-shaped, mirroring Phase 1's spec convention. |

## 13. Codex 1c addressed findings (2026-05-19)

| Finding | Resolution |
|---|---|
| BLOCKING: `_worker_db_keepalive` declared in `tests/fixtures/ebull_test_db.py` is not auto-discovered — pytest only picks up `conftest.py`. Rail 1 silently un-enforced. | §4.3 — added "Discovery wiring" subsection: fixture must be re-exported into `tests/conftest.py` alongside `ebull_test_conn` (line 54-55 today). Wired explicitly in T1 + T2 implementation. |
| MEDIUM: §8 concurrency row "Sweep racing autovacuum" still said `WITH (FORCE)` evicts the autovacuum backend — contradicts §4.5 plain-DROP decision. | §8 row reworded — `pg_stat_activity` skip OR `ObjectInUse` retry-next-invocation. |
| LOW: §7.1 active-backend test uses `_swap_database` + `settings` but neither was imported in the snippet. | §7.1 imports section — added `from app.config import settings` + `_swap_database` to the existing `tests.fixtures.ebull_test_db` import block. |
