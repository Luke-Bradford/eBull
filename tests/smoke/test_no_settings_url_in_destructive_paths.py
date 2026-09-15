"""Static guard: tests must not connect to ``settings.database_url``.

Why this exists
---------------
On 2026-04-08 the user discovered that every pytest run was wiping
their dev database -- ``tests/test_operator_setup_race.py`` ran a
TRUNCATE against ``settings.database_url`` (i.e. ``ebull``, the dev
DB) and the FK CASCADE took out their saved broker credentials too.
The fix on PR #129 isolated that one test to ``ebull_test``, but
without a structural guard a future test author can re-introduce
the same bug just by typing ``psycopg.connect(settings.database_url)``
inside a fixture and adding a TRUNCATE next to it.

This test is the structural guard.

Per #893, the test fixture now provisions a per-worker private
database (``ebull_test_<run_id>_<worker_id>``) so concurrent pytest
invocations cannot collide. The guard's role is unchanged — it ensures
no test reaches around the fixture and connects to the operator's
dev DB directly. The single documented exception is
``tests/smoke/test_app_boots.py``, which drives the FastAPI lifespan
against the real dev DB by contract; that file is wrapped in a
cluster-wide Postgres advisory lock so concurrent invocations
serialise on the lifespan migrations rather than racing them.

What it catches
---------------
The guard greps every test file for any of the patterns in
``_FORBIDDEN_PATTERNS``. The patterns target the *concrete bug
shape* (a connection opened directly against ``settings.database_url``),
not the bare token: this lets the race test refer to
``settings.database_url`` legitimately inside its
``_swap_database`` helper without needing an allowlist entry,
because that helper *derives* an isolated test URL rather than
connecting directly to the dev one.

What it does not catch
----------------------
The grep cannot follow aliases. A test file that does
``db_url = settings.database_url`` and then
``psycopg.connect(db_url)`` will pass the guard but still point at
the dev DB. This is a deliberate trade-off: a string-level grep
catches the direct footgun (the exact pattern that hit the user)
with zero false positives in this codebase, while an AST walk that
follows aliases is significant scope creep and would itself need
tests. Defence in depth for the runtime case lives in
``_assert_test_db`` inside ``test_operator_setup_race.py``, which
runs ``SELECT current_database()`` before any TRUNCATE and refuses
to proceed against anything but ``ebull_test``.

If you are a future test author hitting this guard
---------------------------------------------------
* Do NOT add yourself to ``_ALLOWED`` to make the test pass.
* Use the per-worker isolated DB by importing
  ``test_database_url`` from ``tests.fixtures.ebull_test_db``.
  If the code under test opens its own connection internally
  via ``settings.database_url`` (e.g. dispatcher helpers), use a
  ``monkeypatch.setattr("app.config.settings.database_url",
  test_database_url())`` autouse fixture in the test file —
  that points the helper at the per-worker test DB without
  any production-code change.
* The PREVENTION note on PR #129 round 1 explicitly asked for
  this guard. Removing or weakening it requires a written
  rebuttal in a follow-up PR.
"""

from __future__ import annotations

import re
from pathlib import Path

# Concrete bug-shape patterns. Each entry is a substring search;
# any match in a non-allowlisted test file fails the guard.
#
# These target the exact way you would use ``settings.database_url``
# to perform a destructive operation, not the bare token. Adding a
# new pattern here is the right move whenever a new way to "open a
# connection directly against settings.database_url" appears in
# practice.
_FORBIDDEN_PATTERNS: tuple[str, ...] = (
    # Broad ``.connect(...)`` catch: matches ``psycopg.connect``,
    # ``asyncpg.connect``, ``sqlalchemy.create_engine``-fronted
    # connect calls -- any driver or helper that opens a single
    # connection directly. Also matches the bare ``connect(...)``
    # form if a future test imports the function under a name.
    "connect(settings.database_url",
    # Broad ``*Pool(...)`` catch: matches ``ConnectionPool``,
    # ``AsyncConnectionPool``, and any other pool constructor a
    # future driver might add. The original guard missed
    # ``AsyncConnectionPool`` because it pinned the prefix
    # (PR #129 round 3 review).
    "Pool(settings.database_url",
    # #2224 residual 3. ``JobLock`` opens its OWN psycopg connection
    # inside ``__enter__`` and holds a session-scoped advisory lock on
    # it, so it is precisely the "driver or helper that opens a single
    # connection directly" the broad ``connect(`` pattern above was
    # written for — but the call site reads ``JobLock(...)``, so the
    # substring never matched and three modules
    # (``test_joblock_per_source``, ``test_job_lock_reentrancy``,
    # ``test_db_lane_family_split``) locked real production source keys
    # on the operator's dev DB for months.
    #
    # The harm is NOT a destructive write — it is bidirectional lock
    # contention with the live jobs daemon, which holds those same keys
    # on ``ebull`` (``job_source:etoro`` ~8.8% of wall clock). That
    # broke the tests intermittently AND could refuse a real job fire.
    # Advisory locks are PER-DATABASE (measured, PG 17.9), so routing
    # the acquire at ``test_database_url()`` removes both directions.
    "JobLock(settings.database_url",
    "test_only_per_name(settings.database_url",
)

# Files allowed to contain a forbidden pattern. Read-only paths
# only; every entry must be justified inline.
#
# The race test (``test_operator_setup_race.py``) is intentionally
# *not* on this list. It references the literal token
# ``settings.database_url`` only inside ``_swap_database``, which
# derives the test DB URL -- it never directly opens a connection
# against the dev DB, so it does not match any forbidden pattern
# and needs no allowlist entry. This is the whole point of greping
# the bug shape rather than the bare token.
_ALLOWED: dict[str, str] = {
    # Read-only reachability probe. ``test_app_boots.py`` opens a
    # connection against ``settings.database_url`` to decide whether
    # to skip (no Postgres -> clean skip rather than opaque error)
    # and to drive the FastAPI lifespan via TestClient. Both paths
    # are read-only -- the probe runs ``SELECT 1`` and the lifespan
    # opens the pool and applies migrations, but no test code in
    # this file ever issues a destructive statement against the
    # connection. The smoke gate's *job* is "did the lifespan come
    # up against the same DB the running app uses", which is
    # unanswerable without using ``settings.database_url``.
    "smoke/test_app_boots.py": (
        "documented dev-DB exception (#893 SC#5): smoke gate drives "
        "FastAPI lifespan against the real dev DB by design, wrapped in "
        "a cross-invocation Postgres advisory lock"
    ),
    # The guard itself contains the forbidden patterns as data
    # (the ``_FORBIDDEN_PATTERNS`` literals above). Exclude it to
    # avoid a self-match.
    "smoke/test_no_settings_url_in_destructive_paths.py": "the guard itself",
    # Read-only schema introspection. ``test_schema_drift.py`` (B5
    # of #797 pulled forward into Batch 1 of #788) parses
    # ``CREATE TABLE`` blocks from sql/*.sql and compares declared
    # columns against ``information_schema.columns`` on the live dev
    # DB to catch the migration-093 class of bug (CREATE TABLE IF
    # NOT EXISTS no-op'd onto a pre-existing table with a different
    # shape). The whole point of the gate is to validate against the
    # DB the running app actually uses, so it MUST connect to
    # ``settings.database_url``. No writes anywhere — only
    # ``SELECT column_name FROM information_schema.columns``.
    "smoke/test_schema_drift.py": "read-only information_schema introspection of the live dev DB schema",
    # Read-only safety-primitive tests. ``test_runbook_safety.py``
    # (#1233 Stream A PR-D) exercises ``app/runbooks/safety.py``
    # primitives — ``assert_dev_db`` verifies
    # ``current_database()`` against an allowlist that
    # *specifically targets the dev DB name*, and
    # ``assert_jobs_process_stopped`` /
    # ``wait_for_jobs_process_started`` probe
    # ``JOBS_PROCESS_LOCK_KEY`` which is per-database (the
    # production fence holds against the live dev DB, not a test
    # one). Routing these to ``ebull_test_<worker>`` would defeat
    # the test's premise. Read-only: ``SELECT current_database()`` +
    # ``pg_try_advisory_lock`` + ``pg_advisory_unlock`` — no
    # destructive writes.
    "test_runbook_safety.py": (
        "read-only safety-primitive tests; verify dev-DB-targeted behaviour by design (#1233 Stream A PR-D)"
    ),
    # Read-only pg_settings introspection. ``test_pg_settings_call_sites.py``
    # (#1187) verifies ``max_locks_per_transaction`` GUC on the live
    # dev DB — the value lives in postgresql.conf, not a test
    # template, so a per-worker DB derived from the template would
    # not reflect operator-tunable postgres.conf state. Read-only:
    # ``SHOW max_locks_per_transaction`` only.
    "test_pg_settings_call_sites.py": "read-only pg_settings introspection of the live dev DB GUC (#1187)",
    # ⚠ ``test_jobs_process_probe_fence.py`` was allowlisted here on the
    # justification "read-only singleton-fence advisory-lock tests against
    # the live dev DB (#1233 Stream A PR-D)". That file has used
    # ``test_database_url()`` for every acquire since PR-D's own bench
    # forced the PER-DATABASE correction, and it says so in its module
    # docstring. Entry REMOVED 2026-09-15 — surfaced by
    # ``test_every_allowlist_entry_is_still_load_bearing`` on that check's
    # first run, i.e. exactly the rot it was added to catch.
    #
    # ⚠⚠ That file is also where the correct fact has lived all along. The
    # #1233 correction landed in ONE module and never propagated to the
    # three joblock modules next door, which kept locking real source keys
    # on the dev DB under a "cluster-wide" comment until #2224 residual 3.
    # Per-worker test DB via monkeypatched ``settings.database_url``.
    # ``test_orchestrator_cancel.py`` (#1064 PR6) connects via
    # ``psycopg.connect(settings.database_url)`` INSIDE production
    # code under test (the orchestrator's cancel checkpoint), but the
    # ``settings_use_test_db`` fixture monkeypatches
    # ``settings.database_url`` to ``test_database_url()`` BEFORE any
    # destructive code runs. The substring match in the docstring +
    # the production-code description is a false positive: at runtime
    # all DELETE / INSERT / UPDATE statements hit the per-worker test
    # DB, never the dev DB.
    "test_orchestrator_cancel.py": (
        "monkeypatches settings.database_url to per-worker test DB before any destructive write (#1064 PR6)"
    ),
    # ⚠ ``test_job_lock_reentrancy.py`` was allowlisted here until
    # 2026-09-15 on the justification "monkeypatches
    # settings.database_url to per-worker test DB before any
    # destructive write (#1184)". That was true of 2 of its 8 tests.
    # The other six acquired real advisory locks on the operator's dev
    # DB directly, on the module's own (false) premise that advisory
    # locks are cluster-wide. Entry REMOVED rather than re-justified:
    # the file now routes every acquire at ``test_database_url()`` and
    # matches no forbidden pattern, so it needs no exception.
    # See ``test_every_allowlist_entry_is_still_load_bearing`` below —
    # a stale entry is how the wrong justification survived.
    #
    # Same monkeypatch pattern (#1273 PR2 — bootstrap stage-progress
    # instrumentation tests). Lives under tests/services/, so the key carries
    # the subdirectory prefix per the posix-relative-path contract below.
    "services/test_bootstrap_state_progress.py": (
        "monkeypatches settings.database_url to per-worker test DB before any destructive write"
    ),
    # The conftest contains the forbidden substring inside
    # *docstrings + error-message strings* that warn future authors
    # about the exact footgun. Including the message in the warning
    # is the entire point — the smoke gate's substring grep cannot
    # distinguish "documents the antipattern" from "executes the
    # antipattern". Verified: no live ``psycopg.connect(settings.database_url)``
    # call site in tests/conftest.py.
    "conftest.py": "occurrences are inside docstrings/warning-message strings, not live call sites",
    # #1472 fallout (#1503): the forbidden substring appears once, inside
    # an ASSERTION LITERAL that pins the seam contract — the test asserts
    # production source does NOT contain
    # ``psycopg.connect(settings.database_url, autocommit=True) as conn:``
    # (line ~131). It is documenting/forbidding the antipattern, not
    # executing it. Verified: no live call site; the only DB access is via
    # a ``settings_use_test_db`` fixture that monkeypatches
    # ``settings.database_url`` to ``test_database_url()`` first.
    "test_background_write_seam.py": (
        "forbidden substring is inside an assertion literal pinning the "
        "no-raw-connect seam contract, not a live call site (#1472/#1503)"
    ),
    # #1472 fallout (#1503): the forbidden substring appears once, inside
    # the module DOCSTRING describing the historical footgun
    # (``a raw psycopg.connect(settings.database_url) ... wrote to the dev
    # DB``). No live call site — the tests monkeypatch the jobs-process
    # probe and never open a dev-DB connection.
    "test_tripwire_jobs_exemption.py": (
        "forbidden substring is inside the module docstring describing the "
        "antipattern, not a live call site (#1472/#1503)"
    ),
}

_TESTS_DIR = Path(__file__).resolve().parents[1]


def test_no_test_writes_to_dev_database_url() -> None:
    """Fail if any test file opens a connection directly against
    ``settings.database_url`` outside the explicit allowlist.

    Match keys are full posix-relative paths from ``tests/``
    (including any subdirectory prefix), so a future move of an
    allowlisted file into a subdirectory is caught by the resulting
    failure to match -- not by the file silently slipping past.
    """
    offenders: list[tuple[str, str]] = []
    for path in sorted(_TESTS_DIR.rglob("*.py")):
        rel = path.relative_to(_TESTS_DIR).as_posix()
        if rel in _ALLOWED:
            continue
        text = path.read_text(encoding="utf-8")
        for pattern in _FORBIDDEN_PATTERNS:
            if pattern in text:
                offenders.append((rel, pattern))
                break

    assert not offenders, (
        "The following test files open a connection directly against "
        "settings.database_url, which would point at the dev DB and "
        "silently destroy user data on a destructive write:\n"
        + "\n".join(f"  {f} (matched {p!r})" for f, p in offenders)
        + "\n\nDestructive tests must connect to the isolated ebull_test "
        "database, not the dev DB. See tests/test_operator_setup_race.py "
        "for the pattern, and the docstring of this file for guidance."
    )


def test_every_allowlist_entry_is_still_load_bearing() -> None:
    """Fail if an ``_ALLOWED`` entry no longer matches any forbidden pattern.

    #2224 residual 3. An exception outlives the thing it excuses: the
    ``test_job_lock_reentrancy.py`` entry claimed the file monkeypatched
    ``settings.database_url`` before any destructive write, which was true
    of 2 of its 8 tests — the rest took real advisory locks on the dev DB.
    Nobody re-read the justification because nothing forced them to.

    An entry that matches nothing is either (a) a file that has since been
    fixed, in which case delete the entry, or (b) a file that no longer
    exists, in which case the key is a lie about the tree. Both are caught
    here rather than discovered the next time someone trusts the comment.
    """
    stale: list[str] = []
    for rel in sorted(_ALLOWED):
        path = _TESTS_DIR / rel
        if not path.exists():
            stale.append(f"{rel} (file does not exist)")
            continue
        text = path.read_text(encoding="utf-8")
        if not any(pattern in text for pattern in _FORBIDDEN_PATTERNS):
            stale.append(f"{rel} (matches no forbidden pattern)")

    assert not stale, (
        "These _ALLOWED entries no longer excuse anything:\n"
        + "\n".join(f"  {s}" for s in stale)
        + "\n\nDelete the entry. An allowlist exception that excuses nothing "
        "is a justification nobody is forced to re-read, which is how a "
        "factually wrong one survived from #1184 to #2224."
    )


# #2629 — the third occurrence of the #1887 class (see
# docs/review-prevention-log.md, "A mocked-module rename after a
# connection-helper migration leaves the mock patching a name nobody calls").
#
# The guard above catches a test that CONNECTS to the dev DB. It cannot catch a
# test that patches a module-local ``settings`` alias and hands the mock a
# REACHABLE url, because the leak happens inside production code:
#
#     with patch("app.workers.scheduler.settings") as mock_settings:
#         mock_settings.database_url = _test_database_url()   # real, reachable
#
# ``_tracked_job``'s write had migrated to
# ``app.db.background_write.background_write_connection``, which resolves
# ``settings`` through its OWN module import — so the patch rebound a name the
# write path no longer reads, every ``job_runs`` row landed in the operator's
# dev ``ebull``, and the assertions read an empty test DB. Measured on 2026-08-13:
# 40 such rows had accumulated there since 2026-08-10.
#
# What makes this shape uniquely dangerous is the REACHABILITY of the url, not
# the patch. The 29 other ``*.database_url = ...`` assignments in tests/ all
# assign a bogus literal (``"postgresql://test"``, ``"postgresql://stub/"``): if
# production code ever does consult them the connection FAILS LOUDLY, which is
# a correct-by-construction outcome. Only a real url fails silently.
#
# So the rule is narrow and has zero offenders tree-wide as of #2629: assign a
# STRING LITERAL to a mocked ``database_url``, or redirect the shared object via
# ``monkeypatch.setattr(settings, "database_url", test_database_url())`` /
# ``patch.object(settings, "database_url", ...)``, which keeps every consumer in
# step however many modules the write path crosses.
# The right-hand side is captured and tested in Python rather than excluded in
# the pattern: `\s*(?!["'])` READS like "the RHS must not start with a quote"
# and constrains nothing, because the engine backtracks `\s*` until the
# lookahead is satisfied. A lookahead placed after a backtrackable quantifier
# is always satisfiable.
_MOCK_DB_URL_ASSIGNMENT = re.compile(r"^[ \t]*(\w+\.database_url[ \t]*=[ \t]*)(.+)$", re.MULTILINE)


def test_no_reachable_url_assigned_onto_a_mocked_settings_alias() -> None:
    """Fail if a test assigns a non-literal url to a ``.database_url`` attribute.

    Patching a module-local ``settings`` name only redirects the modules that
    happen to read THAT name. Handing such a mock a reachable url therefore
    fails OPEN when production code moves its connection site — the exact
    #1887 / #2629 failure, silent in the direction that writes to the dev DB.
    """
    offenders: list[tuple[str, str]] = []
    for path in sorted(_TESTS_DIR.rglob("*.py")):
        rel = path.relative_to(_TESTS_DIR).as_posix()
        if rel == "smoke/test_no_settings_url_in_destructive_paths.py":
            continue  # the guard itself carries the shape as documentation
        for match in _MOCK_DB_URL_ASSIGNMENT.finditer(path.read_text(encoding="utf-8")):
            if match.group(2).lstrip().startswith(('"', "'")):
                continue  # bogus literal — a leak through it fails loudly
            offenders.append((rel, match.group(0).strip()))

    assert not offenders, (
        "These tests assign a non-literal (i.e. potentially REACHABLE) url to a "
        "mocked `.database_url`. A module-scoped settings patch only redirects "
        "the module it names, so when production code moves its connection site "
        "the write escapes to the operator's dev DB and nothing fails:\n"
        + "\n".join(f"  {f}: {m}" for f, m in offenders)
        + "\n\nEither assign a bogus string literal (a leak then fails loudly), or "
        "redirect the shared object: "
        'monkeypatch.setattr(settings, "database_url", test_database_url()).'
    )


def test_allowed_keys_resolve_to_real_files() -> None:
    """Every ``_ALLOWED`` key must resolve to an actual file under tests/.

    The guard uses exact dict-key matching (``rel in _ALLOWED``), so a
    typo or stale key (e.g. ``"tests/conftest.py"`` vs the correct
    ``"conftest.py"``) would silently become dead config: it never
    matches anything, but it also never fails the suite. This invariant
    asserts every key points at a file that actually exists today.

    Catches the bot-suggested footgun class: "a bare ``conftest.py``
    silently allowlists every subdir conftest". Under exact-match
    semantics it does NOT (bot was hedging — the matcher is
    ``if rel in _ALLOWED:`` not ``any(k in rel for k in _ALLOWED)``),
    but the invariant test makes that semantic explicit: a key only
    works if it resolves to one specific file.
    """
    missing: list[str] = []
    for key in _ALLOWED:
        if not (_TESTS_DIR / key).is_file():
            missing.append(key)
    assert not missing, (
        "_ALLOWED keys must resolve to actual files under tests/. "
        "These keys point at no file (typo, stale, or wrongly path-prefixed):\n" + "\n".join(f"  {k}" for k in missing)
    )
