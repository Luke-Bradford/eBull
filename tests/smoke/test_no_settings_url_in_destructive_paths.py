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
* Use the isolated ``ebull_test`` pattern from
  ``tests/test_operator_setup_race.py`` instead -- copy
  ``_swap_database`` / ``_ensure_test_db_exists`` /
  ``_apply_migrations_to_test_db`` / ``_assert_test_db``.
* The PREVENTION note on PR #129 round 1 explicitly asked for
  this guard. Removing or weakening it requires a written
  rebuttal in a follow-up PR.
"""

from __future__ import annotations

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
    "smoke/test_app_boots.py": "read-only lifespan + reachability probe",
    # The guard itself contains the forbidden patterns as data
    # (the ``_FORBIDDEN_PATTERNS`` literals above). Exclude it to
    # avoid a self-match.
    "smoke/test_no_settings_url_in_destructive_paths.py": "the guard itself",
    # Read-only reachability probe + advisory-lock semantics test for
    # the JobLock primitive (#13 PR A). The probe runs ``SELECT 1`` to
    # decide whether to skip (no Postgres -> clean skip) and the test
    # bodies exercise ``pg_try_advisory_lock`` / ``pg_advisory_unlock``,
    # both of which are session-scoped state -- they write no rows and
    # cannot CASCADE into broker_credentials. Cannot use the
    # ``ebull_test`` pattern here because the JobLock implementation
    # opens its own connection internally; the test must connect to
    # the same DB the JobLock will resolve to via ``settings.database_url``.
    "test_jobs_locks.py": "read-only reachability probe + advisory-lock semantics; no row writes",
    # #719 dispatcher tests open a connection against settings.database_url
    # to exercise the durable-queue helpers against the real DB. Writes
    # are scoped to the ``pending_job_requests`` table and a per-test
    # cleanup fixture deletes the request_ids it created in teardown,
    # so the dev DB's other tables are never touched. Cannot use
    # ``ebull_test`` because the dispatcher's helpers themselves resolve
    # the URL from ``settings.database_url`` internally.
    "test_sync_orchestrator_dispatcher.py": "scoped writes to pending_job_requests with per-test cleanup",
    # #719 listener / heartbeat / queue-recovery tests open connections
    # against the dev DB to exercise queue claim + heartbeat upsert +
    # boot-recovery branches. Writes are scoped to pending_job_requests,
    # job_runtime_heartbeat, job_runs.linked_request_id, and
    # sync_runs.linked_request_id, with per-test cleanup deleting only
    # the rows the test created. Cannot use ``ebull_test`` because the
    # helpers resolve ``settings.database_url`` internally.
    "test_jobs_listener.py": "scoped writes via dispatcher helpers with per-test cleanup",
    "test_jobs_heartbeat.py": "scoped writes to job_runtime_heartbeat with per-test cleanup",
    "test_jobs_queue_recovery.py": "scoped writes to pending_job_requests + linked-run rows with per-test cleanup",
    "test_jobs_queue_boot_drain.py": "scoped writes to pending_job_requests with per-test cleanup",
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
