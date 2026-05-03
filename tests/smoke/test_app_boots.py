"""Smoke test: the FastAPI app actually boots end-to-end.

Why this exists
---------------
On 2026-04-08 the backend failed to start because
``app/security/master_key.py`` had a SQL typo (``JOIN operators o ON
o.id = bc.operator_id`` -- the operators PK is ``operator_id``, not
``id``). The bug was inside the FastAPI lifespan, so it only fired on
real startup. Every existing unit test mocked the cursor, so 880
pytest checks were green while the running app was dead on its face.

The fix is this file: drive ``app.main.app`` through ``TestClient`` as
a context manager. ``TestClient.__enter__`` runs the lifespan against
the real database (migrations + pool open + master-key bootstrap), so
any SQL/import/config error in the lifespan path fails this test
loudly instead of being discovered by hand at the sign-in screen.

DB requirement and CI behaviour
-------------------------------
The smoke test requires a real Postgres at ``settings.database_url``.
On developer machines and any pipeline that brings up the dev DB this
just works. In a Postgres-less environment the module-level
``_db_reachable()`` probe runs once at collection time and the test
``skip``s with a clear reason instead of failing with an opaque
psycopg connection error. The skip is intentional: the smoke gate's
job is "did the lifespan come up against a real DB", and that
question is unanswerable without a DB -- a noisy failure there would
just train people to ignore the gate.

Keep this file fast and dependency-free: no fixtures, no mocks. The
in-test assertions are deliberately minimal -- the *real* test is
``TestClient.__enter__`` returning at all (that is what the original
master_key crash would have failed). The post-enter assertions are a
cheap coherence check that the lifespan finished setting the
``app.state`` flags it is contracted to set, so a future bug that
silently leaves the app in a half-initialised state (lifespan
returns but ``broker_key_loaded`` / ``boot_state`` never get
populated) also fails this test.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

# State flags the lifespan in ``app/main.py`` is contracted to write.
# Imported here as a module-level constant so the per-test cleanup
# loop and the post-enter coherence assertions reference the same
# canonical list -- if a future lifespan adds another flag, updating
# this tuple in one place keeps both checks in sync.
_LIFESPAN_STATE_FLAGS: tuple[str, ...] = (
    "boot_state",
    "needs_setup",
    "recovery_required",
    "broker_key_loaded",
    "db_pool",
)


def _db_reachable() -> bool:
    """Probe the dev DB once at collection time.

    A short connect timeout keeps the skip path fast in CI envs that
    have no Postgres at all (the default psycopg connect timeout is
    long enough to feel like a hang). Any failure -- import-time
    config errors, settings validation errors, DNS, refused, auth,
    timeout -- is treated identically as "DB not available"; the
    smoke gate is not the place to diagnose connection problems.

    The settings + psycopg imports are inside the function body (not
    at module top-level) so a Pydantic validation error reading
    ``EBULL_DATABASE_URL`` cannot blow up pytest collection -- it
    becomes a clean skip with the same reason string. ``Exception``
    is the right catch breadth here: ``BaseException`` would
    swallow ``KeyboardInterrupt`` and ``SystemExit``, which we
    explicitly want to propagate.
    """
    try:
        import psycopg

        from app.config import settings

        with psycopg.connect(settings.database_url, connect_timeout=2) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _db_reachable(),
    reason="dev Postgres not reachable; smoke test requires the real DB",
)


def test_app_lifespan_boots_and_state_is_coherent() -> None:
    """Drive the real lifespan; fail loud if anything in startup breaks.

    The structural assertion is the ``with`` block returning at all:
    if ``master_key.bootstrap`` (or any other lifespan step) raises,
    ``TestClient.__enter__`` propagates the exception and the test
    fails before any assert runs. That is exactly the failure mode
    the original master_key SQL typo produced.

    The post-enter assertions then verify the lifespan actually
    populated the ``app.state`` contract documented in
    ``app/main.py::lifespan``. ``app`` is a module-level singleton
    and other tests in the session may have entered ``TestClient(app)``
    before this one, leaving stale ``app.state`` attrs from a prior
    lifespan run. A naive ``hasattr`` check could pass against that
    stale state even if *this* test's lifespan was somehow
    short-circuited. To make the assertions prove this run's writes,
    delete every flag from ``app.state`` before entering the
    TestClient -- the post-enter assertions then unambiguously
    reflect what lifespan wrote during this call.
    """
    # Defer the import so the module-level skipif evaluates first;
    # this also keeps a Pydantic settings error from blowing up
    # collection (handled by _db_reachable's skip path).
    from app.main import app

    # Snapshot any pre-existing flags so the teardown loop in
    # ``finally`` can restore them if ``TestClient.__enter__`` raises.
    # Without this, a lifespan failure (the exact thing this test
    # exists to catch) would leave ``app.state`` half-deleted and
    # corrupt subsequent tests in the same session that read those
    # attrs directly. Use a sentinel rather than ``None`` because
    # ``None`` is a legal value for some flags.
    _SENTINEL = object()
    snapshot: dict[str, object] = {flag: getattr(app.state, flag, _SENTINEL) for flag in _LIFESPAN_STATE_FLAGS}
    for flag in _LIFESPAN_STATE_FLAGS:
        if hasattr(app.state, flag):
            delattr(app.state, flag)

    # Other test modules register mock dependency_overrides at import
    # time (the ``setdefault(get_conn, _fallback_conn)`` pattern used
    # by every test_api_*.py file).  When pytest collects those modules
    # before this smoke test, the mock replaces the real ``get_conn``
    # dependency — and endpoint requests inside the TestClient hit the
    # mock's empty result iterator instead of the real DB.  Remove the
    # ``get_conn`` override for the duration of this test so endpoint
    # requests use the real connection pool opened by the lifespan.
    # The auth no-op override (installed by conftest.py) must remain.
    from app.db import get_conn

    had_get_conn = get_conn in app.dependency_overrides
    saved_get_conn = app.dependency_overrides.pop(get_conn, None)

    try:
        with TestClient(app) as client:
            # Lifespan must have populated every flag the rest of the
            # app reads off ``app.state``. Missing attributes here
            # mean the lifespan returned early or skipped its writes
            # -- and because we just deleted them above, a pass here
            # is unambiguously this run's work, not stale state from
            # an earlier test.
            for flag in _LIFESPAN_STATE_FLAGS:
                assert hasattr(app.state, flag), f"lifespan did not set app.state.{flag}"
            # boot_state must be one of the documented values; an
            # unknown string would mean the bootstrap returned a
            # state the rest of the codebase has no branch for.
            assert app.state.boot_state in {
                "clean_install",
                "normal",
                "recovery_required",
            }
            # JobRuntime moved out of process in #719. The API lifespan
            # MUST NOT set ``app.state.job_runtime``; smoke test pins
            # the absence so a future regression that re-introduces
            # in-process scheduling fails this assertion before it
            # ships.
            assert not hasattr(app.state, "job_runtime"), (
                "API lifespan set app.state.job_runtime — JobRuntime must live in the dedicated jobs process (see #719)"
            )
            # /health is the cheapest end-to-end probe that the
            # routing layer is also wired up -- if it 500s, lifespan
            # came up but the app object itself is broken.
            # 200 = all layers healthy; 503 = one or more layers need
            # attention (normal on a dev DB that has not run all syncs).
            # Either is a valid liveness response; 500 would indicate
            # the handler itself is broken.
            resp = client.get("/health")
            assert resp.status_code in {200, 503}, resp.text
            assert resp.json().get("system_state") in {"ok", "needs_attention", "error"}, resp.text

            # /budget exercises the full SQL path in compute_budget_state
            # against the real schema. This catches column-name mismatches
            # (e.g. referencing cm.status or cmp.current_value when the
            # actual columns have different names) that mock-based unit
            # tests silently miss because they never run real SQL.
            # budget_config is seeded by migration 027 so the singleton
            # row is always present on a migrated dev DB.
            resp = client.get("/budget")
            assert resp.status_code == 200, resp.text
            assert "available_for_deployment" in resp.json()
    finally:
        # Restore the snapshot regardless of how the body exited.
        # On the success path TestClient's exit hook has already run
        # the lifespan shutdown and may have written its own values;
        # we still restore the pre-test snapshot so a subsequent
        # test that imported ``app`` for its non-lifespan-managed
        # state sees exactly what it would have seen if this smoke
        # test had not run. On the failure path (lifespan crashed
        # mid-startup) restoration is the whole point: subsequent
        # tests should not inherit a half-deleted state.
        # Restore the get_conn override so subsequent tests that rely on
        # the module-level ``setdefault`` pattern still see their mock.
        if had_get_conn:
            app.dependency_overrides[get_conn] = saved_get_conn  # type: ignore[assignment]

        for flag, value in snapshot.items():
            if value is _SENTINEL:
                if hasattr(app.state, flag):
                    delattr(app.state, flag)
            else:
                setattr(app.state, flag, value)


def test_insider_initial_holdings_value_owned_column_exists() -> None:
    """Recovery gate for migration 093 schema drift (#789).

    Migration 093 created ``insider_initial_holdings`` with a
    ``value_owned`` column, but on a DB that already had the table
    from a parallel experiment ``CREATE TABLE IF NOT EXISTS`` was a
    no-op and the column never landed. Migration 101 explicitly
    ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS value_owned`` to
    repair the drift. This smoke test pins the column's existence so
    a regression on the recovery path fails loud at boot rather than
    silently breaking the Form 3 baseline reader.
    """
    import psycopg

    from app.config import settings

    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'insider_initial_holdings'
                  AND column_name = 'value_owned'
                """,
            )
            row = cur.fetchone()
    assert row is not None, (
        "insider_initial_holdings.value_owned column missing — migration 101 did not apply or was no-op'd."
    )
    assert row[1] == "numeric", f"value_owned has unexpected type {row[1]!r}; expected NUMERIC."
