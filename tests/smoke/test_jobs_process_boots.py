"""Smoke test: the jobs process bootstraps the master key at boot.

Why this exists
---------------
On 2026-05-01 the jobs process boot raised
``MasterKeyNotLoadedError`` on the very first scheduled
``daily_portfolio_sync`` / ``daily_candle_refresh`` tick because
``app/jobs/__main__.py::serve`` never called
``master_key.bootstrap()``. The API process lifespan does — the
jobs entrypoint did not. Every existing unit test mocked the
crypto layer or tested the API process's bootstrap, so 3000+
pytest checks were green while the jobs daemon was crashing every
job that touched encrypted credentials.

This test drives the bootstrap helper directly against the dev DB
so it can run alongside the live jobs daemon (the daemon holds the
singleton advisory lock — a full ``serve()`` invocation would
SystemExit).

Pattern mirrors ``test_app_boots.py``: minimal, fast, no mocks.
The structural assertion is "the helper completes without raising
``MasterKeyNotLoadedError``"; the post-boot probe is a coherence
check that the AES-GCM cipher is callable.
"""

from __future__ import annotations

import pytest


def _db_reachable() -> bool:
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


def test_jobs_bootstrap_master_key_loads_aesgcm() -> None:
    """Drive the jobs-entrypoint bootstrap helper; fail loud if the
    AES-GCM cipher is still uninitialised after it returns.

    Pre-fix the jobs entrypoint NEVER called ``master_key.bootstrap``,
    so a future regression that removes the helper invocation from
    ``serve()`` would silently re-introduce the
    ``MasterKeyNotLoadedError`` boot failure. This test pins the
    contract: after ``_bootstrap_master_key(pool)`` returns, either
    (a) the AES-GCM cipher is callable, or (b) the bootstrap legitimately
    ended in ``recovery_required`` mode (no ``EBULL_SECRETS_KEY`` to
    install). Failure mode the test catches: the helper isn't wired
    in or doesn't propagate the loaded key into ``set_active_key``.
    """
    from app.db.pool import open_pool
    from app.jobs.__main__ import _bootstrap_master_key
    from app.security import master_key
    from app.security.secrets_crypto import (
        MasterKeyNotLoadedError,
        _get_aesgcm,  # type: ignore[attr-defined]
    )

    pool = open_pool("jobs-bootstrap-smoke", min_size=1, max_size=2)
    try:
        _bootstrap_master_key(pool)

        # Outcome A — the bootstrap supplied a key and the helper
        # installed it via ``set_active_key``. Cipher is callable.
        try:
            _get_aesgcm()
            return
        except MasterKeyNotLoadedError:
            pass

        # Outcome B — bootstrap legitimately returned no key (clean
        # install / recovery_required). Confirm by re-reading the
        # bootstrap state directly so a NotLoaded result here is
        # only acceptable when boot ALSO had no key to install.
        with pool.connection() as conn:
            boot = master_key.bootstrap(conn)
        assert boot.broker_encryption_key is None, (
            "_bootstrap_master_key did not install the broker encryption key "
            "despite bootstrap having one available — the jobs entrypoint "
            "fix from #JOBS-MK-BOOTSTRAP regressed."
        )
    finally:
        pool.close()
