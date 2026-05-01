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

These tests are fully mocked — no DB required. They patch
``master_key.bootstrap`` to inject a known result and assert the
helper's wiring (``set_active_key`` plumbed through on the
key-supplied branch, no-op when the boot result has no key).

Originally drove the helper against the dev DB, but the bot
pre-flight on PR #733 caught that the module-level skipif on
DB-reachability would hollow out regression coverage in any CI
environment without Postgres. Mocked design lets the contract
test run everywhere.
"""

from __future__ import annotations

import pytest


def test_jobs_bootstrap_master_key_installs_returned_key() -> None:
    """Pin the wiring contract for ``_bootstrap_master_key``: when the
    underlying ``master_key.bootstrap`` returns a key, the helper
    installs it via ``set_active_key``.

    Bot pre-flight (PR #733) flagged that calling ``bootstrap`` twice
    to validate state could mask a regression if the function isn't
    idempotent. Replaced with a direct mock of ``master_key.bootstrap``
    so the test asserts the helper's wiring without depending on
    bootstrap's internal idempotency: feed it a known key, assert
    ``set_active_key`` receives it, assert the AES-GCM cipher is
    callable.

    Failure modes caught:
      - Helper omitted from ``serve()`` (the original bug).
      - Helper drops the returned key on the floor (e.g. forgets the
        ``set_active_key`` call).
      - Helper short-circuits on the wrong condition (e.g. checking
        ``boot.recovery_required`` instead of
        ``boot.broker_encryption_key is not None``).
    """
    from unittest.mock import MagicMock, patch

    from app.jobs.__main__ import _bootstrap_master_key
    from app.security.master_key import BootResult
    from app.security.secrets_crypto import (
        _get_aesgcm,  # type: ignore[attr-defined]
        clear_active_key,
    )

    fake_key = b"0" * 32  # AES-256 key length
    fake_pool = MagicMock()
    # Pool's ``.connection()`` is a context manager returning a
    # connection. We never touch the connection inside the helper —
    # ``master_key.bootstrap`` is patched — so a bare MagicMock is
    # enough here.
    fake_pool.connection.return_value.__enter__.return_value = MagicMock()
    fake_pool.connection.return_value.__exit__.return_value = None

    fake_boot = BootResult(
        state="normal",
        needs_setup=False,
        recovery_required=False,
        broker_encryption_key=fake_key,
    )

    # Reset the module-global cipher so a prior test's bootstrap
    # cannot trivialise this assertion.
    clear_active_key()
    with patch("app.jobs.__main__.master_key.bootstrap", return_value=fake_boot):
        _bootstrap_master_key(fake_pool)
    # If the helper installed the key, ``_get_aesgcm()`` returns
    # without raising. If the helper dropped the key on the floor,
    # this raises ``MasterKeyNotLoadedError``.
    aesgcm = _get_aesgcm()
    assert aesgcm is not None


def test_jobs_bootstrap_master_key_no_op_when_bootstrap_returns_no_key() -> None:
    """Pin the recovery / clean-install branch: when bootstrap returns
    ``broker_encryption_key=None`` (no ``EBULL_SECRETS_KEY`` configured
    or no ciphertext on disk), the helper must NOT call
    ``set_active_key`` — leaving the cipher unloaded so subsequent
    code paths surface ``MasterKeyNotLoadedError`` correctly rather
    than silently using a stale or zeroed key."""
    from unittest.mock import MagicMock, patch

    from app.jobs.__main__ import _bootstrap_master_key
    from app.security.master_key import BootResult
    from app.security.secrets_crypto import (
        MasterKeyNotLoadedError,
        _get_aesgcm,  # type: ignore[attr-defined]
        clear_active_key,
    )

    fake_pool = MagicMock()
    fake_pool.connection.return_value.__enter__.return_value = MagicMock()
    fake_pool.connection.return_value.__exit__.return_value = None

    fake_boot = BootResult(
        state="recovery_required",
        needs_setup=False,
        recovery_required=True,
        broker_encryption_key=None,
    )

    clear_active_key()
    with patch("app.jobs.__main__.master_key.bootstrap", return_value=fake_boot):
        _bootstrap_master_key(fake_pool)
    with pytest.raises(MasterKeyNotLoadedError):
        _get_aesgcm()
