"""Pre-connection DB-name allowlist tests for ``assert_dev_db_name_in_url``.

Pins the 5 paths from the Run-#8-readiness Item 3 spec acceptance test:

* URL DB name matches ``EBULL_DEV_DB_NAMES`` env (pass).
* URL DB name not in list (fail).
* ``EBULL_DEV_DB_NAMES`` unset + URL DB = ``ebull_dev`` (pass via default).
* ``EBULL_DEV_DB_NAMES`` unset + URL DB = ``ebull`` (fail — matches
  default-list behaviour).
* Malformed URL (raise).

Why: ``assert_dev_db(conn)`` already checks ``current_database()``
post-connection. Operator failure mode: a mis-set ``DATABASE_URL`` (e.g.
pointing at the default ``ebull`` DB on the local dev box) would deep-
stack ``OperationalError`` only after connect, leaving the operator
guessing. This pre-connection check fails fast with an actionable
error naming the env var.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from app.runbooks.safety import RunbookRefused, assert_dev_db_name_in_url


@pytest.fixture(autouse=True)
def _reset_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Ensure EBULL_DEV_DB_NAMES doesn't leak across tests."""
    monkeypatch.delenv("EBULL_DEV_DB_NAMES", raising=False)
    yield


def _patch_settings_url(monkeypatch: pytest.MonkeyPatch, url: str) -> None:
    """Patch the live settings.database_url for the duration of one test.

    safety.assert_dev_db_name_in_url reads `settings.database_url`
    directly so we set the attribute (pydantic-settings exposes the
    field mutable in-test).
    """
    import app.runbooks.safety as safety_module

    monkeypatch.setattr(safety_module.settings, "database_url", url)


def test_url_db_name_matches_env_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    """EBULL_DEV_DB_NAMES set, DB matches → pass."""
    monkeypatch.setenv("EBULL_DEV_DB_NAMES", "ebull_dev,ebull_dev_alt")
    _patch_settings_url(monkeypatch, "postgresql://u:p@localhost:5432/ebull_dev_alt")
    assert_dev_db_name_in_url()


def test_url_db_name_not_in_explicit_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    """EBULL_DEV_DB_NAMES set, DB doesn't match → RunbookRefused."""
    monkeypatch.setenv("EBULL_DEV_DB_NAMES", "ebull_dev")
    _patch_settings_url(monkeypatch, "postgresql://u:p@localhost:5432/ebull_prod")
    with pytest.raises(RunbookRefused) as exc_info:
        assert_dev_db_name_in_url()
    assert "ebull_prod" in exc_info.value.msg


def test_default_allowlist_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    """EBULL_DEV_DB_NAMES unset + URL = ebull_dev → pass via default."""
    _patch_settings_url(monkeypatch, "postgresql://u:p@localhost:5432/ebull_dev")
    assert_dev_db_name_in_url()


def test_default_allowlist_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    """EBULL_DEV_DB_NAMES unset + URL = ebull → RunbookRefused.

    This is the Codex 1 diff-re-pass IMPORTANT — the default
    ``DATABASE_URL`` at app/config.py:16 points at ``/ebull`` which
    is NOT a dev DB. Pre-connection check must fail before the
    post-connection check has a chance to.
    """
    _patch_settings_url(monkeypatch, "postgresql://u:p@localhost:5432/ebull")
    with pytest.raises(RunbookRefused) as exc_info:
        assert_dev_db_name_in_url()
    assert "'ebull'" in exc_info.value.msg


def test_malformed_url_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """URL with no path → RunbookRefused with actionable message."""
    _patch_settings_url(monkeypatch, "postgresql://u:p@localhost:5432")
    with pytest.raises(RunbookRefused) as exc_info:
        assert_dev_db_name_in_url()
    assert "no database name" in exc_info.value.msg


def test_psycopg_url_scheme_handled(monkeypatch: pytest.MonkeyPatch) -> None:
    """``postgresql+psycopg://`` scheme should parse like vanilla
    postgresql. Defensive — Codex 1 diff re-pass focus area 3.
    """
    monkeypatch.setenv("EBULL_DEV_DB_NAMES", "ebull_dev")
    _patch_settings_url(monkeypatch, "postgresql+psycopg://u:p@localhost:5432/ebull_dev")
    assert_dev_db_name_in_url()
