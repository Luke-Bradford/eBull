"""#1406 — Settings.secrets_key must honour the DOCUMENTED env name.

The docs / .env.example / master_key+secrets_crypto docstrings all name the
AES env-key variable ``EBULL_SECRETS_KEY``, but ``Settings`` has no
``env_prefix``. Without an alias the field would read only the bare
``SECRETS_KEY`` and silently drop a documented ``EBULL_SECRETS_KEY`` (verified
in #1406). The ``AliasChoices`` fix honours the documented name while keeping
the legacy bare name working.

``_env_file=None`` disables the repo ``.env`` so its (possibly-present, empty)
``EBULL_SECRETS_KEY=`` cannot mask the env var under test.
"""

from __future__ import annotations

from app.config import Settings


def test_secrets_key_honours_documented_ebull_name(monkeypatch) -> None:
    monkeypatch.delenv("SECRETS_KEY", raising=False)
    monkeypatch.setenv("EBULL_SECRETS_KEY", "via-documented-name")
    assert Settings(_env_file=None).secrets_key == "via-documented-name"


def test_secrets_key_legacy_bare_name_still_works(monkeypatch) -> None:
    monkeypatch.delenv("EBULL_SECRETS_KEY", raising=False)
    monkeypatch.setenv("SECRETS_KEY", "via-legacy-name")
    assert Settings(_env_file=None).secrets_key == "via-legacy-name"


def test_secrets_key_defaults_none_when_unset(monkeypatch) -> None:
    monkeypatch.delenv("EBULL_SECRETS_KEY", raising=False)
    monkeypatch.delenv("SECRETS_KEY", raising=False)
    assert Settings(_env_file=None).secrets_key is None
