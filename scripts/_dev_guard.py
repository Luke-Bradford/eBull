"""Shared dev-environment guard for dev-only scripts (#1765 review).

Any script that opens ``psycopg.connect(settings.database_url)`` for dev tooling
must call ``assert_dev_environment()`` first, so a stray ``DATABASE_URL``
pointing at prod (CI override, env misfire) can never touch a live database.
"""

from __future__ import annotations

from urllib.parse import urlparse

from app.config import settings

_DEV_APP_ENVS = ("dev", "development", "local", "test")
_LOCAL_HOSTS = ("localhost", "127.0.0.1", "::1")


def assert_dev_environment() -> None:
    """Refuse to run unless ``app_env`` is a dev value AND the DB host is local.

    Two independent guards — an exact hostname match (a substring check like
    ``"@localhost" in url`` is bypassed by ``@localhost.evil.com``)."""
    if settings.app_env not in _DEV_APP_ENVS:
        raise SystemExit(f"refusing to run: app_env={settings.app_env!r} is not a dev environment")
    host = urlparse(settings.database_url).hostname
    if host not in _LOCAL_HOSTS:
        raise SystemExit(f"refusing to run: database_url host {host!r} is not localhost (dev-only tool)")
