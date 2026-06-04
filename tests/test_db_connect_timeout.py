"""PR0 (#1472): libpq connect-timeout single-source guard.

`app.config` sets ``PGCONNECT_TIMEOUT`` at import so EVERY psycopg
connection — raw or pooled, in any process — has a bounded connect phase.
Without it a connect that stalls mid-SCRAM-auth under a connection herd
hangs forever, wedging the scheduled-fire wrapper before
``record_job_start`` → APScheduler ``max_instances=1`` then silently
suppresses the job (the 2026-06-04 discovery-layer freeze, #1474).

The import side-effect uses ``setdefault`` so an explicit value in the
PROCESS environment (shell / systemd / Docker) wins — ``PGCONNECT_TIMEOUT``
is a libpq env var, not a Settings/.env field. Both branches are pinned
here via clean subprocesses (deterministic regardless of the developer's
own shell environment).
"""

from __future__ import annotations

import subprocess
import sys

from app.config import DB_CONNECT_TIMEOUT_S

_PRINT_ENV = "import os, app.config; print(os.environ.get('PGCONNECT_TIMEOUT', '<unset>'))"


def test_constant_is_a_valid_libpq_timeout() -> None:
    # libpq treats connect_timeout < 2 as 2; keep the constant in the
    # meaningful range so the bound is real and self-documenting.
    assert isinstance(DB_CONNECT_TIMEOUT_S, int)
    assert DB_CONNECT_TIMEOUT_S >= 2


def _run(env_overrides: dict[str, str]) -> str:
    import os

    env = {**os.environ, **env_overrides}
    out = subprocess.run(
        [sys.executable, "-c", _PRINT_ENV],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    return out.stdout.strip()


def test_import_sets_default_when_unset() -> None:
    # Clean env (PGCONNECT_TIMEOUT removed) → import applies our default.
    import os

    env = {k: v for k, v in os.environ.items() if k != "PGCONNECT_TIMEOUT"}
    out = subprocess.run(
        [sys.executable, "-c", _PRINT_ENV],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    assert out.stdout.strip() == str(DB_CONNECT_TIMEOUT_S)


def test_existing_value_is_respected() -> None:
    # An explicit operator override must win over the import default.
    assert _run({"PGCONNECT_TIMEOUT": "37"}) == "37"
