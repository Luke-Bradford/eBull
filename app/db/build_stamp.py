"""Stamp every order and decision row with the code that wrote it (#3614 item 2).

``orders`` and ``decision_audit`` carry ``code_commit``, ``code_dirty`` and
``uv_lock_sha256`` columns whose DEFAULT reads a session setting
(``current_setting('ebull.code_commit', true)``, sql/474). This module puts
those settings on every connection the process opens, by writing them into
``PGOPTIONS``: libpq reads that variable on every connect, so the raw
``psycopg.connect`` calls (well over a hundred in ``app/``) and the pools are
all covered without touching a single insert site, and a future writer is
covered by the same default. ⚠ A connect that passes its own ``options=``
replaces ``PGOPTIONS`` and must merge it with ``with_inherited_pgoptions``;
``connect_job`` does.

The identity is read ONCE, at process start, on purpose. The rows should name
the code that is LOADED, and a running process does not pick up a later
checkout: the API reloads on ``app/**`` changes and the jobs child is respawned
by ``dev_reload``, and either way the new process stamps again. A HEAD read at
insert time would instead name whatever was checked out at that moment.

Nothing here raises. When git or ``uv.lock`` cannot be read the setting is
left out, the column default resolves to NULL, and NULL means "not recorded" —
an unknown version is never written as a known one.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re

from app.system.git_identity import REPO_ROOT, _git

logger = logging.getLogger(__name__)

#: The session settings sql/474's column defaults read. Single source of truth
#: for the names; the migration spells the same three.
CODE_COMMIT_SETTING = "ebull.code_commit"
CODE_DIRTY_SETTING = "ebull.code_dirty"
UV_LOCK_SHA256_SETTING = "ebull.uv_lock_sha256"

_HEX = re.compile(r"[0-9a-f]{40,64}")


def _tracked_dirty() -> bool | None:
    """Whether a TRACKED file differs from HEAD, or None if git could not say.

    Untracked files are excluded, matching ``unmerged_code_reason``: a stray
    scratch file is not part of the loaded code.
    """
    out = _git("status", "--porcelain", "--untracked-files=no")
    return None if out is None else bool(out)


def _uv_lock_sha256() -> str | None:
    try:
        return hashlib.sha256((REPO_ROOT / "uv.lock").read_bytes()).hexdigest()
    except OSError:
        return None


def build_stamp_settings() -> dict[str, str]:
    """The settings to put on every connection; a value that cannot be read is omitted."""
    stamp: dict[str, str] = {}
    commit = _git("rev-parse", "HEAD")
    # Only hex reaches PGOPTIONS, whose values are space-separated and unquoted.
    if commit and _HEX.fullmatch(commit):
        stamp[CODE_COMMIT_SETTING] = commit
    dirty = _tracked_dirty()
    if dirty is not None:
        stamp[CODE_DIRTY_SETTING] = "true" if dirty else "false"
    lock_sha = _uv_lock_sha256()
    if lock_sha is not None:
        stamp[UV_LOCK_SHA256_SETTING] = lock_sha
    return stamp


def with_build_stamp(pgoptions: str, stamp: dict[str, str]) -> str:
    """``pgoptions`` with any earlier stamp removed and ``stamp`` appended.

    A respawned child inherits its parent's environment, so a stamp already
    present is replaced rather than appended to.
    """
    tokens = pgoptions.split()
    kept: list[str] = []
    i = 0
    while i < len(tokens):
        if tokens[i] == "-c" and i + 1 < len(tokens) and tokens[i + 1].startswith("ebull."):
            i += 2
            continue
        kept.append(tokens[i])
        i += 1
    for name, value in stamp.items():
        kept += ["-c", f"{name}={value}"]
    return " ".join(kept)


def with_inherited_pgoptions(options: str) -> str:
    """An explicit libpq ``options`` value with this process's ``PGOPTIONS`` kept in front of it.

    libpq uses ``PGOPTIONS`` only when ``options`` is not given, so a caller that
    passes its own (``connect_job``'s ``statement_timeout``) would otherwise
    connect unstamped. The explicit value comes last and wins on a repeated name.
    """
    return f"{os.environ.get('PGOPTIONS', '')} {options}".strip()


def install_build_stamp() -> None:
    """Put this checkout's build identity on every Postgres connection the process opens next.

    Call at process start, before the first connection.
    """
    stamp = build_stamp_settings()
    os.environ["PGOPTIONS"] = with_build_stamp(os.environ.get("PGOPTIONS", ""), stamp)
    logger.info("build stamp for order/decision rows: %s", stamp or "unavailable")
