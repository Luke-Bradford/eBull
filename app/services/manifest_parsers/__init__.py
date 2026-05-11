"""Manifest-worker parser registry hub (#873).

The manifest worker (`app/jobs/sec_manifest_worker.py`) dispatches one
parser callable per `ManifestSource`. Pre-#873 nothing registered any
parser at production startup, so every manifest row was debug-skipped
and the worker was a no-op.

This package is the single registration point. Each per-source module
(`eight_k`, `def14a`, `form4`, ...) exposes a `register()` callable.
Importing this package side-effects-populates the worker's global
`_PARSERS` dict by calling every submodule's `register()` once.

Both processes import this package on startup:
- API: `app/main.py` imports `app.services.manifest_parsers` near the
  top so the `/coverage/manifest-parsers` audit endpoint sees the
  same registry the worker has.
- Worker: `app/jobs/__main__.py` imports the same module before
  starting the worker loop.

Module-import-time registration is the architecture invariant. A
parser that registers itself only inside a function body would
break the cross-process registry view — the audit endpoint would
report `has_registered_parser=False` for a source the worker
actually handles, and the operator would mis-diagnose the lane as
stuck.

Test isolation: tests that need a clean registry call
`sec_manifest_worker.clear_registered_parsers()`, then re-register
with `register_all_parsers()`. ``importlib.reload(__name__)`` is not
sufficient because the per-source submodules are cached and their
module-body side effects don't re-fire.
"""

from __future__ import annotations

from app.services.manifest_parsers import def14a as _def14a
from app.services.manifest_parsers import eight_k as _eight_k


def register_all_parsers() -> None:
    """Idempotent: registers every per-source parser with the
    manifest worker. Called once at package import (below) and
    callable again by tests after ``clear_registered_parsers()``.
    """
    _eight_k.register()
    _def14a.register()


# Run once at package import.
register_all_parsers()


__all__ = ("register_all_parsers",)
