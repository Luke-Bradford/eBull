"""Fresh-process import-order regression guard (#2110).

Pre-#2110 the insider modules imported
``app.services.manifest_parsers._classify``, and importing any
submodule of a package executes the package ``__init__`` first — so a
fresh process whose FIRST import was ``insider_transactions`` (or
``insider_form3_ingest``) fired the full parser-registry init, which
imports ``insider_345``, which imports back into the still-partially-
initialised insider module: ``ImportError: cannot import name ...
from partially initialized module``. One-shot CLIs (scripts/rewash.py,
scripts/backfill_828_mislink_repair.py — 14/758 first-run failures on
2026-07-22) needed load-bearing side-effect pre-imports to survive.

#2110 moved the shared leaf to ``app.services.upsert_classify`` (a
plain module, no package side effects), killing the cycle CLASS. This
guard imports each formerly-cycling module as the FIRST import of a
fresh interpreter — the exact entry order that used to crash — so a
future re-introduction of a package-init edge into the insider module
graph fails loudly here instead of in a one-shot script at 2am.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

_ENTRY_MODULES = [
    "app.services.insider_transactions",
    "app.services.insider_form3_ingest",
    "app.services.manifest_parsers.insider_345",
    "app.services.rewash_filings",
    "app.services.instrument_analytics",
]


@pytest.mark.parametrize("module", _ENTRY_MODULES)
def test_fresh_process_first_import_succeeds(module: str) -> None:
    """Each module must import cleanly as a fresh interpreter's first
    import — no side-effect pre-import required."""
    result = subprocess.run(  # noqa: S603 — fixed argv, no user input
        [sys.executable, "-c", f"import {module}"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"fresh-process `import {module}` failed — the #2110 import cycle "
        f"(or a new package-init edge) is back:\n{result.stderr}"
    )
