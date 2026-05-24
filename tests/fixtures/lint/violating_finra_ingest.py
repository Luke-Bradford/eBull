"""CAVEMAN: synthetic violator fixture for caller-owned-tx lint test.

This file deliberately contains a forbidden ``with conn.transaction():``
block at module/function scope. The lint guard
``scripts/check_caller_owned_tx.py`` must flag it (exit 1 + violation
line printed) when pointed at this fixture path explicitly.

NOTE: this fixture lives outside the default ``app/services/finra_*_ingest.py``
glob, so it can only ever be discovered by an explicit-path scan. It
will NEVER trip the production pre-push gate by accident.
"""

from __future__ import annotations


def x(conn) -> None:
    """Trip the lint guard."""
    with conn.transaction():
        pass
