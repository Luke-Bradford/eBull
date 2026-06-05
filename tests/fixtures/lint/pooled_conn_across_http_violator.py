"""CAVEMAN: synthetic violator fixture for the pooled-conn-across-HTTP lint.

This file deliberately defines a route that takes ``conn = Depends(get_conn)``
(so the pooled connection is held for the whole body) AND references an
external-provider HTTP client (``SecFilingsProvider``) in that body. The
guard ``scripts/check_pooled_conn_across_http.py`` must flag it (exit 1 +
violation line printed) when pointed at this fixture path explicitly.

NOTE: this fixture lives OUTSIDE the default ``app/api/*.py`` glob, so it
can only ever be discovered by an explicit-path scan — it will NEVER trip
the production pre-push gate by accident. The names below are typed
``Any`` stubs (the file is never imported — the guard parses it as AST
only), so it stays lint- and type-clean.
"""

from __future__ import annotations

from typing import Any

Depends: Any = None
get_conn: Any = None
SecFilingsProvider: Any = None


def violating_route(conn: Any = Depends(get_conn)) -> None:
    """Trip the guard: pooled conn held across an external SEC fetch."""
    conn.execute("SELECT 1")
    provider = SecFilingsProvider()
    provider.fetch_document_text("https://example.test")


def kw_violating_route(conn: Any = Depends(dependency=get_conn)) -> None:
    """Keyword-form Depends(dependency=get_conn) + external call — flagged."""
    conn.execute("SELECT 1")
    _ = SecFilingsProvider()


def clean_route(conn: Any = Depends(get_conn)) -> None:
    """DB-only — pooled conn, no external I/O. Must NOT be flagged."""
    conn.execute("SELECT 1")
