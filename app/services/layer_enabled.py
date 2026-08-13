"""Per-layer enable/disable flag (spec §3.2 rule 1).

Default: enabled. Absent row counts as enabled so adding a new layer
to the registry never surprises an operator with a disabled-by-default
row.

Audit (#346): every toggle writes both the latest-state row on
``layer_enabled`` (reason + changed_by denormalised for hot-path reads)
and an append-only ``layer_enabled_audit`` row for the full history.
Safety-critical disables (``fx_rates`` / ``portfolio_sync``) require a
reason at the API boundary; this module trusts callers to supply one
when policy demands it.
"""

from __future__ import annotations

from typing import Any

import psycopg

# Layers whose disable carries operational risk (broker drift, P&L
# drift). The API boundary refuses ``enabled=False`` without a
# ``reason`` for these names; documented here so the policy lives next
# to the data definition rather than buried in HTTP-handler code.
SAFETY_CRITICAL_LAYERS: frozenset[str] = frozenset({"fx_rates", "portfolio_sync"})


def is_layer_enabled(conn: psycopg.Connection[Any], layer_name: str) -> bool:
    row = conn.execute(
        "SELECT is_enabled FROM layer_enabled WHERE layer_name = %s",
        (layer_name,),
    ).fetchone()
    if row is None:
        return True
    return bool(row[0])


def set_layer_enabled(
    conn: psycopg.Connection[Any],
    layer_name: str,
    *,
    enabled: bool,
    reason: str | None = None,
    changed_by: str | None = None,
) -> None:
    """Toggle a layer and write a full-history audit row (#346).

    Both writes go on the caller-supplied connection; commit is the
    caller's responsibility (matches the rest of this module's
    contract — see e.g. ``layer_enabled.set_layer_enabled`` in
    ``app/api/sync.py`` which commits after the call). The audit row
    and the latest-state row therefore land atomically: a crash mid-
    function rolls back both.
    """
    conn.execute(
        """
        INSERT INTO layer_enabled (layer_name, is_enabled, reason, changed_by, updated_at)
        VALUES (%s, %s, %s, %s, now())
        ON CONFLICT (layer_name) DO UPDATE
          SET is_enabled = EXCLUDED.is_enabled,
              reason     = EXCLUDED.reason,
              changed_by = EXCLUDED.changed_by,
              updated_at = now()
        """,
        (layer_name, enabled, reason, changed_by),
    )
    conn.execute(
        """
        INSERT INTO layer_enabled_audit (layer_name, is_enabled, reason, changed_by)
        VALUES (%s, %s, %s, %s)
        """,
        (layer_name, enabled, reason, changed_by),
    )


def read_all_enabled(conn: psycopg.Connection[Any], names: list[str]) -> dict[str, bool]:
    """Batched read for the state machine — one query for every layer."""
    if not names:
        return {}
    rows = conn.execute(
        "SELECT layer_name, is_enabled FROM layer_enabled WHERE layer_name = ANY(%s)",
        (names,),
    ).fetchall()
    out = {str(r[0]): bool(r[1]) for r in rows}
    for name in names:
        out.setdefault(name, True)
    return out
