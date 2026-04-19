"""Per-layer enable/disable flag (spec §3.2 rule 1).

Default: enabled. Absent row counts as enabled so adding a new layer
to the registry never surprises an operator with a disabled-by-default
row.
"""

from __future__ import annotations

from typing import Any

import psycopg


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
) -> None:
    conn.execute(
        """
        INSERT INTO layer_enabled (layer_name, is_enabled, updated_at)
        VALUES (%s, %s, now())
        ON CONFLICT (layer_name) DO UPDATE
          SET is_enabled = EXCLUDED.is_enabled,
              updated_at = now()
        """,
        (layer_name, enabled),
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
