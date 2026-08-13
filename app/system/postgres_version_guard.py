"""Postgres minimum-version guard (#1233 PR12).

PR12 rewrites every ``refresh_*_current`` helper to use PG17's
``MERGE … WHEN NOT MATCHED BY SOURCE`` clause. PG15 and PG16 only
support ``WHEN NOT MATCHED [BY TARGET]`` — a PG <17 deployment would
pass every lint + typecheck and then crash at the first refresh call
with ``syntax error at or near "BY"``.

This module asserts ``server_version_num >= 170000`` at lifespan
startup, mirroring the boot-time fail-closed pattern from
``app.db.pg_settings.enforce_max_locks_floor`` (#1187).

Spec: ``docs/superpowers/specs/2026-05-21-pr12-ownership-current-writer-merge.md`` §7.
"""

from __future__ import annotations

from typing import Any, Final

import psycopg

PG_MERGE_NOT_MATCHED_BY_SOURCE_MIN_VERSION_NUM: Final[int] = 170000
"""PG ``server_version_num`` for the lowest release supporting
``MERGE … WHEN NOT MATCHED BY SOURCE``. PG 17.0 → 170000.
"""


def assert_postgres_min_version(
    conn: psycopg.Connection[Any],
    *,
    min_version_num: int = PG_MERGE_NOT_MATCHED_BY_SOURCE_MIN_VERSION_NUM,
) -> None:
    """Raise ``RuntimeError`` if connected PG server is older than ``min_version_num``.

    ``min_version_num`` follows Postgres ``server_version_num`` encoding
    (major × 10000 + minor × 100 + patch). 170000 = PG 17.0.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT current_setting('server_version_num')::int")
        row = cur.fetchone()
    if row is None:
        raise RuntimeError(
            "PG >= 17 required for PR12 MERGE WHEN NOT MATCHED BY SOURCE, "
            "but `SELECT current_setting('server_version_num')` returned no row. "
            f"Configured minimum: {min_version_num}. See #1233."
        )
    actual = int(row[0])
    if actual < min_version_num:
        raise RuntimeError(
            f"Postgres server_version_num={actual} detected — PG >= 17 required "
            f"for PR12 MERGE WHEN NOT MATCHED BY SOURCE clause. Configured minimum: "
            f"{min_version_num}. See #1233 spec §7."
        )
