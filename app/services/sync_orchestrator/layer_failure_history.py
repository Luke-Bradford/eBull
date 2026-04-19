"""Failure-history helpers for `/sync/layers`.

Also exposes `all_layer_histories(conn)` for the `/sync/layers`
endpoint so the per-layer loop can share a single pair of queries
instead of issuing two round-trips per layer (30 total for 15 layers).


The endpoint's response model declares `consecutive_failures` and
`last_error_category`, but the legacy implementation hardcoded
`consecutive_failures=0` and only populated `last_error_category` from
the in-request freshness-predicate exception path. That meant a layer
that has failed 3 consecutive times in `sync_layer_progress` was
indistinguishable on the wire from a healthy layer.

These helpers compute both values from `sync_layer_progress` so the
Problems panel on the AdminPage can trust them.

Contract:
- `consecutive_failures(conn, layer_name)`:
  number of most-recent `status='failed'` rows for the layer, ordered
  by `started_at DESC`, stopping at the first non-failed row. Returns
  0 when the layer has no history or when the latest run was not a
  failure. Pending / running rows count as non-failed (they reset the
  streak conservatively — we do not call a layer "still failing" while
  it is in the middle of a fresh attempt).
- `last_error_category(conn, layer_name)`:
  the `error_category` value from the most recent `sync_layer_progress`
  row for the layer where that column is non-null, regardless of
  status. Returns None when the layer has never recorded an
  error_category. This survives a later partial/skipped run that did
  not overwrite the column, which is the desired "last known error"
  semantics for an operator triage panel.
"""

from __future__ import annotations

from typing import Any

import psycopg
import psycopg.rows


def consecutive_failures(
    conn: psycopg.Connection[Any],
    layer_name: str,
) -> int:
    """Count how many of the most-recent progress rows were 'failed'.

    Stops counting at the first non-failed row (or zero rows). A
    pending / running / complete / skipped / partial row breaks the
    streak. The count includes the most recent row if and only if it
    was a failure.
    """
    # Postgres sorts nulls first on DESC. `started_at` is nullable —
    # pending rows are inserted without it (executor.py:154), and
    # skipped-by-reaper rows only set finished_at (executor.py:448).
    # Without NULLS LAST, an old row with null started_at can sit
    # ahead of fresh failures and falsely zero the streak.
    # `sync_run_id DESC` is a stable tiebreak when two rows share a
    # `started_at` timestamp.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT status
            FROM sync_layer_progress
            WHERE layer_name = %s
            ORDER BY started_at DESC NULLS LAST, sync_run_id DESC
            LIMIT 50
            """,
            (layer_name,),
        )
        rows = cur.fetchall()
    streak = 0
    for row in rows:
        if row["status"] == "failed":
            streak += 1
        else:
            break
    return streak


def last_error_category(
    conn: psycopg.Connection[Any],
    layer_name: str,
) -> str | None:
    """Return the most recent non-null error_category for the layer.

    Orders by `started_at DESC` and returns the first non-null match,
    or None when no row has ever recorded a category. Includes
    successful rows — if a layer partially succeeded but recorded a
    category, we want to surface that. In practice the executor only
    writes `error_category` on non-success paths, so this is a
    defensive allowance rather than an observed case.
    """
    # NULLS LAST + sync_run_id tiebreak for the same reason as
    # `consecutive_failures` — see that function for the rationale.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT error_category
            FROM sync_layer_progress
            WHERE layer_name = %s AND error_category IS NOT NULL
            ORDER BY started_at DESC NULLS LAST, sync_run_id DESC
            LIMIT 1
            """,
            (layer_name,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    category = row["error_category"]
    return str(category) if category is not None else None


def all_layer_histories(
    conn: psycopg.Connection[Any],
    layer_names: list[str] | tuple[str, ...],
) -> tuple[dict[str, int], dict[str, str | None]]:
    """Batched equivalent of (consecutive_failures, last_error_category).

    Returns a pair of dicts keyed by layer_name:
      - consecutive failure streaks (layer -> int, 0 if no failures at head)
      - last non-null error category (layer -> str | None)

    Each map is computed with a single query using window functions,
    replacing the 15 * 2 = 30 per-request round-trips that the
    per-layer helpers would issue when called in a loop.

    Both queries filter to the supplied ``layer_names`` list so
    `sync_layer_progress` is indexed-scanned by layer name rather
    than full-scanned — bounded cost as the table grows.
    """
    if not layer_names:
        return {}, {}
    names = list(layer_names)
    # consecutive_failures: for each layer, count the head-streak of
    # status='failed' rows when ordered by (started_at DESC NULLS LAST,
    # sync_run_id DESC). We compute a row_number per layer in that
    # ordering and count how many of the first N rows are 'failed'
    # before hitting a non-failed one.
    #
    # SQL pattern: find the row_number of the FIRST non-failed row
    # per layer; the streak length is (that row_number - 1), or the
    # total row count for that layer if every row is 'failed'.
    streaks: dict[str, int] = {}
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # Single index scan: `ranked` adds both the row_number and the
        # per-layer total via window functions. `first_non_failed`
        # pulls the `rn` of the first non-failed row (streak = rn - 1),
        # and `totals` is derived from the same CTE so Postgres does
        # not re-scan the table. The LEFT JOIN falls back to `total`
        # when a layer has no non-failed rows at all (pure-failure
        # history).
        cur.execute(
            """
            WITH ranked AS (
                SELECT
                    layer_name,
                    status,
                    ROW_NUMBER() OVER (
                        PARTITION BY layer_name
                        ORDER BY started_at DESC NULLS LAST, sync_run_id DESC
                    ) AS rn,
                    COUNT(*) OVER (PARTITION BY layer_name) AS total
                FROM sync_layer_progress
                WHERE layer_name = ANY(%s)
            ),
            first_non_failed AS (
                SELECT layer_name, MIN(rn) AS rn
                FROM ranked
                WHERE status <> 'failed'
                GROUP BY layer_name
            ),
            totals AS (
                SELECT DISTINCT layer_name, total
                FROM ranked
            )
            SELECT
                t.layer_name,
                COALESCE(f.rn - 1, t.total) AS streak
            FROM totals t
            LEFT JOIN first_non_failed f USING (layer_name);
            """,
            (names,),
        )
        for row in cur.fetchall():
            streaks[str(row["layer_name"])] = int(row["streak"])

    categories: dict[str, str | None] = {}
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            WITH ranked AS (
                SELECT
                    layer_name,
                    error_category,
                    ROW_NUMBER() OVER (
                        PARTITION BY layer_name
                        ORDER BY started_at DESC NULLS LAST, sync_run_id DESC
                    ) AS rn
                FROM sync_layer_progress
                WHERE error_category IS NOT NULL AND layer_name = ANY(%s)
            )
            SELECT layer_name, error_category
            FROM ranked
            WHERE rn = 1;
            """,
            (names,),
        )
        for row in cur.fetchall():
            # WHERE error_category IS NOT NULL in the CTE already
            # guarantees we never see a NULL here — the cast is just
            # for mypy/pyright's benefit on `object` columns.
            categories[str(row["layer_name"])] = str(row["error_category"])

    return streaks, categories
