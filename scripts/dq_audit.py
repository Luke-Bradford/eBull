"""Data-quality audit scanner — the board-feeder (autonomy substrate #3).

Runs cheap full-population anomaly scans against the dev DB and prints a
report. Each finding is a candidate ticket; the operator/agent triages + files
the real ones (after confirming the signature, per the source-rule discipline —
this surfaces candidates, it does not assert bugs).

First check class: **control-group double-count** in the ownership ``*_current``
rollups — within one instrument, a single ``source_accession`` emitting ≥2 rows
with the SAME non-zero ``shares`` but different holders is one beneficial
position reported by a control chain and counted N times. (Found #1764 this way.)

IMPORTANT — audit the OPERATOR-VISIBLE layer, not just the raw table. The
``/ownership-rollup`` endpoint already collapses LARGE same-value control groups
via ``ownership_rollup._reconcile_insider_control_groups`` (#1652), but only
ABOVE a magnitude floor (``_INSIDER_GROUP_MIN_SHARES`` = 1,000,000). So a raw
``*_current`` scan over-reports massively (≥1M groups are fixed downstream); the
genuine live bug is the SUB-floor tail. We split the count on that floor and flag
only the sub-floor subset as the operator-visible candidate. (Lesson: a feeder
that scans the wrong layer cries wolf — #1764 first looked like 74.5B shares;
the real operator-visible figure is ~387M.)

Run::

    uv run python scripts/dq_audit.py
"""

from __future__ import annotations

import sys
from typing import LiteralString, cast

import psycopg
import psycopg.rows

from app.config import settings

# (table, holder-key column, share column) — the three ownership rollups the
# operator sees. The share column differs: blockholders store the amount as
# ``aggregate_amount_owned``, insiders/institutions as ``shares``.
_OWNERSHIP_CURRENT = [
    ("ownership_insiders_current", "holder_identity_key", "shares"),
    ("ownership_blockholders_current", "reporter_cik", "aggregate_amount_owned"),
    ("ownership_institutions_current", "filer_cik", "shares"),
]


# The #1652 insider control-group collapse only fires at/above this magnitude
# (ownership_rollup._INSIDER_GROUP_MIN_SHARES). Groups below it survive to the
# operator-visible rollup — that sub-floor subset is the genuine candidate.
_COLLAPSE_FLOOR = 1_000_000


def _control_group_dup(
    conn: psycopg.Connection[object], table: str, holder_col: str, share_col: str
) -> dict[str, object]:
    """Count (instrument, accession, shares) groups with >1 distinct holder and
    shares>0 — same position double-counted by a control chain — split on the
    #1652 collapse floor so the operator-visible (sub-floor) subset is distinct."""
    # table/holder_col/share_col are from the static module list, never user input.
    sql = f"""
        WITH dup AS (
            SELECT instrument_id, source_accession, {share_col} AS shares,
                   count(DISTINCT {holder_col}) AS nholders
            FROM {table}
            WHERE {share_col} > 0 AND source_accession IS NOT NULL
            GROUP BY 1, 2, 3
            HAVING count(DISTINCT {holder_col}) > 1
        )
        SELECT
            count(*) FILTER (WHERE shares < %(floor)s) AS live_groups,
            count(DISTINCT instrument_id) FILTER (WHERE shares < %(floor)s) AS live_instruments,
            COALESCE(SUM((nholders - 1) * shares) FILTER (WHERE shares < %(floor)s), 0) AS live_overcount,
            count(*) AS total_groups,
            COALESCE(SUM((nholders - 1) * shares), 0) AS total_overcount
        FROM dup
    """  # noqa: S608 — identifiers are static module constants
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(cast(LiteralString, sql), {"floor": _COLLAPSE_FLOOR})
        row = cur.fetchone() or {}
    return {
        "check": "control_group_dup",
        "table": table,
        "live_groups": int(row.get("live_groups", 0)),
        "live_instruments": int(row.get("live_instruments", 0)),
        "live_overcount": float(row.get("live_overcount", 0)),
        "total_groups": int(row.get("total_groups", 0)),
        "total_overcount": float(row.get("total_overcount", 0)),
    }


def main() -> int:
    findings: list[dict[str, object]] = []
    # autocommit so one check's error (e.g. a missing column on a schema change)
    # doesn't poison the transaction for the remaining checks.
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        for table, holder_col, share_col in _OWNERSHIP_CURRENT:
            try:
                findings.append(_control_group_dup(conn, table, holder_col, share_col))
            except psycopg.Error as exc:
                findings.append({"check": "control_group_dup", "table": table, "error": str(exc)})

    print("=== DQ audit — control-group double-count in ownership rollups ===")
    print("(live = operator-visible, below the #1652 collapse floor; total = raw table incl. downstream-collapsed)")
    for f in findings:
        if "error" in f:
            print(f"  {f['table']}: ERROR {f['error']}")
            continue
        live = int(f["live_groups"])  # type: ignore[call-overload]
        flag = "  ⚠ CANDIDATE" if live > 0 else "  ok"
        print(
            f"{flag}  {f['table']}: LIVE {live} groups / "
            f"{f['live_instruments']} instruments / {f['live_overcount']:,.0f} shares "
            f"(total incl. collapsed: {f['total_groups']} groups / {f['total_overcount']:,.0f})"
        )
    print(
        "\nNon-zero LIVE groups = a candidate ticket. Confirm the signature on the "
        "full population + cite the source rule before filing (see #1764)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
