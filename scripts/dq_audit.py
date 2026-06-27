"""Data-quality audit scanner — the board-feeder (autonomy substrate #3).

Runs cheap full-population anomaly scans against the dev DB and prints a
report. Each finding is a candidate ticket; the operator/agent triages + files
the real ones (after confirming the signature, per the source-rule discipline —
this surfaces candidates, it does not assert bugs).

First check class: **control-group double-count** in the ownership ``*_current``
rollups — within one instrument, a single ``source_accession`` emitting ≥2 rows
with the SAME non-zero ``shares`` but different holders is one beneficial
position reported by a control chain (e.g. Buffett indirect == Berkshire
direct) and counted N times. (Found #1764 this way.)

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


def _control_group_dup(
    conn: psycopg.Connection[object], table: str, holder_col: str, share_col: str
) -> dict[str, object]:
    """Count (instrument, accession, shares) groups with >1 distinct holder and
    shares>0 — the same position double-counted by a control chain."""
    # table/holder_col are from the static module list, never user input.
    sql = f"""
        WITH dup AS (
            SELECT instrument_id, source_accession, {share_col} AS shares,
                   count(DISTINCT {holder_col}) AS nholders
            FROM {table}
            WHERE {share_col} > 0 AND source_accession IS NOT NULL
            GROUP BY 1, 2, 3
            HAVING count(DISTINCT {holder_col}) > 1
        )
        SELECT count(*) AS dup_groups,
               count(DISTINCT instrument_id) AS instruments,
               COALESCE(SUM((nholders - 1) * shares), 0) AS overcounted_shares
        FROM dup
    """  # noqa: S608 — identifiers are static module constants
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # sql is built only from static module constants (table/holder/share
        # identifiers) — no user input; cast to satisfy psycopg's LiteralString.
        cur.execute(cast(LiteralString, sql))
        row = cur.fetchone() or {}
    return {
        "check": "control_group_dup",
        "table": table,
        "dup_groups": int(row.get("dup_groups", 0)),
        "instruments": int(row.get("instruments", 0)),
        "overcounted_shares": float(row.get("overcounted_shares", 0)),
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
    for f in findings:
        if "error" in f:
            print(f"  {f['table']}: ERROR {f['error']}")
            continue
        flag = "  ⚠ CANDIDATE" if int(f["dup_groups"]) > 0 else "  ok"  # type: ignore[call-overload]
        print(
            f"{flag}  {f['table']}: {f['dup_groups']} dup groups over "
            f"{f['instruments']} instruments, {f['overcounted_shares']:,.0f} shares overcounted"
        )
    print(
        "\nNon-zero dup_groups = a candidate ticket. Confirm the signature on the "
        "full population + cite the source rule before filing (see #1764)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
