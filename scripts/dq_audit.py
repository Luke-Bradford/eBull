"""Data-quality audit scanner — the board-feeder (autonomy substrate #3).

Runs cheap full-population anomaly scans against the dev DB and prints a
report. Each finding is a candidate ticket; the operator/agent triages + files
the real ones (after confirming the signature, per the source-rule discipline —
this surfaces candidates, it does not assert bugs).

First check class: **control-group double-count** in the ownership ``*_current``
rollups — within one instrument, a single ``source_accession`` emitting ≥2 rows
with the SAME non-zero ``shares`` but different holders is one beneficial
position reported by a control chain and counted N times. (Found #1764 this way.)

IMPORTANT — this raw ``*_current`` scan groups by ``source_accession``, which is
exactly the signature the read path collapses with NO floor: ``#1764``
(insiders + blockholders, same-accession control chain) and ``#788`` (insiders,
dual-pipeline same-cik nature collision). So for insiders/blockholders the raw
"LIVE" figure is structurally a FALSE ALARM — every same-accession dup is
collapsed before the operator sees it. (Post-mortem: a 2026-06-28 full-population
investigation found the raw scan's 317 insider "LIVE" groups were 100% collapsed
by ``#1764``; the genuine residual was a DIFFERENT, dual-pipeline signature, fixed
by ``#788``.) A non-zero raw count for those tables is therefore reported as
``read-path-collapsed`` and MUST be confirmed by RENDERING the rollup
(``get_ownership_rollup`` → a residual same-``(accession, shares)`` insider holder
pair that is NOT a genuine ``#905`` direct+indirect lot) before it is filed.
The ``_INSIDER_GROUP_MIN_SHARES`` = 1,000,000 floor is ``#1652``'s (the fuzzy
cross-accession pass); ``#1764``/``#788`` have no floor.

Run::

    uv run python scripts/dq_audit.py
"""

from __future__ import annotations

import sys
from typing import LiteralString, cast

import psycopg
import psycopg.rows

from app.config import settings
from scripts._dev_guard import assert_dev_environment

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


def _dual_pipeline_insider_collision(conn: psycopg.Connection[object]) -> dict[str, object]:
    """Raw-storage sentinel for the #788 dual-pipeline insider collision.

    The same Form 4/3 ``(holder_cik, source_accession)`` written by BOTH the XML
    manifest parser (bare-accession ``source_document_id``) AND the bulk SEC
    insider dataset (``:NDT:`` / ``:NDH:`` marker) coexists under two
    ``ownership_nature`` values because nature is in the MERGE key — one stake
    stored 2×. The read-path rollup now de-collides this (#788,
    ``_collect_canonical_holders_from_current``: drop the dataset row when an XML
    row shares the accession), so this is a STORAGE-redundancy metric, NOT an
    operator-visible bug — useful only to watch the raw redundancy trend / spot
    an ingest path that should stop double-writing."""
    sql = """
        SELECT count(*) AS groups, count(DISTINCT instrument_id) AS instruments
        FROM (
            SELECT instrument_id, holder_cik, source_accession
            FROM ownership_insiders_current
            WHERE source IN ('form4', 'form3') AND shares > 0 AND holder_cik IS NOT NULL
            GROUP BY 1, 2, 3
            HAVING bool_or(source_document_id ~ ':(NDT|NDH):')
               AND bool_or(source_document_id !~ ':(NDT|NDH):')
               AND count(DISTINCT ownership_nature) > 1
        ) g
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql)
        row = cur.fetchone() or {}
    return {
        "check": "dual_pipeline_insider_collision",
        "groups": int(row.get("groups", 0)),
        "instruments": int(row.get("instruments", 0)),
    }


def main() -> int:
    assert_dev_environment()  # read-only, but dev scripts never touch a remote DB (#1765)
    findings: list[dict[str, object]] = []
    # autocommit so one check's error (e.g. a missing column on a schema change)
    # doesn't poison the transaction for the remaining checks.
    dual_pipeline: dict[str, object] = {}
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        for table, holder_col, share_col in _OWNERSHIP_CURRENT:
            try:
                findings.append(_control_group_dup(conn, table, holder_col, share_col))
            except psycopg.Error as exc:
                findings.append({"check": "control_group_dup", "table": table, "error": str(exc)})
        try:
            dual_pipeline = _dual_pipeline_insider_collision(conn)
        except psycopg.Error as exc:
            dual_pipeline = {"check": "dual_pipeline_insider_collision", "error": str(exc)}

    print("=== DQ audit — control-group double-count in ownership rollups ===")
    print("(raw-table scan; the read-path rollup collapses same-accession dups — see note below)")
    # The same-accession distinct-holder signature these checks count is exactly
    # what the read path collapses with NO floor: #1764 (insiders + blockholders,
    # same accession) and #788 (insiders, dual-pipeline same cik). So a non-zero
    # raw figure here is NOT operator-visible — it must be confirmed by RENDERING
    # the rollup (residual same-(accession, shares) holder pair), never filed off
    # the raw count. (#1764 post-mortem: the raw scan reported 317 phantom insider
    # "LIVE" groups that the render showed were 100% collapsed.)
    for f in findings:
        if "error" in f:
            print(f"  {f['table']}: ERROR {f['error']}")
            continue
        live = int(f["live_groups"])  # type: ignore[call-overload]
        # insiders + blockholders same-accession dups are read-path collapsed
        # (#1764/#788); only institutions (no same-accession collapse pass) is a
        # direct candidate signal.
        collapsed = f["table"] in ("ownership_insiders_current", "ownership_blockholders_current")
        if collapsed:
            flag = "  read-path-collapsed"
        else:
            flag = "  ⚠ CANDIDATE" if live > 0 else "  ok"
        print(
            f"{flag}  {f['table']}: sub-1M {live} groups / "
            f"{f['live_instruments']} instruments / {f['live_overcount']:,.0f} shares "
            f"(raw incl. ≥1M: {f['total_groups']} groups / {f['total_overcount']:,.0f})"
        )

    if "error" in dual_pipeline:
        print(f"  dual_pipeline_insider_collision: ERROR {dual_pipeline['error']}")
    else:
        print(
            f"  storage-sentinel  dual-pipeline insider collision (read-path collapsed by #788): "
            f"{dual_pipeline['groups']} groups / {dual_pipeline['instruments']} instruments "
            f"(raw redundancy; not operator-visible)"
        )

    print(
        "\nFor insiders/blockholders: confirm any candidate by RENDERING the rollup "
        "(get_ownership_rollup → residual same-(accession, shares) insider holders that "
        "are NOT genuine #905 direct+indirect) and cite the source rule before filing "
        "(see #1764, #788)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
