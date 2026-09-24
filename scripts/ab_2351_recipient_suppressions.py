"""#2351 slice 2 — full-population A/B for the DEF 14A recipient-suppression job.

For every instrument in the job's population (issuer CIKs backing >1 instrument),
hashes each operator-visible DEF 14A surface BEFORE and AFTER one job run:

- ``ownership_def14a_current`` and ``ownership_esop_current`` rows;
- the typed latest-holders read (``/instruments/{symbol}/def14a`` query, via the view);
- the CSV export row set (all attributed typed rows);
- ``def14a_drift_alerts`` rows;
- the ownership-history DEF 14A series source (attributed observations).

Pass condition: the set of instruments whose hashes changed EQUALS the set of
instruments whose ledger KEYS the run added or removed — ``(instrument_id,
accession_number)`` and, slice 3b, the row ledger's ``(instrument_id, accession_number,
holder_name)`` — and no instrument's apply failed. Every ``ownership_def14a_current`` row
that changed is listed (slice 3b: a withheld row may fall back to an older accession's). Also lists
instruments whose latest attributed accession fell back to an older,
unsuppressed one.

Usage (applies the job's writes to the connected DB)::

    PYTHONPATH=. uv run python -m scripts.ab_2351_recipient_suppressions --out var/ab_2351/report.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings
from app.services.def14a_recipients import load_population, run_with_sec_provider

_SURFACES: dict[str, str] = {
    "def14a_current": """
        SELECT holder_name_key, ownership_nature, source_accession, shares, percent_of_class
          FROM ownership_def14a_current WHERE instrument_id = %(iid)s ORDER BY 1, 2""",
    "esop_current": """
        SELECT plan_name, ownership_nature, source_accession, shares, percent_of_class
          FROM ownership_esop_current WHERE instrument_id = %(iid)s ORDER BY 1, 2""",
    "typed_latest": """
        WITH latest AS (
            SELECT accession_number FROM def14a_beneficial_holdings_attributed
             WHERE instrument_id = %(iid)s
             ORDER BY as_of_date DESC NULLS LAST, accession_number DESC LIMIT 1)
        SELECT h.accession_number, holder_name, shares, percent_of_class
          FROM def14a_beneficial_holdings_attributed h JOIN latest USING (accession_number)
         WHERE instrument_id = %(iid)s ORDER BY 1, 2, 3""",
    "typed_all": """
        SELECT accession_number, holder_name, shares, percent_of_class
          FROM def14a_beneficial_holdings_attributed WHERE instrument_id = %(iid)s ORDER BY 1, 2, 3""",
    "drift_alerts": """
        SELECT holder_name, accession_number, severity, def14a_shares
          FROM def14a_drift_alerts WHERE instrument_id = %(iid)s ORDER BY 1, 2""",
    "history_obs": """
        SELECT period_end, ownership_nature, source_accession, shares
          FROM ownership_def14a_observations_attributed
         WHERE instrument_id = %(iid)s AND known_to IS NULL AND shares IS NOT NULL ORDER BY 1, 2, 3, 4""",
}


def _latest_accession(conn: psycopg.Connection[Any], iid: int, *, attributed: bool) -> str | None:
    if attributed:
        row = conn.execute(
            """SELECT accession_number FROM def14a_beneficial_holdings_attributed WHERE instrument_id = %s
               ORDER BY as_of_date DESC NULLS LAST, accession_number DESC LIMIT 1""",
            (iid,),
        ).fetchone()
    else:
        row = conn.execute(
            """SELECT accession_number FROM def14a_beneficial_holdings WHERE instrument_id = %s
               ORDER BY as_of_date DESC NULLS LAST, accession_number DESC LIMIT 1""",
            (iid,),
        ).fetchone()
    return str(row[0]) if row else None


def _ledger_keys(conn: psycopg.Connection[Any]) -> set[tuple[Any, ...]]:
    keys: set[tuple[Any, ...]] = {
        (int(i), str(a))
        for i, a in conn.execute("SELECT instrument_id, accession_number FROM def14a_recipient_suppressions").fetchall()
    }
    keys |= {
        (int(i), str(a), str(h))
        for i, a, h in conn.execute(
            "SELECT instrument_id, accession_number, holder_name FROM def14a_recipient_row_suppressions"
        ).fetchall()
    }
    return keys


def _current_rows(conn: psycopg.Connection[Any], iid: int) -> set[tuple[Any, ...]]:
    return {
        tuple(r)
        for r in conn.execute(
            """SELECT holder_name, ownership_nature, source_accession, shares, percent_of_class
                 FROM ownership_def14a_current WHERE instrument_id = %s""",
            (iid,),
        ).fetchall()
    }


def snapshot(conn: psycopg.Connection[Any], iids: list[int]) -> dict[int, dict[str, str]]:
    out: dict[int, dict[str, str]] = {}
    for iid in iids:
        out[iid] = {}
        for name, sql in _SURFACES.items():
            rows = conn.execute(sql, {"iid": iid}).fetchall()  # type: ignore[arg-type]
            out[iid][name] = hashlib.sha256(repr(rows).encode()).hexdigest()
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        population = load_population(conn)
        symbols = {s.instrument_id: s.symbol for sibs in population.values() for s in sibs}
        iids = sorted(symbols)
        keys_before = _ledger_keys(conn)
        iids = sorted(set(iids) | {k[0] for k in keys_before})
        before = snapshot(conn, iids)
        current_before = {iid: _current_rows(conn, iid) for iid in iids}
        report = run_with_sec_provider(conn)
        after = snapshot(conn, iids)
        suppressions = conn.execute(
            """SELECT instrument_id, accession_number, cover_accession, cover_title, witness_instrument_id
                 FROM def14a_recipient_suppressions ORDER BY 1, 2"""
        ).fetchall()

        suppressed_iids = {int(r[0]) for r in suppressions}
        key_delta = keys_before ^ _ledger_keys(conn)
        delta_iids = {k[0] for k in key_delta}
        changed = {iid: sorted(k for k in before[iid] if before[iid][k] != after[iid][k]) for iid in iids}
        changed = {iid: v for iid, v in changed.items() if v}
        current_changes = {}
        for iid in sorted(changed):
            now = _current_rows(conn, iid)
            if now != current_before[iid]:
                current_changes[str(symbols.get(iid))] = {
                    "removed": sorted(current_before[iid] - now),
                    "added": sorted(now - current_before[iid]),
                }
        row_suppressions = conn.execute(
            """SELECT instrument_id, accession_number, holder_name, class_cell, witness_instrument_id
                 FROM def14a_recipient_row_suppressions ORDER BY 1, 2, 3"""
        ).fetchall()
        fallback = []
        for iid in sorted(suppressed_iids):
            raw_latest = _latest_accession(conn, iid, attributed=False)
            attr_latest = _latest_accession(conn, iid, attributed=True)
            if attr_latest is not None and attr_latest != raw_latest:
                fallback.append(
                    {"symbol": symbols.get(iid), "raw_latest": raw_latest, "attributed_latest": attr_latest}
                )

    result = {
        "population_ciks": len(population),
        "population_instruments": len(iids),
        "ledger_rows_before": len(keys_before),
        "ledger_keys_added_or_removed": sorted(" ".join(map(str, (symbols.get(k[0]), *k[1:]))) for k in key_delta),
        "run_report": vars(report),
        "suppressions": [
            {
                "symbol": symbols.get(int(r[0])),
                "accession": r[1],
                "cover": r[2],
                "cover_title": r[3],
                "witness": symbols.get(int(r[4])),
            }
            for r in suppressions
        ],
        "row_suppressions": [
            {
                "symbol": symbols.get(int(r[0])),
                "accession": r[1],
                "holder": r[2],
                "class_cell": r[3],
                "witness": symbols.get(int(r[4])),
            }
            for r in row_suppressions
        ],
        "current_row_changes": current_changes,
        "changed_instruments": {symbols.get(i): v for i, v in sorted(changed.items())},
        "changed_without_key_delta": sorted(str(symbols.get(i)) for i in set(changed) - delta_iids),
        "key_delta_not_changed": sorted(str(symbols.get(i)) for i in delta_iids - set(changed)),
        "fallback_to_older_accession": fallback,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, default=str))
    skip = {"suppressions", "row_suppressions", "current_row_changes", "ledger_keys_added_or_removed"}
    print(json.dumps({k: v for k, v in result.items() if k not in skip}, indent=2, default=str))
    ok = not (result["changed_without_key_delta"] or result["key_delta_not_changed"] or report.instruments_failed)
    print("PASS" if ok else "FAIL: changed set != key-delta set, or an apply failed")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
