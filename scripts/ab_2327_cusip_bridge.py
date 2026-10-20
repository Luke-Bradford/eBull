"""#2327 — A/B of the 13D/G CUSIP bridge in one rolled-back REPEATABLE READ transaction (spec "Deploy" step 2).

Runs the production ``ensure_window_lists`` and ``bind_13dg_cusips`` against the dev DB, then checks:

- ``external_identifiers`` gains exactly the bridge's inserts and loses nothing;
- no pre-existing CUSIP changes instrument in ``load_bulk_cusip_map`` (what the bulk drains resolve with);
- every ``cusip_bridge_bindings`` row names a row it inserted;
- with ``--expected``, the outcome set equals the spec's reference (``var/autonomy/2327/q11_out.json``) CUSIP by
  CUSIP; each difference is printed for tracing to a changed input.

``--lists-dir`` serves missing window lists from local files instead of SEC (``13flist{year}q{quarter}.txt``).
``--out`` writes the outcome set (the recipients for spec steps 3–6, and step 4's comparison). Nothing commits:
the transaction is rolled back, and ``SELECT 1`` runs first because ``conn.transaction()`` on an idle connection
commits (#3893).

    PYTHONPATH=. uv run python scripts/ab_2327_cusip_bridge.py \\
        --expected var/autonomy/2327/q11_out.json --out var/autonomy/2327/ab_outcomes.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

import psycopg

from app.config import settings
from app.services.cusip_bridge_13dg import bind_13dg_cusips, decide, read_bridge_inputs
from app.services.cusip_resolver import load_bulk_cusip_map
from app.services.sec_13f_securities_list import ensure_window_lists, fetch_13f_list_txt

_MIGRATION = Path(__file__).resolve().parent.parent / "sql" / "510_cusip_bridge_bindings.sql"

# q11's labels → rule v1's outcome names.
_REFERENCE_LABELS = {"noop_sec": "noop", "CONFIRM": "confirm", "BIND": "bind"}


def _identifier_rows(conn: psycopg.Connection[tuple]) -> set[tuple[int, str, str, str, int]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT external_identifier_id, provider, identifier_type, identifier_value, instrument_id "
            "FROM external_identifiers"
        )
        return {(int(a), str(b), str(c), str(d), int(e)) for a, b, c, d, e in cur.fetchall()}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected", type=Path)
    parser.add_argument("--lists-dir", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()

    def fetch(year: int, quarter: int) -> tuple[str, str]:
        if args.lists_dir is not None:
            path = args.lists_dir / f"{year}q{quarter}.txt"
            if path.exists():
                return path.read_text(encoding="latin-1"), f"file://{path}"
        return fetch_13f_list_txt(year, quarter)

    failures: list[str] = []
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.execute("SELECT 1")
        try:
            if conn.execute("SELECT to_regclass('cusip_bridge_bindings')").fetchone() == (None,):
                # Before the deploy the migration is not applied; DDL is transactional, so the rollback removes it.
                conn.execute(_MIGRATION.read_text())  # type: ignore[arg-type] — a fixed repo file, not user input
            before_rows = _identifier_rows(conn)
            before_map = load_bulk_cusip_map(conn)
            lists = ensure_window_lists(conn, fetch=fetch)
            print("window:", [(wl.quarter, wl.gap_reason or "available", len(wl.share_cusips)) for wl in lists])

            inputs, _, _ = read_bridge_inputs(conn, lists)
            decisions = decide(inputs)
            result = bind_13dg_cusips(conn, lists=lists, job_run_id=0)
            print(result.note())

            after_rows = _identifier_rows(conn)
            after_map = load_bulk_cusip_map(conn)
            gained = after_rows - before_rows
            lost = before_rows - after_rows
            with conn.cursor() as cur:
                cur.execute("SELECT external_identifier_id, cusip, instrument_id, outcome FROM cusip_bridge_bindings")
                provenance = {int(e): (str(c), int(i), str(o)) for e, c, i, o in cur.fetchall()}

            print(f"external_identifiers gained {len(gained)}, lost {len(lost)}; provenance rows {len(provenance)}")
            if lost:
                failures.append(f"{len(lost)} external_identifiers rows lost")
            if len(gained) != sum(result.inserted.values()):
                failures.append(f"gained {len(gained)} != inserted {sum(result.inserted.values())}")
            gained_ids = {g[0] for g in gained}
            if not set(provenance) <= gained_ids | {r[0] for r in before_rows}:
                failures.append("a provenance row names no external_identifiers row")
            moved = {c: (before_map[c], after_map.get(c)) for c in before_map if after_map.get(c) != before_map[c]}
            print(f"load_bulk_cusip_map: {len(before_map)} → {len(after_map)}; pre-existing CUSIPs moved: {len(moved)}")
            if moved:
                failures.append(f"{len(moved)} pre-existing CUSIPs changed instrument: {sorted(moved.items())[:10]}")

            outcomes = {c: d.outcome for c, d in decisions.items()}
            print("outcomes:", dict(sorted(Counter(outcomes.values()).items())))
            if args.expected is not None:
                expected = {c: _REFERENCE_LABELS.get(o, o) for c, o in json.loads(args.expected.read_text()).items()}
                # The reference holds the candidates past R4 only; compare on those and on ours.
                ours = {c for c, d in decisions.items() if d.instrument_id is not None}
                diff = sorted(
                    (c, expected.get(c), outcomes.get(c))
                    for c in expected.keys() | ours
                    if expected.get(c) != outcomes.get(c)
                )
                print(f"vs reference: {len(diff)} CUSIPs differ")
                for row in diff:
                    print("  ", row)
                if diff:
                    # Each difference must be traced to a changed input before the step passes (spec step 2).
                    failures.append(f"{len(diff)} CUSIPs differ from the reference")
            if args.out is not None:
                recipients = {
                    c: {"outcome": d.outcome, "instrument_id": d.instrument_id, "issuer_cik": d.issuer_cik}
                    for c, d in decisions.items()
                }
                args.out.write_text(json.dumps(recipients, indent=0, sort_keys=True))
        finally:
            conn.rollback()
    for f in failures:
        print("FAIL:", f)
    print("PASS" if not failures else "FAIL")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
