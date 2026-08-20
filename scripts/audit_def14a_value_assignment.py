"""Arm 3 — mechanism audit for the DEF 14A Item 403 value assignment (#2163).

The A/B harness (``scripts/ab_2140_def14a_parser.py``) keys on holder NAME, so
it is blind to a holder that survives with the WRONG numbers — which is exactly
what this ticket is about. It is equally blind to ``holder_role`` drift (#2164's
40-holder ``principal`` -> ``None`` regression was visible only to a role audit).

This captures ``{accession: {holder_name_key: [shares, percent, role]}}`` so the
diff can state, per holder, whether the change moved a value, moved a role,
added a value that was NULL, or dropped one.

Run on BOTH checkouts — a real control, never a simulated one — and diff:

    PYTHONPATH=. uv run python scripts/audit_def14a_value_assignment.py /tmp/a.json
    PYTHONPATH=. uv run python scripts/audit_def14a_value_assignment.py --diff /tmp/a.json /tmp/b.json
"""

from __future__ import annotations

import json
import sys
from typing import Any

import psycopg

import app.providers.implementations.sec_def14a as parser_mod
from app.config import settings


def _scan(out: str) -> int:
    audit: dict[str, dict[str, list[str | None]]] = {}
    n = 0
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET statement_timeout = 0")
        with conn.cursor(name="value_audit") as cur:
            cur.itersize = 20
            cur.execute(
                "SELECT accession_number, payload FROM filing_raw_documents "
                "WHERE document_kind = 'def14a_body' ORDER BY accession_number"
            )
            for acc, payload in cur:
                n += 1
                if n % 2000 == 0:
                    print(f"... {n}", file=sys.stderr, flush=True)
                try:
                    res = parser_mod.parse_beneficial_ownership_table(payload)
                except Exception:  # noqa: BLE001 — the audit must not abort mid-corpus
                    continue
                if not res.rows:
                    continue
                # Keyed exactly as the database keys holders (holder_name_key is
                # lower(trim(holder_name)), sql/116:110) so "the same holder"
                # means the same thing it means in the read path.
                audit[acc] = {
                    h.holder_name.strip().lower(): [
                        str(h.shares) if h.shares is not None else None,
                        str(h.percent_of_class) if h.percent_of_class is not None else None,
                        h.holder_role,
                    ]
                    for h in res.rows
                }
    with open(out, "w") as fh:
        json.dump(audit, fh)
    print(f"DONE {n} accessions, {len(audit)} with rows", file=sys.stderr)
    return 0


def _diff(before_path: str, after_path: str) -> int:
    with open(before_path) as fh:
        before: dict[str, Any] = json.load(fh)
    with open(after_path) as fh:
        after: dict[str, Any] = json.load(fh)

    buckets: dict[str, list[str]] = {
        "shares_changed": [],
        "shares_added": [],
        "shares_dropped": [],
        "percent_changed": [],
        "percent_added": [],
        "percent_dropped": [],
        "role_changed": [],
    }
    for acc, holders in before.items():
        post = after.get(acc, {})
        for name, (b_sh, b_pc, b_role) in holders.items():
            if name not in post:
                continue  # holder-level loss/gain is arm 1's job, not this one
            a_sh, a_pc, a_role = post[name]
            tag = f"{acc} {name[:44]!r}"
            if b_sh != a_sh:
                key = "shares_added" if b_sh is None else ("shares_dropped" if a_sh is None else "shares_changed")
                buckets[key].append(f"{tag} {b_sh} -> {a_sh}")
            if b_pc != a_pc:
                key = "percent_added" if b_pc is None else ("percent_dropped" if a_pc is None else "percent_changed")
                buckets[key].append(f"{tag} {b_pc} -> {a_pc}")
            if b_role != a_role:
                buckets["role_changed"].append(f"{tag} {b_role} -> {a_role}")

    worst = 0
    for key, items in buckets.items():
        print(f"\n== {key}: {len(items)} ==")
        for line in items[:40]:
            print(f"  {line}")
        if len(items) > 40:
            print(f"  ... and {len(items) - 40} more")
        # Role drift and dropped values are the regression-shaped buckets.
        if key in ("role_changed", "shares_dropped", "percent_dropped") and items:
            worst = 1
    return worst


def main(argv: list[str]) -> int:
    if argv and argv[0] == "--diff":
        return _diff(argv[1], argv[2])
    return _scan(argv[0])


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
