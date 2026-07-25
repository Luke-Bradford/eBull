"""#2140 full-population A/B for the DEF 14A beneficial-ownership parser.

Re-parses EVERY stored ``def14a_body`` payload under the current working tree
and writes a JSON summary. Run once on ``main`` and once on the branch, then
diff the two summaries.

Offline — reads ``filing_raw_documents``, never fetches from EDGAR.

    PYTHONPATH=. uv run python scripts/ab_2140_def14a_parser.py --out /tmp/ab-main.json
    PYTHONPATH=. uv run python scripts/ab_2140_def14a_parser.py --out /tmp/ab-branch.json
    PYTHONPATH=. uv run python scripts/ab_2140_def14a_parser.py --diff /tmp/ab-main.json /tmp/ab-branch.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from typing import Any

import psycopg

# Imported for their side effect on the parse path as well as direct use.
import app.providers.implementations.sec_def14a as parser_mod
from app.config import settings

_NAME_EVIDENCE_RE = re.compile(r"[A-Za-z]{2,}")
_GROUP_RE = re.compile(r"as a group", re.IGNORECASE)


def _scan(limit: int | None) -> dict[str, Any]:
    """Re-parse every stored def14a body; return aggregate + per-accession rows."""
    promoted: list[dict[str, Any]] = []

    # Instrument the two-row-header promotion so the promoted-row audit
    # (Codex ckpt-1) covers the whole corpus, not the five-filing panel.
    original_parse_table = parser_mod._parse_table_html
    pending: dict[str, Any] = {}

    def traced_parse_table(table_html: str) -> Any:
        table = original_parse_table(table_html)
        if table is not None and table.column_headers != table.score_headers:
            pending.setdefault("promotions", []).append(
                {"headers": list(table.column_headers), "width": len(table.column_headers)}
            )
        return table

    parser_mod._parse_table_html = traced_parse_table

    totals = {
        "accessions": 0,
        "accessions_with_rows": 0,
        "rows": 0,
        "numeric_names": 0,
        "newline_names": 0,
        "group_rows": 0,
        "group_rows_tagged_group": 0,
        "shares_without_percent": 0,
        "sct_rows": 0,
    }
    per_accession: dict[str, int] = {}
    sct_fingerprints: dict[str, str] = {}

    # ``LIMIT %s`` as a bound parameter, never string-composed — Postgres
    # accepts NULL for "no limit", so one literal query covers both modes and
    # the prevention-log rule against interpolating into SQL is respected.
    sql = """
        SELECT accession_number, payload
        FROM filing_raw_documents
        WHERE document_kind = 'def14a_body'
        ORDER BY accession_number
        LIMIT %(limit)s
    """

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET statement_timeout = 0")
        with conn.cursor(name="def14a_ab") as cur:
            cur.itersize = 20
            cur.execute(sql, {"limit": limit})
            for accession, body in cur:
                totals["accessions"] += 1
                pending.clear()
                try:
                    parsed = parser_mod.parse_beneficial_ownership_table(body)
                except Exception as exc:  # noqa: BLE001 — A/B must not abort mid-corpus
                    per_accession[accession] = -1
                    print(f"PARSE-ERROR {accession}: {type(exc).__name__}: {exc}", file=sys.stderr)
                    continue

                for promo in pending.get("promotions", []):
                    promoted.append({"accession": accession, **promo})

                rows = parsed.rows
                per_accession[accession] = len(rows)
                if rows:
                    totals["accessions_with_rows"] += 1
                for holder in rows:
                    totals["rows"] += 1
                    name = holder.holder_name
                    if not _NAME_EVIDENCE_RE.search(name):
                        totals["numeric_names"] += 1
                    if "\n" in name:
                        totals["newline_names"] += 1
                    if _GROUP_RE.search(name):
                        totals["group_rows"] += 1
                        if holder.holder_role == "group":
                            totals["group_rows_tagged_group"] += 1
                    if holder.shares is not None and holder.percent_of_class is None:
                        totals["shares_without_percent"] += 1

                # Item 402(c) blast-radius proof: the SCT output must not move.
                try:
                    sct = parser_mod.parse_summary_compensation_table(body)
                except Exception:  # noqa: BLE001
                    sct_fingerprints[accession] = "ERROR"
                else:
                    totals["sct_rows"] += len(sct.rows)
                    sct_fingerprints[accession] = "|".join(
                        f"{r.executive_name}~{r.principal_position or ''}~{r.fiscal_year}~{r.total_comp or ''}"
                        for r in sct.rows
                    )

    parser_mod._parse_table_html = original_parse_table
    return {
        "totals": totals,
        "per_accession": per_accession,
        "sct_fingerprints": sct_fingerprints,
        "promoted_rows": promoted,
    }


def _diff(before: dict[str, Any], after: dict[str, Any]) -> int:
    print("== totals ==")
    for key in before["totals"]:
        b, a = before["totals"][key], after["totals"].get(key)
        flag = "" if a == b else "   <-- CHANGED"
        print(f"  {key:28s} {b:>10} -> {a:>10}{flag}")

    print("\n== Item 402(c) SCT drift (must be empty) ==")
    sct_drift = [acc for acc, fp in before["sct_fingerprints"].items() if after["sct_fingerprints"].get(acc) != fp]
    print(f"  accessions with changed SCT output: {len(sct_drift)}")
    for acc in sct_drift[:20]:
        print(f"    {acc}")

    print("\n== per-accession beneficial-ownership row deltas ==")
    lost, gained = [], []
    for acc, b in before["per_accession"].items():
        a = after["per_accession"].get(acc, 0)
        if a < b:
            lost.append((acc, b, a))
        elif a > b:
            gained.append((acc, b, a))
    print(f"  accessions LOSING rows:  {len(lost)}")
    for acc, b, a in sorted(lost, key=lambda x: x[1] - x[2], reverse=True)[:25]:
        print(f"    {acc}  {b} -> {a}")
    print(f"  accessions GAINING rows: {len(gained)}")
    for acc, b, a in sorted(gained, key=lambda x: x[2] - x[1], reverse=True)[:25]:
        print(f"    {acc}  {b} -> {a}")

    print("\n== promoted header rows (audit) ==")
    print(f"  before: {len(before['promoted_rows'])}   after: {len(after['promoted_rows'])}")
    seen = {json.dumps(p["headers"]) for p in before["promoted_rows"]}
    new = [p for p in after["promoted_rows"] if json.dumps(p["headers"]) not in seen]
    print(f"  newly-promoted distinct header shapes: {len({json.dumps(p['headers']) for p in new})}")
    for headers in sorted({json.dumps(p["headers"]) for p in new})[:40]:
        print(f"    {headers}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", help="write a scan summary to this path")
    p.add_argument("--limit", type=int, default=None, help="scan only the first N bodies (smoke)")
    p.add_argument("--diff", nargs=2, metavar=("BEFORE", "AFTER"), help="diff two summaries")
    args = p.parse_args(argv if argv is not None else sys.argv[1:])

    if args.diff:
        with open(args.diff[0]) as f:
            before = json.load(f)
        with open(args.diff[1]) as f:
            after = json.load(f)
        return _diff(before, after)

    if not args.out:
        p.error("one of --out or --diff is required")
    result = _scan(args.limit)
    with open(args.out, "w") as f:
        json.dump(result, f)
    print(json.dumps(result["totals"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
