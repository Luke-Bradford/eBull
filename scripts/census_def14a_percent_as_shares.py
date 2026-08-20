"""#2163 census — size the percent-stored-as-share-count cohort BY RE-PARSE.

The SQL proxy (``shares <> trunc(shares) AND percent_of_class IS NULL``) is a
FLOOR: a percent of exactly ``5`` is stored as ``5`` shares and is invisible.
This traces the real parse path, inspects the WINNING Item 403 table's resolved
``shares_idx`` cell per row, and flags rows where that cell is a PERCENT.

17 CFR 229.403 column 3 = "Amount and nature of beneficial ownership" (a count
of shares); column 4 = "Percent of class". They are distinct columns.

Read-only, offline from ``filing_raw_documents``.

    PYTHONPATH=. uv run python scripts/census_def14a_percent_as_shares.py --out /tmp/census2163.json
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

import psycopg

import app.providers.implementations.sec_def14a as P
from app.config import settings

_PCT_SIGNS = ("%", "(%)")


def _next_nonempty(cells: list[str], start: int) -> str:
    return next((c.strip() for c in cells[start:] if c.strip()), "")


def _inspect_table(table: Any, name_idx: int, shares_idx: int, percent_idx: int) -> list[dict[str, Any]]:
    """Return one record per data row whose shares_idx cell looks like a PERCENT."""
    out: list[dict[str, Any]] = []
    headers = table.column_headers
    hdr_w = len(headers)
    data_w = max((len(r) for r in table.rows), default=0)
    header_blanks = sum(1 for h in headers if not h.strip())
    ragged_header = hdr_w > data_w and header_blanks > 0
    shares_caption = headers[shares_idx].lower() if 0 <= shares_idx < hdr_w else ""
    pct_caption = "percent" in shares_caption or "%" in shares_caption

    for raw_row in table.rows:
        cells = P._pad_row(raw_row, name_idx=name_idx, shares_idx=shares_idx, percent_idx=percent_idx)
        if not any(c.strip() for c in cells):
            continue
        raw_shares = cells[shares_idx] if 0 <= shares_idx < len(cells) else ""
        v = P._parse_share_count(raw_shares)
        if v is None:
            continue
        name_src_idx, raw_name = P._resolve_row_name(cells, name_idx=name_idx, shares_idx=shares_idx)
        holder = P._clean_beneficial_holder_name(raw_name)
        if not holder:
            continue

        fractional = v != v.to_integral_value()
        pct_sibling = _next_nonempty(cells, shares_idx + 1) in _PCT_SIGNS
        pct_in_cell = "%" in raw_shares

        # Is a genuine WHOLE share count available elsewhere in the row?
        alt: str | None = None
        for i, cell in enumerate(cells):
            if i in (name_src_idx, shares_idx):
                continue
            if "%" in cell:
                continue
            cand = P._parse_share_count(cell)
            if cand is not None and cand == cand.to_integral_value() and cand > 0:
                alt = cell.strip()
                break

        if not (fractional or pct_sibling or pct_caption or pct_in_cell):
            continue
        out.append(
            {
                "holder": holder.strip().lower(),
                "shares_cell": raw_shares.strip(),
                "value": str(v),
                "fractional": fractional,
                "pct_sibling": pct_sibling,
                "pct_caption": pct_caption,
                "pct_in_cell": pct_in_cell,
                "ragged_header": ragged_header,
                "hdr_w": hdr_w,
                "data_w": data_w,
                "alt_whole": alt,
                "idx": [name_idx, shares_idx, percent_idx],
                "headers": list(headers),
            }
        )
    return out


def scan(limit: int | None) -> dict[str, Any]:
    original = P._extract_holder_rows
    bucket: list[dict[str, Any]] = []

    def traced(table: Any, *, name_idx: int, shares_idx: int, percent_idx: int, rows: Any, seen: Any) -> None:
        try:
            bucket.extend(_inspect_table(table, name_idx, shares_idx, percent_idx))
        except Exception as exc:  # noqa: BLE001 — census must not abort mid-corpus
            print(f"INSPECT-ERROR {type(exc).__name__}: {exc}", file=sys.stderr)
        return original(table, name_idx=name_idx, shares_idx=shares_idx, percent_idx=percent_idx, rows=rows, seen=seen)

    # Restored in the ``finally`` below (Codex ckpt-3): an exception escaping the
    # loop would otherwise leave the module patched for the rest of the process.
    P._extract_holder_rows = traced

    sql = """
        SELECT accession_number, payload
        FROM filing_raw_documents
        WHERE document_kind = 'def14a_body'
        ORDER BY accession_number
        LIMIT %(limit)s
    """
    hits: dict[str, list[dict[str, Any]]] = {}
    n_acc = 0
    try:
        with psycopg.connect(settings.database_url) as conn:
            conn.execute("SET statement_timeout = 0")
            with conn.cursor(name="c2163") as cur:
                cur.itersize = 20
                cur.execute(sql, {"limit": limit})
                for accession, body in cur:
                    n_acc += 1
                    if n_acc % 2000 == 0:
                        print(f"  ... {n_acc} accessions, {len(hits)} hit", file=sys.stderr)
                    bucket.clear()
                    try:
                        parsed = P.parse_beneficial_ownership_table(body)
                    except Exception as exc:  # noqa: BLE001
                        print(f"PARSE-ERROR {accession}: {type(exc).__name__}: {exc}", file=sys.stderr)
                        continue
                    if not bucket:
                        continue
                    # Only count suspects that actually SURVIVED into stored rows
                    # with that value as their share count.
                    stored = {
                        (r.holder_name.strip().lower(), str(r.shares)) for r in parsed.rows if r.shares is not None
                    }
                    live = [b for b in bucket if (b["holder"], b["value"]) in stored]
                    if live:
                        hits[accession] = live

    finally:
        P._extract_holder_rows = original
    return {"accessions_scanned": n_acc, "hits": hits}


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--limit", type=int, default=None)
    a = ap.parse_args(argv)
    res = scan(a.limit)
    with open(a.out, "w") as fh:
        json.dump(res, fh)
    rows = sum(len(v) for v in res["hits"].values())
    print(f"scanned={res['accessions_scanned']} hit_accessions={len(res['hits'])} hit_rows={rows}")
    frac = sum(1 for v in res["hits"].values() for r in v if r["fractional"])
    ragged = len({k for k, v in res["hits"].items() if any(r["ragged_header"] for r in v)})
    withalt = sum(1 for v in res["hits"].values() for r in v if r["alt_whole"])
    print(f"  fractional={frac}  ragged_header_accessions={ragged}  rows_with_whole_alt={withalt}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
