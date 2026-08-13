"""Arm 3 (mechanism audit) for the DEF 14A row-span expansion (#2175).

The parse-to-parse A/B says WHAT changed. This says HOW MUCH of the corpus the
mechanism touches and — the direction that matters per
``.claude/skills/engineering/full-population-ab.md`` — what the change newly
ADMITS that the old code could not see:

  * ``tables_with_rowspan``     — candidate tables carrying any ``rowspan>1``.
  * ``tables_shifted``          — of those, the ones where expansion actually
    changes a row's cell tuple. A ``rowspan`` on the LAST column of the last
    row shifts nothing; only these are the defect.
  * ``rows_newly_non_empty``    — rows that were entirely empty in the markup
    and now carry spanned text. These are rows the parser has NEVER seen, so no
    guard in the row loop has any coverage of them. This is the admit side.
  * ``header_width_changed``    — tables whose row-0 width moves. Row 0 feeds
    the two-row-header promotion arms (which compare header width against data
    width), so a move here can change WHICH TABLE WINS — the #2356 failure mode
    where a correct-looking cell-level fix silently reselected the table.

Offline — reads ``filing_raw_documents``, never fetches from EDGAR. Run on the
BRANCH: it measures both sides in one process by calling the expansion or not,
which is legitimate here because the expansion is a pure function of the parsed
cells and there is no control BEHAVIOUR to reconstruct (contrast the simulated
control the A/B skill forbids — that one had to re-derive a two-limb condition).

    PYTHONPATH=. uv run python scripts/audit_def14a_rowspan.py --out /tmp/rowspan-audit.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from typing import Any

import psycopg

import app.providers.implementations.sec_def14a as P
from app.config import settings


def _rows_of(table_html: str) -> list[tuple[tuple[str, int, int], ...]] | None:
    """The ``(text, rowspan, colspan)`` rows ``_parse_table_html`` builds, or None."""
    open_match = P._TABLE_OPEN_RE.search(table_html)
    close_idx = table_html.rfind("</table")
    if open_match is None or close_idx == -1:
        return None
    inner = table_html[open_match.end() : close_idx]
    nested = P._scan_outer_tables(inner)
    if nested:
        pieces: list[str] = []
        cursor = 0
        for start, end in nested:
            pieces.append(inner[cursor:start])
            pieces.append(" ")
            cursor = end
        pieces.append(inner[cursor:])
        scrubbed = "".join(pieces)
    else:
        scrubbed = inner
    out: list[tuple[tuple[str, int, int], ...]] = []
    for tr_match in P._TR_RE.finditer(scrubbed):
        out.append(
            tuple(
                (
                    P._strip_inline_html(cell_inner),
                    int(rs.group(1)) if (rs := P._ROWSPAN_RE.search(attrs)) else 1,
                    max(1, int(cs.group(1))) if (cs := P._COLSPAN_RE.search(attrs)) else 1,
                )
                for attrs, cell_inner in P._CELL_RE.findall(tr_match.group(1))
            )
        )
    return out


def _audit(limit: int | None) -> dict[str, Any]:
    totals: Counter[str] = Counter()
    per_accession: dict[str, dict[str, int]] = {}
    max_rowspan: list[tuple[int, str]] = []

    sql = """
        SELECT accession_number, payload
        FROM filing_raw_documents
        WHERE document_kind = 'def14a_body'
        ORDER BY accession_number
        LIMIT %(limit)s
    """
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET statement_timeout = 0")
        with conn.cursor(name="def14a_rowspan_audit") as cur:
            cur.itersize = 20
            cur.execute(sql, {"limit": limit})
            for accession, body in cur:
                totals["accessions"] += 1
                acc_counts: Counter[str] = Counter()
                for window_start, window_end in P._find_section_windows(body):
                    for start, end in P._scan_outer_tables(body, start=window_start, end=window_end):
                        spanned = _rows_of(body[start:end])
                        if spanned is None:
                            continue
                        acc_counts["tables"] += 1
                        biggest = max((rs for row in spanned for _, rs, _ in row), default=1)
                        if biggest <= 1:
                            continue
                        acc_counts["tables_with_rowspan"] += 1
                        max_rowspan.append((biggest, accession))
                        expanded = [row.cells for row in P._expand_row_spans(spanned)]
                        control = [tuple(text for text, _, _ in row) for row in spanned]
                        if expanded != control:
                            acc_counts["tables_shifted"] += 1
                        for before_row, after_row in zip(control, expanded, strict=True):
                            if not any(before_row) and any(after_row):
                                acc_counts["rows_newly_non_empty"] += 1
                        if control and expanded and len(control[0]) != len(expanded[0]):
                            acc_counts["header_width_changed"] += 1
                    # Only the FIRST window that yields tables is scanned, mirroring
                    # the parser's window loop closely enough for a scale figure.
                    if acc_counts["tables"]:
                        break
                totals.update(acc_counts)
                if acc_counts.get("tables_shifted"):
                    per_accession[accession] = dict(acc_counts)

    max_rowspan.sort(reverse=True)
    return {
        "totals": dict(totals),
        "accessions_with_a_shifted_table": len(per_accession),
        "largest_rowspans": max_rowspan[:20],
        "per_accession": per_accession,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    summary = _audit(args.limit)
    with open(args.out, "w") as fh:
        json.dump(summary, fh, indent=2)
    print(json.dumps({k: v for k, v in summary.items() if k != "per_accession"}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
