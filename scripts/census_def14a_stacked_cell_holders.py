"""Full-population census of the ONE-ROW / N-HOLDERS Item 403 shape (#2169).

The defect: an issuer renders several beneficial owners inside a SINGLE
``<tr>``, separated by ``<br>``, so every cell of that row is a stack of
lines rather than one value. ``_clean_beneficial_holder_name`` flattens the
name stack into one holder (correct for #2140 D5's render wrap, wrong here)
and the value cells — which now hold two numbers — fail to parse, so the row
is dropped.

#2169 asks for the census BEFORE any fix, because the known population is one
2018 accession found incidentally and "the same shape riding alongside genuine
holders is uncounted".

What is counted, and why the VALUE side is the discriminator: a name cell with
interior newlines is ambiguous (a render wrap produces exactly that, which is
what #2140 D5's flatten exists for). A cell that parses as TWO OR MORE share
counts is not ambiguous — 17 CFR 229.403 column 3 is one amount per beneficial
owner, so a second whole number on its own line in that cell is a second
holder's amount, not a wrap of the first.

Offline — reads ``filing_raw_documents``, never fetches from EDGAR. Runs on
whichever tree it is invoked from; report the commit alongside the numbers.

    PYTHONPATH=. uv run python scripts/census_def14a_stacked_cell_holders.py --out /tmp/2169-census.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from typing import Any

import psycopg

import app.providers.implementations.sec_def14a as P
from app.config import settings

# The segmentation and the two value classifiers are the PARSER's, imported
# rather than restated (review NITPICK on PR #2359, and the same drift the
# `_SCORE_FLOOR` nitpick before it named): a census whose definition of "this
# segment is a 229.403 amount" differs from the code's is measuring a different
# population than the one the fix acts on.
#
# ⚠ Consequence, and it is deliberate: this script is coupled to a tree where
# `_is_whole_share_segment` / `_is_percent_segment` exist, so it cannot be run
# unmodified against a pre-#2169 checkout. The control-arm numbers in the PR
# were taken with the self-contained earlier revision for exactly that reason.
_segments = P._cell_segments


def _whole_counts(segs: list[str]) -> list[str]:
    """Segments that are a 229.403 column-3 amount — a WHOLE share count."""
    return [seg for seg in segs if P._is_whole_share_segment(seg)]


def _percents(segs: list[str]) -> list[str]:
    """Segments that are UNAMBIGUOUSLY a 229.403 column-4 percent."""
    return [seg for seg in segs if P._is_percent_segment(seg)]


# Arm 2 (Codex checkpoint 2, #2169). ``<br>`` is stripped to a SPACE by
# ``_strip_inline_html``, not to a newline, so an issuer whose markup carries no
# literal source newline after the tag renders two amounts onto ONE line:
# ``'486,340<br>658,400'`` arrives as ``'486,340 658,400'``. Arm 1's newline
# segmentation cannot see that, and neither can the split this ticket adds — but
# ``_parse_share_count`` strips spaces AND commas, so the cell parses to
# 486,340,658,400. That is a WRONG STORED VALUE rather than a dropped row, which
# is strictly worse, so its population has to be measured rather than assumed.
#
# Two or more whitespace-separated groups of >= 3 digits. Three, not one: a
# trailing 1-2 digit group is the unbracketed footnote superscript that
# ``_TRAILING_FOOTNOTE_RE`` already strips ('52,606,862 1' is #2140's case, not
# this one).
_GLUED_AMOUNTS_RE = re.compile(r"^[\s(\[]*\d[\d,]{2,}(?:\s+\d[\d,]{2,})+[\s)\]]*$")


def _candidate_rows(body: str) -> list[dict[str, Any]]:
    """Every data row of every CANDIDATE Item 403 table, censused.

    Deliberately NOT the tables the parser selects. The shape being counted
    empties its own table, so ``_is_item403_eligible`` rejects it, the window
    never qualifies and the concatenation loop never runs — a census hooked
    into that loop reports zero on the ticket's own known accession
    (measured: ``0000351998-18-000006``, score 12, selected tables 0).
    """
    out: list[dict[str, Any]] = []
    seen_offsets: set[tuple[int, int]] = set()
    for window_start, window_end in P._find_section_windows(body):
        for start, end in P._scan_outer_tables(body, start=window_start, end=window_end):
            if (start, end) in seen_offsets:
                continue
            seen_offsets.add((start, end))
            table = P._parse_table_html(body[start:end])
            if table is None or not table.rows:
                continue
            if P._score_table_headers(table.score_headers) < P._WINDOW_SCORE_FLOOR:
                continue
            name_idx, shares_idx, percent_idx = P._resolve_columns(table.column_headers)
            for raw_row in table.rows:
                cells = P._pad_row(raw_row, name_idx=name_idx, shares_idx=shares_idx, percent_idx=percent_idx)
                name_cell = cells[name_idx] if name_idx < len(cells) else ""
                shares_cell = cells[shares_idx] if shares_idx < len(cells) else ""
                pct_cell = cells[percent_idx] if 0 <= percent_idx < len(cells) else ""
                share_segs = _segments(shares_cell)
                out.append(
                    {
                        "name_segments": _segments(name_cell),
                        "share_values": _whole_counts(share_segs),
                        "percent_values": _percents(_segments(pct_cell)),
                        # Arm 2 — one line, two amounts, no newline to split on.
                        "glued_shares": [seg for seg in share_segs if _GLUED_AMOUNTS_RE.match(seg)],
                    }
                )
    return out


def _scan(limit: int | None) -> dict[str, Any]:
    totals = Counter()
    flagged: list[dict[str, Any]] = []
    glued_rows: list[dict[str, Any]] = []

    sql = """
        SELECT accession_number, payload
        FROM filing_raw_documents
        WHERE document_kind = 'def14a_body'
        ORDER BY accession_number
        LIMIT %(limit)s
    """

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET statement_timeout = 0")
        with conn.cursor(name="def14a_stacked_census") as cur:
            cur.itersize = 20
            cur.execute(sql, {"limit": limit})
            for accession, body in cur:
                totals["accessions"] += 1
                if totals["accessions"] % 2000 == 0:
                    print(
                        f"... {totals['accessions']} accessions, {totals['accessions_flagged']} flagged",
                        flush=True,
                    )
                try:
                    captured = _candidate_rows(body)
                except Exception as exc:  # noqa: BLE001 — a census must not abort mid-corpus
                    totals["parse_errors"] += 1
                    print(f"PARSE-ERROR {accession}: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
                    continue

                totals["candidate_rows"] += len(captured)
                glued = [row for row in captured if row["glued_shares"]]
                if glued:
                    totals["rows_glued"] += len(glued)
                    totals["accessions_glued"] += 1
                    glued_rows.append({"accession": accession, "rows": glued[:5]})
                acc_flagged = []
                for row in captured:
                    n_shares = len(row["share_values"])
                    n_pct = len(row["percent_values"])
                    if n_shares < 2 and n_pct < 2:
                        continue
                    acc_flagged.append(row)
                if not acc_flagged:
                    continue
                # Only now is the full parse worth its cost — the census is a
                # scan over 42k proxies and running BOTH paths on every one of
                # them doubled a 40-minute job for a figure needed on the
                # handful that flag.
                try:
                    holders = len(P.parse_beneficial_ownership_table(body).rows)
                except Exception as exc:  # noqa: BLE001
                    totals["parse_errors"] += 1
                    print(f"PARSE-ERROR {accession}: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
                    continue
                totals["accessions_flagged"] += 1
                totals["rows_flagged"] += len(acc_flagged)
                # The extra holders the shape hides: one row carries N amounts,
                # the parser stores at most one.
                totals["hidden_holders"] += sum(
                    max(len(r["share_values"]), len(r["percent_values"])) - 1 for r in acc_flagged
                )
                if holders == 0:
                    totals["accessions_flagged_zero_holders"] += 1
                else:
                    totals["accessions_flagged_with_holders"] += 1
                flagged.append(
                    {
                        "accession": accession,
                        "stored_holders": holders,
                        "rows": acc_flagged,
                    }
                )

    return {"totals": dict(totals), "flagged": flagged, "glued": glued_rows}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", required=True)
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    summary = _scan(args.limit)
    with open(args.out, "w") as fh:
        json.dump(summary, fh, indent=1)

    print("== totals ==")
    for key, value in sorted(summary["totals"].items()):
        print(f"  {key:34s} {value:>10}")
    print(f"\n== arm 2: single-line glued amounts: {len(summary['glued'])} accessions ==")
    for entry in summary["glued"][:20]:
        print(f"  {entry['accession']}  {[r['glued_shares'] for r in entry['rows']]}")
    print(f"\n== flagged accessions: {len(summary['flagged'])} ==")
    for entry in summary["flagged"][:40]:
        print(f"  {entry['accession']}  stored_holders={entry['stored_holders']}  rows={len(entry['rows'])}")
        for row in entry["rows"][:3]:
            print(f"      shares={row['share_values']}  pct={row['percent_values']}")
            print(f"      name  ={row['name_segments'][:8]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
