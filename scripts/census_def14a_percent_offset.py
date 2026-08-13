"""Census for #2376 -- Item 403 rows whose percent is lost to a mis-columned grid.

Sizes the population the ticket deliberately left unmeasured, using the three
clauses it prescribes: the table's header must CARRY a percent caption, the row
must extract no percent from ``percent_idx``, and some cell to the RIGHT of
``percent_idx`` must parse as a percent. The third clause is what separates a
lost percent from an issuer who genuinely omits the column -- Rule 13d-3
sole/shared-power tables carry no percent at all (``_ITEM403_AMOUNT_NATURE_RE``),
and a census without it reports those as defects.

Only tables that CONTRIBUTE holders are counted. ``_is_item403_eligible`` also
calls ``_extract_table_holders``, to judge a table in isolation; it passes no
``rows`` handle, which is the discriminator used here.

Also records the grid diagnostic behind the defect: per contributing table, the
number of self-closing ``<td/>`` tags (which ``_CELL_RE`` does not recognise, so
they neither emit a cell nor pair their attributes correctly) and the header /
data row widths that result.

Emits one JSON object per accession on stdout. Redirect to a FILE and read the
file -- piping buffers the progress lines and returns the pipe's exit status.
"""

from __future__ import annotations

import json
import re
import sys

import psycopg

import app.providers.implementations.sec_def14a as P
from app.config import settings

_SELF_CLOSING_CELL_RE = re.compile(r"<t[dh][^>]*/>", re.IGNORECASE)
_PERCENT_CAPTION_RE = re.compile(r"%|percent", re.IGNORECASE)


def _cell_is_percent(text: str) -> bool:
    """True when TEXT parses as a percent value at all (not the strict test).

    Deliberately looser than ``_extract_holder_rows``' unambiguous-percent rule:
    the question here is "was a percent SITTING there", not "would the existing
    rescue have been allowed to take it". A bare ``14.33`` must count, because
    that is precisely the value the strict rule declines and the ticket is about.
    """
    stripped = text.strip()
    if not stripped:
        return False
    return P._parse_percent(stripped) is not None


def _row_for_holder(holder_name: str, rows: tuple[tuple[str, ...], ...]) -> tuple[str, ...] | None:
    """The raw row a HOLDER came from, matched on its cleaned name cell.

    A positional re-derivation of "would the rescue have taken it" was tried
    first and abandoned: it has to guess ``shares_src_idx``, and on the ticket's
    own 403(b) table the share count is recovered from a different column, so the
    guess skipped the very cell holding ``2.21`` and reported a row the parser
    actually stores correctly. Matching the extracted OUTCOME back to its row
    avoids re-implementing the extractor's recovery rules.

    Matching is on the first 12 characters of the cleaned cell, which is what
    survives ``_clean_beneficial_holder_name``'s footnote and address handling.
    """
    key = holder_name.strip().lower()[:12]
    if not key:
        return None
    for row in rows:
        for cell in row:
            if P._clean_beneficial_holder_name(cell).strip().lower()[:12] == key:
                return row
    return None


def main() -> int:
    original_parse = P._parse_table_html
    original_extract = P._extract_table_holders
    grid: dict[int, dict] = {}
    contributing: list[dict] = []

    def traced_parse(table_html: str, **kwargs):
        table = original_parse(table_html, **kwargs)
        if table is not None:
            grid[id(table)] = {
                "self_closing": len(_SELF_CLOSING_CELL_RE.findall(table_html)),
                "header_width": len(table.column_headers),
                "row_widths": sorted({len(r) for r in table.rows}),
            }
        return table

    def traced_extract(table, *, rows=None, seen=None, drop_non_owner_rows=True):
        before = len(rows) if rows is not None else 0
        result = original_extract(table, rows=rows, seen=seen, drop_non_owner_rows=drop_non_owner_rows)
        if rows is None:  # eligibility probe, not a contributing extraction
            return result
        appended = list(rows[before:])
        try:
            name_idx, shares_idx, percent_idx = P._resolve_columns(table.column_headers)
        except Exception:  # noqa: BLE001
            return result
        headers = table.column_headers
        caption = headers[percent_idx] if 0 <= percent_idx < len(headers) else ""
        header_has_percent_caption = bool(_PERCENT_CAPTION_RE.search(caption))
        # A holder is LOST when the table's header promises a percent, the
        # extractor stored none, and the holder's own row carries a percent to
        # the RIGHT of the resolved percent column. The third clause is the
        # ticket's -- without it an issuer who genuinely omits the column (Rule
        # 13d-3 sole/shared-power tables) reads as a defect.
        lost = 0
        lost_examples: list[list[str]] = []
        null_percent = 0
        for holder in appended:
            if holder.percent_of_class is not None:
                continue
            null_percent += 1
            if not header_has_percent_caption or percent_idx < 0:
                continue
            raw_row = _row_for_holder(holder.holder_name, table.rows)
            if raw_row is None:
                continue
            cells = P._pad_row(raw_row, name_idx=name_idx, shares_idx=shares_idx, percent_idx=percent_idx)
            if any(_cell_is_percent(c) for c in cells[percent_idx + 1 :]):
                lost += 1
                if len(lost_examples) < 3:
                    lost_examples.append([holder.holder_name, *list(raw_row)[:12]])
        contributing.append(
            {
                "headers": list(headers)[:12],
                "idx": [name_idx, shares_idx, percent_idx],
                "header_has_percent_caption": header_has_percent_caption,
                "appended": len(appended),
                "null_percent": null_percent,
                "lost": lost,
                "examples": lost_examples,
                "grid": grid.get(id(table), {}),
            }
        )
        return result

    P._parse_table_html = traced_parse
    P._extract_table_holders = traced_extract

    # Optional accession filter, for smoke-testing the classifier on a known
    # case before committing to the full-corpus pass.
    only = sys.argv[1:]
    sql = """
        SELECT accession_number, payload
        FROM filing_raw_documents
        WHERE document_kind = 'def14a_body'
          AND (%(only)s::text[] IS NULL OR accession_number = ANY(%(only)s::text[]))
        ORDER BY accession_number
    """
    seen_count = 0
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET statement_timeout = 0")
        with conn.cursor(name="def14a_pct_offset") as cur:
            cur.itersize = 20
            cur.execute(sql, {"only": only or None})
            for accession, payload in cur:
                contributing.clear()
                grid.clear()
                try:
                    result = P.parse_beneficial_ownership_table(payload)
                except Exception as exc:  # noqa: BLE001
                    print(json.dumps({"accession": accession, "error": repr(exc)[:200]}), flush=True)
                    continue
                print(
                    json.dumps(
                        {
                            "accession": accession,
                            "final_rows": len(result.rows),
                            "final_null_percent": sum(1 for h in result.rows if h.percent_of_class is None),
                            "tables": contributing,
                        }
                    ),
                    flush=True,
                )
                seen_count += 1
                if seen_count % 500 == 0:
                    print(f"... {seen_count}", file=sys.stderr, flush=True)
    print(f"DONE {seen_count}", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
