"""Census pass 3 -- dump EVERY candidate table in EVERY window, once.

Why a third pass. Pass 2 measured the D2/D4 gate only on the 1,668 score-3-5
NON-winning tables that D3 newly ADMITS. It never re-measured the tables that
already WIN, and that is exactly where the D4 over-rejection lives: issuers
caption Item 403 column 4 as a bare ``Percent`` or ``%`` and leave the class
implied by the neighbouring amount column. The 1,668 -> 45 figure is sound for
what it measured and silent on the regression (#2160 issue comment 5125091295).

`.claude/skills/engineering/full-population-ab.md`: a narrowing change must
enumerate the narrowing side. D2/D4 gate WINNER selection, so the narrowing side
is "tables that win today and would not win under the gate".

Design: dump the SUBSTRATE, not a verdict. The enumeration path
(``_find_section_windows`` -> ``_scan_outer_tables`` -> ``_parse_table_html`` ->
``_resolve_columns`` -> ``_extract_holder_rows``) is byte-identical on ``main``
and on this branch -- only the selector changed -- so one corpus pass supports
offline simulation of ANY gate variant in BOTH directions, with no refetch and
no re-parse per variant. ``analyse_def14a_window_tables.py`` consumes it.

Unlike the real parser this does NOT stop at the first qualifying window: the
gate can empty a window that wins today, in which case selection falls through
to the next one, so every window's tables have to be on record.

Run against the dev DB from any checkout:

    PYTHONPATH=. uv run python scripts/census_def14a_window_tables.py > dump.jsonl
"""

from __future__ import annotations

import json
import sys

import psycopg

import app.providers.implementations.sec_def14a as parser_mod
from app.config import settings

_MAX_WINDOWS = 4
_MAX_TABLES_PER_WINDOW = 80
_MAX_NAMES = 40
_MAX_HEADERS = 12
_NAME_CLIP = 160


def _table_record(html_text: str, start: int, end: int) -> dict | None:
    parsed = parser_mod._parse_table_html(html_text[start:end])
    if parsed is None or not parsed.rows:
        # Mirrors the parser: a table with no DATA rows cannot be the Item 403
        # table and is skipped before scoring (#2158 element 4).
        return None
    score = parser_mod._score_table_headers(parsed.score_headers)
    names: list[str] = []
    try:
        name_idx, shares_idx, percent_idx = parser_mod._resolve_columns(parsed.column_headers)
        holders: list = []
        parser_mod._extract_holder_rows(
            parsed,
            name_idx=name_idx,
            shares_idx=shares_idx,
            percent_idx=percent_idx,
            rows=holders,
            seen=set(),
        )
        names = [h.holder_name[:_NAME_CLIP] for h in holders]
    except Exception:  # noqa: BLE001 -- a table we cannot extract scores 0 identity
        holders = []
    return {
        "s": score,
        "sh": [h[:_NAME_CLIP] for h in parsed.score_headers[:_MAX_HEADERS]],
        "ch": [h[:_NAME_CLIP] for h in parsed.column_headers[:_MAX_HEADERS]],
        "nr": len(parsed.rows),
        "n": len(names),
        "nm": names[:_MAX_NAMES],
    }


def main() -> int:
    sql = """
        SELECT accession_number, payload
        FROM filing_raw_documents
        WHERE document_kind = 'def14a_body'
        ORDER BY accession_number
    """
    n = 0
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET statement_timeout = 0")
        with conn.cursor(name="def14a_census3") as cur:
            cur.itersize = 20
            cur.execute(sql)
            for accession, payload in cur:
                n += 1
                if not payload:
                    print(json.dumps({"accession": accession, "windows": []}), flush=True)
                    continue
                try:
                    windows = parser_mod._find_section_windows(payload)
                    out: list[list[dict]] = []
                    for window_start, window_end in windows[:_MAX_WINDOWS]:
                        spans = parser_mod._scan_outer_tables(payload, start=window_start, end=window_end)
                        tables = []
                        for start, end in spans[:_MAX_TABLES_PER_WINDOW]:
                            rec = _table_record(payload, start, end)
                            if rec is not None:
                                tables.append(rec)
                        out.append(tables)
                except Exception as exc:  # noqa: BLE001
                    print(json.dumps({"accession": accession, "error": repr(exc)[:200]}), flush=True)
                    continue
                print(json.dumps({"accession": accession, "windows": out}), flush=True)
                if n % 250 == 0:
                    print(f"... {n}", file=sys.stderr, flush=True)
    print(f"DONE {n}", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
