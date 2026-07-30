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
# NO per-window table cap. It was 80, then 400, and both silently truncated the
# very table the parser selects: when ``_find_section_windows`` finds no heading
# it falls back to a WHOLE-DOCUMENT window, and a merger proxy (Kenvue
# 0001140361-25-045607) then yields thousands of spans with Item 403 far past any
# cap. The fidelity gate caught it at 400 -- 42 accessions, replay 0 rows vs real
# 41 -- which is exactly what the gate is for. Cost is parse time on a handful of
# huge documents; the DUMP stays small because only tables WITH rows are kept.
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
    n_shares = n_percent = 0
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
        # VALUE-side evidence, for the data-row column signal borrowed from
        # edgartools' ``_build_column_map`` (skill G17). Item 403 prescribes an
        # amount column AND a percent-of-class column; when the CAPTIONS have
        # degraded to empty cells the only remaining evidence that both exist is
        # that both PARSED for the rows.
        n_shares = sum(1 for h in holders if h.shares is not None)
        n_percent = sum(1 for h in holders if h.percent_of_class is not None)
    except Exception:  # noqa: BLE001 -- a table we cannot extract scores 0 identity and 0 value evidence
        holders = []
    return {
        "s": score,
        "sh": [h[:_NAME_CLIP] for h in parsed.score_headers[:_MAX_HEADERS]],
        "ch": [h[:_NAME_CLIP] for h in parsed.column_headers[:_MAX_HEADERS]],
        "nr": len(parsed.rows),
        "n": len(names),
        "nm": names[:_MAX_NAMES],
        "nsh": n_shares,
        "npc": n_percent,
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
                        for start, end in spans:
                            rec = _table_record(payload, start, end)
                            if rec is not None:
                                tables.append(rec)
                        out.append(tables)
                except Exception as exc:  # noqa: BLE001
                    print(json.dumps({"accession": accession, "error": repr(exc)[:200]}), flush=True)
                    continue
                # Ground truth for the analyser's FIDELITY GATE: what the REAL
                # parser returns for this body, same checkout, same payload.
                #
                # #2160 round 1 found 28 accessions where the dump implied "no
                # table scores >=3 with rows" while real ``main`` produced rows.
                # The replay silently disagreed with the control it was standing
                # in for, and every variant ranking rested on it. Recording the
                # truth per accession turns that into an assertion.
                try:
                    truth = len(parser_mod.parse_beneficial_ownership_table(payload).rows)
                except Exception:  # noqa: BLE001
                    truth = -1
                print(json.dumps({"accession": accession, "windows": out, "real_rows": truth}), flush=True)
                if n % 250 == 0:
                    print(f"... {n}", file=sys.stderr, flush=True)
    print(f"DONE {n}", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
