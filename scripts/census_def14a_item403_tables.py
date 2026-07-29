"""Census pass 2 -- EVERY candidate table, not only winners.

Closes the evidence gap Codex flagged at spec ckpt-1: the pass-1 proof covered
winning tables, but D3 admits every table in the winning window that clears the
row-identity floor. The signal must be proven on the population it will newly
admit.

Also fixes pass 1's measurement basis: identity is computed on the resolved NAME
column via the real extraction path (``_resolve_columns`` +
``_extract_holder_rows``), not on the raw first cell. Item 403's prescribed
first column is "Title of class"; the owner name is column 2.
"""

from __future__ import annotations

import json
import sys

import psycopg

import app.providers.implementations.sec_def14a as parser_mod
from app.config import settings


def main() -> int:
    original_parse = parser_mod._parse_table_html
    tables: list[dict] = []

    def traced_parse(table_html: str):
        t = original_parse(table_html)
        if t is None or not t.rows:
            return t
        try:
            name_idx, shares_idx, percent_idx = parser_mod._resolve_columns(t.column_headers)
        except Exception:
            return t
        holders: list = []
        seen: set[str] = set()
        try:
            parser_mod._extract_holder_rows(
                t,
                name_idx=name_idx,
                shares_idx=shares_idx,
                percent_idx=percent_idx,
                rows=holders,
                seen=seen,
            )
        except Exception:
            return t
        tables.append(
            {
                "score": parser_mod._score_table_headers(t.score_headers),
                "headers": list(t.score_headers)[:10],
                "n_raw_rows": len(t.rows),
                "n_extracted": len(holders),
                "names": [h.holder_name for h in holders[:30]],
            }
        )
        return t

    parser_mod._parse_table_html = traced_parse

    sql = """
        SELECT accession_number, payload
        FROM filing_raw_documents
        WHERE document_kind = 'def14a_body'
        ORDER BY accession_number
    """
    n = 0
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET statement_timeout = 0")
        with conn.cursor(name="def14a_census2") as cur:
            cur.itersize = 20
            cur.execute(sql)
            for accession, payload in cur:
                tables.clear()
                try:
                    result = parser_mod.parse_beneficial_ownership_table(payload)
                except Exception as exc:  # noqa: BLE001
                    print(json.dumps({"accession": accession, "error": repr(exc)[:160]}), flush=True)
                    continue
                print(
                    json.dumps(
                        {
                            "accession": accession,
                            "final_rows": len(result.rows),
                            "winning_score": result.raw_table_score,
                            "tables": tables[:60],
                        }
                    ),
                    flush=True,
                )
                n += 1
                if n % 250 == 0:
                    print(f"... {n}", file=sys.stderr, flush=True)
    print(f"DONE {n}", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
