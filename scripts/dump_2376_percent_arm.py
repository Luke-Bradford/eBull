"""Dump one A/B arm's per-holder (shares, percent) for #2376's gain-side check.

``scripts/verify_2376_recovered_percents.py`` corroborates every percent that
moved NULL -> value, and needs the VALUES either side. The A/B summary
(``scripts/ab_2140_def14a_parser.py --out``) carries holder identities but not
their numbers, so it cannot feed that arm; this writes the missing file.

Run the SAME file against both checkouts -- ``PYTHONPATH=.`` resolves ``app`` out
of the working directory, so the harness is byte-identical across the two arms
and only the parser under test differs. Never re-implement it per arm.

    cd <control-worktree> && PYTHONPATH=. uv run python \\
        <branch>/scripts/dump_2376_percent_arm.py > /tmp/2376-control.jsonl
    PYTHONPATH=. uv run python scripts/dump_2376_percent_arm.py > /tmp/2376-treatment.jsonl

Holders are keyed ``lower(trim(holder_name))`` -- the database's own
``holder_name_key`` (sql/116:110) -- so a name matched here is the same row the
ownership rollup would match. Redirect to a FILE and read the file: piping
buffers the progress lines and returns the pipe's exit status.
"""

from __future__ import annotations

import json
import sys

import psycopg

import app.providers.implementations.sec_def14a as parser_mod
from app.config import settings

_SQL = """
    SELECT accession_number, payload
    FROM filing_raw_documents
    WHERE document_kind = 'def14a_body'
    ORDER BY accession_number
"""


def main() -> int:
    seen = 0
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET statement_timeout = 0")
        with conn.cursor(name="def14a_2376_dump") as cur:
            cur.itersize = 20
            cur.execute(_SQL)
            for accession, body in cur:
                try:
                    parsed = parser_mod.parse_beneficial_ownership_table(body)
                except Exception as exc:  # noqa: BLE001 — one bad body must not abort the arm
                    print(f"PARSE-ERROR {accession}: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
                    continue
                holders = {
                    holder.holder_name.strip().lower(): {
                        "shares": None if holder.shares is None else str(holder.shares),
                        "percent": None if holder.percent_of_class is None else str(holder.percent_of_class),
                    }
                    for holder in parsed.rows
                }
                print(json.dumps({"acc": accession, "holders": holders}), flush=True)
                seen += 1
                if seen % 2000 == 0:
                    print(f"... {seen}", file=sys.stderr, flush=True)
    print(f"DONE {seen}", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
