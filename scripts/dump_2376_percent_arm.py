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

``--diff`` then reports the A/B proper off the same two files:

    PYTHONPATH=. uv run python scripts/dump_2376_percent_arm.py --diff \\
        /tmp/2376-control.jsonl /tmp/2376-treatment.jsonl

The headline metric is **distinct holders lost / gained**, never a row count --
#2140 twice found a row-count drop to be the OLD code losing garbage, and chasing
the count would have made the parser worse (``ab_2140_def14a_parser.py``'s module
docstring). Gains are enumerated for the same reason: #2140's last real defect
appeared only on the gain side.

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


def _load(path: str) -> dict[str, dict[str, dict[str, str | None]]]:
    out: dict[str, dict[str, dict[str, str | None]]] = {}
    with open(path) as handle:
        for line in handle:
            entry = json.loads(line)
            out[entry["acc"]] = entry["holders"]
    return out


def _diff(control_path: str, treatment_path: str) -> int:
    control, treatment = _load(control_path), _load(treatment_path)
    shared = control.keys() & treatment.keys()
    print("== corpus ==")
    print(f"  accessions, control                 {len(control):>8}")
    print(f"  accessions, treatment               {len(treatment):>8}")
    # An accession in one arm only means the daemon wrote between the two runs.
    # It is not a parser difference and must not be counted as one.
    print(f"  accessions in BOTH (the A/B set)    {len(shared):>8}")

    lost: list[tuple[str, str]] = []
    gained: list[tuple[str, str]] = []
    null_to_value: list[tuple[str, str, str]] = []
    value_to_null: list[tuple[str, str, str]] = []
    changed: list[tuple[str, str, str, str]] = []

    for accession in sorted(shared):
        before, after = control[accession], treatment[accession]
        lost += [(accession, name) for name in before.keys() - after.keys()]
        gained += [(accession, name) for name in after.keys() - before.keys()]
        for name in before.keys() & after.keys():
            was, now = before[name]["percent"], after[name]["percent"]
            if was == now:
                continue
            if was is None:
                null_to_value.append((accession, name, str(now)))
            elif now is None:
                value_to_null.append((accession, name, str(was)))
            else:
                changed.append((accession, name, str(was), str(now)))

    print("\n== DISTINCT HOLDERS (the metric that matters -- never a row count) ==")
    print(f"  holders LOST                        {len(lost):>8}")
    print(f"  holders GAINED                      {len(gained):>8}")
    for label, rows in (("LOST", lost), ("GAINED", gained)):
        print(f"\n  -- every {label} holder, enumerated --")
        for accession, name in sorted(rows)[:100]:
            print(f"     {accession}  {name[:70]}")
        if len(rows) > 100:
            print(f"     ... and {len(rows) - 100} more (NOT truncated silently -- count above is complete)")

    print("\n== percent_of_class movement, holders present in BOTH arms ==")
    print(f"  NULL -> value  (the fix's yield)    {len(null_to_value):>8}")
    print(f"  value -> NULL  (must be 0)          {len(value_to_null):>8}")
    print(f"  value -> different value (must be 0){len(changed):>8}")
    print(f"  accessions carrying a recovery      {len({a for a, _, _ in null_to_value}):>8}")
    for label, rows in (("value -> NULL", value_to_null), ("value -> CHANGED", changed)):
        if rows:
            print(f"\n  -- {label}, every one --")
            for row in sorted(rows):
                print(f"     {row}")
    return 0


def main() -> int:
    if len(sys.argv) > 1:
        if sys.argv[1] != "--diff" or len(sys.argv) != 4:
            print(__doc__)
            return 2
        return _diff(sys.argv[2], sys.argv[3])
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
