"""Full-population A/B for the DEF 14A Item 402(c) SCT parser (#2350).

Dumps EVERY stored ``def14a_body``'s full SCT output (all ten fields plus the
selected table's score), then diffs two dumps with each field change classified
by direction. ``scripts/ab_2140_def14a_parser.py`` fingerprints only name /
position / year / total, which cannot see a salary or middle-column move.

Offline -- reads ``filing_raw_documents``, never fetches. Run the dump from each
checkout root (control = a detached ``origin/main`` worktree):

    PYTHONPATH=. uv run python scripts/ab_2350_sct_full_rows.py --out /tmp/sct-main.json
    PYTHONPATH=. uv run python scripts/ab_2350_sct_full_rows.py --out /tmp/sct-branch.json
    PYTHONPATH=. uv run python scripts/ab_2350_sct_full_rows.py --diff /tmp/sct-main.json /tmp/sct-branch.json
"""

from __future__ import annotations

import argparse
import collections
import json
import sys
from collections.abc import Iterator
from multiprocessing import Pool
from typing import Any

import psycopg

import app.providers.implementations.sec_def14a as parser_mod
from app.config import settings

_FIELDS = (
    "salary",
    "bonus",
    "stock_awards",
    "option_awards",
    "non_equity_incentive",
    "pension_nqdc",
    "other_comp",
    "total_comp",
)


def _parse(item: tuple[str, str]) -> tuple[str, dict[str, Any]]:
    accession, body = item
    try:
        table = parser_mod.parse_summary_compensation_table(body)
    except Exception as exc:  # noqa: BLE001 -- A/B must not abort mid-corpus
        return accession, {"error": f"{type(exc).__name__}: {exc}"}
    rows = [
        [r.executive_name, r.principal_position, r.fiscal_year]
        + [None if (v := getattr(r, f)) is None else str(v) for f in _FIELDS]
        for r in table.rows
    ]
    return accession, {"score": table.raw_table_score, "rows": rows}


def _stream() -> Iterator[tuple[str, str]]:
    # One connection, one named cursor: the dev cluster has no connection headroom.
    with psycopg.connect(settings.database_url) as conn, conn.cursor(name="ab2350") as cur:
        cur.itersize = 50
        cur.execute(
            "SELECT accession_number, payload FROM filing_raw_documents "
            "WHERE document_kind = 'def14a_body' ORDER BY accession_number"
        )
        yield from cur


def _dump(path: str) -> int:
    out: dict[str, Any] = {}
    with Pool(6) as pool:
        for accession, result in pool.imap(_parse, _stream(), chunksize=8):
            out[accession] = result
    with open(path, "w") as f:
        json.dump(out, f)
    print(f"accessions: {len(out)}  rows: {sum(len(v.get('rows', [])) for v in out.values())}")
    return 0


def _diff(before_path: str, after_path: str) -> int:
    with open(before_path) as f:
        before = json.load(f)
    with open(after_path) as f:
        after = json.load(f)
    names = ("principal_position", *_FIELDS)
    moves: collections.Counter[tuple[str, str]] = collections.Counter()
    where: dict[tuple[str, str], set[str]] = collections.defaultdict(set)
    changed, lost, gained = [], [], []
    for accession, b in before.items():
        a = after.get(accession)
        if a == b:
            continue
        changed.append(accession)
        if a is None or "error" in a or "error" in b:
            continue
        kb = {(r[0], r[2]): r for r in b["rows"]}
        ka = {(r[0], r[2]): r for r in a["rows"]}
        if kb.keys() - ka.keys():
            lost.append(accession)
        if ka.keys() - kb.keys():
            gained.append(accession)
        for key in kb.keys() & ka.keys():
            for i, name in zip((1, *range(3, 11)), names, strict=True):
                x, y = kb[key][i], ka[key][i]
                if x != y:
                    kind = "null->value" if x is None else "value->null" if y is None else "value->value"
                    moves[(name, kind)] += 1
                    where[(name, kind)].add(accession)
    print(f"accessions: {len(before)}  with changed SCT output: {len(changed)}")
    print(f"  (executive_name, fiscal_year) lost in: {len(lost)}  gained in: {len(gained)}")
    for (name, kind), n in sorted(moves.items()):
        print(f"  {name:22} {kind:13} rows {n:4}  accessions {sorted(where[(name, kind)])}")
    print("changed:", " ".join(changed))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", help="dump this checkout's SCT output to this path")
    p.add_argument("--diff", nargs=2, metavar=("BEFORE", "AFTER"))
    args = p.parse_args(argv if argv is not None else sys.argv[1:])
    if args.diff:
        return _diff(*args.diff)
    if args.out:
        return _dump(args.out)
    p.error("pass --out or --diff")
    return 2


if __name__ == "__main__":
    sys.exit(main())
