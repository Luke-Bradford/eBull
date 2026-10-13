"""#3739 slice 3c: the two measurements behind Amendment 1 (spec §3.2 evidence horizon, §3.4 fail branch).

Spec: ``docs/research/2026-10-09-3739-stage-c-panel.md`` §"Amendment 1". Reads only the replica-2022 inputs pinned by
slice 3a and the comparison set pinned by slice 3b; no price, no document, no 2020-replica input.

``gaps`` prints the distribution of days between the distinct acceptance dates of consecutive covers of each (CIK,
class) series in the notes extract: descriptive context for the 203-day evidence horizon, which deadline arithmetic
sets.

``coverage`` prints, for every comparison stamp, the split candidates on its CIK whose interval covers the stamp
date, with covers and ratio facts read through ``--evidence-through`` (the event window and Item 5.03 stay as slice
3a's).

    R=docs/research/3739-event-file/replica-2022
    PYTHONPATH=. uv run python scripts/build_3739_slice3c.py gaps \\
        --notes $R/inputs/fsnds-notes-2022q1-2024q3-extract.jsonl.gz
    PYTHONPATH=. uv run python scripts/build_3739_slice3c.py coverage --evidence-through 2024-09-30 \\
        --notes $R/inputs/fsnds-notes-2022q1-2024q3-extract.jsonl.gz \\
        --extract $R/inputs/submissions-2026-10-09-extract-from-2022-04-01.jsonl.gz \\
        --comparison $R/comparison-intrader-stamps.csv
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from collections.abc import Sequence
from datetime import date
from itertools import pairwise
from pathlib import Path

from scripts.build_3739_slice2 import read_extract, sha256_of
from scripts.build_3739_slice2b import (
    SplitCandidate,
    cover_candidates,
    covers,
    item_503_candidates,
    ny_date,
    read_notes,
    xbrl_candidates,
)
from scripts.build_3739_slice3a import REPLICAS

#: Amendment 1 runs on the 2022 replica only: the 2020 replica's inputs are not read before the revision is frozen.
REPLICA = REPLICAS["2022"]
#: The notes start 2022q1: gaps before a series' first cover in the extract are not observed (left truncation).
NOTES_FROM = date(2022, 1, 1)


def percentile(ordered: Sequence[int], q: float) -> int:
    """The lower order statistic at index ⌊q·(n−1)⌋ of the sorted values."""
    return ordered[int(q * (len(ordered) - 1))]


def gaps(notes: Path) -> None:
    ledger: Counter[str] = Counter()
    series = covers(read_notes(notes), date.max, ledger, NOTES_FROM)
    days: list[int] = []
    for ordered in series.values():
        accepted = sorted({ny_date(c.accepted) for c in ordered})
        days += [(later - earlier).days for earlier, later in pairwise(accepted)]
    days.sort()
    if not days:
        raise SystemExit(f"{notes}: no series has two covers accepted on distinct dates, so there is no gap to report")
    print(
        json.dumps(
            {
                "notes_sha256": sha256_of(notes),
                "percentile": "lower order statistic, index floor(q * (n - 1))",
                "series": len(series),
                "gaps": len(days),
                "percentiles": {str(q): percentile(days, q) for q in (0.5, 0.9, 0.99, 0.999)},
                "over_200": sum(d > 200 for d in days),
                "over_203": sum(d > 203 for d in days),
                "max": days[-1],
            },
            indent=1,
        )
    )


def candidates(notes: Path, extract: Path, evidence_through: date) -> list[SplitCandidate]:
    facts = read_notes(notes)
    start, end = REPLICA.event_start, REPLICA.through
    # Slice 3a's event window: a candidate counts if its interval meets [start, end]; the later evidence cutoff only
    # lets covers and ratio facts accepted after end close an interval that starts inside the window.
    in_window = [
        c
        for c in (
            *xbrl_candidates(facts, evidence_through, start),
            *cover_candidates(covers(facts, evidence_through, Counter(), start), evidence_through, start),
        )
        if c.start <= end
    ]
    return in_window + item_503_candidates(read_extract(extract), end, start, REPLICA.extract_from)


def coverage(notes: Path, extract: Path, comparison: Path, evidence_through: date) -> None:
    by_cik: dict[str, list[SplitCandidate]] = defaultdict(list)
    for candidate in candidates(notes, extract, evidence_through):
        by_cik[candidate.cik].append(candidate)
    with comparison.open(newline="") as handle:
        stamps = [row for row in csv.DictReader(handle) if row["in_u"] == "1"]
    rows = []
    for stamp in stamps:
        day = date.fromisoformat(stamp["bar_date"])
        covering = [c for c in by_cik[stamp["cik"]] if c.start <= day <= c.end]
        rows.append(
            {
                "symbol": stamp["vendor_symbol"],
                "date": stamp["bar_date"],
                "factor": stamp["split_factor"],
                "candidates_on_cik": len(by_cik[stamp["cik"]]),
                "covering": [f"{c.screen} {c.start}..{c.end} ratio={c.ratio}" for c in covering],
            }
        )
    uncovered = [r for r in rows if not r["covering"]]
    print(
        json.dumps(
            {
                "inputs_sha256": {p.name: sha256_of(p) for p in (notes, extract, comparison)},
                "evidence_through": evidence_through.isoformat(),
                "coverage": "CIK-level: a candidate on the stamp's CIK whose interval covers the stamp date",
                "stamps": len(rows),
                "covered": len(rows) - len(uncovered),
                "no_candidate_on_cik": [r["symbol"] for r in rows if not r["candidates_on_cik"]],
                "uncovered": [r["symbol"] for r in uncovered],
                "rows": rows,
            },
            indent=1,
        )
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    gap = sub.add_parser("gaps")
    gap.add_argument("--notes", type=Path, required=True)
    cov = sub.add_parser("coverage")
    cov.add_argument("--notes", type=Path, required=True)
    cov.add_argument("--extract", type=Path, required=True)
    cov.add_argument("--comparison", type=Path, required=True)
    cov.add_argument("--evidence-through", type=date.fromisoformat, required=True)
    args = parser.parse_args(argv)
    if args.command == "gaps":
        gaps(args.notes)
    else:
        coverage(args.notes, args.extract, args.comparison, args.evidence_through)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
