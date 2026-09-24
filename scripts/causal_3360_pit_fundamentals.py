"""#3360 acceptance item 2: full-population causal reference for the PIT fundamentals bundle.

For every sharded CIK and every distinct acceptance NY date ``d`` over its events,
blocking rejections and accession index (plus one date before the first), the answer the
bundle gives at ``D = d + 1`` must equal the answer rebuilt from ONLY the inputs public
before ``D`` (spec ``docs/proposals/ta/2026-09-24-3360-companyfacts-pit-bundle.md``).

Independent path. The reference re-reads the snapshotted raw archives, admits each raw row
once, keeps only rows and accessions whose acceptance NY date is ``< D`` and recomputes
rule 8 (value) and rule 9 (prefix) from that set with its own grouping -- it never calls the
bundle's reader or reads a shard. Admitting once is exact, not an approximation: a row's
outcome is a function of the row and its OWN accession's submissions record, and filtering
the inputs to ``< D`` removes a row together with its accession, so every row that
survives the filter gets the same outcome it gets in the full build.

Usage::

    PYTHONPATH=. uv run python scripts/causal_3360_pit_fundamentals.py \\
        --bundle <bundle dir> --manifest-sha256 <sha> > /tmp/causal3360.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing
import sys
import zipfile
from bisect import bisect_left
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from app.services import pit_fundamentals as pf

_MISMATCH_EXAMPLES = 20


@dataclass
class Tally:
    counts: Counter[str] = field(default_factory=Counter)
    mismatches: list[str] = field(default_factory=list)

    def check(self, kind: str, expected: object, actual: object, where: str) -> None:
        self.counts[f"{kind}_compared"] += 1
        if expected != actual:
            self.counts[f"{kind}_mismatch"] += 1
            if len(self.mismatches) < _MISMATCH_EXAMPLES:
                self.mismatches.append(f"{where}: expected {expected!r} actual {actual!r}")

    def merge(self, other: Tally) -> None:
        self.counts.update(other.counts)
        self.mismatches.extend(other.mismatches[: _MISMATCH_EXAMPLES - len(self.mismatches)])


def _expected_value(rows: list[tuple[Any, ...]], ny: list[date], decision: date) -> tuple[Any, ...]:
    """Rule 8 over the rows public before ``decision``; rows sorted by acceptance."""
    public = rows[: bisect_left(ny, decision)]
    if not public:
        return (pf.ReadStatus.ABSENT, (), (), None)
    latest = public[-1][0]
    at_latest = [row for row in public if row[0] == latest]
    if any(row[1] == "block" for row in at_latest):
        return (pf.ReadStatus.BLOCKED_BY_REJECTION, (), (), latest)
    values = tuple(sorted({row[3] for row in at_latest}))
    accns = tuple(sorted({row[2] for row in at_latest}))
    status = pf.ReadStatus.VALUE if len(values) == 1 else pf.ReadStatus.AMBIGUOUS
    return (status, values, accns, latest)


def verify_cik(
    bundle: pf.PitFundamentalsBundle,
    cik10: str,
    payload: Mapping[str, Any],
    submissions_main: object,
    read_page: Callable[[str], object | None],
) -> Tally:
    tally = Tally()
    index = pf.parse_submissions(cik10, submissions_main, read_page)
    if isinstance(index, str):
        tally.check("integrity", "passes", index, cik10)
        return tally

    # key -> Counter[(acceptance, accn, value)] ; key -> {(acceptance, accn, reason): [row indices]}
    events: dict[tuple[Any, ...], Counter[tuple[str, str, str]]] = defaultdict(Counter)
    blocks: dict[tuple[Any, ...], dict[tuple[str, str, str], list[int]]] = defaultdict(dict)
    for taxonomy, concept, unit, row_index, row in pf.iter_raw_rows(payload):
        outcome, record = pf._admit(taxonomy, concept, unit, row, index)  # pyright: ignore[reportPrivateUsage]
        if outcome is pf.Outcome.STORED:
            events[record[:5]][(record[6], record[5], record[7])] += 1
        elif outcome in pf.BLOCKING:
            blocks[record[:5]].setdefault((record[6], record[5], outcome.value), []).append(row_index)
    accessions = sorted(
        (f.acceptance, f.accn, f.form)
        for f in index.filings.values()
        if f.form in pf.ADMITTED_FORMS and f.acceptance is not None
    )

    ny_of: dict[str, date] = {}

    def ny(acceptance: str) -> date:
        if acceptance not in ny_of:
            ny_of[acceptance] = pf.acceptance_ny_date(acceptance)
        return ny_of[acceptance]

    per_key: dict[tuple[Any, ...], tuple[list[tuple[Any, ...]], list[date]]] = {}
    for key in set(events) | set(blocks):
        rows = [(acc, "event", accn, value) for (acc, accn, value) in events.get(key, ())]
        rows += [(acc, "block", accn, reason) for (acc, accn, reason) in blocks.get(key, {})]
        rows.sort()
        per_key[key] = (rows, [ny(row[0]) for row in rows])

    dates = {ny(acc) for acc, *_ in accessions}
    dates |= {ny(row[0]) for rows, _ in per_key.values() for row in rows}
    if not dates:
        tally.counts["ciks_without_dates"] += 1
        return tally
    decisions = sorted({d + timedelta(days=1) for d in dates} | {min(dates)})
    for decision in decisions:
        if decision > bundle.supported_through:
            tally.counts["decisions_after_capture_skipped"] += 1
            continue
        tally.counts["decisions"] += 1
        for key, (rows, ny_list) in per_key.items():
            read = bundle.value_as_of(cik10, pf.FactKey(*key), decision)
            actual = (read.status, read.values, read.accns, read.acceptance)
            tally.check("value", _expected_value(rows, ny_list, decision), actual, f"{cik10} {key} D={decision}")
        public_accessions = sorted((a, n, f) for a, n, f in accessions if ny(a) < decision)
        for taxonomy, concept in pf.CONCEPT_SET:
            # Counters, not sorted lists: a ``None`` start does not order against a date string.
            expected_events = Counter(
                (*key, accn, acc, value, n)
                for key, counter in events.items()
                if key[:2] == (taxonomy, concept)
                for (acc, accn, value), n in counter.items()
                if ny(acc) < decision
            )
            expected_blocks = Counter(
                (*key, accn, acc, reason, tuple(sorted(rows)))
                for key, found in blocks.items()
                if key[:2] == (taxonomy, concept)
                for (acc, accn, reason), rows in found.items()
                if ny(acc) < decision
            )
            prefix = bundle.public_events(cik10, taxonomy, concept, decision)
            actual_events = Counter(
                (
                    e["taxonomy"],
                    e["concept"],
                    e["unit"],
                    e["start"],
                    e["end"],
                    e["accn"],
                    e["acceptance"],
                    e["value"],
                    e["multiplicity"],
                )
                for e in prefix.events
            )
            actual_blocks = Counter(
                (
                    r["taxonomy"],
                    r["concept"],
                    r["unit"],
                    r["start"],
                    r["end"],
                    r["accn"],
                    r["acceptance"],
                    r["reason"],
                    tuple(r["rows"]),
                )
                for r in prefix.rejections
            )
            actual_accessions = sorted((a["acceptance"], a["accn"], a["form"]) for a in prefix.accessions)
            where = f"{cik10} {taxonomy}/{concept} D={decision}"
            tally.check(
                "prefix",
                (pf.ReadStatus.OK, expected_events, expected_blocks, public_accessions),
                (prefix.status, actual_events, actual_blocks, actual_accessions),
                where,
            )
    return tally


# ------------------------------------------------------------------ process pool

_STATE: dict[str, Any] = {}


def _init(bundle_root: str, manifest_sha: str) -> None:
    root = Path(bundle_root)
    _STATE["bundle"] = pf.load_pit_fundamentals(root, expected_manifest_sha256=manifest_sha)
    _STATE["facts"] = zipfile.ZipFile(root / "inputs" / "companyfacts.zip")
    _STATE["subs"] = subs = zipfile.ZipFile(root / "inputs" / "submissions.zip")
    _STATE["sub_names"] = set(subs.namelist())


def _verify(cik10: str) -> Tally:
    facts: zipfile.ZipFile = _STATE["facts"]
    subs: zipfile.ZipFile = _STATE["subs"]
    names: set[str] = _STATE["sub_names"]
    payload = json.loads(facts.read(f"CIK{cik10}.json"), parse_float=Decimal, parse_int=Decimal)

    def read_page(name: str) -> object | None:
        return json.loads(subs.read(name)) if name in names else None

    tally = verify_cik(_STATE["bundle"], cik10, payload, json.loads(subs.read(f"CIK{cik10}.json")), read_page)
    tally.counts["ciks"] += 1
    return tally


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--processes", type=int, default=max(1, (multiprocessing.cpu_count() or 2) - 2))
    args = parser.parse_args()
    bundle = pf.load_pit_fundamentals(args.bundle, expected_manifest_sha256=args.manifest_sha256)
    # The reference re-reads the retained raw inputs, so they must be the bytes the manifest
    # pinned -- a replaced archive would otherwise be "verified" against shards it never made.
    manifest = json.loads((args.bundle / pf.MANIFEST_FILENAME).read_bytes())
    for name, expected in sorted(manifest["input_sha256"].items()):
        with (args.bundle / "inputs" / f"{name}.zip").open("rb") as handle:
            measured = hashlib.file_digest(handle, "sha256").hexdigest()
        if measured != expected:
            raise SystemExit(f"input {name}.zip digest {measured} != manifest {expected}")
    total = Tally()
    with multiprocessing.get_context("spawn").Pool(
        args.processes, initializer=_init, initargs=(str(args.bundle), args.manifest_sha256)
    ) as pool:
        for tally in pool.imap_unordered(_verify, bundle.ciks, chunksize=16):
            total.merge(tally)
    with Path(__file__).open("rb") as handle:
        script_sha = hashlib.file_digest(handle, "sha256").hexdigest()
    json.dump(
        {
            "manifest_sha256": args.manifest_sha256,
            "policy": pf.policy_sha256(),
            "script_sha256": script_sha,
            "supported_through": bundle.supported_through.isoformat(),
            "counts": dict(sorted(total.counts.items())),
            "mismatch_examples": total.mismatches,
        },
        sys.stdout,
        indent=1,
    )
    mismatched = sum(n for kind, n in total.counts.items() if kind.endswith("_mismatch"))
    return 1 if mismatched else 0


if __name__ == "__main__":
    raise SystemExit(main())
