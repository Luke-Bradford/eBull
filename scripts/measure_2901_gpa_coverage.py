"""#2901 premise measurement: GP/A input coverage on the linked Intrader population.

Spec: ``docs/proposals/ta/2026-09-24-2901-quality-arm.md`` ("Premise measurements"). Descriptive
only: it reads no price at all. Every input is a linkage or fundamentals read at the formation date.

For each June formation D (``census_3360_pit_fundamentals.formation_dates``) it takes every
in-scope Intrader series whose ``link_as_of(S, D)`` is ``linked`` with ``plain``/``class`` grammar
(so D lies inside the series' bar range), de-duplicates to CIKs, and walks each CIK down the spec's
ladder (construction rules 3-6), counting the first rung it fails. Field statuses are also counted
independently of the ladder. It stops before the security choice and executability (rules 2 and
the X(D) bar), so its eligible counts are an UPPER bound on the production E(D). The rules
themselves are ``app/services/r6_quality_universe.py``; this script only drives them.

Usage::

    PYTHONPATH=. uv run python -m scripts.measure_2901_gpa_coverage \\
        --fundamentals <#3360 bundle dir> --fundamentals-sha256 <sha> \\
        --linkage <#3361 bundle dir> --linkage-sha256 <sha> --out <new json file>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing
import sys
import zipfile
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Final

from app.services import pit_fundamentals as pf
from app.services import r6_quality_universe as quality
from app.services import security_linkage as sl
from scripts import census_3360_pit_fundamentals as census_3360

YEARS: Final = tuple(range(2012, 2025))
PRODUCTION_YEARS: Final = tuple(range(2013, 2025))
#: The premise stops before the X(D) row, so ``not_executable`` is never reached here.
RUNGS: Final = tuple(rung for rung in quality.RUNGS if rung != quality.NOT_EXECUTABLE)

_STATE: dict[str, Any] = {}


def _init(root: str, sha: str) -> None:
    bundle = pf.load_pit_fundamentals(Path(root), expected_manifest_sha256=sha)
    _STATE["bundle"] = bundle
    _STATE["subs"] = zipfile.ZipFile(Path(root) / "inputs" / "submissions.zip")


def _sic(cik10: str) -> int | None:
    subs: zipfile.ZipFile = _STATE["subs"]
    try:
        raw = json.loads(subs.read(f"CIK{cik10}.json")).get("sic")
    except KeyError:
        return None
    return quality.parse_sic(raw)


def classify(cik10: str, d: date) -> tuple[str, dict[str, str]]:
    """The first failing rung (or ``eligible``) plus field statuses counted independently."""
    result = quality.classify(_STATE["bundle"], cik10, _sic(cik10), d)
    return result.rung, result.fields


def _job(job: tuple[str, tuple[str, ...]]) -> tuple[str, dict[str, tuple[str, dict[str, str]]]]:
    cik10, days = job
    return cik10, {day: classify(cik10, date.fromisoformat(day)) for day in days}


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fundamentals", type=Path, required=True)
    parser.add_argument("--fundamentals-sha256", required=True)
    parser.add_argument("--linkage", type=Path, required=True)
    parser.add_argument("--linkage-sha256", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--processes", type=int, default=max(1, (multiprocessing.cpu_count() or 2) - 2))
    args = parser.parse_args()
    if args.out.exists():
        parser.error(f"{args.out} exists")

    formations = census_3360.formation_dates(YEARS)
    linkage = sl.load_security_linkage(args.linkage, expected_manifest_sha256=args.linkage_sha256)
    linkage_inputs = json.loads((args.linkage / "manifest.json").read_text())["input_sha256"]
    if linkage_inputs["pit_manifest"] != args.fundamentals_sha256:
        parser.error("the linkage bundle was built over a different #3360 manifest")
    link_reasons: dict[str, Counter[str]] = {}
    linked_series: dict[str, int] = {}
    series_per_cik: dict[str, Counter[int]] = {}
    work: dict[str, list[str]] = defaultdict(list)
    for d in formations:
        reasons: Counter[str] = Counter()
        per_cik: Counter[str] = Counter()
        for sid in linkage.series_ids:
            result = linkage.link_as_of(sid, d)
            if result.reason in (sl.Reason.OUTSIDE_SERIES, sl.Reason.VENDOR_OUT_OF_SCOPE):
                continue
            reasons[result.label] += 1
            if quality.is_candidate(result):
                assert result.cik is not None
                per_cik[result.cik] += 1
        link_reasons[d.isoformat()] = reasons
        linked_series[d.isoformat()] = sum(per_cik.values())
        series_per_cik[d.isoformat()] = Counter(per_cik.values())
        for cik10 in per_cik:
            work[cik10].append(d.isoformat())

    jobs = [(cik10, tuple(days)) for cik10, days in sorted(work.items())]
    rungs: dict[str, Counter[str]] = defaultdict(Counter)
    fields: dict[str, Counter[str]] = defaultdict(Counter)
    with multiprocessing.Pool(
        args.processes, initializer=_init, initargs=(str(args.fundamentals), args.fundamentals_sha256)
    ) as pool:
        for _cik10, by_day in pool.imap_unordered(_job, jobs, chunksize=32):
            for day, (rung, statuses) in by_day.items():
                if rung not in RUNGS:
                    raise RuntimeError(f"undeclared rung {rung}")
                rungs[day][rung] += 1
                for key, value in statuses.items():
                    fields[day][f"{key}={value}"] += 1
                    fields[day][f"{rung}|{key}={value}"] += 1

    here = Path(__file__)
    payload: dict[str, Any] = {
        "code_sha256": {
            path.name: _sha256(path)
            for path in (here, Path(quality.__file__), Path(pf.__file__), Path(sl.__file__), Path(census_3360.__file__))
        },
        "fundamentals_manifest_sha256": args.fundamentals_sha256,
        "linkage_manifest_sha256": args.linkage_sha256,
        "production_formations": [d.isoformat() for d in census_3360.formation_dates(PRODUCTION_YEARS)],
        "formations": {},
    }
    for d in formations:
        day = d.isoformat()
        ladder = {rung: rungs[day][rung] for rung in RUNGS}
        if sum(ladder.values()) != sum(series_per_cik[day].values()):
            raise RuntimeError(f"{day}: ladder does not reconcile to linked CIKs")
        payload["formations"][day] = {
            "link_reasons": dict(sorted(link_reasons[day].items())),
            "linked_plain_or_class_series": linked_series[day],
            "series_per_linked_cik": {str(k): v for k, v in sorted(series_per_cik[day].items())},
            "cik_rungs": ladder,
            "fields": dict(sorted(fields[day].items())),
        }
    with args.out.open("x") as handle:
        json.dump(payload, handle, indent=1, sort_keys=True)
    for day, cell in payload["formations"].items():
        print(day, sum(cell["cik_rungs"].values()), cell["cik_rungs"]["eligible"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
