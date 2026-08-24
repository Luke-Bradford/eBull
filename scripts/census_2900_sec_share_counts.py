"""Census same-accession, split-restated SEC share counts for R6 #2900.

Only filing metadata and share-count facts are read. No price value or return is
opened. The latest annual accession public by each formation close is selected
by :mod:`scripts.census_2900_sec_cover_identity`; both fiscal-year observations
must occur in that same accession so later amendments cannot rewrite history.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import zipfile
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final

from scripts.census_2900_sec_cover_identity import (
    Submission,
    _archive_member,
    _sha256,
    load_submissions,
    select_formation_submissions,
)

SHARE_TAG: Final = "CommonStockSharesOutstanding"
PARSER_VERSION: Final = "r6-sec-same-period-share-census-v2"


def _fact_date(value: str) -> date | None:
    try:
        return datetime.strptime(value.strip(), "%Y%m%d").date()
    except ValueError:
        return None


def _positive_decimal(value: str) -> Decimal | None:
    try:
        parsed = Decimal(value.strip())
    except InvalidOperation:
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


def load_share_facts(
    fsds_dir: Path,
    accessions: frozenset[str],
) -> dict[str, dict[date, frozenset[Decimal]]]:
    values: dict[str, dict[date, set[Decimal]]] = defaultdict(lambda: defaultdict(set))
    for archive in sorted(fsds_dir.glob("20??q?.zip")):
        with zipfile.ZipFile(archive) as zf:
            member = _archive_member(zf, "num.txt")
            with zf.open(member) as raw:
                reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig"), delimiter="\t")
                for row in reader:
                    accession = row.get("adsh", "").strip()
                    if accession not in accessions:
                        continue
                    if (
                        row.get("tag", "").strip() != SHARE_TAG
                        or not row.get("version", "").strip().startswith("us-gaap/")
                        or row.get("uom", "").strip().lower() != "shares"
                        or row.get("qtrs", "").strip() != "0"
                        or row.get("segments", "").strip()
                        or row.get("coreg", "").strip()
                    ):
                        continue
                    fact_date = _fact_date(row.get("ddate", ""))
                    value = _positive_decimal(row.get("value", ""))
                    if fact_date is not None and value is not None:
                        values[accession][fact_date].add(value)
    return {
        accession: {fact_date: frozenset(items) for fact_date, items in by_date.items()}
        for accession, by_date in values.items()
    }


def _share_pair(
    submission: Submission,
    facts: dict[date, frozenset[Decimal]],
) -> tuple[str, dict[str, Any] | None]:
    period = _fact_date(submission.period)
    if period is None:
        return "invalid_submission_period", None
    current = facts.get(period)
    if current is None:
        return "missing_current_fiscal_year", None
    if len(current) != 1:
        return "conflicting_current_fiscal_year", None
    prior_dates = sorted(candidate for candidate in facts if 300 <= (period - candidate).days <= 430)
    if not prior_dates:
        return "missing_prior_fiscal_year", None
    prior_date = prior_dates[-1]
    prior = facts[prior_date]
    if len(prior) != 1:
        return "conflicting_prior_fiscal_year", None
    current_value = next(iter(current))
    prior_value = next(iter(prior))
    return (
        "complete_pair",
        {
            "accession": submission.accession,
            "accepted": submission.accepted.isoformat(),
            "cik": submission.cik,
            "current_date": period.isoformat(),
            "current_shares": str(current_value),
            "prior_date": prior_date.isoformat(),
            "prior_shares": str(prior_value),
        },
    )


def share_census(
    selected: dict[datetime, tuple[Submission, ...]],
    submissions: dict[str, Submission],
    facts: dict[str, dict[date, frozenset[Decimal]]],
) -> dict[str, Any]:
    by_cik: dict[str, list[Submission]] = defaultdict(list)
    for submission in submissions.values():
        by_cik[submission.cik].append(submission)
    formations: dict[str, Any] = {}
    for cutoff, rows in selected.items():
        counts: Counter[str] = Counter(population_ciks=len(rows))
        complete: list[dict[str, Any]] = []
        for row in rows:
            candidates = sorted(
                (
                    candidate
                    for candidate in by_cik[row.cik]
                    if candidate.accepted <= cutoff and candidate.period == row.period
                ),
                key=lambda candidate: (candidate.accepted, candidate.accession),
                reverse=True,
            )
            if not candidates or candidates[0] != row:
                raise RuntimeError(f"selected latest annual accession drifted for CIK {row.cik} at {cutoff}")
            latest_outcome, _ = _share_pair(row, facts.get(row.accession, {}))
            counts[f"latest_{latest_outcome}"] += 1
            for position, candidate in enumerate(candidates):
                outcome, pair = _share_pair(candidate, facts.get(candidate.accession, {}))
                if pair is None:
                    continue
                counts["latest_accession_complete_pair" if position == 0 else "fallback_complete_pair"] += 1
                complete.append(pair)
                break
            else:
                counts["no_complete_pair"] += 1
        formations[cutoff.isoformat()] = {
            "counts": dict(sorted(counts.items())),
            "complete_pairs": complete,
        }
    return {"parser_version": PARSER_VERSION, "share_tag": SHARE_TAG, "formations": formations}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fsds-dir", type=Path, required=True)
    args = parser.parse_args()
    submissions, digests = load_submissions(args.fsds_dir)
    selected = select_formation_submissions(submissions)
    accessions = frozenset(submissions)
    facts = load_share_facts(args.fsds_dir, accessions)
    report = share_census(selected, submissions, facts)
    report["archives"] = digests
    report["script_sha256"] = _sha256(Path(__file__))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
