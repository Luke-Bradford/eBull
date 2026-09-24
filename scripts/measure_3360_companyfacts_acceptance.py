"""Premise measurement for #3360: companyfacts ``filed`` vs the accession's acceptance.

Read-only, full population: every CIK entry in a ``companyfacts.zip``, joined per
accession to ``acceptanceDateTime`` from a ``submissions.zip`` (``recent`` plus every
``filings.files`` page -- sec-edgar skill §7.4). Acceptance is converted UTC -> New
York before its calendar date is compared (§7.8). Both archives are hashed from the
bytes read and printed, because the on-disk copies refresh daily.

Reports (spec: ``docs/proposals/ta/2026-09-24-3360-companyfacts-pit-bundle.md``):
  * accessions with no acceptance in the CIK's own submissions -- including every
    accession of a CIK with no submissions entry or an incomplete page set;
  * accessions whose companyfacts rows disagree on (filed, form), and accessions whose
    submissions rows disagree on acceptance;
  * acceptance NY date vs ``filed``: equal / earlier / LATER (later = a ``filed`` clock
    would call the fact public before EDGAR accepted it);
  * us-gaap ``(concept, unit, start, end)`` keys on >1 accession and how many of those
    carry different values (exact ``Decimal`` comparison -- "value disagreement", not a
    classified restatement).

Usage::

    uv run python scripts/measure_3360_companyfacts_acceptance.py \\
        --companyfacts ~/Library/Application\\ Support/eBull/sec/bulk/companyfacts.zip \\
        --submissions ~/Library/Application\\ Support/eBull/sec/bulk/submissions.zip > /tmp/m3360.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import zipfile
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

_NEW_YORK = ZoneInfo("America/New_York")


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _acceptance_by_accession(
    submissions: zipfile.ZipFile, names: set[str], cik10: str
) -> tuple[str, dict[str, set[str]]]:
    """``(status, accn -> distinct acceptance strings)``; status names why it is empty."""
    entry = f"CIK{cik10}.json"
    if entry not in names:
        return "no_submissions_entry", {}
    payload = json.loads(submissions.read(entry))
    filings = payload.get("filings") or {}
    blocks: list[dict[str, Any]] = [filings.get("recent") or {}]
    for page in filings.get("files") or []:
        if page.get("name") not in names:
            return "submissions_page_missing", {}
        blocks.append(json.loads(submissions.read(page["name"])))
    out: dict[str, set[str]] = defaultdict(set)
    for block in blocks:
        accessions = block.get("accessionNumber", [])
        accepted = block.get("acceptanceDateTime", [])
        if len(accessions) != len(accepted):
            return "submissions_array_length_mismatch", {}
        for accn, value in zip(accessions, accepted, strict=True):
            out[accn].add(value)
    return "ok", out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--companyfacts", type=Path, required=True)
    parser.add_argument("--submissions", type=Path, required=True)
    args = parser.parse_args()

    input_sha = {"companyfacts": _sha256(args.companyfacts), "submissions": _sha256(args.submissions)}
    companyfacts = zipfile.ZipFile(args.companyfacts)
    submissions = zipfile.ZipFile(args.submissions)
    submission_names = set(submissions.namelist())
    counts: Counter[str] = Counter()
    relation: Counter[str] = Counter()
    keys: Counter[str] = Counter()
    forms: Counter[str] = Counter()
    by_year: Counter[int] = Counter()
    examples: dict[str, list[tuple[str, ...]]] = defaultdict(list)

    for name in companyfacts.namelist():
        if not name.endswith(".json"):
            continue
        cik10 = name[3:13]
        payload = json.loads(companyfacts.read(name), parse_float=Decimal, parse_int=Decimal)
        facts = payload.get("facts") or {}
        metadata: dict[str, set[tuple[str, str]]] = defaultdict(set)
        key_values: dict[tuple[Any, ...], set[tuple[str, Decimal]]] = defaultdict(set)
        for taxonomy, section in facts.items():
            for concept, body in (section or {}).items():
                for unit, rows in ((body or {}).get("units") or {}).items():
                    for row in rows or []:
                        metadata[row["accn"]].add((row["filed"], row["form"]))
                        if taxonomy == "us-gaap":
                            key_values[(concept, unit, row.get("start"), row.get("end"))].add((row["accn"], row["val"]))
        counts["ciks"] += 1
        if not metadata:
            counts["ciks_no_facts"] += 1
            continue
        status, accepted_by_accn = _acceptance_by_accession(submissions, submission_names, cik10)
        if status != "ok":
            counts[f"ciks_{status}"] += 1
        for accn, rows in metadata.items():
            counts["accessions"] += 1
            if len(rows) > 1:
                counts["accessions_companyfacts_metadata_conflict"] += 1
                continue
            filed, form = next(iter(rows))
            forms[form] += 1
            accepted_values = accepted_by_accn.get(accn, set())
            if not accepted_values:
                counts["accessions_without_acceptance"] += 1
                continue
            if len(accepted_values) > 1:
                counts["accessions_acceptance_conflict"] += 1
                continue
            accepted = next(iter(accepted_values))
            try:
                parsed = datetime.fromisoformat(accepted.replace("Z", "+00:00"))
            except ValueError:
                counts["accessions_acceptance_unparseable"] += 1
                continue
            if parsed.tzinfo is None:
                counts["accessions_acceptance_without_offset"] += 1
                continue
            accepted_ny = parsed.astimezone(_NEW_YORK)
            by_year[accepted_ny.year] += 1
            filed_date = date.fromisoformat(filed)
            if accepted_ny.date() == filed_date:
                relation["equal"] += 1
            elif accepted_ny.date() < filed_date:
                relation["accepted_earlier"] += 1
            else:
                relation["accepted_LATER"] += 1
                if len(examples["accepted_later"]) < 10:
                    examples["accepted_later"].append((cik10, accn, accepted, filed))
        for pairs in key_values.values():
            keys["keys"] += 1
            if len({accn for accn, _ in pairs}) > 1:
                keys["keys_multi_accession"] += 1
                if len({value for _, value in pairs}) > 1:
                    keys["keys_multi_accession_value_disagreement"] += 1

    json.dump(
        {
            "input_sha256": input_sha,
            "counts": counts,
            "acceptance_ny_date_vs_filed": relation,
            "us_gaap_period_keys": keys,
            "forms": forms.most_common(),
            "accessions_by_acceptance_year": sorted(by_year.items()),
            "examples": examples,
        },
        sys.stdout,
        indent=1,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
