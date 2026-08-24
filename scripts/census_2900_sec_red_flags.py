"""Outcome-blind historical red-flag coverage census for R6 #2900/#2908.

The SEC bulk submissions snapshot supplies accession, acceptance time, form and
8-K item codes. This script checks whether its primary per-CIK ``recent`` block
fully covers the frozen 90-day scorer window at each R6 formation. It does not
read prices or returns.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import zipfile
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Final
from zoneinfo import ZoneInfo

import psycopg

from app.config import settings
from app.services import filings_risk
from app.services.filings_risk import score_filing_red_flag
from scripts.census_2900_sec_cover_identity import (
    load_submissions,
    select_formation_submissions,
)

PARSER_VERSION: Final = "r6-sec-red-flag-census-v2"
LOOKBACK_DAYS: Final = 90
_NEW_YORK: Final = ZoneInfo("America/New_York")


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _column(recent: dict[str, Any], key: str, index: int) -> str:
    values = recent.get(key)
    if not isinstance(values, list) or index >= len(values):
        return ""
    value = values[index]
    return value.strip() if isinstance(value, str) else ""


def _accepted(value: str) -> datetime | None:
    compact = value.strip()
    try:
        if len(compact) == 14 and compact.isdigit():
            return datetime.strptime(compact, "%Y%m%d%H%M%S")
        parsed = datetime.fromisoformat(compact.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed
        return parsed.astimezone(_NEW_YORK).replace(tzinfo=None)
    except ValueError:
        return None


def _items(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def load_severity_mapping() -> dict[str, str]:
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute("SELECT code, severity FROM sec_8k_item_codes ORDER BY code").fetchall()
    return {str(code): str(severity) for code, severity in rows}


def census(
    *,
    submissions_zip: Path,
    formation_ciks: dict[datetime, frozenset[str]],
    severity_by_code: dict[str, str],
) -> dict[str, Any]:
    all_ciks = sorted(set().union(*formation_ciks.values()))
    per_formation: dict[str, dict[str, Any]] = {}
    payloads: dict[str, dict[str, Any]] = {}
    missing_entries: list[str] = []

    with zipfile.ZipFile(submissions_zip) as archive:
        names = set(archive.namelist())
        for cik in all_ciks:
            name = f"CIK{cik}.json"
            if name not in names:
                missing_entries.append(cik)
                continue
            payload = json.loads(archive.read(name))
            if not isinstance(payload, dict):
                raise RuntimeError(f"SEC submissions entry {name} is not an object")
            payloads[cik] = payload

    for cutoff, ciks in formation_ciks.items():
        counts: Counter[str] = Counter(population_ciks=len(ciks))
        incomplete_examples: list[str] = []
        records: list[dict[str, Any]] = []
        for cik in sorted(ciks):
            payload = payloads.get(cik)
            if payload is None:
                counts["missing_cik_entry"] += 1
                records.append({"cik": cik, "complete_recent_history": False, "red_flag_scores": []})
                continue
            filings = payload.get("filings")
            recent = filings.get("recent") if isinstance(filings, dict) else None
            if not isinstance(recent, dict):
                counts["missing_recent_block"] += 1
                records.append({"cik": cik, "complete_recent_history": False, "red_flag_scores": []})
                continue
            accessions = recent.get("accessionNumber")
            if not isinstance(accessions, list):
                counts["missing_accession_array"] += 1
                records.append({"cik": cik, "complete_recent_history": False, "red_flag_scores": []})
                continue

            accepted_values = [
                _accepted(_column(recent, "acceptanceDateTime", index)) for index in range(len(accessions))
            ]
            known = [value for value in accepted_values if value is not None]
            window_start = cutoff - timedelta(days=LOOKBACK_DAYS)
            if not known or min(known) > window_start:
                counts["incomplete_recent_history"] += 1
                if len(incomplete_examples) < 10:
                    incomplete_examples.append(cik)
                records.append({"cik": cik, "complete_recent_history": False, "red_flag_scores": []})
                continue
            counts["complete_recent_history"] += 1

            cik_has_flag = False
            scores: list[float] = []
            for index, accepted_at in enumerate(accepted_values):
                if accepted_at is None:
                    counts["filings_missing_acceptance"] += 1
                    continue
                if not (window_start <= accepted_at <= cutoff):
                    continue
                form = _column(recent, "form", index)
                if not (form.startswith("8-K") or form.startswith("NT")):
                    continue
                counts["scored_filings"] += 1
                item_text = _column(recent, "items", index)
                if form.startswith("8-K") and not item_text:
                    counts["eight_k_missing_items"] += 1
                score = score_filing_red_flag(form, _items(item_text), severity_by_code)
                if score is not None:
                    counts[f"score_{score:.1f}"] += 1
                    cik_has_flag = True
                    scores.append(score)
            if cik_has_flag:
                counts["ciks_with_flag"] += 1
            records.append({"cik": cik, "complete_recent_history": True, "red_flag_scores": scores})

        per_formation[cutoff.isoformat()] = {
            "counts": dict(sorted(counts.items())),
            "incomplete_examples": incomplete_examples,
            "records": records,
        }

    mapping_bytes = json.dumps(severity_by_code, sort_keys=True, separators=(",", ":")).encode()
    return {
        "parser_version": PARSER_VERSION,
        "source_sha256": {
            "app/services/filings_risk.py": _sha256(Path(filings_risk.__file__)),
            "scripts/census_2900_sec_red_flags.py": _sha256(Path(__file__)),
        },
        "lookback_days": LOOKBACK_DAYS,
        "submissions_sha256": _sha256(submissions_zip),
        "severity_mapping": severity_by_code,
        "severity_mapping_sha256": hashlib.sha256(mapping_bytes).hexdigest(),
        "selected_ciks": len(all_ciks),
        "missing_zip_entries": missing_entries,
        "formations": per_formation,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fsds-dir", type=Path, required=True)
    parser.add_argument("--submissions-zip", type=Path, required=True)
    args = parser.parse_args()

    submissions, digests = load_submissions(args.fsds_dir)
    selected = select_formation_submissions(submissions)
    formation_ciks = {cutoff: frozenset(row.cik for row in rows) for cutoff, rows in selected.items()}
    report = census(
        submissions_zip=args.submissions_zip,
        formation_ciks=formation_ciks,
        severity_by_code=load_severity_mapping(),
    )
    report["fsds_archives"] = digests
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
