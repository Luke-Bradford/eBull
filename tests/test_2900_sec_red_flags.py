from __future__ import annotations

import json
import zipfile
from datetime import datetime
from pathlib import Path

from scripts.census_2900_sec_red_flags import _accepted, census


def _write_submissions(path: Path) -> None:
    payload = {
        "filings": {
            "recent": {
                "accessionNumber": ["old", "critical", "late"],
                "acceptanceDateTime": [
                    "2022-03-01T15:00:00.000Z",
                    "2022-06-20T15:00:00.000Z",
                    "2022-06-25T15:00:00.000Z",
                ],
                "form": ["10-Q", "8-K", "NT 10-K"],
                "items": ["", "1.03,9.01", ""],
            }
        }
    }
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("CIK0000000001.json", json.dumps(payload))


def test_acceptance_timestamp_normalizes_to_new_york() -> None:
    assert _accepted("2022-06-30T20:00:00.000Z") == datetime(2022, 6, 30, 16, 0)


def test_red_flag_census_requires_complete_recent_history(tmp_path: Path) -> None:
    archive = tmp_path / "submissions.zip"
    _write_submissions(archive)
    cutoff = datetime(2022, 6, 30, 16, 0)

    result = census(
        submissions_zip=archive,
        formation_ciks={cutoff: frozenset({"0000000001", "0000000002"})},
        severity_by_code={"1.03": "critical", "9.01": "informational"},
    )

    assert result["missing_zip_entries"] == ["0000000002"]
    assert result["formations"][cutoff.isoformat()]["counts"] == {
        "ciks_with_flag": 1,
        "complete_recent_history": 1,
        "missing_cik_entry": 1,
        "population_ciks": 2,
        "score_0.7": 1,
        "score_1.0": 1,
        "scored_filings": 2,
    }
    assert result["formations"][cutoff.isoformat()]["records"] == [
        {"cik": "0000000001", "complete_recent_history": True, "red_flag_scores": [1.0, 0.7]},
        {"cik": "0000000002", "complete_recent_history": False, "red_flag_scores": []},
    ]
