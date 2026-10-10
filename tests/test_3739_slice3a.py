"""#3739 slice 3a: the §3.4 replica's window and one-archive-at-a-time notes (``scripts/build_3739_slice3a.py``)."""

from __future__ import annotations

import gzip
import json
from datetime import date
from pathlib import Path

import pytest

from scripts import build_3739_slice2b as s2b
from scripts import build_3739_slice3a as s3a
from scripts.build_3739_slice2 import Filing, is_entrant_filing
from tests.test_3739_slice2b import fact, write_archive

REPLICA = s3a.REPLICAS["2022"]


def test_replicas_are_the_spec_windows() -> None:
    assert (REPLICA.base, REPLICA.event_start, REPLICA.through) == (
        date(2022, 7, 31),
        date(2022, 8, 1),
        date(2024, 7, 31),
    )
    fallback = s3a.REPLICAS["2020"]
    assert (fallback.base, fallback.event_start, fallback.through) == (
        date(2020, 7, 31),
        date(2020, 8, 1),
        date(2022, 7, 31),
    )


def test_a_replica_whose_extract_misses_the_look_back_refuses() -> None:
    with pytest.raises(ValueError, match="look-back"):
        s3a.Replica(date(2022, 7, 31), date(2022, 8, 1), date(2024, 7, 31), date(2024, 7, 31), date(2022, 5, 2))


def test_replica_entrants_are_accepted_in_f_less_92_days_to_f_plus_24_months() -> None:
    def entrant(accepted: str) -> bool:
        filing = Filing("0000000001", "a", "8-A12B", accepted, ())
        return is_entrant_filing(filing, REPLICA.base, REPLICA.entrant_last)

    # F - 92 days is 2022-04-30 (New York dates; 16:00Z is noon in New York).
    assert not entrant("2022-04-30T16:00:00.000Z")
    assert entrant("2022-05-01T16:00:00.000Z")
    assert entrant("2024-07-31T16:00:00.000Z")
    assert not entrant("2024-08-01T16:00:00.000Z")


def test_screens_read_the_replica_window_not_stage_cs() -> None:
    ratio = fact(s2b.RATIO_TAG, accepted="2023-03-01T16:00:00.000Z", ddate="20221231", uom="pure", value="4")
    assert s2b.xbrl_candidates([ratio], date(2026, 9, 30)) == []  # stage C's window starts 2024-07-01
    (candidate,) = s2b.xbrl_candidates([ratio], REPLICA.through, REPLICA.event_start)
    assert (candidate.start, candidate.end, candidate.ratio) == (date(2021, 12, 31), date(2023, 3, 1), "4")
    late = fact(s2b.RATIO_TAG, accepted="2024-08-01T16:00:00.000Z", uom="pure", value="4")
    assert s2b.xbrl_candidates([late], REPLICA.through, REPLICA.event_start) == []


def test_item_503_screen_refuses_the_stage_c_extract_for_the_replica_window() -> None:
    filing = Filing("0000000001", "a", "8-K", "2022-06-01T16:00:00.000Z", ("5.03",))
    with pytest.raises(ValueError, match="unread"):
        s2b.item_503_candidates([filing], REPLICA.through, REPLICA.event_start)
    (candidate,) = s2b.item_503_candidates([filing], REPLICA.through, REPLICA.event_start, REPLICA.extract_from)
    assert (candidate.start, candidate.end) == (date(2022, 6, 1), date(2022, 9, 1))


def reduced(tmp_path: Path, label: str, adsh: str) -> Path:
    archive = tmp_path / f"fsnds_{label}_notes.zip"
    write_archive(
        archive, [f"{adsh}\tEntityCommonStockSharesOutstanding\tdei/2024\t20240731\t0\tshares\t0x00000000\t0\t5\t\t0"]
    )
    out = tmp_path / f"{label}.reduced.jsonl.gz"
    s3a.notes_reduce(archive, out)
    return out


def test_a_reduced_archive_keeps_its_facts_and_the_archive_digest(tmp_path: Path) -> None:
    path = reduced(tmp_path, "2022q1", "0000000001-24-000001")
    head, facts = s3a.read_reduced(path)
    assert head["archive"] == "fsnds_2022q1_notes.zip"
    assert head["archive_sha256"] == s2b.sha256_of(tmp_path / "fsnds_2022q1_notes.zip")
    assert [(f.archive, f.adsh, f.value) for f in facts] == [("2022q1", "0000000001-24-000001", "5")]


def test_a_reduced_file_holding_another_archives_facts_refuses(tmp_path: Path) -> None:
    path = reduced(tmp_path, "2022q1", "0000000001-24-000001")
    head, *rest = gzip.decompress(path.read_bytes()).decode().splitlines()
    forged = tmp_path / "forged.jsonl.gz"
    relabelled = json.dumps({**json.loads(head), "archive": "fsnds_2022q2_notes.zip"})
    forged.write_bytes(gzip.compress(("\n".join([relabelled, *rest]) + "\n").encode()))
    with pytest.raises(ValueError, match="another archive"):
        s3a.read_reduced(forged)


def test_notes_extract_refuses_overlapping_archives_before_reading_submissions(tmp_path: Path) -> None:
    first = reduced(tmp_path, "2022q1", "0000000001-24-000001")
    second = reduced(tmp_path, "2022q2", "0000000001-24-000001")
    with pytest.raises(ValueError, match="overlap"):
        s3a.notes_extract([first, second], tmp_path / "absent.zip", tmp_path / "out.jsonl.gz")
    with pytest.raises(ValueError, match="reduced twice"):
        s3a.notes_extract([first, first], tmp_path / "absent.zip", tmp_path / "out.jsonl.gz")
