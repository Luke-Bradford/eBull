"""#3619 slice 3 — the pinned PWB captures agree with the archives and the reader that consume them."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from app.services import research_corpus_ingest as ingest
from app.services import total_return_reader as tr
from app.services.price_quarantine import PROVISIONAL_WINDOW_DAYS
from scripts.ingest_2282_research_archive import _SHARDS, CAPTURES, shard_mismatches


def test_each_capture_is_its_own_registered_vendor() -> None:
    vendors = [capture.archive.vendor for capture in CAPTURES.values()]
    assert len(set(vendors)) == len(vendors)
    for capture in CAPTURES.values():
        assert capture.archive in ingest.RESEARCH_ARCHIVES
        assert len(capture.revision) == 40


def test_quarantine_as_of_is_capture_plus_provisional_window_plus_one() -> None:
    for capture_date, capture in CAPTURES.items():
        expected = date.fromisoformat(capture_date) + timedelta(days=PROVISIONAL_WINDOW_DAYS + 1)
        assert capture.archive.quarantine_as_of == expected


def test_reader_reads_the_capture_it_declares() -> None:
    capture = CAPTURES[tr.PWB_CAPTURE_DATE.isoformat()]
    assert capture.archive.vendor == tr.PWB_VENDOR
    assert tr.LAST_PWB_MONTH == (2026, 8)


def test_a_cache_filled_from_another_revision_is_refused(tmp_path: Path) -> None:
    capture = CAPTURES["2026-09-09"]
    assert shard_mismatches(tmp_path, capture) == _SHARDS
    for name in _SHARDS:
        (tmp_path / name).write_bytes(b"another capture")
    assert shard_mismatches(tmp_path, capture) == _SHARDS
