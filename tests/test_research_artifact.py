from __future__ import annotations

import errno
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.services import research_artifact
from app.services.research_artifact import retain_research_artifact


def test_retained_hard_link_survives_mutable_source_replacement(tmp_path: Path) -> None:
    source = tmp_path / "mutable" / "companyfacts.zip"
    source.parent.mkdir()
    source.write_bytes(b"frozen evidence bytes")

    retained = retain_research_artifact(
        source,
        tmp_path / "retained",
        captured_at=datetime(2026, 8, 15, tzinfo=UTC),
    )
    source.unlink()
    source.write_bytes(b"refreshed bytes")

    assert retained.path.read_bytes() == b"frozen evidence bytes"
    assert retained.path.stat().st_ino != source.stat().st_ino
    assert retained.path.with_name("companyfacts.zip.sha256").read_text().strip() == retained.sha256
    metadata = json.loads(retained.metadata_path.read_text())
    assert metadata == {
        "captured_at": "2026-08-15T00:00:00+00:00",
        "sha256": retained.sha256,
        "size_bytes": 21,
        "source_path": str(source.resolve()),
    }


def test_retention_is_idempotent_for_the_same_capture(tmp_path: Path) -> None:
    source = tmp_path / "archive.zip"
    source.write_bytes(b"same bytes")
    captured_at = datetime(2026, 8, 15, 12, 0, tzinfo=UTC)
    first = retain_research_artifact(source, tmp_path / "retained", captured_at=captured_at)
    second = retain_research_artifact(source, tmp_path / "retained", captured_at=captured_at)
    assert second == first
    assert first.path.stat().st_nlink >= 2


def test_later_idempotent_call_preserves_first_capture_time(tmp_path: Path) -> None:
    source = tmp_path / "archive.zip"
    source.write_bytes(b"same bytes")
    first = retain_research_artifact(
        source,
        tmp_path / "retained",
        captured_at=datetime(2026, 8, 15, 12, 0, tzinfo=UTC),
    )
    second = retain_research_artifact(
        source,
        tmp_path / "retained",
        captured_at=datetime(2026, 8, 16, 12, 0, tzinfo=UTC),
    )
    assert second.path == first.path
    assert json.loads(second.metadata_path.read_text())["captured_at"] == "2026-08-15T12:00:00+00:00"


def test_concurrent_callers_publish_one_verified_artifact(tmp_path: Path) -> None:
    source = tmp_path / "archive.zip"
    source.write_bytes(b"same concurrent bytes")
    captured_at = datetime(2026, 8, 15, 12, 0, tzinfo=UTC)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = tuple(
            pool.map(
                lambda _: retain_research_artifact(source, tmp_path / "retained", captured_at=captured_at),
                range(16),
            )
        )
    assert len({item.path for item in results}) == 1
    assert results[0].path.read_bytes() == b"same concurrent bytes"
    assert json.loads(results[0].metadata_path.read_text())["sha256"] == results[0].sha256


def test_existing_digest_target_is_verified_not_trusted(tmp_path: Path) -> None:
    source = tmp_path / "archive.zip"
    source.write_bytes(b"declared bytes")
    first = retain_research_artifact(source, tmp_path / "good")
    corrupt = tmp_path / "retained" / "sha256" / first.sha256 / source.name
    corrupt.parent.mkdir(parents=True)
    corrupt.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="target is corrupt"):
        retain_research_artifact(source, tmp_path / "retained")


def test_existing_metadata_must_remain_a_complete_audit_record(tmp_path: Path) -> None:
    source = tmp_path / "archive.zip"
    source.write_bytes(b"declared bytes")
    retained = retain_research_artifact(source, tmp_path / "retained")
    metadata = json.loads(retained.metadata_path.read_text())
    metadata.pop("captured_at")
    retained.metadata_path.write_text(json.dumps(metadata))
    with pytest.raises(ValueError, match="no capture time"):
        retain_research_artifact(source, tmp_path / "retained")


def test_cross_device_hard_link_failure_has_no_copy_fallback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "archive.zip"
    source.write_bytes(b"source")

    def refuse_link(_source: Path, _target: Path) -> None:
        raise OSError(errno.EXDEV, "cross-device link")

    monkeypatch.setattr(os, "link", refuse_link)
    with pytest.raises(RuntimeError, match="same filesystem"):
        retain_research_artifact(source, tmp_path / "retained")
    assert not tuple((tmp_path / "retained").glob(".retaining-*"))


def test_naive_capture_time_is_refused(tmp_path: Path) -> None:
    source = tmp_path / "archive.zip"
    source.write_bytes(b"source")
    with pytest.raises(ValueError, match="timezone-aware"):
        retain_research_artifact(source, tmp_path / "retained", captured_at=datetime(2026, 8, 15))


def test_new_publications_fsync_their_directories(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "archive.zip"
    source.write_bytes(b"source")
    fsynced_directories: list[Path] = []

    monkeypatch.setattr(research_artifact, "_fsync_directory", fsynced_directories.append)
    retained = retain_research_artifact(source, tmp_path / "retained")

    assert retained.path.parent == retained.metadata_path.parent
    assert fsynced_directories.count(retained.path.parent) == 3
