"""Content-addressed retention for mutable source files used as evidence.

Bulk-source refreshes replace their cache path atomically.  A result checksum
is not reproducible evidence if those bytes then disappear.  This module pins
the current inode by hard link, hashes the retained link, and publishes it
under its measured SHA-256.  It deliberately has no copy fallback: retention
must be atomic, same-filesystem and cheap, or fail closed.
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final
from uuid import uuid4

ALGORITHM: Final = "sha256"


@dataclass(frozen=True)
class RetainedResearchArtifact:
    path: Path
    sha256: str
    size_bytes: int
    metadata_path: Path


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, ALGORITHM).hexdigest()


def _fsync_directory(path: Path) -> None:
    """Make a newly published directory entry durable before returning."""
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish_text_once(path: Path, value: str) -> None:
    """Atomically create immutable small metadata, or verify the incumbent."""
    temporary = path.with_name(f".{path.name}.retaining-{uuid4().hex}")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_text(encoding="utf-8") != value:
                raise ValueError(f"retained artifact metadata conflicts with existing file: {path}")
        else:
            _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _publish_metadata_once(path: Path, metadata: dict[str, object]) -> None:
    """Publish first-capture metadata while accepting a later idempotent read."""

    def validate_existing() -> None:
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"retained artifact metadata is unreadable: {path}") from exc
        for field in ("sha256", "size_bytes"):
            if existing.get(field) != metadata[field]:
                raise ValueError(f"retained artifact metadata conflicts on {field}: {path}")
        if not isinstance(existing.get("captured_at"), str) or not existing["captured_at"]:
            raise ValueError(f"retained artifact metadata has no capture time: {path}")
        if not isinstance(existing.get("source_path"), str) or not existing["source_path"]:
            raise ValueError(f"retained artifact metadata has no source path: {path}")

    if path.exists():
        validate_existing()
        return
    temporary = path.with_name(f".{path.name}.retaining-{uuid4().hex}")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(metadata, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            validate_existing()
        else:
            _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def retain_research_artifact(
    source: Path,
    retention_root: Path,
    *,
    captured_at: datetime | None = None,
) -> RetainedResearchArtifact:
    """Pin ``source`` by hard link and publish it under its measured digest."""
    measured_at = captured_at or datetime.now(UTC)
    if measured_at.tzinfo is None or measured_at.utcoffset() is None:
        raise ValueError("captured_at must be timezone-aware")
    if not source.is_file() or source.is_symlink():
        raise FileNotFoundError(f"research source must be an existing regular non-symlink file: {source}")
    retention_root.mkdir(parents=True, exist_ok=True)
    temporary = retention_root / f".retaining-{source.name}-{uuid4().hex}"
    try:
        try:
            os.link(source, temporary)
        except OSError as exc:
            if exc.errno == errno.EXDEV:
                raise RuntimeError(
                    "research artifact retention requires source and retention root on the same filesystem"
                ) from exc
            raise

        digest = _sha256(temporary)
        size = temporary.stat().st_size
        artifact_dir = retention_root / ALGORITHM / digest
        artifact_dir.mkdir(parents=True, exist_ok=True)
        retained = artifact_dir / source.name
        try:
            os.link(temporary, retained)
        except FileExistsError:
            # Re-hash instead of trusting sidecar metadata: this is an offline
            # evidence boundary where corruption detection outweighs read cost.
            if retained.is_symlink() or retained.stat().st_size != size or _sha256(retained) != digest:
                raise ValueError(f"content-addressed artifact target is corrupt: {retained}")
        else:
            _fsync_directory(artifact_dir)
    finally:
        temporary.unlink(missing_ok=True)

    sidecar = retained.with_name(retained.name + ".sha256")
    _publish_text_once(sidecar, digest + "\n")
    metadata_path = retained.with_name(retained.name + ".artifact.json")
    metadata = {
        "captured_at": measured_at.astimezone(UTC).isoformat(),
        "sha256": digest,
        "size_bytes": size,
        "source_path": str(source.resolve()),
    }
    _publish_metadata_once(metadata_path, metadata)
    return RetainedResearchArtifact(
        path=retained,
        sha256=digest,
        size_bytes=size,
        metadata_path=metadata_path,
    )


__all__ = ["RetainedResearchArtifact", "retain_research_artifact"]
