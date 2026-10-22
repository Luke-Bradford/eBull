"""Capture-only backfill for the #3227 tier (IM) 13F capture.

Spec: docs/specs/ownership/2026-10-22-3227-13f-included-managers.md § Change 3.

For each Form 13F Data Set archive given (default: the four cached archives that carry OTHERMANAGER2.tsv), writes
every accession's capture record and List of Other Included Managers, and UPDATEs ``thirteen_f_segments`` on the
accession's existing 13F observations (``ingest_13f_dataset_archive(capture_only=True)``). It writes no other column,
no unresolved-CUSIP or FIGI row, and deletes nothing: the archives stay in place. One transaction per archive.

Each archive's SHA-256 is computed and checked against its ``.sha256`` sidecar when one exists. The archives are
public (SEC Form 13F Data Sets) and can be re-downloaded.

Usage: ``PYTHONPATH=. uv run python -m scripts.backfill_13f_included_managers [ARCHIVE ...]``
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import time
from pathlib import Path

import psycopg

from app.config import settings
from app.services.sec_13f_dataset_ingest import ingest_13f_dataset_archive

DEFAULT_ARCHIVES = (
    "form13f_01sep2025-30nov2025.zip",
    "form13f_01dec2025-28feb2026.zip",
    "form13f_01mar2026-31may2026.zip",
    "form13f_01jun2026-31aug2026.zip",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("archives", nargs="*", type=Path)
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=Path.home() / "Library/Application Support/eBull/sec/bulk",
        help="directory holding DEFAULT_ARCHIVES when no archive is given",
    )
    args = parser.parse_args(argv)
    archives: list[Path] = args.archives or [args.archive_dir / name for name in DEFAULT_ARCHIVES]

    for archive in archives:
        sha = _sha256(archive)
        sidecar = archive.with_name(archive.name + ".sha256")
        if sidecar.exists() and sidecar.read_text().split()[0] != sha:
            print(f"FAIL {archive.name}: sha256 {sha} differs from {sidecar.name}", flush=True)
            return 1
        print(f"{archive.name} sha256={sha}", flush=True)

    for archive in archives:
        started = time.monotonic()
        with psycopg.connect(settings.database_url) as conn:
            conn.execute("SET statement_timeout = 0")
            result = ingest_13f_dataset_archive(conn=conn, archive_path=archive, capture_only=True)
            conn.commit()
        print(
            f"{archive.name}: captures={dict(sorted(result.captures_by_status.items()))} "
            f"observations_updated={result.rows_written} infotable_rows={result.infotable_seen} "
            f"column7_invalid_lines={result.column7_invalid_lines} seconds={time.monotonic() - started:.0f}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
