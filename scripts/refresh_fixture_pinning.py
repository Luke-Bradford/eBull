"""Refresh tests/fixtures/sec/MANIFEST.toml SHA-256 values from disk.

CAVEMAN: operator changed fixture on purpose -> rerun me -> SHAs match again.

WHEN TO RUN:
    * You intentionally replaced a fixture with a newer SEC snapshot.
    * You hand-edited a synthetic negative-test fixture and accepted the change.
    * You added a new fixture row to MANIFEST.toml with sha256 = "" / wrong value.

WHEN **NOT** TO RUN:
    * Test failed and you don't know why. STOP. Read
      ``tests/test_fixture_pinning.py`` docstring; figure out which of the
      three failure modes you hit BEFORE re-pinning. Re-pinning a fixture
      that was accidentally corrupted hides the bug.

This script ONLY updates the ``sha256`` field of existing [[fixture]] rows.
It does NOT add or remove rows, and does NOT touch ``accession`` / ``source_url``
/ ``captured_at`` / ``parser`` / ``path``. Edit those by hand.

Invoke:
    uv run python scripts/refresh_fixture_pinning.py

Exit code:
    0 — manifest now matches disk (no changes, or changes written).
    1 — a fixture file referenced in MANIFEST.toml is missing on disk.

Spec: docs/proposals/etl/run-8-readiness-fixes.md §Item 9.
"""

from __future__ import annotations

import hashlib
import re
import sys
import tomllib
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_MANIFEST_PATH = _REPO_ROOT / "tests" / "fixtures" / "sec" / "MANIFEST.toml"


def _sha256_of(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def main() -> int:
    if not _MANIFEST_PATH.exists():
        print(f"FATAL: MANIFEST.toml not found at {_MANIFEST_PATH}", file=sys.stderr)
        return 1

    with _MANIFEST_PATH.open("rb") as fh:
        data = tomllib.load(fh)
    rows = data.get("fixture", [])
    if not isinstance(rows, list) or not rows:
        print("FATAL: MANIFEST.toml has no [[fixture]] rows", file=sys.stderr)
        return 1

    # Build path -> new sha map. Bail on missing files BEFORE touching the
    # manifest — partial rewrite is worse than no rewrite.
    new_shas: dict[str, str] = {}
    missing: list[str] = []
    for row in rows:
        rel_path = row.get("path")
        if not isinstance(rel_path, str):
            print(f"FATAL: row missing 'path': {row!r}", file=sys.stderr)
            return 1
        abs_path = _REPO_ROOT / rel_path
        if not abs_path.exists():
            missing.append(rel_path)
            continue
        new_shas[rel_path] = _sha256_of(abs_path)

    if missing:
        print(
            "FATAL: fixture files referenced in MANIFEST.toml are missing:\n  " + "\n  ".join(missing),
            file=sys.stderr,
        )
        return 1

    # Surgical in-place rewrite of the sha256 line that belongs to each
    # path block. We do NOT round-trip through a TOML writer because
    # tomllib is read-only and we want to preserve comments + formatting.
    text = _MANIFEST_PATH.read_text(encoding="utf-8")
    changed = 0
    for rel_path, new_sha in new_shas.items():
        # Match the [[fixture]] block whose path == rel_path, then the next
        # sha256 line within that block. Cheap state machine: one path at
        # a time, anchored on the literal path line.
        path_line = f'path = "{rel_path}"'
        pos = text.find(path_line)
        if pos == -1:
            print(
                f"FATAL: could not find 'path = \"{rel_path}\"' line in MANIFEST.toml",
                file=sys.stderr,
            )
            return 1
        # Search forward for the FIRST sha256 line after this path line.
        sha_match = re.search(r'^sha256 = "([0-9a-f]{64})"', text[pos:], flags=re.MULTILINE)
        if sha_match is None:
            print(
                f"FATAL: no sha256 line found after path={rel_path!r}",
                file=sys.stderr,
            )
            return 1
        old_sha = sha_match.group(1)
        if old_sha == new_sha:
            continue
        # Replace exactly one occurrence at the absolute offset.
        abs_start = pos + sha_match.start(1)
        abs_end = pos + sha_match.end(1)
        text = text[:abs_start] + new_sha + text[abs_end:]
        changed += 1
        print(f"updated  {rel_path}: {old_sha} -> {new_sha}")

    if changed == 0:
        print("no changes — MANIFEST.toml already matches disk")
        return 0

    _MANIFEST_PATH.write_text(text, encoding="utf-8")
    print(f"wrote {changed} updated sha256 entries to {_MANIFEST_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
