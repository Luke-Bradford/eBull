"""Pin SEC parser fixtures to their captured SHA-256 — #1233 Item 9.

CAVEMAN: fixture bytes change -> SHA changes -> test fails LOUD.
No silent fixture drift; no accidental "I just re-saved it" regressions.

A failure here means one of:

1. Someone edited a fixture in-place without thinking.
   Fix: revert the edit, OR if the edit was intentional re-pin via
   ``uv run python scripts/refresh_fixture_pinning.py`` and explain
   in the PR description WHY the source-of-truth changed.

2. A fixture file got swapped (e.g. wrong file copied over the path).
   Fix: restore the right file. Cross-check ``accession`` + ``source_url``
   in ``tests/fixtures/sec/MANIFEST.toml`` against the SEC EDGAR direct
   URL.

3. The MANIFEST.toml entry is wrong (SHA typo at capture time).
   Fix: recompute with ``shasum -a 256 <path>`` and update the manifest.

Scope (v1.3): 4 highest-load-bearing parsers with #932 validation-cliff
exposure: sec_n_csr, sec_n_port, sec_13f_hr, sec_13dg. Per-parser test
functions so a failure isolates the affected parser at glance.

Spec: ``docs/proposals/etl/run-8-readiness-fixes.md`` §Item 9.
"""

from __future__ import annotations

import hashlib
import tomllib
from pathlib import Path

import pytest

# Repo root = parent of tests/.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_MANIFEST_PATH = _REPO_ROOT / "tests" / "fixtures" / "sec" / "MANIFEST.toml"

# Parsers in v1.3 scope. A parser appearing here with zero MANIFEST rows
# is NOT a hard failure — it just means we have no committed fixture to
# pin yet (e.g. sec_n_csr lives in gitignored spike cache; sec_13dg uses
# inline XML strings in test bodies). The session report flags the gap.
_PARSERS_IN_SCOPE = (
    "sec_n_csr",
    "sec_n_port",
    "sec_13f_hr",
    "sec_13dg",
)


def _load_manifest() -> list[dict[str, str]]:
    """Return the [[fixture]] rows from MANIFEST.toml."""
    if not _MANIFEST_PATH.exists():
        pytest.fail(
            f"MANIFEST.toml missing at {_MANIFEST_PATH}. See tests/test_fixture_pinning.py docstring + spec §Item 9."
        )
    with _MANIFEST_PATH.open("rb") as fh:
        data = tomllib.load(fh)
    fixtures = data.get("fixture")
    if not isinstance(fixtures, list):
        pytest.fail(
            f"MANIFEST.toml at {_MANIFEST_PATH} has no [[fixture]] rows. "
            "Expected a list of tables under the 'fixture' key."
        )
    return fixtures


def _sha256_of(path: Path) -> str:
    """Stream-hash a file. 64 KiB chunks — fine for these fixtures, won't
    explode on a stray big one either."""
    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _assert_fixtures_for_parser(parser: str) -> None:
    """Per-parser SHA gate. Skips with a clear message when the parser
    has no committed fixtures to pin (NOT a hard failure — gap is
    reported in PR + memory)."""
    rows = [row for row in _load_manifest() if row.get("parser") == parser]
    if not rows:
        pytest.skip(
            f"No committed fixtures pinned for parser {parser!r}. "
            "This is a known coverage gap — see MANIFEST.toml header comment."
        )

    failures: list[str] = []
    for row in rows:
        rel_path = row.get("path")
        expected_sha = row.get("sha256")
        accession = row.get("accession", "<no-accession>")
        if not isinstance(rel_path, str) or not isinstance(expected_sha, str):
            failures.append(f"  MANIFEST row malformed (missing path/sha256): {row!r}")
            continue
        abs_path = _REPO_ROOT / rel_path
        if not abs_path.exists():
            failures.append(
                f"  {rel_path} (accession={accession}): file MISSING on disk. MANIFEST.toml claims it should exist."
            )
            continue
        actual_sha = _sha256_of(abs_path)
        if actual_sha != expected_sha:
            failures.append(
                f"  {rel_path} (accession={accession}):\n"
                f"      expected sha256 = {expected_sha}\n"
                f"      actual sha256   = {actual_sha}\n"
                "      -> fixture was modified after pinning. Revert the edit "
                "OR re-pin via uv run python scripts/refresh_fixture_pinning.py "
                "AND justify in PR description."
            )

    if failures:
        pytest.fail(
            f"Fixture pin verification failed for parser {parser!r} "
            f"({len(failures)} of {len(rows)} entries):\n" + "\n".join(failures)
        )


def test_sec_n_port_fixtures_pinned() -> None:
    """NPORT-P parser fixtures match captured SHA-256."""
    _assert_fixtures_for_parser("sec_n_port")


def test_sec_13f_hr_fixtures_pinned() -> None:
    """13F-HR parser fixtures match captured SHA-256."""
    _assert_fixtures_for_parser("sec_13f_hr")


def test_sec_n_csr_fixtures_pinned() -> None:
    """N-CSR parser fixtures match captured SHA-256.

    Skips when no fixtures are pinned (current state: spike cache at
    ``.tmp/spike-918/`` is gitignored; no extracted fixture lives in
    ``tests/fixtures/`` yet)."""
    _assert_fixtures_for_parser("sec_n_csr")


def test_sec_13dg_fixtures_pinned() -> None:
    """13D / 13G parser fixtures match captured SHA-256.

    Skips when no fixtures are pinned (current state: parser tests use
    inline XML literals; no separate fixture file exists yet)."""
    _assert_fixtures_for_parser("sec_13dg")


def test_manifest_no_orphan_or_unknown_parser_rows() -> None:
    """Every MANIFEST row points to a parser in v1.3 scope. Catches a
    typo in the ``parser =`` key that would otherwise silently land in
    none of the four per-parser tests."""
    bad: list[str] = []
    for row in _load_manifest():
        parser = row.get("parser")
        if parser not in _PARSERS_IN_SCOPE:
            bad.append(f"  path={row.get('path')!r}: parser={parser!r} not in scope {_PARSERS_IN_SCOPE!r}")
    if bad:
        pytest.fail(
            "MANIFEST.toml contains rows with unknown parser key:\n"
            + "\n".join(bad)
            + "\nEither fix the parser key or extend _PARSERS_IN_SCOPE in this test."
        )
