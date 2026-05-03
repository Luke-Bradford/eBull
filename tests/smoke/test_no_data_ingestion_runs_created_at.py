"""Static guard: no code references ``data_ingestion_runs.created_at``
(#797 B3 — Batch 8 of #788).

The 2026-05-03 postgres log audit found a query firing every ~30s:

    SELECT status FROM data_ingestion_runs
    WHERE source LIKE 'sec_edgar_13%'
      AND created_at > NOW() - INTERVAL '15 minutes'
    ORDER BY created_at DESC LIMIT 2

``data_ingestion_runs`` (migration 032) has ``started_at`` /
``finished_at`` columns; there is no ``created_at`` column. The query
errors silently every time it runs. The hunt for the source came up
empty in the current snapshot — no procs, no views, no live
queries — so the issue was either (a) intermittent against a prior
build, or (b) something external (pgAdmin / DBeaver / ad-hoc
debug script) connected to the dev DB.

This smoke gate is the recurring guard: if any production source
file references ``data_ingestion_runs`` with ``created_at``, fail
loud at test time so the next-30s flap is caught before it lands
in CI rather than after.

Tolerated paths:

  * Test files that EXPECT the bad pattern as input (none currently
    — the codebase has zero hits).
  * This guard's own source (the patterns are data here).
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_FORBIDDEN_PATTERNS: tuple[re.Pattern[str], ...] = (
    # Loose match — any reference to data_ingestion_runs in the same
    # SQL block as ``created_at``. Multi-line tolerated via DOTALL.
    re.compile(
        r"data_ingestion_runs[\s\S]{0,200}?created_at",
        re.IGNORECASE,
    ),
    re.compile(
        r"created_at[\s\S]{0,200}?data_ingestion_runs",
        re.IGNORECASE,
    ),
)

_SCAN_DIRS = (
    _REPO_ROOT / "app",
    _REPO_ROOT / "scripts",
    _REPO_ROOT / "frontend" / "src",
)

_ALLOWED: set[str] = {
    # The guard itself contains the patterns as data.
    "tests/smoke/test_no_data_ingestion_runs_created_at.py",
}


def test_no_code_references_data_ingestion_runs_created_at() -> None:
    """Fail if any production source file pairs
    ``data_ingestion_runs`` with ``created_at`` — the column does
    not exist on that table (migration 032 uses ``started_at`` /
    ``finished_at``).
    """
    offenders: list[tuple[str, str]] = []
    for scan_dir in _SCAN_DIRS:
        if not scan_dir.exists():
            continue
        for path in scan_dir.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix not in {".py", ".ts", ".tsx", ".sql", ".sh"}:
                continue
            rel = path.relative_to(_REPO_ROOT).as_posix()
            if rel in _ALLOWED:
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError, OSError:
                continue
            for pattern in _FORBIDDEN_PATTERNS:
                if pattern.search(text):
                    offenders.append((rel, pattern.pattern[:40]))
                    break

    assert not offenders, (
        "Found code referencing ``data_ingestion_runs.created_at`` — "
        "the table has ``started_at`` / ``finished_at``, not ``created_at`` "
        "(migration 032). Fix the column name or, if the table moved, "
        "update this guard. Offenders:\n" + "\n".join(f"  {f} (matched {p!r}...)" for f, p in offenders)
    )
