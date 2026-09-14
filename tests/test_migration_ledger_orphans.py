"""Ledger rows with no matching file are reported (#2362 item 2).

Pure tests: ``orphaned_ledger_filenames`` takes both populations as arguments
precisely so the rule is checkable without a database. The DB-side wiring is two
call sites (``run_migrations``'s boot warning and ``migration_status``'s extra
rows), and ``migration_status`` is exercised here against a stub connection
rather than a real one — the query it runs is a plain SELECT the smoke gate
already drives on every boot.

⚠ Neither call site repairs anything. See the function's docstring for why an
orphan is a stale audit record rather than a broken schema.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from app.db import migrations
from app.db.migrations import migration_status, orphaned_ledger_filenames


class TestOrphanedLedgerFilenames:
    def test_a_ledger_row_with_no_file_is_reported(self) -> None:
        assert orphaned_ledger_filenames(
            ["218_tender_offer_events.sql", "224_tender_offer_events.sql"],
            ["224_tender_offer_events.sql"],
        ) == ["218_tender_offer_events.sql"]

    def test_an_unapplied_file_is_not_an_orphan(self) -> None:
        # The other direction entirely: a file the ledger has never seen is
        # PENDING, and reporting it here would make every fresh migration look
        # like a defect on the run that introduces it.
        assert orphaned_ledger_filenames(["001_a.sql"], ["001_a.sql", "002_b.sql"]) == []

    def test_result_is_sorted_so_a_boot_log_is_stable(self) -> None:
        assert orphaned_ledger_filenames(["361_z.sql", "218_a.sql", "260_m.sql"], []) == [
            "218_a.sql",
            "260_m.sql",
            "361_z.sql",
        ]

    def test_no_ledger_rows_and_no_files_is_empty_not_an_error(self) -> None:
        assert orphaned_ledger_filenames([], []) == []


class _StubCursor:
    """Returns one ``(filename, applied_at)`` pair per configured row."""

    def __init__(self, rows: list[tuple[str, Any]]) -> None:
        self._rows = rows

    def __iter__(self) -> Any:
        return iter(self._rows)


class _StubConn:
    def __init__(self, rows: list[tuple[str, Any]]) -> None:
        self._rows = rows

    def execute(self, *_args: Any, **_kwargs: Any) -> _StubCursor:
        return _StubCursor(self._rows)


class _Stamp:
    def __init__(self, text: str) -> None:
        self._text = text

    def isoformat(self) -> str:
        return self._text


class TestMigrationStatusReportsOrphans:
    @pytest.fixture(autouse=True)
    def _two_files(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            migrations,
            "_migration_files",
            lambda: [Path("sql/001_a.sql"), Path("sql/002_b.sql")],
        )

    def test_orphan_row_is_appended_after_the_file_rows(self) -> None:
        conn = _StubConn(
            [
                ("001_a.sql", _Stamp("2026-01-01T00:00:00+00:00")),
                ("003_renamed_away.sql", _Stamp("2026-02-02T00:00:00+00:00")),
            ]
        )
        rows = migration_status(conn)  # type: ignore[arg-type]
        assert [(row["file"], row["status"]) for row in rows] == [
            ("001_a.sql", "applied"),
            ("002_b.sql", "pending"),
            ("003_renamed_away.sql", "orphaned"),
        ]
        # The stamp is the one thing an orphan still carries that nothing else
        # records: when that DDL actually ran.
        assert rows[-1]["applied_at"] == "2026-02-02T00:00:00+00:00"

    def test_a_clean_ledger_adds_no_rows(self) -> None:
        conn = _StubConn([("001_a.sql", _Stamp("2026-01-01T00:00:00+00:00"))])
        rows = migration_status(conn)  # type: ignore[arg-type]
        assert [row["status"] for row in rows] == ["applied", "pending"]
