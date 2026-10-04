"""#3622 slice 1 — one snapshot + rows per file; a repeat is not re-stored; a frozen feed fails."""

from __future__ import annotations

import gzip
from datetime import UTC, datetime
from decimal import Decimal

import psycopg
import pytest

from app.services.ibkr_borrow_archive import BorrowFileError, archive_borrow_file

_FILE = (
    b"#BOF|2026.10.04|08:36:44\n#SYM|CUR|NAME|CON|ISIN|REBATERATE|FEERATE|AVAILABLE|FIGI|\n"
    b"GME|USD|GAMESTOP CORP-CLASS A|36285627|XXXXXXXW1099|3.6123|0.2677|>10000000|BBG000BB5BF6|\n"
    b"ZZZ|USD|SOME NAME|42|US0000000000|NA|NA|2000||\n#EOF|2\n"
)


def test_archive_then_unchanged_then_stale_refused(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    conn.autocommit = True
    fresh = lambda: datetime(2026, 10, 4, 13, 0, tzinfo=UTC)  # noqa: E731
    try:
        first = archive_borrow_file(conn, fetch=lambda: _FILE, now=fresh)
        again = archive_borrow_file(conn, fetch=lambda: _FILE, now=fresh)
        assert (first.status, first.row_count, again.status, again.snapshot_id) == (
            "archived",
            2,
            "unchanged",
            first.snapshot_id,
        )
        payload, as_of = conn.execute(
            "SELECT payload_gzip, provider_as_of FROM ibkr_borrow_snapshots WHERE snapshot_id = %s",
            (first.snapshot_id,),
        ).fetchone()  # type: ignore[misc]
        assert gzip.decompress(payload) == _FILE
        assert as_of == datetime(2026, 10, 4, 12, 36, 44, tzinfo=UTC)
        assert conn.execute(
            "SELECT symbol, fee_rate_pct, available_shares, available_capped FROM ibkr_borrow_rates "
            "WHERE snapshot_id = %s ORDER BY conid",
            (first.snapshot_id,),
        ).fetchall() == [("ZZZ", None, 2000, False), ("GME", Decimal("0.2677"), 10_000_000, True)]

        # An old stamp on a NEW file is still data: stored under its own as-of.
        old_file = _FILE.replace(b"2026.10.04", b"2026.10.02")
        assert archive_borrow_file(conn, fetch=lambda: old_file, now=fresh).status == "archived"
        # The same old file again: the feed is frozen.
        with pytest.raises(BorrowFileError, match="not moving"):
            archive_borrow_file(conn, fetch=lambda: old_file, now=fresh)
        assert conn.execute("SELECT count(*) FROM ibkr_borrow_snapshots").fetchone() == (2,)
        with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
            conn.execute("UPDATE ibkr_borrow_rates SET fee_rate_pct = 0 WHERE conid = 42")
    finally:
        conn.execute("DELETE FROM ibkr_borrow_rates")
        conn.execute("DELETE FROM ibkr_borrow_snapshots")
        conn.autocommit = False
