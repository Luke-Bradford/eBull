"""Integration test for the #2002 thesis_outcomes ledger (one DB-tier
file for the genuinely-new SQL mechanism, house rule).

Pins: candidate scan → data-anchored maturity → anchor/realized closes
re-read from price_daily (at-or-before semantics) → insert-once
idempotency (PK + ON CONFLICT DO NOTHING), plus the anchorless-thesis
gap (no rows, counted — never neutral values)."""

from __future__ import annotations

from datetime import UTC, date, datetime

import psycopg
import pytest

from app.services.thesis_outcomes import METHOD_VERSION, capture_thesis_outcomes
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable",
)

_IID = 920_001
_NOW = datetime.now(tz=UTC)
_ANCHOR = date(2026, 1, 5)


def _seed_thesis(conn: psycopg.Connection[tuple], *, with_run: bool) -> int:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (%s, 'OUTC', 'Outcome Test Co', TRUE) ON CONFLICT (instrument_id) DO NOTHING",
        (_IID,),
    )
    row = conn.execute(
        """
        INSERT INTO theses (instrument_id, thesis_version, thesis_type, stance, memo_markdown)
        VALUES (%s, (SELECT COALESCE(MAX(thesis_version), 0) + 1 FROM theses WHERE instrument_id = %s),
                'value', 'watch', 'memo')
        RETURNING thesis_id
        """,
        (_IID, _IID),
    ).fetchone()
    assert row is not None
    thesis_id = int(row[0])
    if with_run:
        conn.execute(
            """
            INSERT INTO thesis_runs (instrument_id, trigger, status, thesis_id, context_summary)
            VALUES (%s, 'manual', 'ok', %s,
                    '{"blocks": {"price_anchor": {"available": true, "as_of": "2026-01-05"}}}'::jsonb)
            """,
            (_IID, thesis_id),
        )
    conn.commit()
    return thesis_id


def _seed_prices(conn: psycopg.Connection[tuple]) -> None:
    # Anchor print, then a gap around the 30d due date (2026-02-04): the
    # at-or-before read must pick 2026-02-02 as realized_date while the
    # 2026-02-05 print is what makes the pair mature.
    for d, close in ((date(2026, 1, 5), 100.0), (date(2026, 2, 2), 110.0), (date(2026, 2, 5), 111.0)):
        conn.execute(
            "INSERT INTO price_daily (instrument_id, price_date, close) VALUES (%s, %s, %s) "
            "ON CONFLICT (instrument_id, price_date) DO UPDATE SET close = EXCLUDED.close",
            (_IID, d, close),
        )
    conn.commit()


def test_capture_inserts_once_and_only_mature_horizons(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    thesis_id = _seed_thesis(conn, with_run=True)
    anchorless_id = _seed_thesis(conn, with_run=False)
    _seed_prices(conn)

    report = capture_thesis_outcomes(conn)

    assert report.inserted == 1  # 30d mature; 90d/365d not printed yet
    assert report.anchorless >= 1  # the run-less thesis is a gap, not a row
    row = conn.execute(
        """
        SELECT horizon_days, anchor_date, anchor_close, realized_date,
               realized_close, realized_return, method_version
        FROM thesis_outcomes WHERE thesis_id = %s
        """,
        (thesis_id,),
    ).fetchone()
    assert row is not None
    horizon, anchor_date, anchor_close, realized_date, realized_close, ret, method = row
    assert horizon == 30
    assert anchor_date == _ANCHOR
    assert float(anchor_close) == pytest.approx(100.0)
    # due = 2026-02-04 (no print): trading day used = last print at-or-before.
    assert realized_date == date(2026, 2, 2)
    assert float(realized_close) == pytest.approx(110.0)
    assert float(ret) == pytest.approx(0.10)
    assert method == METHOD_VERSION

    # Insert-once: a re-run inserts nothing and rewrites nothing.
    rerun = capture_thesis_outcomes(conn)
    assert rerun.inserted == 0
    count = conn.execute(
        "SELECT COUNT(*) FROM thesis_outcomes WHERE thesis_id IN (%s, %s)",
        (thesis_id, anchorless_id),
    ).fetchone()
    assert count is not None and int(count[0]) == 1
