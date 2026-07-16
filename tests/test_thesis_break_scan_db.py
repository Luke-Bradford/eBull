"""Integration test for the #2012 break-predicate scan (one DB-tier file
for the genuinely-new SQL mechanism, house rule).

Pins: predicate upsert + arm → fire on a genuine false→true transition →
UNIQUE-dedup on re-scan → find_stale_instruments rule 5 keys the event to
the LATEST thesis by thesis_id equality (a regenerated thesis with no new
break must NOT report break_fired — Codex ckpt-1 BLOCKING), plus the
already_true premise path.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import psycopg
import pytest

from app.services.thesis import find_stale_instruments
from app.services.thesis_break_scan import run_thesis_break_scan
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable",
)

_IID = 910_001
_NOW = datetime.now(tz=UTC)


def _seed(conn: psycopg.Connection[tuple], *, rsi: float, created_at: datetime | None = None) -> int:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (%s, 'BRKT', 'Break Test Co', TRUE) ON CONFLICT (instrument_id) DO NOTHING",
        (_IID,),
    )
    conn.execute(
        "INSERT INTO coverage (instrument_id, coverage_tier, review_frequency, filings_status) "
        "VALUES (%s, 1, 'monthly', 'analysable') ON CONFLICT (instrument_id) DO NOTHING",
        (_IID,),
    )
    row = conn.execute(
        """
        INSERT INTO theses (instrument_id, thesis_version, thesis_type, stance, memo_markdown,
                            break_conditions_json, created_at)
        VALUES (%s, (SELECT COALESCE(MAX(thesis_version), 0) + 1 FROM theses WHERE instrument_id = %s),
                'value', 'watch', 'memo',
                '["RSI-14 crosses above 70 (overbought territory)", "Loss of key patents"]'::jsonb,
                %s)
        RETURNING thesis_id
        """,
        (_IID, _IID, created_at or _NOW),
    ).fetchone()
    assert row is not None
    _set_rsi(conn, rsi)
    conn.commit()
    return int(row[0])


def _event_count(conn: psycopg.Connection[tuple], thesis_id: int) -> int:
    row = conn.execute("SELECT COUNT(*) FROM thesis_break_events WHERE thesis_id = %s", (thesis_id,)).fetchone()
    assert row is not None
    return int(row[0])


def _set_rsi(conn: psycopg.Connection[tuple], rsi: float) -> None:
    conn.execute(
        """
        INSERT INTO price_daily (instrument_id, price_date, close, rsi_14)
        VALUES (%s, %s, 100.0, %s)
        ON CONFLICT (instrument_id, price_date) DO UPDATE SET rsi_14 = EXCLUDED.rsi_14
        """,
        (_IID, date.today(), rsi),
    )
    conn.commit()


def test_scan_arm_fire_dedup_and_stale_rule(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    thesis_id = _seed(conn, rsi=50.0)

    # Scan 1: predicate extracted (prose sibling gets no row), baseline false → armed.
    report = run_thesis_break_scan(conn)
    assert report.predicates_inserted == 1
    assert report.fired == 0
    assert report.state_counts.get("armed") == 1
    row = conn.execute(
        "SELECT metric, op, threshold, baseline_state FROM thesis_break_predicates WHERE thesis_id = %s",
        (thesis_id,),
    ).fetchone()
    assert row is not None
    assert (row[0], row[1], float(row[2])) == ("rsi_14", ">", 70.0)
    assert row[3] == "armed"

    # No event, no stale flag yet.
    assert _event_count(conn, thesis_id) == 0
    stale = {s.instrument_id: s.reason for s in find_stale_instruments(conn, tier=None, instrument_ids=[_IID])}
    assert _IID not in stale

    # Scan 2 after a genuine transition: armed → fire, exactly one event.
    _set_rsi(conn, 75.0)
    report = run_thesis_break_scan(conn)
    assert report.fired == 1
    events = conn.execute(
        "SELECT observed_value, inputs_json FROM thesis_break_events WHERE thesis_id = %s", (thesis_id,)
    ).fetchall()
    assert len(events) == 1
    assert float(events[0][0]) == 75.0
    assert "price_date" in events[0][1]  # per-input evidence persisted

    # Rule 5: latest thesis now reports break_fired.
    stale = {s.instrument_id: s.reason for s in find_stale_instruments(conn, tier=None, instrument_ids=[_IID])}
    assert stale.get(_IID) == "break_fired"

    # Scan 3: UNIQUE(thesis_id, predicate_index) dedups — no second event.
    report = run_thesis_break_scan(conn)
    assert report.fired == 0
    assert _event_count(conn, thesis_id) == 1

    # Regenerated thesis (the break→re-thesis outcome): the OLD event must NOT
    # re-stale the NEW thesis — event keyed by thesis_id equality, never
    # instrument_id or fired_at (Codex ckpt-1 BLOCKING regression).
    _seed(conn, rsi=75.0)
    stale = {s.instrument_id: s.reason for s in find_stale_instruments(conn, tier=None, instrument_ids=[_IID])}
    assert stale.get(_IID) != "break_fired"

    # The new thesis's predicate baselines already_true (contemporaneous,
    # rsi=75 at first evaluation = the writer's premise) — and can never fire.
    report = run_thesis_break_scan(conn)
    assert report.fired == 0
    assert report.state_counts.get("already_true") == 1


def test_altman_sector_gate_blocks_insert(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Altman Z″ excludes financial firms (Altman 2000): a SIC-division-H
    instrument's altman condition must produce NO predicate row (spec: gate
    at insert — a permanently-unevaluable pending row would lie)."""
    conn = ebull_test_conn
    iid = 910_002
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (%s, 'FINT', 'Fin Test REIT', TRUE) ON CONFLICT (instrument_id) DO NOTHING",
        (iid,),
    )
    conn.execute(
        "INSERT INTO instrument_sec_profile (instrument_id, cik, sic) VALUES (%s, '0009100002', '6798') "
        "ON CONFLICT (instrument_id) DO NOTHING",
        (iid,),
    )
    conn.execute(
        """
        INSERT INTO theses (instrument_id, thesis_version, thesis_type, stance, memo_markdown,
                            break_conditions_json)
        VALUES (%s, 1, 'value', 'watch', 'memo',
                '["Altman Z-score falls below 1.8"]'::jsonb)
        """,
        (iid,),
    )
    conn.commit()

    report = run_thesis_break_scan(conn)
    assert report.sector_gated >= 1
    row = conn.execute("SELECT COUNT(*) FROM thesis_break_predicates WHERE instrument_id = %s", (iid,)).fetchone()
    assert row is not None and int(row[0]) == 0
