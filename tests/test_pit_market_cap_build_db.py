"""#3841 slice 2: the rebuild transaction and ``instrument_market_cap_pit`` against real tables.

One test per new SQL mechanism: every reader statement runs against the real schema inside the build's REPEATABLE
READ transaction, a rebuild replaces the previous build's steps and appends its own build row, and the view's
``basis_stale`` comes from the revision COUNT, so a revision committed with a lower id than the step saw still moves it.
"""

from __future__ import annotations

import json
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import psycopg
import pytest

from app.services.pit_market_cap_build import run_market_cap_pit_rebuild
from tests.fixtures.ebull_test_db import STOCKS_TYPE_ID, seed_universe_anchor, test_database_url

Conn = psycopg.Connection[Any]
CIK = "0000003841"
NOW = datetime(2026, 10, 16, 8, 20, tzinfo=UTC)


def _seed(conn: Conn) -> None:
    seed_universe_anchor(conn)
    conn.execute("INSERT INTO exchanges (exchange_id, asset_class) VALUES ('us3841', 'us_equity')")
    for iid, symbol in ((1, "PIT"), (2, "NOREV")):
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable, exchange, instrument_type_id) "
            "VALUES (%s, %s, %s, TRUE, 'us3841', %s)",
            (iid, symbol, f"{symbol} Inc", STOCKS_TYPE_ID),
        )
    conn.execute(
        "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary) "
        "VALUES (1, 'sec', 'cik', %s, TRUE)",
        (CIK,),
    )
    conn.execute(
        "INSERT INTO financial_facts_raw (instrument_id, taxonomy, concept, unit, period_end, val, accession_number, "
        "form_type, filed_date) VALUES (1, 'dei', 'EntityCommonStockSharesOutstanding', 'shares', '2026-07-25', "
        "1000000, 'A-1', '10-Q', '2026-08-01')"
    )
    for iid in (1, 2):
        for day, close in (("2026-10-14", 10), ("2026-10-15", 11)):
            conn.execute(
                "INSERT INTO price_daily (instrument_id, price_date, close, volume) VALUES (%s, %s, %s, 1000)",
                (iid, day, close),
            )
    conn.execute(
        "INSERT INTO price_daily_revision (revision_id, instrument_id, price_date, revised_at, cause) "
        "VALUES (100, 1, '2026-10-14', %s, 'incremental')",
        (NOW,),
    )
    conn.commit()


def _submissions(tmp_path: Path) -> Path:
    path = tmp_path / "submissions.zip"
    main = {
        "cik": int(CIK),
        "filings": {
            "recent": {
                "accessionNumber": ["A-1"],
                "acceptanceDateTime": ["2026-08-01T20:30:00.000Z"],
                "form": ["10-Q"],
                "items": [""],
            },
            "files": [],
        },
    }
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(f"CIK{CIK}.json", json.dumps(main))
    return path


def _rebuild(path: Path) -> int:
    with psycopg.connect(test_database_url(), autocommit=True) as conn:
        return run_market_cap_pit_rebuild(
            conn, submissions_path=path, build_date=date(2026, 10, 16), now=lambda: NOW
        ).build_id


def _view(conn: Conn, iid: int) -> list[tuple[Any, ...]]:
    return conn.execute(
        "SELECT price_date, reason, market_cap FROM instrument_market_cap_pit WHERE instrument_id = %s ORDER BY 1",
        (iid,),
    ).fetchall()


def test_rebuild_replaces_steps_and_appends_its_build_row(ebull_test_conn: Conn, tmp_path: Path) -> None:
    conn = ebull_test_conn
    _seed(conn)
    path = _submissions(tmp_path)

    first = _rebuild(path)
    steps = conn.execute(
        "SELECT instrument_id, valid_from, shares_concept, accession, refusal, revision_count, backdated_insert_count, "
        "bars_through, build_id "
        "FROM instrument_share_count_pit"
    ).fetchall()
    # No 13F anchor exists, so the count is selected and refused, never published on an unmeasured basis.
    assert steps == [(1, date(2026, 8, 3), "cover", "A-1", "no_left_anchor", 1, 0, date(2026, 10, 15), first)]
    assert _view(conn, 1) == [
        (date(2026, 10, 14), "no_left_anchor", None),
        (date(2026, 10, 15), "no_left_anchor", None),
    ]
    # Out of scope (no SEC CIK): no step, so no view row.
    assert _view(conn, 2) == []

    second = _rebuild(path)
    assert second > first
    assert conn.execute("SELECT DISTINCT build_id FROM instrument_share_count_pit").fetchall() == [(second,)]
    builds = conn.execute(
        "SELECT build_id, status, summary -> 'instruments' FROM market_cap_pit_builds ORDER BY 1"
    ).fetchall()
    assert builds == [
        (first, "complete", {"scope": 1, "with_steps": 1}),
        (second, "complete", {"scope": 1, "with_steps": 1}),
    ]
    with pytest.raises(psycopg.errors.RaiseException):
        conn.execute("DELETE FROM market_cap_pit_builds")
    conn.rollback()


def _publish(conn: Conn, iid: int, build_id: int, revision_count: int) -> None:
    conn.execute(
        "INSERT INTO instrument_share_count_pit (instrument_id, valid_from, valid_to, shares_concept, accession, "
        "context_date, accepted_at, shares_reported, split_factor, refusal, evidence, revision_count, "
        "backdated_insert_count, bars_through, build_id) "
        "VALUES (%s, '2026-10-01', '2026-10-17', 'cover', 'A-1', '2026-07-25', %s, 1000000, 2.0, NULL, '{}', %s, 0, "
        "'2026-10-15', %s)",
        (iid, NOW, revision_count, build_id),
    )


def test_view_goes_stale_on_a_revision_committed_below_the_max_id(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn)
    conn.execute(
        "INSERT INTO market_cap_pit_builds (build_id, started_at, finished_at, status, build_date, rule_version, "
        "submissions_sha256, summary) VALUES (1, %s, %s, 'complete', '2026-10-16', 'r', 's', '{}')",
        (NOW, NOW),
    )
    _publish(conn, 1, 1, revision_count=1)
    # Instrument 2 has no revision rows: a count of zero matches and publishes.
    _publish(conn, 2, 1, revision_count=0)
    conn.commit()
    assert _view(conn, 1) == [(date(2026, 10, 14), None, 20_000_000.0), (date(2026, 10, 15), None, 22_000_000.0)]
    assert _view(conn, 2) == [(date(2026, 10, 14), None, 20_000_000.0), (date(2026, 10, 15), None, 22_000_000.0)]

    # revision_id is not commit-ordered: a row committed now with an id below the existing max still counts.
    conn.execute(
        "INSERT INTO price_daily_revision (revision_id, instrument_id, price_date, revised_at, cause) "
        "VALUES (5, 1, '2026-10-15', %s, 'incremental')",
        (NOW,),
    )
    conn.commit()
    assert [row[1] for row in _view(conn, 1)] == ["basis_stale", "basis_stale"]
    assert [row[2] for row in _view(conn, 1)] == [None, None]
    assert [row[1] for row in _view(conn, 2)] == [None, None]


def test_view_refuses_a_bar_the_build_never_saw(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn)
    conn.execute(
        "INSERT INTO market_cap_pit_builds (build_id, started_at, finished_at, status, build_date, rule_version, "
        "submissions_sha256, summary) VALUES (1, %s, %s, 'complete', '2026-10-16', 'r', 's', '{}')",
        (NOW, NOW),
    )
    _publish(conn, 2, 1, revision_count=0)
    # A forward bar after the build's last bar (2026-10-15) writes no revision row; it is still refused.
    conn.execute("INSERT INTO price_daily (instrument_id, price_date, close, volume) VALUES (2, '2026-10-16', 12, 1)")
    conn.commit()
    assert [row[1] for row in _view(conn, 2)] == [None, None, "basis_stale"]
    # A bar inserted below the frontier writes a backdated-insert row, not a revision; that moves every session.
    conn.execute(
        "INSERT INTO price_daily_backdated_insert (instrument_id, price_date, frontier_before, inserted_at, cause) "
        "VALUES (2, '2026-10-02', '2026-10-15', %s, 'incremental')",
        (NOW,),
    )
    conn.commit()
    assert [row[1] for row in _view(conn, 2)] == ["basis_stale", "basis_stale", "basis_stale"]
