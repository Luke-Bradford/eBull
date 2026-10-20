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

from app.services.pit_market_cap_build import _TRADES_SQL, _UNIT_VALUES_SQL, run_market_cap_pit_rebuild
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


def test_trades_reader_keeps_only_the_instruments_own_issuer(ebull_test_conn: Conn) -> None:
    """Gate 1 reads a Form 4 only when its issuer is one of the instrument's SEC CIKs: a Form 4 the issuer filed as a
    reporting owner of ANOTHER issuer is attributed to it by ``insider_transactions`` (BAC's PIMCO fund trades), and
    must not test its price basis; a non-primary CIK (an (a)(3) predecessor) still counts."""
    conn = ebull_test_conn
    _seed(conn)
    conn.execute(
        "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary) "
        "VALUES (1, 'sec', 'cik', '0000000077', FALSE)"
    )
    # Own issuer at the primary CIK, own issuer at the predecessor CIK, and another issuer the instrument filed on.
    for accession, issuer in (("F-OWN", CIK), ("F-PRED", "77"), ("F-OTHER", "0000908993")):
        conn.execute(
            "INSERT INTO insider_filings (accession_number, instrument_id, document_type, issuer_cik, "
            "period_of_report) VALUES (%s, 1, '4', %s, '2026-09-01')",
            (accession, issuer),
        )
        conn.execute(
            "INSERT INTO insider_transactions (instrument_id, accession_number, txn_row_num, filer_name, txn_date, "
            "txn_code, price, security_title) VALUES (1, %s, 1, 'Owner', '2026-09-01', 'P', 10, 'Common Stock')",
            (accession,),
        )
    conn.commit()

    rows = conn.execute(
        _TRADES_SQL, {"ids": [1], "first": date(2024, 6, 1), "report_floor": date(2023, 6, 1)}
    ).fetchall()
    assert sorted(row[1] for row in rows) == ["F-OWN", "F-PRED"]


def test_unit_values_reader_selects_the_latest_active_economic_report_then_qualifies(ebull_test_conn: Conn) -> None:
    """Spec Amendment 2 (#3908): the anchors read ``ownership_institutions_observations``. Superseded rows and voting
    overlays are out before selection; the latest report is chosen over every exposure; only then do its EQUITY rows
    qualify, so a newer options-only report removes the filer instead of reviving its older equity row."""
    conn = ebull_test_conn
    _seed(conn)
    quarter = date(2026, 3, 31)
    rows = (
        # (filer, accession, filed, exposure, shares, value, nature, known_to). Filer 12's latest report holds
        # no equity; filer 14's row is a voting overlay, not a position.
        (
            "0000000011",
            "0000000011-26-000001",
            "2026-05-01",
            "EQUITY",
            100,
            1000,
            "economic",
            None,
        ),  # replaced by its amendment
        ("0000000011", "0000000011-26-000002", "2026-05-10", "EQUITY", 100, 1200, "economic", None),
        ("0000000012", "0000000012-26-000001", "2026-05-01", "EQUITY", 100, 1000, "economic", None),
        (
            "0000000012",
            "0000000012-26-000002",
            "2026-05-10",
            "PUT",
            50,
            600,
            "economic",
            None,
        ),  # latest report holds no equity
        ("0000000013", "0000000013-26-000001", "2026-05-01", "EQUITY", 100, 1000, "economic", None),
        ("0000000013", "0000000013-26-000009", "2026-05-20", "EQUITY", 100, 9900, "economic", NOW),  # superseded
        (
            "0000000014",
            "0000000014-26-000001",
            "2026-05-01",
            "EQUITY",
            100,
            5000,
            "voting",
            None,
        ),  # overlay, not a position
    )
    for filer, accession, filed, exposure, shares, value, nature, known_to in rows:
        conn.execute(
            "INSERT INTO ownership_institutions_observations (instrument_id, filer_cik, filer_name, ownership_nature, "
            "source, source_document_id, source_accession, filed_at, period_end, known_to, ingest_run_id, shares, "
            "market_value_usd, exposure_kind) VALUES (1, %s, %s, %s, '13f', %s, %s, %s, %s, %s, gen_random_uuid(), "
            "%s, %s, %s)",
            (filer, f"Filer {filer}", nature, accession, accession, filed, quarter, known_to, shares, value, exposure),
        )
    conn.commit()

    got = {
        filer: (unit_value, accessions)
        for _iid, _q, filer, unit_value, accessions, _filed in conn.execute(
            _UNIT_VALUES_SQL, {"ids": [1], "quarters": [quarter]}
        ).fetchall()
    }
    assert got == {
        "0000000011": (12.0, ["0000000011-26-000002"]),
        "0000000013": (10.0, ["0000000013-26-000001"]),
    }
