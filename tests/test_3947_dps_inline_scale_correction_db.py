"""#3947 correction against real SQL: the re-normalise drain and the per-instrument normaliser lock.

Spec ``docs/specs/fundamentals/2026-10-21-3947-dps-inline-scale.md`` §The scan ("Draining", "Normaliser writers are
serialised") and acceptance 3 (DB tests for the job). The scan's own state machine is in
``test_3947_dps_inline_scale_scan_db.py``.
"""

from __future__ import annotations

import threading
import time
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.config import settings
from app.services import dps_inline_scale_scan as scan
from app.services import fundamentals
from app.services.dps_inline_scale_scan import drain_renormalise_queue, run_dps_inline_scale_scan
from app.services.fundamentals import NormalizationSummary, lock_instrument_for_normalize
from tests.fixtures.ebull_test_db import test_database_url
from tests.test_3947_dps_inline_scale_scan_db import ACC, TODAY, _doc, _Provider, _queue

Conn = psycopg.Connection[Any]


def _seed(conn: Conn, iid: int = 1) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
        (iid, f"S{iid}", f"S{iid} Inc"),
    )
    conn.execute(
        "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary)"
        " VALUES (%s, 'sec', 'cik', '0001411342', TRUE) ON CONFLICT DO NOTHING",
        (iid,),
    )
    conn.execute(
        "INSERT INTO financial_facts_raw (instrument_id, concept, unit, period_start, period_end, val,"
        " accession_number, form_type, filed_date, fiscal_year, fiscal_period)"
        " VALUES (%s, 'CommonStockDividendsPerShareDeclared', 'USD/shares', '2025-07-01', '2025-09-30', 390, %s,"
        " '10-Q', '2025-11-01', 2025, 'Q3')",
        (iid, ACC),
    )
    conn.commit()


def _dps(conn: Conn, iid: int = 1) -> list[tuple[Any, ...]]:
    return conn.execute(
        "SELECT fiscal_year, period_type, dps_declared FROM financial_periods WHERE instrument_id = %s"
        " ORDER BY period_end_date",
        (iid,),
    ).fetchall()


def _scan_then_drain(conn: Conn, monkeypatch: pytest.MonkeyPatch) -> scan.ScanResult:
    conn.commit()
    run_dps_inline_scale_scan(conn, _Provider(_doc(3)), today=TODAY)  # flag off: fills the queue only
    monkeypatch.setattr(settings, "dps_inline_scale_enabled", True)
    return run_dps_inline_scale_scan(conn, _Provider(_doc(3)), today=TODAY, drain=True)


def test_drain_restores_the_displayed_value_and_empties_the_queue(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _seed(conn)
    fundamentals.normalize_financial_periods(conn, [1], dps_inline_scale=False)
    conn.commit()
    assert _dps(conn) == [(2025, "Q3", Decimal("390.0000"))]

    result = _scan_then_drain(conn, monkeypatch)
    assert result.drain is not None
    assert (result.drain.normalised, result.drain.failed, result.drain.requeued) == (1, 0, 0)
    assert _dps(conn) == [(2025, "Q3", Decimal("0.3900"))]
    assert _queue(conn) == []
    assert "drained 1" in result.digest()

    # Rollback step 3: with the flag off, a re-normalisation reads the stored value again.
    fundamentals.normalize_financial_periods(conn, [1], dps_inline_scale=False)
    conn.commit()
    assert _dps(conn) == [(2025, "Q3", Decimal("390.0000"))]


def test_flag_off_skips_the_drain(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn)
    result = run_dps_inline_scale_scan(conn, _Provider(_doc(3)), today=TODAY)
    assert result.drain is None and "drain off" in result.digest()
    assert [i for i, _ in _queue(conn)] == [1]


def test_a_row_requeued_during_draining_survives(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    _seed(conn)
    conn.commit()
    run_dps_inline_scale_scan(conn, _Provider(_doc(3)), today=TODAY)
    ((_, first_revision),) = _queue(conn)
    conn.commit()
    monkeypatch.setattr(settings, "dps_inline_scale_enabled", True)
    real = fundamentals.normalize_financial_periods

    def normalise_then_requeue(c: Conn, ids: list[int], **kwargs: Any) -> NormalizationSummary:
        summary = real(c, ids, **kwargs)
        # A scan publishing meanwhile takes a new revision for the same instrument.
        with psycopg.connect(test_database_url(), autocommit=True) as other:
            other.execute(
                "UPDATE sec_inline_scale_renormalise SET revision = nextval('sec_inline_scale_renormalise_revision')"
            )
        return summary

    monkeypatch.setattr(fundamentals, "normalize_financial_periods", normalise_then_requeue)
    result = drain_renormalise_queue(conn, admit_until=None)
    assert (result.normalised, result.requeued) == (1, 1)
    ((iid, revision),) = _queue(conn)
    assert iid == 1 and revision > first_revision


def test_a_failed_normalisation_counts_and_keeps_the_row(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _seed(conn)
    conn.commit()
    run_dps_inline_scale_scan(conn, _Provider(_doc(3)), today=TODAY)
    monkeypatch.setattr(settings, "dps_inline_scale_enabled", True)
    monkeypatch.setattr(
        fundamentals,
        "normalize_financial_periods",
        lambda c, ids, **_: NormalizationSummary(len(ids), 0, 0, instruments_failed=1),
    )
    for expected in (1, 2):
        result = drain_renormalise_queue(conn, admit_until=None)
        assert (result.normalised, result.failed) == (0, 1)
        row = conn.execute("SELECT instrument_id, failures FROM sec_inline_scale_renormalise").fetchall()
        conn.commit()
        assert row == [(1, expected)]


def test_no_instrument_is_started_after_the_admission_deadline(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    monkeypatch.setattr(settings, "dps_inline_scale_enabled", True)
    _seed(conn, 1)
    _seed(conn, 2)
    conn.execute(
        "INSERT INTO sec_inline_scale_renormalise (instrument_id, revision)"
        " SELECT i, nextval('sec_inline_scale_renormalise_revision') FROM unnest(ARRAY[1, 2]) AS i"
    )
    conn.commit()
    ticks = iter([0.0, 5.0])
    result = drain_renormalise_queue(conn, admit_until=1.0, clock=lambda: next(ticks))
    assert (result.normalised, result.deferred) == (1, 1)
    # Revision order: instrument 1 was queued first and drained; 2 stays.
    assert [i for i, _ in _queue(conn)] == [2]


def test_two_normalisations_of_one_instrument_serialise(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn)
    fundamentals.normalize_financial_periods(conn, [1], dps_inline_scale=False)
    conn.commit()
    before = _dps(conn)

    holder = psycopg.connect(test_database_url())
    lock_instrument_for_normalize(holder, 1)
    done = threading.Event()

    def normalise() -> None:
        with psycopg.connect(test_database_url(), autocommit=True) as worker:
            fundamentals.normalize_financial_periods(worker, [1], dps_inline_scale=False)
        done.set()

    thread = threading.Thread(target=normalise)
    thread.start()
    try:
        waiting = False
        for _ in range(100):
            waiting = bool(
                conn.execute(
                    # A bigint advisory key shows as classid = high 32 bits, objid = low 32 bits, objsubid 1.
                    "SELECT count(*) FROM pg_locks l, hashtextextended('normalize_financial_periods:1', 0) AS k(key)"
                    " WHERE l.locktype = 'advisory' AND NOT l.granted AND l.objsubid = 1"
                    " AND l.classid::bigint = ((k.key >> 32) & 4294967295) AND l.objid::bigint = (k.key & 4294967295)"
                ).fetchone()[0]  # type: ignore[index]
            )
            conn.rollback()
            if waiting:
                break
            time.sleep(0.05)
        assert waiting, "the second normalisation did not wait on the instrument lock"
        assert not done.is_set()
    finally:
        holder.rollback()
        holder.close()
        thread.join(timeout=30)
    assert done.is_set()
    assert _dps(conn) == before


def test_the_lock_key_takes_a_bigint_id(ebull_test_conn: Conn) -> None:
    lock_instrument_for_normalize(ebull_test_conn, 3_000_000_000)  # past int4: must not raise
    ebull_test_conn.rollback()


def test_the_drain_refuses_with_the_flag_off_or_inside_a_transaction(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _seed(conn)
    run_dps_inline_scale_scan(conn, _Provider(_doc(3)), today=TODAY)
    with pytest.raises(RuntimeError, match="off"):
        drain_renormalise_queue(conn, admit_until=None)
    with pytest.raises(RuntimeError, match="off"):
        drain_renormalise_queue(conn, admit_until=None, enabled=False)
    monkeypatch.setattr(settings, "dps_inline_scale_enabled", True)
    conn.execute("SELECT 1")  # an open transaction
    with pytest.raises(RuntimeError, match="outside a transaction"):
        drain_renormalise_queue(conn, admit_until=None)
    conn.rollback()
    assert [i for i, _ in _queue(conn)] == [1]
