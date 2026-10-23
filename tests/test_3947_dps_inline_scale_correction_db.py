"""#3947 correction against real SQL: the re-normalise drain and the per-instrument normaliser lock.

Spec ``docs/specs/fundamentals/2026-10-21-3947-dps-inline-scale.md`` §The scan ("Draining", "Normaliser writers are
serialised") and acceptance 3 (DB tests for the job). The scan's own state machine is in
``test_3947_dps_inline_scale_scan_db.py``.
"""

from __future__ import annotations

import threading
import time
from datetime import date
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.config import settings
from app.providers.fundamentals import XbrlFact
from app.services import dps_inline_scale_scan as scan
from app.services import fundamentals
from app.services.dps_inline_scale_scan import drain_renormalise_queue, refill_and_normalise
from app.services.financial_facts_retention import sweep_retention_for_instrument
from app.services.fundamentals import NormalizationSummary, lock_instrument_for_normalize
from tests.fixtures.ebull_test_db import test_database_url
from tests.test_3947_dps_inline_scale_scan_db import ACC, _doc, _Facts, _Provider, _queue, scan_run

Conn = psycopg.Connection[Any]


# A waiter on instrument 1's normaliser lock. A bigint advisory key shows as classid = high 32 bits, objid = low 32
# bits, objsubid 1.
_WAITING_ON_INSTRUMENT_1 = (
    "SELECT count(*) FROM pg_locks l, hashtextextended('normalize_financial_periods:1', 0) AS k(key)"
    " WHERE l.locktype = 'advisory' AND NOT l.granted AND l.objsubid = 1"
    " AND l.classid::bigint = ((k.key >> 32) & 4294967295) AND l.objid::bigint = (k.key & 4294967295)"
)


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


def _facts(conn: Conn, iid: int = 1) -> list[fundamentals.FactRow]:
    rows = conn.execute(
        "SELECT concept, unit, period_start, period_end, val, frame, form_type, fiscal_year, fiscal_period,"
        " accession_number, filed_date FROM financial_facts_raw WHERE instrument_id = %s",
        (iid,),
    ).fetchall()
    return [fundamentals.FactRow(*r) for r in rows]


def _scan_then_drain(conn: Conn, monkeypatch: pytest.MonkeyPatch) -> scan.ScanResult:
    conn.commit()
    scan_run(conn, _Provider(_doc(3)))  # flag off: fills the queue only
    monkeypatch.setattr(settings, "dps_inline_scale_enabled", True)
    return scan_run(conn, _Provider(_doc(3)), drain=True)


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


def test_over_swept_raw_an_incomplete_group_keeps_its_stored_dps(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Amendment 1 A3: the label's stored Q1 has aged out of raw, so the restored Q3 may not join it.
    conn = ebull_test_conn
    _seed(conn)
    conn.execute(
        "INSERT INTO financial_facts_raw (instrument_id, concept, unit, period_start, period_end, val,"
        " accession_number, form_type, filed_date, fiscal_year, fiscal_period)"
        " VALUES (1, 'CommonStockDividendsPerShareDeclared', 'USD/shares', '2025-01-01', '2025-03-31', 380,"
        " '0001411342-25-000000', '10-Q', '2025-05-01', 2025, 'Q1')"
    )
    fundamentals.normalize_financial_periods(conn, [1], dps_inline_scale=False)
    conn.execute("DELETE FROM financial_facts_raw WHERE accession_number = '0001411342-25-000000'")  # the sweep
    conn.commit()
    assert _dps(conn) == [(2025, "Q1", Decimal("380.0000")), (2025, "Q3", Decimal("390.0000"))]

    result = _scan_then_drain(conn, monkeypatch)
    assert result.drain is not None and result.drain.normalised == 1
    assert _dps(conn) == [(2025, "Q1", Decimal("380.0000")), (2025, "Q3", Decimal("390.0000"))]
    periods, outcomes = fundamentals.derive_gated_dps_periods(conn, 1, _facts(conn))
    conn.rollback()
    assert [o.branch for o in outcomes.values()] == ["kept_stored"]
    assert [(p.period_type, p.dps_declared) for p in periods] == [("Q3", Decimal("390.0000"))]


def test_flag_off_skips_the_drain(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn)
    result = scan_run(conn, _Provider(_doc(3)))
    assert result.drain is None and "drain off" in result.digest()
    assert [i for i, _ in _queue(conn)] == [1]


def test_a_row_requeued_during_draining_survives(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    _seed(conn)
    conn.commit()
    scan_run(conn, _Provider(_doc(3)))
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
    result = drain_renormalise_queue(conn, facts_provider=_Facts(), admit_until=None)
    assert (result.normalised, result.requeued) == (1, 1)
    ((iid, revision),) = _queue(conn)
    assert iid == 1 and revision > first_revision


def test_a_failed_normalisation_counts_and_keeps_the_row(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _seed(conn)
    conn.commit()
    scan_run(conn, _Provider(_doc(3)))
    monkeypatch.setattr(settings, "dps_inline_scale_enabled", True)
    monkeypatch.setattr(
        fundamentals,
        "normalize_financial_periods",
        lambda c, ids, **_: NormalizationSummary(len(ids), 0, 0, instruments_failed=1),
    )
    for expected in (1, 2):
        result = drain_renormalise_queue(conn, facts_provider=_Facts(), admit_until=None)
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
    result = drain_renormalise_queue(conn, facts_provider=_Facts(), admit_until=1.0, clock=lambda: next(ticks))
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
                conn.execute(_WAITING_ON_INSTRUMENT_1).fetchone()[0]  # type: ignore[index]
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
    scan_run(conn, _Provider(_doc(3)))
    with pytest.raises(RuntimeError, match="off"):
        drain_renormalise_queue(conn, facts_provider=_Facts(), admit_until=None)
    with pytest.raises(RuntimeError, match="off"):
        drain_renormalise_queue(conn, facts_provider=_Facts(), admit_until=None, enabled=False)
    monkeypatch.setattr(settings, "dps_inline_scale_enabled", True)
    conn.execute("SELECT 1")  # an open transaction
    with pytest.raises(RuntimeError, match="outside a transaction"):
        drain_renormalise_queue(conn, facts_provider=_Facts(), admit_until=None)
    conn.rollback()
    assert [i for i, _ in _queue(conn)] == [1]


OLD_ACC = "0001411342-25-000000"
# The aged-out Q1 fact, as companyfacts serves it again.
Q1_FACT = XbrlFact(
    concept="CommonStockDividendsPerShareDeclared",
    taxonomy="us-gaap",
    unit="USD/shares",
    period_start=date(2025, 1, 1),
    period_end=date(2025, 3, 31),
    val=Decimal("380"),
    frame=None,
    accession_number=OLD_ACC,
    form_type="10-Q",
    filed_date=date(2025, 5, 1),
    fiscal_year=2025,
    fiscal_period="Q1",
    decimals=None,
)


def _seed_swept(conn: Conn) -> None:
    """Instrument 1 with stored Q1 and Q3 rows, whose Q1 fact has since been swept from raw."""
    _seed(conn)
    conn.execute(
        "INSERT INTO financial_facts_raw (instrument_id, concept, unit, period_start, period_end, val,"
        " accession_number, form_type, filed_date, fiscal_year, fiscal_period)"
        " VALUES (1, 'CommonStockDividendsPerShareDeclared', 'USD/shares', '2025-01-01', '2025-03-31', 380,"
        " %s, '10-Q', '2025-05-01', 2025, 'Q1')",
        (OLD_ACC,),
    )
    fundamentals.normalize_financial_periods(conn, [1], dps_inline_scale=False)
    conn.execute("DELETE FROM financial_facts_raw WHERE accession_number = %s", (OLD_ACC,))
    conn.commit()


def test_the_drain_refills_raw_so_the_group_is_complete(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    _seed_swept(conn)
    scan_run(conn, _Provider(_doc(3)))
    monkeypatch.setattr(settings, "dps_inline_scale_enabled", True)
    result = drain_renormalise_queue(conn, facts_provider=_Facts([Q1_FACT]), admit_until=None)
    assert (result.normalised, result.failed) == (1, 0)
    assert _queue(conn) == []
    # The refill brought the aged-out fact back, so the label is complete: gated, not kept as stored. Its Q1 leaf is
    # untested, so the group takes decisions-off (A3), and Q3 stays 390 rather than pairing 0.39 with Q1's 380.
    restored = conn.execute("SELECT count(*) FROM financial_facts_raw WHERE accession_number = %s", (OLD_ACC,))
    assert restored.fetchone() == (1,)
    _, outcomes = fundamentals.derive_gated_dps_periods(conn, 1, _facts(conn))
    conn.rollback()
    assert {(o.branch, o.reason) for o in outcomes.values()} == {("off", "unverified_leaf")}
    assert _dps(conn) == [(2025, "Q1", Decimal("380.0000")), (2025, "Q3", Decimal("390.0000"))]


def test_a_fetch_failure_counts_and_keeps_the_row(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    _seed(conn)
    scan_run(conn, _Provider(_doc(3)))
    monkeypatch.setattr(settings, "dps_inline_scale_enabled", True)
    result = drain_renormalise_queue(conn, facts_provider=_Facts(RuntimeError("503")), admit_until=None)
    assert (result.normalised, result.failed, result.fetch_failed) == (0, 1, 1)
    assert [i for i, _ in _queue(conn)] == [1]
    assert conn.execute("SELECT failures FROM sec_inline_scale_renormalise").fetchall() == [(1,)]
    conn.rollback()


def test_refill_and_normalise_refuses_outside_a_transaction(ebull_test_conn: Conn) -> None:
    ebull_test_conn.commit()
    with pytest.raises(RuntimeError, match="inside the caller's transaction"):
        refill_and_normalise(ebull_test_conn, 1, [], ingestion_run_id=0, dps_inline_scale=False)


def test_the_sweep_waits_for_a_refill_in_progress(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed_swept(conn)
    run_id = fundamentals.start_ingestion_run(conn, source="sec_edgar", endpoint="test", instrument_count=1)
    conn.commit()
    # A refill holds the instrument's lock until its transaction ends.
    conn.execute("SELECT 1")
    refill_and_normalise(conn, 1, [Q1_FACT], ingestion_run_id=run_id, dps_inline_scale=False)
    done = threading.Event()

    def sweep() -> None:
        with psycopg.connect(test_database_url(), autocommit=True) as worker, worker.transaction():
            sweep_retention_for_instrument(worker, instrument_id=1, keep_10q=1)
        done.set()

    thread = threading.Thread(target=sweep)
    thread.start()
    try:
        waiting = False
        with psycopg.connect(test_database_url(), autocommit=True) as observer:
            for _ in range(100):
                waiting = bool(
                    observer.execute(_WAITING_ON_INSTRUMENT_1).fetchone()[0]  # type: ignore[index]
                )
                if waiting:
                    break
                time.sleep(0.05)
        assert waiting, "the sweep did not wait on the instrument lock"
        assert not done.is_set()
        # The refill normalised over the refilled raw while the sweep waited: Q1 is derivable from raw.
        assert conn.execute(
            "SELECT count(*) FROM financial_periods_raw WHERE instrument_id = 1 AND period_type = 'Q1'"
        ).fetchone() == (1,)
    finally:
        conn.commit()
        thread.join(timeout=30)
    assert done.is_set()
    # The sweep ran after the commit: with one 10-Q kept, the older accession is gone again.
    assert conn.execute(
        "SELECT count(*) FROM financial_facts_raw WHERE accession_number = %s", (OLD_ACC,)
    ).fetchone() == (0,)


def test_an_unexpected_error_still_finishes_the_drains_ingestion_run(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _seed(conn)
    scan_run(conn, _Provider(_doc(3)))
    monkeypatch.setattr(settings, "dps_inline_scale_enabled", True)

    def explode(*_: Any, **__: Any) -> NormalizationSummary:
        raise ValueError("unexpected")

    monkeypatch.setattr(fundamentals, "normalize_financial_periods", explode)
    with pytest.raises(ValueError, match="unexpected"):
        drain_renormalise_queue(conn, facts_provider=_Facts(), admit_until=None)
    status = conn.execute(
        "SELECT status, error FROM data_ingestion_runs WHERE endpoint = '/api/xbrl/companyfacts'"
        " ORDER BY ingestion_run_id DESC LIMIT 1"
    ).fetchone()
    conn.rollback()
    assert status is not None and status[0] == "failed" and "unexpected" in status[1]
    assert [i for i, _ in _queue(conn)] == [1]  # the refill rolled back; the row stays queued


def test_the_v1_generation_serves_until_a_v2_success(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    """Amendment 2 B3 and B4.6 (DB): a v2 failure and a v2 exhaustion each keep the v1 generation active, and a
    flag-on normalisation reads its occurrences; a v2 success replaces them."""
    conn = ebull_test_conn
    _seed(conn)
    v2 = scan.PARSER_VERSION
    monkeypatch.setattr(scan, "PARSER_VERSION", "dps-inline-scale-v1")
    scan_run(conn, _Provider(_doc(3)))
    conn.execute("UPDATE sec_inline_scale_scan SET path = NULL")  # a v1 generation predates the column (sql/517)
    conn.commit()
    monkeypatch.setattr(scan, "PARSER_VERSION", v2)

    def flag_on() -> list[tuple[Any, ...]]:
        fundamentals.normalize_financial_periods(conn, [1], dps_inline_scale=True)
        conn.commit()
        return _dps(conn)

    def raw_state(result: scan.ScanResult) -> dict[str, int]:
        return result.work_state.classes["raw"]

    restored = [(2025, "Q3", Decimal("0.3900"))]  # the v1 occurrence's scale 3 restores 390 to 0.39
    assert flag_on() == restored

    result = scan_run(conn, _Provider(RuntimeError("boom")))
    assert raw_state(result) == {"done": 0, "pending": 0, "retrying": 1, "exhausted": 0}
    assert flag_on() == restored
    for _ in range(scan.MAX_ATTEMPTS - 1):
        conn.execute("UPDATE sec_inline_scale_scan SET next_attempt_at = now() - interval '1 second'")
        result = scan_run(conn, _Provider(RuntimeError("boom")))
    assert raw_state(result) == {"done": 0, "pending": 0, "retrying": 0, "exhausted": 1}
    assert conn.execute("SELECT parser_version, path FROM sec_inline_scale_scan").fetchone() == (
        "dps-inline-scale-v1",
        None,
    )
    assert flag_on() == restored

    # Re-admitted, the v2 scan finds scale 0: the stored value stands.
    conn.execute("UPDATE sec_inline_scale_scan SET attempts = 0, next_attempt_at = now()")
    result = scan_run(conn, _Provider(_doc(None)))
    assert raw_state(result) == {"done": 1, "pending": 0, "retrying": 0, "exhausted": 0}
    assert conn.execute("SELECT parser_version, path FROM sec_inline_scale_scan").fetchone() == (v2, "archive")
    assert flag_on() == [(2025, "Q3", Decimal("390.0000"))]
