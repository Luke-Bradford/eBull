"""#3947 Amendment 1 against real SQL: the durable population, discovery, candidates and the queue ledger.

Spec ``docs/specs/fundamentals/2026-10-21-3947-dps-inline-scale.md`` §Amendment 1 (A1, A4) and its fixture list
("Population and discovery writes", the discovery and candidate enqueues, the partial write, the ledger). The group
gating, ``refill_and_normalise`` and the sweep lock arrive with build PR (d).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.config import settings
from app.providers.fundamentals import XbrlFact
from app.services import dps_inline_scale_scan as scan
from app.services import fundamentals
from app.services.fundamentals import NormalizationSummary, start_ingestion_run, upsert_facts_for_instrument
from tests.test_3947_dps_inline_scale_scan_db import ACC, TODAY, _doc, _Facts, _Provider, _queue, _seed, scan_run

Conn = psycopg.Connection[Any]
OTHER = "0001411342-24-000002"


def _instrument(conn: Conn, iid: int, *, cik: str | None = "0001411342") -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)"
        " ON CONFLICT DO NOTHING",
        (iid, f"S{iid}", f"S{iid} Inc"),
    )
    if cik is not None:
        conn.execute(
            "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary)"
            " VALUES (%s, 'sec', 'cik', %s, TRUE) ON CONFLICT DO NOTHING",
            (iid, cik),
        )


def _fact(accession: str, *, concept: str = scan.CONCEPT, filed: date = date(2025, 11, 1)) -> XbrlFact:
    return XbrlFact(
        concept=concept,
        taxonomy="us-gaap",
        unit="USD/shares",
        period_start=date(2025, 7, 1),
        period_end=date(2025, 9, 30),
        val=Decimal("0.39"),
        frame=None,
        accession_number=accession,
        form_type="10-Q",
        filed_date=filed,
        fiscal_year=2025,
        fiscal_period="Q3",
        decimals="2",
    )


def _upsert(conn: Conn, iid: int, facts: list[XbrlFact], *, complete_payload: bool) -> None:
    with conn.transaction():
        run_id = start_ingestion_run(conn, source="sec_edgar", endpoint="test")
        upsert_facts_for_instrument(
            conn, instrument_id=iid, facts=facts, ingestion_run_id=run_id, complete_payload=complete_payload
        )
    conn.commit()


def _population(conn: Conn) -> list[tuple[Any, ...]]:
    return conn.execute(
        "SELECT accession_number, instrument_id, filed_date FROM sec_inline_scale_population ORDER BY 1, 2"
    ).fetchall()


def _discovery(conn: Conn, iid: int) -> tuple[Any, ...] | None:
    return conn.execute(
        "SELECT discovered_at IS NOT NULL, attempts, next_attempt_at IS NOT NULL, last_error IS NOT NULL"
        " FROM sec_inline_scale_discovery WHERE instrument_id = %s",
        (iid,),
    ).fetchone()


def _mark_drained(conn: Conn) -> None:
    conn.execute("UPDATE sec_inline_scale_renormalise SET drained_revision = revision")
    conn.commit()


def test_upsert_records_population_and_only_a_complete_payload_discovers(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _instrument(conn, 1)
    _instrument(conn, 2)
    _upsert(conn, 1, [_fact(ACC), _fact(OTHER, concept="Revenues")], complete_payload=True)
    _upsert(conn, 2, [_fact(OTHER, filed=date(2024, 5, 1))], complete_payload=False)
    # Only accessions holding the concept are members, each with its facts' filed date.
    assert _population(conn) == [(OTHER, 2, date(2024, 5, 1)), (ACC, 1, date(2025, 11, 1))]
    assert _discovery(conn, 1) == (True, 0, False, False)
    # A partial write records population rows but never marks discovery.
    assert _discovery(conn, 2) is None


def test_seed_from_source_ref_is_insert_only_and_fills_a_date_once_known(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _instrument(conn, 1)
    conn.execute(
        "INSERT INTO financial_periods (instrument_id, period_end_date, period_type, fiscal_year, source, source_ref,"
        " reported_currency, dps_declared, superseded_at, normalization_status)"
        " VALUES (1, '2025-09-30', 'Q3', 2025, 'sec_edgar', %s, 'USD', 0.39, NULL, 'normalized')",
        (f"{ACC}, {OTHER}",),
    )
    # ACC dates from raw (any concept); OTHER has no date source yet.
    conn.execute(
        "INSERT INTO financial_facts_raw (instrument_id, concept, unit, period_end, val, accession_number, form_type,"
        " filed_date) VALUES (1, 'Revenues', 'USD', '2025-09-30', 1, %s, '10-Q', '2025-11-01')",
        (ACC,),
    )
    conn.commit()
    assert scan.seed_population(conn) == 2
    assert _population(conn) == [(OTHER, 1, None), (ACC, 1, date(2025, 11, 1))]
    assert _discovery(conn, 1) == (False, 0, False, False)  # a population instrument gets a discovery row

    # A reselection drops OTHER from source_ref; a date for it arrives. Membership stays, the date is filled.
    conn.execute("UPDATE financial_periods SET source_ref = %s", (ACC,))
    conn.execute(
        "INSERT INTO financial_facts_raw (instrument_id, concept, unit, period_end, val, accession_number, form_type,"
        " filed_date) VALUES (1, 'Revenues', 'USD', '2024-03-31', 1, %s, '10-Q', '2024-05-01')",
        (OTHER,),
    )
    conn.commit()
    scan.seed_population(conn)
    conn.execute("DELETE FROM financial_facts_raw WHERE accession_number = %s", (OTHER,))
    conn.commit()
    assert scan.seed_population(conn) == 0
    assert _population(conn) == [(OTHER, 1, date(2024, 5, 1)), (ACC, 1, date(2025, 11, 1))]


def test_new_membership_of_a_scanned_positive_accession_enqueues(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn)
    scan_run(conn, _Provider(_doc(3)))
    assert [i for i, _ in _queue(conn)] == [1]
    _mark_drained(conn)

    # Instrument 2 joins the population through an unrelated accession; discovery then attaches it to ACC.
    _instrument(conn, 2)
    _upsert(conn, 2, [_fact(OTHER, filed=date(2024, 5, 1))], complete_payload=False)
    facts = _Facts([_fact(ACC), _fact(OTHER, filed=date(2024, 5, 1))])
    result = scan_run(conn, _Provider(_doc(0)), facts_provider=facts)
    assert result.discovery is not None and result.discovery.discovered == 1
    assert facts.calls == ["0001411342"]
    assert (ACC, 2, date(2025, 11, 1)) in _population(conn)
    assert _discovery(conn, 2) == (True, 0, False, False)
    # Instrument 2 is queued; instrument 1, already drained and unchanged, is not.
    assert [i for i, _ in _queue(conn)] == [2]


def test_an_unscaled_first_scan_on_a_candidate_enqueues_it(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn)
    scan_run(conn, _Provider(_doc(3)))
    _mark_drained(conn)
    # A second accession of the candidate, untested until now, scans unscaled: neither generation holds a
    # positive-scale occurrence, but the instrument is a candidate, so a group may unlock.
    _seed(conn, accession=OTHER, filed=date(2024, 5, 1))
    result = scan_run(conn, _Provider(_doc(0)))
    assert (result.succeeded, result.instruments_enqueued) == (1, 1)
    assert [i for i, _ in _queue(conn)] == [1]


def test_the_drain_keeps_the_row_as_a_ledger_and_a_reenqueue_queues_it_again(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _seed(conn)
    scan_run(conn, _Provider(_doc(3)))
    monkeypatch.setattr(settings, "dps_inline_scale_enabled", True)
    monkeypatch.setattr(
        fundamentals, "normalize_financial_periods", lambda c, ids, **_: NormalizationSummary(len(ids), 0, 0)
    )
    result = scan.drain_renormalise_queue(conn, facts_provider=_Facts(), admit_until=None)
    assert result.normalised == 1 and _queue(conn) == []
    ledger = conn.execute("SELECT instrument_id, revision = drained_revision FROM sec_inline_scale_renormalise")
    assert ledger.fetchall() == [(1, True)]
    conn.commit()

    monkeypatch.setattr(scan, "PARSER_VERSION", "v-next")
    scan_run(conn, _Provider(_doc(0)))  # the old generation's positive scale queues the reversal
    assert [i for i, _ in _queue(conn)] == [1]
    assert scan_run(conn, _Provider(_doc(0))).work_state.queue == 1


def test_discovery_backs_off_and_exhausts(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn)
    _instrument(conn, 2, cik=None)
    conn.execute("INSERT INTO sec_inline_scale_population VALUES (%s, 2, '2025-11-01')", (ACC,))
    conn.commit()
    facts = _Facts(RuntimeError("companyfacts 503"))
    result = scan_run(conn, _Provider(_doc(0)), facts_provider=facts)
    assert result.discovery is not None and (result.discovery.discovered, result.discovery.failed) == (0, 2)
    assert facts.calls == ["0001411342"]  # instrument 2 has no CIK: it fails before any request
    assert _discovery(conn, 1) == (False, 1, True, True)
    hours = conn.execute(
        "SELECT round(extract(epoch FROM next_attempt_at - now()) / 3600) FROM sec_inline_scale_discovery"
        " WHERE instrument_id = 1"
    ).fetchone()
    assert hours is not None and hours[0] == 2
    # Inside the backoff: no request.
    scan_run(conn, _Provider(_doc(0)), facts_provider=facts)
    assert len(facts.calls) == 1

    for _ in range(scan.MAX_ATTEMPTS - 1):
        conn.execute("UPDATE sec_inline_scale_discovery SET next_attempt_at = now() - interval '1 second'")
        conn.commit()
        result = scan_run(conn, _Provider(_doc(0)), facts_provider=facts)
    assert len(facts.calls) == scan.MAX_ATTEMPTS
    assert result.work_state.discovery == {"discovered": 0, "pending": 0, "retrying": 0, "exhausted": 2}


def test_an_undated_accession_is_counted_not_scanned_and_its_generation_stays(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _instrument(conn, 1)
    conn.execute("INSERT INTO sec_inline_scale_population VALUES (%s, 1, NULL)", (OTHER,))
    conn.execute(
        "INSERT INTO sec_inline_scale_scan (accession_number, outcome, documents, parser_version, scanned_at)"
        " VALUES (%s, 'inline', 1, 'an-earlier-version', now())",
        (OTHER,),
    )
    conn.commit()
    provider = _Provider(_doc(3))
    result = scan_run(conn, provider)
    assert (result.attempted, provider.fetched, result.work_state.undated) == (0, 0, 1)
    row = conn.execute("SELECT parser_version FROM sec_inline_scale_scan WHERE accession_number = %s", (OTHER,))
    assert row.fetchone() == ("an-earlier-version",)


def test_order_is_raw_then_candidate_history_then_history(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    raw, candidate_history, history, positive = (
        "0001411342-20-000003",
        "0001411342-20-000002",
        "0001411342-20-000001",
        "0001411342-21-000001",
    )
    _seed(conn, accession=raw, filed=date(2020, 5, 3))
    for iid in (2, 3):
        _instrument(conn, iid)
    conn.execute(
        "INSERT INTO sec_inline_scale_population VALUES (%s, 2, '2020-05-02'), (%s, 3, '2020-05-01'),"
        " (%s, 2, '2021-05-01')",
        (candidate_history, history, positive),
    )
    # Instrument 2 is a candidate: its scanned accession holds a positive-scale occurrence.
    conn.execute(
        "INSERT INTO sec_inline_scale_scan (accession_number, outcome, documents, parser_version, scanned_at,"
        " attempt_version) VALUES (%s, 'inline', 1, %s, now(), %s)",
        (positive, scan.PARSER_VERSION, scan.PARSER_VERSION),
    )
    conn.execute(
        "INSERT INTO sec_inline_dps_occurrence (accession_number, document, element_ordinal, display_text, unresolved,"
        " scale) VALUES (%s, 'a.htm', 0, '0.39', 'context_ref', 3)",
        (positive,),
    )
    # A newer raw accession of the candidate stays behind the older raw one: candidacy orders history only.
    _seed(conn, accession="0001411342-20-000004", filed=date(2020, 5, 4), iid=2)
    conn.commit()
    order = [t.accession_number for t in scan.select_targets(conn, today=TODAY)]
    # Oldest first would be the reverse; the class order wins.
    assert order == [raw, "0001411342-20-000004", candidate_history, history]
    state = scan.work_state(conn)
    assert state.classes["raw"]["pending"] == 2
    assert state.classes["history"]["pending"] == 2 and state.classes["history"]["done"] == 1
    assert state.classes["history_candidate"]["pending"] == 1


def test_the_ab_coverage_seed_is_undone_by_the_snapshot_rollback(ebull_test_conn: Conn) -> None:
    from scripts.ab_3947_dps_inline_scale import _coverage

    conn = ebull_test_conn
    _instrument(conn, 1)
    conn.execute(
        "INSERT INTO financial_facts_raw (instrument_id, concept, unit, period_start, period_end, val,"
        " accession_number, form_type, filed_date)"
        " VALUES (1, %s, 'USD/shares', '2025-07-01', '2025-09-30', 390, %s, '10-Q', '2025-11-01')",
        (scan.CONCEPT, ACC),
    )
    # An already-scanned positive-scale accession, so the seed's new membership also writes the queue.
    conn.execute(
        "INSERT INTO sec_inline_scale_scan (accession_number, outcome, documents, parser_version, scanned_at,"
        " attempt_version) VALUES (%s, 'inline', 1, 'an-earlier-version', now(), 'an-earlier-version')",
        (ACC,),
    )
    conn.execute(
        "INSERT INTO sec_inline_dps_occurrence (accession_number, document, element_ordinal, display_text, unresolved,"
        " scale) VALUES (%s, 'a.htm', 0, '0.39', 'context_ref', 3)",
        (ACC,),
    )
    conn.commit()

    def counts() -> tuple[Any, ...] | None:
        return conn.execute(
            "SELECT (SELECT count(*) FROM sec_inline_scale_population),"
            " (SELECT count(*) FROM sec_inline_scale_discovery), (SELECT count(*) FROM sec_inline_scale_renormalise)"
        ).fetchone()

    conn.execute("SELECT 1")  # the A/B's snapshot: seed_population's transaction is a savepoint inside it
    coverage = _coverage(conn)
    # An unseeded table does not read as full coverage: the seed ran inside the snapshot, and wrote all three tables.
    assert coverage["incomplete"] and coverage["work_state"]["raw"]["pending"] == 1
    assert counts() == (1, 1, 1)
    conn.rollback()
    assert counts() == (0, 0, 0)


def test_the_run_refuses_an_open_transaction_and_a_discovery_write_error_neither_burns_nor_stops(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _seed(conn)
    conn.execute("SELECT 1")  # an open transaction: fetches would run inside it
    with pytest.raises(RuntimeError, match="outside a transaction"):
        scan.run_dps_inline_scale_scan(conn, _Provider(_doc(0)), facts_provider=_Facts(), today=TODAY)
    conn.rollback()

    def broken_write(*_: Any, **__: Any) -> tuple[int, int]:
        raise psycopg.errors.QueryCanceled("statement timeout")

    monkeypatch.setattr(fundamentals, "upsert_facts_for_instrument", broken_write)
    result = scan_run(conn, _Provider(_doc(0)))
    # Counted, no attempt burned, the ingestion run closed, and the run still scanned.
    assert result.discovery is not None and (result.discovery.write_failed, result.discovery.failed) == (1, 0)
    assert _discovery(conn, 1) == (False, 0, False, False)
    assert result.succeeded == 1
    status = conn.execute("SELECT status FROM data_ingestion_runs ORDER BY ingestion_run_id DESC LIMIT 1").fetchone()
    assert status == ("failed",)
