"""#3947 ``sec_dps_inline_scale_scan`` against real SQL (spec §The scan; acceptance 3, DB tests for the job).

These cover the scan's state machine and queue; draining and the normaliser lock are in
``test_3947_dps_inline_scale_correction_db.py``.
"""

from __future__ import annotations

import io
import re
import zipfile
from datetime import date, timedelta
from typing import Any

import psycopg
import pytest

from app.services import dps_inline_scale_scan as scan
from app.services.dps_inline_scale_scan import run_dps_inline_scale_scan, select_targets

Conn = psycopg.Connection[Any]
TODAY = date(2026, 10, 21)
ACC = "0001411342-25-000001"


def _doc(scale: int | None) -> str:
    attr = "" if scale is None else f' scale="{scale}"'
    return (
        '<html xmlns="http://www.w3.org/1999/xhtml" xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"'
        ' xmlns:xbrli="http://www.xbrl.org/2003/instance" xmlns:iso4217="http://www.xbrl.org/2003/iso4217"'
        ' xmlns:us-gaap="http://fasb.org/us-gaap/2025"><body><ix:header><ix:resources>'
        '<xbrli:context id="c1"><xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">1411342'
        "</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:startDate>2025-07-01</xbrli:startDate>"
        "<xbrli:endDate>2025-09-30</xbrli:endDate></xbrli:period></xbrli:context>"
        '<xbrli:unit id="u"><xbrli:divide><xbrli:unitNumerator><xbrli:measure>iso4217:USD</xbrli:measure>'
        "</xbrli:unitNumerator><xbrli:unitDenominator><xbrli:measure>xbrli:shares</xbrli:measure>"
        "</xbrli:unitDenominator></xbrli:divide></xbrli:unit></ix:resources></ix:header>"
        f'<ix:nonFraction name="us-gaap:CommonStockDividendsPerShareDeclared" contextRef="c1" unitRef="u"{attr}>'
        "0.39</ix:nonFraction></body></html>"
    )


# The SGML header page (``-index-headers.html``): one ``<DOCUMENT>`` record typing ``a.htm`` (Amendment 2 B1.3).
_HEADER = "&lt;DOCUMENT&gt;\n&lt;TYPE&gt;10-Q\n&lt;FILENAME&gt;a.htm\n&lt;/DOCUMENT&gt;\n"
_ZIP_ACCESSION = re.compile(r"([0-9]{10}-[0-9]{2}-[0-9]{6})-xbrl\.zip$")


def _zip(body: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("a.htm", body.encode())
    return buffer.getvalue()


class _Provider:
    """Every accession's ``-xbrl.zip`` holds one ``a.htm``, typed ``10-Q`` by the header. ``body`` is its text, or a
    dict of it by accession: ``None`` 404s the archive and the listed ``a.htm``; an exception is raised by the archive
    request. ``archive=False`` 404s every archive, so the listing path serves ``a.htm``. ``fetched`` counts byte
    requests."""

    def __init__(
        self, body: str | None | Exception | dict[str, str | None | Exception], *, archive: bool = True
    ) -> None:
        self.body = body
        self.archive = archive
        self.fetched = 0

    def fetch_filing_index(self, provider_filing_id: str, *, issuer_cik: str | None = None) -> dict[str, object] | None:
        return {"directory": {"item": [{"name": "a.htm", "size": "1"}]}}

    def fetch_document_text(self, absolute_url: str) -> str | None:
        return _HEADER if absolute_url.endswith("-index-headers.html") else None

    def fetch_document_bytes(self, absolute_url: str, *, max_bytes: int) -> bytes | None:
        self.fetched += 1
        archive = _ZIP_ACCESSION.search(absolute_url)
        if archive and not self.archive:
            return None
        body = self.body
        if isinstance(body, dict):
            # The archive URL names the accession; a listed file sits under its dashless folder.
            key = (archive.group(1) if archive else absolute_url.split("/")[-2]).replace("-", "")
            body = next(v for k, v in body.items() if k.replace("-", "") == key)
        if isinstance(body, Exception):
            raise body
        if body is None:
            return None
        return _zip(body) if archive else body.encode()


class _Facts:
    """The companyfacts fetch for discovery: ``facts`` for every CIK (default none), or an exception raised."""

    def __init__(self, facts: list[Any] | Exception | None = None) -> None:
        self.facts = facts
        self.calls: list[str] = []

    def extract_facts_and_catalog(self, symbol: str, cik: str) -> tuple[list[Any], list[object]]:
        self.calls.append(cik)
        if isinstance(self.facts, Exception):
            raise self.facts
        return list(self.facts or []), []


def scan_run(conn: Conn, provider: _Provider, **kwargs: Any) -> scan.ScanResult:
    kwargs.setdefault("facts_provider", _Facts())
    kwargs.setdefault("today", TODAY)
    conn.commit()  # the run refuses an open transaction
    return run_dps_inline_scale_scan(conn, provider, **kwargs)


def _seed(conn: Conn, *, accession: str = ACC, filed: date = date(2025, 11, 1), iid: int = 1) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)"
        " ON CONFLICT DO NOTHING",
        (iid, f"S{iid}", f"S{iid} Inc"),
    )
    conn.execute(
        "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary)"
        " VALUES (%s, 'sec', 'cik', '0001411342', TRUE) ON CONFLICT DO NOTHING",
        (iid,),
    )
    conn.execute(
        "INSERT INTO financial_facts_raw (instrument_id, concept, unit, period_start, period_end, val,"
        " accession_number, form_type, filed_date)"
        " VALUES (%s, 'CommonStockDividendsPerShareDeclared', 'USD/shares', '2025-07-01', '2025-09-30', 390, %s,"
        " '10-Q', %s)",
        (iid, accession, filed),
    )
    conn.commit()
    scan.seed_population(conn)  # what each run does first; lets a test call select_targets directly
    conn.commit()


def _scan_row(conn: Conn, accession: str = ACC) -> tuple[Any, ...] | None:
    return conn.execute(
        "SELECT outcome, documents, parser_version, attempt_version, attempts, next_attempt_at IS NOT NULL,"
        " last_error IS NOT NULL FROM sec_inline_scale_scan WHERE accession_number = %s",
        (accession,),
    ).fetchone()


def _occurrence_scales(conn: Conn) -> list[Any]:
    return [r[0] for r in conn.execute("SELECT scale FROM sec_inline_dps_occurrence ORDER BY element_ordinal")]


def _queue(conn: Conn) -> list[tuple[int, int]]:
    """Queued rows: the ledger keeps drained rows (``drained_revision = revision``)."""
    return conn.execute(
        "SELECT instrument_id, revision FROM sec_inline_scale_renormalise"
        " WHERE drained_revision IS DISTINCT FROM revision ORDER BY instrument_id"
    ).fetchall()


def _run(conn: Conn, provider: _Provider) -> scan.ScanResult:
    conn.commit()
    return scan_run(conn, provider)


def _expire_backoff(conn: Conn) -> None:
    conn.execute("UPDATE sec_inline_scale_scan SET next_attempt_at = now() - interval '1 second'")
    conn.commit()


def test_positive_scale_publishes_and_queues_then_an_unscaled_rescan_queues_the_reversal(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _seed(conn)
    result = _run(conn, _Provider(_doc(3)))
    assert (result.succeeded, result.instruments_enqueued, result.work_state.classes["raw"]["done"]) == (1, 1, 1)
    assert _scan_row(conn) == ("inline", 1, scan.PARSER_VERSION, scan.PARSER_VERSION, 0, False, False)
    assert _occurrence_scales(conn) == [3]
    first = _queue(conn)
    assert [i for i, _ in first] == [1]

    # Done at this version: not selected again.
    assert select_targets(conn, today=TODAY) == []

    # The queue is drained; a new version's rescan finds scale 0, and the OLD generation's positive scale queues
    # the instrument again so its restore is reverted.
    conn.execute("DELETE FROM sec_inline_scale_renormalise")
    monkeypatch.setattr(scan, "PARSER_VERSION", "v-next")
    result = _run(conn, _Provider(_doc(None)))
    assert (result.succeeded, result.instruments_enqueued) == (1, 1)
    assert _occurrence_scales(conn) == [0]
    assert [i for i, _ in _queue(conn)] == [1]

    # Scale 0 both before and after: nothing is queued.
    conn.execute("DELETE FROM sec_inline_scale_renormalise")
    monkeypatch.setattr(scan, "PARSER_VERSION", "v-third")
    result = _run(conn, _Provider(_doc(None)))
    assert (result.succeeded, result.instruments_enqueued, _queue(conn)) == (1, 0, [])


def test_a_later_enqueue_takes_a_new_revision(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    _seed(conn)
    _run(conn, _Provider(_doc(3)))
    (before,) = _queue(conn)
    monkeypatch.setattr(scan, "PARSER_VERSION", "v-next")
    _run(conn, _Provider(_doc(3)))
    (after,) = _queue(conn)
    assert after[0] == before[0] and after[1] > before[1]


def test_a_failed_rescan_keeps_the_active_generation(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    _seed(conn)
    first = scan.PARSER_VERSION
    _run(conn, _Provider(_doc(3)))
    conn.execute("DELETE FROM sec_inline_scale_renormalise")
    monkeypatch.setattr(scan, "PARSER_VERSION", "v-next")
    result = _run(conn, _Provider(None))  # the archive and the listed document 404
    assert (result.failed, result.succeeded) == (1, 0)
    row = _scan_row(conn)
    assert row is not None
    # The old generation stays in force; the work state at v-next records the failure.
    assert row[:3] == ("inline", 1, first)
    assert row[3:] == ("v-next", 1, True, True)
    assert _occurrence_scales(conn) == [3]
    assert _queue(conn) == []
    # Inside the backoff: not eligible.
    assert select_targets(conn, today=TODAY) == []


def test_version_bump_resets_attempts_once(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    _seed(conn)
    for _ in range(2):
        _run(conn, _Provider(RuntimeError("boom")))
        _expire_backoff(conn)
    assert _scan_row(conn) == (None, None, None, scan.PARSER_VERSION, 2, True, True)

    monkeypatch.setattr(scan, "PARSER_VERSION", "v-next")
    _run(conn, _Provider(RuntimeError("boom")))
    # The first attempt at the new version starts from zero, so this failure is attempt 1, backed off 2 hours.
    assert _scan_row(conn) == (None, None, None, "v-next", 1, True, True)
    hours = conn.execute(
        "SELECT round(extract(epoch FROM next_attempt_at - now()) / 3600) FROM sec_inline_scale_scan"
    ).fetchone()
    assert hours is not None and hours[0] == 2
    _expire_backoff(conn)
    _run(conn, _Provider(RuntimeError("boom")))
    assert _scan_row(conn) == (None, None, None, "v-next", 2, True, True)


def test_exhaustion_and_readmission(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn)
    for _ in range(scan.MAX_ATTEMPTS):
        _run(conn, _Provider(RuntimeError("boom")))
        _expire_backoff(conn)
    result = _run(conn, _Provider(_doc(3)))
    assert result.attempted == 0
    assert result.work_state.classes["raw"] == {"done": 0, "pending": 0, "retrying": 0, "exhausted": 1}

    # Re-admission per spec: attempts = 0, next_attempt_at = now().
    conn.execute("UPDATE sec_inline_scale_scan SET attempts = 0, next_attempt_at = now()")
    result = _run(conn, _Provider(_doc(3)))
    assert (result.succeeded, result.work_state.classes["raw"]["done"]) == (1, 1)


def test_not_inline_and_superseded(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn)
    (target,) = select_targets(conn, today=TODAY)
    # Another writer moved the work state after selection: nothing is published.
    conn.execute(
        "INSERT INTO sec_inline_scale_scan (accession_number, attempt_version, attempts, next_attempt_at)"
        " VALUES (%s, 'other', 1, now())",
        (ACC,),
    )
    conn.commit()
    assert scan.publish_success(conn, target, None, 1, "archive") is None
    assert scan.record_failure(conn, target, "x") is False
    assert _scan_row(conn) == (None, None, None, "other", 1, True, False)

    result = _run(conn, _Provider("<html><body>no inline</body></html>"))
    assert result.succeeded == 1
    assert _scan_row(conn) == ("not_inline", 1, scan.PARSER_VERSION, scan.PARSER_VERSION, 0, False, False)
    assert _occurrence_scales(conn) == [] and _queue(conn) == []


def test_unresolved_occurrence_queues(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn)
    result = _run(conn, _Provider(_doc(None).replace('unitRef="u"', 'unitRef="missing"')))
    assert (result.succeeded, result.instruments_enqueued) == (1, 1)
    unresolved = conn.execute("SELECT unresolved, scale FROM sec_inline_dps_occurrence").fetchall()
    assert unresolved == [("unit_ref", None)]


def test_order_is_recent_first_then_oldest_and_the_cap_defers(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn, accession="0001411342-20-000001", filed=date(2020, 5, 1))
    _seed(conn, accession="0001411342-19-000001", filed=date(2019, 6, 15))
    _seed(conn, accession="0001411342-26-000009", filed=TODAY - timedelta(days=2))
    _seed(conn, accession="0001411342-19-000000", filed=date(2019, 6, 14))  # before the cutoff: out of scope
    order = [t.accession_number for t in select_targets(conn, today=TODAY)]
    assert order == ["0001411342-26-000009", "0001411342-19-000001", "0001411342-20-000001"]

    # Clock reads: the run's start, discovery's one instrument, then one per accession.
    ticks = iter([0.0, 0.0, 0.0, 1.0, 10_000.0])
    conn.commit()
    result = scan_run(conn, _Provider(_doc(None)), clock=lambda: next(ticks))
    assert (result.attempted, result.deferred) == (2, 1)
    assert result.work_state.classes["raw"] == {"done": 2, "pending": 1, "retrying": 0, "exhausted": 0}


def test_occurrence_check_rejects_a_resolved_row_missing_its_key(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    conn.execute(
        "INSERT INTO sec_inline_scale_scan (accession_number, outcome, documents, parser_version, scanned_at)"
        " VALUES (%s, 'inline', 1, 'v', now())",
        (ACC,),
    )
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(
            "INSERT INTO sec_inline_dps_occurrence (accession_number, document, element_ordinal, display_text)"
            " VALUES (%s, 'a.htm', 0, '0.39')",
            (ACC,),
        )


def test_each_attempt_ticks_the_progress_heartbeat(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    _seed(conn, accession="0001411342-20-000001", filed=date(2020, 5, 1))
    _seed(conn, accession="0001411342-21-000001", filed=date(2021, 5, 1))
    ticks: list[tuple[int, int | None, bool]] = []
    monkeypatch.setattr(scan, "report_progress", lambda done, total, *, force=False: ticks.append((done, total, force)))
    _run(conn, _Provider(RuntimeError("boom")))
    # Discovery's one instrument, then the two accessions.
    assert ticks == [(0, 1, False), (0, 2, False), (1, 2, False), (2, 2, True)]


def test_each_generation_records_its_path_and_the_run_counts_fallbacks(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _seed(conn)
    result = _run(conn, _Provider(_doc(3)))
    assert (result.archive, result.listing, result.fallbacks, result.documents, result.requests) == (1, 0, {}, 1, 2)
    assert conn.execute("SELECT path FROM sec_inline_scale_scan").fetchall() == [("archive",)]

    # B2.4: a gone archive takes the listing path (index.json, a.htm; no FilingSummary.xml listed), then the header.
    monkeypatch.setattr(scan, "PARSER_VERSION", "v-next")
    result = _run(conn, _Provider(_doc(3), archive=False))
    assert (result.succeeded, result.archive, result.listing) == (1, 0, 1)
    assert (result.fallbacks, result.requests) == ({"archive_not_found": 1}, 4)
    assert "archive 0, listing 1 (fallbacks: archive_not_found 1)" in result.digest()
    assert conn.execute("SELECT path FROM sec_inline_scale_scan").fetchall() == [("listing",)]


def test_a_v2_rescan_of_a_clean_v1_accession_of_a_candidate_enqueues_it(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    """B4.6: A4's candidate predicate at v2. Neither generation of ACC holds a positive-scale or unresolved
    occurrence; the instrument is a candidate through another accession's active v1 generation."""
    conn = ebull_test_conn
    other = "0001411342-25-000002"
    _seed(conn)
    _seed(conn, accession=other)
    v2 = scan.PARSER_VERSION
    monkeypatch.setattr(scan, "PARSER_VERSION", "dps-inline-scale-v1")
    _run(conn, _Provider({ACC: _doc(None), other: _doc(3)}))
    conn.execute("DELETE FROM sec_inline_scale_renormalise")
    monkeypatch.setattr(scan, "PARSER_VERSION", v2)
    # The other accession's v2 attempt fails, so its scale-3 v1 generation stays active and makes the candidate.
    result = _run(conn, _Provider({ACC: _doc(None), other: RuntimeError("boom")}))
    assert (result.succeeded, result.failed, result.instruments_enqueued) == (1, 1, 1)
    assert [i for i, _ in _queue(conn)] == [1]
