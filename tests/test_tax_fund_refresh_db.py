"""``refresh_fund_evidence`` against the test database (#3616 slice 3d-3c-1; spec tests 5-7).

Auto-marked ``db`` (pulls ``ebull_test_conn``, which wipes between tests). A generation holding SPY (a US fund with a
CIK and an N-CEN) and CSPX.L (a fund with no CIK) is seeded as sql/488 holds one; every run is
``refresh_fund_evidence`` on its own autocommit connection, as the job calls it. No test touches the network: the
sources are an ``httpx.MockTransport`` serving the committed fixtures and synthetic bodies, and the clock is fake.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import psycopg
import pytest

from app.services import tax_fund_evidence as tfe
from app.services import tax_fund_refresh as tfr
from app.services.tax_fund_evidence import DeadlineExceeded, figi_request_key
from app.services.tax_fund_refresh import HMRC_CONTENT_KEY, FundEvidenceReport, refresh_fund_evidence
from app.services.tax_matching import effective_tax_matching, run_tax_matching
from tests.fixtures.ebull_test_db import test_database_url
from tests.test_tax_fund_evidence import (
    ACC,
    ISIN,
    ISINS_BODY,
    LEI,
    NCEN_BODY,
    RECORD_BODY,
    content_xml,
    data_row,
    figi_body,
    ods,
)
from tests.test_tax_matching_db import _generation, _instrument, _scope

T = datetime(2026, 10, 14, 18, 0, tzinfo=UTC)
SPY, CSPX = 3000, 3434
CIK = "0000884394"
NCEN_URL = "https://www.sec.gov/Archives/edgar/data/884394/000141036826023904/primary_doc.xml"
ASSET = "https://assets.publishing.service.gov.uk/media/abc/20261006_Master-Weblist.ods"
KEYS = ["SPDR1", "SPDRSP500"]
FIGI_KEY = figi_request_key([ISIN], "ARCX")
LIST = ods(content_xml(data_row(), data_row(ref="B0002"), data_row(ref="C0003")))
SOURCES = ("sec_ncen", "gleif_record", "gleif_isins", "openfigi_mapping", "hmrc_content_item", "hmrc_reporting_list")


def _connect() -> psycopg.Connection[tuple]:
    return psycopg.connect(test_database_url(), autocommit=True)


def _rows(query: str, params: tuple[Any, ...] = ()) -> list[tuple]:
    with _connect() as conn:
        return conn.execute(query, params).fetchall()  # type: ignore[arg-type]


def _content(url: str = ASSET, size: int = len(LIST), published: str = "2026-10-12T11:30:44+01:00") -> bytes:
    return json.dumps(
        {"public_updated_at": published, "details": {"attachments": [{"url": url, "file_size": size}]}}
    ).encode()


def _source(request: httpx.Request) -> str:
    host, path = request.url.host, request.url.path
    if host == "www.sec.gov":
        return "sec_ncen"
    if host == "api.gleif.org":
        return "gleif_isins" if path.endswith("/isins") else "gleif_record"
    if host == "api.openfigi.com":
        return "openfigi_mapping"
    return "hmrc_content_item" if host == "www.gov.uk" else "hmrc_reporting_list"


Responder = httpx.Response | Callable[[httpx.Request], httpx.Response]


@dataclass
class Web:
    """The public sources, by source name; each request is logged."""

    responses: dict[str, Responder] = field(
        default_factory=lambda: {
            "sec_ncen": httpx.Response(200, content=NCEN_BODY),
            "gleif_record": httpx.Response(200, content=RECORD_BODY),
            "gleif_isins": httpx.Response(200, content=ISINS_BODY),
            "openfigi_mapping": httpx.Response(200, content=figi_body("SPY"), headers={"ratelimit-reset": "60"}),
            "hmrc_content_item": httpx.Response(200, content=_content()),
            "hmrc_reporting_list": httpx.Response(200, content=LIST),
        }
    )
    log: list[str] = field(default_factory=list)

    def handle(self, request: httpx.Request) -> httpx.Response:
        source = _source(request)
        self.log.append(source)
        r = self.responses[source]
        return r(request) if callable(r) else httpx.Response(r.status_code, headers=r.headers, content=r.content)


@dataclass
class FakeClock:
    t: datetime = T
    mono: float = 0.0

    def clock(self) -> tfr.Clock:
        return tfr.Clock(now=lambda: self.t, monotonic=lambda: self.mono, sleep=self.sleep)

    def sleep(self, seconds: float) -> None:
        self.mono += seconds


def _run(web: Web, clock: FakeClock | None = None) -> FundEvidenceReport:
    with _connect() as conn, httpx.Client(transport=httpx.MockTransport(web.handle)) as client:
        return refresh_fund_evidence(conn, client, (clock or FakeClock()).clock())


def _counts() -> dict[str, int]:
    tables = ("tax_fund_documents", "tax_fund_fetches", "tax_fund_list_versions", "tax_fund_list_searches")
    return {t: _rows(f"SELECT count(*) FROM {t}")[0][0] for t in tables}  # noqa: S608 - fixed names


def _fetches(source: str) -> list[tuple]:
    return _rows(
        "SELECT fetch_id, request_key, fetched_at, parent_fetch_id FROM tax_fund_fetches WHERE source = %s "
        "ORDER BY fetch_id",
        (source,),
    )


def _obs(security: int = SPY) -> dict[str, Any]:
    with _connect() as conn:
        cur = conn.execute(
            "SELECT * FROM tax_fund_observations WHERE tax_security_id = %s ORDER BY observation_id DESC LIMIT 1",
            (security,),
        )
        row = cur.fetchone()
        assert row is not None and cur.description is not None
        return {d.name: v for d, v in zip(cur.description, row, strict=True)}


def _seed(source: str, key: str, body: bytes, at: datetime, parent: int | None = None) -> int:
    url = key if source in ("hmrc_reporting_list",) else f"https://example.test/{source}/{key}"
    with _connect() as conn:
        return tfr._store(conn, source, key, url, at, body, parent=parent)  # pyright: ignore[reportPrivateUsage]


@pytest.fixture
def funds(ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """SPY (CIK 884394, its 2025 N-CEN) and CSPX.L (no CIK), both ETFs, in one published generation. The list's
    bootstrap floor is lowered so a three-row synthetic version is usable."""
    monkeypatch.setattr(tfe, "BOOTSTRAP_MIN_ROWS", 1)
    _scope()
    _instrument(SPY, "SPY")
    _instrument(CSPX, "CSPX.L")
    with _connect() as conn:
        conn.execute("UPDATE instruments SET instrument_type_id = 6 WHERE instrument_id IN (%s, %s)", (SPY, CSPX))
        conn.execute("UPDATE instruments SET country = 'GB' WHERE instrument_id = %s", (CSPX,))
        conn.execute(
            "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (%s, 'sec', 'cik', %s, TRUE)",
            (SPY, CIK),
        )
    _filing(ACC, "N-CEN", date(2026, 3, 13), date(2025, 12, 31), _rendered(ACC))
    _generation(
        [("order:1", "admitted", SPY), ("order:2", "admitted", CSPX)],
        [
            ("L1", "order:1", SPY, "acquisition", date(2026, 1, 5), "10", "100"),
            ("L2", "order:2", CSPX, "acquisition", date(2026, 1, 6), "5", "50"),
        ],
    )
    yield


def _rendered(accession: str, cik: str = "884394") -> str:
    return (
        f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession.replace('-', '')}/xslFormN-CEN_X01/primary_doc.xml"
    )


def _filing(accession: str, form: str, filed: date, report: date, url: str) -> None:
    with _connect() as conn:
        conn.execute(
            "INSERT INTO filing_events (instrument_id, filing_date, filing_type, provider, provider_filing_id, "
            "report_date, primary_document_url) VALUES (%s, %s, %s, 'sec', %s, %s, %s)",
            (SPY, filed, form, accession, report, url),
        )


# --- 5. capture ------------------------------------------------------------------------------------------------------


def test_bootstrap_fetches_every_due_source_and_observes_both_funds(funds: None) -> None:
    with _connect() as conn:
        before = run_tax_matching(conn)
        effective = effective_tax_matching(conn)
    web = Web()
    report = _run(web)
    assert report.failures == ()
    assert dict(report.stored) == dict.fromkeys(SOURCES, 1)
    assert (report.assessed, report.searched) == (1, 1)
    assert sorted(web.log) == sorted(SOURCES)
    assert _rows("SELECT request_key FROM tax_fund_fetches WHERE source = 'openfigi_mapping'") == [(FIGI_KEY,)]
    assert _rows("SELECT rate_reset FROM tax_fund_fetches WHERE source = 'openfigi_mapping'") == [("60",)]
    (content_id, _k, _at, _p), (list_id, list_key, _at2, parent) = (
        _fetches("hmrc_content_item")[0],
        _fetches("hmrc_reporting_list")[0],
    )
    assert (list_key, parent) == (ASSET, content_id)
    assert _rows("SELECT usable, data_rows, baseline_rows FROM tax_fund_list_versions") == [(True, 3, None)]

    spy = _obs(SPY)
    assert (spy["outcome"], spy["search_outcome"], spy["refusal"]) == ("candidate", "not_found", None)
    assert (spy["lei"], spy["isin"], spy["mic"], spy["ncen_accession"]) == (LEI, ISIN, "ARCX", ACC)
    assert (spy["cik"], spy["name_keys"], spy["isin_set"]) == (CIK, KEYS, [ISIN])
    assert (spy["content_fetch_id"], spy["list_fetch_id"]) == (content_id, list_id)
    assert spy["coverage"] == [{"list_fetch_id": list_id, "state": "searched"}]
    assert all(spy[c] is not None for c in ("ncen_fetch_id", "record_fetch_id", "isins_fetch_id", "figi_fetch_id"))
    assert spy["listings"] == [["ARCX", "SPY"]]
    searched = _rows(
        "SELECT isin, name_keys, identifier_count, identifier_in_text_count, near_identifier_count, name_count "
        "FROM tax_fund_list_searches"
    )
    assert searched == [(ISIN, KEYS, 0, 0, 0, 0)]

    cspx = _obs(CSPX)
    assert cspx["outcome"] == "cik_unestablished"
    nulls = ("cik", "ncen_accession", "lei", "isin", "name_keys", "search_outcome", "content_fetch_id", "coverage")
    assert all(cspx[c] is None for c in nulls)

    with _connect() as conn:  # matching reads none of these tables: the effective run and its digest are unchanged
        assert effective_tax_matching(conn) == effective
        after = run_tax_matching(conn)
    assert (after.outcome, after.matching_run_id) == ("already_published", before.matching_run_id)


def test_nothing_due_adds_no_fetch_assessment_or_search(funds: None) -> None:
    _run(Web())
    counts = _counts()
    web = Web()
    clock = FakeClock(T + timedelta(days=1))
    report = _run(web, clock)
    assert (web.log, report.failures, _counts()) == ([], (), counts)
    assert (_obs(SPY)["search_outcome"], _obs(SPY)["observed_at"]) == ("not_found", T + timedelta(days=1))
    assert _rows("SELECT count(*) FROM tax_fund_observations")[0][0] == 4


def test_an_unchanged_refetch_adds_fetch_rows_and_no_document_rows(funds: None) -> None:
    _run(Web())
    documents = _counts()["tax_fund_documents"]
    web = Web()
    report = _run(web, FakeClock(T + timedelta(days=8)))
    assert report.failures == ()
    assert "sec_ncen" not in web.log  # once per accession
    assert sorted(web.log) == sorted(SOURCES[1:])
    assert _counts()["tax_fund_documents"] == documents
    lists = _fetches("hmrc_reporting_list")
    assert [p for *_x, p in lists] == [f[0] for f in _fetches("hmrc_content_item")]
    # the new version is assessed against the first, and searched once for the key
    assert _rows("SELECT data_rows, baseline_rows FROM tax_fund_list_versions ORDER BY list_fetch_id") == [
        (3, None),
        (3, 3),
    ]
    assert _rows("SELECT count(*) FROM tax_fund_list_searches")[0][0] == 2
    assert _obs(SPY)["coverage"] == [{"list_fetch_id": f[0], "state": "searched"} for f in lists]


def test_a_superseded_childless_content_fetch_is_never_retried(funds: None) -> None:
    web = Web()
    web.responses["hmrc_reporting_list"] = httpx.Response(200, content=LIST + b"x")  # wrong size
    report = _run(web)
    assert any(f.startswith("hmrc_reporting_list") for f in report.failures)
    assert _fetches("hmrc_reporting_list") == []
    assert (_obs(SPY)["search_outcome"], _obs(SPY)["refusal"]) == ("unavailable", "no list for the content fetch")

    bigger = LIST + b"\0"  # a same-URL list with new bytes and a new size, under a new content item
    web = Web()
    web.responses["hmrc_content_item"] = httpx.Response(200, content=_content(size=len(bigger)))
    web.responses["hmrc_reporting_list"] = httpx.Response(200, content=bigger)
    _run(web, FakeClock(T + timedelta(days=8)))
    first, second = (f[0] for f in _fetches("hmrc_content_item"))
    assert [(k, p) for _f, k, _at, p in _fetches("hmrc_reporting_list")] == [(ASSET, second)]
    assert first != second
    web = Web()
    _run(web, FakeClock(T + timedelta(days=9)))
    assert "hmrc_reporting_list" not in web.log


@pytest.mark.parametrize(
    ("source", "response", "failure"),
    [
        ("sec_ncen", httpx.Response(200, content=b"<html>not an N-CEN</html>"), "unsound body"),
        ("sec_ncen", httpx.Response(301, headers={"location": "https://www.sec.gov/x"}), "http 301"),
        ("gleif_record", httpx.Response(200, content=b" " * (1024 * 1024 + 1)), "body past"),
        ("hmrc_content_item", httpx.Response(200, content=_content(published="2026-10-15T00:00:00Z")), "unsound"),
        (
            "hmrc_content_item",
            httpx.Response(
                200,
                content=json.dumps(
                    {
                        "public_updated_at": "2026-10-12T11:30:44+01:00",
                        "details": {"attachments": [{"url": ASSET, "file_size": 1}, {"url": ASSET, "file_size": 1}]},
                    }
                ).encode(),
            ),
            "unsound",
        ),
        ("hmrc_reporting_list", httpx.Response(200, content=LIST[:-1]), "unsound"),
    ],
)
def test_a_failed_fetch_stores_nothing_and_fails_the_step_after_the_rest(
    funds: None, source: str, response: httpx.Response, failure: str
) -> None:
    web = Web()
    web.responses[source] = response
    report = _run(web)
    assert [f for f in report.failures if f.startswith(source)] and failure in " ".join(report.failures)
    assert _fetches(source) == []
    assert _obs(CSPX)["outcome"] == "cik_unestablished"
    if source == "sec_ncen":
        spy = _obs(SPY)
        assert (spy["outcome"], spy["refusal"], spy["ncen_fetch_id"]) == (
            "registration_unavailable",
            "no stored document",
            None,
        )
        assert spy["ncen_accession"] == ACC
        assert web.log == ["sec_ncen"]  # nothing downstream is due without the N-CEN


def test_a_sound_ncen_failing_the_reader_is_registration_unusable(funds: None) -> None:
    web = Web()
    web.responses["sec_ncen"] = httpx.Response(
        200, content=NCEN_BODY.replace(b"<registrantClassificationType>S-6", b"<registrantClassificationType>N-1A")
    )
    _run(web)
    spy = _obs(SPY)
    assert (spy["outcome"], spy["ncen_fetch_id"]) == ("registration_unusable", _fetches("sec_ncen")[0][0])
    assert spy["lei"] is None


def test_registration_final_keeps_its_accession_fetch_and_lei(funds: None) -> None:
    web = Web()
    web.responses["sec_ncen"] = httpx.Response(
        200, content=NCEN_BODY.replace(b"<isRegistrantLastFiling>N<", b"<isRegistrantLastFiling>Y<")
    )
    _run(web)
    spy = _obs(SPY)
    assert (spy["outcome"], spy["ncen_accession"], spy["lei"]) == ("registration_final", ACC, LEI)
    assert spy["ncen_fetch_id"] == _fetches("sec_ncen")[0][0]
    assert spy["record_fetch_id"] is None


def test_a_429_fails_that_fetch_and_a_current_mapping_still_gives_candidate(funds: None) -> None:
    _run(Web())
    web = Web()
    web.responses["openfigi_mapping"] = httpx.Response(429)
    report = _run(web, FakeClock(T + timedelta(days=8)))
    assert [f for f in report.failures if f.startswith("openfigi_mapping")] == [
        f"openfigi_mapping {FIGI_KEY}: http 429"
    ]
    assert web.log.count("openfigi_mapping") == 1
    assert _obs(SPY)["outcome"] == "candidate"  # the stored mapping is 8 days old: current
    web = Web()
    web.responses["openfigi_mapping"] = httpx.Response(429)
    _run(web, FakeClock(T + timedelta(days=31)))
    assert _obs(SPY)["outcome"] == "isin_mapping_unavailable"  # expired


def test_a_changed_isin_set_with_a_failed_mapping_is_unavailable(funds: None) -> None:
    _run(Web())
    other = "US78462N1030"  # valid; the set now holds two ISINs, so the job list changed
    web = Web()
    web.responses["gleif_isins"] = httpx.Response(
        200,
        content=json.dumps(
            {
                "meta": {"pagination": {"currentPage": 1, "lastPage": 1, "total": 2}},
                "data": [{"attributes": {"isin": ISIN}}, {"attributes": {"isin": other}}],
            }
        ).encode(),
    )
    web.responses["openfigi_mapping"] = httpx.Response(429)
    _run(web, FakeClock(T + timedelta(days=8)))
    spy = _obs(SPY)
    assert (spy["outcome"], spy["isin_set"], spy["figi_fetch_id"]) == ("isin_mapping_unavailable", [ISIN, other], None)


def test_a_concurrent_invocation_fails_at_once_and_leaves_the_first_intact(funds: None) -> None:
    with _connect() as holder:
        holder.execute("SELECT pg_advisory_lock(%s)", (tfr.FUND_EVIDENCE_LOCK_KEY,))
        web = Web()
        report = _run(web)
        assert (report.failures, web.log, report.observations) == (("concurrent refresh",), [], ())
    assert _counts()["tax_fund_fetches"] == 0
    assert _run(Web()).failures == ()


def test_a_slow_drip_response_is_cut_at_the_request_cap(funds: None) -> None:
    clock = FakeClock()

    def drip(_request: httpx.Request) -> httpx.Response:
        def chunks() -> Iterator[bytes]:
            for i in range(0, len(RECORD_BODY), 100):
                clock.mono += 10
                yield RECORD_BODY[i : i + 100]

        return httpx.Response(200, content=chunks())

    web = Web()
    web.responses["gleif_record"] = drip
    report = _run(web, clock)
    assert "gleif_record 549300NZAMSJ8FXPQQ63: request past its time cap" in report.failures
    assert _fetches("gleif_record") == []
    assert _obs(SPY)["outcome"] == "lei_record_unavailable"


def test_the_cap_bounds_a_request_in_real_time(funds: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """A peer that stalls inside each network operation's timeout is still cut at the request cap: the exchange runs
    on a worker joined for the cap, and the step moves on."""
    monkeypatch.setattr(tfr, "REQUEST_CAP_S", 0.2)
    released = threading.Event()

    def stall(_request: httpx.Request) -> httpx.Response:
        released.wait(5)
        return httpx.Response(200, content=RECORD_BODY)

    web = Web()
    web.responses["gleif_record"] = stall
    started = time.monotonic()
    report = _run(web)
    released.set()
    assert time.monotonic() - started < 3
    assert "gleif_record 549300NZAMSJ8FXPQQ63: request past its time cap" in report.failures
    assert _fetches("gleif_record") == [] and _obs(SPY)["outcome"] == "lei_record_unavailable"


def test_the_deadline_stops_the_remaining_fetches(funds: None) -> None:
    clock = FakeClock()

    def late(_request: httpx.Request) -> httpx.Response:
        clock.mono += tfr.STEP_DEADLINE_S
        return httpx.Response(200, content=RECORD_BODY)

    web = Web()
    web.responses["gleif_record"] = late
    report = _run(web, clock)
    assert "deadline: fetches still due were not tried" in report.failures
    assert web.log == ["sec_ncen", "gleif_record"]
    assert _fetches("sec_ncen") and not _fetches("gleif_record")
    assert _obs(SPY)["outcome"] == "lei_record_unavailable"


def _expire_inside(monkeypatch: pytest.MonkeyPatch, name: str, after: int = 0) -> None:
    """The deadline passes inside the ``after``-th later call of ``name`` (``read_reporting_list`` or
    ``search_version``), as a tick inside decompression, a row or a search would raise it."""
    real = getattr(tfr, name)
    calls = {"n": 0}

    def wrapped(*args: Any) -> Any:
        calls["n"] += 1
        if calls["n"] > after:

            def expired() -> None:
                raise DeadlineExceeded

            real(*args[:-1], expired)
            raise DeadlineExceeded  # a version shorter than one tick interval: the deadline passed at its end
        return real(*args)

    monkeypatch.setattr(tfr, name, wrapped)


def test_a_deadline_inside_assessment_leaves_the_version_pending(funds: None, monkeypatch: pytest.MonkeyPatch) -> None:
    with monkeypatch.context() as m:
        _expire_inside(m, "read_reporting_list")
        report = _run(Web())
    assert "deadline: list assessment or search interrupted" in report.failures
    assert _fetches("hmrc_reporting_list") and _counts()["tax_fund_list_versions"] == 0
    spy = _obs(SPY)
    assert (spy["outcome"], spy["search_outcome"], spy["refusal"]) == ("candidate", "unavailable", "list pending")
    assert spy["coverage"] == [{"list_fetch_id": _fetches("hmrc_reporting_list")[0][0], "state": "pending"}]
    report = _run(Web(), FakeClock(T + timedelta(hours=1)))  # the next run assesses, then searches
    assert (report.failures, report.assessed, report.searched) == ((), 1, 1)
    assert _obs(SPY)["search_outcome"] == "not_found"


def test_a_deadline_between_assessment_and_search_records_usable_unsearched(
    funds: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    with monkeypatch.context() as m:
        _expire_inside(m, "search_version")
        _run(Web())
    first = _obs(SPY)
    list_id = _fetches("hmrc_reporting_list")[0][0]
    assert (first["search_outcome"], first["refusal"]) == ("unavailable", "unsearched version")
    assert first["coverage"] == [{"list_fetch_id": list_id, "state": "usable_unsearched"}]
    _run(Web(), FakeClock(T + timedelta(hours=1)))
    second = _obs(SPY)
    assert second["coverage"] == [{"list_fetch_id": list_id, "state": "searched"}]
    assert second["search_outcome"] == "not_found"
    earlier = _rows("SELECT coverage FROM tax_fund_observations WHERE observation_id = %s", (first["observation_id"],))[
        0
    ][0]
    assert earlier == [{"list_fetch_id": list_id, "state": "usable_unsearched"}]


def _two_versions() -> tuple[int, int]:
    """A three-row version, then a two-row one (under 90% of three), each under its own content item."""
    two = ods(content_xml(data_row(), data_row(ref="B0002")))
    ids = []
    for body, at in ((LIST, T - timedelta(minutes=2)), (two, T - timedelta(minutes=1))):
        content = _seed("hmrc_content_item", HMRC_CONTENT_KEY, _content(size=len(body)), at)
        ids.append(_seed("hmrc_reporting_list", ASSET, body, at, parent=content))
    return ids[0], ids[1]


def _versions() -> list[tuple]:
    return _rows(
        "SELECT list_fetch_id, usable, data_rows, refusal, baseline_rows FROM tax_fund_list_versions "
        "ORDER BY list_fetch_id"
    )


@pytest.mark.parametrize("after", [0, 1])
def test_an_interrupted_assessment_resumes_in_fetch_order(
    funds: None, monkeypatch: pytest.MonkeyPatch, after: int
) -> None:
    """``after`` 0: the larger predecessor's assessment is interrupted, so the later version is not assessed while it
    is pending. ``after`` 1: the first assessment commits and the second is interrupted. Either way the next run
    assesses the later version against the first, and the first's commits survive."""
    first, second = _two_versions()
    with monkeypatch.context() as m:
        _expire_inside(m, "read_reporting_list", after=after)
        report = _run(Web())
    assert (report.assessed, "deadline: list assessment or search interrupted" in report.failures) == (after, True)
    assert [v[0] for v in _versions()] == [first, second][:after]
    assert _obs(SPY)["refusal"] == "list pending"
    _run(Web(), FakeClock(T + timedelta(minutes=1)))
    assert _versions() == [(first, True, 3, None, None), (second, False, None, "rows 2 below 2.7", 3)]
    assert _rows("SELECT list_fetch_id FROM tax_fund_list_searches") == [(first,)]
    spy = _obs(SPY)
    assert (spy["search_outcome"], spy["refusal"]) == ("unavailable", "list unusable")
    assert [c["state"] for c in spy["coverage"]] == ["searched", "unusable"]


def test_the_bootstrap_floor_applies_until_a_version_is_usable(funds: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tfe, "BOOTSTRAP_MIN_ROWS", 3)
    broken = ods(b"<not-odf/>")
    small = ods(content_xml(data_row(), data_row(ref="B0002")))
    for i, body in enumerate((broken, small, LIST)):
        at = T - timedelta(minutes=3 - i)
        content = _seed("hmrc_content_item", HMRC_CONTENT_KEY, _content(size=len(body)), at)
        _seed("hmrc_reporting_list", ASSET, body, at, parent=content)
    _run(Web())
    assert [(usable, rows, baseline) for _f, usable, rows, _r, baseline in _versions()] == [
        (False, None, None),
        (False, None, None),
        (True, 3, None),
    ]
    assert _versions()[1][3] == "rows 2 below 3"
    assert _obs(SPY)["search_outcome"] == "not_found"


def test_a_listed_older_version_stays_listed_after_a_clean_newer_one(funds: None) -> None:
    listed = ods(content_xml(data_row(isin=ISIN), data_row(ref="B0002"), data_row(ref="C0003")))
    web = Web()
    web.responses["hmrc_content_item"] = httpx.Response(200, content=_content(size=len(listed)))
    web.responses["hmrc_reporting_list"] = httpx.Response(200, content=listed)
    _run(web)
    assert _obs(SPY)["search_outcome"] == "listed"
    _run(Web(), FakeClock(T + timedelta(days=8)))
    spy = _obs(SPY)
    assert spy["search_outcome"] == "listed"
    assert [c["state"] for c in spy["coverage"]] == ["searched", "searched"]


def test_a_new_alias_is_a_new_key_that_searches_every_stored_version(funds: None) -> None:
    _run(Web())
    _run(Web(), FakeClock(T + timedelta(days=8)))
    doc = json.loads(RECORD_BODY)
    doc["data"]["attributes"]["entity"]["otherNames"] = [
        *doc["data"]["attributes"]["entity"].get("otherNames", []),
        {"name": "Spiders Trust", "language": "en"},
    ]
    web = Web()
    web.responses["gleif_record"] = httpx.Response(200, content=json.dumps(doc).encode())
    report = _run(web, FakeClock(T + timedelta(days=16)))
    assert report.searched == 3  # two stored versions and today's, for the new key
    assert _obs(SPY)["name_keys"] == ["SPDR1", "SPDRSP500", "SPIDERS"]


# --- 6. provenance ---------------------------------------------------------------------------------------------------


def test_an_unsupported_url_keeps_its_source_url(funds: None) -> None:
    other = "0001410368-27-000001"
    url = f"https://www.sec.gov/Archives/edgar/data/1/{other.replace('-', '')}/primary_doc.xml"
    _filing(other, "N-CEN", date(2027, 3, 1) - timedelta(days=200), date(2026, 6, 30), url)
    web = Web()
    _run(web)
    spy = _obs(SPY)
    assert (spy["outcome"], spy["refusal"], spy["ncen_source_url"]) == (
        "registration_unavailable",
        "unsupported URL",
        url,
    )
    assert "sec_ncen" not in web.log


def test_an_amendment_only_newest_period_keeps_its_period_and_accessions(funds: None) -> None:
    amended = "0001410368-26-099999"
    _filing(amended, "N-CEN/A", date(2026, 9, 1), date(2026, 6, 30), _rendered(amended))
    _run(Web())
    spy = _obs(SPY)
    assert (spy["outcome"], spy["newest_period"], spy["amendment_accessions"]) == (
        "registration_unavailable",
        date(2026, 6, 30),
        [amended],
    )


# --- 7. currency -----------------------------------------------------------------------------------------------------


_UNAVAILABLE = {
    "gleif_record": ("lei_record_unavailable", None),
    "gleif_isins": ("isin_unavailable", None),
    "openfigi_mapping": ("isin_mapping_unavailable", None),
    "hmrc_content_item": ("candidate", "unavailable"),
}


def _seed_chain(target: str, at: datetime) -> None:
    """Every source stored a day before ``T``, ``target`` (and a content item's list) at ``at`` instead."""
    day = T - timedelta(days=1)
    _seed("sec_ncen", ACC, NCEN_BODY, day)
    for source, key, body in (
        ("gleif_record", LEI, RECORD_BODY),
        ("gleif_isins", LEI, ISINS_BODY),
        ("openfigi_mapping", FIGI_KEY, figi_body("SPY")),
    ):
        _seed(source, key, body, at if source == target else day)
    when = at if target == "hmrc_content_item" else day
    content = _seed("hmrc_content_item", HMRC_CONTENT_KEY, _content(published="2026-01-01T00:00:00Z"), when)
    _seed("hmrc_reporting_list", ASSET, LIST, when, parent=content)


@pytest.mark.parametrize("target", list(_UNAVAILABLE))
@pytest.mark.parametrize(
    ("age", "current"),
    [(timedelta(days=30), True), (timedelta(days=30, microseconds=1), False), (-timedelta(seconds=1), False)],
)
def test_currency_with_a_failed_refresh(funds: None, target: str, age: timedelta, current: bool) -> None:
    _seed_chain(target, T - age)
    web = Web()
    web.responses[target] = httpx.Response(503)
    report = _run(web)
    assert any(f.startswith(f"{target} ") for f in report.failures)
    spy = _obs(SPY)
    got = (spy["outcome"], spy["search_outcome"])
    assert got == (("candidate", "not_found") if current else _UNAVAILABLE[target])


@pytest.mark.parametrize("target", list(_UNAVAILABLE))
def test_an_unchanged_refetch_restores_currency(funds: None, target: str) -> None:
    _seed_chain(target, T - timedelta(days=31))
    report = _run(Web())
    assert report.failures == ()
    assert (_obs(SPY)["outcome"], _obs(SPY)["search_outcome"]) == ("candidate", "not_found")


@pytest.mark.parametrize(
    ("target", "newer", "expected"),
    [
        ("gleif_record", lambda: _gleif_lapsed(), ("lei_record_unsupported", None)),
        (
            "gleif_isins",
            lambda: (
                b'{"meta":{"pagination":{"currentPage":1,"lastPage":2,"total":1}},"data":[{"attributes":{"isin":"US78462F1030"}}]}'
            ),
            ("isin_set_unsupported", None),
        ),
        ("openfigi_mapping", lambda: figi_body(None), ("isin_unresolved", None)),
    ],
)
def test_a_newer_fetch_failing_a_rule_has_no_fallback(
    funds: None, target: str, newer: Callable[[], bytes], expected: tuple[str, str | None]
) -> None:
    _seed_chain(target, T - timedelta(days=2))
    key = {"gleif_record": LEI, "gleif_isins": LEI, "openfigi_mapping": FIGI_KEY}[target]
    _seed(target, key, newer(), T - timedelta(days=1))
    _run(Web())
    spy = _obs(SPY)
    assert (spy["outcome"], spy["search_outcome"]) == expected


def test_a_newer_unusable_list_with_an_older_usable_one_is_unavailable(funds: None) -> None:
    _run(Web())
    broken = ods(b"<not-odf/>")
    web = Web()
    web.responses["hmrc_content_item"] = httpx.Response(200, content=_content(size=len(broken)))
    web.responses["hmrc_reporting_list"] = httpx.Response(200, content=broken)
    _run(web, FakeClock(T + timedelta(days=8)))
    spy = _obs(SPY)
    assert (spy["search_outcome"], spy["refusal"]) == ("unavailable", "list unusable")
    assert [c["state"] for c in spy["coverage"]] == ["searched", "unusable"]


def _gleif_lapsed() -> bytes:
    doc = json.loads(RECORD_BODY)
    doc["data"]["attributes"]["registration"]["status"] = "LAPSED"
    return json.dumps(doc).encode()
