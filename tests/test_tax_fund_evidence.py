"""Pure tests for #3616 slice 3d-3c-1's readers and decisions (spec tests 1-4).

Spec: docs/specs/metrics/2026-10-14-3616-slice3d3c1-fund-identity.md. Fixtures and their provenance:
tests/fixtures/tax_fund/README.md. No test touches the network or the DB.
"""

from __future__ import annotations

import io
import json
import zipfile
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape

import pytest

from app.services import tax_fund_evidence as tfe
from app.services.tax_fund_evidence import (
    LIST_HEADERS,
    DeadlineExceeded,
    Filing,
    consolidate,
    figi_request_key,
    identity_outcome,
    match_row,
    ncen_sound,
    ncen_xml_url,
    newest_period,
    read_content_item,
    read_figi,
    read_gleif_isins,
    read_gleif_record,
    read_ncen,
    read_reporting_list,
    search_outcome,
    search_version,
    valid_isin,
    valid_lei,
)

FIXTURES = Path(__file__).parent / "fixtures" / "tax_fund"
ACC = "0001410368-26-023904"
CIK = "0000884394"
LEI = "549300NZAMSJ8FXPQQ63"
ISIN = "US78462F1030"
NCEN_BODY = (FIXTURES / f"ncen_{ACC}.xml").read_bytes()
RECORD_BODY = (FIXTURES / f"gleif_record_{LEI}.json").read_bytes()
ISINS_BODY = (FIXTURES / f"gleif_isins_{LEI}.json").read_bytes()
ARCHIVE = "https://www.sec.gov/Archives/edgar/data/884394/000141036826023904/"
SPY_FILING = Filing(ACC, "N-CEN", date(2026, 3, 13), date(2025, 12, 31), ARCHIVE + "xslFormN-CEN_X01/primary_doc.xml")
TODAY = date(2026, 10, 14)
KEYS = ["SPDR1", "SPDRSP500"]


def figi_body(*results: Any) -> bytes:
    """A synthetic OpenFIGI response in fact 4's shapes: a ticker string gives a ``data`` row, None a warning."""
    out: list[Any] = []
    for r in results:
        if r is None:
            out.append({"warning": "No identifier found."})
        elif isinstance(r, dict):
            out.append(r)
        else:
            out.append({"data": [{"ticker": r, "exchCode": "UP", "compositeFIGI": "BBG000BDTBL9"}]})
    return json.dumps(out).encode()


def ncen(old: str = "", new: str = "") -> bytes:
    if not old:
        return NCEN_BODY
    text = NCEN_BODY.decode()
    assert old in text, old
    return text.replace(old, new, 1).encode()


def record(**entity: Any) -> bytes:
    doc = json.loads(RECORD_BODY)
    status = entity.pop("registration_status", None)
    if status is not None:
        doc["data"]["attributes"]["registration"]["status"] = status
    doc["data"]["attributes"]["entity"].update(entity)
    return json.dumps(doc).encode()


def isins(entries: Sequence[str], **pagination: Any) -> bytes:
    pag: dict[str, Any] = {"currentPage": 1, "lastPage": 1, "total": len(entries), **pagination}
    return json.dumps(
        {"meta": {"pagination": pag}, "data": [{"type": "isins", "attributes": {"isin": i}} for i in entries]}
    ).encode()


def outcome(
    *,
    ciks: Sequence[str] = ("884394",),
    symbol: str | None = "SPY",
    filings: list[Filing] | None = None,
    ncen_body: bytes | None = NCEN_BODY,
    record_body: bytes | None = RECORD_BODY,
    isins_body: bytes | None = ISINS_BODY,
    figi: tuple[Sequence[str], bytes] | None = ((ISIN,), figi_body("SPY")),
) -> tuple[str, dict[str, Any]]:
    filings = [SPY_FILING] if filings is None else filings
    return identity_outcome(
        ciks=ciks,
        symbol=symbol,
        filings=filings,
        ncen={ACC: ncen_body} if ncen_body is not None else {},
        records={LEI: record_body} if record_body is not None else {},
        isin_sets={LEI: isins_body} if isins_body is not None else {},
        mappings={figi_request_key(figi[0], "ARCX"): figi[1]} if figi is not None else {},
    )


# --- test 4: readers on fixtures ---------------------------------------------------------------------------------


def test_fixtures_give_candidate() -> None:
    result, got = outcome()
    assert result == "candidate"
    assert got["lei"] == LEI and got["isin"] == ISIN and got["mic"] == "ARCX" and got["isin_set"] == [ISIN]
    assert got["listings"] == [["ARCX", "SPY"]] and got["name_keys"] == KEYS
    assert got["ncen_accession"] == ACC and got["ncen_report_date"] == date(2025, 12, 31)
    assert got["trustee_countries"] == ["US"] and got["registrant_country"] == "US" and got["short_period"] == "N"


def test_fixture_readers() -> None:
    doc = read_ncen(NCEN_BODY, CIK)
    assert doc is not None and doc.report_period == "2025-12-31" and doc.last_filing == "N"
    rec = read_gleif_record(RECORD_BODY, LEI)
    assert rec is not None and (rec.registration_status, rec.entity_status, rec.category) == (
        "ISSUED",
        "ACTIVE",
        "FUND",
    )
    assert rec.names == ("SPDR S&P 500 ETF TRUST", "SPDR Trust, Series 1") and not rec.successor
    parsed = read_gleif_isins(ISINS_BODY)
    assert parsed is not None and parsed.entries == (ISIN,) and (parsed.total, parsed.last_page) == (1, 1)
    assert ncen_sound(NCEN_BODY) and valid_lei(LEI)


@pytest.mark.parametrize(
    ("isin", "valid"),
    [
        ("US78462F1030", True),
        ("US78462N1030", True),
        ("US78462F1048", True),
        ("IE00B5BMR087", True),
        ("IE0031442068", True),
        ("KYG4330C1097", False),
    ],
)
def test_isin_check_digits(isin: str, valid: bool) -> None:
    assert valid_isin(isin) is valid


# --- test 1: each identity line alone, in order -------------------------------------------------------------------


@pytest.mark.parametrize("ciks", [(), ("884394", "1"), ("88439X",)])
def test_cik_unestablished(ciks: tuple[str, ...]) -> None:
    assert outcome(ciks=ciks) == ("cik_unestablished", {})


def test_two_spellings_of_one_cik_pass() -> None:
    assert outcome(ciks=("884394", "0000884394"))[0] == "candidate"


@pytest.mark.parametrize(
    "rows",
    [
        [SPY_FILING, Filing(ACC, "N-CEN/A", SPY_FILING.filing_date, SPY_FILING.report_date, SPY_FILING.url)],
        [SPY_FILING, Filing(ACC, "N-CEN", date(2026, 3, 14), SPY_FILING.report_date, SPY_FILING.url)],
        [SPY_FILING, Filing(ACC, "N-CEN", SPY_FILING.filing_date, date(2024, 12, 31), SPY_FILING.url)],
        [Filing(ACC, "N-CEN", SPY_FILING.filing_date, None, SPY_FILING.url)],
        [Filing(ACC, "N-CEN", date(2025, 12, 30), date(2025, 12, 31), SPY_FILING.url)],
        [Filing(ACC, "N-CEN", TODAY + timedelta(days=1), date(2025, 12, 31), SPY_FILING.url)],
    ],
)
def test_metadata_conflict(rows: list[Filing]) -> None:
    assert consolidate(rows, TODAY) is None
    assert identity_outcome(
        ciks=("884394",), symbol="SPY", filings=None, ncen={}, records={}, isin_sets={}, mappings={}
    ) == ("registration_metadata_conflict", {"cik": CIK})


def test_consolidate_keeps_smallest_url() -> None:
    raw = Filing(ACC, "N-CEN", SPY_FILING.filing_date, SPY_FILING.report_date, ARCHIVE + "primary_doc.xml")
    out = consolidate([SPY_FILING, raw], TODAY)
    assert out is not None and [f.url for f in out] == [ARCHIVE + "primary_doc.xml"]


AMEND = Filing("0001410368-26-099999", "N-CEN/A", date(2026, 4, 1), date(2025, 12, 31), ARCHIVE + "primary_doc.xml")
OLD_AMEND = Filing("0001410368-25-099999", "N-CEN/A", date(2025, 4, 1), date(2024, 12, 31), ARCHIVE + "x.xml")


def test_newest_period_with_only_an_amendment() -> None:
    result, got = outcome(filings=[AMEND])
    assert result == "registration_unavailable" and got["refusal"] == "no N-CEN for the newest report period"
    assert got["newest_period"] == date(2025, 12, 31) and got["amendment_accessions"] == [AMEND.accession]


def test_amendment_for_the_newest_period() -> None:
    result, got = outcome(filings=[SPY_FILING, AMEND])
    assert result == "registration_amended" and got["ncen_accession"] == ACC
    assert got["amendment_accessions"] == [AMEND.accession]


def test_amendment_for_an_older_period_passes() -> None:
    assert outcome(filings=[SPY_FILING, OLD_AMEND])[0] == "candidate"


def test_newest_period_picks_newest_original() -> None:
    older = Filing("0001410368-26-000001", "N-CEN", date(2026, 3, 1), date(2025, 12, 31), SPY_FILING.url)
    period, newest, amendments = newest_period([older, SPY_FILING])
    assert period == date(2025, 12, 31) and newest == SPY_FILING and amendments == []


@pytest.mark.parametrize(
    "url",
    [
        "https://www.sec.gov/Archives/edgar/data/1/000141036826023904/primary_doc.xml",
        "https://www.sec.gov/Archives/edgar/data/884394/000141036826000001/primary_doc.xml",
        "https://example.com/Archives/edgar/data/884394/000141036826023904/primary_doc.xml",
        None,
    ],
)
def test_unsupported_url(url: str | None) -> None:
    f = Filing(ACC, "N-CEN", SPY_FILING.filing_date, SPY_FILING.report_date, url)
    assert ncen_xml_url(f, CIK) is None
    result, got = outcome(filings=[f])
    assert result == "registration_unavailable" and got["refusal"] == "unsupported URL"
    assert got["ncen_source_url"] == url


def test_raw_primary_doc_url_passes() -> None:
    f = Filing(ACC, "N-CEN", SPY_FILING.filing_date, SPY_FILING.report_date, ARCHIVE + "primary_doc.xml")
    assert ncen_xml_url(f, CIK) == ARCHIVE + "primary_doc.xml"
    assert ncen_xml_url(SPY_FILING, CIK) == ARCHIVE + "primary_doc.xml"
    assert outcome(filings=[f])[0] == "candidate"


def test_no_stored_document() -> None:
    result, got = outcome(ncen_body=None)
    assert result == "registration_unavailable" and got["refusal"] == "no stored document"


_REG = "<registrantCik>0000884394</registrantCik>"


@pytest.mark.parametrize(
    ("old", "new"),
    [
        # cardinality
        (
            "<registrantInfo>",
            "<registrantInfo><registrantCik>0000884394</registrantCik></registrantInfo><registrantInfo>",
        ),
        ("<exchangeSeriesInfo>", "<exchangeSeriesInfo><exchangeTradedFund/>"),
        (_REG, _REG + _REG),
        # period
        (' reportEndingPeriod="2025-12-31"', ""),
        ('reportEndingPeriod="2025-12-31"', 'reportEndingPeriod="2025-12-3"'),
        # registrant
        (_REG, "<registrantCik>0000000001</registrantCik>"),
        (LEI, "549300NZAMSJ8FXPQQ64"),
        (LEI, "549300NZAMSJ8FXPQQ6A"),
        (LEI, " " + LEI),
        (LEI, LEI.lower()),
        # form
        ("<registrantClassificationType>S-6<", "<registrantClassificationType>N-1A<"),
        ("<numOfExistingSeries>1<", "<numOfExistingSeries>2<"),
        ('<seriesCIK cik="0000884394"/>', '<seriesCIK cik="0000000001"/>'),
        ('xmlns="http://www.sec.gov/edgar/ncen"', 'xmlns="http://www.sec.gov/edgar/ncen2"'),
        ("<submissionType>N-CEN<", "<submissionType>N-CEN/A<"),
        # last filing
        ("<isRegistrantLastFiling>N</isRegistrantLastFiling>", ""),
        # listings
        ('fundExchange="ARCX"', 'fundExchange="ARCX1"'),
        (' fundsTickerSymbol="SPY"', ""),
        ('fundsTickerSymbol="SPY"', 'fundsTickerSymbol=" "'),
        (
            '<securityExchange fundExchange="ARCX" fundsTickerSymbol="SPY"/>',
            '<securityExchange fundExchange="ARCX" fundsTickerSymbol="SPY"/>'
            '<securityExchange fundExchange="XNAS" fundsTickerSymbol=""/>',
        ),
        # a scalar with a child element
        (f"<registrantLei>{LEI}</registrantLei>", f"<registrantLei>{LEI}<x/></registrantLei>"),
    ],
)
def test_registration_unusable(old: str, new: str) -> None:
    body = ncen(old, new)
    assert read_ncen(body, CIK) is None
    assert outcome(ncen_body=body)[0] == "registration_unusable"


def test_period_differing_from_the_db_report_date() -> None:
    body = ncen('reportEndingPeriod="2025-12-31"', 'reportEndingPeriod="2025-11-30"')
    assert read_ncen(body, CIK) is not None
    assert outcome(ncen_body=body)[0] == "registration_unusable"


def test_registration_final_keeps_accession_and_lei() -> None:
    body = ncen("<isRegistrantLastFiling>N<", "<isRegistrantLastFiling>Y<")
    result, got = outcome(ncen_body=body)
    assert result == "registration_final" and got["ncen_accession"] == ACC and got["lei"] == LEI


def test_lower_case_ticker_is_stored_as_filed() -> None:
    body = ncen('fundsTickerSymbol="SPY"', 'fundsTickerSymbol="spy"')
    result, got = outcome(ncen_body=body)
    assert result == "candidate" and got["listings"] == [["ARCX", "spy"]]


def test_capture_soundness() -> None:
    assert not ncen_sound(b"<html><body>SEC</body></html>")
    assert not ncen_sound(ncen("<submissionType>N-CEN<", "<submissionType>N-CEN/A<"))
    assert not ncen_sound(ncen("<submissionType>N-CEN</submissionType>", ""))


@pytest.mark.parametrize("symbol", ["QQQ", None])
def test_listing_symbol_absent(symbol: str | None) -> None:
    assert outcome(symbol=symbol)[0] == "fund_listing_unmatched"


def test_listing_symbol_twice() -> None:
    twice = '<securityExchange fundExchange="ARCX" fundsTickerSymbol="SPY"/>'
    body = ncen(twice, twice + '<securityExchange fundExchange="XNYS" fundsTickerSymbol="SPY"/>')
    assert outcome(ncen_body=body)[0] == "fund_listing_unmatched"


def test_lei_record_unavailable() -> None:
    assert outcome(record_body=None)[0] == "lei_record_unavailable"
    other = json.loads(RECORD_BODY)
    other["data"]["id"] = "5493000000000000000X"
    assert outcome(record_body=json.dumps(other).encode())[0] == "lei_record_unavailable"


@pytest.mark.parametrize(
    "status", ["PENDING_VALIDATION", "DUPLICATE", "LAPSED", "MERGED", "RETIRED", "ANNULLED", "CANCELLED", "TRANSFERRED"]
)
def test_refused_registration_status(status: str) -> None:
    assert outcome(record_body=record(registration_status=status))[0] == "lei_record_unsupported"


@pytest.mark.parametrize("status", ["PENDING_TRANSFER", "PENDING_ARCHIVAL"])
def test_allowed_registration_status(status: str) -> None:
    assert outcome(record_body=record(registration_status=status))[0] == "candidate"


@pytest.mark.parametrize(
    "entity",
    [
        {"status": "INACTIVE"},
        {"category": "GENERAL"},
        {"successorEntity": {"lei": "5493000000000000000X", "name": None}},
        {"successorEntity": {"lei": None, "name": "Successor Trust"}},
        {"successorEntity": {"lei": "", "name": None}},
        {"successorEntities": [{"lei": "5493000000000000000X"}]},
        {"legalName": {"name": "The ETF Trust"}},
    ],
)
def test_lei_record_unsupported(entity: dict[str, Any]) -> None:
    assert outcome(record_body=record(**entity))[0] == "lei_record_unsupported"


@pytest.mark.parametrize(
    "entity",
    [
        {"otherNames": {"name": "x"}},
        {"otherNames": [{"name": 7}]},
        {"successorEntity": {"lei": 549300, "name": None}},
        {"successorEntities": {}},
        {"status": None},
    ],
)
def test_lei_record_unsound(entity: dict[str, Any]) -> None:
    assert read_gleif_record(record(**entity), LEI) is None
    assert outcome(record_body=record(**entity))[0] == "lei_record_unavailable"


def test_isin_unavailable() -> None:
    assert outcome(isins_body=None)[0] == "isin_unavailable"


@pytest.mark.parametrize(
    "body",
    [
        isins([ISIN], lastPage=2),
        isins([ISIN], total=2),
        isins([ISIN] + [f"US00000000{i:02d}" for i in range(10)]),
        isins([]),
        isins([ISIN, ISIN]),
    ],
)
def test_isin_set_unsupported(body: bytes) -> None:
    assert outcome(isins_body=body)[0] == "isin_set_unsupported"


@pytest.mark.parametrize("body", [isins([ISIN], currentPage=2), isins([ISIN], total=True)])
def test_isin_set_unsound(body: bytes) -> None:
    assert read_gleif_isins(body) is None
    assert outcome(isins_body=body)[0] == "isin_unavailable"


@pytest.mark.parametrize("entry", ["US78462F1031", ISIN + " ", ISIN.lower()])
def test_isin_set_unusable(entry: str) -> None:
    assert outcome(isins_body=isins([entry]))[0] == "isin_set_unusable"


TWO = ["US78462F1030", "US0000000002"]


def test_mapping_unavailable() -> None:
    assert outcome(figi=None)[0] == "isin_mapping_unavailable"
    assert outcome(figi=(("US0000000002",), figi_body("SPY")))[0] == "isin_mapping_unavailable"
    reversed_order = (tuple(reversed(TWO)), figi_body("SPY", None))
    assert outcome(isins_body=isins(TWO), figi=reversed_order)[0] == "isin_mapping_unavailable"


def test_mapping_is_decoded_by_job_position() -> None:
    # The response lists SPY for the first job; decoded against the stored order, the first job is US78462F1030.
    body = figi_body("SPY", "QQQ")
    result, got = outcome(isins_body=isins(TWO), figi=(TWO, body))
    assert result == "candidate" and got["isin"] == TWO[0]
    # Decoded against reversed jobs, the same body would bind the other ISIN, which the request key prevents.
    assert outcome(isins_body=isins(list(reversed(TWO))), figi=(TWO, body))[0] == "isin_mapping_unavailable"


@pytest.mark.parametrize(
    "body", [json.dumps([{"error": "Invalid idType"}]).encode(), figi_body("SPY", "SPY"), b"[{}]", b"not json"]
)
def test_mapping_unsound(body: bytes) -> None:
    assert read_figi(body, 1) is None
    assert outcome(figi=((ISIN,), body))[0] == "isin_mapping_unavailable"


@pytest.mark.parametrize(
    "second",
    [
        None,
        {"data": []},
        {"data": [{"ticker": ""}]},
        {"data": [{"ticker": "   "}]},
        {"data": [{"exchCode": "UP"}]},
    ],
)
def test_isin_unresolved(second: Any) -> None:
    result, _ = outcome(isins_body=isins(TWO), figi=(TWO, figi_body("SPY", second)))
    assert result == "isin_unresolved"


def test_isin_unestablished() -> None:
    assert outcome(isins_body=isins(TWO), figi=(TWO, figi_body("SPY", "SPY")))[0] == "isin_unestablished"
    assert outcome(figi=((ISIN,), figi_body("QQQ")))[0] == "isin_unestablished"


def test_figi_request_key_is_ordered_canonical_json() -> None:
    assert figi_request_key([ISIN], "ARCX") == '[{"idType":"ID_ISIN","idValue":"US78462F1030","micCode":"ARCX"}]'
    assert figi_request_key(TWO, "ARCX") != figi_request_key(list(reversed(TWO)), "ARCX")


# --- the content item (Design 1's table) --------------------------------------------------------------------------

FETCHED = datetime(2026, 10, 14, 12, tzinfo=UTC)
ODS_URL = "https://assets.publishing.service.gov.uk/media/x/20261006_Master-Weblist.ods"


def content(published: Any = "2026-10-12T11:30:44+01:00", attachments: Any = None) -> bytes:
    if attachments is None:
        attachments = [{"url": ODS_URL, "file_size": 4549717}, {"url": "https://x/a.pdf"}]
    return json.dumps({"public_updated_at": published, "details": {"attachments": attachments}}).encode()


def test_content_item_sound() -> None:
    item = read_content_item(content(), FETCHED)
    assert item is not None and item.attachment_url == ODS_URL and item.file_size == 4549717


@pytest.mark.parametrize(
    "body",
    [
        b"[]",
        content(published="2026-99-14T11:30:44+01:00"),
        content(published="2026-10-12T11:30:44+00:60"),
        content(published="2026-10-12T11:30:44+01:99"),
        content(published="2026-10-12T11:30:44"),
        content(published="2026-10-15T11:30:44+01:00"),
        content(attachments=[{"url": ODS_URL, "file_size": 1}, {"url": ODS_URL, "file_size": 1}]),
        content(attachments=[{"url": "https://example.com/x.ods", "file_size": 1}]),
        content(attachments=[{"url": "http://assets.publishing.service.gov.uk/x.ods", "file_size": 1}]),
        content(attachments=[{"url": "https://assets.publishing.service.gov.uk:8443/x.ods", "file_size": 1}]),
        content(attachments=[{"url": ODS_URL, "file_size": "1"}]),
        content(attachments=[{"url": ODS_URL, "file_size": 0}]),
        content(attachments=[{"url": ODS_URL, "file_size": True}]),
        content(attachments={"url": ODS_URL}),
    ],
)
def test_content_item_unsound(body: bytes) -> None:
    assert read_content_item(body, FETCHED) is None


# --- ODS builder for tests 2 and 3 --------------------------------------------------------------------------------

_NS = (
    'xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0" '
    'xmlns:table="urn:oasis:names:tc:opendocument:xmlns:table:1.0" '
    'xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"'
)


def cell(text: str = "", raw: bool = False, attrs: str = 'office:value-type="string"') -> str:
    if not text:
        return f"<table:table-cell {attrs}/>" if attrs else "<table:table-cell/>"
    return f"<table:table-cell {attrs}><text:p>{text if raw else escape(text)}</text:p></table:table-cell>"


def row(*cells: str, attrs: str = "") -> str:
    return f"<table:table-row {attrs}>{''.join(cells)}</table:table-row>"


def data_row(
    parent: str = "Some Fund plc", isin: str = "no data", cusip: str = "no data", sub: str = "", ref: str = "A0001"
) -> str:
    return row(*(cell(c) for c in ("A", ref, parent, "A0001-0001", sub, isin, cusip, "01/01/2020", "no data")))


HEADER = row(*(cell(h) for h in LIST_HEADERS))


def table(*rows: str, columns: str = "<table:table-column/>") -> str:
    return f'<table:table table:name="Sheet1">{columns}{"".join(rows)}</table:table>'


def content_xml(*rows: str, body: str | None = None) -> bytes:
    inner = body if body is not None else table(HEADER, *rows)
    return (
        f'<?xml version="1.0" encoding="UTF-8"?><office:document-content {_NS}><office:body>'
        f"<office:spreadsheet>{inner}</office:spreadsheet></office:body></office:document-content>"
    ).encode()


def ods(xml: bytes, *, members: Sequence[tuple[str, bytes]] = (), method: int = zipfile.ZIP_DEFLATED) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("mimetype", "application/vnd.oasis.opendocument.spreadsheet", compress_type=zipfile.ZIP_STORED)
        z.writestr("content.xml", xml, compress_type=method)
        for name, data in members:
            z.writestr(name, data)
    return buf.getvalue()


def assess(*rows: str, baseline: int | None = 1, body: str | None = None) -> tfe.ListAssessment:
    return read_reporting_list(ods(content_xml(*rows, body=body)), baseline)


def usable(*rows: str, baseline: int | None = 1, body: str | None = None) -> tuple[tuple[str, ...], ...]:
    a = assess(*rows, baseline=baseline, body=body)
    assert a.refusal is None and a.rows is not None, a.refusal
    return a.rows


def refused(*rows: str, baseline: int | None = 1, body: str | None = None) -> str:
    a = assess(*rows, baseline=baseline, body=body)
    assert a.rows is None and a.refusal, a
    return a.refusal


def search(rows: Sequence[Sequence[str]], isin: str = ISIN, keys: Sequence[str] = KEYS) -> tfe.VersionSearch:
    return search_version(isin, keys, rows)


def r9(parent: str = "Other Fund", isin: str = "no data", cusip: str = "no data", sub: str = "") -> tuple[str, ...]:
    return ("A", "A0001", parent, "A0001-0001", sub, isin, cusip, "01/01/2020", "no data")


# --- test 2: match tests and outcome ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("r", "method", "key"),
    [
        (r9(isin="US78462F1030 "), "identifier", ISIN),
        (r9(isin="us78462f1030"), "identifier", ISIN),
        (r9(cusip="78462F103"), "identifier", "78462F103"),
        (r9(isin="78462F103"), "identifier", "78462F103"),
        (r9(isin="XUS78462F1030Y"), "identifier_in_text", ISIN),
        (r9(isin="US78462N1030", parent="SPDR S&P 500 ETF Trust"), "near_identifier", ISIN),
        (r9(isin="US78462F0130"), "near_identifier", ISIN),
        (r9(cusip="78462F104"), "near_identifier", "78462F103"),
        (r9(parent="SPDR S&P500 ETF Tr"), "name", "SPDRSP500"),
        (r9(parent="SPDR Trust, Series 1"), "name", "SPDR1"),
        (r9(parent="SPDR S&P 500 ETF TRUST", isin="US78462F1048"), "name", "SPDRSP500"),
    ],
)
def test_match_tests(r: tuple[str, ...], method: str, key: str) -> None:
    assert match_row(r, ISIN, KEYS) == (method, key)


def test_name_test_skips_a_valid_foreign_isin() -> None:
    assert match_row(r9(parent="SPDR S&P 500 UCITS ETF", isin="IE00B6YX5C33"), ISIN, KEYS) is None


def test_nsin_is_not_searched_for_an_ie_isin() -> None:
    ie = "IE00B5BMR087"
    assert match_row(r9(cusip="00B5BMR08"), ie, ["X"]) is None


def test_name_written_with_text_s_between_words() -> None:
    sp = '<text:s text:c="3"/>'
    rows = usable(row(*(cell(c, raw=True) for c in ("A", "A0001", f"SPDR{sp}S&amp;P 500", "x", "", "no data", "x"))))
    assert match_row(rows[0], ISIN, KEYS) == ("name", "SPDRSP500")


def test_counts_complete_with_25_examples_per_method() -> None:
    rows = [r9(parent="SPDR S&P 500 ETF") for _ in range(100)] + [r9(isin=ISIN)]
    found = search(rows)
    assert dict(found.counts) == {"identifier": 1, "identifier_in_text": 0, "near_identifier": 0, "name": 100}
    by: dict[str, int] = {}
    for e in found.examples:
        by[e["method"]] = by.get(e["method"], 0) + 1
    assert by == {"name": 25, "identifier": 1}
    assert search(list(reversed(rows))).counts == found.counts
    assert search_outcome([found.counts], ["searched"], content_current=True, child_list_usable=True) == "listed"


ZERO = dict.fromkeys(tfe.METHODS, 0)
IDENT = {**ZERO, "identifier": 1}
NAME = {**ZERO, "name": 2}


@pytest.mark.parametrize(
    ("searches", "coverage", "current", "usable_child", "expected"),
    [
        ([IDENT, ZERO], ["searched", "searched"], True, True, "listed"),  # older version listed, newest absent
        ([NAME, ZERO], ["searched", "searched"], True, True, "unresolved"),
        ([ZERO], ["searched", "usable_unsearched"], True, True, "unavailable"),  # the deadline came first
        ([IDENT], ["searched", "usable_unsearched"], True, True, "listed"),
        ([ZERO], ["pending", "searched"], True, True, "unavailable"),  # an older version pending
        ([IDENT], ["searched", "pending"], True, True, "listed"),
        ([ZERO], ["unusable", "searched"], True, True, "not_found"),
        ([ZERO], ["searched"], False, True, "unavailable"),
        ([ZERO], ["searched", "unusable"], True, False, "unavailable"),  # a newer unusable list
        ([], [], True, False, "unavailable"),
        ([ZERO, ZERO], ["searched", "searched"], True, True, "not_found"),
    ],
)
def test_search_outcome(
    searches: list[dict[str, int]], coverage: list[Any], current: bool, usable_child: bool, expected: str
) -> None:
    got = search_outcome(searches, coverage, content_current=current, child_list_usable=usable_child)
    assert got == expected


def test_search_checks_the_deadline() -> None:
    calls = []

    def tick() -> None:
        calls.append(1)
        if len(calls) > 1:
            raise DeadlineExceeded

    with pytest.raises(DeadlineExceeded):
        search_version(ISIN, KEYS, [r9()] * (3 * tfe.TICK_ROWS), tick)


# --- test 3: list usability ----------------------------------------------------------------------------------------


def test_minimal_list_is_usable() -> None:
    rows = usable(data_row(), data_row(parent="SPDR Fund"))
    assert len(rows) == 2 and rows[0][2] == "Some Fund plc" and all(len(r) == 9 for r in rows)


def test_archive_member_count() -> None:
    xml = content_xml(data_row())
    assert read_reporting_list(ods(xml, members=[("content.xml", xml)]), 1).refusal == "content_xml_count"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("styles.xml", "x")
    assert read_reporting_list(buf.getvalue(), 1).refusal == "content_xml_count"


def test_archive_declared_size(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tfe, "CONTENT_XML_MAX", 100)
    assert read_reporting_list(ods(content_xml(data_row())), 1).refusal == "content_xml_declared_size"


def test_archive_actual_size_is_capped() -> None:
    capped = tfe._Capped(io.BytesIO(b"x" * 10), 5, lambda: None)  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(ValueError):
        io.BufferedReader(capped).read()


def test_unsupported_compression() -> None:
    body = ods(content_xml(data_row()), method=zipfile.ZIP_BZIP2)
    assert read_reporting_list(body, 1).refusal == "content_xml_unsupported_compression"


def _patch_member(body: bytes, fn: Any) -> bytes:
    """Rewrite content.xml's local header and data in place, via ``fn(header: bytearray, data_offset, size)``."""
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        info = z.getinfo("content.xml")
    out = bytearray(body)
    start = info.header_offset
    name_len = int.from_bytes(out[start + 26 : start + 28], "little")
    extra_len = int.from_bytes(out[start + 28 : start + 30], "little")
    fn(out, start, start + 30 + name_len + extra_len, info.compress_size)
    return bytes(out)


def test_encrypted_content_xml() -> None:
    body = ods(content_xml(data_row()))
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        offset = z.getinfo("content.xml").header_offset
    patched = bytearray(body)
    # Set the encryption flag (bit 0) in the central directory entry and the local header.
    i = body.find(b"PK\x01\x02")
    while i != -1:
        n = int.from_bytes(patched[i + 28 : i + 30], "little")
        if patched[i + 46 : i + 46 + n] == b"content.xml":
            patched[i + 8] |= 0x1
        i = body.find(b"PK\x01\x02", i + 1)
    patched[offset + 6] |= 0x1
    assert read_reporting_list(bytes(patched), 1).refusal == "content_xml_unsupported_compression"


@pytest.mark.parametrize("damage", ["deflate", "crc", "truncate"])
def test_archive_failures_are_unusable(damage: str) -> None:
    body = ods(content_xml(*[data_row() for _ in range(50)]))
    if damage == "deflate":

        def fn(out: bytearray, _start: int, data: int, size: int) -> None:
            out[data : data + size] = b"\xff" * size

        body = _patch_member(body, fn)
    elif damage == "crc":
        with zipfile.ZipFile(io.BytesIO(body)) as z:
            crc = z.getinfo("content.xml").CRC.to_bytes(4, "little")
        body = body.replace(crc, bytes(b ^ 0xFF for b in crc))
    else:
        body = body[: len(body) // 2]
    result = read_reporting_list(body, 1)
    assert result.rows is None and result.refusal and result.refusal.startswith("unusable:")


def test_two_tables() -> None:
    assert refused(body=table(HEADER, data_row()) + table(HEADER, data_row())) == "tables=2"


def test_table_outside_spreadsheet() -> None:
    xml = (
        f'<?xml version="1.0"?><office:document-content {_NS}><office:body><office:text>'
        f"{table(HEADER, data_row())}</office:text></office:body></office:document-content>"
    ).encode()
    assert read_reporting_list(ods(xml), 1).refusal == "table_outside_spreadsheet"


def test_row_outside_table() -> None:
    assert refused(body=table(HEADER, data_row()) + data_row()) == "row_outside_table"


def test_row_group() -> None:
    body = table(HEADER, f"<table:table-row-group>{data_row()}</table:table-row-group>")
    assert refused(body=body) in ("row_outside_table", "table_child")


@pytest.mark.parametrize(
    "bad",
    [
        "<table:covered-table-cell/>",
        cell("x", attrs='table:number-columns-spanned="2"'),
        cell("x", attrs='table:formula="of:=1"'),
        cell("1", attrs='office:value-type="float"'),
        cell("x", attrs='office:value-type="string" office:string-value="x"'),
        "<table:table-cell><text:p>x<text:note><text:p>n</text:p></text:note></text:p></table:table-cell>",
        "<table:table-cell><text:p>x<text:s>y</text:s></text:p></table:table-cell>",
        "<table:table-cell><text:p>x<text:s><text:span/></text:s></text:p></table:table-cell>",
        "<table:table-cell><text:h>x</text:h></table:table-cell>",
    ],
)
def test_cell_refusals(bad: str) -> None:
    good = [cell(c) for c in ("A", "A0001", "Fund", "x", "", "no data", "no data", "", "")]
    good[8] = bad
    assert refused(row(*good))


def _text_of(xml_cell: str) -> str:
    cells = [cell(c) for c in ("A", "A0001", "Fund")] + [xml_cell]
    rows = usable(row(*cells))
    return rows[0][3]


@pytest.mark.parametrize(
    ("xml", "text"),
    [
        ("<text:p>A <text:span> B</text:span></text:p>", "A B"),
        ("<text:p>  A  </text:p>", "A"),
        ("<text:p>A<text:span>   </text:span>B</text:p>", "A B"),
        ('<text:p>A<text:s text:c="2"/>B</text:p>', "A  B"),
        ('<text:p>A <text:s text:c="2"/> B</text:p>', "A    B"),
        ("<text:p>A\tB\nC</text:p>", "A B C"),
        ("<text:p>A<text:tab/>B<text:line-break/>C</text:p>", "A\tB\nC"),
        ("<text:p>A</text:p><text:p>B</text:p>", "A\nB"),
        ("<text:p><text:s/><text:tab/>x</text:p>", " \tx"),
    ],
)
def test_white_space(xml: str, text: str) -> None:
    assert _text_of(f"<table:table-cell>{xml}</table:table-cell>") == text


def test_raw_cell_budget_across_paragraphs() -> None:
    half = " " * (tfe.MAX_CELL_RAW // 2)
    ok = f"<table:table-cell><text:p>a{half[1:]}</text:p><text:p>{half}</text:p></table:table-cell>"
    assert _text_of(ok) == "a\n"
    over = f"<table:table-cell><text:p>a{half}</text:p><text:p>{half}</text:p></table:table-cell>"
    cells = [cell(c) for c in ("A", "A0001", "Fund")] + [over]
    assert refused(row(*cells)) == "cell character data past its limit"


def test_version_text_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tfe, "MAX_VERSION_TEXT", 200)
    assert refused(*[data_row() for _ in range(5)]) == "version text past its limit"


@pytest.mark.parametrize("c", ["0", "-1", "1000000000000", "x"])
def test_bad_text_c(c: str) -> None:
    xml = f'<table:table-cell><text:p>A<text:s text:c="{c}"/></text:p></table:table-cell>'
    assert refused(row(*[cell(x) for x in ("A", "A0001", "Fund")], xml)) == "bad text:c"


def test_cell_logical_limits() -> None:
    many = "<table:table-cell>" + "<text:p/>" * 4098 + "</table:table-cell>"
    assert refused(row(*[cell(x) for x in ("A", "A0001", "Fund")], many)) == "cell text past its limit"
    long = cell("x" * 4097)
    assert refused(row(*[cell(x) for x in ("A", "A0001", "Fund")], long)) == "cell text past its limit"
    assert _text_of(cell("x" * 4096)) == "x" * 4096


@pytest.mark.parametrize(
    ("bad", "refusal"),
    [
        (row(cell("A"), attrs='table:number-rows-repeated="1048577"'), "bad number-rows-repeated"),
        (row(cell("A", attrs='table:number-columns-repeated="0"')), "bad number-columns-repeated"),
        (
            row(*[cell(c) for c in ("A", "A0001", "Fund")], attrs='table:number-rows-repeated="2"'),
            "repeated_row_with_content",
        ),
        (row(*[cell(c) for c in ("A", "A0001", "Fund", "", "", "", "", "", "", "ten")]), "content_past_column_nine"),
        (data_row(ref="1234"), "row_structure"),
        (data_row(parent="", sub=" "), "row_structure"),
    ],
)
def test_row_refusals(bad: str, refusal: str) -> None:
    assert refused(data_row(), bad) == refusal


def test_too_many_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tfe, "MAX_DATA_ROWS", 3)
    assert usable(*[data_row() for _ in range(3)])
    assert refused(*[data_row() for _ in range(4)]) == "too_many_rows"


def test_header_must_match() -> None:
    body = table(row(*(cell(h) for h in LIST_HEADERS[:-1])), data_row())
    assert refused(body=body) == "header"
    assert refused(body=table()) == "header"


def test_size_floors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tfe, "BOOTSTRAP_MIN_ROWS", 3)
    assert refused(body=table(HEADER), baseline=None) == "rows 0 below 3"  # header only
    assert refused(data_row(), data_row(), baseline=None) == "rows 2 below 3"
    assert len(usable(data_row(), data_row(), data_row(), baseline=None)) == 3
    rows = [data_row() for _ in range(89)]
    assert refused(*rows, baseline=100) == "rows 89 below 90.0"
    assert len(usable(*rows, data_row(), baseline=100)) == 90


def test_passes() -> None:
    short = row(*[cell(c) for c in ("A", "A0001", "Fund")])
    wide_empty = row(*[cell(c) for c in ("A", "A0001", "Fund")], cell(attrs='table:number-columns-repeated="16384"'))
    trailing = row(cell(), attrs='table:number-rows-repeated="923553"')
    rows = usable(short, wide_empty, trailing)
    assert rows == (("A", "A0001", "Fund", "", "", "", "", "", ""),) * 2


@pytest.mark.parametrize(
    "parent",
    [
        "SPDR S&amp;P 500",
        "SPDR  S&amp;P\t500",
        "SPDR\nS&amp;P 500",
        '<text:s text:c="2"/>SPDR<text:s/>S&amp;P<text:tab/>500',
    ],
)
def test_white_space_gives_one_name_key(parent: str) -> None:
    rows = usable(row(*(cell(c, raw=True) for c in ("A", "A0001", parent, "x", "", "no data", "no data"))))
    assert tfe.name_key(" ".join(rows[0][2:5])) == "SPDRSP500X"


def test_reader_checks_the_deadline() -> None:
    def tick() -> None:
        raise DeadlineExceeded

    with pytest.raises(DeadlineExceeded):
        read_reporting_list(ods(content_xml(data_row())), 1, tick)
