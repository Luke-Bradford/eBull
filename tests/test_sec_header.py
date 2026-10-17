"""#3714: the EDGAR header acceptance parser, its Eastern -> UTC conversion, and the refill retry policy.

Expected instants are written out by hand from the America/New_York rules (EST = UTC-5, EDT = UTC-4; 2026 DST starts
03-08 02:00 and ends 11-01 02:00), not computed by the code under test.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from app.providers.implementations import sec_edgar
from app.providers.implementations.sec_header import (
    FINAL_OUTCOMES,
    RETRYABLE_OUTCOMES,
    classify_header,
    eastern_to_utc,
    fetch_header_acceptance,
    header_url,
)
from app.services.sec_acceptance_refill import next_retry_at

ACC = "0000320193-26-000006"
URL = header_url("0000320193", ACC)


def _header(*lines: str, accession: str = ACC) -> str:
    body = "\n".join((f"<ACCESSION-NUMBER>{accession}", "<TYPE>8-K", *lines))
    return f"<SEC-HEADER>0000320193-26-000006.hdr.sgml : 20260129\n{body}\n</SEC-HEADER>\n"


def test_header_url_is_built_from_the_manifest_cik() -> None:
    assert URL == "https://www.sec.gov/Archives/edgar/data/320193/000032019326000006/0000320193-26-000006.hdr.sgml"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("20260129163033", datetime(2026, 1, 29, 21, 30, 33, tzinfo=UTC)),  # winter, EST
        ("20250731163025", datetime(2025, 7, 31, 20, 30, 25, tzinfo=UTC)),  # summer, EDT
        ("20260308015959", datetime(2026, 3, 8, 6, 59, 59, tzinfo=UTC)),  # last EST second before spring-forward
        ("20260308030000", datetime(2026, 3, 8, 7, 0, 0, tzinfo=UTC)),  # first EDT wall time
        ("20261101005959", datetime(2026, 11, 1, 4, 59, 59, tzinfo=UTC)),  # last EDT second before the fold
        ("20261101020000", datetime(2026, 11, 1, 7, 0, 0, tzinfo=UTC)),  # first wall time after the fold
        ("20260308023000", None),  # spring-forward gap: never happened
        ("20261101013000", None),  # fall-back fold: two instants
        ("2026012916303", None),  # 13 digits
        ("202601291630330", None),  # 15 digits
        ("20260230120000", None),  # not a calendar date
        ("20260129246000", None),  # not a clock time
        ("2026-01-29T16:", None),
    ],
)
def test_eastern_to_utc(raw: str, expected: datetime | None) -> None:
    assert eastern_to_utc(raw) == expected


def test_found() -> None:
    check = classify_header(ACC, URL, 200, _header("<ACCEPTANCE-DATETIME>20260129163033", "<FILING-DATE>20260129"))
    assert (check.outcome, check.raw_value, check.acceptance_utc, check.http_status) == (
        "found",
        "20260129163033",
        datetime(2026, 1, 29, 21, 30, 33, tzinfo=UTC),
        200,
    )


def test_crlf_line_endings_still_found() -> None:
    text = _header("<ACCEPTANCE-DATETIME>20260129163033").replace("\n", "\r\n")
    assert classify_header(ACC, URL, 200, text).acceptance_utc == datetime(2026, 1, 29, 21, 30, 33, tzinfo=UTC)


@pytest.mark.parametrize(
    ("text", "outcome"),
    [
        (_header("<FILING-DATE>19990129"), "absent"),
        # A tag after the block is not the header's tag.
        (_header("<FILING-DATE>19990129") + "<ACCEPTANCE-DATETIME>20260129163033\n", "absent"),
        (_header("<ACCEPTANCE-DATETIME>2026012916303"), "invalid_value"),
        (_header("<ACCEPTANCE-DATETIME>202601291630330"), "invalid_value"),
        (_header("<ACCEPTANCE-DATETIME>20261101013000"), "invalid_value"),
        (_header("<ACCEPTANCE-DATETIME>20260129163033", "<ACCEPTANCE-DATETIME>20260129163034"), "bad_response"),
        (_header("<ACCEPTANCE-DATETIME>20260129163033", accession="0000320193-26-000007"), "bad_response"),
        (_header("<ACCEPTANCE-DATETIME>20260129163033").split("</SEC-HEADER>")[0], "bad_response"),  # truncated
        ("<html>rate limited</html>", "bad_response"),
        (_header("<ACCEPTANCE-DATETIME>20260129163033") * 2, "bad_response"),
    ],
)
def test_classification(text: str, outcome: str) -> None:
    check = classify_header(ACC, URL, 200, text)
    assert check.outcome == outcome
    assert (check.acceptance_utc is not None) == (outcome == "found")


# Verbatim from https://www.sec.gov/Archives/edgar/data/3453/000000345395000005/0000003453-95-000005.hdr.sgml
# (fetched 2026-10-17), the 1993-96 envelope.
_IMS_ACC = "0000003453-95-000005"
_IMS = """<IMS-HEADER>0000003453-95-000005.hdr.sgml : 19950609
<ACCESSION-NUMBER>0000003453-95-000005
<TYPE>DEF 14A
<PUBLIC-DOCUMENT-COUNT>1
<PERIOD>19941231
<FILING-DATE>19950306
<SROS>NONE
<FILER>
<COMPANY-DATA>
<CONFORMED-NAME>ALEXANDER & BALDWIN INC
<CIK>0000003453
<ASSIGNED-SIC>4400
<IRS-NUMBER>990032630
<STATE-OF-INCORPORATION>HI
<FISCAL-YEAR-END>1231
</COMPANY-DATA>
<FILING-VALUES>
<FORM-TYPE>DEF 14A
<ACT>34
<FILE-NUMBER>000-00565
<FILM-NUMBER>95518737
</FILING-VALUES>
<BUSINESS-ADDRESS>
<STREET1>822 BISHOP STREET
<CITY>HONOLULU
<STATE>HI
<ZIP>96813
<PHONE>8085256611
</BUSINESS-ADDRESS>
<MAIL-ADDRESS>
<STREET1>PO BOX 3440
<CITY>HONOLULU
<STATE>HI
<ZIP>96801
</MAIL-ADDRESS>
</FILER>
</IMS-HEADER>
"""


@pytest.mark.parametrize(
    ("text", "outcome"),
    [
        (_IMS, "absent"),  # the real header: final, no longer retried forever as bad_response
        (_IMS.replace("<SROS>NONE", "<ACCEPTANCE-DATETIME>19950306101500"), "found"),
        (_IMS.replace("</IMS-HEADER>", "</SEC-HEADER>"), "bad_response"),  # mismatched envelope
        (_IMS.replace("</IMS-HEADER>\n", ""), "bad_response"),  # truncated
        (_IMS + _header("<FILING-DATE>19990129"), "bad_response"),  # two blocks of different kinds
        (_IMS.replace("0000003453-95-000005\n<TYPE>", "0000003453-95-000006\n<TYPE>"), "bad_response"),
    ],
)
def test_ims_header_block(text: str, outcome: str) -> None:
    check = classify_header(_IMS_ACC, header_url("0000003453", _IMS_ACC), 200, text)
    assert check.outcome == outcome
    if outcome == "found":
        assert check.acceptance_utc == datetime(1995, 3, 6, 15, 15, 0, tzinfo=UTC)  # EST = UTC-5


def test_invalid_value_keeps_the_raw_text() -> None:
    check = classify_header(ACC, URL, 200, _header("<ACCEPTANCE-DATETIME>20260308023000"))
    assert (check.outcome, check.raw_value) == ("invalid_value", "20260308023000")


@pytest.mark.parametrize("status", [301, 403, 404, 410])
def test_returned_non_200_is_http_error(status: int) -> None:
    check = classify_header(ACC, URL, status, "")
    assert (check.outcome, check.http_status) == ("http_error", status)


class _Provider:
    def __init__(self, result: tuple[int, str] | BaseException) -> None:
        self._result = result
        self.urls: list[str] = []

    def fetch_submission_header(self, absolute_url: str) -> tuple[int, str]:
        self.urls.append(absolute_url)
        if isinstance(self._result, BaseException):
            raise self._result
        return self._result


def _status_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", URL)
    return httpx.HTTPStatusError("x", request=request, response=httpx.Response(status, request=request))


@pytest.mark.parametrize(
    ("result", "outcome", "status"),
    [
        ((200, _header("<ACCEPTANCE-DATETIME>20260129163033")), "found", 200),
        ((404, "not found"), "http_error", 404),
        (_status_error(429), "http_error", 429),
        (_status_error(503), "http_error", 503),
        (httpx.ConnectError("refused"), "transport_error", None),
        (httpx.ReadTimeout("slow"), "transport_error", None),
    ],
)
def test_fetch_branches(result: tuple[int, str] | BaseException, outcome: str, status: int | None) -> None:
    provider = _Provider(result)
    check = fetch_header_acceptance(provider, "0000320193", ACC)  # type: ignore[arg-type]
    assert (check.outcome, check.http_status, check.url) == (outcome, status, URL)
    assert provider.urls == [URL]


def test_outcome_sets_partition_the_constraint_list() -> None:
    assert FINAL_OUTCOMES.isdisjoint(RETRYABLE_OUTCOMES)
    assert FINAL_OUTCOMES | RETRYABLE_OUTCOMES == {
        "found", "absent", "invalid_value", "transport_error", "http_error", "bad_response",
    }  # fmt: skip


def test_header_client_shares_the_process_sec_throttle_and_never_retries() -> None:
    with sec_edgar.SecFilingsProvider(user_agent="eBull test test@example.com") as provider:
        client = provider._http_header  # pyright: ignore[reportPrivateUsage]
        assert client._last_request_at is sec_edgar._PROCESS_RATE_LIMIT_CLOCK  # pyright: ignore[reportPrivateUsage]
        assert client._throttle_lock is sec_edgar._PROCESS_RATE_LIMIT_LOCK  # pyright: ignore[reportPrivateUsage]
        assert client._max_retries == 0  # pyright: ignore[reportPrivateUsage]


def test_retry_backoff_doubles_then_reconsiders_weekly() -> None:
    now = datetime(2026, 10, 17, tzinfo=UTC)
    assert [next_retry_at(a, now) - now for a in range(1, 8)] == [timedelta(hours=h) for h in (1, 2, 4, 8, 16, 32)] + [
        timedelta(days=7)
    ]
