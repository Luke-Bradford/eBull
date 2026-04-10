"""
Unit tests for filings and fundamentals normalisation.

No network calls, no database — all tests use in-memory fixtures.
"""

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from app.providers.filings import FilingSearchResult, FilingsProvider
from app.providers.fundamentals import FundamentalsSnapshot
from app.providers.implementations.companies_house import (
    _normalise_filing_event as ch_normalise_event,
)
from app.providers.implementations.companies_house import (
    _normalise_filings as ch_normalise_filings,
)
from app.providers.implementations.fmp import (
    _build_snapshot,
    _decimal_or_none,
    _int_or_none,
    _margin_or_none,
)
from app.providers.implementations.sec_edgar import (
    _normalise_filings as sec_normalise_filings,
)
from app.providers.implementations.sec_edgar import (
    _parse_cik_mapping,
    _zero_pad_cik,
)
from app.services.fundamentals import _current_quarter_start, _fundamentals_are_fresh

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FIXTURE_FMP_BS = {
    "date": "2024-03-31",
    "cashAndCashEquivalents": "29965000000",
    "totalDebt": "108040000000",
    "netDebt": "78075000000",
    "commonStock": "15441000000",
    "bookValuePerShare": "4.24",
}

FIXTURE_FMP_INCOME_TTM = {
    "revenue": "383285000000",
    "grossProfitRatio": "0.4531",
    "operatingIncomeRatio": "0.2985",
    "epsdiluted": "6.43",
}

FIXTURE_FMP_CF_TTM = {
    "freeCashFlow": "99584000000",
}

FIXTURE_SEC_TICKERS = {
    "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
    "1": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft Corp"},
    "2": {"cik_str": 0, "ticker": "", "title": "Bad entry — skipped because ticker is empty"},
}

FIXTURE_SEC_SUBMISSIONS = {
    "tickers": ["AAPL"],
    "filings": {
        "recent": {
            "accessionNumber": [
                "0000320193-24-000123",
                "0000320193-23-000456",
                "0000320193-23-000789",
            ],
            "filingDate": ["2024-02-02", "2023-10-27", "2023-08-04"],
            "form": ["10-Q", "10-K", "10-Q"],
            "primaryDocument": ["aapl-20231231.htm", "aapl-20230930.htm", "aapl-20230701.htm"],
            "reportDate": ["2023-12-31", "2023-09-30", "2023-07-01"],
        }
    },
}

FIXTURE_CH_ITEMS = [
    {
        "transaction_id": "MmQ1YzM4ZTliYWM4YzM2",
        "date": "2024-03-15",
        "type": "AA",
        "links": {"filing": {"href": "/filing/04234567/MmQ1YzM4ZTliYWM4YzM2"}},
    },
    {
        "transaction_id": "abc123def456",
        "date": "2023-09-01",
        "type": "CS01",
        "links": {},
    },
    {
        # Missing transaction_id — should be skipped
        "date": "2023-01-01",
        "type": "AA",
    },
]


# ---------------------------------------------------------------------------
# FMP normaliser tests
# ---------------------------------------------------------------------------


class TestBuildSnapshot:
    def test_full_data_produces_snapshot(self) -> None:
        snap = _build_snapshot("AAPL", FIXTURE_FMP_BS, FIXTURE_FMP_INCOME_TTM, FIXTURE_FMP_CF_TTM)
        assert snap is not None
        assert snap.symbol == "AAPL"
        assert snap.as_of_date == date(2024, 3, 31)

    def test_as_of_date_from_balance_sheet(self) -> None:
        snap = _build_snapshot("AAPL", FIXTURE_FMP_BS, FIXTURE_FMP_INCOME_TTM, FIXTURE_FMP_CF_TTM)
        assert snap is not None
        assert snap.as_of_date == date(2024, 3, 31)

    def test_revenue_from_income(self) -> None:
        snap = _build_snapshot("AAPL", FIXTURE_FMP_BS, FIXTURE_FMP_INCOME_TTM, FIXTURE_FMP_CF_TTM)
        assert snap is not None
        assert snap.revenue_ttm == Decimal("383285000000")

    def test_margins_from_income(self) -> None:
        snap = _build_snapshot("AAPL", FIXTURE_FMP_BS, FIXTURE_FMP_INCOME_TTM, FIXTURE_FMP_CF_TTM)
        assert snap is not None
        assert snap.gross_margin == Decimal("0.4531")
        assert snap.operating_margin == Decimal("0.2985")

    def test_fcf_from_cashflow(self) -> None:
        snap = _build_snapshot("AAPL", FIXTURE_FMP_BS, FIXTURE_FMP_INCOME_TTM, FIXTURE_FMP_CF_TTM)
        assert snap is not None
        assert snap.fcf == Decimal("99584000000")

    def test_balance_sheet_fields(self) -> None:
        snap = _build_snapshot("AAPL", FIXTURE_FMP_BS, FIXTURE_FMP_INCOME_TTM, FIXTURE_FMP_CF_TTM)
        assert snap is not None
        assert snap.cash == Decimal("29965000000")
        assert snap.debt == Decimal("108040000000")
        assert snap.net_debt == Decimal("78075000000")
        assert snap.book_value == Decimal("4.24")

    def test_shares_outstanding_as_int(self) -> None:
        snap = _build_snapshot("AAPL", FIXTURE_FMP_BS, FIXTURE_FMP_INCOME_TTM, FIXTURE_FMP_CF_TTM)
        assert snap is not None
        assert snap.shares_outstanding == 15441000000

    def test_missing_income_leaves_ttm_fields_none(self) -> None:
        snap = _build_snapshot("AAPL", FIXTURE_FMP_BS, None, FIXTURE_FMP_CF_TTM)
        assert snap is not None
        assert snap.revenue_ttm is None
        assert snap.gross_margin is None
        assert snap.operating_margin is None
        assert snap.eps is None

    def test_missing_cashflow_leaves_fcf_none(self) -> None:
        snap = _build_snapshot("AAPL", FIXTURE_FMP_BS, FIXTURE_FMP_INCOME_TTM, None)
        assert snap is not None
        assert snap.fcf is None

    def test_missing_date_returns_none(self) -> None:
        bs = {k: v for k, v in FIXTURE_FMP_BS.items() if k != "date"}
        assert _build_snapshot("AAPL", bs, None, None) is None

    def test_bad_date_returns_none(self) -> None:
        bs = {**FIXTURE_FMP_BS, "date": "not-a-date"}
        assert _build_snapshot("AAPL", bs, None, None) is None

    def test_returns_fundamentals_snapshot(self) -> None:
        snap = _build_snapshot("AAPL", FIXTURE_FMP_BS, FIXTURE_FMP_INCOME_TTM, FIXTURE_FMP_CF_TTM)
        assert isinstance(snap, FundamentalsSnapshot)


class TestFmpHelpers:
    def test_decimal_or_none_numeric_string(self) -> None:
        assert _decimal_or_none("12345.67") == Decimal("12345.67")

    def test_decimal_or_none_integer(self) -> None:
        assert _decimal_or_none(42) == Decimal("42")

    def test_decimal_or_none_none(self) -> None:
        assert _decimal_or_none(None) is None

    def test_decimal_or_none_invalid(self) -> None:
        assert _decimal_or_none("not-a-number") is None

    def test_int_or_none_large_value(self) -> None:
        assert _int_or_none("15441000000") == 15441000000

    def test_int_or_none_zero_returns_none(self) -> None:
        assert _int_or_none("0") is None

    def test_int_or_none_none(self) -> None:
        assert _int_or_none(None) is None

    def test_margin_or_none_ratio(self) -> None:
        assert _margin_or_none("0.4531") == Decimal("0.4531")


# ---------------------------------------------------------------------------
# SEC EDGAR normaliser tests
# ---------------------------------------------------------------------------


class TestParseCikMapping:
    def test_parses_known_tickers(self) -> None:
        mapping = _parse_cik_mapping(FIXTURE_SEC_TICKERS)
        assert mapping["AAPL"] == "0000320193"
        assert mapping["MSFT"] == "0000789019"

    def test_empty_ticker_skipped(self) -> None:
        mapping = _parse_cik_mapping(FIXTURE_SEC_TICKERS)
        assert "" not in mapping

    def test_uppercases_tickers(self) -> None:
        raw = {"0": {"cik_str": 12345, "ticker": "aapl", "title": "Apple"}}
        mapping = _parse_cik_mapping(raw)
        assert "AAPL" in mapping

    def test_non_dict_input_returns_empty(self) -> None:
        assert _parse_cik_mapping(["not", "a", "dict"]) == {}

    def test_zero_pads_cik(self) -> None:
        raw = {"0": {"cik_str": 1, "ticker": "X", "title": "Test"}}
        mapping = _parse_cik_mapping(raw)
        assert mapping["X"] == "0000000001"


class TestZeroPadCik:
    def test_pads_short_cik(self) -> None:
        assert _zero_pad_cik(320193) == "0000320193"

    def test_leaves_10_digit_unchanged(self) -> None:
        assert _zero_pad_cik("0000320193") == "0000320193"

    def test_handles_string_input(self) -> None:
        assert _zero_pad_cik("12345") == "0000012345"


class TestSecNormaliseFilings:
    def test_returns_all_filings_when_no_filter(self) -> None:
        results = sec_normalise_filings(FIXTURE_SEC_SUBMISSIONS, "0000320193", None, None, None)
        assert len(results) == 3

    def test_sorted_oldest_first(self) -> None:
        results = sec_normalise_filings(FIXTURE_SEC_SUBMISSIONS, "0000320193", None, None, None)
        assert results[0].filed_at < results[1].filed_at < results[2].filed_at

    def test_filing_type_filter(self) -> None:
        results = sec_normalise_filings(FIXTURE_SEC_SUBMISSIONS, "0000320193", None, None, ["10-K"])
        assert len(results) == 1
        assert results[0].filing_type == "10-K"

    def test_start_date_filter(self) -> None:
        results = sec_normalise_filings(FIXTURE_SEC_SUBMISSIONS, "0000320193", date(2024, 1, 1), None, None)
        assert len(results) == 1
        assert results[0].filed_at.date() == date(2024, 2, 2)

    def test_end_date_filter(self) -> None:
        results = sec_normalise_filings(FIXTURE_SEC_SUBMISSIONS, "0000320193", None, date(2023, 9, 1), None)
        assert len(results) == 1

    def test_provider_filing_id_is_accession_number(self) -> None:
        results = sec_normalise_filings(FIXTURE_SEC_SUBMISSIONS, "0000320193", None, None, None)
        accessions = {r.provider_filing_id for r in results}
        assert "0000320193-24-000123" in accessions

    def test_primary_document_url_constructed(self) -> None:
        results = sec_normalise_filings(FIXTURE_SEC_SUBMISSIONS, "0000320193", None, None, None)
        # Most recent filing (after sort)
        newest = results[-1]
        assert newest.primary_document_url is not None
        assert "edgar/data" in newest.primary_document_url

    def test_period_of_report_parsed(self) -> None:
        results = sec_normalise_filings(FIXTURE_SEC_SUBMISSIONS, "0000320193", None, None, ["10-Q"])
        for r in results:
            assert r.period_of_report is not None

    def test_empty_submissions_returns_empty(self) -> None:
        raw: dict[str, object] = {"filings": {"recent": {}}}
        assert sec_normalise_filings(raw, "0000320193", None, None, None) == []

    def test_returns_filing_search_result(self) -> None:
        results = sec_normalise_filings(FIXTURE_SEC_SUBMISSIONS, "0000320193", None, None, None)
        assert all(isinstance(r, FilingSearchResult) for r in results)


# ---------------------------------------------------------------------------
# Companies House normaliser tests
# ---------------------------------------------------------------------------


class TestChNormaliseFilings:
    def test_returns_valid_items(self) -> None:
        results = ch_normalise_filings("04234567", FIXTURE_CH_ITEMS, None, None, None)
        assert len(results) == 2  # third item missing transaction_id, skipped

    def test_sorted_oldest_first(self) -> None:
        results = ch_normalise_filings("04234567", FIXTURE_CH_ITEMS, None, None, None)
        assert results[0].filed_at < results[1].filed_at

    def test_filing_type_filter(self) -> None:
        results = ch_normalise_filings("04234567", FIXTURE_CH_ITEMS, None, None, ["AA"])
        assert len(results) == 1
        assert results[0].filing_type == "AA"

    def test_provider_filing_id_format(self) -> None:
        results = ch_normalise_filings("04234567", FIXTURE_CH_ITEMS, None, None, None)
        # Should be "company_number/transaction_id"
        for r in results:
            assert r.provider_filing_id.startswith("04234567/")

    def test_document_url_from_links(self) -> None:
        results = ch_normalise_filings("04234567", FIXTURE_CH_ITEMS, None, None, ["AA"])
        assert results[0].primary_document_url is not None
        assert "company-information.service.gov.uk" in results[0].primary_document_url

    def test_missing_links_gives_none_url(self) -> None:
        results = ch_normalise_filings("04234567", FIXTURE_CH_ITEMS, None, None, ["CS01"])
        assert results[0].primary_document_url is None

    def test_start_date_filter(self) -> None:
        results = ch_normalise_filings("04234567", FIXTURE_CH_ITEMS, date(2024, 1, 1), None, None)
        assert len(results) == 1
        assert results[0].filed_at.date() == date(2024, 3, 15)

    def test_missing_transaction_id_skipped(self) -> None:
        items: list[dict[str, object]] = [{"date": "2024-01-01", "type": "AA"}]  # no transaction_id
        results = ch_normalise_filings("04234567", items, None, None, None)
        assert results == []

    def test_returns_filing_search_result(self) -> None:
        results = ch_normalise_filings("04234567", FIXTURE_CH_ITEMS, None, None, None)
        assert all(isinstance(r, FilingSearchResult) for r in results)


class TestChNormaliseEvent:
    def test_full_item(self) -> None:
        item = FIXTURE_CH_ITEMS[0]
        event = ch_normalise_event("04234567/MmQ1YzM4ZTliYWM4YzM2", "04234567", item)
        assert event.provider_filing_id == "04234567/MmQ1YzM4ZTliYWM4YzM2"
        assert event.filing_type == "AA"
        assert event.filed_at.date() == date(2024, 3, 15)

    def test_extracted_summary_none(self) -> None:
        event = ch_normalise_event("04234567/x", "04234567", FIXTURE_CH_ITEMS[0])
        assert event.extracted_summary is None

    def test_red_flag_score_none(self) -> None:
        event = ch_normalise_event("04234567/x", "04234567", FIXTURE_CH_ITEMS[0])
        assert event.red_flag_score is None


# ---------------------------------------------------------------------------
# FilingsProvider interface
# ---------------------------------------------------------------------------


class TestFilingsProviderIsAbstract:
    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError):
            FilingsProvider()  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# Fundamentals freshness skip (#168)
# ---------------------------------------------------------------------------


class TestCurrentQuarterStart:
    def test_q1(self) -> None:
        assert _current_quarter_start(date(2026, 2, 15)) == date(2026, 1, 1)

    def test_q2(self) -> None:
        assert _current_quarter_start(date(2026, 4, 10)) == date(2026, 4, 1)

    def test_q3(self) -> None:
        assert _current_quarter_start(date(2026, 9, 30)) == date(2026, 7, 1)

    def test_q4(self) -> None:
        assert _current_quarter_start(date(2026, 12, 1)) == date(2026, 10, 1)

    def test_first_day_of_quarter(self) -> None:
        assert _current_quarter_start(date(2026, 7, 1)) == date(2026, 7, 1)


def _mock_conn_fundamentals_fresh(has_row: bool) -> MagicMock:
    """Build a mock connection for fundamentals freshness check."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (1,) if has_row else None
    mock_conn.execute.return_value = mock_cursor
    return mock_conn


class TestFundamentalsAreFresh:
    def test_fresh_when_current_quarter_data_exists(self) -> None:
        today = date(2026, 4, 10)
        conn = _mock_conn_fundamentals_fresh(has_row=True)
        assert _fundamentals_are_fresh(conn, "1", today) is True

    def test_stale_when_no_current_quarter_data(self) -> None:
        today = date(2026, 4, 10)
        conn = _mock_conn_fundamentals_fresh(has_row=False)
        assert _fundamentals_are_fresh(conn, "1", today) is False

    def test_uses_correct_quarter_start_in_query(self) -> None:
        today = date(2026, 5, 20)
        conn = _mock_conn_fundamentals_fresh(has_row=False)
        _fundamentals_are_fresh(conn, "42", today)
        call_args = conn.execute.call_args
        params = call_args[0][1]
        assert params["quarter_start"] == date(2026, 4, 1)
        assert params["instrument_id"] == "42"
