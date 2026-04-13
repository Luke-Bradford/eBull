"""Tests for FMP currency enrichment via /profile endpoint."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from app.providers.implementations.fmp import FmpFundamentalsProvider, InstrumentProfile

FIXTURE_PROFILE_RESPONSE = [
    {
        "symbol": "AAPL",
        "currency": "USD",
        "exchangeShortName": "NASDAQ",
        "industry": "Consumer Electronics",
        "sector": "Technology",
    }
]

FIXTURE_PROFILE_GBP = [
    {
        "symbol": "BP.L",
        "currency": "GBp",  # FMP returns "GBp" (pence) for LSE
        "exchangeShortName": "LSE",
        "industry": "Oil & Gas",
        "sector": "Energy",
    }
]


def _make_provider() -> FmpFundamentalsProvider:
    """Build a provider with a mocked httpx.Client so no real HTTP occurs."""
    with patch("app.providers.implementations.fmp.httpx.Client"):
        provider = FmpFundamentalsProvider(api_key="test-key")
    return provider


def _mock_response(*, status_code: int = 200, json_data: object = None) -> MagicMock:
    """Build a mock httpx.Response with the given status and JSON body."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data if json_data is not None else []
    return resp


# ---------------------------------------------------------------------------
# get_instrument_profile
# ---------------------------------------------------------------------------


class TestGetInstrumentProfile:
    def test_returns_profile_with_currency(self) -> None:
        provider = _make_provider()
        provider._http = MagicMock()
        provider._http.get.return_value = _mock_response(json_data=FIXTURE_PROFILE_RESPONSE)

        profile = provider.get_instrument_profile("AAPL")

        assert profile is not None
        assert profile.symbol == "AAPL"
        assert profile.currency == "USD"
        assert profile.exchange == "NASDAQ"
        assert profile.sector == "Technology"
        assert profile.industry == "Consumer Electronics"

    def test_empty_response_returns_none(self) -> None:
        provider = _make_provider()
        provider._http = MagicMock()
        provider._http.get.return_value = _mock_response(json_data=[])

        profile = provider.get_instrument_profile("UNKNOWN")

        assert profile is None

    def test_gbp_pence_normalised(self) -> None:
        """FMP returns 'GBp' for LSE stocks — normalise to 'GBP'."""
        provider = _make_provider()
        provider._http = MagicMock()
        provider._http.get.return_value = _mock_response(json_data=FIXTURE_PROFILE_GBP)

        profile = provider.get_instrument_profile("BP.L")

        assert profile is not None
        assert profile.currency == "GBP"

    def test_non_200_returns_none(self) -> None:
        provider = _make_provider()
        provider._http = MagicMock()
        provider._http.get.return_value = _mock_response(status_code=404)

        profile = provider.get_instrument_profile("INVALID")

        assert profile is None

    def test_missing_currency_defaults_to_usd(self) -> None:
        """If the API returns no currency field, default to USD."""
        provider = _make_provider()
        provider._http = MagicMock()
        provider._http.get.return_value = _mock_response(
            json_data=[{"symbol": "X", "exchangeShortName": "NYSE"}],
        )

        profile = provider.get_instrument_profile("X")

        assert profile is not None
        assert profile.currency == "USD"

    def test_passes_apikey_param(self) -> None:
        provider = _make_provider()
        provider._http = MagicMock()
        provider._http.get.return_value = _mock_response(json_data=FIXTURE_PROFILE_RESPONSE)

        provider.get_instrument_profile("AAPL")

        provider._http.get.assert_called_once_with(
            "/v3/profile/AAPL",
            params={"apikey": "test-key"},
        )

    def test_returns_frozen_dataclass(self) -> None:
        provider = _make_provider()
        provider._http = MagicMock()
        provider._http.get.return_value = _mock_response(json_data=FIXTURE_PROFILE_RESPONSE)

        profile = provider.get_instrument_profile("AAPL")

        assert isinstance(profile, InstrumentProfile)
        with pytest.raises(AttributeError):
            profile.currency = "EUR"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# enrich_instrument_currencies
# ---------------------------------------------------------------------------


class TestEnrichInstrumentCurrencies:
    def test_enriches_instruments_with_null_currency(self) -> None:
        from app.services.universe import enrich_instrument_currencies

        provider = _make_provider()
        provider._http = MagicMock()
        provider._http.get.return_value = _mock_response(json_data=FIXTURE_PROFILE_RESPONSE)

        mock_conn = MagicMock()
        # First call: SELECT to find instruments needing enrichment
        mock_select_cursor = MagicMock()
        mock_select_cursor.fetchall.return_value = [("inst-1", "AAPL")]
        # Second call: UPDATE to set currency
        mock_update_cursor = MagicMock()
        mock_conn.execute.side_effect = [mock_select_cursor, mock_update_cursor]

        count = enrich_instrument_currencies(provider, mock_conn)

        assert count == 1
        # Verify the UPDATE was called with correct params
        update_call = mock_conn.execute.call_args_list[1]
        update_params = update_call[0][1]
        assert update_params["currency"] == "USD"
        assert update_params["instrument_id"] == "inst-1"

    def test_skips_instruments_with_no_profile(self) -> None:
        from app.services.universe import enrich_instrument_currencies

        provider = _make_provider()
        provider._http = MagicMock()
        provider._http.get.return_value = _mock_response(json_data=[])

        mock_conn = MagicMock()
        mock_select_cursor = MagicMock()
        mock_select_cursor.fetchall.return_value = [("inst-1", "UNKNOWN")]
        mock_conn.execute.return_value = mock_select_cursor

        count = enrich_instrument_currencies(provider, mock_conn)

        assert count == 0
        # Only the SELECT should have been called, no UPDATE
        assert mock_conn.execute.call_count == 1

    def test_returns_zero_when_no_instruments_need_enrichment(self) -> None:
        from app.services.universe import enrich_instrument_currencies

        provider = _make_provider()

        mock_conn = MagicMock()
        mock_select_cursor = MagicMock()
        mock_select_cursor.fetchall.return_value = []
        mock_conn.execute.return_value = mock_select_cursor

        count = enrich_instrument_currencies(provider, mock_conn)

        assert count == 0

    def test_enriches_multiple_instruments(self) -> None:
        from app.services.universe import enrich_instrument_currencies

        provider = _make_provider()
        provider._http = MagicMock()

        # Return different profiles for different symbols
        def _profile_for_symbol(url: str, *, params: object = None) -> MagicMock:
            if "AAPL" in url:
                return _mock_response(json_data=FIXTURE_PROFILE_RESPONSE)
            if "BP.L" in url:
                return _mock_response(json_data=FIXTURE_PROFILE_GBP)
            return _mock_response(json_data=[])

        provider._http.get.side_effect = _profile_for_symbol

        mock_conn = MagicMock()
        mock_select_cursor = MagicMock()
        mock_select_cursor.fetchall.return_value = [
            ("inst-1", "AAPL"),
            ("inst-2", "BP.L"),
            ("inst-3", "UNKNOWN"),
        ]
        mock_update_cursor = MagicMock()
        mock_conn.execute.side_effect = [
            mock_select_cursor,
            mock_update_cursor,  # UPDATE for AAPL
            mock_update_cursor,  # UPDATE for BP.L
        ]

        count = enrich_instrument_currencies(provider, mock_conn)

        assert count == 2
