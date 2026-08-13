"""Tests for app.providers.implementations.frankfurter."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import httpx
import pytest

from app.providers.implementations.frankfurter import fetch_latest_rates


class TestFetchLatestRates:
    def test_empty_targets_returns_empty(self) -> None:
        rates, ecb_date = fetch_latest_rates("USD", [])
        assert rates == {}
        assert ecb_date is None

    @patch("app.providers.implementations.frankfurter.httpx.Client")
    def test_parses_rates_and_date(self, mock_client_cls: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "base": "USD",
            "date": "2026-04-13",
            "rates": {"GBP": 0.7812, "EUR": 0.9234},
        }
        mock_response.raise_for_status = MagicMock()
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        rates, ecb_date = fetch_latest_rates("USD", ["GBP", "EUR"])

        assert ("USD", "GBP") in rates
        assert ("USD", "EUR") in rates
        assert rates[("USD", "GBP")] == Decimal("0.7812")
        assert rates[("USD", "EUR")] == Decimal("0.9234")
        assert ecb_date == "2026-04-13"

    @patch("app.providers.implementations.frankfurter.httpx.Client")
    def test_http_error_propagates(self, mock_client_cls: MagicMock) -> None:
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "500", request=MagicMock(), response=MagicMock()
        )
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        with pytest.raises(httpx.HTTPStatusError):
            fetch_latest_rates("USD", ["GBP"])

    @patch("app.providers.implementations.frankfurter.httpx.Client")
    def test_skips_unparseable_rate(self, mock_client_cls: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "rates": {"GBP": "not_a_number", "EUR": 0.92},
        }
        mock_response.raise_for_status = MagicMock()
        mock_client = MagicMock()
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client_cls.return_value = mock_client

        rates, _ecb_date = fetch_latest_rates("USD", ["GBP", "EUR"])

        # GBP should still be parsed (Decimal("not_a_number") actually raises)
        # but EUR should succeed
        assert ("USD", "EUR") in rates
