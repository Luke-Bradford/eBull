"""
Unit tests for the eToro broker provider rewrite.

Tests verify endpoint routing, request body shape, response normalisation,
error handling, and environment-scoped path prefixes.

No network calls — all HTTP interactions are mocked.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import httpx

from app.providers.implementations.etoro_broker import (
    EtoroBrokerProvider,
    _normalise_close_order_response,
    _normalise_open_order_response,
    _normalise_order_info_response,
)

# ---------------------------------------------------------------------------
# Fixtures — documented eToro API response shapes
# ---------------------------------------------------------------------------

FIXTURE_OPEN_ORDER_RESPONSE = {
    "orderForOpen": {
        "orderID": 12345,
        "statusID": "Executed",
        "instrumentID": 1001,
        "executionPrice": 185.50,
        "units": 0.54,
        "fees": 0.0,
    },
}

FIXTURE_CLOSE_ORDER_RESPONSE = {
    "orderForClose": {
        "positionID": 98765,
        "orderID": 12346,
        "statusID": "Executed",
        "instrumentID": 1001,
        "executionPrice": 190.25,
        "units": 0.54,
        "fees": 0.0,
    },
}

FIXTURE_ORDER_INFO_RESPONSE = {
    "orderID": 12345,
    "statusID": "Pending",
    "instrumentID": 1001,
    "amount": 100.0,
    "units": 0.54,
    "positions": [{"positionID": 98765}],
}

FIXTURE_PORTFOLIO_RESPONSE = {
    "clientPortfolio": {
        "positions": [
            {"instrumentID": 1001, "positionID": 98765},
            {"instrumentID": 1002, "positionID": 98766},
        ],
    },
}


# ---------------------------------------------------------------------------
# Environment-scoped path prefixes
# ---------------------------------------------------------------------------


class TestEnvironmentPrefixes:
    def test_demo_env_uses_demo_prefix(self) -> None:
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            assert broker._exec_prefix == "/api/v1/trading/execution/demo"
            assert broker._info_prefix == "/api/v1/trading/info/demo"

    def test_real_env_omits_demo_segment(self) -> None:
        with EtoroBrokerProvider(api_key="k", user_key="u", env="real") as broker:
            assert broker._exec_prefix == "/api/v1/trading/execution"
            assert broker._info_prefix == "/api/v1/trading/info"


# ---------------------------------------------------------------------------
# place_order
# ---------------------------------------------------------------------------


class TestPlaceOrderByAmount:
    def test_correct_endpoint_and_body(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_OPEN_ORDER_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._client = MagicMock()
            broker._client.post.return_value = mock_resp

            broker.place_order(1001, "BUY", amount=Decimal("100"), units=None)

            broker._client.post.assert_called_once()
            call_args = broker._client.post.call_args
            endpoint = call_args.args[0]
            body = call_args.kwargs["json"]

            assert endpoint == "/api/v1/trading/execution/demo/market-open-orders/by-amount"
            assert body["InstrumentID"] == 1001
            assert body["IsBuy"] is True
            assert body["Leverage"] == 1
            assert body["Amount"] == 100.0
            assert "AmountInUnits" not in body

    def test_returns_filled_result(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_OPEN_ORDER_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._client = MagicMock()
            broker._client.post.return_value = mock_resp

            result = broker.place_order(1001, "BUY", amount=Decimal("100"), units=None)

            assert result.status == "filled"
            assert result.broker_order_ref == "12345"
            assert result.filled_price == Decimal("185.5")
            assert result.filled_units == Decimal("0.54")

    def test_domain_action_preserved_in_raw_payload(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {**FIXTURE_OPEN_ORDER_RESPONSE}

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._client = MagicMock()
            broker._client.post.return_value = mock_resp

            result = broker.place_order(1001, "ADD", amount=Decimal("50"), units=None)

            assert result.raw_payload["_ebull_action"] == "ADD"


class TestPlaceOrderByUnits:
    def test_correct_endpoint_and_body(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_OPEN_ORDER_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._client = MagicMock()
            broker._client.post.return_value = mock_resp

            broker.place_order(1001, "BUY", amount=None, units=Decimal("0.5"))

            call_args = broker._client.post.call_args
            endpoint = call_args.args[0]
            body = call_args.kwargs["json"]

            assert endpoint == "/api/v1/trading/execution/demo/market-open-orders/by-units"
            assert body["InstrumentID"] == 1001
            assert body["AmountInUnits"] == 0.5
            assert "Amount" not in body


class TestPlaceOrderActionGuard:
    def test_exit_action_returns_failed(self) -> None:
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            result = broker.place_order(1001, "EXIT", amount=Decimal("100"), units=None)

            assert result.status == "failed"
            assert "EXIT" in result.raw_payload["error"]

    def test_unrecognised_action_returns_failed(self) -> None:
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            result = broker.place_order(1001, "SELL", amount=Decimal("100"), units=None)

            assert result.status == "failed"
            assert "SELL" in result.raw_payload["error"]

    def test_hold_action_returns_failed(self) -> None:
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            result = broker.place_order(1001, "HOLD", amount=Decimal("100"), units=None)

            assert result.status == "failed"

    def test_no_amount_or_units_returns_failed(self) -> None:
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            result = broker.place_order(1001, "BUY", amount=None, units=None)

            assert result.status == "failed"
            assert "Neither" in result.raw_payload["error"]


class TestPlaceOrderRealEnv:
    def test_real_env_uses_correct_prefix(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_OPEN_ORDER_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="real") as broker:
            broker._client = MagicMock()
            broker._client.post.return_value = mock_resp

            broker.place_order(1001, "BUY", amount=Decimal("100"), units=None)

            endpoint = broker._client.post.call_args.args[0]
            assert endpoint == "/api/v1/trading/execution/market-open-orders/by-amount"
            assert "/demo/" not in endpoint


# ---------------------------------------------------------------------------
# close_position
# ---------------------------------------------------------------------------


class TestClosePosition:
    def test_portfolio_lookup_then_close(self) -> None:
        """Verifies two-step flow: GET portfolio → POST close."""
        portfolio_resp = MagicMock()
        portfolio_resp.json.return_value = FIXTURE_PORTFOLIO_RESPONSE

        close_resp = MagicMock()
        close_resp.json.return_value = FIXTURE_CLOSE_ORDER_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._client = MagicMock()
            # First call: portfolio GET; second call: close POST
            broker._client.get.return_value = portfolio_resp
            broker._client.post.return_value = close_resp

            result = broker.close_position(1001)

            # Portfolio lookup
            broker._client.get.assert_called_once()
            get_endpoint = broker._client.get.call_args.args[0]
            assert get_endpoint == "/api/v1/trading/info/demo/portfolio"

            # Close call uses resolved positionId
            broker._client.post.assert_called_once()
            post_endpoint = broker._client.post.call_args.args[0]
            assert post_endpoint == "/api/v1/trading/execution/demo/market-close-orders/positions/98765"

            # Close body
            body = broker._client.post.call_args.kwargs["json"]
            assert body["InstrumentID"] == 1001
            assert body["UnitsToDeduct"] is None

            assert result.status == "filled"
            assert result.broker_order_ref == "12346"

    def test_no_open_position_returns_failed(self) -> None:
        portfolio_resp = MagicMock()
        portfolio_resp.json.return_value = {
            "clientPortfolio": {"positions": []},
        }

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._client = MagicMock()
            broker._client.get.return_value = portfolio_resp

            result = broker.close_position(9999)

            assert result.status == "failed"
            assert "No open position" in result.raw_payload["error"]
            # No close POST should have been attempted
            broker._client.post.assert_not_called()

    def test_portfolio_lookup_failure_returns_failed_with_error(self) -> None:
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._client = MagicMock()
            broker._client.get.side_effect = httpx.ConnectError("connection refused")

            result = broker.close_position(1001)

            assert result.status == "failed"
            assert "No open position" in result.raw_payload["error"]
            broker._client.post.assert_not_called()


# ---------------------------------------------------------------------------
# get_order_status
# ---------------------------------------------------------------------------


class TestGetOrderStatus:
    def test_correct_endpoint(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_ORDER_INFO_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._client = MagicMock()
            broker._client.get.return_value = mock_resp

            broker.get_order_status("12345")

            broker._client.get.assert_called_once()
            endpoint = broker._client.get.call_args.args[0]
            assert endpoint == "/api/v1/trading/info/demo/orders/12345"

    def test_returns_pending_status(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_ORDER_INFO_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._client = MagicMock()
            broker._client.get.return_value = mock_resp

            result = broker.get_order_status("12345")

            assert result.status == "pending"
            assert result.broker_order_ref == "12345"

    def test_preserves_ref_on_failure(self) -> None:
        """When HTTP fails, the original broker_order_ref is preserved."""
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._client = MagicMock()
            broker._client.get.side_effect = httpx.ConnectError("timeout")

            result = broker.get_order_status("12345")

            assert result.status == "failed"
            assert result.broker_order_ref == "12345"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    def test_http_status_error_returns_failed_with_payload(self) -> None:
        error_resp = MagicMock()
        error_resp.status_code = 400
        error_resp.json.return_value = {"message": "Bad request"}
        error_resp.text = '{"message": "Bad request"}'

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._client = MagicMock()
            broker._client.post.side_effect = httpx.HTTPStatusError(
                "400",
                request=MagicMock(),
                response=error_resp,
            )

            result = broker.place_order(1001, "BUY", amount=Decimal("100"), units=None)

            assert result.status == "failed"
            assert result.raw_payload["message"] == "Bad request"
            assert result.raw_payload["_ebull_action"] == "BUY"

    def test_network_error_returns_failed_with_error_string(self) -> None:
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._client = MagicMock()
            broker._client.post.side_effect = httpx.ConnectError("connection refused")

            result = broker.place_order(1001, "BUY", amount=Decimal("100"), units=None)

            assert result.status == "failed"
            assert "connection refused" in result.raw_payload["error"]
            assert result.raw_payload["_ebull_action"] == "BUY"

    def test_non_json_error_response_fallback(self) -> None:
        """When error response is not JSON, raw_text is captured."""
        error_resp = MagicMock()
        error_resp.status_code = 500
        error_resp.json.side_effect = ValueError("not JSON")
        error_resp.text = "Internal Server Error"

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._client = MagicMock()
            broker._client.post.side_effect = httpx.HTTPStatusError(
                "500",
                request=MagicMock(),
                response=error_resp,
            )

            result = broker.place_order(1001, "BUY", amount=Decimal("100"), units=None)

            assert result.status == "failed"
            assert result.raw_payload["raw_text"] == "Internal Server Error"


# ---------------------------------------------------------------------------
# Response normalisers
# ---------------------------------------------------------------------------


class TestNormaliseOpenOrderResponse:
    def test_extracts_order_for_open_fields(self) -> None:
        result = _normalise_open_order_response(FIXTURE_OPEN_ORDER_RESPONSE)

        assert result.broker_order_ref == "12345"
        assert result.status == "filled"
        assert result.filled_price == Decimal("185.5")
        assert result.filled_units == Decimal("0.54")
        assert result.fees == Decimal("0")

    def test_unknown_status_defaults_to_pending(self) -> None:
        raw = {"orderForOpen": {"orderID": 1, "statusID": "UnknownStatus"}}
        result = _normalise_open_order_response(raw)
        assert result.status == "pending"

    def test_missing_order_for_open_uses_raw_directly(self) -> None:
        """Fallback: if orderForOpen key is absent, use the raw dict itself."""
        raw = {"orderID": 999, "statusID": "Executed"}
        result = _normalise_open_order_response(raw)
        assert result.broker_order_ref == "999"
        assert result.status == "filled"


class TestNormaliseCloseOrderResponse:
    def test_extracts_order_for_close_fields(self) -> None:
        result = _normalise_close_order_response(FIXTURE_CLOSE_ORDER_RESPONSE)

        assert result.broker_order_ref == "12346"
        assert result.status == "filled"
        assert result.filled_price == Decimal("190.25")

    def test_missing_optional_fields(self) -> None:
        raw = {"orderForClose": {"orderID": 1, "statusID": "Pending"}}
        result = _normalise_close_order_response(raw)
        assert result.filled_price is None
        assert result.filled_units is None
        assert result.fees == Decimal("0")


class TestNormaliseOrderInfoResponse:
    def test_extracts_order_info_fields(self) -> None:
        result = _normalise_order_info_response(FIXTURE_ORDER_INFO_RESPONSE, "12345")

        assert result.broker_order_ref == "12345"
        assert result.status == "pending"
        assert result.filled_units == Decimal("0.54")

    def test_fallback_ref_used_when_order_id_missing(self) -> None:
        raw = {"statusID": "Executed"}
        result = _normalise_order_info_response(raw, "fallback-ref")
        assert result.broker_order_ref == "fallback-ref"

    def test_no_status_defaults_to_pending(self) -> None:
        raw = {"orderID": 1}
        result = _normalise_order_info_response(raw, "1")
        assert result.status == "pending"


# ---------------------------------------------------------------------------
# Request body shape validation
# ---------------------------------------------------------------------------


class TestRequestBodyShape:
    """Verify eToro-specific constraints on request bodies."""

    def test_by_amount_body_has_required_fields(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_OPEN_ORDER_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._client = MagicMock()
            broker._client.post.return_value = mock_resp

            broker.place_order(1001, "BUY", amount=Decimal("250"), units=None)

            body = broker._client.post.call_args.kwargs["json"]
            assert body["IsBuy"] is True
            assert body["Leverage"] == 1
            assert body["StopLossRate"] is None
            assert body["TakeProfitRate"] is None
            assert body["IsTslEnabled"] is False
            assert body["IsNoStopLoss"] is True
            assert body["IsNoTakeProfit"] is True

    def test_by_units_body_uses_amount_in_units_field(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_OPEN_ORDER_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._client = MagicMock()
            broker._client.post.return_value = mock_resp

            broker.place_order(1001, "BUY", amount=None, units=Decimal("3.5"))

            body = broker._client.post.call_args.kwargs["json"]
            # Field is AmountInUnits, NOT Units
            assert body["AmountInUnits"] == 3.5
            assert "Units" not in body
            assert "Amount" not in body

    def test_close_body_has_required_fields(self) -> None:
        portfolio_resp = MagicMock()
        portfolio_resp.json.return_value = FIXTURE_PORTFOLIO_RESPONSE
        close_resp = MagicMock()
        close_resp.json.return_value = FIXTURE_CLOSE_ORDER_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._client = MagicMock()
            broker._client.get.return_value = portfolio_resp
            broker._client.post.return_value = close_resp

            broker.close_position(1001)

            body = broker._client.post.call_args.kwargs["json"]
            assert body["InstrumentID"] == 1001
            assert body["UnitsToDeduct"] is None
