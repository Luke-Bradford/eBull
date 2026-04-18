"""
Unit tests for the eToro broker provider rewrite.

Tests verify endpoint routing, request body shape, response normalisation,
error handling, and environment-scoped path prefixes.

No network calls — all HTTP interactions are mocked.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import httpx

from app.providers.broker import (
    BrokerMirror,
    BrokerMirrorPosition,
    BrokerPortfolio,
    OrderParams,
)
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
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = mock_resp

            broker.place_order(1001, "BUY", amount=Decimal("100"), units=None)

            broker._http_write.post.assert_called_once()
            call_args = broker._http_write.post.call_args
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
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = mock_resp

            result = broker.place_order(1001, "BUY", amount=Decimal("100"), units=None)

            assert result.status == "filled"
            assert result.broker_order_ref == "12345"
            assert result.filled_price == Decimal("185.5")
            assert result.filled_units == Decimal("0.54")

    def test_domain_action_preserved_in_raw_payload(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {**FIXTURE_OPEN_ORDER_RESPONSE}

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = mock_resp

            result = broker.place_order(1001, "ADD", amount=Decimal("50"), units=None)

            assert result.raw_payload["_ebull_action"] == "ADD"


class TestPlaceOrderByUnits:
    def test_correct_endpoint_and_body(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_OPEN_ORDER_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = mock_resp

            broker.place_order(1001, "BUY", amount=None, units=Decimal("0.5"))

            call_args = broker._http_write.post.call_args
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

    def test_zero_amount_returns_failed(self) -> None:
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            result = broker.place_order(1001, "BUY", amount=Decimal("0"), units=None)

            assert result.status == "failed"
            assert "positive" in result.raw_payload["error"]

    def test_negative_units_returns_failed(self) -> None:
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            result = broker.place_order(1001, "BUY", amount=None, units=Decimal("-1"))

            assert result.status == "failed"
            assert "positive" in result.raw_payload["error"]

    def test_both_amount_and_units_returns_failed(self) -> None:
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            result = broker.place_order(
                1001,
                "BUY",
                amount=Decimal("100"),
                units=Decimal("0.5"),
            )

            assert result.status == "failed"
            assert "Both" in result.raw_payload["error"]


class TestPlaceOrderRealEnv:
    def test_real_env_uses_correct_prefix(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_OPEN_ORDER_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="real") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = mock_resp

            broker.place_order(1001, "BUY", amount=Decimal("100"), units=None)

            endpoint = broker._http_write.post.call_args.args[0]
            assert endpoint == "/api/v1/trading/execution/market-open-orders/by-amount"
            assert "/demo/" not in endpoint


# ---------------------------------------------------------------------------
# place_order — SL/TP params
# ---------------------------------------------------------------------------


class TestPlaceOrderParams:
    def test_place_order_passes_sl_tp_to_request_body(self) -> None:
        """SL/TP params appear in the eToro request body."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_OPEN_ORDER_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = mock_resp

            params = OrderParams(
                stop_loss_rate=Decimal("140.00"),
                take_profit_rate=Decimal("200.00"),
                is_tsl_enabled=True,
                leverage=2,
            )
            broker.place_order(
                instrument_id=1,
                action="BUY",
                amount=Decimal("100"),
                units=None,
                params=params,
            )

            body = broker._http_write.post.call_args.kwargs["json"]
            assert body["StopLossRate"] == 140.00
            assert body["TakeProfitRate"] == 200.00
            assert body["IsTslEnabled"] is True
            assert body["Leverage"] == 2
            assert body["IsNoStopLoss"] is False
            assert body["IsNoTakeProfit"] is False

    def test_place_order_none_params_uses_defaults(self) -> None:
        """None params preserves current behaviour: no SL, no TP, leverage 1."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_OPEN_ORDER_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = mock_resp

            broker.place_order(
                instrument_id=1,
                action="BUY",
                amount=Decimal("100"),
                units=None,
                params=None,
            )

            body = broker._http_write.post.call_args.kwargs["json"]
            assert body["StopLossRate"] is None
            assert body["TakeProfitRate"] is None
            assert body["IsTslEnabled"] is False
            assert body["Leverage"] == 1
            assert body["IsNoStopLoss"] is True
            assert body["IsNoTakeProfit"] is True


# ---------------------------------------------------------------------------
# close_position
# ---------------------------------------------------------------------------


class TestClosePosition:
    def test_close_position_posts_to_correct_endpoint(self) -> None:
        """close_position takes a position_id directly — no portfolio lookup."""
        close_resp = MagicMock()
        close_resp.json.return_value = FIXTURE_CLOSE_ORDER_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = close_resp

            result = broker.close_position(98765)

            broker._http_write.post.assert_called_once()
            post_endpoint = broker._http_write.post.call_args.args[0]
            assert post_endpoint == "/api/v1/trading/execution/demo/market-close-orders/positions/98765"

            body = broker._http_write.post.call_args.kwargs["json"]
            assert body["UnitsToDeduct"] is None
            assert "InstrumentID" not in body

            assert result.status == "filled"
            assert result.broker_order_ref == "12346"

    def test_close_position_partial_close(self) -> None:
        """units_to_deduct is passed through when provided."""
        close_resp = MagicMock()
        close_resp.json.return_value = FIXTURE_CLOSE_ORDER_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = close_resp

            broker.close_position(98765, units_to_deduct=Decimal("2.5"))

            body = broker._http_write.post.call_args.kwargs["json"]
            assert body["UnitsToDeduct"] == 2.5

    def test_close_position_network_error_returns_failed(self) -> None:
        """Network error during close POST returns a failed result."""
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.side_effect = httpx.ConnectError("connection refused")

            result = broker.close_position(98765)

            assert result.status == "failed"
            assert "Network error" in result.raw_payload["error"]


# ---------------------------------------------------------------------------
# get_order_status
# ---------------------------------------------------------------------------


class TestGetOrderStatus:
    def test_correct_endpoint(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_ORDER_INFO_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp

            broker.get_order_status("12345")

            broker._http_read.get.assert_called_once()
            endpoint = broker._http_read.get.call_args.args[0]
            assert endpoint == "/api/v1/trading/info/demo/orders/12345"

    def test_returns_pending_status(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_ORDER_INFO_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp

            result = broker.get_order_status("12345")

            assert result.status == "pending"
            assert result.broker_order_ref == "12345"

    def test_preserves_ref_on_failure(self) -> None:
        """When HTTP fails, the original broker_order_ref is preserved."""
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.side_effect = httpx.ConnectError("timeout")

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
            broker._http_write = MagicMock()
            broker._http_write.post.side_effect = httpx.HTTPStatusError(
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
            broker._http_write = MagicMock()
            broker._http_write.post.side_effect = httpx.ConnectError("connection refused")

            result = broker.place_order(1001, "BUY", amount=Decimal("100"), units=None)

            assert result.status == "failed"
            assert "Network error" in result.raw_payload["error"]
            assert result.raw_payload["_ebull_action"] == "BUY"

    def test_non_json_success_response_returns_failed(self) -> None:
        """When a 200 response body is not valid JSON, return status=failed."""
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.side_effect = ValueError("not JSON")

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = mock_resp

            result = broker.place_order(1001, "BUY", amount=Decimal("100"), units=None)

            assert result.status == "failed"
            assert "Non-JSON" in result.raw_payload["error"]

    def test_non_json_error_response_fallback(self) -> None:
        """When error response is not JSON, raw_text is captured."""
        error_resp = MagicMock()
        error_resp.status_code = 500
        error_resp.json.side_effect = ValueError("not JSON")
        error_resp.text = "Internal Server Error"

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.side_effect = httpx.HTTPStatusError(
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
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = mock_resp

            broker.place_order(1001, "BUY", amount=Decimal("250"), units=None)

            body = broker._http_write.post.call_args.kwargs["json"]
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
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = mock_resp

            broker.place_order(1001, "BUY", amount=None, units=Decimal("3.5"))

            body = broker._http_write.post.call_args.kwargs["json"]
            # Field is AmountInUnits, NOT Units
            assert body["AmountInUnits"] == 3.5
            assert "Units" not in body
            assert "Amount" not in body

    def test_close_body_has_required_fields(self) -> None:
        close_resp = MagicMock()
        close_resp.json.return_value = FIXTURE_CLOSE_ORDER_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = close_resp

            broker.close_position(98765)

            body = broker._http_write.post.call_args.kwargs["json"]
            assert "InstrumentID" not in body
            assert body["UnitsToDeduct"] is None


# ---------------------------------------------------------------------------
# get_portfolio
# ---------------------------------------------------------------------------

# Field names match the real eToro /portfolio endpoint:
# - `openRate` (not `openPrice`) is the entry price
# - no current-price field exists in this endpoint — current prices
#   must be fetched separately from /instruments/rates
FIXTURE_FULL_PORTFOLIO_RESPONSE = {
    "clientPortfolio": {
        "positions": [
            {
                "instrumentID": 1001,
                "positionID": 98765,
                "units": 5.0,
                "openRate": 150.00,
                "openDateTime": "2026-03-15T10:30:00Z",
                "openConversionRate": 1.0,
                "amount": 750.00,
                "initialAmountInDollars": 750.00,
                "isBuy": True,
                "leverage": 1,
                "stopLossRate": 130.00,
                "takeProfitRate": 200.00,
                "isNoStopLoss": False,
                "isNoTakeProfit": False,
                "isTslEnabled": False,
                "totalFees": 2.50,
            },
            {
                "instrumentID": 1002,
                "positionID": 98766,
                "units": 10.0,
                "openRate": 50.00,
                "openDateTime": "2026-03-10T08:00:00Z",
                "openConversionRate": 0.78,
                "amount": 500.00,
                "initialAmountInDollars": 500.00,
                "isBuy": True,
                "leverage": 1,
                "isNoStopLoss": True,
                "isNoTakeProfit": True,
                "totalFees": 0.0,
            },
        ],
        "credit": 50000.50,
    },
}


@patch("app.providers.implementations.etoro_broker.raw_persistence.persist_raw_if_new")
class TestGetPortfolio:
    def test_returns_positions_and_cash(self, _mock_persist: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_FULL_PORTFOLIO_RESPONSE
        mock_resp.content = b"{}"
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp

            result = broker.get_portfolio()

        assert len(result.positions) == 2
        assert result.available_cash == Decimal("50000.50")

        p1 = result.positions[0]
        assert p1.instrument_id == 1001
        assert p1.units == Decimal("5.0")
        assert p1.open_price == Decimal("150.0")
        # current_price is a neutral placeholder (= open_price) because the
        # portfolio endpoint doesn't provide a current price. This makes
        # sync-time PnL aggregation evaluate to zero instead of producing
        # bogus negative values.
        assert p1.current_price == Decimal("150.0")
        # Per-position fields (migration 024)
        assert p1.position_id == 98765
        assert p1.is_buy is True
        assert p1.stop_loss_rate == Decimal("130.0")
        assert p1.take_profit_rate == Decimal("200.0")
        assert p1.is_no_stop_loss is False
        assert p1.is_no_take_profit is False
        assert p1.total_fees == Decimal("2.5")
        assert p1.leverage == 1

        p2 = result.positions[1]
        assert p2.instrument_id == 1002
        assert p2.units == Decimal("10.0")
        assert p2.open_price == Decimal("50.0")
        assert p2.current_price == Decimal("50.0")
        # Per-position fields — no SL/TP set
        assert p2.position_id == 98766
        assert p2.is_no_stop_loss is True
        assert p2.is_no_take_profit is True
        assert p2.stop_loss_rate is None
        assert p2.take_profit_rate is None

    def test_empty_portfolio(self, _mock_persist: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"clientPortfolio": {"positions": [], "credit": 100000}}
        mock_resp.content = b"{}"
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp

            result = broker.get_portfolio()

        assert len(result.positions) == 0
        assert result.available_cash == Decimal("100000")

    def test_missing_credit_defaults_to_zero(self, _mock_persist: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"clientPortfolio": {"positions": []}}
        mock_resp.content = b"{}"
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp

            result = broker.get_portfolio()

        assert result.available_cash == Decimal("0")

    def test_calls_correct_endpoint(self, _mock_persist: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"clientPortfolio": {"positions": [], "credit": 0}}
        mock_resp.content = b"{}"
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp

            broker.get_portfolio()

            url = broker._http_read.get.call_args.args[0]
            assert url == "/api/v1/trading/info/demo/portfolio"


# ---------------------------------------------------------------------------
# BrokerMirrorPosition / BrokerMirror / BrokerPortfolio.mirrors
# ---------------------------------------------------------------------------


def test_broker_mirror_position_round_trip() -> None:
    pos = BrokerMirrorPosition(
        position_id=1001,
        parent_position_id=5001,
        instrument_id=42,
        is_buy=True,
        units=Decimal("6.28927"),
        amount=Decimal("101.08"),
        initial_amount_in_dollars=Decimal("101.08"),
        open_rate=Decimal("1207.4994"),
        open_conversion_rate=Decimal("0.01331"),
        open_date_time=datetime(2026, 4, 10, 0, 0, tzinfo=UTC),
        take_profit_rate=None,
        stop_loss_rate=None,
        total_fees=Decimal("0"),
        leverage=1,
        raw_payload={"positionID": 1001},
    )
    assert pos.units == Decimal("6.28927")
    assert pos.open_conversion_rate == Decimal("0.01331")
    assert pos.is_buy is True
    assert pos.raw_payload["positionID"] == 1001


def test_broker_mirror_round_trip() -> None:
    mirror = BrokerMirror(
        mirror_id=15712187,
        parent_cid=111,
        parent_username="thomaspj",
        initial_investment=Decimal("20000"),
        deposit_summary=Decimal("0"),
        withdrawal_summary=Decimal("0"),
        available_amount=Decimal("2800.33"),
        closed_positions_net_profit=Decimal("-110.34"),
        stop_loss_percentage=None,
        stop_loss_amount=None,
        mirror_status_id=None,
        mirror_calculation_type=None,
        pending_for_closure=False,
        started_copy_date=datetime(2025, 1, 1, tzinfo=UTC),
        positions=(),
        raw_payload={"mirrorID": 15712187},
    )
    assert mirror.mirror_id == 15712187
    assert mirror.parent_username == "thomaspj"
    assert mirror.positions == ()


def test_broker_portfolio_mirrors_defaults_to_empty_tuple() -> None:
    """Existing callers must still be able to construct BrokerPortfolio
    without supplying mirrors (spec §2.1 non-breaking addition)."""
    portfolio = BrokerPortfolio(
        positions=(),
        available_cash=Decimal("0"),
        raw_payload={},
    )
    assert portfolio.mirrors == ()
