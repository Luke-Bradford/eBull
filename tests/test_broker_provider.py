"""
Unit tests for the eToro broker provider rewrite.

Tests verify endpoint routing, request body shape, response normalisation,
error handling, and environment-scoped path prefixes.

No network calls — all HTTP interactions are mocked.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import httpx
import pytest

from app.providers.broker import (
    BrokerCoreOrder,
    BrokerMirror,
    BrokerMirrorPosition,
    BrokerOrderNotFound,
    BrokerOrderSubmissionError,
    BrokerOrderSubmissionUncertain,
    BrokerPortfolio,
    BrokerPositionMutationError,
    BrokerStrategyOrder,
    BrokerWhatIfOrder,
    OrderParams,
)
from app.providers.implementations.etoro_broker import (
    EtoroBrokerProvider,
    OrderDetailParseError,
    TradingPreflightParseError,
    _normalise_close_order_response,
    _normalise_open_order_response,
    _normalise_order_info_response,
    _parse_order_detail,
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

FIXTURE_ORDER_DETAIL_RESPONSE = {
    "orderId": 13902598,
    "status": {"id": 3, "name": "Filled", "errorCode": 0, "errorMessage": None},
    "asset": {"instrumentId": 1001, "symbol": "AAPL"},
    "positionExecutions": [
        {
            "positionId": 9001,
            "state": "open",
            "remainingUnits": 6.5,
            "openingData": {
                "executionTime": "2026-08-09T09:00:01Z",
                "units": 6.5,
                "avgPrice": 95.25,
                "fees": 2.5,
            },
        },
        {
            "positionId": 9002,
            "state": "open",
            "remainingUnits": 4,
            "openingData": {
                "executionTime": "2026-08-09T09:00:02Z",
                "units": 4,
                "avgPrice": 95.5,
                "fees": 1.5,
            },
        },
    ],
    "lastUpdate": "2026-08-09T09:00:02Z",
}

FIXTURE_PORTFOLIO_RESPONSE = {
    "clientPortfolio": {
        "positions": [
            {"instrumentID": 1001, "positionID": 98765},
            {"instrumentID": 1002, "positionID": 98766},
        ],
    },
}

FIXTURE_ELIGIBILITY_RESPONSE = {
    "currency": "USD",
    "eligibilities": [
        {
            "instrumentId": 1001,
            "symbol": "AAPL",
            "minPositionExposure": 50,
            "maxUnitsPerOrder": 10000,
            "allowOpenPosition": True,
            "allowClosePosition": True,
            "allowPartialClosePosition": True,
            "allowTrailingStopLoss": True,
            "leverageConfigs": [
                {
                    "settlementType": "CFD",
                    "direction": "LONG",
                    "leverageValues": [1, 2, 5],
                    "minPositionAmount": 50,
                    "allowEditStopLoss": True,
                    "allowEditTakeProfit": True,
                    "allowStopLossTakeProfit": True,
                }
            ],
        }
    ],
    "notFoundInstrumentIds": [9999],
    "notFoundSymbols": [],
}

FIXTURE_WHAT_IF_COST_RESPONSE = {
    "instrumentId": 1001,
    "symbol": "AAPL",
    "costs": [
        {"costType": "marketSpread", "amount": 0.03, "currency": "USD"},
        {"costType": "transactionFee", "amount": 1, "currency": "USD"},
        {"costType": "overnightFee", "value": 0.0, "currency": "USD"},
    ],
    "lastUpdated": "2026-05-25T08:30:00Z",
}

FIXTURE_ACCOUNT_PNL_RESPONSE = {
    "clientPortfolio": {
        # Portal schema: "Currency ID of the account (1 = USD)".
        "accountCurrencyId": 1,
        "credit": 1000,
        # Portal: `isBuy` is "true for long (buy) positions, false for short (sell)".
        # Required on every direct position since #2704.
        "positions": [
            {"instrumentID": 1001, "amount": 200, "isBuy": True, "unrealizedPnL": {"pnL": 20}},
            {"instrumentID": 1002, "amount": 100, "isBuy": True, "unrealizedPnL": {"pnL": -5}},
        ],
        "mirrors": [
            {
                "availableAmount": 50,
                "closedPositionsNetProfit": 10,
                "positions": [
                    {"instrumentID": 1001, "amount": 25, "unrealizedPnL": {"pnL": 2}},
                ],
            }
        ],
        "ordersForOpen": [
            {"instrumentID": 1001, "mirrorID": 0, "amount": 40, "totalExternalCosts": 1},
            {"instrumentID": 1002, "mirrorID": 99, "amount": 999, "totalExternalCosts": 999},
        ],
        "orders": [{"instrumentID": 1002, "amount": 30}],
    }
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
# v2 non-executing trading preflight (#2437)
# ---------------------------------------------------------------------------


class TestTradingPreflight:
    def test_eligibility_posts_bounded_ids_to_current_demo_endpoint(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_ELIGIBILITY_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = mock_resp

            result = broker.check_instrument_eligibility([1001, 9999])

            call = broker._http_write.post.call_args
            assert call.args[0] == "/api/v2/trading/info/demo/eligibility"
            assert call.kwargs["json"] == {"instrumentIds": [1001, 9999], "currency": "USD"}
            assert result.currency == "USD"
            assert result.eligibilities[0].allow_open_position is True
            assert result.eligibilities[0].leverage_configs[0].leverage_values == (1, 2, 5)
            assert result.not_found_instrument_ids == (9999,)

    def test_eligibility_refuses_unbounded_or_ambiguous_requests(self) -> None:
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            for ids in ([], [1, 1], [0], list(range(1, 102))):
                try:
                    broker.check_instrument_eligibility(ids)
                except ValueError:
                    pass
                else:  # pragma: no cover - assertion helper branch
                    raise AssertionError(f"expected ValueError for {ids[:3]}")

    def test_eligibility_fails_closed_when_permission_field_is_missing(self) -> None:
        malformed = {
            **FIXTURE_ELIGIBILITY_RESPONSE,
            "eligibilities": [
                {
                    **FIXTURE_ELIGIBILITY_RESPONSE["eligibilities"][0],
                    "allowOpenPosition": None,
                }
            ],
        }
        mock_resp = MagicMock()
        mock_resp.json.return_value = malformed

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = mock_resp
            try:
                broker.check_instrument_eligibility([1001])
            except TradingPreflightParseError as exc:
                assert "allowOpenPosition" in str(exc)
            else:  # pragma: no cover - assertion helper branch
                raise AssertionError("missing permission must fail closed")

    def test_what_if_costs_posts_order_shape_and_preserves_open_cost_vocabulary(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_WHAT_IF_COST_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = mock_resp
            result = broker.get_what_if_costs(
                BrokerWhatIfOrder(
                    instrument_id=1001,
                    transaction="buy",
                    settlement_type="real",
                    amount=Decimal("1000"),
                )
            )

            call = broker._http_write.post.call_args
            assert call.args[0] == "/api/v2/trading/info/demo/costs"
            assert call.kwargs["json"] == {
                "action": "open",
                "transaction": "buy",
                "instrumentId": 1001,
                "settlementType": "real",
                "orderType": "mkt",
                "leverage": 1,
                "orderCurrency": "usd",
                "amount": 1000.0,
            }
            assert [(cost.cost_type, cost.amount, cost.value) for cost in result.costs] == [
                ("marketSpread", Decimal("0.03"), None),
                ("transactionFee", Decimal("1"), None),
                ("overnightFee", None, Decimal("0.0")),
            ]
            assert result.last_updated.tzinfo is not None

    def test_what_if_costs_sends_the_CLOSE_arm_with_its_position_ids(self) -> None:
        """The close arm exists and needs the position named — measured 2026-08-14
        (#2712): 400 "PositionIds must be provided for close action" without it, 200 with
        it.  ⚠ The live portal documents `positionIds` as "currently rejected"; the
        endpoint disagrees and the endpoint won.
        """
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_WHAT_IF_COST_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = mock_resp
            broker.get_what_if_costs(
                BrokerWhatIfOrder(
                    instrument_id=1001,
                    transaction="sell",
                    settlement_type="real",
                    amount=Decimal("1000"),
                    action="close",
                    position_ids=(3308441892,),
                )
            )

            body = broker._http_write.post.call_args.kwargs["json"]
            assert body["action"] == "close"
            assert body["transaction"] == "sell"
            assert body["positionIds"] == [3308441892]

    def test_the_close_arm_refuses_locally_rather_than_spending_a_doomed_request(self) -> None:
        """Validated in the dataclass, not left to the server: the 20/60s informational
        lane is a shared budget and a request that CANNOT succeed should not consume it.
        """
        with pytest.raises(ValueError, match="close arm requires position_ids"):
            BrokerWhatIfOrder(
                instrument_id=1001,
                transaction="sell",
                settlement_type="real",
                amount=Decimal("1000"),
                action="close",
            )

    def test_position_ids_are_rejected_on_the_OPEN_arm(self) -> None:
        """Both directions, so the presence of the tuple IS the arm and the two cannot
        drift apart in the request builder.
        """
        with pytest.raises(ValueError, match="meaningless on the open arm"):
            BrokerWhatIfOrder(
                instrument_id=1001,
                transaction="buy",
                settlement_type="real",
                amount=Decimal("1000"),
                position_ids=(3308441892,),
            )

    def test_an_action_and_transaction_that_do_not_pair_are_refused(self) -> None:
        """`Literal` is static only, so a dynamically built order arrives unvalidated.
        ⚠ The pairing is INFERRED from the vocabulary's structure, not measured — the
        probe never sent open/sell — so this is a local refusal of a meaningless
        combination, relaxable at the cost of one request if the inference is wrong.
        """
        for action, transaction in (("open", "sell"), ("close", "buy")):
            with pytest.raises(ValueError, match="not a"):
                BrokerWhatIfOrder(
                    instrument_id=1001,
                    transaction=transaction,  # type: ignore[arg-type]
                    settlement_type="real",
                    amount=Decimal("1000"),
                    action=action,  # type: ignore[arg-type]
                    position_ids=(1,) if action == "close" else (),
                )

    def test_what_if_order_requires_exactly_one_positive_size(self) -> None:
        for amount, units in (
            (None, None),
            (Decimal("1"), Decimal("1")),
            (Decimal("0"), None),
        ):
            try:
                BrokerWhatIfOrder(
                    instrument_id=1001,
                    transaction="buy",
                    settlement_type="real",
                    amount=amount,
                    units=units,
                )
            except ValueError:
                pass
            else:  # pragma: no cover - assertion helper branch
                raise AssertionError(f"invalid what-if size accepted: amount={amount}, units={units}")


class TestStrategyAccountRisk:
    def test_official_pnl_formula_counts_manual_positions_and_pending_orders(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_ACCOUNT_PNL_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp
            result = broker.get_account_risk_snapshot()

        assert result.available_cash == Decimal("930")  # 1000 - 40 - 30
        assert result.total_invested == Decimal("436")  # 200+100+(50-10)+25+40+1+30
        assert result.unrealized_pnl == Decimal("27")  # 20-5+2+10
        assert result.equity == Decimal("1393")
        assert [(row.instrument_id, row.amount) for row in result.instrument_investments] == [
            (1001, Decimal("266")),  # 200 direct + 25 mirror + (40 + 1) pending
            (1002, Decimal("130")),  # 100 direct + 30 order
        ]

    def test_direct_long_market_value_excludes_mirrors_and_pending_orders(self) -> None:
        """The core sleeve is a DIRECT holding; `amount` folds in three other things.

        Measured on the live demo account (#2704): 33 of 38 reported instruments had
        no direct position at all, so this separation is the common case rather than
        an edge one.
        """
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_ACCOUNT_PNL_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp
            result = broker.get_account_risk_snapshot()

        assert [
            (row.instrument_id, row.direct_long_market_value, row.direct_long_positions)
            for row in result.instrument_investments
        ] == [
            # 200 + 20, NOT 266: the mirror lot and the pending order are not the sleeve.
            (1001, Decimal("220"), 1),
            (1002, Decimal("95"), 1),  # 100 - 5, NOT 130.
        ]
        assert all(row.direct_short_positions == 0 for row in result.instrument_investments)

    def test_direct_long_lots_net_and_shorts_are_counted_not_valued(self) -> None:
        """Two lots net; a short is counted so a caller can REFUSE, never valued.

        ⚠ The short arm is unobserved live -- 7/7 demo positions were `isBuy: true`.
        The count exists because no money total can carry "a short exists": two lots
        can offset to zero and one short can sit at `amount + pnL == 0`.
        """
        payload = {
            "clientPortfolio": {
                "accountCurrencyId": 1,
                "credit": 1000,
                "positions": [
                    {"instrumentID": 1001, "amount": 200, "isBuy": True, "unrealizedPnL": {"pnL": 20}},
                    {"instrumentID": 1001, "amount": 100, "isBuy": True, "unrealizedPnL": {"pnL": -30}},
                    # Sums to exactly zero -- invisible to any money-valued short field.
                    {"instrumentID": 1001, "amount": 50, "isBuy": False, "unrealizedPnL": {"pnL": -50}},
                ],
                "mirrors": [],
                "ordersForOpen": [],
                "orders": [],
            }
        }
        mock_resp = MagicMock()
        mock_resp.json.return_value = payload

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp
            result = broker.get_account_risk_snapshot()

        (row,) = result.instrument_investments
        assert row.amount == Decimal("350")  # every direction, committed
        assert row.direct_long_market_value == Decimal("290")  # (200+20) + (100-30)
        assert row.direct_long_positions == 2
        assert row.direct_short_positions == 1

    def test_a_negative_direct_long_market_value_does_not_fail_the_parse(self) -> None:
        """A signed sum going negative is an extreme state, not response drift.

        `amount` sums documented non-negative terms, so a negative one IS drift and
        stays fail-closed.  Refusing here instead would take the paper executor's
        unrelated cash checks down with it; `_state_refusal` owns the refusal.
        """
        payload = {
            "clientPortfolio": {
                "accountCurrencyId": 1,
                "credit": 1000,
                "positions": [
                    {"instrumentID": 1001, "amount": 200, "isBuy": True, "unrealizedPnL": {"pnL": -250}},
                ],
                "mirrors": [],
                "ordersForOpen": [],
                "orders": [],
            }
        }
        mock_resp = MagicMock()
        mock_resp.json.return_value = payload

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp
            result = broker.get_account_risk_snapshot()

        (row,) = result.instrument_investments
        assert row.amount == Decimal("200")
        assert row.direct_long_market_value == Decimal("-50")

    def test_direct_position_direction_fails_closed(self) -> None:
        """Absent or non-boolean `isBuy` raises: defaulting it books a short as a long."""
        for position in (
            {"instrumentID": 1001, "amount": 200, "unrealizedPnL": {"pnL": 20}},
            {"instrumentID": 1001, "amount": 200, "isBuy": "true", "unrealizedPnL": {"pnL": 20}},
            {"instrumentID": 1001, "amount": 200, "isBuy": 1, "unrealizedPnL": {"pnL": 20}},
        ):
            payload = {
                "clientPortfolio": {
                    "accountCurrencyId": 1,
                    "credit": 1000,
                    "positions": [position],
                    "mirrors": [],
                    "ordersForOpen": [],
                    "orders": [],
                }
            }
            mock_resp = MagicMock()
            mock_resp.json.return_value = payload

            with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
                broker._http_read = MagicMock()
                broker._http_read.get.return_value = mock_resp
                try:
                    broker.get_account_risk_snapshot()
                except TradingPreflightParseError as exc:
                    assert "isBuy" in str(exc)
                else:  # pragma: no cover - assertion helper branch
                    raise AssertionError(f"missing/malformed isBuy must fail closed: {position}")

    def test_account_currency_id_is_read_from_the_payload(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_ACCOUNT_PNL_RESPONSE
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp
            assert broker.get_account_risk_snapshot().account_currency_id == 1

    def test_absent_account_currency_id_is_none_not_usd(self) -> None:
        """Absence must reach the evidence writer as absence (#2602 item 2)."""
        payload = {
            "clientPortfolio": {
                key: value
                for key, value in FIXTURE_ACCOUNT_PNL_RESPONSE["clientPortfolio"].items()
                if key != "accountCurrencyId"
            }
        }
        mock_resp = MagicMock()
        mock_resp.json.return_value = payload
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp
            assert broker.get_account_risk_snapshot().account_currency_id is None

    @pytest.mark.parametrize("value", ["1", 1.0, True, None])
    def test_malformed_account_currency_id_fails_closed(self, value: object) -> None:
        """A present-but-wrong-typed id is response drift, not absence."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "clientPortfolio": {
                **FIXTURE_ACCOUNT_PNL_RESPONSE["clientPortfolio"],
                "accountCurrencyId": value,
            }
        }
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp
            with pytest.raises(TradingPreflightParseError, match="accountCurrencyId"):
                broker.get_account_risk_snapshot()

    def test_account_risk_fails_closed_on_partial_pnl_shape(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "clientPortfolio": {
                **FIXTURE_ACCOUNT_PNL_RESPONSE["clientPortfolio"],
                "orders": None,
            }
        }
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp
            try:
                broker.get_account_risk_snapshot()
            except TradingPreflightParseError as exc:
                assert "orders" in str(exc)
            else:  # pragma: no cover
                raise AssertionError("partial P&L response must fail closed")

    def test_account_risk_fails_closed_without_live_envelope(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_ACCOUNT_PNL_RESPONSE["clientPortfolio"]
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp
            try:
                broker.get_account_risk_snapshot()
            except TradingPreflightParseError as exc:
                assert "clientPortfolio" in str(exc)
            else:  # pragma: no cover
                raise AssertionError("unwrapped P&L response must fail closed")


class TestDemoStrategyOrder:
    def test_v2_writer_is_demo_only_x1_fixed_exit_and_idempotent(self) -> None:
        request_id = UUID("1c94300c-90aa-4303-9d00-dec376d74efb")
        token = UUID("066faaee-e1e9-49d2-a568-c6e1cc336ad8")
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "token": str(token),
            "orderId": 13902598,
            "referenceId": str(request_id),
        }
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = mock_resp
            result = broker.place_demo_strategy_order(
                BrokerStrategyOrder(
                    instrument_id=1001,
                    amount=Decimal("100"),
                    settlement_type="real",
                    stop_loss_rate=Decimal("90"),
                    take_profit_rate=Decimal("120"),
                ),
                request_id=request_id,
            )
            call = broker._http_write.post.call_args
        assert call.args[0] == "/api/v2/trading/execution/demo/orders"
        assert call.kwargs["headers"] == {"x-request-id": str(request_id)}
        assert call.kwargs["json"]["leverage"] == 1
        assert call.kwargs["json"]["stopLossType"] == "fixed"
        assert call.kwargs["json"]["settlementType"] == "real"
        assert result.broker_order_ref == "13902598"
        assert result.reference_id == request_id

    def test_the_order_payload_is_the_cost_model_lane(self) -> None:
        """⚠ #2720: the cost model's carry/FX structural-zero closure holds for
        exactly the lane this writer trades, and this is the wire that holds
        the two together. NOT a tautology (the "#2240 phase 5c" prevention
        entry): the payload side is built from the writer's own literals, the
        lane side from ``cost_model``'s — neither imports the other. A future
        short / leveraged / non-USD writer change fails HERE, naming the cost
        model as the thing that must move with it.
        """
        from app.services.cost_model import STRUCTURAL_ZERO_LANE

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "token": "066faaee-e1e9-49d2-a568-c6e1cc336ad8",
            "orderId": 13902598,
            "referenceId": "1c94300c-90aa-4303-9d00-dec376d74efb",
        }
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = mock_resp
            broker.place_demo_strategy_order(
                BrokerStrategyOrder(
                    instrument_id=1001,
                    amount=Decimal("100"),
                    settlement_type="real",
                    stop_loss_rate=Decimal("90"),
                    take_profit_rate=Decimal("120"),
                ),
                request_id=UUID("1c94300c-90aa-4303-9d00-dec376d74efb"),
            )
            body = broker._http_write.post.call_args.kwargs["json"]

        # `transaction: buy` opening a position IS the long direction — the
        # only open transactions are buy (long) and sellShort (short).
        assert (body["transaction"], STRUCTURAL_ZERO_LANE.direction) == ("buy", "long")
        assert body["action"] == "open"
        assert body["leverage"] == STRUCTURAL_ZERO_LANE.leverage
        assert body["settlementType"] == STRUCTURAL_ZERO_LANE.settlement
        assert body["orderCurrency"] == STRUCTURAL_ZERO_LANE.order_currency.lower()

    def test_real_credentials_cannot_select_a_strategy_writer(self) -> None:
        with EtoroBrokerProvider(api_key="k", user_key="u", env="real") as broker:
            try:
                broker.place_demo_strategy_order(
                    BrokerStrategyOrder(
                        instrument_id=1001,
                        amount=Decimal("100"),
                        settlement_type="real",
                        stop_loss_rate=Decimal("90"),
                        take_profit_rate=Decimal("120"),
                    ),
                    request_id=uuid4(),
                )
            except BrokerOrderSubmissionError:
                pass
            else:  # pragma: no cover
                raise AssertionError("real credentials must not reach the paper writer")


class TestDemoCoreOrder:
    def test_writer_is_demo_only_buy_x1_real_usd_without_synthetic_exits(self) -> None:
        request_id = UUID("1c94300c-90aa-4303-9d00-dec376d74efb")
        raw = {
            "token": "066faaee-e1e9-49d2-a568-c6e1cc336ad8",
            "orderId": 13902598,
            "referenceId": str(request_id),
        }
        response = MagicMock()
        response.json.return_value = raw
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = response
            result = broker.place_demo_core_order(
                BrokerCoreOrder(instrument_id=3417, amount=Decimal("250")),
                request_id=request_id,
            )
            call = broker._http_write.post.call_args

        assert call.args[0] == "/api/v2/trading/execution/demo/orders"
        assert call.kwargs["headers"] == {"x-request-id": str(request_id)}
        assert call.kwargs["json"] == {
            "action": "open",
            "transaction": "buy",
            "instrumentId": 3417,
            "settlementType": "real",
            "orderType": "mkt",
            "leverage": 1,
            "amount": 250.0,
            "orderCurrency": "usd",
        }
        assert result.broker_order_ref == "13902598"
        assert result.reference_id == request_id
        assert result.response_digest == "0caf67206160f1c97554b988d9ff09aa6bec3813df377e1fc4aeb8f2f231c513"
        assert not hasattr(result, "token")

    def test_real_credentials_cannot_select_the_core_writer(self) -> None:
        with EtoroBrokerProvider(api_key="k", user_key="u", env="real") as broker:
            with pytest.raises(BrokerOrderSubmissionError, match="demo credentials"):
                broker.place_demo_core_order(
                    BrokerCoreOrder(instrument_id=3417, amount=Decimal("250")),
                    request_id=uuid4(),
                )

    def test_reference_mismatch_is_uncertain(self) -> None:
        response = MagicMock()
        response.json.return_value = {
            "token": "066faaee-e1e9-49d2-a568-c6e1cc336ad8",
            "orderId": 13902598,
            "referenceId": str(uuid4()),
        }
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = response
            with pytest.raises(BrokerOrderSubmissionUncertain, match="does not match"):
                broker.place_demo_core_order(
                    BrokerCoreOrder(instrument_id=3417, amount=Decimal("250")),
                    request_id=uuid4(),
                )


class TestDemoStrategyPositionMutations:
    def test_edit_uses_exact_v2_demo_route_and_validates_acceptance_identity(self) -> None:
        request_id = UUID("f95eab17-c3ac-4948-a281-d94fd1e2764b")
        operation_id = UUID("2165467c-73b8-4d2c-ac3c-b00968f0cfe3")
        response = MagicMock()
        response.json.return_value = {
            "operationId": str(operation_id),
            "positionId": 9001,
            "referenceId": str(request_id),
        }
        persisted: list[dict[str, object]] = []
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.patch.return_value = response
            result = broker.edit_demo_strategy_position(
                position_id=9001,
                stop_loss_rate=Decimal("101.25"),
                take_profit_rate=Decimal("120"),
                request_id=request_id,
                persist_response=persisted.append,
            )
            call = broker._http_write.patch.call_args
        assert call.args[0] == "/api/v2/trading/demo/positions/9001"
        assert call.kwargs["headers"] == {"x-request-id": str(request_id)}
        assert call.kwargs["json"] == {
            "stopLossRate": 101.25,
            "stopLossType": "fixed",
            "takeProfitRate": 120.0,
        }
        assert result.operation_id == operation_id
        assert result.raw_payload == response.json.return_value
        assert persisted == [response.json.return_value]

    def test_close_uses_exact_demo_route_and_close_lookup_proves_affected_position(self) -> None:
        request_id = UUID("f95eab17-c3ac-4948-a281-d94fd1e2764b")
        accepted = MagicMock()
        accepted.json.return_value = {"orderForClose": {"orderID": 12346, "positionID": 9001, "statusID": 1}}
        detail = MagicMock()
        detail.json.return_value = {
            "orderID": 12346,
            "statusID": 1,
            "referenceID": str(request_id),
            "errorCode": None,
            "positions": [{"positionID": 9001}],
        }
        persisted: list[dict[str, object]] = []
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_read = MagicMock()
            broker._http_write.post.return_value = accepted
            broker._http_read.get.return_value = detail
            submission = broker.close_demo_strategy_position(
                position_id=9001,
                instrument_id=1001,
                request_id=request_id,
                persist_response=persisted.append,
            )
            resolved = broker.get_demo_close_order(
                order_id=submission.broker_order_ref,
                persist_response=persisted.append,
            )
            close_call = broker._http_write.post.call_args
        assert close_call.args[0] == "/api/v1/trading/execution/demo/market-close-orders/positions/9001"
        assert close_call.kwargs["json"] == {"InstrumentID": 1001, "UnitsToDeduct": None}
        assert resolved.status == "filled"
        assert resolved.position_ids == (9001,)
        assert submission.raw_payload == accepted.json.return_value
        assert resolved.raw_payload == detail.json.return_value
        assert persisted == [accepted.json.return_value, detail.json.return_value]

    def test_real_credentials_cannot_patch_or_close_strategy_positions(self) -> None:
        with EtoroBrokerProvider(api_key="k", user_key="u", env="real") as broker:
            broker._http_write = MagicMock()
            with pytest.raises(BrokerPositionMutationError, match="demo credentials"):
                broker.edit_demo_strategy_position(
                    position_id=9001,
                    stop_loss_rate=Decimal("100"),
                    take_profit_rate=None,
                    request_id=uuid4(),
                )
            with pytest.raises(BrokerPositionMutationError, match="demo credentials"):
                broker.close_demo_strategy_position(
                    position_id=9001,
                    instrument_id=1001,
                    request_id=uuid4(),
                )
            broker._http_write.assert_not_called()


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

    def test_uses_caller_owned_request_id_for_idempotency(self) -> None:
        request_id = UUID("6f0b1702-99f8-41fe-97d7-0841c448e603")
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_OPEN_ORDER_RESPONSE

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = mock_resp

            broker.place_order(
                1001,
                "BUY",
                amount=Decimal("100"),
                units=None,
                request_id=request_id,
            )

            headers = broker._http_write.post.call_args.kwargs["headers"]
            assert headers["x-request-id"] == str(request_id)

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


class TestDetailedOrderLookup:
    def test_reference_id_routes_to_v2_and_preserves_exact_executions(self) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_ORDER_DETAIL_RESPONSE
        reference_id = "1c94300c-90aa-4303-9d00-dec376d74efb"

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp
            result = broker.lookup_order(reference_id=reference_id)

        call = broker._http_read.get.call_args
        assert call.args[0] == "/api/v2/trading/info/demo/orders:lookup"
        assert call.kwargs["params"] == {"referenceId": reference_id}
        assert result.broker_order_ref == "13902598"
        assert result.instrument_id == 1001
        assert [execution.position_id for execution in result.position_executions] == [9001, 9002]
        assert result.position_executions[0].opening_units == Decimal("6.5")
        assert result.position_executions[0].average_price == Decimal("95.25")

    def test_order_id_is_mutually_exclusive_and_positive(self) -> None:
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            for kwargs in ({}, {"order_id": "1", "reference_id": str(uuid4())}, {"order_id": "0"}):
                try:
                    broker.lookup_order(**kwargs)  # type: ignore[arg-type]
                except ValueError:
                    pass
                else:  # pragma: no cover - assertion helper branch
                    raise AssertionError("unsafe lookup identity must be refused")

    def test_404_is_distinct_from_transport_failure(self) -> None:
        response = httpx.Response(404, request=httpx.Request("GET", "https://example.test"))
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.side_effect = httpx.HTTPStatusError(
                "not found", request=response.request, response=response
            )
            try:
                broker.lookup_order(order_id="123")
            except BrokerOrderNotFound:
                pass
            else:  # pragma: no cover - assertion helper branch
                raise AssertionError("404 must remain distinguishable for crash reconciliation")

    def test_parser_refuses_duplicate_position_identity(self) -> None:
        malformed = {
            **FIXTURE_ORDER_DETAIL_RESPONSE,
            "positionExecutions": [
                FIXTURE_ORDER_DETAIL_RESPONSE["positionExecutions"][0],
                FIXTURE_ORDER_DETAIL_RESPONSE["positionExecutions"][0],
            ],
        }
        try:
            _parse_order_detail(malformed, reference_id=None)
        except OrderDetailParseError:
            pass
        else:  # pragma: no cover - assertion helper branch
            raise AssertionError("duplicate exact position ids must fail closed")

    def test_parser_refuses_execution_without_fill_facts(self) -> None:
        execution = dict(FIXTURE_ORDER_DETAIL_RESPONSE["positionExecutions"][0])
        execution["openingData"] = {"executionTime": "2026-08-09T09:00:01Z", "fees": 0}
        malformed = {**FIXTURE_ORDER_DETAIL_RESPONSE, "positionExecutions": [execution]}
        try:
            _parse_order_detail(malformed, reference_id=None)
        except OrderDetailParseError as exc:
            assert "units" in str(exc)
        else:  # pragma: no cover - assertion helper branch
            raise AssertionError("position identity without positive fill facts must fail closed")


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


class TestGetPortfolio:
    def test_returns_positions_and_cash(self) -> None:
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

    def test_empty_portfolio(self) -> None:
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

    def test_missing_credit_defaults_to_zero(self) -> None:
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

    def test_calls_correct_endpoint(self) -> None:
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
