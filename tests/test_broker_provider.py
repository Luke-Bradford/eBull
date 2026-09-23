"""
Unit tests for the eToro broker provider rewrite.

Tests verify endpoint routing, request body shape, response normalisation,
error handling, and environment-scoped path prefixes.

No network calls — all HTTP interactions are mocked.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import httpx
import pytest

from app.providers.broker import (
    BrokerCloseOrderDetail,
    BrokerCoreOrder,
    BrokerMirror,
    BrokerMirrorPosition,
    BrokerOrderNotFound,
    BrokerOrderSubmissionError,
    BrokerOrderSubmissionUncertain,
    BrokerPortfolio,
    BrokerPositionMutationError,
    BrokerPositionMutationUncertain,
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
    _parse_account_risk_snapshot,
    _parse_direct_position,
    _parse_order_detail,
)

#: Sentinel meaning "delete this key" in the payload builders below. A literal `None`
#: cannot express it: `None` is itself a value the parsers must reject, and conflating
#: "absent" with "present and null" would make half the fail-closed cases untested.
_ABSENT = object()

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

# ⚠ The eToro close ACK shape, verbatim from an attended demo session on
# 2026-09-22 (#3007): a numeric `statusID`, and NONE of `executionPrice`,
# `units` or `unitsToDeduct`. The fixture that stood here before was invented
# under the heading "documented eToro API response shapes" -- it carried
# `statusID: "Executed"` plus a price and units, a shape neither the committed
# contract nor the broker has ever produced, and it pinned the normaliser to
# it. Same defect as the #2942 prevention-log entry: a request/response-shape
# test is the one place the source's shape must win over ours.
FIXTURE_CLOSE_ORDER_ACK = {
    "orderForClose": {
        "positionID": 3602456774,
        "instrumentID": 3434,
        "orderID": 383127337,
        "orderType": 19,
        "statusID": 1,
        "CID": 20661956,
        "openDateTime": "2026-09-22T14:45:00.6711552Z",
        "lastUpdate": "2026-09-22T14:45:00.6711552Z",
    },
    "token": "17df1d41-bf63-4320-9d32-c2b47dd6242d",
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
            {
                "positionID": 9001,
                "instrumentID": 1001,
                "amount": 200,
                "units": 10,
                "isBuy": True,
                "isPartiallyAltered": False,
                "unrealizedPnL": {"pnL": 20, "closeRate": 22, "closeConversionRate": 1, "assetCurrencyId": 1},
            },
            {
                "positionID": 9002,
                "instrumentID": 1002,
                "amount": 100,
                "units": 10,
                "isBuy": True,
                "isPartiallyAltered": False,
                "unrealizedPnL": {"pnL": -5, "closeRate": 9.5, "closeConversionRate": 1, "assetCurrencyId": 1},
            },
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

        assert [
            (
                row.position_id,
                row.instrument_id,
                row.amount,
                row.unrealized_pnl,
                row.market_value,
                row.is_partially_altered,
            )
            for row in result.direct_positions
        ] == [
            (9001, 1001, Decimal("200"), Decimal("20"), Decimal("220"), False),
            (9002, 1002, Decimal("100"), Decimal("-5"), Decimal("95"), False),
        ]

    @pytest.mark.parametrize("position_id", [None, True, 1.2, "9001", 0, -1, 9_223_372_036_854_775_808])
    def test_direct_position_id_fails_closed(self, position_id: object) -> None:
        position = dict(FIXTURE_ACCOUNT_PNL_RESPONSE["clientPortfolio"]["positions"][0])
        position["positionID"] = position_id
        payload = {
            "clientPortfolio": {
                **FIXTURE_ACCOUNT_PNL_RESPONSE["clientPortfolio"],
                "positions": [position],
            }
        }
        mock_resp = MagicMock()
        mock_resp.json.return_value = payload
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp
            with pytest.raises(TradingPreflightParseError, match="position id"):
                broker.get_account_risk_snapshot()

    def test_documented_position_id_alias_is_accepted_and_conflict_refuses(self) -> None:
        position = dict(FIXTURE_ACCOUNT_PNL_RESPONSE["clientPortfolio"]["positions"][0])
        position["positionId"] = position.pop("positionID")
        payload = {
            "clientPortfolio": {
                **FIXTURE_ACCOUNT_PNL_RESPONSE["clientPortfolio"],
                "positions": [position],
            }
        }
        mock_resp = MagicMock()
        mock_resp.json.return_value = payload
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp
            assert broker.get_account_risk_snapshot().direct_positions[0].position_id == 9001

        position["positionID"] = 9002
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp
            with pytest.raises(TradingPreflightParseError, match="aliases disagree"):
                broker.get_account_risk_snapshot()

        position["positionId"] = 1
        position["positionID"] = True
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp
            with pytest.raises(TradingPreflightParseError, match="must be an integer"):
                broker.get_account_risk_snapshot()

    def test_duplicate_direct_position_ids_fail_closed(self) -> None:
        first, second = [dict(row) for row in FIXTURE_ACCOUNT_PNL_RESPONSE["clientPortfolio"]["positions"]]
        second["positionID"] = first["positionID"]
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "clientPortfolio": {
                **FIXTURE_ACCOUNT_PNL_RESPONSE["clientPortfolio"],
                "positions": [first, second],
            }
        }
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp
            with pytest.raises(TradingPreflightParseError, match="must be unique"):
                broker.get_account_risk_snapshot()

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
                    {
                        "positionID": 9001,
                        "instrumentID": 1001,
                        "amount": 200,
                        "units": 10,
                        "isBuy": True,
                        "isPartiallyAltered": False,
                        "unrealizedPnL": {"pnL": 20, "closeRate": 22, "closeConversionRate": 1, "assetCurrencyId": 1},
                    },
                    {
                        "positionID": 9002,
                        "instrumentID": 1001,
                        "amount": 100,
                        "units": 10,
                        "isBuy": True,
                        "isPartiallyAltered": False,
                        "unrealizedPnL": {"pnL": -30, "closeRate": 7, "closeConversionRate": 1, "assetCurrencyId": 1},
                    },
                    # Sums to exactly zero -- invisible to any money-valued short field.
                    {
                        "positionID": 9003,
                        "instrumentID": 1001,
                        "amount": 50,
                        "units": 10,
                        "isBuy": False,
                        "isPartiallyAltered": False,
                        "unrealizedPnL": {"pnL": -50, "closeRate": 5, "closeConversionRate": 1, "assetCurrencyId": 1},
                    },
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
                    {
                        "positionID": 9001,
                        "instrumentID": 1001,
                        "amount": 200,
                        "units": 10,
                        "isBuy": True,
                        "isPartiallyAltered": False,
                        "unrealizedPnL": {"pnL": -250, "closeRate": 3, "closeConversionRate": 1, "assetCurrencyId": 1},
                    },
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
            {
                "positionID": 9001,
                "instrumentID": 1001,
                "amount": 200,
                "isPartiallyAltered": False,
                "unrealizedPnL": {"pnL": 20, "closeRate": 22, "closeConversionRate": 1, "assetCurrencyId": 1},
            },
            {
                "positionID": 9001,
                "instrumentID": 1001,
                "amount": 200,
                "units": 10,
                "isBuy": "true",
                "isPartiallyAltered": False,
                "unrealizedPnL": {"pnL": 20, "closeRate": 22, "closeConversionRate": 1, "assetCurrencyId": 1},
            },
            {
                "positionID": 9001,
                "instrumentID": 1001,
                "amount": 200,
                "units": 10,
                "isBuy": 1,
                "isPartiallyAltered": False,
                "unrealizedPnL": {"pnL": 20, "closeRate": 22, "closeConversionRate": 1, "assetCurrencyId": 1},
            },
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

    def test_position_units_are_read_so_the_official_mark_is_derivable(self) -> None:
        """#3068 — `units` was published on this row and unread.

        With it, `(amount + pnL) / units` is the position's mark AT the snapshot
        instant. Without it the only comparand available was a total priced at some
        other time, which is the whole defect.
        """
        mock_resp = MagicMock()
        mock_resp.json.return_value = FIXTURE_ACCOUNT_PNL_RESPONSE
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = mock_resp
            positions = {p.position_id: p for p in broker.get_account_risk_snapshot().direct_positions}
        assert positions[9001].units == Decimal("10")
        # amount 200 + pnL 20 over 10 units.
        assert positions[9001].market_value / positions[9001].units == Decimal("22")

    def test_unusable_position_units_fail_closed_rather_than_becoming_a_mark(self) -> None:
        """A divisor is not a field that may be defaulted.

        Absent, non-numeric, non-finite, zero or negative `units` all raise. A coerced
        one would produce a mark that looks like an observation, and a zero one would
        raise deep inside a consumer instead of at the parse boundary. `_instrument_id`
        already fails closed on the same "documented, required, positive" grounds.
        """
        base = {
            "positionID": 9001,
            "instrumentID": 1001,
            "amount": 200,
            "isBuy": True,
            "isPartiallyAltered": False,
            "unrealizedPnL": {"pnL": 20, "closeRate": 22, "closeConversionRate": 1, "assetCurrencyId": 1},
        }
        for units in (None, "abc", float("nan"), 0, -5, True):
            position = dict(base)
            if units is not None:
                position["units"] = units
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
                    assert "units" in str(exc)
                else:  # pragma: no cover - assertion helper branch
                    raise AssertionError(f"unusable units must fail closed: {units!r}")

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
                BrokerCoreOrder(
                    instrument_id=3417,
                    amount=Decimal("250"),
                    stop_loss_rate=Decimal("379.93"),
                    take_profit_rate=Decimal("2279.58"),
                ),
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
            "stopLossRate": 379.93,
            "takeProfitRate": 2279.58,
            "stopLossType": "fixed",
        }
        assert result.broker_order_ref == "13902598"
        assert result.reference_id == request_id
        assert result.response_digest == "0caf67206160f1c97554b988d9ff09aa6bec3813df377e1fc4aeb8f2f231c513"
        assert not hasattr(result, "token")

    def test_writer_refuses_an_amount_that_would_change_in_json(self) -> None:
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            with pytest.raises(BrokerOrderSubmissionError, match="represented exactly"):
                broker.place_demo_core_order(
                    BrokerCoreOrder(
                        instrument_id=3417,
                        amount=Decimal("9007199254.740991"),
                        stop_loss_rate=Decimal("379.93"),
                        take_profit_rate=Decimal("2279.58"),
                    ),
                    request_id=uuid4(),
                )
            broker._http_write.post.assert_not_called()

    def test_real_credentials_cannot_select_the_core_writer(self) -> None:
        with EtoroBrokerProvider(api_key="k", user_key="u", env="real") as broker:
            with pytest.raises(BrokerOrderSubmissionError, match="demo credentials"):
                broker.place_demo_core_order(
                    BrokerCoreOrder(
                        instrument_id=3417,
                        amount=Decimal("250"),
                        stop_loss_rate=Decimal("379.93"),
                        take_profit_rate=Decimal("2279.58"),
                    ),
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
                    BrokerCoreOrder(
                        instrument_id=3417,
                        amount=Decimal("250"),
                        stop_loss_rate=Decimal("379.93"),
                        take_profit_rate=Decimal("2279.58"),
                    ),
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
            "proceeds": 24.99,
            "positions": [
                {"positionID": 9001, "occurred": "2026-09-23T07:59:23.223Z", "rate": 5816.7, "units": 0.323024}
            ],
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
            resolved = broker.get_close_order(
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

    @pytest.mark.parametrize(
        ("env", "expected_path"),
        [
            ("demo", "/api/v1/trading/info/demo/close-orders/12346"),
            ("real", "/api/v1/trading/info/real/close-orders/12346"),
        ],
    )
    def test_the_close_order_lookup_spells_its_environment_out(self, env: str, expected_path: str) -> None:
        """#3007 half 2's measured trap, pinned in both directions.

        ``_info_prefix`` is ``/api/v1/trading/info{"/demo" if demo else ""}``, so
        reusing it here would build ``/api/v1/trading/info/close-orders/12346``
        in real mode — a path eToro documents nowhere. ``close-orders`` and
        ``pnl`` are the only two v1 info routes carrying an explicit ``/real/``
        segment, and both concrete operations are in
        ``tests/fixtures/etoro/openapi_v1.375.0.json``.
        """
        detail = MagicMock()
        detail.json.return_value = {"orderID": 12346, "statusID": 3, "errorCode": None, "positions": []}
        with EtoroBrokerProvider(api_key="k", user_key="u", env=env) as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = detail
            resolved = broker.get_close_order(order_id="12346")
            call = broker._http_read.get.call_args
        assert call.args[0] == expected_path
        assert resolved.status == "pending"

    @pytest.mark.parametrize(
        ("payload", "expected"),
        [
            # Verbatim attended reads of close order 383344846 (#3007, 2026-09-23).
            (
                {
                    "orderID": 383344846,
                    "statusID": 2,
                    "errorCode": 0,
                    "proceeds": 0.0,
                    "positions": [{"positionID": 3602947846}],
                },
                "pending",
            ),
            (
                {
                    "orderID": 383344846,
                    "statusID": 3,
                    "errorCode": 0,
                    "proceeds": 24.99,
                    "positions": [
                        {
                            "positionID": 3602947861,
                            "occurred": "2026-09-23T07:59:23.223Z",
                            "rate": 5816.7,
                            "units": 0.323024,
                            "conversionRate": 0.013303,
                            "amount": 25.0,
                        }
                    ],
                },
                "filled",
            ),
            # One executed entry does not vouch for another.
            (
                {
                    "orderID": 383344846,
                    "statusID": 3,
                    "errorCode": 0,
                    "proceeds": 24.99,
                    "positions": [
                        {"positionID": 1, "occurred": "2026-09-23T07:59:23Z", "rate": 1.0, "units": 1.0},
                        {"positionID": 2, "occurred": "2026-09-23T07:59:23Z", "rate": None, "units": 1.0},
                    ],
                },
                "pending",
            ),
            (
                {
                    "orderID": 383344846,
                    "statusID": 3,
                    "errorCode": 0,
                    "positions": [{"positionID": 1, "occurred": "2026-09-23T07:59:23Z", "rate": 1.0, "units": 1.0}],
                },
                "pending",
            ),
        ],
    )
    def test_close_order_is_filled_only_on_its_own_execution_fields(
        self, payload: dict[str, object], expected: str
    ) -> None:
        """#3320: ``statusID 2`` listed the position with no execution fields and
        normalised to ``filled``, which released core ownership on a close that
        had not executed. ``statusID`` has no enum, so the fields decide."""
        detail = MagicMock()
        detail.json.return_value = payload
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = detail
            resolved = broker.get_close_order(order_id="383344846")
        assert resolved.status == expected
        assert resolved.broker_status == str(payload["statusID"])

    @staticmethod
    def _close_order_from_wire(body: str, order_id: str) -> BrokerCloseOrderDetail:
        """``get_close_order`` against a real ``httpx.Response`` built from wire text."""
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_read = MagicMock()
            broker._http_read.get.return_value = httpx.Response(
                200, content=body.encode(), request=httpx.Request("GET", f"https://x/{order_id}")
            )
            return broker.get_close_order(order_id=order_id)

    @pytest.mark.parametrize(
        ("body", "expected"),
        [
            # Verbatim wire text of both attended closes (#3007, 2026-09-23): the partial
            # 383344846 at statusID 3, and the whole close 383339190.
            (
                '{"orderID":383344846,"statusID":3,"errorCode":0,"instrumentID":3075,"proceeds":24.99,'
                '"positions":[{"positionID":3602947861,"occurred":"2026-09-23T07:59:23.223Z","rate":5816.7,'
                '"units":0.323024,"conversionRate":0.013303,"amount":25.0}]}',
                (3602947861, Decimal("5816.7"), Decimal("0.323024"), "2026-09-23T07:59:23.223Z"),
            ),
            (
                '{"orderID":383339190,"statusID":3,"errorCode":0,"instrumentID":3075,"proceeds":24.99,'
                '"positions":[{"positionID":3602947846,"occurred":"2026-09-23T08:01:53.83Z","rate":5817.29,'
                '"units":0.323024,"conversionRate":0.013303,"amount":25.0}]}',
                (3602947846, Decimal("5817.29"), Decimal("0.323024"), "2026-09-23T08:01:53.83Z"),
            ),
        ],
    )
    def test_close_order_positions_carry_the_raw_fill_fields_as_decimals(
        self, body: str, expected: tuple[object, ...]
    ) -> None:
        """#3007 part 2: decoded with ``parse_float=Decimal`` so the booking's exact
        units check holds against the wire digits, and otherwise UNPARSED."""
        detail = self._close_order_from_wire(body, str(json.loads(body)["orderID"]))
        (fill,) = detail.positions
        assert (fill.position_id, fill.rate, fill.units, fill.occurred) == expected
        assert isinstance(fill.units, Decimal)
        assert detail.position_ids == (expected[0],)
        # The persisted/audited payload stays plain JSON.
        assert detail.raw_payload["positions"][0]["units"] == 0.323024

    # #3331 — wire text of close order 383424950 (#2603 acceptance A, 2026-09-23 13:35Z)
    # as posted on the issue; ``orderID`` added, which the issue's excerpt elided.
    _REPEATED_POSITION_CLOSE = (
        '{"orderID":383424950,"statusID":3,"errorCode":0,"proceeds":228.75,"assetCurrencyID":1,'
        '"accountCurrencyID":1,"instrumentID":3417,"positions":[{"positionID":3601264304,'
        '"occurred":"2026-09-23T13:35:31.583Z","rate":772.4,"units":0.296155,"conversionRate":1.0,'
        '"amount":225.04},{"positionID":3601264304}]}'
    )

    def test_an_executed_close_that_repeats_its_position_bare_is_filled(self) -> None:
        """#3331: the broker listed the executed position twice, once bare. Judged per
        distinct id, it is filled, with one id and one fill, so the whole-close
        consumers (``position_ids == (owned,)``, one-fill late EXIT booking) see
        the shape they already accept."""
        detail = self._close_order_from_wire(self._REPEATED_POSITION_CLOSE, "383424950")
        assert detail.status == "filled"
        assert detail.position_ids == (3601264304,)
        (fill,) = detail.positions
        assert (fill.rate, fill.units, fill.occurred) == (
            Decimal("772.4"),
            Decimal("0.296155"),
            "2026-09-23T13:35:31.583Z",
        )
        # The audited payload keeps both entries verbatim.
        assert len(detail.raw_payload["positions"]) == 2

    @pytest.mark.parametrize(
        ("positions", "expected_ids"),
        [
            # The statusID 2 shape, repeated: still nothing executed.
            ([{"positionID": 7}, {"positionID": 7}], (7,)),
            # An executed entry vouches only for its own id.
            (
                [
                    {"positionID": 7, "occurred": "2026-09-23T13:35:31Z", "rate": 1.0, "units": 1.0},
                    {"positionID": 8},
                ],
                (7, 8),
            ),
            # A PARTIAL repeat is not bare: it keeps the order pending.
            (
                [
                    {"positionID": 7, "occurred": "2026-09-23T13:35:31Z", "rate": 1.0, "units": 1.0},
                    {"positionID": 7, "rate": 1.0},
                ],
                (7,),
            ),
        ],
    )
    def test_a_bare_repeat_does_not_complete_an_unexecuted_position(
        self, positions: list[dict[str, object]], expected_ids: tuple[int, ...]
    ) -> None:
        body = {"orderID": 383424950, "statusID": 3, "errorCode": 0, "proceeds": 1.0, "positions": positions}
        detail = self._close_order_from_wire(json.dumps(body), "383424950")
        assert detail.status == "pending"
        assert detail.position_ids == expected_ids

    def test_a_malformed_duplicate_id_still_fails_the_lookup(self) -> None:
        """Dropping a bare duplicate must not drop its id check first."""
        body = json.loads(self._REPEATED_POSITION_CLOSE)
        body["positions"][1]["positionID"] = True
        with pytest.raises(BrokerPositionMutationUncertain, match="malformed"):
            self._close_order_from_wire(json.dumps(body), "383424950")

    @pytest.mark.parametrize(
        "patch",
        [
            {"orderID": True},
            {"orderID": "383339190.0"},
            {"orderID": 383339190.0},
            {"instrumentID": True},
            {"instrumentID": "٣٠٧٥"},
            {"instrumentID": 0},
            {"instrumentID": 3075.5},
            {"positions": [{"positionID": True}]},
            {"positions": [{"positionID": -1}]},
            {"positions": [{"positionID": " 3602947846"}]},
        ],
    )
    def test_close_order_ids_are_strict(self, patch: dict[str, object]) -> None:
        """A bool, a float, a non-ASCII digit string or a non-positive value is not an
        id; ``int()`` would have turned each into a plausible one."""
        body = {"orderID": 383339190, "statusID": 3, "errorCode": 0, "instrumentID": 3075, "positions": []}
        body.update(patch)
        with pytest.raises(BrokerPositionMutationUncertain, match="malformed|identity"):
            self._close_order_from_wire(json.dumps(body), "383339190")

    def test_close_order_accepts_ids_as_ascii_digit_strings_and_an_absent_instrument(self) -> None:
        body = {"orderID": "383339190", "statusID": 3, "positions": [{"positionID": "3602947846"}]}
        detail = self._close_order_from_wire(json.dumps(body), "383339190")
        assert detail.position_ids == (3602947846,)
        assert detail.instrument_id is None

    def test_an_unknown_environment_cannot_invent_a_close_order_path(self) -> None:
        """The path interpolates ``self._env``, so an environment outside the
        documented two would silently address a route that does not exist.
        Refused before any request, not after a 404."""
        with EtoroBrokerProvider(api_key="k", user_key="u", env="sandbox") as broker:
            broker._http_read = MagicMock()
            with pytest.raises(BrokerPositionMutationError, match="environment"):
                broker.get_close_order(order_id="12346")
            broker._http_read.get.assert_not_called()

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
        close_resp.json.return_value = FIXTURE_CLOSE_ORDER_ACK

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = close_resp

            result = broker.close_position(3602456774, instrument_id=3434)

            broker._http_write.post.assert_called_once()
            post_endpoint = broker._http_write.post.call_args.args[0]
            assert post_endpoint == "/api/v1/trading/execution/demo/market-close-orders/positions/3602456774"

            body = broker._http_write.post.call_args.kwargs["json"]
            assert body["UnitsToDeduct"] is None
            # eToro documents InstrumentID as REQUIRED on this body
            # (api-reference/trading--demo/close-demo-position-by-units).
            # The prior version of this test asserted its ABSENCE (#2942).
            assert body["InstrumentID"] == 3434

            # #3007: the ack is not a fill. Numeric statusID, no price, no units.
            assert result.status == "pending"
            assert result.broker_order_ref == "383127337"

    def test_close_position_partial_close(self) -> None:
        """units_to_deduct is passed through when provided."""
        close_resp = MagicMock()
        close_resp.json.return_value = FIXTURE_CLOSE_ORDER_ACK

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = close_resp

            broker.close_position(98765, units_to_deduct=Decimal("2.5"), instrument_id=1001)

            body = broker._http_write.post.call_args.kwargs["json"]
            assert body["UnitsToDeduct"] == 2.5

    def test_close_position_requires_instrument_id(self) -> None:
        """The documented required field cannot be silently omitted (#2942)."""
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()

            with pytest.raises(ValueError, match="instrument_id"):
                broker.close_position(98765)

            broker._http_write.post.assert_not_called()

    def test_close_position_network_error_is_uncertain(self) -> None:
        """A transport failure does not prove the close failed (#2942).

        It previously returned status='failed', which the caller booked as a
        terminal failure — for an order that may well have landed.
        """
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.side_effect = httpx.ConnectError("connection refused")

            with pytest.raises(BrokerOrderSubmissionUncertain) as excinfo:
                broker.close_position(98765, instrument_id=1001)

            assert "Network error" in excinfo.value.raw_payload["error"]

    def test_close_position_carries_the_caller_request_id(self) -> None:
        """The committed UUID must reach the broker as x-request-id (#2942)."""
        close_resp = MagicMock()
        close_resp.json.return_value = FIXTURE_CLOSE_ORDER_ACK
        request_id = UUID("11111111-2222-3333-4444-555555555555")

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = close_resp

            broker.close_position(98765, instrument_id=1001, request_id=request_id)

            headers = broker._http_write.post.call_args.kwargs["headers"]
            assert headers["x-request-id"] == str(request_id)


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

    def test_network_error_is_uncertain(self) -> None:
        """A transport failure leaves the submission's fate unknown (#2942)."""
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.side_effect = httpx.ConnectError("connection refused")

            with pytest.raises(BrokerOrderSubmissionUncertain) as excinfo:
                broker.place_order(1001, "BUY", amount=Decimal("100"), units=None)

            assert "Network error" in excinfo.value.raw_payload["error"]
            assert excinfo.value.raw_payload["_ebull_action"] == "BUY"

    def test_non_json_success_response_is_uncertain(self) -> None:
        """A 200 we cannot read is neither a success nor a rejection (#2942)."""
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.side_effect = ValueError("not JSON")

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = mock_resp

            with pytest.raises(BrokerOrderSubmissionUncertain) as excinfo:
                broker.place_order(1001, "BUY", amount=Decimal("100"), units=None)

            assert "Non-JSON" in excinfo.value.raw_payload["error"]

    def test_non_object_success_body_is_uncertain(self) -> None:
        """A 200 whose body is not the documented object (#2942)."""
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = ["unexpected"]

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = mock_resp

            with pytest.raises(BrokerOrderSubmissionUncertain) as excinfo:
                broker.place_order(1001, "BUY", amount=Decimal("100"), units=None)

            assert excinfo.value.raw_payload["raw_json"] == ["unexpected"]

    def test_5xx_is_uncertain_and_captures_raw_text(self) -> None:
        """A 5xx does not prove the order failed; evidence is retained (#2942)."""
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

            with pytest.raises(BrokerOrderSubmissionUncertain) as excinfo:
                broker.place_order(1001, "BUY", amount=Decimal("100"), units=None)

            assert excinfo.value.raw_payload["raw_text"] == "Internal Server Error"
            assert excinfo.value.raw_payload["http_status"] == 500

    @pytest.mark.parametrize("status_code", [408, 409, 425, 429])
    def test_ambiguous_4xx_statuses_are_uncertain(self, status_code: int) -> None:
        """A timeout, conflict, too-early or exhausted throttle proves nothing (#2942)."""
        error_resp = MagicMock()
        error_resp.status_code = status_code
        error_resp.json.return_value = {"message": "try again"}

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.side_effect = httpx.HTTPStatusError(
                str(status_code),
                request=MagicMock(),
                response=error_resp,
            )

            with pytest.raises(BrokerOrderSubmissionUncertain):
                broker.place_order(1001, "BUY", amount=Decimal("100"), units=None)

    def test_plain_4xx_stays_a_rejection(self) -> None:
        """The broker answered and said no — that is terminal, not uncertain."""
        error_resp = MagicMock()
        error_resp.status_code = 400
        error_resp.json.return_value = {"message": "Bad request"}

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
    def test_live_ack_is_pending_with_no_fill_fields(self) -> None:
        """#3007: the observed close ack carries an identity and nothing else.

        Numeric `statusID`, no `executionPrice`, no `units`. The only honest
        reading is "acknowledged, outcome unknown".
        """
        result = _normalise_close_order_response(FIXTURE_CLOSE_ORDER_ACK)

        assert result.broker_order_ref == "383127337"
        assert result.status == "pending"
        assert result.filled_price is None
        assert result.filled_units is None
        assert result.fees == Decimal("0")

    def test_numeric_status_is_never_resolved_to_a_fill(self) -> None:
        """The contract types `statusId` as an integer with NO enum.

        `OrderForClose.statusId` is `{"type": "integer"}` in
        `tests/fixtures/etoro/openapi_v1.375.0.json` — there is no documented
        code-to-meaning table, so no numeric code may resolve to a terminal
        status. `3` is the code the lookup route reported for a FILLED close
        (#2961); it must still not book a fill from an acknowledgement.
        """
        for code in (0, 1, 2, 3, 19, "1", "3"):
            raw = {"orderForClose": {"orderID": 1, "statusID": code}}
            assert _normalise_close_order_response(raw).status == "pending"

    def test_units_to_deduct_is_not_read_as_a_fill(self) -> None:
        """#3007 proposed reading `unitsToDeduct` as the executed units.

        Rebutted: it is the deduction we requested, echoed back before the
        broker executed (the ack's `openDateTime` equals its `lastUpdate`, and
        the lookup route still 404'd seconds later — #2961). Executed units
        come from the lookup, never from the acknowledgement.
        """
        raw = {"orderForClose": {"orderID": 1, "statusID": 1, "unitsToDeduct": 2, "lotsToDeduct": 2}}
        result = _normalise_close_order_response(raw)
        assert result.filled_units is None
        assert result.status == "pending"

    def test_missing_optional_fields(self) -> None:
        raw = {"orderForClose": {"orderID": 1, "statusID": "Pending"}}
        result = _normalise_close_order_response(raw)
        assert result.filled_price is None
        assert result.filled_units is None
        assert result.fees == Decimal("0")

    def test_textual_status_still_maps(self) -> None:
        """A textual status keeps its documented meaning — only numerics abstain."""
        raw = {"orderForClose": {"orderID": 1, "statusID": "Executed", "executionPrice": 10, "units": 2}}
        result = _normalise_close_order_response(raw)
        assert result.status == "filled"
        assert result.filled_units == Decimal("2")


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
        close_resp.json.return_value = FIXTURE_CLOSE_ORDER_ACK

        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write = MagicMock()
            broker._http_write.post.return_value = close_resp

            broker.close_position(98765, instrument_id=1001)

            body = broker._http_write.post.call_args.kwargs["json"]
            # Both fields the portal documents for this body. This assertion
            # used to read `"InstrumentID" not in body`, pinning the omission
            # of a field the source marks REQUIRED (#2942).
            assert body["InstrumentID"] == 1001
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


class TestAccountPnlMarkOperands:
    """#3068 — the broker's own close rate, parsed as a comparand operand.

    ⚠ These are read from ``unrealizedPnL`` and not derived. ``(amount + pnL) / units``
    is equity per unit, not a price: the portal documents ``amount`` as including
    "additional margin allocated to the position as collateral", so the two coincide only
    at leverage 1 with no added collateral — and where they do coincide, the substitution
    built on the quotient is identically zero, i.e. a comparison that cannot fail.
    """

    @staticmethod
    def _payload(**pnl_overrides: object) -> dict[str, object]:
        pnl: dict[str, object] = {
            "pnL": 20,
            "closeRate": 22,
            "closeConversionRate": 1,
            "assetCurrencyId": 1,
        }
        pnl.update(pnl_overrides)
        for key in [k for k, v in pnl.items() if v is _ABSENT]:
            del pnl[key]
        return {
            "clientPortfolio": {
                "accountCurrencyId": 1,
                "credit": 1000,
                "positions": [
                    {
                        "positionID": 9001,
                        "instrumentID": 1001,
                        "amount": 200,
                        "units": 10,
                        "isBuy": True,
                        "isPartiallyAltered": False,
                        "unrealizedPnL": pnl,
                    }
                ],
                "mirrors": [],
                "ordersForOpen": [],
                "orders": [],
            }
        }

    def test_the_published_mark_is_read_not_derived(self) -> None:
        """The stored close rate must be the broker's 22, NOT (200 + 20) / 10 = 22.

        Those agree here by construction, which is why the test asserts the mark against a
        payload where they do NOT: below.
        """
        snapshot = _parse_account_risk_snapshot(self._payload(), observed_at=datetime.now(UTC))
        position = snapshot.direct_positions[0]
        assert position.close_rate == Decimal("22")
        assert position.close_conversion_rate == Decimal("1")
        assert position.asset_currency_id == 1

    def test_a_leveraged_row_keeps_the_published_mark_not_the_quotient(self) -> None:
        """amount 50 of margin on 1 unit entered at 100, broker mark 110, P&L 10.

        The quotient is (50 + 10) / 1 = 60. The mark is 110. A parser that derived it
        would be out by 50 on a healthy position.
        """
        payload = self._payload(pnL=10, closeRate=110)
        positions = payload["clientPortfolio"]["positions"]  # type: ignore[index]
        positions[0].update({"amount": 50, "units": 1, "leverage": 2})
        snapshot = _parse_account_risk_snapshot(payload, observed_at=datetime.now(UTC))
        position = snapshot.direct_positions[0]
        assert position.close_rate == Decimal("110")
        assert (position.amount + position.unrealized_pnl) / position.units == Decimal("60")

    def test_the_pnl_timestamp_is_carried_when_present(self) -> None:
        """⚠ Seven fractional digits, which is what eToro actually sends."""
        snapshot = _parse_account_risk_snapshot(
            self._payload(timestamp="2026-09-15T04:02:27.3169794Z"), observed_at=datetime.now(UTC)
        )
        stamped = snapshot.direct_positions[0].pnl_timestamp
        assert stamped is not None
        assert stamped.tzinfo is not None
        assert stamped.isoformat().startswith("2026-09-15T04:02:27")

    @pytest.mark.parametrize("overrides", [{"timestamp": _ABSENT}, {"timestamp": "not-a-date"}, {"timestamp": 7}])
    def test_an_unusable_timestamp_is_dropped_rather_than_failing_the_snapshot(
        self, overrides: dict[str, object]
    ) -> None:
        """Evidence, not an operand. No verdict reads it, so losing the whole comparand
        over it would protect a diagnostic at the expense of the thing being diagnosed."""
        snapshot = _parse_account_risk_snapshot(self._payload(**overrides), observed_at=datetime.now(UTC))
        assert snapshot.direct_positions[0].pnl_timestamp is None
        assert snapshot.direct_positions[0].close_rate == Decimal("22")

    @pytest.mark.parametrize(
        "overrides",
        [
            {"closeRate": _ABSENT},
            {"closeRate": 0},
            {"closeRate": -1},
            {"closeRate": "NaN"},
            {"closeRate": True},
            {"closeRate": "abc"},
            {"closeConversionRate": _ABSENT},
            {"closeConversionRate": 0},
            {"closeConversionRate": "Infinity"},
            {"assetCurrencyId": _ABSENT},
            {"assetCurrencyId": 0},
            {"assetCurrencyId": True},
            {"assetCurrencyId": "1"},
        ],
    )
    def test_an_unusable_comparand_operand_fails_closed(self, overrides: dict[str, object]) -> None:
        """A defaulted or coerced operand produces a number that LOOKS like an observation.

        ⚠ `True` is tested explicitly on both the numeric and the integer field: `bool` is
        an `int` subclass in Python, so an unguarded read would store 1 — a plausible
        conversion rate and a plausible currency id.
        """
        with pytest.raises(TradingPreflightParseError):
            _parse_account_risk_snapshot(self._payload(**overrides), observed_at=datetime.now(UTC))


class TestDirectPositionDirection:
    """#3068 — `/portfolio` direction is read, not defaulted."""

    @staticmethod
    def _position_payload(**overrides: object) -> dict[str, object]:
        payload: dict[str, object] = {
            "positionID": 9001,
            "instrumentID": 1001,
            "openRate": 20,
            "units": 10,
            "amount": 200,
            "isBuy": True,
        }
        payload.update(overrides)
        for key in [k for k, v in payload.items() if v is _ABSENT]:
            del payload[key]
        return payload

    def test_direction_is_read(self) -> None:
        assert _parse_direct_position(self._position_payload()).is_buy is True
        assert _parse_direct_position(self._position_payload(isBuy=False)).is_buy is False

    @pytest.mark.parametrize("overrides", [{"isBuy": _ABSENT}, {"isBuy": 1}, {"isBuy": "true"}, {"isBuy": None}])
    def test_an_absent_or_coerced_direction_fails_closed(self, overrides: dict[str, object]) -> None:
        """⚠⚠ This used to read `bool(payload.get("isBuy", True))`, so an ABSENT direction
        became LONG and any truthy value became LONG. `broker_positions.is_buy` signs the
        end-of-day mark-to-market and is compared against the broker's own `isBuy` as a
        two-endpoint check — which a default makes vacuous.
        """
        with pytest.raises(ValueError):
            _parse_direct_position(self._position_payload(**overrides))
