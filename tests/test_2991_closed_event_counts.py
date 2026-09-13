"""``get_closed_position_event_counts`` parse + lane attribution (#2991, #2993).

Pure: no DB, no network, no broker mutation. An ``httpx.MockTransport`` is swapped onto
the provider's client after construction, so the REAL method runs against a canned body.

⚠ The method exists so that reading this endpoint is METERED, not because the domain
layer consumes it — ``etoro_request_log`` classifies an observed request by matching
``etoro_quota_lanes.CALL_SITES``, so a caller reaching the path outside the provider is
recorded as ``unclassified`` and its draw on lane G goes uncounted. The lane assertion
below is therefore the point of the method, not incidental to it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import httpx
import pytest

from app.providers.implementations.etoro_broker import (
    ClosedPositionEventCount,
    ClosedPositionEventCountParseError,
    EtoroBrokerProvider,
)
from app.providers.implementations.etoro_request_log import etoro_lane_counters, reset_etoro_lane_counters

if TYPE_CHECKING:
    from collections.abc import Iterator

#: The (lane, src, env) key this endpoint must land on.
_READ_KEY = ("G_default_shared", "broker_read", "demo")


@pytest.fixture(autouse=True)
def _clean_counters() -> Iterator[None]:
    reset_etoro_lane_counters()
    yield
    reset_etoro_lane_counters()


@pytest.fixture
def broker() -> Iterator[EtoroBrokerProvider]:
    with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as provider:
        provider._http_read._min_interval = 0.0
        yield provider


def _serve(broker: EtoroBrokerProvider, response: httpx.Response) -> list[httpx.Request]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return response

    broker._client._transport = httpx.MockTransport(handler)
    return seen


def _body() -> list[dict[str, Any]]:
    return [
        {"closeYear": 2021, "assetType": "Stocks", "closedPositionEvents": 113},
        {"closeYear": 2026, "assetType": "Crypto Currencies", "closedPositionEvents": 1},
    ]


def test_counts_are_parsed_and_the_request_lands_on_the_read_lane(broker: EtoroBrokerProvider) -> None:
    seen = _serve(broker, httpx.Response(200, json=_body()))

    counts = broker.get_closed_position_event_counts()

    assert counts == (
        ClosedPositionEventCount(close_year=2021, asset_type="Stocks", closed_position_events=113),
        ClosedPositionEventCount(close_year=2026, asset_type="Crypto Currencies", closed_position_events=1),
    )
    assert len(seen) == 1
    assert seen[0].url.path == "/api/v1/data/positions/closed-events/history"
    # No env segment and no query parameters: the operation answers for the caller's GCID.
    assert not seen[0].url.params
    assert etoro_lane_counters()[_READ_KEY].attempts == 1


def test_an_empty_history_is_not_an_error(broker: EtoroBrokerProvider) -> None:
    """Documented: a customer with no closed positions returns 200 with an empty array."""
    _serve(broker, httpx.Response(200, json=[]))

    assert broker.get_closed_position_event_counts() == ()


@pytest.mark.parametrize(
    ("body", "fragment"),
    [
        ({"closeYear": 2021}, "expected a JSON array"),
        ([[2021, "Stocks", 1]], "row 0: expected an object"),
        ([{"assetType": "Stocks", "closedPositionEvents": 1}], "row 0"),
        ([{"closeYear": "nineteen", "assetType": "Stocks", "closedPositionEvents": 1}], "row 0"),
        # A count cannot be negative; an int that parses is not therefore a figure.
        ([{"closeYear": 2021, "assetType": "Stocks", "closedPositionEvents": -3}], "must be >= 0"),
    ],
)
def test_a_malformed_body_raises_rather_than_returning_a_partial_set(
    broker: EtoroBrokerProvider, body: Any, fragment: str
) -> None:
    """Never a silently short tuple: a partial count reads as a real one."""
    _serve(broker, httpx.Response(200, json=body))

    with pytest.raises(ClosedPositionEventCountParseError, match=fragment):
        broker.get_closed_position_event_counts()


def test_a_non_json_body_raises_the_same_parse_error(broker: EtoroBrokerProvider) -> None:
    _serve(broker, httpx.Response(200, content=b"<html>gateway</html>"))

    with pytest.raises(ClosedPositionEventCountParseError, match="not JSON"):
        broker.get_closed_position_event_counts()


def test_an_http_error_propagates_to_the_caller(broker: EtoroBrokerProvider) -> None:
    """403 is a real outcome on this token family — the caller decides what it means."""
    _serve(broker, httpx.Response(403, json={"errorCode": "InsufficientPermissions"}))

    with pytest.raises(httpx.HTTPStatusError):
        broker.get_closed_position_event_counts()
