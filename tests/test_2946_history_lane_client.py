"""The trade-history paginator rides its own lane-G client (#2946 step 3 item 1).

Pure: no DB, no network, no broker mutation.  An ``httpx.MockTransport`` is swapped onto
the provider's client after construction, so the REAL ``get_trade_history`` loop runs —
including its pagination, its parse and its error paths — against canned pages.

⚠ What makes this worth a file of its own: the defect it guards is a method that pages
back-to-back on a client meant for single reads, and **a single-page test cannot see
it**.  Every assertion here therefore counts attempts per lane/src rather than checking
that the call returned.  Attribution comes from the request artefact's own counters, so
a page on the wrong client shows up as a ``broker_read`` attempt rather than as silence.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import httpx
import pytest

from app.providers.implementations.etoro_broker import EtoroBrokerProvider, TradeHistoryParseError
from app.providers.implementations.etoro_request_log import (
    etoro_lane_counters,
    reset_etoro_lane_counters,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

_MIN_DATE = datetime(2026, 1, 1, tzinfo=UTC)

#: The (lane, src, env) key every page of this endpoint must land on.
_HISTORY_KEY = ("G_default_shared", "broker_history", "demo")
#: The key a page lands on if it rides the shared read client instead.
_READ_KEY = ("G_default_shared", "broker_read", "demo")


def _row(position_id: int) -> dict[str, Any]:
    """One trade-history row with exactly the fields ``_parse_closed_trade`` requires."""
    return {
        "positionId": position_id,
        "instrumentId": 1001,
        "units": "1.5",
        "openTimestamp": "2026-01-02T10:00:00Z",
        "closeTimestamp": "2026-01-03T10:00:00Z",
    }


@pytest.fixture(autouse=True)
def _clean_counters() -> Iterator[None]:
    reset_etoro_lane_counters()
    yield
    reset_etoro_lane_counters()


@pytest.fixture
def broker() -> Iterator[EtoroBrokerProvider]:
    """A real provider whose throttle sleeps are removed.

    ⚠ The floor VALUE is not what this file tests — pacing identity is asserted in
    ``test_2946_etoro_throttle_lock.py`` and the budget arithmetic in
    ``test_etoro_quota_lanes.py``.  Leaving the real 3.33s floor in would add ~3.3s per
    page here and test the clock a third time.
    """
    with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as provider:
        provider._http_history._min_interval = 0.0
        provider._http_read._min_interval = 0.0
        yield provider


def _serve(broker: EtoroBrokerProvider, pages: list[httpx.Response]) -> list[httpx.Request]:
    """Serve ``pages`` in order; return the requests the provider actually issued."""
    seen: list[httpx.Request] = []
    remaining = list(pages)

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if not remaining:  # pragma: no cover - a page count regression, asserted below
            raise AssertionError(f"provider asked for more pages than the test served: {request.url}")
        return remaining.pop(0)

    broker._client._transport = httpx.MockTransport(handler)
    return seen


def _json_page(rows: list[dict[str, Any]]) -> httpx.Response:
    return httpx.Response(200, json=rows)


def test_a_single_page_fetch_lands_on_the_history_lane(broker: EtoroBrokerProvider) -> None:
    _serve(broker, [_json_page([_row(1)])])

    trades = broker.get_trade_history(_MIN_DATE, page_size=2)

    assert len(trades) == 1
    assert etoro_lane_counters()[_HISTORY_KEY].attempts == 1
    assert _READ_KEY not in etoro_lane_counters()


def test_every_page_of_a_multi_page_fetch_lands_on_the_history_lane(broker: EtoroBrokerProvider) -> None:
    """The defect a one-page test cannot see: a loop that pages on the read client.

    Three requests, because the second full page is followed by a short one.
    """
    seen = _serve(
        broker,
        [
            _json_page([_row(1), _row(2)]),
            _json_page([_row(3), _row(4)]),
            _json_page([_row(5)]),
        ],
    )

    trades = broker.get_trade_history(_MIN_DATE, page_size=2)

    assert [t.position_id for t in trades] == [1, 2, 3, 4, 5]
    assert [request.url.params["page"] for request in seen] == ["1", "2", "3"]
    assert etoro_lane_counters()[_HISTORY_KEY].attempts == 3
    assert _READ_KEY not in etoro_lane_counters(), "a page rode the shared read client"


def test_an_exactly_full_terminal_page_still_costs_a_second_request(broker: EtoroBrokerProvider) -> None:
    """``len(rows) < page_size`` is the exit, so exactly ``page_size`` rows cannot end it.

    This is the boundary the lane map's worst-case arithmetic turns on: the burst starts
    at >= page_size rows in the window, not > page_size.
    """
    _serve(broker, [_json_page([_row(1), _row(2)]), _json_page([])])

    trades = broker.get_trade_history(_MIN_DATE, page_size=2)

    assert [t.position_id for t in trades] == [1, 2]
    assert etoro_lane_counters()[_HISTORY_KEY].attempts == 2


def test_an_empty_first_page_costs_one_request(broker: EtoroBrokerProvider) -> None:
    _serve(broker, [_json_page([])])

    assert broker.get_trade_history(_MIN_DATE, page_size=2) == []
    assert etoro_lane_counters()[_HISTORY_KEY].attempts == 1


def test_a_failure_on_a_later_page_is_still_counted_against_the_history_lane(
    broker: EtoroBrokerProvider,
) -> None:
    """A 403 on page 2 must not vanish from the lane's accounting.

    ⚠ 403 deliberately, not 429 or 500: those are retryable, so ``ResilientClient`` would
    place three more stamps and the count would stop being the obvious one.
    """
    _serve(broker, [_json_page([_row(1), _row(2)]), httpx.Response(403, json={"errorCode": "Forbidden"})])

    with pytest.raises(httpx.HTTPStatusError):
        broker.get_trade_history(_MIN_DATE, page_size=2)

    counter = etoro_lane_counters()[_HISTORY_KEY]
    assert counter.attempts == 2
    assert _READ_KEY not in etoro_lane_counters()


def test_a_retry_places_its_own_stamp_on_the_history_lane(broker: EtoroBrokerProvider) -> None:
    """A rolling-window quota counts stamps, and a retry is one.

    The retry backoff is removed here; the floor is what paces production, and it is
    asserted elsewhere.
    """
    broker._http_history._backoff = (0.0, 0.0, 0.0)
    _serve(broker, [httpx.Response(429, json={"errorCode": "TooManyRequests"}), _json_page([_row(1)])])

    trades = broker.get_trade_history(_MIN_DATE, page_size=2)

    assert len(trades) == 1
    counter = etoro_lane_counters()[_HISTORY_KEY]
    assert (counter.attempts, counter.http_429) == (2, 1)


def test_a_transport_failure_is_counted_against_the_history_lane(broker: EtoroBrokerProvider) -> None:
    """A read timeout can arrive AFTER eToro received the request — it spent quota."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("body never arrived", request=request)

    broker._client._transport = httpx.MockTransport(handler)

    with pytest.raises(httpx.ReadTimeout):
        broker.get_trade_history(_MIN_DATE, page_size=2)

    counter = etoro_lane_counters()[_HISTORY_KEY]
    assert counter.attempts >= 1
    assert counter.transport_errors == counter.attempts


def test_a_parse_failure_on_a_later_page_keeps_the_earlier_pages_counted(
    broker: EtoroBrokerProvider,
) -> None:
    """The quota was spent whether or not the body parsed."""
    _serve(broker, [_json_page([_row(1), _row(2)]), httpx.Response(200, json={"not": "a list"})])

    with pytest.raises(TradeHistoryParseError):
        broker.get_trade_history(_MIN_DATE, page_size=2)

    assert etoro_lane_counters()[_HISTORY_KEY].attempts == 2


def test_the_real_environment_path_also_classifies_as_history(broker: EtoroBrokerProvider) -> None:
    """The env segment moves for real (`/trade/history`, no `/real/`), so the lane
    resolver has to match a DIFFERENT template shape — a demo-only test would not see a
    real-env path falling through to ``unclassified``.
    """
    with EtoroBrokerProvider(api_key="k", user_key="u", env="real") as real_broker:
        real_broker._http_history._min_interval = 0.0
        _serve(real_broker, [_json_page([_row(1)])])

        real_broker.get_trade_history(_MIN_DATE, page_size=2)

    assert etoro_lane_counters()[("G_default_shared", "broker_history", "real")].attempts == 1
    assert "unclassified" not in {key[0] for key in etoro_lane_counters()}
