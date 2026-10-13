"""``EtoroBrokerProvider.lookup_order_raw`` returns each response as received (#3616 slice 3c, spec Design 3).

Pure: no DB, no network. An ``httpx.MockTransport`` is swapped onto the provider's client.
"""

from __future__ import annotations

from collections.abc import Iterator

import httpx
import pytest

from app.providers.implementations.etoro_broker import EtoroBrokerProvider
from app.providers.implementations.etoro_request_log import reset_etoro_lane_counters


@pytest.fixture(autouse=True)
def _clean_counters() -> Iterator[None]:
    reset_etoro_lane_counters()
    yield
    reset_etoro_lane_counters()


@pytest.fixture
def broker() -> Iterator[EtoroBrokerProvider]:
    with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as provider:
        provider._http_read._min_interval = 0.0
        provider._http_read._backoff = (0.0, 0.0, 0.0)
        yield provider


def _serve(broker: EtoroBrokerProvider, handler) -> list[httpx.Request]:  # noqa: ANN001
    seen: list[httpx.Request] = []

    def wrapped(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return handler(request)

    broker._client._transport = httpx.MockTransport(wrapped)
    return seen


def test_a_found_body_is_returned_as_text(broker: EtoroBrokerProvider) -> None:
    text = '{"orderId": 7, "totalCosts": 0.10}'
    seen = _serve(broker, lambda _r: httpx.Response(200, text=text))
    raw = broker.lookup_order_raw(7)
    assert (raw.http_status, raw.body_text, raw.error_detail) == (200, text, None)
    assert seen[0].url.path == "/api/v2/trading/info/demo/orders:lookup"
    assert seen[0].url.params["orderId"] == "7"


def test_a_404_is_returned_not_raised(broker: EtoroBrokerProvider) -> None:
    _serve(broker, lambda _r: httpx.Response(404, text='{"errorMessage": "Order category for 7 not found"}'))
    raw = broker.lookup_order_raw(7)
    assert raw.http_status == 404 and raw.body_text is not None and "not found" in raw.body_text


def test_a_sustained_429_keeps_its_status_after_the_retries(broker: EtoroBrokerProvider) -> None:
    seen = _serve(broker, lambda _r: httpx.Response(429, text="slow down"))
    raw = broker.lookup_order_raw(7)
    assert (raw.http_status, raw.body_text) == (429, "slow down")
    assert len(seen) > 1  # the client retried before giving up


def test_a_transport_error_has_no_status(broker: EtoroBrokerProvider) -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    _serve(broker, refuse)
    raw = broker.lookup_order_raw(7)
    assert raw.http_status is None and raw.body_text is None
    assert raw.error_detail is not None and raw.error_detail.startswith("ConnectError")


@pytest.mark.parametrize("bad", [0, -1, True])
def test_a_non_id_is_refused_before_any_request(broker: EtoroBrokerProvider, bad: int) -> None:
    seen = _serve(broker, lambda _r: httpx.Response(200, text="{}"))
    with pytest.raises(ValueError, match="positive integer"):
        broker.lookup_order_raw(bad)
    assert seen == []
