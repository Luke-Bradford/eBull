from __future__ import annotations

from datetime import datetime, timedelta

import httpx
import pytest

import app.services.alpaca_delayed_sip_probe as probe


def _bar(stamp: datetime, *, scale: int = 1) -> dict[str, object]:
    return {
        "t": stamp.isoformat().replace("+00:00", "Z"),
        "o": 100 * scale,
        "h": 102 * scale,
        "l": 99 * scale,
        "c": 101 * scale,
        "v": 1_000 // scale,
    }


def test_credentials_fail_closed_without_printing_secret_values() -> None:
    with pytest.raises(probe.ProbeRefusal, match="both required"):
        probe.credential_headers(key_id="", secret_key="secret")
    with pytest.raises(probe.ProbeRefusal, match="placeholder"):
        probe.credential_headers(key_id="<key>", secret_key="secret")

    headers = probe.credential_headers(key_id="id-value", secret_key="secret-value")

    assert headers["APCA-API-KEY-ID"] == "id-value"
    assert headers["APCA-API-SECRET-KEY"] == "secret-value"


def test_bar_validation_rejects_bad_bounds_and_nonascending_rows() -> None:
    first = datetime.fromisoformat("2026-07-01T13:30:00+00:00")
    bad = _bar(first)
    bad["h"] = 98
    with pytest.raises(probe.ProbeRefusal, match="OHLC bounds"):
        probe.validate_bars([bad])

    with pytest.raises(probe.ProbeRefusal, match="strictly ascending"):
        probe.validate_bars([_bar(first), _bar(first)])

    fractional_volume = _bar(first)
    fractional_volume["v"] = 1.5
    with pytest.raises(probe.ProbeRefusal, match="volume must be an integer"):
        probe.validate_bars([fractional_volume])

    naive_timestamp = _bar(first)
    naive_timestamp["t"] = "2026-07-01T13:30:00"
    with pytest.raises(probe.ProbeRefusal, match="timestamp must carry an offset"):
        probe.validate_bars([naive_timestamp])


def test_fetch_bar_scenario_follows_bounded_pagination_and_pins_contract() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        token = request.url.params.get("page_token")
        start = datetime.fromisoformat("2016-01-04T14:30:00+00:00")
        rows = [_bar(start + timedelta(minutes=2 if token else 0))]
        return httpx.Response(
            200,
            json={"bars": rows, "next_page_token": None if token else "next"},
            headers={"x-ratelimit-limit": "200"},
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        summary, headers = probe.fetch_bar_scenario(
            client,
            probe._CallBudget(),
            symbol="AAPL",
            start="2016-01-04T14:30:00Z",
            end="2016-01-04T14:36:00Z",
            asof="2016-01-04",
            limit=1,
        )

    assert summary["rows"] == 2
    assert summary["pages"] == 2
    assert headers == {"x-ratelimit-limit": "200"}
    assert len(requests) == 2
    assert requests[0].url.params["feed"] == "sip"
    assert requests[0].url.params["adjustment"] == "raw"
    assert requests[0].url.params["asof"] == "2016-01-04"
    assert requests[1].url.params["page_token"] == "next"


def test_fetch_refuses_entitlement_failure_without_retrying_or_weakening_feed() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(403, json={"message": "subscription does not permit SIP"})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(probe.ProbeRefusal, match="HTTP 403.*does not permit SIP"):
            probe.fetch_bar_scenario(
                client,
                probe._CallBudget(),
                symbol="AAPL",
                start="2026-07-01T13:30:00Z",
                end="2026-07-01T13:36:00Z",
                asof="2026-07-01",
            )

    assert len(requests) == 1
    assert requests[0].url.params["feed"] == "sip"


def test_fetch_refuses_to_report_an_incomplete_bounded_page_set() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        start = datetime.fromisoformat("2016-01-04T14:30:00+00:00")
        offset = 1 if request.url.params.get("page_token") else 0
        next_token = "more-2" if offset else "more-1"
        return httpx.Response(
            200,
            json={"bars": [_bar(start + timedelta(minutes=offset))], "next_page_token": next_token},
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(probe.ProbeRefusal, match="exceeded the 2-page bound"):
            probe.fetch_bar_scenario(
                client,
                probe._CallBudget(),
                symbol="AAPL",
                start="2016-01-04T14:30:00Z",
                end="2016-01-04T14:33:00Z",
                asof="2016-01-04",
                limit=1,
            )


def test_complete_probe_is_bounded_and_returns_only_compact_evidence() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        path = request.url.path
        if path.endswith("/calendar"):
            return httpx.Response(200, json=[{"date": "2025-11-28", "open": "09:30", "close": "13:00"}])
        if path.endswith("/corporate-actions"):
            return httpx.Response(
                200,
                json={
                    "corporate_actions": {
                        "forward_splits": [
                            {
                                "id": "split",
                                "symbol": "TSLA",
                                "new_rate": 3,
                                "old_rate": 1,
                                "ex_date": "2022-08-25",
                            }
                        ]
                    }
                },
            )
        if path.endswith("/assets/TWTR"):
            return httpx.Response(
                200,
                json={"symbol": "TWTR", "status": "inactive", "exchange": "NYSE", "id": "asset-id"},
            )
        if "/bars" in path:
            start = datetime.fromisoformat(request.url.params["start"].replace("Z", "+00:00"))
            token = request.url.params.get("page_token")
            offset = 2 if token else 0
            scale = 3 if request.url.params.get("adjustment") == "split" else 1
            body: dict[str, object] = {"bars": [_bar(start + timedelta(minutes=offset), scale=scale)]}
            if request.url.params.get("limit") == "2" and token is None:
                body["next_page_token"] = "page-2"
            return httpx.Response(
                200,
                json=body,
                headers={"x-ratelimit-limit": "200", "x-ratelimit-remaining": str(200 - calls)},
            )
        raise AssertionError(f"unexpected probe URL {request.url}")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = probe.run_probe(client)

    assert result["status"] == "qualified"
    assert result["calls_used"] == 11
    assert result["call_budget"] == 12
    assert calls == 11
    assert result["rate_limit_headers"]["x-ratelimit-limit"] == "200"  # type: ignore[index]
    rendered = str(result)
    assert "APCA-API" not in rendered
    assert "secret" not in rendered


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ({"corporate_actions": {"forward_splits": []}}, "no forward split"),
        (
            {"corporate_actions": {"forward_splits": [{"symbol": "AAPL", "new_rate": 4, "old_rate": 1}]}},
            "did not contain TSLA",
        ),
        (
            {"corporate_actions": {"forward_splits": [{"symbol": "TSLA", "new_rate": 1, "old_rate": 1}]}},
            "does not increase shares",
        ),
        (
            {"corporate_actions": {"forward_splits": [{"symbol": "TSLA", "new_rate": 1, "old_rate": 3}]}},
            "does not increase shares",
        ),
    ],
)
def test_forward_split_check_requires_actual_matching_action(body: object, message: str) -> None:
    with pytest.raises(probe.ProbeRefusal, match=message):
        probe._require_tsla_forward_split(body)


def test_inactive_asset_check_requires_twtr_and_inactive_status() -> None:
    with pytest.raises(probe.ProbeRefusal, match="did not resolve as an inactive asset"):
        probe._require_inactive_twtr({"symbol": "TWTR", "status": "active"})

    summary = probe._require_inactive_twtr({"symbol": "TWTR", "status": "inactive", "exchange": "NYSE"})

    assert summary == {"name": "inactive_asset", "symbol": "TWTR", "status": "inactive", "exchange": "NYSE"}


def test_call_budget_fails_before_an_unbounded_request() -> None:
    budget = probe._CallBudget(maximum=1)
    budget.take()

    with pytest.raises(probe.ProbeRefusal, match="budget exhausted"):
        budget.take()
