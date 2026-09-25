"""#3381 slice 3: the perishables recorder's pure rules (spec §Local projections, §Envelope rules, §Arms)."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from app.providers.implementations.etoro_perishables import RawResponse
from app.services.perishables_recorder import (
    MAX_CONSECUTIVE_ERRORS,
    ArmCapacity,
    EligibilityRow,
    EnvelopeViolation,
    PerishableSnapshotRefused,
    Request,
    _Phase,
    arm_capacity,
    decide_arms,
    execute,
    failure_category,
    number,
    parse_eligibility,
    parse_rates,
    parse_what_if,
    partial_category,
    timestamp,
)
from app.services.sync_orchestrator.exception_classifier import classify_exception
from app.services.sync_orchestrator.layer_types import FailureCategory

T0 = datetime(2026, 9, 25, 19, 7, tzinfo=UTC)


def _config(settlement: str, direction: str, leverages: list[int], potential: bool = False) -> dict[str, object]:
    return {"settlementType": settlement, "direction": direction, "leverageValues": leverages, "isPotential": potential}


# The shape observed live on AAPL, 2026-09-25 (demo).
AAPL_CONFIGS = [
    _config("real", "long", [1]),
    _config("cfd", "short", [1, 2, 5, 10, 20]),
    _config("cfd", "long", [2, 5, 10, 20]),
]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (1, Decimal(1)),
        (1.25, Decimal("1.25")),
        ("0.5", Decimal("0.5")),
        (True, None),
        (None, None),
        ("abc", None),
        ("NaN", None),
        (float("inf"), None),
        ([1], None),
    ],
)
def test_number_accepts_only_finite_numbers(value: object, expected: Decimal | None) -> None:
    assert number(value) == expected


def test_timestamp_requires_a_timezone() -> None:
    # The live what-if clock carries 7 fractional digits; the 7th is truncated.
    assert timestamp("2026-09-25T15:28:40.1184428Z") == datetime(2026, 9, 25, 15, 28, 40, 118442, tzinfo=UTC)
    assert timestamp("2026-09-25T15:28:40") is None
    assert timestamp(1727277000) is None


def test_arm_capacity_on_the_observed_shape() -> None:
    assert arm_capacity(AAPL_CONFIGS) == ArmCapacity("available", "available", "real", 20)


def test_potential_only_arms_are_potential_not_absent() -> None:
    configs = [_config("cfd", "long", [1], potential=True), _config("cfd", "short", [1, 2], potential=True)]
    assert arm_capacity(configs) == ArmCapacity("potential", "potential", None, None)


def test_a_missing_arm_is_absent_and_short_reads_cfd_only() -> None:
    # A short x1 on a non-CFD settlement is not an eToro short (a short is a CFD).
    configs = [_config("cfd", "long", [2, 5]), _config("real", "short", [1])]
    assert arm_capacity(configs) == ArmCapacity("absent", "absent", None, None)


def test_long_settlement_follows_the_total_order() -> None:
    configs = [_config("cfd", "long", [1]), _config("real", "long", [1]), _config("marginTrade", "long", [1])]
    assert arm_capacity(configs).long_x1_settlement == "real"
    assert arm_capacity([_config("cfd", "long", [1])]).long_x1_settlement == "cfd"


def test_a_malformed_config_makes_its_side_unknown_never_false() -> None:
    bad_short = [_config("real", "long", [1]), {"settlementType": "cfd", "direction": "short", "leverageValues": [1]}]
    assert arm_capacity(bad_short) == ArmCapacity("available", None, "real", None)
    # An unreadable direction could be either arm.
    unknown_side = [
        _config("real", "long", [1]),
        {"settlementType": "cfd", "leverageValues": [1], "isPotential": False},
    ]
    assert arm_capacity(unknown_side) == ArmCapacity(None, None, None, None)
    assert arm_capacity(None) == ArmCapacity(None, None, None, None)
    assert arm_capacity([_config("mystery", "long", [1])]).long_x1 is None


def test_a_malformed_config_leaves_projections_it_cannot_belong_to() -> None:
    # A long x2 config cannot be the long x1 arm, whatever its isPotential says.
    not_x1 = [*AAPL_CONFIGS, {"settlementType": "cfd", "direction": "long", "leverageValues": [2]}]
    assert arm_capacity(not_x1) == ArmCapacity("available", "available", "real", 20)
    # A non-CFD short is not an eToro short, so it cannot make the short arm or its leverage unknown.
    not_cfd = [*AAPL_CONFIGS, {"settlementType": "real", "direction": "short", "leverageValues": [1]}]
    assert arm_capacity(not_cfd) == ArmCapacity("available", "available", "real", 20)
    # A cfd short x5 of unknown potential cannot be the x1 arm but could raise the max leverage.
    unknown_max = [*AAPL_CONFIGS, {"settlementType": "cfd", "direction": "short", "leverageValues": [5]}]
    assert arm_capacity(unknown_max) == ArmCapacity("available", "available", "real", None)
    # Known potential cannot count toward the max, so it leaves the max alone.
    potential_max = [*AAPL_CONFIGS, {"settlementType": "cfd", "leverageValues": [5], "isPotential": True}]
    assert arm_capacity(potential_max) == ArmCapacity("available", "available", "real", 20)


def test_parse_eligibility_found_not_found_and_omitted() -> None:
    body = {
        "eligibilities": [
            {
                "instrumentId": 1001,
                "allowOpenPosition": True,
                "allowClosePosition": True,
                "leverageConfigs": AAPL_CONFIGS,
            }
        ],
        "notFoundInstrumentIds": [7],
    }
    rows = parse_eligibility([1001, 7, 8], body)
    assert [(r.instrument_id, r.answer) for r in rows] == [(1001, "found"), (7, "not_found")]  # 8 omitted
    assert rows[0].capacity.short_x1 == "available"
    assert rows[0].min_position_exposure is None  # absent → NULL, never 0


@pytest.mark.parametrize(
    "body",
    [
        [],
        {"eligibilities": []},
        {"eligibilities": [{"instrumentId": 9}], "notFoundInstrumentIds": []},  # unrequested
        {"eligibilities": [{"instrumentId": 1}], "notFoundInstrumentIds": [1]},  # both found and not found
        {"eligibilities": [{"instrumentId": 1}, {"instrumentId": 1}], "notFoundInstrumentIds": []},
        {"eligibilities": ["x"], "notFoundInstrumentIds": []},
    ],
)
def test_parse_eligibility_envelope_violations(body: object) -> None:
    with pytest.raises(EnvelopeViolation):
        parse_eligibility([1, 2], body)


def test_parse_rates_keeps_values_as_served_and_skips_unidentified_entries() -> None:
    body = {
        "rates": [
            {"instrumentID": 1, "bid": 0, "ask": 1.5, "date": "2026-09-25T19:07:00Z"},  # zero kept, no filter
            {"bid": 1, "ask": 2},  # no id: raw only
            {"instrumentID": 2, "bid": "x", "ask": None, "date": "not a date"},
        ]
    }
    rows = parse_rates([1, 2, 3], body)
    assert [(r.instrument_id, r.bid, r.ask) for r in rows] == [(1, Decimal(0), Decimal("1.5")), (2, None, None)]
    assert rows[0].quote_at == datetime(2026, 9, 25, 19, 7, tzinfo=UTC)
    assert rows[1].quote_at is None


@pytest.mark.parametrize(
    "body",
    [
        {"rates": "oops"},
        {},
        {"rates": [{"instrumentID": 1}, {"instrumentID": 1}]},
        {"rates": [{"instrumentID": 9}]},
    ],
)
def test_parse_rates_envelope_violations(body: object) -> None:
    with pytest.raises(EnvelopeViolation):
        parse_rates([1, 2], body)


def test_parse_what_if_checks_identity_and_shape() -> None:
    ok = {"instrumentId": 1001, "costs": [], "lastUpdated": "2026-09-25T15:28:43Z"}
    assert parse_what_if(1001, ok) == datetime(2026, 9, 25, 15, 28, 43, tzinfo=UTC)
    assert parse_what_if(1001, {**ok, "lastUpdated": None}) is None
    with pytest.raises(EnvelopeViolation):
        parse_what_if(1002, ok)
    with pytest.raises(EnvelopeViolation):
        parse_what_if(1001, {"instrumentId": 1001})


def _found(**kwargs: object) -> EligibilityRow:
    base: dict[str, object] = {
        "instrument_id": 1001,
        "answer": "found",
        "allow_open_position": True,
        "capacity": arm_capacity(AAPL_CONFIGS),
    }
    return EligibilityRow(**{**base, **kwargs})  # type: ignore[arg-type]


def test_decide_arms_plans_both_sides_from_the_eligibility_answer() -> None:
    long, short = decide_arms(1001, (_found(), 3))
    assert (long.outcome, short.outcome) == (None, None)
    assert long.order is not None and short.order is not None
    assert (long.order.transaction, long.order.settlement_type, long.order.leverage) == ("buy", "real", 1)
    assert (short.order.transaction, short.order.settlement_type) == ("sellShort", "cfd")
    assert long.order.amount == Decimal("1000")
    assert long.eligibility_seq == short.eligibility_seq == 3


def test_decide_arms_refusal_states() -> None:
    no_answer = decide_arms(1, None)
    assert [a.outcome for a in no_answer] == ["undecided", "undecided"]
    assert [a.eligibility_seq for a in no_answer] == [None, None]
    not_found = EligibilityRow(instrument_id=1, answer="not_found")
    assert [a.outcome for a in decide_arms(1, (not_found, 0))] == ["not_eligible", "not_eligible"]
    assert [a.outcome for a in decide_arms(1, (_found(allow_open_position=False), 0))] == ["not_eligible"] * 2
    assert [a.outcome for a in decide_arms(1, (_found(allow_open_position=None), 0))] == ["undecided"] * 2
    potential_short = _found(capacity=ArmCapacity("available", "potential", "real", None))
    assert [a.outcome for a in decide_arms(1, (potential_short, 0))] == [None, "not_offered"]


def _request(outcome: str, status: int | None = 200) -> Request:
    return Request("rates", 0, (1,), {}, T0, outcome, status, {})


def _response(status: int, body: object) -> RawResponse:
    return RawResponse(status, body, T0)


def test_execute_classifies_each_failure_and_keeps_the_status() -> None:
    def run(call: object) -> Request:
        request, _ = execute("rates", 0, [1], {}, call, lambda body: parse_rates([1], body), lambda: T0)  # type: ignore[arg-type]
        return request

    ok = run(lambda: _response(200, {"rates": [{"instrumentID": 1, "bid": 1}]}))
    assert (ok.outcome, ok.http_status) == ("ok", 200)
    malformed = run(lambda: _response(200, {"rates": "oops"}))
    assert (malformed.outcome, malformed.http_status, malformed.raw) == ("error", 200, {"rates": "oops"})
    refused = run(lambda: _response(400, "bad request"))
    assert (refused.outcome, refused.http_status, refused.raw) == ("error", 400, "bad request")

    response = httpx.Response(503, text="down", request=httpx.Request("GET", "https://x"))

    def exhausted() -> RawResponse:
        raise httpx.HTTPStatusError("503", request=response.request, response=response)

    def transport() -> RawResponse:
        raise httpx.ConnectTimeout("timed out")

    assert (run(exhausted).http_status, run(exhausted).raw) == (503, "down")
    dropped = run(transport)
    assert (dropped.outcome, dropped.http_status, dropped.raw) == ("error", None, "ConnectTimeout: timed out")


def test_phase_systemic_rules() -> None:
    with pytest.raises(PerishableSnapshotRefused, match="401"):
        _Phase(3).add(_request("error", 401))
    with pytest.raises(PerishableSnapshotRefused, match="403"):
        _Phase(3).add(_request("error", 403))

    phase = _Phase(100)
    for _ in range(MAX_CONSECUTIVE_ERRORS - 1):
        phase.add(_request("error", 500))
    phase.add(_request("ok"))  # an ok resets the run of errors
    for _ in range(MAX_CONSECUTIVE_ERRORS - 1):
        phase.add(_request("error", 500))
    with pytest.raises(PerishableSnapshotRefused, match="consecutive"):
        phase.add(_request("error", None))

    failed = _Phase(1)
    failed.add(_request("error", 500))
    with pytest.raises(PerishableSnapshotRefused, match="no rates request succeeded"):
        failed.finish("rates")
    _Phase(0).finish("rates")  # nothing planned is not a failure


def test_failures_keep_an_operator_actionable_category() -> None:
    assert failure_category(_request("error", None)) is FailureCategory.SOURCE_DOWN
    assert failure_category(_request("error", 503)) is FailureCategory.SOURCE_DOWN
    assert failure_category(_request("error", 429)) is FailureCategory.RATE_LIMITED
    assert failure_category(_request("error", 401)) is FailureCategory.AUTH_EXPIRED
    assert failure_category(_request("error", 200)) is FailureCategory.SCHEMA_DRIFT
    assert failure_category(_request("error", 400)) is FailureCategory.INTERNAL_ERROR
    # The most actionable category wins; a transport blip does not mask drift.
    mixed = [_request("error", None), _request("error", 200), _request("ok")]
    assert partial_category(mixed) is FailureCategory.SCHEMA_DRIFT

    with pytest.raises(PerishableSnapshotRefused) as refused:
        _Phase(3).add(_request("error", 403))
    assert classify_exception(refused.value) is FailureCategory.AUTH_EXPIRED
