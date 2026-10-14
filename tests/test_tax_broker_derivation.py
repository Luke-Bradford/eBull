"""Pure tests for the slice 3c derivation (#3616; spec Design 4 and 5): partition, checks, lots, sterling, fingerprint.

The canonical shapes mirror the dev account (spec facts 5-10) with invented ids and round numbers: an AAPL-like order
split by a partial close into two fully closed pieces, and a sync-only order still held.
"""

from __future__ import annotations

import copy
import hashlib
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, Inexact
from pathlib import Path
from typing import Any

import pytest

from app.providers.implementations.govuk_calendar import parse_bank_holidays
from app.services.tax_broker_derivation import (
    TAX_EVIDENCE_POLICY_ID,
    EvidenceLot,
    FoundLookup,
    FxRefusal,
    HeldPosition,
    LedgerEvent,
    LotDraft,
    RetainedObservation,
    Scope,
    derive_all,
    derive_order,
    evidence_context,
    fingerprint_bytes,
    lookup_field_failures,
    lot_fingerprint,
    parse_lookup_body,
    partition,
    price_lots,
)
from app.services.tax_fx import TAX_FX_POLICY_ID, TaxFxState
from app.services.trade_events import normalise_payload

D = Decimal
ACCOUNT = 900
SCOPE = Scope("demo", ACCOUNT)
ORDER = 1000
BROKER_INSTRUMENT = 1001
APP_INSTRUMENT = 7
OPENED = "2026-09-23T13:30:00.5Z"
OPENED_AT = datetime(2026, 9, 23, 13, 30, 0, 500000, tzinfo=UTC)
CLOSED_A = datetime(2026, 9, 24, 14, 0, tzinfo=UTC)
CLOSED_B = datetime(2026, 9, 25, 15, 0, tzinfo=UTC)


def _iso(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# --- builders ---------------------------------------------------------------------------------------------------


def history_row(position_id: int, units: str, closed: datetime, **over: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "positionId": position_id,
        "orderId": ORDER,
        "instrumentId": BROKER_INSTRUMENT,
        "isBuy": True,
        "leverage": 1,
        "socialTradeId": 0,
        "parentPositionId": 0,
        "initialInvestment": D("100"),
        "openRate": D("100"),
        "openTimestamp": OPENED,
        "units": D(units),
        "closeTimestamp": _iso(closed),
        "investment": D("50"),
        "netProfit": D("1"),
        "fees": D("0"),
        "closeRate": D("102"),
    }
    row.update(over)
    return row


def sync_payload(position_id: int, **over: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "CID": ACCOUNT,
        "orderID": ORDER,
        "positionID": position_id,
        "instrumentID": BROKER_INSTRUMENT,
        "settlementTypeID": 1,
        "isBuy": True,
        "isPartiallyAltered": False,
        "leverage": 1,
        "mirrorID": 0,
        "parentPositionID": 0,
        "initialUnits": D("1"),
        "initialAmountInDollars": D("100"),
        "totalExternalFees": D("0"),
        "totalExternalTaxes": D("0"),
        "totalFees": D("0"),
        "openRate": D("100"),
        "openConversionRate": D("1"),
        "openDateTime": OPENED,
    }
    payload.update(over)
    return payload


def execution(position_id: int, state: str = "closed", remaining: str = "0.5", **opening: Any) -> dict[str, Any]:
    opening_data: dict[str, Any] = {
        "openTime": "2026-09-23T13:30:00.4Z",
        "orderId": ORDER,
        "executionTime": OPENED,
        "units": D("1"),
        "avgPrice": D("100"),
        "avgConversionRate": D("1"),
        "marketSpread": D("0.02"),
        "markup": D("0"),
        "fees": D("0"),
        "taxes": D("0"),
    }
    opening_data.update(opening)
    return {
        "positionId": position_id,
        "state": state,
        "addedFunds": D("0"),
        "remainingUnits": D(remaining),
        "openingData": opening_data,
    }


def lookup_body(executions: list[dict[str, Any]], **over: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "accountId": ACCOUNT,
        "orderId": ORDER,
        "action": "open",
        "transaction": "buy",
        "status": {"id": 3, "name": "Filled"},
        "asset": {
            "instrumentId": BROKER_INSTRUMENT,
            "currency": "USD",
            "settlementType": "REAL",
            "leverage": 1,
            "side": "long",
        },
        "orderCurrency": "usd",
        "totalCosts": D("0"),
        "positionExecutions": executions,
    }
    body.update(over)
    return body


_EVENT_IDS = iter(range(1, 1_000_000))


def event(
    kind: str,
    position_id: int,
    payload: dict[str, Any],
    *,
    source: str = "etoro_history",
    units: str = "1",
    executed_at: datetime = OPENED_AT,
    instrument_id: int | None = APP_INSTRUMENT,
    broker_instrument: int = BROKER_INSTRUMENT,
    order_id: int | None = ORDER,
) -> LedgerEvent:
    return LedgerEvent(
        event_id=next(_EVENT_IDS),
        position_id=position_id,
        etoro_instrument_id=broker_instrument,
        instrument_id=instrument_id,
        event_kind=kind,  # type: ignore[arg-type]
        units=D(units),
        executed_at=executed_at,
        order_id=order_id if source == "etoro_history" else None,
        source=source,
        raw_payload=payload,
    )


_OBS_IDS = iter(range(1, 1_000_000))


def observation(
    of: LedgerEvent | None,
    kind: str,
    payload: dict[str, Any],
    *,
    source: str | None = None,
    position_id: int | None = None,
    event_kind: str | None = None,
    executed_at: datetime | None = None,
) -> RetainedObservation:
    src = source or (of.source if of else "etoro_sync")
    ek = event_kind or (of.event_kind if of else "open")
    return RetainedObservation(
        observation_id=next(_OBS_IDS),
        event_id=None if kind == "unscoped" else (of.event_id if of else None),
        position_id=position_id if position_id is not None else (of.position_id if of else 0),
        event_kind=ek,  # type: ignore[arg-type]
        executed_at=executed_at if executed_at is not None else (of.executed_at if of and ek == "close" else None),
        kind=kind,  # type: ignore[arg-type]
        source=src,
        observed_normalised=normalise_payload(src, ek, payload),  # type: ignore[arg-type]
        observed_raw=payload,
    )


def found(body: dict[str, Any], lookup_id: int = 1) -> FoundLookup:
    return FoundLookup(lookup_id, datetime(2026, 10, 1, tzinfo=UTC), TAX_EVIDENCE_POLICY_ID, body)


def held(position_id: int, units: str = "1", amount: str = "100", **payload: Any) -> HeldPosition:
    return HeldPosition(position_id, D(units), D(amount), {"CID": ACCOUNT, "orderID": ORDER, **payload})


class Split:
    """The AAPL split shape (fact 5): one execution, two closed pieces, history-only, a lookup naming both."""

    def __init__(self) -> None:
        a = history_row(11, "0.5", CLOSED_A)
        b = history_row(12, "0.5", CLOSED_B)
        self.open_a = event("open", 11, a, units="0.5")
        self.open_b = event("open", 12, b, units="0.5")
        self.close_a = event("close", 11, a, units="0.5", executed_at=CLOSED_A)
        self.close_b = event("close", 12, b, units="0.5", executed_at=CLOSED_B)
        self.events = [self.open_a, self.open_b, self.close_a, self.close_b]
        self.body = lookup_body([execution(11), execution(12)])
        self.observations: list[RetainedObservation] = []
        self.held: dict[int, HeldPosition | None] = {11: None, 12: None}

    def derive(self, *bodies: dict[str, Any]) -> Any:
        lookups = [found(b, i + 1) for i, b in enumerate(bodies or (self.body,))]
        return derive_order(SCOPE, ORDER, self.events, self.observations, lookups, self.held)


class Held:
    """The NXH shape: one sync-observed position, still held, no lookup (execution_evidence = sync_only)."""

    def __init__(self, **sync: Any) -> None:
        self.payload = sync_payload(21, **sync)
        self.open = event("open", 21, self.payload, source="etoro_sync")
        self.events = [self.open]
        self.observations: list[RetainedObservation] = []
        self.held: dict[int, HeldPosition | None] = {21: held(21)}

    def derive(self, lookups: list[FoundLookup] | None = None) -> Any:
        return derive_order(SCOPE, ORDER, self.events, self.observations, lookups or [], self.held)


def reasons_of(result: Any) -> list[str]:
    assert result.outcome == "unresolved", result
    return list(result.reasons)


# --- admitted shapes --------------------------------------------------------------------------------------------


def test_split_shape_is_admitted_with_one_acquisition_and_two_disposals() -> None:
    case = Split()
    result = case.derive()
    assert (result.outcome, result.reasons, result.execution_evidence, result.product) == (
        "admitted",
        (),
        "lookup",
        "real",
    )
    assert result.total_fees_unobserved_position_ids == (11, 12)
    acquisition, *disposals = result.lots
    assert acquisition == LotDraft(
        lot_key=f"acq:demo:{ORDER}",
        subject=f"order:{ORDER}",
        order_id=ORDER,
        instrument_id=APP_INSTRUMENT,
        etoro_instrument_id=BROKER_INSTRUMENT,
        direction="acquisition",
        position_ids=(11, 12),
        trade_event_ids=tuple(sorted((case.open_a.event_id, case.open_b.event_id))),
        executed_at=OPENED_AT,
        units=D("1"),
        usd_amount=D("100"),
        execution_evidence="lookup",
    )
    assert [(d.lot_key, d.position_ids, d.trade_event_ids, d.units, d.usd_amount) for d in disposals] == [
        ("dis:demo:11:2026-09-24T14:00:00.000000Z", (11,), (case.close_a.event_id,), D("0.5"), D("51")),
        ("dis:demo:12:2026-09-25T15:00:00.000000Z", (12,), (case.close_b.event_id,), D("0.5"), D("51")),
    ]
    assert acquisition.uk_date == date(2026, 9, 23) and acquisition.tax_year == "2026/27"


def test_sync_only_order_is_admitted_with_sync_only_evidence() -> None:
    result = Held().derive()
    assert (result.outcome, result.execution_evidence) == ("admitted", "sync_only")
    assert [lot.execution_evidence for lot in result.lots] == ["sync_only"]
    assert result.total_fees_unobserved_position_ids == ()


def test_sync_only_order_with_a_partially_altered_lifecycle_observation_is_unestablished() -> None:
    case = Held()
    case.observations.append(observation(case.open, "lifecycle", sync_payload(21, isPartiallyAltered=True)))
    assert reasons_of(case.derive()) == ["executions_unestablished"]


def test_a_lookup_and_a_held_position_bind_an_open_piece() -> None:
    case = Held()
    result = case.derive([found(lookup_body([execution(21, "open", "1")]))])
    assert (result.outcome, result.execution_evidence) == ("admitted", "lookup")


# --- splits, pieces and amendments of the lookup -----------------------------------------------------------------


def test_non_usd_split_reconstructs_one_opening_and_needs_a_close_conversion() -> None:
    case = Split()
    case.body["asset"]["currency"] = "GBX"
    assert reasons_of(case.derive()) == ["close_conversion_unestablished"]


def test_a_split_with_one_piece_still_open() -> None:
    case = Split()
    case.events.remove(case.close_b)
    case.held[12] = held(12, units="0.5", amount="50")
    case.body["positionExecutions"][1] = execution(12, "open", "0.5")
    assert "unsupported_shape:partially_open_split" in reasons_of(case.derive())


def test_a_piece_added_after_capture_is_not_an_amendment() -> None:
    case = Split()
    before = lookup_body([execution(11, "open", "1")])
    assert case.derive(before, case.body).outcome == "admitted"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda b: b["positionExecutions"].pop(),
        lambda b: b["positionExecutions"][0]["openingData"].update(avgPrice=D("101")),
        lambda b: b.update(totalCosts=D("0.01")),
        lambda b: b.pop("orderCurrency"),
    ],
    ids=["drops_a_piece", "changes_opening", "changes_total_costs", "drops_a_field"],
)
def test_a_later_body_that_changes_more_than_the_lifecycle_is_lookup_amended(
    mutate: Callable[[dict[str, Any]], Any],
) -> None:
    case = Split()
    later = copy.deepcopy(case.body)
    mutate(later)
    assert "lookup_amended" in reasons_of(case.derive(case.body, later))


def test_lookup_amended_is_sticky() -> None:
    case = Split()
    contradictory = copy.deepcopy(case.body)
    contradictory["asset"]["settlementType"] = "SPACESHIP"
    assert case.derive(case.body).outcome == "admitted"
    assert "lookup_amended" in reasons_of(case.derive(case.body, contradictory))
    assert reasons_of(case.derive(case.body, contradictory, case.body)) == ["lookup_amended"]
    fee = copy.deepcopy(case.body)
    fee["positionExecutions"][0]["openingData"]["fees"] = D("1")
    del fee["positionExecutions"][0]["openingData"]["orderId"]
    assert reasons_of(case.derive(case.body, fee, case.body)) == ["lookup_amended"]


@pytest.mark.parametrize("remaining", ["0.5", "0"])
def test_a_closed_piece_with_no_stored_close(remaining: str) -> None:
    case = Split()
    case.events.remove(case.close_b)
    case.body["positionExecutions"][1]["remainingUnits"] = D(remaining)
    assert "piece_coverage_incomplete" in reasons_of(case.derive())


def test_two_different_openings_are_multiple_executions() -> None:
    case = Split()
    case.body["positionExecutions"][1]["openingData"]["units"] = D("0.5")
    assert "multiple_executions" in reasons_of(case.derive())


# --- binding checks ---------------------------------------------------------------------------------------------


def test_close_only_position_has_no_open_event() -> None:
    case = Split()
    case.events.remove(case.open_b)
    assert "open_event_missing" in reasons_of(case.derive())


def test_close_units_column_differing_from_payload() -> None:
    case = Split()
    case.close_a.raw_payload["units"] = D("0.4")  # type: ignore[index]
    assert "identity_conflict:units" in reasons_of(case.derive())


def test_events_disagreeing_on_instrument() -> None:
    case = Split()
    case.events[1] = event("open", 12, history_row(12, "0.5", CLOSED_B), units="0.5", broker_instrument=5)
    assert "instrument_conflict" in reasons_of(case.derive())


def test_account_conflict_and_unestablished() -> None:
    case = Split()
    case.body["accountId"] = ACCOUNT + 1
    assert reasons_of(case.derive()) == ["account_conflict"]
    history_only = Split()
    assert derive_order(SCOPE, ORDER, history_only.events, [], [], history_only.held).reasons == (
        "account_unestablished",
        "settlement_unestablished",
    )


def test_missing_fees_is_not_zero() -> None:
    case = Split()
    del case.close_a.raw_payload["fees"]  # type: ignore[attr-defined]
    assert reasons_of(case.derive()) == ["missing_field:fees"]


def test_two_spellings_disagreeing_in_one_payload() -> None:
    case = Held(orderId=ORDER + 1)
    assert "field_conflict:orderID" in reasons_of(case.derive())


def test_lowercase_enum_and_unknown_settlement_type() -> None:
    case = Split()
    case.body["asset"]["settlementType"] = "real"
    assert case.derive().outcome == "admitted"
    case.body["asset"]["settlementType"] = "SPACESHIP"
    assert reasons_of(case.derive()) == ["invalid_field:asset.settlementType"]


def test_cfd_real_conflict() -> None:
    case = Held(settlementTypeID=0)
    assert "settlement_conflict" in reasons_of(case.derive([found(lookup_body([execution(21, "open", "1")]))]))


def test_a_position_without_a_sync_observation_and_no_lookup() -> None:
    case = Split()
    assert "settlement_unestablished" in reasons_of(derive_order(SCOPE, ORDER, case.events, [], [], case.held))


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda b: b["positionExecutions"].pop(), "lookup_mismatch:positions"),
        (lambda b: b["positionExecutions"].append(execution(12)), "lookup_mismatch:positions"),
        (lambda b: b["positionExecutions"][0]["openingData"].update(orderId=ORDER + 1), "lookup_mismatch:order_id"),
        (lambda b: b["asset"].update(instrumentId=5), "lookup_mismatch:instrument_id"),
    ],
)
def test_lookup_binding(mutate: Callable[[dict[str, Any]], Any], reason: str) -> None:
    case = Split()
    mutate(case.body)
    assert reason in reasons_of(case.derive())


def test_body_spelling_order_id_with_a_fee() -> None:
    case = Split()
    case.body["orderID"] = case.body.pop("orderId")
    case.body["totalCosts"] = D("1")
    assert reasons_of(case.derive()) == ["charges_nonzero:totalCosts"]


def test_body_with_two_order_id_spellings() -> None:
    case = Split()
    case.body["orderID"] = ORDER + 1
    assert "field_conflict:orderId" in reasons_of(case.derive())


def test_minimal_found_body_fails_with_missing_fields() -> None:
    body = parse_lookup_body(f'{{"orderId": {ORDER}, "asset": {{}}, "positionExecutions": [{{}}]}}')
    failures = lookup_field_failures(body)
    assert "missing_field:accountId" in failures and "missing_field:asset.settlementType" in failures
    assert "missing_field:openingData" in failures


# --- exclusion --------------------------------------------------------------------------------------------------


def _cfd_case() -> Held:
    case = Held(settlementTypeID=0, totalExternalFees=D("1.5"))
    return case


def test_a_bound_cfd_order_is_excluded_whatever_its_fees() -> None:
    body = lookup_body([execution(21, "open", "1", fees=D("1.5"))], totalCosts=D("1.5"))
    body["asset"]["settlementType"] = "CFD"
    result = _cfd_case().derive([found(body)])
    assert (result.outcome, result.reasons, result.lots, result.product) == (
        "excluded_non_share_product",
        (),
        (),
        "cfd",
    )


@pytest.mark.parametrize("defect", ["instrument", "quarantine", "amendment"])
def test_a_cfd_order_failing_a_binding_check_is_unresolved_not_excluded(defect: str) -> None:
    case = _cfd_case()
    body = lookup_body([execution(21, "open", "1")])
    body["asset"]["settlementType"] = "CFD"
    partition_reasons: list[str] = []
    if defect == "instrument":
        body["asset"]["instrumentId"] = 5
    elif defect == "quarantine":
        partition_reasons.append("position_quarantined")
    else:
        case.observations.append(observation(case.open, "amendment", sync_payload(21, settlementTypeID=0, leverage=2)))
    result = derive_order(SCOPE, ORDER, case.events, case.observations, [found(body)], case.held, partition_reasons)
    assert result.outcome == "unresolved"


# --- real-asset checks ------------------------------------------------------------------------------------------


def test_status_not_filled_and_unknown_execution_state() -> None:
    case = Split()
    case.body["status"]["name"] = "Cancelled"
    case.body["positionExecutions"][0]["state"] = "pending"
    assert reasons_of(case.derive()) == ["invalid_field:positionExecutions.state", "unsupported_shape:status.name"]


def test_a_copied_history_row() -> None:
    case = Split()
    case.close_a.raw_payload["socialTradeId"] = 5  # type: ignore[index]
    assert "unsupported_shape:socialTradeId" in reasons_of(case.derive())


def test_a_supplemental_sync_observation_beside_a_history_first_open() -> None:
    case = Split()
    case.observations.append(
        observation(case.open_a, "supplemental", sync_payload(11, mirrorID=7), source="etoro_sync")
    )
    assert "unsupported_shape:mirrorID" in reasons_of(case.derive())


def test_mixed_acquisition_dates_and_a_close_before_its_open() -> None:
    case = Split()
    case.close_b.raw_payload["openTimestamp"] = "2026-09-22T10:00:00Z"  # type: ignore[index]
    assert "acquisition_date_conflict" in reasons_of(case.derive())
    early = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)
    payload = history_row(12, "0.5", early)
    case = Split()
    case.events[3] = event("close", 12, payload, units="0.5", executed_at=early)
    assert "close_before_open" in reasons_of(case.derive())


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda b: b.update(totalCosts=D("1")), "charges_nonzero:totalCosts"),
        (
            lambda b: b["positionExecutions"][0].update(addedFunds=D("5")),
            "charges_nonzero:positionExecutions.addedFunds",
        ),
        # markup is not part of an opening's identity (Design 4.8), so one execution's markup is a charge only.
        (
            lambda b: b["positionExecutions"][0]["openingData"].update(markup=D("0.1")),
            "charges_nonzero:openingData.markup",
        ),
    ],
)
def test_charge_indicators(mutate: Callable[[dict[str, Any]], Any], reason: str) -> None:
    case = Split()
    mutate(case.body)
    assert reason in reasons_of(case.derive())


def test_markup_and_market_spread() -> None:
    case = Split()
    for e in case.body["positionExecutions"]:
        e["openingData"]["markup"] = D("0.1")
    assert "charges_nonzero:openingData.markup" in reasons_of(case.derive())
    case = Split()
    for e in case.body["positionExecutions"]:
        e["openingData"]["marketSpread"] = D("3")
    assert case.derive().outcome == "admitted"


def test_sync_external_fee_beside_a_zero_lookup_fee() -> None:
    case = Held(totalExternalFees=D("0.5"))
    assert reasons_of(case.derive([found(lookup_body([execution(21, "open", "1")]))])) == [
        "charges_nonzero:totalExternalFees"
    ]


@pytest.mark.parametrize("stored_fee", [D("5"), D("0")])
def test_total_fees_in_the_stored_row_or_an_amendment(stored_fee: Decimal) -> None:
    case = Held(totalFees=stored_fee)
    case.observations.append(observation(case.open, "amendment", sync_payload(21, totalFees=D("5") - stored_fee)))
    assert reasons_of(case.derive()) == ["source_amended", "charges_nonzero:totalFees"]


def test_opening_units_and_rate_conflicts() -> None:
    case = Held(initialUnits=D("9"))
    case.held[21] = held(21, units="10", amount="1000")
    body = lookup_body([execution(21, "open", "10", units=D("10"))])
    assert "opening_units_conflict" in reasons_of(case.derive([found(body)]))
    case = Held(openRate=D("200"))
    assert "opening_rate_conflict" in reasons_of(case.derive([found(lookup_body([execution(21, "open", "1")]))]))


def test_units_and_investment_conservation() -> None:
    case = Held()
    case.held[21] = held(21, units="0.9")
    assert "units_not_conserved" in reasons_of(case.derive())
    case = Held()
    case.held[21] = held(21, amount="99")
    assert "investment_not_conserved" in reasons_of(case.derive())


def test_consideration_conflict_and_implausible() -> None:
    case = Split()
    case.close_b.raw_payload["initialInvestment"] = D("101")  # type: ignore[index]
    assert "consideration_conflict" in reasons_of(case.derive())
    case = Held(initialAmountInDollars=D("150"))
    case.held[21] = held(21, amount="150")
    assert reasons_of(case.derive()) == ["consideration_implausible"]


def test_a_no_lookup_order_with_closes() -> None:
    case = Held()
    close = event("close", 21, history_row(21, "1", CLOSED_A, investment=D("100")), executed_at=CLOSED_A)
    case.events.append(close)
    case.held[21] = None
    assert reasons_of(case.derive()) == ["pricing_currency_unestablished"]


def test_proceeds_implausible_and_out_of_range() -> None:
    case = Split()
    case.close_a.raw_payload["closeRate"] = D("200")  # type: ignore[index]
    assert reasons_of(case.derive()) == ["proceeds_implausible"]
    case = Split()
    big = D("9999999999999999.9999")
    case.close_a.raw_payload.update(investment=big, netProfit=big)  # type: ignore[attr-defined]
    assert "invalid_field:proceeds" in reasons_of(case.derive())


def test_unresolved_app_instrument() -> None:
    case = Held()
    case.events[0] = event("open", 21, case.payload, source="etoro_sync", instrument_id=None)
    assert reasons_of(case.derive()) == ["instrument_unresolved"]


def test_too_many_decimal_places_fails_rather_than_rounds() -> None:
    case = Held(openConversionRate=D("1.00000000001"))
    assert "invalid_field:openConversionRate" in reasons_of(case.derive())


def test_several_failures_are_all_recorded_and_dependent_checks_skipped() -> None:
    case = Held(leverage=2, totalFees=D("1"))
    del case.payload["initialUnits"]
    result = reasons_of(case.derive())
    assert result == ["missing_field:initialUnits", "unsupported_shape:leverage", "charges_nonzero:totalFees"]


def test_a_float_payload_is_a_loader_defect() -> None:
    case = Held(totalFees=0.0)
    with pytest.raises(TypeError, match="float"):
        case.derive()


# --- amendments of the ledger and unscoped observations ---------------------------------------------------------


def test_amendment_of_a_history_close() -> None:
    case = Split()
    case.observations.append(
        observation(case.close_a, "amendment", history_row(11, "0.5", CLOSED_A, investment=D("49")))
    )
    assert "source_amended" in reasons_of(case.derive())


RE_RENDERED = "2026-09-23T13:30:00.503Z"


def test_a_sync_open_re_rendered_by_milliseconds_is_not_an_amendment() -> None:
    case = Held()
    case.observations.append(observation(case.open, "amendment", sync_payload(21, openDateTime=RE_RENDERED)))
    result = case.derive()
    assert (result.outcome, result.execution_evidence) == ("admitted", "sync_only")
    assert [lot.executed_at for lot in result.lots] == [OPENED_AT]


def test_a_re_rendering_beside_a_lifecycle_change_is_an_amendment() -> None:
    # No re-rendering beside an isPartiallyAltered change has been observed (fact 19), so v2 does not waive it.
    case = Held()
    case.observations.append(
        observation(case.open, "amendment", sync_payload(21, openDateTime=RE_RENDERED, isPartiallyAltered=True))
    )
    assert reasons_of(case.derive()) == ["source_amended", "executions_unestablished"]


def test_a_re_rendered_cfd_order_without_a_lookup_is_excluded() -> None:
    case = Held(settlementTypeID=0)
    case.observations.append(
        observation(case.open, "amendment", sync_payload(21, settlementTypeID=0, openDateTime=RE_RENDERED))
    )
    result = case.derive()
    assert (result.outcome, result.product) == ("excluded_non_share_product", "cfd")


def test_re_renderings_spanning_a_second_are_an_amendment() -> None:
    case = Held()
    for moment in ("2026-09-23T13:30:00.9Z", "2026-09-23T13:30:01.5Z"):
        case.observations.append(observation(case.open, "amendment", sync_payload(21, openDateTime=moment)))
    assert reasons_of(case.derive()) == ["source_amended"]


def test_a_re_rendering_beside_another_change_is_an_amendment() -> None:
    case = Held()
    case.observations.append(
        observation(case.open, "amendment", sync_payload(21, openDateTime=RE_RENDERED, mirrorID=7))
    )
    assert "source_amended" in reasons_of(case.derive())


def test_a_malformed_nested_sync_field_beside_a_re_rendering_fails_its_field_check() -> None:
    case = Held(mirrorID={"x": D("1.2")})
    case.observations.append(
        observation(case.open, "amendment", sync_payload(21, mirrorID={"x": D("1.2")}, openDateTime=RE_RENDERED))
    )
    assert reasons_of(case.derive()) == ["invalid_field:mirrorID"]


def test_a_re_rendering_across_london_midnight_is_a_date_conflict() -> None:
    case = Held(openDateTime="2026-09-23T22:59:59.999Z")  # 23:59:59.999 BST
    case.observations.append(
        observation(case.open, "amendment", sync_payload(21, openDateTime="2026-09-23T23:00:00.002Z"))
    )
    assert reasons_of(case.derive()) == ["source_amended", "acquisition_date_conflict"]


@pytest.mark.parametrize("with_lookup", [False, True])
def test_a_close_inside_the_re_rendering_span_is_before_the_open(with_lookup: bool) -> None:
    case = Held()
    case.observations.append(observation(case.open, "amendment", sync_payload(21, openDateTime=RE_RENDERED)))
    inside = OPENED_AT + timedelta(milliseconds=1)
    case.events.append(event("close", 21, history_row(21, "0.5", inside), units="0.5", executed_at=inside))
    lookups = [found(lookup_body([execution(21, "open", "0.5")]))] if with_lookup else None
    assert "close_before_open" in reasons_of(case.derive(lookups))


def test_a_history_first_open_takes_its_time_from_scoped_sync_evidence_only() -> None:
    history_open = event("open", 21, history_row(21, "1", CLOSED_A))
    case = Held()
    case.open, case.events = history_open, [history_open]
    case.observations += [
        observation(None, "unscoped", sync_payload(21, openDateTime="2026-09-23T13:30:00.497Z"), position_id=21),
        observation(history_open, "supplemental", sync_payload(21), source="etoro_sync"),
    ]
    result = case.derive()
    assert (result.outcome, [lot.executed_at for lot in result.lots]) == ("admitted", [OPENED_AT])


def test_an_unscoped_open_marking_the_position_altered_is_unestablished() -> None:
    case = Held()
    case.observations.append(observation(None, "unscoped", sync_payload(21, isPartiallyAltered=True), position_id=21))
    assert reasons_of(case.derive()) == ["executions_unestablished"]


def test_an_unscoped_re_rendering_across_london_midnight_is_a_conflict() -> None:
    case = Held(openDateTime="2026-09-23T22:59:59.999Z")
    late = "2026-09-23T23:00:00.002Z"
    case.observations.append(observation(None, "unscoped", sync_payload(21, openDateTime=late), position_id=21))
    assert reasons_of(case.derive()) == ["unscoped_conflict", "acquisition_date_conflict"]


def test_an_unscoped_re_rendering_is_not_a_conflict_but_a_second_is() -> None:
    case = Held()
    case.observations.append(observation(None, "unscoped", sync_payload(21, openDateTime=RE_RENDERED), position_id=21))
    assert case.derive().outcome == "admitted"

    case = Held()
    late = "2026-09-23T13:30:01.5Z"
    case.observations.append(observation(None, "unscoped", sync_payload(21, openDateTime=late), position_id=21))
    assert reasons_of(case.derive()) == ["unscoped_conflict"]


def test_a_history_open_timestamp_re_rendering_is_still_an_amendment() -> None:
    case = Split()
    case.observations.append(
        observation(case.close_a, "amendment", history_row(11, "0.5", CLOSED_A, openTimestamp=RE_RENDERED))
    )
    assert "source_amended" in reasons_of(case.derive())


def test_unscoped_conflict_unmatched_and_identical() -> None:
    case = Held()
    case.observations.append(observation(None, "unscoped", sync_payload(21, totalFees=D("5")), position_id=21))
    assert "unscoped_conflict" in reasons_of(case.derive())

    case = Held()
    case.observations.append(observation(None, "unscoped", sync_payload(21), position_id=21))
    assert case.derive().outcome == "admitted"

    case = Split()
    elsewhere = datetime(2026, 9, 26, tzinfo=UTC)
    case.observations.append(
        observation(
            None,
            "unscoped",
            history_row(11, "0.5", elsewhere),
            source="etoro_history",
            position_id=11,
            event_kind="close",
            executed_at=elsewhere,
        )
    )
    assert "unscoped_unmatched" in reasons_of(case.derive())

    case = Split()  # a sync payload for a history-only position: no same-source baseline
    case.observations.append(observation(None, "unscoped", sync_payload(11), position_id=11))
    assert "unscoped_unmatched" in reasons_of(case.derive())


# --- partition --------------------------------------------------------------------------------------------------


def test_partition_puts_every_event_in_exactly_one_subject() -> None:
    case = Split()
    no_id = history_row(13, "1", CLOSED_A)
    del no_id["orderId"]
    lacking = event("open", 11, history_row(11, "0.5", CLOSED_A), order_id=None)
    lacking.raw_payload.pop("orderId")  # type: ignore[attr-defined]
    stray = event("open", 13, no_id, order_id=None)
    other = history_row(14, "1", CLOSED_A)
    conflicted = [event("open", 14, other), event("close", 14, {**other, "orderId": 2000}, order_id=2000)]
    second_order = event("open", 15, {**history_row(15, "1", CLOSED_A), "orderId": 2000}, order_id=2000)
    events = [lacking, case.open_b, case.close_a, case.close_b, stray, *conflicted, second_order]
    orphan = HeldPosition(16, D("1"), D("100"), {"CID": ACCOUNT, "orderID": 3000})
    joined_orphan = HeldPosition(17, D("1"), D("100"), {"CID": ACCOUNT, "orderID": ORDER})
    subjects, unattached = partition(events, [], {16: orphan, 17: joined_orphan})
    by_subject = {s.subject: s for s in subjects}
    assert sorted(by_subject) == [f"order:{ORDER}", "order:2000", "position:13", "position:14", "position:16"]
    assert by_subject["position:13"].reasons == ["missing_field:order_id"]
    assert by_subject["position:14"].reasons == ["identity_conflict:order_id"]
    assert by_subject["position:16"].reasons == ["orphan_position"]
    assert set(by_subject[f"order:{ORDER}"].reasons) == {"position_quarantined", "orphan_position"}
    assert by_subject["order:2000"].reasons == ["position_quarantined"]
    seen = [e.event_id for s in subjects for e in s.events]
    assert sorted(seen) == sorted(e.event_id for e in events) and unattached == []

    derived = {s.subject: s for s in derive_all(SCOPE, events, [], {}, {16: orphan, 17: joined_orphan})}
    assert "missing_field:order_id" in derived[f"order:{ORDER}"].reasons
    assert all(s.outcome == "unresolved" for k, s in derived.items() if k.startswith("position:"))


def test_unscoped_observations_attach_by_position_then_order_then_nothing() -> None:
    case = Held()
    by_position = observation(None, "unscoped", sync_payload(21), position_id=21)
    by_order = observation(None, "unscoped", sync_payload(99), position_id=99)
    nowhere = observation(None, "unscoped", sync_payload(98, orderID=5555), position_id=98)
    subjects, unattached = partition(case.events, [by_position, by_order, nowhere], {})
    (subject,) = subjects
    assert subject.observations == [by_position, by_order] and unattached == [nowhere]
    (derived,) = derive_all(SCOPE, case.events, [by_position, by_order, nowhere], {}, {21: held(21)})
    assert derived.reasons == ("unscoped_unmatched",)


# --- arithmetic, sterling, fingerprint ----------------------------------------------------------------------------

SEED = parse_bank_holidays(Path("docs/research/3616-slice3b-inputs/bank-holidays_2026-10-13.json").read_bytes())


def _fx_state(observations: dict[date, Decimal], *, healthy: bool = True) -> TaxFxState:
    return TaxFxState(
        healthy, "applied" if healthy else None, TAX_FX_POLICY_ID, 7, observations, SEED, tuple(sorted(observations))
    )


def _draft(**over: Any) -> LotDraft:
    fields: dict[str, Any] = {
        "lot_key": "acq:demo:1000",
        "subject": "order:1000",
        "order_id": 1000,
        "instrument_id": 7,
        "etoro_instrument_id": 1001,
        "direction": "acquisition",
        "position_ids": (12, 11),
        "trade_event_ids": (4, 3),
        "executed_at": OPENED_AT,
        "units": D("1.50000000"),
        "usd_amount": D("100.10"),
        "execution_evidence": "lookup",
    }
    fields.update(over)
    return LotDraft(**fields)


def test_sterling_published_carried_forward_and_pending() -> None:
    state = _fx_state({date(2026, 9, 25): D("1.3000"), date(2026, 9, 28): D("1.3100")})
    lots = price_lots(
        [
            _draft(executed_at=datetime(2026, 9, 25, 12, tzinfo=UTC)),
            _draft(lot_key="b", executed_at=datetime(2026, 9, 27, 12, tzinfo=UTC)),
            _draft(lot_key="c", executed_at=datetime(2026, 10, 2, 12, tzinfo=UTC)),
        ],
        state,
    )
    assert not isinstance(lots, FxRefusal)
    assert [(lot.fx_status, lot.fx_observation_date) for lot in lots] == [
        ("published", date(2026, 9, 25)),
        ("carried_forward", date(2026, 9, 25)),
        ("pending", None),
    ]
    # 1/1.3 does not terminate: 3b rounds it outside the trapped context, and the product is exact.
    assert lots[0].fx_gbp_per_usd == D("0.7692307692")
    assert lots[0].amount_gbp == D("100.10") * D("0.7692307692") and lots[2].amount_gbp is None


def test_sterling_unavailable_refuses_the_run() -> None:
    refusal = price_lots([_draft()], _fx_state({date(2026, 9, 25): D("1.3")}, healthy=False))
    assert refusal == FxRefusal(reason="fx_unavailable", lot_key="acq:demo:1000")


def test_extreme_operands_are_exact_and_a_rounding_raises() -> None:
    money, rate = D("9999999999999999.9999"), D("9999999999.9999999999")
    state = _fx_state({date(2026, 9, 23): D("1")})
    (lot,) = price_lots([_draft(usd_amount=money)], state)  # type: ignore[misc]
    assert lot.amount_gbp == money
    with evidence_context():
        assert money * rate == D(99999999999999999999 * 99999999999999999999).scaleb(-14)
        with pytest.raises(Inexact):
            _ = D(1) / D(3)


FIXED_BYTES = (
    b'{"amount_gbp":"76.99999999692","canonical_version":1,"consideration_status":"broker_stated_unreconciled",'
    b'"direction":"acquisition","etoro_instrument_id":1001,"evidence_policy_id":"broker_evidence_v2",'
    b'"executed_at":"2026-09-23T13:30:00.500000Z","execution_evidence":"lookup","fx_gbp_per_usd":"0.7692307692",'
    b'"fx_observation_date":"2026-09-23","fx_policy_id":"'
    + TAX_FX_POLICY_ID.encode()
    + b'","fx_rate_date":"2026-09-23",'
    b'"fx_status":"published","fx_usd_per_gbp":"1.3","instrument_id":7,"lot_key":"acq:demo:1000","order_id":1000,'
    b'"position_ids":[11,12],"subject":"order:1000","tax_year":"2026/27","trade_event_ids":[3,4],'
    b'"uk_date":"2026-09-23","units":"1.5","usd_amount":"100.1"}'
)


def _fixed_lot() -> EvidenceLot:
    (lot,) = price_lots([_draft()], _fx_state({date(2026, 9, 23): D("1.3000")}))  # type: ignore[misc]
    return lot


def test_fingerprint_byte_vector() -> None:
    lot = _fixed_lot()
    assert fingerprint_bytes(lot) == FIXED_BYTES
    assert lot.fingerprint == hashlib.sha256(FIXED_BYTES).hexdigest()
    assert lot.fingerprint == "5aa639db6932325664ee1a7cc83687280a676af02f1ef7f5094c9dbea4630a61"


def test_fingerprint_is_stable_under_reordering_and_excludes_run_ids() -> None:
    lot = _fixed_lot()
    reordered = price_lots(
        [_draft(position_ids=(11, 12), trade_event_ids=(3, 4))], _fx_state({date(2026, 9, 23): D("1.3")})
    )
    other_run = TaxFxState(
        True, "applied", TAX_FX_POLICY_ID, 99, {date(2026, 9, 23): D("1.3")}, SEED, (date(2026, 9, 23),)
    )
    (moved,) = price_lots([_draft()], other_run)  # type: ignore[misc]
    assert reordered[0].fingerprint == lot.fingerprint == moved.fingerprint  # type: ignore[index]
    assert moved.fx_run_id == 99
    assert lot_fingerprint(lot, "broker_evidence_v1") != lot.fingerprint
    assert b"run_id" not in fingerprint_bytes(lot) and b"fingerprint" not in fingerprint_bytes(lot)


def test_a_failing_found_body_is_a_field_failure() -> None:
    body = lookup_body([execution(11)])
    assert lookup_field_failures(body) == []
    body["asset"]["settlementType"] = "SPACESHIP"
    assert lookup_field_failures(body) == ["invalid_field:asset.settlementType"]
    cfd = lookup_body([execution(11)], totalCosts=D("-1"))
    cfd["asset"]["settlementType"] = "CFD"
    assert lookup_field_failures(cfd) == []  # real fields are not required of a CFD body


def test_held_positions_are_recorded_per_position() -> None:
    case = Split()
    result = case.derive()
    assert result.held_positions == {11: None, 12: None}
    assert Held().derive().held_positions == {21: held(21)}


def test_the_acquisition_time_comes_from_the_lookup() -> None:
    case = Split()
    later = _iso(OPENED_AT + timedelta(seconds=1))
    for e in case.body["positionExecutions"]:
        e["openingData"]["executionTime"] = later
    acquisition = case.derive().lots[0]
    assert acquisition.executed_at == OPENED_AT + timedelta(seconds=1)


def test_a_rerendered_timestamp_or_a_field_outside_the_table_is_not_an_amendment() -> None:
    case = Split()
    later = copy.deepcopy(case.body)
    for e in later["positionExecutions"]:
        e["openingData"]["executionTime"] = "2026-09-23T13:30:00.500+00:00"
        e["openingData"]["priceId"] = 42
    assert case.derive(case.body, later).outcome == "admitted"


def test_a_history_first_open_with_a_supplemental_sync_observation_and_no_lookup() -> None:
    payload = history_row(21, "1", CLOSED_A)
    opened = event("open", 21, payload)
    obs = observation(opened, "supplemental", sync_payload(21), source="etoro_sync")
    result = derive_order(SCOPE, ORDER, [opened], [obs], [], {21: held(21)})
    assert (result.outcome, result.execution_evidence) == ("admitted", "sync_only")
    assert result.lots[0].executed_at == OPENED_AT


def test_an_observation_naming_another_instrument() -> None:
    case = Split()
    case.observations.append(
        observation(case.open_a, "supplemental", sync_payload(11, instrumentID=5), source="etoro_sync")
    )
    assert "identity_conflict:instrument_id" in reasons_of(case.derive())


def test_an_orphan_holding_joined_to_an_order_is_recorded() -> None:
    case = Held()
    orphan = held(22)
    (result,) = derive_all(SCOPE, case.events, [], {}, {21: held(21), 22: orphan})
    assert result.reasons == ("orphan_position",) and result.held_positions == {21: held(21), 22: orphan}


def test_an_extreme_exponent_is_an_invalid_field() -> None:
    case = Split()
    case.body["totalCosts"] = D("1e1000000")
    assert "invalid_field:totalCosts" in reasons_of(case.derive())
    tiny = {**Split().body, "totalCosts": parse_lookup_body("1e-1000000")}
    assert lookup_field_failures(tiny) == ["invalid_field:totalCosts"]


def test_an_enum_in_another_case_is_not_an_amendment() -> None:
    case = Split()
    later = copy.deepcopy(case.body)
    later["asset"]["settlementType"] = "real"
    later["status"]["name"] = "FILLED"
    assert case.derive(case.body, later).outcome == "admitted"


def test_a_sync_only_amendment_across_london_dates_is_a_date_conflict() -> None:
    # 23:30 UTC on 2026-09-23 is 00:30 on the 24th in London (BST).
    case = Held()
    case.observations.append(observation(case.open, "amendment", sync_payload(21, openDateTime="2026-09-23T23:30:00Z")))
    assert reasons_of(case.derive()) == ["source_amended", "acquisition_date_conflict"]
