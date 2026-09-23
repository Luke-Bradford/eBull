"""The whole one-shot close verdict (#2965) — pure, table-driven.

The field shapes are the broker's own, copied from the attended 2026-09-23 session
(``tests/fixtures/etoro/attended_2026-09-23-01_partial_close.jsonl``) and the live
``etoro_sync`` open row: the snapshot uses ``positionID``/``orderID``/``instrumentID``,
history uses ``positionId``/``orderId``/``instrumentId``.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from app.services.broker_closed_release import (
    RELEASE_REASON,
    WholeCloseEvidence,
    evaluate_whole_close,
)

POSITION = 3602886855
ORDER = 383320185
INSTRUMENT = 1001
OPENED = datetime(2026, 9, 23, 0, 5, 2, 483000, tzinfo=UTC)
CLOSED = datetime(2026, 9, 23, 0, 7, 50, 100000, tzinfo=UTC)
OBSERVED = datetime(2026, 9, 23, 1, 0, tzinfo=UTC)


def _open(**raw: Any) -> dict[str, Any]:
    payload = {
        "positionID": POSITION,
        "orderID": ORDER,
        "instrumentID": INSTRUMENT,
        "isBuy": True,
        "leverage": 1,
        "units": 0.147071,
        "initialUnits": 0.147071,
        "isPartiallyAltered": False,
        "initialAmountInDollars": 50.0,
        "openDateTime": "2026-09-23T00:05:02.483Z",
    }
    payload.update(raw)
    return {
        "position_id": POSITION,
        "event_kind": "open",
        "side": "buy",
        "source": "etoro_sync",
        "units": Decimal("0.14707100"),
        "executed_at": OPENED,
        "order_id": None,
        "instrument_id": INSTRUMENT,
        "etoro_instrument_id": INSTRUMENT,
        "realized_pnl_usd": None,
        "raw_payload": payload,
    }


def _close(**raw: Any) -> dict[str, Any]:
    payload = {
        "positionId": POSITION,
        "orderId": ORDER,
        "instrumentId": INSTRUMENT,
        "isBuy": True,
        "leverage": 1,
        "units": 0.147071,
        "investment": 50.0,
        "initialInvestment": 50.0,
        "openTimestamp": "2026-09-23T00:05:02.483Z",
        "closeTimestamp": "2026-09-23T00:07:50.1Z",
    }
    payload.update(raw)
    return {
        "position_id": POSITION,
        "event_kind": "close",
        "side": "sell",
        "source": "etoro_history",
        "units": Decimal("0.14707100"),
        "executed_at": CLOSED,
        "order_id": ORDER,
        "instrument_id": INSTRUMENT,
        "etoro_instrument_id": INSTRUMENT,
        "realized_pnl_usd": Decimal("-0.0100"),
        "raw_payload": payload,
    }


def _evidence(**overrides: Any) -> WholeCloseEvidence:
    base = WholeCloseEvidence(
        broker_position_id=POSITION,
        instrument_id=INSTRUMENT,
        entry_broker_order_refs=[str(ORDER)],
        open_rows=[_open()],
        close_rows=[_close()],
        sibling_close_count=0,
        blocking_operation_count=0,
        entry_reconciliation_states=["resolved"],
    )
    return replace(base, **overrides)


def test_a_whole_one_shot_close_releases_at_the_broker_close_time() -> None:
    verdict = evaluate_whole_close(_evidence(), observed_at=OBSERVED)
    assert verdict.release is True
    assert verdict.reason_code == RELEASE_REASON
    assert verdict.released_at == CLOSED


def test_a_snapshot_timestamp_in_another_offset_is_the_same_opening() -> None:
    shifted = _close(openTimestamp="2026-09-23T01:05:02.483+01:00")
    assert evaluate_whole_close(_evidence(close_rows=[shifted]), observed_at=OBSERVED).release is True


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        # The attended partial close's shape: the slice lives under a NEW id, same order.
        ({"sibling_close_count": 1}, "partial_close_slice_present"),
        ({"close_rows": []}, "close_witness_not_unique"),
        ({"close_rows": [_close(), _close()]}, "close_witness_not_unique"),
        ({"open_rows": []}, "open_witness_not_unique"),
        ({"blocking_operation_count": 1}, "operation_unresolved"),
        ({"entry_reconciliation_states": ["pending"]}, "entry_not_resolved"),
        ({"entry_reconciliation_states": [None]}, "entry_not_resolved"),
        ({"entry_broker_order_refs": []}, "entry_reference_not_unique"),
        (
            {"entry_broker_order_refs": ["1", "2"], "entry_reconciliation_states": ["resolved", "resolved"]},
            "entry_reference_not_unique",
        ),
        ({"entry_broker_order_refs": [None]}, "entry_reference_invalid"),
        ({"entry_broker_order_refs": ["38x"]}, "entry_reference_invalid"),
        ({"entry_broker_order_refs": ["999"]}, "open_witness_identity_mismatch"),
        ({"open_rows": [{**_open(), "source": "etoro_history"}]}, "open_witness_not_snapshot"),
        ({"open_rows": [_open(positionID=1)]}, "open_witness_identity_mismatch"),
        ({"open_rows": [_open(instrumentID=2)]}, "open_witness_identity_mismatch"),
        ({"open_rows": [_open(isBuy=False)]}, "open_witness_not_unleveraged_long"),
        ({"open_rows": [_open(leverage=2)]}, "open_witness_not_unleveraged_long"),
        ({"open_rows": [_open(isPartiallyAltered=True)]}, "open_witness_partially_altered"),
        ({"open_rows": [_open(isPartiallyAltered=None)]}, "open_witness_partially_altered"),
        ({"open_rows": [_open(initialUnits=0.2)]}, "open_witness_not_whole"),
        ({"open_rows": [_open(units=0.073535)]}, "open_witness_not_whole"),
        ({"open_rows": [_open(initialUnits="nan")]}, "open_witness_not_whole"),
        ({"open_rows": [_open(initialAmountInDollars=True)]}, "open_witness_malformed"),
        ({"open_rows": [{**_open(), "raw_payload": "not-an-object"}]}, "open_witness_malformed"),
        ({"close_rows": [{**_close(), "source": "etoro_sync"}]}, "close_witness_not_history_sell"),
        ({"close_rows": [{**_close(), "side": "buy"}]}, "close_witness_not_history_sell"),
        ({"close_rows": [{**_close(), "order_id": None}]}, "close_witness_identity_mismatch"),
        ({"close_rows": [_close(orderId=1)]}, "close_witness_identity_mismatch"),
        ({"close_rows": [_close(positionId=3602887089)]}, "close_witness_identity_mismatch"),
        ({"close_rows": [_close(instrumentId=2)]}, "close_witness_identity_mismatch"),
        ({"close_rows": [_close(openTimestamp="2026-09-22T00:05:02.483Z")]}, "close_witness_identity_mismatch"),
        ({"close_rows": [_close(openTimestamp="garbage")]}, "close_witness_identity_mismatch"),
        ({"close_rows": [_close(leverage=5)]}, "close_witness_not_unleveraged_long"),
        # A partial booked under the live id would carry fewer units than the whole.
        ({"close_rows": [{**_close(), "units": Decimal("0.07353500")}]}, "close_not_whole_units"),
        ({"close_rows": [_close(units=0.073535)]}, "close_not_whole_units"),
        ({"close_rows": [_close(investment=25.0)]}, "close_not_whole_investment"),
        ({"close_rows": [_close(investment=25.0, initialInvestment=25.0)]}, "close_not_whole_investment"),
        ({"close_rows": [{**_close(), "executed_at": OBSERVED + timedelta(seconds=1)}]}, "close_witness_out_of_time"),
        ({"close_rows": [{**_close(), "executed_at": OPENED - timedelta(seconds=1)}]}, "close_witness_out_of_time"),
        ({"close_rows": [{**_close(), "realized_pnl_usd": None}]}, "close_witness_unpriced"),
        ({"close_rows": [{**_close(), "realized_pnl_usd": Decimal("NaN")}]}, "close_witness_unpriced"),
    ],
)
def test_every_other_shape_refuses(overrides: dict[str, Any], reason: str) -> None:
    verdict = evaluate_whole_close(_evidence(**overrides), observed_at=OBSERVED)
    assert verdict.release is False
    assert verdict.reason_code == reason
    assert verdict.released_at is None
