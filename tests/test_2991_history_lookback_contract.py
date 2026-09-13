"""The trade-history lookback contract, pinned against eToro's own OpenAPI document (#2991).

Pure logic, offline. Both inputs are committed: eToro's document
(``tests/fixtures/etoro/openapi_v1.375.0.json``) and the demo observation set
(``history_lookback_probe_2026-09-13.json``, written by
``scripts/probe_2991_history_lookback.py``).

WHAT #2991 ASKED FOR, AND WHY IT IS NOT BUILT
---------------------------------------------
#2991 proposed windowing ``get_trade_history``'s range in steps of at most 364 days. The
document does not permit that shape: the operation takes ``x-request-id``, ``minDate``
(required), ``page`` and ``pageSize``, and **no upper-bound date parameter**. Every
request therefore ends at the present, so advancing ``minDate`` forward only ever SHRINKS
the returned set — it cannot tile a range.

The one reading under which "advance ``minDate`` per batch" does tile is that the server
caps each response at ``[minDate, minDate + 1 year]``. ``test_a_request_beyond_the_
documented_maximum_returned_a_close_far_outside_a_one_year_cap`` falsifies that from the
recorded demo observation.

⚠⚠ THE REBUTTAL IS DELIBERATELY SELF-INVALIDATING, which is the whole point of pinning it
here rather than writing it in a comment. ``test_neither_history_operation_exposes_an_
upper_bound_parameter`` fails the moment a re-pinned document adds ``maxDate`` / ``toDate``
— at which point windowing becomes implementable and #2991's fix must actually be built.

⚠ What this does NOT establish: that old rows are never silently dropped. The demo
account's only close is 2025-11-14, inside a compliant 364-day window, so no arm on this
account can decide truncation of a row older than a year. What the observation set does
show is that the shortfall against ``getClosedPositionEventsHistory`` appears identically
in the COMPLIANT arm, so it is not a lookback effect either way (see #2993).
"""

from __future__ import annotations

import json
import pathlib
from datetime import datetime, timedelta
from typing import Any

import pytest

from app.services.trade_events import HISTORY_EPOCH
from scripts.refresh_2946_openapi_census import SOURCE_FIXTURE, load_operations, operation_key

#: The operation's own wording. Hand-written here on purpose: deriving the expectation
#: from the document would turn every comparison below into an identity test
#: (``docs/review-prevention-log.md``, "a derivation can silently turn a comparison test
#: into an identity test").
_LOOKBACK_SENTENCE = (
    "Keep each request's lookback to less than 1 year (maximum 1 year minus 1 day). "
    "For longer history, split the range into successive windows of at most that length "
    "(advance `minDate` per batch) and use `page` / `pageSize` within each window."
)

#: "1 year minus 1 day", as the sentence states it.
_DOCUMENTED_MAX_LOOKBACK = timedelta(days=364)

_HISTORY_OPERATIONS = (
    operation_key("get", "/api/v1/trading/info/trade/demo/history"),
    operation_key("get", "/api/v1/trading/info/trade/history"),
)

#: Exactly what the operation accepts. An upper-bound parameter appearing here is the
#: trigger to build #2991's windowing.
_EXPECTED_PARAMETERS = frozenset({"x-request-id", "minDate", "page", "pageSize"})

_PROBE_FIXTURE = pathlib.Path(__file__).resolve().parents[1] / (
    "tests/fixtures/etoro/history_lookback_probe_2026-09-13.json"
)


@pytest.fixture(scope="module")
def operations() -> dict[str, dict[str, Any]]:
    return load_operations(json.loads(pathlib.Path(SOURCE_FIXTURE).read_text()))


@pytest.fixture(scope="module")
def probe() -> dict[str, Any]:
    return json.loads(_PROBE_FIXTURE.read_text())


def _arm(probe: dict[str, Any], name: str) -> dict[str, Any]:
    for arm in probe["arms"]:
        if arm["arm"] == name:
            return arm
    raise AssertionError(f"observation set has no arm {name!r}; arms={[a['arm'] for a in probe['arms']]}")


@pytest.mark.parametrize("key", _HISTORY_OPERATIONS)
def test_both_history_operations_state_the_lookback_rule(key: str, operations: dict[str, dict[str, Any]]) -> None:
    """The constraint lives on the OPERATION, not on the ``minDate`` parameter."""
    assert _LOOKBACK_SENTENCE in operations[key]["description"], (
        f"{key}'s description no longer carries the lookback sentence verbatim. Re-read it "
        "before trusting anything in this module — the rule it pins may have changed."
    )


@pytest.mark.parametrize("key", _HISTORY_OPERATIONS)
def test_the_min_date_parameter_does_not_repeat_the_rule(key: str, operations: dict[str, dict[str, Any]]) -> None:
    """A reader checking the parameter alone would miss the constraint entirely."""
    min_date = next(p for p in operations[key]["parameters"] if p["name"] == "minDate")
    assert "lookback" not in min_date["description"]
    assert min_date["required"] is True


@pytest.mark.parametrize("key", _HISTORY_OPERATIONS)
def test_neither_history_operation_exposes_an_upper_bound_parameter(
    key: str, operations: dict[str, dict[str, Any]]
) -> None:
    """#2991's windowing is unimplementable while this holds — and buildable the moment it fails."""
    names = {p["name"] for p in operations[key]["parameters"]}
    assert names == _EXPECTED_PARAMETERS, (
        f"{key}'s parameter set changed to {sorted(names)}. If an upper-bound date parameter "
        "(maxDate/toDate) has appeared, #2991's rebuttal has expired: the range can now be "
        "tiled and get_trade_history must window it."
    )


def test_the_deep_backfill_min_date_exceeds_the_documented_maximum_by_design(probe: dict[str, Any]) -> None:
    """``HISTORY_EPOCH`` knowingly breaches the documented lookback, and must keep doing so.

    It cannot comply: with no upper-bound parameter, a compliant request can only ever
    reach back 364 days, and ledger §4's synthesized-open transform needs the WHOLE
    account lifetime in one batch.
    """
    observed_at = datetime.fromisoformat(probe["_meta"]["observed_at"])
    assert observed_at - HISTORY_EPOCH > _DOCUMENTED_MAX_LOOKBACK


def test_a_request_beyond_the_documented_maximum_was_accepted(probe: dict[str, Any]) -> None:
    """Measured, not assumed: eToro served the ~9.7-year deep-backfill request."""
    arm = _arm(probe, "A_epoch_deep_backfill")
    assert arm["exceeds_documented_max"] is True
    assert arm["lookback_days"] > _DOCUMENTED_MAX_LOOKBACK.days
    assert arm["outcome"] == "ok", f"the deep-backfill request now fails: {arm}"


def test_a_request_beyond_the_documented_maximum_returned_a_close_far_outside_a_one_year_cap(
    probe: dict[str, Any],
) -> None:
    """Falsifies "the server caps each response at ``minDate`` + 1 year".

    Under that model the ~9.7-year arm would have returned only 2017-2018 closes and this
    account has none, so it would have come back empty. It did not.
    """
    arm = _arm(probe, "A_epoch_deep_backfill")
    assert arm["row_count"] > 0
    assert arm["max_close_minus_min_date_days"] > _DOCUMENTED_MAX_LOOKBACK.days


def test_the_compliant_control_arm_returned_the_same_rows(probe: dict[str, Any]) -> None:
    """The 364-day arm is the control: within the documented maximum, nothing to truncate."""
    deep = _arm(probe, "A_epoch_deep_backfill")
    compliant = _arm(probe, "B_within_documented_max")
    assert compliant["exceeds_documented_max"] is False
    # Strictly inside, not on the bound: an arm sitting exactly on 364 days is already
    # over it by the time the gateway reads it, so it could fail for the same reason as
    # the arm it controls.
    assert compliant["lookback_days"] < _DOCUMENTED_MAX_LOOKBACK.days
    assert compliant["outcome"] == "ok"
    assert compliant["position_ids"] == deep["position_ids"]
