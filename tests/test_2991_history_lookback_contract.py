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
upper_bound_parameter`` fails the moment a re-pinned document carries an upper-bound
parameter — at which point windowing becomes implementable and #2991's fix must be built.

⚠ Self-invalidating on a RE-PIN, not on the portal. Every input here is a committed file,
so an upstream change trips nothing until someone runs
``scripts/refresh_2946_openapi_census.py`` and the document changes underneath these
assertions. That is the same bound the #2946 census carries and is deliberate — a test
that fetched the portal would fail in CI and train people to skip it.

⚠ Note too that an upper-bound parameter appearing would not by itself prove an ancient
``minDate`` becomes permitted: the documented rule is about a request's DURATION, and
whether old data stays reachable at all is a separate question the document does not
answer.

⚠ What this does NOT establish: that old rows are never silently dropped. The only close
this account has ever RETURNED is 2025-11-14, inside a compliant window — and "the only
one it has" is precisely what is in question, so it cannot be assumed. What the
observation set does show is that the 2026 portion of the shortfall against
``getClosedPositionEventsHistory`` appears in the COMPLIANT arm too, where no lookback
rule can bite; that part of the gap is therefore not a lookback effect. The rest of the
gap, and the counter's scope, are open (#2993).
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
def spec() -> dict[str, Any]:
    return json.loads(pathlib.Path(SOURCE_FIXTURE).read_text())


@pytest.fixture(scope="module")
def operations(spec: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return load_operations(spec)


def _effective_parameters(spec: dict[str, Any], key: str) -> list[dict[str, Any]]:
    """Operation parameters PLUS the path item's, which an operation inherits.

    ⚠ ``load_operations`` returns the operation object alone, so reading
    ``op["parameters"]`` misses anything declared at the path-item level. OpenAPI 3 says
    a path-level parameter applies to every operation under that path unless overridden
    by name+location — so a ``maxDate`` added there is fully in effect and invisible to
    an operation-only read. Verified by injection, not by reasoning: see
    ``test_the_parameter_guard_sees_a_path_level_parameter``.
    """
    verb, path = key.split(" ", 1)
    item = spec["paths"][path]
    inherited = [p for p in item.get("parameters", []) if isinstance(p, dict)]
    own = list(item[verb.lower()].get("parameters", []))
    overridden = {(p.get("name"), p.get("in")) for p in own}
    return own + [p for p in inherited if (p.get("name"), p.get("in")) not in overridden]


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
def test_the_min_date_parameter_does_not_repeat_the_rule(key: str, spec: dict[str, Any]) -> None:
    """A reader checking the parameter alone would miss the constraint entirely."""
    min_date = next(p for p in _effective_parameters(spec, key) if p["name"] == "minDate")
    assert "lookback" not in min_date["description"]
    assert min_date["required"] is True


@pytest.mark.parametrize("key", _HISTORY_OPERATIONS)
def test_neither_history_operation_exposes_an_upper_bound_parameter(key: str, spec: dict[str, Any]) -> None:
    """#2991's windowing is unimplementable while this holds — and buildable the moment it fails."""
    names = {p["name"] for p in _effective_parameters(spec, key)}
    assert names == _EXPECTED_PARAMETERS, (
        f"{key}'s parameter set changed to {sorted(names)}. ⚠ This asserts the WHOLE set, not "
        "the absence of two names, so any added parameter trips it — deliberately, because a "
        "new parameter on this operation is worth reading whatever it is called. If an "
        "upper-bound date parameter has appeared under any name, #2991's rebuttal has expired: "
        "the range can now be tiled and get_trade_history must window it."
    )


def test_the_parameter_guard_sees_a_path_level_parameter(spec: dict[str, Any]) -> None:
    """The guard above is only worth anything if inheritance cannot hide the parameter.

    Injects ``maxDate`` at the path-item level — where an operation-only read does not
    look — and asserts it surfaces. This is the test for the test, and it exists because
    the first version of the guard passed with exactly this injection in place.
    """
    key = _HISTORY_OPERATIONS[0]
    path = key.split(" ", 1)[1]
    mutated = json.loads(json.dumps(spec))
    mutated["paths"][path]["parameters"] = [
        {"name": "maxDate", "in": "query", "required": False, "schema": {"type": "string", "format": "date"}}
    ]
    assert "maxDate" in {p["name"] for p in _effective_parameters(mutated, key)}


def test_the_deep_backfill_min_date_exceeds_the_documented_maximum_by_design(probe: dict[str, Any]) -> None:
    """``HISTORY_EPOCH`` knowingly breaches the documented lookback, and must keep doing so.

    It cannot comply: with no upper-bound parameter, a compliant request can only ever
    reach back 364 days, and ledger §4's synthesized-open transform needs the WHOLE
    account lifetime in one batch.
    """
    observed_at = datetime.fromisoformat(probe["_meta"]["observed_at"])
    assert observed_at - HISTORY_EPOCH > _DOCUMENTED_MAX_LOOKBACK


def test_the_deep_backfill_arm_measured_the_min_date_this_repo_actually_sends(probe: dict[str, Any]) -> None:
    """Binds the recorded arm to ``HISTORY_EPOCH``.

    ⚠ Without this every assertion below is satisfiable by an arm that asked for
    something else entirely — a fixture recording yesterday's date would still pass a
    test that only reads ``exceeds_documented_max``.
    """
    arm = _arm(probe, "A_epoch_deep_backfill")
    assert datetime.fromisoformat(arm["min_date"]) == HISTORY_EPOCH


def test_a_request_beyond_the_documented_maximum_was_accepted(probe: dict[str, Any]) -> None:
    """Measured, not assumed: eToro served the deep-backfill request."""
    arm = _arm(probe, "A_epoch_deep_backfill")
    # Recomputed from the recorded timestamps rather than read off the arm's own derived
    # flags, which would be the fixture agreeing with itself.
    lookback = datetime.fromisoformat(arm["requested_at"]) - datetime.fromisoformat(arm["min_date"])
    assert lookback > _DOCUMENTED_MAX_LOOKBACK
    assert arm["exceeds_documented_max"] is True
    assert arm["outcome"] == "ok", f"the deep-backfill request now fails: {arm}"


def test_a_request_beyond_the_documented_maximum_returned_a_close_far_outside_a_one_year_cap(
    probe: dict[str, Any],
) -> None:
    """Falsifies "the server caps each response at ``minDate`` + 1 year".

    Under that model the arm would have returned only closes within a year of 2017-01-01,
    of which this account has returned none, so it would have come back empty. It did not.
    """
    arm = _arm(probe, "A_epoch_deep_backfill")
    assert arm["row_count"] > 0
    span = datetime.fromisoformat(arm["max_close_timestamp"]) - datetime.fromisoformat(arm["min_date"])
    assert span > _DOCUMENTED_MAX_LOOKBACK


def test_the_closed_event_counter_arm_is_recorded_and_disagrees(probe: dict[str, Any]) -> None:
    """The disagreement #2993 is about must stay in the evidence, not be quietly dropped.

    ⚠ Asserts only that the two numbers differ. It asserts NOTHING about which is right:
    the counter's scope is undetermined, so this is a recorded disagreement and not a
    completeness verdict.
    """
    counter = _arm(probe, "C_closed_event_counts")
    assert counter["outcome"] == "ok"
    assert counter["total_closed_position_events"] == sum(c["closedPositionEvents"] for c in counter["counts"])
    assert counter["total_closed_position_events"] != _arm(probe, "A_epoch_deep_backfill")["row_count"]


def test_the_compliant_control_arm_returned_the_same_slices(probe: dict[str, Any]) -> None:
    """The control sits inside the documented maximum, so nothing there can be truncated."""
    deep = _arm(probe, "A_epoch_deep_backfill")
    compliant = _arm(probe, "B_within_documented_max")
    assert compliant["outcome"] == "ok"
    assert compliant["exceeds_documented_max"] is False
    # Strictly inside, not on the bound: an arm sitting exactly on 364 days is already
    # over it by the time the gateway reads it, so it could fail for the same reason as
    # the arm it controls.
    lookback = datetime.fromisoformat(compliant["requested_at"]) - datetime.fromisoformat(compliant["min_date"])
    assert lookback < _DOCUMENTED_MAX_LOOKBACK
    # Per-slice, not per-position: a partial close reduces the same positionId, so equal
    # id sets do not establish equal rows.
    assert compliant["slices"] == deep["slices"]
