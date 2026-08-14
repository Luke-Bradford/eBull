"""#2603 item 3 step 3b-1 — the world-facing refusal vocabulary, and its SOURCES.

Pure-logic: no database.  The interesting half of the preflight is a 16-way
precedence order over one observation, so it is tested as a pure function of that
observation rather than through a fixture per pair.

⚠ Two classes of test here and they are not interchangeable:

1. **Precedence and behaviour** — each refusal fires, and fires *ahead of* the ones
   below it.  A test that only asserts "code X is returned for input X" cannot tell
   a correct order from a shuffled one.
2. **Coupling to the source** — each freshness bound still agrees with the
   scheduler cadence it was derived from, and the session allow-list still agrees
   with the one calendar this repo has.  A constant and its source in two files
   with nothing binding them is two constants (step 3a's device).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal, get_args

import pytest

from app.services.strategy_core_preflight import (
    CORE_MAX_HALT_FEED_AGE_SECONDS,
    CORE_MAX_QUOTE_AGE_SECONDS,
    CORE_PREFLIGHT_POLICY_VERSION,
    CorePreflightObservation,
    CorePreflightRefusal,
    StrategyCorePreflightError,
    _freshness_bound,
    decide_core_preflight,
)

#: A Wednesday, 15:00 UTC = 11:00 ET — inside the regular session, not a holiday.
_OPEN = datetime(2026, 8, 12, 15, 0, tzinfo=UTC)

#: The declared precedence, in order.  Imported from the `Literal` rather than
#: re-typed, so a code added to the module without a place in this list fails the
#: completeness test below rather than silently going untested.
_ORDER: tuple[str, ...] = get_args(CorePreflightRefusal)


def _healthy(**overrides: Any) -> CorePreflightObservation:
    """An observation that is admitted, so every test states only its own defect."""
    base: dict[str, Any] = {
        "instrument_present": True,
        "symbol": "IVV",
        "is_tradable": True,
        "asset_class": "us_equity",
        "kill_switch_active": False,
        "execution_blocked": False,
        "is_halted": False,
        "halt_feed_at": _OPEN - timedelta(seconds=60),
        "quoted_at": _OPEN - timedelta(seconds=60),
        "bid": Decimal("100.00"),
        "ask": Decimal("100.02"),
        "spread_flag": False,
    }
    base.update(overrides)
    return CorePreflightObservation(**base)


def _decide(
    observation: CorePreflightObservation,
    *,
    action: Literal["buy_core", "sell_core"] = "buy_core",
    now: datetime = _OPEN,
) -> Any:
    return decide_core_preflight(observation, core_instrument_id=3138, action=action, now=now)


# --------------------------------------------------------------------------- #
# The admitted case, and the side selection that depends on the action
# --------------------------------------------------------------------------- #


def test_a_healthy_observation_is_admitted_and_carries_no_reason() -> None:
    verdict = _decide(_healthy())
    assert verdict.admitted is True
    assert verdict.reason_code is None
    assert verdict.policy_version == CORE_PREFLIGHT_POLICY_VERSION


def test_the_returned_price_is_the_side_the_action_actually_trades() -> None:
    """A buy lifts the ask; a sell hits the bid.

    ⚠ The signal path validates ``ask`` ONLY, because it only ever buys
    (`strategy_paper_executor` hardcodes ``transaction="buy"``).  A core rebalance
    emits both, so sizing a sell off the ask would overstate the proceeds by the
    whole spread.
    """
    observation = _healthy(bid=Decimal("100.00"), ask=Decimal("100.02"))
    assert _decide(observation, action="buy_core").price == Decimal("100.02")
    assert _decide(observation, action="sell_core").price == Decimal("100.00")


def test_a_refusal_never_carries_a_price() -> None:
    """A price attached to a refusal is a price that is eventually used."""
    for observation in (
        _healthy(kill_switch_active=True),
        _healthy(spread_flag=True),
        _healthy(quoted_at=None),
    ):
        verdict = _decide(observation)
        assert verdict.admitted is False
        assert verdict.price is None
        assert verdict.quoted_at is None


# --------------------------------------------------------------------------- #
# Each refusal fires
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"kill_switch_active": True}, "core_kill_switch_active_or_missing"),
        ({"kill_switch_active": None}, "core_kill_switch_active_or_missing"),
        ({"execution_blocked": True}, "core_execution_block_active"),
        ({"instrument_present": False}, "core_instrument_missing"),
        ({"is_tradable": False}, "core_instrument_not_tradable"),
        ({"asset_class": "uk_equity"}, "core_unsupported_market_session"),
        ({"asset_class": None}, "core_unsupported_market_session"),
        ({"halt_feed_at": None}, "core_halt_feed_missing"),
        ({"is_halted": True}, "core_instrument_halted"),
        ({"quoted_at": None}, "core_quote_missing"),
        ({"bid": Decimal("0")}, "core_quote_price_invalid"),
        ({"ask": Decimal("-1")}, "core_quote_price_invalid"),
        ({"bid": None}, "core_quote_price_invalid"),
        ({"ask": "not a number"}, "core_quote_price_invalid"),
        ({"bid": Decimal("100.05"), "ask": Decimal("100.02")}, "core_quote_crossed"),
        ({"spread_flag": True}, "core_quote_spread_flagged"),
    ],
)
def test_each_defect_produces_its_own_refusal(overrides: dict[str, Any], expected: str) -> None:
    verdict = _decide(_healthy(**overrides))
    assert verdict.admitted is False
    assert verdict.reason_code == expected


def test_an_absent_kill_switch_row_refuses_rather_than_reading_as_inactive() -> None:
    """``NULL`` is "no row", and an absent kill switch is not an inactive one.

    Same rule as ``strategy_paper_executor.py``'s
    ``kill_row is None or bool(kill_row[0])``.  The detail distinguishes the two,
    because "someone deleted the singleton" and "the operator pulled the switch"
    need different responses.
    """
    assert _decide(_healthy(kill_switch_active=None)).detail == "no kill_switch row"
    assert _decide(_healthy(kill_switch_active=True)).detail == "kill switch is active"


def test_a_crossed_quote_refuses_even_though_spread_flag_is_false() -> None:
    """The whole point of ``core_quote_crossed``: ``spread_flag`` cannot catch it.

    ``compute_spread_pct`` is ``(ask - bid) / mid * 100``, so a crossed book gives a
    NEGATIVE spread, which cannot exceed ``max_spread_pct``, so
    ``market_data.py``'s ``spread_pct > max_spread_pct`` leaves the flag FALSE.
    A gate that trusted the flag alone would admit the one quote shape that most
    clearly means "do not trade on this".
    """
    verdict = _decide(_healthy(bid=Decimal("100.05"), ask=Decimal("100.02"), spread_flag=False))
    assert verdict.reason_code == "core_quote_crossed"


def test_the_untraded_side_is_validated_too() -> None:
    """A buy refuses on a corrupt BID, which it would never have used.

    Corruption of the untraded side is evidence the quote is incoherent; sizing off
    the other half is not safer for being arithmetically possible.
    """
    assert _decide(_healthy(bid=Decimal("-1")), action="buy_core").reason_code == "core_quote_price_invalid"
    assert _decide(_healthy(ask=Decimal("-1")), action="sell_core").reason_code == "core_quote_price_invalid"


# --------------------------------------------------------------------------- #
# Precedence — the half a per-code test cannot see
# --------------------------------------------------------------------------- #

#: One observation defect per code, in declared precedence order.  Applying a
#: prefix of this list must always report the FIRST code in that prefix.
_DEFECT_BY_CODE: dict[str, dict[str, Any]] = {
    "core_kill_switch_active_or_missing": {"kill_switch_active": True},
    "core_execution_block_active": {"execution_blocked": True},
    "core_instrument_missing": {"instrument_present": False},
    "core_instrument_not_tradable": {"is_tradable": False},
    "core_unsupported_market_session": {"asset_class": "crypto"},
    "core_halt_feed_missing": {"halt_feed_at": None},
    "core_instrument_halted": {"is_halted": True},
    "core_quote_missing": {"quoted_at": None},
    "core_quote_price_invalid": {"bid": Decimal("0")},
    "core_quote_crossed": {"bid": Decimal("999"), "ask": Decimal("1")},
    "core_quote_spread_flagged": {"spread_flag": True},
}


def test_every_declared_refusal_code_is_exercised_somewhere() -> None:
    """The two clock-driven codes are covered by their own tests, below.

    ⚠ Completeness is asserted against the ``Literal`` itself, so a code added to
    the module without a test fails HERE rather than shipping untested.
    """
    clock_driven = {"core_market_session_closed", "core_halt_feed_stale", "core_quote_stale"}
    caller_driven = {"core_runtime_config_corrupt", "core_auto_trading_disabled"}
    assert set(_ORDER) == set(_DEFECT_BY_CODE) | clock_driven | caller_driven


def test_precedence_holds_when_several_defects_are_true_at_once() -> None:
    """Accumulate every defect from the end backwards; the FIRST must win.

    Without this, a refactor that reorders the returns keeps every per-code test
    green while silently changing which explanation is recorded — and the recorded
    explanation is what an operator acts on.
    """
    ordered = [code for code in _ORDER if code in _DEFECT_BY_CODE]
    for index, expected in enumerate(ordered):
        overrides: dict[str, Any] = {}
        # Applied LAST-first, so when two defects share a field the EARLIER one
        # wins.  `core_quote_price_invalid` (bid=0) and `core_quote_crossed`
        # (bid>ask) both write `bid`, and a crossed book whose bid is invalid is
        # an invalid quote — so the earlier code is the one that must be reported,
        # and composing the other way round would erase the defect under test.
        for code in reversed(ordered[index:]):
            overrides.update(_DEFECT_BY_CODE[code])
        assert _decide(_healthy(**overrides)).reason_code == expected, (
            f"expected {expected} to take precedence over {ordered[index + 1 :]}"
        )


def test_feed_health_is_reported_ahead_of_the_halt_it_supports() -> None:
    """A stale feed plus an open halt row reports the FEED.

    Both refuse, so the order only changes which code is recorded — but blaming the
    instrument for an infrastructure fault sends the operator to the wrong place,
    and a halt row is only worth believing once the feed behind it is.
    """
    observation = _healthy(
        halt_feed_at=_OPEN - timedelta(seconds=CORE_MAX_HALT_FEED_AGE_SECONDS + 1),
        is_halted=True,
    )
    assert _decide(observation).reason_code == "core_halt_feed_stale"


# --------------------------------------------------------------------------- #
# Clock-driven refusals and their boundaries
# --------------------------------------------------------------------------- #


def test_the_session_is_closed_outside_regular_hours_and_on_a_holiday() -> None:
    weekend = datetime(2026, 8, 15, 15, 0, tzinfo=UTC)  # Saturday
    pre_open = datetime(2026, 8, 12, 13, 0, tzinfo=UTC)  # 09:00 ET
    after_close = datetime(2026, 8, 12, 20, 30, tzinfo=UTC)  # 16:30 ET
    for now in (weekend, pre_open, after_close):
        assert _decide(_healthy(), now=now).reason_code == "core_market_session_closed"


def test_the_open_is_inclusive_and_the_close_is_exclusive() -> None:
    """09:30 ET admits; 16:00 ET refuses — a submission at the bell does not land."""
    at_open = datetime(2026, 8, 12, 13, 30, tzinfo=UTC)
    at_close = datetime(2026, 8, 12, 20, 0, tzinfo=UTC)
    assert _decide(_healthy(halt_feed_at=at_open, quoted_at=at_open), now=at_open).admitted is True
    assert _decide(_healthy(), now=at_close).reason_code == "core_market_session_closed"


@pytest.mark.parametrize(
    ("field", "bound", "code"),
    [
        ("quoted_at", CORE_MAX_QUOTE_AGE_SECONDS, "core_quote_stale"),
        ("halt_feed_at", CORE_MAX_HALT_FEED_AGE_SECONDS, "core_halt_feed_stale"),
    ],
)
def test_the_freshness_bound_is_inclusive_at_the_boundary_and_refuses_one_second_past(
    field: str, bound: int, code: str
) -> None:
    assert _decide(_healthy(**{field: _OPEN - timedelta(seconds=bound)})).admitted is True
    assert _decide(_healthy(**{field: _OPEN - timedelta(seconds=bound + 1)})).reason_code == code


@pytest.mark.parametrize("field", ["quoted_at", "halt_feed_at"])
def test_a_future_stamped_row_refuses_rather_than_reading_as_maximally_fresh(field: str) -> None:
    """Without this, a corrupted future timestamp never ages out — it gets fresher.

    Five seconds of skew is tolerated, matching the executor's ``_age_ok``.
    """
    assert _decide(_healthy(**{field: _OPEN + timedelta(seconds=4)})).admitted is True
    assert _decide(_healthy(**{field: _OPEN + timedelta(seconds=30)})).admitted is False


# --------------------------------------------------------------------------- #
# Coupling to the sources the constants were derived from
# --------------------------------------------------------------------------- #


def test_each_freshness_bound_still_agrees_with_its_producers_registered_cadence() -> None:
    """The bound is derived from the PRODUCER, so it must break when the producer moves.

    ⚠ This is the assertion that makes the derivation real rather than narrated.
    ``quotes_refresh`` is hourly and ``strategy_halt_feed_refresh`` every five
    minutes TODAY; re-cadencing either without revisiting the bound would leave a
    constant whose stated derivation no longer holds. Read from the scheduler's own
    registered ``ScheduledJob`` rows, never re-typed here.

    The invariant is the derived INTERVAL — ``period <= bound < 2 * period`` — not
    the midpoint, which is a construction choice frozen in the policy version.
    """
    from app.workers.scheduler import (
        JOB_QUOTES_REFRESH,
        JOB_STRATEGY_HALT_FEED_REFRESH,
        SCHEDULED_JOBS,
    )

    by_name = {job.name: job for job in SCHEDULED_JOBS}
    for job_name, bound in (
        (JOB_QUOTES_REFRESH, CORE_MAX_QUOTE_AGE_SECONDS),
        (JOB_STRATEGY_HALT_FEED_REFRESH, CORE_MAX_HALT_FEED_AGE_SECONDS),
    ):
        cadence = by_name[job_name].cadence
        period = _period_seconds(cadence)
        assert period <= bound < 2 * period, (
            f"{job_name}: bound {bound}s is outside [{period}, {2 * period}) — "
            "the cadence moved and the derivation no longer holds"
        )
        assert bound == _freshness_bound(period)


def _period_seconds(cadence: Any) -> int:
    """The nominal seconds between two fires of a registered cadence.

    Switches on ``kind`` rather than sniffing which fields are non-zero: the
    dataclass defaults every field to 0, so a field-presence test cannot tell
    ``hourly(minute=0)`` from ``every_n_minutes`` at all.
    """
    if cadence.kind == "every_n_minutes":
        return int(cadence.interval_minutes) * 60
    if cadence.kind == "hourly":
        return 3_600
    if cadence.kind == "daily":
        return 86_400
    raise AssertionError(f"unhandled cadence kind for a preflight producer: {cadence.kind}")


def test_the_session_allow_list_names_only_classes_this_repo_has_a_calendar_for() -> None:
    """``us_market_status`` is the only calendar here, so ``us_equity`` is the set.

    ⚠ An ALLOW-list, and the direction is the safety property: ``asset_class`` is a
    CHECK vocabulary that has already grown once (``mena_equity``, added by
    ``sql/068`` over ``sql/067``'s original nine), and a value added later lands on
    the REFUSE side with no code change.  This test pins the *shape* of that
    argument — if a second calendar is ever added, the allow-list must grow
    deliberately rather than by a reviewer's assumption that it already did.
    """
    from app.services import strategy_core_preflight as module

    assert module._SESSION_SUPPORTED_ASSET_CLASSES == frozenset({"us_equity"})
    for other in ("eu_equity", "uk_equity", "asia_equity", "mena_equity", "crypto", "fx", "index", "unknown"):
        assert _decide(_healthy(asset_class=other)).reason_code == "core_unsupported_market_session"


def test_an_unknown_action_raises_rather_than_guessing_a_side() -> None:
    """A ``Literal`` is a static promise, not a runtime invariant.

    The action decides which price a trade is sized off, so there is no safe
    default — and a refusal would let the caller log it and carry on.

    ⚠ The ``ignore`` is the POINT of the test, not a workaround: pyright rejects
    this call, which is exactly why an unchecked runtime path would never be
    exercised by anything except a caller that had already lost its types.
    """
    bad_action: Any = "sell_everything"
    with pytest.raises(StrategyCorePreflightError, match="unknown core rebalance action"):
        decide_core_preflight(_healthy(), core_instrument_id=3138, action=bad_action, now=_OPEN)
