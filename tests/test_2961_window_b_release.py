"""#2961 window B — the pure halves of the attended release (no database).

Spec: ``docs/proposals/execution/2026-09-23-core-window-b-attended-release.md``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from app.services.strategy_core_window_b_release import (
    WINDOW_B_ATTESTATION_MAX_CHARS,
    WINDOW_B_CLOCK_SKEW_ALLOWANCE,
    WINDOW_B_VISIBILITY_WAIT,
    WindowBRefused,
    attendance_refusal,
    evaluate_window_b_witness,
    release_window_b_core_entry,
    sender_death_refusal,
    validate_attestation,
    window_b_wait_remaining,
)

INSTRUMENT = 3417
OTHER = 1001
CREATED = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)
DEADLINE = CREATED + WINDOW_B_VISIBILITY_WAIT
OBSERVED = DEADLINE + timedelta(seconds=1)


def _payload(
    *,
    positions: Any = None,
    orders_for_open: Any = None,
    orders: Any = None,
) -> dict[str, Any]:
    return {
        "clientPortfolio": {
            "positions": [] if positions is None else positions,
            "ordersForOpen": [] if orders_for_open is None else orders_for_open,
            "orders": [] if orders is None else orders,
        }
    }


def _position(opened: datetime, *, instrument: int = INSTRUMENT, position_id: Any = 42) -> dict[str, Any]:
    return {
        "instrumentID": instrument,
        "positionID": position_id,
        "openDateTime": opened.isoformat().replace("+00:00", "Z"),
        "mirrorID": 0,
    }


def _judge(payload: Any, *, observed_at: datetime = OBSERVED) -> Any:
    return evaluate_window_b_witness(
        payload,
        instrument_id=INSTRUMENT,
        authority_created_at=CREATED,
        observed_at=observed_at,
        not_before=DEADLINE,
    )


# ---------------------------------------------------------------------------
# Witness evaluator
# ---------------------------------------------------------------------------


def test_an_empty_account_passes() -> None:
    witness = _judge(_payload())
    assert witness.refusal is None
    assert (witness.total_positions, witness.total_pending) == (0, 0)


@pytest.mark.parametrize(
    "opened",
    [
        CREATED - WINDOW_B_CLOCK_SKEW_ALLOWANCE,  # the boundary refuses
        CREATED,
        CREATED + timedelta(seconds=5),
    ],
)
def test_a_position_opened_inside_the_skew_window_refuses(opened: datetime) -> None:
    assert _judge(_payload(positions=[_position(opened)])).refusal == "window_b_witness_position_after_authority"


def test_an_older_position_passes_and_is_kept_in_the_evidence() -> None:
    old = _position(CREATED - timedelta(days=30))
    witness = _judge(_payload(positions=[old]))
    assert witness.refusal is None
    assert witness.positions_on_instrument == (old,)


def test_positions_on_another_instrument_are_ignored_but_counted() -> None:
    witness = _judge(_payload(positions=[_position(CREATED, instrument=OTHER)]))
    assert witness.refusal is None
    assert witness.positions_on_instrument == ()
    assert witness.total_positions == 1


def test_a_non_mirror_pending_open_refuses_and_a_mirror_one_passes() -> None:
    direct = {"instrumentID": INSTRUMENT, "mirrorID": 0, "amount": 100}
    mirror = {"instrumentID": INSTRUMENT, "mirrorID": 77, "amount": 100}
    assert _judge(_payload(orders_for_open=[direct])).refusal == "window_b_witness_pending_order"
    passed = _judge(_payload(orders_for_open=[mirror]))
    assert passed.refusal is None
    assert passed.orders_for_open_on_instrument == (mirror,)


@pytest.mark.parametrize("mirror_id", [0, 77])
def test_any_orders_entry_on_the_instrument_refuses_whatever_its_mirror(mirror_id: int) -> None:
    entry = {"instrumentID": INSTRUMENT, "mirrorID": mirror_id, "amount": 100}
    assert _judge(_payload(orders=[entry])).refusal == "window_b_witness_pending_order"


@pytest.mark.parametrize("missing", ["positions", "ordersForOpen", "orders"])
def test_a_missing_array_refuses(missing: str) -> None:
    payload = _payload()
    del payload["clientPortfolio"][missing]
    assert _judge(payload).refusal == "window_b_witness_incomplete"


@pytest.mark.parametrize("payload", [{}, {"clientPortfolio": []}, [], None])
def test_a_missing_envelope_refuses(payload: Any) -> None:
    assert _judge(payload).refusal == "window_b_witness_incomplete"


def test_an_array_that_is_not_a_list_refuses() -> None:
    payload = _payload()
    payload["clientPortfolio"]["orders"] = {"0": {}}
    assert _judge(payload).refusal == "window_b_witness_incomplete"


@pytest.mark.parametrize(
    "entry",
    [
        "not-an-object",
        {"positionID": 1, "openDateTime": "2020-01-01T00:00:00Z"},  # no instrument at all
        {"instrumentID": "3417", "positionID": 1, "openDateTime": "2020-01-01T00:00:00Z"},
        {"instrumentID": True, "positionID": 1, "openDateTime": "2020-01-01T00:00:00Z"},
        {"instrumentId": INSTRUMENT, "instrumentID": OTHER, "positionID": 1, "openDateTime": "2020-01-01T00:00:00Z"},
        {"instrumentID": INSTRUMENT, "positionID": "1", "openDateTime": "2020-01-01T00:00:00Z"},
        {"instrumentID": INSTRUMENT, "positionID": 1, "openDateTime": "2020-01-01T00:00:00"},  # naive
        {"instrumentID": INSTRUMENT, "positionID": 1, "openDateTime": "yesterday"},
        {"instrumentID": INSTRUMENT, "positionID": 1, "openDateTime": "2020-01-01T00:00:00Z", "mirrorID": "0"},
    ],
)
def test_a_malformed_position_refuses(entry: Any) -> None:
    assert _judge(_payload(positions=[entry])).refusal == "window_b_witness_malformed"


def test_a_read_taken_before_the_deadline_refuses() -> None:
    witness = _judge(_payload(), observed_at=DEADLINE - timedelta(microseconds=1))
    assert witness.refusal == "window_b_witness_before_deadline"


def test_the_deadline_itself_is_late_enough() -> None:
    assert _judge(_payload(), observed_at=DEADLINE).refusal is None


# ---------------------------------------------------------------------------
# Sender liveness
# ---------------------------------------------------------------------------


def _probe_raising(exc: BaseException | None) -> Any:
    calls: list[tuple[int, int]] = []

    def probe(pid: int, sig: int) -> None:
        calls.append((pid, sig))
        if exc is not None:
            raise exc

    probe.calls = calls  # type: ignore[attr-defined]
    return probe


def _liveness(**overrides: Any) -> str | None:
    kwargs: dict[str, Any] = {
        "pid": 4242,
        "host": "box",
        "entered_at": CREATED,
        "this_host": "box",
        "this_pid": 1,
        "probe": _probe_raising(ProcessLookupError()),
    }
    kwargs.update(overrides)
    return sender_death_refusal(**kwargs)


def test_only_process_lookup_error_is_death() -> None:
    assert _liveness() is None


@pytest.mark.parametrize(
    ("exc", "slug"),
    [
        (None, "window_b_sender_alive"),
        (PermissionError(), "window_b_sender_liveness_unknown"),
        (OSError(), "window_b_sender_liveness_unknown"),
    ],
)
def test_any_other_probe_outcome_refuses(exc: BaseException | None, slug: str) -> None:
    assert _liveness(probe=_probe_raising(exc)) == slug


@pytest.mark.parametrize("field", ["pid", "host", "entered_at"])
def test_an_unrecorded_identity_refuses(field: str) -> None:
    assert _liveness(**{field: None}) == "window_b_sender_identity_unrecorded"


@pytest.mark.parametrize("pid", [0, -1, -4242])
def test_a_non_positive_pid_refuses_without_probing(pid: int) -> None:
    probe = _probe_raising(ProcessLookupError())
    assert _liveness(pid=pid, probe=probe) == "window_b_sender_pid_invalid"
    assert probe.calls == []


def test_another_host_refuses_without_probing() -> None:
    probe = _probe_raising(ProcessLookupError())
    assert _liveness(host="other-box", probe=probe) == "window_b_sender_other_host"
    assert probe.calls == []


def test_our_own_pid_refuses() -> None:
    assert _liveness(this_pid=4242) == "window_b_sender_is_this_process"


# ---------------------------------------------------------------------------
# Attestation, accident controls, wait arithmetic
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("text", ["", "   ", "x" * (WINDOW_B_ATTESTATION_MAX_CHARS + 1)])
def test_an_invalid_attestation_refuses(text: str) -> None:
    with pytest.raises(WindowBRefused) as exc:
        validate_attestation(text)
    assert exc.value.slug == "window_b_attestation_invalid"


def test_an_attestation_is_stripped() -> None:
    assert validate_attestation("  checked the account  ") == "checked the account"


def test_the_accident_controls() -> None:
    assert attendance_refusal(linked_worktree=True, stdin_is_tty=True) == "window_b_linked_worktree"
    assert attendance_refusal(linked_worktree=False, stdin_is_tty=False) == "window_b_no_tty"
    assert attendance_refusal(linked_worktree=False, stdin_is_tty=True) is None


def test_the_wait_runs_from_the_later_of_death_and_the_marker() -> None:
    wait = WINDOW_B_VISIBILITY_WAIT.total_seconds()
    # Death observed now, marker long ago: the monotonic leg governs.
    assert window_b_wait_remaining(
        dead_monotonic=100.0, now_monotonic=100.0, entered_at=CREATED - timedelta(hours=1), now_wall=CREATED
    ) == pytest.approx(wait)
    # Marker just committed, death seen a while back: the wall leg governs.
    assert window_b_wait_remaining(
        dead_monotonic=0.0, now_monotonic=wait, entered_at=CREATED, now_wall=CREATED + timedelta(seconds=10)
    ) == pytest.approx(wait - 10)
    # Both passed.
    assert (
        window_b_wait_remaining(
            dead_monotonic=0.0, now_monotonic=wait, entered_at=CREATED, now_wall=CREATED + WINDOW_B_VISIBILITY_WAIT
        )
        == 0.0
    )


def test_a_wall_clock_step_forward_does_not_shorten_the_monotonic_leg() -> None:
    remaining = window_b_wait_remaining(
        dead_monotonic=50.0,
        now_monotonic=51.0,
        entered_at=CREATED,
        now_wall=CREATED + timedelta(days=1),
    )
    assert remaining == pytest.approx(WINDOW_B_VISIBILITY_WAIT.total_seconds() - 1)


# ---------------------------------------------------------------------------
# Pre-database refusals: none of them may touch the connection
# ---------------------------------------------------------------------------


class _UntouchableConn:
    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"the connection was touched ({name}) before the pre-database checks refused")


def _pre_db(**overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "order_id": 7,
        "operator_id": "op",
        "attestation": "checked",
        "broker_factory": lambda *_: (_ for _ in ()).throw(AssertionError("broker reached")),
        "environment": "demo",
        "attendance": lambda: None,
    }
    kwargs.update(overrides)
    return release_window_b_core_entry(_UntouchableConn(), **kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("overrides", "slug"),
    [
        ({"attestation": " "}, "window_b_attestation_invalid"),
        ({"attendance": lambda: "window_b_no_tty"}, "window_b_no_tty"),
        ({"environment": "real"}, "window_b_environment_not_demo"),
    ],
)
def test_pre_database_refusals_never_touch_the_connection(overrides: dict[str, Any], slug: str) -> None:
    result = _pre_db(**overrides)
    assert (result.passed, result.slug, result.decision_id) == (False, slug, None)
