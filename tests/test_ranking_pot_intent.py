"""#2842 slice 5b — the pot's loader gate map, slot ledger, levels and cost cap (spec §7.2), pure."""

from __future__ import annotations

from decimal import Decimal
from fractions import Fraction

import pytest

from app.services import ranking_pot_intent as pi
from app.services import strategy_paper_executor
from app.services.ranking_pot_executor import pot_cost_cap_reason
from app.services.ranking_pot_intent import PAPER_GATE_MAP, SlotLifecycle, occupancy_refusal, sent_levels, slot_wealth
from app.services.strategy_paper_executor import _load_intent
from tests.test_ai_trial_intent import _reason_codes

PAPER_CODES = _reason_codes(_load_intent, strategy_paper_executor)
POT_CODES = _reason_codes(pi.load_pot_intent, pi) | {"decision_expired", "decision_not_yet_due"}


def test_the_parser_reads_the_pot_loader() -> None:
    assert {"pot_identity_mismatch", "pot_not_executing", "market_session_closed", "basis_changed"} <= POT_CODES


def test_every_paper_gate_has_a_disposition() -> None:
    assert PAPER_CODES == PAPER_GATE_MAP.keys()


@pytest.mark.parametrize(("paper_code", "disposition"), sorted(PAPER_GATE_MAP.items()))
def test_kept_and_replacing_codes_are_returned_by_the_pot_loader(paper_code: str, disposition: str) -> None:
    if disposition == "kept":
        assert paper_code in POT_CODES
    elif disposition.startswith("replaced:"):
        assert disposition.removeprefix("replaced:") in POT_CODES
    else:
        assert disposition == "dropped"
        assert paper_code not in POT_CODES


def test_the_pot_intent_carries_no_evidence_field() -> None:
    fields = set(pi.PotIntent.__dataclass_fields__)
    assert not fields & {"forecast_id", "ranking_member_id", "gross_expectancy_ci_low_pct", "scan_at"}


def _lc(
    lid: int,
    slot: int,
    status: str,
    *,
    iid: int | None = None,
    committed: bool = False,
    pnl: str | None = None,
    fees_ambiguous: bool = False,
) -> SlotLifecycle:
    return SlotLifecycle(
        lifecycle_id=lid,
        instrument_id=iid or 1000 + lid,
        slot=slot,
        status=status,  # type: ignore[arg-type]
        committed=committed,
        booked_pnl=None if pnl is None else Decimal(pnl),
        fees_ambiguous=fees_ambiguous,
    )


def test_slot_wealth_books_closed_lifecycles_of_the_slot_only() -> None:
    lcs = [
        _lc(1, 1, "closed", pnl="7.50"),
        _lc(2, 1, "failed"),
        _lc(3, 1, "expired"),
        _lc(4, 2, "closed", pnl="-100"),
        _lc(5, 1, "closed", pnl="-2.505"),
    ]
    # 1000 / 25 + 7.50 − 2.505 = 44.995 → rounded DOWN to the cent.
    assert slot_wealth(Decimal(1000), 25, 1, lcs) == Decimal("44.99")
    assert slot_wealth(Decimal(1000), 25, 3, lcs) == Decimal("40.00")
    # Unbooked → deferral; ambiguous fees → the permanent refusal, whichever comes first.
    assert slot_wealth(Decimal(1000), 25, 1, [*lcs, _lc(6, 1, "closed")]) == "pot_slot_ledger_incomplete"
    assert (
        slot_wealth(Decimal(1000), 25, 1, [_lc(6, 1, "closed"), _lc(7, 1, "closed", pnl="1", fees_ambiguous=True)])
        == "pot_slot_fees_ambiguous"
    )
    with pytest.raises(ValueError):
        slot_wealth(Decimal(0), 25, 1, [])


def test_occupancy_defers_an_unreleased_slot_then_refuses_name_and_capacity() -> None:
    # This lifecycle (9, slot 1, name 2842) is itself uncommitted and never counts against itself.
    me = _lc(9, 1, "entry_pending", iid=2842)
    assert occupancy_refusal(lifecycle_id=9, instrument_id=2842, slot=1, n=2, lifecycles=[me]) is None
    # The predecessor in slot 1 is still open (stamped, not yet closed): defer.
    open_pred = _lc(1, 1, "open", committed=True)
    assert (
        occupancy_refusal(lifecycle_id=9, instrument_id=2842, slot=1, n=2, lifecycles=[me, open_pred])
        == "pot_slot_not_released"
    )
    # Another live lifecycle holds the name.
    other_name = _lc(2, 2, "entry_pending", iid=2842, committed=True)
    assert (
        occupancy_refusal(lifecycle_id=9, instrument_id=2842, slot=1, n=3, lifecycles=[me, other_name])
        == "pot_name_in_flight"
    )
    # N lifecycles already commit capital.
    full = [_lc(2, 2, "open", committed=True), _lc(3, 3, "open", committed=True)]
    assert (
        occupancy_refusal(lifecycle_id=9, instrument_id=2842, slot=1, n=2, lifecycles=[me, *full]) == "pot_slots_full"
    )


def test_queued_unfunded_replacements_do_not_exhaust_n() -> None:
    """Codex ckpt-1 #3: N = 2; A (slot 1) closed, B (slot 2) open and stamped; replacements C (slot 1) and D (slot 2)
    queued with no funding decision. C is free to go: D commits nothing yet."""
    lcs = [
        _lc(1, 1, "closed", pnl="0"),
        _lc(2, 2, "open", committed=True),
        _lc(3, 1, "entry_pending"),
        _lc(4, 2, "entry_pending"),
    ]
    assert occupancy_refusal(lifecycle_id=3, instrument_id=1003, slot=1, n=2, lifecycles=lcs) is None
    assert occupancy_refusal(lifecycle_id=4, instrument_id=1004, slot=2, n=2, lifecycles=lcs) == "pot_slot_not_released"


def test_sent_levels_are_the_frozen_three_atr_two_r_floored_to_the_cent_and_validated_as_sent() -> None:
    assert sent_levels(Decimal(100), Fraction(2)) == (Decimal("94.00"), Decimal("112.00"))
    stop, take = sent_levels(Decimal("33.333333"), Fraction(1, 3))  # type: ignore[misc]
    assert (str(stop), str(take)) == ("32.33", "35.33")
    # 3 × ATR at or above the ask: no valid stop.
    assert sent_levels(Decimal(5), Fraction(2)) is None
    # §7.4 "The broker-held levels": what the cent newly refuses. A SPAC-like ATR of 0.4¢: the floored stop sits
    # 5 ATR away (10.788 → 10.78), past the 4-ATR cap (at 6 dp it was 3 ATR and passed).
    assert sent_levels(Decimal("10.80"), Fraction(4, 1000)) is None
    # A stop that floors to 0.
    assert sent_levels(Decimal(3), Fraction(999, 1000)) is None


@pytest.mark.parametrize(
    ("cost", "amount", "refused"),
    [("0.40", "40", False), ("0.41", "40", True), ("0", "40", False), ("-1", "40", True), ("1", "0", True)],
)
def test_pot_cost_cap(cost: str, amount: str, refused: bool) -> None:
    assert (pot_cost_cap_reason(Decimal(cost), Decimal(amount)) == "pot_cost_cap") is refused


def test_the_execute_job_is_hourly_on_the_trial_lane_and_inert_before_its_window() -> None:
    from datetime import UTC, datetime
    from unittest.mock import MagicMock

    from app.jobs.runtime import _INVOKERS, EXECUTION_LANE_PAPER, execution_lane_for
    from app.services.ranking_pot_executor import run_pot_execution
    from app.workers.scheduler import JOB_RANKING_POT_EXECUTE, SCHEDULED_JOBS

    job = next(j for j in SCHEDULED_JOBS if j.name == JOB_RANKING_POT_EXECUTE)
    assert (job.source, job.cadence.kind, job.cadence.minute) == ("ai_trial", "hourly", 5)
    assert JOB_RANKING_POT_EXECUTE in _INVOKERS and execution_lane_for(JOB_RANKING_POT_EXECUTE) == EXECUTION_LANE_PAPER
    conn = MagicMock()
    # 14:05 UTC on a session day, and a Saturday: nothing is read.
    for at in (datetime(2026, 11, 2, 14, 5, tzinfo=UTC), datetime(2026, 11, 7, 16, 5, tzinfo=UTC)):
        result = run_pot_execution(conn, broker=MagicMock(), refresh_halts=MagicMock(), clock=lambda at=at: at)
        assert (result.session_open, result.note) == (False, "session_closed")
    conn.execute.assert_not_called()


def test_the_loss_check_runs_every_in_session_fire_and_entries_wait_for_15_utc(monkeypatch: pytest.MonkeyPatch) -> None:
    from datetime import UTC, datetime
    from unittest.mock import MagicMock

    from app.services import ranking_pot_executor as px

    calls: list[object] = []
    monkeypatch.setattr(px, "evaluate_losses", lambda conn, *, broker: calls.append(broker) or {7: "ok"})
    monkeypatch.setattr(px, "due_pot_entries", MagicMock(side_effect=AssertionError("no entries before 15:00")))
    # 14:40 UTC on 2026-11-02 (EST): the session is open, entries are not yet due.
    at = datetime(2026, 11, 2, 14, 40, tzinfo=UTC)
    result = px.run_pot_execution(MagicMock(), broker=MagicMock(), refresh_halts=MagicMock(), clock=lambda: at)
    assert (result.session_open, result.note, len(calls)) == (False, "session_closed loss[7]=ok", 1)
