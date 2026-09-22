"""#3284 item 4b — when a refusal streak becomes an alert, and when it must not.

Pure: the verdict takes an already-read list of ``OwnedRepairStreak`` and returns a
verdict per position, so every rule here is table-testable without Postgres.  The reader's
LEFT JOIN and the ``status='active'`` filter are the genuinely-new SQL mechanism and live
in ``test_3284_exit_protection_db.py`` (separate module — the ``db`` marker is applied per
MODULE, so one DB test here would evict every one of these from the fast tier).

The two tests worth reading first are the ones that encode a falsified premise rather than
a feature: ``test_a_present_but_wrong_stop_still_alerts...`` and
``test_the_verdict_never_reads_the_broker_snapshot``.
"""

from __future__ import annotations

import inspect
from datetime import UTC, datetime

import pytest

from app.services.strategy_exit_protection import (
    DEFAULT_REFUSAL_BUDGET,
    REFUSAL_BUDGET_BY_REASON,
    OwnedRepairStreak,
    alerting_exit_protection,
    assess_exit_protection,
    refusal_budget_for,
)

_NOW = datetime(2026, 9, 22, 12, 0, tzinfo=UTC)


def _streak(
    *,
    ownership_id: int = 1,
    refusals: int | None,
    reason: str | None = None,
    first_refused_at: datetime | None = None,
) -> OwnedRepairStreak:
    return OwnedRepairStreak(
        ownership_id=ownership_id,
        strategy_trade_id=ownership_id * 10,
        broker_position_id=ownership_id * 100,
        consecutive_refusals=refusals,
        first_refused_at=first_refused_at or (_NOW if refusals else None),
        last_refusal_reason=reason,
        last_checked_at=_NOW,
    )


def _one(streak: OwnedRepairStreak):
    verdicts = assess_exit_protection([streak])
    assert len(verdicts) == 1
    return verdicts[0]


def test_an_ownership_the_arm_has_never_visited_reports_it_without_claiming_a_fault() -> None:
    """NULL join is a different fact from zero, and must not read ``ok``.

    A position opened seconds ago legitimately has no visit yet. Reporting ``ok`` would
    assert protection nobody has observed; alerting would fire on every open. So it gets
    its own non-alerting state, which is the same call ``_derive_overall_status`` already
    made for a job with ``last_status is None``.
    """
    verdict = _one(_streak(refusals=None))
    assert verdict.status == "never_checked"
    assert verdict.is_alerting is False
    assert verdict.consecutive_refusals == 0
    assert verdict.detail == "no fixed-exit repair visit recorded yet"


def test_a_cleared_streak_is_ok() -> None:
    verdict = _one(_streak(refusals=0))
    assert verdict.status == "ok"
    assert verdict.is_alerting is False


def test_a_refusal_inside_its_budget_is_the_expected_transient_and_does_not_alert() -> None:
    """One stale quote is an event, not a condition.

    ⚠ This state exists so the alarm never ships with a documented "ignore this"
    attached — the ``retired`` precedent in ``strategy_scan_freshness``. It is still
    REPORTED, with its count and budget, so an operator watching a repair in flight can
    see it happening.
    """
    verdict = _one(_streak(refusals=1, reason="fixed_exit_quote_unsafe"))
    assert verdict.status == "repairing"
    assert verdict.is_alerting is False
    assert (verdict.consecutive_refusals, verdict.refusal_budget) == (1, 2)


def test_a_second_consecutive_quote_refusal_alerts() -> None:
    """The explanation was re-tested and failed again — that is the condition."""
    verdict = _one(_streak(refusals=2, reason="fixed_exit_quote_unsafe"))
    assert verdict.status == "unrepairable"
    assert verdict.is_alerting is True


def test_a_broker_capability_refusal_alerts_on_the_very_first_visit() -> None:
    """The exception to the rule, and the evidence for it is in the refusing branch.

    ``_repair_fixed_exit`` tests ``arms[0].allow_edit_stop_loss is not True`` — a verdict
    about the instrument, re-read from the same eligibility payload every visit. Retrying
    cannot change it, so a budget would only delay the alert.
    """
    verdict = _one(_streak(refusals=1, reason="broker_fixed_exit_edit_not_allowed"))
    assert verdict.status == "unrepairable"
    assert verdict.is_alerting is True
    assert verdict.refusal_budget == 1


def test_a_submitted_edit_that_never_lands_alerts_on_the_second_visit() -> None:
    """``broker_edit_pending`` is item 4a's third class and the design comment predates it.

    Transient in shape — the next visit either confirms it landed or counts it again — so
    it takes the rule (N=2), not the capability exception.
    """
    assert _one(_streak(refusals=1, reason="broker_edit_pending")).is_alerting is False
    assert _one(_streak(refusals=2, reason="broker_edit_pending")).is_alerting is True


def test_a_reason_code_nobody_has_classified_still_alerts() -> None:
    """⚠⚠ The default must not be silence.

    A refusal code this module has never seen, on a position observed unprotected, is
    exactly the silent carry-on #3284 item 4 forbids. It takes the rule (2), not the
    exception (1) — we have no evidence that retrying is futile for an unseen class, and
    N=1 on an unseen transient would ship a false alarm.
    """
    assert refusal_budget_for("some_refusal_shipped_next_year") == DEFAULT_REFUSAL_BUDGET
    assert _one(_streak(refusals=1, reason="some_refusal_shipped_next_year")).is_alerting is False
    assert _one(_streak(refusals=2, reason="some_refusal_shipped_next_year")).is_alerting is True


def test_a_present_but_wrong_stop_still_alerts_because_the_streak_is_the_witness() -> None:
    """⛔ The handoff design's second conjunct, falsified.

    #3284's research comment specified *"alerting when a position is currently
    unprotected (``is_no_stop_loss``) AND its streak has reached the class threshold"*.
    On the core arm ``stop_gap = is_no_stop_loss or not core_exit_level_satisfied(...)``
    — a DISJUNCTION — and ``core_exit_levels`` is a pure function of the CURRENT weighted
    entry, which eToro re-weights on an ADD. So a position whose stop is present but
    computed from the previous entry has ``is_no_stop_loss = False`` while its repair
    refuses every visit, and the conjunct would silence the alarm in precisely the case
    item 2 of this ticket exists for.

    The streak needs no help: ``record_repair_visit(state="rejected")`` is reached ONLY
    from inside ``if intent.has_gap:``, so a non-zero streak already means "a gap was
    observed and not closed".
    """
    verdict = _one(_streak(refusals=3, reason="fixed_exit_quote_unsafe"))
    assert verdict.status == "unrepairable"
    assert verdict.is_alerting is True


def test_the_verdict_never_reads_the_broker_snapshot() -> None:
    """The guard against re-adding the suppressing conjunct, and against a stale source.

    ``is_no_stop_loss`` lives in ``broker_positions``, written by ``portfolio_sync``.
    Measured on dev 2026-09-21T23:33Z, every row's ``updated_at`` was ``21:21:33Z`` — a
    2h12m-old snapshot — while the streak table is written by the paper cycle's own fresh
    ``get_portfolio`` observation. Joining it would import a second staleness source into
    a live-safety alarm for no information gain.
    """
    from app.services import strategy_exit_protection

    assert "broker_positions" not in strategy_exit_protection._OWNED_STREAKS_SQL  # noqa: SLF001
    assert "is_no_stop_loss" not in inspect.getsource(strategy_exit_protection.assess_exit_protection)
    assert "is_no_stop_loss" not in OwnedRepairStreak.__dataclass_fields__


def test_every_budgeted_reason_is_a_reason_the_manager_can_actually_return() -> None:
    """Pin the vocabularies without an import edge.

    ``strategy_position_manager`` imports item 4a's recorder, so this module importing the
    manager back would close a cycle. The reason codes are string literals in the
    refusing branches; a rename that missed this table would silently grant the renamed
    class the default budget instead of its exception.
    """
    manager_source = inspect.getsource(
        __import__("app.services.strategy_position_manager", fromlist=["_repair_fixed_exit"])
    )
    for reason in REFUSAL_BUDGET_BY_REASON:
        assert f'"{reason}"' in manager_source, f"{reason} is not a literal the manager returns"


def test_one_verdict_per_ownership_in_a_stable_order() -> None:
    """A position with no verdict is a position nothing reports on — the 4a condition."""
    verdicts = assess_exit_protection(
        [
            _streak(ownership_id=3, refusals=0),
            _streak(ownership_id=1, refusals=None),
            _streak(ownership_id=2, refusals=5, reason="fixed_exit_quote_unsafe"),
        ]
    )
    assert [v.ownership_id for v in verdicts] == [1, 2, 3]
    assert [v.ownership_id for v in alerting_exit_protection(verdicts)] == [2]


@pytest.mark.parametrize("refusals", [0, 1, 2, 7])
def test_the_count_and_budget_always_travel_with_the_verdict(refusals: int) -> None:
    """An operator seeing ``unrepairable`` must be able to tell 1-of-1 from 3-of-2."""
    verdict = _one(_streak(refusals=refusals, reason="fixed_exit_quote_unsafe" if refusals else None))
    assert verdict.consecutive_refusals == refusals
    assert verdict.refusal_budget >= 1


def test_the_headline_degrades_on_an_unrepairable_position_and_not_on_a_repairing_one() -> None:
    """#3284 item 4b's contribution to ``/system/status``' overall verdict.

    ``degraded``, not ``down``: "down" means nothing is updating (kill switch, dead
    engine, layer error), and here the cycle is still visiting the position — it is the
    BROKER EDIT that will not take.
    """
    from app.api.system import _derive_overall_status
    from app.services.ops_monitor import JobHealth, LayerHealth

    layers = [LayerHealth(layer="prices", status="ok", latest=_NOW)]
    jobs = [JobHealth(job_name="daily_candle_refresh", last_status="success", last_finished_at=_NOW)]
    repairing = assess_exit_protection([_streak(refusals=1, reason="fixed_exit_quote_unsafe")])
    unrepairable = assess_exit_protection([_streak(refusals=2, reason="fixed_exit_quote_unsafe")])

    assert _derive_overall_status(layers, jobs, False, set(), exit_protection=repairing) == "ok"
    assert _derive_overall_status(layers, jobs, False, set(), exit_protection=unrepairable) == "degraded"
    # A kill switch still outranks it: "intentionally halted" is not "one stop is stuck".
    assert _derive_overall_status(layers, jobs, True, set(), exit_protection=unrepairable) == "down"
