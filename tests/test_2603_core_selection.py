from datetime import UTC, date, datetime
from typing import Any, cast
from unittest.mock import MagicMock

import psycopg
import pytest

from app.services.strategy_core_selection import (
    CORE_SELECTION_EVIDENCE_NOT_BEFORE,
    CORE_SELECTION_VERDICT_BOUND_VERSION,
    CoreSelectionError,
    earliest_possible_verdict_at,
    load_core_selection,
    require_selected_core_instrument,
)
from scripts.verify_2833_core_selection import load_declaration


def _empty_connection() -> psycopg.Connection[Any]:
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = []
    return cast(psycopg.Connection[Any], conn)


def _candidate_connection() -> psycopg.Connection[Any]:
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        # Venue classes are the DEV-MEASURED ones (2026-09-14): SPY.RTH is exchange
        # `33` / `us_equity`; CSPX.L and IUSA.L are both exchange `7` = LSE /
        # `uk_equity`, which the core execution path has no session calendar for.
        (3417, "SPY.RTH", 5, None, None, 4, "us_equity", date(2026, 8, 28), None),
        (3434, "CSPX.L", 5, None, None, 4, "uk_equity", date(2026, 8, 28), None),
        (3075, "IUSA.L", 5, None, None, 4, "uk_equity", date(2026, 8, 28), None),
    ]
    return cast(psycopg.Connection[Any], conn)


def _selection_with_verdict(
    monkeypatch: pytest.MonkeyPatch,
    instrument_id: int,
    *,
    conn: psycopg.Connection[Any] | None = None,
) -> Any:
    """Load the selection as if #2833's verdict constants named ``instrument_id``."""
    monkeypatch.setattr("app.services.strategy_core_selection.SELECTED_CORE_OUTCOME", "pass")
    monkeypatch.setattr("app.services.strategy_core_selection.SELECTED_CORE_INSTRUMENT_ID", instrument_id)
    monkeypatch.setattr("app.services.strategy_core_selection.SELECTED_CORE_EVIDENCE_REF", "#2833 verdict")
    return load_core_selection(conn if conn is not None else _candidate_connection())


def _clear_verdict(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the PRE-verdict state these cases are about.

    Until 2026-09-18 the shipped constants were `None`, so a test about "before the
    verdict" needed no setup. #2833's verdict has since been transcribed into the
    module, so the precondition has to be stated rather than inherited — otherwise
    these cases quietly start exercising the post-verdict path and stop testing what
    their names claim. Mirror of `_selection_with_verdict`; neither weakens an
    assertion, both only supply the constant state the case is written about.
    """
    monkeypatch.setattr("app.services.strategy_core_selection.SELECTED_CORE_OUTCOME", None)
    monkeypatch.setattr("app.services.strategy_core_selection.SELECTED_CORE_INSTRUMENT_ID", None)
    monkeypatch.setattr("app.services.strategy_core_selection.SELECTED_CORE_EVIDENCE_REF", None)


def test_missing_candidates_are_unavailable_not_perpetually_collecting(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_verdict(monkeypatch)
    selection = load_core_selection(_empty_connection())
    assert selection.state == "unavailable"
    assert selection.selected_instrument_id is None
    assert selection.observed_trading_days == 0
    assert selection.missing_candidate_ids == (3417, 3434, 3075)
    assert selection.configuration_error is None


def test_page_coverage_uses_the_frozen_prospective_boundary() -> None:
    declaration = load_declaration()
    assert CORE_SELECTION_EVIDENCE_NOT_BEFORE == datetime.fromisoformat(
        str(declaration["evidence_not_before"]).replace("Z", "+00:00")
    )


def test_overall_progress_counts_common_dates_not_the_minimum_individual_count() -> None:
    selection = load_core_selection(_candidate_connection())
    assert [candidate.observed_trading_days for candidate in selection.candidates] == [5, 5, 5]
    assert selection.observed_trading_days == 4


def test_mandate_enablement_refuses_before_2833_verdict(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_verdict(monkeypatch)
    with pytest.raises(CoreSelectionError, match="five-trading-day cost verdict"):
        require_selected_core_instrument(_candidate_connection(), instrument_id=3417)


def test_the_refusal_names_the_state_it_is_in_not_a_generic_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    """#3037: "until #2833 completes" is FALSE once the verdict is complete and says cash.

    One sentence for every non-ready state meant the operator was told to keep waiting
    for a study that had already finished.
    """
    _clear_verdict(monkeypatch)
    monkeypatch.setattr("app.services.strategy_core_selection.SELECTED_CORE_OUTCOME", "cash")
    monkeypatch.setattr("app.services.strategy_core_selection.SELECTED_CORE_EVIDENCE_REF", "#2833 verdict")
    # ⚠ The instrument id must stay None: a declared 'cash' that also names an
    # instrument is a DIFFERENT refusal ("declares outcome 'cash' but also names
    # instrument"), and this case is about the cash wording, not that one.
    with pytest.raises(CoreSelectionError, match="reviewed verdict is cash"):
        require_selected_core_instrument(_candidate_connection(), instrument_id=3417)


def test_absent_candidate_rows_refuse_on_coverage_not_on_a_pending_verdict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_verdict(monkeypatch)
    with pytest.raises(CoreSelectionError, match="candidate coverage is incomplete"):
        require_selected_core_instrument(_empty_connection(), instrument_id=3417)


@pytest.mark.parametrize("unexecutable_id", [3434, 3075])
def test_a_verdict_naming_a_venue_we_cannot_session_check_is_refused_at_declaration(
    monkeypatch: pytest.MonkeyPatch,
    unexecutable_id: int,
) -> None:
    """#2833 may only name a sleeve the core execution path can actually trade.

    Two of the three declared candidates are LSE / ``uk_equity``, and
    ``decide_core_preflight`` refuses those ``core_unsupported_market_session``
    with every other input healthy. Without this gate the refusal first appears
    in the operator-attended session, which is the most expensive place to find
    it; ``require_selected_core_instrument`` keys on ``ready``, so refusing here
    blocks the mandate writer and the executor together.
    """
    selection = _selection_with_verdict(monkeypatch, unexecutable_id)
    assert selection.state == "unavailable"
    assert selection.selected_instrument_id is None
    assert selection.selected_symbol is None
    assert selection.configuration_error is not None
    # The identity half is what the wrap exists for -- `session_support_reason` alone
    # names the CLASS, not which selection is at fault.
    assert str(unexecutable_id) in selection.configuration_error
    assert "uk_equity" in selection.configuration_error
    # ⚠ The refusal names the VENUE fault, not a pending verdict (#3037). Saying "until
    # #2833 completes" here would point the operator at a wait that will never clear it --
    # the study has answered; its answer is one this repo cannot execute.
    assert selection.declared_outcome == "pass"
    # ⚠ The REASON changed on 2026-09-16 (#2312) while the refusal did not: the LSE
    # calendar now exists, so `uk_equity` is held out by the missing halt feed, not
    # by a missing calendar. Matching the old phrase here would pass only until the
    # message told the truth.
    with pytest.raises(CoreSelectionError, match="no halt feed covers that venue"):
        require_selected_core_instrument(_candidate_connection(), instrument_id=unexecutable_id)


def test_a_verdict_naming_an_executable_venue_is_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    """The control arm — without it the test above passes on a gate that refuses
    everything, which is the failure mode a fail-closed check invites."""
    selection = _selection_with_verdict(monkeypatch, 3417)
    assert selection.state == "ready"
    assert selection.selected_instrument_id == 3417
    assert selection.selected_symbol == "SPY.RTH"
    assert selection.configuration_error is None


def test_a_candidate_with_no_exchanges_row_is_refused_not_admitted(monkeypatch: pytest.MonkeyPatch) -> None:
    """A NULL ``asset_class`` is an unknown venue, which is exactly what the
    allow-list exists to catch — it must not read as "nothing objected"."""
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        (3417, "SPY.RTH", 5, None, None, 4, None, date(2026, 8, 28), None),
        (3434, "CSPX.L", 5, None, None, 4, "uk_equity", date(2026, 8, 28), None),
        (3075, "IUSA.L", 5, None, None, 4, "uk_equity", date(2026, 8, 28), None),
    ]
    selection = _selection_with_verdict(monkeypatch, 3417, conn=cast(psycopg.Connection[Any], conn))
    assert selection.state == "unavailable"
    assert selection.configuration_error is not None
    assert "asset_class=None" in selection.configuration_error


@pytest.mark.parametrize(
    ("instrument_id", "evidence_ref"),
    [(3417, None), (999999, "#2833 verdict")],
)
def test_partial_or_foreign_verdict_constants_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    instrument_id: int,
    evidence_ref: str | None,
) -> None:
    monkeypatch.setattr("app.services.strategy_core_selection.SELECTED_CORE_OUTCOME", "pass")
    monkeypatch.setattr("app.services.strategy_core_selection.SELECTED_CORE_INSTRUMENT_ID", instrument_id)
    monkeypatch.setattr("app.services.strategy_core_selection.SELECTED_CORE_EVIDENCE_REF", evidence_ref)
    selection = load_core_selection(_candidate_connection())
    assert selection.state == "unavailable"
    assert selection.selected_instrument_id is None
    assert selection.configuration_error is not None


@pytest.mark.parametrize(
    ("observed", "last_common", "window_close", "now", "expected", "why"),
    [
        (
            1,
            date(2026, 8, 25),
            None,
            datetime(2026, 9, 14, 0, 46, tzinfo=UTC),
            datetime(2026, 9, 18, tzinfo=UTC),
            "the live 2026-09-14 state: four sessions left, Mon-Thu, boundary the Friday",
        ),
        (
            1,
            date(2026, 8, 25),
            None,
            datetime(2026, 9, 13, 23, 42, tzinfo=UTC),
            datetime(2026, 9, 18, tzinfo=UTC),
            "a Sunday clock reaches the same answer -- the weekend is not a session",
        ),
        (
            4,
            date(2026, 9, 17),
            None,
            datetime(2026, 9, 17, 20, 0, tzinfo=UTC),
            datetime(2026, 9, 19, tzinfo=UTC),
            "the fifth session cannot be the fourth session's own day",
        ),
        (
            5,
            date(2026, 9, 18),
            date(2026, 9, 18),
            datetime(2026, 9, 20, tzinfo=UTC),
            datetime(2026, 9, 19, tzinfo=UTC),
            "a complete window's boundary is a historical fact and stops moving",
        ),
        (
            6,
            date(2026, 9, 21),
            date(2026, 9, 18),
            datetime(2026, 9, 21, 20, 0, tzinfo=UTC),
            datetime(2026, 9, 19, tzinfo=UTC),
            "a SIXTH observation cannot move a boundary that already opened (Codex ckpt-2)",
        ),
        (
            0,
            None,
            None,
            datetime(2026, 8, 20, tzinfo=UTC),
            datetime(2026, 9, 1, tzinfo=UTC),
            "the walk never starts before the prospective boundary (2026-08-25)",
        ),
    ],
)
def test_the_verdict_bound_is_computed_from_the_evidence_and_the_clock(
    observed: int,
    last_common: date | None,
    window_close: date | None,
    now: datetime,
    expected: datetime,
    why: str,
) -> None:
    assert (
        earliest_possible_verdict_at(
            observed_trading_days=observed,
            last_common_observed_date=last_common,
            verdict_window_close_date=window_close,
            now=now,
        )
        == expected
    ), why


def test_the_bound_never_overstates_and_the_holiday_omission_is_the_only_slack() -> None:
    """#2833 hand-derived 2026-09-02 on the 2026-08-24 inputs; this returns 2026-09-01.

    The one-day gap IS the 31 August LSE Summer Bank Holiday, which this construction
    deliberately does not model (#2312 owns the calendars).  Asserted rather than left
    implicit so the direction is pinned: a missing holiday can only make the bound
    EARLIER, and a lower bound is allowed to be early and never late.
    """
    bound = earliest_possible_verdict_at(
        observed_trading_days=0,
        last_common_observed_date=None,
        verdict_window_close_date=None,
        now=datetime(2026, 8, 24, 12, 0, tzinfo=UTC),
    )
    assert bound == datetime(2026, 9, 1, tzinfo=UTC)
    assert bound < datetime(2026, 9, 2, tzinfo=UTC)


def test_the_bound_the_page_serves_moves_with_the_evidence() -> None:
    """The defect: a hardcoded bound cannot answer a window that stalled for 17 days."""
    selection = load_core_selection(
        _candidate_connection(),
        now=datetime(2026, 9, 14, 0, 46, tzinfo=UTC),
    )
    # 4 of 5 common dates seen, so one session remains: Monday 2026-09-14 itself.
    assert selection.earliest_possible_verdict_at == datetime(2026, 9, 15, tzinfo=UTC)


def test_an_undescribable_population_still_yields_a_future_bound() -> None:
    """No candidate row at all: 0/5, no last common date, and a bound ahead of the clock."""
    selection = load_core_selection(_empty_connection(), now=datetime(2026, 9, 14, 0, 46, tzinfo=UTC))
    assert selection.observed_trading_days == 0
    assert selection.earliest_possible_verdict_at == datetime(2026, 9, 19, tzinfo=UTC)


def test_a_naive_clock_is_refused_rather_than_read_in_the_host_timezone() -> None:
    """`astimezone` on a naive stamp assumes LOCAL time, which moves the day boundary
    this function exists to name. Same refusal `sample_bucket` makes."""
    with pytest.raises(ValueError, match="timezone-aware"):
        earliest_possible_verdict_at(
            observed_trading_days=1,
            last_common_observed_date=date(2026, 8, 25),
            verdict_window_close_date=None,
            now=datetime(2026, 9, 14, 0, 46),
        )


def test_the_frozen_construction_is_pinned_to_its_version() -> None:
    """The reader that makes `CORE_SELECTION_VERDICT_BOUND_VERSION` mean something.

    No published formulation exists for "when can a five-common-session window close",
    so `.claude/CLAUDE.md` requires the rule be fixed BY CONSTRUCTION and frozen in a
    version. A version nothing checks is decoration: this test is the check. The four
    clauses below ARE v1 -- changing any of them must move the version string, and
    that is the point of failing here when it does not.
    """
    assert CORE_SELECTION_VERDICT_BOUND_VERSION == "core-verdict-bound-v1"
    friday = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)
    # 1. weekends are skipped; 2. the walk starts no earlier than today;
    # 3. the boundary is the 00:00 UTC AFTER the fifth session.
    assert earliest_possible_verdict_at(
        observed_trading_days=1,
        last_common_observed_date=date(2026, 8, 25),
        verdict_window_close_date=None,
        now=friday,
    ) == datetime(2026, 9, 17, tzinfo=UTC), "Fri 11th + Mon-Thu = four sessions, boundary the 17th"
    # 4. a closed window ignores the walk entirely.
    assert earliest_possible_verdict_at(
        observed_trading_days=1,
        last_common_observed_date=date(2026, 8, 25),
        verdict_window_close_date=date(2026, 9, 4),
        now=friday,
    ) == datetime(2026, 9, 5, tzinfo=UTC)
