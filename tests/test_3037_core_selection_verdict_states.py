"""#3037: every terminal state of #2833's verdict must be nameable.

Pure-logic and table-driven against :func:`classify_core_selection`.  The combinations
that decide precedence -- a ``pass`` whose OTHER candidate is missing, a ``cash`` carrying
an instrument id, an undeclared outcome with a stray evidence ref -- are unreachable from
a realistic DB fixture, which is why the classifier is a separate pure function at all.
"""

from __future__ import annotations

import pathlib
from datetime import UTC, datetime

import pytest

from app.services.strategy_core_selection import (
    CORE_SELECTION_CANDIDATE_IDS,
    SELECTED_CORE_EVIDENCE_REF,
    SELECTED_CORE_INSTRUMENT_ID,
    SELECTED_CORE_OUTCOME,
    classify_core_selection,
)

_US = "us_equity"
_UK = "uk_equity"
_OPENS = datetime(2026, 9, 18, tzinfo=UTC)
_BEFORE = datetime(2026, 9, 17, 23, 59, 59, tzinfo=UTC)
_AFTER = datetime(2026, 9, 18, 0, 0, 1, tzinfo=UTC)
_REF = "docs/proposals/ta/2026-09-18-core-selection-result.json"
_PASS_ID = CORE_SELECTION_CANDIDATE_IDS[0]
_UK_ID = CORE_SELECTION_CANDIDATE_IDS[1]


def _classify(
    *,
    outcome: object = None,
    instrument_id: int | None = None,
    evidence_ref: str | None = None,
    asset_class: str | None = _US,
    symbol: str | None = "SPY.RTH",
    missing: tuple[int, ...] = (),
    window_closed: bool = True,
    now: datetime = _AFTER,
):
    return classify_core_selection(
        outcome=outcome,
        instrument_id=instrument_id,
        evidence_ref=evidence_ref,
        selected_asset_class=asset_class,
        selected_symbol=symbol,
        missing_candidate_ids=missing,
        verdict_window_closed=window_closed,
        verdict_window_open_at=_OPENS,
        now=now,
    )


def test_the_shipped_constants_are_never_half_written() -> None:
    """The invariant that outlived "this PR records no verdict".

    Until 2026-09-18 this asserted all three constants were ``None``, because a shipped
    non-None value would have BEEN the adoption. #2833's verdict has since opened at its
    declared boundary and been transcribed, so that literal assertion has expired — but
    the thing it was protecting has not. All three are hand-edited, and a partially
    applied edit is the dangerous state: ``classify_core_selection`` has a distinct
    refusal for each half, and every one of those refusals reads as a configuration
    fault on the operator's surface.

    What the constants SAY is checked against the committed evidence payload in
    ``tests/test_2833_transcribed_verdict.py`` — deliberately not here, because
    restating the literals in a second file would pass just as happily if both were
    typed wrong together.
    """
    written = [
        value is not None for value in (SELECTED_CORE_OUTCOME, SELECTED_CORE_INSTRUMENT_ID, SELECTED_CORE_EVIDENCE_REF)
    ]
    assert len(set(written)) == 1, (
        "the #2833 verdict constants are half-written: outcome="
        f"{SELECTED_CORE_OUTCOME!r} instrument={SELECTED_CORE_INSTRUMENT_ID!r} "
        f"evidence_ref={SELECTED_CORE_EVIDENCE_REF!r}"
    )


# ---------------------------------------------------------------- the state table


def test_an_open_window_is_collecting() -> None:
    assert _classify(window_closed=False, now=_AFTER).state == "evidence_collecting"


def test_a_closed_window_before_its_boundary_is_still_collecting() -> None:
    """The boundary is the verifier's own ``opens_at``; a second early is not open."""
    assert _classify(now=_BEFORE).state == "evidence_collecting"


def test_a_closed_window_exactly_at_its_boundary_is_awaiting_the_verdict() -> None:
    """``>=``, matching ``verify_2833_core_selection.evaluate``'s ``now < opens_at``."""
    assert _classify(now=_OPENS).state == "awaiting_verdict"


def test_a_closed_window_past_its_boundary_is_awaiting_the_verdict() -> None:
    verdict = _classify(now=_AFTER)
    assert verdict.state == "awaiting_verdict"
    assert verdict.declared_outcome is None
    assert verdict.configuration_error is None


def test_a_pass_naming_a_session_checkable_candidate_is_ready() -> None:
    verdict = _classify(outcome="pass", instrument_id=_PASS_ID, evidence_ref=_REF)
    assert (verdict.state, verdict.declared_outcome, verdict.configuration_error) == ("ready", "pass", None)


def test_a_cash_verdict_is_terminal_and_not_an_error() -> None:
    """The defect this ticket exists for: `cash` used to render as a misconfiguration."""
    verdict = _classify(outcome="cash", instrument_id=None, evidence_ref=_REF)
    assert (verdict.state, verdict.declared_outcome, verdict.configuration_error) == ("cash", "cash", None)


def test_a_cash_verdict_survives_a_missing_candidate_row() -> None:
    """An instruments-table gap must not RETRACT a completed study.

    Coverage is undescribable, but the verdict already answered "no sleeve" -- there is
    nothing about a candidate row that the answer depends on any more.
    """
    assert _classify(outcome="cash", evidence_ref=_REF, missing=(_UK_ID,)).state == "cash"


# ------------------------------------------------- precedence: rows are NOT disjoint


def test_a_pass_whose_other_candidate_is_missing_refuses() -> None:
    """Matches rows 1 and 2; row 1 must win."""
    verdict = _classify(outcome="pass", instrument_id=_PASS_ID, evidence_ref=_REF, missing=(_UK_ID,))
    assert verdict.state == "unavailable"
    assert verdict.configuration_error is not None
    assert str(_UK_ID) in verdict.configuration_error


def test_a_cash_verdict_carrying_an_instrument_id_refuses() -> None:
    """Matches rows 1 and 3; row 1 must win."""
    verdict = _classify(outcome="cash", instrument_id=_PASS_ID, evidence_ref=_REF)
    assert verdict.state == "unavailable"
    assert verdict.configuration_error is not None
    assert "also names instrument" in verdict.configuration_error


def test_an_undeclared_outcome_with_a_stray_ref_refuses_rather_than_awaiting() -> None:
    """Matches rows 1 and 4; row 1 must win.

    Without this, a half-written verdict reads as "openable, nothing recorded" -- the one
    state that tells the operator there is no transcription to check.
    """
    verdict = _classify(evidence_ref=_REF, now=_AFTER)
    assert verdict.state == "unavailable"
    assert verdict.declared_outcome is None


# ------------------------------------------------------------- consistency diagnostics


@pytest.mark.parametrize(
    ("kwargs", "expected_fragment"),
    [
        ({"outcome": "pas", "evidence_ref": _REF}, "unrecognised outcome"),
        ({"outcome": "PASS", "evidence_ref": _REF}, "unrecognised outcome"),
        ({"instrument_id": _PASS_ID}, "without naming the verdict outcome"),
        ({"outcome": "pass", "instrument_id": _PASS_ID}, "with no evidence ref"),
        ({"outcome": "pass", "instrument_id": _PASS_ID, "evidence_ref": "   "}, "with no evidence ref"),
        ({"outcome": "cash"}, "with no evidence ref"),
        ({"outcome": "pass", "instrument_id": 999999, "evidence_ref": _REF}, "not one of #2833's declared"),
        ({"outcome": "pass", "evidence_ref": _REF}, "not one of #2833's declared"),
        ({"outcome": "pass", "instrument_id": _PASS_ID, "evidence_ref": _REF, "symbol": None}, "no coverage row"),
    ],
)
def test_each_inconsistency_refuses_with_its_own_message(kwargs: dict, expected_fragment: str) -> None:
    verdict = _classify(**kwargs)
    assert verdict.state == "unavailable"
    assert verdict.configuration_error is not None
    assert expected_fragment in verdict.configuration_error


def test_an_unrecognised_outcome_never_falls_through_to_a_healthy_state() -> None:
    """``Literal`` is a typecheck-time claim only; a typo must refuse at runtime.

    The dangerous direction is silence: a typo'd outcome that fell through to
    ``evidence_collecting`` would report a countdown that has already finished.
    """
    verdict = _classify(outcome="pas", evidence_ref=_REF)
    assert verdict.state == "unavailable"
    assert verdict.declared_outcome is None


@pytest.mark.parametrize("unexecutable_id", list(CORE_SELECTION_CANDIDATE_IDS[1:]))
def test_a_pass_naming_an_unsession_checkable_venue_refuses_but_keeps_its_outcome(unexecutable_id: int) -> None:
    """The two axes coming apart — the reason ``declared_outcome`` exists.

    A reviewed ``pass`` on an LSE candidate cannot operate (#2603/#2312), but a surface
    that reported only ``unavailable`` could not say a verdict had been reached at all.
    """
    verdict = _classify(
        outcome="pass",
        instrument_id=unexecutable_id,
        evidence_ref=_REF,
        asset_class=_UK,
        symbol="CSPX.L",
    )
    assert verdict.state == "unavailable"
    assert verdict.declared_outcome == "pass"
    assert verdict.configuration_error is not None
    assert "CSPX.L" in verdict.configuration_error


# ------------------------------------------------------------------ the ready predicate


@pytest.mark.parametrize("instrument_id", list(CORE_SELECTION_CANDIDATE_IDS))
@pytest.mark.parametrize("evidence_ref", [None, "", "   ", _REF])
@pytest.mark.parametrize("asset_class", [None, _US, _UK])
@pytest.mark.parametrize("symbol", [None, "SPY.RTH"])
def test_ready_is_equivalent_to_the_pre_3037_predicate(
    instrument_id: int,
    evidence_ref: str | None,
    asset_class: str | None,
    symbol: str | None,
) -> None:
    """Adding a required ``outcome`` term must not NARROW who reaches ``ready``.

    The pre-#3037 predicate was: the id is a declared candidate, a coverage row exists,
    the evidence ref is non-blank, and the venue is session-checkable. With
    ``outcome="pass"`` supplied, the new classifier must agree with it on every input --
    otherwise mandate enablement, pool activation and both executor selection checks all
    silently narrow.
    """
    legacy_ready = (
        instrument_id in CORE_SELECTION_CANDIDATE_IDS
        and symbol is not None
        and evidence_ref is not None
        and bool(evidence_ref.strip())
        and asset_class == _US
    )
    verdict = _classify(
        outcome="pass",
        instrument_id=instrument_id,
        evidence_ref=evidence_ref,
        asset_class=asset_class,
        symbol=symbol,
    )
    assert (verdict.state == "ready") is legacy_ready


def test_the_recovery_fixture_restores_every_constant_it_sets() -> None:
    """A leaked ``SELECTED_CORE_OUTCOME`` is a declaration with no instrument id.

    Caught at Codex checkpoint 2: `select_core_instrument` sets three constants, and both
    `core_world` fixtures registered only two for restoration — so every later test in the
    process ran against a half-written verdict and read `unavailable`.
    """
    import inspect

    from tests.fixtures import core_restart

    source = inspect.getsource(core_restart.select_core_instrument)
    assigned = {
        name
        for name in ("SELECTED_CORE_OUTCOME", "SELECTED_CORE_INSTRUMENT_ID", "SELECTED_CORE_EVIDENCE_REF")
        if f"strategy_core_selection.{name} =" in source
    }
    for module in ("tests/test_2949_core_restart_recovery_db.py", "tests/test_2949_core_close_recovery_db.py"):
        text = pathlib.Path(module).read_text(encoding="utf-8")
        restored = {name for name in assigned if f'"{name}", None, raising=False' in text}
        assert restored == assigned, f"{module} does not restore {sorted(assigned - restored)}"


def test_no_state_other_than_ready_authorises_the_sleeve() -> None:
    """The invariant every execution path keys on, asserted over the reachable set."""
    reachable = [
        _classify(window_closed=False),
        _classify(now=_AFTER),
        _classify(outcome="cash", evidence_ref=_REF),
        _classify(outcome="pass", instrument_id=_PASS_ID, evidence_ref=_REF, asset_class=_UK, symbol="CSPX.L"),
        _classify(outcome="pass", instrument_id=_PASS_ID, evidence_ref=_REF),
    ]
    assert [verdict.state for verdict in reachable].count("ready") == 1
    assert {verdict.state for verdict in reachable} == {
        "evidence_collecting",
        "awaiting_verdict",
        "cash",
        "unavailable",
        "ready",
    }
