"""Pure tests for the #2947 feasibility loader and its CLI.

No database: the projection and the argument parsing are pure over their inputs,
which is the whole reason the loader was split out of the screen. The SQL's
ORDERING is NOT tested here -- a pure test cannot exercise it. That lives in
``tests/test_2947_feasibility_loader_db.py``, which asserts returned proof ids
over deliberately conflicting rows.
"""

from __future__ import annotations

import pathlib
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from app.services.portfolio_feasibility import AccountScope
from app.services.portfolio_feasibility_loader import (
    FeasibilityLoaderError,
    load_leg_eligibility,
    project_leg_eligibility,
)
from scripts.screen_portfolio_feasibility import (
    EXIT_CONFIG,
    ConfigError,
    _build_parser,
    _decimal,
    _parse_legs,
    main,
)

_OBSERVED = datetime(2026, 8, 22, 21, 24, 1, tzinfo=UTC)


def _scope() -> AccountScope:
    return AccountScope(
        provider="etoro",
        environment="demo",
        operator_id=uuid4(),
        api_key_credential_id=uuid4(),
        user_key_credential_id=uuid4(),
    )


def _row(
    *,
    instrument_id: int = 3417,
    proof_id: int = 21,
    verdict: str = "underlying",
    reason_code: str | None = None,
    arm_count: int = 1,
    allow_open: bool | None = True,
    exposure: object = Decimal("10"),
    amount: object = Decimal("25"),
) -> tuple[object, ...]:
    """One ``_LATEST_IN_SCOPE_SQL`` row, in its column order."""
    return (
        instrument_id,
        proof_id,
        _OBSERVED,
        verdict,
        reason_code,
        "usd",
        arm_count,
        allow_open,
        exposure,
        amount,
        "core-eligibility-v1",
    )


# ---------------------------------------------------------------------------
# Projection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("recorded", [True, False, None])
def test_allow_open_position_keeps_all_three_states_distinct(recorded: bool | None) -> None:
    """``None`` is "not recorded" -- neither false nor true.

    Coercing it is a wrong answer in one of the two dangerous directions: to
    ``False`` it invents a broker refusal, to ``True`` it invents permission.
    """
    leg, evidence = project_leg_eligibility(_row(allow_open=recorded), _scope())
    assert leg.allow_open_position is recorded
    assert evidence.allow_open_position is recorded


def test_the_two_minimums_stay_separate_and_are_not_swapped() -> None:
    """Distinct VALUES, so a transposition cannot pass by both being non-null."""
    leg, evidence = project_leg_eligibility(_row(exposure=Decimal("10"), amount=Decimal("25")), _scope())
    assert leg.min_position_exposure == Decimal("10")
    assert leg.min_position_amount == Decimal("25")
    assert evidence.min_position_exposure == Decimal("10")
    assert evidence.min_position_amount == Decimal("25")


def test_a_missing_minimum_stays_none_and_is_never_coerced_to_zero() -> None:
    leg, _ = project_leg_eligibility(_row(exposure=None, amount=None), _scope())
    assert leg.min_position_exposure is None
    assert leg.min_position_amount is None


def test_an_unresolved_verdict_keeps_its_reason_while_the_arm_count_is_zero() -> None:
    """The defect this projection exists to avoid.

    ``evaluate_core_eligibility`` leaves ``qualifying_arm_count`` at ``0`` for an
    ``unresolved`` proof because it never evaluated an arm.  A projection carrying
    only the count cannot tell that apart from a broker that answered "no", and
    would report a definitive refusal where the truth is "the response did not
    answer the question".
    """
    leg, _ = project_leg_eligibility(
        _row(verdict="unresolved", reason_code="instrument_not_resolved", arm_count=0, allow_open=None),
        _scope(),
    )
    assert leg.verdict == "unresolved"
    assert leg.reason_code == "instrument_not_resolved"
    assert leg.qualifying_arm_count == 0


def test_projection_carries_the_proof_id_so_the_selected_row_is_identifiable() -> None:
    """Without it an artifact cannot distinguish a correct selection from a
    plausible-looking wrong one."""
    _, evidence = project_leg_eligibility(_row(proof_id=77), _scope())
    assert evidence.proof_id == 77


# ---------------------------------------------------------------------------
# Loader keying
# ---------------------------------------------------------------------------


class _StubConn:
    """Returns the given rows for the first query, none for the follow-up."""

    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._results = [rows, []]

    def execute(self, _sql: str, _params: object = None) -> _StubConn:
        self._current = self._results.pop(0) if self._results else []
        return self

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._current


def test_the_mapping_is_keyed_by_the_rows_own_instrument_id() -> None:
    """⚠⚠ The screen does NOT verify that its mapping key matches the proof it
    holds -- ``{1: proof(instrument_id=999)}`` returns ``feasible`` today.  Keying
    off the row is what makes that unreachable from this path, so assert it.
    """
    loaded = load_leg_eligibility(
        _StubConn([_row(instrument_id=3434)]),  # type: ignore[arg-type]
        instrument_ids=[3434],
        scope=_scope(),
    )
    assert set(loaded.by_instrument) == {3434}
    assert loaded.by_instrument[3434].instrument_id == 3434
    assert loaded.evidence[3434].instrument_id == 3434


def test_an_instrument_with_no_proof_is_absent_rather_than_fabricated() -> None:
    """Never invent an ``unresolved`` projection for an instrument never asked
    about: ``unresolved`` is a real recorded verdict meaning "the broker's response
    did not answer", not "we did not ask"."""
    loaded = load_leg_eligibility(
        _StubConn([_row(instrument_id=3417)]),  # type: ignore[arg-type]
        instrument_ids=[3417, 3434],
        scope=_scope(),
    )
    assert set(loaded.by_instrument) == {3417}
    assert 3434 not in loaded.evidence


def test_no_instruments_requested_is_a_loader_error() -> None:
    with pytest.raises(FeasibilityLoaderError):
        load_leg_eligibility(_StubConn([]), instrument_ids=[], scope=_scope())  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# CLI parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw", ["NaN", "-NaN", "Infinity", "-Infinity", "sNaN"])
def test_non_finite_numbers_are_rejected_at_parse(raw: str) -> None:
    """``Decimal`` accepts all of these, and Postgres NUMERIC NaN does not compare
    like IEEE (``'NaN' >= 0`` is TRUE), so admitting one is a known trap."""
    with pytest.raises(ConfigError):
        _decimal(raw, field="--assigned-capital")


def test_a_decimal_is_parsed_from_the_literal_string_without_a_float_hop() -> None:
    """``Decimal(float("0.1"))`` is not ``Decimal("0.1")`` -- a weight is a
    declaration and must not be silently renormalised."""
    assert _decimal("0.1", field="--leg") == Decimal("0.1")
    assert str(_decimal("1.000", field="--leg")) == "1.000"


def _args(argv: list[str]):  # noqa: ANN202 -- argparse.Namespace
    return _build_parser().parse_args(
        [
            *argv,
            "--assigned-capital",
            "1000",
            "--capital-currency",
            "USD",
            "--cash-reserve-fraction",
            "0.02",
            "--out",
            "/dev/null",
        ]
    )


def test_leg_and_equal_weight_are_mutually_exclusive_and_one_is_required() -> None:
    with pytest.raises(ConfigError):
        _parse_legs(_args(["--leg", "3417:1", "--equal-weight", "3417,3434"]))
    with pytest.raises(ConfigError):
        _parse_legs(_args([]))


def test_equal_weight_sums_to_exactly_one() -> None:
    """Exactly, not within tolerance.  The screen has a tolerance, but leaning on
    it would put an arithmetic artefact into a DECLARATION."""
    legs = _parse_legs(_args(["--equal-weight", "3417,3434,3075"]))
    assert len(legs) == 3
    assert sum((w for _, w in legs), Decimal(0)) == Decimal(1)


def test_duplicate_instrument_ids_are_rejected_in_both_forms() -> None:
    with pytest.raises(ConfigError):
        _parse_legs(_args(["--leg", "3417:0.5", "--leg", "3417:0.5"]))
    with pytest.raises(ConfigError):
        _parse_legs(_args(["--equal-weight", "3417,3417"]))


@pytest.mark.parametrize("spec", ["3417", "notanid:1", "3417:notaweight", "3417:NaN"])
def test_a_malformed_leg_spec_is_a_configuration_error(spec: str) -> None:
    with pytest.raises(ConfigError):
        _parse_legs(_args(["--leg", spec]))


# ---------------------------------------------------------------------------
# Exit codes are a VERDICT vocabulary — no non-verdict failure may borrow one
# ---------------------------------------------------------------------------


def test_a_bad_flag_exits_config_not_indeterminate() -> None:
    """⚠ ``argparse`` exits 2 by default, which is this script's ``indeterminate``.

    Left alone, a typo'd option reads as a feasibility finding about the
    portfolio. Fails before any connection is attempted.
    """
    assert main(["--no-such-flag"]) == EXIT_CONFIG


def test_help_still_exits_zero() -> None:
    """The remap above must not swallow ``--help``, which legitimately exits 0."""
    with pytest.raises(SystemExit) as raised:
        main(["--help"])
    assert raised.value.code in (0, None)


def test_a_failed_run_removes_a_stale_artifact(tmp_path: pathlib.Path) -> None:
    """⚠ Atomic replacement does not cover this: a failed run writes nothing, so a
    PREVIOUS artifact survives at the same path looking like this run's result.
    "No artifact" is the honest state.
    """
    artifact = tmp_path / "previous.json"
    artifact.write_text('{"report": "from an earlier run"}', encoding="utf-8")

    # Duplicate legs fail in argument handling, before any connection.
    code = main(
        [
            "--leg",
            "3417:0.5",
            "--leg",
            "3417:0.5",
            "--assigned-capital",
            "1000",
            "--capital-currency",
            "USD",
            "--cash-reserve-fraction",
            "0.02",
            "--out",
            str(artifact),
        ]
    )
    assert code == EXIT_CONFIG
    assert not artifact.exists()


def test_an_unexpected_error_still_removes_the_stale_artifact(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """⚠ The discard must be UNCONDITIONAL, not a property of the handled-exception
    list. An error nobody anticipated would otherwise skip it and leave the
    previous artifact standing as this run's result -- and the traceback must still
    surface, so the exception is re-raised rather than swallowed.
    """
    artifact = tmp_path / "previous.json"
    artifact.write_text('{"report": "from an earlier run"}', encoding="utf-8")

    def _boom(_args: object) -> None:
        raise RuntimeError("an error nobody anticipated")

    monkeypatch.setattr("scripts.screen_portfolio_feasibility._parse_legs", _boom)

    with pytest.raises(RuntimeError, match="nobody anticipated"):
        main(
            [
                "--leg",
                "3417:1",
                "--assigned-capital",
                "1000",
                "--capital-currency",
                "USD",
                "--cash-reserve-fraction",
                "0.02",
                "--out",
                str(artifact),
            ]
        )
    assert not artifact.exists()
