"""Pure tests for #2508's decision-context input census.

No database. The census's own SQL is exercised by running it; what is tested here is
the part that decides the READOUT — the availability rules and the VIX prior-session
step — because a wrong rule produces a plausible number rather than an error.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from app.services.strategy_decision_context import DEFINITION
from scripts.census_2508_decision_context_inputs import missing_inputs, vix_refusals

LOOKBACK = DEFINITION.volume_lookback_sessions


def row(**overrides: Any) -> dict[str, Any]:
    """A decision whose every input resolves; each test removes exactly one."""
    base: dict[str, Any] = {
        "has_classification": True,
        "has_security_type": True,
        "has_listing": True,
        "has_industry": True,
        "has_decision_close": True,
        "has_unadjusted_basis": True,
        "window_usable_sessions": LOOKBACK,
        "mean_share_volume": Decimal("1000"),
        "has_decision_volume": True,
        "has_prior_close": True,
        "has_decision_open": True,
        "has_stored_volatility": True,
        "intraday_bars": 40,
        "spread_observations": 3,
        "has_current_quote_row": True,
    }
    base.update(overrides)
    return base


def test_only_the_undefined_z_is_unavailable_by_construction() -> None:
    """⚠⚠ EXACTLY ONE INPUT IS HARD-CODED, AND THE TEST EXISTS TO KEEP IT AT ONE.

    A first draft hard-coded ``spread_bps`` too and reported the guaranteed zero as a
    finding; ``sql/306``'s panel store falsified that. ``market_sector_residual_z`` is the
    only remaining constant, and it is constant because no ``_z`` is DEFINED anywhere —
    not because no arithmetic exists.

    ⚠ WHAT THIS DOES AND DOES NOT CATCH (Codex ckpt-2, P3). It fails if a SECOND input is
    hard-coded unavailable, which is the mistake that was actually made. It CANNOT notice
    a residual-z producer appearing, because both this fixture and the census would keep
    returning the same constant — nothing in a pure test observes the repo. Closing that
    would need availability to come from a source query, and there is no source to query;
    so the guard is the reviewer's, and this test's job is to make the assumption
    impossible to miss rather than to detect its expiry.
    """
    assert missing_inputs(row(), vix_refusal=None) == ("market_sector_residual_z",)


def test_the_required_input_set_is_the_contract_s_not_a_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The census must fail LOUDLY if ``_REQUIRED_INPUTS`` gains or loses a member.

    The failure mode this guards is silent under-reporting: a new required input that
    the census does not know about would simply never appear in the taxonomy, and the
    readout would claim more decisions are attributable than the CHECK constraint
    admits.
    """
    import scripts.census_2508_decision_context_inputs as census

    monkeypatch.setattr(census, "_REQUIRED_INPUTS", (*census._REQUIRED_INPUTS, "a_newly_required_input"))
    with pytest.raises(AssertionError, match="required-input set drifted"):
        missing_inputs(row(), vix_refusal=None)


@pytest.mark.parametrize(
    ("override", "expected"),
    [
        ({"has_decision_close": False}, "as_traded_price"),
        ({"has_unadjusted_basis": False}, "as_traded_price_basis"),
        ({"intraday_bars": 0}, "intraday_coverage"),
        ({"spread_observations": 0}, "spread_bps"),
        ({"has_stored_volatility": False}, "realised_volatility"),
        ({"has_decision_open": False}, "gap_pct"),
        ({"has_prior_close": False}, "gap_pct"),
        ({"has_classification": False}, "point_in_time_classification"),
        ({"has_security_type": False}, "security_type"),
        ({"has_listing": False}, "primary_listing_market"),
        ({"has_industry": False}, "provider_industry_id"),
    ],
)
def test_each_unavailable_input_is_named(override: dict[str, Any], expected: str) -> None:
    assert expected in missing_inputs(row(**override), vix_refusal=None)


def test_a_missing_classification_does_not_also_report_its_parts() -> None:
    """``build_decision_context`` appends the parts only in the ``else`` branch.

    Reporting both would double-count one gap and make the per-input column sum to more
    than the refusal count.
    """
    names = missing_inputs(
        row(has_classification=False, has_security_type=False, has_industry=False),
        vix_refusal=None,
    )
    assert "point_in_time_classification" in names
    assert "security_type" not in names
    assert "provider_industry_id" not in names


def test_the_vix_refusal_is_appended_under_its_typed_name() -> None:
    """``build_decision_context`` appends ``f"vix_{reason}"``, never a bare ``vix``."""
    stale = "vix_stale_source:2026-09-07<expected:2026-09-04"
    assert "vix_missing_source" in missing_inputs(row(), vix_refusal="vix_missing_source")
    assert stale in missing_inputs(row(), vix_refusal=stale)
    assert not any(name.startswith("vix") for name in missing_inputs(row(), vix_refusal=None))


@pytest.mark.parametrize("sessions", [0, 1, LOOKBACK - 1])
def test_a_short_window_refuses_every_volume_input(sessions: int) -> None:
    names = missing_inputs(row(window_usable_sessions=sessions), vix_refusal=None)
    for name in (
        "trailing_mean_share_volume",
        "trailing_median_share_volume",
        "trailing_mean_dollar_volume",
        "trailing_median_dollar_volume",
        "zero_volume_frequency",
        "relative_volume",
    ):
        assert name in names


def test_a_full_window_with_one_unusable_session_still_refuses() -> None:
    """``sql/304`` wants ``lookback`` COMPLETED sessions, not ``lookback`` rows.

    A bar carrying a NULL volume OR a non-positive close is a row the window contains and
    a session the baseline cannot use — dollar volume needs both legs — and averaging
    over the remainder would silently shorten the declared lookback.
    """
    names = missing_inputs(row(window_usable_sessions=LOOKBACK - 1), vix_refusal=None)
    assert "trailing_mean_share_volume" in names
    assert "trailing_mean_dollar_volume" in names


@pytest.mark.parametrize("mean", [None, Decimal("0")])
def test_relative_volume_needs_a_non_zero_denominator(mean: Decimal | None) -> None:
    names = missing_inputs(row(mean_share_volume=mean), vix_refusal=None)
    assert "relative_volume" in names


def test_relative_volume_also_needs_the_decision_bar_s_own_volume() -> None:
    names = missing_inputs(row(has_decision_volume=False), vix_refusal=None)
    assert "relative_volume" in names


@pytest.mark.parametrize(
    ("decision_day", "expected_prior"),
    [
        # Tuesday steps back to Monday.
        (date(2026, 9, 15), date(2026, 9, 14)),
        # Monday steps back over the weekend to Friday.
        (date(2026, 9, 14), date(2026, 9, 11)),
        # The session after US Independence Day 2025 (observed Friday 4 July) steps
        # back over the holiday AND the weekend to Thursday 3 July.
        (date(2025, 7, 7), date(2025, 7, 3)),
    ],
)
def test_the_prior_us_session_steps_over_weekends_and_closures(decision_day: date, expected_prior: date) -> None:
    assert vix_refusals(frozenset({expected_prior}), [decision_day])[decision_day] is None


def test_no_earlier_bar_at_all_is_a_missing_source_not_a_stale_one() -> None:
    day = date(2026, 9, 14)
    assert vix_refusals(frozenset({date(2026, 9, 21)}), [day])[day] == "vix_missing_source"


def test_a_close_one_session_too_early_is_not_a_fallback() -> None:
    """``load_decision_vix`` returns ``stale_source`` rather than the nearest bar."""
    day = date(2026, 9, 14)
    assert vix_refusals(frozenset({date(2026, 9, 10)}), [day])[day] == (
        "vix_stale_source:2026-09-10<expected:2026-09-11"
    )


def test_a_stray_non_session_bar_poisons_an_otherwise_available_date() -> None:
    """⚠ Membership is NOT the rule — the loader SELECTS before it compares.

    Friday's correct close is stored AND a Saturday row exists. ``load_vix_close_as_known``
    takes the latest bar before Monday, i.e. the Saturday, and ``load_decision_vix`` then
    refuses it against the expected prior session. A census testing only "is Friday in the
    set" reports this decision available, which is the optimistic direction. Codex
    reproduced this against the real loader at checkpoint 1, and correcting it moved 103
    real decisions (2026-09-08, the session after Labor Day) out of "available".
    """
    friday, saturday, monday = date(2026, 9, 11), date(2026, 9, 12), date(2026, 9, 14)
    assert vix_refusals(frozenset({friday}), [monday])[monday] is None
    assert vix_refusals(frozenset({friday, saturday}), [monday])[monday] == (
        "vix_stale_source:2026-09-12<expected:2026-09-11"
    )
