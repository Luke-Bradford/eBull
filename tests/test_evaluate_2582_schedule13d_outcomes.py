from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from scripts.evaluate_2582_schedule13d_outcomes import (
    _SOURCE_EVENTS_SQL,
    ACKNOWLEDGEMENT,
    OutcomeGateRefusal,
    PriceWindow,
    SourceEvent,
    bucket,
    match_tie_break,
    next_regular_session_strictly_after,
    nth_regular_session,
    regular_sessions_ending_before,
    require_outcome_gate,
    required_event_sessions,
    total_return_pct,
)


def test_gate_refuses_without_explicit_acknowledgement() -> None:
    with pytest.raises(OutcomeGateRefusal, match="remain closed"):
        require_outcome_gate(
            acknowledgement=None,
            contract_path=Path("docs/proposals/ta/contracts/schedule13d-public-catalyst-v1.json"),
        )


def test_source_population_uses_public_manifest_clock_and_cannot_read_prices() -> None:
    lowered = _SOURCE_EVENTS_SQL.lower()
    assert "sec_filing_manifest" in lowered
    assert "m.filed_at::date" in lowered
    assert "research_price_daily" not in lowered
    assert "b.filed_at" not in lowered
    assert "between %(first_source_date)s and %(last_complete_filing_date)s" in lowered


def test_gate_refuses_even_with_ack_until_trial_is_declared() -> None:
    with pytest.raises(OutcomeGateRefusal, match="absent from trial-register"):
        require_outcome_gate(
            acknowledgement=ACKNOWLEDGEMENT,
            contract_path=Path("docs/proposals/ta/contracts/schedule13d-public-catalyst-v1.json"),
        )


def test_next_session_is_strict_and_observes_weekend_and_holiday() -> None:
    assert next_regular_session_strictly_after(date(2026, 7, 2)) == date(2026, 7, 6)
    assert next_regular_session_strictly_after(date(2026, 8, 10)) == date(2026, 8, 11)
    assert nth_regular_session(date(2026, 7, 6), 10) == date(2026, 7, 17)


def test_total_return_uses_adjustment_factor_and_subtracts_adverse_cost() -> None:
    # Raw price halves, but the 2-for-1 adjustment factor doubles: zero gross.
    result = total_return_pct(
        entry_open=Decimal("100"),
        entry_close=Decimal("100"),
        entry_adj_close=Decimal("50"),
        exit_close=Decimal("50"),
        exit_adj_close=Decimal("50"),
    )
    assert result == Decimal("-0.500")


@pytest.mark.parametrize("bad", [Decimal("0"), Decimal("NaN"), Decimal("-1")])
def test_total_return_refuses_bad_adjustment_inputs(bad: Decimal) -> None:
    with pytest.raises(ValueError):
        total_return_pct(
            entry_open=Decimal("10"),
            entry_close=bad,
            entry_adj_close=Decimal("10"),
            exit_close=Decimal("11"),
            exit_adj_close=Decimal("11"),
        )


def test_matching_helpers_are_boundary_stable_and_deterministic() -> None:
    edges = tuple(map(Decimal, ("5", "10", "25")))
    assert bucket(Decimal("9.99"), edges) == 1
    assert bucket(Decimal("10"), edges) == 2
    assert match_tie_break("a", "b") == match_tie_break("a", "b")
    assert match_tie_break("a", "b") != match_tie_break("a", "c")


def _source_event(**changes: object) -> SourceEvent:
    values: dict[str, object] = {
        "accession_number": "0000000001-26-000001",
        "issuer_cik": "0000000001",
        "instrument_id": 42,
        "public_filing_date": date(2026, 7, 2),
        "maximum_percent_of_class": Decimal("7.5"),
        "prior_active": False,
        "prior_passive": False,
        "same_public_date_peer": False,
        "reporter_identity_complete": True,
        "current_security_eligible": True,
        "series_ids": (99,),
        "series_adjustment_bases": ("split_adjusted",),
    }
    values.update(changes)
    return SourceEvent(**values)  # type: ignore[arg-type]


def test_primary_source_refusal_is_explicit_and_fail_closed() -> None:
    assert _source_event().primary_source_refusal is None
    assert _source_event(prior_passive=True).primary_source_refusal == "prior_passive_chain"
    assert _source_event(series_ids=(1, 2)).primary_source_refusal == "research_series_missing_or_ambiguous"
    assert (
        _source_event(series_adjustment_bases=("raw",)).primary_source_refusal
        == "research_series_adjustment_basis_unexpected"
    )
    assert _source_event(reporter_identity_complete=False).primary_source_refusal == "reporter_identity_missing"


def test_required_sessions_are_exact_and_do_not_skip_market_dates() -> None:
    sessions = required_event_sessions(_source_event())
    assert len(sessions) == 70
    assert sessions[-10] == date(2026, 7, 6)
    assert sessions[-1] == date(2026, 7, 17)
    assert regular_sessions_ending_before(date(2026, 7, 6), 2) == (
        date(2026, 7, 1),
        date(2026, 7, 2),
    )


def _price_window(**changes: object) -> PriceWindow:
    values: dict[str, object] = {
        "event": _source_event(),
        "entry_date": date(2026, 7, 6),
        "exit_date": date(2026, 7, 17),
        "stock_bars_present": 70,
        "market_bars_present": 70,
        "positive_ohlcv_bars": 70,
        "positive_adjustment_bars": 70,
        "quarantine_covered_bars": 70,
        "return_usable": True,
        "entry_open": Decimal("10"),
        "entry_close": Decimal("10.1"),
        "entry_adj_close": Decimal("10.1"),
        "exit_close": Decimal("11"),
        "exit_adj_close": Decimal("11"),
        "trailing_median_dollar_volume": Decimal("20000000"),
        "prior_20_stock_return_pct": Decimal("2"),
        "prior_20_market_return_pct": Decimal("1"),
        "holding_market_return_pct": Decimal("0.5"),
    }
    values.update(changes)
    return PriceWindow(**values)  # type: ignore[arg-type]


def test_price_window_refuses_missing_exact_bar_before_assessing_return() -> None:
    assert _price_window().outcome_refusal is None
    assert _price_window(stock_bars_present=69).outcome_refusal == "exact_stock_session_missing"
    assert _price_window(quarantine_covered_bars=69).outcome_refusal == "quarantine_coverage_incomplete"
    assert (
        _price_window(positive_adjustment_bars=69).outcome_refusal
        == "corporate_action_adjustment_missing_or_nonpositive"
    )
