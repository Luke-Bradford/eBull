from __future__ import annotations

from datetime import date
from decimal import Decimal

from scripts.schedule13d_challengers import (
    MatchFeatures,
    exact_stratum,
    match_initial_13g_without_replacement,
    select_random_time_sessions,
)


def _features(
    accession: str,
    *,
    rule: str | None = None,
    price: str = "25",
    entry: date = date(2026, 2, 3),
) -> MatchFeatures:
    return MatchFeatures(
        accession_number=accession,
        issuer_cik=accession,
        filing_date=entry,
        entry_date=entry,
        entry_price=Decimal(price),
        trailing_median_dollar_volume=Decimal("25000000"),
        prior_20_market_return_pct=Decimal("0"),
        rule=rule,  # type: ignore[arg-type]
    )


def test_exact_stratum_uses_right_closed_edge_assignment() -> None:
    assert exact_stratum(_features("a", price="25"))[2:] == (3, 2, 2)
    assert exact_stratum(_features("a", price="24.99"))[2:] == (2, 2, 2)
    month_end = _features("a", entry=date(2026, 2, 2))
    month_end = MatchFeatures(**{**month_end.__dict__, "filing_date": date(2026, 1, 30)})
    assert exact_stratum(month_end)[:2] == (2026, 1)


def test_13g_matching_is_deterministic_without_replacement_or_rule_pooling() -> None:
    treatments = (_features("t-2"), _features("t-1"))
    challengers = (
        _features("c-1", rule="1b"),
        _features("c-2", rule="1b"),
        _features("c-unknown", rule="unknown"),
    )
    first = match_initial_13g_without_replacement(treatments, challengers, rule="1b")
    second = match_initial_13g_without_replacement(tuple(reversed(treatments)), challengers, rule="1b")
    assert first == second
    assert len(first) == 2
    assert len({item.challenger_accession for item in first}) == 2
    assert "c-unknown" not in {item.challenger_accession for item in first}


def test_random_time_selection_is_seeded_and_retains_empty_as_unmatched() -> None:
    candidates = {
        "t-1": (date(2026, 2, 3), date(2026, 2, 4), date(2026, 2, 5)),
        "t-2": (date(2026, 2, 6),),
    }
    prohibited = {"t-1": {date(2026, 2, 4)}, "t-2": {date(2026, 2, 6)}}
    first = select_random_time_sessions(candidates, prohibited)
    assert first == select_random_time_sessions(candidates, prohibited)
    assert first["t-1"] in {date(2026, 2, 3), date(2026, 2, 5)}
    assert "t-2" not in first
