from __future__ import annotations

from datetime import date, timedelta

import pytest

from scripts.schedule13d_statistics import (
    EventOutcome,
    PairedDifference,
    holm_adjust,
    paired_clustered_difference_test,
    summarise_outcomes,
    two_way_pigeonhole_bootstrap,
)


def _outcome(index: int, value: float, *, issuer: str | None = None, entry: date | None = None) -> EventOutcome:
    start = entry or date(2026, 1, index + 2)
    return EventOutcome(
        accession_number=f"a-{index}",
        issuer_cik=issuer or f"issuer-{index}",
        entry_date=start,
        exit_date=start + timedelta(days=14),
        net_return_pct=value,
        maximum_percent_of_class=float(index + 5),
        sector="technology" if index % 2 else "healthcare",
    )


def test_two_way_bootstrap_is_deterministic_and_cluster_effective_n_is_bounded() -> None:
    outcomes = tuple(_outcome(i, float(i - 2)) for i in range(6))
    first = two_way_pigeonhole_bootstrap(outcomes, resamples=200)
    second = two_way_pigeonhole_bootstrap(outcomes, resamples=200)
    assert first == second
    assert 0 < first.effective_sample_size <= len(outcomes)
    assert first.lower_95_pct < first.mean_pct < first.upper_95_pct


def test_summary_reports_tails_concentration_stability_and_break_even_cost() -> None:
    outcomes = (
        _outcome(0, -4.0, issuer="shared", entry=date(2025, 2, 3)),
        _outcome(1, 1.0, issuer="shared", entry=date(2025, 8, 4)),
        _outcome(2, 2.0, entry=date(2026, 2, 3)),
        _outcome(3, 3.0, entry=date(2026, 3, 3)),
    )
    summary = summarise_outcomes(outcomes, resamples=200)
    assert summary.hit_rate_pct == 75
    assert summary.profit_factor == pytest.approx(1.5)
    assert summary.expected_shortfall_5_pct == -4
    assert summary.worst_net_event_return_pct == -4
    assert summary.break_even_cost_bps == 100
    assert summary.maximum_concurrency >= 1
    assert len(summary.stability) == 4


def test_nonpositive_book_fails_concentration_safely() -> None:
    summary = summarise_outcomes((_outcome(0, -1), _outcome(1, -2)), resamples=100)
    assert summary.maximum_issuer_positive_concentration_pct == 100
    assert summary.maximum_entry_session_positive_concentration_pct == 100


def test_paired_clustered_difference_is_deterministic_and_one_sided() -> None:
    differences = tuple(
        PairedDifference(f"a-{index}", f"issuer-{index}", date(2026, 1, index + 2), value)
        for index, value in enumerate((1.0, 1.5, 2.0, 2.5, 3.0, 3.5))
    )
    first = paired_clustered_difference_test(differences, resamples=500)
    second = paired_clustered_difference_test(differences, resamples=500)
    assert first == second
    assert first.mean_difference_pct == 2.25
    assert first.lower_95_pct > 0
    assert first.one_sided_p_value < 0.05


def test_holm_adjust_is_monotone_in_sorted_p_values_and_restores_order() -> None:
    adjusted = holm_adjust((0.03, 0.001, 0.02, 0.5))
    assert adjusted == pytest.approx((0.06, 0.004, 0.06, 0.5))
    assert holm_adjust((0.01,)) == (0.01,)
    with pytest.raises(ValueError):
        holm_adjust((float("nan"),))
