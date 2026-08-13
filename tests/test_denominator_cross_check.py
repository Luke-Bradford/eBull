"""Pure-logic table tests for the ownership denominator cross-check (#1647 part 5).

The decision core ``_classify_cross_check`` is pure (no DB) — these table-test the
band semantics, the dual-class subset bound, and the unavailable degradations. The
SQL readers (`_read_shares_outstanding_near`, `_sum_sibling_class_shares`) are
exercised end-to-end by the dev-verify step on the panel (repo lean-test posture);
one nearest-pick DB test guards the novel ORDER BY mechanism."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.ownership_rollup import (
    _DENOM_CROSS_AGREE_TOL,
    _DENOM_CROSS_DIVERGE_TOL,
    _classify_cross_check,
)

_P = date(2026, 3, 31)  # primary period
_C = date(2026, 3, 28)  # comparison period (3 days off)


def _single(primary: str, comparison: str):
    return _classify_cross_check(
        method="independent_concept",
        primary_value=Decimal(primary),
        primary_concept="dei:EntityCommonStockSharesOutstanding",
        primary_as_of=_P,
        comparison_value=Decimal(comparison),
        comparison_concept="us-gaap:CommonStockSharesOutstanding",
        comparison_as_of=_C,
    )


def _dual(sibling_sum: str, combined: str):
    return _classify_cross_check(
        method="per_class_subset_bound",
        primary_value=Decimal(sibling_sum),
        primary_concept="sum of resolved per-class FSDS counts (sibling instruments)",
        primary_as_of=date(2024, 12, 31),
        comparison_value=Decimal(combined),
        comparison_concept="us-gaap:CommonStockSharesOutstanding (combined all-class)",
        comparison_as_of=date(2024, 12, 31),
    )


# --- single-class band -------------------------------------------------------


def test_single_class_agrees_panel_like() -> None:
    # AAPL-like: dei 14,687,356,000 vs us-gaap 14,667,688,000 = +0.13%.
    cc = _single("14687356000", "14667688000")
    assert cc.method == "independent_concept"
    assert cc.status == "agrees"
    assert cc.pct_diff is not None and abs(cc.pct_diff) < Decimal("0.002")
    assert cc.as_of_delta_days == 3
    assert "reconciled" in cc.note


def test_single_class_minor_skew_at_three_percent() -> None:
    cc = _single("10300", "10000")  # +3%
    assert cc.status == "minor_skew"
    assert cc.pct_diff == Decimal("0.03")
    assert "skew" in cc.note


def test_single_class_diverges_on_two_x_denominator() -> None:
    # The #1646 class: denominator 2x too big (combined vs per-class) = +107%.
    cc = _single("12116000000", "5835000000")
    assert cc.status == "diverges"
    assert cc.pct_diff is not None and cc.pct_diff > Decimal("1")
    assert "suspect" in cc.note


def test_single_class_negative_diff_uses_magnitude() -> None:
    # primary 2% BELOW comparison → still agrees (band is on |pct_diff|).
    cc = _single("9800", "10000")
    assert cc.pct_diff == Decimal("-0.02")
    assert cc.status == "agrees"


def test_single_class_band_boundaries() -> None:
    # Exactly the agree tol → agrees; one tick over → minor_skew.
    assert _DENOM_CROSS_AGREE_TOL == Decimal("0.02")
    assert _DENOM_CROSS_DIVERGE_TOL == Decimal("0.05")
    assert _single("10200", "10000").status == "agrees"  # 0.02
    assert _single("10201", "10000").status == "minor_skew"  # 0.0201
    assert _single("10500", "10000").status == "minor_skew"  # 0.05
    assert _single("10501", "10000").status == "diverges"  # 0.0501


# --- dual-class subset bound -------------------------------------------------


def test_dual_class_plausible_alphabet_like() -> None:
    # ClassA 5,835M + ClassC 5,515M = 11,350M vs combined 12,116M → valid subset.
    cc = _dual("11350000000", "12116000000")
    assert cc.method == "per_class_subset_bound"
    assert cc.status == "plausible"
    assert cc.pct_diff is not None and cc.pct_diff < 0  # traded classes below total
    assert "subset" in cc.note


def test_dual_class_diverges_when_siblings_exceed_combined() -> None:
    # Impossible: traded classes exceed the all-class total → FSDS mis-resolution.
    cc = _dual("12500000000", "12116000000")
    assert cc.status == "diverges"
    assert "EXCEED" in cc.note


def test_dual_class_untraded_heavy_sweep_no_floor() -> None:
    # NO arbitrary floor (Codex BLOCKER): a 40% traded subset is just as "plausible"
    # as 95% — only the >100% overage diverges. Combined=100 for clarity.
    for sib in ("40", "50", "55", "95", "100"):
        assert _dual(sib, "100").status == "plausible", sib
    assert _dual("101", "100").status == "diverges"


# --- unavailable degradations ------------------------------------------------


def test_unavailable_on_missing_or_nonpositive() -> None:
    # primary None, comparison None, primary<=0, comparison<=0 all → unavailable
    # (never divides; Codex MED).
    assert (
        _classify_cross_check(
            method="independent_concept",
            primary_value=None,
            primary_concept="x",
            primary_as_of=_P,
            comparison_value=Decimal("10000"),
            comparison_concept="y",
            comparison_as_of=_C,
        ).status
        == "unavailable"
    )
    assert _single_value_none_comparison().status == "unavailable"
    assert _single("0", "10000").status == "unavailable"
    assert _single("10000", "0").status == "unavailable"


def _single_value_none_comparison():
    return _classify_cross_check(
        method="independent_concept",
        primary_value=Decimal("10000"),
        primary_concept="x",
        primary_as_of=_P,
        comparison_value=None,
        comparison_concept="y",
        comparison_as_of=_C,
    )


def test_unavailable_has_null_facts() -> None:
    cc = _single("0", "10000")
    assert cc.method == "unavailable"
    assert cc.primary_value is None
    assert cc.comparison_value is None
    assert cc.pct_diff is None
    assert cc.as_of_delta_days is None


def test_as_of_delta_none_when_a_date_missing() -> None:
    cc = _classify_cross_check(
        method="independent_concept",
        primary_value=Decimal("10000"),
        primary_concept="x",
        primary_as_of=None,
        comparison_value=Decimal("10000"),
        comparison_concept="y",
        comparison_as_of=_C,
    )
    assert cc.as_of_delta_days is None
    assert cc.status == "agrees"  # 0% diff still classifies
