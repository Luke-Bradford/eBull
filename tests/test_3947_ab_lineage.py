"""#3947 A/B gate: a changed ``dps_declared`` cell is explained only by its actual lineage (Codex checkpoint 2)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from app.services.fundamentals import DpsScaleDecision, FactRow
from scripts.ab_3947_dps_inline_scale import _lineage

FY_END = date(2025, 12, 31)


def _fact(val: str, start: date, end: date = FY_END, fp: str = "FY") -> FactRow:
    return FactRow(
        "CommonStockDividendsPerShareDeclared", "USD/shares", start, end, Decimal(val), None, "10-K", 2025, fp,
        "acc", date(2026, 2, 20),
    )  # fmt: skip


def _row(kind: str, value: str | None, *, derived: bool = False, end: date = FY_END) -> dict[str, Any]:
    return {
        "period_end_date": end,
        "period_type": kind,
        "fiscal_year": 2025,
        "dps_declared": None if value is None else Decimal(value),
        "is_derived": derived,
    }


FY = _fact("520", date(2025, 1, 1))
RESTORE = {FY: DpsScaleDecision("restore", "scaled", 3, Decimal("0.52"))}
FY_KEY = (1, FY_END, "FY", 2025, None)
Q4_KEY = (1, FY_END, "Q4", 2025, 4)


def test_a_restored_fy_explains_its_own_cell_only_at_the_restored_value() -> None:
    assert _lineage(FY_KEY, {FY_KEY: _row("FY", "0.5200")}, {FY_KEY}, [FY], RESTORE) == "direct"
    assert _lineage(FY_KEY, {FY_KEY: _row("FY", "0.6000")}, {FY_KEY}, [FY], RESTORE) is None


def test_an_fy_restore_does_not_explain_a_reported_q4_sharing_its_end_date() -> None:
    q4 = _fact("0.13", date(2025, 10, 1), fp="Q4")
    on = {FY_KEY: _row("FY", "0.5200"), Q4_KEY: _row("Q4", "0.1300")}
    assert _lineage(Q4_KEY, on, {FY_KEY, Q4_KEY}, [FY, q4], RESTORE) is None


def test_a_derived_q4_must_equal_fy_minus_the_quarters() -> None:
    quarters = {
        (1, date(2025, m, d), f"Q{n}", 2025, n): _row(f"Q{n}", "0.1300", end=date(2025, m, d))
        for n, (m, d) in enumerate([(3, 31), (6, 30), (9, 30)], start=1)
    }
    on = {FY_KEY: _row("FY", "0.5200"), **quarters, Q4_KEY: _row("Q4", "0.1300", derived=True)}
    assert _lineage(Q4_KEY, on, {FY_KEY, Q4_KEY}, [FY], RESTORE) == "derived_q4"
    on[Q4_KEY] = _row("Q4", "0.2000", derived=True)
    assert _lineage(Q4_KEY, on, {FY_KEY, Q4_KEY}, [FY], RESTORE) is None
