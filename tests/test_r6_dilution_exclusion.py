from __future__ import annotations

import math
from decimal import Decimal

from app.services.r6_dilution_exclusion import (
    NsiInput,
    assign_nsi_portfolios,
    exclusions,
    has_filing_red_flag,
)


def _row(symbol: str, ratio: float, *, exchange: str = "NYSE") -> NsiInput:
    prior = 1_000_000
    current = round(prior * ratio)
    return NsiInput(symbol, exchange, Decimal(current), Decimal(prior), (), True)


def test_nsi_uses_log_share_ratio_and_published_sign_buckets() -> None:
    rows = tuple(
        [_row("NEG1", 0.8), _row("NEG2", 0.9), _row("ZERO", 1.0)]
        + [_row(f"POS{i}", math.exp(i / 100)) for i in range(1, 8)]
    )

    portfolios = assign_nsi_portfolios(rows, nyse_exchange_names=frozenset({"NYSE"}))

    assert portfolios["NEG1"] == 1
    assert portfolios["NEG2"] == 2
    assert portfolios["ZERO"] == 3
    assert [portfolios[f"POS{i}"] for i in range(1, 8)] == list(range(4, 11))


def test_non_nyse_names_are_assigned_on_nyse_breakpoints() -> None:
    nyse = tuple(_row(f"N{i}", math.exp(i / 100)) for i in range(1, 8))
    rows = (_row("NEG1", 0.8), _row("NEG2", 0.9), *nyse, _row("NASDAQ", math.exp(0.08), exchange="NASDAQ"))

    portfolios = assign_nsi_portfolios(rows, nyse_exchange_names=frozenset({"NYSE"}))

    assert portfolios["NASDAQ"] == 10


def test_missing_red_flag_history_is_neutral_and_union_is_explicit() -> None:
    flagged = NsiInput("FLAG", "NYSE", 120, 100, (0.7,), True)
    incomplete = NsiInput("MISSING", "NYSE", 120, 100, (1.0,), False)
    assert has_filing_red_flag(flagged)
    assert not has_filing_red_flag(incomplete)

    result = exclusions((flagged, incomplete), {"FLAG": 9, "MISSING": 10})
    assert result == {
        "dilution": frozenset({"MISSING"}),
        "red_flag": frozenset({"FLAG"}),
        "union": frozenset({"FLAG", "MISSING"}),
    }
