"""#3437: a FINRA symbol carrying a SIAC/CQS lowercase type suffix is not the common stock."""

from __future__ import annotations

import pytest

from app.services.finra_short_interest_ingest import is_cqs_suffix_symbol, normalise_symbol


@pytest.mark.parametrize("symbol", ["TpC", "BCpC", "AUBpA", "CpN", "ABRpD", "XYZr", "XYZw", "XYZpw"])
def test_suffix_symbols_are_recognised(symbol: str) -> None:
    assert is_cqs_suffix_symbol(symbol)


@pytest.mark.parametrize("symbol", ["TPC", "BCPC", "AAPL", "BRK.A", "BRKA", "ABRPRD", "T"])
def test_common_and_upper_case_symbols_are_not(symbol: str) -> None:
    assert not is_cqs_suffix_symbol(symbol)


def test_normalising_a_suffix_symbol_would_land_on_an_unrelated_ticker() -> None:
    """The defect the guard exists for: upper-casing collapses the two securities."""
    assert normalise_symbol("TpC") == normalise_symbol("TPC")
