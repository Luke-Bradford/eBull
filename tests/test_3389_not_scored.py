"""Pure-logic tests for #3389 (d): why a never-scored instrument has no score.

No DB. `not_scored_reason` must follow `compute_rankings`' eligibility gate, and a
non-stock outside the filings model must not read as thin filings or as pending.
"""

from __future__ import annotations

import inspect

import pytest

from app.api.scores import not_scored_reason
from app.services.scoring import compute_rankings


@pytest.mark.parametrize(
    ("is_tradable", "filings_status", "instrument_type", "has_inputs", "expected"),
    [
        (False, "analysable", "Stocks", True, "not_tradable"),  # durable exclusion wins
        (False, "no_primary_sec_cik", "ETF", False, "not_tradable"),
        (True, "no_primary_sec_cik", "ETF", True, "not_a_stock"),  # core-sleeve ETFs (VOO, QQQ)
        (True, None, "Crypto", True, "not_a_stock"),  # no coverage row
        (True, "fpi", None, True, "not_a_stock"),  # unknown type is not claimed to be a stock
        (True, "fpi", "Stocks", True, "not_analysable"),
        (True, None, "Stocks", True, "not_analysable"),
        (True, "analysable", "Stocks", False, "no_inputs"),
        (True, "analysable", "ETF", True, "pending_run"),  # analysable ETFs ARE scored
        (True, "analysable", "Stocks", True, "pending_run"),
    ],
)
def test_not_scored_reason(
    is_tradable: bool, filings_status: str | None, instrument_type: str | None, has_inputs: bool, expected: str
) -> None:
    assert (
        not_scored_reason(
            is_tradable=is_tradable,
            filings_status=filings_status,
            instrument_type=instrument_type,
            has_inputs=has_inputs,
        )
        == expected
    )


def test_gate_matches_compute_rankings() -> None:
    """The reason mirrors the gate; if the gate's inputs change, this reason must too."""
    source = inspect.getsource(compute_rankings)
    for fragment in (
        "i.is_tradable = TRUE",
        "c.filings_status = 'analysable'",
        "FROM theses t",
        "FROM fundamentals_snapshot f",
        "FROM price_daily p",
    ):
        assert fragment in source, fragment
