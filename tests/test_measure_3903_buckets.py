"""#3903 measurement scripts: the ratio buckets the spec cites, pinned at every edge (review PREVENTION, PR #3910)."""

from __future__ import annotations

from decimal import Decimal

import pytest

from scripts.measure_3903_ib_deviation import bucket as ib_bucket
from scripts.measure_3903_premise import bucket as premise_bucket


@pytest.mark.parametrize(
    ("x", "label"),
    [
        ("0.7499", "<0.75"),
        ("0.75", "[0.75,0.95)"),
        ("0.9499", "[0.75,0.95)"),
        ("0.95", "[0.95,1.05]"),
        ("1.05", "[0.95,1.05]"),
        ("1.0501", "(1.05,1.25]"),
        ("1.25", "(1.05,1.25]"),
        ("1.2501", ">1.25"),
    ],
)
def test_ib_deviation_bucket_edges(x: str, label: str) -> None:
    assert ib_bucket(Decimal(x)) == label


@pytest.mark.parametrize(
    ("x", "label"),
    [
        ("0.4999", "<0.5"),
        ("0.5", "[0.5,0.8)"),
        ("0.7999", "[0.5,0.8)"),
        ("0.8", "[0.8,1.25]"),
        ("1.25", "[0.8,1.25]"),
        ("1.2501", "(1.25,2]"),
        ("2", "(1.25,2]"),
        ("2.0001", ">2"),
    ],
)
def test_premise_bucket_edges(x: str, label: str) -> None:
    assert premise_bucket(Decimal(x)) == label
