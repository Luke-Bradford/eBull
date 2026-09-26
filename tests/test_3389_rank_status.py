"""Pure-logic tests for #3389 (b): the verdict's current-rank gate and contributions.

No DB. `not_ranked_reason` must accept exactly what `GET /rankings` lists, and the
contributions must be the score's real composition under the row's weights.
"""

from __future__ import annotations

import pytest

from app.api.scores import family_contributions, not_ranked_reason
from app.services.scoring import family_weights


@pytest.mark.parametrize(
    ("in_latest_run", "is_tradable", "filings_status", "rank", "expected"),
    [
        (True, True, "analysable", 3, None),
        (True, False, "analysable", 3, "not_tradable"),
        (False, False, "insufficient", 3, "not_tradable"),  # durable exclusion wins
        (True, True, "insufficient", 3, "not_analysable"),
        (True, True, None, 3, "not_analysable"),  # no coverage row
        (False, True, "analysable", 3, "not_in_latest_run"),
        (True, True, "analysable", None, "no_rank"),
    ],
)
def test_not_ranked_reason(
    in_latest_run: bool, is_tradable: bool, filings_status: str | None, rank: int | None, expected: str | None
) -> None:
    assert (
        not_ranked_reason(
            in_latest_run=in_latest_run, is_tradable=is_tradable, filings_status=filings_status, rank=rank
        )
        == expected
    )


def test_contributions_are_weight_times_score_largest_first() -> None:
    weights = {"quality": 0.5, "value": 0.3, "momentum": 0.2}
    out = family_contributions(weights, {"quality": 0.2, "value": 0.9, "momentum": 0.4})
    assert [c.family for c in out] == ["value", "quality", "momentum"]
    assert out[0].contribution == pytest.approx(0.27)


def test_a_family_without_a_stored_score_is_omitted_not_zeroed() -> None:
    out = family_contributions({"quality": 0.5, "value": 0.5}, {"quality": 0.4, "value": None})
    assert [c.family for c in out] == ["quality"]


def test_unknown_model_version_has_no_contributions() -> None:
    assert family_weights("v0-nonexistent") is None
    assert family_contributions(None, {"quality": 0.4}) == []


def test_default_model_weights_sum_to_one() -> None:
    weights = family_weights("v1.5-balanced")
    assert weights is not None
    assert sum(weights.values()) == pytest.approx(1.0)
