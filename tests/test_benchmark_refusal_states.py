"""#2602 item 5 — the benchmark fields refuse by name, and the refusal is true.

⚠ Deliberately its OWN module rather than an addition to
``tests/test_strategy_monitoring.py``.  The ``db`` marker is applied per-MODULE at
collection, so a pure-logic test living beside a DB-backed one is evicted from the
``-m "not db"`` push gate and stops guarding anything on the path that runs.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import BaseModel, ValidationError

from app.api.strategies import (
    BENCHMARK_IDENTITY_UNVERIFIED,
    BENCHMARK_REFUSAL_CODES,
    BENCHMARK_REFUSALS,
    BENCHMARK_SERIES_NOT_INGESTED,
    BENCHMARK_SOURCE_UNLICENSED,
    StrategyOverviewResponse,
    StrategyPnlHistoryPoint,
    StrategyPnlHistoryResponse,
    StrategyWealthHistoryPoint,
    StrategyWealthHistoryResponse,
)

#: The models that make a benchmark claim, and therefore owe a reason for it.
REFUSING_RESPONSES: tuple[type[BaseModel], ...] = (
    StrategyOverviewResponse,
    StrategyPnlHistoryResponse,
    StrategyWealthHistoryResponse,
)

#: The two fields F-0 owes the operator.  Named here so a silent removal fails.
REQUIRED_BENCHMARKS = {"sp500_total_return", "cpih_real_return"}


def test_every_refusing_response_carries_a_reason() -> None:
    """A flag saying "no benchmark" without a reason is what item 5 replaces."""
    for model in REFUSING_RESPONSES:
        refusals = model.model_fields["benchmark_refusals"].default
        assert refusals, f"{model.__name__} claims no benchmark and gives no reason"


def test_all_three_responses_share_one_refusal_constant() -> None:
    """Three copies of a licence claim is three chances for one to go stale."""
    for model in REFUSING_RESPONSES:
        assert model.model_fields["benchmark_refusals"].default is BENCHMARK_REFUSALS


def test_both_required_benchmarks_are_present_exactly_once() -> None:
    keys = [refusal.benchmark for refusal in BENCHMARK_REFUSALS]
    assert sorted(keys) == sorted(REQUIRED_BENCHMARKS)
    assert len(keys) == len(set(keys))


def test_every_refusal_is_well_formed() -> None:
    for refusal in BENCHMARK_REFUSALS:
        assert refusal.label.strip(), f"{refusal.benchmark} has no operator-facing label"
        assert refusal.reasons, f"{refusal.benchmark} refuses without saying why"
        for reason in refusal.reasons:
            assert reason.code in BENCHMARK_REFUSAL_CODES, f"undeclared code {reason.code}"
            # The detail is what the UI renders; an empty one puts the client back
            # in the business of inventing prose about a third party's licence.
            assert len(reason.detail.strip()) > 20, f"{reason.code} carries no evidence"


def test_cpih_is_not_stamped_with_a_licence_or_identity_blocker() -> None:
    """The finding this test exists to pin (#2602, 2026-08-23).

    ONS publishes most of its website content under the Open Government Licence,
    which permits commercial reuse with attribution — so ``unlicensed`` is NOT
    established for CPIH, and a code asserting it would be a false statement about
    a public dataset: the mirror image of the price-only splice item 5 forbids.
    ``identity_unverified`` is equally false — there is no candidate series whose
    identity could be in doubt, because none is ingested.

    ⚠ The converse is not claimed either: "most content" is not "this series".
    The code we DO use states a fact about our own system, which is verifiable
    here.  A later tidy-up that closes the vocabulary back to the ticket's two
    literal codes must fail this test rather than pass quietly.
    """
    cpih = next(r for r in BENCHMARK_REFUSALS if r.benchmark == "cpih_real_return")
    codes = {reason.code for reason in cpih.reasons}
    assert codes == {BENCHMARK_SERIES_NOT_INGESTED}
    assert BENCHMARK_SOURCE_UNLICENSED not in codes
    assert BENCHMARK_IDENTITY_UNVERIFIED not in codes


def test_sp500_names_both_blockers_that_are_true_of_it() -> None:
    sp500 = next(r for r in BENCHMARK_REFUSALS if r.benchmark == "sp500_total_return")
    assert {reason.code for reason in sp500.reasons} == {
        BENCHMARK_SOURCE_UNLICENSED,
        BENCHMARK_IDENTITY_UNVERIFIED,
    }


def test_the_constant_cannot_be_mutated_in_place() -> None:
    """Module-global model instances are shared by every response in the process."""
    with pytest.raises(ValidationError):
        BENCHMARK_REFUSALS[0].reasons[0].code = "anything"  # type: ignore[misc]


@pytest.mark.parametrize("model", [StrategyPnlHistoryResponse, StrategyWealthHistoryResponse])
def test_availability_flags_cannot_be_flipped_to_true(model: type[BaseModel]) -> None:
    for field in ("benchmark_comparison_available", "total_return_available"):
        with pytest.raises(ValidationError):
            model(points=[], **{field: True})


@pytest.mark.parametrize(
    "model",
    [
        StrategyPnlHistoryResponse,
        StrategyWealthHistoryResponse,
        StrategyPnlHistoryPoint,
        StrategyWealthHistoryPoint,
    ],
)
def test_no_benchmark_valued_field_exists_on_the_history_contract(model: type[BaseModel]) -> None:
    """A splice would have to add a benchmark-named field. This is that tripwire.

    ⚠ Scope, stated honestly: this binds the SHAPE of these four models only.  It
    catches ``benchmark_total_return: Decimal`` on a response or a point; it does
    NOT catch a field named ``sp500_return``, a client deriving a comparison from
    ETF closes, or a new endpoint.  The rule those need is the one in the module
    comment — no price-only series labelled as total return — and no test replaces
    reading it.
    """
    allowed = {"benchmark_comparison_available", "benchmark_refusals"}
    named = {name for name in model.model_fields if "benchmark" in name}
    assert named <= allowed, f"{model.__name__} grew an unreviewed benchmark field: {named - allowed}"


def test_a_numeric_benchmark_value_is_not_silently_accepted() -> None:
    """Extra keys must not survive into the payload as though they were a series."""
    response = StrategyPnlHistoryResponse(points=[], benchmark_total_return=Decimal("12.5"))  # type: ignore[call-arg]
    assert "benchmark_total_return" not in response.model_dump()
