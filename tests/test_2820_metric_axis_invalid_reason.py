"""#2820 — the metric-axis gate must name the clause that closed it.

Backtest run 111610 evaluated the full S-1..S-10 set for 5h11m and died at the
write phase on a one-code refusal-set delta (`metric_axis_unproven`), persisting
nothing. `metric_axis_is_valid` returned a bare `bool` over eight independent
clauses, so the only route to "which clause" was to pay for the run again.

⚠ The clause names are DIAGNOSTIC. `metric_axis_unproven` remains one closed
refusal code — splitting it would change what a frozen
`STRUCTURAL_REFUSAL_POLICY_VERSION` declaration means — so nothing branches on
these strings and no test asserts a verdict from one.

⚠ Note what is NOT tested here, and why it is not a gap.
`ResultIdentity.__post_init__` already RAISES on the provenance clauses
(`digest_mismatch`, `start_not_axis_first`, `end_not_axis_last`,
`axis_outside_window`, `in_sample_axis_crosses_holdout_boundary`,
`opportunity_set_digest_absent`, `rule_version_moved`), so a constructed
identity cannot reach those returns at all — they are reachable only for rows
loaded from the database. That is itself the load-bearing fact for #2820: a
freshly built result CANNOT be refused for provenance, so the s1/masked failure
must be one of the three metrics-reconciliation clauses below.

Pure — no DB. `tests/test_strategy_result.py` is fully `db`-marked and therefore
off the pre-push gate.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import Any

import pytest

from app.services.equity_curve import BENCHMARK_RULE_ID, SIZING_RULE_ID
from app.services.strategy_result import (
    CORPUS_VERSION,
    EVALUATION_WINDOW_END,
    EVALUATION_WINDOW_START,
    LEGACY_RETURN_BASIS,
    METRIC_AXIS_RULE_VERSION,
    ResultIdentity,
    metric_axis_invalid_reason,
    metric_axis_is_valid,
    metric_axis_sha256,
    periods_per_year,
)

_AXIS = (EVALUATION_WINDOW_START, date(2021, 6, 28))
_PPY = periods_per_year(_AXIS)
_YEARS = (len(_AXIS) - 1) / _PPY
_TOTAL_RETURN_PCT = 21.0
_CAGR = (1.21 ** (1.0 / _YEARS) - 1.0) * 100.0


def _identity(**overrides: object) -> ResultIdentity:
    base: dict[str, object] = {
        "strategy_id": "s1-time-series-momentum",
        "strategy_version": "strategy-registry-v1+aaaaaaaaaaaa",
        "result_scope": "sleeve",
        "namespace": "in_sample",
        "ambiguity_arm": "worst_case",
        "quarantine_arm": "masked",
        "sizing_rule": SIZING_RULE_ID,
        "benchmark_rule": BENCHMARK_RULE_ID,
        "cost_model_id": "static-p75-insession-v1",
        "corpus_version": CORPUS_VERSION,
        "window_start": EVALUATION_WINDOW_START,
        "window_end": EVALUATION_WINDOW_END,
        "position_rule_set_version": "position-builder-v1+bbbbbbbbbbbb",
        "outcome_rule_set_version": "outcome-resolver-v1+cccccccccccc",
        "input_rule_set_version": "price-quarantine-v1+dddddddddddd",
        "return_basis": LEGACY_RETURN_BASIS,
        "metric_axis_rule_version": METRIC_AXIS_RULE_VERSION,
        "metric_axis_dates": _AXIS,
        "metric_axis_start": _AXIS[0],
        "metric_axis_end": _AXIS[-1],
        "metric_axis_digest": metric_axis_sha256(_AXIS),
        "opportunity_set_digest": "a" * 64,
    }
    base.update(overrides)
    return ResultIdentity(**base)  # type: ignore[arg-type]


def _metrics(**overrides: object) -> Any:
    """Only the three fields the axis rule reads."""
    base: dict[str, object] = {
        "periods_per_year": _PPY,
        "total_return_pct": _TOTAL_RETURN_PCT,
        "cagr_pct": _CAGR,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_a_coherent_axis_has_no_reason() -> None:
    assert metric_axis_invalid_reason(_identity(), _metrics()) is None


def test_the_bool_wrapper_still_agrees_with_the_reason() -> None:
    """One implementation, so the verdict cannot drift from the diagnosis."""
    assert metric_axis_is_valid(_identity(), _metrics()) is True
    assert metric_axis_is_valid(_identity(), _metrics(cagr_pct=_CAGR + 5.0)) is False


def test_a_legacy_all_null_identity_reports_an_absent_axis() -> None:
    """The pre-#2697 rows: refused, and now they say what for."""
    legacy = _identity(
        metric_axis_rule_version=None,
        metric_axis_dates=None,
        metric_axis_start=None,
        metric_axis_end=None,
        metric_axis_digest=None,
        opportunity_set_digest=None,
    )

    assert metric_axis_invalid_reason(legacy, _metrics()) == "axis_absent"


@pytest.mark.parametrize(
    ("overrides", "expected_prefix"),
    [
        ({"periods_per_year": _PPY * 1.5}, "periods_per_year_mismatch"),
        ({"total_return_pct": -150.0}, "total_return_below_negative_100pct"),
        ({"cagr_pct": _CAGR + 5.0}, "cagr_does_not_reconcile"),
    ],
)
def test_each_reachable_reconciliation_clause_names_itself(overrides: dict[str, object], expected_prefix: str) -> None:
    """The three clauses a FRESHLY BUILT result can actually be refused on.

    `build_result` constructs a `ResultIdentity`, and that constructor raises on
    every provenance clause — so if a run's write phase reports
    `metric_axis_unproven`, it is one of these three and the message now says
    which.
    """
    reason = metric_axis_invalid_reason(_identity(), _metrics(**overrides))

    assert reason is not None
    assert reason.startswith(expected_prefix)


def test_the_reason_carries_both_numbers_an_operator_would_otherwise_recompute() -> None:
    """A label alone still costs a re-run to act on; the figures are the point."""
    reason = metric_axis_invalid_reason(_identity(), _metrics(periods_per_year=_PPY * 1.5))

    assert reason is not None
    assert repr(_PPY * 1.5) in reason  # what the metrics stored
    assert repr(_PPY) in reason  # what the axis implies
