"""Phase 5e-1 — the access record's shape and the storage round-trip, pure tier.

No database. What is table-testable here is the validation that runs BEFORE any
statement is issued, plus the float ↔ ``NUMERIC`` conversion, which is a pure
function and is the one place a stored metric can silently stop equalling the
metric that was computed.

The namespace mechanism itself — the view, the trigger, the check option — is a
property of the relations and is exercised in
``tests/test_strategy_holdout_namespace.py`` against a real database. A mocked
cursor would assert the parameters while passing against a trigger that refuses
them.
"""

from __future__ import annotations

from dataclasses import fields
from decimal import Decimal
from typing import cast

import psycopg
import pytest

from app.services.deflated_sharpe import DeflatedSharpeResult
from app.services.random_entry_cohort import SyntheticControl
from app.services.result_ledger import (
    HOLDOUT_ACCESS_KINDS,
    HoldoutAccess,
    _numeric,
    store_holdout_result,
    store_in_sample_result,
)
from app.services.strategy_result import (
    BENCHMARK_RULE,
    CORPUS_VERSION,
    EVALUATION_WINDOW_END,
    EVALUATION_WINDOW_START,
    LEGACY_RETURN_BASIS,
    SIZING_RULE,
    ResultIdentity,
    StrategyResult,
)
from app.services.strategy_statistics import StrategyMetrics

#: ⚠ Transcribed from sql/264, not imported from it — the #2240 S-3 lesson that
#: a reference importing the constant it validates is a tautology.
SPEC_ACCESS_KINDS = {"evaluate", "read"}


_IDENTITY_FIELDS = frozenset(f.name for f in fields(ResultIdentity))


#: A COMPLETE criterion-3 block (sql/265), with the same awkward-float
#: discipline as ``build_metrics`` below. ⚠ Its nine columns are bound
#: all-or-nothing, so this is applied as a SET — passing one field alone raises
#: in ``StrategyMetrics`` before the database is reached.
BOOTSTRAP_BLOCK: dict[str, object] = {
    "effective_sample_size": 144749.30217,
    "expectancy_ci_low_pct": -0.48013926,
    "expectancy_ci_high_pct": -0.40317744,
    "bootstrap_block_length": 14,
    "bootstrap_cluster_count": 15577,
    "bootstrap_resamples": 2000,
    "bootstrap_seed": 20260807,
    "bootstrap_design_effect": 21.6449283,
    "bootstrap_model_id": "c3-block-bootstrap-v1",
}


def build_deflated(**overrides: object) -> DeflatedSharpeResult:
    """A COMPLETE criterion-6 block (sql/266), same awkward-float discipline.

    ⚠ Every float is deliberately distinct and irregular so a swapped pair of
    same-typed columns cannot coincide — this object has SEVEN adjacent NUMERIC
    fields, which is the largest run of interchangeable types in the row and so
    the easiest place for a positional read to drift unnoticed.
    """
    base: dict[str, object] = {
        "deflated_sharpe": 0.71792143,
        "expected_max_sharpe": 0.01520778,
        "trade_sharpe": 0.01703391,
        "skewness": -0.40218764,
        "kurtosis": 8.13472905,
        # ⚠ MUST equal ``BOOTSTRAP_BLOCK``'s. The DSR has no sample-size column
        # of its own — it consumes criterion 3's — and ``StrategyResult``
        # refuses a pair that disagrees.
        "effective_sample_size": BOOTSTRAP_BLOCK["effective_sample_size"],
        "declared_trials": 11,
        "independent_trials": 9.00341827,
        "average_trial_correlation": 0.20419638,
        "trial_sharpe_variance": 0.00010374,
        "measured_trials": 2,
        "trial_register_version": "trial-register-2026-08-07",
    }
    base.update(overrides)
    return DeflatedSharpeResult(**base)  # type: ignore[arg-type]


def build_result_with_dsr(**overrides: object) -> StrategyResult:
    """``build_result`` carrying a full DSR, with the two scalars bound to it.

    ⚠ ``trial_count`` and ``deflated_sharpe`` are NOT free here —
    ``StrategyResult.__post_init__`` requires them to agree with the object, so
    a caller cannot assemble a row whose stored count describes a different
    correction from the one that was computed.
    """
    deflated = build_deflated()
    return build_result(
        deflated=deflated,
        trial_count=deflated.declared_trials,
        deflated_sharpe=Decimal(repr(deflated.deflated_sharpe)),
        metrics=build_metrics(**BOOTSTRAP_BLOCK),
        **overrides,
    )


def build_control(metrics: StrategyMetrics, **overrides: object) -> SyntheticControl:
    """A COMPLETE §9 control (sql/268), same awkward-float discipline.

    ⚠ The two strategy-side figures are taken FROM the metric set rather than
    restated — ``StrategyResult.__post_init__`` binds them, and a literal here
    would be a second copy of a number this row already carries.

    ⚠ The thresholds are chosen so the verdict is FALSE: ``build_metrics``'s
    Sharpe is -3.73 and its total return -100%, which is the shape today's
    pipeline actually produces. A fixture whose verdict was true would exercise
    only the branch that cannot happen yet.
    """
    base: dict[str, object] = {
        "model_id": "permuted-entry-uniform-gap-v1",
        "cohort_size": 1000,
        "root_seed": 20260808,
        "mean_return_pct": -97.5634812,
        "mean_return_ci_low_pct": -99.2083176,
        "mean_return_ci_high_pct": -96.3142907,
        "sharpe_percentile": 95.0,
        "cohort_sharpe_threshold": -0.16704293,
        "strategy_sharpe": metrics.sharpe,
        "cohort_return_threshold_pct": -96.39971148,
        "strategy_return_pct": metrics.total_return_pct,
    }
    base.update(overrides)
    return SyntheticControl(**base)  # type: ignore[arg-type]


def build_metrics(**overrides: object) -> StrategyMetrics:
    """A complete criterion-7 set with DELIBERATELY AWKWARD floats.

    ⚠ Not round numbers. Every value here is chosen so that a lossy
    float → ``NUMERIC`` → float trip shows up as an inequality; a metric set of
    ``0.5`` and ``1.0`` would survive almost any conversion bug.
    """
    base: dict[str, object] = {
        "expectancy_per_trade_pct": -0.44163847291,
        "profit_factor": 0.7128994,
        "cagr_pct": -43.29218837,
        "annualised_volatility_pct": 1 / 3,
        "sharpe": -3.7278459,
        "sortino": -4.3042117,
        "max_drawdown_pct": -99.99999997,
        "exposure_time_pct": 66.48013,
        "turnover_annualised": 47.8632991,
        "trade_count": 3133100,
        "effective_sample_size": None,
        "return_vs_buy_and_hold_pct": -989443125.1,
        "losing_trade_count": 1900007,
        "losing_period_count": 7101,
        "open_trade_count": 2255,
        "unpriced_trade_count": 0,
        "periods_per_year": 251.66446,
        "total_return_pct": -100.0,
        "buy_and_hold_return_pct": 989443125.1,
    }
    base.update(overrides)
    return StrategyMetrics(**base)  # type: ignore[arg-type]


def build_result(**overrides: object) -> StrategyResult:
    """One assembled ``StrategyResult``. Shared with the DB-tier file.

    ⚠ Lives in the PURE-tier module and is imported by the DB-tier one, never
    the other way round: a pure test importing a module that opens a connection
    gets auto-marked ``db`` at collection (``tests/conftest.py``) and silently
    leaves the fast tier.
    """
    identity_overrides = {k: v for k, v in overrides.items() if k in _IDENTITY_FIELDS}
    result_overrides = {k: v for k, v in overrides.items() if k not in _IDENTITY_FIELDS}
    identity_base: dict[str, object] = {
        "strategy_id": "S-1",
        "strategy_version": "strategy-registry-v1+aaaaaaaaaaaa",
        "result_scope": "sleeve",
        "namespace": "hold_out",
        "ambiguity_arm": "worst_case",
        "quarantine_arm": "masked",
        "sizing_rule": SIZING_RULE,
        "benchmark_rule": BENCHMARK_RULE,
        "cost_model_id": "static-p75-insession-v1",
        "corpus_version": CORPUS_VERSION,
        "window_start": EVALUATION_WINDOW_START,
        "window_end": EVALUATION_WINDOW_END,
        "position_rule_set_version": "position-builder-v1+bbbbbbbbbbbb",
        "outcome_rule_set_version": "outcome-resolver-v1+cccccccccccc",
        "input_rule_set_version": "price-quarantine-v1+dddddddddddd",
        "return_basis": LEGACY_RETURN_BASIS,
    }
    identity_base.update(identity_overrides)
    result_base: dict[str, object] = {
        "identity": ResultIdentity(**identity_base),  # type: ignore[arg-type]
        "purpose": "capital_candidate",
        "metrics": build_metrics(),
        "universe_basis": "survivor_only",
        "carry_unmodelled": True,
        "fx_unmodelled": True,
        "evaluated_instrument_count": 5266,
        "trial_count": None,
        "deflated_sharpe": None,
    }
    result_base.update(result_overrides)
    return StrategyResult(**result_base)  # type: ignore[arg-type]


def _access(**overrides: object) -> HoldoutAccess:
    base: dict[str, object] = {
        "strategy_id": "S-1",
        "strategy_version": "strategy-registry-v1+aaaaaaaaaaaa",
        "access_kind": "evaluate",
        "accessed_by": "headless-loop",
        "purpose": "stage 5e-1 acceptance",
        "result_version": "strategy-result-v1+bbbbbbbbbbbb",
    }
    base.update(overrides)
    return HoldoutAccess(**base)  # type: ignore[arg-type]


class TestTheAccessVocabulary:
    def test_the_kinds_are_the_two_sql_264_declares(self) -> None:
        assert set(HOLDOUT_ACCESS_KINDS) == SPEC_ACCESS_KINDS

    def test_an_unknown_kind_is_refused(self) -> None:
        with pytest.raises(ValueError, match="unknown access kind"):
            _access(access_kind="peek")


class TestAnAccessRecordMustActuallyRecord:
    """⚠ Every case here is a record that a ``NOT NULL`` column would accept.

    The #2286 shape: a present-but-empty value is PRESENT, and an access log of
    blank actors and blank purposes is a count wearing the name of an audit.
    """

    @pytest.mark.parametrize("field", ["strategy_id", "strategy_version", "accessed_by", "purpose"])
    def test_a_blank_identity_field_is_refused(self, field: str) -> None:
        with pytest.raises(ValueError, match=f"{field} is blank"):
            _access(**{field: ""})

    def test_a_blank_result_version_is_refused_rather_than_read_as_absent(self) -> None:
        """⚠ ``''`` and ``None`` are different states and only one is legal.

        The trigger matches ``a.result_version = NEW.result_version``, so a
        blank one authorises nothing while looking like a record that does.
        """
        with pytest.raises(ValueError, match="result_version is blank"):
            _access(result_version="")

    def test_an_evaluate_must_name_the_result_it_authorises(self) -> None:
        """⚠ sql/264's CHECK, mirrored. An unversioned evaluate record would
        stand in for every hold-out row of that strategy version at once."""
        with pytest.raises(ValueError, match="must name the result_version"):
            _access(access_kind="evaluate", result_version=None)

    def test_a_read_may_span_versions(self) -> None:
        """The complement, and it is a real state: a read is not per-row."""
        assert _access(access_kind="read", result_version=None).result_version is None


class TestTheNumericConversionIsAnActualRoundTrip:
    """⚠⚠ THE CASE THIS CLASS EXISTS FOR.

    ``StrategyMetrics`` is float-typed on purpose and ``sql/263`` stores
    ``NUMERIC``, so every metric crosses a representation boundary twice. If the
    conversion is not exact, a stored Sharpe is not the Sharpe that was computed
    — and nothing downstream would ever say so.

    ``Decimal(repr(x))`` is exact because ``repr`` of a float is its shortest
    round-tripping decimal form. ``Decimal(x)`` is NOT the same thing and the
    first test below is what separates them.
    """

    @pytest.mark.parametrize(
        "value",
        [
            0.1,
            1 / 3,
            -0.4416,
            251.66446,
            0.0,
            -100.0,
            1e-9,
            1.7976931348623157e308,
            -3.7278459,
        ],
    )
    def test_every_float_survives_the_trip(self, value: float) -> None:
        stored = _numeric(value)
        assert stored is not None
        assert float(stored) == value

    def test_repr_and_not_the_binary_expansion(self) -> None:
        """The discriminator between the two constructions.

        ``Decimal(0.1)`` is 0.1000000000000000055511151231257827021181583404541015625.
        Both round-trip through ``float()``; only one is a number a human reading
        the row can recognise, and only one keeps ``NUMERIC``'s scale finite.
        """
        assert _numeric(0.1) == Decimal("0.1")
        assert _numeric(0.1) != Decimal(0.1)

    def test_none_stays_none(self) -> None:
        """⚠ ``profit_factor`` and ``sortino`` are null exactly when their
        denominator is empty (sql/263). Coercing that to 0 would turn a real
        state into a measurement nobody made."""
        assert _numeric(None) is None


class TestTheTwoWritersRefuseEachOthersNamespace:
    """The call-site guard, checked before any statement is issued.

    ⚠ NOT the mechanism — the database is (the view's check option and the
    store's trigger, both measured in the DB-tier file). This names the mistake
    where it was made instead of surfacing it as a driver error three frames
    down.
    """

    def test_in_sample_writer_refuses_a_hold_out_result(self) -> None:
        conn = cast(psycopg.Connection[tuple], object())
        with pytest.raises(ValueError, match="store_holdout_result"):
            store_in_sample_result(conn, build_result(namespace="hold_out"))

    def test_hold_out_writer_refuses_an_in_sample_result(self) -> None:
        """⚠ The reason is not symmetry. Recording a hold-out access for an
        in-sample write would inflate the very count criterion 5 audits."""
        conn = cast(psycopg.Connection[tuple], object())
        with pytest.raises(ValueError, match="inflate"):
            store_holdout_result(conn, build_result(namespace="in_sample"), accessed_by="t", purpose="t")
