"""Phase 5e-1 — the result writer, and criterion 5's hold-out access log.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §5.2 (the frozen
split), §6 (#2288 clauses 2-4), §8 (stage 5e), acceptance C5. Parent:
``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md`` criterion 5.
Row shape: ``sql/262`` + ``sql/263``. Namespace and trigger: ``sql/264``.
Gate and frozen literals: ``app/services/strategy_result.py``. Refs #2240, #2288.

⚠⚠ THE TWO WRITERS TARGET DIFFERENT RELATIONS, AND THAT IS THE MECHANISM.

``store_in_sample_result`` inserts into the VIEW ``strategy_results``, which
``sql/264`` created ``WITH CASCADED CHECK OPTION`` over
``WHERE namespace = 'in_sample'``. So it cannot write a hold-out row even if
every guard in this file were deleted — the database refuses with
``WithCheckOptionViolation`` (measured 2026-08-07, SQLSTATE 44000).

``store_holdout_result`` names the STORE, and the store carries a trigger
refusing any hold-out row whose ``(strategy_id, strategy_version,
result_version)`` has no ``evaluate`` access record (SQLSTATE 23000, measured).
So it cannot write an unrecorded one.

Neither property is a convention this module enforces; both are properties of
the relation each statement names. Same construction as ``signal_ledger``, whose
header puts it as *"a StrategySignal carries a bar INDEX and no fill field, so a
strategy cannot express a fill at all"* — the guard is in the shape, not in a
check somebody has to remember to run.

⚠ WHAT THIS MODULE DOES NOT DO

- It does not COMPUTE anything. ``strategy_statistics`` (5d) produces the
  metrics and ``position_builder`` (5a) the trades; this stores what they made.
- It does not decide promotability. ``strategy_result.check_promotable`` does,
  and this module supplies two of its inputs via ``holdout_access_counts``.
- It does not make the hold-out unreadable to a determined reader. ``sql/264``'s
  header is explicit about the limit: RLS was measured and does not bind this
  app's superuser connection, so what ships is that the OBVIOUS name cannot
  express a hold-out row. Reading the withheld side means naming the store, and
  ``read_holdout_results`` is the door that records the access.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Final, Literal, get_args

import psycopg

from app.services.deflated_sharpe import DeflatedSharpeResult
from app.services.strategy_result import (
    ResultIdentity,
    StrategyResult,
)
from app.services.strategy_statistics import StrategyMetrics

#: ``sql/264``'s ``access_kind`` vocabulary. ⚠ Two kinds because they are
#: different governance events: an ``evaluate`` is a hold-out number being
#: PRODUCED, a ``read`` is one being LOOKED AT, and criterion 5's *"evaluated
#: more than once"* is about the second.
HoldoutAccessKind = Literal["evaluate", "read"]
HOLDOUT_ACCESS_KINDS: Final[frozenset[str]] = frozenset(get_args(HoldoutAccessKind))


@dataclass(frozen=True)
class HoldoutAccess:
    """One criterion-5 access record.

    ⚠ ``accessed_by`` and ``purpose`` are REQUIRED and non-empty, and neither
    has a default. A count of accesses with no intent attached answers *"how
    many times"* and never *"should that have happened"*, and criterion 5 is a
    governance criterion — the second question is the whole point of the log.

    ⚠ The validation below MIRRORS ``sql/264``'s CHECKs rather than deferring to
    them, which is the same deliberate duplication ``signal_ledger.LedgerRow``
    documents: a bad record fails here with a message naming the field, and the
    constraints stay as the backstop for any writer that bypasses this class.
    """

    strategy_id: str
    strategy_version: str
    access_kind: HoldoutAccessKind
    accessed_by: str
    purpose: str
    #: ⚠ NULL only for a ``read``. A read may span every result version a
    #: strategy has and naming one would be a fiction; an ``evaluate``
    #: authorises exactly one row and the trigger matches on the triple, so a
    #: null there would silently never match.
    result_version: str | None = None

    def __post_init__(self) -> None:
        if self.access_kind not in HOLDOUT_ACCESS_KINDS:
            raise ValueError(f"unknown access kind {self.access_kind!r}; must be one of {sorted(HOLDOUT_ACCESS_KINDS)}")
        for field_name in ("strategy_id", "strategy_version", "accessed_by", "purpose"):
            if not getattr(self, field_name):
                raise ValueError(
                    f"{field_name} is blank — a present-but-empty access record counts as governance and records "
                    "nothing (#2286)"
                )
        if self.result_version is not None and not self.result_version:
            raise ValueError("result_version is blank — pass None for a read that spans versions, never ''")
        if self.access_kind == "evaluate" and self.result_version is None:
            raise ValueError(
                "an evaluate access must name the result_version it authorises — one unversioned record would stand "
                "in for every hold-out row of that strategy version at once"
            )


@dataclass(frozen=True)
class HoldoutAccessCounts:
    """``check_promotable``'s two hold-out inputs, read off the database.

    ⚠ THE TWO COUNT DIFFERENT RELATIONS ON PURPOSE, and the asymmetry is what
    makes the gate's ``recorded_accesses < holdout_evaluations`` a live check
    rather than a tautology:

    - ``holdout_evaluations`` counts hold-out RESULT ROWS — evaluations that
      actually happened and were stored;
    - ``recorded_accesses`` counts ``evaluate`` ACCESS RECORDS.

    ``sql/264``'s trigger makes the second ≥ the first for every row written
    through a live trigger. Count them off the same relation instead and the
    gate could never fire — which is the dead-branch shape stage 5d's probes
    caught (*"a test named after a branch that cannot fire is a test that passes
    for the wrong reason"*). Counted apart, the inequality catches exactly the
    state it names: rows that entered while the trigger was disabled, dropped or
    bypassed.
    """

    holdout_evaluations: int
    recorded_accesses: int


# ---------------------------------------------------------------------------
# Float ↔ NUMERIC
# ---------------------------------------------------------------------------


def _numeric(value: float | None) -> Decimal | None:
    """A float, ready for a ``NUMERIC`` column, with the round-trip preserved.

    ⚠ ``Decimal(repr(x))`` and NOT ``Decimal(x)`` or a bare float. ``repr`` of a
    float is its shortest round-tripping decimal form, so ``float(read_back)``
    returns the identical float. ``Decimal(x)`` would store the full binary
    expansion (``Decimal(0.1)`` is 55 digits), and handing psycopg the bare
    float leaves the ``float8 → numeric`` cast to decide the digits — neither is
    a round trip anybody declared.

    ``StrategyMetrics`` is float-typed deliberately (*"a Decimal field would
    advertise an exactness the path does not have"*), so this conversion exists
    at the storage boundary and nowhere else.
    """
    return None if value is None else Decimal(repr(value))


def _as_float(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


# ---------------------------------------------------------------------------
# The statements
# ---------------------------------------------------------------------------

#: ⚠ FIXED statements, never f-strings built from a column list. psycopg types
#: ``query`` as ``LiteralString`` precisely to stop dynamic SQL, and the
#: chokepoint lint catches the f-string form — correctly.
_RECORD_ACCESS = """
    INSERT INTO strategy_holdout_accesses (
        strategy_id, strategy_version, result_version, access_kind, accessed_by, purpose
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(result_version)s, %(access_kind)s, %(accessed_by)s, %(purpose)s
    )
    RETURNING access_id
"""

_RESULT_COLUMNS = """
    strategy_id, strategy_version, result_version, result_scope, namespace,
    ambiguity_arm, window_start, window_end, universe_basis, corpus_version,
    cost_model_id, carry_unmodelled, sizing_rule, position_rule_set_version,
    outcome_rule_set_version, input_rule_set_version, evaluated_instrument_count,
    trial_count, deflated_sharpe,
    expectancy_per_trade_pct, profit_factor, cagr_pct, annualised_volatility_pct, sharpe, sortino,
    max_drawdown_pct, exposure_time_pct, turnover_annualised, trade_count, effective_sample_size,
    return_vs_buy_and_hold_pct, losing_trade_count, losing_period_count, open_trade_count,
    unpriced_trade_count, periods_per_year, total_return_pct, buy_and_hold_return_pct, metric_set_id,
    expectancy_ci_low_pct, expectancy_ci_high_pct, bootstrap_block_length, bootstrap_cluster_count,
    bootstrap_resamples, bootstrap_seed, bootstrap_design_effect, bootstrap_model_id,
    dsr_trade_sharpe, dsr_skewness, dsr_kurtosis, dsr_expected_max_sharpe, dsr_independent_trials,
    dsr_average_trial_correlation, dsr_trial_sharpe_variance, dsr_measured_trials, dsr_model_id,
    trial_register_version
"""

_RESULT_VALUES = """
    %(strategy_id)s, %(strategy_version)s, %(result_version)s, %(result_scope)s, %(namespace)s,
    %(ambiguity_arm)s, %(window_start)s, %(window_end)s, %(universe_basis)s, %(corpus_version)s,
    %(cost_model_id)s, %(carry_unmodelled)s, %(sizing_rule)s, %(position_rule_set_version)s,
    %(outcome_rule_set_version)s, %(input_rule_set_version)s, %(evaluated_instrument_count)s,
    %(trial_count)s, %(deflated_sharpe)s,
    %(expectancy_per_trade_pct)s, %(profit_factor)s, %(cagr_pct)s, %(annualised_volatility_pct)s,
    %(sharpe)s, %(sortino)s, %(max_drawdown_pct)s, %(exposure_time_pct)s, %(turnover_annualised)s,
    %(trade_count)s, %(effective_sample_size)s, %(return_vs_buy_and_hold_pct)s, %(losing_trade_count)s,
    %(losing_period_count)s, %(open_trade_count)s, %(unpriced_trade_count)s, %(periods_per_year)s,
    %(total_return_pct)s, %(buy_and_hold_return_pct)s, %(metric_set_id)s,
    %(expectancy_ci_low_pct)s, %(expectancy_ci_high_pct)s, %(bootstrap_block_length)s,
    %(bootstrap_cluster_count)s, %(bootstrap_resamples)s, %(bootstrap_seed)s,
    %(bootstrap_design_effect)s, %(bootstrap_model_id)s,
    %(dsr_trade_sharpe)s, %(dsr_skewness)s, %(dsr_kurtosis)s, %(dsr_expected_max_sharpe)s,
    %(dsr_independent_trials)s, %(dsr_average_trial_correlation)s, %(dsr_trial_sharpe_variance)s,
    %(dsr_measured_trials)s, %(dsr_model_id)s, %(trial_register_version)s
"""

#: ⚠⚠ TARGETS THE VIEW. ``sql/264`` gave ``strategy_results`` a cascaded check
#: option over ``namespace = 'in_sample'``, so this statement is incapable of
#: writing a hold-out row — the guard is the relation, not the caller.
_INSERT_IN_SAMPLE = f"""
    INSERT INTO strategy_results ({_RESULT_COLUMNS}) VALUES ({_RESULT_VALUES}) RETURNING result_id
"""  # noqa: S608 - both fragments are module-level literals, no caller input reaches them

#: ⚠⚠ TARGETS THE STORE, which is the only relation a hold-out row can enter —
#: and the store's trigger refuses one with no ``evaluate`` access record.
_INSERT_HOLDOUT = f"""
    INSERT INTO strategy_results_store ({_RESULT_COLUMNS}) VALUES ({_RESULT_VALUES}) RETURNING result_id
"""  # noqa: S608 - as above

_SELECT_HOLDOUT = f"""
    SELECT {_RESULT_COLUMNS}
    FROM strategy_results_store
    WHERE strategy_id = %(strategy_id)s
      AND strategy_version = %(strategy_version)s
      AND namespace = 'hold_out'
    ORDER BY result_version, result_scope, ambiguity_arm
"""  # noqa: S608 - as above

_COUNT_HOLDOUT_RESULTS = """
    SELECT count(*)
    FROM strategy_results_store
    WHERE strategy_id = %(strategy_id)s
      AND strategy_version = %(strategy_version)s
      AND namespace = 'hold_out'
"""

_COUNT_EVALUATE_ACCESSES = """
    SELECT count(*)
    FROM strategy_holdout_accesses
    WHERE strategy_id = %(strategy_id)s
      AND strategy_version = %(strategy_version)s
      AND access_kind = 'evaluate'
"""


def _row_params(result: StrategyResult) -> dict[str, object]:
    """One ``StrategyResult`` flattened to ``sql/262`` + ``sql/263``'s columns."""
    identity = result.identity
    metrics = result.metrics
    return {
        "strategy_id": identity.strategy_id,
        "strategy_version": identity.strategy_version,
        "result_version": identity.version,
        "result_scope": identity.result_scope,
        "namespace": identity.namespace,
        "ambiguity_arm": identity.ambiguity_arm,
        "window_start": identity.window_start,
        "window_end": identity.window_end,
        "universe_basis": result.universe_basis,
        "corpus_version": identity.corpus_version,
        "cost_model_id": identity.cost_model_id,
        "carry_unmodelled": result.carry_unmodelled,
        "sizing_rule": identity.sizing_rule,
        "position_rule_set_version": identity.position_rule_set_version,
        "outcome_rule_set_version": identity.outcome_rule_set_version,
        "input_rule_set_version": identity.input_rule_set_version,
        "evaluated_instrument_count": result.evaluated_instrument_count,
        "trial_count": result.trial_count,
        "deflated_sharpe": result.deflated_sharpe,
        "expectancy_per_trade_pct": _numeric(metrics.expectancy_per_trade_pct),
        "profit_factor": _numeric(metrics.profit_factor),
        "cagr_pct": _numeric(metrics.cagr_pct),
        "annualised_volatility_pct": _numeric(metrics.annualised_volatility_pct),
        "sharpe": _numeric(metrics.sharpe),
        "sortino": _numeric(metrics.sortino),
        "max_drawdown_pct": _numeric(metrics.max_drawdown_pct),
        "exposure_time_pct": _numeric(metrics.exposure_time_pct),
        "turnover_annualised": _numeric(metrics.turnover_annualised),
        "trade_count": metrics.trade_count,
        "effective_sample_size": _numeric(metrics.effective_sample_size),
        "return_vs_buy_and_hold_pct": _numeric(metrics.return_vs_buy_and_hold_pct),
        "losing_trade_count": metrics.losing_trade_count,
        "losing_period_count": metrics.losing_period_count,
        "open_trade_count": metrics.open_trade_count,
        "unpriced_trade_count": metrics.unpriced_trade_count,
        "periods_per_year": _numeric(metrics.periods_per_year),
        "total_return_pct": _numeric(metrics.total_return_pct),
        "buy_and_hold_return_pct": _numeric(metrics.buy_and_hold_return_pct),
        "metric_set_id": metrics.metric_set_id,
        # ⚠ sql/265's criterion-3 block. All nine columns (including
        # `effective_sample_size` above) are bound by
        # `strategy_results_bootstrap_all_or_nothing`, so omitting these eight
        # would make every bootstrap-carrying result UNWRITABLE — the row would
        # arrive with one field set and eight null and be refused.
        "expectancy_ci_low_pct": _numeric(metrics.expectancy_ci_low_pct),
        "expectancy_ci_high_pct": _numeric(metrics.expectancy_ci_high_pct),
        "bootstrap_block_length": metrics.bootstrap_block_length,
        "bootstrap_cluster_count": metrics.bootstrap_cluster_count,
        "bootstrap_resamples": metrics.bootstrap_resamples,
        "bootstrap_seed": metrics.bootstrap_seed,
        "bootstrap_design_effect": _numeric(metrics.bootstrap_design_effect),
        "bootstrap_model_id": metrics.bootstrap_model_id,
        # ⚠ sql/266's criterion-6 block, and the same trap sql/265's block
        # carries: all twelve columns (including `trial_count` and
        # `deflated_sharpe` above) are bound by
        # `strategy_results_dsr_all_or_nothing`, so omitting these ten would
        # make every DSR-carrying result UNWRITABLE. `deflated` is None until
        # 5e-3 runs, and the all-absent case is what existing rows satisfy.
        **_dsr_params(result.deflated),
    }


def _dsr_params(deflated: DeflatedSharpeResult | None) -> dict[str, object]:
    """``sql/266``'s ten columns, all present or all null.

    ⚠ Written as one function returning every key rather than a conditional
    spread, so the two branches cannot diverge in WHICH keys they set. A missing
    key is a psycopg ``ProgrammingError`` at execute time, not a null.
    """
    if deflated is None:
        return {
            "dsr_trade_sharpe": None,
            "dsr_skewness": None,
            "dsr_kurtosis": None,
            "dsr_expected_max_sharpe": None,
            "dsr_independent_trials": None,
            "dsr_average_trial_correlation": None,
            "dsr_trial_sharpe_variance": None,
            "dsr_measured_trials": None,
            "dsr_model_id": None,
            "trial_register_version": None,
        }
    return {
        "dsr_trade_sharpe": _numeric(deflated.trade_sharpe),
        "dsr_skewness": _numeric(deflated.skewness),
        "dsr_kurtosis": _numeric(deflated.kurtosis),
        "dsr_expected_max_sharpe": _numeric(deflated.expected_max_sharpe),
        "dsr_independent_trials": _numeric(deflated.independent_trials),
        "dsr_average_trial_correlation": _numeric(deflated.average_trial_correlation),
        "dsr_trial_sharpe_variance": _numeric(deflated.trial_sharpe_variance),
        "dsr_measured_trials": deflated.measured_trials,
        "dsr_model_id": deflated.model_id,
        "trial_register_version": deflated.trial_register_version,
    }


def _result_from_row(row: Sequence[object]) -> StrategyResult:
    """The inverse of ``_row_params``, in ``_RESULT_COLUMNS`` order.

    ⚠ Positional, and the order is the one literal both statements share. A
    dict-row read would look safer and would silently tolerate the two lists
    drifting apart; a positional read plus the round-trip test in
    ``tests/test_result_ledger.py`` is what actually pins them together.
    """
    (
        strategy_id,
        strategy_version,
        result_version,
        result_scope,
        namespace,
        ambiguity_arm,
        window_start,
        window_end,
        universe_basis,
        corpus_version,
        cost_model_id,
        carry_unmodelled,
        sizing_rule,
        position_rule_set_version,
        outcome_rule_set_version,
        input_rule_set_version,
        evaluated_instrument_count,
        trial_count,
        deflated_sharpe,
        expectancy_per_trade_pct,
        profit_factor,
        cagr_pct,
        annualised_volatility_pct,
        sharpe,
        sortino,
        max_drawdown_pct,
        exposure_time_pct,
        turnover_annualised,
        trade_count,
        effective_sample_size,
        return_vs_buy_and_hold_pct,
        losing_trade_count,
        losing_period_count,
        open_trade_count,
        unpriced_trade_count,
        periods_per_year,
        total_return_pct,
        buy_and_hold_return_pct,
        metric_set_id,
        expectancy_ci_low_pct,
        expectancy_ci_high_pct,
        bootstrap_block_length,
        bootstrap_cluster_count,
        bootstrap_resamples,
        bootstrap_seed,
        bootstrap_design_effect,
        bootstrap_model_id,
        dsr_trade_sharpe,
        dsr_skewness,
        dsr_kurtosis,
        dsr_expected_max_sharpe,
        dsr_independent_trials,
        dsr_average_trial_correlation,
        dsr_trial_sharpe_variance,
        dsr_measured_trials,
        dsr_model_id,
        trial_register_version,
    ) = row

    identity = ResultIdentity(
        strategy_id=str(strategy_id),
        strategy_version=str(strategy_version),
        result_scope=result_scope,  # type: ignore[arg-type]
        namespace=namespace,  # type: ignore[arg-type]
        ambiguity_arm=ambiguity_arm,  # type: ignore[arg-type]
        sizing_rule=str(sizing_rule),
        cost_model_id=str(cost_model_id),
        corpus_version=str(corpus_version),
        window_start=window_start,  # type: ignore[arg-type]
        window_end=window_end,  # type: ignore[arg-type]
        position_rule_set_version=str(position_rule_set_version),
        outcome_rule_set_version=str(outcome_rule_set_version),
        input_rule_set_version=str(input_rule_set_version),
    )
    # ⚠ The stored `result_version` is the hash of everything above, so a
    # mismatch means the stored row and the identity it claims have diverged —
    # a re-hash under a changed `ResultIdentity` payload, or a hand-written row.
    # Refused rather than returned: a result whose version does not describe it
    # is exactly the "different strategy inherits a track record" failure
    # criterion 11 exists to prevent.
    if identity.version != result_version:
        raise ValueError(
            f"stored result_version {result_version!r} does not match the identity it carries "
            f"({identity.version!r}) — the row and its hash have diverged"
        )

    metrics = StrategyMetrics(
        expectancy_per_trade_pct=float(expectancy_per_trade_pct),  # type: ignore[arg-type]
        profit_factor=_as_float(profit_factor),  # type: ignore[arg-type]
        cagr_pct=float(cagr_pct),  # type: ignore[arg-type]
        annualised_volatility_pct=float(annualised_volatility_pct),  # type: ignore[arg-type]
        sharpe=float(sharpe),  # type: ignore[arg-type]
        sortino=_as_float(sortino),  # type: ignore[arg-type]
        max_drawdown_pct=float(max_drawdown_pct),  # type: ignore[arg-type]
        exposure_time_pct=float(exposure_time_pct),  # type: ignore[arg-type]
        turnover_annualised=float(turnover_annualised),  # type: ignore[arg-type]
        trade_count=int(trade_count),  # type: ignore[arg-type]
        effective_sample_size=_as_float(effective_sample_size),  # type: ignore[arg-type]
        return_vs_buy_and_hold_pct=float(return_vs_buy_and_hold_pct),  # type: ignore[arg-type]
        losing_trade_count=int(losing_trade_count),  # type: ignore[arg-type]
        losing_period_count=int(losing_period_count),  # type: ignore[arg-type]
        open_trade_count=int(open_trade_count),  # type: ignore[arg-type]
        unpriced_trade_count=int(unpriced_trade_count),  # type: ignore[arg-type]
        periods_per_year=float(periods_per_year),  # type: ignore[arg-type]
        total_return_pct=float(total_return_pct),  # type: ignore[arg-type]
        buy_and_hold_return_pct=float(buy_and_hold_return_pct),  # type: ignore[arg-type]
        metric_set_id=str(metric_set_id),
        # ⚠ sql/265. `int(...)`/`str(...)` would turn the wholly-absent set into
        # `0`/`"None"` and construct a metric set claiming a bootstrap that never
        # ran, so each of these preserves NULL — `StrategyMetrics` then refuses
        # any partial set on the way back in, which is what makes the round trip
        # a check rather than a copy.
        expectancy_ci_low_pct=_as_float(expectancy_ci_low_pct),  # type: ignore[arg-type]
        expectancy_ci_high_pct=_as_float(expectancy_ci_high_pct),  # type: ignore[arg-type]
        bootstrap_block_length=None if bootstrap_block_length is None else int(bootstrap_block_length),  # type: ignore[arg-type]
        bootstrap_cluster_count=None if bootstrap_cluster_count is None else int(bootstrap_cluster_count),  # type: ignore[arg-type]
        bootstrap_resamples=None if bootstrap_resamples is None else int(bootstrap_resamples),  # type: ignore[arg-type]
        bootstrap_seed=None if bootstrap_seed is None else int(bootstrap_seed),  # type: ignore[arg-type]
        bootstrap_design_effect=_as_float(bootstrap_design_effect),  # type: ignore[arg-type]
        bootstrap_model_id=None if bootstrap_model_id is None else str(bootstrap_model_id),
    )
    # ⚠ sql/266, and the same NULL-preserving rule as the bootstrap block above.
    # The set is all-or-nothing, so ONE probe decides it — `dsr_model_id`, which
    # is the field a partial write is least likely to have set. `StrategyResult`
    # then re-checks the reconstructed object against `trial_count` and
    # `deflated_sharpe`, which is what makes the round trip a check.
    deflated = (
        None
        if dsr_model_id is None
        else DeflatedSharpeResult(
            deflated_sharpe=float(deflated_sharpe),  # type: ignore[arg-type]
            expected_max_sharpe=float(dsr_expected_max_sharpe),  # type: ignore[arg-type]
            trade_sharpe=float(dsr_trade_sharpe),  # type: ignore[arg-type]
            skewness=float(dsr_skewness),  # type: ignore[arg-type]
            kurtosis=float(dsr_kurtosis),  # type: ignore[arg-type]
            effective_sample_size=float(effective_sample_size),  # type: ignore[arg-type]
            declared_trials=int(trial_count),  # type: ignore[arg-type]
            independent_trials=float(dsr_independent_trials),  # type: ignore[arg-type]
            average_trial_correlation=float(dsr_average_trial_correlation),  # type: ignore[arg-type]
            trial_sharpe_variance=float(dsr_trial_sharpe_variance),  # type: ignore[arg-type]
            measured_trials=int(dsr_measured_trials),  # type: ignore[arg-type]
            trial_register_version=str(trial_register_version),
            model_id=str(dsr_model_id),
        )
    )
    return StrategyResult(
        identity=identity,
        metrics=metrics,
        universe_basis=str(universe_basis),
        carry_unmodelled=bool(carry_unmodelled),
        evaluated_instrument_count=int(evaluated_instrument_count),  # type: ignore[arg-type]
        trial_count=None if trial_count is None else int(trial_count),  # type: ignore[arg-type]
        deflated_sharpe=deflated_sharpe,  # type: ignore[arg-type]
        deflated=deflated,
    )


# ---------------------------------------------------------------------------
# The public writers
# ---------------------------------------------------------------------------


def record_holdout_access(conn: psycopg.Connection[tuple], access: HoldoutAccess) -> int:
    """Write one criterion-5 access record. Returns its ``access_id``.

    ⚠ IN THE CALLER'S TRANSACTION, deliberately, and the consequence is worth
    stating: a rolled-back hold-out write rolls back its access record too, so
    the log counts COMMITTED evaluations. Postgres has no autonomous
    transaction, and the alternative — a second connection — would record an
    access for work that never happened and would break the trigger, which needs
    to SEE the record in the same transaction as the row it authorises.
    """
    row = conn.execute(
        _RECORD_ACCESS,
        {
            "strategy_id": access.strategy_id,
            "strategy_version": access.strategy_version,
            "result_version": access.result_version,
            "access_kind": access.access_kind,
            "accessed_by": access.accessed_by,
            "purpose": access.purpose,
        },
    ).fetchone()
    if row is None:  # pragma: no cover - RETURNING on a successful INSERT always yields a row
        raise RuntimeError("access record INSERT returned no access_id")
    return int(row[0])


def store_in_sample_result(conn: psycopg.Connection[tuple], result: StrategyResult) -> int:
    """Store an in-sample result. Returns its ``result_id``.

    ⚠ Writes through the VIEW, so the database refuses a hold-out row here with
    ``WithCheckOptionViolation`` even if the check below were removed. The check
    below exists to name the mistake at the call site rather than at the driver.
    """
    if result.identity.namespace != "in_sample":
        raise ValueError(
            f"store_in_sample_result got a {result.identity.namespace!r} result — a hold-out result goes through "
            "store_holdout_result, which records criterion 5's access"
        )
    row = conn.execute(_INSERT_IN_SAMPLE, _row_params(result)).fetchone()
    if row is None:  # pragma: no cover - RETURNING on a successful INSERT always yields a row
        raise RuntimeError("in-sample result INSERT returned no result_id")
    return int(row[0])


def store_holdout_result(
    conn: psycopg.Connection[tuple],
    result: StrategyResult,
    *,
    accessed_by: str,
    purpose: str,
) -> int:
    """Record the access, then store a hold-out result. Returns its ``result_id``.

    ⚠ THE ORDER IS FORCED BY THE TRIGGER, not chosen: ``sql/264`` refuses a
    hold-out row whose ``(strategy_id, strategy_version, result_version)`` has no
    ``evaluate`` record, so the record must exist first and in the same
    transaction.

    ⚠ ``accessed_by`` and ``purpose`` are KEYWORD-ONLY and have no defaults.
    They are the two fields that turn a count into an audit, and a default
    ``purpose`` would be every caller's purpose.
    """
    if result.identity.namespace != "hold_out":
        raise ValueError(
            f"store_holdout_result got a {result.identity.namespace!r} result — recording a hold-out access for an "
            "in-sample write would inflate the very count criterion 5 audits"
        )
    record_holdout_access(
        conn,
        HoldoutAccess(
            strategy_id=result.identity.strategy_id,
            strategy_version=result.identity.strategy_version,
            result_version=result.identity.version,
            access_kind="evaluate",
            accessed_by=accessed_by,
            purpose=purpose,
        ),
    )
    row = conn.execute(_INSERT_HOLDOUT, _row_params(result)).fetchone()
    if row is None:  # pragma: no cover - RETURNING on a successful INSERT always yields a row
        raise RuntimeError("hold-out result INSERT returned no result_id")
    return int(row[0])


def read_holdout_results(
    conn: psycopg.Connection[tuple],
    strategy_id: str,
    strategy_version: str,
    *,
    accessed_by: str,
    purpose: str,
) -> tuple[StrategyResult, ...]:
    """The sanctioned door to the withheld side. Records the access first.

    ⚠ THE ACCESS IS RECORDED EVEN WHEN THE READ RETURNS NOTHING. Looking is the
    event criterion 5 governs, and logging only successful looks would make the
    log a function of what happened to be stored rather than of what was asked
    for.
    """
    record_holdout_access(
        conn,
        HoldoutAccess(
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            result_version=None,
            access_kind="read",
            accessed_by=accessed_by,
            purpose=purpose,
        ),
    )
    rows = conn.execute(
        _SELECT_HOLDOUT,
        {"strategy_id": strategy_id, "strategy_version": strategy_version},
    ).fetchall()
    return tuple(_result_from_row(row) for row in rows)


def holdout_access_counts(
    conn: psycopg.Connection[tuple],
    strategy_id: str,
    strategy_version: str,
) -> HoldoutAccessCounts:
    """``PromotionCandidate``'s two hold-out inputs. See ``HoldoutAccessCounts``."""
    params = {"strategy_id": strategy_id, "strategy_version": strategy_version}
    evaluations = conn.execute(_COUNT_HOLDOUT_RESULTS, params).fetchone()
    accesses = conn.execute(_COUNT_EVALUATE_ACCESSES, params).fetchone()
    if evaluations is None or accesses is None:  # pragma: no cover - count() always returns a row
        raise RuntimeError("count query returned no row")
    return HoldoutAccessCounts(holdout_evaluations=int(evaluations[0]), recorded_accesses=int(accesses[0]))


__all__ = [
    "HOLDOUT_ACCESS_KINDS",
    "HoldoutAccess",
    "HoldoutAccessCounts",
    "HoldoutAccessKind",
    "holdout_access_counts",
    "read_holdout_results",
    "record_holdout_access",
    "store_holdout_result",
    "store_in_sample_result",
]
