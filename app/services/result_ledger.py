"""Phase 5e-1 — the result writer, and criterion 5's hold-out access log.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §5.2 (the frozen
split), §6 (#2288 clauses 2-4), §8 (stage 5e), acceptance C5. Parent:
``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md`` criterion 5.
Row shape: ``sql/262`` + ``sql/263``. Namespace and trigger: ``sql/264``.
Gate and frozen literals: ``app/services/strategy_result.py``. Refs #2240, #2288.

⚠ STAGE 5e-5c ADDED THE TWO WRITERS THE EARLIER SUB-STAGES LEFT UNWRITTEN, and
both are pair/whole writers rather than row writers, for one reason: the state
each of them makes unreachable is a HALF-WRITTEN one that reads as complete.
``store_*_arm_pair`` cannot leave criterion 9 with a single arm, and
``store_walk_forward_folds`` (``sql/269``) cannot leave criterion 5's split with
three folds of four.

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
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Final, Literal, cast, get_args

import psycopg

from app.services.deflated_sharpe import DeflatedSharpeResult
from app.services.prereg_contract import (
    ForwardShadowFloor,
    PreregDeclaration,
    PreregPurpose,
    declaration_refusals,
)
from app.services.random_entry_cohort import SyntheticControl
from app.services.strategy_result import (
    ResultIdentity,
    StrategyResult,
)
from app.services.strategy_statistics import StrategyMetrics
from app.services.walk_forward import (
    WALK_FORWARD_MODEL_ID,
    Fold,
    FoldCensus,
    FoldRecord,
    WalkForwardFolds,
)

#: ``sql/264``'s ``access_kind`` vocabulary. ⚠ Two kinds because they are
#: different governance events: an ``evaluate`` is a hold-out number being
#: PRODUCED, a ``read`` is one being LOOKED AT, and criterion 5's *"evaluated
#: more than once"* is about the second.
HoldoutAccessKind = Literal["evaluate", "read"]
HOLDOUT_ACCESS_KINDS: Final[frozenset[str]] = frozenset(get_args(HoldoutAccessKind))


class PreregDeclarationRefused(RuntimeError):
    """#2599 refused a preregistration freeze or an outcome look.

    ⚠ AN EXCEPTION AND NOT A RETURN VALUE, which is the opposite of
    ``check_promotable``'s shape, and the asymmetry is deliberate. The promotion
    gate's caller is phase 7, which must WRITE a decision row either way. This
    gate's job is to stop a look from happening at all — and a caller that can
    ignore the answer by not reading it is not a gate.

    ⚠ Carries the codes, never a bare message: "refused" with no code tells an
    operator nothing about which of the five rules fired.
    """

    def __init__(self, strategy_id: str, strategy_version: str, refusals: tuple[str, ...]) -> None:
        self.strategy_id = strategy_id
        self.strategy_version = strategy_version
        self.refusals = refusals
        super().__init__(
            f"preregistration declaration for {strategy_id}/{strategy_version} refused: {', '.join(refusals)}"
        )


@dataclass(frozen=True)
class FrozenPreregistration:
    """A declaration as it was stored, with the identity the row carries.

    ⚠ The ``declaration_sha256`` is the STORED digest, kept separate from
    ``declaration.sha256`` (which recomputes it) precisely so the two can be
    compared. Equal means the row still says what it said when it was frozen.
    """

    declaration_id: int
    declaration: PreregDeclaration
    declaration_sha256: str

    @property
    def digest_intact(self) -> bool:
        return self.declaration_sha256 == self.declaration.sha256


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

_FREEZE_DECLARATION = """
    INSERT INTO strategy_preregistration_declarations (
        strategy_id, strategy_version, contract_version, prereg_purpose,
        structural_refusal_policy_version, declared_universe_basis, declared_carry_unmodelled,
        declared_fx_unmodelled,
        expected_structural_refusals, min_forward_decision_dates, min_forward_calendar_weeks,
        forward_shadow_derivation, declared_by, declaration_sha256
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(contract_version)s, %(prereg_purpose)s,
        %(structural_refusal_policy_version)s, %(declared_universe_basis)s, %(declared_carry_unmodelled)s,
        %(declared_fx_unmodelled)s,
        %(expected_structural_refusals)s, %(min_forward_decision_dates)s, %(min_forward_calendar_weeks)s,
        %(forward_shadow_derivation)s, %(declared_by)s, %(declaration_sha256)s
    )
    RETURNING declaration_id
"""

_SELECT_DECLARATION = """
    SELECT declaration_id, strategy_id, strategy_version, contract_version, prereg_purpose,
           structural_refusal_policy_version, declared_universe_basis, declared_carry_unmodelled,
           declared_fx_unmodelled,
           expected_structural_refusals, min_forward_decision_dates, min_forward_calendar_weeks,
           forward_shadow_derivation, declared_by, declaration_sha256
    FROM strategy_preregistration_declarations
    WHERE strategy_id = %(strategy_id)s AND strategy_version = %(strategy_version)s
"""

#: #2614 — the provenance re-check for an evaluator that stores no result row.
#: Joins the declaration to ONE named access row and lets the database compare
#: the two server-side timestamps, so no client clock enters the ordering.
_SELECT_ACCESS_PROVENANCE = """
    SELECT a.access_kind,
           a.strategy_id,
           a.strategy_version,
           d.frozen_at < a.accessed_at AS declared_before_access,
           NOT EXISTS (
               SELECT 1
               FROM strategy_holdout_accesses later
               WHERE later.strategy_id = a.strategy_id
                 AND later.strategy_version = a.strategy_version
                 AND later.access_kind = 'read'
                 AND (later.accessed_at, later.access_id) > (a.accessed_at, a.access_id)
           ) AS is_latest_read
    FROM strategy_holdout_accesses a
    JOIN strategy_preregistration_declarations d
      ON d.strategy_id = a.strategy_id AND d.strategy_version = a.strategy_version
    WHERE a.access_id = %(access_id)s AND d.declaration_id = %(declaration_id)s
"""

_RESULT_COLUMNS = """
    strategy_id, strategy_version, result_version, result_scope, namespace,
    ambiguity_arm, quarantine_arm, window_start, window_end, purpose, universe_basis, corpus_version,
    cost_model_id, carry_unmodelled, fx_unmodelled, sizing_rule, benchmark_rule, return_basis,
    position_rule_set_version,
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
    trial_register_version,
    synthetic_control_model_id, synthetic_control_size, synthetic_control_root_seed,
    synthetic_control_mean_return_pct, synthetic_control_mean_return_ci_low_pct,
    synthetic_control_mean_return_ci_high_pct, synthetic_control_sharpe_percentile,
    synthetic_control_sharpe_threshold, synthetic_control_return_threshold_pct,
    synthetic_control_passed
"""

_RESULT_VALUES = """
    %(strategy_id)s, %(strategy_version)s, %(result_version)s, %(result_scope)s, %(namespace)s,
    %(ambiguity_arm)s, %(quarantine_arm)s, %(window_start)s, %(window_end)s, %(purpose)s,
    %(universe_basis)s, %(corpus_version)s,
    %(cost_model_id)s, %(carry_unmodelled)s, %(fx_unmodelled)s, %(sizing_rule)s, %(benchmark_rule)s,
    %(return_basis)s,
    %(position_rule_set_version)s,
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
    %(dsr_measured_trials)s, %(dsr_model_id)s, %(trial_register_version)s,
    %(synthetic_control_model_id)s, %(synthetic_control_size)s, %(synthetic_control_root_seed)s,
    %(synthetic_control_mean_return_pct)s, %(synthetic_control_mean_return_ci_low_pct)s,
    %(synthetic_control_mean_return_ci_high_pct)s, %(synthetic_control_sharpe_percentile)s,
    %(synthetic_control_sharpe_threshold)s, %(synthetic_control_return_threshold_pct)s,
    %(synthetic_control_passed)s
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
    ORDER BY result_version, result_scope, ambiguity_arm, quarantine_arm
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

#: ⚠ Counts BOTH sibling versions in one statement rather than probing twice —
#: two round trips could straddle a concurrent write and report a pair that was
#: never simultaneously present.
_COUNT_ARM_PAIR = """
    SELECT count(*)
    FROM strategy_results_store
    WHERE strategy_id = %(strategy_id)s
      AND strategy_version = %(strategy_version)s
      AND result_version = ANY(%(result_versions)s)
"""

#: ``sql/269``. Column order is shared with the read below and with
#: ``_fold_row``; the round-trip test is what pins the three together.
_FOLD_COLUMNS = """
    fold_index, walk_forward_model_id, fold_count, first_index, last_index,
    first_date, last_date, bar_count, embargo_bars,
    test_count, train_count, purged_count, embargoed_count
"""

_INSERT_FOLD = f"""
    INSERT INTO strategy_result_folds (result_id, {_FOLD_COLUMNS}) VALUES (
        %(result_id)s, %(fold_index)s, %(walk_forward_model_id)s, %(fold_count)s,
        %(first_index)s, %(last_index)s, %(first_date)s, %(last_date)s, %(bar_count)s, %(embargo_bars)s,
        %(test_count)s, %(train_count)s, %(purged_count)s, %(embargoed_count)s
    )
"""  # noqa: S608 - `_FOLD_COLUMNS` is a module-level literal, no caller input reaches it

_SELECT_FOLDS = f"""
    SELECT {_FOLD_COLUMNS}
    FROM strategy_result_folds
    WHERE result_id = %(result_id)s
    ORDER BY fold_index
"""  # noqa: S608 - as above


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
        "quarantine_arm": identity.quarantine_arm,
        "window_start": identity.window_start,
        "window_end": identity.window_end,
        "purpose": result.purpose,
        "universe_basis": result.universe_basis,
        "corpus_version": identity.corpus_version,
        "cost_model_id": identity.cost_model_id,
        "carry_unmodelled": result.carry_unmodelled,
        "fx_unmodelled": result.fx_unmodelled,
        "sizing_rule": identity.sizing_rule,
        "benchmark_rule": identity.benchmark_rule,
        "return_basis": identity.return_basis,
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
        # ⚠ sql/268's §9 block, bound by `strategy_results_synthetic_all_or_nothing`
        # — the same trap as the two blocks above, and the same one-function
        # answer so the present and absent branches cannot set different keys.
        **_synthetic_params(result.synthetic_control),
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


def _synthetic_params(control: SyntheticControl | None) -> dict[str, object]:
    """``sql/268``'s ten columns, all present or all null.

    ⚠ THE STRATEGY'S OWN SHARPE AND RETURN ARE NOT AMONG THEM. Both are already
    on the row, and `strategy_results_synthetic_verdict_derived` re-derives
    `synthetic_control_passed` from the stored `sharpe` and the thresholds here —
    so a second copy would be a second thing to keep in step. `StrategyResult`
    binds the object's copy to `metrics` at assembly time.
    """
    if control is None:
        return {
            "synthetic_control_model_id": None,
            "synthetic_control_size": None,
            "synthetic_control_root_seed": None,
            "synthetic_control_mean_return_pct": None,
            "synthetic_control_mean_return_ci_low_pct": None,
            "synthetic_control_mean_return_ci_high_pct": None,
            "synthetic_control_sharpe_percentile": None,
            "synthetic_control_sharpe_threshold": None,
            "synthetic_control_return_threshold_pct": None,
            "synthetic_control_passed": None,
        }
    return {
        "synthetic_control_model_id": control.model_id,
        "synthetic_control_size": control.cohort_size,
        "synthetic_control_root_seed": control.root_seed,
        "synthetic_control_mean_return_pct": _numeric(control.mean_return_pct),
        "synthetic_control_mean_return_ci_low_pct": _numeric(control.mean_return_ci_low_pct),
        "synthetic_control_mean_return_ci_high_pct": _numeric(control.mean_return_ci_high_pct),
        "synthetic_control_sharpe_percentile": _numeric(control.sharpe_percentile),
        "synthetic_control_sharpe_threshold": _numeric(control.cohort_sharpe_threshold),
        "synthetic_control_return_threshold_pct": _numeric(control.cohort_return_threshold_pct),
        "synthetic_control_passed": control.passed,
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
        quarantine_arm,
        window_start,
        window_end,
        purpose,
        universe_basis,
        corpus_version,
        cost_model_id,
        carry_unmodelled,
        fx_unmodelled,
        sizing_rule,
        benchmark_rule,
        return_basis,
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
        synthetic_control_model_id,
        synthetic_control_size,
        synthetic_control_root_seed,
        synthetic_control_mean_return_pct,
        synthetic_control_mean_return_ci_low_pct,
        synthetic_control_mean_return_ci_high_pct,
        synthetic_control_sharpe_percentile,
        synthetic_control_sharpe_threshold,
        synthetic_control_return_threshold_pct,
        synthetic_control_passed,
    ) = row

    identity = ResultIdentity(
        strategy_id=str(strategy_id),
        strategy_version=str(strategy_version),
        result_scope=result_scope,  # type: ignore[arg-type]
        namespace=namespace,  # type: ignore[arg-type]
        ambiguity_arm=ambiguity_arm,  # type: ignore[arg-type]
        quarantine_arm=quarantine_arm,  # type: ignore[arg-type]
        sizing_rule=str(sizing_rule),
        benchmark_rule=str(benchmark_rule),
        return_basis=str(return_basis),
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
    # ⚠ sql/268, same NULL-preserving rule and same one-probe decision as the
    # two blocks above — `synthetic_control_model_id`, which is the field a
    # partial write is least likely to have set. The strategy side is rebuilt
    # FROM `metrics`, because the table deliberately stores only the cohort side.
    control = (
        None
        if synthetic_control_model_id is None
        else SyntheticControl(
            model_id=str(synthetic_control_model_id),
            cohort_size=int(synthetic_control_size),  # type: ignore[arg-type]
            root_seed=int(synthetic_control_root_seed),  # type: ignore[arg-type]
            mean_return_pct=float(synthetic_control_mean_return_pct),  # type: ignore[arg-type]
            mean_return_ci_low_pct=float(synthetic_control_mean_return_ci_low_pct),  # type: ignore[arg-type]
            mean_return_ci_high_pct=float(synthetic_control_mean_return_ci_high_pct),  # type: ignore[arg-type]
            sharpe_percentile=float(synthetic_control_sharpe_percentile),  # type: ignore[arg-type]
            cohort_sharpe_threshold=float(synthetic_control_sharpe_threshold),  # type: ignore[arg-type]
            strategy_sharpe=metrics.sharpe,
            cohort_return_threshold_pct=float(synthetic_control_return_threshold_pct),  # type: ignore[arg-type]
            strategy_return_pct=metrics.total_return_pct,
        )
    )
    # ⚠ THE STORED VERDICT IS RE-DERIVED AND COMPARED, which is what makes this
    # a round trip rather than a copy. `sql/268` already CHECKs the same
    # implication in SQL; a disagreement here means the Python rule and the SQL
    # rule have drifted apart, which no single-sided test would show.
    if control is not None and bool(synthetic_control_passed) != control.passed:
        raise ValueError(
            f"stored synthetic_control_passed {synthetic_control_passed!r} disagrees with the verdict its own stored "
            f"inputs produce ({control.passed!r}) — the row's thresholds and its flag have diverged"
        )
    return StrategyResult(
        identity=identity,
        purpose=purpose,  # type: ignore[arg-type]
        metrics=metrics,
        universe_basis=str(universe_basis),
        carry_unmodelled=bool(carry_unmodelled),
        fx_unmodelled=bool(fx_unmodelled),
        evaluated_instrument_count=int(evaluated_instrument_count),  # type: ignore[arg-type]
        trial_count=None if trial_count is None else int(trial_count),  # type: ignore[arg-type]
        deflated_sharpe=deflated_sharpe,  # type: ignore[arg-type]
        deflated=deflated,
        synthetic_control=control,
    )


# ---------------------------------------------------------------------------
# The public writers
# ---------------------------------------------------------------------------


def _refuse_incoherent_declaration(
    conn: psycopg.Connection[tuple], strategy_id: str, strategy_version: str
) -> FrozenPreregistration | None:
    """Re-check a frozen declaration, or pass when the trial froze none.

    ⚠ RE-CHECKED ON EVERY LOOK, not only at freeze time. A declaration frozen
    under a structural-refusal policy that has since been superseded stops
    authorising looks the moment the policy moves — which is the point of
    versioning it, and the same shape ``trial_register_superseded`` already has.

    ⚠ The stored digest is verified too. A row edited around the immutability
    trigger (a superuser can disable one) no longer matches the bytes it was
    frozen over, and a declaration that has been rewritten is not a declaration.
    """
    frozen = load_preregistration(conn, strategy_id, strategy_version)
    if frozen is None:
        return None
    refusals = [str(code) for code in declaration_refusals(frozen.declaration)]
    if not frozen.digest_intact:
        refusals.append("declaration_digest_mismatch")
    if refusals:
        raise PreregDeclarationRefused(strategy_id, strategy_version, tuple(refusals))
    return frozen


def freeze_preregistration(conn: psycopg.Connection[tuple], declaration: PreregDeclaration) -> int:
    """Freeze one #2599 declaration. Returns its ``declaration_id``.

    ⚠ REFUSES AN INCOHERENT DECLARATION AT FREEZE TIME, which is the only time
    refusing it is cheap. After this returns, the row is immutable (``sql/333``
    barred UPDATE *and* DELETE — "unfreeze, look, re-freeze" is the same
    fabrication with an extra step) and the trial's terms are settled.

    ⚠ A ``falsification_only`` declaration over survivor-only stamps is COHERENT
    and is accepted. It still charges the trial register, as any look must; what
    it cannot do is happen silently.
    """
    refusals = declaration_refusals(declaration)
    if refusals:
        raise PreregDeclarationRefused(
            declaration.strategy_id, declaration.strategy_version, tuple(str(code) for code in refusals)
        )
    row = conn.execute(
        _FREEZE_DECLARATION,
        {
            "strategy_id": declaration.strategy_id,
            "strategy_version": declaration.strategy_version,
            "contract_version": declaration.contract_version,
            "prereg_purpose": declaration.prereg_purpose,
            "structural_refusal_policy_version": declaration.structural_refusal_policy_version,
            "declared_universe_basis": declaration.declared_universe_basis,
            "declared_carry_unmodelled": declaration.declared_carry_unmodelled,
            "declared_fx_unmodelled": declaration.declared_fx_unmodelled,
            "expected_structural_refusals": list(declaration.expected_structural_refusals),
            "min_forward_decision_dates": declaration.forward_shadow.min_independent_decision_dates,
            "min_forward_calendar_weeks": declaration.forward_shadow.min_calendar_weeks,
            "forward_shadow_derivation": declaration.forward_shadow.derivation,
            "declared_by": declaration.declared_by,
            "declaration_sha256": declaration.sha256,
        },
    ).fetchone()
    if row is None:  # pragma: no cover - RETURNING on a successful INSERT always yields a row
        raise RuntimeError("preregistration declaration INSERT returned no declaration_id")
    return int(row[0])


def load_preregistration(
    conn: psycopg.Connection[tuple], strategy_id: str, strategy_version: str
) -> FrozenPreregistration | None:
    """The frozen declaration for one trial, or ``None`` if nothing is frozen.

    ⚠ ``None`` is NOT a refusal here. A trial that never froze one behaves as it
    did before #2599 — no retroactive invalidation, which is what keeps the 224
    existing access rows and every current evaluator working. The paths that
    REQUIRE a declaration say so themselves (``require_outcome_access``).
    """
    row = conn.execute(
        _SELECT_DECLARATION,
        {"strategy_id": strategy_id, "strategy_version": strategy_version},
    ).fetchone()
    if row is None:
        return None
    return FrozenPreregistration(
        declaration_id=int(row[0]),
        declaration=PreregDeclaration(
            strategy_id=str(row[1]),
            strategy_version=str(row[2]),
            contract_version=str(row[3]),
            prereg_purpose=cast(PreregPurpose, str(row[4])),
            structural_refusal_policy_version=str(row[5]),
            declared_universe_basis=str(row[6]),
            declared_carry_unmodelled=bool(row[7]),
            declared_fx_unmodelled=bool(row[8]),
            expected_structural_refusals=tuple(row[9] or ()),
            forward_shadow=ForwardShadowFloor(
                min_independent_decision_dates=int(row[10]),
                min_calendar_weeks=int(row[11]),
                derivation=str(row[12]),
            ),
            declared_by=str(row[13]),
        ),
        declaration_sha256=str(row[14]),
    )


def record_holdout_access(conn: psycopg.Connection[tuple], access: HoldoutAccess) -> int:
    """Write one criterion-5 access record. Returns its ``access_id``.

    ⚠ IN THE CALLER'S TRANSACTION, deliberately, and the consequence is worth
    stating: a rolled-back hold-out write rolls back its access record too, so
    the log counts COMMITTED evaluations. Postgres has no autonomous
    transaction, and the alternative — a second connection — would record an
    access for work that never happened and would break the trigger, which needs
    to SEE the record in the same transaction as the row it authorises.

    ⚠⚠ #2599's GATE SITS HERE, AND THE PLACEMENT IS THE DESIGN. All three paved
    doors to the withheld side — ``store_holdout_result``,
    ``read_holdout_results`` and ``store_walk_forward_folds`` — already funnel
    through this function, so one check covers every one of them and no future
    door has to remember a convention. A trial with no frozen declaration is
    unaffected; a trial that HAS frozen one cannot escape through the old door.
    """
    _refuse_incoherent_declaration(conn, access.strategy_id, access.strategy_version)
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


def _refuse_declared_stamp_substitution(conn: psycopg.Connection[tuple], result: StrategyResult) -> None:
    """Refuse a hold-out row whose stamps disagree with the frozen declaration.

    ⚠ THREE CODES, NOT ONE, because they are different substitutions with
    different operator actions: a universe-basis swap means the corpus the run
    used is not the corpus that was declared, and a carry or FX swap means the
    cost model is not the one that was declared. ⚠ #2363 split the cost pair —
    each is checked on its own, so a run that substituted BOTH reports both
    rather than one standing in for the other.
    """
    frozen = load_preregistration(conn, result.identity.strategy_id, result.identity.strategy_version)
    if frozen is None:
        return
    declared = frozen.declaration
    refusals: list[str] = []
    if result.universe_basis != declared.declared_universe_basis:
        refusals.append("declared_universe_basis_substituted")
    if result.carry_unmodelled != declared.declared_carry_unmodelled:
        refusals.append("declared_carry_unmodelled_substituted")
    if result.fx_unmodelled != declared.declared_fx_unmodelled:
        refusals.append("declared_fx_unmodelled_substituted")
    if refusals:
        raise PreregDeclarationRefused(result.identity.strategy_id, result.identity.strategy_version, tuple(refusals))


def require_outcome_access(conn: psycopg.Connection[tuple], access: HoldoutAccess) -> int:
    """#2599's paved door: a look that REQUIRES a frozen declaration first.

    ⚠ THE DIFFERENCE FROM ``record_holdout_access`` IS THE MISSING CASE. That
    one leaves a trial with no declaration alone, because #2599 does not
    retroactively invalidate the trials that predate it. This one refuses it
    (``preregistration_not_frozen``) — so a NEW evaluator cannot open outcomes
    on a trial that never declared its purpose.

    New evaluator scripts call this. Existing ones keep their current door and
    are enforced the moment their trial freezes a declaration.

    ⚠ Stated limit, not papered over: a direct ``SELECT`` against
    ``strategy_results_store`` remains physically possible — ``sql/264``'s
    header measured that RLS does not bind this app's superuser connection.
    This closes every path that goes through the ledger.

    ⚠⚠ CORRECTED BY #2614. This docstring used to add *"which is every path we
    have written"*, and that clause was FALSE when it was written. The ledger
    covers every path that WRITES A RESULT ROW. A sealed study that computes its
    own statistics from raw price windows and emits a signed artifact stores
    nothing in ``strategy_results_store``, so there is no ledger call to
    intercept — and ``scripts/evaluate_2582_schedule13d_outcomes.py`` was exactly
    that shape, complete and runnable, on the day #2599 merged. Three such
    scripts existed. C-4 now calls this function explicitly and re-checks the
    result through ``verify_outcome_access_provenance``; the general rule is that
    a second path needs its own gate, and
    ``tests/test_sealed_outcome_scripts_are_gated.py`` is what notices a new one.
    """
    if load_preregistration(conn, access.strategy_id, access.strategy_version) is None:
        raise PreregDeclarationRefused(access.strategy_id, access.strategy_version, ("preregistration_not_frozen",))
    return record_holdout_access(conn, access)


def verify_outcome_access_provenance(
    conn: psycopg.Connection[tuple],
    *,
    strategy_id: str,
    strategy_version: str,
    declaration_id: int,
    access_id: int,
) -> FrozenPreregistration:
    """#2614's read-only re-check, for an evaluator that writes no result row.

    ⚠⚠ IT VERIFIES TWO ROWS, AND CHECKING ONLY THE DECLARATION WOULD BE
    BYPASSABLE. A caller holding a real ``declaration_id`` can construct whatever
    value object the evaluator's gate happens to be, so a check that reads only
    the declaration lets every price window open with no access row written at
    all. Re-loading the access row BY ID is what turns a returned ``access_id``
    from decoration into enforcement — and because a rolled-back INSERT leaves no
    visible row, the same lookup is what proves the access COMMITTED.

    ⚠ ``frozen_at < accessed_at`` is ASSERTED, not assumed. Statement ordering
    gives it for free in the normal case, which is precisely why it is worth
    stating: "the declaration predates the look" is the entire property #2599
    exists to establish, and an invariant nobody asserts is one nobody notices
    breaking. Both timestamps are server-side ``DEFAULT now()``.

    ⚠ READ-ONLY BY CONSTRUCTION — it runs inside the evaluator's
    ``REPEATABLE READ READ ONLY`` transaction, where an INSERT would fail. The
    write half is ``require_outcome_access``, which the caller must have run and
    committed first.

    ⚠⚠ STATED LIMITS, because overstating one is what #2614 exists to fix — and
    the first draft of this docstring did exactly that until Codex checkpoint 3
    called it. Three things this does NOT establish:

    1. **It is not single-use.** The named access must be the NEWEST ``read``
       for the trial, which rules out reuse of a STALE id. A caller holding the
       newest id can reuse it until some other read is recorded. Real single-use
       needs consumable state, and this is an append-only audit log.
    2. **It is snapshot-relative.** Callers run inside a ``REPEATABLE READ``
       transaction, so a read committed after that snapshot is invisible and two
       concurrent gates can both pass.
    3. **It does not bind a caller who bypasses these helpers.** One who
       hand-constructs the value object AND records a fresh access first is
       indistinguishable from a legitimate run; one who ``SELECT``s the price
       tables directly is not observed at all — the same acknowledged class as
       ``sql/264``'s RLS finding.

    What IS enforced: every path that loads outcomes THROUGH THIS EVALUATOR
    presents a coherent, digest-intact declaration frozen strictly before a
    committed, non-superseded access bound to the same trial. That is a narrower
    claim than "no path exists", and narrower is the point.

    Raises ``PreregDeclarationRefused`` carrying every code that fired.
    """
    frozen = _refuse_incoherent_declaration(conn, strategy_id, strategy_version)
    refusals: list[str] = []
    if frozen is None:
        refusals.append("preregistration_not_frozen")
    elif frozen.declaration_id != declaration_id:
        refusals.append("declaration_identity_mismatch")
    row = conn.execute(
        _SELECT_ACCESS_PROVENANCE,
        {"access_id": access_id, "declaration_id": declaration_id},
    ).fetchone()
    if row is None:
        # ⚠ Covers three distinct failures with one code on purpose: no access
        # row, an access row for a DIFFERENT trial, and an access whose
        # declaration is not the one named. All three mean the same thing to an
        # operator — this look is not the one that was authorised.
        refusals.append("outcome_access_not_recorded")
    else:
        access_kind, access_strategy_id, access_strategy_version, frozen_before_access, is_latest_read = row
        if (str(access_strategy_id), str(access_strategy_version)) != (strategy_id, strategy_version):
            refusals.append("outcome_access_trial_mismatch")
        if str(access_kind) != "read":
            refusals.append("outcome_access_kind_mismatch")
        if not frozen_before_access:
            refusals.append("declaration_not_frozen_before_access")
        # ⚠ #2614 Codex checkpoint 2 — refuses a SUPERSEDED access id. A real run
        # always names the newest `read`, because it has just written it.
        #
        # ⚠⚠ WHAT THIS DOES NOT DO, stated because overstating an enforcement is
        # the defect this whole ticket exists to fix (Codex checkpoint 3 caught
        # the first draft of this comment claiming it stopped "look after look").
        # It does NOT make an access single-use: a caller holding the NEWEST
        # `access_id` can reuse it until some other read is recorded. What it
        # rules out is reuse of a STALE id — the careless case, where an old
        # gate value is carried forward past later activity. Genuine single-use
        # would need consumable state, and `strategy_holdout_accesses` is an
        # append-only audit log by design.
        #
        # ⚠ And it is snapshot-relative: every caller is inside the evaluator's
        # REPEATABLE READ transaction, so a read committed after that snapshot is
        # invisible here. Two concurrent gates can therefore both pass. Within
        # one run the four population calls share the snapshot, so they cannot
        # disagree with each other.
        #
        # Ordered on `(accessed_at, access_id)` so two accesses sharing a
        # transaction timestamp still order deterministically.
        if not is_latest_read:
            refusals.append("outcome_access_superseded_by_a_later_look")
    if refusals:
        raise PreregDeclarationRefused(strategy_id, strategy_version, tuple(refusals))
    return cast(FrozenPreregistration, frozen)


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
    # ⚠ #2599 — the row's ACTUAL stamps must match what the trial declared. The
    # declaration's whole force comes from being made before the run; a trial
    # that declares `survivorship_free` and then stores `survivor_only` has
    # substituted the thing that was checked, and only this comparison catches
    # it. Runs BEFORE the check, so the substituted row is never written.
    _refuse_declared_stamp_substitution(conn, result)
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


# ---------------------------------------------------------------------------
# Criterion 9's arm PAIR (stage 5e-5c)
# ---------------------------------------------------------------------------


def _check_arm_pair(masked: StrategyResult, admitted: StrategyResult) -> None:
    """Refuse anything that is not one measurement under both arms.

    ⚠⚠ THE CHECK IS THAT THE TWO IDENTITIES DIFFER IN THE ARM AND IN NOTHING
    ELSE, and it is written as one comparison rather than as a field-by-field
    sweep: rebuild the masked identity with the admitted arm and require it to
    EQUAL the admitted one. A sweep would have to be extended by hand every time
    ``ResultIdentity`` gains a member — and the member most likely to be
    forgotten is the newest one, which is the one a pair is most likely to
    differ in.

    ⚠ WHY A PAIR WRITER EXISTS AT ALL, when ``store_in_sample_result`` can
    already write either arm: criterion 9 is satisfied by the COMPARISON, not by
    an arm. A lone ``admitted`` row is a number nobody may quote (``sql/267``)
    and a lone ``masked`` row is the state the promotion gate refuses as
    ``quarantine_arms_not_compared``. Storing them through one call makes the
    half-written state unreachable rather than merely discouraged — a rolled-back
    transaction leaves neither, and a raise here leaves neither.
    """
    if masked.identity.quarantine_arm != "masked" or admitted.identity.quarantine_arm != "admitted":
        raise ValueError(
            f"arms are mislabelled: {masked.identity.quarantine_arm!r} / {admitted.identity.quarantine_arm!r} — the "
            "pair is (masked, admitted) in that order, and the admitted arm is never the number to quote"
        )
    expected = replace(masked.identity, quarantine_arm="admitted")
    if expected != admitted.identity:
        raise ValueError(
            "the two arms do not describe one measurement: the admitted identity is not the masked identity with the "
            f"arm flipped ({expected.version} expected, {admitted.identity.version} given). A delta between results "
            "that differ in anything else is a comparison of populations, not of handling"
        )


def store_in_sample_arm_pair(
    conn: psycopg.Connection[tuple],
    masked: StrategyResult,
    admitted: StrategyResult,
) -> tuple[int, int]:
    """Store criterion 9's two in-sample arms together. Returns both ``result_id``s.

    ⚠ TWO FUNCTIONS AND NOT ONE BRANCHING ON THE NAMESPACE, deliberately — the
    module header's mechanism is that *the two writers target different
    relations*, and a single pair writer that decided at runtime would put a
    hold-out write behind a name that does not say hold-out.

    ⚠⚠ THE PAIR OWNS ITS OWN TRANSACTION, and this is the one place in the
    module that does. The single-row writers deliberately run in the caller's
    transaction; a PAIR writer cannot, because its whole claim is that the
    lone-arm state is unreachable — and on an autocommit connection (this repo
    opens several, e.g. ``app/main.py``'s lifespan guards) the first insert
    would COMMIT before the second failed. ``conn.transaction()`` is a savepoint
    inside an existing transaction and a real one otherwise, so the guarantee
    does not depend on how the caller connected. Found by Codex at checkpoint 2.
    """
    _check_arm_pair(masked, admitted)
    with conn.transaction():
        return (store_in_sample_result(conn, masked), store_in_sample_result(conn, admitted))


def store_holdout_arm_pair(
    conn: psycopg.Connection[tuple],
    masked: StrategyResult,
    admitted: StrategyResult,
    *,
    accessed_by: str,
    purpose: str,
) -> tuple[int, int]:
    """Store criterion 9's two hold-out arms together. Returns both ``result_id``s.

    ⚠ TWO ``evaluate`` ACCESS RECORDS, one per arm, because ``sql/264``'s
    trigger matches on ``result_version`` and the arms have different ones. That
    is the correct count and not double-counting: two hold-out numbers were
    produced, and criterion 5 audits evaluations rather than sessions.

    ⚠⚠ OWNS ITS TRANSACTION, for the reason ``store_in_sample_arm_pair`` gives —
    and with one more consequence here: four statements have to stand or fall
    together, since a committed access record for a row that never landed would
    inflate exactly the count criterion 5 audits.
    """
    _check_arm_pair(masked, admitted)
    with conn.transaction():
        return (
            store_holdout_result(conn, masked, accessed_by=accessed_by, purpose=purpose),
            store_holdout_result(conn, admitted, accessed_by=accessed_by, purpose=purpose),
        )


def quarantine_arms_compared(
    conn: psycopg.Connection[tuple],
    identity: ResultIdentity,
    *,
    accessed_by: str,
    purpose: str,
) -> bool:
    """``PromotionCandidate.quarantine_arms_compared``, read off the database.

    True when BOTH arms of ``identity`` are stored — the identity's own version
    and its sibling with the arm flipped. ⚠ A BOOLEAN AND NOT A MAGNITUDE:
    criterion 9 requires the exclusion visible, not small, and
    ``PromotionRefusal``'s comment records why no ``quarantine_material`` twin
    exists.

    ⚠⚠ IT COUNTS ROWS, NEVER METRICS, and for a ``hold_out`` identity it still
    RECORDS A READ. Presence is a fact about the withheld side, so the access is
    logged for the reason ``read_holdout_results`` logs an empty read: *looking
    is the event criterion 5 governs*. An ``in_sample`` identity records
    nothing — inflating the log with in-sample lookups would make the audit
    trail a count of automation rather than of governance.

    ⚠ ``accessed_by`` and ``purpose`` are required on BOTH paths even though one
    discards them. The caller is a gate assembler that does not branch on the
    namespace, and an optional audit field is one a caller learns it needed at
    the moment it cannot supply one.
    """
    sibling = replace(identity, quarantine_arm=("admitted" if identity.quarantine_arm == "masked" else "masked"))
    if identity.namespace == "hold_out":
        record_holdout_access(
            conn,
            HoldoutAccess(
                strategy_id=identity.strategy_id,
                strategy_version=identity.strategy_version,
                result_version=None,
                access_kind="read",
                accessed_by=accessed_by,
                purpose=purpose,
            ),
        )
    row = conn.execute(
        _COUNT_ARM_PAIR,
        {
            "strategy_id": identity.strategy_id,
            "strategy_version": identity.strategy_version,
            "result_versions": [identity.version, sibling.version],
        },
    ).fetchone()
    if row is None:  # pragma: no cover - count() always returns a row
        raise RuntimeError("arm-pair count query returned no row")
    return int(row[0]) == 2


# ---------------------------------------------------------------------------
# The walk-forward split (stage 5e-5c)
# ---------------------------------------------------------------------------


def store_walk_forward_folds(
    conn: psycopg.Connection[tuple],
    result_id: int,
    split: WalkForwardFolds,
) -> int:
    """Store one result's whole walk-forward split. Returns the rows written.

    ⚠⚠ THE WHOLE SPLIT OR NOTHING. ``WalkForwardFolds`` refuses a partial or
    discontiguous set before this function is reached, and every row goes in one
    ``executemany`` inside a transaction THIS FUNCTION OWNS — so a stored split
    is always ``FOLD_COUNT`` contiguous folds measured over one population. A
    per-fold writer would make "three of four folds stored" representable, and
    it would read as a completed cross-validation.

    ⚠⚠ THE ``conn.transaction()`` HERE IS DEFENCE IN DEPTH AND IS NOT WHAT MAKES
    THE BATCH ATOMIC — MEASURED, NOT ASSUMED. Codex raised at checkpoint 2 that
    an autocommit caller would commit each statement and leave the earlier folds
    standing after a later one was refused. Measured on psycopg **3.3.3**
    (2026-08-08): an autocommit connection, a temp table with a primary key, and
    an ``executemany`` whose THIRD statement violates it — the two rows before it
    do **not** survive. `executemany` runs the batch in its own transaction. So
    the wrapper is kept for the guarantee to be explicit and independent of a
    driver implementation detail, and it is stated as belt-and-braces rather
    than sold as the mechanism. ⚠ Its absence is therefore not observable by any
    test, which is why ``scripts/probe_2240_result_ledger.py`` carries no probe
    for it and says so.

    ⚠ The rowcount checks below are inside it regardless, so a short batch
    unwinds rather than raising over rows that are already committed.

    ⚠ THE MODEL ID MUST BE TODAY'S. A write happens under the construction this
    module currently implements, so stamping an older one would label rows with
    a split that did not produce them. The asymmetry with ``read_walk_forward_folds``
    — which returns whatever was stored — is the correct direction: old rows
    keep their own construction, and new rows cannot claim one.

    ⚠ THERE IS NO PYTHON NAMESPACE CHECK HERE, unlike the two single-row
    writers, and the reason is the signature: this function holds a
    ``result_id`` and not a ``StrategyResult``, so it would have to QUERY to
    learn the parent's namespace. ``sql/269``'s trigger refuses a fold row on a
    ``hold_out`` result for every writer including this one, and
    ``tests/test_strategy_result_folds.py`` exercises it on INSERT and on UPDATE.
    """
    if split.model_id != WALK_FORWARD_MODEL_ID:
        raise ValueError(
            f"split declares model {split.model_id!r} but this module implements {WALK_FORWARD_MODEL_ID!r} — a stored "
            "split labelled with a construction that did not produce it is unauditable"
        )
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            _INSERT_FOLD,
            [
                {
                    "result_id": result_id,
                    "fold_index": record.fold.index,
                    "walk_forward_model_id": split.model_id,
                    "fold_count": len(split.folds),
                    "first_index": record.fold.first_index,
                    "last_index": record.fold.last_index,
                    "first_date": record.first_date,
                    "last_date": record.last_date,
                    "bar_count": record.bar_count,
                    "embargo_bars": record.embargo_bars,
                    "test_count": record.census.test,
                    "train_count": record.census.train,
                    "purged_count": record.census.purged,
                    "embargoed_count": record.census.embargoed,
                }
                for record in split.folds
            ],
        )
        # psycopg3's ``executemany`` rowcount is cumulative across the batch.
        # ⚠ -1 is psycopg's "the server reported nothing" sentinel and must not
        # be returned as a count (prevention log: "psycopg v3 rowcount sentinel
        # (-1) treated as valid count"). ⚠ Read rather than assumed from
        # ``len(split.folds)``: the two agree only if every statement in the
        # batch landed, and returning the input's length would report a
        # complete split for whatever the database actually took.
        written = cur.rowcount
        if written < 0:
            raise RuntimeError(f"strategy_result_folds INSERT reported rowcount {written} for {len(split.folds)} folds")
        if written != len(split.folds):
            raise ValueError(
                f"strategy_result_folds INSERT wrote {written} of {len(split.folds)} folds — a partial split is a "
                "cross-validation that did not finish"
            )
    return written


def read_walk_forward_folds(conn: psycopg.Connection[tuple], result_id: int) -> WalkForwardFolds | None:
    """One result's split, or ``None`` when it has none.

    ⚠ ``None`` and not an empty ``WalkForwardFolds``: the type cannot express an
    empty split (see its own header), and "this result carries no walk-forward
    evidence" is a real state that a caller must handle rather than a degenerate
    collection it can iterate over zero times.

    ⚠ Positional read in ``_FOLD_COLUMNS`` order, matching ``_result_from_row``'s
    reason: a dict read would tolerate the statement and the unpacking drifting
    apart, and the round-trip test is what actually pins them.
    """
    rows = conn.execute(_SELECT_FOLDS, {"result_id": result_id}).fetchall()
    if not rows:
        return None
    model_ids = {str(row[1]) for row in rows}
    if len(model_ids) > 1:
        raise ValueError(
            f"result {result_id} carries folds from {sorted(model_ids)} — one split is one construction, and a mixed "
            "set is two runs whose rows landed on one result"
        )
    return WalkForwardFolds(
        model_id=model_ids.pop(),
        folds=tuple(_fold_from_row(row) for row in rows),
    )


def _fold_from_row(row: Sequence[object]) -> FoldRecord:
    """One ``sql/269`` row, in ``_FOLD_COLUMNS`` order."""
    (
        fold_index,
        _walk_forward_model_id,
        _fold_count,
        first_index,
        last_index,
        first_date,
        last_date,
        bar_count,
        embargo_bars,
        test_count,
        train_count,
        purged_count,
        embargoed_count,
    ) = row
    return FoldRecord(
        fold=Fold(
            index=int(fold_index),  # type: ignore[arg-type]
            first_index=int(first_index),  # type: ignore[arg-type]
            last_index=int(last_index),  # type: ignore[arg-type]
        ),
        first_date=first_date,  # type: ignore[arg-type]
        last_date=last_date,  # type: ignore[arg-type]
        bar_count=int(bar_count),  # type: ignore[arg-type]
        embargo_bars=int(embargo_bars),  # type: ignore[arg-type]
        census=FoldCensus(
            test=int(test_count),  # type: ignore[arg-type]
            train=int(train_count),  # type: ignore[arg-type]
            purged=int(purged_count),  # type: ignore[arg-type]
            embargoed=int(embargoed_count),  # type: ignore[arg-type]
        ),
    )


__all__ = [
    "HOLDOUT_ACCESS_KINDS",
    "FrozenPreregistration",
    "HoldoutAccess",
    "HoldoutAccessCounts",
    "HoldoutAccessKind",
    "PreregDeclarationRefused",
    "freeze_preregistration",
    "holdout_access_counts",
    "load_preregistration",
    "quarantine_arms_compared",
    "read_holdout_results",
    "read_walk_forward_folds",
    "record_holdout_access",
    "require_outcome_access",
    "store_holdout_arm_pair",
    "store_holdout_result",
    "store_in_sample_arm_pair",
    "store_in_sample_result",
    "store_walk_forward_folds",
    "verify_outcome_access_provenance",
]
