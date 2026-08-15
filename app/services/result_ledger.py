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

import logging
import math
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Final, Literal, NoReturn, cast, get_args

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from app.services.cost_model import CARRY_UNMODELLED, COST_MODEL_ID, FX_UNMODELLED
from app.services.deflated_sharpe import DeflatedSharpeResult
from app.services.prereg_contract import (
    ForwardShadowFloor,
    PreregDeclaration,
    PreregPurpose,
    Supersession,
    changed_supersession_terms,
    declaration_refusals,
    supersession_refusals,
)
from app.services.random_entry_cohort import SyntheticControl
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_result import (
    PromotionRefusal,
    ResultIdentity,
    StrategyResult,
    deflation_promotion_refusals,
    metric_axis_promotion_refusals,
    purpose_promotion_refusals,
    synthetic_control_promotion_refusals,
)
from app.services.strategy_statistics import StrategyMetrics, periods_per_year
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

_LOG = logging.getLogger(__name__)


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

    ⚠ #2634 — THIS IS THE *CURRENT* REVISION OF A CHAIN, not the only row. A
    declaration stranded by a structural-refusal policy bump is repaired by a
    superseding row rather than an edit, so a trial may hold several, of which
    exactly one is current. ``chain_declaration_ids`` is every revision oldest
    first, and it exists because ``assess_live_gate`` has to recognise a policy
    that binds an EARLIER revision — every row in a chain carries identical
    terms, so honouring an ancestor cannot loosen anything.
    """

    declaration_id: int
    declaration: PreregDeclaration
    declaration_sha256: str
    #: Every revision for this trial, oldest first, ending at ``declaration_id``.
    #: A trial with no supersession has exactly one entry.
    chain_declaration_ids: tuple[int, ...] = ()
    supersedes_declaration_id: int | None = None
    supersession_reason: str | None = None
    supersession_attestation: str | None = None

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
class RefusedAccess:
    """#2611 — one refused outcome-access attempt, as ``sql/340`` stored it.

    ⚠ NOT AN ACCESS RECORD, and the type is separate from ``HoldoutAccess`` so
    the two cannot be mixed up at a call site. Nothing was returned to the
    caller, so a row of these is neither a criterion-5 evaluation nor exposure
    for #2634's supersession check — see ``sql/340``'s header for why that
    distinction is load-bearing rather than tidy.
    """

    refusal_id: int
    strategy_id: str
    strategy_version: str
    result_version: str | None
    access_kind: HoldoutAccessKind
    accessed_by: str
    purpose: str
    refusals: tuple[str, ...]
    declaration_id: int | None
    refused_at: datetime


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
        strategy_id, strategy_version, result_version, access_kind, accessed_by, purpose, declaration_id
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(result_version)s, %(access_kind)s, %(accessed_by)s, %(purpose)s,
        %(declaration_id)s
    )
    RETURNING access_id
"""

#: #2611 — the refused-attempt audit row. ⚠ NAMES A DIFFERENT RELATION FROM
#: ``_RECORD_ACCESS`` ON PURPOSE, and sql/340's header carries the two reasons:
#: ``holdout_access_counts`` feeds criterion 5 off the access log, and
#: ``supersede_preregistration`` reads it as exposure. Nothing was returned by a
#: refused look, so it is neither.
_RECORD_ACCESS_REFUSAL = """
    INSERT INTO strategy_holdout_access_refusals (
        strategy_id, strategy_version, result_version, access_kind, accessed_by, purpose,
        refusals, declaration_id
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(result_version)s, %(access_kind)s, %(accessed_by)s, %(purpose)s,
        %(refusals)s, %(declaration_id)s
    )
"""

#: The governance read. ⚠ Most recent first and BOUNDED — an audit table nothing
#: prunes is exactly the shape whose read grows without anybody noticing.
_SELECT_ACCESS_REFUSALS = """
    SELECT refusal_id, strategy_id, strategy_version, result_version, access_kind,
           accessed_by, purpose, refusals, declaration_id, refused_at
    FROM strategy_holdout_access_refusals
    WHERE strategy_id = %(strategy_id)s AND strategy_version = %(strategy_version)s
    ORDER BY refusal_id DESC
    LIMIT %(limit)s
"""

#: #2634 — the per-trial mutex freeze, access and supersession all take.
#:
#: ⚠⚠ AN ADVISORY LOCK AND NOT ``FOR SHARE``/``FOR UPDATE``, for three measured
#: reasons. Row locks are barred inside a ``READ ONLY`` transaction, which is
#: where ``verify_outcome_access_provenance`` runs; they lock ZERO rows for a
#: trial that has not frozen anything yet, so they cannot order an access
#: against a concurrent FIRST freeze; and "every row of the trial" has no
#: deterministic lock order across plans, which is a deadlock shape. One lock on
#: the trial identity has none of those. A hash collision merely over-serialises
#: two unrelated trials, which costs nothing here.
#:
#: Pattern: ``app/api/strategies.py:1384``.
_LOCK_TRIAL = "SELECT pg_advisory_xact_lock(hashtext(%(trial)s))"

#: The exposure disqualifiers, counted together so a refusal names both.
#:
#: ⚠ TWO COUNTS, NOT ONE. The access ledger and the result rows can disagree: a
#: ``hold_out`` row written before #2599's chokepoint existed, or by a path that
#: bypassed it, is exposure with no access row to count.
_COUNT_TRIAL_EXPOSURE = """
    SELECT
        (SELECT count(*) FROM strategy_holdout_accesses
          WHERE strategy_id = %(strategy_id)s AND strategy_version = %(strategy_version)s) AS access_count,
        (SELECT count(*) FROM strategy_results_store
          WHERE strategy_id = %(strategy_id)s AND strategy_version = %(strategy_version)s
            AND namespace = 'hold_out') AS holdout_result_count
"""

_FREEZE_DECLARATION = """
    INSERT INTO strategy_preregistration_declarations (
        strategy_id, strategy_version, contract_version, prereg_purpose,
        structural_refusal_policy_version, declared_universe_basis, declared_carry_unmodelled,
        declared_fx_unmodelled,
        expected_structural_refusals, min_forward_decision_dates, min_forward_calendar_weeks,
        forward_shadow_derivation, declared_by, declaration_sha256,
        supersedes_declaration_id, supersession_reason, supersession_attestation
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(contract_version)s, %(prereg_purpose)s,
        %(structural_refusal_policy_version)s, %(declared_universe_basis)s, %(declared_carry_unmodelled)s,
        %(declared_fx_unmodelled)s,
        %(expected_structural_refusals)s, %(min_forward_decision_dates)s, %(min_forward_calendar_weeks)s,
        %(forward_shadow_derivation)s, %(declared_by)s, %(declaration_sha256)s,
        %(supersedes_declaration_id)s, %(supersession_reason)s, %(supersession_attestation)s
    )
    RETURNING declaration_id
"""

#: ⚠ EVERY REVISION FOR THE TRIAL, oldest first — not one row. #2634 made a
#: trial's declarations a chain, and ``sql/337``'s constraints (one root, no
#: branching, no cycles) mean the rows come back as a single acyclic list whose
#: last entry is the current declaration. Ordering by ``declaration_id`` is the
#: chain order because the same CHECK that bars cycles requires every edge to
#: point at a smaller id.
_SELECT_DECLARATION = """
    SELECT declaration_id, strategy_id, strategy_version, contract_version, prereg_purpose,
           structural_refusal_policy_version, declared_universe_basis, declared_carry_unmodelled,
           declared_fx_unmodelled,
           expected_structural_refusals, min_forward_decision_dates, min_forward_calendar_weeks,
           forward_shadow_derivation, declared_by, declaration_sha256,
           supersedes_declaration_id, supersession_reason, supersession_attestation
    FROM strategy_preregistration_declarations
    WHERE strategy_id = %(strategy_id)s AND strategy_version = %(strategy_version)s
    ORDER BY declaration_id
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
      ON d.declaration_id = a.declaration_id
    WHERE a.access_id = %(access_id)s AND d.declaration_id = %(declaration_id)s
"""

_RESULT_COLUMNS = """
    strategy_id, strategy_version, result_version, result_scope, namespace,
    ambiguity_arm, quarantine_arm, window_start, window_end, purpose, universe_basis, corpus_version,
    cost_model_id, carry_unmodelled, fx_unmodelled, sizing_rule, benchmark_rule, return_basis,
    ambiguity_rule_version,
    metric_axis_rule_version, metric_axis_dates, metric_axis_start, metric_axis_end,
    metric_axis_digest, opportunity_set_digest, evidence_window_id,
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
    trial_register_version, median_hold_days, hold_days_p25, hold_days_p75,
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
    %(return_basis)s, %(ambiguity_rule_version)s,
    %(metric_axis_rule_version)s, %(metric_axis_dates)s, %(metric_axis_start)s, %(metric_axis_end)s,
    %(metric_axis_digest)s, %(opportunity_set_digest)s, %(evidence_window_id)s,
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
    %(median_hold_days)s, %(hold_days_p25)s, %(hold_days_p75)s,
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

#: ⚠⚠ ONE STATEMENT, TWO SCALAR SUBQUERIES, AND THAT IS THE POINT (#2639). The
#: two counts used to run as separate statements, which under READ COMMITTED is
#: two snapshots — so a ``store_holdout_result`` committing between them could
#: return a pair that never simultaneously existed. The direction that matters
#: is ``accesses < evaluations``, a false ``holdout_accesses_unrecorded``, and
#: it fires exactly when a hold-out row lands between the two reads.
#:
#: ⚠ This buys ONE snapshot for the pair, not atomicity with whatever the caller
#: does next. ``promote_strategy`` decides on counts that were true when read;
#: the hold-out writers do not take its advisory lock, so a write may still
#: commit between the count and the promotion INSERT. Named as a bound on #2639
#: rather than assumed away.
_COUNT_HOLDOUT_EVALUATIONS_AND_ACCESSES = """
    SELECT
        (SELECT count(*)
           FROM strategy_results_store
          WHERE strategy_id = %(strategy_id)s
            AND strategy_version = %(strategy_version)s
            AND namespace = 'hold_out'),
        (SELECT count(*)
           FROM strategy_holdout_accesses
          WHERE strategy_id = %(strategy_id)s
            AND strategy_version = %(strategy_version)s
            AND access_kind = 'evaluate')
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

#: The batch form of the above (#2641) — which versions exist, rather than how
#: many rows. See ``_arm_pairs_present`` for why the two agree.
_SELECT_ARM_VERSIONS = """
    SELECT result_version
    FROM strategy_results_store
    WHERE strategy_id = %(strategy_id)s
      AND strategy_version = %(strategy_version)s
      AND result_version = ANY(%(result_versions)s::text[])
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
    _assert_axis_metric_reconciliation(identity, metrics)
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
        "ambiguity_rule_version": identity.ambiguity_rule_version,
        "metric_axis_rule_version": identity.metric_axis_rule_version,
        "metric_axis_dates": None if identity.metric_axis_dates is None else list(identity.metric_axis_dates),
        "metric_axis_start": identity.metric_axis_start,
        "metric_axis_end": identity.metric_axis_end,
        "metric_axis_digest": identity.metric_axis_digest,
        "opportunity_set_digest": identity.opportunity_set_digest,
        "evidence_window_id": identity.evidence_window_id,
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
        # #2623 gap 1. NULL-preserving: these are legitimately absent on a result
        # with no realised trades, and `sql/347`'s CHECK ties that to metric_set_id.
        "median_hold_days": _numeric(metrics.median_hold_days),
        "hold_days_p25": _numeric(metrics.hold_days_p25),
        "hold_days_p75": _numeric(metrics.hold_days_p75),
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


def _assert_axis_metric_reconciliation(identity: ResultIdentity, metrics: StrategyMetrics) -> None:
    """Recompute the annualisation facts from the exact stored tuple."""
    if identity.metric_axis_dates is None:
        return
    expected_ppy = periods_per_year(identity.metric_axis_dates)
    if not math.isclose(metrics.periods_per_year, expected_ppy, rel_tol=1e-12, abs_tol=1e-12):
        raise ValueError(
            f"periods_per_year {metrics.periods_per_year} does not reconcile with metric axis ({expected_ppy})"
        )
    years = (len(identity.metric_axis_dates) - 1) / expected_ppy
    final_multiple = 1.0 + metrics.total_return_pct / 100.0
    expected_cagr = -100.0 if final_multiple == 0.0 else (final_multiple ** (1.0 / years) - 1.0) * 100.0
    if not math.isclose(metrics.cagr_pct, expected_cagr, rel_tol=1e-10, abs_tol=1e-10):
        raise ValueError(f"cagr_pct {metrics.cagr_pct} does not reconcile with total return and metric axis")


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
        ambiguity_rule_version,
        metric_axis_rule_version,
        metric_axis_dates,
        metric_axis_start,
        metric_axis_end,
        metric_axis_digest,
        opportunity_set_digest,
        evidence_window_id,
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
        # ⚠ POSITION MATTERS: this tuple is unpacked from `_RESULT_COLUMNS`, so
        # the order here must match that constant exactly, not read naturally.
        median_hold_days,
        hold_days_p25,
        hold_days_p75,
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
        ambiguity_rule_version=str(ambiguity_rule_version),
        cost_model_id=str(cost_model_id),
        corpus_version=str(corpus_version),
        window_start=window_start,  # type: ignore[arg-type]
        window_end=window_end,  # type: ignore[arg-type]
        position_rule_set_version=str(position_rule_set_version),
        outcome_rule_set_version=str(outcome_rule_set_version),
        input_rule_set_version=str(input_rule_set_version),
        metric_axis_rule_version=None if metric_axis_rule_version is None else str(metric_axis_rule_version),
        metric_axis_dates=None if metric_axis_dates is None else tuple(metric_axis_dates),  # type: ignore[arg-type]
        metric_axis_start=metric_axis_start,  # type: ignore[arg-type]
        metric_axis_end=metric_axis_end,  # type: ignore[arg-type]
        metric_axis_digest=None if metric_axis_digest is None else str(metric_axis_digest),
        opportunity_set_digest=None if opportunity_set_digest is None else str(opportunity_set_digest),
        evidence_window_id=None if evidence_window_id is None else str(evidence_window_id),
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
        # #2623 gap 1 — NULL-preserving for the same reason the bootstrap block
        # above is: a legacy `criterion7-v1` row genuinely has no holding period,
        # and `float(None)` would raise while `0.0` would invent a same-day close.
        median_hold_days=_as_float(median_hold_days),  # type: ignore[arg-type]
        hold_days_p25=_as_float(hold_days_p25),  # type: ignore[arg-type]
        hold_days_p75=_as_float(hold_days_p75),  # type: ignore[arg-type]
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
    _assert_axis_metric_reconciliation(identity, metrics)
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


def _audit_conninfo(conn: psycopg.Connection[tuple]) -> str:
    """The connection string for #2611's audit write, derived from the caller's.

    ⚠⚠ DERIVED FROM ``conn``, NEVER FROM ``settings.database_url``. The refusal
    belongs in the same database as the transaction that was refused — which for
    a DB test is the 5433 test cluster. Reading the settings URL would write a
    test's refusal into the operator's dev DB and trip ``tests/conftest.py``'s
    dev-DB tripwire, and in an environment with more than one database it would
    silently audit the wrong one.

    ⚠ ``conn.info.dsn`` STRIPS THE PASSWORD (measured, psycopg 3.3.3: it returns
    ``user=… connect_timeout=10 dbname=… host=…`` and no ``password=``), so it
    is re-attached from ``conn.info.password`` — and only when there is one, so a
    trust / passfile / service-file deployment still connects.

    ⚠ The connect phase is already bounded everywhere by ``PGCONNECT_TIMEOUT``
    (``app/config.py``), which is why the derived DSN carries
    ``connect_timeout=10``. The two server-side timeouts below bound the rest:
    this write sits on an EXCEPTION path, and an audit that hangs would convert
    a refusal into a stall. ``lock_timeout`` specifically covers a concurrent
    ``TRUNCATE`` of this table (the DB-test cleanup planner takes ACCESS
    EXCLUSIVE on it).

    ⚠ ``options`` IS APPENDED TO THE CALLER'S, NOT SUBSTITUTED FOR IT.
    ``make_conninfo`` merges per KEYWORD, not inside a keyword's value, so
    passing ``options=`` replaces the caller's whole string — measured (psycopg
    3.3.3): ``make_conninfo("… options='-c application_name=caller -c
    lock_timeout=99s'", options="-c lock_timeout=2s -c statement_timeout=5s")``
    renders only the second, dropping ``application_name``. libpq forwards
    ``options`` as server command-line arguments where a repeated ``-c`` is
    LAST-WINS (measured against the dev cluster: caller's ``lock_timeout=99s``
    then ours ``2s`` → ``SHOW lock_timeout`` = ``2s``, and ``application_name``
    survives as ``caller``), so appending keeps the caller's settings AND keeps
    these two timeouts binding. Prepending would let a caller disable the guard.
    """
    info = conn.info
    audit_options = "-c lock_timeout=2s -c statement_timeout=5s"
    caller_options = conninfo_to_dict(info.dsn).get("options")
    if caller_options:
        audit_options = f"{caller_options} {audit_options}"
    extras: dict[str, str | int | None] = {"options": audit_options}
    if info.password:
        extras["password"] = info.password
    return make_conninfo(info.dsn, **extras)


def _record_access_refusal(
    conn: psycopg.Connection[tuple],
    access: HoldoutAccess,
    refusals: tuple[str, ...],
    declaration_id: int | None,
) -> None:
    """Write one ``sql/340`` row on its OWN connection. Best effort.

    ⚠⚠ A SECOND CONNECTION, AND THE ARGUMENT IS AN ASYMMETRY #2599's docstring
    did not draw. ``record_holdout_access`` writes in the caller's transaction
    deliberately: an access record is a claim about DATA, ``sql/264``'s trigger
    must see it alongside the row it authorises, and a rolled-back evaluation did
    not happen. A refusal record is a claim about an ACT OF THE CALLER — it
    completes when ``PreregDeclarationRefused`` is constructed, the caller
    rolling back does not un-attempt it, and a caller that retries N times really
    did attempt N times. Postgres has no autonomous transaction, and in the
    caller's transaction this row would be lost in every case it exists for,
    because the refusal is an exception.

    ⚠⚠ NO ADVISORY LOCK HERE, AND SQL/340 CARRIES NO FK. Measured 2026-08-13:
    ``pg_advisory_xact_lock`` blocks across connections. ``record_holdout_access``
    takes the trial lock and still holds it when it refuses, so an audit write
    that took the same lock would block until the caller's transaction ended.

    ⚠⚠ BEST EFFORT, AND IT NEVER MASKS THE REFUSAL. The caller raises regardless.
    A gate that can be disabled by breaking its audit is not a gate — so a
    failure here is logged at ERROR **with the refusal codes inline**, which is
    what stops a failed write being a silent no-op (the "reports success and
    writes nothing" shape this repo has been bitten by).
    """
    try:
        with psycopg.connect(_audit_conninfo(conn), autocommit=True) as audit:
            audit.execute(
                _RECORD_ACCESS_REFUSAL,
                {
                    "strategy_id": access.strategy_id,
                    "strategy_version": access.strategy_version,
                    "result_version": access.result_version,
                    "access_kind": access.access_kind,
                    "accessed_by": access.accessed_by,
                    "purpose": access.purpose,
                    "refusals": list(refusals),
                    "declaration_id": declaration_id,
                },
            )
    except Exception:
        _LOG.exception(
            "#2611 audit write failed; the refusal itself still stands. "
            "trial=%s/%s kind=%s by=%s purpose=%r refusals=%s declaration_id=%s",
            access.strategy_id,
            access.strategy_version,
            access.access_kind,
            access.accessed_by,
            access.purpose,
            ",".join(refusals),
            declaration_id,
        )


def _refuse_access(
    conn: psycopg.Connection[tuple],
    access: HoldoutAccess,
    refusals: tuple[str, ...],
    declaration_id: int | None = None,
) -> NoReturn:
    """#2611's single exit from a refused outcome-access attempt. Never returns.

    ⚠ THE ONE PLACE THE ACCESS PATH MAY CONSTRUCT ``PreregDeclarationRefused``.
    A convention would rot the way every other one in M9 has ("the control
    exists and sits on a path the decision does not take"), so
    ``tests/test_2611_refused_access_audit.py`` walks this module's AST and fails
    if any access-path function raises it directly. AST rather than a substring
    grep, which an import line satisfies (#2631).
    """
    _record_access_refusal(conn, access, refusals, declaration_id)
    raise PreregDeclarationRefused(access.strategy_id, access.strategy_version, refusals)


def _declaration_refusal_codes(
    conn: psycopg.Connection[tuple], strategy_id: str, strategy_version: str
) -> tuple[FrozenPreregistration | None, tuple[str, ...]]:
    """The trial's current declaration and every reason it may not authorise a look.

    ⚠ SPLIT OUT BY #2611 SO THE TWO CALLERS CAN REFUSE DIFFERENTLY. The access
    door audits its refusal (``_refuse_access``); ``verify_outcome_access_provenance``
    does not, because it is a RE-CHECK — it requires an ``access_id`` that a
    committed access row already accounts for, so auditing there would record a
    second attempt for one look. Returning the codes rather than raising is what
    lets each caller make that choice explicitly instead of inheriting one.

    ⚠ Empty codes with a non-None declaration means coherent. Empty codes with
    ``None`` means the trial froze nothing — NOT a refusal here (#2599 does not
    retroactively invalidate), and the callers that require one say so
    themselves.
    """
    frozen = load_preregistration(conn, strategy_id, strategy_version)
    if frozen is None:
        return None, ()
    codes = [str(code) for code in declaration_refusals(frozen.declaration)]
    if not frozen.digest_intact:
        codes.append("declaration_digest_mismatch")
    return frozen, tuple(codes)


def _refuse_incoherent_declaration(
    conn: psycopg.Connection[tuple], access: HoldoutAccess
) -> FrozenPreregistration | None:
    """Re-check a frozen declaration, or pass when the trial froze none.

    ⚠ RE-CHECKED ON EVERY LOOK, not only at freeze time. A declaration frozen
    under a structural-refusal policy that has since been superseded stops
    authorising looks the moment the policy moves — which is the point of
    versioning it, and the same shape ``trial_register_superseded`` already has.

    ⚠ The stored digest is verified too. A row edited around the immutability
    trigger (a superuser can disable one) no longer matches the bytes it was
    frozen over, and a declaration that has been rewritten is not a declaration.

    ⚠ #2611 — TAKES THE WHOLE ``HoldoutAccess`` RATHER THAN THE IDENTITY PAIR.
    The audit row records who attempted what and why, and none of that is
    derivable from ``(strategy_id, strategy_version)``. The refusal exits through
    ``_refuse_access``.
    """
    frozen, refusals = _declaration_refusal_codes(conn, access.strategy_id, access.strategy_version)
    if refusals:
        _refuse_access(conn, access, refusals, None if frozen is None else frozen.declaration_id)
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

    ⚠ #2634 — the row is no longer the trial's ONLY declaration; it is the ROOT
    of a chain a later supersession may extend. What stays true is that a trial
    has exactly one root (``sql/337``'s partial unique index), so a second call
    here still raises ``UniqueViolation``.
    """
    refusals = declaration_refusals(declaration)
    if refusals:
        raise PreregDeclarationRefused(
            declaration.strategy_id, declaration.strategy_version, tuple(str(code) for code in refusals)
        )
    # ⚠ FREEZE-TIME ONLY, deliberately not in ``PreregDeclaration.__post_init__``:
    # that class is also the read-back of stored rows, and rows frozen under an
    # earlier cost model legitimately declare stamps today's constants do not
    # produce. A NEW freeze of a MANIFEST strategy has no such licence — its
    # runs go through ``backtest_run``, which stamps the ``cost_model`` module
    # constants on every row, so a declaration that cannot match burns an
    # immutable trial (sql/333 bars UPDATE and DELETE; sql/337 chains cost a
    # supersession). Added by #2720, whose closure flipped both constants to
    # False.
    #
    # ⚠ SCOPED TO THE MANIFEST on purpose: a bespoke contract trial (the #2582
    # schedule-13D catalyst charges its own flat 50 bps and models no carry)
    # OWNS its stamps — declaring ``True`` there is the honest state, not a
    # stale copy of this module's, and refusing it would force a false
    # "modelled" claim onto a run that charges no carry.
    if declaration.strategy_id in STRATEGY_MANIFEST:
        current = (CARRY_UNMODELLED, FX_UNMODELLED)
        declared = (declaration.declared_carry_unmodelled, declaration.declared_fx_unmodelled)
        if declared != current:
            raise ValueError(
                f"declaration for {declaration.strategy_id}@{declaration.strategy_version} declares "
                f"(carry_unmodelled, fx_unmodelled) = {declared}, but the current cost model "
                f"({COST_MODEL_ID}) stamps {current} on every manifest-strategy row — freezing it would "
                "burn an immutable trial on stamps its run cannot produce. Re-derive the declaration "
                "from the current cost_model constants."
            )
    _lock_trial(conn, declaration.strategy_id, declaration.strategy_version)
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
            # A root declaration supersedes nothing. `sql/337` CHECKs that the
            # three move together, so a half-filled root is not a state.
            "supersedes_declaration_id": None,
            "supersession_reason": None,
            "supersession_attestation": None,
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
    did before #2599 — no retroactive invalidation, which is what keeps the 304
    existing access rows and every current evaluator working. The paths that
    REQUIRE a declaration say so themselves (``require_outcome_access``).

    ⚠⚠ #2634 — RETURNS THE CURRENT REVISION OF THE CHAIN. Before supersession
    existed this read one row and `.fetchone()` was the whole answer; with
    several rows possible per trial, an arbitrary one would be a coin flip
    between a stranded declaration and its repair. The current revision is the
    row nothing supersedes, which ``sql/337``'s constraints make unique — and
    ``_current_of_chain`` asserts that rather than trusting it, because a
    disagreement here means the constraints are not doing what this function
    claims they do.
    """
    rows = conn.execute(
        _SELECT_DECLARATION,
        {"strategy_id": strategy_id, "strategy_version": strategy_version},
    ).fetchall()
    if not rows:
        return None
    row = _current_of_chain(rows, strategy_id, strategy_version)
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
        chain_declaration_ids=tuple(int(candidate[0]) for candidate in rows),
        supersedes_declaration_id=None if row[15] is None else int(row[15]),
        supersession_reason=None if row[16] is None else str(row[16]),
        supersession_attestation=None if row[17] is None else str(row[17]),
    )


def _current_of_chain(
    rows: Sequence[tuple], strategy_id: str, strategy_version: str
) -> tuple:  # pragma: no mutate - shape is asserted below
    """The one revision no other supersedes.

    ⚠ ASSERTED, NOT ASSUMED. ``sql/337`` gives exactly one current row per trial
    — one root, no branching, and no cycles because every edge must point at a
    smaller ``declaration_id``. If this ever finds zero or two, the constraints
    are not holding and the honest response is to refuse rather than to pick
    one: picking would silently answer an outcome look from a revision nobody
    can name.
    """
    superseded = {int(row[15]) for row in rows if row[15] is not None}
    current = [row for row in rows if int(row[0]) not in superseded]
    if len(current) != 1:
        raise RuntimeError(
            f"preregistration chain for {strategy_id}/{strategy_version} has {len(current)} current declarations, "
            f"not 1 — sql/337's one-root/no-branch/no-cycle constraints are not holding"
        )
    return current[0]


def _lock_trial(conn: psycopg.Connection[tuple], strategy_id: str, strategy_version: str) -> None:
    """Serialise freeze, access and supersession for one trial. See ``_LOCK_TRIAL``."""
    conn.execute(_LOCK_TRIAL, {"trial": f"{strategy_id}/{strategy_version}"})


def supersede_preregistration(
    conn: psycopg.Connection[tuple],
    successor: PreregDeclaration,
    supersession: Supersession,
) -> int:
    """#2634 — repair a declaration stranded by a policy bump. Returns the new id.

    ⚠⚠ WHAT MAKES THIS SAFE IS THAT IT CAN EXPRESS ALMOST NOTHING. A
    re-declaration path is an obvious adaptivity vector: an author who has seen
    sample counts, missingness or corpus composition re-declares more
    favourably. ``supersession_refusals`` permits the successor to differ from
    its predecessor in the policy version, the refusal list recomputed from it,
    and the declarer's name — and in nothing else. Purpose, both cost stamps,
    the universe basis and both forward-shadow floors are compared field by
    field. A trial that wants different terms is a different trial, and the new
    ``strategy_version`` remains the identity boundary it always was.

    ⚠⚠ "NO ACCESS ROWS" IS NECESSARY AND NOT SUFFICIENT, AND THIS FUNCTION DOES
    NOT PRETEND OTHERWISE. ``strategy_holdout_accesses`` records committed
    paved-path looks. A direct ``SELECT`` against ``strategy_results_store``
    leaves no row (``sql/264``'s header measured that RLS does not bind this
    app's superuser connection), a rolled-back transaction removes its own
    record, and outcomes may already sit in a signed artifact, an export, a log
    or another database. The counts are a cheap automatic disqualifier; the
    ``Supersession.attestation`` is what carries the rest, and an attestation is
    a claim rather than a proof. It is frozen with the row, where ``sql/333``'s
    immutability trigger makes it unrewritable.

    ⚠ REQUIRES ``READ COMMITTED``. The concurrency argument is that a caller
    which loses the advisory-lock race re-reads and finds the winner's row —
    which needs a fresh snapshot per statement. Under ``REPEATABLE READ`` the
    post-lock read returns the pre-lock snapshot and the check silently
    regresses to the race it exists to close, so the isolation level is checked
    rather than assumed.
    """
    isolation = conn.execute("SELECT current_setting('transaction_isolation')").fetchone()
    if isolation is not None and str(isolation[0]).lower() != "read committed":
        raise RuntimeError(
            f"supersede_preregistration needs READ COMMITTED and this transaction is {isolation[0]!r}: the "
            "post-lock re-read would return the pre-lock snapshot, so a losing racer would not see the "
            "supersession that beat it"
        )

    _lock_trial(conn, successor.strategy_id, successor.strategy_version)

    frozen = load_preregistration(conn, successor.strategy_id, successor.strategy_version)
    if frozen is None:
        raise PreregDeclarationRefused(
            successor.strategy_id, successor.strategy_version, ("supersession_nothing_frozen",)
        )

    refusals = [str(code) for code in supersession_refusals(frozen.declaration, successor)]
    changed = changed_supersession_terms(frozen.declaration, successor)

    exposure = conn.execute(
        _COUNT_TRIAL_EXPOSURE,
        {"strategy_id": successor.strategy_id, "strategy_version": successor.strategy_version},
    ).fetchone()
    access_count = 0 if exposure is None else int(exposure[0])
    holdout_result_count = 0 if exposure is None else int(exposure[1])
    if access_count:
        refusals.append("supersession_trial_already_exposed")
    if holdout_result_count:
        refusals.append("supersession_trial_has_holdout_results")

    if refusals:
        detail = tuple(refusals)
        if changed:
            # The code vocabulary is closed, so the field names travel in the
            # message — five more codes would all mean the same operator action.
            detail = (*detail, f"changed_terms={','.join(changed)}")
        raise PreregDeclarationRefused(successor.strategy_id, successor.strategy_version, detail)

    try:
        row = conn.execute(
            _FREEZE_DECLARATION,
            {
                "strategy_id": successor.strategy_id,
                "strategy_version": successor.strategy_version,
                "contract_version": successor.contract_version,
                "prereg_purpose": successor.prereg_purpose,
                "structural_refusal_policy_version": successor.structural_refusal_policy_version,
                "declared_universe_basis": successor.declared_universe_basis,
                "declared_carry_unmodelled": successor.declared_carry_unmodelled,
                "declared_fx_unmodelled": successor.declared_fx_unmodelled,
                "expected_structural_refusals": list(successor.expected_structural_refusals),
                "min_forward_decision_dates": successor.forward_shadow.min_independent_decision_dates,
                "min_forward_calendar_weeks": successor.forward_shadow.min_calendar_weeks,
                "forward_shadow_derivation": successor.forward_shadow.derivation,
                "declared_by": successor.declared_by,
                "declaration_sha256": successor.sha256,
                "supersedes_declaration_id": frozen.declaration_id,
                "supersession_reason": supersession.reason,
                "supersession_attestation": supersession.attestation,
            },
        ).fetchone()
    except psycopg.errors.UniqueViolation as exc:
        # ⚠ THE BACKSTOP FOR A WRITER THAT SKIPPED THE LOCK. Two supersessions
        # that both take it cannot reach here — the loser re-reads and refuses
        # with `supersession_not_required`. `UNIQUE (supersedes_declaration_id)`
        # is what catches the one that did not, and a raw driver error escaping
        # would be a refusal nobody can act on.
        raise PreregDeclarationRefused(
            successor.strategy_id, successor.strategy_version, ("supersession_predecessor_already_superseded",)
        ) from exc
    if row is None:  # pragma: no cover - RETURNING on a successful INSERT always yields a row
        raise RuntimeError("superseding declaration INSERT returned no declaration_id")
    return int(row[0])


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

    ⚠⚠ #2634 — THE LOCK COMES FIRST, AND IT IS WHAT ORDERS THIS AGAINST A
    SUPERSESSION. Without it, a re-declaration can count zero accesses, this can
    insert one, and both commit: a trial re-declared after it was looked at,
    which is the fabrication the whole arrangement exists to prevent. With it,
    whichever side gets there first wins cleanly — an access first makes the
    supersession refuse ``supersession_trial_already_exposed``, and a
    supersession first makes this re-read and authorise against the new
    revision. ⚠ It also covers the case row locks cannot: a trial with no
    declaration yet locks no rows but does lock its identity.

    ⚠ The ``declaration_id`` written is the one this call CHECKED, not a second
    load. Two loads could resolve differently under a concurrent supersession,
    and an access attributed to a revision that did not authorise it is worse
    than no attribution at all.
    """
    _lock_trial(conn, access.strategy_id, access.strategy_version)
    frozen = _refuse_incoherent_declaration(conn, access)
    row = conn.execute(
        _RECORD_ACCESS,
        {
            "strategy_id": access.strategy_id,
            "strategy_version": access.strategy_version,
            "result_version": access.result_version,
            "access_kind": access.access_kind,
            "accessed_by": access.accessed_by,
            "purpose": access.purpose,
            "declaration_id": None if frozen is None else frozen.declaration_id,
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

    ⚠ #2611 — THE REFUSAL IS NOW RECORDED (``sql/340``), on its own connection so
    it survives the caller's rollback. ⚠ What it records is a SNAPSHOT-RELATIVE
    observation: this check runs before the trial lock is taken, so a concurrent
    freeze that commits a moment later leaves a
    ``preregistration_not_frozen`` row that is nonetheless a true statement about
    what this look saw. The audit is of the attempt, not of the trial.
    """
    if load_preregistration(conn, access.strategy_id, access.strategy_version) is None:
        _refuse_access(conn, access, ("preregistration_not_frozen",))
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

    ⚠⚠ #2634 — THE JOIN IS ON ``a.declaration_id``, NOT ON TRIAL IDENTITY. With
    a chain of revisions per trial, joining on the trial proved only that SOME
    declaration for it predates the look; equality on the access row's own
    column proves the named revision is the one that authorised THIS access.
    That is a tightening rather than a break: with 0 declarations stored, the
    old join produced no rows for any existing access either.

    ⚠ It deliberately does NOT require the declaration to be current *now*.
    Provenance is "was current when the access happened"; authorisation is
    ``record_holdout_access``'s job, one call earlier. Requiring current-now
    would also break every caller that persists a ``declaration_id`` into a
    signed artifact and verifies later — which
    ``scripts/evaluate_2582_schedule13d_outcomes.py``,
    ``scripts/schedule13d_artifact.py`` and ``scripts/sealed_rerun_gate.py`` all
    do.

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
    # ⚠ #2611 — RAISES HERE WITHOUT AN AUDIT ROW, DELIBERATELY, and the
    # behaviour is byte-identical to what `_refuse_incoherent_declaration` did
    # before it grew one. This is a RE-CHECK: it requires an `access_id` whose
    # committed row already records the attempt, so auditing here would file a
    # second refusal for a single look. See `_declaration_refusal_codes`.
    frozen, declaration_codes = _declaration_refusal_codes(conn, strategy_id, strategy_version)
    if declaration_codes:
        raise PreregDeclarationRefused(strategy_id, strategy_version, declaration_codes)
    refusals: list[str] = []
    if frozen is None:
        refusals.append("preregistration_not_frozen")
    elif declaration_id not in frozen.chain_declaration_ids:
        # ⚠⚠ #2634 — MEMBERSHIP IN THE CHAIN, NOT EQUALITY WITH THE CURRENT
        # REVISION, and the two disagree the moment a supersession lands. Codex
        # checkpoint 2 caught the equality form still standing while the
        # docstring above already promised historical revisions were accepted —
        # a doc and its code disagreeing, which is the shape #2614 was filed
        # for. Equality here would refuse every signed artifact naming the
        # predecessor, which is precisely the wedge #2634 exists to remove.
        #
        # It does not weaken the check: `_SELECT_ACCESS_PROVENANCE` joins on
        # `a.declaration_id`, so the named revision must be the one recorded on
        # the access row itself. Chain membership adds only that the revision
        # belongs to THIS trial — which is what `declaration_identity_mismatch`
        # has always meant.
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


def read_access_refusals(
    conn: psycopg.Connection[tuple],
    strategy_id: str,
    strategy_version: str,
    *,
    limit: int = 100,
) -> tuple[RefusedAccess, ...]:
    """#2611's governance read: what was refused on this trial, newest first.

    ⚠ RECORDS NOTHING AND IS NOT AN ACCESS. Reading the list of looks that were
    DENIED exposes no hold-out number, so it needs no access record of its own —
    which is the whole reason the refusals live in their own relation rather than
    behind ``read_holdout_results``' door.

    ⚠ BOUNDED. Nothing prunes this table, and an unbounded read of an audit log
    is a resource problem that arrives quietly and late.
    """
    if limit <= 0:
        raise ValueError("limit must be positive — a zero-row governance read reports 'nothing was refused'")
    rows = conn.execute(
        _SELECT_ACCESS_REFUSALS,
        {"strategy_id": strategy_id, "strategy_version": strategy_version, "limit": limit},
    ).fetchall()
    return tuple(
        RefusedAccess(
            refusal_id=int(row[0]),
            strategy_id=str(row[1]),
            strategy_version=str(row[2]),
            result_version=None if row[3] is None else str(row[3]),
            access_kind=cast(HoldoutAccessKind, str(row[4])),
            accessed_by=str(row[5]),
            purpose=str(row[6]),
            refusals=tuple(str(code) for code in (row[7] or ())),
            declaration_id=None if row[8] is None else int(row[8]),
            refused_at=cast(datetime, row[9]),
        )
        for row in rows
    )


def holdout_access_counts(
    conn: psycopg.Connection[tuple],
    strategy_id: str,
    strategy_version: str,
) -> HoldoutAccessCounts:
    """``PromotionCandidate``'s two hold-out inputs. See ``HoldoutAccessCounts``.

    ⚠ RECORDS NOTHING — two pure ``COUNT``s, so it is safe to call from the
    promotion transition. #2639's inventory originally said otherwise; the
    function that records is ``quarantine_arms_compared``, and only on a
    ``hold_out`` identity.

    ⚠ ONE STATEMENT, ONE SNAPSHOT. See ``_COUNT_HOLDOUT_EVALUATIONS_AND_ACCESSES``.
    """
    row = conn.execute(
        _COUNT_HOLDOUT_EVALUATIONS_AND_ACCESSES,
        {"strategy_id": strategy_id, "strategy_version": strategy_version},
    ).fetchone()
    if row is None:  # pragma: no cover - count() always returns a row
        raise RuntimeError("count query returned no row")
    return HoldoutAccessCounts(holdout_evaluations=int(row[0]), recorded_accesses=int(row[1]))


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
    return quarantine_arm_pair_present(conn, identity)


def quarantine_arm_pair_present(conn: psycopg.Connection[tuple], identity: ResultIdentity) -> bool:
    """The same count as ``quarantine_arms_compared``, RECORDING NOTHING (#2639).

    ⚠⚠ THE ONLY DIFFERENCE IS THE ACCESS RECORD, AND THAT IS WHY IT IS SPLIT
    OUT RATHER THAN COPIED. ``quarantine_arms_compared`` is the door for a
    caller ASKING THE DATABASE ABOUT THE WITHHELD SIDE, where *looking is the
    event criterion 5 governs*. The promotion transition is not such a caller:
    it holds a result it has already pinned, it already reads that row's own
    columns, and promotion is not an evaluation. A transition that recorded here
    would write one ``read`` row per promotion attempt into the log it is
    auditing — the prevention-log rule *"it must not ask the database a question
    it is the answer to"*, one layer further out. The verdict is identical
    either way, so a second copy of the count would be a second thing to keep in
    step for no gain.

    ⚠ THE SIBLING IS DERIVED, NEVER NAMED. ``ResultIdentity.version`` is a hash
    of the whole identity, so the flipped-arm version is the ONLY row that can
    satisfy the pair — which is what a stored ``sibling_result_id`` pointer
    (#2639's first draft, killed at Codex checkpoint 1) would have given up: a
    pointer is chosen by the writer and can name a compatible row that is not
    the one the identity admits.

    ⚠ MONOTONE, WHICH IS WHY THE POLICY CALLS THIS ``frozen`` rather than
    ``today``. Both arms are rows written at result time and the store has no
    delete path, so the answer moves only from "one arm" to "both arms". Nothing
    about today's world enters it.
    """
    sibling = replace(identity, quarantine_arm=("admitted" if identity.quarantine_arm == "masked" else "masked"))
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


def _arm_pairs_present(
    conn: psycopg.Connection[tuple], identities: Sequence[ResultIdentity]
) -> dict[ResultIdentity, bool]:
    """``quarantine_arm_pair_present`` for many identities, in ONE statement (#2641).

    ⚠ MEMBERSHIP, NOT A COUNT — and the two are equivalent only because of
    ``sql/262_strategy_results.sql:182``, ``UNIQUE (strategy_id,
    strategy_version, result_version)``. The singular form asks for
    ``count(*) == 2`` over the two arm versions, which without that constraint
    could be satisfied by two rows of the SAME arm. Verified on the full
    population 2026-08-13: 0 duplicate triples over 324 stored rows. If the
    constraint is ever dropped, this batch and its singular sibling stop
    agreeing — that is the reason to name it here rather than in a commit
    message.

    Every identity in one promotion shares a ``(strategy_id, strategy_version)``
    by construction (they are results pinned to one version), so a single
    predicate over the union of arm versions covers all of them; a mixed batch
    would silently ask the wrong question, hence the assertion.
    """
    if not identities:
        return {}
    strategy_ids = {(identity.strategy_id, identity.strategy_version) for identity in identities}
    if len(strategy_ids) != 1:
        raise RuntimeError(f"arm-pair batch spans {len(strategy_ids)} strategy versions; expected exactly one")
    strategy_id, strategy_version = next(iter(strategy_ids))
    siblings = {
        identity: replace(identity, quarantine_arm=("admitted" if identity.quarantine_arm == "masked" else "masked"))
        for identity in identities
    }
    wanted = sorted({identity.version for identity in identities} | {s.version for s in siblings.values()})
    rows = conn.execute(
        _SELECT_ARM_VERSIONS,
        {
            "strategy_id": strategy_id,
            "strategy_version": strategy_version,
            "result_versions": wanted,
        },
    ).fetchall()
    present = {str(row[0]) for row in rows}
    return {identity: identity.version in present and siblings[identity].version in present for identity in identities}


_SELECT_RESULT_BY_ID = f"""
    SELECT {_RESULT_COLUMNS}
    FROM strategy_results_store
    WHERE result_id = %(result_id)s
"""  # noqa: S608 - a module-level literal, no caller input reaches the fragment

_SELECT_RESULTS_BY_IDS = f"""
    SELECT result_id, {_RESULT_COLUMNS}
    FROM strategy_results_store
    WHERE result_id = ANY(%(result_ids)s::bigint[])
"""  # noqa: S608 - a module-level literal, no caller input reaches the fragment

_SELECT_CONTROL_SUPPORT = """
    SELECT holdout_result_id, candidate_count, control_result_id
    FROM strategy_result_control_support
    WHERE holdout_result_id = ANY(%(result_ids)s::bigint[])
"""


def stored_result_promotion_refusals(conn: psycopg.Connection[tuple], result_id: int) -> tuple[PromotionRefusal, ...]:
    """Every refusal the STORED ROW itself decides, for the transition (#2639).

    ``promote_strategy`` cannot call ``check_promotable`` — it holds a
    ``result_id``, not a ``StrategyResult`` — so #2625 replayed the structural
    stamps and left the purpose, deflation, effective-sample-size and
    synthetic-control clauses trusting a write-time verdict that died with
    ``WrittenRow``. All of them are columns on the row the caller has already
    pinned. This rebuilds the row through the existing ``_result_from_row`` and
    applies the SAME pure functions ``check_promotable`` applies, in its order.

    ⚠⚠ IT RETURNS CODES AND NEVER A ``StrategyResult``, DELIBERATELY. A public
    ``load_result_by_id`` would be a new UNAUDITED DOOR TO THE WITHHELD SIDE —
    ``read_holdout_results`` is the sanctioned one and it records the access
    first, and 300 of the 324 stored rows are ``hold_out``. Keeping the withheld
    numbers inside this module and handing the transition a refusal list means
    this function cannot become that door.

    ⚠ Reading a pinned row's own columns is not a new governance cost:
    ``promote_strategy`` already selects six of them, and
    ``holdout_access_counts`` already counts these rows, neither recording an
    access.

    ⚠ ``_result_from_row`` RAISES where the gate refuses — on a ``result_version``
    that does not match the identity it carries, on a stored
    ``synthetic_control_passed`` that disagrees with its own inputs, and on a
    partially-written DSR or control block. That is ``load_result_ambiguity``'s
    precedent (corruption is an integrity failure to surface loudly, not a gate
    verdict to report politely) and it carries the same named cost: the raise
    aborts before the other refusals are gathered, so it MASKS them. Verified on
    the full population 2026-08-13 — 324 of 324 stored rows reconstruct, so no
    stored row takes that path today.

    ⚠ THE STRUCTURAL STAMPS ARE NOT RETURNED HERE. ``promote_strategy`` keeps its
    own read of ``universe_basis`` / ``carry_unmodelled`` / ``fx_unmodelled``
    because it coerces a NULL cost stamp to ``True`` (unmodelled), while
    ``_result_from_row`` coerces with ``bool(...)``, which reads NULL as
    *modelled* — fail-open on a Tier 1 refusal. Both columns are ``NOT NULL``
    (``sql/262``, ``sql/335``), so this is defence in depth; the two coercions
    must not be collapsed onto the weaker one.
    """
    return stored_result_promotion_refusals_for(conn, [result_id])[result_id]


def _refusals_for_result(
    result: StrategyResult,
    *,
    arm_pair_present: bool,
    control_support: StrategyResult | None = None,
) -> tuple[PromotionRefusal, ...]:
    """The pure half: ``check_promotable``'s order over one rebuilt row.

    Split out so the single and batch reads apply the same clauses in the same
    order — the acceptance on #2641 is that batching changes the number of
    statements and nothing about the verdict.
    """
    refusals: list[PromotionRefusal] = []
    # `check_promotable`'s order, minus the blocks the transition replays from
    # their own frozen records (universe, ambiguity) or reads separately (the
    # structural stamps, the #2505 evidence).
    refusals.extend(purpose_promotion_refusals(result.purpose))
    refusals.extend(metric_axis_promotion_refusals(result))
    refusals.extend(
        deflation_promotion_refusals(
            deflated_sharpe=result.deflated_sharpe,
            trial_count=result.trial_count,
            deflated=result.deflated,
            effective_sample_size=result.metrics.effective_sample_size,
        )
    )
    # Criterion 9 — re-derived from the two arms' rows rather than from a
    # recorded boolean. See `quarantine_arm_pair_present` for why the recording
    # door is not the one the transition uses.
    if not arm_pair_present:
        refusals.append("quarantine_arms_not_compared")
    # #2737 — a hold-out row deliberately owns no 1,000-member random-entry
    # cohort: running it there would be 1,000 outcome looks. Its control clause
    # is replayed from the exact in-sample companion derived by the database
    # view. An in-sample result continues to stand on its own control. Missing,
    # ambiguous or control-free support remains `synthetic_control_not_run`.
    control = result.synthetic_control
    if result.identity.namespace == "hold_out":
        control = None if control_support is None else control_support.synthetic_control
    refusals.extend(synthetic_control_promotion_refusals(control))
    return tuple(refusals)


def stored_result_promotion_refusals_for(
    conn: psycopg.Connection[tuple], result_ids: Sequence[int]
) -> dict[int, tuple[PromotionRefusal, ...]]:
    """``stored_result_promotion_refusals`` for a whole batch (#2641/#2737).

    One row read for every result and one arm-version read for every identity.
    A batch containing hold-out rows adds one derived-support census and one
    batched read of the distinct in-sample support rows: at most four statements,
    never one pair per result. The hold-out control verdict comes from those
    support rows; every other clause and its order are unchanged.

    ⚠ Same ``RuntimeError`` on a missing row, and the same masking cost: a
    corrupt or absent row anywhere in the batch now raises BEFORE any result's
    refusals are returned, where the per-result loop would have reported the
    earlier results first. That reordering is inherent to batching and is why it
    is stated rather than assumed — the raise is an integrity failure either
    way, but which one an operator sees first has changed.
    """
    if not result_ids:
        return {}
    rows = conn.execute(_SELECT_RESULTS_BY_IDS, {"result_ids": list(result_ids)}).fetchall()
    results = {int(row[0]): _result_from_row(row[1:]) for row in rows}
    for result_id in result_ids:
        if result_id not in results:
            # ⚠ RAISES, does not refuse. `PromotionRefusal` is a closed vocabulary
            # of reasons a REAL result may not be promoted; "the row does not exist"
            # is a caller error, and `promote_strategy` has already refused an
            # unknown result_id with its own message before reaching here. Inventing
            # a refusal code for it would put a programming error into the operator's
            # list of things to fix about a strategy.
            raise RuntimeError(f"no stored result row for result_id {result_id}")
    pairs = _arm_pairs_present(conn, [results[result_id].identity for result_id in result_ids])

    # The view derives candidates from immutable identity pins; the caller
    # cannot supply a favourable support id. `candidate_count != 1` is the
    # fail-closed state and deliberately maps to a missing control below.
    holdout_ids = [result_id for result_id in result_ids if results[result_id].identity.namespace == "hold_out"]
    support_ids_by_result: dict[int, int] = {}
    if holdout_ids:
        support_rows = conn.execute(_SELECT_CONTROL_SUPPORT, {"result_ids": holdout_ids}).fetchall()
        support_census = {int(row[0]): (int(row[1]), None if row[2] is None else int(row[2])) for row in support_rows}
        for result_id in holdout_ids:
            candidate_count, support_id = support_census.get(result_id, (0, None))
            if candidate_count == 1 and support_id is not None:
                support_ids_by_result[result_id] = support_id

    support_results: dict[int, StrategyResult] = {}
    if support_ids_by_result:
        support_rows = conn.execute(
            _SELECT_RESULTS_BY_IDS,
            {"result_ids": sorted(set(support_ids_by_result.values()))},
        ).fetchall()
        support_results = {int(row[0]): _result_from_row(row[1:]) for row in support_rows}
        missing_support = set(support_ids_by_result.values()) - set(support_results)
        if missing_support:
            raise RuntimeError(f"derived control support result(s) disappeared: {sorted(missing_support)}")

    return {
        result_id: _refusals_for_result(
            results[result_id],
            arm_pair_present=pairs[results[result_id].identity],
            control_support=(
                support_results[support_ids_by_result[result_id]] if result_id in support_ids_by_result else None
            ),
        )
        for result_id in result_ids
    }


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
    "RefusedAccess",
    "freeze_preregistration",
    "holdout_access_counts",
    "load_preregistration",
    "quarantine_arm_pair_present",
    "quarantine_arms_compared",
    "read_access_refusals",
    "read_holdout_results",
    "read_walk_forward_folds",
    "record_holdout_access",
    "require_outcome_access",
    "store_holdout_arm_pair",
    "store_holdout_result",
    "store_in_sample_arm_pair",
    "store_in_sample_result",
    "store_walk_forward_folds",
    "stored_result_promotion_refusals",
    "supersede_preregistration",
    "verify_outcome_access_provenance",
]
