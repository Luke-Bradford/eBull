"""Open S-H arm 1's in-sample exploration under its frozen declaration. Refs #2840.

Contract: ``docs/proposals/ta/2026-08-22-sh-volatile-regime-gated-breakout.md``
(``sh-volatile-regime-gate-2026-08-22``), declaration 11, frozen by
``scripts/freeze_2840_sh_regime_gate_declaration.py`` at ``fa144b7c``/``8eaf9053``.

⚠⚠ A SEPARATE FILE FROM THE FREEZE SCRIPT, AND THAT IS THE POINT. Two files with
two commits make "the bar was written before the numbers were seen" a property of
the repository rather than of the author's word — the same rule
``scripts/measure_2837_se_overlay.py`` follows.

⚠⚠ THIS SCRIPT DECIDES NOTHING THE CONTRACT DID NOT ALREADY DECIDE. The pass leg
(`bear_volatile` expectancy positive in BOTH quarantine arms), the four reported
cells, the banned metrics and the cohort-n reference points are read off §"Readout
and abort bar" and are not re-derived here. A tightening — requiring the bootstrap
CI to clear zero, say, or conjoining the `best_case` ambiguity arm — would be a bar
chosen after the look, so both are REPORTED and neither is decisive.

⚠ It computes no statistic. Everything below is already stored by
``strategy_backtest_run``: this is the read, the gate and the readout.

Two doors, deliberately different (``result_ledger``'s own distinction):

* **s11** goes through ``require_outcome_access`` — #2599's refusing door. It has a
  frozen declaration, so a look that skipped the door would be the exact
  fabrication the contract exists to prevent.
* **s4** goes through ``record_holdout_access`` — the non-refusing door. S-4 is the
  turnover control the contract's step 5 names, and it has no frozen declaration;
  ``require_outcome_access`` would refuse it ``preregistration_not_frozen`` and
  ``_refuse_access`` would mint a permanent refusal row recording a governance
  failure that did not happen. ``record_holdout_access``'s docstring is explicit
  that a trial with no declaration is left alone, which is the honest charge here.

Run AFTER a ``strategy_backtest_run`` invocation carrying no ``evidence_window``
and no hold-out pair, so the withheld side is neither computed nor written::

    PYTHONPATH=. uv run python -m scripts.measure_2840_sh_regime_gate --run-id <id>

⚠ ``--run-id`` is required and the job's status is read BEFORE any outcome
relation, following ``scripts/audit_2745_in_sample_run.py``: a non-successful run
exits without opening a result, preserving the invocation's atomic evidence
boundary. A partial run's cohorts are not evidence and must not be looked at.

⚠⚠ AND THE RUN ID IS *BOUND* TO THE ROWS, not merely status-checked. Caught at
Codex checkpoint 2: the first draft validated ``--run-id`` and then selected every
stored row matching the two strategy versions, so an unrelated successful run id
would have reported months-old results as this invocation's. See
``_assert_run_is_evidence`` for the binding and its one stated limit.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Final, TextIO

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.services.prereg_contract import declaration_refusals
from app.services.result_ledger import (
    HoldoutAccess,
    PreregDeclarationRefused,
    load_preregistration,
    record_holdout_access,
    require_outcome_access,
    verify_outcome_access_provenance,
)
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_result import HOLDOUT_BOUNDARY
from app.services.strategy_result_identity import BACKTEST_UNIVERSE, COST_MODEL_ID
from app.services.trial_register import TRIAL_REGISTER
from scripts.freeze_2840_sh_regime_gate_declaration import (
    CONTRACT_VERSION,
    STRATEGY_ID,
    STRATEGY_VERSION,
)

#: S-4 ungated, the control the gate is defined against. ⚠ It must be in the same
#: invocation: criterion 6's Deflated Sharpe deflates by the variance of Sharpes
#: ACROSS the measured trials, so an s11-only run stores rows permanently refused
#: ``deflated_sharpe_not_computed``.
CONTROL_STRATEGY_ID: Final = "s4-volatility-compression-breakout"

#: ⚠ COMPUTED from the merged manifest on the same two pins the freeze script uses,
#: never pasted. A hand-typed hash that stopped matching the module would charge the
#: access against a strategy version that does not exist.
CONTROL_STRATEGY_VERSION: Final = (
    STRATEGY_MANIFEST[CONTROL_STRATEGY_ID].identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID).version
)

#: The declared ambiguity arm for the decision. ``best_case`` is read and printed
#: beside it as context; conjoining it would be a bar chosen after the look.
DECISION_AMBIGUITY_ARM: Final = "worst_case"
CONTEXT_AMBIGUITY_ARM: Final = "best_case"

#: The four cells, in the contract's order.
DECIDING_REGIME: Final = "bear_volatile"
CONTEXT_REGIME: Final = "bull_volatile"
REGIMES: Final[tuple[str, str]] = (DECIDING_REGIME, CONTEXT_REGIME)
QUARANTINE_ARMS: Final[tuple[str, str]] = ("masked", "admitted")

#: §"Readout and abort bar" — the cohort the HYPOTHESIS came from, reported beside
#: the exploration cohort and read as a verdict in neither direction. Dates are the
#: unit that binds; ``trade_count`` counts one market-wide regime date fanned out
#: across hundreds of instruments.
REFERENCE_COHORT_N: Final[Mapping[tuple[str, str], tuple[int, int]]] = {
    ("bear_volatile", "masked"): (429, 14),
    ("bear_volatile", "admitted"): (508, 14),
    ("bull_volatile", "masked"): (555, 18),
    ("bull_volatile", "admitted"): (811, 18),
}


class ReadoutRefused(RuntimeError):
    """The look must not happen, or the run it would read is not evidence."""


# ---------------------------------------------------------------------------
# The pure half — the contract's arithmetic, table-tested with no database.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Cell:
    """One {regime} x {quarantine arm} cohort, as stored."""

    regime: str
    quarantine_arm: str
    trade_count: int
    decision_date_count: int
    instrument_count: int
    expectancy_pct: float | None
    profit_factor: float | None
    expectancy_ci_low_pct: float | None
    expectancy_ci_high_pct: float | None


@dataclass(frozen=True)
class Verdict:
    """The frozen pass leg, and the two failure shapes the contract names."""

    passed: bool
    reasons: tuple[str, ...]
    #: True when the deciding regime fails while `bull_volatile`-on-`masked` is
    #: positive — the shape §"Readout and abort bar" calls out by name, because
    #: that cell is the one already known to flip sign under the sensitivity arm.
    carried_by_bull_volatile_masked_alone: bool


def cell_verdict(cells: Mapping[tuple[str, str], Cell]) -> Verdict:
    """§"Readout and abort bar": PASS iff `bear_volatile` expectancy is positive in
    BOTH quarantine arms.

    ⚠ The POINT estimate, not the bootstrap CI. Requiring the CI to clear zero
    would be a tighter bar than the one frozen before the look, and a bar chosen
    after the numbers are visible is not a preregistration.

    ⚠ A missing cell is a FAIL, never a skip. An absent cohort row and a cohort
    that traded and lost are different states, and collapsing them would let a run
    that produced no `bear_volatile` trades read as anything at all.
    """
    reasons: list[str] = []
    for arm in QUARANTINE_ARMS:
        cell = cells.get((DECIDING_REGIME, arm))
        if cell is None:
            reasons.append(f"{DECIDING_REGIME}/{arm}: cohort absent")
        elif cell.expectancy_pct is None:
            reasons.append(f"{DECIDING_REGIME}/{arm}: expectancy not stored")
        elif cell.expectancy_pct <= 0.0:
            reasons.append(f"{DECIDING_REGIME}/{arm}: expectancy {cell.expectancy_pct:+.4f}% is not positive")

    bull_masked = cells.get((CONTEXT_REGIME, "masked"))
    bull_masked_positive = bull_masked is not None and (bull_masked.expectancy_pct or 0.0) > 0.0
    carried = bool(reasons) and bull_masked_positive
    return Verdict(passed=not reasons, reasons=tuple(reasons), carried_by_bull_volatile_masked_alone=carried)


@dataclass(frozen=True)
class TurnoverCheck:
    """§"Readout and abort bar": the gate must CUT trade count against ungated S-4."""

    gated_trades: int
    control_trades: int
    mechanism_holds: bool
    note: str


def turnover_check(gated_trades: int, control_trades: int) -> TurnoverCheck:
    """A gate that does not cut trade count fails its stated mechanism.

    ⚠ Reported even when the expectancy leg passes. "Expectancy improved" with an
    unchanged trade count means the improvement did not come from the gate, and the
    contract says so explicitly: *"that is reported as a fail of the stated
    mechanism even if expectancy improves"*.
    """
    if control_trades <= 0:
        return TurnoverCheck(gated_trades, control_trades, False, "control traded nothing; ratio undefined")
    holds = gated_trades < control_trades
    ratio = gated_trades / control_trades
    return TurnoverCheck(
        gated_trades,
        control_trades,
        holds,
        f"{gated_trades} / {control_trades} = {ratio:.3f}x ungated S-4",
    )


# ---------------------------------------------------------------------------
# The impure half — the gate and the read.
# ---------------------------------------------------------------------------


def _open_the_gate(conn: psycopg.Connection[tuple]) -> tuple[int, int]:
    """#2599's door for s11, then the non-refusing door for the s4 control.

    ⚠ THE CALLER OWNS THE COMMIT, matching ``measure_2837_se_overlay._open_the_gate``:
    ``require_outcome_access`` writes in this transaction, the measurement afterwards
    opens with ``SET TRANSACTION ISOLATION LEVEL ... READ ONLY`` (valid only as a
    transaction's first statement), and committing the access first keeps the look
    logged if the read then dies.
    """
    trial = TRIAL_REGISTER.trial_for_declaration(STRATEGY_ID, STRATEGY_VERSION)
    if trial is None:
        raise ReadoutRefused(
            f"no trial in {TRIAL_REGISTER.version} claims {STRATEGY_ID}@{STRATEGY_VERSION}; criterion 6's M does "
            "not count this search, so the look must not happen"
        )
    frozen = load_preregistration(conn, STRATEGY_ID, STRATEGY_VERSION)
    if frozen is None:
        raise PreregDeclarationRefused(STRATEGY_ID, STRATEGY_VERSION, ("preregistration_not_frozen",))
    if frozen.declaration.contract_version != CONTRACT_VERSION:
        raise ReadoutRefused(
            f"frozen declaration names contract {frozen.declaration.contract_version!r}, "
            f"this readout implements {CONTRACT_VERSION!r}"
        )
    refusals = declaration_refusals(frozen.declaration)
    if refusals:
        raise PreregDeclarationRefused(STRATEGY_ID, STRATEGY_VERSION, tuple(str(code) for code in refusals))

    access_id = require_outcome_access(
        conn,
        HoldoutAccess(
            strategy_id=STRATEGY_ID,
            strategy_version=STRATEGY_VERSION,
            result_version=None,
            access_kind="read",
            accessed_by="scripts/measure_2840_sh_regime_gate.py",
            purpose=f"open S-H arm 1's in-sample regime-cohort exploration under {CONTRACT_VERSION}",
        ),
    )
    record_holdout_access(
        conn,
        HoldoutAccess(
            strategy_id=CONTROL_STRATEGY_ID,
            strategy_version=CONTROL_STRATEGY_VERSION,
            result_version=None,
            access_kind="read",
            accessed_by="scripts/measure_2840_sh_regime_gate.py",
            purpose=f"read ungated S-4 as {CONTRACT_VERSION}'s declared turnover control",
        ),
    )
    return access_id, frozen.declaration_id


_RESULT_SQL: Final = """
SELECT result_id, strategy_id, strategy_version, ambiguity_arm, quarantine_arm,
       window_start, window_end, metric_axis_start, metric_axis_end,
       universe_basis, corpus_version, cost_model_id, carry_unmodelled, fx_unmodelled,
       namespace, purpose, trial_register_version, trial_count,
       expectancy_per_trade_pct, profit_factor, deflated_sharpe, dsr_measured_trials,
       trade_count, turnover_annualised, evaluated_instrument_count,
       effective_sample_size, synthetic_control_model_id
FROM strategy_results
WHERE strategy_id = ANY(%(strategy_ids)s)
  AND strategy_version = ANY(%(strategy_versions)s)
  AND namespace = 'in_sample'
  AND created_at BETWEEN %(run_started_at)s AND %(run_finished_at)s
ORDER BY strategy_id, ambiguity_arm, quarantine_arm
"""

_COHORT_SQL: Final = """
SELECT result_id, regime, trade_count, instrument_count, decision_date_count,
       expectancy_pct, profit_factor, expectancy_ci_low_pct, expectancy_ci_high_pct,
       worst_trade_pct, effective_sample_size
FROM strategy_result_regime_cohorts
WHERE result_id = ANY(%(result_ids)s::bigint[])
"""


def _float(value: object) -> float | None:
    """⚠ ``numeric`` arrives as ``Decimal``; every comparison below is float."""
    return None if value is None else float(value)  # type: ignore[arg-type]


def _assert_run_is_evidence(conn: psycopg.Connection[tuple], run_id: int) -> tuple[datetime, datetime]:
    """Read the job's status BEFORE any outcome relation (``audit_2745``'s rule),
    and return the interval that BINDS the rows below to this invocation.

    ⚠⚠ RETURNING THE INTERVAL IS THE POINT, NOT THE STATUS CHECK. A ``--run-id``
    that is only checked for ``success`` and then never used again proves that
    *some* run succeeded and nothing at all about the rows the readout goes on to
    open — an unrelated successful run id would let this report months-old results
    while claiming this invocation produced them. There is no ``run_id`` column on
    ``strategy_results`` (checked: no ``strategy%`` table has one), so the binding
    is temporal, and it is sound because ``strategy_backtest`` is a lane of ONE
    (``app/jobs/sources.py``: *"``strategy_backtest_run`` (#2394 §3.2) only"*), so
    no second backtest can have committed a row inside this run's window.

    ⚠ Stated limit: a row written by something OTHER than the jobs lane — a
    developer calling the service by hand during the run — would also fall inside
    the interval. The lane excludes a concurrent job, not a concurrent human.
    """
    row = conn.execute(
        "SELECT job_name, status, error_msg, row_count, started_at, finished_at FROM job_runs WHERE run_id = %s",
        (run_id,),
    ).fetchone()
    if row is None:
        raise ReadoutRefused(f"job run {run_id} does not exist")
    job_name, status, error, row_count, started_at, finished_at = row
    if str(job_name) != "strategy_backtest_run":
        raise ReadoutRefused(f"run {run_id} is {job_name!r}, not strategy_backtest_run")
    if str(status) != "success":
        raise ReadoutRefused(f"run {run_id} is {status!r} ({error!r}); a partial run's cohorts are not evidence")
    if not row_count:
        raise ReadoutRefused(f"run {run_id} wrote row_count={row_count!r}; zero result rows is never a success here")
    if started_at is None or finished_at is None:
        raise ReadoutRefused(
            f"run {run_id} has started_at={started_at!r} finished_at={finished_at!r}; cannot bind rows"
        )
    return started_at, finished_at


def _assert_axis_is_in_sample(rows: Sequence[Mapping[str, object]]) -> None:
    """⚠ THE METRIC AXIS IS THE DISCRIMINATOR, NOT ``window_end``.

    ``window_start``/``window_end`` are the declared evaluation window and span the
    whole corpus on every stored row (1962-01-02 .. 2026-07-08 today). What is
    actually scored is ``metric_axis_dates``, and ``ResultIdentity`` refuses an
    ``in_sample`` row whose axis reaches ``HOLDOUT_BOUNDARY``. Asserted here rather
    than assumed, because reading ``window_end`` to answer "did this touch the
    hold-out?" returns the wrong answer on every row in the table.
    """
    for row in rows:
        axis_end = row["metric_axis_end"]
        if axis_end is None or axis_end >= HOLDOUT_BOUNDARY:  # type: ignore[operator]
            raise ReadoutRefused(
                f"result {row['result_id']} has metric_axis_end={axis_end!r}, at or past the frozen hold-out "
                f"boundary {HOLDOUT_BOUNDARY}; this is not an in-sample exploration"
            )


def _render(
    rows: Sequence[Mapping[str, object]],
    cohorts: Mapping[int, list[Mapping[str, object]]],
    out: TextIO,
) -> tuple[Verdict, TurnoverCheck]:
    by_key = {(str(r["strategy_id"]), str(r["ambiguity_arm"]), str(r["quarantine_arm"])): r for r in rows}

    out.write(f"\ncontract {CONTRACT_VERSION}   universe {BACKTEST_UNIVERSE}   cost model {COST_MODEL_ID}\n")
    out.write(f"decision arm: ambiguity={DECISION_AMBIGUITY_ARM} (best_case printed as context, never decisive)\n")

    for strategy_id, version in ((STRATEGY_ID, STRATEGY_VERSION), (CONTROL_STRATEGY_ID, CONTROL_STRATEGY_VERSION)):
        out.write(f"\n{'=' * 78}\n{strategy_id}  @{version}\n")
        for ambiguity in (DECISION_AMBIGUITY_ARM, CONTEXT_AMBIGUITY_ARM):
            for arm in QUARANTINE_ARMS:
                row = by_key.get((strategy_id, ambiguity, arm))
                if row is None:
                    out.write(f"  {ambiguity}/{arm}: NO ROW\n")
                    continue
                out.write(
                    f"  {ambiguity}/{arm}: expectancy {_fmt(row['expectancy_per_trade_pct'], '+.4f')}%  "
                    f"PF {_fmt(row['profit_factor'], '.4f')}  DSR {_fmt(row['deflated_sharpe'], '.4f')}"
                    f" (M={row['dsr_measured_trials']})  trades {row['trade_count']}  "
                    f"turnover {_fmt(row['turnover_annualised'], '.3f')}\n"
                )
                for cohort in sorted(cohorts.get(int(row["result_id"]), []), key=lambda c: str(c["regime"])):  # type: ignore[arg-type]
                    if str(cohort["regime"]) not in REGIMES:
                        continue
                    reference = REFERENCE_COHORT_N.get((str(cohort["regime"]), arm))
                    ref = f"   [hypothesis cohort {reference[0]} trades / {reference[1]} dates]" if reference else ""
                    out.write(
                        f"      {cohort['regime']:<14} trades {cohort['trade_count']:>6}  "
                        f"dates {cohort['decision_date_count']:>4}  instruments {cohort['instrument_count']:>6}  "
                        f"expectancy {_fmt(cohort['expectancy_pct'], '+.4f')}% "
                        f"[{_fmt(cohort['expectancy_ci_low_pct'], '+.4f')}, "
                        f"{_fmt(cohort['expectancy_ci_high_pct'], '+.4f')}]  "
                        f"PF {_fmt(cohort['profit_factor'], '.4f')}{ref}\n"
                    )

    cells: dict[tuple[str, str], Cell] = {}
    for arm in QUARANTINE_ARMS:
        row = by_key.get((STRATEGY_ID, DECISION_AMBIGUITY_ARM, arm))
        if row is None:
            continue
        for cohort in cohorts.get(int(row["result_id"]), []):  # type: ignore[arg-type]
            regime = str(cohort["regime"])
            if regime not in REGIMES:
                continue
            cells[(regime, arm)] = Cell(
                regime=regime,
                quarantine_arm=arm,
                trade_count=int(cohort["trade_count"]),  # type: ignore[arg-type]
                decision_date_count=int(cohort["decision_date_count"]),  # type: ignore[arg-type]
                instrument_count=int(cohort["instrument_count"]),  # type: ignore[arg-type]
                expectancy_pct=_float(cohort["expectancy_pct"]),
                profit_factor=_float(cohort["profit_factor"]),
                expectancy_ci_low_pct=_float(cohort["expectancy_ci_low_pct"]),
                expectancy_ci_high_pct=_float(cohort["expectancy_ci_high_pct"]),
            )

    verdict = cell_verdict(cells)
    gated = by_key.get((STRATEGY_ID, DECISION_AMBIGUITY_ARM, "admitted"))
    control = by_key.get((CONTROL_STRATEGY_ID, DECISION_AMBIGUITY_ARM, "admitted"))
    turnover = turnover_check(
        int(gated["trade_count"]) if gated else 0,  # type: ignore[arg-type]
        int(control["trade_count"]) if control else 0,  # type: ignore[arg-type]
    )

    out.write(f"\n{'=' * 78}\n")
    out.write(f"S-H ARM 1 VERDICT: {'PASS' if verdict.passed else 'FAIL'}\n")
    for reason in verdict.reasons:
        out.write(f"  - {reason}\n")
    if verdict.carried_by_bull_volatile_masked_alone:
        out.write(
            "  ⚠ bull_volatile/masked is positive while the deciding regime is not. The contract calls this\n"
            "    shape out by name: that cell is the one already known to flip sign under the sensitivity arm.\n"
        )
    mechanism = "HOLDS" if turnover.mechanism_holds else "FAILS"
    out.write(f"stated mechanism (gate CUTS trade count): {mechanism} — {turnover.note}\n")
    return verdict, turnover


def _fmt(value: object, spec: str) -> str:
    if value is None:
        return "     n/a"
    return format(float(value), spec)  # type: ignore[arg-type]


def main(argv: list[str] | None = None) -> int:
    """⚠⚠ THERE IS NO --skip-gate. Using one would itself be the first look the
    preregistration exists to precede — the defect wearing the costume of a
    debugging aid. The arithmetic is covered by ``tests/test_2840_sh_regime_gate_readout.py``
    against synthetic cells, with no database at all.
    """
    parser = argparse.ArgumentParser(description="S-H arm 1 in-sample readout (#2840 step 3)")
    parser.add_argument("--run-id", type=int, required=True, help="the strategy_backtest_run that wrote the rows")
    args = parser.parse_args(argv)

    with psycopg.connect(settings.database_url) as conn:
        try:
            run_started_at, run_finished_at = _assert_run_is_evidence(conn, args.run_id)
        except ReadoutRefused as refused:
            sys.stderr.write(f"REFUSED: {refused}\n")
            return 2

        access_id, declaration_id = _open_the_gate(conn)
        conn.commit()
        # #2614's re-check, and it only means anything AFTER the commit: a
        # rolled-back INSERT leaves no visible row, so re-loading the access BY ID
        # is what proves it committed under a declaration frozen strictly before it.
        verify_outcome_access_provenance(
            conn,
            strategy_id=STRATEGY_ID,
            strategy_version=STRATEGY_VERSION,
            declaration_id=declaration_id,
            access_id=access_id,
        )
        conn.rollback()
        sys.stdout.write(
            f"#2599 access recorded and verified: access_id={access_id}, "
            f"declaration_id={declaration_id}, contract={CONTRACT_VERSION}\n"
        )

        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        with conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                _RESULT_SQL,
                {
                    "strategy_ids": [STRATEGY_ID, CONTROL_STRATEGY_ID],
                    "strategy_versions": [STRATEGY_VERSION, CONTROL_STRATEGY_VERSION],
                    "run_started_at": run_started_at,
                    "run_finished_at": run_finished_at,
                },
            )
            rows = list(cursor.fetchall())
            _assert_axis_is_in_sample(rows)
            cursor.execute(_COHORT_SQL, {"result_ids": [int(r["result_id"]) for r in rows]})
            cohorts: dict[int, list[Mapping[str, object]]] = {}
            for cohort in cursor.fetchall():
                cohorts.setdefault(int(cohort["result_id"]), []).append(cohort)
        conn.rollback()

    if not rows:
        sys.stderr.write(
            f"REFUSED: run {args.run_id} committed no in-sample row for either strategy version between "
            f"{run_started_at} and {run_finished_at}; the run id and the rows are not the same evidence\n"
        )
        return 2

    verdict, _turnover = _render(rows, cohorts, sys.stdout)
    if not verdict.passed:
        sys.stdout.write(
            "\nA fail is terminal for arm 1 under this declaration. A different gate, band or regime proxy is a\n"
            "NEW declared search charging the shared trial register again — record the lesson on #2840 first.\n"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CONTRACT_VERSION",
    "CONTROL_STRATEGY_ID",
    "Cell",
    "ReadoutRefused",
    "TurnoverCheck",
    "Verdict",
    "cell_verdict",
    "main",
    "turnover_check",
]
