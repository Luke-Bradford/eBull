"""Full-population verification of phase-5e-5c's two result writers (#2240).

    PYTHONPATH=. uv run python scripts/verify_2240_result_writers.py --all

⚠ EVERY WRITE HERE IS ROLLED BACK. Both arms open a transaction against the dev
database, exercise the real relations — the FK, the primary key, ``sql/269``'s
trigger, ``sql/264``'s view and access trigger — and then unwind it. W4 and P4
re-count the two tables afterwards and FAIL if the occupancy moved, so "it
rolled back" is asserted rather than assumed. ⚠ Sequences do not rewind; that is
expected and is not what is counted.

⚠ NEVER PIPE THIS INTO ``head``/``tail``. A pipe returns the pipe's status, so a
failure reads as success, and it buffers the progress lines a long run is judged
by (`.claude/CLAUDE.md`). Redirect to a file and read the file.

⚠⚠ EVERY FIGURE IS IN-SAMPLE ONLY, for the reason
``verify_2240_walk_forward.py`` gives: the split is cut inside the in-sample
side and a measurement spanning the hold-out would repeat, one level up, the
error §5.3 rejected *measured p99* for.

THE ARMS
--------
``--split`` — **the full-population arm.** One sweep of the corpus produces
S-1's and S-3's in-sample label windows; the four bar-weighted folds, each
fold's MEASURED panel embargo and its four-way census are then assembled into
the object that gets stored.

  W1  **The measured split constructs.** ``WalkForwardFolds`` refuses a set that
      is not four contiguous folds counting one population — and on a hand-drawn
      axis that is trivially satisfied. On the real corpus it is a claim: the
      folds come from ``bar_weighted_folds`` over 14,975 lumpy dates, the
      censuses from four independent passes over millions of observations, and
      the equal-totals check is what would catch two of them having been run
      over different populations.
  W2  **The round trip is exact, on the real magnitudes.** Written through
      ``store_walk_forward_folds``, read back through ``read_walk_forward_folds``,
      compared as whole objects. ⚠ This is where an ``INTEGER`` count column or
      a slipped position in ``_FOLD_COLUMNS`` shows up: S-1 contributes ~2.5M
      observations per fold and an embargo of 931, and a fixture of 30 and 7
      exercises neither magnitude.
  W3  **The trigger refuses the same split on a hold-out result.** The identical
      object, the identical statement, one different parent — so a pass proves
      the refusal is the parent's namespace and not something about the payload.
  W4  **The transaction really unwound**: both tables' occupancy is re-counted
      after the rollback.

``--pair`` — criterion 9's arm pair, against the real relations. ⚠ A MECHANISM
arm and not a measurement: the two arms' METRIC DELTA is stage 5e-5a's and is
reported by ``verify_2240_quarantine_sensitivity.py``, which is an 83-minute
corpus sweep and is not re-run here to re-measure a number nothing in this stage
changes. What is asserted is the storage behaviour that stage could not have.

  P1  **Both arms land through one call**, under different ``result_version``s.
  P2  **The pair reads back as compared**, from EITHER arm's identity, and a
      lone arm does not.
  P3  **The promotion gate's ``quarantine_arms_not_compared`` clears** on the
      pair and on nothing else — every other refusal stands, which is §6's
      stated initial state.
  P4  **The transaction really unwound.**

⚠ WHAT IS NOT COVERED, STATED SO THE GAP IS A DECISION. Only S-1 and S-3 sweep,
for phase 5a's reason (S-2 needs its whole panel resident, S-4 the resolver over
the corpus) — the same gap ``verify_2240_walk_forward.py`` declares, and neither
strategy changes a rule these writers apply: a split is a function of dates and
a pair is a function of two identities.

⚠ THE RESULT ROW THE FOLDS HANG OFF IS A CARRIER, NOT A MEASUREMENT. Its metric
set is not read by any assertion here; storing folds needs a parent row and
``sql/263`` makes sixteen metric columns NOT NULL. The one figure taken from the
sweep is the trade count, so the carrier cannot be mistaken for a result of a
different size. Real metric sets are ``verify_2240_statistics.py``'s.
"""

from __future__ import annotations

import argparse
import sys
import time

import psycopg

from app.config import settings
from app.services.result_ledger import (
    quarantine_arms_compared,
    read_walk_forward_folds,
    store_holdout_result,
    store_in_sample_arm_pair,
    store_in_sample_result,
    store_walk_forward_folds,
)
from app.services.strategy_result import (
    BENCHMARK_RULE,
    CORPUS_VERSION,
    EVALUATION_WINDOW_END,
    EVALUATION_WINDOW_START,
    SIZING_RULE,
    TOTAL_RETURN_BASIS,
    PromotionCandidate,
    ResultIdentity,
    StrategyResult,
    check_promotable,
)
from app.services.strategy_statistics import StrategyMetrics
from app.services.walk_forward import (
    FOLD_COUNT,
    WALK_FORWARD_MODEL_ID,
    FoldRecord,
    WalkForwardFolds,
    bar_weighted_folds,
    census,
    training_embargo_bars,
)

# ⚠ REUSED, not re-derived. The corpus→observations path is 5e-4's and a second
# copy here would be a second place for the fill rule to drift.
from scripts.verify_2240_walk_forward import _collect

_ACTOR = "scripts/verify_2240_result_writers.py"
_PURPOSE = "stage 5e-5c full-population verification (rolled back)"

#: A version string no strategy can produce, so a row that somehow survived a
#: rollback is identifiable rather than mistaken for a real result.
_CARRIER_VERSION = "strategy-registry-v1+5e5cverify"


class _Rollback(Exception):
    """Unwinds a probe transaction. ⚠ Never a real error."""


def _carrier_metrics(trade_count: int) -> StrategyMetrics:
    """A minimal, self-consistent criterion-7 set. See the module header.

    ⚠ ``profit_factor`` and ``sortino`` are ``None`` BECAUSE the losing counts
    are zero — ``StrategyMetrics`` binds each null to its own count and refuses
    a set where they disagree, so this is the shape the type allows rather than
    a set of convenient blanks.
    """
    return StrategyMetrics(
        expectancy_per_trade_pct=0.0,
        profit_factor=None,
        cagr_pct=0.0,
        annualised_volatility_pct=0.0,
        sharpe=0.0,
        sortino=None,
        max_drawdown_pct=0.0,
        exposure_time_pct=0.0,
        turnover_annualised=0.0,
        trade_count=trade_count,
        effective_sample_size=None,
        return_vs_buy_and_hold_pct=0.0,
        losing_trade_count=0,
        losing_period_count=0,
        open_trade_count=0,
        unpriced_trade_count=0,
        periods_per_year=252.0,
        total_return_pct=0.0,
        buy_and_hold_return_pct=0.0,
    )


def _carrier(strategy_id: str, *, namespace: str, quarantine_arm: str, trade_count: int) -> StrategyResult:
    return StrategyResult(
        identity=ResultIdentity(
            strategy_id=strategy_id,
            strategy_version=_CARRIER_VERSION,
            result_scope="sleeve",
            namespace=namespace,  # type: ignore[arg-type]
            ambiguity_arm="worst_case",
            quarantine_arm=quarantine_arm,  # type: ignore[arg-type]
            sizing_rule=SIZING_RULE,
            benchmark_rule=BENCHMARK_RULE,
            cost_model_id="static-p75-insession-v1",
            corpus_version=CORPUS_VERSION,
            window_start=EVALUATION_WINDOW_START,
            window_end=EVALUATION_WINDOW_END,
            position_rule_set_version="position-builder-verify",
            outcome_rule_set_version="outcome-resolver-verify",
            input_rule_set_version="price-quarantine-verify",
            return_basis=TOTAL_RETURN_BASIS,
        ),
        purpose="capital_candidate",
        metrics=_carrier_metrics(trade_count),
        universe_basis="survivor_only",
        carry_unmodelled=True,
        fx_unmodelled=True,
        evaluated_instrument_count=1,
        trial_count=None,
        deflated_sharpe=None,
    )


def _occupancy(conn: psycopg.Connection[tuple]) -> tuple[int, int]:
    rows = conn.execute(
        "SELECT (SELECT count(*) FROM strategy_results_store), (SELECT count(*) FROM strategy_result_folds)"
    ).fetchone()
    if rows is None:  # pragma: no cover - a scalar subquery pair always returns a row
        raise RuntimeError("occupancy query returned no row")
    return int(rows[0]), int(rows[1])


# ---------------------------------------------------------------------------
# --split
# ---------------------------------------------------------------------------


def _build_split(sleeve, *, axis, bar_counts) -> tuple[WalkForwardFolds | None, list[str]]:
    """Assemble one sleeve's measured split. W1 is this function succeeding.

    ⚠ A refusal is REPORTED as a property violation, never raised: a traceback
    would abandon the other sleeve's arm and lose the fold table printed above
    it, and the fold table is the evidence for what went wrong.
    """
    problems: list[str] = []
    folds = bar_weighted_folds(bar_counts, fold_count=FOLD_COUNT)
    total_bars = sum(bar_counts)
    records: list[FoldRecord] = []
    print(f"\n  [{sleeve.label}]  {len(sleeve.starts):,} in-sample observations")
    for fold in folds:
        bars = sum(bar_counts[fold.first_index : fold.last_index + 1])
        embargo = training_embargo_bars(sleeve.starts, sleeve.ends, fold=fold)
        counted = census(sleeve.starts, sleeve.ends, fold=fold, embargo_bars=embargo)
        records.append(
            FoldRecord(
                fold=fold,
                first_date=axis[fold.first_index],
                last_date=axis[fold.last_index],
                bar_count=bars,
                embargo_bars=embargo,
                census=counted,
            )
        )
        print(
            f"    fold {fold.index}  {axis[fold.first_index]} … {axis[fold.last_index]}   "
            f"{bars:>12,} bars ({100.0 * bars / total_bars:5.2f}%)   embargo {embargo:>5,}   "
            f"test {counted.test:>10,} · train {counted.train:>10,} · purged {counted.purged:>8,} · "
            f"embargoed {counted.embargoed:>8,}"
        )
    try:
        split = WalkForwardFolds(model_id=WALK_FORWARD_MODEL_ID, folds=tuple(records))
    except ValueError as exc:
        problems.append(f"{sleeve.label}: W1 the measured split did not construct — {exc}")
        return None, problems
    print(f"      W1 constructs · {split.observation_count:,} observations classified by every fold")
    return split, problems


def _run_split(*, limit: int | None) -> list[str]:
    print("\n=== --split : the measured walk-forward split, written and read back ===")
    observations, axis, bar_counts, _axis_dates = _collect(limit=limit)
    problems: list[str] = []
    splits: dict[str, WalkForwardFolds] = {}
    for label, sleeve in observations.items():
        if not sleeve.starts:
            problems.append(f"{label}: no closed positions — there is no split to store")
            continue
        if sleeve.off_axis:
            problems.append(f"{label}: {sleeve.off_axis:,} fill dates off the in-sample axis (must be 0)")
        split, sleeve_problems = _build_split(sleeve, axis=axis, bar_counts=bar_counts)
        problems.extend(sleeve_problems)
        if split is not None:
            splits[label] = split

    if not splits:
        return problems

    with psycopg.connect(settings.database_url) as conn:
        before = _occupancy(conn)
        print(f"\n  occupancy before   results {before[0]:,} · folds {before[1]:,}")
        try:
            with conn.transaction():
                for label, split in splits.items():
                    trade_count = split.observation_count
                    result_id = store_in_sample_result(
                        conn, _carrier(label, namespace="in_sample", quarantine_arm="masked", trade_count=trade_count)
                    )
                    written = store_walk_forward_folds(conn, result_id, split)
                    read_back = read_walk_forward_folds(conn, result_id)
                    if written != FOLD_COUNT:
                        problems.append(f"{label}: W2 wrote {written} fold rows, expected {FOLD_COUNT}")
                    if read_back != split:
                        problems.append(f"{label}: W2 the split did not survive the round trip")
                    else:
                        print(f"  [{label}] W2 round trip exact — {written} folds, result {result_id}")

                    # W3 — the same object, the same statement, a hold-out parent.
                    holdout_id = store_holdout_result(
                        conn,
                        _carrier(label, namespace="hold_out", quarantine_arm="masked", trade_count=trade_count),
                        accessed_by=_ACTOR,
                        purpose=_PURPOSE,
                    )
                    refused = False
                    try:
                        # ⚠ A SAVEPOINT, so the refusal does not poison the
                        # outer probe transaction — the next sleeve still has to
                        # run inside it.
                        with conn.transaction():
                            store_walk_forward_folds(conn, holdout_id, split)
                    except psycopg.errors.IntegrityError:
                        refused = True
                    if refused:
                        print(f"  [{label}] W3 the trigger refused the same split on a hold-out result")
                    else:
                        problems.append(f"{label}: W3 a hold-out result accepted a walk-forward split")
                raise _Rollback
        except _Rollback:
            pass
        after = _occupancy(conn)
        print(f"  occupancy after    results {after[0]:,} · folds {after[1]:,}")
        if after != before:
            problems.append(f"W4 the probe transaction did not roll back: {before} → {after}")
    return problems


# ---------------------------------------------------------------------------
# --pair
# ---------------------------------------------------------------------------


def _run_pair() -> list[str]:
    print("\n=== --pair : criterion 9's arm pair, against the real relations ===")
    problems: list[str] = []
    masked = _carrier("S-1", namespace="in_sample", quarantine_arm="masked", trade_count=3133100)
    admitted = _carrier("S-1", namespace="in_sample", quarantine_arm="admitted", trade_count=3133792)
    gate_inputs = {
        "evaluated_instrument_ids": frozenset({1}),
        "validated_universe_ids": frozenset({1}),
    }
    with psycopg.connect(settings.database_url) as conn:
        before = _occupancy(conn)
        print(f"  occupancy before   results {before[0]:,} · folds {before[1]:,}")
        try:
            with conn.transaction():
                lone = quarantine_arms_compared(conn, masked.identity, accessed_by=_ACTOR, purpose=_PURPOSE)
                if lone:
                    problems.append("P2 an unwritten pair read back as compared")
                first, second = store_in_sample_arm_pair(conn, masked, admitted)
                if first == second:
                    problems.append("P1 both arms landed on one row")
                else:
                    print(f"  P1 both arms stored — result ids {first}, {second}")
                from_masked = quarantine_arms_compared(conn, masked.identity, accessed_by=_ACTOR, purpose=_PURPOSE)
                from_admitted = quarantine_arms_compared(conn, admitted.identity, accessed_by=_ACTOR, purpose=_PURPOSE)
                if not (from_masked and from_admitted):
                    problems.append(
                        f"P2 the stored pair did not read back as compared (masked {from_masked}, "
                        f"admitted {from_admitted})"
                    )
                else:
                    print("  P2 the pair reads as compared from either arm's identity")

                refusals_before = check_promotable(
                    PromotionCandidate(result=masked, quarantine_arms_compared=lone, **gate_inputs)  # type: ignore[arg-type]
                )
                refusals_after = check_promotable(
                    PromotionCandidate(result=masked, quarantine_arms_compared=from_masked, **gate_inputs)  # type: ignore[arg-type]
                )
                cleared = set(refusals_before) - set(refusals_after)
                if cleared != {"quarantine_arms_not_compared"}:
                    problems.append(
                        f"P3 storing the pair cleared {sorted(cleared)} — exactly "
                        "quarantine_arms_not_compared was expected"
                    )
                else:
                    print(
                        f"  P3 the gate cleared quarantine_arms_not_compared and nothing else "
                        f"({len(refusals_after)} refusals stand)"
                    )
                raise _Rollback
        except _Rollback:
            pass
        after = _occupancy(conn)
        print(f"  occupancy after    results {after[0]:,} · folds {after[1]:,}")
        if after != before:
            problems.append(f"P4 the probe transaction did not roll back: {before} → {after}")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split", action="store_true", help="the measured split, written and read back (full sweep)")
    parser.add_argument("--pair", action="store_true", help="criterion 9's arm pair against the real relations")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--limit", type=int, default=None, help="first N series only — a smoke run, never a figure")
    args = parser.parse_args()
    if not (args.split or args.pair or args.all):
        parser.error("choose --split, --pair or --all")

    started = time.monotonic()
    problems: list[str] = []
    if args.pair or args.all:
        problems.extend(_run_pair())
    if args.split or args.all:
        problems.extend(_run_split(limit=args.limit))

    print(f"\n=== {len(problems)} property violation(s) in {time.monotonic() - started:.1f}s ===")
    for problem in problems:
        print(f"  *** {problem}")
    if args.limit is not None:
        print("\n⚠ --limit was set: these are SMOKE figures, not a full-population result.")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
