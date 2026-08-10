"""Full-population verification of criterion 5's split wired into the backtest run.

    PYTHONPATH=. uv run python scripts/verify_2240_fold_wiring.py --all

Two arms, and only the first is expensive:

``--splits``
    Loads the real corpus (which asserts the in-sample-axis prefix invariant),
    evaluates every runnable strategy on both quarantine arms, cuts the split
    each in-sample result row would carry, and CROSS-CHECKS the masked arms
    against the figures ``verify_2240_result_writers.py`` published in the
    bounded-backtester spec §8.8. Those were produced by a DIFFERENT
    construction — a window truncated at the boundary, rather than the full
    window partitioned by ``namespace_for_position`` — so agreement is evidence
    that the two constructions select the same in-sample population, which is
    the assumption this wiring rests on.

``--writer``
    Exercises the REAL writer against the REAL table: stores a split on an
    existing in-sample result, reads it back, compares it field by field, and
    confirms ``sql/269``'s trigger refuses the same split on a hold-out result.

⚠⚠ ``--writer`` WRITES NOTHING THAT SURVIVES. Every statement runs inside a
transaction the script rolls back, and it asserts the fold count is back to its
starting value afterwards. It is the only way to exercise the write path today:
``assert_no_existing_results`` refuses a re-run of the job while the 24 rows
from ``2eb45c62`` stand, and there is no repair path that would attach folds to
rows already written (see the PR's residual-risk note).

Gate on the EXIT CODE. 0 means every property held; 1 means at least one did
not, and every violation is printed with the numbers that produced it.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date

import psycopg

from app.config import settings
from app.services.backtest_run import BACKTEST_UNIVERSE as UNIVERSE
from app.services.backtest_run import (
    QUARANTINE_ARM_ORDER,
    build_in_sample_split,
    evaluate_arm,
    load_corpus,
    runnable_strategies,
)
from app.services.cost_model import COST_MODEL_ID
from app.services.result_ledger import read_walk_forward_folds, store_walk_forward_folds
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.walk_forward import FOLD_COUNT, WALK_FORWARD_MODEL_ID, WalkForwardFolds

#: §8.8's published table, produced by ``verify_2240_result_writers.py --all``
#: on the same corpus. ⚠ TRANSCRIBED, not imported — an expected value that
#: imports the thing it validates is a tautology (prevention log, 2026-08-06).
#: ⚠ The masked arm: ``verify_2240_walk_forward`` calls ``load_masked_series``
#: on its default, which is ``arm="masked"``.
PUBLISHED: dict[str, dict[str, object]] = {
    "s1-time-series-momentum": {
        "observations": 2_456_097,
        "embargo": (615, 931, 931, 931),
        "purged": (0, 597, 111, 606),
        "embargoed": (122_530, 332_214, 399_280, 0),
    },
    "s3-mean-reversion-in-trend": {
        "observations": 22_811,
        "embargo": (10, 10, 10, 10),
        "purged": (0, 20, 1, 3),
        "embargoed": (14, 2, 3, 0),
    },
}


def _report_splits(*, limit: int | None) -> list[str]:
    problems: list[str] = []
    started = time.monotonic()
    runnable, excluded = runnable_strategies()
    print("\n=== --splits : the split every in-sample row would carry ===\n")
    print(f"  runnable  {list(runnable)}")
    for entry in excluded:
        print(f"  EXCLUDED  {entry.strategy_id} — {entry.reason.splitlines()[0]}")

    with psycopg.connect(settings.database_url) as conn:
        corpus = load_corpus(conn, limit=limit)
        # ⚠ Reaching here at all is the prefix invariant holding: `load_corpus`
        # raises when the in-sample axis is not `axis`'s pre-boundary prefix.
        print(
            f"\n  evaluation axis  {len(corpus.axis):,} dates"
            f"\n  in-sample axis   {len(corpus.in_sample_axis):,} dates, "
            f"{sum(corpus.in_sample_bar_counts):,} bars   (prefix invariant HELD)"
            f"\n  series           {len(corpus.pairs):,}",
            flush=True,
        )
        if limit is not None:
            print(f"  ⚠ LIMITED to {limit} series — NOT a full-population figure")

        for strategy_id in runnable:
            entry = STRATEGY_MANIFEST[strategy_id]
            identity = entry.identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)
            for arm in QUARANTINE_ARM_ORDER:
                elapsed = time.monotonic()
                measurement = evaluate_arm(
                    conn,
                    entry,
                    corpus=corpus,
                    quarantine_arm=arm,
                    # One conservative population is enough to verify fold
                    # wiring; the production runner measures both S-4 bounds.
                    ambiguity_arm="worst_case" if entry.exit_levels is not None else None,
                    identity=identity,
                    namespaces=("in_sample",),
                )
                outcome = measurement.namespaces.get("in_sample")
                if outcome is None:
                    problems.append(f"{strategy_id}/{arm}: produced no in-sample measurement")
                    continue
                split = build_in_sample_split(
                    outcome.label_starts,
                    outcome.label_ends,
                    axis=corpus.in_sample_axis,
                    bar_counts=corpus.in_sample_bar_counts,
                )
                # ⚠ ``flush=True`` on every progress line. A full pass is tens
                # of minutes and stdout is block-buffered when redirected to a
                # file, so an unflushed run leaves a 0-byte output file while it
                # is perfectly healthy — the prevention-log lesson that cost
                # seven minutes diagnosing a stall that did not exist.
                print(
                    f"\n  [{strategy_id} / {arm}]  {time.monotonic() - elapsed:.1f}s"
                    f"\n      in-sample positions   {outcome.position_count:>12,}"
                    f"\n      label windows (curve) {split.observation_count:>12,}"
                    f"      (the census population; the gap is §3.4's uncosted exclusions)",
                    flush=True,
                )
                for record in split.folds:
                    print(
                        f"      fold {record.fold.index}  {record.first_date} … {record.last_date}  "
                        f"idx {record.fold.first_index:>6,}-{record.fold.last_index:<6,} "
                        f"bars {record.bar_count:>10,}  embargo {record.embargo_bars:>5,}  "
                        f"test {record.census.test:>9,} train {record.census.train:>9,} "
                        f"purged {record.census.purged:>6,} embargoed {record.census.embargoed:>9,}"
                    )
                    # F1 — conservation. Every fold classifies every observation.
                    if record.census.total != split.observation_count:
                        problems.append(
                            f"{strategy_id}/{arm} fold {record.fold.index}: census totals "
                            f"{record.census.total:,} against {split.observation_count:,} observations"
                        )
                if split.model_id != WALK_FORWARD_MODEL_ID:
                    problems.append(f"{strategy_id}/{arm}: split labelled {split.model_id}")
                if len(split.folds) != FOLD_COUNT:
                    problems.append(f"{strategy_id}/{arm}: {len(split.folds)} folds")
                problems.extend(_cross_check(strategy_id, arm, split))

    print(f"\n  splits elapsed  {time.monotonic() - started:.1f}s")
    return problems


def _cross_check(strategy_id: str, arm: str, split: WalkForwardFolds) -> list[str]:
    """The masked arm against §8.8's independently-produced table.

    ⚠ A DIFFERENCE IS A FINDING, NOT A TOLERANCE. The two constructions select
    the in-sample population differently — this job partitions the full window
    by ``namespace_for_position``, the published run truncated the window at the
    boundary — so equality is the evidence that they agree. A mismatch means one
    of them admits a position the other does not, and every fold census here
    would then be counting a different population than the table it is compared
    against.
    """
    if arm != "masked" or strategy_id not in PUBLISHED:
        return []
    expected = PUBLISHED[strategy_id]
    problems: list[str] = []
    actual = {
        "observations": split.observation_count,
        "embargo": tuple(record.embargo_bars for record in split.folds),
        "purged": tuple(record.census.purged for record in split.folds),
        "embargoed": tuple(record.census.embargoed for record in split.folds),
    }
    for key, want in expected.items():
        got = actual[key]
        verdict = "OK " if got == want else "*** MISMATCH"
        print(f"      cross-check §8.8 {key:<13} published {want}  measured {got}   {verdict}")
        if got != want:
            problems.append(f"{strategy_id}/{arm}: {key} measured {got} against §8.8's published {want}")
    return problems


#: ⚠ A result that has NO folds yet, preferred over simply the first one.
#: Once the patched job has run, every in-sample result carries a split, and
#: storing a second one collides on `(result_id, fold_index)` — the arm would
#: then abort on a primary key before reaching any of its checks. The
#: `ORDER BY` puts fold-free results first and falls back to the lowest id, and
#: `_report_writer` clears that result's folds INSIDE the rolled-back
#: transaction when the fallback is what it got.
_IN_SAMPLE_RESULT = """
    SELECT r.result_id, EXISTS (SELECT 1 FROM strategy_result_folds f WHERE f.result_id = r.result_id)
    FROM strategy_results_store r
    WHERE r.namespace = 'in_sample'
    ORDER BY 2, r.result_id
    LIMIT 1
"""
_HOLDOUT_RESULT = """
    SELECT result_id FROM strategy_results_store
    WHERE namespace = 'hold_out' ORDER BY result_id LIMIT 1
"""


def _report_writer() -> list[str]:
    """The real writer, the real trigger, the real read-back — all rolled back."""
    problems: list[str] = []
    print("\n=== --writer : sql/269 exercised against the live table, then rolled back ===\n")
    # A split whose numbers do not matter: this arm tests the WRITE, and the
    # numbers are what --splits tests. Cut over a synthetic axis so the arm
    # costs nothing.
    axis_len = 400
    split = build_in_sample_split(
        [0, 40, 120, 300],
        [30, 90, 200, 360],
        axis=tuple(date.fromordinal(730000 + n) for n in range(axis_len)),
        bar_counts=(1,) * axis_len,
    )
    with psycopg.connect(settings.database_url) as conn:
        before = conn.execute("SELECT count(*) FROM strategy_result_folds").fetchone()
        assert before is not None
        print(f"  strategy_result_folds before  {before[0]:,}")
        in_sample = conn.execute(_IN_SAMPLE_RESULT).fetchone()
        hold_out = conn.execute(_HOLDOUT_RESULT).fetchone()
        if in_sample is None or hold_out is None:
            return ["no stored result rows to attach a split to — run the backtest job first"]

        try:
            with conn.transaction():
                if in_sample[1]:
                    # ⚠ Inside the transaction that is about to be rolled back,
                    # so the real split is restored on the way out — the
                    # `count(*)` below is what proves it.
                    conn.execute(
                        "DELETE FROM strategy_result_folds WHERE result_id = %(id)s", {"id": int(in_sample[0])}
                    )
                    print(f"  result {in_sample[0]} already carries a split — cleared inside the rolled-back txn")
                written = store_walk_forward_folds(conn, int(in_sample[0]), split)
                print(f"  stored on in-sample result {in_sample[0]}: {written} fold(s)")
                if written != FOLD_COUNT:
                    problems.append(f"the writer reported {written} folds against {FOLD_COUNT}")

                read_back = read_walk_forward_folds(conn, int(in_sample[0]))
                if read_back != split:
                    problems.append("the split read back is not the split written")
                else:
                    print("  read back through read_walk_forward_folds: IDENTICAL")

                # ⚠ sql/269's trigger — the invariant `_assert_every_runnable_
                # produced_rows` mirrors in Python. A fold row on a hold-out
                # result claims a cross-validation of the withheld side.
                refused = False
                try:
                    with conn.transaction():
                        store_walk_forward_folds(conn, int(hold_out[0]), split)
                except psycopg.Error as exc:
                    # ⚠ THE MESSAGE IS CHECKED, NOT MERELY THE RAISE. Any
                    # database error would satisfy "it refused" — a dead
                    # connection would read as the invariant holding. The
                    # trigger surfaces as IntegrityConstraintViolation rather
                    # than RaiseException, so the class alone is not the signal
                    # either; the wording is what identifies WHICH rule fired.
                    refused = "IN-SAMPLE" in str(exc)
                    print(f"  hold-out result {hold_out[0]} refused with: {str(exc).splitlines()[0]}")
                    if not refused:
                        problems.append(f"the hold-out write failed for a reason other than sql/269's trigger: {exc}")
                if not refused:
                    problems.append(f"the trigger accepted a fold row on hold-out result {hold_out[0]}")
                # ⚠ THE ONLY WAY OUT THAT LEAVES NOTHING BEHIND. psycopg3's
                # ``Transaction`` has no ``rollback()``; raising ``Rollback``
                # inside the block is the documented exit, and the ``count(*)``
                # below is what proves it worked rather than assuming it.
                raise psycopg.Rollback
        except psycopg.Rollback:
            pass

        after = conn.execute("SELECT count(*) FROM strategy_result_folds").fetchone()
        assert after is not None
        print(f"  strategy_result_folds after   {after[0]:,}   (rolled back)")
        if after[0] != before[0]:
            problems.append(f"the writer arm left {after[0] - before[0]} row(s) behind")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--splits", action="store_true")
    parser.add_argument("--writer", action="store_true")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--limit", type=int, default=None, help="smoke only — NOT a full-population figure")
    args = parser.parse_args()
    if not (args.splits or args.writer or args.all):
        parser.error("choose --splits, --writer or --all")

    problems: list[str] = []
    if args.writer or args.all:
        problems.extend(_report_writer())
    if args.splits or args.all:
        problems.extend(_report_splits(limit=args.limit))

    print("\n" + "=" * 78)
    if problems:
        print(f"{len(problems)} PROPERTY VIOLATION(S):")
        for problem in problems:
            print(f"  *** {problem}")
        return 1
    print("every property held")
    return 0


if __name__ == "__main__":
    sys.exit(main())
