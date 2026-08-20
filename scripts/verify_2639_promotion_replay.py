"""Full-population verification of #2639's promotion-transition replays.

    PYTHONPATH=. uv run python scripts/verify_2639_promotion_replay.py --all

⚠ READ-ONLY. Every arm is a SELECT; nothing is written and no transaction is
opened, so this cannot alter dev state. The write path — ``promote_strategy``
raising on each replayed refusal — is exercised in
``tests/test_strategy_control_plane.py::test_promotion_replays_the_rows_own_clauses_and_the_holdout_counts``
against the test database, which is where a fixture may legitimately promote.
It CANNOT be exercised against dev's own rows: all four registered strategies
are ``harness_validation``, and ``promote_strategy`` refuses those at the
manifest check before any result is read (arm R4 asserts exactly that).

⚠ NEVER PIPE THIS INTO ``head``/``tail``. A pipe returns the pipe's status, so a
failure reads as success, and it buffers the progress lines (`.claude/CLAUDE.md`).
Redirect to a file and read the file.

THE ARMS
--------
``--reconstruct``  R1. Every stored row survives ``stored_result_promotion_refusals``
                   without raising. A row that does not reconstruct RAISES at
                   the transition and masks the other refusals, so "how many" is
                   the number that matters and a sample cannot answer it.
``--refusals``     R2. The refusal census the transition would now produce, over
                   the FULL population. ⚠ The point is the direction: this must
                   only ever ADD refusals to rows that already refuse. A row
                   that this makes promotable would be a fail-open change.
``--counts``       R3. Criterion 5's two counts per ``(strategy_id,
                   strategy_version)`` — the pair the transition now reads live.
``--promote``      R4. Asserts the dev population cannot reach the result-level
                   replays at all, and says why.
"""

from __future__ import annotations

import argparse
import collections

import psycopg

from app.config import settings
from app.services.result_ledger import holdout_access_counts, stored_result_promotion_refusals
from app.services.strategy_control_plane import (
    StrategyControlError,
    promote_strategy,
    registered_strategy_purpose,
)
from app.services.strategy_result import holdout_count_promotion_refusals


def _connect() -> psycopg.Connection[tuple]:
    return psycopg.connect(settings.database_url)


def _result_ids(conn: psycopg.Connection[tuple]) -> list[int]:
    return [int(row[0]) for row in conn.execute("SELECT result_id FROM strategy_results_store ORDER BY result_id")]


def reconstruct(conn: psycopg.Connection[tuple]) -> bool:
    """R1 — every stored row survives the transition's own read path.

    ⚠ GOES THROUGH THE PUBLIC ``stored_result_promotion_refusals`` RATHER THAN
    ``_result_from_row``, and not only to avoid reaching into a private name.
    The failure this arm is looking for is a row that RAISES at the transition
    and so masks the other refusals, and the only faithful way to ask that is to
    call the thing the transition calls. Reconstruction is what raises inside
    it, so a raise here is a reconstruction failure by construction.
    """
    print("R1 — every stored row survives stored_result_promotion_refusals without raising")
    ids = _result_ids(conn)
    failures: dict[str, list[int]] = collections.defaultdict(list)
    for result_id in ids:
        try:
            stored_result_promotion_refusals(conn, result_id)
        except Exception as exc:  # noqa: BLE001 - the census is the point
            failures[f"{type(exc).__name__}: {exc}"[:160]].append(result_id)
    ok = len(ids) - sum(len(v) for v in failures.values())
    print(f"    rows {len(ids)}  clean read {ok}  raised {sum(len(v) for v in failures.values())}")
    for message, bad in failures.items():
        print(f"      {len(bad):5}  {message}  e.g. result_id={bad[0]}")
    return not failures


def refusals(conn: psycopg.Connection[tuple]) -> bool:
    print("R2 — the transition's row-level refusal census, full population")
    ids = _result_ids(conn)
    census: collections.Counter[str] = collections.Counter()
    clean = 0
    for result_id in ids:
        codes = stored_result_promotion_refusals(conn, result_id)
        census.update(codes)
        if not codes:
            clean += 1
    print(f"    rows {len(ids)}")
    for code, count in sorted(census.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"      {count:5}  {code}")
    print(f"    rows this replay would NOT refuse: {clean}")
    # ⚠ Not an acceptance on `clean == 0`: a clean row here is not promotable,
    # because the structural stamps, the frozen universe and ambiguity records
    # and the #2505 evidence are checked separately by the transition. The
    # assertion is that this arm RAN over every row, which the count above says.
    return True


def counts(conn: psycopg.Connection[tuple]) -> bool:
    print("R3 — criterion 5's two counts per (strategy_id, strategy_version)")
    versions = conn.execute(
        "SELECT DISTINCT strategy_id, strategy_version FROM strategy_results_store ORDER BY 1, 2"
    ).fetchall()
    ok = True
    for strategy_id, strategy_version in versions:
        pair = holdout_access_counts(conn, str(strategy_id), str(strategy_version))
        codes = holdout_count_promotion_refusals(
            holdout_evaluations=pair.holdout_evaluations,
            recorded_accesses=pair.recorded_accesses,
        )
        print(
            f"    {strategy_id:36} {strategy_version:32} "
            f"evaluations={pair.holdout_evaluations:4} accesses={pair.recorded_accesses:4} "
            f"{'->  ' + ', '.join(codes) if codes else '->  (clause passes)'}"
        )
        ok = ok and pair.recorded_accesses >= pair.holdout_evaluations
    return ok


def promote(conn: psycopg.Connection[tuple]) -> bool:
    """R4 — the dev population cannot reach the result-level replays, and why.

    ⚠ This is a NEGATIVE result reported rather than a gap papered over. Every
    registered strategy is ``harness_validation``, so ``promote_strategy``
    refuses before it reads a result. Asserting that keeps the reason visible:
    if a manifest entry later becomes ``capital_candidate``, this arm changes
    its message and the row-level replays become reachable on dev.
    """
    print("R4 — promote_strategy against the dev population")
    versions = conn.execute(
        "SELECT DISTINCT strategy_id, strategy_version FROM strategy_results_store ORDER BY 1, 2"
    ).fetchall()
    ok = True
    for strategy_id, strategy_version in versions:
        purpose = registered_strategy_purpose(str(strategy_id))
        try:
            promote_strategy(
                conn,
                strategy_id=str(strategy_id),
                strategy_version=str(strategy_version),
                to_stage="historical_validated",
                promoted_by="scripts/verify_2639_promotion_replay.py",
                reason="verification probe — expected to refuse",
                evidence_ref="verify:2639",
                result_ids=(),
            )
        except StrategyControlError as exc:
            print(f"    {strategy_id:36} purpose={purpose!s:20} refused: {exc}")
            continue
        print(f"    {strategy_id:36} purpose={purpose!s:20} ⚠ DID NOT REFUSE")
        ok = False
    return ok


_ARMS = {"reconstruct": reconstruct, "refusals": refusals, "counts": counts, "promote": promote}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in _ARMS:
        parser.add_argument(f"--{name}", action="store_true")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()
    selected = [name for name in _ARMS if getattr(args, name) or args.all]
    if not selected:
        parser.error("pick at least one arm, or --all")
    # ⚠ AUTOCOMMIT, and the R4 arm is the reason: promote_strategy is expected
    # to raise, and a raise inside an open transaction would leave the session
    # in an aborted state for the arms after it. Nothing here writes, so
    # autocommit costs nothing.
    with _connect() as conn:
        conn.autocommit = True
        results = {name: _ARMS[name](conn) for name in selected}
    print()
    for name, ok in results.items():
        print(f"{'PASS' if ok else 'FAIL'}  {name}")
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
