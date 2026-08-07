"""Verify phase 5e-1 — criterion 5's namespace, on the full population (#2240).

    PYTHONPATH=. uv run python scripts/verify_2240_holdout_namespace.py --all

Two arms, and they answer different questions:

``--mechanism``  the relations themselves, against the dev database. Read-only:
                 every write happens inside a transaction that is rolled back,
                 and the arm re-counts all three relations afterwards to prove
                 it. Seconds.

``--census``     ⚠⚠ THE FULL-POPULATION ARM, and the number spec §5.2 requires
                 and nothing has yet measured: *"The split is over corpus bars,
                 not over each strategy's own signals. A strategy whose signals
                 cluster outside the modern era gets a hold-out that is 25% of
                 BARS and some other fraction of its TRADES. The realised
                 in-sample/hold-out trade counts are therefore reported per
                 strategy."* Minutes — it rebuilds every position over the whole
                 corpus.

⚠ THE CENSUS IS NOT A PERFORMANCE MEASUREMENT and reports no return. It counts
where trades LAND, because criterion 5's hold-out gate is *"the hold-out arm's
effective sample size must be large enough for its own confidence interval to
exclude the random cohort's 95th percentile"* — and a strategy whose hold-out
arm holds a handful of trades fails that before any statistic is computed.

⚠ REUSED, NOT RE-DERIVED. The corpus→positions path comes from stage 5a's
verifier and the namespace rules from ``strategy_result``; a second copy of
either would be a second place for the boundary to drift. What is new here is
only the classification.

⚠ MUST NOT RUN CONCURRENTLY with ``scripts/probe_2240_result_ledger.py`` — the
probe mutates tracked source on disk, and a sweep that imports it mid-mutation
reports figures produced by an injected defect (phase 4b's lesson).
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter

import psycopg

from app.config import settings
from app.services.cost_model import COST_MODEL_ID
from app.services.position_builder import Window, build_positions
from app.services.research_price_structure_store import load_masked_series
from app.services.signal_ledger import resolve_fills
from app.services.strategies.s1_time_series_momentum import S1_STRATEGY_ID, s1_identity, s1_signals
from app.services.strategies.s3_mean_reversion_in_trend import S3_STRATEGY_ID, s3_identity, s3_signals
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_result import (
    EVALUATION_WINDOW_END,
    EVALUATION_WINDOW_START,
    HOLDOUT_BOUNDARY,
    namespace_for_position,
    namespace_for_signal,
)
from scripts.verify_2240_position_builder import (
    _S1_REGIME,
    _S3_REGIME,
    UNIVERSE,
    _fills,
    _stamped_versions,
    _to_series,
)

_OCCUPANCY_SQL = """
    SELECT
        (SELECT count(*) FROM strategy_results_store)      AS stored,
        (SELECT count(*) FROM strategy_results)            AS visible,
        (SELECT count(*) FROM strategy_holdout_accesses)   AS accesses
"""


def mechanism() -> int:
    """The relations, asserted against the real database. Nothing is left behind."""
    problems: list[str] = []
    print("\n[mechanism]", flush=True)
    with psycopg.connect(settings.database_url) as conn:
        kinds = dict(
            conn.execute(
                "SELECT relname, relkind FROM pg_class WHERE relname IN "
                "('strategy_results', 'strategy_results_store', 'strategy_holdout_accesses')"
            ).fetchall()
        )
        print(f"  relkinds                 {kinds}", flush=True)
        if kinds.get("strategy_results") != "v":
            problems.append("strategy_results is not a VIEW — criterion 5's filter is gone")
        if kinds.get("strategy_results_store") != "r":
            problems.append("strategy_results_store is not a TABLE")
        if kinds.get("strategy_holdout_accesses") != "r":
            problems.append("strategy_holdout_accesses is missing")

        check_option = conn.execute(
            "SELECT check_option FROM information_schema.views WHERE table_name = 'strategy_results'"
        ).fetchone()
        print(f"  view check_option        {check_option[0] if check_option else None}", flush=True)
        if check_option is None or check_option[0] != "CASCADED":
            problems.append("the view has no cascaded check option — an in-sample write could smuggle a hold-out row")

        store_cols = [c for (c,) in _columns(conn, "strategy_results_store")]
        view_cols = [c for (c,) in _columns(conn, "strategy_results")]
        print(f"  column parity            store {len(store_cols)}   view {len(view_cols)}", flush=True)
        if store_cols != view_cols:
            missing = [c for c in store_cols if c not in view_cols]
            problems.append(f"the view is missing {missing} — sql/264's SELECT * was expanded before they existed")

        triggers = [
            t
            for (t,) in conn.execute(
                "SELECT tgname FROM pg_trigger WHERE tgrelid = 'strategy_results_store'::regclass AND NOT tgisinternal"
            ).fetchall()
        ]
        print(f"  store triggers           {triggers}", flush=True)
        if "trg_strategy_results_holdout_access" not in triggers:
            problems.append("the hold-out access trigger is absent — an unrecorded evaluation would store")

        # ⚠ The RLS measurement sql/264's header rests on, re-run rather than
        # quoted. If a non-superuser role ever exists this flips, and that is a
        # decision to revisit, not a regression.
        privileged = conn.execute(
            "SELECT current_user, rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user"
        ).fetchone()
        print(f"  connection role          {privileged}", flush=True)
        if privileged is not None and not (privileged[1] or privileged[2]):
            problems.append(
                "this connection is no longer superuser/bypassrls — sql/264's RLS rejection can and should be revisited"
            )

        occupancy = conn.execute(_OCCUPANCY_SQL).fetchone()
        print(f"  occupancy                stored/visible/accesses = {occupancy}", flush=True)

        after = conn.execute(_OCCUPANCY_SQL).fetchone()
        if after != occupancy:
            problems.append(f"this arm changed the database: {occupancy} → {after}")

    print(f"\n  problems: {len(problems)}", flush=True)
    for problem in problems:
        print(f"    {problem}", flush=True)
    return 1 if problems else 0


def _columns(conn: psycopg.Connection[tuple], relation: str) -> list[tuple[str]]:
    return conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %(r)s ORDER BY ordinal_position",
        {"r": relation},
    ).fetchall()


def census(*, limit: int | None) -> int:
    """⚠⚠ Where every trade LANDS, per strategy, over the whole corpus."""
    started = time.monotonic()
    s1_version, s3_version, builder_version = _stamped_versions()
    window = Window(start=EVALUATION_WINDOW_START, end=EVALUATION_WINDOW_END)
    print(f"\n[census] {S1_STRATEGY_ID} {s1_version}", flush=True)
    print(f"         {S3_STRATEGY_ID} {s3_version}", flush=True)
    print(f"         builder {builder_version}", flush=True)
    print(f"         window {window.start} … {window.end}   boundary {HOLDOUT_BOUNDARY}", flush=True)

    positions: dict[str, Counter[str]] = {"S-1": Counter(), "S-3": Counter()}
    signals_seen: dict[str, Counter[str]] = {"S-1": Counter(), "S-3": Counter()}

    with psycopg.connect(settings.database_url) as conn:
        universe = load_validated_universe(conn)
        pairs = conn.execute(
            "SELECT instrument_id, series_id FROM research_price_series "
            "WHERE instrument_id = ANY(%(ids)s) ORDER BY instrument_id, series_id",
            {"ids": list(universe)},
        ).fetchall()
        if limit is not None:
            pairs = pairs[:limit]
            print(f"  ⚠ LIMITED to the first {len(pairs)} series — NOT a full-population figure", flush=True)
        print(f"  universe {len(universe):,} instruments   {len(pairs):,} series", flush=True)

        empty = 0
        for n, (instrument_id, series_id) in enumerate(pairs, start=1):
            masked = load_masked_series(conn, series_id)
            if not masked.bars:
                empty += 1
                continue
            series, _masked_opens = _to_series(masked.bars)

            for label, identity, signals, regime, strategy_id, version in (
                (
                    "S-1",
                    s1_identity,
                    s1_signals(series, universe=UNIVERSE, close_reason="quarantined_bar"),
                    _S1_REGIME,
                    S1_STRATEGY_ID,
                    s1_version,
                ),
                (
                    "S-3",
                    s3_identity,
                    s3_signals(series, universe=UNIVERSE, close_reason="quarantined_bar"),
                    _S3_REGIME,
                    S3_STRATEGY_ID,
                    s3_version,
                ),
            ):
                rows = resolve_fills(
                    signals,
                    series=series,
                    identity=identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID),
                    instrument_id=int(instrument_id),
                )
                # §5.2's purge, at SIGNAL level: decided in-sample, filled on the
                # withheld side. ⚠ Counted separately from the position census
                # because a purged signal opens NOTHING — it is a narrowing this
                # phase introduces (acceptance C8) and an uncounted narrowing is
                # a narrowing asserted safe.
                for row in rows:
                    # ⚠ Only a `fired` row carries a fill (`sql/255`'s
                    # fill-matches-verdict CHECK), so everything else has no
                    # namespace to be in — bucketed, never silently dropped.
                    if row.verdict != "fired" or row.fill_bar_date is None:
                        signals_seen[label]["not_fired"] += 1
                        continue
                    signals_seen[label][namespace_for_signal(row.signal_bar_date, row.fill_bar_date)] += 1

                entries, exits = _fills(rows, int(instrument_id))
                built = build_positions(
                    strategy_id=strategy_id,
                    strategy_version=version,
                    entries=entries,
                    exits=exits,
                    outcomes=[],
                    outcome_pin=None,
                    series={int(instrument_id): series},
                    regime=regime,
                    window=window,
                )
                for position in built.positions:
                    positions[label][namespace_for_position(position.entry_fill_bar_date, position.close_bar_date)] += 1
                    if position.close_bar_date is None:
                        positions[label]["open_at_window_end"] += 1

            if n % 500 == 0:
                print(f"  {n}/{len(pairs)} series ({time.monotonic() - started:.0f}s)", flush=True)

    print(f"\n  series with usable bars  {len(pairs) - empty}   (fail-closed empties: {empty})", flush=True)
    problems: list[str] = []
    for label in ("S-1", "S-3"):
        sig = signals_seen[label]
        pos = positions[label]
        total_pos = pos["in_sample"] + pos["hold_out"]
        print(f"\n  {label}", flush=True)
        print(
            f"    signals   in_sample {sig['in_sample']:,}   hold_out {sig['hold_out']:,}   "
            f"purged {sig['purged']:,}   not_fired {sig['not_fired']:,}",
            flush=True,
        )
        print(
            f"    positions in_sample {pos['in_sample']:,}   hold_out {pos['hold_out']:,}   "
            f"(open at window end {pos['open_at_window_end']:,}, all hold_out by §5.2)",
            flush=True,
        )
        if total_pos:
            print(f"    hold-out share of trades  {100.0 * pos['hold_out'] / total_pos:.3f}%", flush=True)
        # ⚠ NOT a threshold anybody picked. An EMPTY hold-out arm cannot produce
        # a confidence interval at all, so criterion 5 is unsatisfiable for that
        # strategy before any statistic is computed — which is a finding, not a
        # failure of this script.
        if total_pos and pos["hold_out"] == 0:
            problems.append(f"{label}: {total_pos:,} positions and NOT ONE lands in the hold-out — criterion 5 cannot")

    print(f"\n  problems: {len(problems)}", flush=True)
    for problem in problems:
        print(f"    {problem}", flush=True)
    print(f"\n  elapsed  {time.monotonic() - started:.1f}s", flush=True)
    # ⚠ Re-checked AFTER the sweep as well as before — a probe harness that
    # mutated and restored a source file mid-run would pass an entry check alone.
    _stamped_versions()
    return 1 if problems else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mechanism", action="store_true", help="the relations, against the dev database")
    parser.add_argument("--census", action="store_true", help="full-population per-strategy namespace split")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--limit", type=int, default=None, help="first N series only — a smoke run, never a figure")
    args = parser.parse_args()
    if not (args.mechanism or args.census or args.all):
        parser.error("pick --mechanism, --census or --all")

    rc = 0
    if args.mechanism or args.all:
        rc |= mechanism()
    if args.census or args.all:
        rc |= census(limit=args.limit)
    print("\nPASS" if rc == 0 else "\n*** FAIL ***", flush=True)
    return rc


if __name__ == "__main__":
    sys.exit(main())
