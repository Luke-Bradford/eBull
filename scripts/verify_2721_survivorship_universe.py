"""#2721 step 3 acceptance — the universe selection, measured on the dev DB.

Two assertions and a census, all FULL-POPULATION (no samples):

1. ``survivor_only`` restoration: the pinned admitted set equals EXACTLY the
   legacy predicate (``instrument_id = ANY(validated)``) restricted to the
   survivor vendor, and contains zero Intrader series. The pin is a
   restoration, not a change — asserted as a set comparison, never a count.
2. ``survivorship_free``: the capture date assertion holds, the strata
   reconcile exactly to the vendor's series total, and the termination-class
   split is printed at the register state of this run (the 2013-2021 re-link
   moves it; the acceptance pins THIS run's census, not a lower bound).

Read-only. Run: ``uv run python -m scripts.verify_2721_survivorship_universe``

``--smoke-run N`` additionally executes a LIMITED survivorship-free backtest
(one non-level strategy, N series, in-sample only) inside a transaction that is
ROLLED BACK — the ``verify_2429_total_return`` pattern: the measurement never
charges the result store or the trial register. ⚠ A limited pass is a harness
smoke, not a full-population figure, and its numbers must not be quoted as one.
"""

from __future__ import annotations

import argparse
from collections import Counter

import psycopg

from app.config import settings
from app.services.series_termination import classify_termination
from app.services.strategies.validated_universe import load_validated_universe
from app.services.universe_selection import (
    SURVIVOR_ONLY_VENDOR,
    SURVIVORSHIP_FREE_VENDOR,
    load_universe_selection,
)


def main() -> None:
    with psycopg.connect(settings.database_url) as conn:
        validated = frozenset(load_validated_universe(conn))

        # 1. survivor_only restoration, exact set comparison.
        survivor = load_universe_selection(conn, universe="survivor_only", validated_ids=validated)
        legacy = {
            (int(row[0]), int(row[1]))
            for row in conn.execute(
                "SELECT instrument_id, series_id FROM research_price_series "
                "WHERE instrument_id = ANY(%(ids)s) AND vendor = %(vendor)s AND bar_count IS NOT NULL",
                {"ids": list(validated), "vendor": SURVIVOR_ONLY_VENDOR},
            ).fetchall()
        }
        pinned = {(series.name_key, series.series_id) for series in survivor.admitted}
        assert pinned == legacy, (
            f"survivor_only pin is NOT a restoration: pinned {len(pinned)} vs legacy-on-vendor {len(legacy)}; "
            f"diff sample {sorted(pinned ^ legacy)[:5]}"
        )
        intrader_ids = {
            int(row[0])
            for row in conn.execute(
                "SELECT series_id FROM research_price_series WHERE vendor = %(vendor)s",
                {"vendor": SURVIVORSHIP_FREE_VENDOR},
            ).fetchall()
        }
        assert not ({series.series_id for series in survivor.admitted} & intrader_ids), (
            "survivor_only admits Intrader series — the vendor pin is not holding"
        )
        print(f"1. survivor_only restoration: EXACT match, {len(pinned)} series, 0 Intrader")

        # 2. survivorship_free selection + reconciliation.
        free = load_universe_selection(conn, universe="survivorship_free", validated_ids=validated)
        live = sum(1 for series in free.admitted if series.termination is None)
        terminating = sum(1 for series in free.admitted if series.termination is not None)
        reconciled = len(free.admitted) + free.unlinked_alive_excluded + free.unharvested_excluded
        assert reconciled == free.vendor_series_total, (
            f"strata do not reconcile: admitted {len(free.admitted)} + unlinked_alive "
            f"{free.unlinked_alive_excluded} + unharvested {free.unharvested_excluded} "
            f"= {reconciled} != vendor total {free.vendor_series_total}"
        )
        classes = Counter(
            classify_termination(series.termination).value for series in free.admitted if series.termination is not None
        )
        print(
            f"2. survivorship_free: capture {free.capture_date}, vendor total {free.vendor_series_total}, "
            f"admitted {len(free.admitted)} (live {live}, terminating {terminating}, "
            f"reuse-suspect {free.linked_early_reuse_suspect}), "
            f"excluded unlinked-alive {free.unlinked_alive_excluded}, unharvested {free.unharvested_excluded}"
        )
        print("   termination classes at this register state:")
        for name, count in sorted(classes.items(), key=lambda item: -item[1]):
            print(f"     {name}: {count}")
        negatives = sum(1 for series in free.admitted if series.name_key < 0)
        print(f"   synthetic name keys (unlinked): {negatives}; strata reconcile EXACTLY")


def smoke_run(limit: int) -> None:
    """A rolled-back limited survivorship-free pass: termination + census, end to end."""
    from app.services.backtest_run import run_backtest
    from app.services.strategy_result_universe import load_result_universe, load_termination_census

    with psycopg.connect(settings.database_url) as conn:
        report = run_backtest(
            conn,
            strategy_id="s1-time-series-momentum",
            universe="survivorship_free",
            limit=limit,
        )
        terminated = Counter()
        for row in report.rows:
            census = load_termination_census(conn, row.result_id)
            record = load_result_universe(conn, row.result_id)
            assert census, f"survivorship_free row {row.result_id} stored without a census"
            assert record is not None and not any(item <= 0 for item in record.evaluated_instrument_ids), (
                "a synthetic name key leaked into evaluated_instrument_ids"
            )
            assert "universe_basis_not_survivorship_free" not in row.refusals
            for stratum, count in census.items():
                if stratum.startswith("terminated_"):
                    terminated[stratum] += count
        print(
            f"smoke ({limit} series, ROLLED BACK): {len(report.rows)} rows; "
            f"terminated strata across rows: {dict(terminated) or 'none fired in this slice'}"
        )
        conn.rollback()
        print("rolled back — nothing persisted")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke-run", type=int, default=None, metavar="N")
    args = parser.parse_args()
    main()
    if args.smoke_run is not None:
        smoke_run(args.smoke_run)
