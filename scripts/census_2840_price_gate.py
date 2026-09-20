"""Census for #2840 arm 2 — can a cheapest-cost-band price gate be measured at all?

Read-only. Counts BARS, SERIES and DATES. No strategy is evaluated and no outcome
is read, so this is the same class of pre-declaration fact as arm 1's regime-day
priors in ``scripts/freeze_2840_sh_regime_gate_declaration.py``.

Three questions, and the third is the one a reader is most likely to assume:

1. **Supply** — how much of the in-sample corpus sits in the cheapest band, by bars,
   by distinct series and by distinct dates. The last of those is the unit arm 1
   died on (a market-wide gate left 104 decision dates); this gate is per-name, so
   the three are expected to disagree and all three are printed.
2. **Era mix** — by decade, because the band table is a 2026 snapshot applied to
   1962 prices and the gate's selectivity is therefore not constant over the span.
3. **Proxy miss, BOTH directions** — the gate reads ``close(t)`` because that is
   what is knowable at the decision, while the charged band keys on the entry FILL,
   which ``signal_ledger.resolve_fills`` puts at the next bar's OPEN. So the gate
   admits some legs that are charged a dearer band (``close >= edge``, next
   ``open < edge``) and rejects some that would have been charged the cheapest one
   (``close < edge``, next ``open >= edge``). Measuring only the first direction
   describes the cost leak and hides the selection effect.

⚠ The edge is READ from ``cost_model``, never retyped, and it is the CHEAPEST band
rather than ``BANDS[-1]``: the hypothesis is about what is charged. They are the
same band today and the module asserts that they still are.

⚠ The in-sample boundary is ``strategy_result.HOLDOUT_BOUNDARY`` and the split is
``<``. In-sample already IS pre-2022; no date argument is accepted.

Refs #2840, #2832, #3238.
"""

from __future__ import annotations

import argparse

import psycopg

from app.config import settings
from app.services.backtest_run import EVALUATION_WINDOW_START
from app.services.cost_model import BANDS
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_result import HOLDOUT_BOUNDARY
from app.services.universe_selection import load_universe_selection, vendor_for

#: The band the gate selects — cheapest CHARGED, not highest priced.
CHEAPEST_BAND = min(BANDS, key=lambda band: band.p75_spread_pct)

#: ``survivorship_free`` is ``backtest_run.BACKTEST_UNIVERSE`` and the only universe
#: whose pinned archive stores as-traded prices. ``survivor_only`` is deliberately
#: NOT censused: its prices are split-adjusted, so a numeric threshold over them is
#: not a nominal band and the counts would invite exactly that misreading.
UNIVERSE = "survivorship_free"

#: Mirrors ``_OPPORTUNITY_SERIES_SQL``'s usability filter so the census counts bars
#: a run could act on. ⚠ Both OHLC legs are required: the gate reads ``close`` and
#: the fill reads the next ``open``, so a bar with only one of them is usable for
#: neither question.
_USABLE = """
      AND d.close > 0 AND d.open > 0
      AND d.close::text NOT IN ('NaN', 'Infinity', '-Infinity')
      AND d.open::text NOT IN ('NaN', 'Infinity', '-Infinity')
"""

_SUPPLY_SQL = f"""
    SELECT count(*),
           count(*) FILTER (WHERE d.close >= %(edge)s),
           count(DISTINCT d.series_id),
           count(DISTINCT d.series_id) FILTER (WHERE d.close >= %(edge)s),
           count(DISTINCT d.bar_date),
           count(DISTINCT d.bar_date) FILTER (WHERE d.close >= %(edge)s),
           min(d.bar_date), max(d.bar_date)
    FROM research_price_daily d
    WHERE d.series_id = ANY(%(ids)s)
      AND d.bar_date >= %(start)s AND d.bar_date < %(boundary)s
      {_USABLE}
"""

_DECADE_SQL = f"""
    SELECT (extract(year from d.bar_date)::int / 10) * 10,
           count(*),
           count(*) FILTER (WHERE d.close >= %(edge)s),
           count(DISTINCT d.series_id) FILTER (WHERE d.close >= %(edge)s)
    FROM research_price_daily d
    WHERE d.series_id = ANY(%(ids)s)
      AND d.bar_date >= %(start)s AND d.bar_date < %(boundary)s
      {_USABLE}
    GROUP BY 1 ORDER BY 1
"""

#: ⚠ The pair is the bar and its SUCCESSOR IN THE SERIES, which is what the fill
#: model uses — not "the next calendar day". A series with a hole therefore pairs
#: across the hole, exactly as a run would.
_PROXY_SQL = f"""
    WITH paired AS (
        SELECT d.close AS decision_close,
               lead(d.open) OVER (PARTITION BY d.series_id ORDER BY d.bar_date) AS fill_open
        FROM research_price_daily d
        WHERE d.series_id = ANY(%(ids)s)
          AND d.bar_date >= %(start)s AND d.bar_date < %(boundary)s
          {_USABLE}
    )
    SELECT count(*),
           count(*) FILTER (WHERE decision_close >= %(edge)s),
           count(*) FILTER (WHERE decision_close >= %(edge)s AND fill_open < %(edge)s),
           count(*) FILTER (WHERE decision_close < %(edge)s AND fill_open >= %(edge)s)
    FROM paired
    WHERE fill_open IS NOT NULL
"""

#: Admitted series holding no usable in-sample bar at all. Printed because the
#: supply query's series denominator is the series it SEES, and quoting a share of
#: that without saying what it excludes is the defect the instruction set names.
_ABSENT_SQL = f"""
    SELECT count(*) FROM unnest(%(ids)s::bigint[]) AS s(series_id)
    WHERE NOT EXISTS (
        SELECT 1 FROM research_price_daily d
        WHERE d.series_id = s.series_id
          AND d.bar_date >= %(start)s AND d.bar_date < %(boundary)s
          {_USABLE}
    )
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    # ⚠ Default resolved at USE. The DSN carries a password and an argparse
    # default is one `ArgumentDefaultsHelpFormatter` away from being printed.
    parser.add_argument("--database-url", default=None)
    args = parser.parse_args()

    edge = CHEAPEST_BAND.lower
    if edge is None or CHEAPEST_BAND.upper is not None:
        raise RuntimeError(
            f"cheapest band {CHEAPEST_BAND.label} is not the open-above band; a `>=` gate no longer expresses it"
        )
    print(
        f"cheapest charged band {CHEAPEST_BAND.label}: round trip {CHEAPEST_BAND.p75_spread_pct}% "
        f"(half {CHEAPEST_BAND.half_spread_pct}%), n={CHEAPEST_BAND.sample_size}, edge {edge}"
    )
    print(f"in-sample span {EVALUATION_WINDOW_START} .. < {HOLDOUT_BOUNDARY}   universe {UNIVERSE}")

    with psycopg.connect(args.database_url or settings.database_url) as conn:
        conn.execute("SET statement_timeout = '1800s'")
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        validated = load_validated_universe(conn)
        selection = load_universe_selection(conn, universe=UNIVERSE, validated_ids=frozenset(validated))
        ids = [series.series_id for series in selection.admitted]
        print(f"pinned vendor {vendor_for(UNIVERSE)}   admitted series {len(ids):,}")
        params = {"ids": ids, "start": EVALUATION_WINDOW_START, "boundary": HOLDOUT_BOUNDARY, "edge": edge}

        row = conn.execute(_SUPPLY_SQL, params).fetchone()
        assert row is not None
        bars, gated_bars, series, gated_series, dates, gated_dates, first, last = row
        absent = conn.execute(_ABSENT_SQL, params).fetchone()
        assert absent is not None
        print(f"\nspan observed {first} .. {last}")
        print(f"  bars    {bars:>12,}  gated {gated_bars:>12,}  {100 * gated_bars / bars:6.2f}%")
        print(f"  series  {series:>12,}  gated {gated_series:>12,}  {100 * gated_series / series:6.2f}%")
        print(f"  dates   {dates:>12,}  gated {gated_dates:>12,}  {100 * gated_dates / dates:6.2f}%")
        print(f"  ⚠ {int(absent[0]):,} of {len(ids):,} admitted series hold NO usable in-sample bar and are")
        print("    absent from the series denominator above, not counted as ungated.")

        print("\ndecade        bars       gated_bars    gated%   gated_series")
        for decade, dbars, dgated, dseries in conn.execute(_DECADE_SQL, params).fetchall():
            print(f"  {decade}  {dbars:>12,}  {dgated:>12,}  {100 * dgated / dbars:6.2f}%  {dseries:>8,}")

        proxy = conn.execute(_PROXY_SQL, params).fetchone()
        assert proxy is not None
        pairs, gated_pairs, leaks, misses = (int(value) for value in proxy)
        print(f"\nproxy miss on {pairs:,} decision/fill pairs (gate reads close, band keys on the next open)")
        print(
            f"  admitted but charged dearer   {leaks:>12,}  "
            f"{100 * leaks / gated_pairs if gated_pairs else 0:6.2f}% of admitted"
        )
        print(
            f"  rejected though it was cheap  {misses:>12,}  "
            f"{100 * misses / (pairs - gated_pairs) if pairs > gated_pairs else 0:6.2f}% of rejected"
        )
        conn.rollback()


if __name__ == "__main__":
    main()
