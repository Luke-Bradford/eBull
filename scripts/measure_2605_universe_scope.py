"""#2605 — measure the v1 capital-universe scope on the FULL population.

Every figure the settled-decisions entry, the promotion-contract note and the PR
description quote comes from this script, run at write time. Nothing here is
hand-written: `.claude/CLAUDE.md` forbids a derived statistic living in prose,
and the two numbers this decision rests on (how much of the tradable universe is
non-US, and how much of it the validated universe actually admits) both move with
every `sync_universe` run.

Run: `PYTHONPATH=. uv run python scripts/measure_2605_universe_scope.py`
"""

from __future__ import annotations

import sys

import psycopg

from app.config import settings
from app.db.snapshot import snapshot_read
from app.services.strategies.validated_universe import (
    US_EQUITY_ASSET_CLASS,
    load_validated_universe,
    resolve_stocks_type_id,
)

_TRADABLE_SQL = "SELECT count(*) FROM instruments WHERE is_tradable"

_BY_ASSET_CLASS_SQL = """
    SELECT e.asset_class, count(*)
    FROM instruments i
    JOIN exchanges e ON e.exchange_id = i.exchange
    WHERE i.is_tradable
    GROUP BY e.asset_class
    ORDER BY count(*) DESC
"""

#: The non-US half of the scope claim. `asset_class` lives on `exchanges`, never
#: on `instruments` — §4.0 measured that and the intuitive join is the wrong one.
_NON_US_EQUITY_SQL = """
    SELECT count(*)
    FROM instruments i
    JOIN exchanges e ON e.exchange_id = i.exchange
    WHERE i.is_tradable
      AND e.asset_class LIKE '%%equity%%'
      AND e.asset_class <> %(us_asset_class)s
"""

#: The ETF split inside `us_equity` — the reason the validated universe is
#: narrower than "US tradable".
_US_EQUITY_BY_TYPE_SQL = """
    SELECT t.description, count(*)
    FROM instruments i
    JOIN exchanges e ON e.exchange_id = i.exchange
    LEFT JOIN etoro_instrument_types t ON t.instrument_type_id = i.instrument_type_id
    WHERE i.is_tradable
      AND e.asset_class = %(us_asset_class)s
    GROUP BY t.description
    ORDER BY count(*) DESC
"""

#: The FX bound this decision hands #2363. Quote currency lives on `exchanges`
#: alongside `asset_class`, so the US-venue cut fixes it — but only for the
#: strategy path. The live execution path is wider and is #2363's to bound.
_VALIDATED_CURRENCY_SQL = """
    SELECT e.currency, count(*)
    FROM instruments i
    JOIN exchanges e ON e.exchange_id = i.exchange
    WHERE i.instrument_id = ANY(%(ids)s)
    GROUP BY e.currency
    ORDER BY count(*) DESC
"""

_TRADABLE_CURRENCY_SQL = """
    SELECT e.currency, count(*)
    FROM instruments i
    JOIN exchanges e ON e.exchange_id = i.exchange
    WHERE i.is_tradable
    GROUP BY e.currency
    ORDER BY count(*) DESC
"""

#: The venue axis is only meaningful if `us_equity` and `country = 'US'` agree.
#: A disagreeing row would mean "US-only" names two different populations.
_VENUE_COHERENCE_SQL = """
    SELECT e.country, e.asset_class, count(*)
    FROM instruments i
    JOIN exchanges e ON e.exchange_id = i.exchange
    WHERE i.is_tradable
      AND (e.asset_class = %(us_asset_class)s) <> (e.country = 'US')
    GROUP BY e.country, e.asset_class
    ORDER BY count(*) DESC
"""

#: Every stored result row's universe basis, to show the gate's live input.
_RESULT_BASIS_SQL = """
    SELECT universe_basis, carry_unmodelled, count(*)
    FROM strategy_results_store
    GROUP BY universe_basis, carry_unmodelled
    ORDER BY count(*) DESC
"""


#: The loader's join is INNER, so an instrument whose exchange row is missing
#: leaves the universe silently. A currency census over a set that quietly
#: excluded rows would read as "all USD" for the wrong reason.
_ORPHAN_EXCHANGE_SQL = """
    SELECT count(*)
    FROM instruments i
    LEFT JOIN exchanges e ON e.exchange_id = i.exchange
    WHERE i.is_tradable AND e.exchange_id IS NULL
"""


def main() -> int:
    """Print the census and FAIL on the two properties the decision asserts.

    ⚠ The exit code is the point. A script that only prints leaves "verified on
    the full population" resting on whoever last read the output — which is the
    hand-written-statistic failure in a different costume. The two claims that
    are asserted rather than displayed:

    1. the US-venue axis is coherent (``asset_class = 'us_equity'`` and
       ``country = 'US'`` name the same tradable rows), so "US-only" is one
       population and not two; and
    2. the validated universe is uniformly USD-quoted, which is the FX bound
       this decision hands #2363.

    Neither is a schema constraint — both are corpus properties that a
    ``sync_universe`` run, an exchange reclassification or a newly admitted
    ``us_equity`` venue can break while every test stays green. That is exactly
    why they are re-measured here rather than written down anywhere.

    ⚠ ONE SNAPSHOT FOR THE WHOLE REPORT (``snapshot_read``, review NITPICK on
    PR #2622). Ten statements against ``instruments``/``exchanges`` under READ
    COMMITTED would each see a different `sync_universe` state, so the currency
    census could describe a population the size line above it never counted —
    and the assertions would then be over a set that never existed. A report
    that asserts has to read one snapshot; a report that only printed could
    have got away with this.
    """
    violations: list[str] = []
    with psycopg.connect(settings.database_url) as conn, snapshot_read(conn):
        tradable = conn.execute(_TRADABLE_SQL).fetchone()
        assert tradable is not None
        print(f"tradable instruments: {tradable[0]:,}")

        print("\ntradable by exchanges.asset_class:")
        for asset_class, count in conn.execute(_BY_ASSET_CLASS_SQL).fetchall():
            print(f"  {asset_class!s:>16}  {count:>7,}")

        non_us = conn.execute(_NON_US_EQUITY_SQL, {"us_asset_class": US_EQUITY_ASSET_CLASS}).fetchone()
        assert non_us is not None
        print(f"\nnon-US tradable equity: {non_us[0]:,}")

        print(f"\ntradable {US_EQUITY_ASSET_CLASS} by instrument type:")
        for description, count in conn.execute(
            _US_EQUITY_BY_TYPE_SQL, {"us_asset_class": US_EQUITY_ASSET_CLASS}
        ).fetchall():
            print(f"  {description!s:>16}  {count:>7,}")

        type_id = resolve_stocks_type_id(conn)
        universe = load_validated_universe(conn)
        print(f"\nStocks instrument_type_id: {type_id}")
        print(f"§4.0 validated universe: {len(universe):,}")

        orphans = conn.execute(_ORPHAN_EXCHANGE_SQL).fetchone()
        assert orphans is not None
        print(f"\ntradable instruments with no exchange row (dropped by the loader's inner join): {orphans[0]:,}")
        if orphans[0]:
            violations.append(
                f"{orphans[0]:,} tradable instruments have no exchange row, so every census below is over a "
                "silently narrowed population"
            )

        print("\nquote currency of the validated universe:")
        validated_currencies = conn.execute(_VALIDATED_CURRENCY_SQL, {"ids": list(universe)}).fetchall()
        for currency, count in validated_currencies:
            print(f"  {currency!s:>16}  {count:>7,}")
        if [row[0] for row in validated_currencies] != ["USD"]:
            violations.append(
                "the validated universe is no longer uniformly USD-quoted — #2363's FX bound rested on that, so "
                "the strategy path now carries a quote-conversion cost nothing models"
            )

        print("\nquote currency across ALL tradable instruments:")
        for currency, count in conn.execute(_TRADABLE_CURRENCY_SQL).fetchall():
            print(f"  {currency!s:>16}  {count:>7,}")

        incoherent = conn.execute(_VENUE_COHERENCE_SQL, {"us_asset_class": US_EQUITY_ASSET_CLASS}).fetchall()
        print(f"\ntradable rows where asset_class='{US_EQUITY_ASSET_CLASS}' disagrees with country='US': ", end="")
        print(f"{sum(row[2] for row in incoherent):,}")
        for country, asset_class, count in incoherent:
            print(f"  {country!s:>8} {asset_class!s:>14}  {count:>7,}")
        if incoherent:
            violations.append(
                "the venue axis has split — 'US-only' now names one population by asset_class and a different one "
                "by country, and the settled decision does not say which"
            )

        print("\nstrategy_results_store by (universe_basis, carry_unmodelled):")
        for basis, carry, count in conn.execute(_RESULT_BASIS_SQL).fetchall():
            print(f"  {basis!s:>20}  carry_unmodelled={carry!s:<5}  {count:>6,}")

    if violations:
        print("\nVIOLATIONS:\n  " + "\n  ".join(violations))
        return 1
    print("\nboth asserted properties hold: venue axis coherent, validated universe uniformly USD-quoted")
    return 0


if __name__ == "__main__":
    sys.exit(main())
