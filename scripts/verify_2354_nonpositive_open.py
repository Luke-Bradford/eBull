"""Full-population verification of #2354 — the non-positive open (Refs #2240).

    PYTHONPATH=. uv run python scripts/verify_2354_nonpositive_open.py --all

⚠ READ-ONLY. Nothing here writes, so there is no rollback to assert.

⚠ NEVER PIPE THIS INTO ``head``/``tail``. A pipe returns the pipe's status, so a
failure reads as success, and it buffers the progress lines (`.claude/CLAUDE.md`).
Redirect to a file and read the file.

⚠⚠ WHY THIS IS A FULL-POPULATION RUN WITHOUT SWEEPING 7,693 SERIES.
---------------------------------------------------------------------------
The change masks a bar's OPEN when it is NULL or ``<= 0`` and refuses to fill on
one. The population it can possibly touch is therefore exactly the set of bars
satisfying that predicate — which is a SQL question over the whole corpus, not a
sample of it. ``--census`` asks it of both corpora and of the quarantine, and
``--loader`` / ``--writer`` then exercise **every series the census names**, not
a subset. A series the census does not name cannot contain an affected bar, so
sweeping it would add runtime and no evidence.

⚠ The census computes its own numbers. Nothing here hardcodes a count — a
figure written down by hand goes stale the moment the corpus moves, in the place
a reader trusts most (`.claude/CLAUDE.md`).

THE ARMS
--------
``--census`` — the affected population, both corpora, with the quarantine's own
verdict beside it. C1 is the claim the design rests on: **the rule is
``price_quarantine.rule_b1``'s own clause**, so every bar this masks should
already be `B1`-quarantined on both axes. A bar that is not would mean the
loader is inventing an exclusion the quarantine never made, and that is a
finding, not a rounding error.

``--loader`` — ``load_masked_series`` over EVERY affected series, both arms.

  L1  The masked arm returns ``open is None`` on exactly the census's bar dates.
  L2  ⚠ **And on no others.** Compared field by field against the stored rows,
      so an over-masking regression cannot hide behind a count that happens to
      match.
  L3  The admitted arm returns the STORED open on those bars — criterion 9's
      *"admitted at their stored values rather than masked"*, which an exception
      for the open would quietly break.

``--writer`` — ``resolve_fills`` over every affected series, one ``fired`` entry
signal per bar, through the real loader.

  W1  **No row carries a non-positive fill price.** The defect in one assertion.
  W2  Each affected bar's PREDECESSOR resolves ``unusable_fill_price``, and the
      count matches the census's — the tenth code counts what it rejects.
  W3  ``no_fill_bar`` still means the series edge: exactly one per series, on
      the last bar, whatever else the series contains.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date
from decimal import Decimal
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import settings  # noqa: E402
from app.services.indicator_series import BarSeries  # noqa: E402
from app.services.research_price_structure_store import load_masked_series  # noqa: E402
from app.services.signal_ledger import resolve_fills  # noqa: E402
from app.services.strategy_registry import StrategyIdentity, StrategySignal  # noqa: E402
from app.services.technical_analysis import OHLCVRow  # noqa: E402

_IDENTITY = StrategyIdentity(
    strategy_id="S-2354-PROBE",
    params={},
    universe="survivor_only",
    cost_model_id="static-v1",
    source_hash="verify-2354",
)

#: The predicate the loader and the writer both apply, as SQL. ⚠ ``<= 0`` and
#: ``IS NULL``, matching ``rule_b1``'s clause — not ``= 0``, which is what the
#: corpus happens to hold today.
#:
#: ⚠ [review NITPICK] Written out qualified, not derived by string-replacing
#: ``open`` into ``d.open``. That trick reads as a way to keep one source for the
#: predicate and is not one — it rewrites every occurrence of the substring,
#: so adding ``open_interest`` or a comment containing the word to this constant
#: would silently corrupt the WHERE clause of both queries at once.
_UNUSABLE = "(d.open IS NULL OR d.open <= 0)"

_RESEARCH_CENSUS = f"""
    SELECT d.series_id,
           d.bar_date,
           d.open,
           q.rules,
           q.range_usable,
           q.return_usable
      FROM research_price_daily d
      LEFT JOIN research_bar_quarantine q
             ON q.series_id = d.series_id
            AND q.bar_date  = d.bar_date
     WHERE {_UNUSABLE}
     ORDER BY d.series_id, d.bar_date
"""

_VENUE_CENSUS = f"""
    SELECT d.instrument_id,
           d.price_date,
           d.open,
           q.rules,
           q.range_usable,
           q.return_usable
      FROM price_daily d
      LEFT JOIN price_bar_quarantine q
             ON q.instrument_id = d.instrument_id
            AND q.price_date    = d.price_date
     WHERE {_UNUSABLE}
     ORDER BY d.instrument_id, d.price_date
"""

_STORED_BARS = """
    SELECT bar_date, open, high, low, close, volume
      FROM research_price_daily
     WHERE series_id = %(series_id)s
     ORDER BY bar_date
"""


def _census(conn: psycopg.Connection[tuple]) -> tuple[list[tuple], list[tuple], list[str]]:
    """Both corpora's affected bars, and C1's check that the quarantine agrees."""
    research = conn.execute(_RESEARCH_CENSUS).fetchall()
    venue = conn.execute(_VENUE_CENSUS).fetchall()
    totals = conn.execute(
        "SELECT (SELECT count(*) FROM research_price_daily), (SELECT count(*) FROM price_daily)"
    ).fetchone()
    assert totals is not None
    research_total, venue_total = totals

    print("=== C1 — the affected population, and the quarantine's own verdict ===")
    problems: list[str] = []
    for label, rows, total, key in (
        ("research_price_daily", research, research_total, "series"),
        ("price_daily", venue, venue_total, "instruments"),
    ):
        subjects = {row[0] for row in rows}
        negatives = sum(1 for row in rows if row[2] is not None and row[2] < 0)
        nulls = sum(1 for row in rows if row[2] is None)
        print(
            f"  {label:<22} {len(rows):>6,} bars / {len(subjects):>3,} {key}   "
            f"of {total:>12,} bars   ({nulls} NULL, {negatives} negative)"
        )
        # C1 — every one should be B1 on both axes. `rules` is a text[]; a bar
        # with no quarantine row reads as NULL and is a FINDING, not a pass.
        by_rules: dict[tuple, int] = {}
        for _, _, _, rules, range_usable, return_usable in rows:
            by_rules[(tuple(rules) if rules else None, range_usable, return_usable)] = (
                by_rules.get((tuple(rules) if rules else None, range_usable, return_usable), 0) + 1
            )
        for (rules, range_usable, return_usable), count in sorted(by_rules.items(), key=lambda kv: -kv[1]):
            print(
                f"      rules={rules!s:<12} range_usable={range_usable!s:<5} "
                f"return_usable={return_usable!s:<5} {count:>6,}"
            )
            if rules is None or "B1" not in rules:
                problems.append(
                    f"C1 {label}: {count} bar(s) with an unusable open carry rules={rules!r} — the masking rule is "
                    "rule_b1's own clause, so a bar B1 did not flag means the loader is inventing an exclusion"
                )
            if range_usable is not False or return_usable is not False:
                problems.append(
                    f"C1 {label}: {count} bar(s) with an unusable open are usable on an axis "
                    f"(range={range_usable}, return={return_usable}) — B1 both-falses by construction"
                )
    return research, venue, problems


def _to_series(bars: list) -> BarSeries:
    rows: list[OHLCVRow] = [
        {"open": b.open, "high": b.high, "low": b.low, "close": b.close, "volume": b.volume}  # type: ignore[typeddict-item]
        for b in bars
    ]
    return BarSeries(dates=tuple(b.bar_date for b in bars), rows=tuple(rows))


def _run_loader(conn: psycopg.Connection[tuple], affected: dict[int, set[date]]) -> list[str]:
    problems: list[str] = []
    print("\n=== L1-L3 — the loader, over every affected series ===")
    for series_id, bar_dates in sorted(affected.items()):
        masked = load_masked_series(conn, series_id, arm="masked")
        admitted = load_masked_series(conn, series_id, arm="admitted")
        if not masked.bars:
            # Fail-closed at the series level (no coverage row, or a stale rule
            # set) — the bar never reaches a consumer at all. Reported, not a
            # violation: it is a stronger exclusion than masking, not a weaker one.
            print(f"  series {series_id:<6} fail-closed at the series level — no bars returned")
            continue
        stored = {row[0]: row for row in conn.execute(_STORED_BARS, {"series_id": series_id}).fetchall()}

        masked_dates = {bar.bar_date for bar in masked.bars if bar.open is None}
        in_range = {d for d in bar_dates if any(bar.bar_date == d for bar in masked.bars)}
        if masked_dates != in_range:
            problems.append(
                f"L1 series {series_id}: masked opens on {sorted(masked_dates)} but the census names {sorted(in_range)}"
            )
        # L2 — over-masking check. Every OTHER bar's open must equal its stored
        # value; a count-only check would pass while masking the wrong bars.
        for bar in masked.bars:
            if bar.bar_date in in_range:
                continue
            if bar.open != stored[bar.bar_date][1]:
                problems.append(
                    f"L2 series {series_id} {bar.bar_date}: open {bar.open!r} != stored {stored[bar.bar_date][1]!r}"
                )
        for bar in admitted.bars:
            if bar.bar_date in in_range and bar.open != stored[bar.bar_date][1]:
                problems.append(
                    f"L3 series {series_id} {bar.bar_date}: the admitted arm returned {bar.open!r}, "
                    f"not the stored {stored[bar.bar_date][1]!r}"
                )
        print(
            f"  series {series_id:<6} bars {len(masked.bars):>7,}   masked opens {len(masked_dates)}   "
            f"admitted opens {sum(1 for b in admitted.bars if b.open is None)}"
        )
    return problems


def _run_writer(conn: psycopg.Connection[tuple], affected: dict[int, set[date]]) -> list[str]:
    problems: list[str] = []
    print("\n=== W1-W3 — the writer, over every affected series ===")
    totals = {"rows": 0, "unusable_fill_price": 0, "no_fill_bar": 0, "fired": 0}
    for series_id in sorted(affected):
        masked = load_masked_series(conn, series_id, arm="masked")
        if not masked.bars:
            continue
        series = _to_series(list(masked.bars))
        rows = resolve_fills(
            [StrategySignal(verdict="fired", signal_index=i) for i in range(len(series))],
            series=series,
            identity=_IDENTITY,
            instrument_id=series_id,
        )
        totals["rows"] += len(rows)
        for row in rows:
            if row.not_evaluable_reason in totals:
                totals[row.not_evaluable_reason] += 1
            if row.verdict == "fired":
                totals["fired"] += 1
            # W1 — the defect itself.
            if row.fill_price is not None and row.fill_price <= Decimal(0):
                problems.append(f"W1 series {series_id} {row.signal_bar_date}: fill_price {row.fill_price}")

        # W2 — the PREDECESSOR of each masked bar is the signal that cannot fill.
        unusable = {r.signal_bar_date for r in rows if r.not_evaluable_reason == "unusable_fill_price"}
        expected = {series.dates[i - 1] for i in range(1, len(series)) if series.rows[i].get("open") is None}
        if unusable != expected:
            problems.append(
                f"W2 series {series_id}: unusable_fill_price on {sorted(unusable)}, expected {sorted(expected)}"
            )

        # W3 — the series edge, still exactly one and still the last bar.
        edges = [r.signal_bar_date for r in rows if r.not_evaluable_reason == "no_fill_bar"]
        if edges != [series.dates[-1]]:
            problems.append(f"W3 series {series_id}: no_fill_bar on {edges}, expected [{series.dates[-1]}]")

    print(
        f"  rows {totals['rows']:,}   fired {totals['fired']:,}   "
        f"unusable_fill_price {totals['unusable_fill_price']:,}   no_fill_bar {totals['no_fill_bar']:,}"
    )
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--census", action="store_true", help="the affected population in both corpora")
    parser.add_argument("--loader", action="store_true", help="load_masked_series over every affected series")
    parser.add_argument("--writer", action="store_true", help="resolve_fills over every affected series")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()
    if not (args.census or args.loader or args.writer or args.all):
        parser.error("choose --census, --loader, --writer or --all")

    started = time.monotonic()
    problems: list[str] = []
    with psycopg.connect(settings.database_url) as conn:
        research, _venue, census_problems = _census(conn)
        if args.census or args.all:
            problems.extend(census_problems)
        affected: dict[int, set[date]] = {}
        for series_id, bar_date, *_rest in research:
            affected.setdefault(series_id, set()).add(bar_date)
        if args.loader or args.all:
            problems.extend(_run_loader(conn, affected))
        if args.writer or args.all:
            problems.extend(_run_writer(conn, affected))

    print(f"\n=== {len(problems)} property violation(s) in {time.monotonic() - started:.1f}s ===")
    for problem in problems:
        print(f"  *** {problem}")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
