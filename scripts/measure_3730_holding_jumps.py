"""#3730: unstamped jumps in the research corpus, and what JKP's holding-return winsorisation does to the panel.

Read-only. Two modes:

``--precision`` (dev DB). Every pair of consecutive positive-``adj_close`` Intrader observations whose ratio is
above 4 or below 0.1 (the §"Daily screen" bounds of the #3609 step 1 spec, +300% / -90%), classified against every
``paperswithbacktest`` series with the same vendor symbol over the same two dates: ``smooth`` (ratio within
[0.5, 2]), ``same_jump``, ``opposite``, ``unserved`` (no reference bar pair), ``invalid_reference`` (a reference
endpoint not positive) or ``ambiguous`` (reference series disagree). A symbol match is not a verified identity.
⚠ The reference is a Yahoo scrape (``research-price-corpus`` skill), so its served set is survival-correlated and
``smooth`` means the two vendors disagree, not that the Intrader bar is wrong. Then every Intrader month return above
+300% in 2014-09 .. 2024-08 (last positive ``adj_close`` of consecutive months), split by whether its month crosses
a ``research_transition_quarantine`` row that is quarantined (``rules`` non-empty) or admitted back on a turnover
``spike``.

``--ab ARTEFACT --cutoffs CSV``. The published stage-A panel artefact's admitted holding returns, before and after
JKP's rule: ``GlobalFactors/portfolios.R`` (``bkelly-lab/ReplicationCrisis`` at ``67174c7f``, lines 102 and 138-142)
caps a non-CRSP ``ret_exc_lead1m`` at the holding month's ``ret_exc_99_9`` and floors it at ``ret_exc_0_1``, from
``return_cutoffs`` (``GlobalFactors/main.sas:79``, ``crsp_only=0``). Our holdings are total returns, so they are
compared with the same file's ``ret_99_9`` / ``ret_0_1``: one risk-free rate per month shifts every percentile by
that rate, so the two pairs differ by it and the rule is the same. The CSV is JKP's published
``https://jkpfactors-data.s3.amazonaws.com/public/other/return_cutoffs.csv``; its sha256 is printed. Each arm is
winsorised. Populations are ``report_3621_books.populations`` at M over the artefact's frozen NYSE cutoffs.

Usage::

    PYTHONPATH=. uv run python -m scripts.measure_3730_holding_jumps --precision
    PYTHONPATH=. uv run python -m scripts.measure_3730_holding_jumps --ab <stage-A artefact dir> --cutoffs <csv>
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import math
from collections import defaultdict
from collections.abc import Iterator, Mapping, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Final, LiteralString

import psycopg

from app.config import settings
from scripts.report_3621_books import Population, populations

INTRADER: Final = "icyDenev/Intrader"
#: §"Daily screen": a daily return outside [-0.9, +3.0], as adjacent-bar ratios.
RATIO_HIGH: Final = 4.0
RATIO_LOW: Final = 0.1
#: Return months 2014-09 .. 2024-08; August 2014 is read as the first month's base.
WINDOW: Final = ("2014-08-01", "2024-08-31")
FIRST_RETURN_MONTH: Final = "2014-09-01"
#: The +300% the issue counts.
EXTREME: Final = 3.0
ARMS: Final = ("best_case", "worst_case")
#: Float noise in a CSV of decimal strings, far below any risk-free rate.
SHIFT_TOLERANCE: Final = 1e-9

_PAIRS: Final[LiteralString] = """
WITH b AS (
  SELECT d.series_id, s.vendor_symbol, d.bar_date, d.adj_close,
         lag(d.bar_date) OVER w AS prior_date, lag(d.adj_close) OVER w AS prior_adj
    FROM research_price_daily d JOIN research_price_series s USING (series_id)
   WHERE s.vendor = %(vendor)s AND d.adj_close > 0
  WINDOW w AS (PARTITION BY d.series_id ORDER BY d.bar_date)
), j AS (
  SELECT * FROM b WHERE prior_adj > 0 AND (adj_close / prior_adj > %(high)s OR adj_close / prior_adj < %(low)s)
)
SELECT CASE WHEN j.adj_close / j.prior_adj > 1 THEN 'up' ELSE 'down' END AS direction,
       CASE WHEN o.n = 0 THEN 'unserved'
            WHEN o.invalid > 0 THEN 'invalid_reference'
            WHEN o.classes > 1 THEN 'ambiguous'
            ELSE o.class END AS reference,
       count(*)
  FROM j CROSS JOIN LATERAL (
    SELECT count(*) AS n,
           count(*) FILTER (WHERE NOT (x.adj_close > 0 AND xp.adj_close > 0)) AS invalid,
           count(DISTINCT c.class) AS classes, min(c.class) AS class
      FROM research_price_series os
      JOIN research_price_daily x ON x.series_id = os.series_id AND x.bar_date = j.bar_date
      JOIN research_price_daily xp ON xp.series_id = os.series_id AND xp.bar_date = j.prior_date
      CROSS JOIN LATERAL (
        SELECT CASE WHEN NOT (x.adj_close > 0 AND xp.adj_close > 0) THEN NULL
                    WHEN x.adj_close / xp.adj_close BETWEEN 0.5 AND 2 THEN 'smooth'
                    WHEN (x.adj_close / xp.adj_close > 2) = (j.adj_close / j.prior_adj > 1) THEN 'same_jump'
                    ELSE 'opposite' END AS class
      ) c
     WHERE os.vendor LIKE 'paperswithbacktest%%' AND os.vendor_symbol = j.vendor_symbol
  ) o
 GROUP BY 1, 2 ORDER BY 1, 2
"""

_MONTHS: Final[LiteralString] = """
WITH me AS (
  SELECT DISTINCT ON (d.series_id, date_trunc('month', d.bar_date))
         d.series_id, date_trunc('month', d.bar_date) AS m, d.bar_date, d.adj_close
    FROM research_price_daily d JOIN research_price_series s USING (series_id)
   WHERE s.vendor = %(vendor)s AND d.bar_date BETWEEN %(lo)s AND %(hi)s AND d.adj_close > 0
   ORDER BY d.series_id, date_trunc('month', d.bar_date), d.bar_date DESC
), mr AS (
  SELECT series_id, m, bar_date AS end_d, lag(bar_date) OVER w AS start_d, adj_close / lag(adj_close) OVER w - 1 AS r
    FROM me WINDOW w AS (PARTITION BY series_id ORDER BY m)
)
SELECT EXISTS (SELECT 1 FROM research_transition_quarantine t WHERE t.series_id = mr.series_id
                  AND t.bar_date > mr.start_d AND t.bar_date <= mr.end_d AND cardinality(t.rules) > 0) AS quarantined,
       EXISTS (SELECT 1 FROM research_transition_quarantine t WHERE t.series_id = mr.series_id
                  AND t.bar_date > mr.start_d AND t.bar_date <= mr.end_d AND cardinality(t.rules) = 0
                  AND t.corroboration = 'spike') AS admitted_spike,
       count(*), avg(r), max(r)
  FROM mr
 WHERE r > %(extreme)s AND start_d IS NOT NULL AND m >= %(first)s::date
   AND m = date_trunc('month', start_d) + interval '1 month'
 GROUP BY 1, 2 ORDER BY 1, 2
"""


def precision() -> None:
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        params = {"vendor": INTRADER, "high": RATIO_HIGH, "low": RATIO_LOW}
        print("Intrader adjacent-bar pairs outside the daily screen, by reference vendor:")
        for direction, reference, n in conn.execute(_PAIRS, params).fetchall():
            print(f"  {direction:<4} {reference:<10} {n:>6}")
        params = {"vendor": INTRADER, "lo": WINDOW[0], "hi": WINDOW[1], "first": FIRST_RETURN_MONTH, "extreme": EXTREME}
        span = f"{FIRST_RETURN_MONTH[:7]}..{WINDOW[1][:7]}"
        print(f"Intrader month returns above +{EXTREME:.0%} ({span}), by quarantine rows crossed:")
        for quarantined, spike, n, mean, peak in conn.execute(_MONTHS, params).fetchall():
            print(f"  quarantined={quarantined!s:<5} admitted_spike={spike!s:<5} n={n:>5}", end="")
            print(f" mean={mean:.2f} max={peak:.1f}")


def read_return_cutoffs(path: Path) -> dict[str, tuple[float, float]]:
    """JKP ``return_cutoffs.csv``: holding month ``YYYY-MM`` -> (``ret_0_1``, ``ret_99_9``), the USD total-return pair.

    Refuses a non-month-end ``eom``, a duplicate month, a non-finite bound, ``low > high``, or a row whose total and
    excess pairs are not one shift apart (the equivalence Amendment 3 relies on).
    """
    out: dict[str, tuple[float, float]] = {}
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            eom = date.fromisoformat(row["eom"])
            if (eom + timedelta(days=1)).day != 1:
                raise ValueError(f"{path}: {eom} is not a month end")
            low, high = float(row["ret_0_1"]), float(row["ret_99_9"])
            if not (math.isfinite(low) and math.isfinite(high) and low <= high):
                raise ValueError(f"{path}: {eom} bounds {low}, {high}")
            # The total-return pair is JKP's excess pair shifted by one rate: the same shift at both ends.
            shift = (high - float(row["ret_exc_99_9"])) - (low - float(row["ret_exc_0_1"]))
            if abs(shift) > SHIFT_TOLERANCE:
                raise ValueError(f"{path}: {eom} total and excess bounds differ by unequal shifts ({shift})")
            month = row["eom"][:7]
            if month in out:
                raise ValueError(f"{path}: duplicate month {month}")
            out[month] = (low, high)
    return out


def winsorise(value: float, bounds: tuple[float, float]) -> float:
    low, high = bounds
    return min(max(value, low), high)


def _jsonl(path: Path) -> Iterator[Any]:
    with gzip.open(path, "rt") as handle:
        for line in handle:
            yield json.loads(line)


def nyse_cutoffs_usd(path: Path) -> dict[str, tuple[float, float, float]]:
    """The artefact's frozen JKP ``nyse_cutoffs``: month end -> (p20, p50, p80) in USD."""
    by_month: dict[str, dict[str, float]] = defaultdict(dict)
    for key, month_end, value, unit in _jsonl(path):
        if key in ("nyse_p20", "nyse_p50", "nyse_p80"):
            if unit != "usd_millions":
                raise ValueError(f"{key} {month_end}: unit {unit!r}")
            if key in by_month[month_end]:
                raise ValueError(f"{key} {month_end}: duplicate")
            by_month[month_end][key] = float(value) * 1e6
    return {m: (v["nyse_p20"], v["nyse_p50"], v["nyse_p80"]) for m, v in by_month.items() if len(v) == 3}


def ab(artefact: Path, cutoffs_csv: Path) -> None:
    print(f"cutoffs: {cutoffs_csv} sha256={hashlib.sha256(cutoffs_csv.read_bytes()).hexdigest()}")
    bounds = read_return_cutoffs(cutoffs_csv)
    nyse = nyse_cutoffs_usd(artefact / "inputs" / "reference_snapshot_jkp_nyse_cutoffs.jsonl.gz")
    by_formation: dict[str, dict[int, tuple[float, Mapping[str, float]]]] = defaultdict(dict)
    holding_month: dict[str, str] = {}
    for row in _jsonl(artefact / "rows.jsonl.gz"):
        if row["exclusion"] is not None:
            continue
        me = row["me"]["value"]
        if me is None:
            raise ValueError(f"admitted row without ME: {row['M']} {row['series_id']}")
        holding = row["prices"]["holding"]
        if "raw_by_arm" in holding:
            raise ValueError("artefact built under Amendment 3: its by_arm is already winsorised; read raw_by_arm")
        arms = holding["by_arm"]
        if not all(math.isfinite(arms[arm]) for arm in ARMS):
            raise ValueError(f"non-finite holding: {row['M']} {row['series_id']}")
        sid = int(row["series_id"])
        if sid in by_formation[row["M"]]:
            raise ValueError(f"duplicate admitted row: {row['M']} {sid}")
        by_formation[row["M"]][sid] = (float(me), arms)
        if holding_month.setdefault(row["M"], row["holding_month"]) != row["holding_month"]:
            raise ValueError(f"formation {row['M']} has two holding months")

    counts: dict[tuple[Population, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
    book: dict[tuple[Population, str], list[tuple[float, float]]] = defaultdict(list)
    peaks: dict[tuple[Population, str], list[float]] = defaultdict(lambda: [-math.inf, -math.inf])
    for formation in sorted(by_formation):
        names = by_formation[formation]
        month_bounds = bounds[holding_month[formation]]
        pops = populations({sid: me for sid, (me, _) in names.items()}, nyse[formation])
        for population, members in pops.items():
            for arm in ARMS:
                key = (population, arm)
                before = [names[sid][1][arm] for sid in members]
                after = [winsorise(r, month_bounds) for r in before]
                if not before:
                    continue
                tally = counts[key]
                tally["name_months"] += len(before)
                tally["capped"] += sum(r > month_bounds[1] for r in before)
                tally["floored"] += sum(r < month_bounds[0] for r in before)
                tally["above_300pct"] += sum(r > EXTREME for r in before)
                book[key].append((sum(before) / len(before), sum(after) / len(after)))
                peaks[key] = [max(peaks[key][0], *before), max(peaks[key][1], *after)]

    print(f"stage-A artefact {artefact.name}: {len(by_formation)} formations")
    print("population arm         name_months  >+300%  capped  floored", end="")
    print("  max_before  max_after  EW_mean_mo_before  after")
    for population in Population:
        for arm in ARMS:
            key = (population, arm)
            if key not in counts:
                continue
            tally, months = counts[key], book[key]
            mean_before = sum(b for b, _ in months) / len(months)
            mean_after = sum(a for _, a in months) / len(months)
            print(
                f"{population!s:<10} {arm:<11} {tally['name_months']:>11} {tally['above_300pct']:>7}"
                f" {tally['capped']:>7} {tally['floored']:>8} {peaks[key][0]:>11.2f} {peaks[key][1]:>10.2f}"
                f" {mean_before:>18.5f} {mean_after:>9.5f}"
            )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n", 1)[0])
    parser.add_argument("--precision", action="store_true")
    parser.add_argument("--ab", type=Path, metavar="ARTEFACT")
    parser.add_argument("--cutoffs", type=Path, help="JKP return_cutoffs.csv")
    args = parser.parse_args(argv)
    if args.precision:
        precision()
    if args.ab is not None:
        if args.cutoffs is None:
            parser.error("--ab needs --cutoffs")
        ab(args.ab, args.cutoffs)
    if not args.precision and args.ab is None:
        parser.error("choose --precision and/or --ab")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
