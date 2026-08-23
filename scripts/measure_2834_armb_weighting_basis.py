"""ARM B step 0: can the 12-2 replication be VALUE-weighted on our corpus? Refs #2834.

#2834's ARM B names one prerequisite before any replication runs::

    Step 0 (prerequisite): assemble point-in-time shares outstanding from stored
    SEC XBRL facts (`instrument_class_shares_outstanding` holds 62 rows —
    value-weighting is blocked without this). Declared fallback if coverage <80%
    of cap-weight: dollar-volume weighting, reported as such.

This script answers the coverage question the fallback is conditioned on, on the
FULL corpus rather than a sample. It assembles nothing and stores nothing: the
measurement runs FIRST because a shares table built for value weighting is wasted
if the weighting it exists to serve cannot be reached. (That is a claim about ARM
B's use of such a table, not about the table having no other use — see the FAIL
message's scope note.)

⚠ IT DECIDES NOTHING #2834 DID NOT ALREADY DECIDE. The 80% bar, the fallback and
"reported as such" are read off the ticket. What this script chooses — and says so
in the output — is only the three things the ticket left unconstructed, below.

Source rule
-----------

**The share count.** ``share_count_history`` (``sql/273``) is this repo's settled
point-in-time count: cover-page ``dei:EntityCommonStockSharesOutstanding``
preferred, ``us-gaap:CommonStockSharesOutstanding`` as fallback, positive values
only (#2232). Its concept pair and positivity rule are adopted here unchanged.
⚠ The IFRS share concepts are CORROBORATION ONLY and are deliberately excluded —
see the block comment at ``app/providers/implementations/sec_fundamentals.py:364``
and its ``AFYA`` / ``SLSR`` counter-examples, where adding them would have handed
both gaining instruments a WRONG denominator.

⚠⚠ But the AVAILABILITY test reads ``financial_facts_raw`` directly, not the view —
see ``_PIT_SQL``. The view deduplicates to the newest filing of each fact, which
erases the evidence that an earlier version was already public at a historical
formation. That understates coverage; the raw fact rows are the faithful source for
"was any acceptable count public by date D".

**The as-of date is the FILED date, not ``period_end``.** A formation on date D may
only use a count the market could already read on D. ``share_count_history`` exposes
both, and ``shares_outstanding_filed_date`` (#2411) is the filed date of whichever
COALESCE arm produced the count — ``latest_filed_date`` is ``MAX`` over five
concepts and would fail open. This is #2231's as-of rule pointed the other way:
blast radius asks when a fact was TRUE (``period_end``), knowledge time asks when
it became PUBLIC (``filed_date``).

Three constructions, because no published rule fixes them
---------------------------------------------------------

1. **"80% of cap-weight" cannot be evaluated as stated** — cap-weight is exactly what
   an absent share count makes unknowable, so coverage cannot be expressed as a
   fraction of it. This is reported as unmeasurable rather than quietly redefined.

   ⚠⚠ **The DECISION arm is therefore the UNWEIGHTED SERIES COUNT**, which
   presupposes nothing. The first version of this script used aggregate dollar
   volume as the size proxy and decided on that — which is CIRCULAR, because dollar
   volume is #2834's own declared fallback, so the fallback was being used to decide
   whether the fallback binds. Caught at Codex checkpoint 1, 2026-08-23.

   Dollar volume (``close * volume`` summed over the formation month) is still
   reported, as a SECONDARY reading and explicitly not as the decision. Two caveats
   travel with it and are the reason it cannot be promoted to the decision arm:
   summed monthly dollar volume is **turnover, not size** (a high-turnover microcap
   outweighs a low-turnover mega-cap), and ``research_price_series`` mixes
   adjustment bases, so ``close * volume`` is only split-invariant where the vendor
   adjusted both legs consistently. Reproduce the mix with::

       SELECT adjustment_basis, count(*) FROM research_price_series GROUP BY 1;

   (2026-08-23: 22,880 ``unadjusted`` / 7,711 ``split_adjusted`` — recompute rather
   than quoting these, a re-harvest moves them.)

   The two arms do not disagree about the verdict, which is what makes the choice
   non-load-bearing — see the script's own output.

2. **Staleness.** ``share_count_history`` keeps a filer's last count forever, so a
   name that stopped filing — deregistered, delisted, acquired — reads as "covered"
   indefinitely. Both arms are therefore reported:

   * ``any``       — any positive count filed on or before the formation date;
   * ``fresh``     — additionally filed within ``_FRESH_DAYS`` of it.

   ``fresh`` is the stricter reading (a count filed a decade before the formation is
   not a denominator). ⚠ ``_FRESH_DAYS = 460`` is fixed **BY CONSTRUCTION and has no
   published formulation**. Exchange Act Rules 13a-1 / 13a-13 set the annual and
   quarterly reporting obligations, and a domestic filer therefore restates the
   cover-page count roughly every 90 days — but the Rules do not imply 460, and
   saying they did was an over-claim corrected at Codex checkpoint 1. 460 is
   365 + one quarter + 5 days' slack, chosen to be loose enough that one late annual
   filing does not refuse an otherwise-current count. It is reported, not defended:
   both arms are printed at every granularity precisely so the reader can see that
   the verdict does not turn on it. The predicate is ``>= D - 460``, so a count filed
   exactly 460 days before the formation is INCLUDED (an earlier ``>`` made the bound
   459 while the prose said 460 — Codex checkpoint 3).

3. **"Alive at the formation."** ``_ALIVE_WINDOW_DAYS = 7``, also by construction. A
   series counts in a month's cross-section only if its last bar that month sits within
   7 calendar days of the formation date. Without the bound a series whose only bar was
   on the 3rd would be counted in a month-end cross-section it had already left, which
   inflates the denominator with names that are not investable at formation.

   ⚠ Relatedly, the formation date is now **corpus-wide per month** (the last bar any
   series printed that month), not each series' own last bar. Both were Codex
   checkpoint-3 findings: per-series dates meant one reported cross-section applied a
   different point-in-time cutoff to every name in it.

Run::

    PYTHONPATH=. uv run python -m scripts.measure_2834_armb_weighting_basis

⚠ Exit code is the point. It exits 1 when the 80% bar fails, because #2834's
fallback then binds ARM B's declaration and a run whose output nobody read must
not look like a pass.

⚠ The pass/fail rule is POOLED coverage across the window AND the per-formation
count is printed beside it, because #2834 does not say which it means. Both are
shown so the rule is visible rather than silently chosen; on the measured corpus
they agree, and where they would not, the per-formation line is the one that says
which sub-windows are usable.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date
from typing import Any, Final, TextIO

import psycopg
from psycopg import IsolationLevel

from app.config import settings

#: #2834's declared bar. Coverage at or above this keeps value weighting; below it
#: the ticket's own fallback (dollar-volume weighting, reported as such) binds.
COVERAGE_BAR_PCT: Final[float] = 80.0

#: See module docstring construction 2. Fixed BY CONSTRUCTION — no published rule
#: gives it, and Rules 13a-1 / 13a-13 motivate the ~90-day cadence without implying
#: this number. Both staleness arms are printed so the verdict does not turn on it.
_FRESH_DAYS: Final[int] = 460

#: ARM B's replication window, from the ticket: "(i) 2000-2026 replication window
#: (~320 monthly formations)". ⚠ BOUNDED AT BOTH ENDS. An open upper bound would let
#: a run in 2027 silently measure a different experiment than the one reported here.
_WINDOW_START: Final[date] = date(2000, 1, 1)
_WINDOW_END: Final[date] = date(2027, 1, 1)  # exclusive

#: How close to the common formation date a series' last bar must sit for the series
#: to count as alive at that formation. ⚠ BY CONSTRUCTION (7 calendar days ~ one
#: trading week). Without it a series whose only bar that month was on the 3rd would
#: be counted in a month-end cross-section it had already left.
_ALIVE_WINDOW_DAYS: Final[int] = 7

#: ONE formation date per month, shared by every series — the corpus-wide last bar of
#: that month. ⚠ An earlier version took each SERIES' own last bar, which gave one
#: reported cross-section a different PIT cutoff per series and made "alive" true for
#: a name that stopped trading on the 3rd. Caught at Codex checkpoint 3.
_FORMATION_SQL = """
    CREATE TEMP TABLE armb_formation ON COMMIT DROP AS
    SELECT date_trunc('month', d.bar_date)::date AS month_start,
           max(d.bar_date)                       AS formation_date
    FROM research_price_daily d
    WHERE d.bar_date >= %(window_start)s AND d.bar_date < %(window_end)s
    GROUP BY 1
"""

#: Monthly close/volume aggregate per series. One pass over the 76M-bar corpus.
#: ``close * volume`` is the dollar-volume proxy; a NULL volume contributes zero
#: rather than dropping the series, so a series with no volume at all reads as
#: zero size instead of vanishing from the denominator.
_MONTHLY_SQL = """
    CREATE TEMP TABLE armb_month ON COMMIT DROP AS
    SELECT d.series_id,
           date_trunc('month', d.bar_date)::date AS month_start,
           max(d.bar_date)                       AS series_last_bar,
           sum(d.close * coalesce(d.volume, 0))  AS dollar_volume,
           count(*)                              AS bars
    FROM research_price_daily d
    WHERE d.bar_date >= %(window_start)s AND d.bar_date < %(window_end)s
    GROUP BY 1, 2
"""

#: Every FILING OCCURRENCE of a positive point-in-time share count, read from
#: ``financial_facts_raw`` directly rather than through ``share_count_history``.
#:
#: ⚠⚠ THE VIEW IS THE WRONG SOURCE FOR AN AVAILABILITY TEST, and using it was a real
#: defect (Codex checkpoint 3). ``sql/273``'s ``latest_fact`` CTE is
#: ``DISTINCT ON (instrument_id, concept, period_end, period_start) ... ORDER BY
#: filed_date DESC`` — it keeps only the NEWEST filing of each fact. So a count first
#: filed in 2015 and amended in 2016 leaves only the 2016 row, and a 2015 formation
#: then reads "no count was public" when one was. The error is a FALSE NEGATIVE, i.e.
#: it understates coverage, which is the conservative direction for a FAIL verdict —
#: but it is still wrong and the fix is free.
#:
#: The view remains the settled source for the count's VALUE (its DEI-before-us-gaap
#: COALESCE, #2411's matching filed date). This query asks a different question — *was
#: any acceptable count public by date D* — for which the un-deduplicated fact rows are
#: the faithful source. The concept pair and the ``val > 0`` rule are taken from the
#: view unchanged, and ``taxonomy`` is pinned so the IFRS concepts stay excluded per
#: ``app/providers/implementations/sec_fundamentals.py:364``.
_PIT_SQL = """
    CREATE TEMP TABLE armb_pit ON COMMIT DROP AS
    SELECT DISTINCT f.instrument_id, f.filed_date
    FROM financial_facts_raw f
    WHERE f.taxonomy IN ('us-gaap', 'dei')
      AND f.concept IN ('EntityCommonStockSharesOutstanding', 'CommonStockSharesOutstanding')
      AND f.val > 0
      AND f.filed_date IS NOT NULL
"""

#: Per formation month: the cross-section alive that month, and how much of it —
#: by count and by dollar volume — carries a knowable-at-formation share count.
#:
#: ⚠ The ``EXISTS`` correlates on the SERIES' instrument, so a corpus series with
#: no ``instrument_id`` at all can never be covered. That is the honest reading
#: and not a join defect: no instrument row means no CIK route to SEC facts.
_COVERAGE_SQL = """
    SELECT f.month_start,
           f.formation_date,
           count(*)                                          AS series_alive,
           count(*) FILTER (WHERE cov.any_filed)             AS series_any,
           count(*) FILTER (WHERE cov.fresh_filed)           AS series_fresh,
           coalesce(sum(m.dollar_volume), 0)                                AS dv_total,
           coalesce(sum(m.dollar_volume) FILTER (WHERE cov.any_filed), 0)   AS dv_any,
           coalesce(sum(m.dollar_volume) FILTER (WHERE cov.fresh_filed), 0) AS dv_fresh
    FROM armb_month m
    JOIN armb_formation f ON f.month_start = m.month_start
    JOIN research_price_series s ON s.series_id = m.series_id
    CROSS JOIN LATERAL (
        SELECT
            EXISTS (
                SELECT 1 FROM armb_pit p
                WHERE p.instrument_id = s.instrument_id
                  AND p.filed_date <= f.formation_date
            ) AS any_filed,
            EXISTS (
                SELECT 1 FROM armb_pit p
                WHERE p.instrument_id = s.instrument_id
                  AND p.filed_date <= f.formation_date
                  AND p.filed_date >= f.formation_date - %(fresh_days)s::int
            ) AS fresh_filed
    ) cov
    WHERE m.series_last_bar >= f.formation_date - %(alive_days)s::int
    GROUP BY f.month_start, f.formation_date
    ORDER BY f.month_start
"""

#: Survivorship character of the corpus, which #2834 step 0 also asks for
#: ("census the corpus's pre-2022 survivorship character"). A corpus that is
#: survivorship-free only because it retains delisted series is only as good as
#: the count of series that actually terminate.
_SURVIVORSHIP_SQL = """
    SELECT count(*)                                                    AS series,
           count(*) FILTER (WHERE s.delisting_date IS NOT NULL)        AS with_delisting_date,
           count(*) FILTER (WHERE s.last_bar < %(cutoff)s)             AS terminated_pre_cutoff,
           count(*) FILTER (WHERE s.instrument_id IS NOT NULL)         AS with_instrument,
           count(*) FILTER (WHERE s.first_bar < %(window_start)s)      AS starts_before_window
    FROM research_price_series s
"""


def pct(numerator: float, denominator: float) -> float:
    """Percentage, with a zero denominator reported as 0.0 rather than raising.

    A formation month can legitimately have zero aggregate dollar volume (every
    alive series carrying NULL or zero volume), and that is a coverage of nothing
    rather than an error.
    """
    return 0.0 if denominator <= 0 else 100.0 * numerator / denominator


@dataclass(frozen=True)
class FormationCoverage:
    """One monthly formation's coverage under both staleness arms."""

    month_start: date
    formation_date: date
    series_alive: int
    series_any: int
    series_fresh: int
    dv_total: float
    dv_any: float
    dv_fresh: float

    @property
    def dv_any_pct(self) -> float:
        return pct(self.dv_any, self.dv_total)

    @property
    def dv_fresh_pct(self) -> float:
        return pct(self.dv_fresh, self.dv_total)

    @property
    def series_fresh_pct(self) -> float:
        return pct(self.series_fresh, self.series_alive)


def _load(conn: psycopg.Connection[tuple[Any, ...]]) -> list[FormationCoverage]:
    """Build the three temp tables and read the per-formation coverage.

    ⚠ Deliberately NOT wrapped in ``app.db.snapshot.snapshot_read``. That helper's
    contract is read-only ("the caller MUST NOT issue writes inside the block") and
    ``CREATE TEMP TABLE`` is DDL. The caller opens one REPEATABLE READ transaction
    instead, which gives the same single-snapshot property the helper exists for.
    """
    bounds = {"window_start": _WINDOW_START, "window_end": _WINDOW_END}
    conn.execute(_FORMATION_SQL, bounds)
    conn.execute("CREATE INDEX ON armb_formation (month_start)")
    conn.execute(_MONTHLY_SQL, bounds)
    conn.execute("CREATE INDEX ON armb_month (month_start)")
    conn.execute(_PIT_SQL)
    conn.execute("CREATE INDEX ON armb_pit (instrument_id, filed_date)")
    rows = conn.execute(_COVERAGE_SQL, {"fresh_days": _FRESH_DAYS, "alive_days": _ALIVE_WINDOW_DAYS}).fetchall()
    return [
        FormationCoverage(
            month_start=r[0],
            formation_date=r[1],
            series_alive=int(r[2]),
            series_any=int(r[3]),
            series_fresh=int(r[4]),
            dv_total=float(r[5]),
            dv_any=float(r[6]),
            dv_fresh=float(r[7]),
        )
        for r in rows
    ]


def _report_by_year(rows: list[FormationCoverage], out: TextIO) -> None:
    """Per-year rollup. Formation-level detail is too long to read and the year is
    the granularity at which the fact store's by-filed-year shape is visible.

    Both arms are printed for both metrics: reporting ``fresh`` alone on the series
    count and ``any`` alone on dollar volume would let a reader compare two arms
    that were never the same measurement.
    """
    print(
        f"\n{'year':>6} {'formations':>10} {'avg_series':>11} "
        f"{'ser_any%':>9} {'ser_fresh%':>11} {'dv_any%':>8} {'dv_fresh%':>10}",
        file=out,
    )
    by_year: dict[int, list[FormationCoverage]] = {}
    for r in rows:
        by_year.setdefault(r.month_start.year, []).append(r)
    for year in sorted(by_year):
        group = by_year[year]
        dv_total = sum(g.dv_total for g in group)
        dv_any = sum(g.dv_any for g in group)
        dv_fresh = sum(g.dv_fresh for g in group)
        alive = sum(g.series_alive for g in group)
        ser_any = sum(g.series_any for g in group)
        ser_fresh = sum(g.series_fresh for g in group)
        print(
            f"{year:>6} {len(group):>10} {alive // max(len(group), 1):>11} "
            f"{pct(ser_any, alive):>9.1f} {pct(ser_fresh, alive):>11.1f} "
            f"{pct(dv_any, dv_total):>8.1f} {pct(dv_fresh, dv_total):>10.1f}",
            file=out,
        )


def main() -> int:
    """Print the coverage census and FAIL when #2834's 80% bar is not met.

    The failing exit code is the finding, not an error: it is what makes the
    ticket's declared fallback bind rather than remain optional.
    """
    out = sys.stdout
    with psycopg.connect(settings.database_url) as conn:
        # One snapshot for the whole report: the coverage census and the
        # survivorship census must describe the same corpus, and a concurrent
        # research-corpus write between them would silently make them disagree.
        conn.isolation_level = IsolationLevel.REPEATABLE_READ
        with conn.transaction():
            rows = _load(conn)
            surv = conn.execute(
                _SURVIVORSHIP_SQL, {"cutoff": date(2022, 1, 1), "window_start": _WINDOW_START}
            ).fetchone()

    if not rows:
        print("NO FORMATION MONTHS — corpus empty over the window; nothing measured.", file=out)
        return 1

    in_window = [r for r in rows if r.month_start >= _WINDOW_START]

    # DECISION ARM — unweighted series count. Presupposes no size model, and in
    # particular does not presuppose the dollar-volume fallback whose binding is
    # the question. See module docstring construction 1.
    alive_total = sum(r.series_alive for r in in_window)
    pooled_series_any = pct(sum(r.series_any for r in in_window), alive_total)
    pooled_series_fresh = pct(sum(r.series_fresh for r in in_window), alive_total)

    # SECONDARY — dollar volume. Reported, never decisive.
    dv_total = sum(r.dv_total for r in in_window)
    pooled_dv_any = pct(sum(r.dv_any for r in in_window), dv_total)
    pooled_dv_fresh = pct(sum(r.dv_fresh for r in in_window), dv_total)

    print("#2834 ARM B step 0 — point-in-time share-count coverage", file=out)
    print(f"  window            {_WINDOW_START} .. {in_window[-1].month_start}", file=out)
    print(f"  formation months  {len(in_window)}", file=out)
    print(f"  freshness bound   {_FRESH_DAYS} days (BY CONSTRUCTION — no published rule)", file=out)
    print("  decision arm      unweighted series count", file=out)
    print("  secondary arm     aggregate monthly close*volume (turnover, not size)", file=out)
    print("  cap-weight        UNMEASURABLE — it is what the missing counts withhold", file=out)

    _report_by_year(in_window, out)

    passing_series = [r for r in in_window if r.series_fresh_pct >= COVERAGE_BAR_PCT]
    print(
        f"\npooled series coverage (DECISION)  any={pooled_series_any:.1f}%  fresh={pooled_series_fresh:.1f}%",
        file=out,
    )
    print(
        f"pooled dollar-volume coverage      any={pooled_dv_any:.1f}%  fresh={pooled_dv_fresh:.1f}%",
        file=out,
    )
    print(
        f"formations clearing {COVERAGE_BAR_PCT:.0f}% on the decision arm: {len(passing_series)} / {len(in_window)}",
        file=out,
    )

    assert surv is not None
    print(
        f"\nsurvivorship census — series={surv[0]} with_delisting_date={surv[1]} "
        f"terminated_before_2022={surv[2]} with_instrument_id={surv[3]} "
        f"first_bar_before_window={surv[4]}",
        file=out,
    )
    print(
        "  ⚠ with_delisting_date counts a RECORDED delisting, not a terminated series. "
        "A last bar before the cutoff can also be vendor truncation, a ticker change or "
        "a symbol migration, so neither figure alone establishes survivorship-freeness.",
        file=out,
    )

    if pooled_series_fresh >= COVERAGE_BAR_PCT:
        print("\nPASS — value weighting is assemblable; #2834's fallback does not fire.", file=out)
        return 0

    print(
        f"\nFAIL — pooled series coverage {pooled_series_fresh:.1f}% (fresh) is below the "
        f"{COVERAGE_BAR_PCT:.0f}% bar, and the secondary dollar-volume arm agrees at "
        f"{pooled_dv_fresh:.1f}%. #2834's declared fallback BINDS: ARM B weights by dollar "
        f"volume and reports it as such.\n"
        f"⚠ SCOPE OF THIS FAIL — it says value weighting is not assemblable FROM THIS "
        f"SOURCE (SEC XBRL cover-page + balance-sheet counts in financial_facts_raw), "
        f"under THIS identity map "
        f"(research_price_series.instrument_id), over THIS window. It does not say a "
        f"point-in-time shares table has no use: it says one must not be built for ARM B's "
        f"value weighting on the strength of this measurement.",
        file=out,
    )
    return 1


if __name__ == "__main__":  # pragma: no cover - script entry point
    raise SystemExit(main())
