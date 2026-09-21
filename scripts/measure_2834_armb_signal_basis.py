"""ARM B blocker 3: is s2's momentum SCORE basis-sensitive on the survivorship-free corpus? Refs #2834.

#2834's ARM B prototype must replicate s2's signal exactly, or it measures
selection drift instead of weighting. This script checks whether that signal is
stable on the corpus ARM B runs against, BEFORE the prototype is written.

The premise under test
----------------------

``app/services/strategies/s2_cross_sectional_momentum.py`` scores on ``close``
and justifies it as a SOURCE RULE::

    ⚠ PRICE RETURNS, NOT TOTAL RETURNS — §4 says so explicitly, and the corpus
    agrees by construction: ``research_price_daily.close`` is the SPLIT-adjusted
    close that is consistent with OHLC, while the dividend-adjusted series lives
    in ``adj_close`` (sql/251).

That sentence is TRUE for the vendor ``sql/251`` measured — the HF archive
``paperswithbacktest/Stocks-Daily-Price`` — and it is FALSE for the vendor s2
actually runs on. ``BACKTEST_UNIVERSE = "survivorship_free"`` pins
``icyDenev/Intrader`` (``app/services/universe_selection.py``), and
``app/services/research_corpus_ingest.py`` MEASURED that archive as the
opposite::

    ⚠ ``unadjusted`` is MEASURED, and it is the opposite of what #2398 recorded
    for the same vendor. ... this archive's OHLC carry NEITHER the split nor the
    dividend adjustment, and its ninth CSV column — stored as ``adj_close`` —
    carries both. Consumers computing returns must read ``adj_close`` (#2400).

Reproduced on the bars, AAPL's 4:1 split of 2020-08-31::

    vendor                                basis           bar_date     close   adj_close
    icyDenev/Intrader                     unadjusted      2020-08-27  500.04  122.169117
    icyDenev/Intrader                     unadjusted      2020-08-31  129.04  126.107534
    paperswithbacktest/Stocks-Daily-Price split_adjusted  2020-08-27  125.01  121.256416
    paperswithbacktest/Stocks-Daily-Price split_adjusted  2020-08-31  129.04  125.165390

A momentum score is a return, so on this vendor a corporate action inside the
``t-252 .. t-21`` window divides the score's gross ratio by the adjustment
factor.

⚠⚠ THE DIRECTION IS NOT ONE-SIDED, and an earlier draft of this docstring said
it was. A forward split divides the raw gross return (suppressing the score) and
a REVERSE split multiplies it — a 1-for-4 reverse inflates an otherwise
unchanged name's gross return by 4x. So this displaces names in BOTH directions
and the population effect is not "winners are suppressed". That is why this
script measures displacement and does not attribute a mechanism.

What this script measures, and what it does NOT
------------------------------------------------

It measures ONE thing: how much of s2's top decile changes when the identical
rule is scored on ``adj_close`` instead of ``close``.

⚠ Neither arm is "the truth". ``close`` is s2-exact and corporate-action
contaminated on this vendor; ``adj_close`` is a TOTAL return, which §4
explicitly does not want (it "systematically understates high-yield names over
an 11-month lookback, which §4 also says"). The correct basis is a SPLIT-ONLY
adjustment and this vendor does not store one — that is the finding. The
disagreement between the arms is a sensitivity, not a corrected signal.

⚠⚠ THE CAUSE IS DELIBERATELY NOT DECOMPOSED. An earlier version of this script
attributed each displaced name to "split-scale" or "dividend-scale" by the
change in its own ``close/adj_close`` factor. That is OBSERVATIONAL, not causal,
and it is wrong: a name's decile membership can change because a COMPETITOR's
score moved, so an entirely unadjusted name displaced by a split-corrected rival
was being labelled "dividend-scale". The arithmetic was wrong too — for a cash
distribution fraction ``y`` the factor gap is ``1/(1-y)``, so a 1.5 gap is a 33%
distribution and not the 50% the prose claimed, and small stock dividends (3%
annual is a real and recurring corporate policy) and small splits (5:4 -> 1.25,
11:10 -> 1.1) both land inside the band the separation assumed was empty.
Removed rather than repaired: identifying the cause needs corporate-action
evidence, not endpoint ratios.

Construction
------------

Admission is the SETTLED rule, not a vendor filter:
``universe_selection.load_universe_selection(universe="survivorship_free")`` —
vendor pin, capture-date assertion, alive-at-capture cut, duplicate-name-key
refusal and exchange-test-issue exclusion. ⚠ An earlier version selected every
series on the vendor, which silently redefined a settled term.

The selection rule is s2's, reused from
``scripts/verify_2240_s2_cross_sectional.py``'s ``_RANKING_SQL`` (already
equivalence-tested set-for-set against the module): the ``LOOKBACK_BARS`` /
``SKIP_BARS`` window, the >=273-bar eligibility, the ``MIN_CLOSE`` floor, the
last-bar refusal, the ``MIN_CROSS_SECTION`` thin-panel refusal and the
``n // DECILE`` cut ordered "score descending, then ``name_key`` ascending".
``name_key`` is the engine's own total key (``AdmittedSeries.name_key``:
instrument id, or ``-series_id`` for an admitted unlinked series).

⚠ DECLARED FIDELITY LIMITS — this is a basis-sensitivity comparison, not a
backtest replication, and these deviations from the production path are stated
rather than fixed because BOTH arms carry them identically:

1. **Weekends are dropped before the positional lags.** s2's ``rebalance_dates``
   drops them from the rebalance CALENDAR (#2797) while its loader keeps them in
   the bar history, so ``lag(close, 21)`` spans a different elapsed time there
   than here. Dropping them from the calendar alone would leave weekend
   artefacts inside the window, which is the worse of the two.
2. **No segment resets.** Production evaluates through
   ``strategy_segmented_evaluation.segmented_member``, which restarts warm-up at
   an unresolved price break and refuses segment-final bars. This ranks across
   those boundaries.
3. **Postgres ``numeric`` arithmetic**, not Python float — the two can disagree
   at a tie.
4. **Reads are not bounded by the window.** ``n_bars`` and the lags read the
   series' whole history; only FORMATION dates are bounded.

None of these is a like-for-like production claim and none is asserted to be
harmless in absolute terms; the claim is only that the two arms differ in the
price column and in nothing else.

Run::

    PYTHONPATH=. uv run python -m scripts.measure_2834_armb_signal_basis

⚠ Exit code is the point. It exits 1 when the arms disagree on more than
``_DISAGREEMENT_BAR_PCT`` of top-decile membership, because ARM B's prototype
then cannot claim to replicate "s2's signal" without the claim depending on
which price column it happened to read.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date
from typing import Any, Final, TextIO

import psycopg

from app.config import settings
from app.services.price_quarantine import RULE_SET_VERSION
from app.services.strategies.s2_cross_sectional_momentum import (
    DECILE,
    LOOKBACK_BARS,
    MIN_CLOSE,
    MIN_CROSS_SECTION,
    SKIP_BARS,
)
from app.services.strategies.validated_universe import load_validated_universe
from app.services.universe_selection import load_universe_selection

#: s2's eligibility gate. ⚠ The parent spec's two numbers disagree (the window
#: needs 253 bars, the stated eligibility is 273) and the module ships both
#: literally — score from ``t-252``, refuse until 273 bars. Same reading here.
_ELIGIBILITY_BARS: Final[int] = 273

#: Exploration window. ⚠ BOUNDED AT BOTH ENDS so a later run cannot silently
#: measure a different experiment. The lower bound is the first date
#: ``spy_chain_v1`` classifies a regime (1993-11-11); the upper bound clears
#: every registered hold-out on this corpus.
_WINDOW_START: Final[date] = date(1994, 1, 1)
_WINDOW_END: Final[date] = date(2021, 6, 29)  # exclusive

#: Above this, "replicate s2's signal" is not a well-defined instruction without
#: also naming the price column. BY CONSTRUCTION — no published rule fixes how
#: much selection disagreement invalidates a replication. Deliberately LOW: the
#: arms differ only by corporate actions, so any large disagreement IS the result.
_DISAGREEMENT_BAR_PCT: Final[float] = 1.0

#: One formation date per month, shared by every series, built from the ADMITTED
#: series only and with weekends removed first.
#:
#: ⚠ Both restrictions are load-bearing and an earlier version had neither. The
#: calendar takes the FIRST qualifying bar of each month, so a single stray
#: weekday from a series the admission rule excludes would hand that whole
#: month's rebalance to whoever happened to print that day.
_CALENDAR_SQL = """
    CREATE TEMP TABLE sb_rebalance ON COMMIT DROP AS
    WITH weekdays AS (
        SELECT DISTINCT d.bar_date
        FROM research_price_daily d
        WHERE d.series_id = ANY(%(series_ids)s)
          AND extract(isodow FROM d.bar_date) < 6
    ),
    seq AS (
        SELECT bar_date, lag(bar_date) OVER (ORDER BY bar_date) AS previous
        FROM weekdays
    )
    SELECT bar_date
    FROM seq
    WHERE previous IS NOT NULL
      AND date_trunc('month', bar_date) <> date_trunc('month', previous)
      AND bar_date >= %(window_start)s
      AND bar_date < %(window_end)s
"""

#: The panel, scored on BOTH bases in one pass.
#:
#: ⚠ The quarantine join pins ``rule_set_version`` on BOTH the coverage and the
#: verdict table, per ``_RANKING_SQL``. Joining verdicts alone fails OPEN outside
#: the rows the current rule set actually covers, and joining coverage on a
#: different version silently admits bars judged by superseded code.
_PANEL_SQL = """
    CREATE TEMP TABLE sb_panel ON COMMIT DROP AS
    WITH bars AS (
        SELECT d.series_id,
               d.bar_date,
               CASE WHEN COALESCE(q.return_usable, TRUE) THEN d.close END     AS close,
               CASE WHEN COALESCE(q.return_usable, TRUE) THEN d.adj_close END AS adj_close
        FROM research_price_daily d
        JOIN research_price_quarantine_coverage cov
          ON cov.series_id = d.series_id
         AND cov.rule_set_version = %(version)s
         AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
        LEFT JOIN research_bar_quarantine q
          ON q.series_id = d.series_id
         AND q.bar_date = d.bar_date
         AND q.rule_set_version = %(version)s
        WHERE d.series_id = ANY(%(series_ids)s)
          AND extract(isodow FROM d.bar_date) < 6
    ),
    windowed AS (
        SELECT series_id,
               bar_date,
               close,
               lag(close, %(skip)s)         OVER s AS c_skip,
               lag(close, %(lookback)s)     OVER s AS c_back,
               lag(adj_close, %(skip)s)     OVER s AS a_skip,
               lag(adj_close, %(lookback)s) OVER s AS a_back,
               row_number()                 OVER s AS rn,
               count(*) OVER (PARTITION BY series_id) AS n_bars
        FROM bars
        WINDOW s AS (PARTITION BY series_id ORDER BY bar_date)
    )
    SELECT w.series_id,
           w.bar_date,
           w.c_skip / w.c_back - 1 AS score_close,
           w.a_skip / w.a_back - 1 AS score_adj
    FROM windowed w
    JOIN sb_rebalance r ON r.bar_date = w.bar_date
    WHERE w.rn >= %(eligibility)s
      AND w.rn < w.n_bars
      AND w.close IS NOT NULL
      AND w.close >= %(floor)s
      AND w.c_skip IS NOT NULL AND w.c_skip > 0
      AND w.c_back IS NOT NULL AND w.c_back > 0
      AND w.a_skip IS NOT NULL AND w.a_skip > 0
      AND w.a_back IS NOT NULL AND w.a_back > 0
"""

#: Both deciles per formation, and their overlap. ``n / DECILE`` is integer
#: division, matching the module's ``len(scores) // DECILE``.
#:
#: ⚠ The two arms share ONE eligible cross-section by construction (a row is in
#: ``sb_panel`` only if BOTH bases produced a usable score), so ``n`` and the
#: decile size are identical across arms and the overlap is like-for-like.
#: ⚠ That shared support is itself a restriction — a name scoreable on raw close
#: but not on the adjusted series is excluded from BOTH arms rather than counted
#: as a disagreement. ``_EXCLUSION_SQL`` sizes it.
_OVERLAP_SQL = """
    WITH ranked AS (
        SELECT p.bar_date,
               p.series_id,
               nk.name_key,
               row_number() OVER (PARTITION BY p.bar_date ORDER BY p.score_close DESC, nk.name_key) AS pos_close,
               row_number() OVER (PARTITION BY p.bar_date ORDER BY p.score_adj   DESC, nk.name_key) AS pos_adj,
               count(*)     OVER (PARTITION BY p.bar_date) AS n
        FROM sb_panel p
        JOIN sb_name nk ON nk.series_id = p.series_id
    ),
    cut AS (
        SELECT bar_date,
               n,
               n / %(decile)s AS decile_size,
               pos_close <= n / %(decile)s AS in_close,
               pos_adj   <= n / %(decile)s AS in_adj
        FROM ranked
        WHERE n >= %(min_cross_section)s
    )
    SELECT bar_date,
           max(n)                                          AS cross_section,
           max(decile_size)                                AS decile_size,
           count(*) FILTER (WHERE in_close AND in_adj)     AS shared,
           count(*) FILTER (WHERE in_close AND NOT in_adj) AS close_only,
           count(*) FILTER (WHERE in_adj AND NOT in_close) AS adj_only
    FROM cut
    GROUP BY bar_date
    ORDER BY bar_date
"""


@dataclass(frozen=True)
class Formation:
    """One rebalance date's two deciles and their overlap."""

    bar_date: date
    cross_section: int
    decile_size: int
    shared: int
    close_only: int
    adj_only: int

    @property
    def disagreement_pct(self) -> float:
        """Share of the decile that changes when the price basis changes.

        ⚠ Symmetric by construction: the two deciles are the same SIZE (one
        shared cross-section, one ``n // DECILE`` cut), so ``close_only`` and
        ``adj_only`` are equal and either one measures the displacement. Both
        are carried anyway so the report can assert it — if they ever differ,
        the shared-cross-section invariant has broken and the reader should see
        that rather than read a number whose meaning has quietly changed.
        """
        return 0.0 if self.decile_size <= 0 else 100.0 * self.close_only / self.decile_size


def _admitted(conn: psycopg.Connection[tuple[Any, ...]]) -> dict[int, int]:
    """``series_id -> name_key`` for the settled ``survivorship_free`` admission."""
    validated = frozenset(load_validated_universe(conn))
    selection = load_universe_selection(conn, universe="survivorship_free", validated_ids=validated)
    return {row.series_id: row.name_key for row in selection.admitted}


def _load(conn: psycopg.Connection[tuple[Any, ...]], names: dict[int, int]) -> list[Formation]:
    """Build the calendar, name and panel temp tables, then read the overlap."""
    series_ids = list(names)
    params: dict[str, Any] = {
        "series_ids": series_ids,
        "window_start": _WINDOW_START,
        "window_end": _WINDOW_END,
        "version": RULE_SET_VERSION,
        "skip": SKIP_BARS,
        "lookback": LOOKBACK_BARS,
        "eligibility": _ELIGIBILITY_BARS,
        "floor": MIN_CLOSE,
        "decile": DECILE,
        "min_cross_section": MIN_CROSS_SECTION,
    }
    conn.execute("CREATE TEMP TABLE sb_name (series_id bigint PRIMARY KEY, name_key bigint) ON COMMIT DROP")
    with conn.cursor() as cur:
        cur.executemany("INSERT INTO sb_name (series_id, name_key) VALUES (%s, %s)", list(names.items()))
    conn.execute(_CALENDAR_SQL, params)
    conn.execute(_PANEL_SQL, params)
    return [
        Formation(
            bar_date=row[0],
            cross_section=int(row[1]),
            decile_size=int(row[2]),
            shared=int(row[3]),
            close_only=int(row[4]),
            adj_only=int(row[5]),
        )
        for row in conn.execute(_OVERLAP_SQL, params).fetchall()
    ]


def expected_months(start: date, end: date) -> int:
    """Calendar months whose first day falls in ``[start, end)``.

    The denominator the formation count is reported against. A formation is
    missing when the admitted corpus printed no weekday bar that month or the
    cross-section fell below ``MIN_CROSS_SECTION``; reporting the count alone
    would conceal that conditioning behind the words "full population".
    """
    return (end.year - start.year) * 12 + (end.month - start.month)


def _report(formations: list[Formation], *, stream: TextIO) -> float:
    """Print the per-decade summary and return pooled decile disagreement."""
    expected = expected_months(_WINDOW_START, _WINDOW_END)
    print(f"window           {_WINDOW_START} .. {_WINDOW_END} (exclusive)", file=stream)
    print(f"quarantine       {RULE_SET_VERSION}", file=stream)
    print(f"formations       {len(formations):,} of {expected:,} calendar months", file=stream)
    if not formations:
        return 0.0

    decile_total = sum(f.decile_size for f in formations)
    displaced = sum(f.close_only for f in formations)
    asymmetry = sum(abs(f.close_only - f.adj_only) for f in formations)
    pooled = 0.0 if decile_total <= 0 else 100.0 * displaced / decile_total

    print("\nper decade — decile membership that changes with the price basis", file=stream)
    header = f"  {'decade':<8} {'formations':>10} {'mean x-sec':>11} {'decile':>8} {'displaced':>10} {'pct':>7}"
    print(header, file=stream)
    for decade in sorted({f.bar_date.year // 10 * 10 for f in formations}):
        rows = [f for f in formations if f.bar_date.year // 10 * 10 == decade]
        d_total = sum(f.decile_size for f in rows)
        d_moved = sum(f.close_only for f in rows)
        mean_x = sum(f.cross_section for f in rows) / len(rows)
        pct = 0.0 if d_total <= 0 else 100.0 * d_moved / d_total
        print(
            f"  {decade:<8} {len(rows):>10,} {mean_x:>11,.0f} {d_total:>8,} {d_moved:>10,} {pct:>6.2f}%",
            file=stream,
        )

    worst = max(formations, key=lambda f: f.disagreement_pct)
    print(f"\npooled decile displacement  {pooled:.2f}%  ({displaced:,} of {decile_total:,})", file=stream)
    print(f"worst formation             {worst.bar_date}  {worst.disagreement_pct:.2f}%", file=stream)
    print(f"arm asymmetry (must be 0)   {asymmetry:,}", file=stream)
    print(
        "\n⚠ displacement is slot-weighted (sum of displaced / sum of decile slots), so a\n"
        "  formation with a large cross-section counts for more than a small one. An\n"
        "  equal-per-month weighting answers a different and equally legitimate question.\n"
        "⚠ the CAUSE is not decomposed — see the module docstring.",
        file=stream,
    )
    return pooled


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET statement_timeout = 1800000")
        names = _admitted(conn)
        print(f"admitted series  {len(names):,}", file=sys.stdout)
        formations = _load(conn, names)
        pooled = _report(formations, stream=sys.stdout)

    if not formations:
        print("\nREFUSED: no formation produced a usable cross-section — nothing was measured.", file=sys.stdout)
        return 2
    if pooled > _DISAGREEMENT_BAR_PCT:
        print(
            f"\nFAIL: {pooled:.2f}% of top-decile membership changes with the price basis, above the "
            f"{_DISAGREEMENT_BAR_PCT:.2f}% bar. On this vendor `close` is the RAW traded level, so a "
            "corporate action inside the window rescales the score's gross ratio. ARM B's prototype "
            "cannot claim to replicate `s2's signal` without the claim depending on which price "
            "column it read — a third blocker alongside step 0's share coverage and step 1's kernel.",
            file=sys.stdout,
        )
        return 1
    print(f"\nPASS: {pooled:.2f}% displacement, at or below the {_DISAGREEMENT_BAR_PCT:.2f}% bar.", file=sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
