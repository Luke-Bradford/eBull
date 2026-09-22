"""Full-population verification of S-2 cross-sectional momentum (#2240).

    PYTHONPATH=. uv run python scripts/verify_2240_s2_cross_sectional.py --all

⚠ NOTHING IS WRITTEN. Every arm reads and prints. Gate on the EXIT CODE — 0
means every arm passed, 1 means at least one did not.

⚠ NEVER PIPE THIS INTO ``head``/``tail``. A pipe returns the pipe's status, so a
failure reads as success, and it buffers the progress lines a long run is judged
by (`.claude/CLAUDE.md`). Redirect to a file and read the file.

THREE ARMS, MEASURING DIFFERENT THINGS
--------------------------------------
``--equivalence`` — every bar of BOTH corpora, ``research_price_daily`` and
``price_daily``, on RAW bars. The module's 12-1 score is compared against a
Postgres re-derivation from ``lag(close, 21)`` / ``lag(close, 252)``. Two
independent derivations: Python walks a list by index, SQL has no index and no
state.

``--census`` — the §4.0 validated universe (US stocks ex-ETF) on MASKED bars,
through the fail-closed loader. Reports the verdict and refusal distribution
(criterion 9's *"measure what you reject"*), the cross-section size per rebalance
date, and four narrowings this strategy makes that nothing else counts: the
20-bar eligibility gap, the §9 Q3 price floor, decision bars lost to a masked
close, and members that were listed on a rebalance date but had no bar on it.

``--ranking`` — the arm the other two cannot cover: **the decile cut itself**,
re-derived end to end in SQL (masking, eligibility, the price floor, the month
boundary and ``row_number()``) over the same validated universe, and compared
set-for-set against the Python selection on every rebalance date.

⚠ The arms are NOT comparable and the numbers must not be pooled: different
populations, different bars (raw vs masked), different purpose.
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

import psycopg
from psycopg import sql as pgsql

from app.config import settings
from app.services.cost_model import COST_MODEL_ID
from app.services.indicator_series import BarSeries, Universe
from app.services.research_price_structure_store import QUARANTINE_RULE_SET_VERSION, load_masked_series
from app.services.research_split_corrected_reader import load_ratio_basis
from app.services.strategies.s2_cross_sectional_momentum import (
    DECILE,
    ELIGIBILITY_BARS,
    LOOKBACK_BARS,
    MIN_CLOSE,
    MIN_CROSS_SECTION,
    S2_STRATEGY_ID,
    SKIP_BARS,
    _source_hash,
    momentum_series,
    rebalance_dates,
    s2_identity,
    s2_member,
    s2_select,
)
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_registry import stage_cross_sectional_member
from app.services.technical_analysis import OHLCVRow
from app.services.universe_selection import load_universe_selection

#: The research corpus is survivor-only (#2284: no free source serves the
#: delisted cohort), and ``price_daily`` is eToro's listing as it stands today.
#: Both are ``survivor_only`` and every figure below inherits that label (#2288).
#: ``--census`` / ``--ranking`` take ``--universe``; this is the default.
UNIVERSE: Universe = "survivor_only"

#: ⚠ IMPORTED, never restated. Stage 5b froze the model, so the identity hash
#: records a real cost basis; a local literal here would be a second source of
#: truth for a value that is hashed into every stored strategy version.

#: Relative margin below which a disagreement is arithmetic, not logic. Postgres
#: divides ``numeric`` exactly; the module divides ``float``.
TIE_TOLERANCE = 1e-9

#: ⚠ Read at IMPORT, microseconds after the strategy module is loaded, so it is
#: the hash of the code this process actually runs. See ``_stamped_version``.
_SOURCE_AT_IMPORT = _source_hash()


# ---------------------------------------------------------------------------
# --equivalence
# ---------------------------------------------------------------------------

_BAR_TEMPLATE = """
SELECT {key},
       {date_column},
       close,
       lag(close, {skip}) OVER w  AS c_skip,
       lag(close, {lookback}) OVER w AS c_back,
       open, high, low, volume
FROM {table}
WINDOW w AS (PARTITION BY {key} ORDER BY {date_column})
ORDER BY {key}, {date_column}
"""


def _bar_sql(table: str, key: str, date_column: str) -> pgsql.Composed:
    """Per-bar closes plus the two lagged closes the score divides.

    Composed through ``psycopg.sql`` rather than an f-string: the table and key
    vary per corpus and a window offset cannot be a query parameter, so names go
    through ``Identifier`` and the offsets through ``Literal``.
    """
    return pgsql.SQL(_BAR_TEMPLATE).format(
        table=pgsql.Identifier(table),
        key=pgsql.Identifier(key),
        date_column=pgsql.Identifier(date_column),
        skip=pgsql.Literal(SKIP_BARS),
        lookback=pgsql.Literal(LOOKBACK_BARS),
    )


CORPORA = (
    ("research_price_daily", _bar_sql("research_price_daily", "series_id", "bar_date")),
    ("price_daily", _bar_sql("price_daily", "instrument_id", "price_date")),
)


def _sql_scores(
    c_skip: list[Decimal | None],
    c_back: list[Decimal | None],
) -> list[float | None]:
    """The 12-1 score per bar from Postgres' lags only, mirroring the module's
    contract without sharing a line of its code: refused inside the 273-bar
    warm-up, and refused when either lagged close is missing or non-positive."""
    out: list[float | None] = []
    for index in range(len(c_skip)):
        if index + 1 < ELIGIBILITY_BARS:
            out.append(None)
            continue
        past, recent = c_back[index], c_skip[index]
        if past is None or recent is None or past <= 0 or recent <= 0:
            out.append(None)
            continue
        out.append(float(recent) / float(past) - 1.0)
    return out


@dataclass
class _Tally:
    series: int = 0
    bars: int = 0
    mismatches: list[str] = field(default_factory=list)
    ties: int = 0
    max_tie_margin: float = 0.0
    min_real_margin: float = float("inf")


def _compare(key: int, dates: list[date], rows: list[OHLCVRow], expected: list[float | None], tally: _Tally) -> None:
    series = BarSeries(dates=tuple(dates), rows=tuple(rows))
    actual = momentum_series(series, universe=UNIVERSE)
    tally.series += 1
    tally.bars += len(series)
    for index, want in enumerate(expected):
        got = actual.values[index]
        if (got is None) != (want is None):
            if len(tally.mismatches) < 20:
                tally.mismatches.append(f"{key} {dates[index]}: python={got!r} sql={want!r} (presence)")
            else:
                tally.mismatches.append("")
            continue
        if got is None or want is None:
            continue
        margin = abs(got - want) / max(abs(want), 1e-12)
        if margin == 0.0:
            continue
        if margin < TIE_TOLERANCE:
            tally.ties += 1
            tally.max_tie_margin = max(tally.max_tie_margin, margin)
            continue
        tally.min_real_margin = min(tally.min_real_margin, margin)
        if len(tally.mismatches) < 20:
            tally.mismatches.append(f"{key} {dates[index]}: python={got:.12g} sql={want:.12g} margin={margin:.3e}")
        else:
            tally.mismatches.append("")


def equivalence() -> int:
    """Every bar of both corpora: the module's score vs a SQL re-derivation."""
    failures = 0
    for table, sql in CORPORA:
        started = time.monotonic()
        tally = _Tally()
        print(f"\n[{table}] streaming…", flush=True)
        with psycopg.connect(settings.database_url) as conn, conn.cursor(name=f"s2_{table}") as cur:
            cur.itersize = 50_000
            cur.execute(sql)
            current: int | None = None
            dates: list[date] = []
            rows: list[OHLCVRow] = []
            c_skip: list[Decimal | None] = []
            c_back: list[Decimal | None] = []
            for row in cur:
                key = row[0]
                if key != current:
                    if current is not None:
                        _compare(current, dates, rows, _sql_scores(c_skip, c_back), tally)
                        if tally.series % 500 == 0:
                            print(
                                f"  {tally.series} series, {tally.bars} bars, "
                                f"{len(tally.mismatches)} mismatches, {tally.ties} ties "
                                f"({time.monotonic() - started:.0f}s)",
                                flush=True,
                            )
                    current, dates, rows, c_skip, c_back = key, [], [], [], []
                dates.append(row[1])
                rows.append({"open": row[5], "high": row[6], "low": row[7], "close": row[2], "volume": row[8]})
                c_skip.append(row[3])
                c_back.append(row[4])
            if current is not None:
                _compare(current, dates, rows, _sql_scores(c_skip, c_back), tally)

        real = len(tally.mismatches)
        print(f"  series           {tally.series}")
        print(f"  bars             {tally.bars}")
        print(f"  MISMATCHES       {real}")
        print(f"  ties (< {TIE_TOLERANCE:g})  {tally.ties}   max margin {tally.max_tie_margin:.3e}")
        if real:
            print(f"  smallest real margin {tally.min_real_margin:.3e}")
            for problem in [m for m in tally.mismatches if m][:20]:
                print("   ", problem)
            failures += 1
        print(f"  elapsed          {time.monotonic() - started:.1f}s", flush=True)
    return failures


# ---------------------------------------------------------------------------
# The shared streaming pass — --census and --ranking both read it
# ---------------------------------------------------------------------------


def _window_usable(closes: Sequence[float | None], index: int) -> bool:
    """Would the 12-1 window at ``index`` yield a score, ignoring the 273-bar gate?

    ⚠ Deliberately the module's DATA rule without its ELIGIBILITY rule — that is
    exactly the counterfactual the narrowing counters ask about ("would this bar
    have been evaluable under the looser 253-bar reading"). It is not a second
    copy of ``momentum_series``: it answers a question that function cannot,
    because that function refuses the whole range first.
    """
    if index < LOOKBACK_BARS:
        return False
    past, recent = closes[index - LOOKBACK_BARS], closes[index - SKIP_BARS]
    return past is not None and recent is not None and past > 0.0 and recent > 0.0


@dataclass
class _PanelRun:
    """One pass over the validated universe, with the ranking already resolved.

    ⚠ The panel is streamed one series at a time and only the per-date scores
    are kept. Holding 5,266 members' bars at once would be gigabytes, which is
    why ``stage_cross_sectional_member`` is public: this reuses the contract's
    own staging pass rather than re-implementing it, and a census that
    re-implements the strategy it measures is a census that can agree with
    nothing.
    """

    verdicts: Counter[str] = field(default_factory=Counter)
    reasons: Counter[str] = field(default_factory=Counter)
    selected: dict[date, frozenset[int]] = field(default_factory=dict)
    participants: dict[date, int] = field(default_factory=dict)
    thin_dates: set[date] = field(default_factory=set)
    series_with_bars: int = 0
    empty_series: int = 0
    bars: int = 0
    return_masked: int = 0
    eligibility_narrowed: int = 0
    floor_rejected: int = 0
    masked_decision_bars: int = 0
    listed_but_silent: int = 0
    boundary_ties: int = 0
    #: #2834 §7 slice C. ⚠ How each series reached its ratio basis, COUNTED and
    #: not inferred: `split_corrected` and `vendor_already_adjusted` both leave
    #: series whose two bases happen to be equal, and reporting the corpus as
    #: "corrected" when half of it was merely already adjusted would credit this
    #: change for work the vendor did.
    ratio_basis_methods: Counter[str] = field(default_factory=Counter)
    #: Bars the correction actually moved (scale != 1), summed over the panel.
    correction_moved_bars: int = 0
    #: ``series_id -> name_key`` the pass streamed, so ``ranking`` re-derives over
    #: the SAME admitted set rather than loading the admission a second time.
    panel: dict[int, int] = field(default_factory=dict)


def _admitted_panel(conn: psycopg.Connection[tuple], *, universe: Universe, progress: bool = True) -> dict[int, int]:
    """``series_id -> name_key`` for the series ``universe`` admits — the engine's own selection.

    #2834 part 2. This used to select every research series on a validated
    instrument with NO vendor filter, so every instrument carrying both an
    Intrader and an HF series refused the whole census. The collision was
    already resolved upstream: #2721's ``universe_selection`` admits SERIES,
    vendor-pinned per universe label, and the engine reads nothing else. A census
    that picks its own series measures a panel the engine never runs.

    ⚠ Do not "fix" this by splicing the two vendors onto one instrument.
    ``survivorship_free`` cannot extend past ``INTRADER_CAPTURE_DATE`` (#2721's
    hard bound), and some two-vendor instruments are two DIFFERENT issuers under one
    ``instrument_id`` — a reused ticker, confirmed by SEC Form 25 issuer CIK (see
    ``scripts/measure_2834_series_identity.py``).
    """
    validated = load_validated_universe(conn)
    selection = load_universe_selection(conn, universe=universe, validated_ids=frozenset(validated))
    if progress:
        print(
            f"  validated universe {len(validated)} instruments (US stocks ex-ETF, §4.0); "
            f"{universe} admits {len(selection.admitted)} series from {selection.vendor}",
            flush=True,
        )
    return {admitted.series_id: admitted.name_key for admitted in selection.admitted}


def _stream_panel(conn: psycopg.Connection[tuple], *, universe: Universe, progress: bool = True) -> _PanelRun:
    by_series = _admitted_panel(conn, universe=universe, progress=progress)
    if len(set(by_series.values())) != len(by_series):
        raise RuntimeError(
            "two admitted series share one name_key — the panel would rank one name against itself; "
            "universe_selection's admission is broken, fix it before trusting this census"
        )
    if progress:
        print(f"  research series in it {len(by_series)}", flush=True)

    # ⚠ The calendar is the union of the dates the panel's MASKED bars actually
    # cover, not of every raw bar. A raw-only date would otherwise be declared
    # the month's first bar and no member would rebalance that month at all.
    calendar = [
        row[0]
        for row in conn.execute(
            """
            SELECT DISTINCT d.bar_date
            FROM research_price_daily d
            JOIN research_price_quarantine_coverage cov
              ON cov.series_id = d.series_id
             AND cov.rule_set_version = %(version)s
             AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
            WHERE d.series_id = ANY(%(ids)s)
            """,
            {"ids": list(by_series), "version": QUARANTINE_RULE_SET_VERSION},
        ).fetchall()
    ]
    rebals = rebalance_dates(calendar)
    if progress:
        print(f"  panel calendar {len(calendar)} dates · {len(rebals)} rebalance dates", flush=True)

    run = _PanelRun(panel=by_series)
    scores_by_date: dict[date, dict[int, float]] = {}
    started = time.monotonic()

    for n, (series_id, instrument_id) in enumerate(by_series.items(), start=1):
        masked = load_masked_series(conn, series_id)
        if not masked.bars:
            run.empty_series += 1
            continue
        run.series_with_bars += 1
        run.return_masked += masked.return_masked
        rows: list[OHLCVRow] = [
            {"open": b.open, "high": b.high, "low": b.low, "close": b.close, "volume": b.volume}  # type: ignore[typeddict-item]
            for b in masked.bars
        ]
        series = BarSeries(dates=tuple(b.bar_date for b in masked.bars), rows=tuple(rows))
        run.bars += len(series)

        # #2834 §7 slice C — the momentum ratio reads the split-corrected basis,
        # the floor and every level test read these as-traded bars. On the
        # `split_adjusted` half of the corpus the two are the same object, and
        # `method` records which happened so the census can report the split
        # rather than infer it from an equality that has two meanings.
        corrected, method = load_ratio_basis(conn, series_id, series)
        run.ratio_basis_methods[method] += 1
        run.correction_moved_bars += corrected.moved_bars

        member = s2_member(
            series,
            ratio_basis=corrected.ratio_basis,
            panel_rebalance_dates=rebals,
            universe=universe,
            close_reason="quarantined_bar",
        )
        staged = stage_cross_sectional_member(member)
        for verdict in staged.verdicts:
            if verdict is None:
                continue
            run.verdicts[verdict.verdict] += 1
            if verdict.reason is not None:
                run.reasons[verdict.reason] += 1
        for when, score in staged.scores.items():
            scores_by_date.setdefault(when, {})[instrument_id] = score

        # The four narrowings, counted rather than asserted harmless.
        #
        # ⚠ EACH COUNTER REQUIRES THE BAR TO BE OTHERWISE EVALUABLE, and the
        # first draft did not (Codex, checkpoint 2). A bar in the 20-bar
        # eligibility gap whose lookback close is also masked would have been
        # refused either way, so attributing it to the eligibility gate
        # overstates what that gate uniquely costs — and a census that
        # overstates a rejection is the same defect as one that hides it.
        closes = series.float_closes
        own_dates = set(series.dates)
        for index, when in enumerate(series.dates):
            if index == len(series) - 1:
                continue
            close = closes[index]
            usable_window = _window_usable(closes, index)
            if LOOKBACK_BARS <= index < ELIGIBILITY_BARS - 1 and usable_window and close is not None:
                run.eligibility_narrowed += 1
            if when not in rebals or index < ELIGIBILITY_BARS - 1 or not usable_window:
                continue
            if close is None:
                run.masked_decision_bars += 1
            elif close < MIN_CLOSE:
                run.floor_rejected += 1
        first, last = series.dates[0], series.dates[-1]
        run.listed_but_silent += sum(1 for when in rebals if first <= when <= last and when not in own_dates)

        if progress and n % 500 == 0:
            print(f"  {n}/{len(by_series)} series, {run.bars} bars ({time.monotonic() - started:.0f}s)", flush=True)

    for when in sorted(scores_by_date):
        scores = scores_by_date[when]
        run.participants[when] = len(scores)
        if len(scores) < MIN_CROSS_SECTION:
            run.thin_dates.add(when)
            run.verdicts["not_evaluable"] += len(scores)
            run.reasons["thin_cross_section"] += len(scores)
            continue
        winners = s2_select(when, scores)
        run.selected[when] = winners
        run.verdicts["fired"] += len(winners)
        run.verdicts["not_fired"] += len(scores) - len(winners)
        # A tie SPANNING the cut is the only one that changes who is selected.
        #
        # ⚠ `DECILE`, not a literal 10 (review NITPICK). This arm is
        # deliberately parameter-COUPLED to the module — it re-derives the
        # algorithm independently, not the constants — and a second copy of the
        # cut would silently disagree the day the decile moves. The literals
        # that pin the constants against the SPEC live in
        # `tests/test_strategy_s2.py::TestSpecConstants`, which is the right
        # place for them: a script cannot fail a build.
        ordered = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
        cut = len(scores) // DECILE
        if 0 < cut < len(ordered) and ordered[cut - 1][1] == ordered[cut][1]:
            run.boundary_ties += 1
    return run


def census(universe: Universe) -> int:
    """Verdict + refusal distribution over the §4.0 validated universe, masked."""
    started = time.monotonic()
    print(f"\n[census] strategy {S2_STRATEGY_ID} version {_stamped_version(universe)}", flush=True)
    with psycopg.connect(settings.database_url) as conn:
        run = _stream_panel(conn, universe=universe)

    total = sum(run.verdicts.values())
    print(f"  series with bars  {run.series_with_bars}   (fail-closed empties: {run.empty_series})")
    print(f"  bars              {run.bars}")
    print(f"  masked closes     {run.return_masked}")
    # #2834 §7 slice C. ⚠ Printed as a DENOMINATED pair, never as a bare
    # "corrected" count: the two methods partition the panel and the share that
    # took each one is what says how much of this corpus the correction reaches.
    methods_total = sum(run.ratio_basis_methods.values())
    print(f"  ratio basis       {methods_total} series")
    for method, count in sorted(run.ratio_basis_methods.items()):
        share = 100.0 * count / methods_total if methods_total else 0.0
        print(f"      {method:<24} {count:>8,}  {share:6.2f}%")
    print(f"      bars moved by the correction {run.correction_moved_bars:,} of {run.bars:,}")
    print("  verdicts:")
    for verdict in ("fired", "not_fired", "not_evaluable"):
        count = run.verdicts[verdict]
        share = 100.0 * count / total if total else 0.0
        print(f"      {verdict:<16} {count:>12,}  {share:6.3f}%")
    for reason, count in sorted(run.reasons.items()):
        print(f"        reason {reason:<20} {count:>12,}")
    sizes = sorted(run.participants.values())
    if sizes:
        print(f"  rebalance dates   {len(sizes)}   thin (< {MIN_CROSS_SECTION}): {len(run.thin_dates)}")
        print(f"  cross-section     min {sizes[0]} · median {int(statistics.median(sizes))} · max {sizes[-1]}")
    print(f"  boundary ties at the decile cut: {run.boundary_ties}")
    print(f"  bars narrowed by the 273-bar eligibility (window was computable): {run.eligibility_narrowed:,}")
    print(f"  decision bars rejected by the ${MIN_CLOSE:.2f} floor: {run.floor_rejected:,}")
    print(f"  decision bars refused for a masked close: {run.masked_decision_bars:,}")
    print(f"  listed-but-silent member/date pairs (no bar on a rebalance date): {run.listed_but_silent:,}")
    print(f"  elapsed           {time.monotonic() - started:.1f}s", flush=True)
    # A census reports; it has no pass/fail of its own beyond running clean.
    return 0


# ---------------------------------------------------------------------------
# --ranking
# ---------------------------------------------------------------------------

_RANKING_SQL = """
WITH bars AS (
    SELECT d.series_id,
           d.bar_date,
           CASE WHEN COALESCE(q.return_usable, TRUE) THEN d.close END AS close,
           -- #2834 §7 slice C. ⚠ ROUTED, not COALESCEd: `split_factor` is NULL
           -- across every bar of the `split_adjusted` vendor, so a bare
           -- COALESCE(...,1) would read "this vendor ships no stamps"
           -- identically to "no split on this bar".
           CASE
               WHEN s.adjustment_basis = 'unadjusted' AND s.corporate_action_stamps = 'vendor_supplied'
               THEN d.split_factor
               ELSE 1
           END AS split_factor
    FROM research_price_daily d
    JOIN research_price_series s ON s.series_id = d.series_id
    JOIN research_price_quarantine_coverage cov
      ON cov.series_id = d.series_id
     AND cov.rule_set_version = %(version)s
     AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
    LEFT JOIN research_bar_quarantine q
      ON q.series_id = d.series_id
     AND q.bar_date = d.bar_date
     AND q.rule_set_version = %(version)s
    WHERE d.series_id = ANY(%(ids)s)
      -- ⚠⚠ THE REFUSAL IS A PREDICATE, NOT A FALL-THROUGH (review bot, PR #3285).
      -- The CASE above sends every un-named shape to a literal 1, which for
      -- `unadjusted` + `absent` means NO CORRECTION — while
      -- `research_split_corrected_reader.ratio_basis_method_for` RAISES on
      -- exactly that shape (as-traded bars whose corporate actions are
      -- unknown: neither basis is a return). An oracle that silently scores a
      -- series the production reader refuses is not an independent derivation
      -- of the same rule, it is a different rule that happens to agree most of
      -- the time. Restricting the population here makes the two arms disagree
      -- LOUDLY on such a series — Python raises, and this returns no row for
      -- it — instead of quietly.
      --
      -- ⚠ NOT expressible as `ELSE NULL`: SQL's `sum()` SKIPS nulls, so a
      -- series with some null factors would silently sum the rest and return a
      -- plausible scale. The predicate is the only form that cannot do that.
      AND (
            (s.adjustment_basis = 'unadjusted'     AND s.corporate_action_stamps = 'vendor_supplied')
         OR (s.adjustment_basis = 'split_adjusted' AND s.corporate_action_stamps <> 'vendor_supplied')
      )
),
calendar AS (
    SELECT bar_date, lag(bar_date) OVER (ORDER BY bar_date) AS previous
    FROM (SELECT DISTINCT bar_date FROM bars) c
),
rebalances AS (
    SELECT bar_date FROM calendar
    WHERE previous IS NOT NULL
      AND date_trunc('month', bar_date) <> date_trunc('month', previous)
),
windowed AS (
    SELECT series_id,
           bar_date,
           close,
           lag(close, %(skip)s) OVER s      AS c_skip,
           lag(close, %(lookback)s) OVER s  AS c_back,
           -- The corrected ratio equals the as-traded ratio times the product
           -- of the factors in the HALF-OPEN interval (back, skip] — every
           -- factor after `skip` appears in both scales and cancels. In row
           -- terms that is rows i-lookback+1 .. i-skip, i.e. this frame.
           --
           -- ⚠ exp(sum(ln)) because Postgres has no product aggregate. For an
           -- unsplit window every factor is exactly 1, ln(1) is exactly 0 and
           -- the sum is exactly 0, so this is EXACT wherever the correction
           -- does nothing and carries ~1e-15 relative error only where it
           -- does. That is a float tolerance on the ORACLE, not on the
           -- strategy: the comparison below is on decile MEMBERSHIP, and an
           -- exact score tie at the cut is counted separately as a boundary
           -- tie rather than silently resolved by drift.
           exp(sum(ln(split_factor)) OVER (
               PARTITION BY series_id ORDER BY bar_date
               ROWS BETWEEN %(lookback)s - 1 PRECEDING AND %(skip)s PRECEDING
           )) AS window_scale,
           row_number() OVER s              AS rn,
           count(*) OVER (PARTITION BY series_id) AS n_bars
    FROM bars
    WINDOW s AS (PARTITION BY series_id ORDER BY bar_date)
),
eligible AS (
    -- ⚠ The instrument id is resolved HERE, before the ranking, and that is not
    -- cosmetic: the tie-break is "score descending, then INSTRUMENT id
    -- ascending". A first version of this arm ranked on `series_id` and mapped
    -- to the instrument afterwards, which disagreed with the module on the two
    -- rebalance dates (1989-01-03, 1999-10-01) where the decile cut lands on an
    -- EXACT tie — 0.5 and 1.0 in both arithmetics, so not float drift, a
    -- different tie-break key. The arm was wrong; the strategy was not.
    --
    -- #2834 part 2: the key is the ADMITTED name key passed in beside the series
    -- ids (`universe_selection`), not a join back to `research_price_series`,
    -- which has no key for an unlinked terminating series.
    SELECT ps.name_key, w.bar_date, (w.c_skip / w.c_back) * w.window_scale - 1 AS score
    FROM windowed w
    JOIN rebalances r ON r.bar_date = w.bar_date
    JOIN unnest(%(ids)s::bigint[], %(keys)s::bigint[]) AS ps(series_id, name_key) ON ps.series_id = w.series_id
    WHERE w.rn >= %(eligibility)s
      AND w.rn < w.n_bars
      AND w.close IS NOT NULL
      AND w.close >= %(floor)s
      AND w.c_skip IS NOT NULL AND w.c_skip > 0
      AND w.c_back IS NOT NULL AND w.c_back > 0
),
ranked AS (
    SELECT name_key, bar_date, score,
           row_number() OVER (PARTITION BY bar_date ORDER BY score DESC, name_key) AS position,
           count(*)     OVER (PARTITION BY bar_date) AS n
    FROM eligible
)
SELECT bar_date, name_key
FROM ranked
WHERE n >= %(min_cross_section)s
  AND position <= n / %(decile)s
ORDER BY bar_date, name_key
"""


def ranking(universe: Universe) -> int:
    """The decile cut re-derived end to end in SQL, set-for-set against Python.

    ⚠ This is the arm ``--equivalence`` cannot cover. That one checks the SCORE;
    this checks everything built on top of it — the month boundary, the
    eligibility gate, the price floor, the last-bar refusal, the thin-panel rule
    and the ``N // 10`` cut with its tie-break — against a derivation that shares
    no code with the module.
    """
    started = time.monotonic()
    print(f"\n[ranking] strategy {S2_STRATEGY_ID} version {_stamped_version(universe)}", flush=True)
    with psycopg.connect(settings.database_url) as conn:
        run = _stream_panel(conn, universe=universe)
        panel = run.panel
        print(
            f"  python selection: {len(run.selected)} dates, {sum(map(len, run.selected.values()))} picks", flush=True
        )
        print("  re-deriving in SQL…", flush=True)
        sql_selected: dict[date, set[int]] = {}
        for bar_date, name_key in conn.execute(
            _RANKING_SQL,
            {
                "version": QUARANTINE_RULE_SET_VERSION,
                "ids": list(panel),
                "keys": list(panel.values()),
                "skip": SKIP_BARS,
                "lookback": LOOKBACK_BARS,
                "eligibility": ELIGIBILITY_BARS,
                "floor": MIN_CLOSE,
                "min_cross_section": MIN_CROSS_SECTION,
                "decile": DECILE,
            },
        ).fetchall():
            sql_selected.setdefault(bar_date, set()).add(int(name_key))

    print(f"  sql selection:    {len(sql_selected)} dates, {sum(map(len, sql_selected.values()))} picks", flush=True)
    problems: list[str] = []
    for when in sorted(set(run.selected) | set(sql_selected)):
        mine = set(run.selected.get(when, frozenset()))
        theirs = sql_selected.get(when, set())
        if mine != theirs:
            problems.append(f"{when}: python-only {sorted(mine - theirs)[:5]} sql-only {sorted(theirs - mine)[:5]}")
    print(f"  dates compared    {len(set(run.selected) | set(sql_selected))}")
    print(f"  MISMATCHED DATES  {len(problems)}")
    for problem in problems[:20]:
        print("   ", problem)
    print(f"  elapsed           {time.monotonic() - started:.1f}s", flush=True)
    return 1 if problems else 0


def _stamped_version(universe: Universe = UNIVERSE) -> str:
    """The strategy version, with the source pinned either side of reading it.

    ⚠ ``_source_hash`` re-reads the strategy file at CALL time while Python
    imported the module once at process start, so a probe run that mutates the
    file concurrently can stamp clean figures with an INJECTED version (#2240
    S-1, measured). A start-vs-end comparison does not catch a transient
    mutation; the check has to bracket the read itself, against an anchor taken
    at import.
    """
    if _source_hash() != _SOURCE_AT_IMPORT:
        raise RuntimeError(f"strategy source moved before stamping (expected {_SOURCE_AT_IMPORT}) — refusing to report")
    version = s2_identity(universe=universe, cost_model_id=COST_MODEL_ID).version
    if _source_hash() != _SOURCE_AT_IMPORT:
        raise RuntimeError(f"strategy source moved while stamping (expected {_SOURCE_AT_IMPORT}) — refusing to report")
    return version


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--equivalence", action="store_true", help="both corpora, raw bars, score vs SQL lags")
    parser.add_argument("--census", action="store_true", help="validated universe, masked bars, verdict distribution")
    parser.add_argument("--ranking", action="store_true", help="the decile cut, re-derived in SQL")
    parser.add_argument("--all", action="store_true")
    parser.add_argument(
        "--universe",
        choices=("survivor_only", "survivorship_free"),
        default=UNIVERSE,
        help="which universe_selection admission --census/--ranking read (default %(default)s)",
    )
    args = parser.parse_args()
    if not (args.equivalence or args.census or args.ranking or args.all):
        parser.error("pick at least one arm: --equivalence, --census, --ranking or --all")

    failures = 0
    if args.equivalence or args.all:
        failures += equivalence()
    if args.census or args.all:
        failures += census(args.universe)
    if args.ranking or args.all:
        failures += ranking(args.universe)
    print(f"\nverdict: {'*** FAIL ***' if failures else 'PASS'}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
