"""Full-population verification of the phase-5c result model (#2240).

    PYTHONPATH=. uv run python scripts/verify_2240_result_model.py --all

⚠ NOTHING IS WRITTEN. Every arm reads and prints. Gate on the EXIT CODE — 0
means every arm passed, 1 means at least one did not.

⚠ NEVER PIPE THIS INTO ``head``/``tail``. A pipe returns the pipe's status, so a
failure reads as success, and it buffers the progress lines a long run is judged
by (``.claude/CLAUDE.md``). Redirect to a file and read the file.

WHY A FULL-POPULATION ARM EXISTS FOR A PURE MODULE
--------------------------------------------------
``strategy_result`` reads no database, so a table test can pin every branch.
What a table test cannot do is check the FROZEN LITERALS, and those are the part
that can be wrong without any test noticing: ``HOLDOUT_BOUNDARY`` is a claim
about 23 million bars, and ``CORPUS_VERSION`` is a claim about which vendor
produced them. Both are transcribed constants, and a transcribed constant is
exactly the class ``.claude/CLAUDE.md`` says to compute rather than write down.

So ``--frozen`` re-derives the boundary from the corpus and asserts the literal
still equals it — acceptance C5: *"The frozen boundary literal must equal the
corpus's bar-weighted boundary or the run FAILS rather than re-splitting."*

⚠⚠ IT FAILS; IT DOES NOT UPDATE. §5.2: *"It is a function of the corpus, and the
corpus grows; a recomputed boundary walks forward silently and re-admits
hold-out data into training between runs."* A run that quietly rewrote the
literal would be the defect this arm exists to catch.

THE ARMS
--------
``--frozen``  the corpus identity and the split point, re-derived. Vendor set,
              frozen last bar, window start, and the bar-weighted 75/25
              boundary with its in-sample / hold-out counts.

``--split``   the split PRIMITIVES over the full population: every trading date
              in the slice classified by ``namespace_for_bar`` and reconciled
              against SQL, and every real boundary-straddling bar pair fed to
              ``namespace_for_signal`` and required to purge.

``--gate``    the promotion refusal against today's real state — the actual
              validated universe, the actual corpus∩universe slice, and the
              actual ``cost_model`` carry flag. Plus the two arms a
              refuses-everything gate would also pass: a genuinely clean
              candidate must CLEAR, and a real out-of-universe instrument must
              be caught.

⚠ WHAT IS NOT COVERED HERE. The ``strategy_results`` CONSTRAINTS are exercised
by ``tests/test_strategy_results_table.py`` against a real database, not here —
this script writes nothing, and a constraint test has to attempt a write.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date
from decimal import Decimal

import psycopg

from app.config import settings
from app.services.cost_model import CARRY_UNMODELLED, COST_MODEL_ID, FX_UNMODELLED
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_result import (
    BENCHMARK_RULE,
    CORPUS_FROZEN_LAST_BAR,
    CORPUS_VENDORS,
    CORPUS_VERSION,
    EVALUATION_WINDOW_END,
    EVALUATION_WINDOW_START,
    HOLDOUT_BOUNDARY,
    SIZING_RULE,
    TOTAL_RETURN_BASIS,
    PromotionCandidate,
    ResultIdentity,
    StrategyResult,
    check_promotable,
    namespace_for_bar,
    namespace_for_signal,
)
from app.services.strategy_statistics import StrategyMetrics

#: §5.2's split fraction. ⚠ Criterion 5's own words — *"the final 25% of history
#: is withheld"* — so the in-sample share is what the boundary search targets.
IN_SAMPLE_FRACTION = Decimal("0.75")

#: Bars per date across the corpus ∩ validated-universe slice, ascending.
#: ⚠ Aggregated IN SQL. The slice is 23.3M bars and pulling them into Python to
#: count would be gigabytes for a statistic whose support is 16,236 dates.
#:
#: ⚠⚠ BOUNDED BY THE FROZEN WINDOW ON BOTH SIDES, and the bound is the whole
#: point of the arm rather than a tidy-up. §5.2: appended data *"sits outside
#: the frozen window until a deliberate re-freeze"*. Unbounded, this query
#: derives the boundary over WHATEVER the corpus holds today, so the next
#: `research_corpus_ingest` run walks the derived boundary forward and
#: ``--frozen`` starts FAILING — reporting that the frozen literal is wrong when
#: the literal is the only thing that was right. The window is the constant; the
#: corpus is not.
#:
#: ⚠ Defence-in-depth today, not a live wrong-row fix: measured 2026-08-07,
#: **0** bars in this slice fall after ``EVALUATION_WINDOW_END`` and **0** before
#: ``EVALUATION_WINDOW_START``. Bounded anyway, for #2317's reason — a window
#: that only holds while the corpus happens not to have grown is not a window.
#: Found at Codex checkpoint 2; no test could have caught it, because on today's
#: corpus the bounded and unbounded queries return identical rows.
_BARS_PER_DATE_SQL = """
    SELECT d.bar_date, count(*)::bigint
    FROM research_price_daily d
    JOIN research_price_series s ON s.series_id = d.series_id
    WHERE s.instrument_id = ANY(%(ids)s)
      AND d.bar_date BETWEEN %(window_start)s AND %(window_end)s
    GROUP BY d.bar_date
    ORDER BY d.bar_date
"""

#: How much of the corpus the frozen window excludes. ⚠ REPORTED on every run:
#: a growing number is the signal that a deliberate re-freeze is due, and it is
#: invisible if the bounded query simply never mentions the rows it drops.
_OUTSIDE_WINDOW_SQL = """
    SELECT count(*) FILTER (WHERE d.bar_date > %(window_end)s),
           count(*) FILTER (WHERE d.bar_date < %(window_start)s)
    FROM research_price_daily d
    JOIN research_price_series s ON s.series_id = d.series_id
    WHERE s.instrument_id = ANY(%(ids)s)
"""

_SLICE_SHAPE_SQL = """
    SELECT count(DISTINCT s.series_id), min(s.first_bar), max(s.last_bar)
    FROM research_price_series s
    WHERE s.instrument_id = ANY(%(ids)s)
"""

_VENDORS_SQL = "SELECT DISTINCT vendor FROM research_price_series ORDER BY vendor"

#: Every series holding bars on BOTH sides of the boundary, with the last
#: in-sample bar and the first hold-out bar. ⚠ That pair is the ONLY
#: boundary-straddling consecutive pair the series can produce, so this is the
#: complete purge population — not a sample of it.
_STRADDLING_PAIRS_SQL = """
    SELECT s.series_id,
           max(d.bar_date) FILTER (WHERE d.bar_date <  %(boundary)s),
           min(d.bar_date) FILTER (WHERE d.bar_date >= %(boundary)s)
    FROM research_price_daily d
    JOIN research_price_series s ON s.series_id = d.series_id
    WHERE s.instrument_id = ANY(%(ids)s)
      AND d.bar_date BETWEEN %(window_start)s AND %(window_end)s
    GROUP BY s.series_id
    HAVING count(*) FILTER (WHERE d.bar_date <  %(boundary)s) > 0
       AND count(*) FILTER (WHERE d.bar_date >= %(boundary)s) > 0
"""

#: Corpus series that DID resolve to an instrument which is nonetheless outside
#: the §4.0 validated universe — ETFs, non-``us_equity`` listings, untradable
#: names. ⚠ This is the real population the universe clause has to catch, and it
#: is not the same as the 2,424 series with no ``instruments`` row at all: those
#: carry no id to feed the gate.
_RESOLVED_OUTSIDE_UNIVERSE_SQL = """
    SELECT DISTINCT s.instrument_id
    FROM research_price_series s
    WHERE s.instrument_id IS NOT NULL
      AND NOT (s.instrument_id = ANY(%(ids)s))
    ORDER BY s.instrument_id
"""


def _window(universe: list[int]) -> dict[str, object]:
    """Query params pinning EVERY bar read to the frozen evaluation window.

    ⚠ ONE helper rather than a literal per query, so a re-freeze moves every arm
    together. An arm bounded differently from its neighbour would report a
    disagreement between the classifier and SQL that is the QUERY's fault, and
    the triage would go straight to the classifier.
    """
    return {"ids": universe, "window_start": EVALUATION_WINDOW_START, "window_end": EVALUATION_WINDOW_END}


def _one(conn: psycopg.Connection[tuple], sql: str, params: dict[str, object]) -> tuple:
    """One row, or raise.

    ⚠ ``fetchone`` is typed ``tuple | None`` and every call here is an aggregate
    that returns exactly one row. Unpacking the ``None`` directly is what pyright
    flags; swallowing it with a default would turn "the query matched nothing"
    into a silent zero, which on an aggregate arm is a PASS on no data.
    """
    row = conn.execute(sql, params).fetchone()  # type: ignore[arg-type]
    if row is None:
        raise RuntimeError(f"expected exactly one row from:{sql}")
    return row


def _metrics(*, effective_sample_size: float | None) -> StrategyMetrics:
    """A criterion-7 set for the gate arms.

    ⚠ THE VALUES ARE PLACEHOLDERS AND THE NULL IS NOT. Stage 5d always returns
    ``effective_sample_size=None`` (criterion 3's block bootstrap is 5e), so the
    "today" arm passes ``None`` to exercise the real state, and the "clean" arm
    passes a figure to prove the gate can be cleared at all. Everything else on
    the row is shape, not measurement — this script verifies the GATE, and
    ``scripts/verify_2240_statistics.py`` is where the metrics are measured.
    """
    return StrategyMetrics(
        expectancy_per_trade_pct=0.0,
        profit_factor=None,
        cagr_pct=0.0,
        annualised_volatility_pct=0.0,
        sharpe=0.0,
        sortino=None,
        max_drawdown_pct=0.0,
        exposure_time_pct=0.0,
        turnover_annualised=0.0,
        trade_count=0,
        effective_sample_size=effective_sample_size,
        return_vs_buy_and_hold_pct=0.0,
        losing_trade_count=0,
        losing_period_count=0,
        open_trade_count=0,
        unpriced_trade_count=0,
        periods_per_year=251.66,
        total_return_pct=0.0,
        buy_and_hold_return_pct=0.0,
    )


def _fail(message: str) -> int:
    print(f"   ✗ {message}")
    return 1


def _ok(message: str) -> int:
    print(f"   ✓ {message}")
    return 0


def _derive_boundary(bars_per_date: list[tuple[date, int]]) -> tuple[date, int, int]:
    """§5.2's boundary, re-derived: (first hold-out date, in-sample bars, hold-out bars).

    ⚠⚠ TWO RULES, NOT ONE, and the second is where a re-derivation goes wrong.

    - SELECTION: the first trading date whose CUMULATIVE bar count strictly
      exceeds 75% of the total.
    - SPLIT: that date is the FIRST HOLD-OUT BAR. Its own bars are withheld.

    Measured on this corpus the two differ by 4,021 bars — the count stamped on
    2021-06-29 itself. Fold the selection into the split and every one of them
    lands in training: 0.02% of the corpus, invisible in any summary statistic,
    and exactly criterion 5's leak.
    """
    total = sum(count for _, count in bars_per_date)
    threshold = Decimal(total) * IN_SAMPLE_FRACTION
    running = 0
    for when, count in bars_per_date:
        running += count
        if Decimal(running) > threshold:
            in_sample = running - count
            return when, in_sample, total - in_sample
    raise RuntimeError(f"no date's cumulative count exceeds {IN_SAMPLE_FRACTION} of {total} — the slice is empty")


def frozen(conn: psycopg.Connection[tuple]) -> int:
    """Re-derive the corpus identity and the split point; assert the literals hold."""
    print("\n── FROZEN LITERALS ─────────────────────────────────────────────")
    status = 0
    universe = list(load_validated_universe(conn))
    print(f"   §4.0 validated universe: {len(universe):,} instruments")

    vendors = tuple(row[0] for row in conn.execute(_VENDORS_SQL).fetchall())
    if vendors == CORPUS_VENDORS:
        status |= _ok(f"vendor set matches CORPUS_VENDORS: {vendors[0]}")
    else:
        status |= _fail(
            f"corpus vendors {vendors} != CORPUS_VENDORS {CORPUS_VENDORS} — a second vendor is a "
            "corpus-version event (§5.2), so bump CORPUS_VERSION deliberately rather than pooling them"
        )

    series_count, first_bar, last_bar = _one(conn, _SLICE_SHAPE_SQL, {"ids": universe})
    print(f"   corpus ∩ universe: {series_count:,} series · {first_bar} → {last_bar}")

    if first_bar == EVALUATION_WINDOW_START:
        status |= _ok(f"EVALUATION_WINDOW_START == the slice's first bar ({first_bar})")
    else:
        status |= _fail(f"EVALUATION_WINDOW_START {EVALUATION_WINDOW_START} != the slice's first bar {first_bar}")

    # ⚠ `>=`, NOT `==`. The corpus GROWS, and §5.2 says appended data "sits
    # outside the frozen window until a deliberate re-freeze". A later last bar
    # is expected drift and is reported; an EARLIER one means the window is
    # frozen at bars that do not exist, which is a real failure.
    if last_bar >= CORPUS_FROZEN_LAST_BAR:
        drift = (last_bar - CORPUS_FROZEN_LAST_BAR).days
        status |= _ok(
            f"corpus reaches the frozen last bar {CORPUS_FROZEN_LAST_BAR}"
            + (f" (+{drift}d of appended data, outside the frozen window by design)" if drift else " exactly")
        )
    else:
        status |= _fail(
            f"corpus last bar {last_bar} is BEFORE the frozen {CORPUS_FROZEN_LAST_BAR} — the evaluation window "
            "is frozen at bars the corpus does not hold"
        )

    print(f"   corpus_version = {CORPUS_VERSION}")

    # ⚠ REPORTED, not asserted. Bars outside the frozen window are CORRECT — §5.2
    # puts appended data there deliberately — but a bounded query that never
    # mentions what it dropped makes a growing exclusion invisible, and the size
    # of it is the signal that a deliberate re-freeze is due.
    after, before = _one(conn, _OUTSIDE_WINDOW_SQL, _window(universe))
    print(f"   bars OUTSIDE the frozen window: {after:,} after {EVALUATION_WINDOW_END} · {before:,} before")

    started = time.monotonic()
    bars_per_date = [(row[0], int(row[1])) for row in conn.execute(_BARS_PER_DATE_SQL, _window(universe)).fetchall()]
    total_bars = sum(count for _, count in bars_per_date)
    print(f"   {len(bars_per_date):,} trading dates · {total_bars:,} bars   ({time.monotonic() - started:.1f}s)")

    derived, in_sample, hold_out = _derive_boundary(bars_per_date)
    on_boundary = dict(bars_per_date)[derived]
    share = Decimal(in_sample) * 100 / Decimal(total_bars)
    print(f"   derived boundary {derived} · in-sample {in_sample:,} ({share:.3f}%) · hold-out {hold_out:,}")
    print(f"   bars ON the boundary date: {on_boundary:,}  ← the selection/split gap")

    if derived == HOLDOUT_BOUNDARY:
        status |= _ok(f"HOLDOUT_BOUNDARY == the re-derived bar-weighted boundary ({derived})")
    else:
        status |= _fail(
            f"HOLDOUT_BOUNDARY {HOLDOUT_BOUNDARY} != the re-derived {derived}. ⚠ DO NOT UPDATE THE LITERAL — "
            "§5.2 freezes it, and a recomputed boundary walks forward silently and re-admits hold-out data "
            "into training. A move is a deliberate re-freeze that invalidates prior hold-out results."
        )

    holdout_dates = sum(1 for when, _ in bars_per_date if when >= HOLDOUT_BOUNDARY)
    print(f"   hold-out spans {holdout_dates:,} trading dates, {HOLDOUT_BOUNDARY} → {EVALUATION_WINDOW_END}")
    return status


def split(conn: psycopg.Connection[tuple]) -> int:
    """The split primitives, over every date and every real straddling bar pair."""
    print("\n── SPLIT PRIMITIVES, FULL POPULATION ───────────────────────────")
    status = 0
    universe = list(load_validated_universe(conn))

    bars_per_date = [(row[0], int(row[1])) for row in conn.execute(_BARS_PER_DATE_SQL, _window(universe)).fetchall()]

    # ⚠ The module's classifier reconciled against SQL's own predicate, over
    # every date rather than a fixture. A `>` for `>=` in `namespace_for_bar`
    # shows up here as a one-date, 4,021-bar disagreement.
    py_in_sample = sum(count for when, count in bars_per_date if namespace_for_bar(when) == "in_sample")
    py_hold_out = sum(count for when, count in bars_per_date if namespace_for_bar(when) == "hold_out")
    sql_in_sample, sql_hold_out = _one(
        conn,
        """
        SELECT count(*) FILTER (WHERE d.bar_date <  %(boundary)s),
               count(*) FILTER (WHERE d.bar_date >= %(boundary)s)
        FROM research_price_daily d
        JOIN research_price_series s ON s.series_id = d.series_id
        WHERE s.instrument_id = ANY(%(ids)s)
          AND d.bar_date BETWEEN %(window_start)s AND %(window_end)s
        """,
        _window(universe) | {"boundary": HOLDOUT_BOUNDARY},
    )
    print(f"   namespace_for_bar: in-sample {py_in_sample:,} · hold-out {py_hold_out:,}")
    print(f"   SQL              : in-sample {sql_in_sample:,} · hold-out {sql_hold_out:,}")
    if (py_in_sample, py_hold_out) == (sql_in_sample, sql_hold_out):
        status |= _ok(f"the classifier agrees with SQL on all {py_in_sample + py_hold_out:,} bars")
    else:
        status |= _fail("namespace_for_bar disagrees with SQL — the boundary is inclusive on the hold-out side")

    dates_on_boundary = [count for when, count in bars_per_date if when == HOLDOUT_BOUNDARY]
    if dates_on_boundary and namespace_for_bar(HOLDOUT_BOUNDARY) == "hold_out":
        status |= _ok(f"the boundary date's own {dates_on_boundary[0]:,} bars are WITHHELD, not trained on")
    else:
        status |= _fail("the boundary date's bars are not on the hold-out side")

    started = time.monotonic()
    pairs = conn.execute(_STRADDLING_PAIRS_SQL, _window(universe) | {"boundary": HOLDOUT_BOUNDARY}).fetchall()
    print(f"   {len(pairs):,} series straddle the boundary   ({time.monotonic() - started:.1f}s)")

    # ⚠ THE COMPLETE PURGE POPULATION, not a sample: a series can straddle the
    # boundary in exactly one consecutive bar pair, so one row here is one
    # purgeable signal slot.
    not_purged = 0
    for _series_id, last_in, first_out in pairs:
        if namespace_for_signal(last_in, first_out) != "purged":
            not_purged += 1
    if not_purged == 0:
        status |= _ok(f"every one of the {len(pairs):,} real straddling (signal, fill) pairs PURGES (§5.2)")
    else:
        status |= _fail(f"{not_purged:,} straddling pairs were assigned to a namespace instead of purged")

    # The converse, and the one that catches a function that purges everything:
    # the pair ENTIRELY inside each side must NOT purge.
    inside = _one(
        conn,
        """
        SELECT max(d.bar_date) FILTER (WHERE d.bar_date < %(boundary)s),
               min(d.bar_date) FILTER (WHERE d.bar_date >= %(boundary)s)
        FROM research_price_daily d
        JOIN research_price_series s ON s.series_id = d.series_id
        WHERE s.instrument_id = ANY(%(ids)s)
          AND d.bar_date BETWEEN %(window_start)s AND %(window_end)s
        """,
        _window(universe) | {"boundary": HOLDOUT_BOUNDARY},
    )
    last_in_sample_date, first_hold_out_date = inside
    if (
        namespace_for_signal(EVALUATION_WINDOW_START, last_in_sample_date) == "in_sample"
        and namespace_for_signal(first_hold_out_date, EVALUATION_WINDOW_END) == "hold_out"
    ):
        status |= _ok("a pair inside either side is assigned, not purged — the purge is not swallowing everything")
    else:
        status |= _fail("a pair wholly inside one side was purged")
    return status


def gate(conn: psycopg.Connection[tuple]) -> int:
    """The promotion refusal, against today's real universe and corpus."""
    print("\n── PROMOTION GATE, TODAY'S REAL STATE ──────────────────────────")
    status = 0
    universe = frozenset(load_validated_universe(conn))
    evaluated = frozenset(
        int(row[0])
        for row in conn.execute(
            "SELECT DISTINCT s.instrument_id FROM research_price_series s WHERE s.instrument_id = ANY(%(ids)s)",
            {"ids": list(universe)},
        ).fetchall()
    )
    print(f"   validated universe {len(universe):,} · corpus∩universe {len(evaluated):,}")

    identity = ResultIdentity(
        strategy_id="S-1",
        strategy_version="strategy-registry-v1+unmeasured",
        result_scope="sleeve",
        namespace="hold_out",
        ambiguity_arm="worst_case",
        quarantine_arm="masked",
        sizing_rule=SIZING_RULE,
        benchmark_rule=BENCHMARK_RULE,
        cost_model_id=COST_MODEL_ID,
        corpus_version=CORPUS_VERSION,
        window_start=EVALUATION_WINDOW_START,
        window_end=EVALUATION_WINDOW_END,
        position_rule_set_version="position-builder-v1+unmeasured",
        outcome_rule_set_version="outcome-resolver-v1+unmeasured",
        input_rule_set_version="price-quarantine-v1+unmeasured",
        return_basis=TOTAL_RETURN_BASIS,
    )
    # ⚠ `carry_unmodelled` is read from `cost_model` HERE — this is the WRITER's
    # position, which is the one place it is correct to. The gate reads the
    # stored value, never the module (see the field's comment).
    today = PromotionCandidate(
        result=StrategyResult(
            identity=identity,
            purpose="capital_candidate",
            metrics=_metrics(effective_sample_size=None),
            universe_basis="survivor_only",
            carry_unmodelled=CARRY_UNMODELLED,
            fx_unmodelled=FX_UNMODELLED,
            evaluated_instrument_count=len(evaluated),
        ),
        evaluated_instrument_ids=evaluated,
        validated_universe_ids=universe,
    )
    refusals = check_promotable(today)
    print(f"   result_version = {identity.version}")
    for reason in refusals:
        print(f"      refused: {reason}")
    if refusals:
        status |= _ok(f"today's real state is NOT promotable — {len(refusals)} refusals (§6's initial state)")
    else:
        status |= _fail("today's real state was reported PROMOTABLE — the gate is not failing closed")

    # ⚠ The arm a refuses-everything gate would also pass. Without it, deleting
    # the whole function body and returning a constant would look identical.
    clean = PromotionCandidate(
        result=StrategyResult(
            identity=identity,
            purpose="capital_candidate",
            metrics=_metrics(effective_sample_size=128.5),
            universe_basis="survivorship_free",
            carry_unmodelled=False,
            fx_unmodelled=False,
            evaluated_instrument_count=len(evaluated),
            trial_count=41,
            deflated_sharpe=Decimal("0.31"),
        ),
        evaluated_instrument_ids=evaluated,
        validated_universe_ids=universe,
        holdout_evaluations=1,
        recorded_accesses=1,
        ambiguity_material=False,
    )
    clean_refusals = check_promotable(clean)
    if not clean_refusals:
        status |= _ok(f"a fully-satisfied candidate over the same {len(evaluated):,} instruments CLEARS")
    else:
        status |= _fail(f"a fully-satisfied candidate was refused: {clean_refusals}")

    outside = [int(row[0]) for row in conn.execute(_RESOLVED_OUTSIDE_UNIVERSE_SQL, {"ids": list(universe)}).fetchall()]
    print(f"   corpus series resolving to an instrument OUTSIDE the §4.0 universe: {len(outside):,}")
    if not outside:
        status |= _fail(
            "no resolved-but-outside instrument exists on this corpus, so the universe clause cannot be "
            "exercised on real data — state this rather than reporting the arm as passed"
        )
    else:
        contaminated = PromotionCandidate(
            result=clean.result,
            evaluated_instrument_ids=evaluated | {outside[0]},
            validated_universe_ids=universe,
            holdout_evaluations=1,
            recorded_accesses=1,
            ambiguity_material=False,
        )
        contaminated_refusals = check_promotable(contaminated)
        if contaminated_refusals == ("instrument_outside_validated_universe",):
            status |= _ok(
                f"one real out-of-universe instrument ({outside[0]}) added to {len(evaluated):,} clean ones "
                "is caught, and is the ONLY refusal"
            )
        else:
            status |= _fail(f"an out-of-universe instrument produced {contaminated_refusals} instead of the one code")
    return status


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--frozen", action="store_true", help="re-derive the corpus identity and the split point")
    parser.add_argument("--split", action="store_true", help="the split primitives over every date and bar pair")
    parser.add_argument("--gate", action="store_true", help="the promotion refusal against today's real state")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()
    if not (args.frozen or args.split or args.gate or args.all):
        parser.error("pick at least one arm: --frozen, --split, --gate or --all")

    started = time.monotonic()
    status = 0
    with psycopg.connect(settings.database_url) as conn:
        if args.frozen or args.all:
            status |= frozen(conn)
        if args.split or args.all:
            status |= split(conn)
        if args.gate or args.all:
            status |= gate(conn)
    print(f"\n{'PASS' if status == 0 else 'FAIL'}   ({time.monotonic() - started:.1f}s)")
    return status


if __name__ == "__main__":
    sys.exit(main())
