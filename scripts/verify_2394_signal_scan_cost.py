"""Full-population measurement for the daily signal scan (#2394 §3.1).

    PYTHONPATH=. uv run python scripts/verify_2394_signal_scan_cost.py --all

⚠ NOTHING IS WRITTEN. Every arm reads and prints. Gate on the EXIT CODE — 0
means every arm passed, 1 means at least one did not.

⚠ NEVER PIPE THIS INTO ``head``/``tail``. A pipe returns the pipe's status, so a
failure reads as success, and it buffers the progress lines a multi-minute run
is judged by (`.claude/CLAUDE.md`).

⚠⚠ THIS READS THE **LIVE** CORPUS (``price_daily``), NOT THE RESEARCH CORPUS.
Every phase-5 figure on this epic was measured on ``research_price_daily``,
which is a frozen archive keyed on ``series_id``. A daily scan cannot run on it:
``--population`` prints both corpora's last bar and the share of research series
that even carry an ``instrument_id``. The strategy code is corpus-agnostic; the
LOADER is not.

⚠ IT LOADS THROUGH ``app.services.price_masked_bars``, THE SHIPPED LOADER. This
script originally carried its own copy — the prototype the spec was measured from
— and the spec said so: *"``_load_live_masked`` is the measured prototype, not the
shipping code."* The shipping code now exists (#2394 §3.1), and a second copy of a
masking rule is a place for the two to drift, which would make every figure below
a measurement of the script rather than of the scan.

FOUR ARMS, MEASURING DIFFERENT THINGS
-------------------------------------
``--population`` — who a daily scan would cover, on the live corpus, today:
validated-universe size, how many carry bars, quarantine coverage at the current
rule-set version (the loader is fail-closed, so an uncovered instrument is an
excluded one), per-instrument depth, and the last-bar-date histogram. That last
one is §4 question 3 — *"after the candle refresh" must name a concrete
condition* — and the histogram is the reason it cannot be ``max(price_date)``.

``--arrears`` — ⚠⚠ THE CLAIM MOST WORTH FALSIFYING, and the one that decides the
job's shape. The final bar of any series has no ``t+1``, so no decision on it can
be filled, and ``strategy_registry.evaluate`` stamps it ``not_evaluable`` /
``no_fill_bar``. ``sql/255`` carries no ``ON CONFLICT``, so that row is
**terminal**: when tomorrow's bar arrives, the corrected row collides on
``(strategy_id, strategy_version, instrument_id, signal_bar_date, signal_kind)``
and raises. This arm computes each series' verdict at its SECOND-TO-LAST bar
twice — with the next bar present (a one-day-arrears scan) and without it (a
same-day scan) — and counts the disagreement. ⚠ Its first version asked
``resolve_fills`` instead and measured 0 of 0, because the refusal happens one
layer up; see the function's own docstring.

``--cost`` — §4 question 4: *"Cost of a full daily scan is unmeasured, and
§3.1's 'cheap' is an assertion until it is."* Streams every validated-universe
instrument through the masked loader and runs all four manifest entries over the
full history, timed. Reports the per-strategy verdict census at the frontier
date — the daily WRITE volume, which is also §4 question 5's observability
denominator.

⚠ S-2 IS STAGED, NOT PANEL-MATERIALISED. ``evaluate_cross_sectional`` holds every
member's whole score series in memory at once, which §2 of the spec calls
explicitly unsafe at corpus scale. A daily scan needs ONE date's cross-section,
so this arm keeps only the frontier date's score per member and calls
``s2_select`` once. That is a finding, not an optimisation: the panel a daily
scan needs is a slice, not the corpus.

``--truncation`` — §3.1.2 item 1: *"Strategy functions emit full-series verdicts,
not a single bar. The runner either gains single-date entry points or recomputes
history and filters — decide, and state which."* ``rsi_series`` and
``atr_series`` are **Wilder-smoothed from the series start** (recursive, infinite
memory — ``indicator_series`` says so in ``atr_window_series``' docstring and
measures the gap on a 36-bar fixture). So a trailing-K-bar recompute is a
DIFFERENT function, not a cheaper one. This arm runs both and counts how many
instruments disagree at the frontier bar, per K, per strategy. A non-zero count
settles the question by measurement rather than by argument.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from datetime import date
from typing import Any

import psycopg

from app.config import settings
from app.services.cost_model import COST_MODEL_ID
from app.services.indicator_series import BarSeries
from app.services.price_masked_bars import (
    MASKED_REASON,
    QUARANTINE_RULE_SET_VERSION,
    load_masked_bars,
)
from app.services.signal_ledger import resolve_fills
from app.services.strategies.s2_cross_sectional_momentum import S2_STRATEGY_ID
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_manifest import STRATEGY_MANIFEST, StrategyEntry

#: The live corpus is today's tradable list, so every figure here inherits the
#: survivorship label #2288 put on the research one. ``instrument_universe_
#: membership`` (#2290) is the fix and is empty by design until the next
#: ``nightly_universe_sync`` — ``--population`` prints its row count so the
#: claim is checked rather than repeated.
UNIVERSE = "survivor_only"

#: Trailing windows the truncation arm compares against full history. 250 ≈ one
#: trading year (S-1 needs ``sma_200``, so anything shorter cannot evaluate it at
#: all and the comparison would be against a refusal); 750 ≈ three.
TRUNCATION_WINDOWS = (250, 750)


def _tail(series: BarSeries, window: int) -> BarSeries:
    """The last ``window`` bars, as its own series."""
    return BarSeries(dates=series.dates[-window:], rows=series.rows[-window:])


def _per_series_entries() -> list[StrategyEntry]:
    """Every ``per_series`` manifest entry, in id order.

    ⚠ Read from the manifest rather than imported by name — that asymmetry is
    the defect #2394 §2 closed, and re-opening it in the script that measures
    the runner would be the same mistake one layer out.
    """
    return [entry for _, entry in sorted(STRATEGY_MANIFEST.items()) if entry.strategy_class == "per_series"]


def _cross_sectional_entry() -> StrategyEntry:
    entry = STRATEGY_MANIFEST[S2_STRATEGY_ID]
    assert entry.member is not None and entry.select is not None and entry.min_participants is not None
    return entry


def _universe_with_bars(conn: psycopg.Connection[Any]) -> list[int]:
    """Validated-universe ids that carry at least one bar inside a covered range.

    ⚠ The predicate is the LOADER's, not ``EXISTS (price_daily)``. An instrument
    whose bars all sit outside its quarantine coverage loads as zero bars, and
    counting it as covered would report a population the scan does not reach.
    """
    ids = load_validated_universe(conn)
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS _scan_uni (instrument_id BIGINT PRIMARY KEY)")
    conn.execute("TRUNCATE _scan_uni")
    with conn.cursor().copy("COPY _scan_uni (instrument_id) FROM STDIN") as copy:
        for instrument_id in ids:
            copy.write_row((instrument_id,))
    rows = conn.execute(
        """
        SELECT u.instrument_id
        FROM _scan_uni u
        WHERE EXISTS (
            SELECT 1 FROM price_daily d
            JOIN price_quarantine_coverage cov
              ON cov.instrument_id = d.instrument_id
             AND cov.rule_set_version = %(v)s
             AND d.price_date BETWEEN cov.first_bar AND cov.last_bar
            WHERE d.instrument_id = u.instrument_id
        )
        ORDER BY u.instrument_id
        """,
        {"v": QUARANTINE_RULE_SET_VERSION},
    ).fetchall()
    return [int(row[0]) for row in rows]


def population() -> bool:
    """Who the scan covers, and on which corpus."""
    print("POPULATION — the live corpus, today")
    with psycopg.connect(settings.database_url) as conn:
        live = conn.execute(
            "SELECT count(*), count(DISTINCT instrument_id), max(price_date) FROM price_daily"
        ).fetchone()
        research = conn.execute(
            "SELECT count(*), count(DISTINCT series_id), max(bar_date) FROM research_price_daily"
        ).fetchone()
        mapped = conn.execute("SELECT count(*), count(instrument_id) FROM research_price_series").fetchone()
        assert live is not None and research is not None and mapped is not None
        print(f"  price_daily          bars {live[0]}  instruments {live[1]}  last bar {live[2]}")
        print(f"  research_price_daily bars {research[0]}  series {research[1]}  last bar {research[2]}")
        print(f"  research series carrying an instrument_id: {mapped[1]}/{mapped[0]}")

        universe = load_validated_universe(conn)
        with_bars = _universe_with_bars(conn)
        print(f"  validated universe {len(universe)}; loadable through the masked loader {len(with_bars)}")

        # ⚠ Three separate literal statements rather than a loop over a tuple of
        # strings: psycopg 3.3's `execute` is typed on `LiteralString`, and a
        # string bound to a loop variable is no longer one.
        ledger = conn.execute(
            """
            SELECT (SELECT count(*) FROM strategy_signals),
                   (SELECT count(*) FROM strategy_outcomes),
                   (SELECT count(*) FROM instrument_universe_membership)
            """
        ).fetchone()
        assert ledger is not None
        print(f"  strategy_signals {ledger[0]} rows; strategy_outcomes {ledger[1]} rows")
        print(f"  instrument_universe_membership {ledger[2]} rows (#2290 — empty until the next universe sync)")

        # ⚠⚠ BOTH QUERIES BELOW GO THROUGH THE COVERAGE JOIN, not through raw
        # `price_daily`. An earlier version joined `_scan_uni` to the raw table
        # and called the result "the loadable universe" — which counts bars the
        # fail-closed loader does not return, so the depth and the histogram
        # would describe a population the scan never sees. Caught by Codex at
        # checkpoint 1.
        print("  last-bar-date histogram over the LOADABLE universe (top 6):")
        hist = conn.execute(
            """
            SELECT last_bar::text, count(*) FROM (
                SELECT d.instrument_id, max(d.price_date) AS last_bar
                FROM price_daily d
                JOIN _scan_uni u ON u.instrument_id = d.instrument_id
                JOIN price_quarantine_coverage cov
                  ON cov.instrument_id = d.instrument_id
                 AND cov.rule_set_version = %(v)s
                 AND d.price_date BETWEEN cov.first_bar AND cov.last_bar
                GROUP BY 1
            ) t GROUP BY 1 ORDER BY 2 DESC LIMIT 6
            """,
            {"v": QUARANTINE_RULE_SET_VERSION},
        ).fetchall()
        for last_bar, count in hist:
            print(f"    {last_bar}: {count}")
        depth = conn.execute(
            """
            SELECT min(n), percentile_disc(0.5) WITHIN GROUP (ORDER BY n), max(n), sum(n),
                   count(*) FILTER (WHERE n < 200),
                   round(100.0 * count(*) FILTER (WHERE n < 200) / count(*), 1)
            FROM (
                SELECT count(*) AS n
                FROM price_daily d
                JOIN _scan_uni u ON u.instrument_id = d.instrument_id
                JOIN price_quarantine_coverage cov
                  ON cov.instrument_id = d.instrument_id
                 AND cov.rule_set_version = %(v)s
                 AND d.price_date BETWEEN cov.first_bar AND cov.last_bar
                GROUP BY d.instrument_id
            ) t
            """,
            {"v": QUARANTINE_RULE_SET_VERSION},
        ).fetchone()
        assert depth is not None
        print(f"  bars per instrument: min {depth[0]}  median {depth[1]}  max {depth[2]}  total {depth[3]}")
        research_depth = float(research[0]) / float(research[1]) if research[1] else 0.0
        print(f"  research bars per series for contrast: {research[0]}/{research[1]} = {research_depth:,.0f}")
        print(f"  instruments with fewer than 200 bars (sma_200 cannot evaluate): {depth[4]} ({depth[5]}%)")
    return True


def _frontier_date(series_by_id: dict[int, BarSeries]) -> date:
    """The modal last bar across the loaded population.

    ⚠ NOT ``max``. The histogram in ``--population`` is why: a handful of
    instruments carry a bar the rest of the corpus does not have yet, so a scan
    keyed on the maximum would evaluate a date most of the universe is missing
    and manufacture thousands of refusals out of a refresh still in flight.

    ⚠ Ties break on the LATER date, and the modal SHARE is printed rather than
    assumed — the spec's completeness condition is a floor on that share, so a
    reader has to be able to see what it is on the day.
    """
    counts = Counter(series.dates[-1] for series in series_by_id.values() if len(series))
    best = max(counts.items(), key=lambda item: (item[1], item[0]))
    total = sum(counts.values())
    print(f"  frontier {best[0]} held by {best[1]}/{total} loadable series ({100.0 * best[1] / max(total, 1):.1f}%)")
    return best[0]


def _run_population(conn: psycopg.Connection[Any], instrument_ids: list[int]) -> dict[int, BarSeries]:
    loaded: dict[int, BarSeries] = {}
    started = time.monotonic()
    for n, instrument_id in enumerate(instrument_ids, start=1):
        series = load_masked_bars(conn, instrument_id).series
        if len(series):
            loaded[instrument_id] = series
        if n % 1000 == 0:
            print(f"  loaded {n}/{len(instrument_ids)} ({time.monotonic() - started:.0f}s)", flush=True)
    print(
        f"  loaded {len(loaded)} series, {sum(len(s) for s in loaded.values())} bars "
        f"in {time.monotonic() - started:.1f}s",
        flush=True,
    )
    return loaded


def arrears() -> bool:
    """What a scan-of-today writes down that a scan-of-yesterday would not.

    ⚠⚠ AN EARLIER VERSION OF THIS ARM MEASURED THE WRONG LAYER AND ITS PASS WAS
    VACUOUS. It asked ``resolve_fills`` how many FIRED last-bar signals it
    downgraded, and got 0 of 0 — because the strategy never emits ``fired``
    there in the first place. ``strategy_registry.evaluate`` stamps the final bar
    ``not_evaluable`` / ``no_fill_bar`` before the writer ever sees it, and
    ``resolve_fills``' own docstring says so (*"a no-op on the normal path"*).
    A 0-of-0 comparison proves nothing, which is the repo's probe rule applied to
    a measurement.

    The question a scan actually faces is a DIFFERENT one: what does bar ``D``'s
    verdict become once ``D+1`` exists? So this arm computes each series' verdict
    at its SECOND-TO-LAST bar twice — once over the full series (bar ``D+1`` is
    present, which is what a scan running one day in arrears sees) and once over
    the series truncated so that bar is last (what a same-day scan sees). Every
    row where the two differ is a decision a same-day scan writes down wrongly,
    and ``sql/255`` has no ``ON CONFLICT``, so it can never be corrected.
    """
    print("ARREARS — what a same-day scan records that a one-day-arrears scan would not")
    with psycopg.connect(settings.database_url) as conn:
        instrument_ids = _universe_with_bars(conn)
        loaded = _run_population(conn, instrument_ids)

    frontier = _frontier_date(loaded)
    print(f"  frontier (modal last) bar date: {frontier}")

    compared: Counter[str] = Counter()
    differ: Counter[str] = Counter()
    lost_fired: Counter[str] = Counter()
    transitions: Counter[tuple[str, str, str]] = Counter()
    started = time.monotonic()
    for entry in _per_series_entries():
        assert entry.signals is not None
        for series in loaded.values():
            if len(series) < 2:
                continue
            index = len(series) - 2
            same_day = _tail(series, len(series) - 1)
            same_day_index = len(same_day) - 1
            with_next = {
                (s.kind, s.verdict, s.reason)
                for s in entry.signals(series, universe=UNIVERSE, masked_reason=MASKED_REASON, market=None)
                if s.signal_index == index
            }
            without = {
                (s.kind, s.verdict, s.reason)
                for s in entry.signals(same_day, universe=UNIVERSE, masked_reason=MASKED_REASON, market=None)
                if s.signal_index == same_day_index
            }
            compared[entry.strategy_id] += len(with_next)
            for kind, verdict, reason in sorted(with_next):
                match = [row for row in without if row[0] == kind]
                got = match[0] if match else ("", "absent", None)
                if (verdict, reason) == (got[1], got[2]):
                    continue
                differ[entry.strategy_id] += 1
                transitions[(entry.strategy_id, f"{verdict}/{reason or '-'}", f"{got[1]}/{got[2] or '-'}")] += 1
                if verdict == "fired":
                    lost_fired[entry.strategy_id] += 1

    print("  per strategy, at each series' SECOND-TO-LAST bar:")
    total_compared = total_differ = total_lost = 0
    for entry in _per_series_entries():
        key = entry.strategy_id
        print(f"    {key}: compared {compared[key]}, differ {differ[key]}, of which fired-with-next {lost_fired[key]}")
        total_compared += compared[key]
        total_differ += differ[key]
        total_lost += lost_fired[key]
    print("  transitions (verdict with D+1 present -> verdict without it):")
    for (key, was, now), count in sorted(transitions.items(), key=lambda item: -item[1]):
        print(f"    {key}: {was} -> {now}: {count}")
    print(
        f"  TOTAL compared {total_compared}, differ {total_differ}, fired-and-lost {total_lost} "
        f"({time.monotonic() - started:.1f}s)"
    )
    if not total_compared:
        print("  ⚠⚠ nothing compared — this arm measured no population and its verdict is worthless")
        return False
    if not total_differ:
        print("  ⚠ no difference measured; the arrears argument does NOT hold on this corpus")
        return False
    print("  ⚠⚠ a same-day scan records a different verdict for the same bar, and store_signals has no")
    print("     ON CONFLICT, so the corrected row can never be written. The scan must run in ARREARS.")
    return True


def cost() -> bool:
    """§4 q4 — what a full daily scan actually costs, and what it writes.

    ⚠ THE CENSUS IS TAKEN AT THE WRITE DATE, WHICH IS THE FRONTIER MINUS ONE BAR,
    not at the frontier. An earlier version censused the frontier itself and
    reported 5,783 ``no_fill_bar`` rows per leg — structurally guaranteed, since
    the frontier IS the last bar and no decision there can be filled. It measured
    the refusal, not the scan. ``--arrears`` is the arm that establishes the
    offset; this one reports what the job actually stores.
    """
    print("COST — full-history recompute over the live validated universe")
    with psycopg.connect(settings.database_url) as conn:
        instrument_ids = _universe_with_bars(conn)
        loaded = _run_population(conn, instrument_ids)

    frontier = _frontier_date(loaded)
    at_frontier = {i: s for i, s in loaded.items() if s.dates[-1] == frontier and len(s) >= 2}
    bars = sum(len(series) for series in loaded.values())
    write_dates = Counter(series.dates[-2] for series in at_frontier.values())
    write_date = write_dates.most_common(1)[0][0]
    print(f"  frontier bar date {frontier}; {len(loaded)} series, {bars} bars")
    print(f"  write date = frontier - 1 bar; series eligible to be written {len(at_frontier)}")
    # ⚠ The write date is PER INSTRUMENT — the bar before the frontier on that
    # instrument's own calendar — so it is a distribution, not a constant, and a
    # watermark keyed on one date has to answer to this spread.
    spread = ", ".join(f"{when.isoformat()}={n}" for when, n in write_dates.most_common(5))
    print(f"  write-date distribution across eligible series: {spread}")

    # ⚠⚠ THE CENSUS RUNS THROUGH `resolve_fills`, NOT OFF THE RAW VERDICTS.
    # A `fired` verdict whose t+1 open is absent or non-positive is STORED as
    # `not_evaluable` / `unusable_fill_price` (#2354), so a census of strategy
    # output is not a census of the ledger. Caught by Codex at checkpoint 1.
    verdicts: Counter[tuple[str, str, str, str]] = Counter()
    per_strategy_seconds: dict[str, float] = {}
    for entry in _per_series_entries():
        assert entry.signals is not None
        identity = entry.identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)
        started = time.monotonic()
        for instrument_id, series in loaded.items():
            signals = entry.signals(series, universe=UNIVERSE, masked_reason=MASKED_REASON, market=None)
            if instrument_id not in at_frontier:
                continue
            rows = resolve_fills(signals, series=series, identity=identity, instrument_id=instrument_id)
            target = series.dates[-2]
            for row in rows:
                if row.signal_bar_date != target:
                    continue
                verdicts[(entry.strategy_id, row.signal_kind, row.verdict, row.not_evaluable_reason or "")] += 1
        per_strategy_seconds[entry.strategy_id] = time.monotonic() - started
        print(f"  {entry.strategy_id}: {per_strategy_seconds[entry.strategy_id]:.1f}s", flush=True)

    s2 = _cross_sectional_entry()
    assert s2.member is not None and s2.select is not None and s2.min_participants is not None
    started = time.monotonic()
    # ⚠⚠ THE REBALANCE CALENDAR IS THE PANEL'S UNION, NOT EACH MEMBER'S OWN.
    # `rebalance_dates` fires on the FIRST bar whose calendar month differs from
    # the previous bar's — the start of the new month, and causal for the reason
    # its docstring gives: the last session of a month is not knowable at that
    # session. Given a per-member calendar it fires on that member's own first
    # bar of each month, so members that resumed on different days rebalance on
    # different dates and the cross-section collapses. An earlier version of this
    # arm did exactly that and reported `thin_cross_section (1 < 10)`, which is a
    # measurement of the bug rather than of S-2.
    union = sorted({when for series in loaded.values() for when in series.dates})
    panel_dates = s2.decision_calendar(union)
    assert panel_dates is not None
    scores: dict[int, float] = {}
    s2_participants = 0
    s2_rows = 0
    for instrument_id, series in loaded.items():
        member = s2.member(series, panel_decision_dates=panel_dates, universe=UNIVERSE, masked_reason=MASKED_REASON)
        if instrument_id not in at_frontier:
            continue
        index = len(series) - 2
        # ⚠ EVERY member gets a row at the write date, decision bar or not.
        # `CrossSectionalMember`: "Everything else is an ordinary not_fired …
        # It is a verdict, not an absence." So S-2's write volume is the whole
        # eligible population on every scan day, not just on a rebalance.
        s2_rows += 1
        if index not in member.decision_indices:
            continue
        s2_participants += 1
        value = member.score.values[index]
        if value is not None:
            scores[instrument_id] = value
    s2_seconds = time.monotonic() - started
    span_years = (union[-1] - union[0]).days / 365.25 if len(union) > 1 else 0.0
    print(
        f"  s2 panel rebalance dates on the union calendar: {len(panel_dates)} over "
        f"{span_years:.1f} years ({len(panel_dates) / max(span_years, 1e-9):.1f}/year)"
    )
    if len(scores) < s2.min_participants:
        s2_selected = 0
        s2_note = f"thin_cross_section ({len(scores)} < {s2.min_participants})"
    else:
        s2_selected = len(s2.select(write_date, scores))
        s2_note = "selected"
    print(
        f"  {s2.strategy_id}: {s2_seconds:.1f}s, rows at the write date {s2_rows}, "
        f"decision-bar participants {s2_participants}, scored {len(scores)}, {s2_note} {s2_selected}"
    )

    print("  write-date verdict census — the daily write volume, as the ledger would store it:")
    for (key, kind, verdict, reason), count in sorted(verdicts.items()):
        suffix = f" ({reason})" if reason else ""
        print(f"    {key} {kind} {verdict}{suffix}: {count}")
    per_leg: Counter[tuple[str, str]] = Counter()
    for (key, kind, _verdict, _reason), count in verdicts.items():
        per_leg[(key, kind)] += count
    ok = True
    for (key, kind), n in sorted(per_leg.items()):
        if n != len(at_frontier):
            print(f"  ⚠⚠ {key} {kind} censused {n} rows against {len(at_frontier)} eligible series — a leg is short")
            ok = False
    daily_rows = sum(verdicts.values()) + s2_rows
    print(f"  TOTAL rows per scan day {daily_rows} (per-series legs {sum(verdicts.values())} + s2 {s2_rows})")
    print(f"  at 252 trading days that is {daily_rows * 252:,} rows/year at one strategy_version")
    warmup = sum(n for (key, kind, verdict, reason), n in verdicts.items() if reason == "insufficient_warmup")
    print(
        f"  insufficient_warmup rows {warmup} of {sum(verdicts.values())} per-series rows "
        f"({100.0 * warmup / max(sum(verdicts.values()), 1):.1f}%)"
    )
    evaluation = sum(per_strategy_seconds.values()) + s2_seconds
    print(
        f"  strategy evaluation total {evaluation:.1f}s over {bars} bars "
        f"({bars * len(STRATEGY_MANIFEST) / max(evaluation, 1e-9):,.0f} bar-evaluations/s)"
    )
    return ok


def truncation() -> bool:
    """§3.1.2 q1 — is a trailing-window recompute the same function? Measure it."""
    print("TRUNCATION — trailing-window recompute vs full history, at the frontier bar")
    with psycopg.connect(settings.database_url) as conn:
        instrument_ids = _universe_with_bars(conn)
        loaded = _run_population(conn, instrument_ids)

    frontier = _frontier_date(loaded)
    eligible = {i: s for i, s in loaded.items() if s.dates[-1] == frontier}
    print(f"  frontier {frontier}; series ending there {len(eligible)}")

    disagreements: Counter[tuple[int, str]] = Counter()
    compared: Counter[tuple[int, str]] = Counter()
    deeper_than: Counter[int] = Counter()
    started = time.monotonic()
    for entry in _per_series_entries():
        assert entry.signals is not None
        for series in eligible.values():
            index = len(series) - 1
            emitted = entry.signals(series, universe=UNIVERSE, masked_reason=MASKED_REASON, market=None)
            full = {(s.kind, s.verdict, s.reason) for s in emitted if s.signal_index == index}
            for window in TRUNCATION_WINDOWS:
                if len(series) <= window:
                    continue
                deeper_than[window] += 1
                tail = _tail(series, window)
                tail_index = len(tail) - 1
                got = {
                    (s.kind, s.verdict, s.reason)
                    for s in entry.signals(tail, universe=UNIVERSE, masked_reason=MASKED_REASON, market=None)
                    if s.signal_index == tail_index
                }
                compared[(window, entry.strategy_id)] += 1
                if got != full:
                    disagreements[(window, entry.strategy_id)] += 1

    for window in TRUNCATION_WINDOWS:
        deeper = deeper_than[window] // max(len(_per_series_entries()), 1)
        print(f"  window {window} bars — series deeper than it: {deeper}")
        for entry in _per_series_entries():
            key = (window, entry.strategy_id)
            n = compared[key]
            bad = disagreements[key]
            share = f"{100.0 * bad / n:.2f}%" if n else "n/a"
            print(f"    {entry.strategy_id}: {bad}/{n} frontier verdicts differ ({share})")
    print(f"  elapsed {time.monotonic() - started:.1f}s")
    if not sum(disagreements.values()):
        print("  ⚠ NO disagreement measured. That does NOT license truncation — rsi/atr are Wilder-recursive,")
        print("    so equality here is a property of this corpus's depth, not of the functions.")
    else:
        print("  ⚠⚠ a trailing-window recompute is a DIFFERENT function. The scan must recompute from the")
        print("     series start, and identity records no window, so the two cannot be told apart once stored.")
    return True


ARMS = {
    "population": population,
    "arrears": arrears,
    "cost": cost,
    "truncation": truncation,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ARMS:
        parser.add_argument(f"--{name}", action="store_true")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    selected = [name for name in ARMS if getattr(args, name)] or (list(ARMS) if args.all else [])
    if not selected:
        parser.print_help()
        return 2

    ok = True
    for name in selected:
        print()
        ok = ARMS[name]() and ok
    print()
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
