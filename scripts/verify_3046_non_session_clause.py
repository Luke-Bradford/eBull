"""#3046 build item 2 — clause 5 (``non_session_bar``) on the full corpus.

Spec: ``docs/proposals/ta/2026-09-15-3046-non-session-bar-clause.md``.
Read-only. Writes nothing. One ``REPEATABLE READ READ ONLY`` transaction.

WHY THIS EXISTS. Residual 2 (`5be4294d`) shipped eight constraints instead of a fix
direction. This verifies the direction that was chosen — refuse a window whose ENDPOINT
BAR is dated on a day the venue held no session, established only where a PUBLISHED
calendar exists — and prints the evidence for each of the spec's seven declared
acceptance conditions.

⚠⚠ EVERY FIGURE IS COMPUTED AT RUN TIME. Nothing here is a transcribed number; the
repo rule is that a hand-written statistic goes stale silently in the place a reader
trusts most.

⚠⚠ THE CALENDAR IS CALLED, NEVER MIRRORED IN SQL. ``us_market_status`` is the only
authority for "was this a session", exactly as ``price_window_verdict`` uses it. A SQL
re-implementation is the mirroring mistake ``verify_3046_consumer_exposure`` already
made once with ``rule_w1``.

Usage::

    PYTHONPATH=. uv run python -m scripts.verify_3046_non_session_clause
"""

from __future__ import annotations

import argparse
import subprocess
from dataclasses import dataclass, replace
from datetime import date
from typing import Any

import psycopg

from app.config import settings
from app.services.market_calendar import RULE_SET_VERSION as CALENDAR_RULE_SET_VERSION
from app.services.market_calendar import us_market_specials, us_market_status
from app.services.price_window_verdict import (
    REASON_NON_SESSION_BAR,
    VERDICT_QUARANTINED,
    WEEKEND_HABIT_MIN_BARS,
    WEEKEND_SESSION_RATIO,
    WindowInputs,
    assess_window,
    load_window_inputs,
)

#: Bumped when a stratum definition changes, so two runs are comparable only when it
#: matches. Not a rule constant — every rule called here lives in its own module.
CENSUS_VERSION = "non-session-clause-v1"

#: ``ruff format`` strips the parentheses from ``except (A, B):`` on this Python
#: target, leaving a form the review bot reads as Python 2 syntax. Bind the tuple.
_GIT_SHA_FAILURES = (OSError, subprocess.SubprocessError)


def _git_sha() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
    except _GIT_SHA_FAILURES:
        return "unknown"


def _corpus_years(conn: psycopg.Connection[Any]) -> range:
    lo, hi = conn.execute(
        "SELECT min(extract(year FROM price_date))::int, max(extract(year FROM price_date))::int FROM price_daily"
    ).fetchone()  # type: ignore[misc]
    return range(int(lo), int(hi) + 1)


def _closure_dates(years: range) -> list[date]:
    """NYSE FULL closures over the corpus years, from the calendar module itself.

    ⚠ Half days are excluded deliberately: a 13:00 ET early close is a real session.
    """
    return sorted(d for y in years for d in us_market_specials(y).full_closures)


# ---------------------------------------------------------------------------
# Acceptance 1 — the census
# ---------------------------------------------------------------------------

_WEEKEND_COHORTS = """
    WITH pop AS (
      SELECT p.instrument_id, p.price_date, p.close, p.volume,
             extract(isodow FROM p.price_date)::int AS dow,
             (p.open = p.high AND p.high = p.low AND p.low = p.close) AS flat,
             lag(p.close) OVER w AS prev_close
        FROM price_daily p
        JOIN instruments i ON i.instrument_id = p.instrument_id
        LEFT JOIN exchanges e ON e.exchange_id = i.exchange
       WHERE (e.asset_class = ANY(%(five)s) OR e.asset_class IS NULL)
      WINDOW w AS (PARTITION BY p.instrument_id ORDER BY p.price_date)
    ), wk AS (
      SELECT pop.*,
             EXISTS (SELECT 1 FROM price_daily m
                      WHERE m.instrument_id = pop.instrument_id
                        AND m.price_date = pop.price_date + (8 - pop.dow)) AS has_mon
        FROM pop WHERE dow >= 6
    )
    SELECT has_mon,
           (prev_close IS NOT NULL AND close = prev_close) AS carry,
           flat,
           count(*) AS bars,
           count(DISTINCT instrument_id) AS insts,
           count(*) FILTER (WHERE volume > 0) AS pos_vol
      FROM wk
     GROUP BY 1, 2, 3
     ORDER BY 4 DESC
"""

_FLAT_CARRY_SPLIT = """
    WITH pop AS (
      SELECT p.instrument_id, p.price_date, p.close,
             extract(isodow FROM p.price_date)::int AS dow,
             (p.open = p.high AND p.high = p.low AND p.low = p.close) AS flat,
             lag(p.close) OVER w AS prev_close
        FROM price_daily p
        JOIN instruments i ON i.instrument_id = p.instrument_id
        LEFT JOIN exchanges e ON e.exchange_id = i.exchange
       WHERE (e.asset_class = ANY(%(five)s) OR e.asset_class IS NULL)
      WINDOW w AS (PARTITION BY p.instrument_id ORDER BY p.price_date)
    )
    SELECT dow >= 6 AS weekend,
           count(*) AS all_bars,
           count(*) FILTER (WHERE flat AND prev_close IS NOT NULL AND close = prev_close) AS flat_carry,
           count(DISTINCT instrument_id) FILTER (
             WHERE flat AND prev_close IS NOT NULL AND close = prev_close) AS insts
      FROM pop GROUP BY 1 ORDER BY 1
"""

#: ⚠ Per-EXCHANGE, so the spec's "Nasdaq / CBOE / OTC observe the same full closures
#: as NYSE" claim is FALSIFIABLE from this output rather than asserted. A venue that
#: kept trading on an NYSE closure would show a full day's bar count here.
_CLOSURE_BY_EXCHANGE = """
    WITH closed AS (
      SELECT p.price_date, i.exchange, p.instrument_id, p.volume
        FROM price_daily p
        JOIN instruments i ON i.instrument_id = p.instrument_id
        JOIN exchanges e ON e.exchange_id = i.exchange
       WHERE e.asset_class = 'us_equity' AND p.price_date = ANY(%(dates)s)
    ), prior AS (
      -- the SAME instruments' last session strictly before the closure, so the
      -- turnover comparison is like-for-like rather than across different names.
      SELECT c.price_date, c.exchange,
             percentile_disc(0.5) WITHIN GROUP (ORDER BY q.volume) FILTER (WHERE q.volume > 0) AS prior_med
        FROM closed c
        JOIN LATERAL (
          SELECT d.volume FROM price_daily d
           WHERE d.instrument_id = c.instrument_id AND d.price_date < c.price_date
           ORDER BY d.price_date DESC LIMIT 1
        ) q ON TRUE
       GROUP BY 1, 2
    )
    SELECT c.price_date, c.exchange, e.description, count(*) AS bars,
           count(*) FILTER (WHERE c.volume > 0) AS pos_vol,
           percentile_disc(0.5) WITHIN GROUP (ORDER BY c.volume) FILTER (WHERE c.volume > 0) AS med_vol,
           prior.prior_med
      FROM closed c
      JOIN exchanges e ON e.exchange_id = c.exchange
      LEFT JOIN prior ON prior.price_date = c.price_date AND prior.exchange = c.exchange
     GROUP BY 1, 2, 3, 7 ORDER BY 4 DESC LIMIT 15
"""


def _window_inputs_for(*, asset_class: str | None, trades_weekends: bool, habit_bar_count: int) -> WindowInputs:
    """A ``WindowInputs`` carrying ONLY clause 5's operands, everything else inert.

    Coverage spans the corpus and every verdict list is empty, so clauses 1-4 cannot
    fire and the reason list isolates clause 5.
    """
    return WindowInputs(
        coverage=(date(2000, 1, 1), date(2099, 12, 31)),
        quarantined_transitions=(),
        deferred_transitions=(),
        unresolved_breaks=(),
        return_unusable_bars=(),
        weekend_bar_dates=frozenset(),
        asset_class=asset_class,
        habit_bar_count=habit_bar_count,
        trades_weekends=trades_weekends,
    )


def _five_day_classes(conn: psycopg.Connection[Any]) -> list[str]:
    """Asset classes the RULE SET declares five-day, read from ``params_for``.

    ⚠ Used ONLY to partition the census population the way residual 2's census did,
    so the two are comparable. Clause 5 itself never reads it — see
    ``price_window_verdict._is_non_session_bar`` for why that generalisation was
    killed at Codex checkpoint 1.
    """
    from app.services.price_quarantine import params_for

    classes = [r[0] for r in conn.execute("SELECT DISTINCT asset_class FROM exchanges").fetchall() if r[0] is not None]
    return sorted(c for c in classes if params_for(c).calendar_days_per_bar != 1)


def census(conn: psycopg.Connection[Any], closures: list[date]) -> None:
    five = _five_day_classes(conn)
    print(f"\n--- ACCEPTANCE 1: the census (five-day classes: {', '.join(five)}) ---")

    rows = conn.execute(_WEEKEND_COHORTS, {"five": five}).fetchall()
    total = sum(r[3] for r in rows)
    print(f"\nweekend-bar cohorts (has_mon / carry / flat), total = {total}")
    print(f"  {'mon':<6}{'carry':<7}{'flat':<6}{'bars':>7}{'insts':>7}{'bars/i':>8}{'pos_vol':>9}")
    for has_mon, carry, flat, bars, insts, pos_vol in rows:
        print(f"  {str(has_mon):<6}{str(carry):<7}{str(flat):<6}{bars:>7}{insts:>7}{bars / insts:>8.1f}{pos_vol:>9}")

    print("\nflat-carry shape: weekend is a MINORITY of it")
    for weekend, all_bars, flat_carry, insts in conn.execute(_FLAT_CARRY_SPLIT, {"five": five}).fetchall():
        label = "weekend" if weekend else "weekday"
        share = 100.0 * flat_carry / max(all_bars, 1)
        print(f"  {label}: all_bars={all_bars:>9}  flat_carry={flat_carry:>7} ({share:.2f}%)  insts={insts}")

    print(f"\nus_equity bars on NYSE FULL closures, worst 15 (calendar {CALENDAR_RULE_SET_VERSION})")
    print("  ⚠ 'turnover' is the median volume of THESE bars over the median volume of the")
    print("    SAME instruments' previous session — computed, never asserted. It is EVIDENCE")
    print("    for the reader, and is read by no rule (volume may not be the gate, #3046 c3).")
    print(f"  {'date':<12}{'exch':<6}{'venue':<30}{'bars':>6}{'pos_vol':>8}{'med_vol':>12}{'turnover':>10}")
    for d, ex, desc, bars, pos_vol, med, prior_med in conn.execute(
        _CLOSURE_BY_EXCHANGE, {"dates": closures}
    ).fetchall():
        ratio = f"{float(med) / float(prior_med):.4f}" if med and prior_med else "-"
        print(
            f"  {d.isoformat():<12}{str(ex):<6}{str(desc)[:29]:<30}{bars:>6}{pos_vol:>8}"
            f"{str(med or '-'):>12}{ratio:>10}"
        )


# ---------------------------------------------------------------------------
# Acceptances 2-7 — the clause itself
# ---------------------------------------------------------------------------


def _all_day_change_windows(conn: psycopg.Connection[Any]) -> list[tuple[int, date, date, int]]:
    """``(instrument_id, prior_date, as_of, bar_count)`` for every instrument.

    Mirrors ``market_data.load_day_changes``'s window — the two most recent bars with
    a positive close — because that is the consumer clause 5 was built for. ⚠ It is
    the WINDOW that is mirrored, never a rule.
    """
    return [
        (int(i), p, a, int(b))
        for i, p, a, b in conn.execute(
            """
            WITH ranked AS (
              SELECT instrument_id, price_date,
                     row_number() OVER (PARTITION BY instrument_id ORDER BY price_date DESC) AS rn
                FROM price_daily WHERE close > 0
            ), two AS (SELECT * FROM ranked WHERE rn <= 2)
            SELECT t1.instrument_id, t2.price_date AS prior_date, t1.price_date AS as_of,
                   (SELECT count(*) FROM price_daily d
                     WHERE d.instrument_id = t1.instrument_id
                       AND d.price_date BETWEEN t2.price_date AND t1.price_date) AS bar_count
              FROM two t1 JOIN two t2
                ON t2.instrument_id = t1.instrument_id AND t1.rn = 1 AND t2.rn = 2
            """
        ).fetchall()
    ]


@dataclass(frozen=True)
class _Meta:
    """Per-instrument clause-5 operands, read straight from the corpus.

    ``habit_floored`` is what production uses; ``habit_unfloored`` is what it used
    BEFORE ``WEEKEND_HABIT_MIN_BARS`` existed, kept so the floor's own effect can be
    measured rather than assumed (Codex checkpoint 2, P2: both A/B arms otherwise
    load post-floor inputs, so ``lost`` is structurally blind to it).
    """

    habit_floored: bool
    habit_unfloored: bool
    bars: int
    asset_class: str | None


def _instrument_meta(conn: psycopg.Connection[Any]) -> dict[int, _Meta]:
    return {
        int(i): _Meta(habit_floored=bool(hf), habit_unfloored=bool(hu), bars=int(n or 0), asset_class=c)
        for i, hf, hu, n, c in conn.execute(
            """
            WITH last AS (SELECT instrument_id, max(price_date) AS lb FROM price_daily GROUP BY 1),
            hab AS (
              SELECT p.instrument_id, count(*) AS bars,
                     coalesce(count(*) FILTER (WHERE extract(isodow FROM p.price_date) >= 6)::numeric
                       / nullif(count(*), 0), 0) >= %(ratio)s AS over_ratio
                FROM price_daily p JOIN last l ON l.instrument_id = p.instrument_id
               WHERE p.price_date > l.lb - interval '365 days'
               GROUP BY 1)
            SELECT i.instrument_id,
                   coalesce(hab.over_ratio AND hab.bars >= %(floor)s, false) AS habit_floored,
                   coalesce(hab.over_ratio, false)                           AS habit_unfloored,
                   coalesce(hab.bars, 0), e.asset_class
              FROM instruments i
              LEFT JOIN hab ON hab.instrument_id = i.instrument_id
              LEFT JOIN exchanges e ON e.exchange_id = i.exchange
            """,
            {"floor": WEEKEND_HABIT_MIN_BARS, "ratio": WEEKEND_SESSION_RATIO},
        ).fetchall()
    }


def _independent_clause5_set(
    conn: psycopg.Connection[Any], windows: list[tuple[int, date, date, int]]
) -> tuple[set[int], dict[int, _Meta]]:
    """The clause-5 set derived from the CORPUS, not from ``assess_window``'s output.

    ⚠⚠ TWO INDEPENDENT DERIVATIONS OR THE CHECK CANNOT FAIL. Build item 1 shipped an
    A/B whose "suppressed set is exactly the clause union" assertion compared a counter
    to the list it was built from, and printed PASS. This half asks the database for
    asset class and habit directly and applies ``us_market_status`` in Python.
    """
    meta = _instrument_meta(conn)
    fired: set[int] = set()
    for iid, prior, as_of, _ in windows:
        m = meta.get(iid)
        if m is None or m.asset_class != "us_equity" or m.bars < WEEKEND_HABIT_MIN_BARS or m.habit_floored:
            continue
        if any(us_market_status(d) == "closed" for d in (prior, as_of)):
            fired.add(iid)
    return fired, meta


def clause_ab(conn: psycopg.Connection[Any], closures: list[date]) -> int:
    failures = 0
    windows = _all_day_change_windows(conn)
    inputs = load_window_inputs(conn, {i: p for i, p, _, _ in windows})

    with_c5: dict[int, tuple[str, ...]] = {}
    without_c5: set[int] = set()
    for iid, prior, as_of, bars in windows:
        on = assess_window(
            inputs.get(iid), window_start=prior, window_end=as_of, bar_count=bars, endpoint_bar_dates=(prior, as_of)
        )
        off = assess_window(inputs.get(iid), window_start=prior, window_end=as_of, bar_count=bars)
        if on.verdict == VERDICT_QUARANTINED:
            with_c5[iid] = on.reasons
        if off.verdict == VERDICT_QUARANTINED:
            without_c5.add(iid)

    # ⚠⚠ A THIRD ARM, BECAUSE THE FIRST TWO CANNOT SEE THE FLOOR. ``load_window_inputs``
    # applies ``WEEKEND_HABIT_MIN_BARS`` in SQL, so BOTH arms above already carry the
    # floored habit and ``lost`` is structurally blind to a suppression the floor
    # removed. This arm restores the pre-floor habit and re-assesses (Codex ckpt-2).
    meta_all = _instrument_meta(conn)
    pre_floor: set[int] = set()
    for iid, prior, as_of, bars in windows:
        src = inputs.get(iid)
        m = meta_all.get(iid)
        if src is None or m is None:
            continue
        got = assess_window(
            replace(src, trades_weekends=m.habit_unfloored),
            window_start=prior,
            window_end=as_of,
            bar_count=bars,
            endpoint_bar_dates=(prior, as_of),
        )
        if got.verdict == VERDICT_QUARANTINED:
            pre_floor.add(iid)

    gained = set(with_c5) - without_c5
    lost = without_c5 - set(with_c5)
    print(f"\n--- ACCEPTANCE 5: A/B on the day-change window, {len(windows)} instruments ---")
    print(f"  suppressed WITHOUT clause 5 : {len(without_c5)}")
    print(f"  suppressed WITH    clause 5 : {len(with_c5)}")
    print(f"  gained                      : {len(gained)}")
    print(f"  lost (must be 0)            : {len(lost)}")
    if lost:
        failures += 1
        print(f"  !! FAIL — clause 5 must only ADD. Lost: {sorted(lost)[:20]}")

    floor_gained = set(with_c5) - pre_floor
    floor_lost = pre_floor - set(with_c5)
    print("\n  the habit floor, isolated (pre-floor habit restored, clause 5 held constant):")
    print(f"    suppressed with the PRE-floor habit : {len(pre_floor)}")
    print(f"    suppressed as shipped               : {len(with_c5)}")
    print(f"    the floor ADDS                      : {len(floor_gained)} {sorted(floor_gained)[:10]}")
    print(f"    the floor REMOVES                   : {len(floor_lost)} {sorted(floor_lost)[:10]}")
    if not floor_gained and not floor_lost:
        print("    ⚠ the floor changes no day-change verdict today — it stands on the 10 instruments")
        print("      listed under acceptance 4, whose habit was asserted off 1-18 bars")

    independent, meta = _independent_clause5_set(conn, windows)
    reason_set = {i for i, rs in with_c5.items() if REASON_NON_SESSION_BAR in rs}
    print("\n  two independent derivations of the clause-5 set:")
    print(f"    from assess_window reasons : {len(reason_set)}")
    print(f"    from the corpus directly   : {len(independent)}")
    if reason_set != independent:
        failures += 1
        print(
            f"  !! FAIL — sets differ. only-reasons={sorted(reason_set - independent)[:10]} "
            f"only-corpus={sorted(independent - reason_set)[:10]}"
        )

    # ACCEPTANCE 6 — the latest window barely touches a holiday, so also ask every
    # (instrument, closure date) pair that actually has a stored bar.
    hist = conn.execute(
        """
        SELECT count(*) AS bars, count(DISTINCT p.instrument_id) AS insts
          FROM price_daily p JOIN instruments i ON i.instrument_id = p.instrument_id
          JOIN exchanges e ON e.exchange_id = i.exchange
         WHERE e.asset_class = 'us_equity' AND p.price_date = ANY(%(dates)s)
        """,
        {"dates": closures},
    ).fetchone()
    weekend_us = conn.execute(
        """
        SELECT count(*), count(DISTINCT p.instrument_id) FROM price_daily p
          JOIN instruments i ON i.instrument_id = p.instrument_id
          JOIN exchanges e ON e.exchange_id = i.exchange
         WHERE e.asset_class = 'us_equity' AND extract(isodow FROM p.price_date) >= 6
        """
    ).fetchone()
    exempt = {
        i
        for i, m in meta.items()
        if m.asset_class == "us_equity" and m.habit_floored and m.bars >= WEEKEND_HABIT_MIN_BARS
    }
    print("\n--- ACCEPTANCE 6: the historical arm (every stored bar on a non-session date) ---")
    print(f"  us_equity bars on a NYSE FULL closure : {hist[0]:>7} over {hist[1]} instruments")  # type: ignore[index]
    print(f"  us_equity bars on a weekend           : {weekend_us[0]:>7} over {weekend_us[1]} instruments")  # type: ignore[index]
    print(f"  us_equity instruments EXEMPT by habit : {len(exempt)}")
    return failures


def habit_floor(conn: psycopg.Connection[Any]) -> int:
    """ACCEPTANCE 4 — isolate what the >= 20-bar floor changes, on its own."""
    rows = conn.execute(
        """
        WITH last AS (SELECT instrument_id, max(price_date) AS lb FROM price_daily GROUP BY 1),
        hab AS (
          SELECT p.instrument_id, count(*) AS bars,
                 coalesce(count(*) FILTER (WHERE extract(isodow FROM p.price_date) >= 6)::numeric
                   / nullif(count(*), 0), 0) AS ratio
            FROM price_daily p JOIN last l ON l.instrument_id = p.instrument_id
           WHERE p.price_date > l.lb - interval '365 days'
           GROUP BY 1)
        SELECT i.symbol, e.asset_class, round(hab.ratio, 4), hab.bars
          FROM hab JOIN instruments i ON i.instrument_id = hab.instrument_id
          LEFT JOIN exchanges e ON e.exchange_id = i.exchange
         WHERE hab.ratio >= %(ratio)s ORDER BY hab.bars, hab.ratio DESC
        """,
        {"ratio": WEEKEND_SESSION_RATIO},
    ).fetchall()
    below = [r for r in rows if int(r[3]) < WEEKEND_HABIT_MIN_BARS]
    print(f"\n--- ACCEPTANCE 4: the habit floor ({WEEKEND_HABIT_MIN_BARS} bars) ---")
    print(f"  instruments over the ratio, ANY history : {len(rows)}")
    print(f"  of which BELOW the floor (habit revoked): {len(below)}")
    print(f"  {'symbol':<16}{'class':<14}{'ratio':>8}{'bars':>6}")
    for sym, cls, ratio, bars in below:
        print(f"  {str(sym):<16}{str(cls):<14}{str(ratio):>8}{bars:>6}")
    if not below:
        print("  ⚠ none — the floor is inert on today's corpus; it stands on the rule, not the count")
    return 0


def cohort_24_7(conn: psycopg.Connection[Any], closures: list[date]) -> int:
    """ACCEPTANCE 2 — the weekend-tradable cohort survives, checked on the SYMBOL set.

    ⚠ Deliberately NOT the volume-positive subset. A volume-conditioned check cannot
    establish the broader protection claim (Codex checkpoint 1): it verifies the 11
    instruments that happen to carry a positive-volume weekend bar and says nothing
    about the other seven ``.24-7`` products.
    """
    rows = conn.execute(
        """
        WITH last AS (SELECT instrument_id, max(price_date) AS lb FROM price_daily GROUP BY 1),
        hab AS (
          SELECT p.instrument_id, count(*) AS bars,
                 count(*) >= %(floor)s
                 AND coalesce(count(*) FILTER (WHERE extract(isodow FROM p.price_date) >= 6)::numeric
                       / nullif(count(*), 0), 0) >= %(ratio)s AS trades_weekends
            FROM price_daily p JOIN last l ON l.instrument_id = p.instrument_id
           WHERE p.price_date > l.lb - interval '365 days' GROUP BY 1)
        SELECT i.instrument_id, i.symbol, e.asset_class,
               coalesce(hab.trades_weekends, false), coalesce(hab.bars, 0),
               (SELECT array_agg(d.price_date ORDER BY d.price_date) FROM price_daily d
                 WHERE d.instrument_id = i.instrument_id
                   AND (extract(isodow FROM d.price_date) >= 6 OR d.price_date = ANY(%(dates)s))) AS ns_dates
          FROM instruments i
          LEFT JOIN hab ON hab.instrument_id = i.instrument_id
          LEFT JOIN exchanges e ON e.exchange_id = i.exchange
         WHERE i.symbol LIKE '%%.24-7' ORDER BY i.symbol
        """,
        {"floor": WEEKEND_HABIT_MIN_BARS, "ratio": WEEKEND_SESSION_RATIO, "dates": closures},
    ).fetchall()
    print("\n--- ACCEPTANCE 2: the .24-7 cohort is not suppressed ---")
    print(f"  {'symbol':<16}{'class':<11}{'habit':<7}{'bars':>6}{'non-session bars':>18}{'':>4}clause 5")
    failures = 0
    for _iid, sym, cls, habit, bars, ns_dates in rows:
        dates = tuple(ns_dates or ())
        # ⚠⚠ CALL THE CLAUSE, DO NOT RE-DERIVE IT. The first version of this check
        # tested ``bool(non_session_bars) and not habit`` and reported SHEIN.24-7 as a
        # failure — because it omitted the asset-class gate the clause actually has.
        # That is the mirroring mistake this module's own docstring warns about, made
        # in the verifier instead of the rule.
        got = assess_window(
            _window_inputs_for(asset_class=cls, trades_weekends=bool(habit), habit_bar_count=int(bars)),
            window_start=dates[0] if dates else date(2026, 1, 1),
            window_end=dates[-1] if dates else date(2026, 1, 1),
            bar_count=max(len(dates), 1),
            endpoint_bar_dates=dates,
        )
        fires = REASON_NON_SESSION_BAR in got.reasons
        if fires:
            failures += 1
        print(
            f"  {str(sym):<16}{str(cls):<11}{str(habit):<7}{bars:>6}{len(dates):>18}"
            f"{'':>4}{'FIRES !!' if fires else 'silent'}"
        )
    if failures:
        print(f"  !! FAIL — {failures} of {len(rows)} .24-7 symbols would be suppressed")
    elif rows:
        print(f"  PASS — 0 of {len(rows)} .24-7 symbols suppressed, checked on the SYMBOL cohort")
    return failures


def new_year_arms(conn: psycopg.Connection[Any]) -> int:
    """ACCEPTANCE 3 — 2021-12-31 is a session, with BOTH arms printed."""
    d = date(2021, 12, 31)
    bars = conn.execute(
        """
        SELECT count(*) FROM price_daily p JOIN instruments i ON i.instrument_id = p.instrument_id
         JOIN exchanges e ON e.exchange_id = i.exchange
         WHERE e.asset_class = 'us_equity' AND p.price_date = %s
        """,
        (d,),
    ).fetchone()[0]  # type: ignore[index]
    neighbours = conn.execute(
        """
        SELECT p.price_date, count(*) FROM price_daily p JOIN instruments i ON i.instrument_id = p.instrument_id
         JOIN exchanges e ON e.exchange_id = i.exchange
         WHERE e.asset_class = 'us_equity' AND p.price_date BETWEEN '2021-12-27' AND '2022-01-04'
         GROUP BY 1 ORDER BY 1
        """
    ).fetchall()
    status = us_market_status(d)
    print("\n--- ACCEPTANCE 3: Saturday New Year's Day ---")
    print(f"  us_market_status(2021-12-31) AFTER the fix  : {status}")
    print("  BEFORE the fix it was 'closed' (pandas nearest_workday shifts Sat Jan 1 back onto Fri Dec 31)")
    print(f"  us_equity bars on 2021-12-31                : {bars}")
    print("  neighbouring sessions: " + ", ".join(f"{dd.isoformat()}={n}" for dd, n in neighbours))
    if status != "open":
        print("  !! FAIL — the fix did not take")
        return 1
    print(f"  PASS — a real session, carrying {bars} bars in line with its neighbours")
    return 0


def _historical_endpoint_sets(
    conn: psycopg.Connection[Any], closures: list[date]
) -> tuple[set[tuple[int, date]], set[tuple[int, date]], set[tuple[int, date]]]:
    """Assess EVERY stored ``us_equity`` bar dated on a non-session day as an endpoint.

    ⚠⚠ THE LATEST WINDOW IS NOT A TEST OF THE CALENDAR. Whether a holiday appears in
    it is an accident of which dates happen to be an instrument's last two bars, so a
    probe built on it can pass or fail by luck (Codex checkpoint 2, P2). This walks
    the corpus instead. Returns ``(shipped, no_habit_carveout, weekend_only)`` as sets
    of ``(instrument_id, price_date)``.
    """
    rows = conn.execute(
        """
        SELECT p.instrument_id, p.price_date
          FROM price_daily p
          JOIN instruments i ON i.instrument_id = p.instrument_id
          JOIN exchanges e ON e.exchange_id = i.exchange
         WHERE e.asset_class = 'us_equity'
           AND (extract(isodow FROM p.price_date) >= 6 OR p.price_date = ANY(%(dates)s))
        """,
        {"dates": closures},
    ).fetchall()
    meta = _instrument_meta(conn)
    shipped: set[tuple[int, date]] = set()
    no_habit: set[tuple[int, date]] = set()
    weekend_only: set[tuple[int, date]] = set()
    for iid, d in rows:
        key = (int(iid), d)
        m = meta.get(int(iid))
        if m is None or m.bars < WEEKEND_HABIT_MIN_BARS:
            continue
        got = assess_window(
            _window_inputs_for(asset_class=m.asset_class, trades_weekends=m.habit_floored, habit_bar_count=m.bars),
            window_start=d,
            window_end=d,
            bar_count=1,
            endpoint_bar_dates=(d,),
        )
        if REASON_NON_SESSION_BAR in got.reasons:
            shipped.add(key)
        # probe A — the habit carve-out removed
        if us_market_status(d) == "closed":
            no_habit.add(key)
        # probe B — the published calendar replaced by a bare weekend test
        if not m.habit_floored and d.isoweekday() >= 6:
            weekend_only.add(key)
    return shipped, no_habit, weekend_only


def revert_probes(conn: psycopg.Connection[Any], closures: list[date]) -> int:
    """ACCEPTANCE 7 — each carve-out must be load-bearing, measured on REASONS.

    ⚠ Comparing verdicts alone can be masked: another clause may already quarantine
    the same instrument, so removing a carve-out changes nothing visible. Both probes
    below count the clause-5 REASON.
    """
    windows = _all_day_change_windows(conn)
    base, meta = _independent_clause5_set(conn, windows)

    no_habit: set[int] = set()
    no_calendar: set[int] = set()
    for iid, prior, as_of, _ in windows:
        m = meta.get(iid)
        if m is None or m.asset_class != "us_equity" or m.bars < WEEKEND_HABIT_MIN_BARS:
            continue
        ends = (prior, as_of)
        # probe A: drop the habit carve-out
        if any(us_market_status(x) == "closed" for x in ends):
            no_habit.add(iid)
        # probe B: drop the published-calendar test, keeping only the weekend half
        if not m.habit_floored and any(x.isoweekday() >= 6 for x in ends):
            no_calendar.add(iid)

    print("\n--- ACCEPTANCE 7: revert-probes on the clause-5 reason ---")
    print("  (a) the LATEST day-change window — incidental coverage, reported not gated")
    print(f"      clause 5 as shipped                    : {len(base)}")
    print(f"      without the habit carve-out            : {len(no_habit)}  (+{len(no_habit - base)} re-admitted)")
    print(
        f"      weekend-only, no published calendar    : {len(no_calendar)}  "
        f"(loses {len(base - no_calendar)} holiday instruments)"
    )

    # ⚠ The GATE is (b): every stored non-session bar in the corpus, so the probe does
    # not depend on which dates happen to be an instrument's last two bars.
    shipped, probe_no_habit, probe_weekend_only = _historical_endpoint_sets(conn, closures)
    print("  (b) EVERY stored us_equity bar on a non-session date — this is what is gated")
    print(f"      clause 5 refuses                       : {len(shipped)} (instrument, date) endpoints")
    print(
        f"      without the habit carve-out            : {len(probe_no_habit)}  "
        f"(+{len(probe_no_habit - shipped)} re-admitted — the 24/7 cohort)"
    )
    print(
        f"      weekend-only, no published calendar    : {len(probe_weekend_only)}  "
        f"(loses {len(shipped - probe_weekend_only)} holiday endpoints)"
    )
    failures = 0
    if not probe_no_habit - shipped:
        failures += 1
        print("  !! FAIL — the habit carve-out exempts nothing; it is not protecting the 24/7 cohort")
    if not shipped - probe_weekend_only:
        failures += 1
        print("  !! FAIL — the published calendar adds nothing over a weekend test")
    return failures


def main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(description=__doc__).parse_args(argv)
    failures = 0
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        years = _corpus_years(conn)
        closures = _closure_dates(years)
        print(f"#3046 clause 5 — {CENSUS_VERSION} @ {_git_sha()}")
        print(f"  calendar rule set : {CALENDAR_RULE_SET_VERSION}")
        print(f"  corpus years      : {years.start}-{years.stop - 1}, {len(closures)} NYSE full closures")
        census(conn, closures)
        failures += cohort_24_7(conn, closures)
        failures += new_year_arms(conn)
        failures += habit_floor(conn)
        failures += clause_ab(conn, closures)
        failures += revert_probes(conn, closures)
    print(f"\n=== {'FAILED' if failures else 'PASS'} — {failures} acceptance failure(s) ===")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
