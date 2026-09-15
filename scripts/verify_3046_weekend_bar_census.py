"""#3046 residual 2 — what the weekend-bar population actually is, on the full corpus.

Spec: ``docs/proposals/ta/2026-09-15-3046-weekend-bar-write-path.md``.
Read-only. Writes nothing. One ``REPEATABLE READ READ ONLY`` transaction.

WHY THIS EXISTS. Residual 1 found the dominant T2 cluster is weekend bars and left
the population uncharacterised, with one direction attached to it — *"refusing a
weekend bar at ingest ... is a write-path change, not a rule change, so it does not
rotate INPUT_RULE_SETS"*. Both halves of that need testing before anything is built:
whether the population is one shape (it is not), and whether we fabricate the bars
(we do not — the daily-candle path has no gap-fill, carry-forward or synthesis).

⚠⚠ THE 5-DAY/7-DAY FACT IS READ FROM THE RULE SET, NEVER GUESSED FROM A SYMBOL.
``params_for(asset_class).calendar_days_per_bar != 1`` is the test, and
``asset_class`` is derived by exactly the join ``price_quarantine_store._SCOPE_SQL``
uses — ``instruments.exchange -> exchanges.asset_class``. Any other source would
classify a different population than the verdicts were computed under.

⚠⚠ A WEEKEND BAR IS NOT AUTOMATICALLY A DEFECT. eToro sells ``.24-7`` products that
genuinely trade Saturday and Sunday, and ``rule_w2``'s own docstring already uses
``SP.24-7`` as its worked example. They are typed ``us_equity`` / ``commodity`` by
``exchanges.asset_class``, so the rule set hands them a 5-DAY parameter set. A blanket
"refuse weekend bars on 5-day classes" rule deletes real traded data, which is the
"large uncharacterised blast radius" residual 1 warned about — named here rather than
left as a warning.

⚠ NO STRUCTURED 24/7 MARKER EXISTS AND THE CHECK WAS RUN, not assumed. ``instruments``
carries no session or calendar column; its 18 ``.24-7`` rows span 5 ``exchange`` values
and 4 ``instrument_type_id`` values and share both with ordinary Nasdaq/NYSE equities;
``exchanges`` has no session-hours COLUMN (#2312) and its ``capabilities`` JSONB is a
DATA-coverage map (filings/insider/dividends), not a trading calendar. So this script
reports the suffix cohort as an OBSERVED cohort and never uses it as a rule.

⚠⚠ "NO SESSION COLUMN" IS NOT "NO CALENDAR". ``app/services/market_calendar.py`` is a
real NYSE trading calendar — full closures and 13:00 ET early closes, source-ruled to
NYSE's published holidays. It covers ``us_equity``, the largest affected class here
(3,769 of 10,756 bars). This script does not consult it: the census is about what is
STORED, and a published calendar belongs to whatever fix follows. Recorded because
draft 1 of the spec asserted the repo had no calendar at all, on a #2312 reference,
without grepping for a module.

Usage::

    PYTHONPATH=. uv run python -m scripts.verify_3046_weekend_bar_census
"""

from __future__ import annotations

import argparse
import subprocess
from typing import Any

import psycopg

from app.config import settings
from app.services.price_quarantine import RULE_SET_VERSION, params_for

#: Bumped whenever a stratum definition changes, so two runs are comparable only
#: when it matches. Not a rule constant — every rule called here lives in
#: ``price_quarantine``.
CENSUS_VERSION = "weekend-census-v2"  # v2: date-local volume column, explicit NULL/negative branches

#: Sentinel standing in for a NULL ``exchanges.asset_class`` inside ``= ANY(...)``,
#: which never matches NULL. ``params_for(None)`` returns the strict 5-day default, so
#: the class belongs in the population rather than silently outside it.
_NULL_CLASS = "__null__"

#: The only two ways ``git rev-parse`` can fail: the binary is missing (``OSError``)
#: or it exits non-zero (``subprocess.CalledProcessError``). Bound to a name because
#: ``ruff format`` strips the parentheses from ``except (A, B):`` on this Python
#: target, leaving a form indistinguishable from the Python 2 ``except E, name:``.
_GIT_SHA_FAILURES = (OSError, subprocess.SubprocessError)

#: ``price_quarantine_store._SCOPE_SQL``'s join, verbatim in shape: the asset class a
#: bar's verdicts were computed under. Reused rather than re-derived so this census
#: partitions the same population the verdict tables do.
_CLASSED_BARS = """
    SELECT p.instrument_id,
           p.price_date,
           p.open, p.high, p.low, p.close, p.volume,
           e.asset_class,
           i.symbol
      FROM price_daily p
      JOIN instruments i ON i.instrument_id = p.instrument_id
      LEFT JOIN exchanges e ON e.exchange_id = i.exchange
"""


def _git_sha() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
    except _GIT_SHA_FAILURES:
        return "unknown"


def five_day_classes(conn: psycopg.Connection[Any]) -> list[str]:
    """Asset classes the RULE SET declares 5-day, read from ``params_for``.

    ⚠ Not a hand-written list. ``calendar_days_per_bar`` is 7/5 for exchange sessions
    and 1 for the 7-day crypto/FX markets, and that is the only place in the repo
    where the fact is recorded. A literal here would silently disagree with the
    verdicts the moment a class is re-parameterised.

    ⚠ ``fx`` is declared 7-day by the rule set and is therefore EXCLUDED, even though
    real FX venues close from Friday to Sunday evening. That is a rule-module
    question (``price_quarantine`` is in ``INPUT_RULE_SETS``, #3031) and is reported
    below rather than silently corrected here.
    """
    classes = [
        row[0] for row in conn.execute("SELECT DISTINCT asset_class FROM exchanges").fetchall() if row[0] is not None
    ]
    named = sorted(c for c in classes if params_for(c).calendar_days_per_bar != 1)
    # ⚠ A NULL asset_class is a REAL state (an instrument on an exchange we have no
    # row for), and ``params_for(None)`` returns the STRICT default — which is 5-day.
    # Excluding it here while the headline counts it would partition two different
    # populations. Measured 2026-09-15: 0 instruments with bars have a NULL class, so
    # the divergence is vacuous today; it is closed rather than relied on.
    if params_for(None).calendar_days_per_bar != 1:
        named.append(_NULL_CLASS)
    return named


def _params(classes: list[str]) -> dict[str, Any]:
    """Bind params for the arms: the named classes, plus whether NULL joins them."""
    return {
        "classes": [c for c in classes if c != _NULL_CLASS],
        "null_is_five": _NULL_CLASS in classes,
    }


def arm_class_split(conn: psycopg.Connection[Any]) -> None:
    """Every asset class, with its rule-set day count beside its weekend-bar count."""
    print("\nWEEKEND BARS BY ASSET CLASS (day count read from params_for, not assumed)")
    print("-" * 100)
    print(
        f"  {'asset_class':<16} {'days/bar':>9} {'5-day':>6} {'weekend bars':>13} {'instruments':>12} {'all bars':>11}"
    )
    total = instruments = 0
    for cls, wb, wi, ab in conn.execute(
        f"""
        WITH b AS ({_CLASSED_BARS})
        SELECT coalesce(asset_class, '(null)'),
               count(*) FILTER (WHERE EXTRACT(isodow FROM price_date) IN (6, 7)),
               count(DISTINCT instrument_id) FILTER (WHERE EXTRACT(isodow FROM price_date) IN (6, 7)),
               count(*)
          FROM b GROUP BY 1 ORDER BY 2 DESC
        """
    ).fetchall():
        params = params_for(None if cls == "(null)" else cls)
        five = params.calendar_days_per_bar != 1
        if five:
            total += int(wb)
            instruments += int(wi)
        print(f"  {cls:<16} {str(params.calendar_days_per_bar):>9} {str(five):>6} {wb:>13} {wi:>12} {ab:>11}")
    print(f"\n  5-DAY CLASSES: {total} weekend bars across {instruments} instrument/class rows")


def arm_shape(conn: psycopg.Connection[Any], classes: list[str]) -> None:
    """The population is not one shape, and the shapes want different answers.

    ``flat`` is ``open = high = low = close`` — no intraday range at all. Combined
    with an absent volume it is the shape residual 1 found on the Nordic cluster.
    POSITIVE volume is the opposite: evidence that a session happened.

    ⚠ An absent volume is NOT evidence that no session happened. Residual 3 measured
    ``price_daily.volume`` NULL for EVERY bar of 3,010 of 12,284 instruments, so for
    those the field cannot discriminate. The prior-volume column is context that makes
    the absence readable, and it is printed rather than folded into a verdict.

    ⚠⚠ It is STRICTLY EARLIER by construction. A lifetime "does this instrument ever
    report volume" test is LOOK-AHEAD on a corpus that feeds backtests — one positive
    bar in 2026 changes how a 2022 weekend bar reads — and it also counted the measured
    bar, making the column trivially true for every positive-volume row. Both caught at
    Codex checkpoint 1. ⚠ Even in this form it is DESCRIPTIVE. It is not proposed as a
    discriminator: ``price_quarantine._corroboration`` states the governing rule —
    volume is an admit-back signal, NEVER the gate.
    """
    print("\nSHAPE OF THE 5-DAY-CLASS WEEKEND BARS")
    print("-" * 100)
    for flat, vol, n, iids, with_vol in conn.execute(
        f"""
        WITH b AS ({_CLASSED_BARS}),
        wk AS (
            SELECT *,
                   (open = high AND high = low AND low = close) AS flat,
                   -- ⚠ Explicit on every branch. An `ELSE 'zero'` labels a NEGATIVE
                   -- volume 'zero'; the column is NUMERIC and nothing forbids one.
                   -- Measured 2026-09-15: 0 zero rows and 0 negative rows corpus-wide,
                   -- so the mislabel is unreachable today — which is exactly when it is
                   -- free to fix.
                   CASE WHEN volume IS NULL THEN 'null'
                        WHEN volume > 0 THEN 'positive'
                        WHEN volume = 0 THEN 'zero'
                        ELSE 'negative' END AS vol
              FROM b
             WHERE EXTRACT(isodow FROM price_date) IN (6, 7)
               AND (asset_class = ANY(%(classes)s)
                 OR (asset_class IS NULL AND %(null_is_five)s))
        )
        -- ⚠⚠ STRICTLY EARLIER, AND NEVER THE BAR ITSELF. A lifetime "does this
        -- instrument ever report volume" test is look-ahead — one positive bar in
        -- 2026 would change how a 2022 weekend bar reads — and it also counted the
        -- measured bar, which made this column trivially true for every
        -- positive-volume row. Both caught at Codex checkpoint 1.
        SELECT wk.flat, wk.vol, count(*), count(DISTINCT wk.instrument_id),
               count(*) FILTER (WHERE EXISTS (
                   SELECT 1 FROM price_daily pv
                    WHERE pv.instrument_id = wk.instrument_id
                      AND pv.price_date < wk.price_date
                      AND pv.volume > 0))
          FROM wk
         GROUP BY 1, 2 ORDER BY 3 DESC
        """,
        _params(classes),
    ).fetchall():
        print(
            f"  flat={str(flat):<5} volume={vol:<9} {n:>6} bars  {iids:>5} instruments"
            f"   (with positive volume on a STRICTLY EARLIER date: {with_vol})"
        )


def arm_position(conn: psycopg.Connection[Any], classes: list[str]) -> None:
    """Leading sentinel vs interspersed — two defects, not one.

    A weekend bar BEFORE the instrument's first weekday bar is a placeholder in front
    of a series that has not started. One INSIDE a live series is a different thing:
    it adds a bar to every N-bar window and, when flat, contributes a zero return.
    Residual 1 characterised only the first kind.
    """
    row = conn.execute(
        f"""
        WITH b AS ({_CLASSED_BARS}),
        wk AS (SELECT instrument_id, price_date FROM b
                WHERE EXTRACT(isodow FROM price_date) IN (6, 7)
                  AND (asset_class = ANY(%(classes)s)
                    OR (asset_class IS NULL AND %(null_is_five)s))),
        firstwd AS (SELECT instrument_id, min(price_date) AS first_weekday FROM price_daily
                     WHERE EXTRACT(isodow FROM price_date) NOT IN (6, 7) GROUP BY 1),
        placed AS (
            SELECT wk.instrument_id, wk.price_date,
                   (f.first_weekday IS NULL OR wk.price_date < f.first_weekday) AS is_leading
              FROM wk LEFT JOIN firstwd f USING (instrument_id)
        )
        SELECT count(*) FILTER (WHERE is_leading),
               count(DISTINCT instrument_id) FILTER (WHERE is_leading),
               count(*) FILTER (WHERE NOT is_leading),
               count(DISTINCT instrument_id) FILTER (WHERE NOT is_leading)
          FROM placed
        """,
        _params(classes),
    ).fetchone()
    assert row is not None
    print("\nPOSITION IN THE SERIES")
    print("-" * 100)
    print(f"  leading (before the instrument's first weekday bar)  {row[0]:>6} bars  {row[1]:>5} instruments")
    print(f"  interspersed (at or after it)                        {row[2]:>6} bars  {row[3]:>5} instruments")


def arm_verdict_reach(conn: psycopg.Connection[Any], classes: list[str]) -> None:
    """How many of these bars ANY stored verdict already reaches.

    ACCEPTANCE: if the untouched remainder is ~0, residual 5's clause-3 adoption
    subsumes this residual and nothing further is owed. It is not — which is the
    result, and it is the reason a bar-level answer is needed at all.

    ⚠ Both surfaces are asked, because they say different things: a
    ``price_bar_quarantine`` row condemns the BAR's fields, a
    ``price_transition_quarantine`` row condemns a RATIO the bar is an endpoint of.
    """
    row = conn.execute(
        f"""
        WITH b AS ({_CLASSED_BARS}),
        wk AS (SELECT instrument_id, price_date FROM b
                WHERE EXTRACT(isodow FROM price_date) IN (6, 7)
                  AND (asset_class = ANY(%(classes)s)
                    OR (asset_class IS NULL AND %(null_is_five)s)))
        SELECT count(*),
               count(*) FILTER (WHERE EXISTS (
                   SELECT 1 FROM price_bar_quarantine q
                    WHERE q.rule_set_version = %(v)s
                      AND q.instrument_id = wk.instrument_id AND q.price_date = wk.price_date)),
               count(*) FILTER (WHERE EXISTS (
                   SELECT 1 FROM price_transition_quarantine t
                    WHERE t.rule_set_version = %(v)s AND cardinality(t.rules) > 0
                      AND t.instrument_id = wk.instrument_id
                      AND (t.price_date = wk.price_date OR t.prior_date = wk.price_date))),
               count(*) FILTER (WHERE NOT EXISTS (
                       SELECT 1 FROM price_bar_quarantine q
                        WHERE q.rule_set_version = %(v)s
                          AND q.instrument_id = wk.instrument_id AND q.price_date = wk.price_date)
                   AND NOT EXISTS (
                       SELECT 1 FROM price_transition_quarantine t
                        WHERE t.rule_set_version = %(v)s AND cardinality(t.rules) > 0
                          AND t.instrument_id = wk.instrument_id
                          AND (t.price_date = wk.price_date OR t.prior_date = wk.price_date)))
          FROM wk
        """,
        {**_params(classes), "v": RULE_SET_VERSION},
    ).fetchone()
    assert row is not None
    total, bar_rows, trans, neither = int(row[0]), int(row[1]), int(row[2]), int(row[3])
    print("\nWHAT THE EXISTING RULE SET ALREADY REACHES")
    print("-" * 100)
    print(f"  weekend bars                                  {total:>6}")
    # ⚠ An empty population is a REAL state, not an impossible one: re-parameterise
    # every class 7-day and `five_day_classes` returns nothing. A share of zero bars
    # is undefined, so it prints as `n/a` rather than dividing — a crash here would
    # replace the answer "there are none" with a traceback.
    if total == 0:
        print("  ⚠ no weekend bars on any 5-day class — nothing to report, and that IS the answer")
        return
    print(f"  carrying a price_bar_quarantine row           {bar_rows:>6}  ({100 * bar_rows / total:.1f}%)")
    print(f"  an endpoint of a quarantined transition       {trans:>6}  ({100 * trans / total:.1f}%)")
    # ⚠ Computed as a real set complement, NOT `total - max(bar_rows, trans)`. The two
    # surfaces are independent — a bar can carry a B-rule row and be a transition
    # endpoint, or neither — so `max` silently assumes one set contains the other.
    print(f"  reached by NEITHER surface                    {neither:>6}  <- the residual")
    print(
        "\n  ⚠ WHY it is invisible, stated as mechanism rather than inferred from the count:\n"
        "    a single Sat or Sun bar between Friday and Monday makes gaps of 1 and 2 days, far\n"
        "    under every class's hole_days, so T2 cannot fire; and a flat o=h=l=c bar violates\n"
        "    no B-rule — it passes containment. The Nordic cluster fired T2 only because its\n"
        "    placeholders sat two months ahead of the real series."
    )


def arm_traded_cohort(conn: psycopg.Connection[Any], classes: list[str]) -> None:
    """The bars a blanket weekday cut would DESTROY: positive volume on a weekend."""
    print("\nPOSITIVE-VOLUME WEEKEND BARS — what a blanket weekday refusal would delete")
    print("-" * 100)
    rows = conn.execute(
        f"""
        WITH b AS ({_CLASSED_BARS})
        SELECT symbol, coalesce(asset_class, '(null)'), count(*),
               array_agg(DISTINCT EXTRACT(isodow FROM price_date)::int ORDER BY EXTRACT(isodow FROM price_date)::int)
          FROM b
         WHERE EXTRACT(isodow FROM price_date) IN (6, 7)
           AND (asset_class = ANY(%(classes)s) OR (asset_class IS NULL AND %(null_is_five)s)) AND volume > 0
         GROUP BY 1, 2 ORDER BY 3 DESC
        """,
        _params(classes),
    ).fetchall()
    suffix = sum(int(n) for sym, _, n, _ in rows if sym.endswith(".24-7"))
    total = sum(int(n) for _, _, n, _ in rows)
    for sym, cls, n, dows in rows:
        mark = "  <- .24-7 product: genuinely weekend-traded" if sym.endswith(".24-7") else ""
        print(f"  {sym:<16} {cls:<14} {n:>4} bars  dow={dows}{mark}")
    print(f"\n  {suffix} of {total} sit on a `.24-7` symbol — an OBSERVED cohort, never used as a rule here.")
    print("  No structured 24/7 field exists: instruments has no session column, the 18 `.24-7` rows")
    print("  share exchange and instrument_type_id with ordinary equities, and exchanges carries no")
    print("  calendar (#2312). The remainder is undetermined and is NOT claimed either way.")


def main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(description=__doc__).parse_args(argv)
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        frontier = conn.execute("SELECT max(price_date), count(*) FROM price_daily").fetchone()
        assert frontier is not None
        classes = five_day_classes(conn)

        print("=" * 100)
        print("#3046 residual 2 — the weekend-bar population, full corpus")
        print("=" * 100)
        print(f"  git                       {_git_sha()}")
        print(f"  census version            {CENSUS_VERSION}")
        print(f"  quarantine RULE_SET       {RULE_SET_VERSION}")
        print(f"  corpus                    {frontier[1]} bars, frontier {frontier[0]}")
        print(f"  5-day classes (params_for) {classes}")

        arm_class_split(conn)
        arm_shape(conn, classes)
        arm_position(conn, classes)
        arm_verdict_reach(conn, classes)
        arm_traded_cohort(conn, classes)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
