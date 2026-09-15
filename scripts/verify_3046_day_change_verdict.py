"""#3046 build item 1 — the paired full-population A/B for the day-change verdict.

Spec: ``docs/proposals/ta/2026-09-15-3046-day-change-window-verdict.md``.
Read-only. Writes nothing. One ``REPEATABLE READ READ ONLY`` transaction.

⚠⚠ THE CONTROL IS NOT SIMULATED. ``full-population-ab.md`` forbids rebuilding the
old behaviour by hand. It is not rebuilt here: the pre-change ``load_day_changes``
was *"rank the two most recent positive closes, then ``compute_day_change``"*, and
``compute_day_change`` is UNCHANGED by this branch and is still the single tested
source for the formula. The treatment retains ``last_close`` and ``prior_close`` in
every state — including ``quarantined`` — so the control arm is that same function
applied to the treatment's own operands. Same code, same operands, one snapshot.

⚠ DISTINCT-ENTITY METRIC. Every count below is instruments, never rows.

⚠ THE POPULATION IS RECOMPUTED, never fixed at a number from the spec. Its boundary
is printed, and so is every denominator, because a suppression count is not a
correctness gain until you can see what was lost beside it.

Usage::

    PYTHONPATH=. uv run python -m scripts.verify_3046_day_change_verdict
"""

from __future__ import annotations

import argparse
import time
from collections import Counter, defaultdict
from datetime import timedelta
from typing import Any

import psycopg

from app.config import settings
from app.services.market_data import compute_day_change, load_day_changes
from app.services.price_quarantine import RULE_SET_VERSION
from app.services.price_window_verdict import (
    REASON_BAR_RETURN_UNUSABLE,
    REASON_HORIZON_STRETCHED,
    REASON_QUARANTINED_TRANSITION,
    REASON_UNRESOLVED_BREAK,
    VERDICT_OK,
    VERDICT_QUARANTINED,
    VERDICT_UNVERIFIED,
    WEEKEND_HABIT_DAYS,
    WEEKEND_SESSION_RATIO,
    load_window_inputs,
)

#: The four clauses that suppress. Held here as a LITERAL rather than imported from
#: ``_QUARANTINING_REASONS`` so the acceptance check has an operand the module under
#: test does not supply — a set compared against itself is a check that cannot fail.
CLAUSE_REASONS = frozenset(
    {
        REASON_BAR_RETURN_UNUSABLE,
        REASON_UNRESOLVED_BREAK,
        REASON_QUARANTINED_TRANSITION,
        REASON_HORIZON_STRETCHED,
    }
)

#: ⚠ The regression Codex checkpoint 1 caught on the spec. The rule set declares
#: these classes SEVEN-day for hole tolerance, not because their venues trade
#: weekends, so a class-gated weekend qualifier denies them the weekend allowance
#: and fires W2 on every ordinary Friday-to-Monday pair. They must contribute ZERO
#: ``horizon_stretched`` rejections.
WEEKEND_FALSE_POSITIVE_CLASSES = ("fx", "commodity", "index", "mena_equity")

_POPULATION_SQL = """
    SELECT count(*) AS instruments, count(*) FILTER (WHERE pairs >= 2) AS with_day_change
    FROM (
        SELECT instrument_id, count(*) FILTER (WHERE close > 0) AS pairs
        FROM price_daily GROUP BY instrument_id
    ) s
"""

_IDS_SQL = """
    SELECT instrument_id FROM price_daily WHERE close > 0
    GROUP BY instrument_id HAVING count(*) >= 2 ORDER BY instrument_id
"""

#: ⚠ The class a verdict was PRODUCED under (``sql/247:40-46`` — *"as seen at
#: evaluation time"*), not today's metadata. Reporting the rejection census against
#: a class the rules did not use would mis-attribute every row that has since been
#: reclassified.
_CLASS_SQL = """
    SELECT instrument_id, asset_class FROM price_quarantine_coverage
    WHERE rule_set_version = %(version)s
"""


#: ⚠ The histogram that FIXES ``WEEKEND_SESSION_RATIO`` by construction. It is an arm
#: of this script, not a figure typed into a docstring, because a hand-written
#: constant goes stale silently in the place a reader trusts most. ``>= 20`` bars is
#: stated as the boundary rather than hidden: below it the ratio is too noisy to
#: classify a venue, and those instruments are counted separately.
_WEEKEND_HABIT_SQL = """
    WITH last_bar AS (
        SELECT instrument_id, max(price_date) AS mx FROM price_daily GROUP BY instrument_id
    ), habit AS (
        SELECT l.instrument_id,
               count(*) FILTER (WHERE extract(isodow FROM d.price_date) >= 6)::numeric
                 / nullif(count(*), 0) AS ratio,
               count(*) AS bars
        FROM last_bar l
        JOIN price_daily d
          ON d.instrument_id = l.instrument_id
         AND d.price_date > l.mx - %(habit_days)s
        GROUP BY l.instrument_id
    )
    SELECT width_bucket(ratio, 0, 0.45, 18) AS bucket,
           round(min(ratio), 4) AS lo, round(max(ratio), 4) AS hi, count(*) AS n
    FROM habit WHERE bars >= 20 GROUP BY 1 ORDER BY 1
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None, help="cap the population (dev only)")
    parser.add_argument(
        "--weekend-habit",
        action="store_true",
        help="print the weekend-bar ratio histogram that fixes WEEKEND_SESSION_RATIO, and exit",
    )
    args = parser.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        if args.weekend_habit:
            return _weekend_habit(conn)
        return _run(conn, limit=args.limit)


def _weekend_habit(conn: psycopg.Connection[Any]) -> int:
    """Print the ratio histogram, and CHECK that the cut still sits in an empty band.

    ⚠ This is the arm that keeps ``WEEKEND_SESSION_RATIO`` honest as the corpus grows:
    the constant is defensible only while no instrument sits near it, and that is a
    property of the data, not of the code. It FAILS rather than warns.
    """
    rows = conn.execute(_WEEKEND_HABIT_SQL, {"habit_days": timedelta(days=WEEKEND_HABIT_DAYS)}).fetchall()
    cut = float(WEEKEND_SESSION_RATIO)
    print(f"weekend-bar ratio over each instrument's last {WEEKEND_HABIT_DAYS} days (>= 20 bars)")
    print(f"  cut = 1/7 = {cut:.4f}  (half the 2/7 = 0.2857 seven-day signature)")
    print(f"  {'bucket':>7}{'lo':>9}{'hi':>9}{'n':>9}")
    for bucket, lo, hi, n in rows:
        print(f"  {bucket:>7}{lo:>9}{hi:>9}{n:>9}")
    below = [float(hi) for _, _, hi, _ in rows if float(hi) < cut]
    above = [float(lo) for _, lo, _, _ in rows if float(lo) > cut]
    gap_lo, gap_hi = (max(below) if below else 0.0), (min(above) if above else 1.0)
    # A tenth of the cut either side: close enough that one reclassified instrument
    # would change a verdict, which is what "fixed by construction" must exclude.
    margin = cut / 10
    clear = (cut - gap_lo) > margin and (gap_hi - cut) > margin
    print(f"\n  empty band around the cut: {gap_lo:.4f} -> {gap_hi:.4f}")
    print(f"  {'PASS' if clear else 'FAIL'}  the cut sits clear of every populated bucket by > {margin:.4f}")
    return 0 if clear else 1


def _run(conn: psycopg.Connection[Any], *, limit: int | None) -> int:
    total, with_dc = conn.execute(_POPULATION_SQL).fetchone()  # type: ignore[misc]
    ids = [int(r[0]) for r in conn.execute(_IDS_SQL).fetchall()]
    if limit is not None:
        ids = ids[:limit]
    classes = {int(i): c for i, c in conn.execute(_CLASS_SQL, {"version": RULE_SET_VERSION})}

    started = time.perf_counter()
    treatment = load_day_changes(conn, ids)
    treatment_secs = time.perf_counter() - started

    # ⚠ The added read cost, measured rather than asserted: the same loader call the
    # consumer makes, timed on its own. The control's cost is the remainder, which is
    # the base ranked-pair query — not a second hand-written copy of that SQL.
    started = time.perf_counter()
    load_window_inputs(conn, ids, since=min(dc.prior_date for dc in treatment.values()))
    verdict_load_secs = time.perf_counter() - started
    control_query_secs = max(treatment_secs - verdict_load_secs, 0.0)

    verdicts: Counter[str] = Counter()
    reasons: Counter[str] = Counter()
    by_class: defaultdict[str, Counter[str]] = defaultdict(Counter)
    population_by_class: Counter[str] = Counter()
    suppressed: list[tuple[int, Any]] = []
    #: Derived from the REASON side, independently of the verdict field, so the
    #: acceptance check below compares two separately-computed sets.
    clause_fired: set[int] = set()
    ok_mismatch: list[int] = []
    gained: list[int] = []

    for instrument_id, dc in treatment.items():
        cls = classes.get(instrument_id) or "(no coverage row)"
        population_by_class[cls] += 1
        verdicts[dc.verdict] += 1
        for reason in dc.reasons:
            reasons[reason] += 1
            by_class[cls][reason] += 1
        if CLAUSE_REASONS & set(dc.reasons):
            clause_fired.add(instrument_id)

        # The control arm: the unchanged formula on the treatment's own operands.
        control_pct = compute_day_change(dc.last_close, dc.prior_close)

        if dc.verdict == VERDICT_QUARANTINED:
            suppressed.append((instrument_id, dc))
            if control_pct is None:
                gained.append(instrument_id)
        elif dc.change_pct != control_pct:
            ok_mismatch.append(instrument_id)
        if dc.change_pct is not None and control_pct is None:
            gained.append(instrument_id)

    out: list[str] = []
    out.append("#3046 day-change window verdict — paired full-population A/B")
    out.append(f"  rule set                                  {RULE_SET_VERSION}")
    out.append(f"  instruments with any stored bar           {total}")
    out.append(f"  instruments with >=2 positive closes      {with_dc}")
    out.append(f"  population assessed (recomputed)          {len(treatment)}")
    out.append("")
    out.append("VERDICTS (distinct instruments)")
    for verdict in (VERDICT_OK, VERDICT_UNVERIFIED, VERDICT_QUARANTINED):
        n = verdicts[verdict]
        out.append(f"  {verdict:<14} {n:>7}   {n / max(len(treatment), 1):>7.2%}")
    out.append("")
    out.append("REJECTION CENSUS by reason (a reason can co-occur; these are not a partition)")
    for reason, n in reasons.most_common():
        out.append(f"  {reason:<28} {n:>7}   of {len(treatment)} assessed")
    out.append("")
    out.append("REJECTION CENSUS by asset class — denominator is that class's own population")
    for cls in sorted(population_by_class):
        denom = population_by_class[cls]
        cells = ", ".join(f"{r}={n}" for r, n in sorted(by_class[cls].items())) or "none"
        out.append(f"  {cls:<20} n={denom:<6} {cells}")
    out.append("")

    # ⚠ Widest span first: the suppressions whose "day change" is least like a day.
    # The value lost is printed beside each one, because a suppression count on its
    # own cannot show whether a legitimate figure went with it.
    out.append(f"THE GAIN SIDE — every newly suppressed instrument ({len(suppressed)})")
    out.append(f"  {'id':>8}  {'window':<26} {'span':>5}  {'was':>14}  reasons")
    ordered = sorted(suppressed, key=lambda p: (p[1].prior_date - p[1].as_of, p[0]))
    for instrument_id, dc in ordered[:60]:
        was = compute_day_change(dc.last_close, dc.prior_close)
        out.append(
            f"  {instrument_id:>8}  {dc.prior_date} -> {str(dc.as_of):<11} "
            f"{(dc.as_of - dc.prior_date).days:>5}  "
            f"{('—' if was is None else f'{was:+.4f}'):>14}  {','.join(dc.reasons)}"
        )
    if len(ordered) > 60:
        out.append(f"  ... {len(ordered) - 60} more (NOT truncated silently: the count is above)")
    out.append("")

    fp = sum(by_class[c][REASON_HORIZON_STRETCHED] for c in WEEKEND_FALSE_POSITIVE_CLASSES)
    out.append("ACCEPTANCE (declared in the spec before this ran)")
    checks = [
        ("every non-quarantined value is byte-identical to control", not ok_mismatch),
        ("no instrument GAINS a day change", not gained),
        # ⚠ Two INDEPENDENTLY derived sets: the left comes from ``verdict``, the right
        # from ``reasons`` intersected with this script's own literal clause list.
        # ``verdicts[QUARANTINED] == len(suppressed)`` — the first version of this
        # check — compared a counter to the list it was built from and could not fail.
        (
            f"suppressed set is exactly the clause union "
            f"(verdict side {len(suppressed)}, reason side {len(clause_fired)})",
            {i for i, _ in suppressed} == clause_fired,
        ),
        # The suppression is the whole behaviour change, so assert its shape rather
        # than trusting the verdict string: BOTH value fields null, closes retained.
        (
            "every suppressed row nulls change_abs AND change_pct, and keeps both closes",
            all(
                dc.change_abs is None
                and dc.change_pct is None
                and dc.last_close is not None
                and dc.prior_close is not None
                for _, dc in suppressed
            ),
        ),
        (
            f"{'/'.join(WEEKEND_FALSE_POSITIVE_CLASSES)} contribute 0 {REASON_HORIZON_STRETCHED}",
            fp == 0,
        ),
        (
            f"the verdict load costs less than the base query it joins "
            f"({verdict_load_secs:.2f}s added to {control_query_secs:.2f}s; "
            f"{treatment_secs:.2f}s total for {len(treatment)} instruments)",
            verdict_load_secs <= control_query_secs,
        ),
    ]
    for label, passed in checks:
        out.append(f"  {'PASS' if passed else 'FAIL':<5} {label}")
    if ok_mismatch:
        out.append(f"  mismatching instrument_ids: {ok_mismatch[:20]}")
    if gained:
        out.append(f"  gaining instrument_ids: {gained[:20]}")

    print("\n".join(out))
    return 0 if all(passed for _, passed in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
