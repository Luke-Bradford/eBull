"""#3903 Amendment 2 premise: how long the TTM window behind ``dividends_paid_ttm_observed`` is, and whether an
over-long window holds a fifth payment.

Read-only, one REPEATABLE READ snapshot over the deployed sql/507 views. The window is sql/220's
(``financial_periods_ttm``'s dedup, ranking and shape guard). Its span runs from the first member's (``rn = 4``)
``period_start_date`` to the last member's ``period_end_date``. The guard under test publishes only when all four
member intervals are valid (start non-NULL and not after end; ``period_end_date`` is NOT NULL in the schema), no
member starts on or before the previous member's end, and, for a positive total, the last end is before the first
start + 1 year.

Population: every TTM row whose ``dividends_paid_ttm_observed`` is non-NULL (the column the guard changes), and the
subset whose ``instrument_valuation.dividend_yield`` is published.

Prints:

1. the guard's outcome by validity and by ``within_year`` (TRUE / FALSE / unknown) for both populations, and the
   positive publications' span histogram;
2. every refused row: span, publication, yield, the FY screen and its comparator, and the ``dividend_events``
   ex-dates inside the window;
3. the FY screen per guard outcome, listing every retained row outside [0.87, 1.15]. The comparator is the latest
   normalized FY row (latest filed first); a window is "that FY" when both boundaries equal the FY's. The screen
   filters candidates and does not count payments;
4. the end-anchored cross-check: the span from the day after the preceding quarter's end (``rn = 5``) to the last end,
   against the guard. A kept window over a year by end dates is classified "predecessor missing, start filed" when
   ``financial_facts_raw`` holds a fact whose context runs from the stored first start to the first member's end (the
   filing itself dates the start), else "start unverified"; a window with no ``rn = 5`` is "no predecessor stored";
   and member gaps and overlaps;
5. KO's members, the case that failed the acceptance.

Driven from ``financial_periods_ttm`` rows with a non-NULL ``dividends_paid_ttm_observed``; every other relation is
left-joined and the row count is asserted against a direct count. D is recomputed from the selected members and
asserted equal to the view's.

    PYTHONPATH=. uv run python -m scripts.measure_3903_dividend_window_span
"""

from __future__ import annotations

from collections import Counter
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.config import settings

_SQL = """
WITH dq AS (
    SELECT DISTINCT ON (instrument_id, period_end_date)
           instrument_id, period_end_date, period_start_date, dividends_paid
    FROM financial_periods
    WHERE period_type IN ('Q1','Q2','Q3','Q4') AND superseded_at IS NULL AND normalization_status = 'normalized'
    ORDER BY instrument_id, period_end_date, filed_date DESC NULLS LAST
), rq AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY instrument_id ORDER BY period_end_date DESC) AS rn,
           LAG(period_end_date) OVER (PARTITION BY instrument_id ORDER BY period_end_date) AS prev_end
    FROM dq
), w AS (
    SELECT instrument_id,
           MAX(period_end_date) FILTER (WHERE rn = 1) AS e,
           MAX(period_start_date) FILTER (WHERE rn = 4) AS s,
           MAX(period_end_date) FILTER (WHERE rn = 5) AS e5,
           MAX(period_end_date - period_start_date + 1) FILTER (WHERE rn = 4) AS first_len,
           COUNT(*) FILTER (WHERE rn <= 4 AND period_start_date IS NOT NULL
                                 AND period_start_date <= period_end_date) = 4 AS valid,
           SUM(dividends_paid) FILTER (WHERE rn <= 4) AS d_members,
           COUNT(*) FILTER (WHERE rn < 4 AND period_start_date <= prev_end) AS n_overlap,
           COUNT(*) FILTER (WHERE rn < 4 AND period_start_date > prev_end + 1) AS n_gap,
           MAX(period_start_date - prev_end - 1) FILTER (WHERE rn < 4) AS max_gap
    FROM rq WHERE rn <= 5 GROUP BY instrument_id
), fy AS (
    SELECT DISTINCT ON (instrument_id) instrument_id, period_start_date, period_end_date, dividends_paid, filed_date
    FROM financial_periods
    WHERE period_type = 'FY' AND superseded_at IS NULL AND normalization_status = 'normalized'
      AND dividends_paid IS NOT NULL
    ORDER BY instrument_id, period_end_date DESC, filed_date DESC NULLS LAST
)
SELECT t.instrument_id, i.symbol, w.s, w.e, w.e5, w.e - w.s + 1 AS span, w.e - w.e5 AS span_e, w.valid,
       w.first_len, w.d_members,
       w.e < w.s + INTERVAL '1 year' AS within_year, w.e < (w.e5 + 1) + INTERVAL '1 year' AS within_year_e,
       w.n_overlap, w.n_gap, w.max_gap,
       v.dividend_yield, t.dividends_paid_ttm_observed AS d,
       fy.dividends_paid AS fy_d, fy.period_start_date AS fy_s, fy.period_end_date AS fy_e, fy.filed_date AS fy_filed,
       (SELECT count(*) FROM dividend_events de
         WHERE de.instrument_id = t.instrument_id AND de.ex_date BETWEEN w.s AND w.e) AS ex_dates_in_window,
       (SELECT count(*) FROM dividend_events de WHERE de.instrument_id = t.instrument_id) AS ex_dates_any,
       EXISTS (SELECT 1 FROM financial_facts_raw f
               WHERE f.instrument_id = t.instrument_id AND f.period_start = w.s AND f.period_end = t.ttm_start)
           AS start_filed
FROM financial_periods_ttm t
LEFT JOIN w USING (instrument_id)
LEFT JOIN instrument_valuation v USING (instrument_id)
LEFT JOIN instruments i USING (instrument_id)
LEFT JOIN fy USING (instrument_id)
WHERE t.dividends_paid_ttm_observed IS NOT NULL
ORDER BY i.symbol, t.instrument_id
"""

# Every window member, and how many rows share its (instrument, end, filed_date) dedup key: a count above 1 is a tie
# sql/220's DISTINCT ON resolves arbitrarily.
_MEMBERS = """
WITH dq AS (
    SELECT DISTINCT ON (instrument_id, period_end_date) instrument_id, period_end_date, period_start_date, period_type,
           dividends_paid, filed_date
    FROM financial_periods
    WHERE period_type IN ('Q1','Q2','Q3','Q4') AND superseded_at IS NULL AND normalization_status = 'normalized'
    ORDER BY instrument_id, period_end_date, filed_date DESC NULLS LAST
), rq AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY instrument_id ORDER BY period_end_date DESC) AS rn FROM dq
)
SELECT rq.instrument_id, rq.period_type, rq.period_start_date, rq.period_end_date, rq.dividends_paid,
       (SELECT count(*) FROM financial_periods f
         WHERE f.instrument_id = rq.instrument_id AND f.period_end_date = rq.period_end_date
           AND f.filed_date IS NOT DISTINCT FROM rq.filed_date AND f.period_type IN ('Q1','Q2','Q3','Q4')
           AND f.superseded_at IS NULL AND f.normalization_status = 'normalized') AS key_rows
FROM rq WHERE rn <= 4
ORDER BY rq.instrument_id, rq.period_end_date
"""

_FY_TIES = """
SELECT count(*) AS n FROM (
    SELECT instrument_id, period_end_date, filed_date FROM financial_periods
    WHERE period_type = 'FY' AND superseded_at IS NULL AND normalization_status = 'normalized'
      AND dividends_paid IS NOT NULL
    GROUP BY 1, 2, 3 HAVING count(*) > 1
) x
"""

_LOW, _HIGH = Decimal("0.87"), Decimal("1.15")


def _outcome(r: dict[str, Any]) -> str:
    """Conditions in precedence order: 1 valid intervals, 2 no overlap, 3 within one calendar year (positive totals
    only: four valid, non-overlapping, non-negative members summing to zero are zero over any window they span)."""
    if not r["valid"]:
        return "refused: invalid member interval"
    if r["n_overlap"]:
        return "refused: overlapping members"
    if r["d"] == 0 or r["within_year"] is True:
        return "kept"
    return "refused: over one calendar year" if r["within_year"] is False else "refused: unknown"


def _guard(r: dict[str, Any]) -> bool:
    return _outcome(r) == "kept"


_OUTCOMES = (
    "kept",
    "refused: over one calendar year",
    "refused: invalid member interval",
    "refused: overlapping members",
    "refused: unknown",
)


def _screen(r: dict[str, Any]) -> tuple[Decimal | None, str]:
    if r["fy_d"] is None:
        return None, "no normalized FY"
    if r["fy_s"] == r["s"] and r["fy_e"] == r["e"]:
        return None, f"window is that FY, filed {r['fy_filed']} {r['fy_d']}"
    if r["fy_d"] == 0:
        return None, "FY zero"
    return Decimal(r["d"]) / Decimal(r["fy_d"]), f"FY {r['fy_s']}..{r['fy_e']} filed {r['fy_filed']} {r['fy_d']}"


def _ex_dates(r: dict[str, Any]) -> str:
    if r["s"] is None or r["s"] > r["e"]:
        return f"unassessable (no valid window); {r['ex_dates_any']} stored for the instrument"
    return f"{r['ex_dates_in_window']} in window, {r['ex_dates_any']} stored for the instrument"


def _end_anchor(r: dict[str, Any]) -> str:
    if r["d"] == 0:
        return "zero total, condition 3 not applied"
    if r["e5"] is None:
        return "no predecessor stored"
    if r["within_year_e"]:
        return "within a year"
    return "predecessor missing, start filed" if r["start_filed"] else "start unverified"


def main() -> None:
    with psycopg.connect(settings.database_url) as conn, conn.cursor(row_factory=dict_row) as cur:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        rows = cur.execute(_SQL).fetchall()
        direct = cur.execute(
            "SELECT count(*) AS n FROM financial_periods_ttm WHERE dividends_paid_ttm_observed IS NOT NULL"
        ).fetchone()
        members: dict[int, list[dict[str, Any]]] = {}
        for m in cur.execute(_MEMBERS).fetchall():
            members.setdefault(m["instrument_id"], []).append(m)
        fy_ties = cur.execute(_FY_TIES).fetchone()
        ko = cur.execute(
            """
            SELECT period_type, period_start_date, period_end_date, dividends_paid
            FROM financial_periods
            WHERE instrument_id = (SELECT instrument_id FROM instruments WHERE symbol = 'KO'
                                   ORDER BY is_primary_listing DESC, instrument_id LIMIT 1)
              AND superseded_at IS NULL AND normalization_status = 'normalized'
              AND period_end_date >= DATE '2025-01-01'
            ORDER BY period_end_date, period_type
            """
        ).fetchall()
        conn.rollback()
    ids = [r["instrument_id"] for r in rows]
    assert direct is not None and len(rows) == direct["n"] == len(set(ids)), (len(rows), direct, len(set(ids)))
    mismatched = [r["symbol"] for r in rows if r["d_members"] != r["d"]]
    assert not mismatched, mismatched
    tied = sorted({r["symbol"] for r in rows for m in members.get(r["instrument_id"], []) if m["key_rows"] > 1})
    pub = [r for r in rows if r["dividend_yield"] is not None]
    positive = [r for r in pub if r["dividend_yield"] > 0]

    print("1. guard outcome")
    print(f"   rows {len(rows)} = direct count {direct['n']}, unique; every member sum equals the view's D")
    print(
        f"   windows with a member on a tied dedup key (instrument, end, filed_date): {tied or 'none'}; "
        f"tied FY keys: {fy_ties['n'] if fy_ties else None}"
    )
    for name, pop in (("observed TTM rows", rows), ("publications", pub), ("positive publications", positive)):
        print(f"   {name}: {len(pop)}; {dict(Counter(_outcome(r) for r in pop))}")
    print(
        "   positive publications by span:",
        sorted(Counter(r["span"] for r in positive).items(), key=lambda kv: (kv[0] is None, kv[0] or 0)),
    )

    print("\n2. every refused observed row")
    for r in rows:
        if _guard(r):
            continue
        x, how = _screen(r)
        print(
            f"   {r['symbol']}: {_outcome(r)}; window {r['s']}..{r['e']} span {r['span']}; "
            f"published {r['dividend_yield'] is not None} yield {r['dividend_yield']}; "
            f"D/FY {'n/a' if x is None else f'{x:.3f}'} ({how}); ex-dates {_ex_dates(r)}"
        )
        if _outcome(r) == "refused: overlapping members":
            for m in members.get(r["instrument_id"], []):
                print(
                    f"      {m['period_type']} {m['period_start_date']}..{m['period_end_date']} "
                    f"({(m['period_end_date'] - m['period_start_date']).days + 1} days) {m['dividends_paid']}"
                )

    print("\n3. FY screen over positive publications")
    for label in _OUTCOMES:
        sub = [(r, _screen(r)) for r in positive if _outcome(r) == label]
        compared = [(r, x, how) for r, (x, how) in sub if x is not None]
        print(
            f"   {label}: {len(sub)}; screened {len(compared)}; above {_HIGH} "
            f"{sum(1 for _, x, _ in compared if x > _HIGH)}; below {_LOW} "
            f"{sum(1 for _, x, _ in compared if x < _LOW)}; unscreened "
            f"{dict(Counter(h.split(',')[0] for _, (x, h) in sub if x is None))}"
        )
        if label == "kept":
            for r, x, how in sorted(compared, key=lambda t: t[1], reverse=True):
                if not _LOW <= x <= _HIGH:
                    print(f"      {r['symbol']} span {r['span']} D {r['d']} D/FY {x:.3f} ({how})")
    short = [r for r in positive if r["span"] is not None and r["span"] < 364]
    print(f"   positive publications under 364 days: {len(short)}")
    for r in short:
        x, how = _screen(r)
        print(f"      {r['symbol']} span {r['span']} {_outcome(r)} D/FY {'n/a' if x is None else f'{x:.3f}'} ({how})")

    print("\n4. end-anchored cross-check over publications")
    print("   (guard, end-anchored class):", dict(Counter((_guard(r), _end_anchor(r)) for r in pub)))
    for r in pub:
        if _guard(r) and _end_anchor(r) != "within a year":
            print(
                f"      kept, {_end_anchor(r)}: {r['symbol']} window {r['s']}..{r['e']} span {r['span']}; "
                f"preceding end {r['e5']}, end-anchored {r['span_e']}; first member {r['first_len']} days"
            )
    over = [r for r in pub if r["n_overlap"]]
    gap = [r for r in pub if r["n_gap"]]
    print(
        f"   publications with any overlap {len(over)} {[r['symbol'] for r in over]}; any gap {len(gap)}; both "
        f"{sum(1 for r in pub if r['n_overlap'] and r['n_gap'])}; largest gap (days) among gapped "
        f"{sorted(Counter(r['max_gap'] for r in gap).items())}"
    )

    print("\n5. KO members since 2025")
    for q in ko:
        print(f"   {q['period_type']} {q['period_start_date']}..{q['period_end_date']} {q['dividends_paid']}")


if __name__ == "__main__":
    main()
