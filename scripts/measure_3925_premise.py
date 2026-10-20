"""#3925 premise: a scale comparator for a cover count whose accession has no balance-sheet comparator.

Spec: ``docs/specs/fundamentals/2026-10-16-3841-pit-market-cap.md`` ("Amendment 3"). Read-only: one REPEATABLE READ
snapshot over one #3841 PIT build (``--build-id``). ``evaluate`` is an implementation of the amendment's rules 1-5
that shares no code with ``app/services/pit_market_cap.py``; the A/B imports it as the oracle (structural fail 7).

The unit is a distinct (instrument, cover accession) a step of the build selects with a single count. An accession is
``published`` when any step selecting it publishes, else ``refused``; ``stored`` is the build's own scale outcome.

Per accession, from exact stored numerics (operand values de-duplicated exactly, whatever their ``fiscal_period``):
- ``sheet``: its ``us-gaap:CommonStockSharesOutstanding`` at its latest instant, over the build's context-filtered
  read -- ``valid``, ``none``, ``ambiguous`` or ``invalid`` (rule 1);
- its fiscal period focus: the one distinct non-blank ``fiscal_period`` over its rows of the four concepts, else
  unknown;
- periods: the distinct starts of its weighted-average rows at their latest end, when that end is on or before the
  New York acceptance date (rule 3); each period's rule-4 status.

Candidates, each at 100x against the cover count: ``wa`` (the weighted averages at the latest end, unchecked),
``implied`` (numerator / EPS of periods with valid operands) and ``rule`` (both values of every consistent period).

    PYTHONPATH=. uv run python scripts/measure_3925_premise.py --build-id 9 \\
        --trace 9864 8815 6104 1052454 4387 1531 10716 > docs/research/3925-premise.txt
"""

from __future__ import annotations

import argparse
import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, localcontext
from zoneinfo import ZoneInfo

import psycopg

from app.config import settings
from app.services.factor_panel import MAX_SHARES_AGE_MONTHS, add_months
from app.services.pit_market_cap_build import FIRST_SESSION

TOLERANCE = 100.0
PER_SHARE_TOLERANCE = Decimal("0.01")  # DQC_0227 ``$perShareItemTolerance``
SHORT_PERIOD_DAYS = {"FY": 310, "Q2": 120, "Q3": 200}  # DQC_0227 (2026) lines 101-102
WA = "WeightedAverageNumberOfSharesOutstandingBasic"
EPS = "EarningsPerShareBasic"
AVAILABLE = "NetIncomeLossAvailableToCommonStockholdersBasic"
NET_INCOME = "NetIncomeLoss"
NEW_YORK = ZoneInfo("America/New_York")
# The build's cover and balance-sheet read keeps contexts on or after this date (``pit_market_cap_build``); rule 1
# judges the comparator the build actually holds. These are the build's inputs, not its rule.
MIN_CONTEXT = add_months(FIRST_SESSION, -MAX_SHARES_AGE_MONTHS)

_ACCESSIONS_SQL = """
SELECT instrument_id, accession, shares_reported, bool_or(refusal IS NULL), min(accepted_at), min(context_date),
       min(evidence->'checks'->'scale'->>'outcome'), count(DISTINCT evidence->'checks'->'scale'->>'outcome')
FROM instrument_share_count_pit
WHERE build_id = %(build)s AND shares_concept = 'cover' AND shares_reported IS NOT NULL
GROUP BY 1, 2, 3
"""

_NULL_SHARES_SQL = """
SELECT count(DISTINCT (instrument_id, accession)) FROM instrument_share_count_pit
WHERE build_id = %(build)s AND shares_concept = 'cover' AND shares_reported IS NULL
"""

_KEYS = """
  SELECT DISTINCT instrument_id, accession FROM instrument_share_count_pit
  WHERE build_id = %(build)s AND shares_concept = 'cover'
"""

_SHEET_SQL = f"""
WITH k AS ({_KEYS}), f AS (
  SELECT DISTINCT f.instrument_id, f.accession_number, f.period_end, f.val
  FROM financial_facts_raw f JOIN k ON k.instrument_id = f.instrument_id AND k.accession = f.accession_number
  WHERE f.taxonomy = 'us-gaap' AND f.concept = 'CommonStockSharesOutstanding' AND f.unit = 'shares'
    AND f.val IS NOT NULL AND f.val <> 'NaN'::numeric AND f.period_end >= %(min_context)s
)
SELECT instrument_id, accession_number, array_agg(val ORDER BY val)
FROM f JOIN (SELECT instrument_id, accession_number, max(period_end) AS pe FROM f GROUP BY 1, 2) m
  USING (instrument_id, accession_number)
WHERE f.period_end = m.pe GROUP BY 1, 2
"""

_INSTRUMENT_SHEET_SQL = """
SELECT instrument_id, count(*) FROM financial_facts_raw
WHERE instrument_id = ANY(%(ids)s) AND taxonomy = 'us-gaap' AND concept = 'CommonStockSharesOutstanding'
GROUP BY 1
"""

_FACTS_SQL = f"""
WITH k AS ({_KEYS})
SELECT DISTINCT f.instrument_id, f.accession_number, f.concept, f.period_start, f.period_end, f.val, f.fiscal_period
FROM financial_facts_raw f JOIN k ON k.instrument_id = f.instrument_id AND k.accession = f.accession_number
WHERE f.taxonomy = 'us-gaap' AND f.val IS NOT NULL AND f.val <> 'NaN'::numeric
  AND f.period_start IS NOT NULL AND f.period_start <= f.period_end
  AND ((f.concept = 'EarningsPerShareBasic' AND f.unit = 'USD/shares')
    OR (f.concept IN ('NetIncomeLossAvailableToCommonStockholdersBasic', 'NetIncomeLoss') AND f.unit = 'USD')
    OR (f.concept = 'WeightedAverageNumberOfSharesOutstandingBasic' AND f.unit = 'shares'))
"""

_STORED_SQL = """
SELECT concept, count(*), count(*) FILTER (WHERE decimals IS NULL)
FROM financial_facts_raw
WHERE taxonomy = 'us-gaap' AND concept = ANY(%(concepts)s)
GROUP BY 1 ORDER BY 1
"""
_STORED_CONCEPTS = [
    EPS, AVAILABLE, NET_INCOME, WA,
    "EarningsPerShareBasicDistributed", "EarningsPerShareBasicUndistributed", "EarningsPerShareDilutedDistributed",
    "EarningsPerShareDilutedUndistributed", "UndistributedEarningsLossAvailableToCommonShareholdersBasic",
]  # fmt: skip


def _finite_positive(value: Decimal) -> bool:
    as_float = float(value)
    return math.isfinite(as_float) and as_float > 0


def agrees(x: float, y: float) -> bool:
    return x > 0 and y > 0 and 1 / TOLERANCE <= x / y <= TOLERANCE


def sheet_status(values: list[Decimal] | None) -> str:
    """Rule 1: the build's balance-sheet comparator for the accession (its latest instant's distinct values)."""
    if not values:
        return "none"
    if len(values) > 1:
        return "ambiguous"
    return "valid" if _finite_positive(values[0]) else "invalid"


def accession_focus(rows: list[Fact]) -> str | None:
    """The accession's fiscal period focus: its one distinct non-blank ``fiscal_period``, else None (unknown)."""
    found = {r.fiscal_period for r in rows if r.fiscal_period}
    return found.pop() if len(found) == 1 else None


def period_status(
    days: int, focus: str | None, operands: dict[str, set[Decimal]]
) -> tuple[str, Decimal | None, Decimal | None]:
    """Rule 4: (status, weighted average, implied) for one period. The penny test is exact decimal arithmetic,
    |N - E * W| <= 0.01 * W, so a boundary of exactly one cent passes; the 100x bounds use float, as production."""
    if focus is None:
        return "focus_unknown", None, None
    if days < SHORT_PERIOD_DAYS.get(focus, 0):
        return "dqc_short_period", None, None
    wa, eps = operands.get(WA, set()), operands.get(EPS, set())
    numerator = operands[AVAILABLE] if AVAILABLE in operands else operands.get(NET_INCOME, set())
    if len(wa) != 1 or len(eps) != 1 or len(numerator) != 1:
        return "operand_count", None, None
    (w,), (e,), (n,) = wa, eps, numerator
    if not all(math.isfinite(float(x)) for x in (w, e, n)) or w <= 0 or e == 0 or n == 0 or (n > 0) != (e > 0):
        return "operand_invalid", None, None
    with localcontext() as context:
        exponents = [int(x.as_tuple().exponent) for x in (w, e, n)]
        context.prec = sum(len(x.as_tuple().digits) for x in (w, e, n)) + max(exponents) - min(exponents) + 4
        within_penny = abs(n - e * w) <= PER_SHARE_TOLERANCE * w
        implied = n / e
    if not within_penny or not math.isfinite(float(implied)):
        return "identity_disagrees", w, implied
    if not agrees(float(w), float(implied)):
        return "identity_disagrees", w, implied
    return "consistent", w, implied


@dataclass(frozen=True)
class Fact:
    concept: str
    start: date
    end: date
    value: Decimal
    fiscal_period: str | None


@dataclass
class Evaluation:
    """The amendment's scale outcome for one cover selection (rules 1-5), with what decided it."""

    eligible: bool
    sheet: str
    outcome: str  # pass / fail / untested; ``n/a`` when not eligible (the balance-sheet check decides)
    focus: str | None = None
    latest_end: date | None = None
    no_period: str | None = None  # why an eligible accession has no period
    periods: list[tuple[date, date, str, Decimal | None, Decimal | None]] = field(default_factory=list)

    @property
    def comparators(self) -> list[Decimal]:
        return [v for _, _, status, w, i in self.periods if status == "consistent" for v in (w, i) if v is not None]


def evaluate(cover: Decimal, accepted_ny: date, sheet_values: list[Decimal] | None, rows: list[Fact]) -> Evaluation:
    """Rules 1-5 for one cover selection. ``rows`` are the accession's facts of the four concepts (any end)."""
    sheet = sheet_status(sheet_values)
    if sheet == "valid" or not _finite_positive(cover):
        return Evaluation(eligible=False, sheet=sheet, outcome="n/a" if sheet == "valid" else "untested")
    ends = [r.end for r in rows if r.concept == WA]
    result = Evaluation(eligible=True, sheet=sheet, outcome="untested", focus=accession_focus(rows))
    if not ends:
        result.no_period = "no_weighted_average"
        return result
    result.latest_end = max(ends)
    if result.latest_end > accepted_ny:
        result.no_period = "end_after_acceptance"
        return result
    by_start: dict[date, dict[str, set[Decimal]]] = defaultdict(lambda: defaultdict(set))
    for r in rows:
        if r.end == result.latest_end:
            by_start[r.start][r.concept].add(r.value)
    for start, operands in sorted(by_start.items()):
        if WA not in operands:
            continue
        days = (result.latest_end - start).days + 1
        status, w, implied = period_status(days, result.focus, operands)
        result.periods.append((start, result.latest_end, status, w, implied))
    values = result.comparators
    if values:
        result.outcome = "pass" if all(agrees(float(cover), float(v)) for v in values) else "fail"
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-id", type=int, required=True)
    parser.add_argument("--trace", type=int, nargs="*", default=[])
    args = parser.parse_args()
    build, trace = args.build_id, set(args.trace)
    params: dict[str, object] = {"build": build, "min_context": MIN_CONTEXT}
    facts: dict[tuple[int, str], list[Fact]] = defaultdict(list)
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        print(f"build {build}; min_context {MIN_CONTEXT}; snapshot at {conn.execute('SELECT now()').fetchone()}")
        for concept, total, null in conn.execute(_STORED_SQL, {"concepts": _STORED_CONCEPTS}):
            print(f"  stored {concept}: {total:,} rows, decimals NULL on {null:,}")
        null_shares = conn.execute(_NULL_SHARES_SQL, params).fetchone()
        accessions = conn.execute(_ACCESSIONS_SQL, params).fetchall()
        sheets = {(i, a): v for i, a, v in conn.execute(_SHEET_SQL, params)}
        for iid, accession, concept, start, end, value, fiscal_period in conn.execute(_FACTS_SQL, params):
            facts[(iid, accession)].append(Fact(concept, start, end, value, fiscal_period))
        traced_sheets = dict(conn.execute(_INSTRUMENT_SHEET_SQL, {"ids": sorted(trace)})) if trace else {}

    keys = Counter((r[0], r[1]) for r in accessions)
    print(
        f"\n{len(keys):,} (instrument, accession) units with a selected count; {sum(n > 1 for n in keys.values())} "
        f"with more than one selected count; {sum(r[7] > 1 for r in accessions)} with more than one stored scale "
        f"outcome. Cover selections with no single count (shares_reported NULL, excluded): {null_shares}"
    )
    eligibility: Counter[tuple[str, str, str]] = Counter()
    tally: dict[str, Counter[tuple[str, ...]]] = defaultdict(Counter)
    statuses: Counter[tuple[str, str]] = Counter()
    gaps: list[int] = []
    listed: dict[tuple[str, ...], list[str]] = defaultdict(list)
    for iid, accession, cover, published, accepted_at, context, stored, _ in accessions:
        unit = (iid, accession)
        cohort = "published" if published else "refused"
        accepted_ny = accepted_at.astimezone(NEW_YORK).date()
        result = evaluate(cover, accepted_ny, sheets.get(unit), facts.get(unit, []))
        scope = "fallback" if result.eligible else f"sheet {result.sheet}"
        label = result.sheet if _finite_positive(cover) else f"{result.sheet}, invalid cover"
        eligibility[(stored, cohort, label)] += 1
        if result.no_period:
            statuses[(scope, result.no_period)] += 1
        c = float(cover)
        found: dict[str, list[float]] = {"wa": [], "implied": [], "rule": [float(v) for v in result.comparators]}
        # The unchecked candidates are scored on every accession, eligible or not, for the disagreement tables.
        probe = result if result.eligible else evaluate(cover, accepted_ny, None, facts.get(unit, []))
        for _, _, status, w, implied in probe.periods:
            statuses[(scope, status)] += 1
            if w is not None and implied is not None:
                found["implied"].append(float(implied))
        for start, end, status, w, implied in probe.periods:
            if status == "consistent" and w is not None and implied is not None and result.eligible:
                gaps.append((context - end).days)
                if not (0.1 <= float(w) / float(implied) <= 10):
                    listed[("consistent eligible period with m > 10",)].append(f"iid={iid} {accession} {start}")
        found["wa"] = [
            float(r.value)
            for r in facts.get(unit, [])
            if r.concept == WA and r.end == probe.latest_end and _finite_positive(r.value)
        ]
        if not result.eligible:
            found["rule"] = [float(v) for v in probe.comparators]
        for name, values in found.items():
            outcome = "none" if not values else "pass" if all(agrees(c, v) for v in values) else "fail"
            tally[name][(scope, stored, cohort, outcome)] += 1
            if outcome == "fail" and name == "rule":
                listed[(name, scope, stored, cohort, outcome)].append(
                    f"iid={iid} {accession} accepted {accepted_ny} cover={c:,.0f} vs {[f'{v:,.0f}' for v in values]}"
                )
        if iid in trace:
            print(
                f"\n[trace] iid={iid} {accession} accepted {accepted_ny} context {context} cover={c:,.0f} {cohort} "
                f"stored={stored} sheet={result.sheet} {sheets.get(unit)} focus={probe.focus} "
                f"(instrument balance-sheet rows, any accession: {traced_sheets.get(iid, 0)}) -> {result.outcome}"
            )
            for start, end, status, w, implied in probe.periods:
                operands = {
                    r.concept.removeprefix("WeightedAverageNumberOf"): f"{r.value:,}"
                    for r in facts[unit]
                    if r.start == start and r.end == end
                }
                print(f"    {start}..{end}: {status}; implied={implied and f'{implied:,.0f}'}; {operands}")
    print("\nrule 1 eligibility: (stored scale outcome, cohort, sheet comparator) -> accessions")
    for key, n in sorted(eligibility.items()):
        print(f"  {key}: {n:,}")
    for name, counts in tally.items():
        print(f"\n{name}: (scope, stored scale outcome, cohort, candidate outcome) -> accessions")
        for key, n in sorted(counts.items()):
            print(f"  {key}: {n:,}")
    print("\nperiod status (scope, status) -> periods or accessions:")
    for key, n in sorted(statuses.items()):
        print(f"  {key}: {n:,}")
    print(f"\ncover context - consistent eligible period end, days: n={len(gaps):,} min={min(gaps)} max={max(gaps)}")
    print(
        f"consistent eligible periods with m(weighted average / implied) > 10: "
        f"{len(listed.get(('consistent eligible period with m > 10',), []))}"
    )
    for key in sorted(listed):
        print(f"\n{key}: {len(listed[key])}")
        for line in listed[key]:
            print(f"  {line}")


if __name__ == "__main__":
    main()
