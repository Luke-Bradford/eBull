"""#3841 slice 3: acceptance for the point-in-time market cap (spec "Acceptance").

Spec: ``docs/specs/fundamentals/2026-10-16-3841-pit-market-cap.md``. Run on the dev DB, from a worktree:

    PYTHONPATH=. uv run python scripts/measure_3841_acceptance.py [--per-instrument PATH]

Read-only, in one REPEATABLE READ snapshot. It rebuilds every in-scope instrument through the build's own reader path
(``pit_market_cap_build.iter_instrument_builds``) for the latest stored build's ``build_date``, without writing, and
prints:

1. **Drift:** instruments whose rebuilt steps differ from the stored build's. Non-zero means an input changed since
   the build (a new filing, bar or 13F row); the gate and coverage figures below are then a mix of the two.
2. **Gate 1** (price basis, Form 4): tested, untested and failed quarters in total; every failed quarter per
   instrument. ``--per-instrument`` writes every instrument's tallies as TSV.
3. **Gate 2** (count magnitude, public float): pass, fail and uncomparable in total; every failure per instrument.
4. **Coverage:** per month, in-scope instruments x NYSE sessions from 2024-07-01 through the build date, reconciled to
   that denominator: published, each view refusal, and each absence. Then the momentum book's formation-1 universe at
   the last session before the build date.
5. **Dated external check:** the panel against companiesmarketcap, +-5% (``REFERENCE`` below).

Absences, for a session with no view row (first match): ``cik_refused:<reason>`` (the whole CIK has no steps);
``out_of_scope_form`` (no step, and a count from an out-of-scope form, e.g. 20-F, would otherwise be eligible);
``no_eligible_count`` (no step otherwise); ``no_close`` (a stored step covers the session, but no bar is stored).
"""

from __future__ import annotations

import argparse
import bisect
import json
import zipfile
from collections import Counter, defaultdict
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final

import psycopg

from app.config import settings
from app.security.master_key import resolve_data_dir
from app.services.factor_panel import MAX_SHARES_AGE_MONTHS, add_months
from app.services.pit_fundamentals import form_family
from app.services.pit_market_cap import IN_SCOPE_FAMILIES, GateOutcome, ShareFact
from app.services.pit_market_cap_build import (
    FIRST_SESSION,
    InstrumentBuild,
    instrument_submissions_reader,
    iter_instrument_builds,
    load_scope,
    nyse_sessions,
    step_row,
)

#: Spec acceptance 4. companiesmarketcap's per-company chart series (``data = [...]`` on ``/<company>/marketcap/``,
#: retrieved 2026-10-16) is WEEKLY: each point is stamped Saturday 00:00 UTC and holds the Friday close, in units of
#: $100k. It has no 2026-09-30 point, so the check uses the two Fridays bracketing it. Values are the company total.
REFERENCE_SOURCE: Final = "companiesmarketcap.com/<company>/marketcap/ weekly chart series, retrieved 2026-10-16"
REFERENCE: Final[dict[str, dict[date, float]]] = {
    "AAPL": {date(2026, 9, 25): 49776369e5, date(2026, 10, 2): 48699319e5},
    "GME": {date(2026, 9, 25): 118003e5, date(2026, 10, 2): 124612e5},
    "MSFT": {date(2026, 9, 25): 38328439e5, date(2026, 10, 2): 38429430e5},
    "JPM": {date(2026, 9, 25): 9119174e5, date(2026, 10, 2): 8835280e5},
    "HD": {date(2026, 9, 25): 2925226e5, date(2026, 10, 2): 2821965e5},
}
REFERENCE_TOLERANCE: Final = 0.05
#: The spec's dated check day; printed beside the bracketing references.
CHECK_DAY: Final = date(2026, 9, 30)
#: Position of ``evidence`` in a step row without ``instrument_id`` and ``build_id``.
EVIDENCE_COLUMN: Final = 9

_LATEST_BUILD_SQL = """
    SELECT build_id, build_date, code_sha, rule_version, summary FROM market_cap_pit_builds
    ORDER BY build_id DESC LIMIT 1
"""
#: Every column ``step_row`` writes except ``build_id``, in its order: drift is any difference in what a rebuild
#: would publish.
_STORED_STEPS_SQL = """
    SELECT instrument_id, valid_from, valid_to, shares_concept, accession, context_date, accepted_at, shares_reported,
           split_factor, refusal, evidence, revision_count, backdated_insert_count, bars_through
    FROM instrument_share_count_pit
    WHERE instrument_id = ANY(%(ids)s::bigint[]) ORDER BY instrument_id, valid_from
"""
#: Reference tickers resolve as the API does (``app/api/instruments.py``): primary listing first, then lowest id.
_REFERENCE_IDS_SQL = """
    SELECT DISTINCT ON (symbol) symbol, instrument_id FROM instruments
    WHERE symbol = ANY(%(symbols)s::text[]) ORDER BY symbol, is_primary_listing DESC, instrument_id ASC
"""
_VIEW_SQL = """
    SELECT instrument_id, price_date, reason FROM instrument_market_cap_pit
    WHERE instrument_id = ANY(%(ids)s::bigint[]) AND price_date BETWEEN %(first)s AND %(last)s
"""


def _comparable(row: Sequence[Any]) -> tuple[Any, ...]:
    """A step row with its evidence as canonical JSON text, so stored jsonb and rebuilt evidence compare and hash."""
    evidence = row[EVIDENCE_COLUMN]
    if isinstance(evidence, str):
        evidence = json.loads(evidence)
    return (*row[:EVIDENCE_COLUMN], json.dumps(evidence, sort_keys=True), *row[EVIDENCE_COLUMN + 1 :])


def absence(result: InstrumentBuild, session: date) -> str:
    """Why a session has no step (see the module docstring)."""
    if result.refused is not None:
        return f"cik_refused:{result.refused}"
    if any(_out_of_scope_eligible(fact, session) for fact in result.facts):
        return "out_of_scope_form"
    return "no_eligible_count"


def _out_of_scope_eligible(fact: ShareFact, session: date) -> bool:
    """``pit_market_cap.eligible`` with the form test inverted."""
    return (
        form_family(fact.form) not in IN_SCOPE_FAMILIES
        and fact.accepted < session
        and add_months(session, -MAX_SHARES_AGE_MONTHS) <= fact.context
    )


def classify_sessions(
    result: InstrumentBuild,
    sessions: list[date],
    stored: list[tuple[date, date]],
    view: dict[date, str | None],
) -> dict[date, str]:
    """One outcome per session: ``published``, the view's reason, or an absence. Stored steps are sorted, disjoint."""
    starts = [valid_from for valid_from, _ in stored]
    out: dict[date, str] = {}
    for session in sessions:
        i = bisect.bisect_right(starts, session) - 1
        covered = i >= 0 and session < stored[i][1]
        if not covered:
            out[session] = absence(result, session)
        elif session not in view:
            out[session] = "no_close"
        else:
            out[session] = view[session] or "published"
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="#3841 slice 3: point-in-time market cap acceptance")
    parser.add_argument("--per-instrument", type=Path, help="write every instrument's gate tallies here (TSV)")
    args = parser.parse_args()

    print(f"as of {datetime.now(UTC).isoformat(timespec='seconds')}")
    submissions_path = resolve_data_dir() / "sec" / "bulk" / "submissions.zip"
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        with conn.transaction(), submissions_path.open("rb") as handle, zipfile.ZipFile(handle) as archive:
            return _report(conn, instrument_submissions_reader(archive), args.per_instrument)


def _report(conn: psycopg.Connection[Any], index_of: Any, per_instrument: Path | None) -> int:
    build = conn.execute(_LATEST_BUILD_SQL).fetchone()
    if build is None:
        print("no build: run market_cap_pit_rebuild first")
        return 1
    build_id, build_date, code_sha, rule_version, summary = build
    print(f"build {build_id}: build_date {build_date}, code {code_sha}, rule_version {rule_version}")

    symbols = dict(conn.execute("SELECT instrument_id, symbol FROM instruments").fetchall())
    scope = load_scope(conn)
    sessions = list(nyse_sessions(FIRST_SESSION, build_date))
    print(f"scope {len(scope)} instruments x {len(sessions)} NYSE sessions {sessions[0]}..{sessions[-1]}")

    drift: dict[int, date] = {}
    gate1_total: Counter[str] = Counter()
    gate2_total: Counter[str] = Counter()
    gate1_failures: list[str] = []
    gate2_failures: list[str] = []
    tested_instruments = 0
    coverage: dict[str, Counter[str]] = defaultdict(Counter)
    outcomes_by_instrument: dict[int, dict[date, str]] = {}
    tsv: list[str] = []

    for batch in iter_instrument_builds(conn, scope=scope, index_of=index_of, build_date=build_date):
        ids = [result.instrument_id for result in batch.instruments]
        stored: dict[int, list[tuple[Any, ...]]] = defaultdict(list)
        for iid, *row in conn.execute(_STORED_STEPS_SQL, {"ids": ids}):
            stored[iid].append(_comparable(row))
        view: dict[int, dict[date, str | None]] = defaultdict(dict)
        for iid, day, reason in conn.execute(_VIEW_SQL, {"ids": ids, "first": sessions[0], "last": sessions[-1]}):
            view[iid][day] = reason

        for result in batch.instruments:
            iid = result.instrument_id
            symbol = symbols.get(iid, str(iid))
            rebuilt = [_comparable(step_row(iid, step, result.stamp, 0)[1:-1]) for step in result.steps]
            # Both row shapes start at valid_from (``step_row`` minus instrument_id; ``_STORED_STEPS_SQL``'s order).
            differ = set(rebuilt) ^ set(stored.get(iid, []))
            if differ:
                drift[iid] = min(step[0] for step in differ)

            quarters = Counter(str(q.outcome) for q in result.gate1.values())
            untested_trades = result.tallies.get("gate1", Counter())["untested_trades"]
            gate1_total.update(quarters)
            gate1_total["untested_trades"] += untested_trades
            if quarters[GateOutcome.PASS] or quarters[GateOutcome.FAIL]:
                tested_instruments += 1
            for q in result.gate1.values():
                if q.outcome is GateOutcome.FAIL:
                    gate1_failures.append(
                        f"  {symbol} q={q.quarter} trades={q.trades} dates={q.dates} median={q.median_ratio:.4f}"
                    )
            outcomes2 = Counter(str(r.outcome) for r in result.gate2)
            gate2_total.update(outcomes2)
            for r in result.gate2:
                if r.outcome is GateOutcome.FAIL and r.cap is not None:
                    gate2_failures.append(
                        f"  {symbol} context={r.context} session={r.session} cap={r.cap / 1e9:,.3f}bn "
                        f"float={r.public_float / 1e9:,.3f}bn ratio={r.cap / r.public_float:.3f}"
                    )
            tsv.append(
                "\t".join(
                    str(v)
                    for v in (
                        iid,
                        symbol,
                        result.refused or "",
                        len(result.steps),
                        quarters[GateOutcome.PASS],
                        quarters[GateOutcome.FAIL],
                        quarters[GateOutcome.UNTESTED],
                        untested_trades,
                        outcomes2[GateOutcome.PASS],
                        outcomes2[GateOutcome.FAIL],
                        outcomes2[GateOutcome.UNCOMPARABLE],
                    )
                )
            )

            outcomes = classify_sessions(
                result, sessions, [(row[0], row[1]) for row in stored.get(iid, [])], view.get(iid, {})
            )
            for session, outcome in outcomes.items():
                coverage[session.strftime("%Y-%m")][outcome] += 1
            outcomes_by_instrument[iid] = outcomes

    print(f"\n== drift: {len(drift)} of {len(scope)} instruments rebuild differently from build {build_id}")
    if drift:
        by_day = Counter(drift.values())
        print("  earliest differing step start: " + ", ".join(f"{d} {n}" for d, n in sorted(by_day.items())[-10:]))
        print("  first: " + ", ".join(symbols.get(i, str(i)) for i in list(drift)[:20]))
    stored_gates = {name: summary.get(name) for name in ("gate1", "gate2")}
    print(f"  build {build_id} summary gates: {stored_gates}")

    print(f"\n== Gate 1 (price basis): {tested_instruments} instruments with a tested quarter")
    print(
        f"  quarters pass {gate1_total[GateOutcome.PASS]} · fail {gate1_total[GateOutcome.FAIL]} · "
        f"untested {gate1_total[GateOutcome.UNTESTED]} · untested trades {gate1_total['untested_trades']}"
    )
    if gate1_failures:
        print(*gate1_failures, sep="\n")
    else:
        print("  no failed quarter")

    print("\n== Gate 2 (count magnitude, one-sided)")
    print(
        f"  pass {gate2_total[GateOutcome.PASS]} · fail {gate2_total[GateOutcome.FAIL]} · "
        f"uncomparable {gate2_total[GateOutcome.UNCOMPARABLE]}"
    )
    if gate2_failures:
        print(*gate2_failures, sep="\n")
    else:
        print("  no failure")

    _print_coverage(coverage, len(scope), sessions)
    _print_formation(conn, outcomes_by_instrument, symbols, max(s for s in sessions if s < build_date))
    misses = _print_external(conn, symbols)

    if per_instrument is not None:
        header = "instrument_id\tsymbol\tcik_refused\tsteps\tg1_pass\tg1_fail\tg1_untested_q\tg1_untested_trades"
        per_instrument.write_text(header + "\tg2_pass\tg2_fail\tg2_uncomparable\n" + "\n".join(tsv) + "\n")
        print(f"\nper-instrument tallies: {per_instrument} ({len(tsv)} rows)")
    # Drift means the figures above mix the rebuild's snapshot with the stored build's: not acceptance evidence.
    return 1 if misses or drift else 0


def _print_coverage(coverage: dict[str, Counter[str]], instruments: int, sessions: list[date]) -> None:
    per_month = Counter(s.strftime("%Y-%m") for s in sessions)
    outcomes = sorted({o for month in coverage.values() for o in month}, key=lambda o: (o != "published", o))
    print("\n== Coverage: in-scope instruments x NYSE sessions, per month")
    print("month    denominator  " + "  ".join(outcomes) + "  reconciled")
    totals: Counter[str] = Counter()
    for month in sorted(per_month):
        denominator = instruments * per_month[month]
        cells = coverage[month]
        totals.update(cells)
        ok = sum(cells.values()) == denominator
        print(f"{month}  {denominator:>11,}  " + "  ".join(f"{cells[o]:>{len(o)},}" for o in outcomes) + f"  {ok}")
    denominator = instruments * len(sessions)
    print(
        f"total    {denominator:>11,}  "
        + "  ".join(f"{totals[o]:>{len(o)},}" for o in outcomes)
        + f"  {sum(totals.values()) == denominator}"
    )


def _print_formation(
    conn: psycopg.Connection[Any],
    outcomes_by_instrument: dict[int, dict[date, str]],
    symbols: dict[int, str],
    latest: date,
) -> None:
    """At the last session before the build date: the 08:20 UTC build runs before that day's session."""
    row = conn.execute(
        "SELECT ARRAY(SELECT jsonb_array_elements_text(universe)::bigint) FROM momentum_book_universes"
        " WHERE formation_id = 1"
    ).fetchone()
    if row is None:
        print("\n== formation 1: no momentum_book_universes row")
        return
    universe: list[int] = row[0]
    names: dict[str, list[str]] = defaultdict(list)
    for iid in universe:
        outcome = outcomes_by_instrument[iid][latest] if iid in outcomes_by_instrument else "not_in_scope"
        names[outcome].append(symbols.get(iid, str(iid)))
    print(f"\n== formation 1 ({len(universe)} names) at {latest}")
    print("  " + " · ".join(f"{o} {len(n)}" for o, n in sorted(names.items(), key=lambda kv: -len(kv[1]))))
    for outcome, symbols_of in sorted(names.items()):
        if outcome != "published":
            print(f"  {outcome}: " + ", ".join(sorted(symbols_of)))


def _print_external(conn: psycopg.Connection[Any], symbols: dict[int, str]) -> int:
    """Each reference against the view's cap. A ``basis_stale`` row is a freshness state, not a value: its step's
    value (close x shares x factor, as the view would compute it) is compared and the staleness printed beside it.
    Any other refusal, or a value outside the tolerance, is a miss."""
    ids = dict(conn.execute(_REFERENCE_IDS_SQL, {"symbols": list(REFERENCE)}).fetchall())
    days = sorted({CHECK_DAY, *(d for refs in REFERENCE.values() for d in refs)})
    rows = conn.execute(
        "SELECT instrument_id, price_date, reason, coalesce(market_cap, CASE WHEN reason = 'basis_stale' THEN"
        " close::double precision * shares_reported::double precision * split_factor END)"
        " FROM instrument_market_cap_pit"
        " WHERE instrument_id = ANY(%(ids)s::bigint[]) AND price_date = ANY(%(days)s::date[])",
        {"ids": list(ids.values()), "days": days},
    ).fetchall()
    caps = {(iid, day): (cap, reason) for iid, day, reason, cap in rows}
    print(f"\n== Dated external check, +-{REFERENCE_TOLERANCE:.0%} ({REFERENCE_SOURCE})")
    misses = 0
    for symbol, refs in REFERENCE.items():
        iid = ids.get(symbol)
        cap, reason = caps.get((iid, CHECK_DAY), (None, "no view row"))
        line = f"  {symbol:<5} {CHECK_DAY}: " + (f"{cap / 1e9:,.2f}bn" if cap is not None else f"refused {reason}")
        line += f" ({reason})" if cap is not None and reason else ""
        for day, reference in sorted(refs.items()):
            cap, reason = caps.get((iid, day), (None, "no view row"))
            if cap is None:
                misses += 1
                line += f" | {day}: MISS refused {reason}"
                continue
            delta = cap / reference - 1
            verdict = "ok" if abs(delta) <= REFERENCE_TOLERANCE else "MISS"
            misses += verdict == "MISS"
            stale = f" ({reason})" if reason else ""
            line += f" | {day}: ours {cap / 1e9:,.2f}bn{stale} ref {reference / 1e9:,.2f}bn {delta:+.2%} {verdict}"
        print(line)
    print(f"  misses: {misses}")
    return misses


if __name__ == "__main__":
    raise SystemExit(main())
